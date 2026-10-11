package eu.wohlben.qits.eventstream.control;

import eu.wohlben.qits.eventstream.QitsRawEventListener;
import eu.wohlben.qits.http.QitsHttpClients;
import io.opentelemetry.api.OpenTelemetry;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.StringJoiner;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

/**
 * Reading the log back — the other half of {@link EventsPublisher}, and the machinery behind
 * catch-up.
 *
 * <p>Two questions, both answered by {@code GET {qits.events.url}/events/api/events}:
 *
 * <ul>
 *   <li>{@link #after} — the next page of events <b>strictly after</b> a cursor, oldest first. This
 *       is what a watermark is paged forward with, and it is why the log grew an {@code order=asc}
 *       mode: descending walks away from a watermark, so a consumer would have to read all of
 *       history back to its own position and reverse it.
 *   <li>{@link #newest} — the single newest matching event, which is where a consumer that has never
 *       run starts. Descending with {@code limit=1}, the reading the route has always answered.
 * </ul>
 *
 * <p>The cursor is the composite {@code <occurredAt>,<id>} the log hands out, and it is composite
 * because sibling events published by one pipeline run share that run's finish instant: a scalar
 * cursor either repeats a sibling or drops one, and those are exactly the rows a release train is
 * read for.
 *
 * <p><b>A failure is {@link Unavailable} and never an empty page.</b> The distinction is
 * load-bearing: an empty page means the consumer has reached the head of the log and its watermark
 * may advance, while an unreachable log means nothing at all was learned and the watermark must stay
 * where it is. Returning an empty list for both would let one outage settle every event that
 * occurred during it.
 *
 * <p>The client is pinned to HTTP/1.1 and is an instance field for the two reasons {@link
 * EventsPublisher} spells out — a body-carrying {@code h2c} upgrade delivers twice, and a static
 * {@code HttpClient} is built into a native image's heap.
 */
@ApplicationScoped
public class EventsQuery {

  private static final Logger LOG = Logger.getLogger(EventsQuery.class);

  /** The list route under {@code qits.events.url}, which is a bare scheme+host+port. */
  static final String EVENTS_PATH = "/events/api/events";

  /**
   * The read deadline. A constant rather than a config key, like the connect timeout beside it:
   * nothing waits on this call — it runs on the scheduler's thread — so the only thing the number
   * has to do is stop a hung read from holding the next tick out, and no environment has a reason to
   * want a different one.
   */
  private static final Duration READ_TIMEOUT = Duration.ofSeconds(10);

  /** The log could not be read this time. Not an answer about events, an answer about the network. */
  public static class Unavailable extends RuntimeException {
    Unavailable(String message) {
      super(message);
    }

    Unavailable(String message, Throwable cause) {
      super(message, cause);
    }
  }

  /**
   * Wrapped through {@link QitsHttpClients} (qits-1198): a read from qits-events is a
   * service-to-service call like any other, so it opens a CLIENT span and carries {@code
   * X-Qits-Caller} on top of the two headers below, which stay exactly as they were — that pair is
   * the forward-auth principal qits-events authorizes on, and the caller header is diagnostic only.
   */
  private final HttpClient client;

  /**
   * For a consumer that builds this class by hand instead of letting CDI inject it, as several
   * consumer-pact probes across the estate do — {@code new EventsQuery()}, then a package-private
   * field set directly. Kept public and working rather than broken by this change: equivalent to
   * {@code EventsQuery(QitsHttpClients.of(OpenTelemetry.noop(), null))}, no span, no caller
   * header, the raw client's behaviour exactly.
   */
  public EventsQuery() {
    this(QitsHttpClients.of(OpenTelemetry.noop(), null));
  }

  @Inject
  EventsQuery(QitsHttpClients clients) {
    this.client =
        clients.wrap(
            HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofSeconds(2))
                .build());
  }

  @ConfigProperty(name = "qits.events.url")
  String eventsUrl;

  /**
   * The next page of events after {@code cursor}, oldest first.
   *
   * @param names the event names to filter by; {@code "*"} anywhere in it means no filter at all
   * @param cursor the composite {@code <occurredAt>,<id>} to resume after, or null to start at the
   *     oldest row there is
   */
  EventPage after(Collection<String> names, String cursor, int limit) {
    StringBuilder query = new StringBuilder("?order=asc&limit=").append(limit);
    appendNames(query, names);
    if (cursor != null && !cursor.isBlank()) {
      query.append("&cursor=").append(encode(cursor));
    }
    return CanonicalJson.page(get(query.toString()));
  }

  /**
   * The newest event matching {@code names}, or null when the log holds none.
   *
   * <p>Where a consumer that has never run begins: at the head, not at the epoch. Null is the empty
   * log, and the two are the same place — a consumer with nothing behind it has nothing to skip.
   */
  EventFrame newest(Collection<String> names) {
    StringBuilder query = new StringBuilder("?limit=1");
    appendNames(query, names);
    List<EventFrame> events = CanonicalJson.page(get(query.toString())).events();
    return events == null || events.isEmpty() ? null : events.get(0);
  }

  /**
   * The {@code ?name=A,B} filter, which is one comma-separated parameter rather than a repeated one
   * — the log's own spelling. {@code "*"} is not a name the log knows; it means "no filter", so the
   * parameter is left off entirely.
   */
  private static void appendNames(StringBuilder query, Collection<String> names) {
    if (names == null || names.isEmpty() || names.contains(QitsRawEventListener.ALL)) {
      return;
    }
    StringJoiner joined = new StringJoiner(",");
    for (String name : names) {
      if (name != null && !name.isBlank()) {
        joined.add(name.trim());
      }
    }
    if (joined.length() > 0) {
      query.append("&name=").append(encode(joined.toString()));
    }
  }

  private String get(String query) {
    URI target = URI.create(base() + EVENTS_PATH + query);
    HttpResponse<String> response;
    try {
      HttpRequest request =
          HttpRequest.newBuilder(target)
              .timeout(READ_TIMEOUT)
              .header("Accept", "application/json")
              .header("X-Qits-User", "qits-eventstream")
              .header("X-Qits-Roles", "qits:system")
              .GET()
              .build();
      response = client.send(request, HttpResponse.BodyHandlers.ofString());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new Unavailable("interrupted reading " + target);
    } catch (Exception e) {
      throw new Unavailable("could not read " + target + ": " + e, e);
    }
    if (response.statusCode() != 200) {
      // A 400 here is this client's bug — a malformed cursor, an order the log does not know — and
      // a 5xx is the log's. Neither is an answer about events, so both leave the watermark alone.
      throw new Unavailable(target + " answered " + response.statusCode());
    }
    LOG.debugf("read %s", target);
    return response.body();
  }

  /** As in {@link EventsPublisher}: the configured url is a bare origin, and a trailing slash is tolerated. */
  private String base() {
    String url = eventsUrl.trim();
    return url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
  }

  private static String encode(String value) {
    return URLEncoder.encode(value, StandardCharsets.UTF_8);
  }
}
