package eu.wohlben.qits.eventstream.control;

import eu.wohlben.qits.eventstream.EventStreamBearer;
import eu.wohlben.qits.http.CallerHeader;
import eu.wohlben.qits.http.QitsHttpClients;
import eu.wohlben.qits.http.RetryPolicy;
import io.opentelemetry.api.OpenTelemetry;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

/**
 * The single HTTP call this module makes: {@code PUT {qits.events.url}/events/api/events/{id}} with
 * the envelope. Used twice — once inline by {@link eu.wohlben.qits.eventstream.QitsEventBus} and
 * again by {@link OutboxSweeper} — sending the identical request, which is what makes "a retry is
 * the same request" true rather than merely intended. Only the patience differs: the inline
 * attempt rides out a switchover for a few seconds, the outbox's for the standard budget.
 *
 * <p><b>The four answers are four different futures</b>, and collapsing any pair loses something:
 *
 * <ul>
 *   <li><b>200 / 201</b> — delivered. 201 wrote the row, 200 found it already there with the same
 *       content. A replay is a success, not a conflict; that is the whole point of the client
 *       choosing the UUID.
 *   <li><b>400</b> — rejected, permanently. qits-events answers it when the id exists carrying
 *       <em>different</em> content, i.e. a UUID was reused. Retrying cannot change that, and a row
 *       that kept retrying it would look like an outage.
 *   <li><b>any other status</b> — refused. A 5xx, a proxy's 502, a 404 from a path that is not
 *       there: a response <em>arrived</em> and said no. Something is answering, it understood the
 *       request enough to reject it, and the outbox spends its bounded budget on this class.
 *   <li><b>no response at all</b> — unreachable. A connection refused, a name that does not
 *       resolve, a deadline that passed with nothing on the socket. Nothing was learned about the
 *       event, only about the network, so the outbox <b>never gives up</b> on this class.
 * </ul>
 *
 * <p><b>No bearer counts as unreachable, and nothing is sent (qits-1328).</b> The PUT
 * carries the service's own token from {@link EventStreamBearer} and nothing else says who the
 * caller is: the forwarded user/roles header pair is gone, with no fallback. When no
 * token can be had — no {@code quarkus.oidc-client.qits}, a disabled one, a mint that failed — the
 * request is not made at all, and the attempt is {@link Outcome#UNREACHABLE}: nothing was learned
 * about the event, only about this service's configuration, so the outbox keeps the row and retries
 * at its cap rather than spending the budget a misconfiguration would otherwise exhaust.
 *
 * <p><b>The last two used to be one, and merging them cost real events.</b> Measured on the
 * 2026-08-10 bootstraps: a seed qits-ci was pointed at an alias that did not resolve, every publish
 * raised {@code ConnectException}, and five attempts later every row was {@code FAILED} — a hole in
 * the log that no consumer bookkeeping can recover. A refusal is evidence about the event; an
 * unreachable bus is evidence about nothing at all, and a budget is only meaningful against
 * evidence.
 *
 * <p>The call is <b>synchronous and bounded</b>. Synchronous because the caller has to know which
 * of the three happened before it decides whether to persist anything, and bounded because the
 * publish hook sits on qits-ci's single-threaded run worker: an unbounded wait here would park
 * every pipeline on the instance behind one unreachable service. The request deadline is
 * configurable and short by default for exactly that reason — the price of qits-events being down
 * is a few seconds per green build, paid once, after which the outbox owns the problem.
 */
@ApplicationScoped
public class EventsPublisher {

  private static final Logger LOG = Logger.getLogger(EventsPublisher.class);

  /** The events API's own path under {@code qits.events.url}, which is a bare scheme+host+port. */
  static final String EVENTS_PATH = "/events/api/events/";

  /** What came of one attempt. See the class javadoc for why there are four and not three. */
  public enum Outcome {

    /** 200 or 201. The event is in the log. */
    DELIVERED,

    /** 400. The id exists with different content, and no retry can change that. */
    REJECTED,

    /** A response arrived and it was not a success. Retryable, and it spends the attempt budget. */
    REFUSED,

    /** No response arrived. Retryable, and it spends <b>nothing</b>: the budget is for refusals. */
    UNREACHABLE
  }

  /**
   * An attempt's outcome plus the one line of detail an outbox row records.
   *
   * <p><b>There is deliberately no {@code retryable()}.</b> Two of these four are retryable and they
   * are retried differently — one against a budget, one forever — so a single predicate saying
   * "there will be another attempt" is the exact shape in which the distinction gets collapsed back
   * again by someone reading only the method name. Each outcome has its own question instead.
   */
  public record Delivery(Outcome outcome, String detail) {

    public boolean delivered() {
      return outcome == Outcome.DELIVERED;
    }

    /** Permanently refused: the reused-id 400, and nothing else. */
    public boolean rejected() {
      return outcome == Outcome.REJECTED;
    }

    /** A response arrived saying no. This is the class the attempt budget bounds. */
    public boolean refused() {
      return outcome == Outcome.REFUSED;
    }

    /** Nothing answered. Retried indefinitely, at the retry schedule's cap. */
    public boolean unreachable() {
      return outcome == Outcome.UNREACHABLE;
    }
  }

  /**
   * An <b>instance</b> field, not a static one: a static {@code HttpClient} is created at image
   * build time and native-image refuses the heap it lands in. {@code @ApplicationScoped} keeps it
   * one client per process. Same constraint {@code CdBuildNotifier} documents.
   *
   * <p><b>Pinned to HTTP/1.1, and that is not a preference.</b> The JDK client defaults to HTTP/2
   * with an {@code h2c} upgrade, and an upgrade that carries a request body delivers that body
   * <em>twice</em>: measured against the test stub, one PUT arrived once through the server's
   * upgrade handler and again as an HTTP/2 data frame ninety milliseconds later. Idempotency made
   * the duplicate harmless — the second delivery is a 200 replay, which is exactly what the design
   * is for — but it is a wasted round trip on every single publish, and it is the kind of thing
   * that is invisible until something counts requests. qits-events speaks plain HTTP/1.1 on
   * qits-net; there is nothing to upgrade to.
   *
   * <p>Wrapped through {@link QitsHttpClients} (qits-1198): a PUT to qits-events is a
   * service-to-service call like any other, so it opens a CLIENT span and carries {@code
   * X-Qits-Caller} beside the bearer. The caller header is diagnostic only; the bearer — the
   * service's own, from {@link EventStreamBearer} (qits-1328) — is what qits-events authorizes on,
   * and both the inline and the outbox attempt carry it.
   *
   * <p>The wrapper also applies qits-http-core's switchover retry (qits-428), with nothing configured
   * here: this call is a PUT, idempotent, so a connection that failed before sending, a reset before
   * the response, and a {@code 502}/{@code 503}/{@code 504} carrying {@code Retry-After} are all
   * sent again within the policy's budget — the whole of it for {@link #put}, the inline budget for
   * {@link #putInline}. A request timeout is not. The outcome below is classified
   * from the last attempt, exactly as from a single one; README, "Crossing the edge".
   *
   * <p>Do not widen that policy here. A {@code POST} would be resent only when the connection failed
   * before sending, which is the policy's rule; this call is retried more because it is a {@code
   * PUT} to a client-chosen id, not because this class asked for more.
   */
  private final HttpClient client;

  /** Who the PUT says it is. Package-private and settable for the hand-built path, see below. */
  EventStreamBearer bearer;

  /**
   * The same raw client for the INLINE attempt, under the same policy with its budget cut to what
   * {@value #INLINE_RETRY_BUDGET} sets, five seconds by default (qits-428). The inline attempt sits on the publishing service's
   * own thread, and the outbox is the durable path: waiting the standard thirty seconds there would
   * break {@code publish}'s promise not to block for long, and buy nothing the sweeper does not.
   * The outbox's own resend keeps {@link #client} and the whole budget, because nobody waits on it.
   */
  private final HttpClient inlineClient;

  /** The inline client's retry budget, never more than the outbox client's. */
  private final Duration inlineRetryBudget;

  /** The key that sets the inline attempt's retry budget. */
  static final String INLINE_RETRY_BUDGET = "qits.eventstream.publish-retry-budget";

  /** Its shipped default, which is also the hand-built publisher's. */
  static final Duration DEFAULT_INLINE_RETRY_BUDGET = Duration.ofSeconds(5);

  /**
   * For a consumer that builds this class by hand instead of letting CDI inject it, as several
   * consumer-pact probes across the estate do — {@code new EventsPublisher()}, then a
   * package-private field set directly. Kept public and working: no span, no caller header, the
   * standard retry policy (which never retries a loopback target) for {@link #put}, and the default
   * inline budget for {@link #putInline}.
   *
   * <p>A hand-built publisher has <b>no bearer</b> and so sends nothing until {@link #bearer} is set
   * (as the contracts kit's {@code ContractProbe} does): failing closed is the only default there is.
   */
  public EventsPublisher() {
    this(RetryPolicy.standard(), DEFAULT_INLINE_RETRY_BUDGET);
  }

  @Inject
  EventsPublisher(
      QitsHttpClients clients,
      Instance<OpenTelemetry> openTelemetry,
      EventStreamBearer bearer,
      @ConfigProperty(name = INLINE_RETRY_BUDGET, defaultValue = "PT5S") Duration inlineRetryBudget) {
    // RetryPolicy.fromConfig() is the policy the injected bean itself carries, so the inline client
    // differs from the outbox one in its budget and nothing else.
    this(
        clients,
        openTelemetry.isResolvable() ? openTelemetry.get() : OpenTelemetry.noop(),
        CallerHeader.value().orElse(null),
        RetryPolicy.fromConfig(),
        inlineRetryBudget,
        bearer);
  }

  /**
   * A hand-built publisher on {@code policy}, for a test whose stand-in edge needs its own. Like the
   * public one it has no bearer until {@link #bearer} is set.
   */
  EventsPublisher(RetryPolicy policy, Duration inlineRetryBudget) {
    this(
        QitsHttpClients.of(OpenTelemetry.noop(), null, policy),
        OpenTelemetry.noop(),
        null,
        policy,
        inlineRetryBudget,
        Unbuilt.BEARER);
  }

  private EventsPublisher(
      QitsHttpClients clients,
      OpenTelemetry openTelemetry,
      String caller,
      RetryPolicy policy,
      Duration inlineRetryBudget,
      EventStreamBearer bearer) {
    this.bearer = bearer;
    HttpClient raw =
        HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .connectTimeout(Duration.ofSeconds(2))
            .build();
    this.client = clients.wrap(raw);
    this.inlineRetryBudget =
        inlineRetryBudget.compareTo(policy.budget()) < 0 ? inlineRetryBudget : policy.budget();
    this.inlineClient =
        QitsHttpClients.wrap(
            raw, openTelemetry, caller, policy.withBudget(this.inlineRetryBudget));
  }

  @ConfigProperty(name = "qits.events.url")
  String eventsUrl;

  @ConfigProperty(name = "qits.eventstream.publish-timeout", defaultValue = "PT5S")
  Duration publishTimeout;

  /**
   * The outbox's attempt at delivering one event, under the standard retry budget. Never throws.
   */
  public Delivery put(String eventId, EventEnvelope envelope) {
    return send(client, eventId, envelope);
  }

  /**
   * The inline attempt, on the publisher's own thread: the same request, giving up on a switchover
   * once the budget {@value #INLINE_RETRY_BUDGET} sets is spent, so the outbox can own the event. Never throws.
   */
  public Delivery putInline(String eventId, EventEnvelope envelope) {
    return send(inlineClient, eventId, envelope);
  }

  /** The inline attempt's retry budget, as configured and capped. */
  Duration inlineRetryBudget() {
    return inlineRetryBudget;
  }

  private Delivery send(HttpClient via, String eventId, EventEnvelope envelope) {
    URI target = URI.create(base() + EVENTS_PATH + eventId);
    String authorization;
    try {
      authorization = bearer.authorization().await().atMost(publishTimeout);
    } catch (RuntimeException e) {
      // Fail closed: no token, no request. The bearer has already said why (and which client) at
      // WARN; the row records it too, so `status` on an outbox row explains itself.
      return new Delivery(Outcome.UNREACHABLE, "not sent: " + e.getMessage());
    }
    try {
      HttpRequest request =
          HttpRequest.newBuilder(target)
              .timeout(publishTimeout)
              .header("Content-Type", "application/json")
              .header("Authorization", authorization)
              .PUT(
                  HttpRequest.BodyPublishers.ofString(
                      CanonicalJson.envelope(envelope), StandardCharsets.UTF_8))
              .build();
      HttpResponse<String> response = via.send(request, HttpResponse.BodyHandlers.ofString());
      return classify(eventId, response);
    } catch (InterruptedException e) {
      // Restore the flag and treat it as "nothing came back": the caller is a worker being asked to
      // stop, so no answer was reached, and the event is worth exactly as much after the restart.
      Thread.currentThread().interrupt();
      return new Delivery(Outcome.UNREACHABLE, "interrupted");
    } catch (Exception e) {
      // Every throw out of send() is "no response": a refused connection, an unresolvable name, a
      // deadline that passed. A status code is the only thing that makes an attempt a refusal, and
      // there is none here.
      LOG.debugf("PUT %s failed: %s", target, e.toString());
      return new Delivery(Outcome.UNREACHABLE, e.toString());
    }
  }

  private Delivery classify(String eventId, HttpResponse<String> response) {
    int status = response.statusCode();
    if (status == 200 || status == 201) {
      LOG.debugf("event %s delivered (%d)", eventId, status);
      return new Delivery(Outcome.DELIVERED, null);
    }
    String detail = status + " " + firstLine(response.body());
    if (status == 400) {
      // The id exists with different content. The caller reused a UUID, which is a bug in the
      // publisher and not a condition of the network.
      LOG.warnf("event %s rejected as a reused id: %s", eventId, detail);
      return new Delivery(Outcome.REJECTED, detail);
    }
    return new Delivery(Outcome.REFUSED, detail);
  }

  /** {@code qits.events.url} is specified without a path; a trailing slash is tolerated rather than doubled. */
  private String base() {
    String url = eventsUrl.trim();
    return url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
  }

  private static String firstLine(String body) {
    if (body == null || body.isBlank()) {
      return "";
    }
    String line = body.strip().lines().findFirst().orElse("");
    return line.length() > 200 ? line.substring(0, 200) : line;
  }
}
