package eu.wohlben.qits.eventstream.control;

import io.smallrye.mutiny.Uni;
import java.time.Duration;
import java.util.Collection;
import java.util.List;

/**
 * The library's real REST clients, opened to the eventstream pact (qits-1149, qits-1163). {@link
 * EventsQuery} and {@link EventPage} are package-private, so this class sits in their package — in
 * the qits-eventstream-contracts jar, a split package on purpose, which is only ever on a test
 * classpath — and hands {@code contracts/EventstreamPact} the exact code {@link CatchupSweeper} and
 * {@link OutboxSweeper} run, pointed at a pact mock server. No CDI: the clients need only their url
 * and a bearer.
 *
 * <p>The bearer is a fixed placeholder (qits-1328). The real one is the consuming service's own
 * {@code quarkus.oidc-client.qits} token, which a pact cannot hold; the interactions pin no request
 * header, so what is sent there is not part of the contract, only that something is — a hand-built
 * client with no bearer fails closed and would send nothing at all.
 */
public final class ContractProbe {

  /** One page as the catch-up reads it: the events and the cursor that says whether more follow. */
  public record Page(List<EventFrame> events, String nextCursor) {}

  /** The catch-up's own page size, so the pact asks what production asks. */
  public static final int PAGE_SIZE = CatchupSweeper.PAGE_SIZE;

  private final EventsQuery query = new EventsQuery();
  private final EventsPublisher publisher = new EventsPublisher();

  /** The placeholder the probe's clients present. Not a token; the mock server reads no header. */
  static final String PLACEHOLDER_AUTHORIZATION = "Bearer contract-probe";

  public ContractProbe(String eventsUrl) {
    query.bearer = () -> Uni.createFrom().item(PLACEHOLDER_AUTHORIZATION);
    publisher.bearer = query.bearer;
    query.eventsUrl = eventsUrl;
    publisher.eventsUrl = eventsUrl;
    publisher.publishTimeout = Duration.ofSeconds(5);
  }

  /** {@code CatchupSweeper.initialize}'s read: the newest matching event, or null. */
  public EventFrame newest(Collection<String> names) {
    return query.newest(names);
  }

  /** {@code CatchupSweeper.catchUp}'s read: the next page after {@code cursor}, oldest first. */
  public Page after(Collection<String> names, String cursor) {
    EventPage page = query.after(names, cursor, PAGE_SIZE);
    return new Page(page.events(), page.nextCursor());
  }

  /** {@code OutboxSweeper.tick}'s (and {@code QitsEventBus.publish}'s) one attempt. */
  public EventsPublisher.Delivery put(String eventId, EventEnvelope envelope) {
    return publisher.put(eventId, envelope);
  }

  /** The PUT body exactly as the publisher sends it. */
  public static String body(EventEnvelope envelope) {
    return CanonicalJson.envelope(envelope);
  }
}
