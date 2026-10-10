package eu.wohlben.qits.eventstream.control;

import java.time.Duration;
import java.util.Collection;
import java.util.List;

/**
 * The library's real REST clients, opened to the consumer pact (qits-1149). {@link EventsQuery} and
 * {@link EventPage} are package-private, so this test-only class sits in their package and hands
 * {@code contracts/EventsConsumerPactTest} the exact code {@link CatchupSweeper} and {@link
 * OutboxSweeper} run, pointed at a pact mock server. No CDI: the clients need only their url.
 */
public final class ContractProbe {

  /** One page as the catch-up reads it: the events and the cursor that says whether more follow. */
  public record Page(List<EventFrame> events, String nextCursor) {}

  /** The catch-up's own page size, so the pact asks what production asks. */
  public static final int PAGE_SIZE = CatchupSweeper.PAGE_SIZE;

  private final EventsQuery query = new EventsQuery();
  private final EventsPublisher publisher = new EventsPublisher();

  public ContractProbe(String eventsUrl) {
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
