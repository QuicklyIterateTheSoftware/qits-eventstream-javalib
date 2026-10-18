package eu.wohlben.qits.eventstream.control;

import eu.wohlben.qits.eventstream.entity.ConsumerWatermark;
import eu.wohlben.qits.eventstream.entity.OutboxEvent;
import eu.wohlben.qits.eventstream.persistence.ConsumedEvents;
import eu.wohlben.qits.eventstream.persistence.ConsumerWatermarkRepository;
import eu.wohlben.qits.eventstream.persistence.OutboxRepository;
import io.quarkus.narayana.jta.QuarkusTransaction;
import jakarta.inject.Inject;
import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;

/**
 * Base for the eventstream {@code @QuarkusTest}s: empties all three tables outside the test's own
 * transaction, forgets what the stub saw, and puts the clock back at a known instant — so every
 * test starts from the same slate (the {@code CiTestSupport} pattern one module over).
 *
 * <p>The consumer tables are wiped here rather than in the one suite that fills them because a
 * watermark outliving its test is the worst available leak: the next test's "brand-new consumer"
 * would silently be an old one, and it would pass for the wrong reason.
 */
public abstract class EventstreamTestSupport {

  /** Where every test's clock starts. Fixed, so an assertion can name an instant rather than a delta. */
  protected static final Instant T0 = Instant.parse("2026-07-31T12:00:00Z");

  @Inject protected OutboxRepository outboxRows;
  @Inject protected ConsumedEvents consumedEvents;
  @Inject protected ConsumerWatermarkRepository watermarks;
  @Inject protected TestClock clock;

  @BeforeEach
  void resetEventstreamState() {
    QuarkusTransaction.requiringNew()
        .run(
            () -> {
              outboxRows.deleteAll();
              consumedEvents.deleteAll();
              watermarks.deleteAll();
            });
    StubEventsServer.reset();
    clock.set(T0);
  }

  /** Where a durable consumer stands, read outside any transaction of the test's own. */
  protected ConsumerWatermark watermark(String consumerId) {
    return QuarkusTransaction.requiringNew().call(() -> watermarks.findById(consumerId));
  }

  /** How many claims a durable consumer is holding. */
  protected long claims(String consumerId) {
    return QuarkusTransaction.requiringNew().call(() -> consumedEvents.countFor(consumerId));
  }

  /** Whether a durable consumer has claimed one event. */
  protected boolean claimed(String consumerId, String eventId) {
    return QuarkusTransaction.requiringNew().call(() -> consumedEvents.isHandled(consumerId, eventId));
  }

  /**
   * Read a row back the way the sweeper wrote it: in its own transaction, so the read really goes
   * to the database rather than to whatever this thread's persistence context still remembers from
   * before {@code Outbox}'s own {@code requiringNew} committed.
   */
  protected OutboxEvent row(String eventId) {
    return QuarkusTransaction.requiringNew().call(() -> outboxRows.findById(eventId));
  }

  /** How many events are still undelivered. Zero is the healthy number. */
  protected long outboxCount() {
    return QuarkusTransaction.requiringNew().call(() -> outboxRows.count());
  }
}
