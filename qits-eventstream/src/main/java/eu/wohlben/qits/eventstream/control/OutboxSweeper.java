package eu.wohlben.qits.eventstream.control;

import eu.wohlben.qits.eventstream.entity.OutboxEvent;
import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

/**
 * Re-attempts what the inline publish could not deliver.
 *
 * <p>Runs on the scheduler's own thread, never on a caller's, so a qits-events that is down costs
 * the publishing service nothing after the first bounded attempt. {@code ConcurrentExecution.SKIP}
 * matters more than it looks: a tick that is still working through a backlog when the next one
 * fires must not have a second thread re-attempting the same rows, because two in-flight {@code
 * PUT}s of one id is the one situation the server's idempotency was not designed to arbitrate.
 *
 * <p><b>It stamps nothing.</b> A sweep re-sends a <em>stored</em> envelope, causation included, so
 * the one path in this module that could have gone wrong about a parent cannot: the value was fixed
 * when the envelope was built and is read back off the row. That is the property the outbox's
 * {@code parent_id} column exists to protect.
 *
 * <p>{@link #sweep()} is public and returns what it did, which is how the retry schedule is tested:
 * a test moves its clock to the row's {@code nextAttemptAt} and calls this, instead of sleeping
 * through eighty seconds of real backoff.
 *
 * <p><b>An unreachable bus is reported here, once per sweep.</b> Nothing gives up on it any more
 * (see {@link Outbox}), so without a periodic notice the failure it replaced — a give-up WARN per
 * event — would have become no notice at all. One line per sweep rather than one per row: a bus that
 * is down is a single condition however many events are queued behind it.
 */
@ApplicationScoped
public class OutboxSweeper {

  private static final Logger LOG = Logger.getLogger(OutboxSweeper.class);

  @Inject Outbox outbox;

  @Inject EventsPublisher publisher;

  @Inject Clock clock;

  @ConfigProperty(name = "qits.eventstream.enabled")
  boolean enabled;

  /**
   * The tick. The cadence is a floor on how late a retry can be, not the retry spacing itself —
   * that is {@link RetrySchedule}, held per row in {@code nextAttemptAt}, so a short cadence costs
   * one query and never sends anything early.
   */
  @Scheduled(
      every = "{qits.eventstream.sweep-interval}",
      concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
  void tick() {
    sweep();
  }

  /** Attempt every row that is due now. Returns how many were attempted. */
  public int sweep() {
    if (!enabled) {
      return 0;
    }
    Instant now = clock.instant();
    List<OutboxEvent> due = outbox.due(now);
    int unreachable = 0;
    String silence = null;
    for (OutboxEvent row : due) {
      // Rebuilt from the stored columns and from nothing else — including parent_id and
      // environment, both of which the server compares. A parent re-read from the ambient context
      // here would be whatever the SCHEDULER's thread happens to hold, which is never the right
      // answer and is usually null; a tier re-read from config would be whatever the process was
      // reconfigured to since, which is a different request than the one being retried.
      EventsPublisher.Delivery attempt =
          publisher.put(
              row.id,
              new EventEnvelope(
                  row.name,
                  row.occurredAt,
                  row.payload,
                  row.description,
                  row.parentId,
                  row.environment));
      if (attempt.delivered()) {
        outbox.delivered(row.id);
      } else {
        outbox.attemptFailed(row.id, attempt);
        if (attempt.unreachable()) {
          unreachable++;
          silence = attempt.detail();
        }
      }
    }
    if (unreachable > 0) {
      // ONE WARN PER SWEEP, not one per event. A bus that is down is a single condition however many
      // events are waiting behind it, and the give-up WARN that used to be the only notice of this
      // is gone by design — nothing gives up any more, so this is the only thing that will say the
      // log is falling behind. It rate-limits itself as the outage lengthens: rows come due at the
      // backoff, so once the curve is walked out this is five-minutely.
      LOG.warnf(
          "qits-events is unreachable: %d of %d due event(s) got no answer and will keep being"
              + " retried (%s)",
          unreachable, due.size(), silence);
    }
    if (!due.isEmpty()) {
      LOG.debugf("outbox sweep attempted %d row(s)", due.size());
    }
    return due.size();
  }
}
