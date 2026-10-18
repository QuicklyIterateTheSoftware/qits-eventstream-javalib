package eu.wohlben.qits.eventstream.control;

import eu.wohlben.qits.eventstream.entity.OutboxEvent;
import eu.wohlben.qits.eventstream.entity.OutboxStatus;
import eu.wohlben.qits.eventstream.persistence.OutboxRepository;
import io.quarkus.narayana.jta.QuarkusTransaction;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

/**
 * The outbox's state transitions, and the only place they happen.
 *
 * <p><b>Every write is its own transaction.</b> {@link #enqueue} is called from inside whatever the
 * publishing service was doing — for qits-ci, from the SUCCESS transition — and an outbox row that
 * vanished because the caller's transaction rolled back afterwards would be a durability guarantee
 * that holds only when nothing goes wrong. {@code requiringNew} suspends the caller's transaction
 * and commits this row on its own, which is the entire reason the mechanism is worth having.
 *
 * <p>A delivered row is <b>deleted</b> rather than marked. The outbox is what has not arrived; the
 * record of what has is qits-events, which is the point of publishing to it.
 *
 * <p><b>Giving up is bounded by refusals, never by unreachability.</b> {@code
 * qits.eventstream.max-attempts} counts attempts that got an HTTP response saying no; an attempt
 * that got no response at all is rescheduled without spending any of it, so a bus that is down is
 * retried for as long as it is down — at the retry schedule's five-minute cap once the curve is
 * walked out. "The bus is the record" is only true if reaching it is never abandoned, and this is
 * where that sentence is either kept or broken.
 */
@ApplicationScoped
public class Outbox {

  private static final Logger LOG = Logger.getLogger(Outbox.class);

  /** One sweep's bite. Large enough that a real backlog drains, small enough to bound one tick. */
  static final int SWEEP_BATCH = 100;

  @Inject OutboxRepository rows;

  @Inject Clock clock;

  /** The <b>refusal</b> budget. An attempt that got no answer does not draw on it. */
  @ConfigProperty(name = "qits.eventstream.max-attempts", defaultValue = "5")
  int maxAttempts;

  /**
   * Record an event the inline {@code PUT} did not deliver. The attempt just made counts, so the
   * row starts at one and the sweeper's first try is the second attempt.
   */
  public void enqueue(String eventId, EventEnvelope envelope, EventsPublisher.Delivery attempt) {
    QuarkusTransaction.requiringNew()
        .run(
            () -> {
              OutboxEvent row = new OutboxEvent();
              row.id = eventId;
              row.name = envelope.name();
              row.occurredAt = envelope.occurredAt();
              row.payload = envelope.payload();
              row.description = envelope.description();
              // The envelope is stored WHOLE, and the parent and the tier are part of it: both
              // participate in qits-events' idempotency comparison, so a retry that lost either
              // would contradict the attempt it is retrying.
              row.parentId = envelope.parentId();
              row.environment = envelope.environment();
              row.attempts = 1;
              apply(row, attempt);
              rows.persist(row);
            });
  }

  /** The rows the sweeper may attempt at {@code now}. */
  public List<OutboxEvent> due(Instant now) {
    return QuarkusTransaction.requiringNew().call(() -> rows.due(now, SWEEP_BATCH));
  }

  /** It arrived. Nothing left to keep. */
  public void delivered(String eventId) {
    QuarkusTransaction.requiringNew()
        .run(
            () -> {
              rows.deleteById(eventId);
              LOG.debugf("outbox row %s delivered on retry", eventId);
            });
  }

  /** It did not arrive. Count the attempt and decide whether there is another one. */
  public void attemptFailed(String eventId, EventsPublisher.Delivery attempt) {
    QuarkusTransaction.requiringNew()
        .run(
            () -> {
              OutboxEvent row = rows.findById(eventId);
              if (row == null) {
                return;
              }
              row.attempts++;
              apply(row, attempt);
            });
  }

  /**
   * The one decision this class makes, shared by the inline path and the sweeper so the two cannot
   * drift: a rejection is terminal at once, an exhausted <em>refusal</em> budget is terminal, and
   * anything else gets a next time at {@link RetrySchedule}'s spacing.
   *
   * <p><b>The two counters are two different questions and that is the whole of this method.</b>
   * {@code attempts} counts every try and is what the backoff is spaced by, so an unreachable bus
   * still walks the curve out to the five-minute cap instead of hammering a dead socket every
   * second. {@code refusals} counts only the tries that got an answer, and it is the only one {@code
   * max-attempts} bounds — because a refusal is evidence about this event, while a connection that
   * was never made is evidence about the network and says nothing about whether the event is
   * deliverable.
   */
  private void apply(OutboxEvent row, EventsPublisher.Delivery attempt) {
    row.lastError = truncate(attempt.detail());
    if (attempt.rejected()) {
      giveUp(row, "is unretryable");
      return;
    }
    if (attempt.refused()) {
      row.refusals++;
      if (row.refusals >= maxAttempts) {
        giveUp(row, "gave up");
        return;
      }
    }
    row.status = OutboxStatus.PENDING;
    row.nextAttemptAt = clock.instant().plus(RetrySchedule.outboxBackoff(row.attempts));
    LOG.debugf(
        "event %s (%s) attempt %d failed, next at %s: %s",
        row.id, row.name, row.attempts, row.nextAttemptAt, row.lastError);
  }

  /** The end of the line for one row. Both ways there are a WARN: nothing else will mention it. */
  private void giveUp(OutboxEvent row, String what) {
    row.status = OutboxStatus.FAILED;
    row.nextAttemptAt = null;
    LOG.warnf(
        "event %s (%s) %s after %d attempts (%d refused): %s",
        row.id, row.name, what, row.attempts, row.refusals, row.lastError);
  }

  private static String truncate(String detail) {
    if (detail == null) {
      return null;
    }
    return detail.length() > 1024 ? detail.substring(0, 1024) : detail;
  }
}
