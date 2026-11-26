package eu.wohlben.qits.eventstream.control;

import eu.wohlben.qits.eventstream.entity.OutboxEvent;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
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
 *
 * <p><b>And a body this process could not build is reported as exactly that, never as an outage</b>
 * (qits-2055). It used to arrive here as {@code UNREACHABLE} and be announced as "qits-events is
 * unreachable", sending whoever read it to the network for a fault that was in the binary. It has
 * its own count and its own line now, and it is not in the unreachable one.
 *
 * <p><b>It stops before the store does</b> (qits-2716). Quarkus closes the {@code eventstream}
 * persistence unit in a shutdown task and stops the scheduler only afterwards, when ArC destroys
 * the beans — so a tick could still fire, and an offloaded one still finish, against a unit that was
 * gone, and every such tick ended as the scheduler's own ERROR "Error occurred while executing task
 * for trigger …OutboxSweeper#tick" around {@code IllegalArgumentException: Unable to find an
 * EntityManagerFactory for persistence unit 'eventstream'}. The 2026-11-26 qits-agents redeploys
 * filed it as error issue {@code 80c048a7cde7493bc661f725b8fbe833}. {@link ShutdownGate} has the
 * whole order; the consequence here is {@link #onStop}: on {@code ShutdownEvent}, while the unit is
 * still open, no new sweep starts, the one in flight stops at its next row, and the observer waits —
 * bounded by {@code qits.eventstream.shutdown-drain} — for it to let go.
 */
@ApplicationScoped
public class OutboxSweeper {

  private static final Logger LOG = Logger.getLogger(OutboxSweeper.class);

  @Inject Outbox outbox;

  @Inject EventsPublisher publisher;

  @Inject Clock clock;

  @Inject ShutdownGate gate;

  @ConfigProperty(name = "qits.eventstream.enabled")
  boolean enabled;

  /**
   * Held for the whole of a sweep, and the thing {@link #onStop} waits on.
   *
   * <p>{@code SKIP} already keeps two <em>ticks</em> apart; this is what lets the shutdown observer
   * see one, and it keeps a direct {@link #sweep()} call out of a tick's way by the same rule: a
   * caller that finds a sweep in flight does not queue behind it, it returns having attempted
   * nothing. Never a blocking {@code lock()} — a sweeper that waits on a holder is the shape the
   * catch-up's 2026-09-08 incident took ({@link CatchupSweeper}).
   */
  private final ReentrantLock sweepLock = new ReentrantLock();

  /**
   * The tick. The cadence is a floor on how late a retry can be, not the retry spacing itself —
   * that is {@link RetrySchedule}, held per row in {@code nextAttemptAt}, so a short cadence costs
   * one query and never sends anything early.
   *
   * <p><b>While the process runs a throw goes exactly where it always went</b> — out, to the
   * scheduler's ERROR. Only once {@link ShutdownGate} is closed is it caught, and said at DEBUG: a
   * sweep cut off by its own process stopping is not a fault anybody can act on, and an ERROR is a
   * statement that somebody should (qits-2716).
   */
  @Scheduled(
      every = "{qits.eventstream.sweep-interval}",
      concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
  void tick() {
    try {
      sweep();
    } catch (RuntimeException e) {
      if (!gate.stopping()) {
        throw e;
      }
      LOG.debugf(e, "the outbox sweep was cut short by shutdown; its rows stay owed");
    }
  }

  /**
   * The {@code ShutdownEvent} observer: close the gate, then give a sweep in flight until what is
   * left of {@code qits.eventstream.shutdown-drain} to reach its next row and stop.
   *
   * <p>Here and not in a {@code @PreDestroy}, because of the order {@link ShutdownGate} records:
   * this is the last moment the {@code eventstream} unit is open, and a {@code @PreDestroy} runs
   * after it has closed. Past the bound it stops waiting and says what it left behind — the row being
   * attempted is still owed, and whatever that sweep says on its way down is DEBUG.
   */
  void onStop(@Observes ShutdownEvent ignored) {
    gate.stop();
    if (!gate.awaitIdle(sweepLock)) {
      LOG.warnf(
          "an outbox sweep was still running when the %s shutdown drain ran out; it was abandoned"
              + " mid-sweep and the rows it had not settled stay owed",
          gate.drain());
    }
  }

  /**
   * Attempt every row that is due now. Returns how many were attempted.
   *
   * <p>Zero, without touching the database, once the process is stopping or while another sweep is
   * in flight. A sweep that is under way when the stop arrives finishes the row it is on and goes no
   * further — each row is its own transaction, so there is nothing half-done to leave behind.
   */
  public int sweep() {
    if (!enabled || gate.stopping() || !sweepLock.tryLock()) {
      return 0;
    }
    try {
      // Asked again under the lock: a stop that landed between the check above and the lock must
      // not be able to slip one more sweep past the observer that is about to wait on it.
      return gate.stopping() ? 0 : sweepLocked();
    } finally {
      sweepLock.unlock();
    }
  }

  private int sweepLocked() {
    Instant now = clock.instant();
    List<OutboxEvent> due = outbox.due(now);
    int attempted = 0;
    int unreachable = 0;
    String silence = null;
    int unserializable = 0;
    String unbuilt = null;
    for (OutboxEvent row : due) {
      if (gate.stopping()) {
        LOG.debugf(
            "the outbox sweep was cut short by shutdown after %d of %d due row(s); the rest stay"
                + " owed",
            attempted, due.size());
        break;
      }
      attempted++;
      // Rebuilt from the stored columns and from nothing else — including parent_id, environment
      // and publisher, all of which the server compares. A parent re-read from the ambient context
      // here would be whatever the SCHEDULER's thread happens to hold, which is never the right
      // answer and is usually null; a tier re-read from config would be whatever the process was
      // reconfigured to since, which is a different request than the one being retried. The same
      // goes for a publisher re-read from config after an override was added or an upgrade moved
      // the default.
      EventsPublisher.Delivery attempt =
          publisher.put(
              row.id,
              new EventEnvelope(
                  row.name,
                  row.occurredAt,
                  row.payload,
                  row.description,
                  row.parentId,
                  row.environment,
                  row.publisher));
      if (attempt.delivered()) {
        outbox.delivered(row.id);
      } else {
        outbox.attemptFailed(row.id, attempt);
        if (attempt.unreachable()) {
          unreachable++;
          silence = attempt.detail();
        } else if (attempt.unserializable()) {
          unserializable++;
          unbuilt = row.id + " (" + row.name + "): " + attempt.detail();
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
          unreachable, attempted, silence);
    }
    if (unserializable > 0) {
      // Its own line, and never folded into the one above: nothing was sent, so nothing about
      // qits-events was learned. The fix is a build, not a wait.
      LOG.warnf(
          "%d of %d due event(s) could not be serialised here and were not sent: this is a"
              + " packaging/programming error (in a native image, missing reflection), not an"
              + " outage; they stay owed until a fixed deployment can serialise them (last: %s)",
          unserializable, attempted, unbuilt);
    }
    if (attempted > 0) {
      LOG.debugf("outbox sweep attempted %d row(s)", attempted);
    }
    return attempted;
  }
}
