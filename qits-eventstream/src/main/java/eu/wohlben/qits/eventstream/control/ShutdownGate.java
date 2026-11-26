package eu.wohlben.qits.eventstream.control;

import jakarta.enterprise.context.ApplicationScoped;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * The one "this process is stopping" both sweepers read, and the one drain deadline they share.
 *
 * <p><b>Why the sweepers need to be told at all</b> (qits-2716). Quarkus 3.40.1 shuts down in three
 * steps, read out of its jars rather than its docs: (1) {@code ShutdownEvent} observers fire, with
 * every persistence unit still open — {@link EventStreamSubscriber#onStop} already closes the stream
 * there; (2) shutdown tasks run last-registered-first, and Hibernate's {@code JPAConfig.shutdown()}
 * closes the persistence units, this jar's {@code eventstream} unit among them; (3) only then does
 * ArC destroy the beans, and only there does {@code SimpleScheduler}'s {@code @PreDestroy stop()}
 * call {@code shutdownNow()}. So between (2) and (3) {@code @Scheduled} triggers still fire, and a
 * tick that was already offloaded runs to completion, against a persistence unit that is gone. Every
 * one of them died in {@code findById} / {@code due} with {@code IllegalArgumentException: Unable
 * to find an EntityManagerFactory for persistence unit 'eventstream'} — one ERROR per durable
 * listener per catch-up tick, and one scheduler ERROR per outbox tick. That is what the 2026-11-26
 * qits-agents redeploys put on the board as error issues {@code 9e7060829da85e4cbf7a96c68814e162}
 * and {@code 80c048a7cde7493bc661f725b8fbe833}: noise on every redeploy, and noise of the one
 * severity that is supposed to mean something.
 *
 * <p>So the gate is closed in step (1), by each sweeper's own {@code ShutdownEvent} observer, and
 * after that nothing starts a sweep and a sweep in flight stops at its next row or page. Step (1)
 * is the only moment that works: it is the last one in which the store is still there to finish a
 * page against.
 *
 * <p><b>One deadline for both, not one each.</b> The observers run one after the other on the
 * shutdown thread, so two independent {@code qits.eventstream.shutdown-drain} waits would add up —
 * ten seconds by default, which is the whole of docker's default stop grace before its SIGKILL. The
 * first {@link #stop()} fixes the deadline and the second sweeper waits out only what is left of it.
 *
 * <p>Package-private and nobody else's: a consumer has no reason to ask, and {@link #resume()} exists
 * only so a test that pressed the button can give the next test a running module back.
 */
@ApplicationScoped
class ShutdownGate {

  @ConfigProperty(name = "qits.eventstream.shutdown-drain", defaultValue = "PT5S")
  Duration drain;

  private volatile boolean stopping;

  /** {@link System#nanoTime()} past which nobody waits any more. Meaningful only while stopping. */
  private volatile long deadline;

  /** Close the gate, and start the drain clock if this is the first call. Idempotent. */
  synchronized void stop() {
    if (!stopping) {
      deadline = System.nanoTime() + drain.toNanos();
      stopping = true;
    }
  }

  /** Whether the process is stopping, which is the whole of what a sweep has to ask between rows. */
  boolean stopping() {
    return stopping;
  }

  /**
   * Wait, bounded by what is left of the shared deadline, for whoever holds {@code inFlight} to let
   * go — and hand it straight back, because holding it was never the point: with the gate closed
   * nobody who takes it after us will sweep.
   *
   * <p>Real time, never the injected {@code Clock}: this is a wait a process's stop is actually
   * spending, and a test clock that never moves would make it either instant or endless.
   *
   * @return true if the holder finished in time (or there was none); false if the wait ran out or
   *     was interrupted, and the caller should say what it abandoned
   */
  boolean awaitIdle(Lock inFlight) {
    long left = Math.max(0, deadline - System.nanoTime());
    try {
      if (inFlight.tryLock(left, TimeUnit.NANOSECONDS)) {
        inFlight.unlock();
        return true;
      }
      return false;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    }
  }

  /** The configured bound, for the line that says it was spent. */
  Duration drain() {
    return drain;
  }

  /** Open the gate again. Tests only: a real process does not come back from its shutdown. */
  synchronized void resume() {
    stopping = false;
    deadline = 0;
  }
}
