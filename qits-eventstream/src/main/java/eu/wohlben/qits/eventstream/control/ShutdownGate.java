package eu.wohlben.qits.eventstream.control;

import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.interceptor.Interceptor;
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
 * <p><b>A per-instance flag was not enough</b> (qits-2716, second finding). Step (3) above — ArC
 * destroying this and every other {@code @ApplicationScoped} bean — is not necessarily the end of
 * the story either. Measured on qits-edge 2026.1126.183436 at 2026-11-27T04:03:35Z: a scheduler tick
 * fired 2.5 seconds AFTER every {@code ShutdownEvent} observer had already returned, went {@code
 * OutboxSweeper_ClientProxy.tick()} ({@code OutboxSweeper.java:97}) → {@code sweep()} ({@code :139})
 * → {@code sweepLocked()} ({@code :147}), and read {@code gate.stopping()} as {@code false}. The
 * client proxy is what explains it: Quarkus' scheduler keeps calling through the proxy for as long
 * as the trigger is registered, and ArC answers a call to a destroyed {@code @ApplicationScoped}
 * bean's proxy by creating a NEW instance — a new {@code OutboxSweeper}, and, because it is
 * {@code @Inject}ed rather than carried over, a new {@code ShutdownGate} too, with a fresh,
 * never-stopped {@code stopping}. That fresh gate is what let the tick past its own guard, straight
 * into a fresh {@code JPAConfig} that has no persistence units to offer — the same {@code
 * IllegalArgumentException: Unable to find an EntityManagerFactory for persistence unit
 * 'eventstream'} as before, now arriving even though the gate had, correctly, been closed once.
 * {@code stopping} and {@code deadline} are therefore {@code static}: whichever instance ArC hands
 * back to the scheduler, old or freshly re-created, reads the one process-wide flag, and nothing
 * re-creates a bean's way past it. The flag is reset only by {@link #onStart}, i.e. by a fresh
 * {@code StartupEvent} — a real process never gets one after it starts stopping, so in production
 * this is one-directional; what actually fires it a second time is a {@code @QuarkusTest} profile
 * restarting the application in the same JVM, which is also why {@link #onStart} exists at all.
 *
 * <p>Package-private and nobody else's: a consumer has no reason to ask, and {@link #resume()} exists
 * so a test that pressed the button can give the next test a running module back — and so {@link
 * #onStart} has something to call when a process actually does start again.
 */
@ApplicationScoped
class ShutdownGate {

  @ConfigProperty(name = "qits.eventstream.shutdown-drain", defaultValue = "PT5S")
  Duration drain;

  private static volatile boolean stopping;

  /** {@link System#nanoTime()} past which nobody waits any more. Meaningful only while stopping. */
  private static volatile long deadline;

  /** The lock {@link #stop()} and {@link #resume()} share, so every instance sees one decision. */
  private static final Object LOCK = ShutdownGate.class;

  /** Close the gate, and start the drain clock if this is the first call. Idempotent. */
  void stop() {
    synchronized (LOCK) {
      if (!stopping) {
        deadline = System.nanoTime() + drain.toNanos();
        stopping = true;
      }
    }
  }

  /**
   * Re-open the gate on a fresh {@code StartupEvent}, so an application started again in the same
   * JVM — a {@code @QuarkusTest} profile restart, chiefly — begins open rather than inheriting the
   * previous run's stop. {@code PLATFORM_BEFORE} is deliberately early: {@link
   * CatchupSweeper#onStart} reads {@link #stopping()} as part of deciding whether to fire its own
   * startup catch-up thread, and that decision must see this reset having already happened rather
   * than racing it — CDI gives no ordering guarantee between two unrelated beans' observers of the
   * same event without one.
   */
  void onStart(@Observes @Priority(Interceptor.Priority.PLATFORM_BEFORE) StartupEvent ignored) {
    resume();
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

  /**
   * Open the gate again. Tests only, chiefly — a real process does not come back from its own
   * shutdown — plus {@link #onStart}, which is the one caller a restarted-in-place process has.
   */
  void resume() {
    synchronized (LOCK) {
      stopping = false;
      deadline = 0;
    }
  }
}
