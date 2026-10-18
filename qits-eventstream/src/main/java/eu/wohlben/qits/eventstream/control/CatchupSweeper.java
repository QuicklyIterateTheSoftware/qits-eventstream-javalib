package eu.wohlben.qits.eventstream.control;

import eu.wohlben.qits.eventstream.QitsDurableEventListener;
import eu.wohlben.qits.eventstream.entity.ConsumerWatermark;
import eu.wohlben.qits.eventstream.persistence.ConsumedEvents;
import eu.wohlben.qits.eventstream.persistence.ConsumerWatermarkRepository;
import io.quarkus.narayana.jta.QuarkusTransaction;
import io.quarkus.runtime.StartupEvent;
import io.quarkus.scheduler.Scheduled;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

/**
 * Reads the log forward for every {@link QitsDurableEventListener}, from its watermark to the head.
 *
 * <p>This is the cure for the thing the stream cannot do. A frame broadcast while a consumer is
 * down, restarting or mid-cutover is gone from the socket's point of view; it is still in
 * qits-events' log, and this pages it back out. Hence the two moments it runs: <b>on a schedule</b>,
 * so a dropped connection costs at most {@code qits.eventstream.catchup-interval}, and <b>once at
 * startup</b>, which is the cutover case exactly.
 *
 * <p>Per listener, the loop is short and every line of it is a decision:
 *
 * <ul>
 *   <li><b>No watermark yet?</b> Initialize it at the newest matching event — consume-from-now.
 *       Replaying all of history into a consumer that has never run is never the default; a
 *       {@link QitsDurableEventListener#replayFromEpoch()} listener is the opt-in, and continues
 *       paging from the epoch in this same invocation.
 *   <li><b>Page ascending from the watermark</b>, and run every row through {@link DurableFunnel} —
 *       the same funnel the live stream uses, so the claim rows dedupe whatever the stream already
 *       delivered.
 *   <li><b>Advance the watermark only when a page is processed in full.</b> A handler that threw
 *       leaves the sweep where it stood, so the event is offered again next time instead of being
 *       read past.
 *   <li><b>Prune</b> the claims the watermark has left behind by more than the horizon. Below the
 *       watermark an event is settled forever, so its claim protects against nothing.
 * </ul>
 *
 * <p><b>Live frames never advance the watermark</b>, and that asymmetry is the design rather than an
 * omission: a frame is ahead of the watermark by definition, and treating it as progress would read
 * past everything between the two. The claim row is what stops the sweep from handling it twice when
 * it eventually gets there.
 *
 * <p>{@link #catchUp()} remains the scheduler-compatible, count-only seam. An application whose
 * readiness depends on a projection being rebuilt calls {@link #catchUp(String)} instead: its
 * {@link CatchupResult} distinguishes reaching the head from an unavailable log or unfinished
 * work. Both entry points are serialized, so an application's bootstrap run and the scheduled
 * safety net cannot page one watermark concurrently.
 *
 * <h2>A stalled sweep cannot stall forever</h2>
 *
 * <p><b>Serializing the sweeps was right; doing it with {@code synchronized} cost six hours.</b> On
 * 2026-09-08 qits-deployments self-deployed at 00:10:33. Its {@code eventstream-catchup-startup}
 * virtual thread entered {@code catchUp()}, went down {@code read(consumerId)} →
 * {@code ConsumerWatermarkRepository.findById} → Agroal's {@code borrowValidation} →
 * {@code PgConnection.isValid}, and parked in a PostgreSQL socket read that never returned — a
 * half-open TCP connection left behind by the blue-green cutover overlap, with no JDBC socket
 * timeout to end it. At 00:11:45 Narayana's reaper aborted the transaction
 * (ARJUNA012117/012095/012381, the stack ending at {@code CatchupSweeper.read} /
 * {@code catchUp} / {@code catchUp}) and the thread did not move, because <b>a reaper abort does not
 * unpark a socket read</b>. The monitor was held until 06:30. Zero events were consumed in between,
 * every release of that window silently failed to deploy, and every health check stayed green. Only
 * a process restart healed it.
 *
 * <p>So the monitor is gone and there is a {@link ReentrantLock} with a watchdog in front of it:
 *
 * <ul>
 *   <li><b>Nothing waits on the lock without first checking the holder.</b> {@link #claim} tryLocks,
 *       and on failure runs {@link #watch()} <em>before</em> deciding anything — the tick is the
 *       watchdog's heartbeat, and a heartbeat that skipped straight to "skip" is exactly what let
 *       the incident run.
 *   <li><b>A holder past {@code qits.eventstream.sweep-stall-budget} is named in an ERROR and
 *       interrupted.</b> Measured on Temurin 25.0.4.1: a <em>virtual</em> thread parked in a
 *       blocking {@code java.net.Socket} read is freed by {@code Thread.interrupt()} — it unblocked
 *       in 1ms with {@code java.net.SocketException: Closed by interrupt}, with the interrupt status
 *       still set. A <em>platform</em> thread is not: it was still blocked after five seconds,
 *       because blocking-mode {@code SocketInputStream} reads are not interruptible off a virtual
 *       thread. The incident's stalled thread was virtual, so the interrupt is a real cure for it.
 *       The {@code @Scheduled} tick runs on a platform thread, and a tick that wedged the same way
 *       is <b>not</b> freed — for that one the ERROR plus {@link CatchupHealthCheck} going DOWN is
 *       the signal, and it is the signal that was missing. The complementary real cure, a JDBC
 *       {@code socketTimeout} on the datasource, is not part of this change.
 *   <li><b>A long sweep is not a stalled sweep.</b> A consumer draining a day of backlog holds the
 *       lock for as long as that takes and is alive while it does; only the budget being exceeded
 *       makes it the incident's shape.
 * </ul>
 *
 * <p>{@link #census()} is the whole of what any of that is visible as, and {@link
 * CatchupHealthCheck} is its one reader in this jar.
 */
@ApplicationScoped
public class CatchupSweeper {

  private static final Logger LOG = Logger.getLogger(CatchupSweeper.class);

  /**
   * One page's bite. A constant rather than a config key, like {@code Outbox.SWEEP_BATCH}: it is
   * bounded by the log's own {@code limit} cap of 1000, and the loop pages until it reaches the
   * head, so this changes how many round trips a backlog costs and nothing that is visible.
   */
  static final int PAGE_SIZE = 200;

  /**
   * How long a patient caller waits on the lock before it runs the watchdog again. A slice rather
   * than an open-ended wait: the point of waiting here is not merely to get in eventually, it is to
   * keep asking whether the holder is still alive, and a caller parked forever on the lock is one
   * more thread the incident would have swallowed silently.
   */
  private static final long WATCH_SLICE_MILLIS = 1000;

  @Inject EventDispatcher dispatcher;

  @Inject EventsQuery events;

  @Inject DurableFunnel funnel;

  @Inject ConsumerWatermarkRepository watermarks;

  @Inject ConsumedEvents consumed;

  @Inject Clock clock;

  @ConfigProperty(name = "qits.eventstream.enabled")
  boolean enabled;

  @ConfigProperty(name = "qits.eventstream.catchup-at-startup", defaultValue = "true")
  boolean catchUpAtStartup;

  @ConfigProperty(name = "qits.eventstream.prune-horizon", defaultValue = "P1D")
  Duration pruneHorizon;

  @ConfigProperty(name = "qits.eventstream.catchup-interval", defaultValue = "PT30S")
  Duration catchupInterval;

  @ConfigProperty(name = "qits.eventstream.sweep-stall-budget", defaultValue = "PT5M")
  Duration stallBudget;

  /**
   * What {@code synchronized} used to be, and the reason for the change is the whole point: a
   * monitor can only be waited on, never asked about. A lock can be tryLocked, which is what lets a
   * caller that could not get in look at who has it instead of joining the queue behind them.
   */
  private final ReentrantLock sweepLock = new ReentrantLock();

  /** The sweep that holds {@link #sweepLock}, or null. Written by the holder, read by everyone. */
  private final AtomicReference<Sweep> inFlight = new AtomicReference<>();

  /** When a FULL sweep last got through every listener. Null until one has, in this process. */
  private final AtomicReference<Instant> lastCompletedAt = new AtomicReference<>();

  /**
   * When this process started counting. The startup-grace anchor: with nothing completed yet, "how
   * long since the last completed sweep" is measured from here, so a service booting into a real
   * backlog has a whole horizon to finish its first sweep before anybody calls it stale.
   */
  private volatile Instant observingSince;

  /**
   * One sweep holding the lock.
   *
   * @param what what the sweep is, in the words the ERROR log will use
   * @param thread the holder, which is what the watchdog interrupts
   * @param startedAt when it took the lock
   * @param struckAt when the watchdog last interrupted it, null until it has
   */
  private record Sweep(String what, Thread thread, Instant startedAt, Instant struckAt) {

    /** The same sweep, recorded as having just been struck. */
    Sweep struck(Instant at) {
      return new Sweep(what, thread, startedAt, at);
    }
  }

  /** Start the startup grace at the first moment this bean exists rather than at the epoch. */
  @PostConstruct
  void observing() {
    observingSince = clock.instant();
  }

  /**
   * The scheduled tick. {@code SKIP} still forbids two concurrent sweeps, for the reason the
   * outbox's has it: a sweep still paging through a backlog when the next one fires must not have a
   * second thread offering the same rows — the claim would hold, but the work would be done twice.
   *
   * <p><b>And precisely BECAUSE {@code SKIP} means a blocked tick is the last tick</b> — Quarkus
   * fires no further one while this invocation has not returned — the tick must never block. It
   * tryLocks; if a sweep is already in flight it runs the watchdog over that holder and returns
   * without sweeping. That is the inversion the 2026-09-08 incident bought: the tick's job when it
   * cannot sweep is to be the thing that notices, and a tick queued behind a parked thread notices
   * nothing and is never called again.
   */
  @Scheduled(
      every = "{qits.eventstream.catchup-interval}",
      concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
  void tick() {
    if (!enabled) {
      return;
    }
    sweepAll("the scheduled sweep", false);
  }

  /**
   * Take the sweep lock, or say why not.
   *
   * <p><b>The watchdog runs before the decision to skip, never after it.</b> A caller that finds the
   * lock held is the only observer the holder has, so looking at the holder is the first thing it
   * does; only then does an impatient caller (the tick) give up, and a patient one (an explicit
   * {@code catchUp}) wait one slice and look again. A patient caller therefore cannot be wedged
   * silently either: it re-runs the watchdog every second for as long as it waits.
   *
   * @param what what this sweep is, for the log the watchdog may have to write about it
   * @param patient whether to wait for the lock at all
   */
  private boolean claim(String what, boolean patient) {
    while (true) {
      if (sweepLock.tryLock()) {
        inFlight.set(new Sweep(what, Thread.currentThread(), clock.instant(), null));
        return true;
      }
      watch();
      if (!patient) {
        return false;
      }
      try {
        if (sweepLock.tryLock(WATCH_SLICE_MILLIS, TimeUnit.MILLISECONDS)) {
          inFlight.set(new Sweep(what, Thread.currentThread(), clock.instant(), null));
          return true;
        }
      } catch (InterruptedException e) {
        // Somebody interrupted the waiter rather than the holder — a shutdown, or this thread being
        // the holder of some other stall. Restoring the flag is what lets the caller above act on
        // it; swallowing it here would make this method the place an interrupt goes to die.
        Thread.currentThread().interrupt();
        LOG.warnf("%s was interrupted while waiting for the sweep lock; it did not run", what);
        return false;
      }
    }
  }

  /**
   * Look at whoever holds the lock and, if they have held it past the budget, say so and interrupt
   * them.
   *
   * <p><b>This runs OUTSIDE the lock, and it is the only thing here that does.</b> A watchdog that
   * needed the lock to look at the lock's holder is not a watchdog.
   *
   * <p>The compare-and-set is what bounds both the log volume and the re-interrupt cadence to one
   * per stall budget: whoever swaps the struck stamp in writes the line and delivers the interrupt,
   * and everybody else that same minute returns silently. A holder that was already struck is
   * measured from the strike, so an interrupt that did not free it — a platform thread in a
   * blocking socket read, which is measurably not interruptible — is retried once per budget rather
   * than once per tick.
   */
  private void watch() {
    Sweep held = inFlight.get();
    if (held == null) {
      // Released in the meantime. The common case by far, and it is not news.
      return;
    }
    Instant now = clock.instant();
    Instant reference = held.struckAt() == null ? held.startedAt() : held.struckAt();
    if (now.isBefore(reference.plus(stallBudget))) {
      // Long, but inside its budget. A sweep draining a real backlog is alive by definition and
      // killing it would turn one recovery into a loop of them.
      return;
    }
    if (!inFlight.compareAndSet(held, held.struck(now))) {
      return;
    }
    LOG.errorf(
        "%s has held the catch-up sweep on thread %s for %s, past the %s stall budget;"
            + " interrupting it — nothing has been consumed since it started",
        held.what(),
        held.thread().getName(),
        Duration.between(held.startedAt(), now),
        stallBudget);
    held.thread().interrupt();
  }

  /**
   * Give the lock back.
   *
   * <p><b>The order is the contract.</b> The holder is cleared before the unlock, so the next thread
   * in can never see a stale holder and report a sweep that has already finished as stalled.
   *
   * <p>{@code Thread.interrupted()} rather than {@code isInterrupted()} on purpose: it <b>clears</b>
   * the flag. The thread that ran this sweep is very often not ours to keep — the scheduler's is
   * pooled — and an interrupt left set would be carried into whatever unrelated task that thread
   * picks up next, which is a second incident wearing the first one's clothes.
   */
  private void release(String what) {
    inFlight.set(null);
    sweepLock.unlock();
    if (Thread.interrupted()) {
      LOG.warnf(
          "%s ended interrupted; the interrupt is cleared here rather than carried onward", what);
    }
  }

  /**
   * What the module is doing right now, as a stated fact rather than an inference from the process
   * still being up. See {@link SweepCensus} for why that distinction cost six hours.
   */
  public SweepCensus census() {
    Sweep held = inFlight.get();
    return new SweepCensus(
        enabled,
        dispatcher.durableListeners().size(),
        clock.instant(),
        held == null ? null : held.what(),
        held == null ? null : held.thread().getName(),
        held == null ? null : held.startedAt(),
        lastCompletedAt.get(),
        observingSince,
        stallBudget,
        stalenessHorizon());
  }

  /**
   * How long the module may go without a completed sweep before that absence is itself the fault.
   *
   * <p>Three intervals, so an ordinary skipped tick and a slow sweep are both inside it and only a
   * pattern is outside — and never below the stall budget, because a sweep that is still legally
   * running must not be able to make the module stale while it does.
   */
  private Duration stalenessHorizon() {
    Duration threeIntervals = catchupInterval.multipliedBy(3);
    return threeIntervals.compareTo(stallBudget) > 0 ? threeIntervals : stallBudget;
  }

  /**
   * The startup run — the cutover cure, and the reason a restart is not a hole.
   *
   * <p><b>On its own thread, because boot must not wait for it.</b> An unreachable qits-events would
   * otherwise add a connect timeout per listener to every start, and a real backlog would add the
   * time it takes to drain. Nothing about the application depends on the sweep having finished.
   *
   * <p>{@code qits.eventstream.catchup-at-startup=false} turns it off, which is what a test suite
   * wants: a sweep landing behind a test is the same kind of non-determinism {@code
   * quarkus.scheduler.enabled=false} already keeps out, and a suite drives {@link #catchUp()}
   * itself.
   *
   * <p><b>This thread is not special, and that is the fix.</b> It takes the same lock every other
   * entry point takes and is watched by the same watchdog — which matters because it was this exact
   * thread, named {@code eventstream-catchup-startup}, that parked in a socket read on 2026-09-08
   * and held the old monitor for six hours. A start is when a pool is coldest and a cutover's
   * half-open connections are freshest, so the startup sweep is the likeliest one to wedge, not the
   * least.
   */
  void onStart(@Observes StartupEvent ignored) {
    if (!enabled || !catchUpAtStartup || dispatcher.durableListeners().isEmpty()) {
      return;
    }
    Thread.ofVirtual().name("eventstream-catchup-startup").start(this::catchUp);
  }

  /**
   * Catch every durable listener up to the head of the log. Returns how many events were handled.
   *
   * <p>This is the established count-only API. Keep callers that only want the scheduled behavior
   * on it; a bootstrap gate needs {@link #catchUp(String)}'s stronger outcome.
   *
   * <p><b>Patient, where the tick is not.</b> A caller that asked for a sweep still gets one and
   * waits for the lock if it has to — but it cannot be wedged silently while it waits, because it
   * re-runs the watchdog over the holder once a second for as long as the wait lasts.
   */
  public int catchUp() {
    if (!enabled) {
      return 0;
    }
    return sweepAll("a full catch-up", true);
  }

  /**
   * Every durable listener, once, under the sweep lock.
   *
   * <p><b>A listener that failed does not stop this counting as a completed sweep</b>, and that is a
   * decision rather than an oversight. A handler throwing on a poison event leaves that event owed
   * forever — this library has no dead letter, deliberately — so treating a failure as "the module
   * is not consuming" would put a consumer DOWN for as long as one bad event sits in front of one
   * listener, which is exactly the wrong instance to take out of rotation. Only <b>the loop not
   * finishing</b> is that: an interrupt from the watchdog, which means a thread was parked
   * somewhere it could not come back from on its own.
   */
  private int sweepAll(String what, boolean patient) {
    if (!claim(what, patient)) {
      return 0;
    }
    try {
      int handled = 0;
      for (QitsDurableEventListener listener : dispatcher.durableListeners()) {
        if (Thread.currentThread().isInterrupted()) {
          LOG.errorf(
              "%s was interrupted by the stall watchdog and is abandoning the remaining listeners;"
                  + " every event it did not reach is still owed",
              what);
          break;
        }
        handled += catchUp(listener).handled();
      }
      if (!Thread.currentThread().isInterrupted()) {
        lastCompletedAt.set(clock.instant());
      }
      return handled;
    } finally {
      release(what);
    }
  }

  /**
   * Catch one named durable consumer up to the log head and report whether that actually happened.
   *
   * <p>Use the stable {@link QitsDurableEventListener#consumerId()} rather than a bean class name:
   * it is the same id the watermark belongs to and survives a listener refactoring. The call is
   * serialized with scheduled and all-consumer sweeps, patiently — it waits for the lock and runs
   * the stall watchdog over the holder every second while it does.
   *
   * <p><b>It does not record a completed sweep, however it ends.</b> {@code lastCompletedAt} is what
   * says the module as a whole is consuming, and one named consumer reaching its head says nothing
   * about the others: a health check that accepted this as liveness would report a module green on
   * the strength of the one listener somebody happened to gate a bootstrap on.
   */
  public CatchupResult catchUp(String consumerId) {
    if (!enabled) {
      return incomplete(consumerId);
    }
    if (!claim("a named catch-up for " + consumerId, true)) {
      return incomplete(consumerId);
    }
    try {
      QitsDurableEventListener listener = named(consumerId);
      return listener == null ? failed(consumerId) : catchUp(listener);
    } finally {
      release("a named catch-up for " + consumerId);
    }
  }

  /**
   * Rebuild one projection from the start of qits-events' log.
   *
   * <p>This is intentionally narrower than a normal sweep. It is only legal for a listener that
   * explicitly returns true from {@link QitsDurableEventListener#replayFromEpoch()}; that written
   * opt-in is what makes clearing its persisted claim and watermark history safe. The two rows are
   * removed in one transaction, the epoch watermark is written, and this invocation then pages to
   * the head. No second scheduler tick is needed to make the rebuilt projection usable.
   *
   * <p>The operation serializes with all sweeps and takes {@link DurableFunnel}'s exclusive gate,
   * so a live frame cannot claim an event between the ledger reset and the replay. It deliberately
   * replays effects, so call it only when the listener's own projection store has been reset or
   * when its handler is an idempotent replacement.
   */
  public CatchupResult rebuildFromEpoch(String consumerId) {
    if (!enabled) {
      return incomplete(consumerId);
    }
    String what = "an epoch rebuild of " + consumerId;
    if (!claim(what, true)) {
      return incomplete(consumerId);
    }
    try {
      QitsDurableEventListener listener = named(consumerId);
      if (listener == null) {
        return failed(consumerId);
      }
      if (!listener.replayFromEpoch()) {
        LOG.errorf(
            "durable consumer %s refused an epoch rebuild: replayFromEpoch() is not enabled",
            consumerId);
        return failed(consumerId);
      }
      // The funnel's delivery lock is taken INSIDE the sweep lock, which is the same order an
      // ordinary sweep takes them in — offer() takes the delivery lock while its caller already
      // holds the sweep lock. One order everywhere is what keeps a second lock from being a second
      // way to hang.
      return funnel.exclusively(
          () -> {
            try {
              QuarkusTransaction.requiringNew()
                  .run(
                      () -> {
                        consumed.forget(consumerId);
                        watermarks.forget(consumerId);
                        watermarks.put(consumerId, Instant.EPOCH, null);
                      });
              return catchUp(listener);
            } catch (RuntimeException e) {
              LOG.errorf(e, "could not reset durable consumer %s for an epoch rebuild", consumerId);
              return failed(consumerId);
            }
          });
    } finally {
      release(what);
    }
  }

  private QitsDurableEventListener named(String consumerId) {
    if (consumerId == null || consumerId.isBlank()) {
      LOG.error("a named catch-up needs a durable consumerId");
      return null;
    }
    List<QitsDurableEventListener> matches =
        dispatcher.durableListeners().stream()
            .filter(listener -> consumerId.equals(listener.consumerId()))
            .toList();
    if (matches.size() != 1) {
      LOG.errorf(
          "named catch-up for %s found %d durable listeners; exactly one is required",
          consumerId, matches.size());
      return null;
    }
    return matches.getFirst();
  }

  private CatchupResult catchUp(QitsDurableEventListener listener) {
    String consumerId = listener.consumerId();
    if (consumerId == null || consumerId.isBlank()) {
      LOG.errorf(
          "durable listener %s has no consumerId; it cannot be caught up",
          listener.getClass().getName());
      return failed(consumerId);
    }
    Set<String> names = listener.signatures();
    if (names == null || names.isEmpty()) {
      // Wants nothing, so there is nothing to be behind on — and no watermark is written, which
      // keeps a listener that has not made its mind up yet out of the tables entirely. It also
      // cannot certify a projection bootstrap: there is no event vocabulary to prove caught up.
      return incomplete(consumerId);
    }

    try {
      ConsumerWatermark mark = read(consumerId);
      if (mark == null) {
        // This writes the replay cursor and intentionally falls through. In particular a
        // replay-from-epoch listener must not need a second scheduler tick before its projection
        // exists; the same invocation starts paging from the epoch below.
        initialize(listener, consumerId, names);
        mark = read(consumerId);
      }

      int handled = 0;
      String cursor = cursorOf(mark);
      while (true) {
        if (Thread.currentThread().isInterrupted()) {
          // The watchdog struck between pages. Nothing is half-committed by stopping here: the
          // watermark only ever moves on a whole page, which is already the rule, so everything
          // above it is simply still owed and the next sweep reads the same rows.
          LOG.errorf(
              "catch-up for %s was killed by the stall watchdog at %s; the events above it stay owed",
              consumerId, cursor);
          return failed(consumerId, handled);
        }
        EventPage page = events.after(names, cursor, PAGE_SIZE);
        List<EventFrame> rows = page.events();
        if (rows == null || rows.isEmpty()) {
          if (page.nextCursor() != null) {
            // An empty non-final page cannot establish a position: continuing would have no safe
            // cursor and accepting it as head would lie to a bootstrap gate.
            LOG.warnf("catch-up for %s received an empty non-final page", consumerId);
            return incomplete(consumerId, handled);
          }
          prune(consumerId);
          return reachedHead(consumerId, handled);
        }
        for (EventFrame frame : rows) {
          DurableFunnel.Result result = funnel.offer(listener, frame);
          if (Thread.currentThread().isInterrupted()) {
            // Asked before the FAILED branch below, because when both are true the interrupt is the
            // cause and the FAILED is the symptom — a handler unparked by the watchdog throws, and
            // "the sweep was killed" is the line an incident needs to read.
            LOG.errorf(
                "catch-up for %s was killed by the stall watchdog on %s; the watermark stays at %s"
                    + " and that event is still owed",
                consumerId, frame.id(), cursor);
            return failed(consumerId, handled);
          }
          if (result == DurableFunnel.Result.FAILED) {
            // The page is not processed, so the watermark does not move. Everything before this row
            // on this page is claimed and will be skipped next time; this row is owed.
            LOG.warnf(
                "catch-up for %s stopped at %s; the watermark stays at %s",
                consumerId, frame.id(), cursor);
            return failed(consumerId, handled);
          }
          if (result == DurableFunnel.Result.HANDLED) {
            handled++;
          }
        }
        EventFrame last = rows.get(rows.size() - 1);
        cursor = advance(consumerId, last);
        if (page.nextCursor() == null) {
          // The log says this was the last page. A full page is NOT the signal — that is the one
          // thing a reader of this route must not infer for itself.
          prune(consumerId);
          return reachedHead(consumerId, handled);
        }
      }
    } catch (EventsQuery.Unavailable unreachable) {
      // One line per listener per sweep, and the sweep is thirty-secondly: the log being
      // unreachable is a condition rather than an event, and nothing is lost by it — the
      // watermark stayed where it was and the next sweep reads the same rows.
      LOG.warnf("catch-up for %s could not read the log: %s", consumerId, unreachable.getMessage());
      return unavailable(consumerId);
    } catch (RuntimeException e) {
      if (Thread.currentThread().isInterrupted()) {
        // This is the path the incident's own failure takes once the interrupt has freed it: a
        // SocketException: Closed by interrupt out of the wedged read, wrapped by Agroal and
        // Hibernate into an unchecked exception by the time it reaches here. Not a listener's
        // fault, and nothing was settled — the event stays owed, as it does for every other throw.
        LOG.errorf(
            e,
            "catch-up for %s was killed by the stall watchdog mid-read; the event stays owed",
            consumerId);
        return failed(consumerId);
      }
      LOG.errorf(e, "catch-up failed for %s", listener.getClass().getName());
      return failed(consumerId);
    }
  }

  private static CatchupResult reachedHead(String consumerId, int handled) {
    return new CatchupResult(consumerId, CatchupResult.Status.REACHED_HEAD, handled);
  }

  private static CatchupResult unavailable(String consumerId) {
    return new CatchupResult(consumerId, CatchupResult.Status.UNAVAILABLE, 0);
  }

  private static CatchupResult failed(String consumerId) {
    return failed(consumerId, 0);
  }

  private static CatchupResult failed(String consumerId, int handled) {
    return new CatchupResult(consumerId, CatchupResult.Status.FAILED, handled);
  }

  private static CatchupResult incomplete(String consumerId) {
    return incomplete(consumerId, 0);
  }

  private static CatchupResult incomplete(String consumerId, int handled) {
    return new CatchupResult(consumerId, CatchupResult.Status.INCOMPLETE, handled);
  }

  /**
   * A consumer with no watermark starts at the <b>head</b> of the log.
   *
   * <p>The alternative — the epoch — would make the first deployment of any new subscriber replay
   * every matching event the platform has ever recorded, acting a second time on things something
   * else acted on months ago. {@link QitsDurableEventListener#replayFromEpoch()} is there for the
   * consumers that genuinely want that, and it is consulted here and nowhere else.
   *
   * <p>An empty answer is the same place as the epoch and is stored as such: a consumer with nothing
   * behind it has nothing to skip.
   *
   * <p>If the log cannot be read, <b>no watermark is written at all</b> and {@link
   * EventsQuery.Unavailable} propagates. Initializing to a guess would settle every event of the
   * outage.
   */
  private void initialize(QitsDurableEventListener listener, String consumerId, Set<String> names) {
    if (listener.replayFromEpoch()) {
      write(consumerId, Instant.EPOCH, null);
      LOG.infof("durable consumer %s initialized at the start of the log, as it asked", consumerId);
      return;
    }
    EventFrame newest = events.newest(names);
    if (newest == null) {
      write(consumerId, Instant.EPOCH, null);
      LOG.infof("durable consumer %s initialized: the log holds nothing it wants yet", consumerId);
      return;
    }
    write(consumerId, newest.occurredAt(), newest.id());
    LOG.infof(
        "durable consumer %s initialized at the head of the log (%s); it consumes from now on",
        consumerId, newest.id());
  }

  private ConsumerWatermark read(String consumerId) {
    return QuarkusTransaction.requiringNew().call(() -> watermarks.findById(consumerId));
  }

  private void write(String consumerId, Instant occurredAt, String eventId) {
    QuarkusTransaction.requiringNew().run(() -> watermarks.put(consumerId, occurredAt, eventId));
  }

  /** Move the watermark to this row and return the cursor that resumes after it. */
  private String advance(String consumerId, EventFrame last) {
    write(consumerId, last.occurredAt(), last.id());
    return last.occurredAt() + "," + last.id();
  }

  /**
   * A watermark's cursor, or null for "the start of the log" — a null {@code eventId} is the
   * before-the-first-row state, and the log answers a blank cursor id with a 400, so the absence has
   * to be the absence of the parameter.
   */
  private static String cursorOf(ConsumerWatermark mark) {
    return mark.eventId == null ? null : mark.occurredAt + "," + mark.eventId;
  }

  /**
   * Drop the claims the watermark has left behind. The horizon is generous on purpose: the cut mixes
   * the log's clock (the watermark) with this consumer's (the claim), and a day of slack is more
   * than any two hosts of one platform will differ by.
   */
  private void prune(String consumerId) {
    ConsumerWatermark mark = read(consumerId);
    if (mark == null) {
      return;
    }
    Instant cut = mark.occurredAt.minus(pruneHorizon);
    int dropped = QuarkusTransaction.requiringNew().call(() -> consumed.pruneBefore(consumerId, cut));
    if (dropped > 0) {
      LOG.debugf("pruned %d settled claim(s) for %s", dropped, consumerId);
    }
  }
}
