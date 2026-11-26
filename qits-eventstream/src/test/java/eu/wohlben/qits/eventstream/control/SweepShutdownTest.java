package eu.wohlben.qits.eventstream.control;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import eu.wohlben.qits.eventstream.QitsEventBus;
import eu.wohlben.qits.eventstream.control.TestEvents.RecordingDurableListener;
import eu.wohlben.qits.eventstream.control.TestEvents.ThingHappened;
import eu.wohlben.qits.eventstream.entity.ConsumerWatermark;
import eu.wohlben.qits.eventstream.entity.OutboxEvent;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusMock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The sweepers stop before the store does (qits-2716).
 *
 * <p>On the 2026-11-26 qits-agents redeploys every stop logged ERRORs — {@code catch-up failed for
 * …} once per durable listener per tick, and the scheduler's own "Error occurred while executing
 * task for trigger …OutboxSweeper#tick" — around {@code IllegalArgumentException: Unable to find an
 * EntityManagerFactory for persistence unit 'eventstream'}. Quarkus closes the persistence units in a
 * shutdown task and stops the scheduler only when ArC destroys the beans afterwards, so ticks went
 * on firing into a closed unit ({@link ShutdownGate} has the order). The cure is the {@code
 * ShutdownEvent} observers, and every test here calls one by hand with {@code new ShutdownEvent()} —
 * the same call Quarkus makes, at the moment the unit is still open.
 *
 * <p>Four properties, each asserted here: after the hook <b>nothing sweeps</b>; the hook <b>waits</b>
 * for a sweep already in flight, but <b>only as long as the drain bound</b>; and a sweep that fails
 * <b>while stopping</b> says so at DEBUG while the same failure <b>while running</b> is still the
 * ERROR it always was.
 *
 * <p>The gate is application state, so every test gives the next one a running module back with
 * {@link ShutdownGate#resume()} — and the class runs under its own profile, so even a test that
 * failed before its cleanup cannot leave a stopped module to another class. The profile also shrinks
 * the drain bound, which is the one way to assert "it stopped waiting" in seconds rather than five.
 */
@QuarkusTest
@TestProfile(SweepShutdownTest.ShortDrain.class)
@WithTestResource(StubEventsServer.class)
class SweepShutdownTest extends EventstreamTestSupport {

  /** A two-second drain: long against the 300ms "still waiting" probe, short for a suite to spend. */
  public static class ShortDrain implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of("qits.eventstream.shutdown-drain", "PT2S");
    }
  }

  private static final Duration DRAIN = Duration.ofSeconds(2);

  private static final String CONSUMER = TestEvents.DURABLE_CONSUMER_ID;

  /** Every bounded wait here. Generous, because the assertion is on the boolean it returns. */
  private static final Duration PATIENCE = Duration.ofSeconds(10);

  /** How long the hook is watched to be still waiting. Far inside {@link #DRAIN}. */
  private static final Duration STILL_WAITING = Duration.ofMillis(300);

  /** What the closed persistence unit actually said, so the stand-in throw reads like the real one. */
  private static final String CLOSED_UNIT =
      "Unable to find an EntityManagerFactory for persistence unit 'eventstream'";

  @Inject CatchupSweeper catchup;
  @Inject OutboxSweeper outbox;
  @Inject ShutdownGate gate;
  @Inject QitsEventBus bus;
  @Inject RecordingDurableListener durable;

  private final Records records = new Records();
  private final List<Thread> threads = new ArrayList<>();
  private final Map<Logger, Level> levels = new HashMap<>();

  @BeforeEach
  void listen() {
    gate.resume();
    durable.reset();
    for (Class<?> category : List.of(CatchupSweeper.class, OutboxSweeper.class)) {
      Logger logger = Logger.getLogger(category.getName());
      levels.put(logger, logger.getLevel());
      // DEBUG on, so "it was said at DEBUG" is an assertion and not merely "nothing louder".
      logger.setLevel(Level.FINE);
      logger.addHandler(records);
    }
  }

  /** Free whatever a test held, join whatever it started, and hand the next test a running module. */
  @AfterEach
  void restore() throws InterruptedException {
    durable.reset();
    for (Thread thread : threads) {
      thread.join(PATIENCE.toMillis());
      assertFalse(thread.isAlive(), "a thread outlived its test: " + thread.getName());
    }
    threads.clear();
    levels.forEach(
        (logger, level) -> {
          logger.removeHandler(records);
          logger.setLevel(level);
        });
    levels.clear();
    gate.resume();
    assertFalse(catchup.census().sweeping(), "the sweep lock is free for the next test");
  }

  // -- after the hook, nothing sweeps --------------------------------------------------------------

  /**
   * A row that is due stays exactly as it was: no query moved it, no PUT left. Both entry points —
   * the scheduler's tick and the public {@code sweep()} — and nothing said above DEBUG.
   */
  @Test
  void afterTheHookTheOutboxSweeperAttemptsNothing() {
    StubEventsServer.answerWith(503, 201);
    ThingHappened event = new ThingHappened("owed", 1, T0);
    bus.publish(event);
    clock.advance(Duration.ofMinutes(1));

    outbox.onStop(new ShutdownEvent());
    outbox.tick();

    assertEquals(0, outbox.sweep(), "a stopping sweeper attempts nothing");
    assertEquals(1, StubEventsServer.puts().size(), "only the inline attempt ever went out");
    OutboxEvent row = row(event.eventId().toString());
    assertNotNull(row, "the event is still owed");
    assertEquals(1, row.attempts, "and nobody counted an attempt against it");
    assertTrue(records.atLeast(Level.WARNING).isEmpty(), "nothing loud: " + records.all());
  }

  /**
   * No entry point reads the log or writes a watermark once the hook has run — the tick, both
   * {@code catchUp}s, the epoch rebuild and the startup observer — and the named ones say
   * "incomplete", which is the truth: nothing was caught up.
   */
  @Test
  void afterTheHookTheCatchupSweeperReadsAndWritesNothing() {
    seedThree();
    durable.wants("ThingHappened");
    durable.replaysFromEpoch();

    catchup.onStop(new ShutdownEvent());

    catchup.tick();
    assertEquals(0, catchup.catchUp());
    assertEquals(CatchupResult.Status.INCOMPLETE, catchup.catchUp(CONSUMER).status());
    assertEquals(CatchupResult.Status.INCOMPLETE, catchup.rebuildFromEpoch(CONSUMER).status());
    catchup.onStart(new StartupEvent());

    assertNull(watermark(CONSUMER), "no watermark was initialized");
    assertEquals(0, claims(CONSUMER));
    assertTrue(durable.handledIds().isEmpty(), "nothing was handled");
    assertTrue(StubEventsServer.queries().isEmpty(), "the log was never asked");
    assertFalse(catchup.census().sweeping());
    assertTrue(records.atLeast(Level.WARNING).isEmpty(), "nothing loud: " + records.all());
  }

  // -- the hook waits, bounded -----------------------------------------------------------------------

  /**
   * <b>The point of doing this in {@code ShutdownEvent}:</b> the hook does not return while a sweep
   * holds the lock, so the sweep finishes against a store that is still open. It finishes the row
   * it was on and goes no further — the rest of the page is owed, the watermark did not move past
   * it, and it does not count as a completed sweep.
   */
  @Test
  void theHookWaitsForACatchupSweepInFlightAndItStopsAtTheNextRow() throws InterruptedException {
    seedThree();
    clock.advance(Duration.ofMinutes(1));
    Instant completedBefore = catchup.census().lastCompletedAt();
    CountDownLatch release = durable.holdOn("held-1");
    durable.wants("ThingHappened");
    durable.replaysFromEpoch();
    Thread sweep = start("eventstream-catchup-startup", catchup::catchUp);
    assertTrue(durable.awaitHold(PATIENCE), "the sweep reached the hold");

    Thread hook = start("shutdown", () -> catchup.onStop(new ShutdownEvent()));
    hook.join(STILL_WAITING.toMillis());
    assertTrue(hook.isAlive(), "the hook waits while the sweep holds the lock");

    release.countDown();
    hook.join(PATIENCE.toMillis());
    assertFalse(hook.isAlive(), "and returns once it lets go");
    sweep.join(PATIENCE.toMillis());
    assertFalse(sweep.isAlive());

    assertEquals(List.of("held-1"), durable.handledIds(), "the row in hand, and nothing after it");
    ConsumerWatermark mark = watermark(CONSUMER);
    assertNotNull(mark);
    assertNull(mark.eventId, "the watermark stayed before the unfinished page");
    assertEquals(
        completedBefore, catchup.census().lastCompletedAt(), "a cut sweep is not a completed one");
    assertTrue(records.atLeast(Level.WARNING).isEmpty(), "nothing loud: " + records.all());
    assertTrue(records.mentioning(Level.FINE, "cut short by shutdown"), "said at DEBUG");
  }

  /**
   * <b>Bounded.</b> A sweep that does not come back within the drain is left behind with one WARN
   * naming it, so a wedged sweep can never hold a stop hostage. The hook never interrupts it: that
   * is the watchdog's tool, and it reads as the watchdog's ERROR.
   */
  @Test
  void theHookStopsWaitingAtTheDrainBound() throws InterruptedException {
    seedThree();
    CountDownLatch release = durable.holdOn("held-1");
    durable.wants("ThingHappened");
    durable.replaysFromEpoch();
    Thread sweep = start("eventstream-catchup-startup", catchup::catchUp);
    assertTrue(durable.awaitHold(PATIENCE), "the sweep reached the hold");

    long started = System.nanoTime();
    Thread hook = start("shutdown", () -> catchup.onStop(new ShutdownEvent()));
    hook.join(PATIENCE.toMillis());
    long waited = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);

    assertFalse(hook.isAlive(), "the hook gave up rather than waiting on");
    assertTrue(waited >= DRAIN.toMillis() - 100, "after the drain, not before: " + waited + "ms");
    assertTrue(sweep.isAlive(), "the sweep was abandoned, not interrupted");
    assertFalse(sweep.isInterrupted());
    assertTrue(
        records.mentioning(Level.WARNING, "shutdown drain ran out"),
        "one line says what was abandoned: " + records.all());
    assertTrue(records.atLeast(Level.SEVERE).isEmpty(), "and nothing is an ERROR: " + records.all());

    release.countDown();
    sweep.join(PATIENCE.toMillis());
    assertFalse(sweep.isAlive());
    assertEquals(List.of("held-1"), durable.handledIds());
    assertTrue(records.atLeast(Level.SEVERE).isEmpty(), "its way down is quiet: " + records.all());
  }

  /**
   * The outbox's in-flight guard, and the failure the incident actually showed: the query dies
   * because the unit is closing. The hook waits for the sweep; the sweep's throw, arriving once the
   * gate is closed, is DEBUG inside the tick instead of the scheduler's ERROR.
   */
  @Test
  void theHookWaitsForAnOutboxSweepAndItsFailureWhileStoppingIsQuiet()
      throws InterruptedException {
    CountDownLatch reached = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    QuarkusMock.installMockForType(closingOutbox(reached, release), Outbox.class);
    AtomicReference<Throwable> escaped = new AtomicReference<>();
    Thread tick =
        start(
            "outbox-tick",
            () -> {
              try {
                outbox.tick();
              } catch (Throwable e) {
                escaped.set(e);
              }
            });
    assertTrue(reached.await(PATIENCE.toMillis(), TimeUnit.MILLISECONDS), "the sweep is querying");

    Thread hook = start("shutdown", () -> outbox.onStop(new ShutdownEvent()));
    hook.join(STILL_WAITING.toMillis());
    assertTrue(hook.isAlive(), "the hook waits while the outbox sweep is in flight");

    release.countDown();
    hook.join(PATIENCE.toMillis());
    assertFalse(hook.isAlive(), "and returns once it is done");
    tick.join(PATIENCE.toMillis());
    assertFalse(tick.isAlive());

    assertNull(escaped.get(), "nothing escaped the tick for the scheduler to log as an ERROR");
    assertTrue(records.atLeast(Level.WARNING).isEmpty(), "nothing loud: " + records.all());
    assertTrue(records.mentioning(Level.FINE, "cut short by shutdown"), "said at DEBUG");
  }

  /** While running, the tick is what it always was: the throw goes out, to the scheduler's ERROR. */
  @Test
  void anOutboxFailureWhileRunningStillPropagates() {
    QuarkusMock.installMockForType(closingOutbox(null, null), Outbox.class);

    IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, outbox::tick);
    assertEquals(CLOSED_UNIT, thrown.getMessage());
  }

  /**
   * The per-listener catch: the same throw is DEBUG once the process is stopping — here the hook
   * lands while the sweep is initializing the watermark, and the throw follows it.
   */
  @Test
  void aCatchupFailureWhileStoppingIsLoggedBelowError() throws InterruptedException {
    seedThree();
    CountDownLatch reached = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    durable.wants("ThingHappened");
    durable.whenAskedAboutReplay(
        () -> {
          reached.countDown();
          awaitQuietly(release);
          throw new IllegalArgumentException(CLOSED_UNIT);
        });
    Thread sweep = start("eventstream-catchup-startup", catchup::catchUp);
    assertTrue(reached.await(PATIENCE.toMillis(), TimeUnit.MILLISECONDS), "the sweep is under way");

    Thread hook = start("shutdown", () -> catchup.onStop(new ShutdownEvent()));
    hook.join(STILL_WAITING.toMillis());
    assertTrue(hook.isAlive(), "the hook waits for it");
    release.countDown();
    hook.join(PATIENCE.toMillis());
    sweep.join(PATIENCE.toMillis());
    assertFalse(hook.isAlive());
    assertFalse(sweep.isAlive());

    assertNull(watermark(CONSUMER), "nothing was settled");
    assertTrue(records.atLeast(Level.WARNING).isEmpty(), "nothing loud: " + records.all());
    assertTrue(records.mentioning(Level.FINE, "cut short by shutdown"), "said at DEBUG");
  }

  /** And while running, that same throw is still the ERROR it was. */
  @Test
  void theSameCatchupFailureWhileRunningIsStillAnError() {
    seedThree();
    durable.wants("ThingHappened");
    durable.whenAskedAboutReplay(
        () -> {
          throw new IllegalArgumentException(CLOSED_UNIT);
        });

    catchup.catchUp();

    assertTrue(
        records.mentioning(Level.SEVERE, "catch-up failed for"),
        "a failure while running is an ERROR: " + records.all());
  }

  // -- the arrangement -------------------------------------------------------------------------------

  private void seedThree() {
    Instant at = T0;
    for (int i = 1; i <= 3; i++) {
      StubEventsServer.seed("held-" + i, "ThingHappened", at, "{\"what\":\"held-" + i + "\"}");
      at = at.plusSeconds(1);
    }
  }

  private Thread start(String name, Runnable work) {
    Thread thread = Thread.ofVirtual().name(name).unstarted(work);
    threads.add(thread);
    thread.start();
    return thread;
  }

  private static void awaitQuietly(CountDownLatch latch) {
    try {
      latch.await(PATIENCE.toMillis(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /**
   * The outbox with its query replaced by the measured failure: {@code due()} throws what the
   * closed unit threw. With latches it first parks, so a test can put the stop in between.
   *
   * <p>An anonymous inner class on purpose, never a named nested one: {@code @ApplicationScoped} is
   * {@code @Inherited}, and a named subclass in the test sources would be discovered as a second
   * {@link Outbox} bean and make every injection of it ambiguous.
   */
  private Outbox closingOutbox(CountDownLatch reached, CountDownLatch release) {
    return new Outbox() {
      @Override
      public List<OutboxEvent> due(Instant now) {
        if (reached != null) {
          reached.countDown();
          awaitQuietly(release);
        }
        throw new IllegalArgumentException(CLOSED_UNIT);
      }
    };
  }

  /** Every record on the two sweeper categories, with its level, captured where it is written. */
  private static final class Records extends Handler {

    private record Line(Level level, String text) {}

    private final List<Line> lines = new CopyOnWriteArrayList<>();

    @Override
    public void publish(LogRecord record) {
      lines.add(
          new Line(
              record.getLevel(),
              record.getMessage() + " " + Arrays.toString(record.getParameters())));
    }

    @Override
    public void flush() {}

    @Override
    public void close() {}

    List<String> atLeast(Level level) {
      return lines.stream()
          .filter(line -> line.level().intValue() >= level.intValue())
          .map(line -> line.level() + " " + line.text())
          .toList();
    }

    boolean mentioning(Level level, String text) {
      return lines.stream()
          .anyMatch(line -> line.level().intValue() >= level.intValue() && line.text().contains(text));
    }

    List<String> all() {
      return lines.stream().map(line -> line.level() + " " + line.text()).toList();
    }
  }
}
