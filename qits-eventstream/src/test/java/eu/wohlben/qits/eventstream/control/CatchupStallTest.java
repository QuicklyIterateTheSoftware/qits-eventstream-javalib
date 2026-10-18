package eu.wohlben.qits.eventstream.control;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import eu.wohlben.qits.eventstream.control.TestEvents.RecordingDurableListener;
import io.quarkus.runtime.StartupEvent;
import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.common.http.TestHTTPResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.net.URI;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.Readiness;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The stall watchdog: <b>a sweep that stops coming back must not be able to stop the module
 * forever.</b>
 *
 * <p>This suite is the 2026-09-08 incident, reproduced. qits-deployments self-deployed at 00:10:33
 * and its {@code eventstream-catchup-startup} virtual thread parked in a PostgreSQL socket read it
 * never returned from — a half-open connection from the blue-green cutover overlap. {@code
 * catchUp()} was {@code synchronized} and the scheduled tick ran {@code SKIP}, so that one thread
 * held the monitor from 00:11 until a restart at 06:30: nothing consumed, every release of the
 * window silently undeployed, every health check green.
 *
 * <p>Two properties defend against a repeat, and every test here is one of them. <b>The tick
 * notices</b> — it never queues behind the holder, it looks at it, names it in an ERROR and
 * interrupts it. <b>The process says so</b> — {@link CatchupHealthCheck} goes DOWN, which is the
 * statement that was missing for six hours.
 *
 * <p>The hold is a {@link CountDownLatch} inside the durable handler, and it is a faithful stand-in
 * rather than a convenient one: {@code await()} is an interruptible park, which is measurably what
 * a <em>virtual</em> thread's blocking socket read is (Temurin 25.0.4.1 — interrupted, it unblocked
 * in 1ms with {@code java.net.SocketException: Closed by interrupt}). A platform thread's is not,
 * and the incident's thread was virtual, which is why every sweep started here is one too.
 *
 * <p>Time moves by moving the clock, never by sleeping — the house rule — and every wait on another
 * thread is bounded and asserted, so a broken watchdog fails a test instead of wedging the suite
 * the way it wedged the service.
 */
@QuarkusTest
@TestProfile(CatchupStallTest.SweepsAtStartup.class)
@WithTestResource(StubEventsServer.class)
class CatchupStallTest extends EventstreamTestSupport {

  /**
   * The startup sweep back on, which only {@link
   * #theStartupRunTakesTheSameLockAndIsWatchedLikeAnyOther} needs — it is the thread the incident
   * happened on, so it is asserted about rather than assumed.
   *
   * <p>Turning it on for the whole class is harmless because the listener is <b>disarmed at boot</b>:
   * it wants nothing, so the boot sweep asks the log nothing, writes no watermark and returns. The
   * suite-wide reason for {@code catchup-at-startup=false} is a sweep reading the stub while a test
   * arranges it, and a sweep that reads nothing cannot.
   */
  public static class SweepsAtStartup implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of("qits.eventstream.catchup-at-startup", "true");
    }
  }

  private static final String CONSUMER = TestEvents.DURABLE_CONSUMER_ID;

  /** Comfortably past {@code qits.eventstream.sweep-stall-budget}, which ships at {@code PT5M}. */
  private static final Duration PAST_THE_BUDGET = Duration.ofMinutes(6);

  /** Comfortably inside it. A long sweep is a normal sweep. */
  private static final Duration INSIDE_THE_BUDGET = Duration.ofMinutes(1);

  /** Every bounded wait here. Generous, because the assertion is on the boolean it returns. */
  private static final Duration PATIENCE = Duration.ofSeconds(10);

  @Inject CatchupSweeper sweeper;
  @Inject RecordingDurableListener durable;

  /**
   * {@code @Readiness} is a CDI <b>qualifier</b>, so the check carries it instead of {@code @Default}
   * and an injection point has to name it. Worth knowing here rather than in a consumer's build.
   */
  @Inject @Readiness CatchupHealthCheck health;

  @TestHTTPResource("/q/health/ready")
  URL readiness;

  private final List<Thread> sweeps = new ArrayList<>();
  private final SevereRecords severe = new SevereRecords();
  private CountDownLatch release;

  @BeforeEach
  void listenForErrors() {
    durable.reset();
    java.util.logging.Logger.getLogger(CatchupSweeper.class.getName()).addHandler(severe);
  }

  /**
   * Free everything this test started, whatever it asserted. A held sweep that outlived its test is
   * the suite reproducing the incident on itself, and the next test would fail for the wrong reason.
   */
  @AfterEach
  void freeEverySweep() throws InterruptedException {
    java.util.logging.Logger.getLogger(CatchupSweeper.class.getName()).removeHandler(severe);
    // The last thread to reach the hold, which for the startup test is one this class never made.
    // Never this thread: a test that swept on its own thread recorded itself there, and joining the
    // thread you are on is a ten-second way of failing for the wrong reason.
    Thread lastHolder = durable.holder();
    durable.reset();
    if (lastHolder != null && lastHolder != Thread.currentThread()) {
      sweeps.add(lastHolder);
    }
    for (Thread sweep : sweeps) {
      sweep.join(PATIENCE.toMillis());
      assertFalse(sweep.isAlive(), "a sweep thread outlived its test: " + sweep.getName());
    }
    sweeps.clear();
    assertFalse(sweeper.census().sweeping(), "the sweep lock is free for the next test");
  }

  /**
   * <b>The incident, and the thing that ends it.</b> A sweep held past the stall budget is found by
   * the next tick — which does not queue behind it — named in an ERROR, and interrupted. The tick
   * returning at all is half the assertion: under {@code SKIP} a tick that blocked would have been
   * the last tick this process ever ran, which is precisely how six hours passed unremarked.
   */
  @Test
  void aSweepHeldPastTheBudgetIsFoundByTheNextTickLoggedAndInterrupted() throws InterruptedException {
    seedThree();
    Thread stalled = startHeldSweep("held-1");

    clock.advance(PAST_THE_BUDGET);
    assertTrue(tickReturnsPromptly(), "the tick must never wait on the holder it is watching");

    assertTrue(
        severe.mentioning("eventstream-catchup-startup"),
        "the ERROR names the thread an incident's stack would name it by: " + severe.messages());
    assertTrue(
        severe.mentioning("stall budget"), "and says why it acted: " + severe.messages());

    stalled.join(PATIENCE.toMillis());
    assertFalse(stalled.isAlive(), "the interrupt freed the parked sweep");
  }

  /**
   * <b>An interrupted sweep costs a sweep, not a consumer.</b> Nothing was settled by the kill — the
   * watermark only ever moves on a whole page — so the next sweep is offered the same rows and
   * commits them. That is what makes the watchdog safe to fire: the worst it can do is make a
   * consumer re-read what it already owed.
   */
  @Test
  void afterTheInterruptTheNextSweepProceedsAndConsumes() throws InterruptedException {
    seedThree();
    Thread stalled = startHeldSweep("held-1");

    clock.advance(PAST_THE_BUDGET);
    sweeper.tick();
    stalled.join(PATIENCE.toMillis());
    assertFalse(stalled.isAlive());
    assertEquals(0, claims(CONSUMER), "the killed handler's claim rolled back with it");

    release.countDown();
    assertEquals(3, sweeper.catchUp(), "everything that was owed, offered again and committed");

    assertEquals("held-3", watermark(CONSUMER).eventId);
    assertEquals(3, claims(CONSUMER));
  }

  /**
   * <b>The statement that was missing.</b> Readiness is what takes an instance that has stopped
   * consuming out of rotation, and for six hours nothing anywhere said this process was not
   * consuming. UP while idle, DOWN while a sweep is stalled, UP again once one has completed.
   */
  @Test
  void theHealthCheckGoesDownDuringAStalledSweepAndUpAgainAfterItRecovers()
      throws InterruptedException {
    seedThree();
    assertEquals(HealthCheckResponse.Status.UP, health.call().getStatus(), "idle is not ill");

    Thread stalled = startHeldSweep("held-1");
    assertEquals(
        HealthCheckResponse.Status.UP,
        health.call().getStatus(),
        "a sweep inside its budget is alive by definition");

    clock.advance(PAST_THE_BUDGET);
    assertEquals(
        HealthCheckResponse.Status.DOWN, health.call().getStatus(), "this is the incident");

    release.countDown();
    stalled.join(PATIENCE.toMillis());
    assertFalse(stalled.isAlive());
    assertEquals(
        HealthCheckResponse.Status.UP,
        health.call().getStatus(),
        "a completed sweep is the only thing that clears it");
  }

  /**
   * <b>A long sweep is not a stalled sweep.</b> A consumer draining a day of backlog holds the lock
   * for as long as that takes, and a watchdog that killed it would turn one recovery into a loop of
   * recoveries. Only the budget being spent makes a holder the incident's shape.
   */
  @Test
  void aLongSweepInsideItsBudgetIsNotKilled() throws InterruptedException {
    seedThree();
    Thread patient = startHeldSweep("held-1");

    clock.advance(INSIDE_THE_BUDGET);
    sweeper.tick();

    assertFalse(patient.isInterrupted(), "nothing struck a holder that is still within its budget");
    assertTrue(severe.messages().isEmpty(), "and nothing was said about it: " + severe.messages());

    release.countDown();
    patient.join(PATIENCE.toMillis());
    assertFalse(patient.isAlive());
    assertEquals(List.of("held-1", "held-2", "held-3"), durable.handledIds());
    assertEquals("held-3", watermark(CONSUMER).eventId);
  }

  /**
   * <b>Serializing the sweeps survived the change.</b> Replacing {@code synchronized} with a lock
   * and a watchdog must not have bought liveness by letting two sweeps page one watermark at once —
   * that was never the bug. The tick finds the lock held and returns without entering it.
   */
  @Test
  void twoSweepsNeverRunConcurrently() {
    seedThree();
    Thread holder = startHeldSweep("held-1");
    int deliveredBefore = durable.handledIds().size();

    sweeper.tick();

    SweepCensus census = sweeper.census();
    assertTrue(census.sweeping(), "the held sweep still owns the lock");
    assertEquals(holder.getName(), census.thread(), "and the tick did not displace it");
    assertEquals(
        deliveredBefore,
        durable.handledIds().size(),
        "the tick handled nothing, because it never got in");
  }

  /**
   * <b>The startup thread is not special, and that is the fix.</b> It was this exact thread —
   * {@code eventstream-catchup-startup}, spawned by the {@code StartupEvent} observer — that parked
   * on 2026-09-08. It takes the same lock every other entry point takes and is watched by the same
   * watchdog, which a start being the coldest moment for a connection pool makes more important
   * rather than less.
   */
  @Test
  void theStartupRunTakesTheSameLockAndIsWatchedLikeAnyOther() throws InterruptedException {
    seedThree();
    durable.wants("ThingHappened");
    durable.replaysFromEpoch();
    release = durable.holdOn("held-1");

    sweeper.onStart(new StartupEvent());

    assertTrue(durable.awaitHold(PATIENCE), "the startup sweep reached the handler");
    Thread startup = durable.holder();
    assertNotNull(startup);
    assertEquals("eventstream-catchup-startup", startup.getName());

    clock.advance(PAST_THE_BUDGET);
    sweeper.tick();

    startup.join(PATIENCE.toMillis());
    assertFalse(startup.isAlive(), "the watchdog interrupted the startup sweep like any other");
    assertTrue(
        severe.mentioning("eventstream-catchup-startup"),
        "and named it while doing so: " + severe.messages());
  }

  /**
   * <b>A check that compiles is not a check that is found.</b> The bean is discovered from this
   * jar's own jandex index by a consumer that carries {@code quarkus-smallrye-health}, and the only
   * thing that proves it is a real request to the real endpoint. The port is 0 in this suite, which
   * is why the URL is injected rather than written.
   *
   * <p>Only the presence of the entry is asserted. The aggregate status is somebody else's — the
   * datasource check shares that payload — and asserting it here would make this test fail for
   * reasons that have nothing to do with the index.
   */
  @Test
  void theHealthCheckIsDiscoveredFromThisJar() throws Exception {
    HttpResponse<String> response =
        HttpClient.newHttpClient()
            .send(
                HttpRequest.newBuilder(URI.create(readiness.toString())).GET().build(),
                HttpResponse.BodyHandlers.ofString());

    assertTrue(
        response.body().contains("eventstream-catchup"),
        "the readiness payload carries this jar's check: " + response.body());
  }

  // -- the arrangement -----------------------------------------------------------------------------

  /** Three rows one second apart, so a sweep held on the first still has work behind it. */
  private void seedThree() {
    Instant at = T0;
    for (int i = 1; i <= 3; i++) {
      StubEventsServer.seed("held-" + i, "ThingHappened", at, "{\"what\":\"held-" + i + "\"}");
      at = at.plusSeconds(1);
    }
  }

  /**
   * Start a sweep on a virtual thread named the way the incident's was, and return once its handler
   * is actually parked — never once it has merely been started, which would race the assertion.
   */
  private Thread startHeldSweep(String heldId) {
    durable.wants("ThingHappened");
    durable.replaysFromEpoch();
    release = durable.holdOn(heldId);
    Thread sweep =
        Thread.ofVirtual().name("eventstream-catchup-startup").unstarted(() -> sweeper.catchUp());
    sweeps.add(sweep);
    sweep.start();
    assertTrue(durable.awaitHold(PATIENCE), "the sweep reached the hold on " + heldId);
    return sweep;
  }

  /** Run the tick on a thread of its own, so "it did not block" is an assertion and not a timeout. */
  private boolean tickReturnsPromptly() throws InterruptedException {
    Thread tick = new Thread(sweeper::tick, "eventstream-catchup-tick");
    tick.start();
    tick.join(PATIENCE.toMillis());
    return !tick.isAlive();
  }

  /**
   * The ERROR log, captured where it is written.
   *
   * <p>A plain JUL handler on {@link CatchupSweeper}'s own category, because the suite already runs
   * on {@code org.jboss.logmanager.LogManager} (surefire sets it) — so {@code LOG.errorf} arrives
   * here as a {@code SEVERE} record and no new dependency is needed to see it. The ERROR is half of
   * what this change ships: on a platform thread the interrupt is measurably not a cure, and the log
   * plus the readiness check are then the entire signal.
   */
  private static final class SevereRecords extends Handler {

    private final List<String> rendered = new CopyOnWriteArrayList<>();

    @Override
    public void publish(LogRecord record) {
      if (record.getLevel().intValue() >= Level.SEVERE.intValue()) {
        rendered.add(record.getMessage() + " " + Arrays.toString(record.getParameters()));
      }
    }

    @Override
    public void flush() {}

    @Override
    public void close() {}

    List<String> messages() {
      return List.copyOf(rendered);
    }

    boolean mentioning(String text) {
      return rendered.stream().anyMatch(message -> message.contains(text));
    }
  }
}
