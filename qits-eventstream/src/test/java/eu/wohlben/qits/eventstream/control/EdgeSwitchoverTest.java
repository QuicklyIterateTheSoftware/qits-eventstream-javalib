package eu.wohlben.qits.eventstream.control;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import eu.wohlben.qits.eventstream.control.TestEvents.RecordingDurableListener;
import eu.wohlben.qits.eventstream.control.TestEvents.ThingHappened;
import eu.wohlben.qits.eventstream.control.TestEvents.ThingListener;
import eu.wohlben.qits.http.QitsHttpClients;
import eu.wohlben.qits.http.RetryPolicy;
import io.opentelemetry.api.OpenTelemetry;
import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * qits-428: the library against an edge that switches over — every connection to qits-events cut
 * at once, new ones refused for a while, then accepted again on the same address.
 *
 * <p>The edge is {@link CuttableProxy}, a plain TCP proxy between the application and {@link
 * StubEventsServer}; {@code qits.events.url} names the proxy, so the stream, the catch-up reads and
 * the publishes all cross it, as they cross the real edge.
 *
 * <p><b>What the stream promises, and so what is asserted.</b> The live stream is at-most-once and
 * live-only (README, "Consuming"): a typed listener sees what is broadcast while it is connected and
 * nothing else. The durable seam is the one that promises no lost event, and its promise is
 * exactly-once EFFECT per (listener, event id) — the subscriber redials without bound, and the
 * catch-up sweep pages the log forward from the watermark, with the claim deduplicating the two
 * channels. The first case proves that end to end across a cut; the rest prove that the two HTTP
 * calls ride a switchover under qits-http-core's {@link RetryPolicy}.
 *
 * <p>The stub and proxy are loopback, which the standard policy never retries; the retry cases
 * therefore build their client with {@link RetryPolicy#withLoopbackRetries()}, which is that
 * policy's own opt-in for a test whose stand-in edge is local.
 */
@QuarkusTest
@WithTestResource(ProxiedEventsServer.class)
class EdgeSwitchoverTest extends EventstreamTestSupport {

  private static final Duration PATIENCE = Duration.ofSeconds(10);
  private static final String CONSUMER = TestEvents.DURABLE_CONSUMER_ID;

  @Inject EventStreamSubscriber subscriber;
  @Inject CatchupSweeper sweeper;
  @Inject EventsQuery query;
  @Inject RecordingDurableListener durable;
  @Inject ThingListener things;

  private CuttableProxy edge;

  @BeforeEach
  void edgeUp() {
    edge = ProxiedEventsServer.proxy();
    edge.accept();
    durable.reset();
    things.reset();
    await(subscriber::connected, "the stream to be up through the proxy");
  }

  @AfterEach
  void disarm() {
    durable.reset();
    edge.accept();
  }

  /**
   * The cut, mid-stream: one event delivered live, one broadcast as the edge drops every connection
   * (it may or may not make it), one published while the edge refuses, one after the subscriber is
   * back, plus a replayed frame of the first. Afterwards the durable handler has run exactly once for
   * each of the four, and the watermark is at the last — no lost event, no duplicate effect.
   */
  @Test
  void aStreamCutAtTheEdgeRedialsAndTheDurableListenerResumesFromItsWatermark() throws Exception {
    durable.wants("ThingHappened");
    // The marker below is a typed listener's frame only: it is in no log and no claim.
    durable.selectsOnly(f -> !f.id().equals("marker"));
    publish("before", T0, false);
    assertEquals(
        CatchupResult.Status.REACHED_HEAD, sweeper.catchUp(CONSUMER).status(), "initialization");
    assertEquals("before", watermark(CONSUMER).eventId, "a new consumer starts at the head");

    publish("e1", T0.plusSeconds(1), true);
    await(() -> durable.handledIds().contains("e1"), "e1 to arrive live");

    // In flight as the edge goes: whether it lands live is a race, and either answer must end the
    // same way.
    publish("e2", T0.plusSeconds(2), true);
    edge.refuse();
    await(() -> !subscriber.connected(), "the subscriber to notice the cut");
    assertEquals(0, StubEventsServer.openStreams(), "the cut reached the far side too");

    publish("e3", T0.plusSeconds(3), true); // broadcast to nobody: no stream is open
    assertEquals(
        CatchupResult.Status.UNAVAILABLE,
        sweeper.catchUp(CONSUMER).status(),
        "a sweep during the outage reads nothing");
    assertEquals("before", watermark(CONSUMER).eventId, "and moves the watermark nowhere");

    int dialsBefore = edge.accepted();
    Thread.sleep(500); // several redials, each refused (the suite's backoff caps at 0.2s)
    edge.accept();

    assertNotNull(StubEventsServer.awaitSubscribeFrame(PATIENCE), "a resubscribe after the outage");
    await(subscriber::connected, "the stream to be back up");
    assertTrue(edge.accepted() > dialsBefore, "the subscriber dialled the same address again");

    publish("e4", T0.plusSeconds(4), true);
    await(() -> durable.handledIds().contains("e4"), "e4 to arrive live on the new stream");
    // A replay of an already-handled frame: the claim drops it.
    publish("e1", T0.plusSeconds(1), true);
    // And a frame after it, so the replay has very likely been dispatched before the sweep. Frames
    // run on worker threads, so this is not a barrier — nor does it need to be: a replay dispatched
    // late meets the same claim.
    StubEventsServer.broadcast(frame("marker", T0.plusSeconds(5)));
    await(() -> whats().contains("marker"), "the frames after the replay to be dispatched");

    CatchupResult resumed = sweeper.catchUp(CONSUMER);
    assertEquals(CatchupResult.Status.REACHED_HEAD, resumed.status());

    List<String> handled = durable.handledIds();
    assertEquals(
        new HashSet<>(List.of("e1", "e2", "e3", "e4")),
        new HashSet<>(handled),
        "every event after the watermark, and only those: " + handled);
    assertEquals(4, handled.size(), "each exactly once: " + handled);
    for (String id : List.of("e1", "e2", "e3", "e4")) {
      assertTrue(claimed(CONSUMER, id), id + " is claimed");
    }
    assertEquals("e4", watermark(CONSUMER).eventId, "the watermark is at the head again");

    // The contrast the README states: the typed listener is live-only, so the event published while
    // the edge refused never reaches it.
    assertFalse(whats().contains("e3"), "a typed listener is not replayed: " + whats());
    assertTrue(whats().contains("e4"), "but it hears the reconnected stream: " + whats());
  }

  /** A catch-up read against a refusing edge waits it out and gets its page. */
  @Test
  void aCatchupReadRidesOutARefusingEdge() {
    StubEventsServer.seed("r-1", "ThingHappened", T0, "{}");
    EventsQuery patient = new EventsQuery(retryingClients());
    patient.eventsUrl = ProxiedEventsServer.url();

    edge.refuse();
    long started = System.nanoTime();
    acceptAfter(Duration.ofMillis(700));
    EventPage page = patient.after(List.of("ThingHappened"), null, 10);

    assertEquals(List.of("r-1"), page.events().stream().map(EventFrame::id).toList());
    assertTrue(
        elapsed(started).toMillis() >= 600,
        "the read waited for the edge rather than failing: " + elapsed(started));
  }

  /**
   * A publish whose connection is reset after the request went out — before any response — is a
   * PUT, idempotent, so the policy sends it again; the stub sees it once, because the first attempt
   * never got past the proxy. A POST would not be resent here; none of this library's calls is one.
   */
  @Test
  void aPublishResetBeforeItsResponseIsSentAgain() {
    EventsPublisher patient = new EventsPublisher(retryingClients());
    patient.eventsUrl = ProxiedEventsServer.url();
    patient.publishTimeout = Duration.ofSeconds(5);
    ThingHappened event = new ThingHappened(null, "p-1", 1, T0);

    int before = edge.accepted();
    edge.swallowNext(1);
    EventsPublisher.Delivery delivery = patient.put(event.eventId().toString(), EventEnvelope.of(event));

    assertTrue(delivery.delivered(), "delivered on the second attempt: " + delivery);
    assertEquals(2, edge.accepted() - before, "two connections: the reset one and the retry");
    assertEquals(1, StubEventsServer.puts().size(), "and qits-events saw the request once");
  }

  /** A publish against a refusing edge — nothing was sent — is retried until the edge is back. */
  @Test
  void aPublishRidesOutARefusingEdge() {
    EventsPublisher patient = new EventsPublisher(retryingClients());
    patient.eventsUrl = ProxiedEventsServer.url();
    patient.publishTimeout = Duration.ofSeconds(5);
    ThingHappened event = new ThingHappened(null, "p-2", 1, T0);

    edge.refuse();
    acceptAfter(Duration.ofMillis(700));
    EventsPublisher.Delivery delivery = patient.put(event.eventId().toString(), EventEnvelope.of(event));

    assertTrue(delivery.delivered(), "delivered once the edge accepted again: " + delivery);
    assertEquals(1, StubEventsServer.puts().size());
  }

  /**
   * The injected clients carry the standard policy, which never retries a loopback target: this
   * suite's closed-port cases therefore cost one refused connect, not a thirty-second budget.
   */
  @Test
  void theStandardPolicyLeavesALoopbackTargetToOneAttempt() {
    edge.refuse();
    long started = System.nanoTime();
    assertThrows(EventsQuery.Unavailable.class, () -> query.after(List.of("ThingHappened"), null, 1));
    assertTrue(elapsed(started).toMillis() < 2_000, "one attempt, no retry: " + elapsed(started));
  }

  // -- helpers -------------------------------------------------------------------------------------

  private static QitsHttpClients retryingClients() {
    return QitsHttpClients.of(OpenTelemetry.noop(), null, RetryPolicy.standard().withLoopbackRetries());
  }

  /** What qits-events does with a publish: store it, then broadcast it to every open stream. */
  private static void publish(String id, Instant at, boolean broadcast) {
    String payload = CanonicalJson.payload(new ThingHappened(null, id, 1, at));
    StubEventsServer.seed(id, "ThingHappened", at, payload);
    if (broadcast) {
      StubEventsServer.broadcast(
          CanonicalJson.canonicalize(
              new EventFrame(id, "ThingHappened", at, payload, null, null, null)));
    }
  }

  private static String frame(String id, Instant at) {
    return CanonicalJson.canonicalize(
        new EventFrame(
            id,
            "ThingHappened",
            at,
            CanonicalJson.payload(new ThingHappened(null, id, 1, at)),
            null,
            null,
            null));
  }

  private List<String> whats() {
    return things.received().stream().map(ThingHappened::what).toList();
  }

  private void acceptAfter(Duration delay) {
    CompletableFuture.runAsync(
        () -> edge.accept(),
        CompletableFuture.delayedExecutor(delay.toMillis(), TimeUnit.MILLISECONDS));
  }

  private static Duration elapsed(long started) {
    return Duration.ofNanos(System.nanoTime() - started);
  }

  private static void await(BooleanSupplier condition, String what) {
    long deadline = System.nanoTime() + PATIENCE.toNanos();
    while (System.nanoTime() < deadline) {
      if (condition.getAsBoolean()) {
        return;
      }
      try {
        Thread.sleep(20);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException(e);
      }
    }
    assertTrue(condition.getAsBoolean(), "timed out waiting for " + what);
  }
}
