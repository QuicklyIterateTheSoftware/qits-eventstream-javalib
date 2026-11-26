package eu.wohlben.qits.eventstream.control;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import eu.wohlben.qits.eventstream.QitsEventBus;
import eu.wohlben.qits.eventstream.control.TestEvents.CausationProbeListener;
import eu.wohlben.qits.eventstream.control.TestEvents.RecordingDurableListener;
import eu.wohlben.qits.eventstream.control.TestEvents.ThingHappened;
import eu.wohlben.qits.eventstream.control.TestEvents.ThingListener;
import eu.wohlben.qits.eventstream.entity.OutboxEvent;
import eu.wohlben.qits.http.ChainContext;
import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The call chain (qits-2694), end to end on this side, the way {@link CausationStampingTest} proves
 * causation: what the bus stamps from {@link ChainContext}, what the dispatcher and the durable
 * funnel establish from a frame, and what the outbox keeps for the sweep.
 *
 * <p>Asserted on the <b>recorded PUT bodies</b>, because qits-events compares both fields and a
 * value that never left this JVM proves nothing about that.
 */
@QuarkusTest
@WithTestResource(StubEventsServer.class)
class ChainStampingTest extends EventstreamTestSupport {

  private static final ChainContext REQUEST =
      new ChainContext("4bf92f3577b34da6a3ce929d0e0e4736", "9b2c6a1e-6f0e-4c4b-9a55-0a4d6f1c2b3d");
  private static final ChainContext UNRELATED =
      new ChainContext("0af7651916cd43dd8448eb211c80319c", "tok-agent-somebody-else");

  @Inject QitsEventBus bus;
  @Inject OutboxSweeper sweeper;
  @Inject EventDispatcher dispatcher;
  @Inject DurableFunnel funnel;
  @Inject ThingListener things;
  @Inject CausationProbeListener probe;
  @Inject RecordingDurableListener durable;

  @BeforeEach
  void resetListeners() {
    things.reset();
    probe.reset();
    durable.reset();
  }

  @AfterEach
  void leaveNothingBehind() {
    probe.reset();
    durable.reset();
    assertNull(ChainContext.current(), "no test here may leave a chain on the JUnit thread");
  }

  // -- 1. an event published inside a request carries the request's chain -----------------------

  @Test
  void anEventPublishedInsideAChainCarriesBothValues() {
    ChainContext.with(REQUEST, () -> bus.publish(new ThingHappened("shipped", 1, T0)));

    JsonNode put = onlyPut();
    assertEquals(REQUEST.originTraceId(), put.get("originTraceId").asText());
    assertEquals(REQUEST.onBehalfOf(), put.get("onBehalfOf").asText());
  }

  @Test
  void anEventPublishedOutsideAnyChainCarriesExplicitNulls() {
    bus.publish(new ThingHappened("shipped", 1, T0));

    JsonNode put = onlyPut();
    assertTrue(put.has("originTraceId") && put.get("originTraceId").isNull(), put.toString());
    assertTrue(put.has("onBehalfOf") && put.get("onBehalfOf").isNull(), put.toString());
  }

  /** Like causation, the chain is envelope and never payload: the compared bytes stay identical. */
  @Test
  void theChainNeverReachesThePayload() {
    bus.publish(new ThingHappened("shipped", 1, T0));
    ChainContext.with(REQUEST, () -> bus.publish(new ThingHappened("shipped", 1, T0)));

    List<StubEventsServer.Put> puts = StubEventsServer.puts();
    String plain = CanonicalJson.parse(puts.get(0).body()).get("payload").asText();
    String chained = CanonicalJson.parse(puts.get(1).body()).get("payload").asText();
    assertEquals(plain, chained);
    assertFalse(chained.contains(REQUEST.originTraceId()), chained);
  }

  // -- 2. an event published while handling an event inherits the consumed event's chain ---------

  @Test
  void anEventPublishedDuringConsumptionInheritsTheArrivingFramesChain() {
    probe.publishWhileConsuming();

    dispatcher.dispatch(
        frame(UUID.randomUUID().toString(), REQUEST.originTraceId(), REQUEST.onBehalfOf()));

    JsonNode put = onlyPut();
    assertEquals(REQUEST.originTraceId(), put.get("originTraceId").asText());
    assertEquals(REQUEST.onBehalfOf(), put.get("onBehalfOf").asText());
    assertNull(ChainContext.current(), "the dispatch thread is left as it was found");
  }

  @Test
  void aDurableListenerRunsInsideTheFramesChain() {
    durable.wants("ThingHappened");
    EventFrame frame =
        CanonicalJson.frame(frame("e-chain-1", REQUEST.originTraceId(), REQUEST.onBehalfOf()));

    assertEquals(DurableFunnel.Result.HANDLED, funnel.offer(durable, frame));

    assertEquals(List.of(Optional.of(REQUEST)), durable.chains());
  }

  @Test
  void aFrameWithoutAnOriginIsHandledOutsideAnyChain() {
    durable.wants("ThingHappened");
    EventFrame frame = CanonicalJson.frame(frame("e-chain-2", null, null));

    assertEquals(DurableFunnel.Result.HANDLED, funnel.offer(durable, frame));

    assertEquals(List.of(Optional.empty()), durable.chains());
  }

  /** A malformed on-behalf-of is dropped rather than echoed onward; the origin still holds. */
  @Test
  void aMalformedOnBehalfOfIsDroppedAndTheOriginKept() {
    durable.wants("ThingHappened");
    EventFrame frame =
        CanonicalJson.frame(frame("e-chain-3", REQUEST.originTraceId(), "line\nbreak"));

    funnel.offer(durable, frame);

    assertEquals(
        List.of(Optional.of(new ChainContext(REQUEST.originTraceId(), null))), durable.chains());
  }

  @Test
  void aMalformedOriginStartsNoChain() {
    assertNull(EventDispatcher.chainOf(chainFrame("NOT-HEX", "someone")));
    assertNull(EventDispatcher.chainOf(chainFrame("0".repeat(32), "someone")), "the invalid id");
  }

  // -- 3. a swept outbox retry carries the same two values as the first attempt -----------------

  @Test
  void aPublishThatFailsInlineWritesTheChainOnTheRow() {
    StubEventsServer.answerWith(503);
    ThingHappened event = new ThingHappened("shipped", 1, T0);

    ChainContext.with(REQUEST, () -> bus.publish(event));

    OutboxEvent row = row(event.eventId().toString());
    assertNotNull(row);
    assertEquals(REQUEST.originTraceId(), row.originTraceId);
    assertEquals(REQUEST.onBehalfOf(), row.onBehalfOf);
  }

  /** The sweep runs inside a DIFFERENT chain on purpose: what goes out comes from the row. */
  @Test
  void aSweptRetryCarriesTheOriginalChainAndNotTheSweepersContext() {
    StubEventsServer.answerWith(503, 200);
    ThingHappened event = new ThingHappened("shipped", 1, T0);
    ChainContext.with(REQUEST, () -> bus.publish(event));

    clock.jumpTo(row(event.eventId().toString()).nextAttemptAt);
    ChainContext.with(UNRELATED, () -> assertEquals(1, sweeper.sweep()));

    List<StubEventsServer.Put> puts = StubEventsServer.puts();
    assertEquals(2, puts.size());
    assertEquals(puts.get(0).body(), puts.get(1).body(), "a retry is the same request, byte for byte");
    assertEquals(
        REQUEST.originTraceId(),
        CanonicalJson.parse(puts.get(1).body()).get("originTraceId").asText());
    assertEquals(0, outboxCount(), "and the 200 removed the row");
  }

  // -- helpers -----------------------------------------------------------------------------------

  private static JsonNode onlyPut() {
    List<StubEventsServer.Put> puts = StubEventsServer.puts();
    assertEquals(1, puts.size(), "expected exactly one PUT: " + puts);
    return CanonicalJson.parse(puts.get(0).body());
  }

  private static EventFrame chainFrame(String origin, String onBehalfOf) {
    ThingHappened event = new ThingHappened("shipped", 1, T0);
    return new EventFrame(
        "e-x",
        "ThingHappened",
        event.at(),
        CanonicalJson.payload(event),
        null,
        null,
        null,
        null,
        origin,
        onBehalfOf);
  }

  /** The shape qits-events pushes, carrying the chain a test wants listeners to run in. */
  private static String frame(String id, String origin, String onBehalfOf) {
    ThingHappened event = new ThingHappened("shipped", 1, T0);
    return CanonicalJson.canonicalize(
        new EventFrame(
            id,
            "ThingHappened",
            event.at(),
            CanonicalJson.payload(event),
            null,
            null,
            null,
            null,
            origin,
            onBehalfOf));
  }
}
