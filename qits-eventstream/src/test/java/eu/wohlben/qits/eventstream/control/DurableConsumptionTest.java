package eu.wohlben.qits.eventstream.control;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import eu.wohlben.qits.eventstream.CausationScope;
import eu.wohlben.qits.eventstream.control.TestEvents.RecordingDurableListener;
import eu.wohlben.qits.eventstream.control.TestEvents.ThingHappened;
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
 * The funnel: the one path every durable arrival takes, and the four things it can decide.
 *
 * <p>Driven through {@link DurableFunnel} and {@link EventDispatcher} directly rather than over the
 * socket, for the reason {@code RawEventListenerTest} gives — these are claims about what happens to
 * a frame that has arrived, and the arriving is somebody else's test. What is genuinely under test
 * is the pair of tables afterwards: a claim row is the difference between "we ran the handler" and
 * "we will run it again".
 *
 * <p>The listener is a bean of the shared test application, so an arming that outlived this class
 * would reach every other suite in the JVM — and one of those asserts the subscribe frame literally.
 * Disarmed on the way out as well as on the way in.
 */
@QuarkusTest
@WithTestResource(StubEventsServer.class)
class DurableConsumptionTest extends EventstreamTestSupport {

  private static final String CONSUMER = TestEvents.DURABLE_CONSUMER_ID;

  @Inject EventDispatcher dispatcher;
  @Inject DurableFunnel funnel;
  @Inject RecordingDurableListener durable;

  @BeforeEach
  void armNothing() {
    durable.reset();
  }

  @AfterEach
  void disarm() {
    durable.reset();
  }

  /** The claim ledger is what makes this seam different, so it is the first thing asserted. */
  @Test
  void aHandledEventLeavesAClaimNamingTheListenerAndTheEvent() {
    durable.wants("ThingHappened");
    EventFrame frame = frame("e-1");

    assertEquals(DurableFunnel.Result.HANDLED, funnel.offer(durable, frame));

    assertEquals(List.of("e-1"), durable.handledIds());
    assertTrue(claimed(CONSUMER, "e-1"));
    assertEquals(1, claims(CONSUMER));
  }

  /**
   * The duplicate-insert path, which is the whole of "exactly-once effect". The second offer is what
   * a catch-up sweep reading past an event the stream already delivered looks like, and it must cost
   * nothing and say nothing.
   */
  @Test
  void anEventOfferedTwiceIsHandledOnce() {
    durable.wants("ThingHappened");
    EventFrame frame = frame("e-1");

    assertEquals(DurableFunnel.Result.HANDLED, funnel.offer(durable, frame));
    assertEquals(DurableFunnel.Result.SKIPPED, funnel.offer(durable, frame));
    assertEquals(DurableFunnel.Result.SKIPPED, funnel.offer(durable, frame));

    assertEquals(List.of("e-1"), durable.handledIds(), "the handler ran once");
    assertEquals(1, claims(CONSUMER));
  }

  /** The same claim, arrived at down the live path — one funnel, so the two cannot disagree. */
  @Test
  void aLiveFrameAndACatchupRowAreTheSameArrival() {
    durable.wants("ThingHappened");
    EventFrame frame = frame("e-1");

    dispatcher.dispatch(wire(frame));
    assertEquals(DurableFunnel.Result.SKIPPED, funnel.offer(durable, frame), "already handled live");

    assertEquals(List.of("e-1"), durable.handledIds());
  }

  /**
   * A handler that throws takes its claim down with it. That is the entire failure policy: the event
   * is <b>still owed</b>, so the next arrival — live or swept — offers it again rather than finding
   * a row saying it was dealt with.
   */
  @Test
  void aHandlerThatThrowsRollsTheClaimBackAndTheEventStaysOwed() {
    durable.wants("ThingHappened");
    durable.failOn("e-1");

    assertEquals(DurableFunnel.Result.FAILED, funnel.offer(durable, frame("e-1")));

    assertFalse(claimed(CONSUMER, "e-1"), "a claim that survived a rollback would settle the event");
    assertEquals(0, claims(CONSUMER));

    // And it really is owed: fix the handler and the same event is handled on the next offer.
    durable.failOn(null);
    assertEquals(DurableFunnel.Result.HANDLED, funnel.offer(durable, frame("e-1")));
    assertTrue(claimed(CONSUMER, "e-1"));
  }

  /**
   * Selective storage: an event the predicate rejects leaves <b>no row at all</b>. That is what keeps
   * the ledger proportional to the work rather than to the log, and the watermark is what makes it
   * safe — a later widening of the predicate cannot reach back below it.
   */
  @Test
  void anEventThePredicateRejectsStoresNothing() {
    durable.wants("ThingHappened");
    durable.selectsOnly(frame -> frame.id().equals("e-2"));

    assertEquals(DurableFunnel.Result.SKIPPED, funnel.offer(durable, frame("e-1")));
    assertEquals(DurableFunnel.Result.HANDLED, funnel.offer(durable, frame("e-2")));

    assertEquals(List.of("e-2"), durable.handledIds());
    assertFalse(claimed(CONSUMER, "e-1"), "a rejected event is not stored");
    assertEquals(1, claims(CONSUMER));
  }

  /**
   * A predicate that throws is a failure, not a "no". Answering "no" would let the watermark pass an
   * event the listener may well have wanted, and that loss is unrecoverable — the watermark never
   * goes backwards.
   */
  @Test
  void aPredicateThatThrowsLeavesTheEventOwed() {
    durable.wants("ThingHappened");
    durable.failWhenAsked();

    assertEquals(DurableFunnel.Result.FAILED, funnel.offer(durable, frame("e-1")));

    assertEquals(List.of(), durable.handledIds());
    assertEquals(0, claims(CONSUMER));
  }

  /** A durable listener is subscribed for like any other: catch-up is a floor, not a replacement. */
  @Test
  void aDurableListenerIsInTheSubscriptionUnion() {
    durable.wants("DurableOnlyHappened");

    assertTrue(dispatcher.signatures().contains("DurableOnlyHappened"));

    durable.reset();
    assertFalse(
        dispatcher.signatures().contains("DurableOnlyHappened"),
        "and a listener that wants nothing is subscribed for nothing");
  }

  /** Causation is identical to the other seams, which is what the interface promises. */
  @Test
  void theHandlerRunsUnderTheArrivingEventsCause() {
    durable.wants("ThingHappened");
    String id = UUID.randomUUID().toString();

    funnel.offer(durable, frame(id));

    assertEquals(List.of(Optional.of(UUID.fromString(id))), durable.causes());
    assertNull(CausationScope.current(), "and the scope unwound");
  }

  /** An arrival with no id cannot be claimed, so it is dropped loudly rather than handled blindly. */
  @Test
  void aFrameWithNoIdIsNotHandled() {
    durable.wants("ThingHappened");

    assertEquals(
        DurableFunnel.Result.SKIPPED,
        funnel.offer(durable, new EventFrame(null, "ThingHappened", T0, "{}", null, null, null)));

    assertEquals(List.of(), durable.handledIds());
  }

  private static EventFrame frame(String id) {
    return new EventFrame(
        id,
        "ThingHappened",
        T0,
        CanonicalJson.payload(new ThingHappened("shipped", 1, T0)),
        null,
        null, null);
  }

  /** The same frame as it arrives on the socket, so the live path is exercised end to end. */
  private static String wire(EventFrame frame) {
    return CanonicalJson.canonicalize(
        new EventFrame(
            frame.id(), frame.name(), frame.occurredAt(), frame.payload(), null, null, null));
  }
}
