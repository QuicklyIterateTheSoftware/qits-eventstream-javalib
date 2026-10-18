package eu.wohlben.qits.eventstream.control;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import eu.wohlben.qits.eventstream.QitsEventBus;
import eu.wohlben.qits.eventstream.control.TestEvents.ThingHappened;
import eu.wohlben.qits.eventstream.entity.OutboxEvent;
import eu.wohlben.qits.eventstream.entity.OutboxStatus;
import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.time.Duration;
import org.junit.jupiter.api.Test;

/**
 * The failure classes, and the one rule that separates them: <b>a delivery that got no answer never
 * runs a row out of tries.</b>
 *
 * <p>Measured, 2026-08-10. A seed qits-ci was pointed at an alias that did not resolve; every
 * publish raised {@code ConnectException}, the budget counted five of them, and every row ended
 * {@code FAILED}. Those events never reached the log and nothing would try again — a hole no
 * consumer-side bookkeeping can recover. The budget is for refusals now, because a refusal is
 * evidence about the event and an unreachable bus is evidence about the network.
 *
 * <p>The refusal half is asserted over the wire in {@link OutboxFlowTest}, which is unchanged and is
 * the point: splitting the classes cost the refusal path nothing. The unreachable half is staged
 * here from a {@link EventsPublisher.Delivery} rather than from a real outage, because a stub that
 * has to be up for the rest of the class cannot also be down — with one exception below, which
 * dials a port nothing is listening on so that the <em>classification</em> is proved against a real
 * socket rather than against a value this test made up.
 */
@QuarkusTest
@WithTestResource(StubEventsServer.class)
class OutboxFailureClassTest extends EventstreamTestSupport {

  private static final EventsPublisher.Delivery SILENCE =
      new EventsPublisher.Delivery(
          EventsPublisher.Outcome.UNREACHABLE, "java.net.ConnectException: Connection refused");

  private static final EventsPublisher.Delivery REFUSAL =
      new EventsPublisher.Delivery(EventsPublisher.Outcome.REFUSED, "503 Service Unavailable");

  @Inject QitsEventBus bus;
  @Inject Outbox outbox;
  @Inject OutboxSweeper sweeper;

  /**
   * The classification itself, against a real socket that answers nothing. Port 1 is refused rather
   * than merely quiet, so this costs a round trip on the loopback and no timeout.
   */
  @Test
  void anAddressNothingAnswersIsUnreachableAndNotARefusal() {
    EventsPublisher publisher = new EventsPublisher();
    publisher.eventsUrl = "http://127.0.0.1:1";
    publisher.publishTimeout = Duration.ofSeconds(2);
    ThingHappened event = new ThingHappened("shipped", 1, T0);

    EventsPublisher.Delivery attempt =
        publisher.put(event.eventId().toString(), EventEnvelope.of(event));

    assertTrue(attempt.unreachable(), attempt.detail());
    assertFalse(attempt.refused(), "nothing answered, so nothing refused");
    assertFalse(attempt.delivered());
    assertFalse(attempt.rejected());
  }

  /** And a status code — the thing an unreachable attempt does not have — is what makes a refusal. */
  @Test
  void aStatusCodeIsWhatMakesARefusal() {
    StubEventsServer.answerWith(503);
    ThingHappened event = new ThingHappened("shipped", 1, T0);

    bus.publish(event);

    OutboxEvent row = row(event.eventId().toString());
    assertEquals(1, row.attempts);
    assertEquals(1, row.refusals, "an answer of 503 spends one of the five");
  }

  /**
   * Twenty attempts against a bus that never answers, and the row is still {@code PENDING} — where
   * the old budget would have abandoned it on the fifth. The spacing is the other half of the claim:
   * it walks the curve out and then stays at the five-minute cap, so retrying forever is not the
   * same thing as retrying hard.
   */
  @Test
  void anUnreachableBusIsNeverTerminalAndSettlesAtTheCap() {
    ThingHappened event = new ThingHappened("shipped", 1, T0);
    String id = event.eventId().toString();
    outbox.enqueue(id, EventEnvelope.of(event), SILENCE);

    for (int attempt = 1; attempt <= 20; attempt++) {
      OutboxEvent row = row(id);
      assertEquals(OutboxStatus.PENDING, row.status, "attempt " + attempt);
      assertEquals(attempt, row.attempts);
      assertEquals(0, row.refusals, "silence is not a refusal");
      clock.jumpTo(row.nextAttemptAt);
      outbox.attemptFailed(id, SILENCE);
    }

    OutboxEvent row = row(id);
    assertEquals(OutboxStatus.PENDING, row.status);
    assertEquals(21, row.attempts);
    assertEquals(clock.instant().plus(Duration.ofMinutes(5)), row.nextAttemptAt);
  }

  /**
   * The budget is spent by refusals and by nothing else, so an outage does not quietly consume the
   * tries a row would have had once the bus came back answering.
   */
  @Test
  void anOutageLeavesTheRefusalBudgetWholeAndTheFifthRefusalStillEndsIt() {
    ThingHappened event = new ThingHappened("shipped", 1, T0);
    String id = event.eventId().toString();
    outbox.enqueue(id, EventEnvelope.of(event), SILENCE);
    for (int i = 0; i < 10; i++) {
      outbox.attemptFailed(id, SILENCE);
    }
    assertEquals(OutboxStatus.PENDING, row(id).status, "eleven attempts and still owed");

    // Now something starts answering, and says no. Five of those is the whole budget.
    for (int refusal = 1; refusal <= 4; refusal++) {
      outbox.attemptFailed(id, REFUSAL);
      OutboxEvent row = row(id);
      assertEquals(OutboxStatus.PENDING, row.status, "refusal " + refusal);
      assertEquals(refusal, row.refusals);
    }
    outbox.attemptFailed(id, REFUSAL);

    OutboxEvent row = row(id);
    assertEquals(OutboxStatus.FAILED, row.status);
    assertEquals(5, row.refusals);
    assertNull(row.nextAttemptAt);
  }

  /** A reused id is still terminal on the answer, with no budget spent pretending otherwise. */
  @Test
  void aRejectionIsStillTerminalAtOnceHoweverLongTheOutageWas() {
    ThingHappened event = new ThingHappened("shipped", 1, T0);
    String id = event.eventId().toString();
    outbox.enqueue(id, EventEnvelope.of(event), SILENCE);

    outbox.attemptFailed(
        id, new EventsPublisher.Delivery(EventsPublisher.Outcome.REJECTED, "400 reused id"));

    OutboxEvent row = row(id);
    assertEquals(OutboxStatus.FAILED, row.status);
    assertEquals(0, row.refusals, "a rejection is not a refusal to count, it is the end");
    assertNull(row.nextAttemptAt);
  }

  /**
   * What the whole split is for: the event is still there to be delivered when the bus comes back.
   * The old behaviour reached this point with a {@code FAILED} row and an empty log.
   */
  @Test
  void anEventOwedThroughAnOutageIsDeliveredWhenTheBusComesBack() {
    StubEventsServer.answerWith(201);
    ThingHappened event = new ThingHappened("shipped", 1, T0);
    String id = event.eventId().toString();
    outbox.enqueue(id, EventEnvelope.of(event), SILENCE);
    for (int i = 0; i < 10; i++) {
      clock.jumpTo(row(id).nextAttemptAt);
      outbox.attemptFailed(id, SILENCE);
    }

    clock.jumpTo(row(id).nextAttemptAt);
    assertEquals(1, sweeper.sweep());

    assertEquals(0, outboxCount(), "delivered on the eleventh sweep, and not kept");
    assertEquals(1, StubEventsServer.puts().size());
    assertEquals(id, StubEventsServer.puts().get(0).id());
  }
}
