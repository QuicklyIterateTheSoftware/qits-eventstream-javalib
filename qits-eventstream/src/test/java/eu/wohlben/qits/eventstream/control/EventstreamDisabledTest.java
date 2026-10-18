package eu.wohlben.qits.eventstream.control;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

import eu.wohlben.qits.eventstream.CausationScope;
import eu.wohlben.qits.eventstream.QitsEventBus;
import eu.wohlben.qits.eventstream.control.TestEvents.ThingHappened;
import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Dark means dark. {@code qits.eventstream.enabled=false} is the posture every deployable takes
 * in {@code %dev} and {@code %test}, and the reason it is worth a test of its own is that "disabled"
 * has four separate ways to leak: a publish that still dials, an outbox sweeper that still drains, a
 * subscriber that still holds a socket open against a service nobody started, and a catch-up sweep
 * that still reads a log and writes a watermark.
 *
 * <p>The stub is here <b>so that a dial would be visible</b>. Asserting "no exception was thrown"
 * would pass against every one of those leaks; asserting that a real listening server saw nothing
 * at all does not.
 */
@QuarkusTest
@TestProfile(EventstreamDisabledTest.Disabled.class)
@WithTestResource(StubEventsServer.class)
class EventstreamDisabledTest extends EventstreamTestSupport {

  public static class Disabled implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of("qits.eventstream.enabled", "false");
    }
  }

  @Inject QitsEventBus bus;
  @Inject OutboxSweeper sweeper;
  @Inject EventStreamSubscriber subscriber;
  @Inject CatchupSweeper catchup;
  @Inject DurableFunnel funnel;
  @Inject TestEvents.RecordingDurableListener durable;

  @Test
  void publishingIsANoOpThatReachesNothingAndRecordsNothing() {
    bus.publish(new ThingHappened("shipped", 1, T0));

    assertEquals(0, StubEventsServer.puts().size(), "no request may leave a disabled module");
    assertEquals(0, outboxCount(), "and nothing is queued for later either");
  }

  /**
   * Causation changes nothing about darkness, in either of its two shapes: a scope establishes a
   * value on a thread whether or not anything is listening, and an explicit parent is just an
   * argument. Neither may become a reason for a disabled bus to dial, and neither may become a row.
   */
  @Test
  void aDisabledBusStampsNothingBecauseItPublishesNothing() {
    CausationScope.with(
        UUID.fromString("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
        () -> bus.publish(new ThingHappened("shipped", 1, T0)));
    bus.publish(
        new ThingHappened("shipped", 2, T0),
        UUID.fromString("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"));

    assertEquals(0, StubEventsServer.puts().size(), "a cause is not a reason to dial");
    assertEquals(0, outboxCount());
    assertNull(CausationScope.current(), "and the scope still unwound");
  }

  @Test
  void theSweeperDoesNothing() {
    assertEquals(0, sweeper.sweep());
  }

  /**
   * Dark reaches the durable half too, and it has one extra thing to be dark about: <b>no table is
   * touched at runtime.</b> The migrations still ship and Flyway still runs them — that is the same
   * "dark does not mean absent" the datasource has always had — but a disabled module writes no
   * watermark, claims nothing and asks the log nothing, even with a durable listener armed and
   * wanting events.
   */
  @Test
  void theCatchupSweeperDoesNothingAndTouchesNoTable() {
    durable.wants("ThingHappened");
    try {
      assertEquals(0, catchup.catchUp());

      assertNull(watermark(TestEvents.DURABLE_CONSUMER_ID), "no consumer is initialized");
      assertEquals(0, StubEventsServer.queries().size(), "and the log is not read");
      assertEquals(
          DurableFunnel.Result.SKIPPED,
          funnel.offer(durable, new EventFrame("e-1", "ThingHappened", T0, "{}", null, null, null)),
          "and an arrival that somehow reached the funnel stores nothing");
      assertEquals(0, claims(TestEvents.DURABLE_CONSUMER_ID));
    } finally {
      durable.reset();
    }
  }

  @Test
  void theSubscriberNeverDialled() {
    assertFalse(subscriber.connected());
    assertEquals(0, StubEventsServer.openStreams());
  }
}
