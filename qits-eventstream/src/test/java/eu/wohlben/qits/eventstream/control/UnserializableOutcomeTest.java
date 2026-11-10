package eu.wohlben.qits.eventstream.control;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import eu.wohlben.qits.eventstream.QitsEventBus;
import eu.wohlben.qits.eventstream.control.TestEvents.ThingHappened;
import eu.wohlben.qits.eventstream.entity.OutboxEvent;
import eu.wohlben.qits.eventstream.entity.OutboxStatus;
import io.quarkus.arc.ClientProxy;
import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.Readiness;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * <b>A body this process cannot build is its own outcome, and it is loud</b> (qits-2055).
 *
 * <p>The incident: a native image without reflection for {@link EventEnvelope} threw inside {@code
 * CanonicalJson.envelope}, which sat in the same {@code try} as the HTTP call, so every publish came
 * out {@code UNREACHABLE} — DEBUG on the row, "qits-events is unreachable" from the sweeper (it was
 * not), and every readiness check green. A JVM serialises everything whether it was registered or
 * not, so the failure is staged here through the publisher's body seam: the one thing that differs
 * between the incident and this test is <em>why</em> the serialiser threw.
 */
@QuarkusTest
@WithTestResource(StubEventsServer.class)
class UnserializableOutcomeTest extends EventstreamTestSupport {

  /** What the native image's Jackson said, near enough. */
  private static final Function<EventEnvelope, String> NO_REFLECTION =
      envelope -> {
        throw new IllegalArgumentException(
            "cannot serialize eu.wohlben.qits.eventstream.control.EventEnvelope: No serializer"
                + " found, you may need to configure reflection");
      };

  @Inject QitsEventBus bus;
  @Inject Outbox outbox;
  @Inject OutboxSweeper sweeper;
  @Inject EventsPublisher publisher;
  @Inject UnserializableEvents unserializable;
  @Inject @Readiness PublishHealthCheck health;

  private final Warnings warnings = new Warnings();
  private final List<String> touched = new ArrayList<>();

  @BeforeEach
  void listen() {
    for (Class<?> category : List.of(EventsPublisher.class, Outbox.class, OutboxSweeper.class)) {
      java.util.logging.Logger.getLogger(category.getName()).addHandler(warnings);
    }
  }

  /** Every seam back, and nothing left in the shared tracker for the next class to find. */
  @AfterEach
  void restore() {
    for (Class<?> category : List.of(EventsPublisher.class, Outbox.class, OutboxSweeper.class)) {
      java.util.logging.Logger.getLogger(category.getName()).removeHandler(warnings);
    }
    bean().body = CanonicalJson::envelope;
    PublishHealthCheck check = ClientProxy.unwrap(health);
    check.probe = PublishHealthCheck::selfTest;
    check.reprobe();
    touched.forEach(unserializable::cleared);
  }

  private EventsPublisher bean() {
    return ClientProxy.unwrap(publisher);
  }

  private EventsPublisher handBuilt() {
    EventsPublisher handBuilt = new EventsPublisher();
    handBuilt.bearer = () -> io.smallrye.mutiny.Uni.createFrom().item("Bearer t");
    handBuilt.eventsUrl = "http://127.0.0.1:1";
    handBuilt.publishTimeout = Duration.ofSeconds(2);
    handBuilt.body = NO_REFLECTION;
    return handBuilt;
  }

  /**
   * The classification, on both calls, from a hand-built publisher pointed at a port nothing
   * listens on — so had the old code path run, the answer would have been UNREACHABLE for a second,
   * unrelated reason too. It is UNSERIALIZABLE, and the WARN names the event.
   */
  @Test
  void aBodyThatCannotBeBuiltIsUnserializableAndNeverUnreachable() {
    EventsPublisher handBuilt = handBuilt();
    ThingHappened event = new ThingHappened("shipped", 1, T0);
    String id = event.eventId().toString();

    for (EventsPublisher.Delivery attempt :
        List.of(
            handBuilt.put(id, EventEnvelope.of(event)),
            handBuilt.putInline(id, EventEnvelope.of(event)))) {
      assertTrue(attempt.unserializable(), attempt.toString());
      assertFalse(attempt.unreachable(), "nothing was asked of the network");
      assertFalse(attempt.refused());
      assertTrue(attempt.detail().contains("configure reflection"), attempt.detail());
    }
    assertTrue(
        warnings.mentioning(id, "ThingHappened", "could not be serialised"),
        "a WARN naming the id and the name: " + warnings.messages());
    assertFalse(
        handBuilt.unserializable.current().isEmpty(),
        "the hand-built path keeps its own book, and has no null to trip on");
  }

  /**
   * Through the bus: the row is kept, PENDING, and has spent none of its refusal budget — it is
   * owed, exactly as an unreachable one is — while the readiness check is DOWN naming it. Nothing
   * reached the stub, because nothing was sent.
   */
  @Test
  void theBusKeepsTheRowOwedAndReadinessGoesDownUntilItIsDelivered() {
    bean().body = NO_REFLECTION;
    ThingHappened event = new ThingHappened("shipped", 1, T0);
    String id = event.eventId().toString();
    touched.add(id);

    bus.publish(event);

    OutboxEvent row = row(id);
    assertEquals(OutboxStatus.PENDING, row.status);
    assertEquals(1, row.attempts);
    assertEquals(0, row.refusals, "an unbuilt body is not a refusal");
    assertTrue(row.lastError.contains("not serialisable"), row.lastError);
    assertEquals(0, StubEventsServer.puts().size(), "nothing was sent");

    HealthCheckResponse down = health.call();
    assertEquals(HealthCheckResponse.Status.DOWN, down.getStatus());
    String events = String.valueOf(down.getData().orElseThrow().get("events"));
    assertTrue(events.contains(id) && events.contains("ThingHappened"), events);
    assertEquals("passed", down.getData().orElseThrow().get("selfTest"));

    // The fixed deployment: the same row, the same sweeper, a serialiser that works.
    bean().body = CanonicalJson::envelope;
    clock.jumpTo(row.nextAttemptAt);
    assertEquals(1, sweeper.sweep());

    assertEquals(0, outboxCount(), "delivered on the sweep, and not kept");
    assertEquals(1, StubEventsServer.puts().size());
    assertEquals(HealthCheckResponse.Status.UP, health.call().getStatus(), "and ready again");
  }

  /**
   * The sweeper says what happened and does not say the bus is down. Several sweeps, so the budget
   * claim is about the retries and not only the first attempt.
   */
  @Test
  void theSweeperNeverCallsItAnOutageAndSpendsNoBudget() {
    bean().body = NO_REFLECTION;
    ThingHappened event = new ThingHappened("shipped", 1, T0);
    String id = event.eventId().toString();
    touched.add(id);
    outbox.enqueue(
        id,
        EventEnvelope.of(event),
        new EventsPublisher.Delivery(EventsPublisher.Outcome.UNSERIALIZABLE, "not serialisable"));

    for (int sweep = 1; sweep <= 6; sweep++) {
      clock.jumpTo(row(id).nextAttemptAt);
      assertEquals(1, sweeper.sweep(), "sweep " + sweep);
    }

    OutboxEvent row = row(id);
    assertEquals(OutboxStatus.PENDING, row.status, "six tries past the five-refusal budget");
    assertEquals(7, row.attempts);
    assertEquals(0, row.refusals);
    assertEquals(0, StubEventsServer.puts().size());
    assertFalse(
        warnings.mentioning("unreachable"),
        "no line may call this an outage: " + warnings.messages());
    assertTrue(
        warnings.mentioning("could not be serialised here", "not an outage"),
        warnings.messages().toString());
    assertTrue(
        warnings.mentioning(id, "could not be serialised on attempt"),
        "the outbox's own WARN, naming the row: " + warnings.messages());
  }

  /** A binary that cannot round-trip the wire is DOWN before anything is owed. */
  @Test
  void aFailedSelfTestIsDownAndNamesTheFailure() {
    PublishHealthCheck check = ClientProxy.unwrap(health);
    check.probe = () -> Optional.of("EventEnvelope: no serializer");
    check.reprobe();

    HealthCheckResponse response = health.call();

    assertEquals(HealthCheckResponse.Status.DOWN, response.getStatus());
    assertEquals(
        "failed: EventEnvelope: no serializer", response.getData().orElseThrow().get("selfTest"));

    check.probe = () -> {
      throw new LinkageError("missing reflection registration");
    };
    check.reprobe();
    assertEquals(
        HealthCheckResponse.Status.DOWN, health.call().getStatus(), "a throw is a failure");
  }

  /** And on a JVM, where every class reflects, the real self-test passes and the check is UP. */
  @Test
  void theRealSelfTestPassesAndAnIdleProcessIsUp() {
    assertEquals(Optional.empty(), PublishHealthCheck.selfTest());
    assertEquals(HealthCheckResponse.Status.UP, health.call().getStatus());
  }

  /** WARN and above from the publishing path, rendered with their parameters. */
  private static final class Warnings extends Handler {

    private final List<String> rendered = new CopyOnWriteArrayList<>();

    @Override
    public void publish(LogRecord record) {
      if (record.getLevel().intValue() >= Level.WARNING.intValue()) {
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

    boolean mentioning(String... texts) {
      return rendered.stream()
          .anyMatch(message -> Arrays.stream(texts).allMatch(message::contains));
    }
  }
}
