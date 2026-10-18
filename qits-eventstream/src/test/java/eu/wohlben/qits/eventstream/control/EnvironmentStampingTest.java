package eu.wohlben.qits.eventstream.control;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import eu.wohlben.qits.eventstream.QitsEventBus;
import eu.wohlben.qits.eventstream.control.TestEvents.ThingHappened;
import io.quarkus.narayana.jta.QuarkusTransaction;
import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The environment tier, stamped from configuration — the configured half of the rule whose fallback
 * half ({@code "platform"} where no {@code qits.environment} is set) the rest of this suite
 * exercises for free, because the suite never sets the property.
 *
 * <p>Asserted on the <b>recorded PUT bodies</b> for {@code CausationStampingTest}'s reason: the tier
 * is part of qits-events' idempotency comparison, so the wire is the contract and an envelope built
 * in the test proves nothing. The property is spelled {@code qits.environment} — the MicroProfile
 * reading of the {@code QITS_ENVIRONMENT} the deployer injects into environment-tier services.
 */
@QuarkusTest
@TestProfile(EnvironmentStampingTest.DevTier.class)
@WithTestResource(StubEventsServer.class)
class EnvironmentStampingTest extends EventstreamTestSupport {

  public static class DevTier implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of("qits.environment", "dev");
    }
  }

  @Inject QitsEventBus bus;
  @Inject OutboxSweeper sweeper;

  @Test
  void publishStampsTheConfiguredTierIntoTheEnvelopeAndNeverThePayload() {
    StubEventsServer.answerWith(201);
    ThingHappened event = new ThingHappened("shipped", 1, T0);

    bus.publish(event);

    StubEventsServer.Put put = StubEventsServer.puts().get(0);
    assertEquals("dev", CanonicalJson.parse(put.body()).get("environment").asText());
    // The payload stays byte-identical whatever tier it was published from — the same lesson the
    // causation mix-in taught: qits-events compares the payload verbatim, so a tier that entered it
    // would make one occurrence published from two configurations two events.
    String payload = CanonicalJson.parse(put.body()).get("payload").asText();
    assertEquals(CanonicalJson.payload(new ThingHappened("shipped", 1, T0)), payload);
    assertFalse(payload.contains("environment"), "no tier in the compared bytes: " + payload);
  }

  @Test
  void aPublishThatFailsInlineWritesTheTierOnTheRow() {
    StubEventsServer.answerWith(503);
    ThingHappened event = new ThingHappened("shipped", 1, T0);

    bus.publish(event);

    assertEquals("dev", rowEnvironment(event));
  }

  /**
   * The sweeper resends the stored envelope, and the tier is part of the comparison — so a retry
   * that re-read configuration would, after a reconfiguration or an upgrade, send a different
   * request than the attempt it is retrying and 400 against its own landed first attempt. The row
   * is the record; nothing ambient is re-read.
   */
  @Test
  void aSweptRetryCarriesTheOriginalTierByteForByte() {
    StubEventsServer.answerWith(503, 200);
    ThingHappened event = new ThingHappened("shipped", 1, T0);
    bus.publish(event);

    clock.jumpTo(rowNextAttempt(event));
    assertEquals(1, sweeper.sweep());

    List<StubEventsServer.Put> puts = StubEventsServer.puts();
    assertEquals(2, puts.size());
    assertEquals(puts.get(0).body(), puts.get(1).body(), "a retry is the same request, byte for byte");
    assertEquals("dev", CanonicalJson.parse(puts.get(1).body()).get("environment").asText());
    assertEquals(0, outboxCount(), "and the 200 removed the row");
  }

  private String rowEnvironment(ThingHappened event) {
    return QuarkusTransaction.requiringNew()
        .call(() -> outboxRows.findById(event.eventId().toString()).environment);
  }

  private java.time.Instant rowNextAttempt(ThingHappened event) {
    return QuarkusTransaction.requiringNew()
        .call(() -> outboxRows.findById(event.eventId().toString()).nextAttemptAt);
  }
}
