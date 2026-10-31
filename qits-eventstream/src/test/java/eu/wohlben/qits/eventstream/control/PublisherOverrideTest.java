package eu.wohlben.qits.eventstream.control;

import static org.junit.jupiter.api.Assertions.assertEquals;

import eu.wohlben.qits.eventstream.QitsEventBus;
import eu.wohlben.qits.eventstream.control.TestEvents.ThingHappened;
import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The publisher, overridden (qits-1770): {@code qits.eventstream.publisher} set by the application
 * beats the build-time default {@link PublisherStampingTest} sees, on the wire, on the outbox row
 * and on the swept retry. The deployer's spelling is {@code QITS_EVENTSTREAM_PUBLISHER}; a test
 * profile's override stands in for either, both sitting above the recorded default.
 */
@QuarkusTest
@TestProfile(PublisherOverrideTest.Overridden.class)
@WithTestResource(StubEventsServer.class)
class PublisherOverrideTest extends EventstreamTestSupport {

  static final String OVERRIDE = "eu.wohlben.qits:qits-override-probe";

  public static class Overridden implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of("qits.eventstream.publisher", OVERRIDE);
    }
  }

  @Inject QitsEventBus bus;
  @Inject OutboxSweeper sweeper;

  @Test
  void theConfiguredPublisherBeatsTheBuildDefault() {
    StubEventsServer.answerWith(201);

    bus.publish(new ThingHappened("shipped", 1, T0));

    String body = StubEventsServer.puts().get(0).body();
    assertEquals(OVERRIDE, CanonicalJson.parse(body).get("publisher").asText());
  }

  @Test
  void theOverrideIsStoredAndASweptRetryResendsItByteForByte() {
    StubEventsServer.answerWith(503, 200);
    ThingHappened event = new ThingHappened("shipped", 1, T0);
    bus.publish(event);
    String id = event.eventId().toString();
    assertEquals(OVERRIDE, row(id).publisher);

    clock.jumpTo(row(id).nextAttemptAt);
    assertEquals(1, sweeper.sweep());

    List<StubEventsServer.Put> puts = StubEventsServer.puts();
    assertEquals(2, puts.size());
    assertEquals(puts.get(0).body(), puts.get(1).body(), "a retry is the same request, byte for byte");
  }
}
