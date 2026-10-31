package eu.wohlben.qits.eventstream.consumertest;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.wohlben.qits.eventstream.QitsEvent;
import eu.wohlben.qits.eventstream.QitsEventBus;
import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.time.Instant;
import java.util.UUID;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.Test;

/**
 * The build-time default of {@code qits.eventstream.publisher} (qits-1770), seen the way a real
 * consumer sees it: qits-eventstream is a dependency of this application, Quarkus resolves {@code
 * qits-eventstream-deployment} from the runtime jar's descriptor, and its build step records this
 * application's own {@code groupId:artifactId}. Nothing else in the build knows that string, so
 * finding it on the wire proves the extension resolved and its step ran.
 */
@QuarkusTest
@WithTestResource(EmbeddedStore.class)
@WithTestResource(RecordingEventsServer.class)
class PublisherDefaultTest {

  /** This module's coordinates — the application being augmented. */
  static final String THIS_APPLICATION = "eu.wohlben.qits:qits-eventstream-consumer-test";

  record SomethingHappened(UUID eventId, Instant occurredAt) implements QitsEvent {}

  @Inject QitsEventBus bus;

  @ConfigProperty(name = "qits.eventstream.publisher")
  String publisher;

  @Test
  void theDefaultIsTheApplicationsGroupIdAndArtifactId() {
    assertEquals(THIS_APPLICATION, publisher);
  }

  @Test
  void publishStampsItOnTheEnvelope() throws Exception {
    RecordingEventsServer.BODIES.clear();

    bus.publish(new SomethingHappened(UUID.randomUUID(), Instant.parse("2026-10-31T12:00:00Z")));

    assertEquals(1, RecordingEventsServer.BODIES.size());
    assertEquals(
        THIS_APPLICATION,
        new ObjectMapper().readTree(RecordingEventsServer.BODIES.get(0)).get("publisher").asText());
  }
}
