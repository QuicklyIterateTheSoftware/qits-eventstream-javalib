package eu.wohlben.qits.eventstream.control;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import eu.wohlben.qits.eventstream.QitsEventBus;
import eu.wohlben.qits.eventstream.control.TestEvents.ThingHappened;
import io.quarkus.narayana.jta.QuarkusTransaction;
import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.Test;

/**
 * The publisher where NO DEFAULT was recorded (qits-1770) — which is this module's own suite, and
 * the reason the build-time default is proved one module over, in {@code
 * qits-eventstream-consumer-test}.
 *
 * <p>The default ({@code groupId:artifactId} of the consuming application) is recorded by {@code
 * qits-eventstream-deployment}'s build step, and Quarkus loads a deployment module only for an
 * extension that is a DEPENDENCY of the application. Under this module's {@code @QuarkusTest} the
 * application IS this module, and its own {@code quarkus-extension.properties} is never read
 * ({@code ApplicationDependencyResolver} flags extensions among the dependencies, not the root). So
 * {@code qits.eventstream.publisher} is absent here, and the bus must stamp an explicit null — never
 * fail a publish over a missing name, which is what a required {@code String} injection would do.
 *
 * <p>What does not need the build step is proved here: the stamp travels as an explicit null, and a
 * sweep resends the STORED value rather than whatever configuration now says. The configured half
 * is {@link PublisherOverrideTest}.
 */
@QuarkusTest
@WithTestResource(StubEventsServer.class)
class PublisherStampingTest extends EventstreamTestSupport {

  @Inject QitsEventBus bus;
  @Inject OutboxSweeper sweeper;

  @ConfigProperty(name = "qits.eventstream.publisher")
  Optional<String> publisher;

  @Test
  void theApplicationUnderTestIsNotAnExtensionSoNoDefaultIsRecorded() {
    assertEquals(Optional.empty(), publisher);
  }

  @Test
  void withNoPublisherConfiguredTheKeyIsAnExplicitNull() {
    StubEventsServer.answerWith(201);

    bus.publish(new ThingHappened("shipped", 1, T0));

    JsonNode root = CanonicalJson.parse(StubEventsServer.puts().get(0).body());
    assertTrue(root.has("publisher"), "the key is always present: " + root);
    assertTrue(root.get("publisher").isNull(), "and it is an explicit null: " + root);
  }

  /**
   * The sweep resends the STORED publisher and never re-reads configuration. Configuration cannot be
   * changed under a running test, so the row is changed instead — the stand-in for "the process was
   * reconfigured or upgraded between the inline attempt and the sweep" — and the retry must carry
   * the row's value, not the one this process would stamp now (null).
   */
  @Test
  void aSweptRetryCarriesTheStoredPublisherNotTheConfiguredOne() {
    StubEventsServer.answerWith(503, 200);
    ThingHappened event = new ThingHappened("shipped", 1, T0);
    bus.publish(event);
    String id = event.eventId().toString();
    QuarkusTransaction.requiringNew()
        .run(() -> outboxRows.findById(id).publisher = "eu.wohlben.qits:the-inline-attempt");

    clock.jumpTo(row(id).nextAttemptAt);
    assertEquals(1, sweeper.sweep());

    List<StubEventsServer.Put> puts = StubEventsServer.puts();
    assertEquals(2, puts.size());
    assertEquals(
        "eu.wohlben.qits:the-inline-attempt",
        CanonicalJson.parse(puts.get(1).body()).get("publisher").asText());
    assertEquals(0, outboxCount(), "and the 200 removed the row");
  }
}
