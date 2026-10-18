package eu.wohlben.qits.eventstream.contracts;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.wohlben.qits.eventstream.contracts.EventstreamPact.Case;
import eu.wohlben.qits.eventstream.contracts.EventstreamPact.Feature;
import eu.wohlben.qits.eventstream.contracts.fixture.EverythingListener;
import eu.wohlben.qits.eventstream.contracts.fixture.LiveListener;
import eu.wohlben.qits.eventstream.contracts.fixture.NamedListener;
import eu.wohlben.qits.eventstream.contracts.fixture.ReplayingListener;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.io.TempDir;

/**
 * <b>Every building block of the kit, run</b> (qits-1163): a sample service with one listener of
 * each durable shape — a named one, an {@code ALL} one, a replaying one — a raw listener and a
 * publisher, so every {@link Feature} is selected and every row runs the library's real client
 * against a pact mock server. The library commits no pact file of its own any more: the file test
 * here writes into a temporary directory and compares its own output back.
 */
class EventstreamPactTest {

  static final Path FIXTURE = Path.of("target/test-classes/eu/wohlben/qits/eventstream/contracts/fixture");

  static final EventstreamPact PACT =
      EventstreamPact.forConsumer("qits-fixture-service")
          .listeners(new NamedListener(), new EverythingListener(), new ReplayingListener(),
              new LiveListener())
          .publishes(true)
          .guard(FIXTURE);

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @TestFactory
  Stream<DynamicTest> everyRowIsWhatTheLibraryAsksAndUnderstands() {
    return PACT.tests();
  }

  @Test
  void theServicesInputsSelectTheRows() {
    assertEquals(Set.of(Feature.values()), PACT.features());
    List<String> rows = PACT.cases().stream().map(c -> c.description() + " [" + c.state() + "]").toList();
    assertEquals(
        List.of(
            "OutboxSweeper.tick: publishEvent (a new id) [no event with the given id]",
            "OutboxSweeper.tick: publishEvent (a reused id) [an event with the given id]",
            "CatchupSweeper.initialize(fixture-named): listEvents (one name) [no events]",
            "CatchupSweeper.catchUp(fixture-named): listEvents (one name) [no events]",
            "CatchupSweeper.initialize(fixture-everything): listEvents (every name) [no events]",
            "CatchupSweeper.catchUp(fixture-everything): listEvents (every name) [a few recent events]",
            "CatchupSweeper.catchUp(fixture-replaying): listEvents (2 names) [no events]"),
        rows,
        "no initialize row for the replaying listener, no row for the raw one");
    Case replaying = PACT.cases().getLast();
    assertEquals(
        Map.of("order", "asc", "limit", "200", "name", "ProjectChanged,ProjectCreated"),
        replaying.query(),
        "names sorted, one comma-separated parameter");
  }

  @Test
  void aServiceWithoutPublisherOrDurableListenerHasNoRows() {
    EventstreamPact live = EventstreamPact.forConsumer("qits-fixture-service").listeners(new LiveListener());
    assertTrue(live.cases().isEmpty());
    assertEquals(
        List.of(Feature.PUBLISH),
        EventstreamPact.forConsumer("x").publishes(true).cases().stream().map(Case::feature).distinct().toList());
  }

  @Test
  void theFileIsNamedForTheServiceTheLibraryAndTheProvider() {
    assertEquals("qits-fixture-service_qits-eventstream-javalib_qits-events-service.json", PACT.file());
  }

  /**
   * qits-call, qits-trigger (app: the service) and qits-via (the library at this build's version)
   * on every interaction, every value a string.
   */
  @Test
  void everyInteractionCarriesAllThreeReferences() throws IOException {
    JsonNode pact = MAPPER.readTree(PactFiles.normalise(PactFiles.written(PACT.pact())));
    assertEquals("qits-fixture-service", pact.path("consumer").path("name").asText());
    assertEquals("qits-events-service", pact.path("provider").path("name").asText());
    assertEquals("4.0", pact.path("metadata").path("pactSpecification").path("version").asText());

    String version = EventstreamVia.current().version();
    assertFalse(version.isBlank() || version.contains("${"), version);

    JsonNode interactions = pact.path("interactions");
    assertEquals(PACT.cases().size(), interactions.size(), "one interaction per row");
    Set<String> unique = new HashSet<>();
    for (JsonNode interaction : interactions) {
      String description = interaction.path("description").asText();
      JsonNode references = interaction.path("comments").path("references");
      assertEquals(Set.of("qits-call", "qits-trigger", "qits-via"), Set.copyOf(names(references)), description);

      assertEquals("qits-events-service", references.path("qits-call").path("app").asText(), description);
      JsonNode trigger = references.path("qits-trigger");
      assertEquals("schedule", trigger.path("kind").asText(), description);
      assertEquals("qits-fixture-service", trigger.path("app").asText(), description);
      assertTrue(description.startsWith(trigger.path("schedule").asText() + ": "), description);

      JsonNode via = references.path("qits-via");
      assertEquals(Set.of("repository", "artifact", "version"), Set.copyOf(names(via)), description);
      assertEquals("qits-eventstream-javalib", via.path("repository").asText(), description);
      assertEquals("eu.wohlben.qits:qits-eventstream", via.path("artifact").asText(), description);
      assertEquals(version, via.path("version").asText(), description);

      String state = interaction.path("providerStates").path(0).path("name").asText();
      assertTrue(unique.add(description + "\u0000" + state), "(description, state) repeats: " + description);
    }
  }

  @Test
  void thePactFileIsWrittenOnUpdateAndComparedOtherwise(@TempDir Path pacts) throws IOException {
    Path file = pacts.resolve(PACT.file());
    System.setProperty(PactFiles.UPDATE_PROPERTY, "true");
    try {
      PACT.compareOrWritePactFile(pacts);
    } finally {
      System.clearProperty(PactFiles.UPDATE_PROPERTY);
    }
    assertTrue(Files.exists(file));
    PACT.compareOrWritePactFile(pacts);

    Files.writeString(file, Files.readString(file).replace("OutboxSweeper.tick", "Somebody.else"));
    org.junit.jupiter.api.Assertions.assertThrows(
        AssertionError.class, () -> PACT.compareOrWritePactFile(pacts));
  }

  private static List<String> names(JsonNode node) {
    List<String> names = new java.util.ArrayList<>();
    node.fieldNames().forEachRemaining(names::add);
    node.forEach(value -> assertTrue(value.isTextual() || value.isObject(), node.toString()));
    return names;
  }
}
