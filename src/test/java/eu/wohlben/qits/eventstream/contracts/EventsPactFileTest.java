package eu.wohlben.qits.eventstream.contracts;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.com.dius.pact.core.model.DefaultPactWriter;
import au.com.dius.pact.core.model.PactSpecVersion;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * <b>The committed consumer pact, {@value #FILE}</b>, and the references every interaction in it
 * carries (qits-1149). The release packs {@code pacts/*_qits-events-service.json} into {@code
 * eu.wohlben.qits:qits-eventstream-javalib-pacts-qits-events-service} (release.yml {@code
 * contracts:}), which qits-events-service pins and verifies.
 *
 * <p><b>Compare by default.</b> The test normalises what pact-jvm writes (interactions sorted by
 * description then state, pact-jvm's own version stripped, 2-space indentation, one trailing
 * newline) and compares it to the committed file. {@code -Dgolden.update=true} (or {@code
 * QITS_GOLDEN_UPDATE=true}) rewrites it instead.
 */
class EventsPactFileTest {

  static final String FILE = "qits-eventstream-javalib_qits-events-service.json";

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  void theCommittedPactIsWhatTheContractWrites() throws IOException {
    Path committed = Path.of("pacts", FILE);
    String actual = normalise(written());
    boolean update =
        "true".equalsIgnoreCase(System.getenv("QITS_GOLDEN_UPDATE"))
            || Boolean.getBoolean("golden.update");
    if (update) {
      Files.createDirectories(committed.getParent());
      Files.writeString(committed, actual);
      return;
    }
    assertTrue(
        Files.exists(committed),
        committed.toAbsolutePath() + " is missing: run once with -Dgolden.update=true and review it");
    assertEquals(
        Files.readString(committed),
        actual,
        committed + " differs from what EventsContract writes; rerun with -Dgolden.update=true if"
            + " the change is intended");
  }

  /**
   * Both references on every interaction: {@code qits-call} naming the provider operation, {@code
   * qits-trigger} naming the schedule that makes the call, every value a string.
   */
  @Test
  void everyInteractionCarriesBothReferences() throws IOException {
    JsonNode pact = MAPPER.readTree(normalise(written()));
    assertEquals(EventsGoldenMasters.CONSUMER, pact.path("consumer").path("name").asText());
    assertEquals(EventsGoldenMasters.PROVIDER, pact.path("provider").path("name").asText());
    assertEquals("4.0", pact.path("metadata").path("pactSpecification").path("version").asText());

    JsonNode interactions = pact.path("interactions");
    assertEquals(EventsContract.ready().size(), interactions.size(), "one interaction per row");
    Set<String> unique = new HashSet<>();
    for (JsonNode interaction : interactions) {
      String description = interaction.path("description").asText();
      JsonNode references = interaction.path("comments").path("references");

      JsonNode call = references.path("qits-call");
      assertEquals(Set.of("app", "operationId"), keys(call), description);
      assertEquals(EventsGoldenMasters.PROVIDER, call.path("app").asText(), description);

      JsonNode trigger = references.path("qits-trigger");
      assertEquals(
          Map.of("kind", "schedule", "app", EventsGoldenMasters.CONSUMER),
          Map.of("kind", trigger.path("kind").asText(), "app", trigger.path("app").asText()),
          description);
      assertTrue(
          description.startsWith(trigger.path("schedule").asText() + ": "),
          description + ": the description leads with the trigger");

      String state = interaction.path("providerStates").path(0).path("name").asText();
      assertTrue(unique.add(description + "\u0000" + state), "(description, state) repeats: " + description);
    }
  }

  private static Set<String> keys(JsonNode group) {
    Set<String> keys = new HashSet<>();
    group.fieldNames().forEachRemaining(keys::add);
    return keys;
  }

  /** The pact as pact-jvm's own writer serialises it. */
  static String written() {
    StringWriter out = new StringWriter();
    try (PrintWriter writer = new PrintWriter(out)) {
      DefaultPactWriter.INSTANCE.writePact(EventsContract.pact(), writer, PactSpecVersion.V4);
    }
    return out.toString();
  }

  /** Sorted, version-stripped, 2-space indented, one trailing newline. */
  static String normalise(String raw) throws IOException {
    ObjectNode pact = (ObjectNode) MAPPER.readTree(raw);
    if (pact.path("metadata") instanceof ObjectNode meta) {
      meta.remove("pact-jvm");
    }
    if (pact.path("interactions") instanceof ArrayNode interactions) {
      List<JsonNode> sorted = new ArrayList<>();
      interactions.forEach(sorted::add);
      sorted.sort(
          Comparator.comparing((JsonNode i) -> i.path("description").asText())
              .thenComparing(i -> i.path("providerStates").path(0).path("name").asText()));
      interactions.removeAll();
      sorted.forEach(interactions::add);
    }
    StringBuilder out = new StringBuilder();
    print(pact, "", out);
    return out.append('\n').toString();
  }

  /** {@code JSON.stringify(value, null, 2)}: no space before a colon, empty containers as {} / []. */
  private static void print(JsonNode node, String indent, StringBuilder out) throws IOException {
    String inner = indent + "  ";
    if (node.isObject()) {
      if (node.isEmpty()) {
        out.append("{}");
        return;
      }
      out.append("{\n");
      Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
      while (fields.hasNext()) {
        Map.Entry<String, JsonNode> field = fields.next();
        out.append(inner).append(MAPPER.writeValueAsString(field.getKey())).append(": ");
        print(field.getValue(), inner, out);
        out.append(fields.hasNext() ? ",\n" : "\n");
      }
      out.append(indent).append('}');
    } else if (node.isArray()) {
      if (node.isEmpty()) {
        out.append("[]");
        return;
      }
      out.append("[\n");
      for (int i = 0; i < node.size(); i++) {
        out.append(inner);
        print(node.get(i), inner, out);
        out.append(i < node.size() - 1 ? ",\n" : "\n");
      }
      out.append(indent).append(']');
    } else {
      out.append(MAPPER.writeValueAsString(node));
    }
  }
}
