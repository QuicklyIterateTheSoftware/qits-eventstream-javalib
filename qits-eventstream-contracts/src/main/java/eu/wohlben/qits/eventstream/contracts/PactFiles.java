package eu.wohlben.qits.eventstream.contracts;

import au.com.dius.pact.core.model.DefaultPactWriter;
import au.com.dius.pact.core.model.PactSpecVersion;
import au.com.dius.pact.core.model.V4Pact;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;

/**
 * The committed pact file: where it lives, how it is normalised, and the compare-or-rewrite gate —
 * the conventions of qits-pact-consumer's {@code ConsumerPact}, so a service's eventstream pact
 * reads like its other pacts.
 *
 * <p><b>A library bump only refreshes the label</b> (qits-1163). Every interaction's {@code
 * comments.references.qits-via.version} names the qits-eventstream on the classpath, so a bump of
 * the service's pin changes it. The comparison therefore masks that one value on both sides: when
 * the masked forms agree and only the version differs, the committed file is rewritten in place
 * and the test passes, printing one line. The rows themselves always come from the pinned library
 * on every build; only the version label in the committed file can lag, until the next run that
 * sees it rewrites it. Any other difference fails as before (or is rewritten under {@code
 * golden.update}).
 *
 * <p>Normalised: interactions sorted by description then state, pact-jvm's own version stripped,
 * 2-space indentation ({@code JSON.stringify(value, null, 2)}), one trailing newline.
 */
final class PactFiles {

  static final String UPDATE_PROPERTY = "golden.update";
  static final String UPDATE_ENV = "QITS_GOLDEN_UPDATE";
  static final String PACTS_DIR_PROPERTY = "qits.pacts.dir";

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private PactFiles() {}

  /**
   * {@code -Dqits.pacts.dir} when set, else {@code pacts/} in the repository root — the topmost
   * directory of the unbroken chain of {@code pom.xml} directories above the working directory, so
   * it does not matter which module surefire runs in.
   */
  static Path pactsDirectory() {
    String override = System.getProperty(PACTS_DIR_PROPERTY);
    if (override != null && !override.isBlank()) {
      return Path.of(override);
    }
    Path dir = Path.of("").toAbsolutePath();
    if (!Files.isRegularFile(dir.resolve("pom.xml"))) {
      throw new IllegalStateException("no pom.xml in " + dir + "; set -D" + PACTS_DIR_PROPERTY);
    }
    while (dir.getParent() != null && Files.isRegularFile(dir.getParent().resolve("pom.xml"))) {
      dir = dir.getParent();
    }
    return dir.resolve("pacts");
  }

  /** Compare {@code pact} to {@code committed}, or rewrite it; a null pact means "no rows". */
  static void compareOrWrite(Path committed, V4Pact pact) {
    if (pact == null) {
      Assertions.assertFalse(
          Files.exists(committed),
          committed + " is committed, but the pact's inputs select no row any more: delete it");
      Assumptions.abort("no eventstream pact to write: no durable listener and no publish");
    }
    try {
      String actual = normalise(written(pact));
      if ("true".equalsIgnoreCase(System.getenv(UPDATE_ENV)) || Boolean.getBoolean(UPDATE_PROPERTY)) {
        Files.createDirectories(committed.toAbsolutePath().getParent());
        Files.writeString(committed, actual);
        return;
      }
      Assertions.assertTrue(
          Files.exists(committed),
          committed.toAbsolutePath()
              + " is missing: run once with -D" + UPDATE_PROPERTY + "=true and review it");
      String expected = Files.readString(committed);
      if (!expected.equals(actual) && maskVia(expected).equals(maskVia(actual))) {
        Files.writeString(committed, actual);
        System.out.println(
            committed + ": qits-via version refreshed from " + viaVersion(expected) + " to "
                + viaVersion(actual) + " (the rows are unchanged)");
        return;
      }
      Assertions.assertEquals(
          expected,
          actual,
          committed + " differs from what the eventstream pact writes; rerun with -D"
              + UPDATE_PROPERTY + "=true if the change is intended");
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static final String MASK = "<qits-via version>";

  /** {@code text} reprinted with every interaction's qits-via version replaced by {@link #MASK}. */
  static String maskVia(String text) throws IOException {
    ObjectNode pact = (ObjectNode) MAPPER.readTree(text);
    for (JsonNode interaction : pact.path("interactions")) {
      if (interaction.path("comments").path("references").path("qits-via") instanceof ObjectNode via
          && via.has("version")) {
        via.put("version", MASK);
      }
    }
    StringBuilder out = new StringBuilder();
    print(pact, "", out);
    return out.toString();
  }

  /** The distinct qits-via versions the file names, comma-joined (one, in a well-formed file). */
  static String viaVersion(String text) throws IOException {
    java.util.Set<String> versions = new java.util.TreeSet<>();
    for (JsonNode interaction : MAPPER.readTree(text).path("interactions")) {
      JsonNode version = interaction.path("comments").path("references").path("qits-via").path("version");
      if (version.isTextual()) {
        versions.add(version.asText());
      }
    }
    return String.join(",", versions);
  }

  /** The pact as pact-jvm's own writer serialises it. */
  static String written(V4Pact pact) {
    StringWriter out = new StringWriter();
    try (PrintWriter writer = new PrintWriter(out)) {
      DefaultPactWriter.INSTANCE.writePact(pact, writer, PactSpecVersion.V4);
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
