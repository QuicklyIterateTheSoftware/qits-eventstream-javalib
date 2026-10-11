package eu.wohlben.qits.eventstream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/**
 * THE EXTRACTION RULE, which outlived the extraction.
 *
 * <p><b>It was written to make lifting this library out of qits-ci a {@code git mv} plus a pom</b>,
 * by failing the build on any {@code eu.wohlben.qits.ci.*} import that had crept in because the
 * class happened to be right there. That job is done, and on the face of it the rule is now
 * structural: this repo cannot import what is not on its classpath, and adding a consumer to the
 * pom would be visible in the diff that did it.
 *
 * <p><b>It is kept, and widened, because the direction it protects is permanent.</b> A library that
 * has left is not a library that cannot be dragged back: the failure mode is a "temporary"
 * dependency added to this pom to reach one class of a consumer's, at which point every consumer of
 * this jar inherits it and the arrow that made the split worth doing points both ways. The rule is
 * therefore no longer about qits-ci — it is about <em>any</em> consumer — so the grep now rejects
 * every {@code eu.wohlben.qits.*} package except this library's own. qits-ci was only the first
 * consumer; naming it here would have been a rule about a repository rather than about a direction.
 *
 * <p>The class name stays as it was, because "the extraction rule" is what this is called in
 * qits-ci's own notes and in this repo's README, and a rule that changes its name loses the
 * argument that went with it.
 *
 * <p>Grepping the sources rather than the classpath is deliberate and is the part that still earns
 * its keep: a compile-time dependency is caught by the pom, and this catches the thing the pom
 * cannot — a source file reaching for a type that some transitive jar happened to bring along.
 */
class ExtractionRuleTest {

  /**
   * Any package of this platform's that is not this library's own. The negative lookahead is what
   * makes the rule directional rather than a list of repositories to keep up to date.
   *
   * <p><b>{@code eu.wohlben.qits.http} is exempt too (qits-1198).</b> It is qits-http-core —
   * {@code QitsHttpClients}, the wrapper {@link eu.wohlben.qits.eventstream.control.EventsPublisher}
   * and {@link eu.wohlben.qits.eventstream.control.EventsQuery} build their {@code HttpClient}
   * through — and it sits upstream of every consumer of this jar exactly as
   * {@code jakarta.ws.rs} does, never downstream of one: nothing in it names a consumer's package,
   * so a reference to it does not point the arrow this rule polices back at anybody. The rule stays
   * a rejection of consumer packages, not of every {@code eu.wohlben.qits.*} name on principle —
   * qits-db-core is the other upstream library and is kept out of source on purpose (README), but
   * only because its one use is a config string that never needed the class imported at all.
   */
  private static final Pattern FOREIGN =
      Pattern.compile("eu\\.wohlben\\.qits\\.(?!eventstream\\b|http\\b)\\w+");

  @Test
  void noSourceHereNamesAConsumersPackage() throws IOException {
    List<Path> roots =
        Stream.of(Path.of("src/main/java"), Path.of("src/test/java")).filter(Files::isDirectory)
            .toList();
    assertEquals(2, roots.size(), "expected this repo's sources at src/{main,test}/java");

    List<String> offenders = new ArrayList<>();
    for (Path root : roots) {
      try (Stream<Path> sources = Files.walk(root)) {
        for (Path source :
            sources
                .filter(p -> p.toString().endsWith(".java"))
                // This file spells foreign packages on purpose; it is the only one allowed to.
                .filter(p -> !p.getFileName().toString().equals("ExtractionRuleTest.java"))
                .toList()) {
          List<String> lines = Files.readAllLines(source);
          for (int i = 0; i < lines.size(); i++) {
            Matcher hit = FOREIGN.matcher(lines.get(i));
            if (hit.find()) {
              offenders.add(source + ":" + (i + 1) + " " + lines.get(i).strip());
            }
          }
        }
      }
    }

    assertTrue(
        offenders.isEmpty(),
        "this library must know about no consumer of it: only eu.wohlben.qits.eventstream.* may"
            + " appear in these sources, and a dependency pointing back at a consumer is the split"
            + " undone:\n  "
            + String.join("\n  ", offenders));
  }
}
