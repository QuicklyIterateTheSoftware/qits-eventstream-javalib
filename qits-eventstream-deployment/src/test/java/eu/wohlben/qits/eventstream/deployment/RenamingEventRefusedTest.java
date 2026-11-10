package eu.wohlben.qits.eventstream.deployment;

import static org.junit.jupiter.api.Assertions.fail;

import io.quarkus.test.QuarkusUnitTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * A {@code QitsEvent} record with a {@code name} component fails the application's BUILD
 * (qits-2055) — augmentation itself, not a test that happened to look. Without the refusal this
 * event would publish under the value of its {@code name} field, so no listener would ever hear it.
 */
class RenamingEventRefusedTest {

  @RegisterExtension
  static final QuarkusUnitTest BUILD =
      new QuarkusUnitTest()
          .withApplicationRoot(jar -> jar.addClasses(Fixtures.ThingNamed.class))
          .overrideConfigKey("quarkus.devservices.enabled", "false")
          .assertException(
              thrown ->
                  BuildFailures.assertNames(
                      thrown, Fixtures.ThingNamed.class, "record component `name`"));

  @Test
  void theBuildFails() {
    fail("augmentation must have failed before any test ran");
  }
}
