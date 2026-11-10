package eu.wohlben.qits.eventstream.deployment;

import static org.junit.jupiter.api.Assertions.fail;

import io.quarkus.test.QuarkusUnitTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * A {@code QitsEvent} record with a {@code signature} component fails the application's BUILD
 * (qits-2055): {@code name()} defaults to {@code signature()}, so the component renames the event
 * on the wire and in the subscription set alike.
 */
class SignatureComponentRefusedTest {

  @RegisterExtension
  static final QuarkusUnitTest BUILD =
      new QuarkusUnitTest()
          .withApplicationRoot(jar -> jar.addClasses(Fixtures.ThingSigned.class))
          .overrideConfigKey("quarkus.devservices.enabled", "false")
          .assertException(
              thrown ->
                  BuildFailures.assertNames(
                      thrown, Fixtures.ThingSigned.class, "record component `signature`"));

  @Test
  void theBuildFails() {
    fail("augmentation must have failed before any test ran");
  }
}
