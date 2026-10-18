package eu.wohlben.qits.eventstream.contracts;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import eu.wohlben.qits.eventstream.contracts.fixture.EverythingListener;
import eu.wohlben.qits.eventstream.contracts.fixture.LiveListener;
import eu.wohlben.qits.eventstream.contracts.fixture.NamedListener;
import eu.wohlben.qits.eventstream.contracts.fixture.ReplayingListener;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.opentest4j.TestAbortedException;

/** The guard (qits-1163): a pact that leaves out a listener or the publish fails, naming the class. */
class EventstreamGuardTest {

  static final Path FIXTURE = EventstreamPactTest.FIXTURE;

  static final EventstreamPact COMPLETE = EventstreamPactTest.PACT;

  @Test
  void aPactGivenEveryListenerAndThePublishPasses() {
    COMPLETE.checkGuard();
  }

  @Test
  void aMissingDurableListenerIsNamed() {
    AssertionError error = assertThrows(AssertionError.class,
        () -> COMPLETE.listeners(new NamedListener(), new EverythingListener(), new LiveListener())
            .checkGuard());
    assertTrue(error.getMessage().contains(ReplayingListener.class.getName()
        + " is a QitsDurableEventListener the pact was not given"), error.getMessage());
  }

  @Test
  void aMissingRawListenerIsNamed() {
    AssertionError error = assertThrows(AssertionError.class,
        () -> COMPLETE.listeners(new NamedListener(), new EverythingListener(), new ReplayingListener())
            .checkGuard());
    assertTrue(error.getMessage().contains(LiveListener.class.getName()
        + " is a QitsRawEventListener the pact was not given"), error.getMessage());
  }

  @Test
  void aPublisherWithoutPublishesTrueIsNamed() {
    AssertionError error = assertThrows(AssertionError.class, () -> COMPLETE.publishes(false).checkGuard());
    assertTrue(error.getMessage().contains(
        "eu.wohlben.qits.eventstream.contracts.fixture.FixturePublisher uses QitsEventBus"), error.getMessage());
  }

  @Test
  void publishesTrueWithNoPublisherFails(@TempDir Path empty) {
    assertEquals(
        List.of("publishes(true), but no class under [" + empty + "] uses QitsEventBus: set"
            + " publishes(false), or guard the directory that holds the publisher"),
        EventstreamGuard.uncovered(List.of(empty), java.util.Set.of(), true));
  }

  @Test
  void theFileGateRunsTheGuardFirst(@TempDir Path pacts) {
    assertThrows(AssertionError.class, () -> COMPLETE.publishes(false).compareOrWritePactFile(pacts));
  }

  @Test
  void aMissingClassesDirectoryIsNamed() {
    IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
        () -> COMPLETE.guard(Path.of("target/no-such-classes")).checkGuard());
    assertTrue(error.getMessage().contains("no-such-classes"), error.getMessage());
  }

  @Test
  void noRowsAndNoCommittedFileIsSkipped(@TempDir Path empty) {
    EventstreamPact none = EventstreamPact.forConsumer("qits-fixture-service").guard(empty);
    assertThrows(TestAbortedException.class, () -> none.compareOrWritePactFile(empty));
  }
}
