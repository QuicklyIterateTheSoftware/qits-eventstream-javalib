package eu.wohlben.qits.eventstream.contracts;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;

import au.com.dius.pact.consumer.ConsumerPactRunnerKt;
import au.com.dius.pact.consumer.PactVerificationResult;
import au.com.dius.pact.consumer.model.MockProviderConfig;
import au.com.dius.pact.core.model.PactSpecVersion;
import eu.wohlben.qits.eventstream.control.ContractProbe;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

/**
 * <b>The consumer half of the qits-events contract</b> (qits-1149): this library's real {@code
 * EventsQuery} and {@code EventsPublisher} (through {@link ContractProbe}) making a real HTTP call to
 * a pact-jvm mock server that answers exactly what an {@link EventsContract} row promises, and the
 * row's own assertions on what the client made of it.
 *
 * <p>One test per row and one mock server per row: several rows send the same {@code GET} with
 * different queries in the same state. A pending row is skipped with the provider state it needs.
 * Plain JUnit, no Quarkus: the clients need only their url.
 */
class EventsConsumerPactTest {

  static {
    // pact-jvm reports usage metrics over the network unless told not to; a test never should.
    System.setProperty("pact_do_not_track", "true");
  }

  @TestFactory
  Stream<DynamicTest> everyRowIsWhatTheLibraryAsksAndUnderstands() {
    assertFalse(EventsContract.ready().isEmpty());
    return EventsContract.CASES.stream()
        .map(row -> DynamicTest.dynamicTest(row.description() + " [" + row.state() + "]",
            () -> verify(row)));
  }

  private static void verify(EventsContract.Case row) {
    Assumptions.assumeTrue(row.pending() == null, row::pending);
    PactVerificationResult result =
        ConsumerPactRunnerKt.runConsumerTest(
            EventsContract.pact(List.of(row)),
            MockProviderConfig.createDefault(PactSpecVersion.V4),
            (mockServer, context) -> {
              row.call()
                  .run(new ContractProbe(mockServer.getUrl()), EventsGoldenMasters.params(row.state()));
              return null;
            });
    if (!(result instanceof PactVerificationResult.Ok)) {
      fail(describe(result));
    }
  }

  private static String describe(PactVerificationResult result) {
    if (result instanceof PactVerificationResult.Error error) {
      return "error: " + error.getError();
    }
    return result.getDescription() + " — " + result;
  }
}
