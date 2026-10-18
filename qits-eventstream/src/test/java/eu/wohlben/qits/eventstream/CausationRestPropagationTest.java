package eu.wohlben.qits.eventstream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.quarkus.rest.client.reactive.QuarkusRestClientBuilder;
import io.quarkus.test.common.http.TestHTTPResource;
import io.quarkus.test.junit.QuarkusTest;
import java.net.URI;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Causation across an HTTP hop, proved on a real wire: a REST-client call made inside a scope
 * arrives at a blocking resource method that <em>sees the same cause</em>, with neither side
 * naming a parent. That sentence contains every assumption the filters make — the client filter
 * runs where the call was made, the server filter and the resource method share a worker — so it
 * is asserted end to end rather than half by half.
 *
 * <p>The requests loop where staleness is the question, because thread hygiene on a pooled worker
 * cannot be pinned to one request: a leak shows up as some <em>later</em> no-header request
 * answering with an old cause, and a handful of sequential requests is what makes the same worker
 * likely enough to catch it.
 */
@QuarkusTest
class CausationRestPropagationTest {

  private static final UUID CAUSE = UUID.fromString("11111111-1111-4111-8111-111111111111");
  private static final UUID EXPLICIT = UUID.fromString("22222222-2222-4222-8222-222222222222");

  @TestHTTPResource
  URI base;

  private CausationProbeClient client;

  @BeforeEach
  void buildClient() {
    client = QuarkusRestClientBuilder.newBuilder().baseUri(base).build(CausationProbeClient.class);
  }

  /** The feature, whole: scope here, cause there, nobody passed an argument. */
  @Test
  void aCallInsideAScopeArrivesUnderTheSameCause() {
    AtomicReference<String> answer = new AtomicReference<>();

    CausationScope.with(CAUSE, () -> answer.set(client.current()));

    assertEquals(CAUSE.toString(), answer.get());
    assertNull(CausationScope.current(), "the calling thread is left as it was found");
  }

  @Test
  void aCallOutsideAnyScopeArrivesUnderNone() {
    assertEquals("null", client.current());
  }

  /** The client-side precedence rule, mirroring {@code publish(event, parentEventId)}. */
  @Test
  void aHeaderTheCallerSetItselfBeatsTheAmbientScope() {
    AtomicReference<String> answer = new AtomicReference<>();

    CausationScope.with(CAUSE, () -> answer.set(client.currentWith(EXPLICIT.toString())));

    assertEquals(EXPLICIT.toString(), answer.get());
  }

  /** Advisory means lenient: a header that does not parse reads exactly like none at all. */
  @Test
  void aMalformedHeaderIsServedAsNoCause() {
    assertEquals("null", client.currentWith("not-a-uuid"));
  }

  /**
   * The hygiene assertion. Workers are pooled and long-lived, so the request filter's
   * establish-even-when-absent and the response filter's restore have one observable consequence
   * between them: no request ever answers with a cause a previous request carried.
   */
  @Test
  void aRequestWithoutTheHeaderNeverSeesAPreviousRequestsCause() {
    for (int i = 0; i < 8; i++) {
      CausationScope.with(CAUSE, () -> client.current());
      assertEquals("null", client.current(), "a pooled worker lent one request's cause to the next");
    }
  }
}
