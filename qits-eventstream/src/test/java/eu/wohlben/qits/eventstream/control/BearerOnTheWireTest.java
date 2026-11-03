package eu.wohlben.qits.eventstream.control;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpServer;
import eu.wohlben.qits.eventstream.EventStreamBearer;
import io.smallrye.mutiny.Uni;
import io.vertx.core.Vertx;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * What the three callers put on the wire, and what they do not (qits-1328): the bearer and no
 * forwarded user/roles pair when there is a token; no request at all when there is none.
 *
 * <p>No Quarkus: the clients are built by hand, the way the contracts kit builds them, against a JDK
 * server that records every request's headers. The Quarkus suites assert the same thing end to end
 * with the real default bearer ({@code OutboxFlowTest}, {@code EventStreamSubscriberTest}).
 */
class BearerOnTheWireTest {

  private static final EventStreamBearer FIXED = () -> Uni.createFrom().item("Bearer fixed-token");

  private static final EventStreamBearer NONE =
      () ->
          Uni.createFrom()
              .failure(
                  new EventStreamBearer.Missing(
                      "no OIDC client " + QitsClientBearer.CLIENT_KEY + " is configured"));

  private HttpServer server;
  private final List<Headers> requests = new CopyOnWriteArrayList<>();

  @BeforeEach
  void start() throws IOException {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/",
        exchange -> {
          requests.add(exchange.getRequestHeaders());
          exchange.getRequestBody().readAllBytes();
          byte[] body =
              exchange.getRequestMethod().equals("GET")
                  ? "{\"events\":[],\"nextCursor\":null}".getBytes(StandardCharsets.UTF_8)
                  : new byte[0];
          exchange.getResponseHeaders().add("Content-Type", "application/json");
          int status = exchange.getRequestMethod().equals("GET") ? 200 : 201;
          exchange.sendResponseHeaders(status, body.length == 0 ? -1 : body.length);
          if (body.length > 0) {
            exchange.getResponseBody().write(body);
          }
          exchange.close();
        });
    server.start();
  }

  @AfterEach
  void stop() {
    server.stop(0);
  }

  private String url() {
    return "http://127.0.0.1:" + server.getAddress().getPort();
  }

  // -- publisher -------------------------------------------------------------------------------------

  @Test
  void thePublishCarriesTheBearerAndNoForwardedPair() {
    EventsPublisher publisher = publisher(FIXED);

    assertTrue(publisher.put(UUID.randomUUID().toString(), envelope()).delivered());

    assertEquals(1, requests.size());
    Headers sent = requests.get(0);
    assertEquals("Bearer fixed-token", sent.getFirst("Authorization"));
    assertNull(sent.getFirst("X-Qits-User"));
    assertNull(sent.getFirst("X-Qits-Roles"));
  }

  @Test
  void aPublishWithNoBearerSendsNothingAndIsRetriedNotSpent() {
    EventsPublisher publisher = publisher(NONE);

    EventsPublisher.Delivery delivery = publisher.put(UUID.randomUUID().toString(), envelope());

    assertTrue(requests.isEmpty(), "fail closed: nothing is sent");
    assertTrue(delivery.unreachable(), "a missing client must not spend the refusal budget");
    assertTrue(delivery.detail().contains("quarkus.oidc-client.qits"), delivery.detail());
  }

  @Test
  void aHandBuiltPublisherHasNoBearerAndSendsNothing() {
    EventsPublisher publisher = new EventsPublisher();
    publisher.eventsUrl = url();
    publisher.publishTimeout = Duration.ofSeconds(5);

    EventsPublisher.Delivery delivery = publisher.put(UUID.randomUUID().toString(), envelope());

    assertTrue(requests.isEmpty());
    assertTrue(delivery.unreachable());
    assertTrue(delivery.detail().contains("quarkus.oidc-client.qits"), delivery.detail());
  }

  // -- query -----------------------------------------------------------------------------------------

  @Test
  void theReadCarriesTheBearerAndNoForwardedPair() {
    EventsQuery query = query(FIXED);

    assertNull(query.newest(List.of("ThingHappened")));

    assertEquals(1, requests.size());
    Headers sent = requests.get(0);
    assertEquals("Bearer fixed-token", sent.getFirst("Authorization"));
    assertNull(sent.getFirst("X-Qits-User"));
    assertNull(sent.getFirst("X-Qits-Roles"));
  }

  @Test
  void aReadWithNoBearerSendsNothingAndIsUnavailableNotEmpty() {
    EventsQuery query = query(NONE);

    EventsQuery.Unavailable unavailable =
        assertThrows(EventsQuery.Unavailable.class, () -> query.after(List.of("*"), null, 10));

    assertTrue(requests.isEmpty(), "fail closed: nothing is sent");
    assertTrue(unavailable.getMessage().contains("quarkus.oidc-client.qits"), unavailable.getMessage());
  }

  // -- subscriber ------------------------------------------------------------------------------------

  /**
   * The long-running caller: with no bearer it dials nothing, keeps asking on its backoff rather than
   * dying, and is not connected. The far end never sees an upgrade.
   */
  @Test
  void aSubscriberWithNoBearerDialsNothingAndKeepsRetrying() throws Exception {
    AtomicInteger asks = new AtomicInteger();
    Vertx vertx = Vertx.vertx();
    EventStreamSubscriber subscriber = new EventStreamSubscriber();
    subscriber.vertx = vertx;
    subscriber.bearer =
        () -> {
          asks.incrementAndGet();
          return NONE.authorization();
        };
    subscriber.eventsUrl = url();
    subscriber.redialInitialBackoff = Duration.ofMillis(10);
    subscriber.redialMaxBackoff = Duration.ofMillis(40);
    subscriber.pingInterval = Duration.ofSeconds(30);
    subscriber.silenceLimit = Duration.ofSeconds(90);
    try {
      subscriber.start();
      long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
      while (asks.get() < 5 && System.nanoTime() < deadline) {
        Thread.sleep(20);
      }
      assertTrue(asks.get() >= 5, "the subscriber keeps asking for a bearer, asked " + asks.get());
      assertFalse(subscriber.connected());
      assertTrue(requests.isEmpty(), "fail closed: no upgrade was attempted");
    } finally {
      subscriber.stop();
      vertx.close().toCompletionStage().toCompletableFuture().join();
    }
  }

  // -- helpers ---------------------------------------------------------------------------------------

  private EventsPublisher publisher(EventStreamBearer bearer) {
    EventsPublisher publisher = new EventsPublisher();
    publisher.bearer = bearer;
    publisher.eventsUrl = url();
    publisher.publishTimeout = Duration.ofSeconds(5);
    return publisher;
  }

  private EventsQuery query(EventStreamBearer bearer) {
    EventsQuery query = new EventsQuery();
    query.bearer = bearer;
    query.eventsUrl = url();
    return query;
  }

  private static EventEnvelope envelope() {
    return EventEnvelope.of(
        new TestEvents.ThingHappened("shipped", 1, Instant.parse("2026-11-03T12:00:00Z")),
        null,
        "platform");
  }
}
