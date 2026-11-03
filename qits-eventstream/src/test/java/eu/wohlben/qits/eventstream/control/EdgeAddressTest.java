package eu.wohlben.qits.eventstream.control;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpServer;
import eu.wohlben.qits.eventstream.EventStreamBearer;
import io.smallrye.config.PropertiesConfigSource;
import io.smallrye.config.SmallRyeConfig;
import io.smallrye.config.SmallRyeConfigBuilder;
import io.smallrye.mutiny.Uni;
import io.vertx.core.http.UpgradeRejectedException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * qits-1417: {@code qits.events.url} names qits-events' public host, through the edge — what the
 * shipped default resolves to, what the stream dials for it, and what a refused credential sounds
 * like. No Quarkus: the config is the jar's own file read by SmallRye directly, and the clients are
 * built by hand against a JDK server that answers 401, the way the edge answers a bad bearer.
 */
class EdgeAddressTest {

  private static final EventStreamBearer FIXED = () -> Uni.createFrom().item("Bearer fixed-token");

  private HttpServer server;

  @BeforeEach
  void start() throws IOException {
    CredentialRefusal.resetForTest();
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/",
        exchange -> {
          exchange.getRequestBody().readAllBytes();
          exchange.sendResponseHeaders(401, -1);
          exchange.close();
        });
    server.start();
  }

  @AfterEach
  void stop() {
    server.stop(0);
    CredentialRefusal.resetForTest();
  }

  // -- the shipped default ---------------------------------------------------------------------------

  @Test
  void theDefaultIsThePublicNameOnTheInjectedDomain() {
    SmallRyeConfig config = config("prod", Map.of("QITS_DOMAIN", "example.test"));

    assertEquals("https://events.qits.example.test", config.getValue("qits.events.url", String.class));
  }

  @Test
  void withoutADomainProductionHasNoAddressAndSaysSo() {
    SmallRyeConfig config = config("prod", Map.of());

    // No fallback: an unresolvable expression fails the lookup, so a @ConfigProperty fails the boot.
    NoSuchElementException missing =
        assertThrows(
            NoSuchElementException.class, () -> config.getValue("qits.events.url", String.class));
    assertTrue(missing.getMessage().contains("QITS_DOMAIN"), missing.getMessage());
  }

  @Test
  void devAndTestResolveWithoutADomain() {
    for (String profile : List.of("dev", "test")) {
      SmallRyeConfig config = config(profile, Map.of());
      assertEquals(
          "http://dev-qits-events:8080",
          config.getValue("qits.events.url", String.class),
          "%" + profile + " must not need QITS_DOMAIN");
    }
  }

  // -- the stream's address --------------------------------------------------------------------------

  @Test
  void thePublicNameIsDialledAsWssOn443() {
    assertEquals(URI.create("wss://events.qits.example.test:443"), streamBase("https://events.qits.example.test"));
    assertEquals(URI.create("wss://events.qits.example.test:443"), streamBase("https://events.qits.example.test/"));
  }

  @Test
  void anExplicitPortIsKeptAndAPlainHostGets80() {
    assertEquals(URI.create("ws://127.0.0.1:41234"), streamBase("http://127.0.0.1:41234"));
    assertEquals(URI.create("ws://dev-qits-events:8080"), streamBase("http://dev-qits-events:8080"));
    assertEquals(URI.create("ws://localhost:80"), streamBase("http://localhost"));
  }

  @Test
  void aSchemeTheStreamCannotSpeakFailsLoudly() {
    assertThrows(IllegalStateException.class, () -> streamBase("ftp://events.qits.example.test"));
  }

  // -- a refused credential --------------------------------------------------------------------------

  @Test
  void aRejectedUpgradeIsFoundAnywhereInTheCauseChain() {
    UpgradeRejectedException rejected = new UpgradeRejectedException("no", 401, null, null);
    assertEquals(401, CredentialRefusal.rejectedUpgradeStatus(rejected));
    assertEquals(
        403,
        CredentialRefusal.rejectedUpgradeStatus(
            new CompletionException(
                new IllegalStateException(new UpgradeRejectedException("no", 403, null, null)))));
    assertEquals(-1, CredentialRefusal.rejectedUpgradeStatus(new java.net.ConnectException("refused")));
    assertEquals(-1, CredentialRefusal.rejectedUpgradeStatus(null));
  }

  @Test
  void onlyTheCredentialStatusesCount() {
    assertTrue(CredentialRefusal.isCredentialStatus(401));
    assertTrue(CredentialRefusal.isCredentialStatus(403));
    assertFalse(CredentialRefusal.isCredentialStatus(404));
    assertFalse(CredentialRefusal.isCredentialStatus(503));
    assertFalse(CredentialRefusal.isCredentialStatus(-1));
  }

  @Test
  void aPublishRefusedWith401IsSaidAtWarnAndStaysARefusal() {
    EventsPublisher publisher = new EventsPublisher();
    publisher.bearer = FIXED;
    publisher.eventsUrl = url();
    publisher.publishTimeout = Duration.ofSeconds(5);
    int before = CredentialRefusal.WARNINGS.get();

    EventsPublisher.Delivery delivery = publisher.put(UUID.randomUUID().toString(), envelope());

    assertTrue(delivery.refused(), "the outbox's bookkeeping is unchanged: " + delivery);
    assertEquals(before + 1, CredentialRefusal.WARNINGS.get(), "a refused bearer must be a WARN");
  }

  @Test
  void aReadRefusedWith401IsSaidAtWarnAndIsUnavailable() {
    EventsQuery query = new EventsQuery();
    query.bearer = FIXED;
    query.eventsUrl = url();
    int before = CredentialRefusal.WARNINGS.get();

    assertThrows(EventsQuery.Unavailable.class, () -> query.after(List.of("*"), null, 10));

    assertEquals(before + 1, CredentialRefusal.WARNINGS.get(), "a refused bearer must be a WARN");
  }

  @Test
  void theWarningIsThrottledAcrossCallers() {
    int before = CredentialRefusal.WARNINGS.get();

    assertTrue(CredentialRefusal.report("the event stream upgrade", "wss://x:443/events/stream", 401));
    assertFalse(CredentialRefusal.report("PUT of event e", "https://x/events/api/events/e", 401));
    assertFalse(CredentialRefusal.report("a read of the event log", "https://x/events/api/events", 403));

    assertEquals(before + 1, CredentialRefusal.WARNINGS.get(), "once per minute, not once per call");
  }

  // -- helpers ---------------------------------------------------------------------------------------

  private String url() {
    return "http://127.0.0.1:" + server.getAddress().getPort();
  }

  private static URI streamBase(String eventsUrl) {
    EventStreamSubscriber subscriber = new EventStreamSubscriber();
    subscriber.eventsUrl = eventsUrl;
    return subscriber.streamBaseUri();
  }

  /**
   * This jar's own {@code META-INF/microprofile-config.properties} — the one in this module's
   * classes, not a dependency's — at its real ordinal, under {@code profile}, with {@code env} as the
   * only other source. No default sources: the host running the suite may well carry a QITS_DOMAIN.
   */
  private static SmallRyeConfig config(String profile, Map<String, String> env) {
    URL shipped;
    try {
      shipped =
          Collections.list(
                  EdgeAddressTest.class
                      .getClassLoader()
                      .getResources("META-INF/microprofile-config.properties"))
              .stream()
              .filter(url -> url.toString().contains("/qits-eventstream/target/classes/"))
              .findFirst()
              .orElseThrow(() -> new IllegalStateException("this module's config file is not on the classpath"));
      return new SmallRyeConfigBuilder()
          .withProfile(profile)
          .addDefaultInterceptors()
          .withSources(new PropertiesConfigSource(shipped, 100))
          .withSources(new PropertiesConfigSource(env, "env", 300))
          .build();
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  private static EventEnvelope envelope() {
    return EventEnvelope.of(
        new TestEvents.ThingHappened("refused", 1, Instant.parse("2026-11-03T12:00:00Z")),
        null,
        "platform");
  }
}
