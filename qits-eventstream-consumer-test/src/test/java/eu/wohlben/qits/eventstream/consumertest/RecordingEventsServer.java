package eu.wohlben.qits.eventstream.consumertest;

import com.sun.net.httpserver.HttpServer;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * The smallest qits-events that answers a publish: every {@code PUT} under {@code
 * /events/api/events/} gets a 201 and its body is kept verbatim. The library's own {@code
 * StubEventsServer} does far more and lives in another module's test sources, which a consumer
 * cannot see.
 */
public class RecordingEventsServer implements QuarkusTestResourceLifecycleManager {

  static final List<String> BODIES = new CopyOnWriteArrayList<>();

  /** Each PUT's Authorization header, in arrival order ("null" when none was sent). */
  static final List<String> AUTHORIZATIONS = new CopyOnWriteArrayList<>();

  private HttpServer server;

  @Override
  public Map<String, String> start() {
    try {
      server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    server.createContext(
        "/events/api/events/",
        exchange -> {
          AUTHORIZATIONS.add(String.valueOf(exchange.getRequestHeaders().getFirst("Authorization")));
          BODIES.add(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
          exchange.sendResponseHeaders(201, -1);
          exchange.close();
        });
    // The consumer's own idp (qits-1328): the library publishes with the consuming service's
    // `quarkus.oidc-client.qits` token and sends nothing without one.
    server.createContext(
        "/idp/token",
        exchange -> {
          exchange.getRequestBody().readAllBytes();
          byte[] token =
              "{\"access_token\":\"consumer-token\",\"token_type\":\"Bearer\",\"expires_in\":3600}"
                  .getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().add("Content-Type", "application/json");
          exchange.sendResponseHeaders(200, token.length);
          exchange.getResponseBody().write(token);
          exchange.close();
        });
    server.start();
    String base = "http://127.0.0.1:" + server.getAddress().getPort();
    return Map.of("qits.events.url", base, "qits.test.idp.url", base + "/idp");
  }

  @Override
  public void stop() {
    if (server != null) {
      server.stop(0);
    }
  }
}
