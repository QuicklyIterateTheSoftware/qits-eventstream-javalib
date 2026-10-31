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
          BODIES.add(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
          exchange.sendResponseHeaders(201, -1);
          exchange.close();
        });
    server.start();
    return Map.of("qits.events.url", "http://127.0.0.1:" + server.getAddress().getPort());
  }

  @Override
  public void stop() {
    if (server != null) {
      server.stop(0);
    }
  }
}
