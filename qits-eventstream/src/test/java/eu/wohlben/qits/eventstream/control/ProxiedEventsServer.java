package eu.wohlben.qits.eventstream.control;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import java.net.URI;
import java.util.Map;

/**
 * {@link StubEventsServer} behind a {@link CuttableProxy}: the application's {@code qits.events.url}
 * names the proxy, so the stream, the catch-up reads and the publishes all cross it — the shape of a
 * service reaching qits-events through the edge. The stub's own static controls (seed, broadcast,
 * puts) work unchanged; {@link #proxy()} is the edge.
 */
public class ProxiedEventsServer implements QuarkusTestResourceLifecycleManager {

  private static volatile CuttableProxy proxy;
  private final StubEventsServer stub = new StubEventsServer();

  static CuttableProxy proxy() {
    return proxy;
  }

  /** {@code qits.events.url} as the application sees it: the proxy's address. */
  static String url() {
    return "http://127.0.0.1:" + proxy.port();
  }

  @Override
  public Map<String, String> start() {
    Map<String, String> started = stub.start();
    URI direct = URI.create(started.get("qits.events.url"));
    proxy = CuttableProxy.forwardingTo(direct.getPort());
    // The token endpoint stays direct (qits-1328): the cut under test is qits-events', not the idp's.
    return Map.of("qits.events.url", url(), "qits.test.idp.url", started.get("qits.test.idp.url"));
  }

  @Override
  public void stop() {
    if (proxy != null) {
      proxy.close();
    }
    stub.stop();
  }
}
