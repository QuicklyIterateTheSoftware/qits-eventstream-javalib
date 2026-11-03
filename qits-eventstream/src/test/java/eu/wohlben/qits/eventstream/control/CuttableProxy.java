package eu.wohlben.qits.eventstream.control;

import io.vertx.core.Vertx;
import io.vertx.core.net.NetClient;
import io.vertx.core.net.NetClientOptions;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetServerOptions;
import io.vertx.core.net.NetSocket;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A TCP proxy standing in for the edge between this library and qits-events, with the three things
 * an edge switchover does to a client: it <b>cuts</b> every connection at once, it <b>refuses</b>
 * new ones for a while, and then it <b>accepts</b> again on the same address.
 *
 * <p>Plain TCP, not HTTP: it forwards bytes and knows nothing about the PUT, the list route or the
 * websocket upgrade, so whatever the client does on the far side of a cut — redial, resubscribe,
 * resend — is the client's own behaviour and not something this file helped with. Every socket is
 * opened with {@code SO_LINGER 0}, so a cut is a <b>reset</b> on both legs, as when a mesh drops a
 * flow, rather than a polite FIN.
 *
 * <p>Loopback only, both legs. {@link #refuse()} closes the listening socket; {@link #accept()}
 * binds the same port again, which is what "the same address answers again" means to a client.
 */
final class CuttableProxy implements AutoCloseable {

  private final Vertx vertx = Vertx.vertx();
  private final int targetPort;
  private final NetClient upstream;
  private final Set<NetSocket> live = ConcurrentHashMap.newKeySet();
  private final AtomicInteger accepted = new AtomicInteger();
  private final AtomicInteger swallowNext = new AtomicInteger();
  private volatile NetServer server;
  private volatile int port;

  private CuttableProxy(int targetPort) {
    this.targetPort = targetPort;
    this.upstream = vertx.createNetClient(new NetClientOptions().setSoLinger(0));
  }

  /** A proxy on an ephemeral loopback port, forwarding to {@code 127.0.0.1:targetPort}. */
  static CuttableProxy forwardingTo(int targetPort) {
    CuttableProxy proxy = new CuttableProxy(targetPort);
    proxy.listen(0);
    return proxy;
  }

  int port() {
    return port;
  }

  /** How many client connections this proxy has accepted, ever. */
  int accepted() {
    return accepted.get();
  }

  /** Reset every open connection, both legs. The listening socket stays up. */
  void cut() {
    for (NetSocket socket : List.copyOf(live)) {
      socket.close();
    }
  }

  /** Cut everything and stop listening: a dial now gets connection refused. */
  void refuse() {
    NetServer current = server;
    server = null;
    if (current != null) {
      join(current.close());
    }
    cut();
  }

  /** Listen again on the same port. */
  void accept() {
    if (server == null) {
      listen(port);
    }
  }

  /**
   * Accept the next {@code n} connections, read whatever the client sends, and reset them without
   * forwarding a byte — a request that went out and whose connection died before any response.
   */
  void swallowNext(int n) {
    swallowNext.set(n);
  }

  @Override
  public void close() {
    refuse();
    join(vertx.close());
  }

  private void listen(int onPort) {
    NetServer created =
        vertx.createNetServer(
            new NetServerOptions().setHost("127.0.0.1").setPort(onPort).setSoLinger(0));
    created.connectHandler(this::proxy);
    join(created.listen());
    port = created.actualPort();
    server = created;
  }

  private void proxy(NetSocket inbound) {
    accepted.incrementAndGet();
    live.add(inbound);
    if (swallowNext.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
      inbound.handler(ignored -> inbound.close());
      inbound.closeHandler(v -> live.remove(inbound));
      return;
    }
    inbound.pause();
    upstream
        .connect(targetPort, "127.0.0.1")
        .onComplete(
            connected -> {
              if (connected.failed()) {
                live.remove(inbound);
                inbound.close();
                return;
              }
              NetSocket up = connected.result();
              live.add(up);
              inbound.handler(up::write);
              up.handler(inbound::write);
              inbound.closeHandler(
                  v -> {
                    live.remove(inbound);
                    up.close();
                  });
              up.closeHandler(
                  v -> {
                    live.remove(up);
                    inbound.close();
                  });
              inbound.resume();
            });
  }

  private static <T> T join(io.vertx.core.Future<T> future) {
    return future.toCompletionStage().toCompletableFuture().join();
  }
}
