package eu.wohlben.qits.eventstream.control;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.ServerWebSocket;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * A qits-events that never leaves this JVM: a real Vert.x server answering the real {@code PUT
 * /events/api/events/{id}} and accepting a real upgrade on {@code /events/stream}, on an ephemeral
 * port handed to Quarkus as {@code qits.events.url} before it boots.
 *
 * <p>The port is why this is a {@code QuarkusTestResourceLifecycleManager} rather than a fixture a
 * test starts: the value has to exist before the application's config is read, and it cannot be
 * known before something has bound. Same arrangement qits-gateway's {@code StubUpstream} has, for
 * the same reason.
 *
 * <p><b>Deliberately dumb about writes.</b> It holds no idempotency table and decides nothing: a
 * test scripts the status codes it wants answered and asserts on what arrived. That is what makes
 * the uninteresting cases testable at all — a 400 on the third attempt, a 200 replay after four
 * failures — and it keeps this file from becoming a second implementation of qits-events whose
 * agreement with the first nobody checks. The real semantics are Agent A's and are tested there.
 *
 * <p><b>The read side is the one exception, and it has to be.</b> {@link CatchupSweeper} is a paging
 * loop, and a paging loop can only be tested against something that pages: the thing under test is
 * whether it walks a cursor forward without dropping or repeating a row, which a canned answer
 * cannot ask. So {@link #seed} fills a log and {@code GET /events/api/events} serves it with the
 * real route's contract — {@code order=asc}, the composite {@code <occurredAt>,<id>} cursor, the
 * comma-separated {@code name} filter, and {@code nextCursor} null on the last page <em>even when
 * that page came back full</em>, which is the one thing a client must not infer for itself.
 *
 * <p>State is static because the resource is one per JVM and the tests are the only readers. {@link
 * #reset()} between tests is what keeps them independent.
 */
public class StubEventsServer implements QuarkusTestResourceLifecycleManager {

  /** One request that arrived: the id from the path, and the body verbatim. */
  public record Put(String id, String body) {}

  /** One row of the stub's log, as {@code GET /events/api/events} hands it back. */
  public record Logged(String id, String name, Instant occurredAt, String payload) {}

  private static final List<Put> PUTS = Collections.synchronizedList(new ArrayList<>());
  private static final Deque<Integer> SCRIPTED = new ArrayDeque<>();
  private static final List<ServerWebSocket> SOCKETS = Collections.synchronizedList(new ArrayList<>());
  private static final LinkedBlockingQueue<String> SUBSCRIBE_FRAMES = new LinkedBlockingQueue<>();
  private static final List<Logged> LOG_ROWS = Collections.synchronizedList(new ArrayList<>());
  private static final List<String> QUERIES = Collections.synchronizedList(new ArrayList<>());

  private static volatile int defaultStatus = 201;
  private static Vertx vertx;
  private static HttpServer server;
  private static int port;

  // -- what a test drives ------------------------------------------------------------------------

  /** Forget everything and answer 201 again. */
  public static void reset() {
    PUTS.clear();
    SUBSCRIBE_FRAMES.clear();
    LOG_ROWS.clear();
    QUERIES.clear();
    synchronized (SCRIPTED) {
      SCRIPTED.clear();
    }
    defaultStatus = 201;
  }

  /** Put an event in the log this stub serves. Order of seeding does not matter; the route sorts. */
  public static void seed(String id, String name, Instant occurredAt, String payload) {
    LOG_ROWS.add(new Logged(id, name, occurredAt, payload));
  }

  /** Every list query that arrived, as its raw query string — how the wire contract is asserted. */
  public static List<String> queries() {
    synchronized (QUERIES) {
      return List.copyOf(QUERIES);
    }
  }

  /** Answer every PUT with this status. */
  public static void answerWith(int status) {
    synchronized (SCRIPTED) {
      SCRIPTED.clear();
    }
    defaultStatus = status;
  }

  /**
   * Answer the next PUTs with these statuses in order; once they run out, the last one repeats.
   * How "four failures then a replay" is staged without touching the clock.
   */
  public static void answerWith(int first, int... rest) {
    synchronized (SCRIPTED) {
      SCRIPTED.clear();
      SCRIPTED.add(first);
      for (int status : rest) {
        SCRIPTED.add(status);
      }
      defaultStatus = rest.length == 0 ? first : rest[rest.length - 1];
    }
  }

  /** Every PUT that arrived, in order. */
  public static List<Put> puts() {
    synchronized (PUTS) {
      return List.copyOf(PUTS);
    }
  }

  /** The next subscribe frame a client sent, or null if none arrives in time. */
  public static String awaitSubscribeFrame(Duration within) throws InterruptedException {
    return SUBSCRIBE_FRAMES.poll(within.toMillis(), TimeUnit.MILLISECONDS);
  }

  /** Push one frame to every connected subscriber, exactly as a broadcast would. */
  public static void broadcast(String frame) {
    for (ServerWebSocket socket : openSockets()) {
      socket.writeTextMessage(frame);
    }
  }

  /** Drop every stream connection — qits-events restarting, from the client's point of view. */
  public static void dropStreams() {
    for (ServerWebSocket socket : openSockets()) {
      socket.close();
    }
  }

  /**
   * Stop reading every open stream, so pings go unanswered and nothing more arrives — a flow the
   * swarm VIP forgot, from the client's point of view. The sockets stay open on this side.
   */
  public static void silenceStreams() {
    for (ServerWebSocket socket : openSockets()) {
      socket.pause();
    }
  }

  /** How many stream connections are open right now. */
  public static int openStreams() {
    return openSockets().size();
  }

  private static List<ServerWebSocket> openSockets() {
    synchronized (SOCKETS) {
      SOCKETS.removeIf(ServerWebSocket::isClosed);
      return List.copyOf(SOCKETS);
    }
  }

  // -- lifecycle ---------------------------------------------------------------------------------

  @Override
  public Map<String, String> start() {
    vertx = Vertx.vertx();
    server =
        vertx
            .createHttpServer()
            // A Vert.x server carrying only a webSocketHandler NPEs on any plain request, and this
            // one has to answer both — the PUT and the upgrade are the same service.
            .requestHandler(
                request -> {
                  String path = request.path();
                  if (request.method().name().equals("GET")
                      && path.equals(EventsQuery.EVENTS_PATH)) {
                    QUERIES.add(request.query() == null ? "" : request.query());
                    request
                        .response()
                        .putHeader("Content-Type", "application/json")
                        .end(page(request.params()));
                    return;
                  }
                  if (request.method().name().equals("PUT")
                      && path.startsWith(EventsPublisher.EVENTS_PATH)) {
                    String id = path.substring(EventsPublisher.EVENTS_PATH.length());
                    request
                        .body()
                        .onSuccess(
                            body -> {
                              PUTS.add(new Put(id, body.toString()));
                              request.response().setStatusCode(nextStatus()).end();
                            });
                    return;
                  }
                  request.response().setStatusCode(404).end();
                })
            .webSocketHandler(
                socket -> {
                  if (!EventStreamSubscriber.STREAM_PATH.equals(socket.path())) {
                    socket.reject();
                    return;
                  }
                  SOCKETS.add(socket);
                  socket.textMessageHandler(SUBSCRIBE_FRAMES::offer);
                });
    server.listen(0, "127.0.0.1").toCompletionStage().toCompletableFuture().join();
    port = server.actualPort();
    return Map.of("qits.events.url", "http://127.0.0.1:" + port);
  }

  @Override
  public void stop() {
    if (server != null) {
      server.close().toCompletionStage().toCompletableFuture().join();
    }
    if (vertx != null) {
      vertx.close().toCompletionStage().toCompletableFuture().join();
    }
  }

  private static int nextStatus() {
    synchronized (SCRIPTED) {
      Integer scripted = SCRIPTED.poll();
      return scripted == null ? defaultStatus : scripted;
    }
  }

  // -- the list route ------------------------------------------------------------------------------

  /**
   * One page of the seeded log, following the real route's contract.
   *
   * <p>The three parts that are worth having a faithful copy of: the sort and the cursor comparison
   * flip together with {@code order}, the cursor is composite so a tie splits safely across a page
   * boundary, and {@code nextCursor} is null once there is no more history — asked by fetching one
   * row more than the page holds, exactly as the service does.
   */
  private static String page(MultiMap params) {
    boolean ascending = "asc".equalsIgnoreCase(params.get("order"));
    int limit = params.get("limit") == null ? 200 : Integer.parseInt(params.get("limit"));
    List<String> names = namesOf(params.get("name"));
    Instant cursorAt = null;
    String cursorId = null;
    String cursor = params.get("cursor");
    if (cursor != null && !cursor.isBlank()) {
      int comma = cursor.indexOf(',');
      cursorAt = Instant.parse(cursor.substring(0, comma));
      cursorId = cursor.substring(comma + 1);
    }

    List<Logged> rows;
    synchronized (LOG_ROWS) {
      rows = new ArrayList<>(LOG_ROWS);
    }
    Comparator<Logged> order = Comparator.comparing(Logged::occurredAt).thenComparing(Logged::id);
    rows.sort(ascending ? order : order.reversed());

    List<Map<String, Object>> page = new ArrayList<>();
    Logged last = null;
    boolean more = false;
    for (Logged row : rows) {
      if (!names.isEmpty() && !names.contains(row.name())) {
        continue;
      }
      if (cursorAt != null && !beyond(row, cursorAt, cursorId, ascending)) {
        continue;
      }
      if (page.size() == limit) {
        more = true;
        break;
      }
      page.add(json(row));
      last = row;
    }

    Map<String, Object> body = new LinkedHashMap<>();
    body.put("events", page);
    // Null on the last page even when that page is full — the property a client must not infer.
    body.put("nextCursor", more && last != null ? last.occurredAt() + "," + last.id() : null);
    return CanonicalJson.canonicalize(body);
  }

  /** The composite comparison, flipped with the sort. Both halves are needed: occurredAt ties. */
  private static boolean beyond(Logged row, Instant at, String id, boolean ascending) {
    int byTime = row.occurredAt().compareTo(at);
    int byId = row.id().compareTo(id);
    return ascending ? byTime > 0 || (byTime == 0 && byId > 0) : byTime < 0 || (byTime == 0 && byId < 0);
  }

  private static List<String> namesOf(String name) {
    if (name == null || name.isBlank()) {
      return List.of();
    }
    return Arrays.stream(name.split(",")).map(String::trim).filter(each -> !each.isBlank()).toList();
  }

  /**
   * A row as the API returns it — with the {@code createdAt} and {@code updatedAt} the log's own DTO
   * carries and the frame has no field for, so this suite also proves the binding tolerates them.
   */
  private static Map<String, Object> json(Logged row) {
    Map<String, Object> event = new LinkedHashMap<>();
    event.put("id", row.id());
    event.put("name", row.name());
    event.put("occurredAt", row.occurredAt());
    event.put("payload", row.payload());
    event.put("createdAt", row.occurredAt());
    event.put("updatedAt", row.occurredAt());
    return event;
  }
}
