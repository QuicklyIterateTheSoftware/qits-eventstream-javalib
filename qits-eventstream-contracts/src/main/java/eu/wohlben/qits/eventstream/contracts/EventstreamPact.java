package eu.wohlben.qits.eventstream.contracts;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.com.dius.pact.consumer.ConsumerPactRunnerKt;
import au.com.dius.pact.consumer.PactVerificationResult;
import au.com.dius.pact.consumer.dsl.PactBuilder;
import au.com.dius.pact.consumer.model.MockProviderConfig;
import au.com.dius.pact.core.model.PactSpecVersion;
import au.com.dius.pact.core.model.V4Pact;
import eu.wohlben.qits.eventstream.QitsDurableEventListener;
import eu.wohlben.qits.eventstream.QitsEventListener;
import eu.wohlben.qits.eventstream.QitsRawEventListener;
import eu.wohlben.qits.eventstream.contracts.EventsGoldenMasters.Trigger;
import eu.wohlben.qits.eventstream.control.ContractProbe;
import eu.wohlben.qits.eventstream.control.EventEnvelope;
import eu.wohlben.qits.eventstream.control.EventFrame;
import eu.wohlben.qits.eventstream.control.EventsPublisher;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DynamicTest;

/**
 * <b>What qits-eventstream asks qits-events on one service's behalf</b> (qits-1163) — the pact a
 * consuming service commits as {@code pacts/<service>_qits-eventstream-javalib_qits-events-service
 * .json}.
 *
 * <p>A call a library makes for a service is pacted by THAT service, as consumer, against the
 * library version the service pins. The library writes no pact of its own any more; it ships this
 * kit instead, and the rows are derived from the service's real inputs, never copied whole:
 *
 * <ul>
 *   <li>{@link Feature#CATCH_UP_INITIALIZE} — per durable listener that does not {@code
 *       replayFromEpoch()}: {@code CatchupSweeper.initialize(<consumerId>)} reads {@code GET
 *       /events/api/events?limit=1[&name=..]} under {@code no events}.
 *   <li>{@link Feature#CATCH_UP_PAGE} — per durable listener: {@code
 *       CatchupSweeper.catchUp(<consumerId>)} reads {@code ?order=asc&limit=200[&name=..]}, under
 *       {@code a few recent events} when it takes every event ({@code QitsRawEventListener.ALL}),
 *       else under {@code no events}.
 *   <li>{@link Feature#PUBLISH} — when the service publishes through {@code QitsEventBus}: {@code
 *       PUT /events/api/events/{id}} from {@code OutboxSweeper.tick} (and the identical inline
 *       attempt), a new id delivered under {@code no event with the given id}, a reused id rejected
 *       (400) under {@code an event with the given id}.
 * </ul>
 *
 * <p>A raw or typed listener is live-only and rides the websocket, which is not REST: it adds no
 * row. A durable listener with no signatures adds none either, as the sweeper reads nothing for it.
 * Every row runs the library's real {@code EventsQuery} / {@code EventsPublisher} (through {@link
 * ContractProbe}) against a pact mock server. Every interaction carries {@code qits-call}, {@code
 * qits-trigger} (app: the service) and {@code qits-via} ({@link EventstreamVia}).
 *
 * <pre>{@code
 * class EventsPactTest {
 *   static final EventstreamPact PACT = EventstreamPact.forConsumer("qits-ci-service")
 *       .listeners(new CiEventTriggerListener(), new ScmReleaseListener())
 *       .publishes(true);                    // guard(...) defaults to target/classes
 *
 *   @TestFactory Stream<DynamicTest> rows() { return PACT.tests(); }
 *
 *   @Test void theCommittedPactIsWhatTheRowsWrite() { PACT.compareOrWritePactFile(); }
 * }
 * }</pre>
 *
 * <p><b>The guard</b> ({@link #guard}) fails {@link #compareOrWritePactFile} naming any durable or
 * raw listener class in the service's compiled code that was not passed, and any class using {@code
 * QitsEventBus} while {@code publishes} is false. <b>Names are sorted</b> in the {@code name}
 * parameter, so the committed file does not depend on a {@code Set}'s iteration order; qits-events
 * reads the filter as a set.
 *
 * <p>Immutable: every setter returns a new pact.
 */
public final class EventstreamPact {

  static {
    // pact-jvm reports usage metrics over the network unless told not to; a test never should.
    System.setProperty("pact_do_not_track", "true");
  }

  /** The three things the library does over REST; which of them a service's pact holds. */
  public enum Feature {
    PUBLISH,
    CATCH_UP_INITIALIZE,
    CATCH_UP_PAGE
  }

  /** What a row does with the library's client, asserting what that code path makes of it. */
  @FunctionalInterface
  public interface Call {
    void run(ContractProbe client, Map<String, String> params);
  }

  /**
   * One interaction: (trigger, query shape, state), and what the library makes of the answer.
   *
   * @param body the request body, or null
   * @param reads the response paths the call binds — the only ones the pact carries
   */
  public record Case(
      Feature feature,
      Trigger trigger,
      String variant,
      String state,
      String operationId,
      Map<String, String> query,
      String body,
      List<String> reads,
      Call call) {

    public String description() {
      return EventsGoldenMasters.description(operationId, trigger, variant);
    }
  }

  // Provider states, all recorded in qits-events-golden-masters 2026.1010.121628.
  public static final String NO_EVENTS = "no events";
  public static final String A_FEW_RECENT_EVENTS = "a few recent events";
  public static final String NO_EVENT_WITH_THE_ID = "no event with the given id";
  public static final String AN_EVENT_WITH_THE_ID = "an event with the given id";

  public static final String LIST_EVENTS = "listEvents";
  public static final String PUBLISH_EVENT = "publishEvent";

  /** The provider, as the pact names it. */
  public static final String PROVIDER = EventsGoldenMasters.PROVIDER;

  /**
   * Every frame field the library binds ({@code EventFrame}) but {@code environment}, which the
   * state {@value #A_FEW_RECENT_EVENTS} records as null. {@code publisher} (qits-1770) is bound by
   * type: qits-events' provider states stamp a non-null one on every seeded event.
   */
  static final List<String> FRAME =
      List.of(
          "events[*].id",
          "events[*].name",
          "events[*].occurredAt",
          "events[*].payload",
          "events[*].description",
          "events[*].parentId",
          "events[*].publisher");

  /** An empty page: the whole (empty) list is what the library reads. */
  static final List<String> PAGE = List.of("events");

  /**
   * The envelope every publish row sends. Its content must differ from whatever the state {@value
   * #AN_EVENT_WITH_THE_ID} stores at the id, which is what makes the 400. It carries a non-null
   * {@code publisher} (qits-1770), because {@code QitsEventBus} always stamps one in a real
   * consumer — the build-time default is the application's own {@code groupId:artifactId} — so the
   * pact sends what a service sends. The value is the kit's, not the consumer's: one fixed body
   * keeps every service's pact row identical and its provider verification independent of names.
   */
  static final EventEnvelope ENVELOPE =
      new EventEnvelope(
          "DeploymentActive",
          Instant.parse("2026-01-01T00:00:00Z"),
          "{\"application\":\"qits-eventstream-pact\"}",
          null,
          null,
          null,
          "eu.wohlben.qits:qits-eventstream-pact");

  private final String consumer;
  private final List<Object> listeners;
  private final boolean publishes;
  private final List<Path> guard;

  private EventstreamPact(
      String consumer, List<Object> listeners, boolean publishes, List<Path> guard) {
    this.consumer = consumer;
    this.listeners = listeners;
    this.publishes = publishes;
    this.guard = guard;
  }

  /**
   * A pact for {@code consumer}, the service's repository name ({@code qits-ci-service}), with no
   * listeners, no publish, and the guard on {@code target/classes}.
   */
  public static EventstreamPact forConsumer(String consumer) {
    if (consumer == null || consumer.isBlank()) {
      throw new IllegalArgumentException("consumer: the service's repository name");
    }
    return new EventstreamPact(consumer, List.of(), false, List.of(Path.of("target", "classes")));
  }

  /**
   * Every listener the service has: {@link QitsDurableEventListener}s (which make rows), {@link
   * QitsRawEventListener}s and {@link QitsEventListener}s (which do not, but the guard wants the
   * first kind named). Instances, because the rows read {@code consumerId()}, {@code signatures()}
   * and {@code replayFromEpoch()} off them — a plain {@code new} of the bean is enough. Replaces any
   * listeners given before.
   */
  public EventstreamPact listeners(Object... listeners) {
    for (Object listener : listeners) {
      if (!(listener instanceof QitsDurableEventListener)
          && !(listener instanceof QitsRawEventListener)
          && !(listener instanceof QitsEventListener<?>)) {
        throw new IllegalArgumentException(
            (listener == null ? "null" : listener.getClass().getName())
                + " is not a qits-eventstream listener");
      }
    }
    return new EventstreamPact(consumer, List.of(listeners), publishes, guard);
  }

  /** Whether the service publishes through {@code QitsEventBus}: selects {@link Feature#PUBLISH}. */
  public EventstreamPact publishes(boolean publishes) {
    return new EventstreamPact(consumer, listeners, publishes, guard);
  }

  /**
   * The compiled main classes the guard reads — each module's {@code target/classes} that holds
   * listeners or publishers, relative to the test's working directory (the module surefire runs
   * in): {@code guard(Path.of("target/classes"), Path.of("../domain/target/classes"))}. Default:
   * {@code target/classes}.
   */
  public EventstreamPact guard(Path... classesDirs) {
    if (classesDirs.length == 0) {
      throw new IllegalArgumentException("guard(...) needs at least one classes directory");
    }
    return new EventstreamPact(consumer, listeners, publishes, List.of(classesDirs));
  }

  public String consumer() {
    return consumer;
  }

  /** The committed file's name: {@code <consumer>_qits-eventstream-javalib_qits-events-service.json}. */
  public String file() {
    return consumer + "_" + EventstreamVia.REPOSITORY + "_" + PROVIDER + ".json";
  }

  /** The {@code qits-via} every interaction carries: qits-eventstream at the version on the path. */
  public EventstreamVia via() {
    return EventstreamVia.current();
  }

  /** The features the inputs select, in {@link Feature} order. */
  public Set<Feature> features() {
    Set<Feature> features = EnumSet.noneOf(Feature.class);
    cases().forEach(c -> features.add(c.feature()));
    return Collections.unmodifiableSet(features);
  }

  /** Every row the inputs select: publish first, then per durable listener, in the order given. */
  public List<Case> cases() {
    List<Case> cases = new ArrayList<>();
    if (publishes) {
      Trigger outbox = Trigger.schedule(consumer, "OutboxSweeper.tick");
      String body = ContractProbe.body(ENVELOPE);
      cases.add(
          new Case(Feature.PUBLISH, outbox, "a new id", NO_EVENT_WITH_THE_ID, PUBLISH_EVENT,
              Map.of(), body, List.of(),
              (client, params) -> {
                EventsPublisher.Delivery delivery = client.put(params.get("eventId"), ENVELOPE);
                assertTrue(delivery.delivered(), "a new id is delivered: " + delivery);
              }));
      cases.add(
          new Case(Feature.PUBLISH, outbox, "a reused id", AN_EVENT_WITH_THE_ID, PUBLISH_EVENT,
              Map.of(), body, List.of(),
              (client, params) -> {
                EventsPublisher.Delivery delivery = client.put(params.get("eventId"), ENVELOPE);
                assertTrue(delivery.rejected(), "a reused id is rejected for good: " + delivery);
              }));
    }
    Set<String> consumerIds = new HashSet<>();
    for (Object candidate : listeners) {
      if (!(candidate instanceof QitsDurableEventListener listener)) {
        continue;
      }
      String consumerId = listener.consumerId();
      if (consumerId == null || consumerId.isBlank()) {
        throw new IllegalArgumentException(
            listener.getClass().getName() + " has no consumerId; the sweeper cannot catch it up");
      }
      if (!consumerIds.add(consumerId)) {
        throw new IllegalArgumentException("two durable listeners share consumerId " + consumerId);
      }
      Set<String> names = names(listener.signatures());
      if (names.isEmpty()) {
        continue;
      }
      boolean everything = names.contains(QitsRawEventListener.ALL);
      String variant = variant(names);
      if (!listener.replayFromEpoch()) {
        cases.add(
            new Case(Feature.CATCH_UP_INITIALIZE,
                Trigger.schedule(consumer, "CatchupSweeper.initialize(" + consumerId + ")"),
                variant, NO_EVENTS, LIST_EVENTS, newestQuery(names), null, PAGE,
                (client, params) ->
                    assertNull(client.newest(names), "an empty log has no newest event")));
      }
      cases.add(
          new Case(Feature.CATCH_UP_PAGE,
              Trigger.schedule(consumer, "CatchupSweeper.catchUp(" + consumerId + ")"),
              variant, everything ? A_FEW_RECENT_EVENTS : NO_EVENTS, LIST_EVENTS,
              pageQuery(names), null, everything ? FRAME : PAGE,
              everything ? EventstreamPact::readsEveryRecordedEvent : emptyLastPage(names)));
    }
    return List.copyOf(cases);
  }

  /** The pact of every row, as one V4 pact in row order (the committed file is sorted). */
  public V4Pact pact() {
    return pact(cases());
  }

  /** A pact holding only {@code cases} — one mock server per row, see {@link #run}. */
  public V4Pact pact(List<Case> cases) {
    EventstreamVia via = via();
    PactBuilder builder = new PactBuilder(consumer, PROVIDER, PactSpecVersion.V4);
    for (Case c : cases) {
      EventsGoldenMasters.interaction(
          builder, c.state(), c.operationId(), c.trigger(), via, c.variant(), c.query(), c.body(),
          c.reads());
    }
    return builder.toPact();
  }

  /**
   * Run one row: a pact mock server answering exactly that interaction, the library's real client
   * calling it, the row's own assertions on what the client made of the answer. One server per
   * row, because several rows send the same request in different states.
   */
  public void run(Case row) {
    PactVerificationResult result =
        ConsumerPactRunnerKt.runConsumerTest(
            pact(List.of(row)),
            MockProviderConfig.createDefault(PactSpecVersion.V4),
            (mockServer, context) -> {
              row.call()
                  .run(new ContractProbe(mockServer.getUrl()), EventsGoldenMasters.params(row.state()));
              return null;
            });
    if (!(result instanceof PactVerificationResult.Ok)) {
      Assertions.fail(
          row.description() + " [" + row.state() + "]: "
              + (result instanceof PactVerificationResult.Error error
                  ? "error: " + error.getError()
                  : result.getDescription() + " — " + result));
    }
  }

  /** {@link #run} for every row, one dynamic test each, named by description and state. */
  public Stream<DynamicTest> tests() {
    return cases().stream()
        .map(row -> DynamicTest.dynamicTest(
            row.description() + " [" + row.state() + "]", () -> run(row)));
  }

  /** Fails naming every listener or publisher the inputs leave out; see {@link EventstreamGuard}. */
  public void checkGuard() {
    Set<String> passed = new LinkedHashSet<>();
    listeners.forEach(l -> passed.add(l.getClass().getName()));
    List<String> uncovered = EventstreamGuard.uncovered(guard, passed, publishes);
    if (!uncovered.isEmpty()) {
      Assertions.fail(
          "the eventstream pact of " + consumer + " does not cover the service's code:\n  "
              + String.join("\n  ", uncovered));
    }
  }

  /**
   * {@link #checkGuard}, then the pact of every row, normalised, against the committed {@code
   * pacts/<file>} in the repository root (or {@code -Dqits.pacts.dir}): compared by default,
   * rewritten under {@code -Dgolden.update=true} or {@code QITS_GOLDEN_UPDATE=true}. With no row at
   * all the test is skipped, and fails if a file is still committed.
   *
   * <p>A difference in {@code qits-via}'s version ALONE — a bump of the service's qits-eventstream
   * pin — does not fail: the file is rewritten in place and one line says so. The rows always come
   * from the pinned library on every build; only the version label can lag until the next run
   * rewrites it. So an automated library bump never rejects the service's release.
   */
  public void compareOrWritePactFile() {
    compareOrWritePactFile(PactFiles.pactsDirectory());
  }

  /** {@link #compareOrWritePactFile()} against {@code pactsDir} instead of the repository's. */
  public void compareOrWritePactFile(Path pactsDir) {
    checkGuard();
    PactFiles.compareOrWrite(pactsDir.resolve(file()), cases().isEmpty() ? null : pact());
  }

  // --- the rows' shapes ----------------------------------------------------------------------------

  /** The names in the order the pact sends them: sorted, or just {@code *} when it takes all. */
  private static Set<String> names(Set<String> signatures) {
    if (signatures == null) {
      return Set.of();
    }
    Set<String> names = new TreeSet<>();
    for (String name : signatures) {
      if (name != null && !name.isBlank()) {
        names.add(name.trim());
      }
    }
    return names.contains(QitsRawEventListener.ALL)
        ? Set.of(QitsRawEventListener.ALL)
        : Collections.unmodifiableSet(names);
  }

  private static String variant(Set<String> names) {
    if (names.contains(QitsRawEventListener.ALL)) {
      return "every name";
    }
    return names.size() == 1 ? "one name" : names.size() + " names";
  }

  /** {@code EventsQuery.newest}'s query: {@code limit=1}, then the names unless it takes all. */
  private static Map<String, String> newestQuery(Set<String> names) {
    Map<String, String> query = new LinkedHashMap<>();
    query.put("limit", "1");
    nameParameter(names, query);
    return query;
  }

  /** {@code EventsQuery.after}'s first page: ascending, the sweep's page size, the names. */
  private static Map<String, String> pageQuery(Set<String> names) {
    Map<String, String> query = new LinkedHashMap<>();
    query.put("order", "asc");
    query.put("limit", Integer.toString(ContractProbe.PAGE_SIZE));
    nameParameter(names, query);
    return query;
  }

  private static void nameParameter(Set<String> names, Map<String, String> query) {
    if (!names.contains(QitsRawEventListener.ALL)) {
      query.put("name", String.join(",", names));
    }
  }

  private static Call emptyLastPage(Set<String> names) {
    return (client, params) -> {
      ContractProbe.Page page = client.after(names, null);
      assertEquals(List.of(), page.events(), "no events: an empty page");
      assertNull(page.nextCursor(), "an empty page is the last page");
    };
  }

  private static void readsEveryRecordedEvent(ContractProbe client, Map<String, String> params) {
    ContractProbe.Page page = client.after(Set.of(QitsRawEventListener.ALL), null);
    int recorded = EventsGoldenMasters.json(A_FEW_RECENT_EVENTS, LIST_EVENTS).path("events").size();
    assertEquals(recorded, page.events().size(), "one frame per recorded event, oldest first");
    for (EventFrame frame : page.events()) {
      assertNotNull(frame.id(), "id");
      assertNotNull(frame.name(), "name");
      assertNotNull(frame.occurredAt(), "occurredAt");
      assertNotNull(frame.payload(), "payload");
      assertNotNull(frame.publisher(), "publisher");
    }
    assertNull(page.nextCursor(), "the recording fits one page");
  }

  @Override
  public String toString() {
    return "EventstreamPact[" + consumer + ", " + listeners.size() + " listeners, publishes="
        + publishes + ", guard=" + guard + "]";
  }
}
