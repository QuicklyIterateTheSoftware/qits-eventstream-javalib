package eu.wohlben.qits.eventstream.contracts;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.com.dius.pact.consumer.dsl.PactBuilder;
import au.com.dius.pact.core.model.PactSpecVersion;
import au.com.dius.pact.core.model.V4Pact;
import eu.wohlben.qits.eventstream.contracts.EventsGoldenMasters.Trigger;
import eu.wohlben.qits.eventstream.control.ContractProbe;
import eu.wohlben.qits.eventstream.control.EventEnvelope;
import eu.wohlben.qits.eventstream.control.EventFrame;
import eu.wohlben.qits.eventstream.control.EventsPublisher;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * <b>Every REST call this library makes to qits-events, and what it reads</b> (qits-1149) — the
 * one table {@code EventsConsumerPactTest} (each row against a pact mock server) and {@code
 * EventsPactFileTest} (the committed {@code pacts/qits-eventstream-javalib_qits-events-service.json})
 * are built from. One pact here replaces the copies each consuming service used to write.
 *
 * <ul>
 *   <li>{@code GET /events/api/events}, {@code CatchupSweeper.initialize} through {@code
 *       EventsQuery.newest}: {@code ?limit=1[&name=A,B]}. Reads {@code events[0]}, or none.
 *   <li>{@code GET /events/api/events}, {@code CatchupSweeper.catchUp} through {@code
 *       EventsQuery.after}: {@code ?order=asc&limit=200[&name=A,B][&cursor=<occurredAt>,<id>]}. Reads
 *       every frame and {@code nextCursor}.
 *   <li>{@code PUT /events/api/events/{id}}, {@code OutboxSweeper.tick} (and the identical inline
 *       attempt in {@code QitsEventBus.publish}) through {@code EventsPublisher.put}. Status only:
 *       200/201 delivered, 400 a reused id.
 * </ul>
 *
 * <p>A name filter of {@code "*"} leaves {@code name} off. The websocket {@code /events/stream} is
 * not REST and has no pact.
 *
 * <p><b>Pending rows</b> ({@link Case#pending}) need a provider state qits-events has not recorded
 * yet. They stay in the table, are not written to the pact, and the consumer test reports them as
 * skipped.
 */
final class EventsContract {

  // Provider states, all recorded in qits-events-golden-masters 2026.1010.121628.
  static final String NO_EVENTS = "no events";
  static final String A_FEW_RECENT_EVENTS = "a few recent events";
  static final String ONE_EVENT = "one DeploymentActive event";
  static final String MORE_THAN_A_PAGE = "more than a page of DeploymentActive events";
  static final String NO_EVENT_WITH_THE_ID = "no event with the given id";
  static final String AN_EVENT_WITH_THE_ID = "an event with the given id";

  static final String LIST_EVENTS = "listEvents";
  static final String PUBLISH_EVENT = "publishEvent";

  static final Trigger INITIALIZE = Trigger.schedule("CatchupSweeper.initialize");
  static final Trigger CATCH_UP = Trigger.schedule("CatchupSweeper.catchUp");
  static final Trigger OUTBOX = Trigger.schedule("OutboxSweeper.tick");

  static final String DEPLOYMENT_ACTIVE = "DeploymentActive";

  /** Two names, in the order the library joins them: one {@code name=A,B} parameter. */
  static final List<String> TWO_NAMES = List.of("ProjectCreated", "ProjectChanged");

  /** A cursor as the log hands it out, {@code <occurredAt>,<id>}. */
  static final String CURSOR = "2026-01-01T00:00:00Z,00000000-0000-4000-8000-000000000001";

  /**
   * Every frame field the library binds ({@code EventFrame}) but {@code environment}, which the
   * state {@value #A_FEW_RECENT_EVENTS} records as null.
   */
  static final List<String> FRAME =
      List.of(
          "events[*].id",
          "events[*].name",
          "events[*].occurredAt",
          "events[*].payload",
          "events[*].description",
          "events[*].parentId");

  /**
   * The envelope every publish row sends. Its content must differ from whatever the state {@value
   * #AN_EVENT_WITH_THE_ID} stores at the id, which is what makes the 400.
   */
  static final EventEnvelope ENVELOPE =
      new EventEnvelope(
          DEPLOYMENT_ACTIVE,
          Instant.parse("2026-01-01T00:00:00Z"),
          "{\"application\":\"qits-eventstream-pact\"}",
          null,
          null,
          null);

  /** What the library does with the client for one row, asserting what that path makes of it. */
  @FunctionalInterface
  interface Call {
    void run(ContractProbe client, Map<String, String> params);
  }

  /**
   * One (trigger, query shape, state).
   *
   * @param body the request body, or null
   * @param reads the response paths the call binds — the only ones the pact carries
   * @param pending null when the row is in the pact; otherwise why it cannot be yet
   */
  record Case(
      Trigger trigger,
      String variant,
      String state,
      String operationId,
      Map<String, String> query,
      String body,
      List<String> reads,
      Call call,
      String pending) {

    String description() {
      return EventsGoldenMasters.description(operationId, trigger, variant);
    }
  }

  private static Map<String, String> newestQuery(List<String> names) {
    Map<String, String> query = new LinkedHashMap<>();
    query.put("limit", "1");
    if (!names.isEmpty()) {
      query.put("name", String.join(",", names));
    }
    return query;
  }

  private static Map<String, String> pageQuery(List<String> names, String cursor) {
    Map<String, String> query = new LinkedHashMap<>();
    query.put("order", "asc");
    query.put("limit", Integer.toString(ContractProbe.PAGE_SIZE));
    if (!names.isEmpty()) {
      query.put("name", String.join(",", names));
    }
    if (cursor != null) {
      query.put("cursor", cursor);
    }
    return query;
  }

  /** {@link #FRAME} and {@code environment}: the states that record it carry "dev". */
  static final List<String> FRAME_WITH_ENVIRONMENT = concat(FRAME, "events[*].environment");

  private static List<String> filter(List<String> names) {
    return names.isEmpty() ? List.of("*") : names;
  }

  private static Call newestIsNull(List<String> names) {
    return (client, params) ->
        assertNull(client.newest(filter(names)), "an empty log has no newest event");
  }

  private static Call emptyLastPage(List<String> names, String cursor) {
    return (client, params) -> {
      ContractProbe.Page page = client.after(filter(names), cursor);
      assertEquals(List.of(), page.events());
      assertNull(page.nextCursor(), "an empty page is the last page");
    };
  }

  private static void assertFrame(EventFrame frame) {
    assertNotNull(frame.id(), "id");
    assertNotNull(frame.name(), "name");
    assertNotNull(frame.occurredAt(), "occurredAt");
    assertNotNull(frame.payload(), "payload");
  }

  static final List<Case> CASES =
      List.of(
          new Case(INITIALIZE, "one name", NO_EVENTS, LIST_EVENTS,
              newestQuery(List.of(DEPLOYMENT_ACTIVE)), null, List.of("events"),
              newestIsNull(List.of(DEPLOYMENT_ACTIVE)), null),
          new Case(INITIALIZE, "every name", NO_EVENTS, LIST_EVENTS,
              newestQuery(List.of()), null, List.of("events"),
              newestIsNull(List.of()), null),
          new Case(CATCH_UP, "one name, from the start", NO_EVENTS, LIST_EVENTS,
              pageQuery(List.of(DEPLOYMENT_ACTIVE), null), null, List.of("events"),
              emptyLastPage(List.of(DEPLOYMENT_ACTIVE), null), null),
          new Case(CATCH_UP, "two names, after a cursor", NO_EVENTS, LIST_EVENTS,
              pageQuery(TWO_NAMES, CURSOR), null, List.of("events"),
              emptyLastPage(TWO_NAMES, CURSOR), null),
          new Case(CATCH_UP, "every name, from the start", A_FEW_RECENT_EVENTS, LIST_EVENTS,
              pageQuery(List.of(), null), null, FRAME,
              (client, params) -> {
                ContractProbe.Page page = client.after(List.of("*"), null);
                assertEquals(3, page.events().size(), "the state's three events, oldest first");
                page.events().forEach(EventsContract::assertFrame);
                assertNull(page.nextCursor(), "three events fit one page");
              },
              null),
          new Case(INITIALIZE, "one name, found", ONE_EVENT, LIST_EVENTS,
              newestQuery(List.of(DEPLOYMENT_ACTIVE)), null,
              List.of("events[*].id", "events[*].occurredAt", "events[*].environment"),
              (client, params) -> {
                EventFrame newest = client.newest(List.of(DEPLOYMENT_ACTIVE));
                assertNotNull(newest, "the log's one event is its newest");
                assertEquals(params.get("eventId"), newest.id());
                assertNotNull(newest.occurredAt());
                assertEquals("dev", newest.environment());
              },
              null),
          new Case(CATCH_UP, "one name, a full page", MORE_THAN_A_PAGE, LIST_EVENTS,
              pageQuery(List.of(DEPLOYMENT_ACTIVE), null), null,
              concat(FRAME_WITH_ENVIRONMENT, "nextCursor"),
              (client, params) -> {
                ContractProbe.Page page = client.after(List.of(DEPLOYMENT_ACTIVE), null);
                assertEquals(ContractProbe.PAGE_SIZE, page.events().size());
                page.events().forEach(EventsContract::assertFrame);
                page.events().forEach(e -> assertEquals("dev", e.environment()));
                assertNotNull(page.nextCursor(), "a full page with more behind it names the next");
              },
              null),
          new Case(OUTBOX, "a new id", NO_EVENT_WITH_THE_ID, PUBLISH_EVENT,
              Map.of(), ContractProbe.body(ENVELOPE), List.of(),
              (client, params) -> {
                EventsPublisher.Delivery delivery = client.put(params.get("eventId"), ENVELOPE);
                assertTrue(delivery.delivered(), "a new id is delivered: " + delivery);
              },
              null),
          new Case(OUTBOX, "a reused id", AN_EVENT_WITH_THE_ID, PUBLISH_EVENT,
              Map.of(), ContractProbe.body(ENVELOPE), List.of(),
              (client, params) -> {
                EventsPublisher.Delivery delivery = client.put(params.get("eventId"), ENVELOPE);
                assertTrue(delivery.rejected(), "a reused id is rejected for good: " + delivery);
              },
              null));

  private static List<String> concat(List<String> list, String more) {
    List<String> all = new java.util.ArrayList<>(list);
    all.add(more);
    return List.copyOf(all);
  }

  /** The rows the pact holds today. */
  static List<Case> ready() {
    return CASES.stream().filter(c -> c.pending() == null).toList();
  }

  /** The rows waiting on a provider state. */
  static List<Case> pending() {
    return CASES.stream().filter(c -> c.pending() != null).toList();
  }

  private EventsContract() {}

  /** The whole contract as one V4 pact: every ready row, in table order (the file test sorts). */
  static V4Pact pact() {
    return pact(ready());
  }

  /** A pact holding only {@code cases} — one mock server per row, see the pact test. */
  static V4Pact pact(List<Case> cases) {
    PactBuilder builder =
        new PactBuilder(
            EventsGoldenMasters.CONSUMER, EventsGoldenMasters.PROVIDER, PactSpecVersion.V4);
    for (Case c : cases) {
      if (c.pending() != null) {
        throw new IllegalArgumentException(c.description() + " is pending: " + c.pending());
      }
      EventsGoldenMasters.interaction(
          builder, c.state(), c.operationId(), c.trigger(), c.variant(), c.query(), c.body(),
          c.reads());
    }
    return builder.toPact();
  }
}
