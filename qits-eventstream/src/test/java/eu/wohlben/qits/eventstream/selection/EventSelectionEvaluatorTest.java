package eu.wohlben.qits.eventstream.selection;

import static eu.wohlben.qits.eventstream.selection.EventSelection.Condition.absent;
import static eu.wohlben.qits.eventstream.selection.EventSelection.Condition.exact;
import static eu.wohlben.qits.eventstream.selection.EventSelection.Condition.exists;
import static eu.wohlben.qits.eventstream.selection.EventSelection.Condition.prefix;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import eu.wohlben.qits.eventstream.control.EventFrame;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/** The matching rules, over payloads shaped like the platform's real events. */
class EventSelectionEvaluatorTest {

  private static final String ENTITY_TRANSITIONED =
      """
      {"occurredAt":"2026-10-10T10:00:00Z","actor":"agent:abc",
       "entities":[
         {"entityId":"e-1","number":"qits-122","projectId":"qits","kind":"TICKET","status":"IN_PROGRESS"},
         {"entityId":"e-2","number":"qits-123","projectId":"qits","kind":"TICKET","status":"VERIFIED",
          "previousStatus":"DONE","labels":["agents","waits"],"estimate":3,"blocked":false}
       ]}
      """;

  private static final String BUILD_STATUS_CHANGED =
      """
      {"runId":"run-42","status":"SUCCEEDED","previousStatus":"RUNNING",
       "releaseRequestId":"rr-7","attempt":2}
      """;

  private static final String BUILD_STATUS_WITHOUT_RR =
      """
      {"runId":"run-43","status":"FAILED","previousStatus":"RUNNING"}
      """;

  private static final String RELEASE_REQUEST_STATE_CHANGED =
      """
      {"releaseRequestId":"rr-7","projectId":"qits","state":"MERGED","previousState":"GATING",
       "occurredAt":"2026-10-10T10:05:00Z"}
      """;

  private static JsonNode json(String text) {
    return EventSelectionEvaluator.parsePayload(text);
  }

  private static EventFrame frame(String name, String payload) {
    return new EventFrame("id-1", name, Instant.parse("2026-10-10T10:00:00Z"), payload, null, null, "dev");
  }

  @Nested
  class EntityTransitioned {

    @Test
    void oneEntityReachingTheStatusMatches() {
      EventSelection waitFor =
          EventSelection.of(
              "EntityTransitioned",
              exact("entities[].number", "qits-123"),
              exact("entities[].status", "VERIFIED"));
      assertTrue(EventSelectionEvaluator.matches(waitFor, frame("EntityTransitioned", ENTITY_TRANSITIONED)));
    }

    @Test
    void theTwoConditionsMustHoldOnTheSameElement() {
      // qits-122 is in the list, and some entity is VERIFIED, but not the same one.
      EventSelection waitFor =
          EventSelection.of(
              "EntityTransitioned",
              exact("entities[].number", "qits-122"),
              exact("entities[].status", "VERIFIED"));
      assertFalse(EventSelectionEvaluator.matches(waitFor, "EntityTransitioned", json(ENTITY_TRANSITIONED)));
    }

    @Test
    void aSingleArrayConditionNeedsSomeElement() {
      assertTrue(match(exact("entities[].status", "IN_PROGRESS")));
      assertTrue(match(prefix("entities[].number", "qits-12")));
      assertFalse(match(exact("entities[].status", "OBSOLETE")));
    }

    @Test
    void walkingIntoAnArrayWithoutBracketsIsAbsent() {
      assertFalse(match(exact("entities.status", "VERIFIED")));
      assertFalse(match(exists("entities.status")));
      assertTrue(match(absent("entities.status")));
    }

    @Test
    void presenceInsideElementsIsPerElement() {
      // Only qits-123 has a previousStatus; qits-122 lacks one.
      assertTrue(match(exact("entities[].number", "qits-123"), exists("entities[].previousStatus")));
      assertFalse(match(exact("entities[].number", "qits-122"), exists("entities[].previousStatus")));
      assertTrue(match(exact("entities[].number", "qits-122"), absent("entities[].previousStatus")));
    }

    @Test
    void aMissingOrEmptyArrayHasNoElementSoEvenAbsentFails() {
      JsonNode empty = json("{\"entities\":[]}");
      JsonNode none = json("{}");
      EventSelection noField = EventSelection.of("EntityTransitioned", absent("entities[].status"));
      assertFalse(EventSelectionEvaluator.matches(noField, "EntityTransitioned", empty));
      assertFalse(EventSelectionEvaluator.matches(noField, "EntityTransitioned", none));
      EventSelection noList = EventSelection.of("EntityTransitioned", absent("entities"));
      assertTrue(EventSelectionEvaluator.matches(noList, "EntityTransitioned", none));
    }

    @Test
    void aScalarArrayIsSearchedByItsElements() {
      assertTrue(match(exact("entities[].labels[]", "waits")));
      assertFalse(match(exact("entities[].labels[]", "ci")));
      assertTrue(match(exact("entities[].number", "qits-123"), exact("entities[].labels[]", "agents")));
      assertFalse(match(exact("entities[].number", "qits-122"), exact("entities[].labels[]", "agents")));
      assertTrue(match(exists("entities[].labels[]")));
    }

    @Test
    void nonStringsCompareByTheirJsonLiteral() {
      assertTrue(match(exact("entities[].estimate", "3")));
      assertTrue(match(exact("entities[].blocked", "false")));
      assertFalse(match(exact("entities[].estimate", "3.0")));
      // An array value compares as its JSON text when not searched with [].
      assertTrue(match(exact("entities[].labels", "[\"agents\",\"waits\"]")));
    }

    @Test
    void aTopLevelConditionAndAnArrayConditionCombine() {
      assertTrue(match(prefix("actor", "agent:"), exact("entities[].status", "VERIFIED")));
      assertFalse(match(prefix("actor", "user:"), exact("entities[].status", "VERIFIED")));
    }

    @Test
    void aPathThroughAScalarIsAbsent() {
      assertFalse(match(exists("actor.name")));
      assertFalse(match(exact("entities[].status[]", "VERIFIED")));
    }

    private boolean match(EventSelection.Condition... group) {
      return EventSelectionEvaluator.matches(
          EventSelection.of("EntityTransitioned", group), "EntityTransitioned", json(ENTITY_TRANSITIONED));
    }
  }

  @Nested
  class NestedArrays {

    private final JsonNode payload =
        json(
            """
            {"a":[{"id":"x","b":[{"c":1,"d":"no"},{"c":2,"d":"yes"}]},
                  {"id":"y","b":[{"c":1,"d":"yes"}]}]}
            """);

    @Test
    void conditionsThroughTheSameArraysShareElementsAtEveryLevel() {
      assertTrue(holds(exact("a[].id", "x"), exact("a[].b[].c", "2"), exact("a[].b[].d", "yes")));
      assertFalse(holds(exact("a[].id", "x"), exact("a[].b[].c", "1"), exact("a[].b[].d", "yes")));
      assertTrue(holds(exact("a[].id", "y"), exact("a[].b[].c", "1"), exact("a[].b[].d", "yes")));
    }

    private boolean holds(EventSelection.Condition... group) {
      return EventSelectionEvaluator.matchesPayload(EventSelection.of("E", group), payload);
    }
  }

  @Nested
  class BuildStatusChanged {

    @Test
    void aRunReachingAFinalStatusMatchesOneOfTheGroups() {
      EventSelection waitFor =
          new EventSelection(
              "BuildStatusChanged",
              List.of(
                  List.of(exact("runId", "run-42"), exact("status", "SUCCEEDED")),
                  List.of(exact("runId", "run-42"), exact("status", "FAILED"))));
      assertTrue(EventSelectionEvaluator.matches(waitFor, frame("BuildStatusChanged", BUILD_STATUS_CHANGED)));
      assertFalse(EventSelectionEvaluator.matches(waitFor, frame("BuildStatusChanged", BUILD_STATUS_WITHOUT_RR)));
    }

    @Test
    void theNameMustBeExact() {
      EventSelection waitFor = EventSelection.of("BuildStatusChanged", exact("runId", "run-42"));
      assertFalse(EventSelectionEvaluator.matches(waitFor, frame("BuildStatusChange", BUILD_STATUS_CHANGED)));
      assertFalse(EventSelectionEvaluator.matches(waitFor, frame("buildstatuschanged", BUILD_STATUS_CHANGED)));
    }

    @Test
    void presenceAndAbsenceOfAField() {
      EventSelection forReleases = EventSelection.of("BuildStatusChanged", exists("releaseRequestId"));
      assertTrue(EventSelectionEvaluator.matches(forReleases, "BuildStatusChanged", json(BUILD_STATUS_CHANGED)));
      assertFalse(EventSelectionEvaluator.matches(forReleases, "BuildStatusChanged", json(BUILD_STATUS_WITHOUT_RR)));
      EventSelection notForReleases = EventSelection.of("BuildStatusChanged", absent("releaseRequestId"));
      assertTrue(EventSelectionEvaluator.matches(notForReleases, "BuildStatusChanged", json(BUILD_STATUS_WITHOUT_RR)));
    }

    @Test
    void aJsonNullIsAbsent() {
      JsonNode payload = json("{\"runId\":\"run-1\",\"releaseRequestId\":null}");
      assertFalse(EventSelectionEvaluator.matchesPayload(EventSelection.of("B", exists("releaseRequestId")), payload));
      assertTrue(EventSelectionEvaluator.matchesPayload(EventSelection.of("B", absent("releaseRequestId")), payload));
    }

    @Test
    void numbersCompareAsText() {
      assertTrue(
          EventSelectionEvaluator.matchesPayload(
              EventSelection.of("B", exact("attempt", "2")), json(BUILD_STATUS_CHANGED)));
    }

    @Test
    void anUnconditionalSelectionMatchesEveryEventOfTheName() {
      EventSelection any = EventSelection.of("BuildStatusChanged");
      assertTrue(EventSelectionEvaluator.matches(any, frame("BuildStatusChanged", BUILD_STATUS_CHANGED)));
      assertTrue(EventSelectionEvaluator.matches(any, frame("BuildStatusChanged", null)));
      assertFalse(EventSelectionEvaluator.matches(any, frame("BuildSuccessful", BUILD_STATUS_CHANGED)));
    }
  }

  @Nested
  class ReleaseRequestStateChanged {

    private final EventSelection finalStates =
        EventSelectionGrammar.parse(
            "ReleaseRequestStateChanged",
            List.of(
                "releaseRequestId=rr-7 state=MERGED",
                "releaseRequestId=rr-7 state=REJECTED",
                "releaseRequestId=rr-7 state=WITHDRAWN"));

    @Test
    void aFinalStateOfTheRequestMatches() {
      assertTrue(
          EventSelectionEvaluator.matches(
              finalStates, frame("ReleaseRequestStateChanged", RELEASE_REQUEST_STATE_CHANGED)));
    }

    @Test
    void anotherRequestOrAnIntermediateStateDoesNot() {
      String other = RELEASE_REQUEST_STATE_CHANGED.replace("rr-7", "rr-8");
      String gating = RELEASE_REQUEST_STATE_CHANGED.replace("\"MERGED\"", "\"GATING\"");
      assertFalse(EventSelectionEvaluator.matches(finalStates, frame("ReleaseRequestStateChanged", other)));
      assertFalse(EventSelectionEvaluator.matches(finalStates, frame("ReleaseRequestStateChanged", gating)));
    }
  }

  @Nested
  class Payloads {

    @Test
    void aPayloadThatIsNotJsonHasNoPaths() {
      assertNull(EventSelectionEvaluator.parsePayload("not json {"));
      assertNull(EventSelectionEvaluator.parsePayload(""));
      assertNull(EventSelectionEvaluator.parsePayload(null));
      assertTrue(EventSelectionEvaluator.matches(EventSelection.of("E", absent("x")), frame("E", "garbage")));
      assertFalse(EventSelectionEvaluator.matches(EventSelection.of("E", exists("x")), frame("E", "garbage")));
    }

    @Test
    void aNullFrameMatchesNothing() {
      assertFalse(EventSelectionEvaluator.matches(EventSelection.of("E"), (EventFrame) null));
    }

    @Test
    void anArrayPayloadIsSearchable() {
      // A selection cannot address a top-level array, and an object path over one is absent.
      JsonNode payload = json("[{\"a\":1}]");
      assertFalse(EventSelectionEvaluator.matchesPayload(EventSelection.of("E", exists("a")), payload));
    }
  }
}
