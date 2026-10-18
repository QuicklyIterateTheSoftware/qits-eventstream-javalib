package eu.wohlben.qits.eventstream.selection;

import static eu.wohlben.qits.eventstream.selection.EventSelection.Condition.absent;
import static eu.wohlben.qits.eventstream.selection.EventSelection.Condition.exact;
import static eu.wohlben.qits.eventstream.selection.EventSelection.Condition.exists;
import static eu.wohlben.qits.eventstream.selection.EventSelection.Condition.prefix;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** The text form: what each token reads as, what it refuses, and that format reads back. */
class EventSelectionGrammarTest {

  @Test
  void theFourForms() {
    assertEquals(
        List.of(
            exact("entities[].status", "VERIFIED"),
            prefix("version", "2026."),
            exists("releaseRequestId"),
            absent("previousStatus")),
        EventSelectionGrammar.parseGroup(
            "entities[].status=VERIFIED version^=2026. releaseRequestId? !previousStatus"));
  }

  @Test
  void quotedValuesHoldSpacesQuotesAndNothing() {
    assertEquals(
        List.of(exact("title", "two words"), exact("note", ""), exact("q", "say \"hi\" \\ ok")),
        EventSelectionGrammar.parseGroup("title=\"two words\"   note=\"\"\tq=\"say \\\"hi\\\" \\\\ ok\""));
  }

  @Test
  void aValueMayContainOperatorCharactersAfterTheOperator() {
    assertEquals(List.of(exact("ref", "a=b^c?!")), EventSelectionGrammar.parseGroup("ref=a=b^c?!"));
  }

  @Test
  void formatReadsBackToTheSameConditions() {
    List<EventSelection.Condition> group =
        List.of(
            exact("entities[].number", "qits-123"),
            exact("title", "two words"),
            exact("note", ""),
            exact("q", "say \"hi\" \\ ok"),
            prefix("v", "2026."),
            exists("a.b"),
            absent("c"));
    String text = EventSelectionGrammar.format(group);
    assertEquals(group, EventSelectionGrammar.parseGroup(text));
  }

  @Test
  void parseBuildsTheWholeSelection() {
    EventSelection selection =
        EventSelectionGrammar.parse("BuildStatusChanged", List.of("runId=r1 status=SUCCEEDED", "runId=r1 status=FAILED"));
    assertEquals("BuildStatusChanged", selection.event());
    assertEquals(2, selection.when().size());
    assertEquals(
        "BuildStatusChanged when (runId=r1 status=SUCCEEDED) or (runId=r1 status=FAILED)",
        EventSelectionGrammar.describe(selection));
    assertEquals("BuildStatusChanged", EventSelectionGrammar.describe(EventSelectionGrammar.parse("BuildStatusChanged", List.of())));
    assertEquals(
        "E when a=1",
        EventSelectionGrammar.describe(EventSelectionGrammar.parse("E", List.of("a=1"))));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "",
        "   ",
        "status",
        "=VERIFIED",
        "status==VERIFIED",
        "status=",
        "status~x",
        "status?x",
        "!",
        "!status=x",
        "status=\"open",
        "status=ab\"c",
        "status=\"a\"b",
        "status^=",
        "status^=\"\"",
        "entities[status=x",
        "entities[0].status=x",
        "a..b=x",
        ".a=x",
        "a.=x",
        "1a=x",
        "a b c d e f g h i"
      })
  void malformedGroupsAreRefusedWithAReason(String group) {
    EventSelectionException refused =
        assertThrows(EventSelectionException.class, () -> EventSelectionGrammar.parseGroup(group));
    assertTrue(refused.getMessage().length() > 10, refused.getMessage());
  }

  @Test
  void theMessageNamesTheToken() {
    EventSelectionException refused =
        assertThrows(EventSelectionException.class, () -> EventSelectionGrammar.parseGroup("ok=1 status~x"));
    assertTrue(refused.getMessage().startsWith("'status~x'"), refused.getMessage());
  }

  @Test
  void anInvalidPathIsReportedWithTheToken() {
    EventSelectionException refused =
        assertThrows(EventSelectionException.class, () -> EventSelectionGrammar.parseGroup("a[0]=x"));
    assertTrue(refused.getMessage().contains("'a[0]=x'"), refused.getMessage());
    assertTrue(refused.getMessage().contains("entities[].status"), refused.getMessage());
  }

  @Test
  void tooManyConditionsInOneGroup() {
    String nine = "a=1 b=1 c=1 d=1 e=1 f=1 g=1 h=1 i=1";
    EventSelectionException refused =
        assertThrows(EventSelectionException.class, () -> EventSelectionGrammar.parseGroup(nine));
    assertTrue(refused.getMessage().contains("the most is 8"), refused.getMessage());
  }
}
