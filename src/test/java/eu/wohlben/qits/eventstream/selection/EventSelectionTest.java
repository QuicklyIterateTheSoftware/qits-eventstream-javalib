package eu.wohlben.qits.eventstream.selection;

import static eu.wohlben.qits.eventstream.selection.EventSelection.Condition.absent;
import static eu.wohlben.qits.eventstream.selection.EventSelection.Condition.exact;
import static eu.wohlben.qits.eventstream.selection.EventSelection.Condition.exists;
import static eu.wohlben.qits.eventstream.selection.EventSelection.Condition.prefix;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import eu.wohlben.qits.eventstream.selection.EventSelection.Condition;
import eu.wohlben.qits.eventstream.selection.EventSelection.Op;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

/** The model: the JSON wire form, and every check the constructor makes. */
class EventSelectionTest {

  @Test
  void theJsonFormIsTheWireShape() {
    EventSelection selection =
        new EventSelection(
            "EntityTransitioned",
            List.of(
                List.of(exact("entities[].number", "qits-123"), exact("entities[].status", "VERIFIED")),
                List.of(prefix("actor", "agent:"), exists("a"), absent("b"))));
    assertEquals(
        "{\"event\":\"EntityTransitioned\",\"when\":["
            + "[{\"path\":\"entities[].number\",\"op\":\"exact\",\"value\":\"qits-123\"},"
            + "{\"path\":\"entities[].status\",\"op\":\"exact\",\"value\":\"VERIFIED\"}],"
            + "[{\"path\":\"actor\",\"op\":\"prefix\",\"value\":\"agent:\"},"
            + "{\"path\":\"a\",\"op\":\"exists\",\"value\":true},"
            + "{\"path\":\"b\",\"op\":\"exists\",\"value\":false}]]}",
        selection.toJson());
    assertEquals(selection, EventSelection.fromJson(selection.toJson()));
  }

  @Test
  void theJsonFormReadsLeniently() {
    EventSelection read =
        EventSelection.fromJson(
            """
            {"event":"E","when":[[
              {"path":"a","op":"EXISTS"},
              {"path":"b","op":"absent"},
              {"path":"c","op":"exists","value":false},
              {"path":"d","op":"Exact","value":"x"}]]}
            """);
    assertEquals(List.of(exists("a"), absent("b"), absent("c"), exact("d", "x")), read.when().get(0));
    assertEquals(EventSelection.of("E"), EventSelection.fromJson("{\"event\":\"E\"}"));
    assertEquals(EventSelection.of("E"), EventSelection.fromJson("{\"event\":\"E\",\"when\":[]}"));
  }

  @Test
  void invalidJsonIsRefusedWithOurMessage() {
    assertRefused("{\"event\":\"E\",\"when\":[[{\"path\":\"a\",\"op\":\"regex\",\"value\":\"x\"}]]}", "not an operator");
    assertRefused("{\"event\":\"E\",\"when\":[[{\"path\":\"a\",\"op\":\"exact\",\"value\":3}]]}", "values are strings");
    assertRefused("{\"event\":\"E\",\"when\":[[{\"path\":\"a\",\"op\":\"exists\",\"value\":\"no\"}]]}", "true or false");
    assertRefused("{\"event\":\"E\",\"when\":[[{\"path\":\"a\",\"op\":\"exact\"}]]}", "gives no value");
    assertRefused("{\"event\":\"E\",\"when\":[[]]}", "is empty");
    assertRefused("{\"when\":[]}", "names no event");
    assertRefused("{\"event\":\"E\",\"when\":[[{\"path\":\"\",\"op\":\"exists\"}]]}", "empty path");
    assertRefused("{\"event\":", "not valid JSON");
    assertRefused("null", "is empty");
  }

  @Test
  void theEventIsOneExactName() {
    assertThrows(EventSelectionException.class, () -> EventSelection.of("*"));
    assertThrows(EventSelectionException.class, () -> EventSelection.of(" "));
    assertThrows(EventSelectionException.class, () -> EventSelection.of("Build Status"));
    assertThrows(EventSelectionException.class, () -> EventSelection.of("x".repeat(129)));
    assertEquals("SCMRelease", EventSelection.of("SCMRelease").event());
  }

  @Test
  void theCapsHold() {
    List<List<Condition>> nineGroups = Collections.nCopies(9, List.of(exists("a")));
    assertMessage(() -> new EventSelection("E", nineGroups), "9 groups");
    assertEquals(8, new EventSelection("E", Collections.nCopies(8, List.of(exists("a")))).when().size());

    List<Condition> nine = Collections.nCopies(9, exists("a"));
    assertMessage(() -> new EventSelection("E", List.of(nine)), "9 conditions");

    assertMessage(() -> exists("a.b.c.d.e.f.g.h.i"), "9 segments");
    exists("a.b.c.d.e.f.g.h");
    assertMessage(() -> exists("a".repeat(257)), "longer than 256");
    assertMessage(() -> exact("a", "v".repeat(1025)), "longer than 1024");
  }

  @Test
  void conditionsCheckTheirValues() {
    assertMessage(() -> prefix("a", ""), "use exists");
    assertMessage(() -> new Condition("a", Op.EXISTS, "x"), "takes none");
    assertMessage(() -> new Condition("a", null, "x"), "no operator");
    assertEquals("", exact("a", "").value());
  }

  @Test
  void theSelectionIsImmutable() {
    List<Condition> group = new ArrayList<>(List.of(exists("a")));
    List<List<Condition>> when = new ArrayList<>(List.of(group));
    EventSelection selection = new EventSelection("E", when);
    group.add(exists("b"));
    when.add(List.of(exists("c")));
    assertEquals(List.of(List.of(exists("a"))), selection.when());
    assertThrows(UnsupportedOperationException.class, () -> selection.when().add(List.of()));
  }

  private static void assertRefused(String json, String fragment) {
    assertMessage(() -> EventSelection.fromJson(json), fragment);
  }

  private static void assertMessage(Runnable action, String fragment) {
    EventSelectionException refused = assertThrows(EventSelectionException.class, action::run);
    assertTrue(refused.getMessage().contains(fragment), refused.getMessage());
  }
}
