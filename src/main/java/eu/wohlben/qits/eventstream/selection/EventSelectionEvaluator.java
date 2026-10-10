package eu.wohlben.qits.eventstream.selection;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.wohlben.qits.eventstream.control.EventFrame;
import eu.wohlben.qits.eventstream.selection.EventSelection.Condition;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Decides whether one event matches one {@link EventSelection}. Pure: static, no state, no IO.
 *
 * <h2>The rules</h2>
 *
 * <ul>
 *   <li>The event name must equal {@link EventSelection#event()} exactly.
 *   <li>Groups are ORed, conditions in a group ANDed. No groups: every event of the name matches.
 *   <li>A path is field names joined by dots. A plain segment reads an object field. A segment that
 *       reaches an array, a scalar or nothing makes the path absent. So {@code entities.status}
 *       over an array of entities is <b>absent</b>: it does not match {@code =}, {@code ^=} or
 *       {@code ?}, and it does match {@code !}.
 *   <li>JSON null counts as absent.
 *   <li>Values compare as strings: a JSON string by its text, anything else by its JSON literal. So
 *       {@code number=3} matches the number 3 and {@code done=true} the boolean.
 *   <li>{@code ?} holds when the path resolves; {@code !} when it does not. {@code =} and {@code ^=}
 *       fail on an absent path.
 * </ul>
 *
 * <h2>Arrays: {@code []} means "some element"</h2>
 *
 * <p>A segment written {@code name[]} reads the array at {@code name} and holds when <b>some
 * element</b> satisfies the rest of the path. All conditions of one group that go through the same
 * {@code name[]} are checked against the <b>same element</b>: {@code entities[].number=qits-123
 * entities[].status=VERIFIED} needs one entity that is both, not one entity that is qits-123 and
 * another that is VERIFIED. Nested arrays follow the same rule level by level ({@code a[].b[].c}).
 *
 * <p>Two edges follow, both deliberate:
 *
 * <ul>
 *   <li>A missing, empty or non-array value at {@code name[]} has no elements, so every condition
 *       through it fails — {@code !entities[].x} too. To say there is no list, write {@code
 *       !entities}.
 *   <li>"Some entity is X, and some <em>other</em> entity is Y" cannot be written in one group. No
 *       wait the platform needs asks it, and allowing it would make the common case ambiguous.
 * </ul>
 *
 * <p>The payload is read with {@code readTree}, never bound, so a native image needs no reflection
 * for it.
 */
public final class EventSelectionEvaluator {

  /**
   * Private and plain. It only reads trees, so no wire contract depends on its settings, and no
   * application customizer can change what matches.
   */
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private EventSelectionEvaluator() {}

  /**
   * Parses a payload into a tree, or null when it is absent or not JSON. Null is a payload with no
   * paths: only {@code !path} holds over it.
   */
  public static JsonNode parsePayload(String payload) {
    if (payload == null || payload.isBlank()) {
      return null;
    }
    try {
      return MAPPER.readTree(payload);
    } catch (Exception notJson) {
      return null;
    }
  }

  /** Whether the frame matches. Parses the payload; to test many selections, parse once and use the other overload. */
  public static boolean matches(EventSelection selection, EventFrame frame) {
    if (frame == null) {
      return false;
    }
    if (!selection.event().equals(frame.name())) {
      return false;
    }
    return matchesPayload(selection, parsePayload(frame.payload()));
  }

  /** Whether an event of this name with this payload matches. */
  public static boolean matches(EventSelection selection, String name, JsonNode payload) {
    return selection.event().equals(name) && matchesPayload(selection, payload);
  }

  /** The {@code when} alone, without the name check. */
  public static boolean matchesPayload(EventSelection selection, JsonNode payload) {
    if (selection.isUnconditional()) {
      return true;
    }
    for (List<Condition> group : selection.when()) {
      if (groupMatches(group, payload)) {
        return true;
      }
    }
    return false;
  }

  static boolean groupMatches(List<Condition> group, JsonNode payload) {
    List<Pending> pending = new ArrayList<>(group.size());
    for (Condition condition : group) {
      pending.add(new Pending(EventSelection.requirePath(condition.path()), condition));
    }
    return allHold(pending, payload);
  }

  /** A condition with the part of its path still to walk. */
  private record Pending(List<String> segments, Condition condition) {

    Pending rest(int from) {
      return new Pending(segments.subList(from, segments.size()), condition);
    }
  }

  /**
   * Whether every condition holds at this node. Conditions without {@code []} are walked directly;
   * the rest are bucketed by their path up to the first {@code []}, and each bucket needs one
   * element that satisfies all of it.
   */
  private static boolean allHold(List<Pending> conditions, JsonNode node) {
    Map<String, List<Pending>> buckets = new LinkedHashMap<>();
    Map<String, List<String>> bucketPaths = new LinkedHashMap<>();
    for (Pending p : conditions) {
      int array = firstArraySegment(p.segments());
      if (array < 0) {
        if (!holds(p.condition(), walk(node, p.segments()))) {
          return false;
        }
        continue;
      }
      List<String> head = p.segments().subList(0, array + 1);
      String key = String.join(".", head);
      buckets.computeIfAbsent(key, k -> new ArrayList<>()).add(p.rest(array + 1));
      bucketPaths.putIfAbsent(key, head);
    }
    for (Map.Entry<String, List<Pending>> bucket : buckets.entrySet()) {
      if (!someElementHolds(node, bucketPaths.get(bucket.getKey()), bucket.getValue())) {
        return false;
      }
    }
    return true;
  }

  private static boolean someElementHolds(JsonNode node, List<String> head, List<Pending> rest) {
    List<String> names = new ArrayList<>(head.size());
    for (String segment : head) {
      names.add(fieldName(segment));
    }
    JsonNode array = walk(node, names);
    if (array == null || !array.isArray()) {
      return false;
    }
    for (JsonNode element : array) {
      if (allHold(rest, element)) {
        return true;
      }
    }
    return false;
  }

  private static int firstArraySegment(List<String> segments) {
    for (int i = 0; i < segments.size(); i++) {
      if (segments.get(i).endsWith("[]")) {
        return i;
      }
    }
    return -1;
  }

  private static String fieldName(String segment) {
    return segment.endsWith("[]") ? segment.substring(0, segment.length() - 2) : segment;
  }

  /** Walks object fields. Anything that is not an object on the way, or JSON null at the end, is absent. */
  private static JsonNode walk(JsonNode node, List<String> fields) {
    JsonNode current = node;
    for (String field : fields) {
      if (current == null || !current.isObject()) {
        return null;
      }
      current = current.get(field);
    }
    return current == null || current.isNull() ? null : current;
  }

  private static boolean holds(Condition condition, JsonNode at) {
    return switch (condition.op()) {
      case EXISTS -> at != null;
      case ABSENT -> at == null;
      case EXACT -> at != null && asString(at).equals(condition.value());
      case PREFIX -> at != null && asString(at).startsWith(condition.value());
    };
  }

  /** A JSON string is its text; anything else is its JSON literal. */
  static String asString(JsonNode node) {
    return node.isTextual() ? node.textValue() : node.toString();
  }
}
