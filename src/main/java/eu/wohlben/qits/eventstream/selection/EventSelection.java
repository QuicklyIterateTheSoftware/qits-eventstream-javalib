package eu.wohlben.qits.eventstream.selection;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Which events of one name a consumer wants: the event name, matched exactly, and a {@code when}
 * over the payload. Groups are ORed; the conditions in a group are ANDed. An empty {@code when}
 * matches every event of the name.
 *
 * <p>Lifted from qits-ci's {@code when:} selection, without its repository aliasing, plus "any array
 * element" paths. {@link EventSelectionEvaluator} decides a match; {@link EventSelectionGrammar} reads
 * the compact text form. The JSON form is this record:
 *
 * <pre>{@code
 * {"event":"EntityTransitioned",
 *  "when":[[{"path":"entities[].number","op":"exact","value":"qits-123"},
 *           {"path":"entities[].status","op":"exact","value":"VERIFIED"}]]}
 * }</pre>
 *
 * <p>A selection is valid once constructed: the constructor checks every part and the caps, and
 * throws {@link EventSelectionException} with a message for the caller. So a selection read from
 * JSON is checked too.
 *
 * <p>Plain Jackson, no framework. A consumer that builds a native image registers {@code
 * EventSelection}, {@code EventSelection$Condition} and {@code EventSelection$Op} for reflection,
 * like its other wire types.
 */
@JsonPropertyOrder({"event", "when"})
public record EventSelection(String event, List<List<Condition>> when) {

  /** At most this many groups in one selection. */
  public static final int MAX_GROUPS = 8;

  /** At most this many conditions in one group. */
  public static final int MAX_CONDITIONS = 8;

  /** At most this many dot-separated segments in one path. */
  public static final int MAX_PATH_SEGMENTS = 8;

  /** At most this many characters in one path. */
  public static final int MAX_PATH_LENGTH = 256;

  /** At most this many characters in one compared value. */
  public static final int MAX_VALUE_LENGTH = 1024;

  /** At most this many characters in an event name. */
  public static final int MAX_EVENT_LENGTH = 128;

  private static final Pattern EVENT_NAME = Pattern.compile("[A-Za-z][A-Za-z0-9_.-]*");

  /** One path segment: an object key, and {@code []} when the value there is an array to search. */
  static final Pattern SEGMENT = Pattern.compile("([A-Za-z_][A-Za-z0-9_-]*)(\\[])?");

  private static final ObjectMapper JSON = new ObjectMapper();

  public EventSelection {
    event = requireEvent(event);
    if (when == null) {
      when = List.of();
    }
    if (when.size() > MAX_GROUPS) {
      throw new EventSelectionException(
          "the selection has " + when.size() + " groups; the most is " + MAX_GROUPS + ".");
    }
    List<List<Condition>> groups = new ArrayList<>(when.size());
    for (int i = 0; i < when.size(); i++) {
      groups.add(requireGroup(when.get(i), i));
    }
    when = List.copyOf(groups);
  }

  /** Every event of this name, with no condition on the payload. */
  public static EventSelection of(String event) {
    return new EventSelection(event, List.of());
  }

  /** This event name, and one group of conditions. */
  public static EventSelection of(String event, Condition... group) {
    return new EventSelection(event, List.of(List.of(group)));
  }

  /** True when there are no groups: every event of the name matches. */
  @JsonIgnore
  public boolean isUnconditional() {
    return when.isEmpty();
  }

  /** The JSON form. */
  public String toJson() {
    try {
      return JSON.writeValueAsString(this);
    } catch (JsonProcessingException impossible) {
      throw new IllegalStateException("a selection always writes", impossible);
    }
  }

  /**
   * Reads the JSON form. A malformed document or an invalid selection throws {@link
   * EventSelectionException} with the reason, not Jackson's wrapper around it.
   */
  public static EventSelection fromJson(String json) {
    try {
      EventSelection selection = JSON.readValue(json, EventSelection.class);
      if (selection == null) {
        throw new EventSelectionException("the selection is empty; give {\"event\": …}.");
      }
      return selection;
    } catch (JsonProcessingException e) {
      for (Throwable cause = e; cause != null; cause = cause.getCause()) {
        if (cause instanceof EventSelectionException invalid) {
          throw invalid;
        }
      }
      throw new EventSelectionException("the selection is not valid JSON: " + e.getOriginalMessage(), e);
    }
  }

  /** The operators. Values compare as strings; see {@link EventSelectionEvaluator}. */
  public enum Op {
    /** The value at the path equals the given string. */
    EXACT("exact"),
    /** The value at the path starts with the given string. */
    PREFIX("prefix"),
    /** The path resolves to a value that is not JSON null. */
    EXISTS("exists"),
    /** The path does not resolve, or resolves to JSON null. Written as {@code exists: false}. */
    ABSENT("absent");

    private final String wire;

    Op(String wire) {
      this.wire = wire;
    }

    @JsonValue
    public String wire() {
      return wire;
    }

    @JsonCreator
    public static Op of(String text) {
      if (text != null) {
        String lower = text.toLowerCase(Locale.ROOT);
        for (Op op : values()) {
          if (op.wire.equals(lower)) {
            return op;
          }
        }
      }
      throw new EventSelectionException(
          "'" + text + "' is not an operator; use exact, prefix or exists.");
    }
  }

  /**
   * One condition on one path. {@code value} is set for {@link Op#EXACT} and {@link Op#PREFIX}, and
   * null otherwise.
   *
   * <p>On the wire, {@link Op#EXISTS} and {@link Op#ABSENT} are both {@code "op":"exists"}, with
   * {@code "value":true} or {@code false} — the qits-ci form. {@code "op":"absent"} is read too.
   */
  public record Condition(String path, Op op, String value) {

    public Condition {
      requirePath(path);
      if (op == null) {
        throw new EventSelectionException("'" + path + "' has no operator; use exact, prefix or exists.");
      }
      switch (op) {
        case EXACT -> requireValue(path, op, value, false);
        case PREFIX -> requireValue(path, op, value, true);
        case EXISTS, ABSENT -> {
          if (value != null) {
            throw new EventSelectionException(
                "'" + path + "' asks " + op.wire() + " and gives a value; " + op.wire() + " takes none.");
          }
        }
      }
    }

    public static Condition exact(String path, String value) {
      return new Condition(path, Op.EXACT, value);
    }

    public static Condition prefix(String path, String value) {
      return new Condition(path, Op.PREFIX, value);
    }

    public static Condition exists(String path) {
      return new Condition(path, Op.EXISTS, null);
    }

    public static Condition absent(String path) {
      return new Condition(path, Op.ABSENT, null);
    }

    /** Reads the wire form: {@code {path, op, value}}. */
    @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
    static Condition fromWire(JsonNode wire) {
      if (wire == null || !wire.isObject()) {
        throw new EventSelectionException(
            "a condition must be an object {path, op, value}, got: " + wire);
      }
      JsonNode pathNode = wire.get("path");
      String path = pathNode == null || pathNode.isNull() ? null : pathNode.asText();
      JsonNode opNode = wire.get("op");
      if (opNode == null || opNode.isNull()) {
        throw new EventSelectionException("'" + path + "' has no operator; use exact, prefix or exists.");
      }
      Op op = Op.of(opNode.asText());
      JsonNode value = wire.get("value");
      if (op == Op.EXISTS || op == Op.ABSENT) {
        if (value == null || value.isNull()) {
          return new Condition(path, op, null);
        }
        if (!value.isBoolean()) {
          throw new EventSelectionException(
              "'" + path + "' asks exists with value " + value + "; exists takes true or false.");
        }
        boolean present = value.booleanValue() == (op == Op.EXISTS);
        return new Condition(path, present ? Op.EXISTS : Op.ABSENT, null);
      }
      if (value != null && !value.isNull() && !value.isTextual()) {
        throw new EventSelectionException(
            "'" + path + "' compares with " + value + "; values are strings, so quote it: \"" + value.asText() + "\".");
      }
      return new Condition(path, op, value == null || value.isNull() ? null : value.textValue());
    }

    /**
     * Writes the wire form. {@link Op#EXISTS} and {@link Op#ABSENT} are both {@code "op":"exists"},
     * with {@code "value":true} or {@code false}.
     */
    @JsonValue
    Map<String, Object> toWire() {
      Map<String, Object> wire = new LinkedHashMap<>();
      wire.put("path", path);
      switch (op) {
        case EXACT, PREFIX -> {
          wire.put("op", op.wire());
          wire.put("value", value);
        }
        case EXISTS, ABSENT -> {
          wire.put("op", Op.EXISTS.wire());
          wire.put("value", op == Op.EXISTS);
        }
      }
      return wire;
    }
  }

  private static String requireEvent(String event) {
    if (event == null || event.isBlank()) {
      throw new EventSelectionException("the selection names no event; give the event's name, for example BuildStatusChanged.");
    }
    if (event.equals("*")) {
      throw new EventSelectionException("'*' is not an event name; a selection names one event.");
    }
    if (event.length() > MAX_EVENT_LENGTH || !EVENT_NAME.matcher(event).matches()) {
      throw new EventSelectionException(
          "'" + event + "' is not an event name; use letters, digits, '_', '-' or '.', at most " + MAX_EVENT_LENGTH + ".");
    }
    return event;
  }

  private static List<Condition> requireGroup(List<Condition> group, int index) {
    if (group == null || group.isEmpty()) {
      throw new EventSelectionException(
          "group " + (index + 1) + " is empty; it would match every event. Leave out 'when' for that.");
    }
    if (group.size() > MAX_CONDITIONS) {
      throw new EventSelectionException(
          "group " + (index + 1) + " has " + group.size() + " conditions; the most is " + MAX_CONDITIONS + ".");
    }
    for (Condition condition : group) {
      if (condition == null) {
        throw new EventSelectionException("group " + (index + 1) + " has an empty condition.");
      }
    }
    return List.copyOf(group);
  }

  /** Checks a path and returns its segments. */
  static List<String> requirePath(String path) {
    if (path == null || path.isEmpty()) {
      throw new EventSelectionException("a condition has an empty path; name a payload field, for example status.");
    }
    if (path.length() > MAX_PATH_LENGTH) {
      throw new EventSelectionException(
          "the path '" + path.substring(0, 32) + "…' is longer than " + MAX_PATH_LENGTH + " characters.");
    }
    List<String> segments = List.of(path.split("\\.", -1));
    if (segments.size() > MAX_PATH_SEGMENTS) {
      throw new EventSelectionException(
          "the path '" + path + "' has " + segments.size() + " segments; the most is " + MAX_PATH_SEGMENTS + ".");
    }
    for (String segment : segments) {
      if (!SEGMENT.matcher(segment).matches()) {
        throw new EventSelectionException(
            "'" + path + "' is not a payload path; write field names joined by dots, with [] after a"
                + " field that holds a list, for example entities[].status.");
      }
    }
    return segments;
  }

  private static void requireValue(String path, Op op, String value, boolean nonEmpty) {
    if (value == null) {
      throw new EventSelectionException("'" + path + "' asks " + op.wire() + " and gives no value.");
    }
    if (nonEmpty && value.isEmpty()) {
      throw new EventSelectionException(
          "'" + path + "' asks prefix with an empty value, which every value has; use exists.");
    }
    if (value.length() > MAX_VALUE_LENGTH) {
      throw new EventSelectionException(
          "the value for '" + path + "' is longer than " + MAX_VALUE_LENGTH + " characters.");
    }
  }
}
