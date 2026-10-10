package eu.wohlben.qits.eventstream.selection;

import eu.wohlben.qits.eventstream.selection.EventSelection.Condition;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

/**
 * The compact text form of a selection, as a CLI takes it: one string per group, conditions
 * separated by whitespace. The same syntax as {@code qits observe --filter}.
 *
 * <pre>
 *   path=value     the value equals value         status=VERIFIED
 *   path^=value    the value starts with value    version^=2026.
 *   path?          the path is present            releaseRequestId?
 *   !path          the path is absent             !previousStatus
 * </pre>
 *
 * <p>A value may be double-quoted, to hold spaces or to be empty: {@code title="two words"}, {@code
 * note=""}. Inside quotes, {@code \"} and {@code \\} are a quote and a backslash. A path is field
 * names joined by dots, with {@code []} after a field holding a list: {@code entities[].status}. See
 * {@link EventSelectionEvaluator} for what a path means.
 *
 * <p>Errors are {@link EventSelectionException}s naming the token and saying what to write.
 */
public final class EventSelectionGrammar {

  private static final String OPERATOR_CHARACTERS = "=^?!\"";
  private static final String FORMS = "path=value, path^=value, path? or !path";

  private EventSelectionGrammar() {}

  /** A selection from an event name and its groups, one string each. No groups: every event of the name. */
  public static EventSelection parse(String event, List<String> groups) {
    List<List<Condition>> when = new ArrayList<>();
    if (groups != null) {
      for (String group : groups) {
        when.add(parseGroup(group));
      }
    }
    return new EventSelection(event, when);
  }

  /** One group: its conditions, ANDed. */
  public static List<Condition> parseGroup(String text) {
    List<String> tokens = tokens(text == null ? "" : text);
    if (tokens.isEmpty()) {
      throw new EventSelectionException(
          "a group is empty; give conditions such as 'status=VERIFIED', or no group to match every event.");
    }
    if (tokens.size() > EventSelection.MAX_CONDITIONS) {
      throw new EventSelectionException(
          "'" + text.strip() + "' has " + tokens.size() + " conditions; the most is " + EventSelection.MAX_CONDITIONS + ".");
    }
    List<Condition> conditions = new ArrayList<>(tokens.size());
    for (String token : tokens) {
      conditions.add(condition(token));
    }
    return List.copyOf(conditions);
  }

  /** One group in this syntax. {@link #parseGroup} reads it back to the same conditions. */
  public static String format(List<Condition> group) {
    StringJoiner text = new StringJoiner(" ");
    for (Condition c : group) {
      text.add(
          switch (c.op()) {
            case EXACT -> c.path() + "=" + quoteIfNeeded(c.value());
            case PREFIX -> c.path() + "^=" + quoteIfNeeded(c.value());
            case EXISTS -> c.path() + "?";
            case ABSENT -> "!" + c.path();
          });
    }
    return text.toString();
  }

  /**
   * A one-line reading of a whole selection, for a label or a log: {@code
   * EntityTransitioned when entities[].status=VERIFIED or …}.
   */
  public static String describe(EventSelection selection) {
    if (selection.isUnconditional()) {
      return selection.event();
    }
    StringJoiner groups = new StringJoiner(" or ", selection.event() + " when ", "");
    for (List<Condition> group : selection.when()) {
      groups.add(selection.when().size() > 1 ? "(" + format(group) + ")" : format(group));
    }
    return groups.toString();
  }

  /** Splits on whitespace outside double quotes. The quotes stay; {@link #value} reads them. */
  static List<String> tokens(String text) {
    List<String> tokens = new ArrayList<>();
    StringBuilder current = new StringBuilder();
    boolean inToken = false;
    boolean inQuotes = false;
    for (int i = 0; i < text.length(); i++) {
      char c = text.charAt(i);
      if (inQuotes) {
        current.append(c);
        if (c == '\\' && i + 1 < text.length()) {
          current.append(text.charAt(++i));
        } else if (c == '"') {
          inQuotes = false;
        }
        continue;
      }
      if (Character.isWhitespace(c)) {
        if (inToken) {
          tokens.add(current.toString());
          current.setLength(0);
          inToken = false;
        }
        continue;
      }
      inToken = true;
      current.append(c);
      inQuotes = c == '"';
    }
    if (inQuotes) {
      throw usage(current.toString(), "has a quote that is not closed.");
    }
    if (inToken) {
      tokens.add(current.toString());
    }
    return tokens;
  }

  static Condition condition(String token) {
    if (token.startsWith("!")) {
      String path = token.substring(1);
      if (path.isEmpty()) {
        throw usage(token, "names no path. !path asks that the path is absent.");
      }
      if (containsAny(path, OPERATOR_CHARACTERS)) {
        throw usage(token, "is not a condition. !path takes a path alone, and asks that it is absent.");
      }
      return checked(token, path, EventSelection.Op.ABSENT, null);
    }
    int end = 0;
    while (end < token.length() && OPERATOR_CHARACTERS.indexOf(token.charAt(end)) < 0) {
      end++;
    }
    String path = token.substring(0, end);
    String rest = token.substring(end);
    if (path.isEmpty()) {
      throw usage(token, "names no path. Write " + FORMS + ".");
    }
    if (rest.isEmpty()) {
      throw usage(token, "has no operator. Write " + FORMS + ".");
    }
    if (rest.startsWith("?")) {
      if (rest.length() > 1) {
        throw usage(token, "has text after the ?. path? stands alone: it asks that the path is present.");
      }
      return checked(token, path, EventSelection.Op.EXISTS, null);
    }
    EventSelection.Op op;
    String raw;
    if (rest.startsWith("^=")) {
      op = EventSelection.Op.PREFIX;
      raw = rest.substring(2);
    } else if (rest.startsWith("=") && !rest.startsWith("==")) {
      op = EventSelection.Op.EXACT;
      raw = rest.substring(1);
    } else {
      throw usage(token, "has an operator this grammar does not know. Write " + FORMS + ".");
    }
    if (raw.isEmpty()) {
      throw usage(token, "has an empty value. To match an empty string, write " + path + "=\"\".");
    }
    return checked(token, path, op, value(token, raw));
  }

  private static Condition checked(String token, String path, EventSelection.Op op, String value) {
    try {
      return new Condition(path, op, value);
    } catch (EventSelectionException invalid) {
      throw usage(token, "is not valid: " + invalid.getMessage());
    }
  }

  /** The value after the operator: as written, or the inside of a double-quoted value. */
  private static String value(String token, String raw) {
    if (!raw.startsWith("\"")) {
      if (raw.indexOf('"') >= 0) {
        throw usage(token, "has a quote inside the value. Quote the whole value: title=\"two words\".");
      }
      return raw;
    }
    StringBuilder value = new StringBuilder();
    int i = 1;
    for (; i < raw.length(); i++) {
      char c = raw.charAt(i);
      if (c == '\\' && i + 1 < raw.length() && (raw.charAt(i + 1) == '"' || raw.charAt(i + 1) == '\\')) {
        value.append(raw.charAt(++i));
      } else if (c == '"') {
        break;
      } else {
        value.append(c);
      }
    }
    if (i >= raw.length()) {
      throw usage(token, "has a quote that is not closed.");
    }
    if (i != raw.length() - 1) {
      throw usage(token, "has text after the closing quote.");
    }
    return value.toString();
  }

  private static String quoteIfNeeded(String value) {
    boolean plain = !value.isEmpty();
    for (int i = 0; plain && i < value.length(); i++) {
      char c = value.charAt(i);
      plain = !Character.isWhitespace(c) && c != '"';
    }
    if (plain) {
      return value;
    }
    return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
  }

  private static boolean containsAny(String text, String characters) {
    for (int i = 0; i < text.length(); i++) {
      if (characters.indexOf(text.charAt(i)) >= 0) {
        return true;
      }
    }
    return false;
  }

  private static EventSelectionException usage(String token, String reason) {
    return new EventSelectionException("'" + token + "' " + reason);
  }
}
