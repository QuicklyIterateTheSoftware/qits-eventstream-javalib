package eu.wohlben.qits.eventstream.selection;

/**
 * A selection that is not valid: a bad event name, path, operator or value, or one over a cap. The
 * message names the part that is wrong and says what to write instead, so a door or a CLI can hand
 * it to the caller unchanged.
 */
public class EventSelectionException extends IllegalArgumentException {

  public EventSelectionException(String message) {
    super(message);
  }

  public EventSelectionException(String message, Throwable cause) {
    super(message, cause);
  }
}
