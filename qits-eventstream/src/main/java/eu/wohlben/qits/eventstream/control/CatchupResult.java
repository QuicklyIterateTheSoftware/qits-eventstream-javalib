package eu.wohlben.qits.eventstream.control;

/**
 * The outcome of catching one durable consumer up against qits-events' log.
 *
 * <p>{@link #reachedLogHead()} is deliberately stronger than "the call did not throw" or "no
 * event was handled": it is true only after qits-events explicitly marked the final page with a
 * null {@code nextCursor}. A projection may therefore use it as its bootstrap gate and keep its
 * externally visible state dark while the event log is unavailable or a handler remains owed.
 *
 * <p>The result belongs to one call. A live event may arrive immediately afterwards, so it is a
 * certificate that the consumer had reached the log head at that instant, not a claim that it will
 * remain there forever. The live subscription and subsequent scheduled sweeps cover what follows.
 */
public record CatchupResult(String consumerId, Status status, int handled) {

  /** How this attempt ended. Only {@link #REACHED_HEAD} is a successful bootstrap. */
  public enum Status {
    /** Every page was processed and qits-events explicitly said the last one was the final page. */
    REACHED_HEAD,
    /** qits-events could not be read; no conclusion about the listener's position is possible. */
    UNAVAILABLE,
    /** A listener, its configuration, or the catch-up work itself failed. */
    FAILED,
    /** The listener is currently not eligible to establish a complete position. */
    INCOMPLETE
  }

  /** Whether this call successfully reached the log head. */
  public boolean reachedLogHead() {
    return status == Status.REACHED_HEAD;
  }
}
