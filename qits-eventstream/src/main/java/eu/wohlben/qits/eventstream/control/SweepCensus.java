package eu.wohlben.qits.eventstream.control;

import java.time.Duration;
import java.time.Instant;

/**
 * What {@link CatchupSweeper} is doing right now, and when it last finished doing it.
 *
 * <p><b>This record exists because liveness had to stop being an inference.</b> On 2026-09-08 a
 * qits-deployments self-deploy left the startup sweep parked in a PostgreSQL socket read — a
 * half-open connection from the blue-green cutover overlap — while {@code catchUp()} was {@code
 * synchronized} and the scheduled tick ran {@code SKIP}. One thread held the monitor from 00:11 to
 * 06:30. Nothing consumed a single event in those six hours, every release of the window silently
 * failed to deploy, and every health check was green, because the only thing anybody was asking was
 * "is the process up". It was. A process is up long after it has stopped being a consumer.
 *
 * <p>So the sweeper now states the fact instead of leaving it to be deduced: <b>whether a sweep is
 * in flight, which thread holds it, how long it has held it, and when a whole sweep last
 * completed.</b> {@link CatchupHealthCheck} turns exactly this into a readiness answer, and nothing
 * else in the module reads it — the record is deliberately pure data, with no behaviour beyond
 * arithmetic over its own components, so that a reader can be certain looking at it costs nothing
 * and changes nothing.
 *
 * <p>{@link #now} is carried rather than read, because every derived answer here is a comparison
 * against it and two of them read from different clocks would contradict each other inside one
 * census. It is the module's injectable {@code Clock}, which is what lets a suite move it.
 *
 * @param enabled whether {@code qits.eventstream.enabled} is on at all
 * @param durableListeners how many durable listeners this application registered
 * @param now the instant every duration below is measured against
 * @param what the in-flight sweep's own description, null when nothing is in flight
 * @param thread the holder thread's NAME — the name is the whole point, since it is what a stack in
 *     an incident log is identified by — null when nothing is in flight
 * @param startedAt when the in-flight sweep took the lock, null when nothing is in flight
 * @param lastCompletedAt when a FULL sweep last finished all of its listeners, null if none has
 *     since this process started
 * @param observingSince when this process started counting, which is the startup grace anchor
 * @param stallBudget how long a sweep may hold the lock before the watchdog calls it stalled
 * @param stalenessHorizon how long the module may go without a completed sweep before that itself
 *     is the fault
 */
public record SweepCensus(
    boolean enabled,
    int durableListeners,
    Instant now,
    String what,
    String thread,
    Instant startedAt,
    Instant lastCompletedAt,
    Instant observingSince,
    Duration stallBudget,
    Duration stalenessHorizon) {

  /** Whether a sweep holds the lock at this instant. */
  public boolean sweeping() {
    return startedAt != null;
  }

  /**
   * <b>The incident, stated.</b> A sweep is in flight and has held the lock for longer than the
   * budget — which on 2026-09-08 stayed true for six hours while nothing else changed.
   *
   * <p>Measured from {@code startedAt} and not from the watchdog's last strike on purpose: the
   * strike cadence is a log-volume decision, and a holder that was interrupted and did not move is
   * more stalled than it was before, not less.
   */
  public boolean stalled() {
    return startedAt != null && !now.isBefore(startedAt.plus(stallBudget));
  }

  /** How long the in-flight sweep has held the lock; zero when nothing is in flight. */
  public Duration heldFor() {
    return startedAt == null ? Duration.ZERO : Duration.between(startedAt, now);
  }

  /**
   * How long since a whole sweep last finished — <b>or, when none ever has, how long this process
   * has been observing.</b> That second clause is the boot grace and it is the reason the field is
   * carried at all: a service starting into a real backlog has completed nothing yet and is not
   * therefore broken, it is working. It gets a whole horizon to finish its first sweep.
   */
  public Duration sinceLastCompletedSweep() {
    return Duration.between(lastCompletedAt == null ? observingSince : lastCompletedAt, now);
  }

  /**
   * Nothing has completed within the horizon. The complement of {@link #stalled()}: that one names
   * a holder, this one names the absence of progress with nobody to blame for it — a tick that
   * wedged on a platform thread and was never freed by its interrupt leaves exactly this shape.
   */
  public boolean stale() {
    return sinceLastCompletedSweep().compareTo(stalenessHorizon) > 0;
  }
}
