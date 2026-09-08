package eu.wohlben.qits.eventstream.control;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import org.eclipse.microprofile.health.HealthCheck;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.HealthCheckResponseBuilder;
import org.eclipse.microprofile.health.Readiness;

/**
 * Says out loud whether this process is still consuming events.
 *
 * <p><b>The check that did not exist on 2026-09-08 is the whole reason this one does.</b> A
 * qits-deployments self-deploy left the catch-up sweep parked in a PostgreSQL socket read at
 * 00:11 and nothing consumed an event until a restart at 06:30. Every health check was green for
 * all six hours, because every health check was answering a different question — is the HTTP
 * server up, is the datasource reachable — and none of them was answering this one. A consumer that
 * has stopped consuming is not a degraded consumer, it is a silent one, and silence is what took
 * six hours to notice.
 *
 * <p><b>{@code @Readiness} and deliberately not {@code @Liveness}.</b> Readiness takes the instance
 * out of rotation, which is the honest statement this can make: <em>this process is not
 * consuming</em>. Liveness asks for a restart, and a library that shipped {@code @Liveness} would be
 * a library that can restart its consumers' processes — an authority a jar on somebody else's
 * classpath should not hold, and one this jar does not need, because the in-process cure is the
 * watchdog's interrupt (see {@link CatchupSweeper}). A consumer that has decided a stalled sweep
 * should cost it a restart wires this same fact into its own liveness check, where that decision
 * belongs.
 *
 * <p>The rules, in the order they are asked:
 *
 * <ol>
 *   <li><b>Nothing to do is UP.</b> The module dark, or no durable listener registered at all: a
 *       library must never make a consumer red over a feature that consumer does not use, and most
 *       consumers of this jar publish and never consume durably.
 *   <li><b>A stalled sweep is DOWN.</b> This is the incident, exactly.
 *   <li><b>A sweep in flight and inside its budget is UP.</b> A consumer draining a day of backlog
 *       holds the lock for as long as that takes and is alive by definition while it does.
 *   <li><b>No completed sweep within the horizon is DOWN.</b> The complement of the second rule: a
 *       tick that wedged on a platform thread is not freed by its interrupt, so the absence of
 *       progress has to be a fault in its own right and not only a named holder. With nothing
 *       completed yet the horizon runs from {@code observingSince}, which is the boot grace — a
 *       service starting into a real backlog is UP until it has had a whole horizon to finish its
 *       first sweep.
 *   <li>Otherwise UP.
 * </ol>
 */
@Readiness
@ApplicationScoped
public class CatchupHealthCheck implements HealthCheck {

  /** The name this appears under in {@code /q/health/ready}. */
  static final String NAME = "eventstream-catchup";

  @Inject CatchupSweeper sweeper;

  @Override
  public HealthCheckResponse call() {
    SweepCensus census = sweeper.census();
    if (!census.enabled() || census.durableListeners() == 0) {
      return HealthCheckResponse.named(NAME)
          .up()
          .withData("enabled", census.enabled())
          .withData("durableListeners", census.durableListeners())
          .withData("applies", "no: this application consumes no events durably")
          .build();
    }

    HealthCheckResponseBuilder response =
        HealthCheckResponse.named(NAME)
            .withData("enabled", true)
            .withData("durableListeners", census.durableListeners())
            .withData(
                "lastCompletedSweep",
                census.lastCompletedAt() == null ? "never" : census.lastCompletedAt().toString())
            .withData("sinceLastCompletedSweep", census.sinceLastCompletedSweep().toString())
            .withData("stallBudget", census.stallBudget().toString())
            .withData("stalenessHorizon", census.stalenessHorizon().toString());
    if (census.sweeping()) {
      response
          .withData("sweeping", census.what())
          .withData("sweepThread", census.thread())
          .withData("heldFor", census.heldFor().toString());
    }

    if (census.stalled()) {
      return response
          .down()
          .withData(
              "reason",
              "a sweep has held the catch-up lock for "
                  + census.heldFor()
                  + ", past its stall budget; this process is not consuming")
          .build();
    }
    if (census.sweeping()) {
      return response.up().build();
    }
    if (census.stale()) {
      return response
          .down()
          .withData(
              "reason",
              "no sweep has completed within the horizon: "
                  + since(census)
                  + " ago, against a horizon of "
                  + census.stalenessHorizon())
          .build();
    }
    return response.up().build();
  }

  /** How long ago the last full sweep was, in the words the DOWN reason reads best in. */
  private static String since(SweepCensus census) {
    Duration ago = census.sinceLastCompletedSweep();
    return census.lastCompletedAt() == null
        ? ago + " (this process has completed none)"
        : ago.toString();
  }
}
