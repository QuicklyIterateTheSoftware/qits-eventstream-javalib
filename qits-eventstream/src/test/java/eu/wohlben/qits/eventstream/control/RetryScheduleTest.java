package eu.wohlben.qits.eventstream.control;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The arithmetic on its own, so the spacing is asserted somewhere no clock, database or socket can
 * be blamed for it. {@link OutboxFlowTest} proves the sweeper follows this; this proves what "this"
 * is.
 */
class RetryScheduleTest {

  @Test
  void theOutboxSpacingIsOneSecondTimesFourToTheAttempt() {
    assertEquals(
        List.of(
            Duration.ofSeconds(1),
            Duration.ofSeconds(4),
            Duration.ofSeconds(16),
            Duration.ofSeconds(64),
            Duration.ofSeconds(256)),
        List.of(
            RetrySchedule.outboxBackoff(1),
            RetrySchedule.outboxBackoff(2),
            RetrySchedule.outboxBackoff(3),
            RetrySchedule.outboxBackoff(4),
            RetrySchedule.outboxBackoff(5)));
  }

  @Test
  void theOutboxSpacingIsCappedAtFiveMinutes() {
    // Where a row waiting on an unreachable bus lives: those attempts spend no budget, so the curve
    // is walked out and then this is the spacing for as long as the outage lasts.
    assertEquals(Duration.ofMinutes(5), RetrySchedule.outboxBackoff(6));
    assertEquals(Duration.ofMinutes(5), RetrySchedule.outboxBackoff(60));
  }

  @Test
  void theRedialSpacingDoublesFromTheInitialValueAndIsCapped() {
    Duration initial = Duration.ofSeconds(1);
    Duration max = Duration.ofSeconds(30);

    assertEquals(
        List.of(
            Duration.ofSeconds(1),
            Duration.ofSeconds(2),
            Duration.ofSeconds(4),
            Duration.ofSeconds(8),
            Duration.ofSeconds(16),
            Duration.ofSeconds(30),
            Duration.ofSeconds(30)),
        List.of(
            RetrySchedule.redialBackoff(0, initial, max),
            RetrySchedule.redialBackoff(1, initial, max),
            RetrySchedule.redialBackoff(2, initial, max),
            RetrySchedule.redialBackoff(3, initial, max),
            RetrySchedule.redialBackoff(4, initial, max),
            RetrySchedule.redialBackoff(5, initial, max),
            RetrySchedule.redialBackoff(50, initial, max)));
  }
}
