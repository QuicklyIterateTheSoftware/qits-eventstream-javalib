package eu.wohlben.qits.eventstream.deployment;

import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.Map;

/** Reads a failed augmentation for the refusal this module raises, wherever ArC nested it. */
final class BuildFailures {

  /**
   * ArC reports validation problems as one {@code DeploymentException}, with each problem as a
   * cause or a suppressed exception depending on how many there were — so the whole graph is
   * searched rather than one shape of it assumed.
   */
  static void assertNames(Throwable thrown, Class<?> event, String member) {
    Deque<Throwable> pending = new ArrayDeque<>();
    Map<Throwable, Boolean> seen = new IdentityHashMap<>();
    pending.push(thrown);
    StringBuilder all = new StringBuilder();
    while (!pending.isEmpty()) {
      Throwable next = pending.pop();
      if (next == null || seen.put(next, Boolean.TRUE) != null) {
        continue;
      }
      String message = String.valueOf(next.getMessage());
      all.append(next.getClass().getName()).append(": ").append(message).append('\n');
      if (message.contains(event.getName()) && message.contains(member)) {
        return;
      }
      pending.push(next.getCause());
      for (Throwable suppressed : next.getSuppressed()) {
        pending.push(suppressed);
      }
    }
    fail("the build failed, but not naming " + event.getName() + " and " + member + ":\n" + all);
  }

  private BuildFailures() {}
}
