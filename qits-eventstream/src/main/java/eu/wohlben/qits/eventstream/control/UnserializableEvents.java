package eu.wohlben.qits.eventstream.control;

import jakarta.enterprise.context.ApplicationScoped;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The events whose <b>latest attempt in this process</b> could not be serialised (qits-2055), and
 * why — the fact {@link PublishHealthCheck} turns into a DOWN.
 *
 * <p>Written by {@link EventsPublisher} on every attempt, inline and from the outbox alike, because
 * that is the one place both paths pass through: {@link EventsPublisher.Outcome#UNSERIALIZABLE}
 * records the event, any other outcome forgets it. Any other outcome, not only a delivery — a
 * refusal or a silence still means the body <em>was</em> built, so the packaging fault this map
 * exists to report is no longer true of that event, and the outbox's own counters own the rest.
 *
 * <p><b>In memory, and deliberately not a column.</b> The question is "can <em>this</em> binary
 * serialise what it publishes", which a restart into a fixed binary answers afresh: the row is still
 * {@code PENDING} in the outbox, the next sweep re-attempts it, and that attempt either puts it back
 * here or clears it. A persisted flag would hold a fixed deployment DOWN over a fault the previous
 * one had — and it would need a migration for a fact no other process reads.
 *
 * <p>Bounded by the outbox, not by a cap: every entry is an event the outbox still owes, and the
 * sweeper attempts at most one batch a tick. A process that fails to serialise everything holds one
 * entry per owed event, which is exactly the size of what it is failing to deliver.
 */
@ApplicationScoped
public class UnserializableEvents {

  /** One event that could not be serialised: its wire name and the serialiser's own account. */
  public record Failure(String name, String detail) {}

  private final Map<String, Failure> failures = new ConcurrentHashMap<>();

  /** The latest attempt at {@code eventId} could not build its body. */
  void failed(String eventId, String name, String detail) {
    failures.put(eventId, new Failure(name, detail));
  }

  /** The latest attempt at {@code eventId} built its body, whatever became of the request. */
  void cleared(String eventId) {
    failures.remove(eventId);
  }

  /** A copy of what is failing now, by event id. Empty is the healthy answer. */
  public Map<String, Failure> current() {
    return Map.copyOf(failures);
  }
}
