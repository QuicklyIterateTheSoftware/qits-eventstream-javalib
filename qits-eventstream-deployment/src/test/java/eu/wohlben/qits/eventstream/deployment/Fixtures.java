package eu.wohlben.qits.eventstream.deployment;

import eu.wohlben.qits.eventstream.QitsEvent;
import java.time.Instant;
import java.util.UUID;

/**
 * Event classes for the build step's tests: the shapes a consumer writes, legal and not.
 *
 * <p>None of these is ever part of an application this module boots. {@link
 * EventstreamProcessorTest} indexes them by hand, and each {@code QuarkusUnitTest} here puts
 * exactly one into its throwaway application — a module's test classes are not that application's
 * index, so the illegal ones cannot fail a build they were not added to.
 */
final class Fixtures {

  /** The ordinary case: its own fields, its name the class name. */
  record ThingShipped(UUID eventId, Instant occurredAt, String what) implements QitsEvent {}

  /** The measured trap: {@code name} is a perfectly natural field, and it renames the event. */
  record ThingNamed(UUID eventId, Instant occurredAt, String name) implements QitsEvent {}

  /** The same trap one step earlier: {@code name()} defaults to {@code signature()}. */
  record ThingSigned(UUID eventId, Instant occurredAt, String signature) implements QitsEvent {}

  /** The documented override, which stays legal: a method, kept in step by its author. */
  record ThingOverridden(UUID eventId, Instant occurredAt) implements QitsEvent {
    @Override
    public String signature() {
      return "ThingOverridden";
    }
  }

  /** Not a record, and still a rename: any declared {@code name()} overrides the interface's. */
  static final class ThingRelabelled implements QitsEvent {
    @Override
    public UUID eventId() {
      return UUID.fromString("00000000-0000-4000-8000-000000000001");
    }

    @Override
    public Instant occurredAt() {
      return Instant.EPOCH;
    }

    @Override
    public String name() {
      return "Relabelled";
    }
  }

  /** Reached through a sub-interface and an abstract base — still an implementor to register. */
  interface AuditEvent extends QitsEvent {}

  abstract static class AbstractAudit implements AuditEvent {}

  static final class Audited extends AbstractAudit {
    @Override
    public UUID eventId() {
      return UUID.fromString("00000000-0000-4000-8000-000000000002");
    }

    @Override
    public Instant occurredAt() {
      return Instant.EPOCH;
    }
  }

  private Fixtures() {}
}
