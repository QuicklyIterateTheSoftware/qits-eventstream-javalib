package eu.wohlben.qits.eventstream;

import java.util.UUID;

/**
 * An entity whose rows record the event that caused them to exist: the persistence spelling of
 * {@link CausationScope}, the way {@link CausationHeader} is its wire spelling. A row written while
 * a cause is ambient carries that event's id, so tracing can walk from a table back into the chain
 * — "sourcing" a row to its event, without any of this being event sourcing.
 *
 * <p>Participation is three visible lines on the entity, and each is load-bearing:
 *
 * <pre>{@code
 * @Entity
 * @EntityListeners(CausationStamp.class)
 * public class WorkspaceRow extends PanacheEntity implements CausedRow {
 *
 *   @Column(name = "causation_id")
 *   public UUID causationId;
 *
 *   @Override public UUID causationId() { return causationId; }
 *   @Override public void causationId(UUID id) { this.causationId = id; }
 * }
 * }</pre>
 *
 * <p><b>An interface rather than a mapped superclass</b>, because entities have spent their single
 * inheritance already — on {@code PanacheEntity}, or on a base of their own — and a mechanism that
 * dictates the parent class dictates too much. The entity keeps its field, its column name and its
 * migration; this interface is only how {@link CausationStamp} reaches them without reflection,
 * which is what keeps the stamping visible in a native image's build rather than failing quietly
 * inside it.
 *
 * <p><b>The column is advisory and can never be a foreign key.</b> The event it names lives in
 * qits-events' store, another service's database — the same reason the envelope's {@code parentId}
 * is a bare UUID. Nullable, unindexed unless a service's own tracing queries want one.
 *
 * <p>The consuming service owns the migration that adds the column, exactly as it owns its table.
 * Nothing warns about an entity that forgot to participate; that completeness check is the
 * qits-arch-rules suite's job, where forgetting fails a build instead of a trace.
 */
public interface CausedRow {

  /** The id of the event this row was written because of, or {@code null} for a rootless row. */
  UUID causationId();

  void causationId(UUID id);
}
