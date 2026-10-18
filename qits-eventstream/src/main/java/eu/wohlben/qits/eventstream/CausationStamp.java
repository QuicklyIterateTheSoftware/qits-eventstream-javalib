package eu.wohlben.qits.eventstream;

import jakarta.persistence.PrePersist;

/**
 * The JPA entity listener that fills {@link CausedRow#causationId(java.util.UUID)} from {@link
 * CausationScope} when a row is persisted. Attached per entity with {@code
 * @EntityListeners(CausationStamp.class)}; the pair of that annotation and the interface is the
 * whole participation.
 *
 * <p><b>{@code @PrePersist} fires when {@code persist()} is called, on the calling thread</b> — not
 * later at flush — which is the one property that makes a ThreadLocal source safe here. The scope a
 * request filter or a dispatcher established is still standing when the stamp reads it, even when
 * the flush and commit happen after the scope has closed. A {@code persist} handed to another
 * thread needs the same explicit bridge as any other hand-off; the rule is {@link CausationScope}'s
 * and this class adds no second one.
 *
 * <p><b>A value the author set wins</b>, mirroring {@code publish(event, parentEventId)}: the stamp
 * fills only a null. And it is <b>insert-only on purpose</b> — no {@code @PreUpdate} — because the
 * column answers "which event caused this row to exist", which is creation history; a re-stamp on
 * every update would overwrite the one fact the column holds.
 *
 * <p>Outside any scope the stamp writes nothing and the column stays null: a rootless row, exactly
 * as an event published outside any scope is a chain root.
 */
public class CausationStamp {

  @PrePersist
  void stamp(Object entity) {
    if (entity instanceof CausedRow row && row.causationId() == null) {
      row.causationId(CausationScope.current());
    }
  }
}
