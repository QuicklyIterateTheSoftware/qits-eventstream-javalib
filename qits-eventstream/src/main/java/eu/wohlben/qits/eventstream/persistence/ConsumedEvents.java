package eu.wohlben.qits.eventstream.persistence;

import io.quarkus.hibernate.orm.PersistenceUnit;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import java.time.Instant;

/**
 * The claim ledger: which events one durable listener has already handled.
 *
 * <p><b>No entity, and that is deliberate.</b> Nothing here loads an object — the table is written
 * with one conditional insert, read with one existence question and emptied in bulk — so an entity
 * would exist only to give Hibernate a name for a table these three statements already name. It
 * would need an {@code @IdClass} to do it, too, since the key is the pair.
 *
 * <p><b>The claim is {@code on conflict do nothing}, which is the whole concurrency design.</b> Two
 * channels race for the same event by construction: a live frame on the socket's worker thread and a
 * catch-up row on the scheduler's. The second inserter blocks on the first one's uncommitted row and
 * then finds it — or, if the first rolled back, wins the claim itself. A read-then-insert would have
 * a window where both read "not handled"; the primary key has none.
 */
@ApplicationScoped
public class ConsumedEvents {

  /**
   * This library's own persistence unit, named because it must be: a consuming application has its
   * own default one, and the claim ledger belongs to neither it nor its migration history.
   */
  @Inject
  @PersistenceUnit("eventstream")
  EntityManager em;

  /**
   * Claim this event for this listener. True if the claim is ours and the handler should run, false
   * if the row was already there — another channel handled it, and there is nothing to say about it.
   *
   * <p>Must be called inside the same transaction as the handler. That is what makes the claim and
   * the effect commit or roll back together.
   */
  public boolean claim(String listenerId, String eventId, Instant handledAt) {
    return em.createNativeQuery(
                "insert into consumed_event (listener_id, event_id, handled_at)"
                    + " values (?1, ?2, ?3) on conflict do nothing")
            .setParameter(1, listenerId)
            .setParameter(2, eventId)
            .setParameter(3, handledAt)
            .executeUpdate()
        == 1;
  }

  /** Whether this listener has already handled this event. For tests and for diagnostics. */
  public boolean isHandled(String listenerId, String eventId) {
    return count(
            "select count(*) from consumed_event where listener_id = ?1 and event_id = ?2",
            listenerId,
            eventId)
        > 0;
  }

  /** How many claims this listener is holding. The number pruning keeps bounded. */
  public long countFor(String listenerId) {
    return count("select count(*) from consumed_event where listener_id = ?1", listenerId);
  }

  /**
   * Forget the claims this listener made before {@code cut}. Returns how many were dropped.
   *
   * <p>Called with the watermark's own time less the configured horizon, so what it drops is what
   * catch-up can no longer offer: below the watermark an event is settled forever, and a claim
   * protects against nothing.
   *
   * <p><b>It compares {@code handled_at} — this consumer's clock — against a bound derived from
   * {@code occurred_at}, the publisher's.</b> Two clocks, on purpose: the row does not carry the
   * event's time, and the horizon is a day, so the skew between two hosts of one platform cannot
   * reach it. A horizon shortened to minutes would make that assumption load-bearing, which is the
   * reason the default is generous.
   */
  public int pruneBefore(String listenerId, Instant cut) {
    return em.createNativeQuery(
            "delete from consumed_event where listener_id = ?1 and handled_at < ?2")
        .setParameter(1, listenerId)
        .setParameter(2, cut)
        .executeUpdate();
  }

  /**
   * Forget every claim for one consumer as part of an explicit replay-from-epoch rebuild.
   *
   * <p>Not a cleanup operation. The caller must erase the paired watermark in the <em>same
   * transaction</em>, then re-offer the complete log; removing claims on their own deliberately
   * revokes the exactly-once history for this consumer.
   */
  public int forget(String listenerId) {
    return em.createNativeQuery("delete from consumed_event where listener_id = ?1")
        .setParameter(1, listenerId)
        .executeUpdate();
  }

  /** Every claim, for every listener. The suite's reset; nothing in the library calls it. */
  public int deleteAll() {
    return em.createNativeQuery("delete from consumed_event").executeUpdate();
  }

  private long count(String sql, Object... parameters) {
    var query = em.createNativeQuery(sql, Long.class);
    for (int i = 0; i < parameters.length; i++) {
      query.setParameter(i + 1, parameters[i]);
    }
    return (Long) query.getSingleResult();
  }
}
