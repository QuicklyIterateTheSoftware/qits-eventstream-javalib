package eu.wohlben.qits.eventstream.persistence;

import eu.wohlben.qits.eventstream.entity.ConsumerWatermark;
import io.quarkus.hibernate.orm.panache.PanacheRepositoryBase;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Instant;

/** Panache DAO for {@link ConsumerWatermark} (keyed by the consumer's own id). */
@ApplicationScoped
public class ConsumerWatermarkRepository
    implements PanacheRepositoryBase<ConsumerWatermark, String> {

  /**
   * Put this listener's watermark at that row, creating it if this is the first time.
   *
   * <p>An upsert rather than an update because initialization and advancement are the same
   * statement: a consumer that has just been initialized has read past nothing, and one that has
   * finished a page has read past its last row, and the row means the same thing in both cases.
   */
  public void put(String listenerId, Instant occurredAt, String eventId) {
    ConsumerWatermark mark = findById(listenerId);
    if (mark != null) {
      mark.occurredAt = occurredAt;
      mark.eventId = eventId;
      return;
    }
    // Filled in BEFORE persist: Hibernate checks the not-null columns as the row is made
    // persistent, not at flush, so a new instance handed over half-built fails there.
    mark = new ConsumerWatermark();
    mark.listenerId = listenerId;
    mark.occurredAt = occurredAt;
    mark.eventId = eventId;
    persist(mark);
  }

  /**
   * Forget this consumer's position so an explicit projection rebuild can begin at the epoch.
   *
   * <p>Ordinary catch-up never calls this: a watermark is durable memory. It is paired with
   * {@link ConsumedEvents#forget(String)} in one transaction by the catch-up sweeper's opt-in
   * rebuild operation; clearing only one of the two would either skip the historical handlers or
   * leave their old claims pretending the rebuilt projection had already received them.
   */
  public boolean forget(String listenerId) {
    return deleteById(listenerId);
  }
}
