package eu.wohlben.qits.eventstream.entity;

import io.quarkus.hibernate.orm.panache.PanacheEntityBase;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;

/**
 * How far one durable listener has read the event log: the composite {@code (occurredAt, id)} cursor
 * qits-events hands out, kept on this side.
 *
 * <p>One row per {@code consumerId}, and the row is the consumer's whole memory of where it is.
 * <b>Only the catch-up sweep moves it</b>, and only once a page has been processed in full: live
 * frames arrive ahead of it and never advance it, and a handler that throws leaves it where it was
 * so the event is offered again.
 *
 * <p>Composite for the reason the log's own cursor is: sibling events published by one pipeline run
 * share that run's finish instant, so a watermark of an instant alone would either re-offer a
 * sibling forever or skip one.
 *
 * <p><b>Once the watermark has passed an event, that event is settled forever</b> — for this
 * listener, whether it was handled, skipped by the predicate, or never wanted. That is what makes
 * selective storage safe, and it is the sentence to remember when changing a predicate: widening one
 * does not reach back.
 */
@Entity
@Table(name = "consumer_watermark")
public class ConsumerWatermark extends PanacheEntityBase {

  /** {@code QitsDurableEventListener#consumerId()} — a name a person chose, not a class name. */
  @Id
  @Column(name = "listener_id")
  public String listenerId;

  /** The {@code occurredAt} of the last row read past, or the epoch when there is none. */
  @Column(name = "occurred_at", nullable = false)
  public Instant occurredAt;

  /**
   * The id of that row, or <b>null for "before the first row of the log"</b>.
   *
   * <p>Null is a real state rather than a missing value: it is a consumer that opted into replaying
   * from the beginning, and a consumer initialized while the log held nothing it wanted. Both mean
   * "read from the start". It is null rather than blank because a cursor is sent back to qits-events
   * verbatim and the log answers a blank id with a 400 — so the absence has to be expressible as the
   * absence of the parameter.
   */
  @Column(name = "event_id")
  public String eventId;
}
