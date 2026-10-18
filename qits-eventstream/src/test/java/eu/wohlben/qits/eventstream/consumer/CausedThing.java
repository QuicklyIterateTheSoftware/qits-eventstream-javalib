package eu.wohlben.qits.eventstream.consumer;

import eu.wohlben.qits.eventstream.CausationStamp;
import eu.wohlben.qits.eventstream.CausedRow;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EntityListeners;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import java.util.UUID;

/**
 * The fixture entity for {@link CausationRowStampingTest}: a consumer's row, participating exactly
 * as the {@link CausedRow} javadoc prescribes — the annotation, the interface, the field. It lives
 * in the suite's DEFAULT persistence unit (the named {@code eventstream} unit claims only {@code
 * entity/}), which is precisely where a consumer's own entities sit.
 */
@Entity
@EntityListeners(CausationStamp.class)
public class CausedThing implements CausedRow {

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  public Long id;

  @Column(name = "causation_id")
  public UUID causationId;

  public String what;

  @Override
  public UUID causationId() {
    return causationId;
  }

  @Override
  public void causationId(UUID id) {
    this.causationId = id;
  }
}
