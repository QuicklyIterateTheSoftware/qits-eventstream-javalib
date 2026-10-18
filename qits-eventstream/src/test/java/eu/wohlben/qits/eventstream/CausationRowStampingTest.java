package eu.wohlben.qits.eventstream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import eu.wohlben.qits.eventstream.consumer.CausedThing;
import io.quarkus.narayana.jta.QuarkusTransaction;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Row stamping, proved against real Hibernate in the consumer's arrangement: a {@link CausedThing}
 * in the default persistence unit, persisted through a real transaction.
 *
 * <p>The assertion that carries the design is the first one: <b>the scope closes before the
 * transaction commits, and the stamp survives.</b> {@code @PrePersist} firing at {@code persist()}
 * on the calling thread — not later at flush — is what makes a ThreadLocal source safe here, and
 * only a real persistence unit can prove it, because only there does the flush actually happen
 * somewhere else in time.
 */
@QuarkusTest
class CausationRowStampingTest {

  private static final UUID CAUSE = UUID.fromString("11111111-1111-4111-8111-111111111111");
  private static final UUID OTHER = UUID.fromString("22222222-2222-4222-8222-222222222222");

  @Inject EntityManager em;

  @AfterEach
  void theTestThreadIsLeftClean() {
    assertNull(CausationScope.current(), "no test here may leave a cause on the JUnit thread");
    QuarkusTransaction.requiringNew()
        .run(() -> em.createQuery("delete from CausedThing").executeUpdate());
  }

  /** The whole point, timing included: persist inside the scope, flush and commit outside it. */
  @Test
  void aRowPersistedInsideAScopeCarriesTheCauseEvenThoughCommitHappensAfterTheScopeClosed() {
    CausedThing thing = new CausedThing();
    thing.what = "made";

    QuarkusTransaction.requiringNew()
        .run(() -> CausationScope.with(CAUSE, () -> em.persist(thing)));

    assertEquals(CAUSE, reload(thing).causationId);
  }

  @Test
  void aRowPersistedOutsideAnyScopeIsRootless() {
    CausedThing thing = new CausedThing();
    thing.what = "unprompted";

    QuarkusTransaction.requiringNew().run(() -> em.persist(thing));

    assertNull(reload(thing).causationId);
  }

  /** The precedence rule again: a value the author set is knowledge, not a gap to fill. */
  @Test
  void aCauseTheAuthorSetThemselvesBeatsTheAmbientScope() {
    CausedThing thing = new CausedThing();
    thing.what = "attributed by hand";
    thing.causationId = OTHER;

    QuarkusTransaction.requiringNew()
        .run(() -> CausationScope.with(CAUSE, () -> em.persist(thing)));

    assertEquals(OTHER, reload(thing).causationId);
  }

  /** Insert-only: the column is creation history, and an update inside a scope must not rewrite it. */
  @Test
  void anUpdateInsideALaterScopeDoesNotRestampTheRow() {
    CausedThing thing = new CausedThing();
    thing.what = "made";
    QuarkusTransaction.requiringNew()
        .run(() -> CausationScope.with(CAUSE, () -> em.persist(thing)));

    QuarkusTransaction.requiringNew()
        .run(
            () ->
                CausationScope.with(
                    OTHER,
                    () -> {
                      CausedThing loaded = em.find(CausedThing.class, thing.id);
                      loaded.what = "changed";
                    }));

    assertEquals(CAUSE, reload(thing).causationId, "creation history, not last-touched");
  }

  /** A row that was never given a cause stays rootless through updates made inside a scope. */
  @Test
  void anUpdateNeverStampsARootlessRowEither() {
    CausedThing thing = new CausedThing();
    thing.what = "made outside";
    QuarkusTransaction.requiringNew().run(() -> em.persist(thing));

    QuarkusTransaction.requiringNew()
        .run(
            () ->
                CausationScope.with(
                    OTHER,
                    () -> {
                      CausedThing loaded = em.find(CausedThing.class, thing.id);
                      loaded.what = "changed inside";
                    }));

    assertNull(reload(thing).causationId, "no @PreUpdate, by decision");
  }

  private CausedThing reload(CausedThing thing) {
    return QuarkusTransaction.requiringNew().call(() -> em.find(CausedThing.class, thing.id));
  }
}
