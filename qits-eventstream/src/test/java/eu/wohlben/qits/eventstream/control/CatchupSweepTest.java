package eu.wohlben.qits.eventstream.control;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import eu.wohlben.qits.eventstream.control.TestEvents.RecordingDurableListener;
import eu.wohlben.qits.eventstream.entity.ConsumerWatermark;
import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Catch-up: reading the log forward from a watermark, which is the cure for everything the live
 * stream cannot deliver.
 *
 * <p>Against {@link StubEventsServer}'s list route, which pages the way the real one does — that is
 * the only honest way to test a paging loop, because the properties under test are exactly the ones
 * a canned answer cannot exhibit: a cursor that neither drops nor repeats a row across a boundary,
 * and a {@code nextCursor} that is null on the last page even when the page came back full.
 *
 * <p>The sweep is driven by hand. The scheduler is off in this suite and so is the startup run, for
 * the reason the outbox's is: a tick landing behind a test is non-determinism, and the arrangement
 * here is a log being seeded row by row.
 */
@QuarkusTest
@WithTestResource(StubEventsServer.class)
class CatchupSweepTest extends EventstreamTestSupport {

  private static final String CONSUMER = TestEvents.DURABLE_CONSUMER_ID;

  @Inject CatchupSweeper sweeper;
  @Inject EventsQuery query;
  @Inject RecordingDurableListener durable;

  @BeforeEach
  void armNothing() {
    durable.reset();
  }

  @AfterEach
  void disarm() {
    durable.reset();
  }

  /**
   * <b>A consumer that has never run starts at the head of the log, not at the epoch.</b> The first
   * sweep hands it nothing at all and records where "now" was; the second hands it only what arrived
   * since. Replaying the whole log into a new subscriber would have it act on events something else
   * dealt with months ago, so this is the default and the opt-in is below.
   */
  @Test
  void aNewConsumerStartsAtTheHeadOfTheLogAndConsumesFromNow() {
    durable.wants("ThingHappened");
    seed("old-1", T0);
    seed("old-2", T0.plusSeconds(10));

    assertEquals(0, sweeper.catchUp(), "initialization handles nothing");
    assertEquals(List.of(), durable.handledIds());
    ConsumerWatermark mark = watermark(CONSUMER);
    assertNotNull(mark, "the consumer is initialized rather than left unknown");
    assertEquals("old-2", mark.eventId, "at the newest event there is");

    seed("new-1", T0.plusSeconds(20));
    assertEquals(1, sweeper.catchUp());
    assertEquals(List.of("new-1"), durable.handledIds());
    assertEquals("new-1", watermark(CONSUMER).eventId);
  }

  /** With nothing in the log to want, "from now" and "from the start" are the same place. */
  @Test
  void aNewConsumerAgainstAnEmptyLogIsInitializedAtTheStart() {
    durable.wants("ThingHappened");

    assertEquals(0, sweeper.catchUp());

    ConsumerWatermark mark = watermark(CONSUMER);
    assertEquals(Instant.EPOCH, mark.occurredAt);
    assertNull(mark.eventId, "no row is behind it, so there is no cursor to send back");
  }

  /** The opt-in, for a consumer whose point is the whole history. */
  @Test
  void aConsumerThatAsksForTheWholeLogGetsIt() {
    durable.wants("ThingHappened");
    durable.replaysFromEpoch();
    seed("old-1", T0);
    seed("old-2", T0.plusSeconds(10));

    assertEquals(2, sweeper.catchUp(), "initialization and epoch replay are one sweep");
    assertEquals(List.of("old-1", "old-2"), durable.handledIds());
  }

  /** A projection bootstrap needs an explicit head certificate, not merely a handled count. */
  @Test
  void aNamedCatchupCertifiesThatItReachedTheHead() {
    durable.wants("ThingHappened");
    durable.replaysFromEpoch();
    seed("old-1", T0);
    seed("old-2", T0.plusSeconds(10));

    CatchupResult result = sweeper.catchUp(CONSUMER);

    assertEquals(CatchupResult.Status.REACHED_HEAD, result.status());
    assertTrue(result.reachedLogHead());
    assertEquals(2, result.handled());
  }

  /**
   * A reset projection must not inherit qits-eventstream's retained watermark or claim ledger.
   * Rebuild clears both atomically and invokes the handler for the same historic rows again.
   */
  @Test
  void anEpochRebuildForgetsTheRetainedLedgerAndReplaysItInTheSameInvocation() {
    durable.wants("ThingHappened");
    durable.replaysFromEpoch();
    seed("old-1", T0);
    seed("old-2", T0.plusSeconds(10));

    assertEquals(2, sweeper.catchUp());
    assertEquals(2, claims(CONSUMER));

    CatchupResult rebuilt = sweeper.rebuildFromEpoch(CONSUMER);

    assertEquals(CatchupResult.Status.REACHED_HEAD, rebuilt.status());
    assertEquals(2, rebuilt.handled());
    assertEquals(List.of("old-1", "old-2", "old-1", "old-2"), durable.handledIds());
    assertEquals(2, claims(CONSUMER), "old claims were replaced by the rebuilt claims");
    assertEquals("old-2", watermark(CONSUMER).eventId);
  }

  /**
   * The paging loop over more than one page, in the order the log is read: <b>oldest first</b>. Two
   * hundred is the page, so 250 rows is two requests, and the assertion is that the second one
   * resumes exactly where the first stopped.
   */
  @Test
  void theSweepPagesAscendingUntilItReachesTheHead() {
    durable.wants("ThingHappened");
    durable.replaysFromEpoch();
    List<String> seeded = seedMany(250);

    assertEquals(250, sweeper.catchUp());

    assertEquals(seeded, durable.handledIds(), "every row once, oldest first");
    assertEquals(seeded.get(249), watermark(CONSUMER).eventId);
    assertTrue(
        StubEventsServer.queries().stream().anyMatch(q -> q.contains("order=asc")),
        "catch-up reads forward: " + StubEventsServer.queries());
    assertTrue(
        StubEventsServer.queries().stream().anyMatch(q -> q.contains("cursor=")),
        "and resumes from its watermark: " + StubEventsServer.queries());
    assertTrue(
        StubEventsServer.queries().stream().anyMatch(q -> q.contains("name=ThingHappened")),
        "with its own signatures as the filter: " + StubEventsServer.queries());
  }

  /**
   * <b>The watermark advances only when a page is processed in full.</b> A handler that throws in
   * the middle of the second page leaves the mark at the end of the first, so the events between are
   * offered again — and the ones already claimed on that page are skipped when they are.
   */
  @Test
  void aFailureMidPageLeavesTheWatermarkAtTheLastCompletePage() {
    durable.wants("ThingHappened");
    durable.replaysFromEpoch();
    List<String> seeded = seedMany(250);
    durable.failOn(seeded.get(209));

    assertEquals(209, sweeper.catchUp(), "the whole first page, then nine of the second, then the throw");

    assertEquals(
        seeded.get(199),
        watermark(CONSUMER).eventId,
        "the second page was not finished, so the mark stayed at the end of the first");

    // Fix the handler and the sweep picks up exactly what is owed. Fifty rows are above the
    // watermark and are all re-read; nine of them are claimed already and are skipped in silence,
    // which is the overlap the claim ledger exists for.
    durable.failOn(null);
    assertEquals(41, sweeper.catchUp(), "the failed event and the forty after it");
    assertEquals(seeded.get(249), watermark(CONSUMER).eventId);

    // Every row reached the handler, in order, and exactly one reached it twice — the invocation
    // that threw and was rolled back. THAT IS THE GUARANTEE, precisely: one committed effect per
    // event, not one call. A handler is retried, so it must not be surprised to see an event again
    // after it has failed on it.
    assertEquals(seeded, durable.handledIds().stream().distinct().toList());
    assertEquals(seeded.size() + 1, durable.handledIds().size());
  }

  /** {@code "*"} is not a name the log knows: it means no filter, and the query says so. */
  @Test
  void aListenerThatWantsEverythingQueriesWithoutANameFilter() {
    durable.wantsEverything();
    seed("only-1", T0);

    sweeper.catchUp();

    assertTrue(
        StubEventsServer.queries().stream().noneMatch(q -> q.contains("name=")),
        "a name filter of '*' would match nothing: " + StubEventsServer.queries());
  }

  /**
   * Pruning: a claim below the watermark by more than the horizon is dropped, because catch-up can
   * never offer that event again and the claim protects against nothing.
   *
   * <p>The clock is what makes this exact. Claims are stamped with the consumer's clock, held at
   * {@code T0}, and the cut is the watermark's own time less a day — so seeding the log two days
   * ahead puts every claim below the cut, and seeding it at {@code T0} puts every claim above it.
   */
  @Test
  void claimsBelowTheWatermarkArePruned() {
    durable.wants("ThingHappened");
    durable.replaysFromEpoch();
    seed("old-1", T0.plus(Duration.ofDays(2)));
    seed("old-2", T0.plus(Duration.ofDays(2)).plusSeconds(1));

    assertEquals(2, sweeper.catchUp());

    assertEquals(0, claims(CONSUMER), "settled claims are not kept");
  }

  /** And a claim still inside the overlap window is kept, which is what the window is for. */
  @Test
  void claimsInsideTheHorizonAreKept() {
    durable.wants("ThingHappened");
    durable.replaysFromEpoch();
    seed("recent-1", T0);

    assertEquals(1, sweeper.catchUp());

    assertEquals(1, claims(CONSUMER), "the stream may still deliver what the sweep just read");
  }

  /** A listener that wants nothing is not swept, and keeps no watermark to be wrong about. */
  @Test
  void aListenerThatWantsNothingIsNotSwept() {
    seed("some-1", T0);

    assertEquals(0, sweeper.catchUp());

    assertNull(watermark(CONSUMER));
    assertEquals(List.of(), StubEventsServer.queries(), "and it asks the log nothing");
  }

  /**
   * An unreadable log is {@link EventsQuery.Unavailable} and never an empty page. Collapsing the two
   * would let one outage settle every event that occurred during it: an empty page means the
   * consumer has reached the head and may advance, and nothing else may mean that.
   */
  @Test
  void anUnreachableLogIsAFailureAndNotAnEmptyPage() {
    EventsQuery unreachable = new EventsQuery();
    unreachable.eventsUrl = "http://127.0.0.1:1";

    assertThrows(
        EventsQuery.Unavailable.class, () -> unreachable.after(List.of("ThingHappened"), null, 10));
  }

  /** The reachable half of the same claim, so the pair is asserted against one real server. */
  @Test
  void theQueryReadsThePageTheLogHandedBack() {
    seed("a-1", T0);
    seed("a-2", T0.plusSeconds(1));

    EventPage page = query.after(List.of("ThingHappened"), null, 1);

    assertEquals(List.of("a-1"), page.events().stream().map(EventFrame::id).toList());
    assertEquals(T0 + ",a-1", page.nextCursor(), "there is more, and this is where it resumes");
    assertEquals("a-2", query.newest(List.of("ThingHappened")).id());
  }

  private static void seed(String id, Instant occurredAt) {
    StubEventsServer.seed(id, "ThingHappened", occurredAt, "{\"what\":\"" + id + "\"}");
  }

  /** {@code n} events one second apart, ids padded so lexical order matches chronological. */
  private static List<String> seedMany(int n) {
    List<String> ids = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      String id = String.format("e-%04d", i);
      seed(id, T0.plusSeconds(i));
      ids.add(id);
    }
    return ids;
  }
}
