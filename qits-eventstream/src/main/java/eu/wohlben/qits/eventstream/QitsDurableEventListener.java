package eu.wohlben.qits.eventstream;

import eu.wohlben.qits.eventstream.control.EventFrame;
import java.util.Set;

/**
 * Consumes arriving events <b>durably</b>: an event this listener selects is handled exactly once,
 * whether it arrived on the live stream, on a catch-up sweep, or on both.
 *
 * <p>The other two seams are at-most-once and live-only — what is broadcast while the process is
 * away is gone. This one is the answer to that, and it is the seam to reach for whenever missing an
 * event would be a defect rather than a nuisance: a build that never triggers a deployment, a
 * release nobody rolls out. Implementations are ordinary {@code @ApplicationScoped} beans, found
 * exactly as {@link QitsEventListener}s and {@link QitsRawEventListener}s are.
 *
 * <p>Two things make it work, and both are the library's rather than yours. Every arrival — live or
 * caught up — goes through <b>one funnel</b>: in a single transaction the library claims the event
 * for this listener in {@code consumed_event} and then calls {@link #onFrame}, so a second arrival
 * of the same event finds the claim and is dropped. And a <b>watermark</b> per listener is paged
 * forward from qits-events' log on a schedule and once at startup, so a disconnect, a restart or a
 * cutover is caught up rather than lost.
 *
 * <h2>What the library needs from a durable listener</h2>
 *
 * <p>{@link #consumerId()} is the identity both tables key on, {@link #signatures()} is the same
 * vocabulary the other seams use, and {@link #selects} narrows within a signature.
 *
 * <p><b>Only selected events are stored.</b> An event {@link #selects} rejects leaves no row at all,
 * which is what keeps the table from becoming a second copy of the log. The watermark is what makes
 * that safe: catch-up only ever re-reads events <em>above</em> it, so a later widening of the
 * predicate — an environment starts listening to a branch, a repository adds a trigger file —
 * cannot resurrect ancient history. Once the watermark has passed an event, it is settled forever.
 *
 * <h2>Ordering, which is yours and cannot be given to you</h2>
 *
 * <p><b>Catch-up delivers late and out of stream order relative to live frames.</b> Across a restart
 * a handler can see an event older than one it has already handled; on a first sweep it can see a
 * burst of events in one go that the stream would have spread over an hour. The library does not
 * reorder and cannot: it does not know which of your effects commute.
 *
 * <p>So <b>a handler whose effect is last-writer-wins must collapse for itself</b> — check the tip
 * before acting. The deployer's shape is the worked example: on a build-succeeded event, deploy only
 * if that build is still the newest green one for its repository and branch, and otherwise do
 * nothing. A handler that simply applies whatever arrived will, one restart later, roll a stale
 * build over a newer one.
 *
 * <p>Handlers that only accumulate — an audit sink, a notification, an idempotent upsert keyed by
 * the event's own facts — need nothing of the sort.
 *
 * <h2>Failure, and what "exactly once" means</h2>
 *
 * <p>The guarantee is <b>exactly-once effect per (listener, event id)</b>, not exactly-once
 * delivery. {@link #onFrame} runs inside the claiming transaction, so a throw rolls the claim back
 * with it and the event <em>stays owed</em>: it is offered again on the next sweep, and the
 * watermark does not pass it in the meantime. That is the whole failure policy, and it means a
 * handler that throws on a poison event will be handed it again forever — see the log, fix the
 * event or the handler, or make the handler swallow what it cannot use.
 *
 * <p>The handler's own database work joins that transaction, which is the point: the effect and the
 * claim commit together or not at all.
 *
 * <p>Causation behaves exactly as it does on the other seams: {@link #onFrame} runs inside {@link
 * CausationScope} of the arriving event's id, on both channels, so a follow-up published on this
 * thread records the right parent with no argument passed. A hand-off to your own executor leaves
 * the scope behind — capture {@link CausationScope#current()} and re-establish it.
 */
public interface QitsDurableEventListener {

  /**
   * This consumer's stable name, and <b>part of the storage contract</b>: it is the {@code
   * listener_id} of every {@code consumed_event} row and of the {@code consumer_watermark}.
   *
   * <p>It must survive class renames, package moves and refactorings — which is exactly why it is a
   * string a person chose rather than {@code getClass().getName()}. Changing it makes a brand-new
   * consumer: the old rows are orphaned and the new id initializes at the head of the log, silently
   * skipping everything in between.
   *
   * <p><b>Never reuse an id for a different meaning.</b> A listener that inherits another's id
   * inherits its watermark and its claims, so it will believe it has already handled events it has
   * never seen. Prefer a name that says what the consumption is for — {@code
   * "ci.release-train.build-successful"} — over one that says which class does it today.
   */
  String consumerId();

  /**
   * The event names this listener wants, or {@code Set.of(}{@link QitsRawEventListener#ALL}{@code )}
   * for all of them — the same vocabulary and the same {@code "*"} literal the other seams use, so a
   * filter means one thing live and one thing historically.
   *
   * <p>Asked per subscribe, per arriving frame and per catch-up sweep, so it must be cheap. Catch-up
   * queries the log with these as its name filter; {@code "*"} queries with no filter at all.
   *
   * <p><b>An empty set means this listener wants nothing</b>: it is not subscribed for, and it is
   * not swept, so it keeps no watermark. A listener whose interest is not knowable at startup should
   * say {@code ALL} and narrow in {@link #selects}, for the reason {@link QitsRawEventListener}
   * spells out — the wire subscription is re-derived only when the connection is opened.
   *
   * <p>Widening the set later is allowed and has one consequence worth knowing: the watermark is one
   * cursor for the whole listener, so the newly wanted names are caught up from wherever the
   * watermark stands rather than from the moment they were added.
   */
  Set<String> signatures();

  /**
   * Whether this particular event is one this listener acts on. Default: everything it subscribed
   * to.
   *
   * <p>The narrowing that {@link #signatures()} cannot express, because it depends on the event's
   * content — this environment listens to that branch, this release train holds that repository.
   * <b>Only selected events are stored</b>, so this is what keeps the claim table proportional to
   * the work rather than to the log.
   *
   * <p>It must be a pure question about the frame. It runs before the claim and before the handler,
   * on both channels, and a throw is treated as a failure rather than as a "no" — an event whose
   * selection could not be decided stays owed instead of being settled by accident.
   */
  default boolean selects(EventFrame frame) {
    return true;
  }

  /**
   * One arrival, as it came off the wire or out of the log: the envelope plus the event's id. The
   * payload is the canonical JSON string qits-events stored — deserialize it with the event class
   * you expect, or read it as it is.
   *
   * <p>Runs inside the claiming transaction. Returning normally commits the claim; throwing rolls it
   * back and leaves the event owed.
   */
  void onFrame(EventFrame frame);

  /**
   * Whether a <b>brand-new</b> consumer should start at the beginning of the log instead of at its
   * head. Default false, and the default is the important half.
   *
   * <p>A consumer whose id has no watermark yet is initialized at the newest event matching its
   * signatures — <b>consume from now</b>. Replaying all of history into a consumer that has never
   * run is never the right default: the first deployment of a new subscriber would re-announce every
   * build the platform has ever run, and the events it would act on are mostly events something else
   * already acted on months ago.
   *
   * <p>Return true only when the whole history is genuinely the point — a projection being built, an
   * audit sink backfilling. The initialization writes the epoch and immediately pages from it in
   * the same catch-up invocation; it does not wait for a second scheduled sweep. It applies at
   * initialization and never again: once a watermark exists, this answer is not consulted, so
   * flipping it later replays nothing. An intentionally reset projection uses
   * {@code CatchupSweeper.rebuildFromEpoch(consumerId)}, which clears both its watermark and claim
   * ledger before making that same replay.
   */
  default boolean replayFromEpoch() {
    return false;
  }
}
