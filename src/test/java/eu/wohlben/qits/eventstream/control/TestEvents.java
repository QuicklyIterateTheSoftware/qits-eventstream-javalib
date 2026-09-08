package eu.wohlben.qits.eventstream.control;

import eu.wohlben.qits.eventstream.CausationScope;
import eu.wohlben.qits.eventstream.QitsDurableEventListener;
import eu.wohlben.qits.eventstream.QitsEvent;
import eu.wohlben.qits.eventstream.QitsEventBus;
import eu.wohlben.qits.eventstream.QitsEventListener;
import eu.wohlben.qits.eventstream.QitsRawEventListener;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

/**
 * The event types and listeners this module's own suite uses.
 *
 * <p><b>Deliberately not {@code BuildSuccessful}.</b> That class lives in {@code ci-events}, which
 * depends on this module — reaching for it here would invert the arrow the extraction rule exists
 * to protect, and would make the library's tests depend on one consumer's vocabulary. These three
 * types are what a library test should exercise: something with a couple of fields, something else
 * so a subscription set is a set, and one whose listener nothing injects.
 */
public final class TestEvents {

  /**
   * Which path a delivery came down, in the order the dispatcher took them — {@code "typed"} or
   * {@code "raw"}, appended by every listener below as it is called.
   *
   * <p>Cross-listener ordering is not observable from inside any one bean, and "typed first, raw
   * second" is a stated contract rather than an accident, so it needs somewhere shared to be seen.
   * Static because the beans are application-scoped and the reader is a test; cleared by {@link
   * #clearDeliveries()}.
   */
  private static final List<String> DELIVERIES = new CopyOnWriteArrayList<>();

  /** The paths taken, in order, since the last clear. */
  public static List<String> deliveries() {
    return List.copyOf(DELIVERIES);
  }

  public static void clearDeliveries() {
    DELIVERIES.clear();
  }

  /** A two-field event with a nullable member, so the absent-field rule is exercised on the wire. */
  public record ThingHappened(UUID eventId, String what, Integer count, Instant at)
      implements QitsEvent {

    public ThingHappened {
      if (eventId == null) {
        eventId = UUID.randomUUID();
      }
    }

    public ThingHappened(String what, Integer count, Instant at) {
      this(null, what, count, at);
    }

    @Override
    public Instant occurredAt() {
      return at;
    }
  }

  /** A second type, so the subscribe frame has to carry a set rather than a name. */
  public record OtherThingHappened(UUID eventId, String detail, Instant at) implements QitsEvent {

    public OtherThingHappened {
      if (eventId == null) {
        eventId = UUID.randomUUID();
      }
    }

    public OtherThingHappened(String detail, Instant at) {
      this(null, detail, at);
    }

    @Override
    public Instant occurredAt() {
      return at;
    }
  }

  /**
   * A third type whose listener is <b>injected nowhere</b>, on purpose. A real consumer's listener
   * is like this: a bean written to be found, reached only through {@code
   * Instance<QitsEventListener<?>>}, with no other reference to it in the application. That is
   * exactly the shape ArC's unused-bean removal exists for, and a listener silently removed fails
   * in the worst available way — it subscribes to nothing, receives nothing, and logs nothing. Its
   * presence in the subscription set is asserted in {@code EventStreamSubscriberTest}.
   */
  public record QuietlyHappened(UUID eventId, Instant at) implements QitsEvent {

    public QuietlyHappened {
      if (eventId == null) {
        eventId = UUID.randomUUID();
      }
    }

    @Override
    public Instant occurredAt() {
      return at;
    }
  }

  /** A listener that only records — what it received, and the cause it was dispatched under. */
  @ApplicationScoped
  public static class ThingListener implements QitsEventListener<ThingHappened> {

    private final List<ThingHappened> received = new CopyOnWriteArrayList<>();
    private final List<Optional<UUID>> causes = new CopyOnWriteArrayList<>();

    @Override
    public Class<ThingHappened> eventType() {
      return ThingHappened.class;
    }

    @Override
    public void onEvent(ThingHappened event) {
      // Read INSIDE onEvent, which is the only place the answer means anything: the dispatcher's
      // scope is established for the call and unwound after it.
      DELIVERIES.add("typed");
      causes.add(Optional.ofNullable(CausationScope.current()));
      received.add(event);
    }

    public List<ThingHappened> received() {
      return List.copyOf(received);
    }

    /** The ambient cause seen on each arrival, empty where there was none. */
    public List<Optional<UUID>> causes() {
      return List.copyOf(causes);
    }

    public void reset() {
      received.clear();
      causes.clear();
    }
  }

  /**
   * A second listener on {@link ThingHappened}, armed by the test that wants it and inert otherwise.
   *
   * <p>It exists for three things the dispatcher has to get right and that only a listener can
   * observe from the inside: that two listeners on one frame see the <em>same</em> cause, that an
   * event published <em>during</em> consumption is stamped with the arriving frame's id without
   * anybody passing an argument, and that a listener which throws does not leak its scope onto the
   * next frame.
   *
   * <p>On {@link ThingHappened} rather than on a fourth event type on purpose: the subscription set
   * is asserted literally in {@code EventStreamSubscriberTest}, and a new signature would have made
   * this bean a change to that contract instead of an addition to this one. Disarmed by default for
   * the same reason — every other test in the suite dispatches this signature.
   */
  @ApplicationScoped
  public static class CausationProbeListener implements QitsEventListener<ThingHappened> {

    @Inject QitsEventBus bus;

    private final List<Optional<UUID>> causes = new CopyOnWriteArrayList<>();
    private final AtomicBoolean publishOnEvent = new AtomicBoolean();
    private final AtomicBoolean throwOnEvent = new AtomicBoolean();
    private final AtomicReference<OtherThingHappened> published = new AtomicReference<>();

    @Override
    public Class<ThingHappened> eventType() {
      return ThingHappened.class;
    }

    @Override
    public void onEvent(ThingHappened event) {
      DELIVERIES.add("typed");
      causes.add(Optional.ofNullable(CausationScope.current()));
      if (publishOnEvent.get()) {
        // No parent argument anywhere: whatever lands on the wire came from the ambient scope.
        OtherThingHappened followUp = new OtherThingHappened("because of " + event.what(), event.at());
        published.set(followUp);
        bus.publish(followUp);
      }
      if (throwOnEvent.get()) {
        throw new IllegalStateException("a listener that fails mid-frame");
      }
    }

    /** Publish a follow-up event from inside {@code onEvent}, with no explicit parent. */
    public void publishWhileConsuming() {
      publishOnEvent.set(true);
    }

    /** Throw out of {@code onEvent}, after having recorded the cause. */
    public void failWhileConsuming() {
      throwOnEvent.set(true);
    }

    /** The follow-up this listener last published, so a test can name its id. */
    public OtherThingHappened published() {
      return published.get();
    }

    public List<Optional<UUID>> causes() {
      return List.copyOf(causes);
    }

    public void reset() {
      causes.clear();
      publishOnEvent.set(false);
      throwOnEvent.set(false);
      published.set(null);
    }
  }

  /** The second listener, on the second type. */
  @ApplicationScoped
  public static class OtherThingListener implements QitsEventListener<OtherThingHappened> {

    private final List<OtherThingHappened> received = new CopyOnWriteArrayList<>();

    @Override
    public Class<OtherThingHappened> eventType() {
      return OtherThingHappened.class;
    }

    @Override
    public void onEvent(OtherThingHappened event) {
      received.add(event);
    }

    public List<OtherThingHappened> received() {
      return List.copyOf(received);
    }

    public void reset() {
      received.clear();
    }
  }

  /** The listener nobody injects. See {@link QuietlyHappened}. */
  @ApplicationScoped
  public static class QuietListener implements QitsEventListener<QuietlyHappened> {

    @Override
    public Class<QuietlyHappened> eventType() {
      return QuietlyHappened.class;
    }

    @Override
    public void onEvent(QuietlyHappened event) {
      // Nothing. Being registered is the whole of what it is here to prove.
    }
  }

  // -- the raw seam --------------------------------------------------------------------------------

  /**
   * The signature {@link QuietRawListener} wants, named by nothing else in this suite on purpose: it
   * is the entry in the subscribe frame that can only have come from a raw listener.
   */
  public static final String RAW_ONLY_SIGNATURE = "RawOnlyHappened";

  /**
   * A raw listener that records what it was given and can be told to misbehave. Not a bean itself —
   * the two concrete subclasses below are, so that "a raw listener that throws does not stop the
   * other raw listeners" is a claim about two different objects rather than about two calls on one.
   *
   * <p><b>Disarmed by default</b>, meaning it wants nothing: the subscribe frame is asserted
   * literally elsewhere in this suite, so a recording listener that permanently wanted something
   * would make every other test's contract its business.
   */
  public abstract static class RawRecorder implements QitsRawEventListener {

    @Inject QitsEventBus bus;

    private final AtomicReference<Set<String>> wanted = new AtomicReference<>(Set.of());
    private final List<EventFrame> frames = new CopyOnWriteArrayList<>();
    private final List<Optional<UUID>> causes = new CopyOnWriteArrayList<>();
    private final AtomicBoolean failWhenAsked = new AtomicBoolean();
    private final AtomicBoolean throwOnFrame = new AtomicBoolean();
    private final AtomicBoolean publishOnFrame = new AtomicBoolean();
    private final AtomicReference<OtherThingHappened> published = new AtomicReference<>();

    @Override
    public Set<String> signatures() {
      if (failWhenAsked.get()) {
        throw new IllegalStateException("a raw listener that cannot say what it wants");
      }
      return wanted.get();
    }

    @Override
    public void onFrame(EventFrame frame) {
      DELIVERIES.add("raw");
      causes.add(Optional.ofNullable(CausationScope.current()));
      frames.add(frame);
      if (publishOnFrame.get()) {
        // No parent argument: whatever lands on the wire came from the dispatcher's ambient scope,
        // which is the property a raw consumer inherits unchanged from the typed one.
        OtherThingHappened followUp = new OtherThingHappened("raw saw " + frame.name(), T0);
        published.set(followUp);
        bus.publish(followUp);
      }
      if (throwOnFrame.get()) {
        throw new IllegalStateException("a raw listener that fails mid-frame");
      }
    }

    /** Want exactly these signatures from now on. */
    public void wants(String... signatures) {
      wanted.set(Set.of(signatures));
    }

    /** Want everything — the literal the union collapses on. */
    public void wantsEverything() {
      wanted.set(Set.of(ALL));
    }

    /** Throw out of {@code signatures()}, which is a question dispatch and the subscriber both ask. */
    public void failWhenAsked() {
      failWhenAsked.set(true);
    }

    public void failWhileConsuming() {
      throwOnFrame.set(true);
    }

    public void publishWhileConsuming() {
      publishOnFrame.set(true);
    }

    /** Every frame handed over, verbatim. */
    public List<EventFrame> frames() {
      return List.copyOf(frames);
    }

    /** The ambient cause seen on each arrival, empty where there was none. */
    public List<Optional<UUID>> causes() {
      return List.copyOf(causes);
    }

    public OtherThingHappened published() {
      return published.get();
    }

    public void reset() {
      wanted.set(Set.of());
      frames.clear();
      causes.clear();
      failWhenAsked.set(false);
      throwOnFrame.set(false);
      publishOnFrame.set(false);
      published.set(null);
    }
  }

  /** The raw listener the tests drive. */
  @ApplicationScoped
  public static class RecordingRawListener extends RawRecorder {}

  /** A second one, so containment is provable between two raw listeners rather than within one. */
  @ApplicationScoped
  public static class SecondRawListener extends RawRecorder {}

  /**
   * The raw listener <b>nobody injects</b>, wanting a signature no event class in this suite has.
   *
   * <p>Two things at once, and both matter. It is the raw half of what {@link QuietListener} proves
   * for the typed one — that ArC's unused-bean removal leaves a bean reached only through {@code
   * Instance<QitsRawEventListener>} alone, with no {@code @Unremovable} anywhere — and because its
   * signature is one no {@code eventType()} produces, its presence in the subscribe frame is the
   * assertion that the union is a union at all.
   */
  @ApplicationScoped
  public static class QuietRawListener implements QitsRawEventListener {

    @Override
    public Set<String> signatures() {
      return Set.of(RAW_ONLY_SIGNATURE);
    }

    @Override
    public void onFrame(EventFrame frame) {
      // Nothing. Being subscribed for is the whole of what it is here to prove.
    }
  }

  // -- the durable seam ----------------------------------------------------------------------------

  /**
   * The consumer id the durable listener below stores under. A literal a person chose, which is what
   * {@code consumerId()} is for; the tables are wiped between tests, so every test that arms this
   * listener meets it as a brand-new consumer.
   */
  public static final String DURABLE_CONSUMER_ID = "test.durable-recorder";

  /**
   * A durable listener the tests drive, <b>disarmed by default</b> — it wants nothing, so it is
   * absent from the subscribe frame (which another suite asserts literally), it is skipped by the
   * catch-up sweep, and it writes no watermark until a test asks it to.
   *
   * <p>Everything a durable listener can do wrong is a switch on it: refusing an event, failing to
   * decide, throwing out of the handler. Each is one of the funnel's branches, and each is a
   * different row in the two tables afterwards.
   */
  @ApplicationScoped
  public static class RecordingDurableListener implements QitsDurableEventListener {

    private final AtomicReference<Set<String>> wanted = new AtomicReference<>(Set.of());
    private final AtomicReference<Predicate<EventFrame>> selector = new AtomicReference<>(f -> true);
    private final List<EventFrame> frames = new CopyOnWriteArrayList<>();
    private final List<Optional<UUID>> causes = new CopyOnWriteArrayList<>();
    private final AtomicReference<String> failOnId = new AtomicReference<>();
    private final AtomicBoolean failWhenAsked = new AtomicBoolean();
    private final AtomicBoolean fromEpoch = new AtomicBoolean();
    private final AtomicReference<String> holdOnId = new AtomicReference<>();
    private final AtomicReference<CountDownLatch> release = new AtomicReference<>();
    private final AtomicReference<CountDownLatch> reached = new AtomicReference<>();
    private final AtomicReference<Thread> holder = new AtomicReference<>();

    @Override
    public String consumerId() {
      return DURABLE_CONSUMER_ID;
    }

    @Override
    public Set<String> signatures() {
      return wanted.get();
    }

    @Override
    public boolean selects(EventFrame frame) {
      if (failWhenAsked.get()) {
        throw new IllegalStateException("a durable listener that cannot decide");
      }
      return selector.get().test(frame);
    }

    @Override
    public void onFrame(EventFrame frame) {
      // Read inside the handler, which is the only place it means anything — and the claim of the
      // interface's javadoc: causation is identical on both channels.
      causes.add(Optional.ofNullable(CausationScope.current()));
      frames.add(frame);
      // Both latches are read into locals before either is used: reset() clears the hold from
      // another thread, and a handler that read them one at a time could find the second one gone.
      CountDownLatch arrived = reached.get();
      CountDownLatch gate = release.get();
      if (frame.id().equals(holdOnId.get()) && arrived != null && gate != null) {
        holder.set(Thread.currentThread());
        arrived.countDown();
        try {
          gate.await();
        } catch (InterruptedException interrupted) {
          // THE STAND-IN FOR THE WEDGED SOCKET READ, and it is faithful in the one way that
          // matters: await() is an interruptible park, exactly as a virtual thread's blocking
          // socket read measurably is. Restore the flag — the sweep above reads it to tell "the
          // watchdog killed me" from "a handler threw" — and throw, so the funnel rolls the claim
          // back and the event stays owed, which is what the real failure does too.
          Thread.currentThread().interrupt();
          throw new IllegalStateException(
              "a durable handler interrupted while held on " + frame.id());
        }
      }
      if (frame.id().equals(failOnId.get())) {
        throw new IllegalStateException("a durable handler that fails on " + frame.id());
      }
    }

    @Override
    public boolean replayFromEpoch() {
      return fromEpoch.get();
    }

    /** Want exactly these signatures from now on. */
    public void wants(String... signatures) {
      wanted.set(Set.of(signatures));
    }

    /** Want everything — the case that queries the log with no name filter at all. */
    public void wantsEverything() {
      wanted.set(Set.of(QitsRawEventListener.ALL));
    }

    /** Act only on the events this predicate accepts; the rest must leave no row. */
    public void selectsOnly(Predicate<EventFrame> predicate) {
      selector.set(predicate);
    }

    /** Throw out of the handler for one event id, so the claim has to roll back with it. */
    public void failOn(String eventId) {
      failOnId.set(eventId);
    }

    /** Throw out of {@code selects}, which must leave the event owed rather than settle it. */
    public void failWhenAsked() {
      failWhenAsked.set(true);
    }

    /**
     * Hold the handler on this event id until the returned latch is counted down — <b>the sweep
     * that never comes back</b>, which is the shape of the 2026-09-08 wedge.
     *
     * @return the RELEASE latch: count it down to let the handler finish normally
     */
    public CountDownLatch holdOn(String eventId) {
      CountDownLatch releaseLatch = new CountDownLatch(1);
      reached.set(new CountDownLatch(1));
      release.set(releaseLatch);
      holder.set(null);
      holdOnId.set(eventId);
      return releaseLatch;
    }

    /** Block until the handler has actually reached the hold, so a test never races its own gate. */
    public boolean awaitHold(Duration timeout) {
      CountDownLatch gate = reached.get();
      if (gate == null) {
        return false;
      }
      try {
        return gate.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
        return false;
      }
    }

    /** The thread that reached the hold, which is the one the watchdog is supposed to interrupt. */
    public Thread holder() {
      return holder.get();
    }

    /** Start at the beginning of the log instead of at its head. */
    public void replaysFromEpoch() {
      fromEpoch.set(true);
    }

    /** Every frame handled, in the order the handler saw them. */
    public List<EventFrame> frames() {
      return List.copyOf(frames);
    }

    /** The ids handled, which is what a paging assertion compares against. */
    public List<String> handledIds() {
      return frames.stream().map(EventFrame::id).toList();
    }

    /** The ambient cause seen on each arrival, empty where there was none. */
    public List<Optional<UUID>> causes() {
      return List.copyOf(causes);
    }

    public void reset() {
      wanted.set(Set.of());
      selector.set(frame -> true);
      frames.clear();
      causes.clear();
      failOnId.set(null);
      failWhenAsked.set(false);
      fromEpoch.set(false);
      // Free anything still held before forgetting the latch: a reset that dropped the reference
      // would leave a sweep thread parked for the rest of the suite, which is the very failure this
      // fixture exists to reproduce and the last one a test fixture should cause.
      holdOnId.set(null);
      CountDownLatch pending = release.getAndSet(null);
      if (pending != null) {
        pending.countDown();
      }
      reached.set(null);
      holder.set(null);
    }
  }

  /** Where the follow-up events these listeners publish are dated. Matches the suite's clock. */
  private static final Instant T0 = Instant.parse("2026-07-31T12:00:00Z");

  private TestEvents() {}
}
