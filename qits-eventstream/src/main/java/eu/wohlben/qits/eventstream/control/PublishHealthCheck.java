package eu.wohlben.qits.eventstream.control;

import eu.wohlben.qits.eventstream.QitsEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.Supplier;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.health.HealthCheck;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.HealthCheckResponseBuilder;
import org.eclipse.microprofile.health.Readiness;
import org.jboss.logging.Logger;

/**
 * Says out loud whether this process can <b>build</b> what it publishes (qits-2055).
 *
 * <p><b>The incident this answers was green from end to end.</b> A native image without reflection
 * for {@link EventEnvelope} could not write a single PUT body. The failure came out of {@link
 * EventsPublisher} as "unreachable", so every event went to the outbox at DEBUG, was retried
 * forever without spending any budget, and was announced as an outage of a qits-events that was up
 * — while every readiness check stayed green, because none of them asked. A publisher that cannot
 * serialise is not degraded, it is silent, and a silent publisher is precisely the shape {@link
 * CatchupHealthCheck} already exists to refuse on the consuming side.
 *
 * <p>The rules, in the order they are asked:
 *
 * <ol>
 *   <li><b>Dark is UP.</b> With {@code qits.eventstream.enabled=false} nothing is published, so
 *       there is nothing for this to be about, and a library must never make a consumer red over a
 *       feature it has turned off.
 *   <li><b>A failed self-test is DOWN, from boot.</b> On its first call the check writes a probe
 *       envelope and a probe event's payload, and reads a probe frame and a probe page, through the
 *       very {@link CanonicalJson} the bus uses. That is the native-image gap found <em>before</em>
 *       an event is owed rather than after: a binary built without the wire types' reflection fails
 *       here on its first readiness poll. Asked once and remembered, since a binary's reflection
 *       does not change while it runs.
 *   <li><b>An event whose latest attempt could not be serialised is DOWN</b>, for as long as that
 *       is its latest attempt ({@link UnserializableEvents}). The self-test covers the library's own
 *       types; this covers a consumer's event class the self-test cannot know about. A later
 *       attempt that builds its body clears it, whatever became of the request.
 *   <li>Otherwise UP.
 * </ol>
 *
 * <p><b>{@code @Readiness}, not {@code @Liveness}</b>, for {@link CatchupHealthCheck}'s reason: a
 * jar on somebody else's classpath may take an instance out of rotation, which is the honest
 * statement ("this process cannot publish"), but it should not hold the authority to restart it.
 * And a restart would not help here anyway — the fault is in the binary, and the cure is a build.
 */
@Readiness
@ApplicationScoped
public class PublishHealthCheck implements HealthCheck {

  private static final Logger LOG = Logger.getLogger(PublishHealthCheck.class);

  /** The name this appears under in {@code /q/health/ready}. */
  static final String NAME = "eventstream-publish";

  /** How many failing events the response names before it only counts them. */
  static final int NAMED_LIMIT = 10;

  @Inject UnserializableEvents unserializable;

  @ConfigProperty(name = "qits.eventstream.enabled")
  boolean enabled;

  /**
   * The startup self-test: empty when the wire round-trips, otherwise what broke. A field, so that a
   * JVM test can make it fail — on a JVM it never does, which is the whole difficulty.
   */
  Supplier<Optional<String>> probe = PublishHealthCheck::selfTest;

  /** The self-test's remembered answer; null until it has been asked. */
  private volatile Optional<String> probed;

  @Override
  public HealthCheckResponse call() {
    if (!enabled) {
      return HealthCheckResponse.named(NAME)
          .up()
          .withData("enabled", false)
          .withData("applies", "no: qits.eventstream.enabled is false, so nothing is published")
          .build();
    }
    Optional<String> selfTest = selfTestResult();
    Map<String, UnserializableEvents.Failure> failing = unserializable.current();
    HealthCheckResponseBuilder response =
        HealthCheckResponse.named(NAME)
            .withData("enabled", true)
            .withData("selfTest", selfTest.map(failure -> "failed: " + failure).orElse("passed"))
            .withData("unserializable", failing.size());
    if (!failing.isEmpty()) {
      response.withData("events", describe(failing));
    }
    if (selfTest.isPresent()) {
      return response
          .down()
          .withData(
              "reason",
              "this binary cannot serialise the eventstream wire types (in a native image: missing"
                  + " reflection); nothing it publishes can be sent")
          .build();
    }
    if (!failing.isEmpty()) {
      return response
          .down()
          .withData(
              "reason",
              failing.size()
                  + " event(s) could not be serialised on their latest attempt; the outbox keeps"
                  + " them, and a fixed deployment delivers them")
          .build();
    }
    return response.up().build();
  }

  /** Forget the remembered self-test, so the next call asks again. For tests only. */
  void reprobe() {
    probed = null;
  }

  private Optional<String> selfTestResult() {
    Optional<String> answer = probed;
    if (answer == null) {
      answer = run(probe);
      probed = answer;
      answer.ifPresent(
          failure ->
              LOG.warnf(
                  "eventstream self-test failed: this binary cannot serialise the wire types and"
                      + " will publish nothing (%s)",
                  failure));
    }
    return answer;
  }

  private static Optional<String> run(Supplier<Optional<String>> probe) {
    try {
      return probe.get();
    } catch (RuntimeException | LinkageError e) {
      return Optional.of(e.toString());
    }
  }

  /** {@code "<id> (<name>): <detail>"}, sorted by id so two polls of one state read the same. */
  private static String describe(Map<String, UnserializableEvents.Failure> failing) {
    StringBuilder text = new StringBuilder();
    int named = 0;
    Map<String, UnserializableEvents.Failure> sorted = new TreeMap<>(failing);
    for (Map.Entry<String, UnserializableEvents.Failure> entry : sorted.entrySet()) {
      if (named == NAMED_LIMIT) {
        text.append("; and ").append(failing.size() - NAMED_LIMIT).append(" more");
        break;
      }
      if (named++ > 0) {
        text.append("; ");
      }
      text.append(entry.getKey())
          .append(" (")
          .append(entry.getValue().name())
          .append("): ")
          .append(entry.getValue().detail());
    }
    return text.toString();
  }

  /**
   * The event type the self-test serialises. A {@link QitsEvent} like any other, so the extension's
   * build step registers it for reflection with every other implementor — and the mix-in has to
   * hide its {@code eventId} and {@code occurredAt}, or the payload below comes out wrong, which is
   * the quiet failure the mix-in's registration exists to prevent.
   */
  record SelfTestEvent(UUID eventId, Instant occurredAt, String probe) implements QitsEvent {}

  /**
   * The wire, both ways, through {@link CanonicalJson}: an event's payload and an envelope out, a
   * frame and a page in. Empty when every leg round-trips; otherwise which leg did not.
   */
  static Optional<String> selfTest() {
    Instant at = Instant.parse("2026-01-01T00:00:00Z");
    String payload =
        CanonicalJson.payload(
            new SelfTestEvent(UUID.fromString("00000000-0000-4000-8000-000000000000"), at, "p"));
    if (!"{\"probe\":\"p\"}".equals(payload)) {
      return Optional.of("an event's payload came out as " + payload + ", not {\"probe\":\"p\"}");
    }
    String envelope =
        CanonicalJson.envelope(
            new EventEnvelope("SelfTestEvent", at, payload, null, null, "probe", "probe"));
    if (!envelope.contains("\"name\":\"SelfTestEvent\"")
        || !envelope.contains("\"parentId\":null")) {
      return Optional.of("the envelope came out as " + envelope);
    }
    String frameText =
        "{\"id\":\"f\",\"name\":\"SelfTestEvent\",\"occurredAt\":\"2026-01-01T00:00:00Z\","
            + "\"payload\":\"{}\",\"publisher\":\"probe\"}";
    EventFrame frame = CanonicalJson.frame(frameText);
    if (!"f".equals(frame.id())
        || !at.equals(frame.occurredAt())
        || !"probe".equals(frame.publisher())) {
      return Optional.of("a frame read back as " + frame);
    }
    EventPage page = CanonicalJson.page("{\"events\":[" + frameText + "],\"nextCursor\":null}");
    if (page.events().size() != 1 || !"f".equals(page.events().get(0).id())) {
      return Optional.of("a page read back as " + page);
    }
    return Optional.empty();
  }
}
