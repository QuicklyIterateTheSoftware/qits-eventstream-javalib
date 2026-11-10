package eu.wohlben.qits.eventstream.deployment;

import io.quarkus.arc.deployment.ValidationPhaseBuildItem.ValidationErrorBuildItem;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.builditem.CombinedIndexBuildItem;
import io.quarkus.deployment.builditem.RunTimeConfigurationDefaultBuildItem;
import io.quarkus.deployment.builditem.nativeimage.ReflectiveClassBuildItem;
import io.quarkus.deployment.pkg.builditem.CurateOutcomeBuildItem;
import io.quarkus.maven.dependency.ResolvedDependency;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import org.jboss.jandex.ClassInfo;
import org.jboss.jandex.DotName;
import org.jboss.jandex.IndexView;
import org.jboss.jandex.MethodInfo;

/**
 * The build half of qits-eventstream. It exists for the facts only augmentation has: <b>which
 * application is publishing</b> (qits-1770), and <b>which classes it publishes</b> (qits-2055).
 *
 * <p>Every envelope carries {@code publisher}, the dispatching service, read by {@code QitsEventBus}
 * from {@code qits.eventstream.publisher}. Its default is the consuming application's Maven {@code
 * groupId:artifactId} ({@code eu.wohlben.qits:qits-ci-service}). The library jar cannot know that,
 * and nothing at run time does either — a native image has no pom, and {@code quarkus.application.name}
 * is a different, renameable string. Augmentation is the one moment the application model is in
 * hand, so the value is read there and recorded as a runtime DEFAULT: the same in JVM mode, in a
 * native image and under {@code @QuarkusTest}, and below every real config source, so the
 * application property or {@code QITS_EVENTSTREAM_PUBLISHER} overrides it.
 *
 * <p><b>The reflection this library's wire needs is registered here, not by each consumer</b>
 * (qits-2055). {@code CanonicalJson} builds its own {@code ObjectMapper} on purpose, so the graph it
 * serialises is invisible to every build step that scans for Jackson types — and every consuming
 * deployable used to hand-list {@code EventEnvelope}, {@code EventFrame}, {@code EventPage} and the
 * mix-in by string in a {@code @RegisterForReflection} of its own, with a silent failure for each
 * one forgotten: a PUT body that could not be written, or a payload that quietly carried {@code
 * eventId}. This step knows the list because it is the library's list, and it knows the consumer's
 * event classes because they are exactly the implementors of {@code QitsEvent} in the index.
 *
 * <p><b>And a {@code QitsEvent} that would rename itself on the wire fails the build.</b> {@code
 * QitsEvent.name()} defaults to {@code signature()}, and {@code EventEnvelope.of} stamps {@code
 * name()} as the wire name. A record component called {@code name} — or any {@code name()} the
 * class declares — overrides it: the event is published under the component's <em>value</em>
 * instead of its class name, so no {@code QitsEventListener} ever hears it, and the mix-in that hides
 * {@code QitsEvent}'s methods drops the field from the payload as well. A record component called
 * {@code signature} does the same thing one step earlier. Both are invisible in a JVM test that only
 * checks the publish did not throw, so they are refused where nobody can miss them. An explicit
 * {@code signature()} <em>method</em> stays legal: that is the documented override, kept in step
 * with the class name by its author.
 */
class EventstreamProcessor {

  static final String PUBLISHER = "qits.eventstream.publisher";

  static final DotName QITS_EVENT = DotName.createSimple("eu.wohlben.qits.eventstream.QitsEvent");

  /**
   * The library's own wire types, by name: the deployment module must not load runtime classes it
   * merely names, and two of these (the page and the mix-in) are not public. The nested {@code
   * EventSelection} types are here because a consumer binds a selection from JSON with Jackson too.
   */
  static final List<String> WIRE_TYPES =
      List.of(
          "eu.wohlben.qits.eventstream.control.EventEnvelope",
          "eu.wohlben.qits.eventstream.control.EventFrame",
          "eu.wohlben.qits.eventstream.control.EventPage",
          // Jackson reads a mix-in's @JsonIgnores with getDeclaredMethods(): left out, a green
          // native build publishes payloads that carry eventId (AGENTS.md, "the reflection").
          "eu.wohlben.qits.eventstream.control.CanonicalJson$QitsEventMixin",
          "eu.wohlben.qits.eventstream.selection.EventSelection",
          "eu.wohlben.qits.eventstream.selection.EventSelection$Condition",
          "eu.wohlben.qits.eventstream.selection.EventSelection$Op");

  @BuildStep
  RunTimeConfigurationDefaultBuildItem publisherDefault(CurateOutcomeBuildItem curate) {
    ResolvedDependency app = curate.getApplicationModel().getAppArtifact();
    return new RunTimeConfigurationDefaultBuildItem(
        PUBLISHER, app.getGroupId() + ":" + app.getArtifactId());
  }

  /**
   * The wire types and every {@code QitsEvent} implementor in the index, for constructors, methods
   * and fields — what Jackson needs to write a payload, bind one back for a typed listener, and
   * read the mix-in's annotations.
   *
   * <p>The index is the combined one: the application's own classes and every indexed dependency,
   * which is where a deployable's event classes live (its own module, or an events jar that ships a
   * Jandex index). A {@code QitsEvent} in an <em>unindexed</em> jar is not seen here, and neither is
   * a payload's own nested type — a record component of a consumer's record type, say. Those stay
   * the consumer's to register, as any Jackson type outside an index does.
   */
  @BuildStep
  void registerWireReflection(
      CombinedIndexBuildItem combinedIndex, BuildProducer<ReflectiveClassBuildItem> reflection) {
    reflection.produce(
        ReflectiveClassBuildItem.builder(WIRE_TYPES)
            .constructors()
            .methods()
            .fields()
            .reason("qits-eventstream wire types")
            .build());
    List<String> events = eventClassNames(combinedIndex.getIndex());
    if (!events.isEmpty()) {
      reflection.produce(
          ReflectiveClassBuildItem.builder(events)
              .constructors()
              .methods()
              .fields()
              .reason("qits-eventstream QitsEvent implementors")
              .build());
    }
  }

  /**
   * Refuses, as a deployment problem, every {@code QitsEvent} that would publish under a name other
   * than its own (see the class javadoc). Every offender is named at once, rather than the first.
   */
  @BuildStep
  void refuseRenamingEvents(
      CombinedIndexBuildItem combinedIndex, BuildProducer<ValidationErrorBuildItem> errors) {
    List<Throwable> problems = new ArrayList<>();
    for (String problem : renamingEvents(combinedIndex.getIndex())) {
      problems.add(new IllegalStateException(problem));
    }
    if (!problems.isEmpty()) {
      errors.produce(new ValidationErrorBuildItem(problems));
    }
  }

  /** Every class implementing {@code QitsEvent}, directly or not, sorted so a build is stable. */
  static List<String> eventClassNames(IndexView index) {
    TreeSet<String> names = new TreeSet<>();
    for (ClassInfo event : index.getAllKnownImplementations(QITS_EVENT)) {
      if (!event.isInterface()) {
        names.add(event.name().toString());
      }
    }
    return List.copyOf(names);
  }

  /** One message per offending class and member; empty when every event keeps its own name. */
  static List<String> renamingEvents(IndexView index) {
    List<String> problems = new ArrayList<>();
    for (String className : eventClassNames(index)) {
      ClassInfo event = index.getClassByName(className);
      MethodInfo name = event.method("name");
      if (name != null && !name.isSynthetic()) {
        String what =
            event.isRecord() && event.recordComponent("name") != null
                ? "record component `name`"
                : "method name()";
        problems.add(
            className
                + " declares "
                + what
                + ", which overrides QitsEvent.name(): the event would be published under that"
                + " value instead of its signature ("
                + event.simpleName()
                + "), so no listener would hear it, and the field would be dropped from the"
                + " payload. Rename it (qits-2055).");
      }
      if (event.isRecord() && event.recordComponent("signature") != null) {
        problems.add(
            className
                + " declares record component `signature`, which overrides QitsEvent.signature():"
                + " the event would be published and subscribed under that value instead of its"
                + " class name ("
                + event.simpleName()
                + "), and the field would be dropped from the payload. Rename it; an explicit"
                + " signature() method is the supported override (qits-2055).");
      }
    }
    return problems;
  }
}
