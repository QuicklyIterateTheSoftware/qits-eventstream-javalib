package eu.wohlben.qits.eventstream.deployment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import eu.wohlben.qits.eventstream.QitsEvent;
import io.quarkus.arc.deployment.ValidationPhaseBuildItem.ValidationErrorBuildItem;
import io.quarkus.deployment.builditem.CombinedIndexBuildItem;
import io.quarkus.deployment.builditem.nativeimage.ReflectiveClassBuildItem;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.jboss.jandex.Index;
import org.junit.jupiter.api.Test;

/**
 * The two qits-2055 build steps, called directly against an index built here — the reflection list
 * is not observable from a JVM run at all (every class reflects there), so the build items the step
 * produces are the only thing to assert on. {@link RenamingEventRefusedTest} and {@link
 * SignatureComponentRefusedTest} prove the refusal also fails a real augmentation.
 */
class EventstreamProcessorTest {

  private final EventstreamProcessor processor = new EventstreamProcessor();

  private static CombinedIndexBuildItem index(Class<?>... classes) throws IOException {
    Index index = Index.of(classes);
    return new CombinedIndexBuildItem(index, index);
  }

  private List<ReflectiveClassBuildItem> reflection(Class<?>... classes) throws IOException {
    List<ReflectiveClassBuildItem> items = new ArrayList<>();
    processor.registerWireReflection(index(classes), items::add);
    return items;
  }

  private static Set<String> registered(List<ReflectiveClassBuildItem> items) {
    return items.stream()
        .flatMap(item -> item.getClassNames().stream())
        .collect(Collectors.toSet());
  }

  @Test
  void theLibrarysWireTypesAreRegisteredEvenWithNoEventInTheIndex() throws IOException {
    Set<String> names = registered(reflection(QitsEvent.class));

    assertTrue(names.containsAll(EventstreamProcessor.WIRE_TYPES), names.toString());
    assertTrue(names.contains("eu.wohlben.qits.eventstream.control.CanonicalJson$QitsEventMixin"));
    assertTrue(names.contains("eu.wohlben.qits.eventstream.selection.EventSelection$Condition"));
  }

  /** A typo in a string-named class would register nothing and say nothing; load each one. */
  @Test
  void everyWireTypeNamedIsARealClass() throws ClassNotFoundException {
    for (String name : EventstreamProcessor.WIRE_TYPES) {
      Class.forName(name, false, getClass().getClassLoader());
    }
  }

  @Test
  void everyQitsEventImplementorIsRegisteredForWhatJacksonNeeds() throws IOException {
    List<ReflectiveClassBuildItem> items =
        reflection(
            QitsEvent.class,
            Fixtures.ThingShipped.class,
            Fixtures.AuditEvent.class,
            Fixtures.AbstractAudit.class,
            Fixtures.Audited.class);
    Set<String> names = registered(items);

    assertTrue(names.contains(Fixtures.ThingShipped.class.getName()), names.toString());
    assertTrue(names.contains(Fixtures.Audited.class.getName()), "through a sub-interface");
    assertTrue(names.contains(Fixtures.AbstractAudit.class.getName()), "its base's methods too");
    assertFalse(names.contains(Fixtures.AuditEvent.class.getName()), "an interface has no body");
    for (ReflectiveClassBuildItem item : items) {
      assertTrue(item.isConstructors() && item.isMethods() && item.isFields(), item.getReason());
    }
  }

  @Test
  void aNameComponentIsRefusedNamingTheClassAndWhy() throws IOException {
    List<String> problems =
        EventstreamProcessor.renamingEvents(
            index(QitsEvent.class, Fixtures.ThingNamed.class).getIndex());

    assertEquals(1, problems.size(), problems.toString());
    String problem = problems.get(0);
    assertTrue(problem.startsWith(Fixtures.ThingNamed.class.getName()), problem);
    assertTrue(problem.contains("record component `name`"), problem);
    assertTrue(problem.contains("instead of its signature (ThingNamed)"), problem);
  }

  @Test
  void aSignatureComponentIsRefusedButASignatureMethodIsNot() throws IOException {
    List<String> problems =
        EventstreamProcessor.renamingEvents(
            index(QitsEvent.class, Fixtures.ThingSigned.class, Fixtures.ThingOverridden.class)
                .getIndex());

    assertEquals(1, problems.size(), problems.toString());
    assertTrue(problems.get(0).startsWith(Fixtures.ThingSigned.class.getName()), problems.get(0));
    assertTrue(problems.get(0).contains("record component `signature`"), problems.get(0));
  }

  @Test
  void anExplicitNameMethodIsRefusedToo() throws IOException {
    List<String> problems =
        EventstreamProcessor.renamingEvents(
            index(QitsEvent.class, Fixtures.ThingRelabelled.class).getIndex());

    assertEquals(1, problems.size(), problems.toString());
    assertTrue(problems.get(0).contains("method name()"), problems.get(0));
  }

  @Test
  void ordinaryEventsProduceNoValidationError() throws IOException {
    List<ValidationErrorBuildItem> errors = new ArrayList<>();
    processor.refuseRenamingEvents(
        index(QitsEvent.class, Fixtures.ThingShipped.class, Fixtures.ThingOverridden.class),
        errors::add);

    assertTrue(errors.isEmpty());
  }
}
