package eu.wohlben.qits.eventstream.contracts;

import eu.wohlben.qits.eventstream.QitsDurableEventListener;
import eu.wohlben.qits.eventstream.QitsEventBus;
import eu.wohlben.qits.eventstream.QitsRawEventListener;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;
import org.jboss.jandex.ClassInfo;
import org.jboss.jandex.DotName;
import org.jboss.jandex.Index;
import org.jboss.jandex.Indexer;

/**
 * <b>The guard</b> (qits-1163): an {@link EventstreamPact} is only true if it was given every
 * listener the service has and told whether it publishes. This reads the service's compiled
 * classes — Jandex over each {@code .class} file under the given directories, nothing loaded, no
 * code run — and names what the pact's inputs leave out:
 *
 * <ul>
 *   <li>every concrete class implementing {@link QitsDurableEventListener} or {@link
 *       QitsRawEventListener}, directly or through a superclass or subinterface, whose class was not
 *       passed to {@link EventstreamPact#listeners};
 *   <li>every class that uses {@link QitsEventBus} (a reference to it in the class's constant pool,
 *       which a call to {@code publish} always leaves) while {@link EventstreamPact#publishes} is
 *       false — and, the other way round, {@code publishes(true)} with no such class at all.
 * </ul>
 *
 * <p>Typed {@code QitsEventListener}s are not guarded: they are live-only, ride the websocket, and
 * make no REST call.
 */
final class EventstreamGuard {

  static final DotName DURABLE = DotName.createSimple(QitsDurableEventListener.class.getName());
  static final DotName RAW = DotName.createSimple(QitsRawEventListener.class.getName());
  static final DotName BUS = DotName.createSimple(QitsEventBus.class.getName());

  private EventstreamGuard() {}

  /** What the pact leaves out, one sentence per class, sorted; empty when it covers everything. */
  static List<String> uncovered(List<Path> classesDirs, Set<String> passed, boolean publishes) {
    Index index = index(classesDirs);
    Set<String> problems = new TreeSet<>();
    for (DotName listenerType : List.of(DURABLE, RAW)) {
      for (ClassInfo listener : index.getAllKnownImplementations(listenerType)) {
        if (listener.isInterface() || java.lang.reflect.Modifier.isAbstract(listener.flags())) {
          continue;
        }
        String name = listener.name().toString();
        if (!passed.contains(name)) {
          problems.add(
              name + " is a " + listenerType.withoutPackagePrefix() + " the pact was not given: pass an instance"
                  + " to listeners(...)");
        }
      }
    }
    List<String> publishers = new ArrayList<>();
    for (ClassInfo user : index.getKnownUsers(BUS)) {
      publishers.add(user.name().toString());
    }
    if (!publishes) {
      for (String publisher : publishers) {
        problems.add(
            publisher + " uses QitsEventBus, so the service publishes: set publishes(true)");
      }
    } else if (publishers.isEmpty()) {
      problems.add(
          "publishes(true), but no class under " + classesDirs + " uses QitsEventBus: set"
              + " publishes(false), or guard the directory that holds the publisher");
    }
    return List.copyOf(problems);
  }

  private static Index index(List<Path> classesDirs) {
    Indexer indexer = new Indexer();
    for (Path dir : classesDirs) {
      if (!Files.isDirectory(dir)) {
        throw new IllegalArgumentException(
            dir.toAbsolutePath() + " is not a directory of compiled classes; guard(...) takes each"
                + " module's target/classes (run after compile)");
      }
      try (Stream<Path> files = Files.walk(dir)) {
        for (Path file : (Iterable<Path>) files::iterator) {
          if (file.toString().endsWith(".class") && !file.endsWith("module-info.class")) {
            try (InputStream in = Files.newInputStream(file)) {
              indexer.index(in);
            }
          }
        }
      } catch (IOException e) {
        throw new UncheckedIOException("indexing " + dir, e);
      }
    }
    return indexer.complete();
  }
}
