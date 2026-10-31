package eu.wohlben.qits.eventstream.deployment;

import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.builditem.RunTimeConfigurationDefaultBuildItem;
import io.quarkus.deployment.pkg.builditem.CurateOutcomeBuildItem;
import io.quarkus.maven.dependency.ResolvedDependency;

/**
 * The build half of qits-eventstream (qits-1770). It exists for one fact no runtime config holds:
 * <b>which application is publishing.</b>
 *
 * <p>Every envelope carries {@code publisher}, the dispatching service, read by {@code QitsEventBus}
 * from {@code qits.eventstream.publisher}. Its default is the consuming application's Maven {@code
 * groupId:artifactId} ({@code eu.wohlben.qits:qits-ci-service}). The library jar cannot know that,
 * and nothing at run time does either — a native image has no pom, and {@code quarkus.application.name}
 * is a different, renameable string. Augmentation is the one moment the application model is in
 * hand, so the value is read there and recorded as a runtime DEFAULT: the same in JVM mode, in a
 * native image and under {@code @QuarkusTest}, and below every real config source, so the
 * application property or {@code QITS_EVENTSTREAM_PUBLISHER} overrides it.
 */
class EventstreamProcessor {

  static final String PUBLISHER = "qits.eventstream.publisher";

  @BuildStep
  RunTimeConfigurationDefaultBuildItem publisherDefault(CurateOutcomeBuildItem curate) {
    ResolvedDependency app = curate.getApplicationModel().getAppArtifact();
    return new RunTimeConfigurationDefaultBuildItem(
        PUBLISHER, app.getGroupId() + ":" + app.getArtifactId());
  }
}
