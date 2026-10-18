package eu.wohlben.qits.eventstream.contracts;

import eu.wohlben.qits.eventstream.QitsEventBus;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URL;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

/**
 * <b>The library a service's call to qits-events goes through</b> (qits-1163): the {@code qits-via}
 * reference every interaction of an {@link EventstreamPact} carries, next to {@code qits-call} and
 * {@code qits-trigger}.
 *
 * <pre>{@code
 * "qits-via": {"repository": "qits-eventstream-javalib",
 *              "artifact": "eu.wohlben.qits:qits-eventstream",
 *              "version": "<the qits-eventstream version on the classpath>"}
 * }</pre>
 *
 * <p><b>Which version.</b> {@link #current()} names the qits-eventstream that actually runs the
 * probe, which is the one the consuming service pins (its direct dependency wins over this kit's
 * transitive one). It is read from that jar's own {@code
 * META-INF/maven/eu.wohlben.qits/qits-eventstream/pom.properties}, which Maven writes into every
 * jar. Where the library is not a jar — this repository's own reactor build, which puts {@code
 * qits-eventstream/target/classes} on the path — the kit's own version is used, filtered in at build
 * time into {@code META-INF/maven/eu.wohlben.qits/qits-eventstream-contracts/qits-via.properties};
 * in this reactor the two are the same version by construction.
 *
 * @param repository the library's repository name
 * @param artifact the library's maven coordinate, {@code groupId:artifactId}
 * @param version the library version the consuming service pins
 */
public record EventstreamVia(String repository, String artifact, String version) {

  public static final String REPOSITORY = "qits-eventstream-javalib";
  public static final String GROUP_ID = "eu.wohlben.qits";
  public static final String ARTIFACT_ID = "qits-eventstream";
  public static final String ARTIFACT = GROUP_ID + ":" + ARTIFACT_ID;

  static final String LIBRARY_POM_PROPERTIES =
      "META-INF/maven/" + GROUP_ID + "/" + ARTIFACT_ID + "/pom.properties";
  static final String KIT_PROPERTIES =
      "META-INF/maven/" + GROUP_ID + "/qits-eventstream-contracts/qits-via.properties";

  public EventstreamVia {
    Objects.requireNonNull(repository, "repository");
    Objects.requireNonNull(artifact, "artifact");
    Objects.requireNonNull(version, "version");
  }

  /** qits-eventstream at the version on this classpath; see the class javadoc. */
  public static EventstreamVia current() {
    return new EventstreamVia(REPOSITORY, ARTIFACT, libraryVersion());
  }

  /** The {@code qits-via} group: repository, artifact, then version, all strings. */
  public Map<String, String> reference() {
    Map<String, String> ref = new LinkedHashMap<>();
    ref.put("repository", repository);
    ref.put("artifact", artifact);
    ref.put("version", version);
    return ref;
  }

  static String libraryVersion() {
    String fromJar = fromLibraryJar();
    if (fromJar != null) {
      return fromJar;
    }
    String fromKit = read(EventstreamVia.class.getClassLoader().getResource(KIT_PROPERTIES));
    if (fromKit == null || fromKit.isBlank() || fromKit.contains("${")) {
      throw new IllegalStateException(
          "cannot tell which qits-eventstream version is on the classpath: neither "
              + LIBRARY_POM_PROPERTIES + " beside " + QitsEventBus.class.getName() + " nor "
              + KIT_PROPERTIES + " holds a version");
    }
    return fromKit;
  }

  /** {@code pom.properties} of the very jar {@link QitsEventBus} was loaded from, or null. */
  private static String fromLibraryJar() {
    URL clazz =
        QitsEventBus.class.getResource(QitsEventBus.class.getSimpleName() + ".class");
    if (clazz == null || !"jar".equals(clazz.getProtocol())) {
      return null;
    }
    String spec = clazz.toString();
    int bang = spec.indexOf("!/");
    if (bang < 0) {
      return null;
    }
    try {
      return read(URI.create(spec.substring(0, bang + 2) + LIBRARY_POM_PROPERTIES).toURL());
    } catch (IOException | IllegalArgumentException e) {
      return null;
    }
  }

  private static String read(URL url) {
    if (url == null) {
      return null;
    }
    Properties properties = new Properties();
    try (InputStream in = url.openStream()) {
      properties.load(in);
    } catch (java.io.FileNotFoundException e) {
      return null;
    } catch (IOException e) {
      throw new UncheckedIOException("reading " + url, e);
    }
    String version = properties.getProperty("version");
    return version == null ? null : version.trim();
  }
}
