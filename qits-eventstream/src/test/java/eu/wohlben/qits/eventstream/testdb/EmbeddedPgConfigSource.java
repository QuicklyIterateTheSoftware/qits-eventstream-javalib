package eu.wohlben.qits.eventstream.testdb;

import java.util.Map;
import java.util.Set;
import org.eclipse.microprofile.config.spi.ConfigSource;

/**
 * Hands the running {@link EmbeddedPg} to every {@code @QuarkusTest} here, as the three keys a
 * deployment would supply: {@code jdbc.url}, {@code username}, {@code password}.
 *
 * <p>It is a config source rather than three lines in {@code
 * src/test/resources/application.properties} because the port is chosen at run time — the instance
 * takes a free one, so nothing can be written down ahead of the JVM that starts it.
 *
 * <p>The ordinal sits above application.properties (250) so this wins over both the shipped defaults
 * in this jar's {@code META-INF/microprofile-config.properties} (100) and anything the test
 * properties file might carry, and it is registered through {@code META-INF/services}, which is how
 * a config source joins a Quarkus application without being a bean.
 *
 * <p>What it supplies are the same three keys the shipped file resolves from {@code
 * QITS_RESOURCE_EVENTSTREAM_*}. The suite sets the values rather than the variables on purpose: the
 * shipped expressions have no defaults, and a test run that also had to export environment variables
 * would be a test run that could not say what happens when they are missing.
 */
public class EmbeddedPgConfigSource implements ConfigSource {

  /**
   * This repository's database on the embedded instance. Named for the library rather than for a
   * module, so a consumer's suite spawning its own postgres on the same host cannot collide with it.
   */
  private static final String DATABASE = "eventstream_test";

  /**
   * A second database backing the suite's DEFAULT persistence unit — the consumer's shape, stood up
   * so {@code CausationRowStampingTest} can prove the entity-listener stamping against the same
   * arrangement a service has: its own entities in the default unit, this jar's outbox in the named
   * one. Separate databases for the same reason a consumer has separate ones.
   */
  private static final String CONSUMER_DATABASE = "eventstream_consumer_test";

  private static final String PREFIX = "quarkus.datasource.eventstream.";

  private static final String DEFAULT_PREFIX = "quarkus.datasource.";

  private final Map<String, String> values =
      Map.of(
          PREFIX + "jdbc.url", EmbeddedPg.url(DATABASE),
          PREFIX + "username", EmbeddedPg.USER,
          PREFIX + "password", EmbeddedPg.PASSWORD,
          DEFAULT_PREFIX + "jdbc.url", EmbeddedPg.url(CONSUMER_DATABASE),
          DEFAULT_PREFIX + "username", EmbeddedPg.USER,
          DEFAULT_PREFIX + "password", EmbeddedPg.PASSWORD);

  @Override
  public int getOrdinal() {
    return 500;
  }

  @Override
  public Set<String> getPropertyNames() {
    return values.keySet();
  }

  @Override
  public String getValue(String propertyName) {
    return values.get(propertyName);
  }

  @Override
  public String getName() {
    return "embedded-pg";
  }
}
