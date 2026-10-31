package eu.wohlben.qits.eventstream.consumertest;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;

/**
 * The outbox's store, as a consumer's suite provides it: a postgres zonky spawns as a child process
 * (the library's clone-alone, no-docker rule), handed to the eventstream datasource. The library's
 * own {@code testdb/EmbeddedPg} lives in its test sources, which a consumer cannot see.
 */
public class EmbeddedStore implements QuarkusTestResourceLifecycleManager {

  private EmbeddedPostgres postgres;

  @Override
  public Map<String, String> start() {
    try {
      postgres = EmbeddedPostgres.builder().start();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return Map.of(
        "quarkus.datasource.eventstream.jdbc.url",
        postgres.getJdbcUrl("postgres", "postgres"),
        "quarkus.datasource.eventstream.username",
        "postgres",
        "quarkus.datasource.eventstream.password",
        "postgres");
  }

  @Override
  public void stop() {
    if (postgres != null) {
      try {
        postgres.close();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }
}
