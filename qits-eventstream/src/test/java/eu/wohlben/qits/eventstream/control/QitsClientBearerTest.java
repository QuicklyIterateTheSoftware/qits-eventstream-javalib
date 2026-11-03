package eu.wohlben.qits.eventstream.control;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import eu.wohlben.qits.eventstream.EventStreamBearer;
import io.quarkus.oidc.client.OidcClient;
import io.quarkus.oidc.client.OidcClients;
import io.quarkus.oidc.client.Tokens;
import io.quarkus.oidc.client.runtime.DisabledOidcClientException;
import io.quarkus.oidc.client.runtime.OidcClientConfig;
import io.smallrye.mutiny.Uni;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

/**
 * The shipped bearer without Quarkus (qits-1328): the named client {@code qits} looked up through
 * {@link OidcClients}, its token reused until it expires, and every way of not having one failing
 * closed with a message that names {@code quarkus.oidc-client.qits}, said at WARN once rather than
 * on every ask.
 */
class QitsClientBearerTest {

  private static final Duration AWAIT = Duration.ofSeconds(5);

  @Test
  void theNamedClientsTokenIsTheHeaderAndIsReusedUntilItExpires() {
    FakeClient client = new FakeClient(() -> Uni.createFrom().item(tokens("t1", 3600)));
    QitsClientBearer bearer = new QitsClientBearer(new FakeClients(Map.of("qits", client)));

    assertEquals("Bearer t1", bearer.authorization().await().atMost(AWAIT));
    assertEquals("Bearer t1", bearer.authorization().await().atMost(AWAIT));
    assertEquals("Bearer t1", bearer.authorization().await().atMost(AWAIT));
    assertEquals(1, client.mints.get(), "one mint serves every call while the token is fresh");
  }

  @Test
  void anExpiredTokenIsMintedAgain() {
    AtomicInteger n = new AtomicInteger();
    FakeClient client =
        new FakeClient(() -> Uni.createFrom().item(tokens("t" + n.incrementAndGet(), -60)));
    QitsClientBearer bearer = new QitsClientBearer(new FakeClients(Map.of("qits", client)));

    assertEquals("Bearer t1", bearer.authorization().await().atMost(AWAIT));
    assertEquals("Bearer t2", bearer.authorization().await().atMost(AWAIT));
    assertEquals(2, client.mints.get());
  }

  @Test
  void noConfiguredClientFailsClosedNamingIt() {
    QitsClientBearer bearer = new QitsClientBearer(new FakeClients(Map.of()));

    EventStreamBearer.Missing missing = missing(bearer);
    assertTrue(
        missing.getMessage().contains("no OIDC client quarkus.oidc-client.qits is configured"),
        missing.getMessage());
  }

  @Test
  void aDisabledClientFailsClosedNamingIt() {
    FakeClient client =
        new FakeClient(
            () -> {
              // What the extension's DisabledOidcClient does: throw from getTokens itself.
              throw new DisabledOidcClientException("'qits' client configuration is disabled");
            });
    QitsClientBearer bearer = new QitsClientBearer(new FakeClients(Map.of("qits", client)));

    EventStreamBearer.Missing missing = missing(bearer);
    assertTrue(missing.getMessage().contains("quarkus.oidc-client.qits is disabled"), missing.getMessage());
  }

  @Test
  void aFailedMintFailsClosedAndTheNextAskTriesAgain() {
    AtomicInteger n = new AtomicInteger();
    FakeClient client =
        new FakeClient(
            () ->
                n.incrementAndGet() == 1
                    ? Uni.createFrom().failure(new IllegalStateException("idp said 401"))
                    : Uni.createFrom().item(tokens("t2", 3600)));
    QitsClientBearer bearer = new QitsClientBearer(new FakeClients(Map.of("qits", client)));

    EventStreamBearer.Missing missing = missing(bearer);
    assertTrue(missing.getMessage().contains("quarkus.oidc-client.qits could not mint"), missing.getMessage());
    assertEquals("Bearer t2", bearer.authorization().await().atMost(AWAIT), "a failure is not cached");
  }

  @Test
  void aBlankTokenFailsClosed() {
    FakeClient client = new FakeClient(() -> Uni.createFrom().item(tokens(" ", 3600)));
    QitsClientBearer bearer = new QitsClientBearer(new FakeClients(Map.of("qits", client)));

    assertTrue(missing(bearer).getMessage().contains("blank access token"));
  }

  @Test
  void aBearerThatKeepsFailingWarnsOnceNotOnEveryAsk() {
    QitsClientBearer bearer = new QitsClientBearer(new FakeClients(Map.of()));
    for (int i = 0; i < 50; i++) {
      missing(bearer);
    }
    assertEquals(1, bearer.warnings.get(), "one WARN per minute, not one per attempt");
  }

  // -- fakes ---------------------------------------------------------------------------------------

  private static EventStreamBearer.Missing missing(QitsClientBearer bearer) {
    return assertThrows(
        EventStreamBearer.Missing.class, () -> bearer.authorization().await().atMost(AWAIT));
  }

  private static Tokens tokens(String access, long expiresInSeconds) {
    return new Tokens(
        access,
        Instant.now().getEpochSecond() + expiresInSeconds,
        null,
        null,
        null,
        null,
        "test-client");
  }

  private static final class FakeClient implements OidcClient {
    final AtomicInteger mints = new AtomicInteger();
    final Supplier<Uni<Tokens>> answer;

    FakeClient(Supplier<Uni<Tokens>> answer) {
      this.answer = answer;
    }

    @Override
    public Uni<Tokens> getTokens(Map<String, String> additionalGrantParameters) {
      mints.incrementAndGet();
      return answer.get();
    }

    @Override
    public Uni<Tokens> refreshTokens(String refreshToken, Map<String, String> additionalGrantParameters) {
      throw new UnsupportedOperationException("a client-credentials grant carries no refresh token");
    }

    @Override
    public Uni<Boolean> revokeAccessToken(String accessToken, Map<String, String> additionalParameters) {
      return Uni.createFrom().item(true);
    }

    @Override
    public void close() {}
  }

  private record FakeClients(Map<String, OidcClient> named) implements OidcClients {
    @Override
    public OidcClient getClient() {
      throw new UnsupportedOperationException("the library never asks for the default client");
    }

    @Override
    public OidcClient getClient(String id) {
      return named.get(id);
    }

    @Override
    public Uni<OidcClient> newClient(OidcClientConfig config) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void close() {}
  }
}
