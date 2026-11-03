package eu.wohlben.qits.eventstream.control;

import eu.wohlben.qits.eventstream.EventStreamBearer;
import io.quarkus.arc.DefaultBean;
import io.quarkus.oidc.client.OidcClient;
import io.quarkus.oidc.client.OidcClients;
import io.quarkus.oidc.client.Tokens;
import io.quarkus.oidc.client.runtime.DisabledOidcClientException;
import io.quarkus.oidc.client.runtime.TokensHelper;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.jboss.logging.Logger;

/**
 * The shipped {@link EventStreamBearer}: the consuming service's named OIDC client {@code qits}
 * (qits-1328) — the same client, identity and {@code qits:system} role every other service-to-service
 * call of the service uses, so the library needs no client settings of its own.
 *
 * <p><b>One token, reused.</b> {@link TokensHelper} keeps the last grant and only goes back to the
 * idp when the access token has expired (or is inside the client's refresh skew), refreshing with
 * the refresh token where the grant carried one; concurrent callers share one request. A publish
 * therefore costs no mint, and a stream redial costs none either.
 *
 * <p><b>Fails closed on three terms</b>, each with its own message naming {@value #CLIENT_KEY}: no
 * such client is configured ({@code OidcClients} has none by that name), it is disabled ({@code
 * client-enabled=false}, or a configuration the extension could not initialise), or the mint failed
 * or answered a blank token. The failure is logged at WARN at most once per {@link #WARN_EVERY} —
 * the outbox sweep, the catch-up and the stream's redial all ask again on their own schedules, and a
 * WARN per ask would bury the one line that matters — and at DEBUG otherwise. Recovery is logged
 * once at INFO.
 */
@ApplicationScoped
@DefaultBean
public class QitsClientBearer implements EventStreamBearer {

  private static final Logger LOG = Logger.getLogger(QitsClientBearer.class);

  /** The named client, as {@code OidcClients} keys it. */
  static final String CLIENT = "qits";

  /** The client as an operator configures it — what every failure message names. */
  static final String CLIENT_KEY = "quarkus.oidc-client." + CLIENT;

  /** At most one WARN per this long while the bearer keeps failing. */
  static final Duration WARN_EVERY = Duration.ofMinutes(1);

  private final OidcClients clients;

  private final TokensHelper tokens = new TokensHelper();

  /** {@link System#nanoTime()} of the last WARN; only meaningful once {@link #warned} is set. */
  private final AtomicLong lastWarn = new AtomicLong();

  private final AtomicBoolean warned = new AtomicBoolean();

  /** How many WARNs this bean has said — what the throttle is tested by, independent of logging. */
  final AtomicInteger warnings = new AtomicInteger();

  /** Whether the last answer was a failure, so the first success after it is said once. */
  private final AtomicBoolean failing = new AtomicBoolean();

  @Inject
  public QitsClientBearer(OidcClients clients) {
    this.clients = clients;
  }

  @Override
  public Uni<String> authorization() {
    OidcClient client;
    try {
      client = clients.getClient(CLIENT);
    } catch (RuntimeException e) {
      return missing("no OIDC client " + CLIENT_KEY + " could be looked up: " + e, e);
    }
    if (client == null) {
      return missing("no OIDC client " + CLIENT_KEY + " is configured", null);
    }
    return Uni.createFrom()
        // deferred: a disabled client throws from getTokens itself rather than failing the Uni.
        .deferred(() -> tokens.getTokens(client))
        .onItem()
        .transformToUni(this::header)
        .onFailure(failure -> !(failure instanceof Missing))
        .recoverWithUni(failure -> missing(describe(failure), failure))
        .onItem()
        .invoke(
            ignored -> {
              if (failing.compareAndSet(true, false)) {
                LOG.infof("OIDC client %s mints again; calls to qits-events carry its bearer", CLIENT_KEY);
              }
            });
  }

  private Uni<String> header(Tokens minted) {
    String token = minted == null ? null : minted.getAccessToken();
    if (token == null || token.isBlank()) {
      return missing("OIDC client " + CLIENT_KEY + " answered a blank access token", null);
    }
    return Uni.createFrom().item("Bearer " + token);
  }

  private static String describe(Throwable failure) {
    if (failure instanceof DisabledOidcClientException) {
      return "OIDC client " + CLIENT_KEY + " is disabled (" + failure.getMessage() + ")";
    }
    return "OIDC client " + CLIENT_KEY + " could not mint a token: " + failure;
  }

  private Uni<String> missing(String reason, Throwable cause) {
    failing.set(true);
    long now = System.nanoTime();
    long previous = lastWarn.get();
    boolean due =
        (!warned.get() || now - previous >= WARN_EVERY.toNanos())
            && lastWarn.compareAndSet(previous, now);
    if (due) {
      warned.set(true);
      warnings.incrementAndGet();
      LOG.warnf(
          "%s; calls to qits-events fail closed and nothing is sent (said at most once every %s)",
          reason, WARN_EVERY);
    } else {
      LOG.debugf("%s; call to qits-events not made", reason);
    }
    return Uni.createFrom().failure(cause == null ? new Missing(reason) : new Missing(reason, cause));
  }
}
