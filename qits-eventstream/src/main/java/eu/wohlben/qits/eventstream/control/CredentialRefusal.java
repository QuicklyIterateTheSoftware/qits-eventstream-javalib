package eu.wohlben.qits.eventstream.control;

import io.vertx.core.http.UpgradeRejectedException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.jboss.logging.Logger;

/**
 * The one loud line for "qits-events, or the edge in front of it, refused this service's
 * credential" (qits-1417): a {@code 401} or {@code 403} on the stream upgrade, a publish or a
 * catch-up read.
 *
 * <p><b>Why it has to be loud.</b> {@code qits.events.url} names qits-events' public host, so every
 * call crosses the edge, and the edge answers a call with no acceptable bearer {@code 401} before
 * any upstream is involved. {@link QitsClientBearer} already says so when there is no token to send;
 * this is the other half — a token was sent and refused (a client the idp does not know, a secret
 * that does not match, a token without the platform audience). Without it the subscriber's answer to
 * a refused upgrade is the same DEBUG-level redial as for a dropped stream, which is a service that
 * looks up and hears nothing, forever.
 *
 * <p>The line names {@value QitsClientBearer#CLIENT_KEY} and where its values come from, because
 * that is the thing an operator has to fix. It is said at WARN at most once per {@link
 * QitsClientBearer#WARN_EVERY} across all three callers — the stream redials, the sweeper and the
 * catch-up all ask again on their own schedules — and at DEBUG otherwise. Process-wide, not per bean:
 * the publisher and the query are also built by hand (the contracts kit), outside CDI.
 */
public final class CredentialRefusal {

  private static final Logger LOG = Logger.getLogger(CredentialRefusal.class);

  private static final AtomicLong LAST_WARN = new AtomicLong();

  private static final AtomicBoolean WARNED = new AtomicBoolean();

  /** How many WARNs have been said — what the throttle and the callers are tested by. */
  static final AtomicInteger WARNINGS = new AtomicInteger();

  private CredentialRefusal() {}

  /** Whether {@code status} is an answer about the credential rather than about the request. */
  public static boolean isCredentialStatus(int status) {
    return status == 401 || status == 403;
  }

  /**
   * The status of a rejected websocket upgrade anywhere in {@code failure}'s cause chain, or -1 when
   * the failure is not a rejected upgrade (a refused connection, a reset, a TLS failure).
   */
  static int rejectedUpgradeStatus(Throwable failure) {
    for (Throwable t = failure; t != null; t = t.getCause() == t ? null : t.getCause()) {
      if (t instanceof UpgradeRejectedException rejected) {
        return rejected.getStatus();
      }
    }
    return -1;
  }

  /**
   * Say that {@code what} (e.g. "the event stream upgrade") to {@code target} was answered {@code
   * status}. Returns whether this call was the one that said it at WARN.
   */
  public static boolean report(String what, Object target, int status) {
    String message =
        "qits-events refused this service's credential: "
            + what
            + " to "
            + target
            + " answered "
            + status
            + ". Calls to qits-events go through the edge and carry the bearer of the OIDC client "
            + QitsClientBearer.CLIENT_KEY
            + " (client-id from QITS_RESOURCE_IDP_CLIENT_ID, secret from"
            + " QITS_RESOURCE_IDP_CLIENT_SECRET, provisioned by idp:client in the deployment's"
            + " .config/qits/deployments.yml); until it is configured and accepted no event reaches"
            + " or leaves this service";
    long now = System.nanoTime();
    long previous = LAST_WARN.get();
    boolean due =
        (!WARNED.get() || now - previous >= QitsClientBearer.WARN_EVERY.toNanos())
            && LAST_WARN.compareAndSet(previous, now);
    if (due) {
      WARNED.set(true);
      WARNINGS.incrementAndGet();
      LOG.warnf("%s (said at most once every %s)", message, QitsClientBearer.WARN_EVERY);
      return true;
    }
    LOG.debug(message);
    return false;
  }

  /** Forget the throttle, so a test can see the next refusal at WARN. */
  static void resetForTest() {
    WARNED.set(false);
    LAST_WARN.set(0);
  }
}
