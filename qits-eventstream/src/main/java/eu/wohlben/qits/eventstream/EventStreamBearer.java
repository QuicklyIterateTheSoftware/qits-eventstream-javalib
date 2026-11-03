package eu.wohlben.qits.eventstream;

import io.smallrye.mutiny.Uni;

/**
 * Who this service is to qits-events: the {@code Authorization} value every call this library makes
 * on the service's behalf carries — the {@code PUT} of a publish, the catch-up's reads and the
 * stream's upgrade (qits-1328, epic qits-543 6a).
 *
 * <p><b>The shipped implementation is the service's own named OIDC client {@code qits}</b> ({@code
 * quarkus.oidc-client.qits}, the {@code qits:system} client the deployment's {@code idp:client}
 * resource provisions), read through {@code OidcClients}, so the library has no settings of its own.
 * It is a {@code @DefaultBean}: a consumer whose token comes from somewhere else declares its own
 * {@code @ApplicationScoped} bean of this type and that one is used instead.
 *
 * <p><b>There is no fallback.</b> The forwarded user/roles header pair is gone, and a
 * service that has no token sends nothing: {@link #authorization()} fails with {@link Missing},
 * naming what is missing, and the caller treats that as "nothing was sent" — the publish stays in
 * the outbox, the catch-up leaves its watermark, the stream redials with its backoff.
 */
public interface EventStreamBearer {

  /**
   * The header value, {@code "Bearer <token>"}. Never blocks: the stream dials from an event loop.
   * A failure is a {@link Missing} whose message says which client is missing and why.
   */
  Uni<String> authorization();

  /** No token could be had, so the call was not made. The message names the missing client. */
  final class Missing extends RuntimeException {

    public Missing(String message) {
      super(message);
    }

    public Missing(String message, Throwable cause) {
      super(message, cause);
    }
  }
}
