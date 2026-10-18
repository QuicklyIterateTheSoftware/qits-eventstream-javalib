package eu.wohlben.qits.eventstream;

import java.util.UUID;

/**
 * The HTTP header that carries a cause across a service boundary: the wire spelling of {@link
 * CausationScope}.
 *
 * <p><b>The name is inside the gateway's reserved namespace on purpose.</b> qits-gateway drops
 * every client-supplied {@code X-Qits-*} header before it proxies, so an outside caller cannot
 * forge a cause onto a chain — the same property {@code X-Qits-User} leans on, bought by the prefix
 * alone. Service-to-service traffic does not pass the gateway and carries the header untouched.
 *
 * <p>The paired filters are the automatic path. A caller that builds its requests by hand — most of
 * this platform speaks {@code java.net.http.HttpClient}, not the REST client — stamps the same
 * header itself:
 *
 * <pre>{@code
 * UUID cause = CausationScope.current();
 * if (cause != null) builder.header(CausationHeader.NAME, cause.toString());
 * }</pre>
 */
public final class CausationHeader {

  public static final String NAME = "X-Qits-Causation-Id";

  private CausationHeader() {}

  /**
   * The header's value as a cause, or {@code null} for absent or malformed. Lenient because
   * causation is advisory: a request whose header does not parse must be served exactly as one that
   * carries none, for the same reason {@code EventDispatcher} dispatches a frame whose id is not a
   * UUID rather than dropping it.
   */
  static UUID parse(String value) {
    if (value == null || value.isBlank()) {
      return null;
    }
    try {
      return UUID.fromString(value.trim());
    } catch (IllegalArgumentException e) {
      return null;
    }
  }
}
