package eu.wohlben.qits.eventstream;

import jakarta.annotation.Priority;
import jakarta.ws.rs.Priorities;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.container.ContainerResponseFilter;
import jakarta.ws.rs.ext.Provider;
import java.util.UUID;

/**
 * The incoming half of causation over HTTP: a request carrying {@link CausationHeader#NAME} runs
 * its resource method inside {@link CausationScope} of that id, so an event the method publishes is
 * stamped with the cause the caller was acting under — the chain crosses the service boundary
 * whole. Discovered by {@code @Provider}; a consumer without a REST server never instantiates it.
 *
 * <p><b>Every request establishes a scope, including one with no header.</b> An outside request has
 * no ambient cause, and saying so with {@code swap(null)} is what keeps a pooled worker's previous
 * request from lending its cause to this one. Absent and malformed read the same way — causation is
 * advisory, and a request must never fail over a field only the chain graph reads.
 *
 * <p><b>The response filter restores rather than clears</b>, the previous value having ridden the
 * request context (boxed, because "the previous value was null" and "the request filter never ran"
 * — a 404 runs only the response half — must read differently). Restore-not-clear is {@link
 * CausationScope#with}'s own hygiene, kept across the one seam that cannot use {@code with}:
 * request and response are two methods, so the pair uses the package-private {@code swap} and this
 * javadoc is the finally-block.
 *
 * <p>The pairing assumes both halves run on the resource method's thread, which is what RESTEasy
 * Reactive does for a blocking method — the platform's shape. An async method whose completion
 * migrates threads gets the header read but not the restore on the original thread; the
 * establishing swap at the next request's start is what bounds that, and such a method inherits the
 * executor caveat {@link CausationScope} already documents.
 */
@Provider
@Priority(Priorities.HEADER_DECORATOR)
public class CausationServerFilter implements ContainerRequestFilter, ContainerResponseFilter {

  private static final String PREVIOUS = CausationServerFilter.class.getName() + ".previous";

  /** The box whose presence means "the request half ran here", whatever it holds. */
  private record Previous(UUID cause) {}

  @Override
  public void filter(ContainerRequestContext requestContext) {
    UUID cause = CausationHeader.parse(requestContext.getHeaderString(CausationHeader.NAME));
    requestContext.setProperty(PREVIOUS, new Previous(CausationScope.swap(cause)));
  }

  @Override
  public void filter(
      ContainerRequestContext requestContext, ContainerResponseContext responseContext) {
    if (requestContext.getProperty(PREVIOUS) instanceof Previous(UUID cause)) {
      CausationScope.swap(cause);
      requestContext.removeProperty(PREVIOUS);
    }
  }
}
