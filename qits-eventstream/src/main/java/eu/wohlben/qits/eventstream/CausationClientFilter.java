package eu.wohlben.qits.eventstream;

import jakarta.annotation.Priority;
import jakarta.ws.rs.Priorities;
import jakarta.ws.rs.client.ClientRequestContext;
import jakarta.ws.rs.client.ClientRequestFilter;
import jakarta.ws.rs.ext.Provider;
import java.util.UUID;

/**
 * The outgoing half of causation over HTTP: if this thread is doing work <em>because of</em> an
 * event, every REST-client request it sends says so, as the {@link CausationHeader#NAME} header.
 *
 * <p>{@code @Provider} is the whole registration — Quarkus applies an annotated client filter to
 * every REST client in the application, injected or built. A consumer adds nothing; a consumer
 * without the REST client on its classpath simply never instantiates this class.
 *
 * <p><b>A header the caller set itself wins.</b> The filter fills the header only when it is
 * absent, mirroring {@code publish(event, parentEventId)}: an author who names a cause explicitly
 * knows something the ambient scope does not.
 *
 * <p>What travels is {@link CausationScope#current()} <em>on the invoking thread</em>. That is the
 * plain-ThreadLocal deal restated, not a new caveat: a blocking client call runs its filter where
 * it was made, and a call handed to another thread first needs the same explicit bridge as any
 * other hand-off.
 */
@Provider
@Priority(Priorities.HEADER_DECORATOR)
public class CausationClientFilter implements ClientRequestFilter {

  @Override
  public void filter(ClientRequestContext requestContext) {
    UUID cause = CausationScope.current();
    if (cause != null && !requestContext.getHeaders().containsKey(CausationHeader.NAME)) {
      requestContext.getHeaders().add(CausationHeader.NAME, cause.toString());
    }
  }
}
