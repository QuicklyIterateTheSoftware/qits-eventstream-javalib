package eu.wohlben.qits.eventstream;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.Path;

/**
 * The near end: a REST-client interface with {@link CausationClientFilter} applied the way a
 * consumer gets it — through {@code @Provider} discovery, registered by nobody. The second method
 * sets the header itself, which is how the tests reach "an explicit header wins" and "malformed is
 * ignored" without a hand-built request.
 */
@Path("/causation-probe")
public interface CausationProbeClient {

  @GET
  String current();

  @GET
  String currentWith(@HeaderParam(CausationHeader.NAME) String explicitHeader);
}
