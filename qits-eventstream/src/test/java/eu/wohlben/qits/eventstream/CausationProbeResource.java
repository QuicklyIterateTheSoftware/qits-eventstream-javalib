package eu.wohlben.qits.eventstream;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import java.util.UUID;

/**
 * The far end of the causation-over-HTTP tests: a blocking resource method — the platform's shape,
 * and the thread model the filters assume — that answers with the cause it is running under. The
 * literal {@code "null"} names "none", because the REST client reads an <em>empty</em> 200 body
 * back as a null string and the sentinel must survive the trip.
 */
@Path("/causation-probe")
public class CausationProbeResource {

  @GET
  @Produces(MediaType.TEXT_PLAIN)
  public String current() {
    return String.valueOf(CausationScope.current());
  }
}
