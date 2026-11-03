package eu.wohlben.qits.eventstream.control;

import eu.wohlben.qits.eventstream.EventStreamBearer;
import io.smallrye.mutiny.Uni;

/** What a client built by hand, outside CDI, has until someone gives it a bearer: none. */
final class Unbuilt {

  /** Fails every call closed, saying how to get out of it. */
  static final EventStreamBearer BEARER =
      () ->
          Uni.createFrom()
              .failure(
                  new EventStreamBearer.Missing(
                      "built by hand with no EventStreamBearer, so no OIDC client "
                          + QitsClientBearer.CLIENT_KEY
                          + " is read"));

  private Unbuilt() {}
}
