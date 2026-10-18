package eu.wohlben.qits.eventstream.contracts.fixture;

import eu.wohlben.qits.eventstream.QitsRawEventListener;
import eu.wohlben.qits.eventstream.control.EventFrame;
import java.util.Set;

/** A raw, live-only listener: guarded, but it adds no row (the websocket has no pact). */
public class LiveListener implements QitsRawEventListener {

  @Override
  public Set<String> signatures() {
    return Set.of("DeploymentActive");
  }

  @Override
  public void onFrame(EventFrame frame) {}
}
