package eu.wohlben.qits.eventstream.contracts.fixture;

import eu.wohlben.qits.eventstream.QitsDurableEventListener;
import eu.wohlben.qits.eventstream.control.EventFrame;
import java.util.Set;

/** A durable listener asking for one name, starting at the head: initialize + page rows. */
public class NamedListener implements QitsDurableEventListener {

  @Override
  public String consumerId() {
    return "fixture-named";
  }

  @Override
  public Set<String> signatures() {
    return Set.of("DeploymentActive");
  }

  @Override
  public void onFrame(EventFrame frame) {}
}
