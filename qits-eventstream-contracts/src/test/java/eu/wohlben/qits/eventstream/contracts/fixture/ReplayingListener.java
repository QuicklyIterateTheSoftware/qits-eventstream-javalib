package eu.wohlben.qits.eventstream.contracts.fixture;

import eu.wohlben.qits.eventstream.QitsDurableEventListener;
import eu.wohlben.qits.eventstream.control.EventFrame;
import java.util.Set;

/** A durable listener replaying from the epoch, two names: a page row and no initialize row. */
public class ReplayingListener implements QitsDurableEventListener {

  @Override
  public String consumerId() {
    return "fixture-replaying";
  }

  @Override
  public Set<String> signatures() {
    return Set.of("ProjectCreated", "ProjectChanged");
  }

  @Override
  public boolean replayFromEpoch() {
    return true;
  }

  @Override
  public void onFrame(EventFrame frame) {}
}
