package eu.wohlben.qits.eventstream.contracts.fixture;

import eu.wohlben.qits.eventstream.QitsDurableEventListener;
import eu.wohlben.qits.eventstream.QitsRawEventListener;
import eu.wohlben.qits.eventstream.control.EventFrame;
import java.util.Set;

/** A durable listener taking every event: its page row reads "a few recent events". */
public class EverythingListener implements QitsDurableEventListener {

  @Override
  public String consumerId() {
    return "fixture-everything";
  }

  @Override
  public Set<String> signatures() {
    return Set.of(QitsRawEventListener.ALL);
  }

  @Override
  public void onFrame(EventFrame frame) {}
}
