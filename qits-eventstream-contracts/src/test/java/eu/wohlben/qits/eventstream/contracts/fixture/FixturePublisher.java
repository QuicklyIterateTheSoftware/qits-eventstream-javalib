package eu.wohlben.qits.eventstream.contracts.fixture;

import eu.wohlben.qits.eventstream.QitsEvent;
import eu.wohlben.qits.eventstream.QitsEventBus;

/** Publishes through QitsEventBus, which is what makes the guard demand publishes(true). */
public class FixturePublisher {

  QitsEventBus bus;

  void announce(QitsEvent event) {
    bus.publish(event);
  }
}
