package eu.wohlben.qits.eventstream.contracts;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URL;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Several golden-master trees on one classpath (qits-1163 rollout): this module's test resources
 * carry a foreign {@code golden-masters/} (provider qits-githost) that comes FIRST, with a recording
 * at the same path as one of qits-events'; the real qits-events jar comes second. The reader must
 * pick qits-events' index and read its recordings from that jar.
 */
class GoldenMastersClasspathTest {

  @Test
  void theForeignTreeReallyComesFirst() {
    List<URL> indexes = EventsGoldenMasters.indexes();
    assertTrue(indexes.size() >= 2, indexes.toString());
    assertTrue(indexes.getFirst().toString().contains("test-classes"), indexes.toString());
  }

  @Test
  void qitsEventsIndexAndRecordingsAreReadFromItsOwnJar() {
    EventsGoldenMasters.Operation op =
        EventsGoldenMasters.operation(EventstreamPact.A_FEW_RECENT_EVENTS, EventstreamPact.LIST_EVENTS);
    assertEquals("/events/api/events", op.path());
    String body = EventsGoldenMasters.body(EventstreamPact.A_FEW_RECENT_EVENTS, EventstreamPact.LIST_EVENTS);
    assertTrue(body.contains("\"events\""), body);
    assertTrue(!body.contains("foreign"), "read the shadowing recording: " + body);
  }

  @Test
  void noQitsEventsTreeFailsNamingWhatWasFound() {
    URL foreign = EventsGoldenMasters.indexes().getFirst();
    IllegalStateException error =
        assertThrows(IllegalStateException.class, () -> EventsGoldenMasters.select(List.of(foreign)));
    assertTrue(error.getMessage().contains("no golden-masters/index.json on the classpath is qits-events's"),
        error.getMessage());
    assertTrue(error.getMessage().contains("'qits-githost' at " + foreign), error.getMessage());
  }
}
