package eu.wohlben.qits.eventstream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.UUID;
import org.junit.jupiter.api.Test;

/** The lenient read: everything that is not a UUID is "no cause", never an error. Plain JUnit. */
class CausationHeaderTest {

  private static final UUID CAUSE = UUID.fromString("11111111-1111-4111-8111-111111111111");

  @Test
  void aUuidParsesAndSurroundingWhitespaceIsForgiven() {
    assertEquals(CAUSE, CausationHeader.parse(CAUSE.toString()));
    assertEquals(CAUSE, CausationHeader.parse("  " + CAUSE + "\t"));
  }

  @Test
  void absentBlankAndMalformedAllReadAsNoCause() {
    assertNull(CausationHeader.parse(null));
    assertNull(CausationHeader.parse(""));
    assertNull(CausationHeader.parse("   "));
    assertNull(CausationHeader.parse("not-a-uuid"));
  }
}
