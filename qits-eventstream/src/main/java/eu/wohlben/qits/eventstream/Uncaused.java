package eu.wohlben.qits.eventstream;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * The deliberate opt-out: this entity records no cause, and says so where a reviewer reads it. The
 * analog, for a table, of {@code CausationScope.with(null, …)} for a region.
 *
 * <p>It exists for the qits-arch-rules suite, whose entity rule requires every {@code @Entity} to
 * either implement {@link CausedRow} or carry this annotation — which turns "forgot to participate"
 * from a silent hole in the trace into a failed build, and turns the opt-out into one reviewable
 * line. Runtime behaviour is unaffected; nothing reads this annotation while the application runs.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.CLASS)
public @interface Uncaused {}
