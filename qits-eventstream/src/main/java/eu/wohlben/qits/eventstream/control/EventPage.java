package eu.wohlben.qits.eventstream.control;

import io.quarkus.runtime.annotations.RegisterForReflection;
import java.util.List;

/**
 * One page of {@code GET /events/api/events}: the rows, and whether there are more.
 *
 * <p>The rows bind to {@link EventFrame}, which is the same shape the stream pushes — the log's own
 * {@code createdAt} and {@code updatedAt} come along and are dropped, because a consumer is told
 * what happened rather than when this database learned of it. That the two channels deserialize to
 * one type is what lets the funnel have one entry point.
 *
 * <p><b>{@code nextCursor} is null on the last page, and a page's own last row is the cursor.</b>
 * Those are two separate facts and a catch-up loop needs both: the server answers null once there is
 * no more history, even for a page that came back full of rows, so a reader advances its watermark
 * to the last row it processed and uses this field only to decide whether to ask again.
 *
 * <p>Package-private: it is the wire between {@link EventsQuery} and {@link CatchupSweeper} and no
 * consumer of this library ever holds one. {@link EventFrame} is public because a listener receives
 * it; this is not.
 *
 * <p>{@code @RegisterForReflection} is load-bearing, not decoration, and it can live nowhere else:
 * Jackson builds this record reflectively inside a NATIVE image, and a package-private type is one
 * no consumer's own reflection holder can reach. Measured on the first bus-only bootstrap: without
 * it every catch-up sweep died at initialization — the durable safety net off, in the binary only,
 * while every JVM test stayed green.
 */
@RegisterForReflection
record EventPage(List<EventFrame> events, String nextCursor) {}
