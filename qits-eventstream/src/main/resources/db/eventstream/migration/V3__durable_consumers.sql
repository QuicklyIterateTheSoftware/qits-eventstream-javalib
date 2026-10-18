-- The consuming half's durability: what a durable listener has already handled, and how far it has
-- read. The publisher's outbox (V1) is the mirror image of these two tables — one keeps what has not
-- gone out, these keep what has come in.
--
-- WHY THEY EXIST. The stream is live-only: a consumer that is down, restarting or mid-cutover when a
-- frame is broadcast has lost it, and that loss was measured three ways on the 2026-08-10 bootstraps.
-- qits-events already stores the log and can page it ascending from a cursor, so the cure is
-- bookkeeping on this side rather than a queue in the middle.

-- Which events one listener has already handled — the claim ledger the funnel writes.
--
-- The primary key is the whole mechanism. A live frame and a catch-up row both try to insert here
-- before the handler runs, in the same transaction as the handler; whichever gets there first wins
-- and the other is dropped on the conflict. That is what makes the effect exactly-once per
-- (listener, event id) whatever mix of channels produced the arrivals — and it is why a handler that
-- throws takes its claim row down with it, leaving the event owed.
--
-- IT IS NOT A SECOND COPY OF THE LOG, and two things keep it that way. Only events the listener's
-- predicate SELECTS get a row, so a consumer that acts on one repository stores one repository's
-- worth. And rows are pruned once the watermark has passed them by more than the configured horizon:
-- past that point catch-up will never offer the event again, so the claim has nothing left to
-- protect.
create table consumed_event (
    -- QitsDurableEventListener#consumerId, which is a name a person chose and not a class name:
    -- it has to survive renames, because these rows are keyed on it.
    listener_id varchar(255) not null,
    -- The event's id AS QITS-EVENTS SPELLS IT. varchar(255) rather than the outbox's varchar(36):
    -- that column is a UUID this library generated, while this one is whatever the log's own
    -- varchar(255) id column holds, and the log makes no format promise about it.
    event_id varchar(255) not null,
    -- This consumer's clock at the moment of the claim. Not the event's time — see the watermark
    -- below, which is the one that orders anything.
    handled_at timestamp(6) with time zone not null,
    primary key (listener_id, event_id)
);

-- The pruning query, and the only one that is not a primary-key lookup.
create index idx_consumed_event_prune on consumed_event (listener_id, handled_at);

-- How far one listener has read the log: the composite cursor qits-events hands out, mirrored.
--
-- COMPOSITE FOR THE REASON THE LOG'S OWN CURSOR IS. Sibling events published by one pipeline run
-- share that run's finish instant by construction, so a scalar occurred_at watermark would either
-- re-offer a sibling forever or skip one. The pair is tie-safe in both directions.
--
-- ONLY CATCH-UP MOVES IT, and only when a whole page has been processed. Live frames are ahead of
-- the watermark by definition and never advance it; the claim rows are what stops the sweep from
-- handling them twice when it eventually reads past them.
create table consumer_watermark (
    listener_id varchar(255) not null primary key,
    -- The occurred_at of the last row this listener has read past.
    occurred_at timestamp(6) with time zone not null,
    -- The id of that row, or NULL for "before the first row of the log". Null is how a consumer that
    -- opted into replaying from the beginning is spelled, and how a consumer initialized against an
    -- empty log is: both mean "read from the start", which is the same place when there is nothing
    -- behind you. A blank id could not be sent back as a cursor — the log rejects one — so the
    -- absence is a null rather than an empty string.
    event_id varchar(255)
);
