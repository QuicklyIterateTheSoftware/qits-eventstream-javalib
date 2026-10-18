-- The publisher's outbox: events that did not reach qits-events on the first try, and what the
-- sweeper needs to keep trying. Its own lineage on its own datasource, so this library never shares
-- a consumer's database or its migration history.
--
-- ONE V1 AND NO INHERITED LINEAGE. The H2 lineage (V1 + V2) is deleted rather than continued, and
-- that was allowed by one fact rather than by preference: THIS TABLE IS EMPTY IN A HEALTHY PROCESS.
-- A publish that succeeds inline writes nothing here and a row delivered on retry is deleted, so
-- the only thing an H2 file could have been holding at the moment of the move is a handful of
-- undelivered events — and the store moves by re-bootstrap, which is where those would have gone
-- anyway. No postgres database anywhere has ever run the H2 files, so no `V3__move_to_postgres.sql`
-- had a reader. FROM HERE ON THE ORDINARY RULE IS BACK: keep appending, never edit an applied
-- migration.
--
-- The shape below is the two H2 migrations translated and merged — `text` instead of `clob`, and
-- V2's `parent_id` folded into the table it belongs to with NO BACKFILL, because every database
-- reaching this file is empty. Nothing else about the schema changed: no identity column (the key
-- is the EVENT's uuid, never a sequence of ours), and the timestamps were already
-- `timestamp(6) with time zone`, which postgres reads as its own `timestamptz`.
--
-- WHAT THE TRANSLATION DELIBERATELY KEPT, since the sibling components read the other way: the
-- CHECK on `status` survives. qits-platform-deployments dropped its enum checks because H2 2.4.240
-- tied a compiled IN-set to the session that made it and failed a valid insert with 23514 — a
-- defect, not a design. This lineage never met it, postgres has no such behaviour, and the two-value
-- set here is a closed invariant of the sweeper rather than a catalogue that grows: a third status
-- would be a schema change and should read as one.

create table outbox_event (
    -- The EVENT's uuid, not a key of ours. qits-events keys on it, which is what makes a retry a
    -- replay (200) instead of a duplicate.
    id varchar(36) not null primary key,
    -- The envelope, kept whole so a retry re-sends the same bytes rather than re-deriving them:
    -- the server compares payload verbatim, so a row that stored the event's fields instead of its
    -- canonical form could serialize differently later and turn its own retry into a 400.
    name varchar(255) not null,
    occurred_at timestamp(6) with time zone not null,
    -- `text`, and the entity says `columnDefinition = "text"` rather than @Lob, which is the one
    -- mapping the move had to change. On H2 a @Lob String was a clob and the two agreed; on
    -- postgres @Lob means a LARGE OBJECT — Hibernate binds an oid and the insert fails against a
    -- text column. Unbounded either way, which is what the canonical payload needs.
    payload text,
    description text,
    -- The envelope's parent — the event that caused this one, or null for a root. Stored rather
    -- than re-derived, and that is the whole reason the column exists: the cause is ambient at
    -- publish time and gone by the time the sweeper runs, while qits-events compares
    -- name + occurred_at + payload + parent_id to tell an idempotent replay from a reused uuid. A
    -- retry that rebuilt the envelope without it would send a DIFFERENT request than the attempt it
    -- is retrying. Same argument the payload column carries, one field further on.
    --
    -- varchar(36) matches this table's id, which is the same kind of thing. NO INDEX: the sweeper's
    -- only query is (status, next_attempt_at) and nothing here ever selects by parent — the
    -- children-of-X read model belongs to qits-events, which does index it.
    parent_id varchar(36),
    -- Two states only. Delivered rows do not exist; see the header.
    status varchar(16) not null check (status in ('PENDING', 'FAILED')),
    -- Attempts made, INCLUDING the inline one — a row is born at 1.
    attempts int not null,
    -- Null once FAILED.
    next_attempt_at timestamp(6) with time zone,
    last_error varchar(1024),
    created_at timestamp(6) with time zone not null,
    updated_at timestamp(6) with time zone not null
);

-- The sweeper's only query: what is PENDING and due.
create index idx_outbox_event_due on outbox_event (status, next_attempt_at);
