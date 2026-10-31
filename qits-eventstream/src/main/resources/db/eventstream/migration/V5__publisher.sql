-- The envelope's publisher — the service the publish was stamped from (qits-1770) — stored with the
-- row for parent_id's and environment's exact reason (V1, V4): qits-events compares it, so a sweep
-- that rebuilt the envelope from configuration would send a different request than the attempt it
-- is retrying. varchar(255) is qits-events' own bound on the field. No index (the sweeper queries
-- status and next_attempt_at only) and no backfill: a pre-existing row's inline attempt carried no
-- publisher, and null is what resends that faithfully.
alter table outbox_event add column publisher varchar(255);
