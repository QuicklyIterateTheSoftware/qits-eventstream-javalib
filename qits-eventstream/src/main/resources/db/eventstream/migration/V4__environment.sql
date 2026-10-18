-- The envelope's environment — the tier the publish was stamped from — stored with the row for
-- parent_id's exact reason (V1): qits-events compares it, so a sweep that rebuilt the envelope
-- from anything ambient would send a different request than the attempt it is retrying. No index
-- (the sweeper queries status and next_attempt_at only) and no backfill: a pre-existing row's
-- inline attempt carried no tier, and null is what resends that faithfully.
alter table outbox_event add column environment varchar(64);
