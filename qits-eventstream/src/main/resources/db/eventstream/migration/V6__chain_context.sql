-- The envelope's call chain (qits-2694): the trace id of the chain's first call and the subject the
-- chain acts for, stored with the row for parent_id's, environment's and publisher's exact reason
-- (V1, V4, V5): qits-events compares both, so a sweep that rebuilt the envelope from the scheduler's
-- ambient chain would send a different request than the attempt it is retrying. 32 and 255 are
-- qits-events' own bounds on the fields. No index and no backfill: a pre-existing row's inline
-- attempt carried neither, and null is what resends that faithfully.
alter table outbox_event add column origin_trace_id varchar(32);
alter table outbox_event add column on_behalf_of varchar(255);
