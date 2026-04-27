-- Enforce one trade per signal at the schema level. Partial — excludes the
-- `signal_id = 0` sentinel that the backfill stamps on trades it can't pair
-- to a chain signal, since multiple unlinked trades must coexist until a
-- follow-up pass resolves them.
CREATE UNIQUE INDEX trades_signal_id_unique_idx
    ON trades (signal_id)
    WHERE signal_id <> 0;
