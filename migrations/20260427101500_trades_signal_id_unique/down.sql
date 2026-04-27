-- The data backfill in up.sql is irreversible (we don't know which trades
-- had their exit_price set by this migration vs by the executor) — only the
-- index is undone here.
DROP INDEX IF EXISTS trades_signal_id_unique_idx;
