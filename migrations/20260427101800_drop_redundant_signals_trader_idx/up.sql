-- `signals_trader_idx (trader_id)` is fully covered by the leftmost column of
-- `signals_replay_key_idx (trader_id, entry_tx, leader_position_index)`, so
-- queries filtering by `trader_id` alone can use the wider index. Dropping the
-- redundant single-column one saves storage and removes one index to maintain
-- on every signals INSERT/UPDATE.
DROP INDEX IF EXISTS signals_trader_idx;
