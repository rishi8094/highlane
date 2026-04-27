DROP INDEX IF EXISTS signals_natural_key_idx;

CREATE UNIQUE INDEX signals_replay_key_idx
    ON signals (trader_id, entry_tx, leader_position_index);
