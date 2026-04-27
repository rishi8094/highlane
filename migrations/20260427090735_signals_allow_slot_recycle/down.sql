DROP INDEX IF EXISTS signals_replay_key_idx;

CREATE UNIQUE INDEX signals_natural_key_idx
    ON signals (trader_id, leader_pair_index, leader_position_index);
