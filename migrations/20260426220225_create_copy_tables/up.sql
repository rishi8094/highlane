CREATE TYPE trade_side AS ENUM ('long', 'short');
CREATE TYPE dex AS ENUM ('avantis', 'lighter');

CREATE TABLE traders (
    id              SERIAL      PRIMARY KEY,
    wallet_address  TEXT        NOT NULL,
    source_dex      dex         NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX traders_wallet_dex_idx ON traders (wallet_address, source_dex);
SELECT diesel_manage_updated_at('traders');

CREATE TABLE symbols (
    id    SERIAL PRIMARY KEY,
    name  TEXT   NOT NULL UNIQUE
);

CREATE TABLE signals (
    id                     SERIAL      PRIMARY KEY,
    trader_id              INTEGER     NOT NULL,
    source_dex             dex         NOT NULL,
    symbol_id              INTEGER     NOT NULL,
    side                   trade_side  NOT NULL,
    leader_pair_index      BIGINT      NOT NULL,
    leader_position_index  BIGINT      NOT NULL,
    collateral_usd         DOUBLE PRECISION NOT NULL,
    leverage               INTEGER     NOT NULL,
    entry_price            DOUBLE PRECISION NOT NULL,
    entry_tx               TEXT        NOT NULL,
    entry_block            BIGINT      NOT NULL,
    entry_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    exit_price             DOUBLE PRECISION,
    exit_tx                TEXT,
    exit_block             BIGINT,
    exit_at                TIMESTAMPTZ,
    created_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at             TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX signals_trader_idx ON signals (trader_id);
CREATE INDEX signals_symbol_idx ON signals (symbol_id);
CREATE UNIQUE INDEX signals_natural_key_idx
    ON signals (trader_id, leader_pair_index, leader_position_index);
CREATE INDEX signals_open_idx ON signals (trader_id) WHERE exit_at IS NULL;
SELECT diesel_manage_updated_at('signals');

CREATE TABLE trades (
    id                      SERIAL      PRIMARY KEY,
    signal_id               INTEGER     NOT NULL,
    target_dex              dex         NOT NULL,
    market_id               INTEGER     NOT NULL,
    symbol_id               INTEGER     NOT NULL,
    side                    trade_side  NOT NULL,
    base_amount             BIGINT      NOT NULL,
    entry_collateral_usd    DOUBLE PRECISION,
    entry_leverage          INTEGER,
    entry_price             DOUBLE PRECISION,
    entry_tx                TEXT,
    entry_block             BIGINT,
    entry_at                TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    exit_price              DOUBLE PRECISION,
    exit_tx                 TEXT,
    exit_at                 TIMESTAMPTZ,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX trades_signal_idx ON trades (signal_id);
CREATE INDEX trades_symbol_idx ON trades (symbol_id);
CREATE INDEX trades_open_idx ON trades (target_dex, market_id) WHERE exit_at IS NULL;
SELECT diesel_manage_updated_at('trades');
