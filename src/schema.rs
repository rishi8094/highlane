// @generated automatically by Diesel CLI.

pub mod sql_types {
    #[derive(diesel::query_builder::QueryId, diesel::sql_types::SqlType)]
    #[diesel(postgres_type(name = "dex"))]
    pub struct Dex;

    #[derive(diesel::query_builder::QueryId, diesel::sql_types::SqlType)]
    #[diesel(postgres_type(name = "trade_side"))]
    pub struct TradeSide;
}

diesel::table! {
    use diesel::sql_types::*;
    use super::sql_types::Dex;
    use super::sql_types::TradeSide;

    signals (id) {
        id -> Int4,
        trader_id -> Int4,
        source_dex -> Dex,
        symbol_id -> Int4,
        side -> TradeSide,
        leader_pair_index -> Int8,
        leader_position_index -> Int8,
        collateral_usd -> Float8,
        leverage -> Int4,
        entry_price -> Float8,
        entry_tx -> Text,
        entry_block -> Int8,
        entry_at -> Timestamptz,
        exit_price -> Nullable<Float8>,
        exit_tx -> Nullable<Text>,
        exit_block -> Nullable<Int8>,
        exit_at -> Nullable<Timestamptz>,
        created_at -> Timestamptz,
        updated_at -> Timestamptz,
    }
}

diesel::table! {
    symbols (id) {
        id -> Int4,
        name -> Text,
    }
}

diesel::table! {
    use diesel::sql_types::*;
    use super::sql_types::Dex;

    traders (id) {
        id -> Int4,
        wallet_address -> Text,
        source_dex -> Dex,
        created_at -> Timestamptz,
        updated_at -> Timestamptz,
    }
}

diesel::table! {
    use diesel::sql_types::*;
    use super::sql_types::Dex;
    use super::sql_types::TradeSide;

    trades (id) {
        id -> Int4,
        signal_id -> Int4,
        target_dex -> Dex,
        market_id -> Int4,
        symbol_id -> Int4,
        side -> TradeSide,
        base_amount -> Int8,
        entry_collateral_usd -> Nullable<Float8>,
        entry_leverage -> Nullable<Int4>,
        entry_price -> Nullable<Float8>,
        entry_tx -> Nullable<Text>,
        entry_block -> Nullable<Int8>,
        entry_at -> Timestamptz,
        exit_price -> Nullable<Float8>,
        exit_tx -> Nullable<Text>,
        exit_at -> Nullable<Timestamptz>,
        created_at -> Timestamptz,
        updated_at -> Timestamptz,
    }
}

diesel::allow_tables_to_appear_in_same_query!(signals, symbols, traders, trades,);
