use diesel::prelude::*;

use crate::schema::{signals, trades, traders};
use crate::shared::dex::Dex;
use crate::shared::intent::Side;

#[derive(Debug, Insertable)]
#[diesel(table_name = traders)]
pub struct NewTrader<'a> {
    pub wallet_address: &'a str,
    pub source_dex: Dex,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = signals)]
pub struct NewSignal<'a> {
    pub trader_id: i32,
    pub source_dex: Dex,
    pub symbol_id: i32,
    pub side: Side,
    pub leader_pair_index: i64,
    pub leader_position_index: i64,
    pub collateral_usd: f64,
    pub leverage: i32,
    pub entry_price: f64,
    pub entry_tx: &'a str,
    pub entry_block: i64,
}

#[derive(Debug, Queryable, Selectable)]
#[diesel(table_name = trades)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct Trade {
    pub id: i32,
    pub market_id: i32,
    pub side: Side,
    pub size: i64,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = trades)]
pub struct NewTrade<'a> {
    pub signal_id: i32,
    pub target_dex: Dex,
    pub market_id: i32,
    pub symbol_id: i32,
    pub side: Side,
    pub size: i64,
    pub entry_collateral_usd: Option<f64>,
    pub entry_leverage: Option<i32>,
    pub entry_price: Option<f64>,
    pub entry_tx: Option<&'a str>,
    pub entry_block: Option<i64>,
}
