use chrono::Utc;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use eyre::Result;

use crate::db::DbPool;
use crate::db::models::NewSignal;
use crate::schema::signals;
use crate::shared::dex::Dex;
use crate::shared::intent::Side;

/// Idempotent insert keyed by `(trader_id, leader_pair_index, leader_position_index)`.
/// Returns the row's id (existing or new).
#[allow(clippy::too_many_arguments)]
pub async fn record_open(
    pool: &DbPool,
    trader_id: i32,
    source_dex: Dex,
    symbol_id: i32,
    side: Side,
    leader_pair_index: i64,
    leader_position_index: i64,
    collateral_usd: f64,
    leverage: i32,
    entry_price: f64,
    entry_tx: &str,
    entry_block: i64,
) -> Result<i32> {
    let mut conn = pool.get().await?;
    let new = NewSignal {
        trader_id,
        source_dex,
        symbol_id,
        side,
        leader_pair_index,
        leader_position_index,
        collateral_usd,
        leverage,
        entry_price,
        entry_tx,
        entry_block,
    };
    let id: i32 = diesel::insert_into(signals::table)
        .values(&new)
        .on_conflict((
            signals::trader_id,
            signals::leader_pair_index,
            signals::leader_position_index,
        ))
        .do_update()
        .set(signals::trader_id.eq(trader_id))
        .returning(signals::id)
        .get_result(&mut conn)
        .await?;
    Ok(id)
}

/// Mark the signal closed by stamping the exit_* columns. Returns
/// `(signal_id, entry_price)` for the row we updated, so callers can build
/// downstream events (e.g. notifications) without a second round-trip. If the
/// row doesn't exist (leader closed a position we never saw open), returns
/// `None`. Will only update an open row (`exit_at IS NULL`).
pub async fn record_close(
    pool: &DbPool,
    trader_id: i32,
    leader_pair_index: i64,
    leader_position_index: i64,
    exit_price: f64,
    exit_tx: &str,
    exit_block: i64,
) -> Result<Option<(i32, f64)>> {
    let mut conn = pool.get().await?;
    let now = Utc::now();
    let row: Option<(i32, f64)> = diesel::update(
        signals::table
            .filter(signals::trader_id.eq(trader_id))
            .filter(signals::leader_pair_index.eq(leader_pair_index))
            .filter(signals::leader_position_index.eq(leader_position_index))
            .filter(signals::exit_at.is_null()),
    )
    .set((
        signals::exit_price.eq(Some(exit_price)),
        signals::exit_tx.eq(Some(exit_tx)),
        signals::exit_block.eq(Some(exit_block)),
        signals::exit_at.eq(Some(now)),
    ))
    .returning((signals::id, signals::entry_price))
    .get_result(&mut conn)
    .await
    .optional()?;
    Ok(row)
}
