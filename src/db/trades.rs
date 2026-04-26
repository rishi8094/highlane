use chrono::Utc;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use eyre::Result;

use crate::db::DbPool;
use crate::db::models::{NewTrade, Trade};
use crate::schema::trades;
use crate::shared::dex::Dex;
use crate::shared::intent::Side;

#[allow(clippy::too_many_arguments)]
pub async fn record_open(
    pool: &DbPool,
    signal_id: i32,
    target_dex: Dex,
    market_id: i32,
    symbol_id: i32,
    side: Side,
    base_amount: i64,
    entry_collateral_usd: Option<f64>,
    entry_leverage: Option<i32>,
    entry_price: Option<f64>,
    entry_tx: Option<&str>,
    entry_block: Option<i64>,
) -> Result<i32> {
    let mut conn = pool.lock().await;
    let new = NewTrade {
        signal_id,
        target_dex,
        market_id,
        symbol_id,
        side,
        base_amount,
        entry_collateral_usd,
        entry_leverage,
        entry_price,
        entry_tx,
        entry_block,
    };
    let id: i32 = diesel::insert_into(trades::table)
        .values(&new)
        .returning(trades::id)
        .get_result(&mut *conn)
        .await?;
    Ok(id)
}

/// Look up the most recent open trade for a given signal. There should be at
/// most one; if there's drift we still take the newest.
pub async fn find_open_for_signal(pool: &DbPool, signal_id: i32) -> Result<Option<Trade>> {
    let mut conn = pool.lock().await;
    let row = trades::table
        .filter(trades::signal_id.eq(signal_id))
        .filter(trades::exit_at.is_null())
        .order(trades::id.desc())
        .select(Trade::as_select())
        .first(&mut *conn)
        .await
        .optional()?;
    Ok(row)
}

/// Stamp exit_* on a trade row. Only updates rows still open (`exit_at IS NULL`).
pub async fn record_close(
    pool: &DbPool,
    trade_id: i32,
    exit_price: Option<f64>,
    exit_tx: Option<&str>,
) -> Result<()> {
    let mut conn = pool.lock().await;
    let now = Utc::now();
    diesel::update(
        trades::table
            .filter(trades::id.eq(trade_id))
            .filter(trades::exit_at.is_null()),
    )
    .set((
        trades::exit_price.eq(exit_price),
        trades::exit_tx.eq(exit_tx),
        trades::exit_at.eq(Some(now)),
    ))
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// All trades currently open (`exit_at IS NULL`) for the given target DEX.
/// Used by the executor's startup reconciliation against live exchange
/// positions.
pub async fn list_open_for_target(pool: &DbPool, target_dex: Dex) -> Result<Vec<Trade>> {
    let mut conn = pool.lock().await;
    let rows = trades::table
        .filter(trades::exit_at.is_null())
        .filter(trades::target_dex.eq(target_dex))
        .select(Trade::as_select())
        .load(&mut *conn)
        .await?;
    Ok(rows)
}

/// Bulk-close every open trade on `(target_dex, market_id)` by stamping
/// `exit_at = NOW()` with no exit_price/exit_tx — used when reconciliation
/// finds copy-state for a market that the exchange shows as flat. Readers
/// distinguish "leader-closed" (exit_price set) from "reconcile-failed"
/// (exit_at set, exit_price null).
pub async fn fail_open_for_market(
    pool: &DbPool,
    target_dex: Dex,
    market_id: i32,
) -> Result<usize> {
    let mut conn = pool.lock().await;
    let now = Utc::now();
    let n = diesel::update(
        trades::table
            .filter(trades::exit_at.is_null())
            .filter(trades::target_dex.eq(target_dex))
            .filter(trades::market_id.eq(market_id)),
    )
    .set(trades::exit_at.eq(Some(now)))
    .execute(&mut *conn)
    .await?;
    Ok(n)
}
