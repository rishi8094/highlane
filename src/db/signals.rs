use chrono::Utc;
use diesel::dsl::max;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use eyre::Result;

use crate::db::DbPool;
use crate::db::models::NewSignal;
use crate::schema::signals;
use crate::shared::dex::Dex;
use crate::shared::intent::Side;

/// Highest leader block we've ever ingested for `trader_id`, considering both
/// the open-side and close-side block fields. Used by the watcher on startup
/// to seed `last_seen_block` so that `eth_getLogs` can backfill events that
/// fired while the bot was offline.
pub async fn max_seen_block(pool: &DbPool, trader_id: i32) -> Result<Option<i64>> {
    let mut conn = pool.get().await?;
    let max_entry: Option<i64> = signals::table
        .filter(signals::trader_id.eq(trader_id))
        .select(max(signals::entry_block))
        .first(&mut conn)
        .await?;
    let max_exit: Option<i64> = signals::table
        .filter(signals::trader_id.eq(trader_id))
        .select(max(signals::exit_block))
        .first(&mut conn)
        .await?;
    Ok(match (max_entry, max_exit) {
        (Some(a), Some(b)) => Some(a.max(b)),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    })
}

/// Insert one row per leader open. Idempotent on `(trader_id, entry_tx,
/// leader_position_index)` so a re-delivered on-chain log produces no
/// duplicate. Each leader open at a given `leader_position_index` produces a
/// new row — Avantis recycles position slots when a position closes, so we
/// can't key by slot alone or `record_close` would never find the right open
/// row after a slot is reused.
///
/// Returns `(id, was_new)`. `was_new = false` means a row already existed
/// (replay/duplicate delivery); the caller MUST treat this as an
/// already-ingested OPEN and skip emitting a fresh `TradeIntent::Open` —
/// otherwise the executor will fire a duplicate IOC against Lighter for an
/// already-mirrored position.
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
) -> Result<(i32, bool)> {
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
    // ON CONFLICT DO NOTHING ... RETURNING returns the new row only on
    // actual insert; on conflict, RETURNING yields nothing and `optional()`
    // gives us None — the unambiguous "already existed" signal we need.
    // (The previous DO UPDATE SET trader_id = trader_id trick was idempotent
    // but couldn't distinguish first-insert from conflict.)
    let inserted_id: Option<i32> = diesel::insert_into(signals::table)
        .values(&new)
        .on_conflict((
            signals::trader_id,
            signals::entry_tx,
            signals::leader_position_index,
        ))
        .do_nothing()
        .returning(signals::id)
        .get_result(&mut conn)
        .await
        .optional()?;
    if let Some(id) = inserted_id {
        return Ok((id, true));
    }
    let existing_id: i32 = signals::table
        .filter(signals::trader_id.eq(trader_id))
        .filter(signals::entry_tx.eq(entry_tx))
        .filter(signals::leader_position_index.eq(leader_position_index))
        .select(signals::id)
        .first(&mut conn)
        .await?;
    Ok((existing_id, false))
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
