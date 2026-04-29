use std::collections::HashMap;
use std::time::Duration;

use alloy::{
    primitives::Address,
    providers::{Provider, ProviderBuilder, WsConnect},
    rpc::types::Filter,
    sol_types::SolEvent,
};
use eyre::Result;
use futures_util::StreamExt;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use super::contracts::{
    ICallbacks::{LimitExecuted, MarketExecuted},
    ITradingStorage, MULTICALL3, PAIR_STORAGE, TRADING_STORAGE, parse_addr,
};
use super::format::{format_1e10, format_leverage, format_usdc};
use super::pairs::load_pair_names;
use crate::db::{DbPool, signals, symbols};
use crate::shared::dex::Dex;
use crate::shared::intent::{Side, TradeIntent};

const SOURCE_DEX: Dex = Dex::Avantis;

const BACKOFF_INITIAL: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(30);

/// Avantis `LimitExecuted.orderType` enum:
///   0 = TP, 1 = SL, 2 = LIQ (all closes), 3 = LIMIT_OPEN (open).
/// Anything other than 3 is a keeper-driven close. We route on this rather
/// than `ev.isPnl` because `isPnl` is observed `false` even on SL/TP/LIQ
/// closes in production — using it caused us to record TP/SL closes as
/// fresh OPENs at the trigger price (see signal 653 LIT/USD on 2026-04-29).
const ORDER_TYPE_LIMIT_OPEN: u8 = 3;

pub async fn watch_leader(
    ws_url: &str,
    leader: Address,
    trader_id: i32,
    pool: DbPool,
    tx: mpsc::Sender<TradeIntent>,
) -> ! {
    let mut backoff = BACKOFF_INITIAL;
    loop {
        match run_session(ws_url, leader, trader_id, &pool, &tx).await {
            Ok(()) => {
                warn!("subscription stream ended; reconnecting");
                backoff = BACKOFF_INITIAL;
            }
            Err(e) => {
                error!(error = ?e, backoff_secs = backoff.as_secs(), "watcher session failed");
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(BACKOFF_MAX);
                continue;
            }
        }
        tokio::time::sleep(backoff).await;
    }
}

async fn run_session(
    ws_url: &str,
    leader: Address,
    trader_id: i32,
    pool: &DbPool,
    tx: &mpsc::Sender<TradeIntent>,
) -> Result<()> {
    info!(%leader, "connecting to Base WSS");
    let provider = ProviderBuilder::new()
        .connect_ws(WsConnect::new(ws_url.to_string()))
        .await?;

    let trading_storage = ITradingStorage::new(parse_addr(TRADING_STORAGE), &provider);
    let trading_addr = trading_storage.trading().call().await?;
    let callbacks_addr = trading_storage.callbacks().call().await?;
    info!(%trading_addr, %callbacks_addr, "resolved Avantis contracts");

    let pairs = load_pair_names(&provider, parse_addr(PAIR_STORAGE), parse_addr(MULTICALL3))
        .await
        .unwrap_or_else(|e| {
            warn!(error = ?e, "failed to load pair names; symbols will fall back to indices");
            HashMap::new()
        });
    info!(count = pairs.len(), "pair names ready");

    let filter = Filter::new().address(callbacks_addr).event_signature(vec![
        <MarketExecuted as SolEvent>::SIGNATURE_HASH,
        <LimitExecuted as SolEvent>::SIGNATURE_HASH,
    ]);

    let sub = provider.subscribe_logs(&filter).await?;
    info!(%callbacks_addr, "subscribed to MarketExecuted + LimitExecuted logs");

    let mut stream = sub.into_stream();
    while let Some(log) = stream.next().await {
        handle_log(&log, leader, trader_id, pool, &pairs, tx).await;
    }
    Ok(())
}

async fn handle_log(
    log: &alloy::rpc::types::Log,
    leader: Address,
    trader_id: i32,
    pool: &DbPool,
    pairs: &HashMap<u64, String>,
    tx: &mpsc::Sender<TradeIntent>,
) {
    let topic0 = match log.topic0() {
        Some(t) => *t,
        None => return,
    };
    let tx_hash = log
        .transaction_hash
        .map(|h| h.to_string())
        .unwrap_or_else(|| "?".into());
    let block = log.block_number.unwrap_or_default();

    if topic0 == <MarketExecuted as SolEvent>::SIGNATURE_HASH {
        match <MarketExecuted as SolEvent>::decode_log(&log.inner) {
            Ok(decoded) => {
                let ev = &decoded.data;
                if ev.t.trader != leader {
                    return;
                }
                let pair_idx: u64 = ev.t.pairIndex.try_into().unwrap_or(u64::MAX);
                let pos_idx: u64 = ev.t.index.try_into().unwrap_or(u64::MAX);
                let symbol = pair_name(pairs, pair_idx);
                let leverage = format_leverage(ev.t.leverage);
                let collateral = format_usdc(ev.positionSizeUSDC);
                let exec_price = format_1e10(ev.price);
                let side = if ev.t.buy { Side::Long } else { Side::Short };

                let intent = if ev.open {
                    info!(
                        target: "intent",
                        kind = "OPEN", source = "market", %symbol, %side,
                        collateral, leverage, exec_price, block, %tx_hash,
                        "leader OPEN (market)"
                    );
                    let Some((symbol_id, signal_id)) = record_open(
                        pool, trader_id, &symbol, side, pair_idx, pos_idx, collateral, leverage,
                        exec_price, &tx_hash, block,
                    )
                    .await
                    else {
                        return;
                    };
                    TradeIntent::Open {
                        leader,
                        symbol,
                        side,
                        leader_collateral_usd: collateral,
                        leader_leverage: leverage,
                        leader_exec_price: exec_price,
                        source_tx: tx_hash.clone(),
                        source_block: block,
                        signal_id,
                        symbol_id,
                    }
                } else {
                    let pnl = format_pct_1e10(&ev.percentProfit);
                    info!(
                        target: "intent",
                        kind = "CLOSE", source = "market", %symbol,
                        exec_price, pnl_pct = pnl, block, %tx_hash,
                        "leader CLOSE (market)"
                    );
                    let Some((signal_id, leader_entry_price)) = record_close(
                        pool, trader_id, &symbol, pair_idx, pos_idx, exec_price, &tx_hash, block,
                    )
                    .await
                    else {
                        return;
                    };
                    TradeIntent::Close {
                        leader,
                        symbol,
                        leader_pair_index: pair_idx,
                        leader_position_index: pos_idx,
                        leader_exec_price: exec_price,
                        leader_entry_price,
                        // `pnl` from format_pct_1e10 is in percent (1.84 = 1.84%);
                        // intents carry PnL as a fraction so /100 here.
                        leader_pnl_pct: Some(pnl / 100.0),
                        source_tx: tx_hash.clone(),
                        signal_id,
                    }
                };
                if let Err(e) = tx.send(intent).await {
                    error!(error = ?e, "intent channel closed; executor must have crashed");
                }
            }
            Err(e) => warn!(error = ?e, %tx_hash, "failed to decode MarketExecuted"),
        }
    } else if topic0 == <LimitExecuted as SolEvent>::SIGNATURE_HASH {
        match <LimitExecuted as SolEvent>::decode_log(&log.inner) {
            Ok(decoded) => {
                let ev = &decoded.data;
                if ev.t.trader != leader {
                    return;
                }
                let pair_idx: u64 = ev.t.pairIndex.try_into().unwrap_or(u64::MAX);
                let pos_idx: u64 = ev.t.index.try_into().unwrap_or(u64::MAX);
                let symbol = pair_name(pairs, pair_idx);
                let leverage = format_leverage(ev.t.leverage);
                let collateral = format_usdc(ev.positionSizeUSDC);
                let exec_price = format_1e10(ev.price);
                let side = if ev.t.buy { Side::Long } else { Side::Short };
                let pnl_pct = format_pct_1e10(&ev.percentProfit);

                // Route on orderType, NOT ev.isPnl — see ORDER_TYPE_LIMIT_OPEN
                // doc above. Cross-check against price/pct heuristics: a real
                // close has openPrice ≠ exec_price OR percentProfit ≠ 0; an
                // open has both equal/zero. If routing disagrees with that
                // invariant, the Avantis ABI has likely changed — surface a
                // loud warning so we notice before bleeding more positions.
                let is_close = ev.orderType != ORDER_TYPE_LIMIT_OPEN;
                let heuristic_close = ev.t.openPrice != ev.price || pnl_pct != 0.0;
                if is_close != heuristic_close {
                    warn!(
                        order_type = ev.orderType,
                        is_pnl = ev.isPnl,
                        is_close,
                        heuristic_close,
                        leader_open_price = %ev.t.openPrice,
                        exec_price_raw = %ev.price,
                        pct_profit = pnl_pct,
                        %tx_hash,
                        "LimitExecuted: orderType routing disagrees with price/pct heuristic — Avantis ABI may have changed"
                    );
                }

                let intent = if is_close {
                    info!(
                        target: "intent",
                        kind = "CLOSE", source = "keeper",
                        order_type = ev.orderType, is_pnl = ev.isPnl,
                        %symbol, exec_price, pnl_pct, block, %tx_hash,
                        "leader CLOSE (keeper)"
                    );
                    let Some((signal_id, leader_entry_price)) = record_close(
                        pool, trader_id, &symbol, pair_idx, pos_idx, exec_price, &tx_hash, block,
                    )
                    .await
                    else {
                        return;
                    };
                    TradeIntent::Close {
                        leader,
                        symbol,
                        leader_pair_index: pair_idx,
                        leader_position_index: pos_idx,
                        leader_exec_price: exec_price,
                        leader_entry_price,
                        // pnl is in percent (see MarketExecuted branch); /100 → fraction.
                        leader_pnl_pct: Some(pnl_pct / 100.0),
                        source_tx: tx_hash.clone(),
                        signal_id,
                    }
                } else {
                    info!(
                        target: "intent",
                        kind = "OPEN", source = "keeper",
                        order_type = ev.orderType, is_pnl = ev.isPnl,
                        %symbol, %side, collateral, leverage, exec_price, block, %tx_hash,
                        "leader OPEN (keeper)"
                    );
                    let Some((symbol_id, signal_id)) = record_open(
                        pool, trader_id, &symbol, side, pair_idx, pos_idx, collateral, leverage,
                        exec_price, &tx_hash, block,
                    )
                    .await
                    else {
                        return;
                    };
                    TradeIntent::Open {
                        leader,
                        symbol,
                        side,
                        leader_collateral_usd: collateral,
                        leader_leverage: leverage,
                        leader_exec_price: exec_price,
                        source_tx: tx_hash.clone(),
                        source_block: block,
                        signal_id,
                        symbol_id,
                    }
                };
                if let Err(e) = tx.send(intent).await {
                    error!(error = ?e, "intent channel closed; executor must have crashed");
                }
            }
            Err(e) => warn!(error = ?e, %tx_hash, "failed to decode LimitExecuted"),
        }
    } else {
        debug!(%topic0, %tx_hash, "log with unexpected topic0");
    }
}

fn pair_name(pairs: &HashMap<u64, String>, idx: u64) -> String {
    pairs
        .get(&idx)
        .cloned()
        .unwrap_or_else(|| format!("#{idx}"))
}

fn format_pct_1e10(raw: &alloy::primitives::I256) -> f64 {
    let s = raw.to_string();
    s.parse::<f64>().unwrap_or(0.0) / 1e10
}

#[allow(clippy::too_many_arguments)]
async fn record_open(
    pool: &DbPool,
    trader_id: i32,
    symbol: &str,
    side: Side,
    pair_idx: u64,
    pos_idx: u64,
    collateral: f64,
    leverage: u64,
    exec_price: f64,
    tx_hash: &str,
    block: u64,
) -> Option<(i32, i32)> {
    let symbol_id = match symbols::upsert(pool, symbol).await {
        Ok(id) => id,
        Err(e) => {
            error!(error = ?e, %tx_hash, %symbol, "failed to upsert symbol; dropping intent");
            return None;
        }
    };
    let signal_id = match signals::record_open(
        pool,
        trader_id,
        SOURCE_DEX,
        symbol_id,
        side,
        pair_idx as i64,
        pos_idx as i64,
        collateral,
        leverage as i32,
        exec_price,
        tx_hash,
        block as i64,
    )
    .await
    {
        Ok(id) => id,
        Err(e) => {
            error!(error = ?e, %tx_hash, "failed to record signal OPEN; dropping intent");
            return None;
        }
    };
    Some((symbol_id, signal_id))
}

#[allow(clippy::too_many_arguments)]
async fn record_close(
    pool: &DbPool,
    trader_id: i32,
    symbol: &str,
    pair_idx: u64,
    pos_idx: u64,
    exec_price: f64,
    tx_hash: &str,
    block: u64,
) -> Option<(i32, f64)> {
    match signals::record_close(
        pool,
        trader_id,
        pair_idx as i64,
        pos_idx as i64,
        exec_price,
        tx_hash,
        block as i64,
    )
    .await
    {
        Ok(Some(row)) => Some(row),
        Ok(None) => {
            warn!(
                %symbol, pair_idx, pos_idx, %tx_hash,
                "leader CLOSE for unknown signal (likely opened before bot started); skipping"
            );
            None
        }
        Err(e) => {
            error!(error = ?e, %tx_hash, "failed to record signal CLOSE; dropping intent");
            None
        }
    }
}
