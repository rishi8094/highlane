use std::collections::HashMap;
use std::time::{Duration, Instant};

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
use crate::db::{DbPool, signals, symbols, trades};
use crate::shared::dex::Dex;
use crate::shared::intent::{Side, TradeIntent};
use crate::shared::notify::{DiscordNotifier, OrphanAlert, OrphanKind};

const SOURCE_DEX: Dex = Dex::Avantis;

const BACKOFF_INITIAL: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(30);

/// Max blocks per `eth_getLogs` page during backfill. Chosen to fit under
/// typical RPC range caps (Alchemy ≈ 2000 on Base, QuickNode similar) with
/// headroom. With Base's ~2s block time this is ~50 minutes of history per
/// page — long outages are recovered by paging through multiple chunks.
const BACKFILL_PAGE_BLOCKS: u64 = 1500;

/// Outcome of `handle_log`. The watcher loop uses this to decide whether to
/// advance the block cursor: only `Ingested` and `Irrelevant` are safe to
/// advance past. A `Failed` log means a downstream write (DB or intent
/// channel) errored and we want the next session/backfill to re-deliver the
/// log, so the cursor must stay where it was.
#[derive(Debug, Clone, Copy)]
enum HandleStatus {
    /// Log was for our leader and was fully persisted (or was an idempotent
    /// no-op like a re-delivered close).
    Ingested,
    /// Log was not for our leader, had an unexpected topic, failed to decode,
    /// or was a CLOSE for a signal we never opened — none of which a retry
    /// will fix, so the cursor can move past it.
    Irrelevant,
    /// A downstream write failed (DB error, intent channel closed). Retry on
    /// the next backfill is the only path to recovery, so the cursor must
    /// not advance past this log.
    Failed,
}

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
    notifier: DiscordNotifier,
) -> ! {
    let mut backoff = BACKOFF_INITIAL;
    // Fire "disconnected" only on the connected→disconnected edge, and
    // "reconnected" only after we'd previously notified a disconnect — so
    // we don't spam on every retry while still down, and the existing
    // startup embed covers first connect.
    let mut connected = false;
    let mut disconnect_pending_reconnect = false;
    let mut disconnected_at: Option<Instant> = None;
    // Highest block we've successfully ingested. Seeded from the DB on
    // first start so we can backfill events that fired while the bot was
    // offline; advanced on every handled log. Without this, every WS
    // reconnect (or process restart) opens a window where leader trades
    // are silently dropped → drift between copy-state and Lighter.
    let mut last_seen_block: Option<u64> = match signals::max_seen_block(&pool, trader_id).await {
        Ok(Some(n)) if n >= 0 => Some(n as u64),
        Ok(_) => None,
        Err(e) => {
            warn!(error = ?e, "failed to read max_seen_block on startup; backfill disabled until next handled log");
            None
        }
    };
    if let Some(b) = last_seen_block {
        info!(last_seen_block = b, "watcher resume point loaded");
    } else {
        info!("no prior leader signals — watcher will start from live head only");
    }
    loop {
        let result = run_session(
            ws_url,
            leader,
            trader_id,
            &pool,
            &tx,
            &notifier,
            &mut connected,
            &mut disconnect_pending_reconnect,
            &mut disconnected_at,
            &mut last_seen_block,
        )
        .await;
        let was_connected = connected;
        connected = false;
        match result {
            Ok(()) => {
                warn!("subscription stream ended; reconnecting");
                if was_connected {
                    notifier.notify_watcher_disconnected("subscription stream ended");
                    disconnect_pending_reconnect = true;
                    disconnected_at = Some(Instant::now());
                }
                backoff = BACKOFF_INITIAL;
            }
            Err(e) => {
                error!(error = ?e, backoff_secs = backoff.as_secs(), "watcher session failed");
                if was_connected {
                    notifier.notify_watcher_disconnected(&format!("{e}"));
                    disconnect_pending_reconnect = true;
                    disconnected_at = Some(Instant::now());
                }
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(BACKOFF_MAX);
                continue;
            }
        }
        tokio::time::sleep(backoff).await;
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_session(
    ws_url: &str,
    leader: Address,
    trader_id: i32,
    pool: &DbPool,
    tx: &mpsc::Sender<TradeIntent>,
    notifier: &DiscordNotifier,
    connected: &mut bool,
    disconnect_pending_reconnect: &mut bool,
    disconnected_at: &mut Option<Instant>,
    last_seen_block: &mut Option<u64>,
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

    let base_filter = Filter::new().address(callbacks_addr).event_signature(vec![
        <MarketExecuted as SolEvent>::SIGNATURE_HASH,
        <LimitExecuted as SolEvent>::SIGNATURE_HASH,
    ]);

    let sub = provider.subscribe_logs(&base_filter).await?;
    info!(%callbacks_addr, "subscribed to MarketExecuted + LimitExecuted logs");
    *connected = true;
    if *disconnect_pending_reconnect {
        let downtime = disconnected_at.map(|t| t.elapsed().as_secs()).unwrap_or(0);
        notifier.notify_watcher_reconnected(downtime);
        *disconnect_pending_reconnect = false;
        *disconnected_at = None;
    }

    // Backfill any logs that fired while we were offline / between sessions.
    // The live subscription only delivers events from "now" forward, so a
    // process restart or WS drop would otherwise silently miss leader
    // trades — exactly the failure mode that produced the BTC sign-flip
    // drift on 2026-05-05. record_open / record_close are idempotent on
    // (trader_id, entry_tx, leader_position_index) and on `exit_at IS NULL`
    // respectively, so any overlap with the live stream re-delivering the
    // same log is a no-op.
    // Once any log in this session fails to persist, freeze the cursor for
    // the rest of the session. Otherwise a transient DB error on log A
    // followed by a successful log B at a later block would advance the
    // cursor past A, and the next backfill would skip A forever.
    let mut cursor_locked = false;

    if let Some(from) = *last_seen_block {
        match provider.get_block_number().await {
            Ok(latest) if latest >= from => {
                // Replay from `from` (inclusive), not `from + 1`. The cursor
                // is block-resolution but a single block can contain
                // multiple Avantis callback logs; if we crashed between
                // handling two logs in the same block, `from + 1` would
                // skip the un-processed sibling forever. record_open is
                // idempotent (ON CONFLICT DO NOTHING, returns was_new=false
                // on replay) and record_close filters on `exit_at IS NULL`,
                // so re-delivering the boundary block's already-handled
                // log is a no-op.
                //
                // Page through the range so a long outage doesn't trip the
                // RPC's per-call range cap (~2000 blocks on most providers).
                // On any per-page failure we page Discord and stop trying
                // — silently dropping events into a logs-only warning is
                // exactly what produced the BTC drift we're fixing.
                info!(
                    from_block = from,
                    to_block = latest,
                    page_size = BACKFILL_PAGE_BLOCKS,
                    "backfilling missed leader logs"
                );
                let mut page_from = from;
                let mut total = 0usize;
                'pages: while page_from <= latest {
                    let page_to =
                        page_from.saturating_add(BACKFILL_PAGE_BLOCKS - 1).min(latest);
                    let backfill_filter =
                        base_filter.clone().from_block(page_from).to_block(page_to);
                    match provider.get_logs(&backfill_filter).await {
                        Ok(logs) => {
                            total += logs.len();
                            for log in &logs {
                                let status = handle_log(
                                    log, leader, trader_id, pool, &pairs, tx, notifier,
                                )
                                .await;
                                if matches!(status, HandleStatus::Failed) {
                                    cursor_locked = true;
                                }
                                if !cursor_locked
                                    && let Some(b) = log.block_number
                                    && b > last_seen_block.unwrap_or(0)
                                {
                                    *last_seen_block = Some(b);
                                }
                            }
                        }
                        Err(e) => {
                            error!(
                                error = ?e,
                                from_block = page_from,
                                to_block = page_to,
                                "backfill get_logs failed; cannot recover this window"
                            );
                            if notifier.enabled() {
                                notifier.notify_orphan(OrphanAlert {
                                    kind: OrphanKind::BackfillFailed,
                                    symbol: "*".into(),
                                    market_id: -1,
                                    expected_signed_size: 0,
                                    actual_signed_size: 0,
                                    note: format!(
                                        "from_block={page_from} to_block={page_to} error={e}"
                                    ),
                                });
                            }
                            // Lock cursor — anything after this gap is
                            // suspect until an operator reconciles.
                            cursor_locked = true;
                            break 'pages;
                        }
                    }
                    if page_to == latest {
                        break;
                    }
                    page_from = page_to + 1;
                }
                info!(count = total, "backfill complete");
            }
            Ok(_) => {}
            Err(e) => {
                error!(error = ?e, "could not query latest block for backfill");
                if notifier.enabled() {
                    notifier.notify_orphan(OrphanAlert {
                        kind: OrphanKind::BackfillFailed,
                        symbol: "*".into(),
                        market_id: -1,
                        expected_signed_size: 0,
                        actual_signed_size: 0,
                        note: format!("get_block_number failed: {e}"),
                    });
                }
                cursor_locked = true;
            }
        }
    }

    let mut stream = sub.into_stream();
    while let Some(log) = stream.next().await {
        let status = handle_log(&log, leader, trader_id, pool, &pairs, tx, notifier).await;
        if matches!(status, HandleStatus::Failed) {
            cursor_locked = true;
        }
        if !cursor_locked
            && let Some(b) = log.block_number
            && b > last_seen_block.unwrap_or(0)
        {
            *last_seen_block = Some(b);
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn handle_log(
    log: &alloy::rpc::types::Log,
    leader: Address,
    trader_id: i32,
    pool: &DbPool,
    pairs: &HashMap<u64, String>,
    tx: &mpsc::Sender<TradeIntent>,
    notifier: &DiscordNotifier,
) -> HandleStatus {
    let topic0 = match log.topic0() {
        Some(t) => *t,
        None => return HandleStatus::Irrelevant,
    };
    let tx_hash = log
        .transaction_hash
        .map(|h| h.to_string())
        .unwrap_or_else(|| "?".into());
    let block = log.block_number.unwrap_or_default();

    if topic0 == <MarketExecuted as SolEvent>::SIGNATURE_HASH {
        let decoded = match <MarketExecuted as SolEvent>::decode_log(&log.inner) {
            Ok(d) => d,
            Err(e) => {
                warn!(error = ?e, %tx_hash, "failed to decode MarketExecuted");
                return HandleStatus::Irrelevant;
            }
        };
        let ev = &decoded.data;
        if ev.t.trader != leader {
            return HandleStatus::Irrelevant;
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
            match record_open(
                pool, trader_id, &symbol, side, pair_idx, pos_idx, collateral, leverage,
                exec_price, &tx_hash, block,
            )
            .await
            {
                Ok((symbol_id, signal_id, was_new)) => {
                    if !was_new {
                        match replay_should_re_emit(pool, signal_id, &symbol, &tx_hash, notifier).await {
                            Ok(true) => {}
                            Ok(false) => return HandleStatus::Ingested,
                            Err(e) => {
                                error!(error = ?e, signal_id, %tx_hash, "failed to query trade existence for replay decision; cursor will not advance past this log");
                                return HandleStatus::Failed;
                            }
                        }
                    }
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
                }
                Err(e) => {
                    error!(error = ?e, %tx_hash, "failed to record OPEN to DB; cursor will not advance past this log");
                    return HandleStatus::Failed;
                }
            }
        } else {
            let pnl = format_pct_1e10(&ev.percentProfit);
            info!(
                target: "intent",
                kind = "CLOSE", source = "market", %symbol,
                exec_price, pnl_pct = pnl, block, %tx_hash,
                "leader CLOSE (market)"
            );
            match record_close(
                pool, trader_id, &symbol, pair_idx, pos_idx, exec_price, &tx_hash, block,
            )
            .await
            {
                Ok(Some((signal_id, leader_entry_price))) => TradeIntent::Close {
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
                },
                Ok(None) => return HandleStatus::Irrelevant,
                Err(e) => {
                    error!(error = ?e, %tx_hash, "failed to record CLOSE to DB; cursor will not advance past this log");
                    return HandleStatus::Failed;
                }
            }
        };
        if let Err(e) = tx.send(intent).await {
            error!(error = ?e, "intent channel closed; executor must have crashed");
            return HandleStatus::Failed;
        }
        HandleStatus::Ingested
    } else if topic0 == <LimitExecuted as SolEvent>::SIGNATURE_HASH {
        let decoded = match <LimitExecuted as SolEvent>::decode_log(&log.inner) {
            Ok(d) => d,
            Err(e) => {
                warn!(error = ?e, %tx_hash, "failed to decode LimitExecuted");
                return HandleStatus::Irrelevant;
            }
        };
        let ev = &decoded.data;
        if ev.t.trader != leader {
            return HandleStatus::Irrelevant;
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
            match record_close(
                pool, trader_id, &symbol, pair_idx, pos_idx, exec_price, &tx_hash, block,
            )
            .await
            {
                Ok(Some((signal_id, leader_entry_price))) => TradeIntent::Close {
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
                },
                Ok(None) => return HandleStatus::Irrelevant,
                Err(e) => {
                    error!(error = ?e, %tx_hash, "failed to record CLOSE to DB; cursor will not advance past this log");
                    return HandleStatus::Failed;
                }
            }
        } else {
            info!(
                target: "intent",
                kind = "OPEN", source = "keeper",
                order_type = ev.orderType, is_pnl = ev.isPnl,
                %symbol, %side, collateral, leverage, exec_price, block, %tx_hash,
                "leader OPEN (keeper)"
            );
            match record_open(
                pool, trader_id, &symbol, side, pair_idx, pos_idx, collateral, leverage,
                exec_price, &tx_hash, block,
            )
            .await
            {
                Ok((symbol_id, signal_id, was_new)) => {
                    if !was_new {
                        match replay_should_re_emit(pool, signal_id, &symbol, &tx_hash, notifier).await {
                            Ok(true) => {}
                            Ok(false) => return HandleStatus::Ingested,
                            Err(e) => {
                                error!(error = ?e, signal_id, %tx_hash, "failed to query trade existence for replay decision; cursor will not advance past this log");
                                return HandleStatus::Failed;
                            }
                        }
                    }
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
                }
                Err(e) => {
                    error!(error = ?e, %tx_hash, "failed to record OPEN to DB; cursor will not advance past this log");
                    return HandleStatus::Failed;
                }
            }
        };
        if let Err(e) = tx.send(intent).await {
            error!(error = ?e, "intent channel closed; executor must have crashed");
            return HandleStatus::Failed;
        }
        HandleStatus::Ingested
    } else {
        debug!(%topic0, %tx_hash, "log with unexpected topic0");
        HandleStatus::Irrelevant
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

/// On replay (signal row already existed), return whether the executor
/// should be told to re-fire the OPEN. Always returns `false` — we never
/// re-emit on replay, because we cannot distinguish between two crash
/// windows that look identical from the DB:
///
///   1. Executor crashed BEFORE `send_tx`. No IOC was ever sent.
///   2. Executor crashed AFTER `send_tx` but BEFORE `trades::record_open`
///      committed. The IOC went through and Lighter is holding the
///      position we never recorded.
///
/// Re-firing would silently double the position in case (2). The
/// conservative choice — never re-fire — instead leaves case (1) as a
/// missed mirror and case (2) for `reconcile_on_startup` to surface as an
/// ORPHAN. We page on the ambiguous "signals row exists, trades row
/// missing" case so the operator can inspect Lighter and either record
/// the trade manually or accept the missed mirror.
///
/// Closing this race properly requires either (a) deterministic per-signal
/// `client_order_id` with Lighter-side dedup, or (b) a durable pending-trade
/// outbox written before `send_tx`. Both are bigger lifts than this safety
/// net.
async fn replay_should_re_emit(
    pool: &DbPool,
    signal_id: i32,
    symbol: &str,
    tx_hash: &str,
    notifier: &DiscordNotifier,
) -> Result<bool> {
    if trades::any_for_signal(pool, signal_id).await? {
        info!(target: "intent", signal_id, %tx_hash, "OPEN already mirrored — skipping intent (replay)");
    } else {
        warn!(
            target: "intent",
            signal_id, %tx_hash, %symbol,
            "OPEN signal exists but no follower trade — NOT re-emitting (cannot distinguish missed-IOC from sent-but-uncommitted); manual review required"
        );
        if notifier.enabled() {
            notifier.notify_orphan(OrphanAlert {
                kind: OrphanKind::ReplayMirrorMissing,
                symbol: symbol.to_string(),
                // Avantis pair_idx, not Lighter's market_id — the watcher
                // doesn't know the destination's id. Operator can resolve
                // via the symbol.
                market_id: -1,
                expected_signed_size: 0,
                actual_signed_size: 0,
                note: format!(
                    "signal_id={signal_id} leader_tx={tx_hash} — DB has signal but no follower trade. Inspect Lighter for an unrecorded position before deciding whether to re-mirror."
                ),
            });
        }
    }
    Ok(false)
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
) -> Result<(i32, i32, bool)> {
    let symbol_id = symbols::upsert(pool, symbol).await?;
    let (signal_id, was_new) = signals::record_open(
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
    .await?;
    Ok((symbol_id, signal_id, was_new))
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
) -> Result<Option<(i32, f64)>> {
    let row = signals::record_close(
        pool,
        trader_id,
        pair_idx as i64,
        pos_idx as i64,
        exec_price,
        tx_hash,
        block as i64,
    )
    .await?;
    if row.is_none() {
        warn!(
            %symbol, pair_idx, pos_idx, %tx_hash,
            "leader CLOSE for unknown signal (likely opened before bot started); skipping"
        );
    }
    Ok(row)
}
