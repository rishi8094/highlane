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
use crate::shared::dex::lighter::markets::symbol_root;
use crate::shared::intent::{Side, TradeIntent};
use crate::shared::notify::{DiscordNotifier, OrphanAlert, OrphanKind};

const SOURCE_DEX: Dex = Dex::Avantis;

/// One leader the watcher is following on Avantis. Built by `main` from the
/// parsed `config.pkl` plus the resulting `traders` row id.
#[derive(Debug, Clone)]
pub struct WatchedLeader {
    pub wallet: Address,
    pub trader_id: i32,
    pub name: String,
    pub copy_ratio: f64,
    /// Normalized via `markets::symbol_root` + uppercase on load (see
    /// `config::validate`), so runtime comparison is a direct equality check.
    pub allowed_tokens: Vec<String>,
}

const BACKOFF_INITIAL: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(30);

/// Max blocks per `eth_getLogs` page during backfill. Chosen to fit under
/// typical RPC range caps (Alchemy ≈ 2000 on Base, QuickNode similar) with
/// headroom. With Base's ~2s block time this is ~50 minutes of history per
/// page — long outages are recovered by paging through multiple chunks.
const BACKFILL_PAGE_BLOCKS: u64 = 1500;

/// Max time we'll wait on `stream.next()` before assuming the WS log
/// subscription is silently dead and forcing a reconnect. alloy's WS
/// transport reconnects the underlying socket transparently, but the
/// server-side log subscription does NOT survive that reconnect — the
/// stream just stops yielding forever (the 2026-05-13/14 15-hour blackout
/// where the watcher saw zero leader events while the chain kept producing
/// them). On expiry we return Err; the outer loop re-enters `run_session`,
/// which re-subscribes AND replays `eth_getLogs` from `last_seen_block`,
/// so any leader events that fired during the silent window are caught up
/// idempotently. False positives during legitimately-quiet stretches just
/// cost one extra paged `get_logs` and are harmless.
const STREAM_IDLE_TIMEOUT: Duration = Duration::from_secs(5 * 60);

/// Outcome of `handle_log`. The watcher loop uses this to decide whether to
/// advance per-leader block cursors: only the cursor for the matched
/// `trader_id` (carried inside the variant) is touched, so an unrelated
/// leader's cursor never advances on a log that wasn't theirs.
///
/// `Failed` indicates a downstream write (DB or intent channel) errored and
/// the next session/backfill must re-deliver the log — the matched leader's
/// cursor is frozen for the remainder of the session.
#[derive(Debug, Clone, Copy)]
enum HandleStatus {
    /// Log was for a watched leader and was fully persisted (or was an
    /// idempotent no-op like a re-delivered close). Carries the matched
    /// leader's `trader_id` so the loop knows whose cursor to advance.
    Ingested(i32),
    /// Log was not for any watched leader, had an unexpected topic, failed
    /// to decode, or was a CLOSE for a signal we never opened — none of
    /// which advance any leader's cursor.
    Irrelevant,
    /// A downstream write failed (DB error, intent channel closed) for a
    /// matched leader. Carries the matched leader's `trader_id`; only that
    /// leader's cursor is frozen.
    Failed(i32),
}

/// Avantis `LimitExecuted.orderType` enum:
///   0 = TP, 1 = SL, 2 = LIQ (all closes), 3 = LIMIT_OPEN (open).
/// Anything other than 3 is a keeper-driven close. We route on this rather
/// than `ev.isPnl` because `isPnl` is observed `false` even on SL/TP/LIQ
/// closes in production — using it caused us to record TP/SL closes as
/// fresh OPENs at the trigger price (see signal 653 LIT/USD on 2026-04-29).
const ORDER_TYPE_LIMIT_OPEN: u8 = 3;

pub async fn watch_leaders(
    ws_url: &str,
    leaders: Vec<WatchedLeader>,
    pool: DbPool,
    tx: mpsc::Sender<TradeIntent>,
    notifier: DiscordNotifier,
) -> ! {
    if leaders.is_empty() {
        error!("watch_leaders called with empty leader set — nothing to do");
        // Park forever rather than spinning; main will treat watcher-return as
        // a shutdown condition.
        std::future::pending::<()>().await;
        unreachable!()
    }
    let mut backoff = BACKOFF_INITIAL;
    // Fire "disconnected" only on the connected→disconnected edge, and
    // "reconnected" only after we'd previously notified a disconnect — so
    // we don't spam on every retry while still down, and the existing
    // startup embed covers first connect.
    let mut connected = false;
    let mut disconnect_pending_reconnect = false;
    let mut disconnected_at: Option<Instant> = None;
    // Per-leader resume cursor: highest block we've ever ingested for each
    // trader_id. Aggregate-only resume would skip history for any newly
    // added leader whose first activity predates the high-water of an
    // existing leader, so each leader carries its own block.
    let mut resume_blocks: HashMap<i32, Option<u64>> = HashMap::new();
    for l in &leaders {
        let resume = match signals::max_seen_block(&pool, l.trader_id).await {
            Ok(Some(n)) if n >= 0 => Some(n as u64),
            Ok(_) => None,
            Err(e) => {
                warn!(leader = %l.name, error = ?e, "failed to read max_seen_block on startup; backfill disabled for this leader until next handled log");
                None
            }
        };
        if let Some(b) = resume {
            info!(leader = %l.name, last_seen_block = b, "watcher resume point loaded");
        } else {
            info!(leader = %l.name, "no prior leader signals — watcher will start from live head only");
        }
        resume_blocks.insert(l.trader_id, resume);
    }
    loop {
        let result = run_session(
            ws_url,
            &leaders,
            &pool,
            &tx,
            &notifier,
            &mut connected,
            &mut disconnect_pending_reconnect,
            &mut disconnected_at,
            &mut resume_blocks,
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
    leaders: &[WatchedLeader],
    pool: &DbPool,
    tx: &mpsc::Sender<TradeIntent>,
    notifier: &DiscordNotifier,
    connected: &mut bool,
    disconnect_pending_reconnect: &mut bool,
    disconnected_at: &mut Option<Instant>,
    resume_blocks: &mut HashMap<i32, Option<u64>>,
) -> Result<()> {
    let wallets: Vec<String> = leaders.iter().map(|l| l.wallet.to_string()).collect();
    info!(leader_count = leaders.len(), leaders = ?wallets, "connecting to Base WSS");
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

    // Backfill each leader independently. The page filter is shared (one
    // RPC range per leader) but the per-log handler only matches against
    // that leader's wallet, so a rewound cursor for leader A cannot
    // re-emit historical logs that already advanced leader B's cursor.
    //
    // record_open / record_close are idempotent on (trader_id, entry_tx,
    // leader_position_index) and on `exit_at IS NULL` respectively, so any
    // overlap with the live stream re-delivering the same log is a no-op.
    let latest = match provider.get_block_number().await {
        Ok(n) => Some(n),
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
            None
        }
    };

    // Per-leader cursor-lock flags. Live mode below merges them back so a
    // backfill failure for one leader doesn't freeze the live cursor of
    // the others.
    let mut cursor_locked_per_leader: HashMap<i32, bool> =
        leaders.iter().map(|l| (l.trader_id, false)).collect();

    if let Some(latest) = latest {
        for leader in leaders {
            let resume = resume_blocks
                .get(&leader.trader_id)
                .copied()
                .unwrap_or(None);
            let Some(from) = resume else { continue };
            if latest < from {
                continue;
            }
            info!(
                leader = %leader.name,
                from_block = from,
                to_block = latest,
                page_size = BACKFILL_PAGE_BLOCKS,
                "backfilling missed leader logs"
            );
            let mut page_from = from;
            let mut total = 0usize;
            'pages: while page_from <= latest {
                let page_to = page_from
                    .saturating_add(BACKFILL_PAGE_BLOCKS - 1)
                    .min(latest);
                let backfill_filter = base_filter.clone().from_block(page_from).to_block(page_to);
                match provider.get_logs(&backfill_filter).await {
                    Ok(logs) => {
                        total += logs.len();
                        for log in &logs {
                            // Pass a singleton slice so backfill is scoped
                            // strictly to this leader's policy — no other
                            // leader's filter runs over this page.
                            let status = handle_log(
                                log,
                                std::slice::from_ref(leader),
                                pool,
                                &pairs,
                                tx,
                                notifier,
                            )
                            .await;
                            // The matched trader_id from `handle_log` will
                            // always equal `leader.trader_id` here (singleton
                            // slice), but we still drive cursor changes off
                            // the returned variant so the live and backfill
                            // paths share the same shape: advance only on
                            // Ingested, lock only on Failed.
                            match status {
                                HandleStatus::Ingested(_) => {
                                    let locked = cursor_locked_per_leader
                                        .get(&leader.trader_id)
                                        .copied()
                                        .unwrap_or(false);
                                    if !locked && let Some(b) = log.block_number {
                                        let cur =
                                            resume_blocks.entry(leader.trader_id).or_insert(None);
                                        if b > cur.unwrap_or(0) {
                                            *cur = Some(b);
                                        }
                                    }
                                }
                                HandleStatus::Failed(_) => {
                                    cursor_locked_per_leader.insert(leader.trader_id, true);
                                }
                                HandleStatus::Irrelevant => {}
                            }
                        }
                    }
                    Err(e) => {
                        error!(
                            leader = %leader.name,
                            error = ?e,
                            from_block = page_from,
                            to_block = page_to,
                            "backfill get_logs failed; cannot recover this window"
                        );
                        if notifier.enabled() {
                            notifier.notify_orphan(OrphanAlert {
                                kind: OrphanKind::BackfillFailed,
                                symbol: leader.name.clone(),
                                market_id: -1,
                                expected_signed_size: 0,
                                actual_signed_size: 0,
                                note: format!(
                                    "leader={} from_block={page_from} to_block={page_to} error={e}",
                                    leader.name
                                ),
                            });
                        }
                        // Lock this leader's cursor — anything after this
                        // gap is suspect until an operator reconciles.
                        cursor_locked_per_leader.insert(leader.trader_id, true);
                        break 'pages;
                    }
                }
                if page_to == latest {
                    break;
                }
                page_from = page_to + 1;
            }
            info!(leader = %leader.name, count = total, "backfill complete");
        }
    }

    let mut stream = sub.into_stream();
    loop {
        match tokio::time::timeout(STREAM_IDLE_TIMEOUT, stream.next()).await {
            Ok(Some(log)) => {
                // Live WS sees one log at a time; pass the whole leader set
                // so `handle_log` can find the matching leader cheaply. The
                // returned `HandleStatus` carries the matched `trader_id`
                // (when there was a match) so the cursor change is scoped
                // to that one leader — an unrelated leader never advances
                // its own cursor just because some other leader had
                // activity at the same block. That's the per-leader resume
                // guarantee the backfill loop relies on when a leader is
                // newly added and needs to replay its own history.
                let status = handle_log(&log, leaders, pool, &pairs, tx, notifier).await;
                match status {
                    HandleStatus::Ingested(trader_id) => {
                        let locked = cursor_locked_per_leader
                            .get(&trader_id)
                            .copied()
                            .unwrap_or(false);
                        if !locked && let Some(b) = log.block_number {
                            let cur = resume_blocks.entry(trader_id).or_insert(None);
                            if b > cur.unwrap_or(0) {
                                *cur = Some(b);
                            }
                        }
                    }
                    HandleStatus::Failed(trader_id) => {
                        cursor_locked_per_leader.insert(trader_id, true);
                    }
                    HandleStatus::Irrelevant => {}
                }
            }
            Ok(None) => {
                warn!("WS log stream returned None — subscription ended");
                return Ok(());
            }
            Err(_) => {
                warn!(
                    timeout_secs = STREAM_IDLE_TIMEOUT.as_secs(),
                    "WS log subscription idle past timeout — forcing reconnect so backfill replays any missed range"
                );
                return Err(eyre::eyre!(
                    "WS subscription idle past {}s — forcing reconnect",
                    STREAM_IDLE_TIMEOUT.as_secs()
                ));
            }
        }
    }
}

async fn handle_log(
    log: &alloy::rpc::types::Log,
    leaders: &[WatchedLeader],
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
        let Some(leader) = leaders.iter().find(|l| l.wallet == ev.t.trader) else {
            return HandleStatus::Irrelevant;
        };
        let pair_idx: u64 = ev.t.pairIndex.try_into().unwrap_or(u64::MAX);
        let pos_idx: u64 = ev.t.index.try_into().unwrap_or(u64::MAX);
        let symbol = pair_name(pairs, pair_idx);
        let leverage = format_leverage(ev.t.leverage);
        let collateral = format_usdc(ev.positionSizeUSDC);
        let exec_price = format_1e10(ev.price);
        let side = if ev.t.buy { Side::Long } else { Side::Short };

        let intent = if ev.open {
            if !leader_allows_token(leader, &symbol) {
                debug!(
                    target: "intent",
                    leader = %leader.name, %symbol,
                    "leader OPEN on disallowed token — skipping"
                );
                return HandleStatus::Irrelevant;
            }
            info!(
                target: "intent",
                leader = %leader.name,
                kind = "OPEN", source = "market", %symbol, %side,
                collateral, leverage, exec_price, block, %tx_hash,
                "leader OPEN (market)"
            );
            match record_open(
                pool,
                leader.trader_id,
                &symbol,
                side,
                pair_idx,
                pos_idx,
                collateral,
                leverage,
                exec_price,
                &tx_hash,
                block,
            )
            .await
            {
                Ok((symbol_id, signal_id, was_new)) => {
                    if !was_new {
                        match replay_should_re_emit(pool, signal_id, &symbol, &tx_hash, notifier)
                            .await
                        {
                            Ok(true) => {}
                            Ok(false) => return HandleStatus::Ingested(leader.trader_id),
                            Err(e) => {
                                error!(error = ?e, leader = %leader.name, signal_id, %tx_hash, "failed to query trade existence for replay decision; cursor will not advance past this log");
                                return HandleStatus::Failed(leader.trader_id);
                            }
                        }
                    }
                    TradeIntent::Open {
                        leader: leader.wallet,
                        leader_name: leader.name.clone(),
                        copy_ratio: leader.copy_ratio,
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
                    error!(error = ?e, leader = %leader.name, %tx_hash, "failed to record OPEN to DB; cursor will not advance past this log");
                    return HandleStatus::Failed(leader.trader_id);
                }
            }
        } else {
            // Closes are always processed for known leader signals,
            // regardless of allowed_tokens. Skipping a close when the
            // token has since been removed from the allow-list would
            // strand a follower position that reconciliation cannot detect
            // (DB and Lighter would still agree on the open). The signals
            // lookup is keyed on (trader_id, pair_idx, pos_idx) — if no
            // matching open row exists, record_close returns Ok(None) and
            // the close is reported as unmatched downstream.
            let pnl = format_pct_1e10(&ev.percentProfit);
            info!(
                target: "intent",
                leader = %leader.name,
                kind = "CLOSE", source = "market", %symbol,
                exec_price, pnl_pct = pnl, block, %tx_hash,
                "leader CLOSE (market)"
            );
            match record_close(
                pool,
                leader.trader_id,
                &symbol,
                pair_idx,
                pos_idx,
                exec_price,
                &tx_hash,
                block,
            )
            .await
            {
                Ok(Some((signal_id, leader_entry_price))) => TradeIntent::Close {
                    leader: leader.wallet,
                    leader_name: leader.name.clone(),
                    copy_ratio: leader.copy_ratio,
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
                    error!(error = ?e, leader = %leader.name, %tx_hash, "failed to record CLOSE to DB; cursor will not advance past this log");
                    return HandleStatus::Failed(leader.trader_id);
                }
            }
        };
        if let Err(e) = tx.send(intent).await {
            error!(error = ?e, "intent channel closed; executor must have crashed");
            return HandleStatus::Failed(leader.trader_id);
        }
        HandleStatus::Ingested(leader.trader_id)
    } else if topic0 == <LimitExecuted as SolEvent>::SIGNATURE_HASH {
        let decoded = match <LimitExecuted as SolEvent>::decode_log(&log.inner) {
            Ok(d) => d,
            Err(e) => {
                warn!(error = ?e, %tx_hash, "failed to decode LimitExecuted");
                return HandleStatus::Irrelevant;
            }
        };
        let ev = &decoded.data;
        let Some(leader) = leaders.iter().find(|l| l.wallet == ev.t.trader) else {
            return HandleStatus::Irrelevant;
        };
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
                leader = %leader.name,
                kind = "CLOSE", source = "keeper",
                order_type = ev.orderType, is_pnl = ev.isPnl,
                %symbol, exec_price, pnl_pct, block, %tx_hash,
                "leader CLOSE (keeper)"
            );
            match record_close(
                pool,
                leader.trader_id,
                &symbol,
                pair_idx,
                pos_idx,
                exec_price,
                &tx_hash,
                block,
            )
            .await
            {
                Ok(Some((signal_id, leader_entry_price))) => TradeIntent::Close {
                    leader: leader.wallet,
                    leader_name: leader.name.clone(),
                    copy_ratio: leader.copy_ratio,
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
                    error!(error = ?e, leader = %leader.name, %tx_hash, "failed to record CLOSE to DB; cursor will not advance past this log");
                    return HandleStatus::Failed(leader.trader_id);
                }
            }
        } else {
            if !leader_allows_token(leader, &symbol) {
                debug!(
                    target: "intent",
                    leader = %leader.name, %symbol,
                    "leader OPEN on disallowed token — skipping"
                );
                return HandleStatus::Irrelevant;
            }
            info!(
                target: "intent",
                leader = %leader.name,
                kind = "OPEN", source = "keeper",
                order_type = ev.orderType, is_pnl = ev.isPnl,
                %symbol, %side, collateral, leverage, exec_price, block, %tx_hash,
                "leader OPEN (keeper)"
            );
            match record_open(
                pool,
                leader.trader_id,
                &symbol,
                side,
                pair_idx,
                pos_idx,
                collateral,
                leverage,
                exec_price,
                &tx_hash,
                block,
            )
            .await
            {
                Ok((symbol_id, signal_id, was_new)) => {
                    if !was_new {
                        match replay_should_re_emit(pool, signal_id, &symbol, &tx_hash, notifier)
                            .await
                        {
                            Ok(true) => {}
                            Ok(false) => return HandleStatus::Ingested(leader.trader_id),
                            Err(e) => {
                                error!(error = ?e, leader = %leader.name, signal_id, %tx_hash, "failed to query trade existence for replay decision; cursor will not advance past this log");
                                return HandleStatus::Failed(leader.trader_id);
                            }
                        }
                    }
                    TradeIntent::Open {
                        leader: leader.wallet,
                        leader_name: leader.name.clone(),
                        copy_ratio: leader.copy_ratio,
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
                    error!(error = ?e, leader = %leader.name, %tx_hash, "failed to record OPEN to DB; cursor will not advance past this log");
                    return HandleStatus::Failed(leader.trader_id);
                }
            }
        };
        if let Err(e) = tx.send(intent).await {
            error!(error = ?e, "intent channel closed; executor must have crashed");
            return HandleStatus::Failed(leader.trader_id);
        }
        HandleStatus::Ingested(leader.trader_id)
    } else {
        debug!(%topic0, %tx_hash, "log with unexpected topic0");
        HandleStatus::Irrelevant
    }
}

/// True iff `symbol` (e.g. "BTC/USD") normalizes to one of the leader's
/// allowed token roots. `leader.allowed_tokens` is already normalized via
/// `symbol_root` + uppercase by `config::validate`.
fn leader_allows_token(leader: &WatchedLeader, symbol: &str) -> bool {
    let root = symbol_root(symbol);
    leader.allowed_tokens.iter().any(|t| t == &root)
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
