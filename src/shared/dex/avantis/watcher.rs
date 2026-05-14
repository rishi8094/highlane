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
use crate::shared::notify::{DiscordNotifier, DroppedEvents, OrphanAlert, OrphanKind};

const SOURCE_DEX: Dex = Dex::Avantis;

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

/// How often the shadow poller re-fetches recent blocks via `eth_getLogs`
/// and feeds them through the same idempotent `handle_log` path the WS
/// subscription uses. Each tick produces one `eth_getLogs` over
/// `SHADOW_POLL_LOOKBACK_BLOCKS` of history.
const SHADOW_POLL_INTERVAL: Duration = Duration::from_secs(30);

/// How far back the shadow poller looks every tick. Sized for Base's ~2s
/// block time so we always overlap the prior window by ≥ one full tick of
/// chain time — that way a log emitted right before we read `head` can't
/// slip between two consecutive shadow ticks. `handle_log` is idempotent,
/// so the overlap costs only a few extra DB reads per tick.
const SHADOW_POLL_LOOKBACK_BLOCKS: u64 = 150;

/// Lower bound on the gap between consecutive `notify_dropped_events`
/// Discord embeds when drops are sustained. The shadow path keeps
/// ingesting and logging on every tick; this just rate-limits the
/// webhook so a multi-hour outage doesn't fire a Discord embed every
/// 30 seconds.
const DROPPED_EVENTS_NOTIFY_COOLDOWN: Duration = Duration::from_secs(5 * 60);

/// Outcome of `handle_log`. The watcher loop uses this to decide whether to
/// advance the block cursor: only `Ingested` and `Irrelevant` are safe to
/// advance past. A `Failed` log means a downstream write (DB or intent
/// channel) errored and we want the next session/backfill to re-deliver the
/// log, so the cursor must stay where it was.
///
/// `was_new` on `Ingested` distinguishes "this call was the one to record
/// the event into the DB" (true) from "the row was already there, e.g. a
/// re-delivered log or an event the WS path beat us to" (false). The
/// shadow poller uses `was_new=true` from its own re-fetch of recent
/// blocks as the definitive "the WS subscription dropped this event"
/// signal — anything the WS already ingested shows up as `was_new=false`
/// on the shadow's idempotent re-run.
#[derive(Debug, Clone, Copy)]
enum HandleStatus {
    /// Log was for our leader and the underlying record_open / record_close
    /// returned `was_new` as indicated. `was_new=true` means this call
    /// performed the actual DB insert/update; `was_new=false` means the
    /// state was already there (replay / cross-path duplicate).
    Ingested { was_new: bool },
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

    // Spawn the shadow poller. It runs independently of the WS session and
    // is the system's only durable defence against the WS subscription
    // silently dropping events while the underlying socket stays "up". The
    // shadow re-fetches recent blocks via `eth_getLogs` on a tight cadence
    // and re-runs them through the same idempotent `handle_log` path the
    // WS subscription uses — anything the WS dropped is caught and
    // replayed within `SHADOW_POLL_INTERVAL` (~30s) instead of waiting
    // for the next session restart. The number of new ingests per tick
    // is also the runtime metric for how leaky the WS path is.
    {
        let ws_url = ws_url.to_string();
        let pool = pool.clone();
        let tx = tx.clone();
        let notifier = notifier.clone();
        tokio::spawn(async move {
            shadow_poller(ws_url, leader, trader_id, pool, tx, notifier).await;
        });
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
    loop {
        match tokio::time::timeout(STREAM_IDLE_TIMEOUT, stream.next()).await {
            Ok(Some(log)) => {
                let status =
                    handle_log(&log, leader, trader_id, pool, &pairs, tx, notifier).await;
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
            Ok(None) => {
                warn!("WS log stream returned None — subscription ended");
                return Ok(());
            }
            Err(_) => {
                warn!(
                    timeout_secs = STREAM_IDLE_TIMEOUT.as_secs(),
                    last_seen_block = ?*last_seen_block,
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

/// Background task: every `SHADOW_POLL_INTERVAL`, re-fetch the most recent
/// `SHADOW_POLL_LOOKBACK_BLOCKS` of callback-contract logs and run them
/// through the same idempotent `handle_log` path the WS subscription
/// uses. The shadow's *own* `was_new=true` ingests are, by definition,
/// events the WS subscription failed to deliver — that count is the
/// definitive "dropped events" signal we previously had no way to
/// measure. The shadow also self-heals: anything it catches is
/// already-replayed through `handle_log` by the time the alert fires,
/// so copy-state and the executor are catching up in the background.
///
/// Failures inside the inner session (RPC error, decode error, etc.)
/// kick out to the outer retry loop with exponential backoff. The
/// shadow is never expected to exit; if `shadow_session` returns Ok
/// that's a bug.
async fn shadow_poller(
    ws_url: String,
    leader: Address,
    trader_id: i32,
    pool: DbPool,
    tx: mpsc::Sender<TradeIntent>,
    notifier: DiscordNotifier,
) {
    let mut backoff = Duration::from_secs(5);
    loop {
        match shadow_session(&ws_url, leader, trader_id, &pool, &tx, &notifier).await {
            Ok(()) => {
                warn!("shadow poller session ended unexpectedly without error; restarting");
            }
            Err(e) => {
                warn!(error = ?e, backoff_secs = backoff.as_secs(), "shadow poller errored; restarting after backoff");
            }
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(120));
    }
}

async fn shadow_session(
    ws_url: &str,
    leader: Address,
    trader_id: i32,
    pool: &DbPool,
    tx: &mpsc::Sender<TradeIntent>,
    notifier: &DiscordNotifier,
) -> Result<()> {
    // We mirror `run_session`'s setup intentionally: the shadow must NOT
    // share a provider with the main watcher session, because the same
    // alloy WS transport that goes silent under the WS subscription would
    // also silently break a shared provider's `get_logs` call. A second
    // independent connection means a transport-level failure can affect
    // at most one path at a time.
    let provider = ProviderBuilder::new()
        .connect_ws(WsConnect::new(ws_url.to_string()))
        .await?;

    let trading_storage = ITradingStorage::new(parse_addr(TRADING_STORAGE), &provider);
    let callbacks_addr = trading_storage.callbacks().call().await?;

    // Pair-name resolution is best-effort; failure here only degrades the
    // human-readable symbol on log lines, not the ingest correctness.
    let pairs = load_pair_names(&provider, parse_addr(PAIR_STORAGE), parse_addr(MULTICALL3))
        .await
        .unwrap_or_else(|e| {
            warn!(error = ?e, "shadow poller failed to load pair names; symbols will fall back to indices");
            HashMap::new()
        });

    info!(%callbacks_addr, "shadow poller subscribed view ready");

    let base_filter = Filter::new().address(callbacks_addr).event_signature(vec![
        <MarketExecuted as SolEvent>::SIGNATURE_HASH,
        <LimitExecuted as SolEvent>::SIGNATURE_HASH,
    ]);

    // Seed the cursor relative to the current head, NOT to
    // `signals::max_seen_block`. The WS session is already responsible
    // for backfilling from the last persisted block on startup; the
    // shadow only needs to cover NEW drops going forward. Seeding from
    // the head minus the lookback window keeps the first tick small.
    let head_at_start = provider.get_block_number().await?;
    let mut cursor = head_at_start.saturating_sub(SHADOW_POLL_LOOKBACK_BLOCKS);
    info!(
        start_cursor = cursor,
        head = head_at_start,
        lookback = SHADOW_POLL_LOOKBACK_BLOCKS,
        interval_secs = SHADOW_POLL_INTERVAL.as_secs(),
        "shadow poller running"
    );

    let mut tick = tokio::time::interval(SHADOW_POLL_INTERVAL);
    // Consume the first (immediate) tick so we don't double-fire alongside
    // whatever the WS path is doing during startup.
    tick.tick().await;

    // Cooldown floor for the Discord notifier only — local logs always fire.
    let mut last_notified: Option<Instant> = None;

    loop {
        tick.tick().await;

        let head = match provider.get_block_number().await {
            Ok(h) => h,
            Err(e) => {
                warn!(error = ?e, "shadow poller get_block_number failed; will retry next tick");
                continue;
            }
        };
        if head < cursor {
            // Reorg or RPC oddity. Don't rewind further than our lookback
            // window — just wait for head to catch up.
            continue;
        }

        // Always look back a fixed window past the last cursor so a log
        // that landed right before our previous head-read can't slip
        // through. `handle_log` deduplicates against the DB, so overlap
        // is free apart from a few cache-warm reads.
        let from = cursor.saturating_sub(SHADOW_POLL_LOOKBACK_BLOCKS);
        let to = head;
        let filter = base_filter.clone().from_block(from).to_block(to);
        let logs = match provider.get_logs(&filter).await {
            Ok(l) => l,
            Err(e) => {
                warn!(error = ?e, from, to, "shadow poller get_logs failed; will retry next tick");
                continue;
            }
        };

        let mut dropped: u32 = 0;
        let mut samples: Vec<String> = Vec::new();
        // Per-tick cursor lock: if ANY log in this batch produced a
        // `HandleStatus::Failed`, we must NOT advance `cursor` past the
        // batch — the next tick's lookback window has to re-cover the
        // failed range so the durability guarantee holds. Without this,
        // a transient DB blip or a closed intent channel would let the
        // shadow silently drop the very events it exists to recover,
        // turning Layer 2 into a no-op exactly when it's needed.
        // Matches the `cursor_locked` discipline in `run_session`.
        let mut had_failure = false;
        for log in &logs {
            let status = handle_log(log, leader, trader_id, pool, &pairs, tx, notifier).await;
            match status {
                HandleStatus::Ingested { was_new: true } => {
                    dropped += 1;
                    if samples.len() < 5
                        && let Some(h) = log.transaction_hash
                    {
                        samples.push(format!("{h:#x}"));
                    }
                }
                HandleStatus::Failed => {
                    // Keep processing the rest of the batch: handle_log is
                    // idempotent, so later logs that succeed here just get
                    // re-ingested as no-ops on the next tick. The cursor
                    // freeze (below) is what preserves correctness.
                    had_failure = true;
                }
                HandleStatus::Ingested { was_new: false } | HandleStatus::Irrelevant => {}
            }
        }

        if dropped > 0 {
            warn!(
                count = dropped,
                from_block = from,
                to_block = to,
                ?samples,
                "shadow poller ingested events the WS subscription dropped"
            );
            let should_notify = match last_notified {
                None => true,
                Some(t) => t.elapsed() >= DROPPED_EVENTS_NOTIFY_COOLDOWN,
            };
            if should_notify && notifier.enabled() {
                notifier.notify_dropped_events(DroppedEvents {
                    count: dropped,
                    from_block: from,
                    to_block: to,
                    samples: samples.clone(),
                });
                last_notified = Some(Instant::now());
            }
        }

        if had_failure {
            // Cursor stays put. Next tick re-fetches `cursor - LOOKBACK`
            // to the new head, which will include the failed range so
            // long as the failure resolves before the failed blocks
            // drop out of the lookback window (~5 min of chain time).
            // A sustained failure that outlasts the lookback window is
            // a real outage — the surrounding logs (DB error / intent
            // channel closed errors from `handle_log`) are the alert
            // surface for that.
            warn!(
                from_block = from,
                to_block = to,
                cursor,
                "shadow poller saw handle_log Failed in this batch; holding cursor for next-tick retry"
            );
        } else {
            cursor = head;
        }
    }
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
                            Ok(false) => return HandleStatus::Ingested { was_new: false },
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
                // `Ok(None)` means either re-delivery of an already-closed
                // signal (idempotent no-op) or a close for a signal we
                // never opened. Both are "nothing to do here"; we report
                // Irrelevant rather than Ingested so the shadow poller
                // doesn't count this as a recovered drop. The unmatched
                // case has already been paged inside `record_close`.
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
        // Reaching here means either record_open returned was_new=true
        // (new OPEN intent built+sent) or record_close returned Some
        // (new CLOSE intent built+sent). Either way the DB state changed
        // as a direct result of this call → was_new=true.
        HandleStatus::Ingested { was_new: true }
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
                            Ok(false) => return HandleStatus::Ingested { was_new: false },
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
        // See the corresponding comment on the MarketExecuted branch: this
        // return is only reached when DB state changed as a result of this
        // call, so was_new=true.
        HandleStatus::Ingested { was_new: true }
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
