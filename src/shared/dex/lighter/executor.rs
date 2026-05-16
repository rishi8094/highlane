use std::collections::{HashMap, HashSet};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use eyre::Result;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use super::client::{AccountPosition, LighterClient};
use super::markets::{Market, load_markets, symbol_root};
use super::signer::{IS_ASK_BUY, IS_ASK_SELL, LighterSigner, ORDER_TYPE_MARKET, TIF_IOC};
use crate::db::{DbPool, trades};
use crate::shared::dex::Dex;
use crate::shared::intent::{Side, TradeIntent};
use crate::shared::notify::{
    CloseFill, DiscordNotifier, LeaderSummary, OpenFill, OrphanAlert, OrphanKind, StartupInfo,
    UnknownClose, UtilisationAlert, UtilisationSeverity,
};

const TARGET_DEX: Dex = Dex::Lighter;

/// Poll the post-fill position rather than waiting a fixed window: a single
/// 750 ms sleep previously caused us to record `delta == 0` and drop the trade
/// row when Lighter took longer than that to surface a fill via /api/v1/account,
/// leaving an orphan position on the exchange (signal_id=781, 2026-05-03).
const POST_FILL_POLL_INTERVAL: Duration = Duration::from_millis(250);
const POST_FILL_POLL_ATTEMPTS: u32 = 20;

/// Capital utilisation alert thresholds. Each level fires one webhook when
/// crossed and re-arms only after utilisation drops below its rearm point —
/// so we never spam at e.g. 75.1% bouncing across the threshold. The high
/// level (90%) is independent so a critical alert still fires after a soft
/// one disarms it.
const UTIL_FIRE: f64 = 0.75;
const UTIL_REARM: f64 = 0.70;
const UTIL_FIRE_HIGH: f64 = 0.90;
const UTIL_REARM_HIGH: f64 = 0.85;
/// How often the background utilisation poller checks the Lighter account.
const UTIL_POLL_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Clone, Debug)]
pub struct LighterConfig {
    pub base_url: String,
    pub chain_id: i32,
    pub l1_address: String,
    pub account_index_override: Option<i64>,
    pub api_key_index: i32,
    pub api_key_private_hex: String,
    pub slippage_bps: u32,
}

const TX_TYPE_CREATE_ORDER: u8 = 14;

pub async fn run(
    mut rx: mpsc::Receiver<TradeIntent>,
    cfg: LighterConfig,
    pool: DbPool,
    notifier: DiscordNotifier,
    leaders_summary: Vec<LeaderSummary>,
) -> Result<()> {
    let client = LighterClient::new(&cfg.base_url)?;

    let account_index = match cfg.account_index_override {
        Some(idx) => {
            info!(account_index = idx, "using LIGHTER_ACCOUNT_INDEX override");
            idx
        }
        None => {
            let resp = client.accounts_by_l1_address(&cfg.l1_address).await?;
            // Prefer account_type=0 (main cross account) over system slots.
            let chosen = resp
                .sub_accounts
                .iter()
                .find(|a| a.account_type == 0)
                .or_else(|| resp.sub_accounts.first())
                .ok_or_else(|| {
                    eyre::eyre!(
                        "no Lighter sub-account found for L1 address {}",
                        cfg.l1_address
                    )
                })?;
            info!(
                l1_address = %cfg.l1_address,
                account_index = chosen.index,
                account_type = chosen.account_type,
                collateral = chosen.collateral.as_deref().unwrap_or("?"),
                sub_account_count = resp.sub_accounts.len(),
                "resolved Lighter account"
            );
            chosen.index
        }
    };

    // Read the live wallet balance for the startup embed + utilization
    // baselining. With copy_ratio-driven sizing this is no longer required,
    // so failure to read is a warning, not fatal.
    let follower_balance_usd: Option<f64> = match client.account(account_index).await {
        Ok(det) => det.wallet_balance().filter(|b| *b > 0.0),
        Err(e) => {
            warn!(error = ?e, "could not read Lighter account balance at startup; continuing");
            None
        }
    };
    if let Some(bal) = follower_balance_usd {
        info!(account_index, balance_usd = bal, "Lighter wallet balance");
    } else {
        warn!(
            account_index,
            "Lighter balance unavailable or zero at startup; utilization alerts will still poll live"
        );
    }

    let markets = load_markets(&client).await?;
    info!(market_count = markets.len(), "Lighter markets ready");

    let signer_path = LighterSigner::default_library_path()
        .ok_or_else(|| eyre::eyre!("unsupported platform for Lighter signer"))?;
    let signer = LighterSigner::load(
        &signer_path,
        &cfg.base_url,
        cfg.chain_id,
        &cfg.api_key_private_hex,
        cfg.api_key_index,
        account_index,
    )?;

    let open_trades = trades::list_open_for_target(&pool, TARGET_DEX).await?;
    info!(
        positions = open_trades.len(),
        leader_count = leaders_summary.len(),
        "executor ready"
    );

    if let Err(e) = reconcile_on_startup(
        &client,
        &markets,
        &pool,
        &open_trades,
        account_index,
        &notifier,
    )
    .await
    {
        warn!(error = ?e, "startup reconciliation failed; continuing with current state as-is");
    }

    if notifier.enabled() {
        notifier.notify_startup(StartupInfo {
            follower_l1_address: cfg.l1_address.clone(),
            account_index,
            follower_balance_usd,
            slippage_bps: cfg.slippage_bps,
            leaders: leaders_summary.clone(),
        });
    }

    if notifier.enabled() {
        spawn_utilisation_poller(client.clone(), account_index, notifier.clone());
    }

    // Per-leader open notional snapshot. Runs alongside the utilization
    // poller and emits one info line per leader so Axiom can attribute
    // account utilization back to a specific follower-side strategy. Same
    // cadence as the utilization poll to keep the load profile predictable.
    spawn_per_leader_snapshot(
        pool.clone(),
        client.clone(),
        markets.clone(),
        account_index,
        leaders_summary.clone(),
    );

    while let Some(intent) = rx.recv().await {
        if let Err(e) = handle_intent(
            &intent,
            &cfg,
            &client,
            &signer,
            &markets,
            &pool,
            account_index,
            &notifier,
        )
        .await
        {
            error!(error = ?e, leader = %intent.leader(), "intent handling failed");
        }
    }
    warn!("intent channel closed; executor stopping");
    Ok(())
}

fn spawn_per_leader_snapshot(
    pool: DbPool,
    client: LighterClient,
    markets: HashMap<String, Market>,
    account_index: i64,
    leaders: Vec<LeaderSummary>,
) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(UTIL_POLL_INTERVAL);
        // Skip the immediate-fire so we don't compete with startup logging.
        tick.tick().await;
        loop {
            tick.tick().await;
            if let Err(e) =
                log_per_leader_snapshot(&pool, &client, &markets, account_index, &leaders).await
            {
                warn!(target: "snapshot", error = ?e, "per-leader notional snapshot failed");
            }
        }
    });
}

async fn log_per_leader_snapshot(
    pool: &DbPool,
    client: &LighterClient,
    markets: &HashMap<String, Market>,
    account_index: i64,
    leaders: &[LeaderSummary],
) -> Result<()> {
    let rows = trades::list_open_with_trader_for_target(pool, TARGET_DEX).await?;
    let mut by_trader: HashMap<i32, f64> = HashMap::new();
    for r in rows {
        let size_decimals = markets
            .values()
            .find(|m| m.market_id == r.market_id)
            .map(|m| m.size_decimals)
            .unwrap_or(0);
        let size_human = r.size as f64 / 10f64.powi(size_decimals);
        let price = r.entry_price.unwrap_or(0.0);
        *by_trader.entry(r.trader_id).or_default() += size_human * price;
    }

    let utilisation_pct = match client.account(account_index).await {
        Ok(det) => {
            let c = det
                .collateral
                .as_deref()
                .and_then(|s| s.parse::<f64>().ok());
            let a = det
                .available_balance
                .as_deref()
                .and_then(|s| s.parse::<f64>().ok());
            match (c, a) {
                (Some(c), Some(a)) if c > 0.0 => Some(((c - a).max(0.0)) / c),
                _ => None,
            }
        }
        Err(e) => {
            debug!(target: "snapshot", error = ?e, "could not read account for utilization");
            None
        }
    };

    // Emit one line per *configured* leader, even with zero open notional,
    // so the observability story is "I can see every leader's state at a
    // glance" rather than "I can see leaders that currently happen to have
    // open trades." Idle leaders are exactly what an operator wants to
    // confirm during a quiet stretch on Avantis. Anything left in
    // `by_trader` after this loop belongs to a trader_id absent from the
    // current config (e.g. a leader was removed from config.pkl while
    // positions remain on Lighter) — surface that loudly so it can't hide.
    let mut configured_total: f64 = 0.0;
    for l in leaders {
        let notional = by_trader.remove(&l.trader_id).unwrap_or(0.0);
        configured_total += notional;
        info!(
            target: "snapshot",
            leader = %l.name,
            trader_id = l.trader_id,
            copy_ratio = l.copy_ratio,
            open_notional_usd = notional,
            "per-leader open notional"
        );
    }
    let mut unconfigured_total: f64 = 0.0;
    for (trader_id, notional) in &by_trader {
        unconfigured_total += notional;
        warn!(
            target: "snapshot",
            trader_id,
            open_notional_usd = notional,
            "open notional from a trader_id not in current config.pkl — leader was removed but Lighter still holds exposure"
        );
    }
    info!(
        target: "snapshot",
        configured_leaders = leaders.len(),
        unconfigured_leaders = by_trader.len(),
        configured_open_notional_usd = configured_total,
        unconfigured_open_notional_usd = unconfigured_total,
        total_open_notional_usd = configured_total + unconfigured_total,
        utilisation_pct = utilisation_pct,
        "shared-account snapshot"
    );
    Ok(())
}

fn spawn_utilisation_poller(client: LighterClient, account_index: i64, notifier: DiscordNotifier) {
    tokio::spawn(async move {
        let mut armed = true;
        let mut armed_high = true;
        let mut tick = tokio::time::interval(UTIL_POLL_INTERVAL);
        // First tick fires immediately; skip it so we don't double up with the
        // startup webhook.
        tick.tick().await;
        loop {
            tick.tick().await;
            if let Err(e) = check_utilisation(
                &client,
                account_index,
                &mut armed,
                &mut armed_high,
                &notifier,
            )
            .await
            {
                warn!(target: "webhook", error = ?e, "utilisation poll failed");
            }
        }
    });
}

#[allow(clippy::too_many_arguments)]
async fn handle_intent(
    intent: &TradeIntent,
    cfg: &LighterConfig,
    client: &LighterClient,
    signer: &LighterSigner,
    markets: &HashMap<String, Market>,
    pool: &DbPool,
    account_index: i64,
    notifier: &DiscordNotifier,
) -> Result<()> {
    match intent {
        TradeIntent::Open {
            leader: _,
            leader_name,
            copy_ratio,
            symbol,
            side,
            leader_collateral_usd,
            leader_leverage,
            leader_exec_price,
            source_tx,
            source_block,
            signal_id,
            symbol_id,
        } => {
            let root = symbol_root(symbol);
            let Some(market) = markets.get(&root) else {
                warn!(leader = %leader_name, symbol = %symbol, root = %root, "symbol not on Lighter — skipping leader OPEN");
                return Ok(());
            };

            let sized = match size_for_open(
                *leader_collateral_usd,
                *leader_leverage,
                *leader_exec_price,
                *copy_ratio,
                market,
            ) {
                Sizing::Ok(s) => s,
                Sizing::SkipBelowMin {
                    target_notional,
                    min_notional,
                } => {
                    warn!(
                        leader = %leader_name, symbol = %symbol, target_notional, min_notional,
                        "leader OPEN sized below 50%% of Lighter min — skipping"
                    );
                    return Ok(());
                }
            };

            let is_ask = match side {
                Side::Long => IS_ASK_BUY,
                Side::Short => IS_ASK_SELL,
            };
            let price_int = price_with_slippage(
                *leader_exec_price,
                cfg.slippage_bps,
                *side,
                market.price_decimals,
            );

            info!(
                target: "execute",
                leader = %leader_name,
                copy_ratio = *copy_ratio,
                symbol = %symbol, side = %side, %market.market_id,
                size = sized.base_amount_int,
                price = price_int,
                target_collateral = sized.target_collateral_usd,
                target_leverage = sized.target_leverage,
                target_notional = sized.target_notional_usd,
                source_tx = %source_tx,
                "sending Lighter market IOC OPEN"
            );

            let (actual_filled, our_tx_hash) = {
                let (pre_size, nonce) = tokio::try_join!(
                    current_signed_size(client, account_index, market),
                    client.next_nonce(account_index, cfg.api_key_index),
                )?;

                let signed = signer.sign_create_order(
                    market.market_id,
                    client_order_id(),
                    sized.base_amount_int,
                    price_int,
                    is_ask,
                    ORDER_TYPE_MARKET,
                    TIF_IOC,
                    false,
                    0,
                    0,
                    nonce,
                )?;
                let resp = client.send_tx(TX_TYPE_CREATE_ORDER, &signed).await?;
                if resp.code != 0 && resp.code != 200 {
                    return Err(eyre::eyre!(
                        "Lighter sendTx OPEN rejected: code={} msg={}",
                        resp.code,
                        resp.message
                    ));
                }
                info!(
                    target: "execute",
                    tx_hash = %resp.tx_hash, code = resp.code,
                    "Lighter accepted OPEN"
                );
                let our_tx_hash: Option<String> = Some(resp.tx_hash);

                let post_size =
                    poll_post_size_after_fill(client, account_index, market, pre_size).await?;
                let delta = post_size - pre_size;
                let expected_sign: i64 = if matches!(side, Side::Long) { 1 } else { -1 };

                if delta == 0 {
                    warn!(
                        symbol = %symbol, market_id = market.market_id,
                        pre_size, post_size,
                        poll_interval_ms = POST_FILL_POLL_INTERVAL.as_millis() as u64,
                        poll_attempts = POST_FILL_POLL_ATTEMPTS,
                        "Lighter OPEN returned 0 fill after polling; skipping state insert (orphan risk if fill lands later)"
                    );
                    return Ok(());
                }
                if delta.signum() != expected_sign {
                    error!(
                        symbol = %symbol, market_id = market.market_id, delta,
                        expected_sign, pre_size, post_size,
                        "Lighter post-OPEN delta has unexpected sign; not inserting state"
                    );
                    return Ok(());
                }
                let actual = delta.unsigned_abs() as i64;
                if actual != sized.base_amount_int {
                    info!(
                        target: "execute",
                        symbol = %symbol,
                        requested = sized.base_amount_int, filled = actual,
                        "partial fill on Lighter OPEN — recording actual filled amount"
                    );
                }
                (actual, our_tx_hash)
            };

            trades::record_open(
                pool,
                *signal_id,
                TARGET_DEX,
                market.market_id,
                *symbol_id,
                *side,
                actual_filled,
                Some(sized.target_collateral_usd),
                Some(sized.target_leverage as i32),
                Some(*leader_exec_price),
                Some(source_tx),
                Some(*source_block as i64),
            )
            .await?;

            if notifier.enabled() {
                let size_human = actual_filled as f64 / 10f64.powi(market.size_decimals);
                // Lighter doesn't return a per-fill VWAP; the IOC's slippage
                // bound is the worst price we could have accepted, so use it
                // as our notional reference. This will read as 0 bps slippage
                // in the embed when the order filled inside the bound.
                let our_price = price_int as f64 / 10f64.powi(market.price_decimals);
                let notional_usd = size_human * our_price;
                notifier.notify_open(OpenFill {
                    symbol: symbol.clone(),
                    side: *side,
                    leader_price: *leader_exec_price,
                    our_price,
                    size: size_human,
                    notional_usd,
                    collateral_usd: sized.target_collateral_usd,
                    leverage: sized.target_leverage as u32,
                    leader_tx: source_tx.clone(),
                    our_tx: our_tx_hash,
                });
            }
        }

        TradeIntent::Close {
            leader: _,
            leader_name,
            copy_ratio: _,
            symbol,
            leader_pair_index,
            leader_position_index,
            leader_exec_price,
            leader_entry_price,
            leader_pnl_pct,
            source_tx,
            signal_id,
        } => {
            let Some(open) = trades::find_open_for_signal(pool, *signal_id).await? else {
                warn!(
                    leader = %leader_name,
                    %symbol, leader_pair_index, leader_position_index, signal_id, %source_tx,
                    "leader CLOSE for unknown trade (likely opened before we started or different DEX) — skipping"
                );
                if notifier.enabled() {
                    notifier.notify_unknown_close(UnknownClose {
                        symbol: symbol.clone(),
                        leader_pair_index: *leader_pair_index,
                        leader_position_index: *leader_position_index,
                        leader_entry_price: *leader_entry_price,
                        leader_close_price: *leader_exec_price,
                        leader_pnl_pct: *leader_pnl_pct,
                        signal_id: *signal_id,
                        leader_tx: source_tx.clone(),
                    });
                }
                return Ok(());
            };
            let Some(market) = markets.values().find(|m| m.market_id == open.market_id) else {
                warn!(
                    %symbol, market_id = open.market_id,
                    "market metadata missing on CLOSE — skipping"
                );
                return Ok(());
            };

            // Reduce-only counter-side IOC market.
            let close_side = open.side.flip();
            let is_ask = match close_side {
                Side::Long => IS_ASK_BUY,
                Side::Short => IS_ASK_SELL,
            };
            // Lighter rejects price=0 with "OrderPrice should not be less than 1".
            // Use the leader's close exec price as a reference and bias by
            // slippage_bps in the *order* direction (close LONG = SELL needs a
            // lower bound; close SHORT = BUY needs an upper bound) so the IOC
            // crosses. price_with_slippage already encodes this convention.
            let price_int = price_with_slippage(
                *leader_exec_price,
                cfg.slippage_bps,
                close_side,
                market.price_decimals,
            );
            if price_int < 1 {
                warn!(
                    %symbol, market_id = open.market_id, leader_exec_price,
                    "computed CLOSE price < 1 (bad leader exec price?) — skipping"
                );
                return Ok(());
            }

            info!(
                target: "execute",
                symbol = %symbol, side = %close_side, market_id = open.market_id,
                size = open.size, price = price_int,
                "sending Lighter market IOC CLOSE (reduce-only)"
            );

            // Track how much of `open.size` we actually closed on Lighter.
            // - dry-run: pretend we closed everything.
            // - live: poll signed-size before/after to compute the real
            //   reduction. If it didn't move, leave the DB row open so the
            //   next leader CLOSE / startup reconciliation can catch it.
            //   Without this, a non-crossing IOC silently orphans the
            //   position on Lighter (signal_id ≈ 1100, BTC/USD 2026-05-05).
            let (close_tx_hash, actual_closed): (Option<String>, i64) = {
                let (pre_size, nonce) = tokio::try_join!(
                    current_signed_size(client, account_index, market),
                    client.next_nonce(account_index, cfg.api_key_index),
                )?;
                let signed = signer.sign_create_order(
                    open.market_id,
                    client_order_id(),
                    open.size,
                    price_int,
                    is_ask,
                    ORDER_TYPE_MARKET,
                    TIF_IOC,
                    true,
                    0,
                    0,
                    nonce,
                )?;
                let resp = client.send_tx(TX_TYPE_CREATE_ORDER, &signed).await?;
                if resp.code != 0 && resp.code != 200 {
                    return Err(eyre::eyre!(
                        "Lighter sendTx CLOSE rejected: code={} msg={}",
                        resp.code,
                        resp.message
                    ));
                }
                info!(target: "execute", tx_hash = %resp.tx_hash, "Lighter accepted CLOSE");

                let post_size =
                    poll_post_size_after_fill(client, account_index, market, pre_size).await?;
                // Reduce-only IOC can only shrink the magnitude of the
                // position (or do nothing). Compute by comparing absolute
                // sizes so we are robust to the side, and clamp to >=0 in
                // case Lighter's snapshot transiently disagrees.
                let closed = (pre_size.unsigned_abs() as i64)
                    .saturating_sub(post_size.unsigned_abs() as i64)
                    .max(0);
                (Some(resp.tx_hash), closed)
            };

            if actual_closed == 0 {
                warn!(
                    %symbol, market_id = open.market_id, requested = open.size,
                    "Lighter CLOSE returned 0 fill after polling; leaving DB row open (orphan risk if fill lands later)"
                );
                if notifier.enabled() {
                    notifier.notify_orphan(OrphanAlert {
                        kind: OrphanKind::CloseUnfilled,
                        symbol: symbol.clone(),
                        market_id: open.market_id,
                        expected_signed_size: match open.side {
                            Side::Long => open.size,
                            Side::Short => -open.size,
                        },
                        actual_signed_size: match open.side {
                            Side::Long => open.size,
                            Side::Short => -open.size,
                        },
                        note: format!(
                            "leader_tx={} signal_id={} requested_size={}",
                            source_tx, signal_id, open.size
                        ),
                    });
                }
                return Ok(());
            }

            if actual_closed < open.size {
                let residual = open.size - actual_closed;
                info!(
                    target: "execute",
                    %symbol, market_id = open.market_id,
                    requested = open.size, closed = actual_closed, residual,
                    "partial close on Lighter CLOSE — recording residual; DB row stays open"
                );
                trades::reduce_open_size(pool, open.id, residual).await?;
                if notifier.enabled() {
                    notifier.notify_orphan(OrphanAlert {
                        kind: OrphanKind::ClosePartialFill,
                        symbol: symbol.clone(),
                        market_id: open.market_id,
                        expected_signed_size: match open.side {
                            Side::Long => residual,
                            Side::Short => -residual,
                        },
                        actual_signed_size: match open.side {
                            Side::Long => residual,
                            Side::Short => -residual,
                        },
                        note: format!(
                            "leader_tx={} signal_id={} requested={} closed={} residual={}",
                            source_tx, signal_id, open.size, actual_closed, residual
                        ),
                    });
                }
                return Ok(());
            }

            trades::record_close(
                pool,
                open.id,
                Some(*leader_exec_price),
                close_tx_hash.as_deref(),
            )
            .await?;

            if notifier.enabled() {
                let size_human = open.size as f64 / 10f64.powi(market.size_decimals);
                // Close fill price isn't returned by Lighter either; the IOC
                // bound is again our worst-case reference.
                let our_close_price = price_int as f64 / 10f64.powi(market.price_decimals);
                let entry_price = open.entry_price.unwrap_or(0.0);
                let entry_collateral = open.entry_collateral_usd.unwrap_or(0.0);
                let direction: f64 = match open.side {
                    Side::Long => 1.0,
                    Side::Short => -1.0,
                };
                let pnl_usd = (our_close_price - entry_price) * size_human * direction;
                let pnl_pct = if entry_collateral > 0.0 {
                    pnl_usd / entry_collateral
                } else {
                    0.0
                };
                notifier.notify_close(CloseFill {
                    symbol: symbol.clone(),
                    side: open.side,
                    leader_close_price: *leader_exec_price,
                    our_close_price,
                    size: size_human,
                    our_entry_price: entry_price,
                    leader_entry_price: *leader_entry_price,
                    our_pnl_usd: pnl_usd,
                    our_pnl_pct: pnl_pct,
                    leader_pnl_pct: *leader_pnl_pct,
                    our_tx: close_tx_hash,
                });
            }
        }
    }
    Ok(())
}

async fn check_utilisation(
    client: &LighterClient,
    account_index: i64,
    armed: &mut bool,
    armed_high: &mut bool,
    notifier: &DiscordNotifier,
) -> Result<()> {
    let det = client.account(account_index).await?;
    // Both fields must parse — if either is missing we can't compute a
    // meaningful ratio (e.g. defaulting available to 0 would falsely report
    // 100% utilisation and trigger the alert).
    let Some(collateral) = det
        .collateral
        .as_deref()
        .and_then(|s| s.parse::<f64>().ok())
    else {
        debug!(target: "webhook", "skipping utilisation check: missing collateral");
        return Ok(());
    };
    let Some(available) = det
        .available_balance
        .as_deref()
        .and_then(|s| s.parse::<f64>().ok())
    else {
        debug!(target: "webhook", "skipping utilisation check: missing available_balance");
        return Ok(());
    };
    if collateral <= 0.0 {
        return Ok(());
    }
    let used = (collateral - available).max(0.0);
    let pct = used / collateral;

    if *armed_high && pct >= UTIL_FIRE_HIGH {
        info!(
            target: "webhook",
            utilisation = pct, collateral, available, used,
            "capital utilisation crossed CRITICAL threshold"
        );
        notifier.notify_utilisation(UtilisationAlert {
            utilisation_pct: pct,
            collateral_usd: collateral,
            available_usd: available,
            margin_used_usd: used,
            severity: UtilisationSeverity::Critical,
        });
        *armed_high = false;
    } else if !*armed_high && pct < UTIL_REARM_HIGH {
        info!(
            target: "webhook",
            utilisation = pct,
            "capital utilisation back below CRITICAL re-arm threshold"
        );
        *armed_high = true;
    }

    if *armed && pct >= UTIL_FIRE {
        info!(
            target: "webhook",
            utilisation = pct, collateral, available, used,
            "capital utilisation crossed alert threshold"
        );
        notifier.notify_utilisation(UtilisationAlert {
            utilisation_pct: pct,
            collateral_usd: collateral,
            available_usd: available,
            margin_used_usd: used,
            severity: UtilisationSeverity::Warn,
        });
        *armed = false;
    } else if !*armed && pct < UTIL_REARM {
        info!(
            target: "webhook",
            utilisation = pct,
            "capital utilisation back below re-arm threshold"
        );
        *armed = true;
    }
    Ok(())
}

async fn current_signed_size(
    client: &LighterClient,
    account_index: i64,
    market: &Market,
) -> Result<i64> {
    let det = client.account(account_index).await?;
    Ok(signed_size_for_market(
        &det.positions,
        market.market_id,
        market.size_decimals,
    ))
}

/// Poll the account snapshot until the position differs from `pre_size` or we
/// exhaust `POST_FILL_POLL_ATTEMPTS`. Returns the last observed post-size; the
/// caller is responsible for warn-logging if it still equals `pre_size`.
async fn poll_post_size_after_fill(
    client: &LighterClient,
    account_index: i64,
    market: &Market,
    pre_size: i64,
) -> Result<i64> {
    let mut post_size = pre_size;
    for _ in 0..POST_FILL_POLL_ATTEMPTS {
        tokio::time::sleep(POST_FILL_POLL_INTERVAL).await;
        post_size = current_signed_size(client, account_index, market).await?;
        if post_size != pre_size {
            return Ok(post_size);
        }
    }
    Ok(post_size)
}

fn signed_size_for_market(
    positions: &[AccountPosition],
    market_id: i32,
    size_decimals: i32,
) -> i64 {
    positions
        .iter()
        .find(|p| p.market_id == market_id)
        .map(|p| p.signed_base_int(size_decimals))
        .unwrap_or(0)
}

async fn reconcile_on_startup(
    client: &LighterClient,
    markets: &HashMap<String, Market>,
    pool: &DbPool,
    open_trades: &[crate::db::models::Trade],
    account_index: i64,
    notifier: &DiscordNotifier,
) -> Result<()> {
    let det = client.account(account_index).await?;

    // Aggregate open DB trades by market_id, signed.
    let mut state_by_market: HashMap<i32, i64> = HashMap::new();
    for t in open_trades {
        let signed: i64 = match t.side {
            Side::Long => t.size,
            Side::Short => -t.size,
        };
        *state_by_market.entry(t.market_id).or_default() += signed;
    }

    // Resolve size_decimals per market_id from the markets map (fallback 0).
    let dec_for_market = |mid: i32| -> i32 {
        markets
            .values()
            .find(|m| m.market_id == mid)
            .map(|m| m.size_decimals)
            .unwrap_or(0)
    };
    let symbol_for_market = |mid: i32| -> String {
        markets
            .values()
            .find(|m| m.market_id == mid)
            .map(|m| m.symbol.clone())
            .unwrap_or_else(|| format!("#{mid}"))
    };

    let mut market_ids: HashSet<i32> = state_by_market.keys().copied().collect();
    market_ids.extend(det.positions.iter().map(|p| p.market_id));

    let mut markets_to_drop = Vec::new();
    for mid in market_ids {
        let dec = dec_for_market(mid);
        let lighter_size = signed_size_for_market(&det.positions, mid, dec);
        let state_size = *state_by_market.get(&mid).unwrap_or(&0);

        if state_size == 0 && lighter_size != 0 {
            warn!(
                market_id = mid,
                lighter_size,
                "ORPHAN: Lighter holds a position not in copy-state. Not auto-closing — review manually."
            );
            if notifier.enabled() {
                notifier.notify_orphan(OrphanAlert {
                    kind: OrphanKind::ReconcileDrift,
                    symbol: symbol_for_market(mid),
                    market_id: mid,
                    expected_signed_size: 0,
                    actual_signed_size: lighter_size,
                    note: "ORPHAN at startup: Lighter has a position the DB does not — close on Lighter manually".into(),
                });
            }
        } else if state_size != 0 && lighter_size == 0 {
            warn!(
                market_id = mid,
                state_size,
                "STALE: copy-state has entries but Lighter shows flat — failing state for this market."
            );
            markets_to_drop.push(mid);
        } else if state_size != lighter_size {
            // Sign flip or magnitude mismatch — both sides hold a position
            // but they disagree, so we cannot trust DB-side `size` as the
            // basis for any subsequent CLOSE sizing. Fail the DB rows for
            // this market (clears copy-state) and surface the residual
            // Lighter position as an orphan that needs manual review.
            //
            // BUT: AccountPosition.signed_base_int parses Lighter's decimal
            // string through f64 then `.round() as i64`, which can produce
            // ±1-unit noise at the smallest size_decimals bit. We tolerate
            // ≤ 2 same-sign units to avoid alarming on that float jitter
            // and nothing more — partial OPEN fills are already recorded
            // as the actual filled amount in the DB, so any residual past
            // float noise is real drift, not legitimate slack. A wider
            // percentage tolerance would silently leave orphan units on
            // Lighter after the leader's next close (a 1000-vs-1004 row
            // reduces by 1000 and strands the extra 4 forever).
            const DRIFT_TOLERANCE_UNITS: u64 = 2;
            let same_sign = state_size.signum() == lighter_size.signum();
            let diff = state_size.abs_diff(lighter_size); // u64
            if same_sign && diff <= DRIFT_TOLERANCE_UNITS {
                info!(
                    market_id = mid,
                    state_size,
                    lighter_size,
                    diff,
                    "reconciled OK (within float-rounding tolerance)"
                );
            } else {
                warn!(
                    market_id = mid,
                    state_size,
                    lighter_size,
                    diff,
                    tolerance = DRIFT_TOLERANCE_UNITS,
                    "DRIFT: copy-state and Lighter sizes differ — failing state for this market and surfacing orphan."
                );
                markets_to_drop.push(mid);
                if notifier.enabled() {
                    notifier.notify_orphan(OrphanAlert {
                        kind: OrphanKind::ReconcileDrift,
                        symbol: symbol_for_market(mid),
                        market_id: mid,
                        expected_signed_size: state_size,
                        actual_signed_size: lighter_size,
                        note: "DRIFT at startup: DB rows failed; Lighter still holds the residual — close manually if undesired".into(),
                    });
                }
            }
        } else {
            info!(market_id = mid, size = lighter_size, "reconciled OK");
        }
    }

    for mid in markets_to_drop {
        let dropped = trades::fail_open_for_market(pool, TARGET_DEX, mid).await?;
        info!(
            market_id = mid,
            count = dropped,
            "failed stale state entries"
        );
    }
    Ok(())
}

/// Lighter caps `client_order_index` at 2^48 - 1 (`281_474_976_710_655`).
/// Milliseconds since epoch is ~1.78e12 → fits with ~159× headroom and stays
/// valid for thousands of years. Same-ms collisions are theoretically possible
/// but unlikely given our 750ms post-fill snapshot delay.
fn client_order_id() -> i64 {
    let ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    (ms as i64) & ((1_i64 << 48) - 1)
}

fn price_with_slippage(exec: f64, slippage_bps: u32, side: Side, price_decimals: i32) -> i32 {
    let factor = match side {
        Side::Long => 1.0 + (slippage_bps as f64) / 10_000.0,
        Side::Short => 1.0 - (slippage_bps as f64) / 10_000.0,
    };
    let scaled = exec * factor * 10f64.powi(price_decimals);
    if scaled <= 0.0 {
        return 0;
    }
    if scaled > i32::MAX as f64 {
        return i32::MAX;
    }
    scaled.round() as i32
}

pub struct SizedOrder {
    pub target_collateral_usd: f64,
    pub target_leverage: u64,
    pub target_notional_usd: f64,
    pub base_amount_int: i64,
}

pub enum Sizing {
    Ok(SizedOrder),
    SkipBelowMin {
        target_notional: f64,
        min_notional: f64,
    },
}

pub fn size_for_open(
    leader_collateral_usd: f64,
    leader_leverage: u64,
    leader_exec_price: f64,
    copy_ratio: f64,
    market: &Market,
) -> Sizing {
    let target_collateral = leader_collateral_usd * copy_ratio;
    let target_leverage = leader_leverage.min(market.max_leverage);
    let target_notional = target_collateral * target_leverage as f64;
    if leader_exec_price <= 0.0 {
        return Sizing::SkipBelowMin {
            target_notional: 0.0,
            min_notional: 0.0,
        };
    }
    let target_base = target_notional / leader_exec_price;
    let lot = 10f64.powi(-market.size_decimals);
    let mut base_amount = (target_base / lot).round() * lot;

    let min_base = market.min_base_amount.max(lot);
    let min_notional = min_base * leader_exec_price;
    if base_amount < min_base {
        if target_notional >= min_notional * 0.5 {
            base_amount = min_base;
        } else {
            return Sizing::SkipBelowMin {
                target_notional,
                min_notional,
            };
        }
    }

    let base_amount_int = (base_amount * 10f64.powi(market.size_decimals)).round() as i64;
    Sizing::Ok(SizedOrder {
        target_collateral_usd: target_collateral,
        target_leverage,
        target_notional_usd: base_amount * leader_exec_price,
        base_amount_int,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn btc_market() -> Market {
        Market {
            market_id: 1,
            symbol: "BTC".into(),
            size_decimals: 4,
            price_decimals: 2,
            min_base_amount: 0.001,
            max_leverage: 25,
        }
    }

    // Pre-migration the bot computed sizing from
    //   ratio = follower_budget_usd / leader_max_exposure_usd
    // (defaults $1k / $125k = 0.008). copy_ratio is now passed directly;
    // these regressions keep the same numerical baseline so a parity run
    // produces identical sizes.
    const PARITY_RATIO: f64 = 1_000.0 / 125_000.0; // 0.008

    #[test]
    fn scaling_25k_at_100x_caps_to_lighter_max() {
        // Leader: $25k collateral at 100x → $2.5M notional on Avantis
        // copy_ratio = 0.008 → $200 follower collateral, capped to 25x → $5,000 notional
        let s = match size_for_open(25_000.0, 100, 77_000.0, PARITY_RATIO, &btc_market()) {
            Sizing::Ok(s) => s,
            Sizing::SkipBelowMin { .. } => panic!("should size, not skip"),
        };
        assert!((s.target_collateral_usd - 200.0).abs() < 0.01);
        assert_eq!(s.target_leverage, 25);
        // base_amount around 5000/77000 = 0.0649, rounded to 4 decimals
        let base_decimal = (s.base_amount_int as f64) / 10f64.powi(4);
        assert!((base_decimal - 0.0649).abs() < 0.001, "got {base_decimal}");
        // notional ≈ base × price ≈ 0.0649 * 77000 = 4997.3
        assert!((s.target_notional_usd - 4_997.3).abs() < 5.0);
    }

    #[test]
    fn tiny_leader_trade_below_min_skips() {
        // Leader: $10 collateral at 5x → $50 notional. copy_ratio 0.008 → $0.40 target.
        let market = Market {
            market_id: 1,
            symbol: "BTC".into(),
            size_decimals: 4,
            price_decimals: 2,
            min_base_amount: 0.001, // min $77 at $77k
            max_leverage: 25,
        };
        match size_for_open(10.0, 5, 77_000.0, PARITY_RATIO, &market) {
            Sizing::SkipBelowMin { .. } => {} // expected
            Sizing::Ok(s) => panic!("should skip, but got base={}", s.base_amount_int),
        }
    }

    #[test]
    fn close_to_min_rounds_up() {
        // Place a leader trade whose follower-side notional is ≥ 50% of min — round up.
        let market = Market {
            market_id: 1,
            symbol: "BTC".into(),
            size_decimals: 4,
            price_decimals: 2,
            min_base_amount: 0.001, // $77 at $77k
            max_leverage: 25,
        };
        // Want target ≈ $50 (≥ 50% of $77). Leader: $6,250 at 1x → $50 follower notional.
        let s = match size_for_open(6_250.0, 1, 77_000.0, PARITY_RATIO, &market) {
            Sizing::Ok(s) => s,
            Sizing::SkipBelowMin {
                target_notional,
                min_notional,
            } => {
                panic!("should round up; target={target_notional} min={min_notional}")
            }
        };
        let base_decimal = (s.base_amount_int as f64) / 10f64.powi(4);
        assert!((base_decimal - 0.001).abs() < 1e-9, "got {base_decimal}");
    }

    #[test]
    fn copy_ratio_scales_linearly() {
        // Double the ratio → double the follower collateral (everything else equal).
        let small = match size_for_open(10_000.0, 1, 77_000.0, 0.1, &btc_market()) {
            Sizing::Ok(s) => s,
            Sizing::SkipBelowMin { .. } => panic!("should size"),
        };
        let big = match size_for_open(10_000.0, 1, 77_000.0, 0.2, &btc_market()) {
            Sizing::Ok(s) => s,
            Sizing::SkipBelowMin { .. } => panic!("should size"),
        };
        assert!((small.target_collateral_usd - 1_000.0).abs() < 0.01);
        assert!((big.target_collateral_usd - 2_000.0).abs() < 0.01);
    }
}
