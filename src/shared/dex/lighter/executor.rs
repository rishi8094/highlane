use std::collections::{HashMap, HashSet};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use eyre::Result;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

use super::client::{AccountPosition, LighterClient};
use super::markets::{Market, load_markets, symbol_root};
use super::signer::{
    IS_ASK_BUY, IS_ASK_SELL, LighterSigner, ORDER_TYPE_MARKET, TIF_IOC,
};
use super::state::{CopyState, OpenPosition};
use crate::shared::intent::{Side, TradeIntent};

/// How long to wait after sendTx before snapshotting the post-fill position
/// for delta computation. Lighter IOC market orders settle within hundreds of ms.
const POST_FILL_DELAY: Duration = Duration::from_millis(750);

#[derive(Clone, Debug)]
pub struct LighterConfig {
    pub base_url: String,
    pub chain_id: i32,
    pub l1_address: String,
    pub account_index_override: Option<i64>,
    pub api_key_index: i32,
    pub api_key_private_hex: String,
    pub follower_budget_usd: f64,
    pub leader_max_exposure_usd: f64,
    pub dry_run: bool,
    pub slippage_bps: u32,
}

const TX_TYPE_CREATE_ORDER: u8 = 14;

pub async fn run(mut rx: mpsc::Receiver<TradeIntent>, cfg: LighterConfig) -> Result<()> {
    let client = LighterClient::new(&cfg.base_url)?;

    let account_index = match cfg.account_index_override {
        Some(idx) => {
            info!(account_index = idx, "using LIGHTER_ACCOUNT_INDEX override");
            idx
        }
        None => {
            let resp = client
                .accounts_by_l1_address(&cfg.l1_address)
                .await?;
            let idx = resp
                .sub_accounts
                .first()
                .ok_or_else(|| {
                    eyre::eyre!(
                        "no Lighter account found for L1 address {}",
                        cfg.l1_address
                    )
                })?
                .index;
            info!(l1_address = %cfg.l1_address, account_index = idx, "resolved Lighter account");
            idx
        }
    };

    let markets = load_markets(&client).await?;
    info!(market_count = markets.len(), "Lighter markets ready");

    let signer = if cfg.dry_run {
        None
    } else {
        let path = LighterSigner::default_library_path()
            .ok_or_else(|| eyre::eyre!("unsupported platform for Lighter signer"))?;
        Some(LighterSigner::load(
            &path,
            &cfg.base_url,
            cfg.chain_id,
            &cfg.api_key_private_hex,
            cfg.api_key_index,
            account_index,
        )?)
    };

    let mut state = CopyState::load();
    info!(
        positions = state.len(),
        dry_run = cfg.dry_run,
        budget_usd = cfg.follower_budget_usd,
        leader_max_exposure_usd = cfg.leader_max_exposure_usd,
        "executor ready"
    );

    if let Err(e) = reconcile_on_startup(&client, &markets, &mut state, account_index).await {
        warn!(error = ?e, "startup reconciliation failed; continuing with current state as-is");
    }

    while let Some(intent) = rx.recv().await {
        if let Err(e) = handle_intent(&intent, &cfg, &client, signer.as_ref(), &markets, &mut state, account_index).await {
            error!(error = ?e, leader = %intent.leader(), "intent handling failed");
        }
    }
    warn!("intent channel closed; executor stopping");
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn handle_intent(
    intent: &TradeIntent,
    cfg: &LighterConfig,
    client: &LighterClient,
    signer: Option<&LighterSigner>,
    markets: &HashMap<String, Market>,
    state: &mut CopyState,
    account_index: i64,
) -> Result<()> {
    match intent {
        TradeIntent::Open {
            leader,
            symbol,
            side,
            leader_collateral_usd,
            leader_leverage,
            leader_exec_price,
            leader_pair_index,
            leader_position_index,
            source_tx,
            source_block,
        } => {
            let root = symbol_root(symbol);
            let Some(market) = markets.get(&root) else {
                warn!(symbol = %symbol, root = %root, "symbol not on Lighter — skipping leader OPEN");
                return Ok(());
            };

            let sized = match size_for_open(
                *leader_collateral_usd,
                *leader_leverage,
                *leader_exec_price,
                cfg.follower_budget_usd,
                cfg.leader_max_exposure_usd,
                market,
            ) {
                Sizing::Ok(s) => s,
                Sizing::SkipBelowMin {
                    target_notional,
                    min_notional,
                } => {
                    warn!(
                        symbol = %symbol, target_notional, min_notional,
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
                symbol = %symbol, side = %side, %market.market_id,
                base_amount = sized.base_amount_int,
                price = price_int,
                target_collateral = sized.target_collateral_usd,
                target_leverage = sized.target_leverage,
                target_notional = sized.target_notional_usd,
                source_tx = %source_tx,
                "sending Lighter market IOC OPEN"
            );

            let actual_filled = if let Some(signer) = signer {
                let pre_size = current_signed_size(client, account_index, market).await?;

                let nonce = client
                    .next_nonce(account_index, cfg.api_key_index)
                    .await?;
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
                let resp = client
                    .send_tx(TX_TYPE_CREATE_ORDER, &signed.tx_info)
                    .await?;
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

                tokio::time::sleep(POST_FILL_DELAY).await;
                let post_size = current_signed_size(client, account_index, market).await?;
                let delta = post_size - pre_size;
                let expected_sign: i64 = if matches!(side, Side::Long) { 1 } else { -1 };

                if delta == 0 {
                    warn!(
                        symbol = %symbol, market_id = market.market_id,
                        "Lighter OPEN returned 0 fill; skipping state insert"
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
                actual
            } else {
                info!(target: "execute", dry_run = true, "[DRY] would send OPEN");
                sized.base_amount_int
            };

            state.insert(OpenPosition {
                leader: *leader,
                leader_pair_index: *leader_pair_index,
                leader_position_index: *leader_position_index,
                symbol: symbol.clone(),
                side: *side,
                market_id: market.market_id,
                base_amount: actual_filled,
                opened_block: *source_block,
                opened_tx: source_tx.clone(),
            })?;
        }

        TradeIntent::Close {
            leader,
            symbol,
            leader_pair_index,
            leader_position_index,
            ..
        } => {
            let key = (*leader, *leader_pair_index, *leader_position_index);
            let Some(open) = state.get(&key).cloned() else {
                warn!(
                    %symbol, leader_pair_index, leader_position_index,
                    "leader CLOSE for unknown position (likely opened before we started or different DEX) — skipping"
                );
                return Ok(());
            };

            // Reduce-only counter-side IOC market
            let close_side = open.side.flip();
            let is_ask = match close_side {
                Side::Long => IS_ASK_BUY,
                Side::Short => IS_ASK_SELL,
            };
            // For close we don't have a great reference price; we use price=0 with
            // price_protection=true so Lighter applies its own bound. Many Lighter
            // SDK examples send price=0 for IOC market reduce-only.
            let price_int = 0;

            info!(
                target: "execute",
                symbol = %symbol, side = %close_side, market_id = open.market_id,
                base_amount = open.base_amount,
                "sending Lighter market IOC CLOSE (reduce-only)"
            );

            if let Some(signer) = signer {
                let nonce = client
                    .next_nonce(account_index, cfg.api_key_index)
                    .await?;
                let signed = signer.sign_create_order(
                    open.market_id,
                    client_order_id(),
                    open.base_amount,
                    price_int,
                    is_ask,
                    ORDER_TYPE_MARKET,
                    TIF_IOC,
                    true,
                    0,
                    0,
                    nonce,
                )?;
                let resp = client
                    .send_tx(TX_TYPE_CREATE_ORDER, &signed.tx_info)
                    .await?;
                if resp.code != 0 && resp.code != 200 {
                    return Err(eyre::eyre!(
                        "Lighter sendTx CLOSE rejected: code={} msg={}",
                        resp.code,
                        resp.message
                    ));
                }
                info!(target: "execute", tx_hash = %resp.tx_hash, "Lighter accepted CLOSE");
            } else {
                info!(target: "execute", dry_run = true, "[DRY] would send CLOSE");
            }

            state.remove(&key)?;
        }
    }
    Ok(())
}

async fn current_signed_size(
    client: &LighterClient,
    account_index: i64,
    market: &Market,
) -> Result<i64> {
    let det = client.account(account_index).await?;
    Ok(signed_size_for_market(&det.positions, market.market_id, market.size_decimals))
}

fn signed_size_for_market(positions: &[AccountPosition], market_id: i32, size_decimals: i32) -> i64 {
    positions
        .iter()
        .find(|p| p.market_id == market_id)
        .map(|p| p.signed_base_int(size_decimals))
        .unwrap_or(0)
}

async fn reconcile_on_startup(
    client: &LighterClient,
    markets: &HashMap<String, Market>,
    state: &mut CopyState,
    account_index: i64,
) -> Result<()> {
    let det = client.account(account_index).await?;

    // Aggregate state by market_id, signed.
    let mut state_by_market: HashMap<i32, i64> = HashMap::new();
    for (_, pos) in state.iter() {
        let signed: i64 = if matches!(pos.side, Side::Long) {
            pos.base_amount
        } else {
            -pos.base_amount
        };
        *state_by_market.entry(pos.market_id).or_default() += signed;
    }

    // Resolve size_decimals per market_id from the markets map (fallback 0).
    let dec_for_market = |mid: i32| -> i32 {
        markets
            .values()
            .find(|m| m.market_id == mid)
            .map(|m| m.size_decimals)
            .unwrap_or(0)
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
        } else if state_size != 0 && lighter_size == 0 {
            warn!(
                market_id = mid,
                state_size,
                "STALE: copy-state has entries but Lighter shows flat — dropping state for this market."
            );
            markets_to_drop.push(mid);
        } else if state_size != lighter_size {
            warn!(
                market_id = mid, state_size, lighter_size,
                "DRIFT: copy-state and Lighter sizes differ but neither is 0 — leaving state alone."
            );
        } else {
            info!(market_id = mid, size = lighter_size, "reconciled OK");
        }
    }

    for mid in markets_to_drop {
        let dropped = state.drop_market(mid)?;
        info!(market_id = mid, count = dropped.len(), "dropped stale state entries");
    }
    Ok(())
}

fn client_order_id() -> i64 {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    (nanos as i64) & i64::MAX
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
    follower_budget_usd: f64,
    leader_max_exposure_usd: f64,
    market: &Market,
) -> Sizing {
    let ratio = follower_budget_usd / leader_max_exposure_usd;
    let target_collateral = leader_collateral_usd * ratio;
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

    #[test]
    fn scaling_25k_at_100x_caps_to_lighter_max() {
        // Leader: $25k collateral at 100x → $2.5M notional on Avantis
        // Follower budget $1k, leader cap $125k → ratio = 0.008
        // Expected follower: $200 collateral, capped to 25x → $5,000 notional
        let s = match size_for_open(25_000.0, 100, 77_000.0, 1_000.0, 125_000.0, &btc_market()) {
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
        // Leader: $10 collateral at 5x → $50 notional. Follower ratio 0.008 → $0.40 target.
        let market = Market {
            market_id: 1,
            symbol: "BTC".into(),
            size_decimals: 4,
            price_decimals: 2,
            min_base_amount: 0.001, // min $77 at $77k
            max_leverage: 25,
        };
        match size_for_open(10.0, 5, 77_000.0, 1_000.0, 125_000.0, &market) {
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
        let s = match size_for_open(6_250.0, 1, 77_000.0, 1_000.0, 125_000.0, &market) {
            Sizing::Ok(s) => s,
            Sizing::SkipBelowMin { target_notional, min_notional } => {
                panic!("should round up; target={target_notional} min={min_notional}")
            }
        };
        let base_decimal = (s.base_amount_int as f64) / 10f64.powi(4);
        assert!((base_decimal - 0.001).abs() < 1e-9, "got {base_decimal}");
    }
}
