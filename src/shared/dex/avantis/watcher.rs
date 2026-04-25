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
use crate::shared::intent::{Side, TradeIntent};

const BACKOFF_INITIAL: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(30);

pub async fn watch_leader(ws_url: &str, leader: Address, tx: mpsc::Sender<TradeIntent>) -> ! {
    let mut backoff = BACKOFF_INITIAL;
    loop {
        match run_session(ws_url, leader, &tx).await {
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
        handle_log(&log, leader, &pairs, tx).await;
    }
    Ok(())
}

async fn handle_log(
    log: &alloy::rpc::types::Log,
    leader: Address,
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
                    TradeIntent::Open {
                        leader,
                        symbol,
                        side,
                        leader_collateral_usd: collateral,
                        leader_leverage: leverage,
                        leader_exec_price: exec_price,
                        leader_pair_index: pair_idx,
                        leader_position_index: pos_idx,
                        source_tx: tx_hash.clone(),
                        source_block: block,
                    }
                } else {
                    let pnl = format_pct_1e10(&ev.percentProfit);
                    info!(
                        target: "intent",
                        kind = "CLOSE", source = "market", %symbol,
                        exec_price, pnl_pct = pnl, block, %tx_hash,
                        "leader CLOSE (market)"
                    );
                    TradeIntent::Close {
                        leader,
                        symbol,
                        leader_pair_index: pair_idx,
                        leader_position_index: pos_idx,
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

                let intent = if ev.isPnl {
                    let pnl = format_pct_1e10(&ev.percentProfit);
                    info!(
                        target: "intent",
                        kind = "CLOSE", source = "keeper", order_type = ev.orderType,
                        %symbol, exec_price, pnl_pct = pnl, block, %tx_hash,
                        "leader CLOSE (keeper)"
                    );
                    TradeIntent::Close {
                        leader,
                        symbol,
                        leader_pair_index: pair_idx,
                        leader_position_index: pos_idx,
                    }
                } else {
                    info!(
                        target: "intent",
                        kind = "OPEN", source = "keeper", order_type = ev.orderType,
                        %symbol, %side, collateral, leverage, exec_price, block, %tx_hash,
                        "leader OPEN (keeper)"
                    );
                    TradeIntent::Open {
                        leader,
                        symbol,
                        side,
                        leader_collateral_usd: collateral,
                        leader_leverage: leverage,
                        leader_exec_price: exec_price,
                        leader_pair_index: pair_idx,
                        leader_position_index: pos_idx,
                        source_tx: tx_hash.clone(),
                        source_block: block,
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
