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
use tracing::{debug, error, info, warn};

use super::contracts::{
    ICallbacks::{LimitExecuted, MarketExecuted},
    ITradingStorage, MULTICALL3, PAIR_STORAGE, TRADING_STORAGE, parse_addr,
};
use super::format::{format_1e10, format_leverage, format_usdc};
use super::pairs::load_pair_names;

const BACKOFF_INITIAL: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(30);

pub async fn watch_leader(ws_url: &str, leader: Address) -> ! {
    let mut backoff = BACKOFF_INITIAL;
    loop {
        match run_session(ws_url, leader).await {
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

async fn run_session(ws_url: &str, leader: Address) -> Result<()> {
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
        handle_log(&log, leader, &pairs);
    }
    Ok(())
}

fn handle_log(log: &alloy::rpc::types::Log, leader: Address, pairs: &HashMap<u64, String>) {
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
                let symbol = pair_name(pairs, pair_idx);
                let kind = if ev.open { "OPEN" } else { "CLOSE" };
                let side = if ev.t.buy { "LONG" } else { "SHORT" };
                let leverage = format_leverage(ev.t.leverage);
                let collateral = format_usdc(ev.positionSizeUSDC);
                let notional = collateral * leverage as f64;
                let mut line = format!(
                    "[{kind} market] {symbol} {side} collateral=${collateral:.2} pos=${notional:.2} lev={leverage}x execPrice={px:.4} block={block} tx={tx_hash}",
                    px = format_1e10(ev.price),
                );
                if !ev.open {
                    line.push_str(&format!(
                        " pnl%={pp:.4} usdcSent=${sent:.2}",
                        pp = format_pct_1e10(&ev.percentProfit),
                        sent = format_usdc(ev.usdcSentToTrader),
                    ));
                }
                println!("{line}");
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
                let symbol = pair_name(pairs, pair_idx);
                let side = if ev.t.buy { "LONG" } else { "SHORT" };
                let leverage = format_leverage(ev.t.leverage);
                let collateral = format_usdc(ev.positionSizeUSDC);
                let notional = collateral * leverage as f64;
                let kind = if ev.isPnl { "CLOSE" } else { "OPEN" };
                let mut line = format!(
                    "[{kind} keeper:{ot}] {symbol} {side} collateral=${collateral:.2} pos=${notional:.2} lev={leverage}x execPrice={px:.4} block={block} tx={tx_hash}",
                    ot = ev.orderType,
                    px = format_1e10(ev.price),
                );
                if ev.isPnl {
                    line.push_str(&format!(
                        " pnl%={pp:.4} usdcSent=${sent:.2}",
                        pp = format_pct_1e10(&ev.percentProfit),
                        sent = format_usdc(ev.usdcSentToTrader),
                    ));
                }
                println!("{line}");
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
    // percentProfit is signed, scaled by 1e10
    let s = raw.to_string();
    s.parse::<f64>().unwrap_or(0.0) / 1e10
}
