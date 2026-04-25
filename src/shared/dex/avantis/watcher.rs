use std::collections::HashMap;
use std::time::Duration;

use alloy::{
    primitives::{Address, B256, Bytes},
    providers::{Provider, ProviderBuilder, WsConnect},
    sol_types::SolCall,
};
use eyre::Result;
use futures_util::StreamExt;
use serde::Deserialize;
use tracing::{debug, error, info, warn};

use super::contracts::{
    ITrading, ITradingStorage, MULTICALL3, PAIR_STORAGE, TRADING_STORAGE, parse_addr,
};
use super::format::{format_1e10, format_usdc};
use super::pairs::load_pair_names;

const BACKOFF_INITIAL: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(30);

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct PendingTx {
    hash: B256,
    from: Address,
    #[serde(default)]
    to: Option<Address>,
    input: Bytes,
}

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

    let params = (
        "alchemy_pendingTransactions",
        serde_json::json!({ "fromAddress": [leader] }),
    );
    let sub = provider.subscribe::<_, PendingTx>(params).await?;
    info!("subscribed to alchemy_pendingTransactions");

    let mut stream = sub.into_stream();
    while let Some(tx) = stream.next().await {
        handle_tx(&tx, &pairs, trading_addr);
    }
    Ok(())
}

fn handle_tx(tx: &PendingTx, pairs: &HashMap<u64, String>, trading_addr: Address) {
    let data = tx.input.as_ref();
    if data.len() < 4 {
        debug!(hash = %tx.hash, "skipping tx with no calldata");
        return;
    }
    let selector: [u8; 4] = data[..4].try_into().unwrap();
    let to_match = tx
        .to
        .map(|a| if a == trading_addr { "trading" } else { "other" })
        .unwrap_or("none");

    match selector {
        s if s == <ITrading::openTradeCall as SolCall>::SELECTOR => {
            match <ITrading::openTradeCall as SolCall>::abi_decode(data) {
                Ok(call) => {
                    let t = call.t;
                    let pair_idx: u64 = t.pairIndex.try_into().unwrap_or(u64::MAX);
                    let symbol = pair_name(pairs, pair_idx);
                    let side = if t.buy { "LONG" } else { "SHORT" };
                    let leverage: u128 = t.leverage.try_into().unwrap_or(0);
                    println!(
                        "[OPEN] {symbol} {side} size=${:.2} lev={lev}x price={:.4} tp={:.4} sl={:.4} orderType={ot} slippageP={sp} tx={hash} to={to}",
                        format_usdc(t.positionSizeUSDC),
                        format_1e10(t.openPrice),
                        format_1e10(t.tp),
                        format_1e10(t.sl),
                        lev = leverage,
                        ot = call._type,
                        sp = call._slippageP,
                        hash = tx.hash,
                        to = to_match,
                    );
                }
                Err(e) => warn!(hash = %tx.hash, error = ?e, "failed to decode openTrade"),
            }
        }
        s if s == <ITrading::closeTradeMarketCall as SolCall>::SELECTOR => {
            match <ITrading::closeTradeMarketCall as SolCall>::abi_decode(data) {
                Ok(call) => {
                    let pair_idx: u64 = call._pairIndex.try_into().unwrap_or(u64::MAX);
                    println!(
                        "[CLOSE market] {} idx={} amount={} tx={} to={to_match}",
                        pair_name(pairs, pair_idx),
                        call._index,
                        call._amount,
                        tx.hash,
                    );
                }
                Err(e) => warn!(hash = %tx.hash, error = ?e, "failed to decode closeTradeMarket"),
            }
        }
        s if s == <ITrading::cancelOpenLimitOrderCall as SolCall>::SELECTOR => {
            match <ITrading::cancelOpenLimitOrderCall as SolCall>::abi_decode(data) {
                Ok(call) => {
                    let pair_idx: u64 = call._pairIndex.try_into().unwrap_or(u64::MAX);
                    println!(
                        "[CLOSE cancel-limit] {} idx={} tx={} to={to_match}",
                        pair_name(pairs, pair_idx),
                        call._index,
                        tx.hash,
                    );
                }
                Err(e) => {
                    warn!(hash = %tx.hash, error = ?e, "failed to decode cancelOpenLimitOrder")
                }
            }
        }
        s if s == <ITrading::cancelPendingMarketOrderCall as SolCall>::SELECTOR => {
            match <ITrading::cancelPendingMarketOrderCall as SolCall>::abi_decode(data) {
                Ok(call) => {
                    println!(
                        "[CANCEL pending-market] orderId={} tx={} to={to_match}",
                        call._id, tx.hash,
                    );
                }
                Err(e) => {
                    warn!(hash = %tx.hash, error = ?e, "failed to decode cancelPendingMarketOrder")
                }
            }
        }
        s if s == <ITrading::updateOpenLimitOrderCall as SolCall>::SELECTOR => {
            if let Ok(call) = <ITrading::updateOpenLimitOrderCall as SolCall>::abi_decode(data) {
                let pair_idx: u64 = call._pairIndex.try_into().unwrap_or(u64::MAX);
                println!(
                    "[MODIFY limit] {} idx={} price={:.4} tp={:.4} sl={:.4} slippageP={} tx={} to={to_match}",
                    pair_name(pairs, pair_idx),
                    call._index,
                    format_1e10(call._price),
                    format_1e10(call._tp),
                    format_1e10(call._sl),
                    call._slippageP,
                    tx.hash,
                );
            }
        }
        s if s == <ITrading::updateTpAndSlCall as SolCall>::SELECTOR => {
            if let Ok(call) = <ITrading::updateTpAndSlCall as SolCall>::abi_decode(data) {
                let pair_idx: u64 = call._pairIndex.try_into().unwrap_or(u64::MAX);
                println!(
                    "[MODIFY tp/sl] {} idx={} newSl={:.4} newTp={:.4} tx={} to={to_match}",
                    pair_name(pairs, pair_idx),
                    call._index,
                    format_1e10(call._newSl),
                    format_1e10(call._newTP),
                    tx.hash,
                );
            }
        }
        s if s == <ITrading::updateSlCall as SolCall>::SELECTOR => {
            if let Ok(call) = <ITrading::updateSlCall as SolCall>::abi_decode(data) {
                let pair_idx: u64 = call._pairIndex.try_into().unwrap_or(u64::MAX);
                println!(
                    "[MODIFY sl] {} idx={} newSl={:.4} tx={} to={to_match}",
                    pair_name(pairs, pair_idx),
                    call._index,
                    format_1e10(call._newSl),
                    tx.hash,
                );
            }
        }
        s if s == <ITrading::updateMarginCall as SolCall>::SELECTOR => {
            if let Ok(call) = <ITrading::updateMarginCall as SolCall>::abi_decode(data) {
                let pair_idx: u64 = call._pairIndex.try_into().unwrap_or(u64::MAX);
                println!(
                    "[MODIFY margin] {} idx={} type={} amount={} tx={} to={to_match}",
                    pair_name(pairs, pair_idx),
                    call._index,
                    call._type,
                    call._amount,
                    tx.hash,
                );
            }
        }
        _ => {
            debug!(
                hash = %tx.hash,
                from = %tx.from,
                to = ?tx.to,
                selector = format!("0x{}", hex_short(&selector)),
                "unrecognized selector from leader",
            );
        }
    }
}

fn pair_name(pairs: &HashMap<u64, String>, idx: u64) -> String {
    pairs
        .get(&idx)
        .cloned()
        .unwrap_or_else(|| format!("#{idx}"))
}

fn hex_short(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
