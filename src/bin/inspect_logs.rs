//! Inspect on-chain MarketExecuted / LimitExecuted logs from the Avantis
//! callbacks contract to verify our watcher's open-vs-close classification.
//!
//! What this confirms:
//! - For each emitted event we print the on-chain `isPnl` (LimitExecuted) and
//!   `open` (MarketExecuted) flags, plus enough of the Trade struct to match
//!   the event back to a row in our `signals` table.
//!
//! Workflow for the LIT signal 653 case:
//!   # 1. find the tx our watcher saw (which we currently classified as OPEN)
//!   doppler run -- psql "$DATABASE_URL" -tAc \
//!     "SELECT entry_tx FROM signals WHERE id = 653;"
//!
//!   # 2. decode the on-chain log:
//!   doppler run -- cargo run --bin inspect_logs -- tx 0x<entry_tx>
//!
//! If the printed event is a LimitExecuted with `isPnl=true`, the watcher
//! misclassified a CLOSE as an OPEN (bug). If `isPnl=false` and the event
//! looks like a fresh open at the same price as the leader's just-hit SL,
//! that's a Martingale — surprising but not a watcher bug.
//!
//! Usage (always under `doppler run --` because BASE_WSS_URL comes from Doppler):
//!   doppler run -- cargo run --bin inspect_logs -- tx 0x<hash> [0x<hash2> ...]
//!   doppler run -- cargo run --bin inspect_logs -- range <from_block> <to_block> [trader_address]

use std::env;
use std::str::FromStr;

use alloy::{
    primitives::{Address, B256},
    providers::{Provider, ProviderBuilder, WsConnect},
    rpc::types::Filter,
    sol,
    sol_types::SolEvent,
};
use eyre::{Result, bail};

use ICallbacks::{LimitExecuted, MarketExecuted};

// Mirrors the on-chain Trade struct + ICallbacks events declared in
// src/shared/dex/avantis/contracts.rs. Duplicated rather than imported because
// `highlane` currently has no library target — keeping this binary fully
// self-contained avoids touching Cargo.toml's [lib] / [bin] split.
sol! {
    #[derive(Debug)]
    struct Trade {
        address trader;
        uint256 pairIndex;
        uint256 index;
        uint256 initialPosToken;
        uint256 positionSizeUSDC;
        uint256 openPrice;
        bool buy;
        uint256 leverage;
        uint256 tp;
        uint256 sl;
        uint256 timestamp;
    }

    #[sol(rpc)]
    interface ICallbacks {
        event MarketExecuted(
            uint256 orderId,
            Trade t,
            bool open,
            uint256 price,
            uint256 positionSizeUSDC,
            int256 percentProfit,
            uint256 usdcSentToTrader,
            bool isPnl
        );

        event LimitExecuted(
            uint256 orderId,
            uint256 limitIndex,
            Trade t,
            uint8 orderType,
            uint256 price,
            uint256 positionSizeUSDC,
            int256 percentProfit,
            uint256 usdcSentToTrader,
            bool isPnl
        );
    }

    #[sol(rpc)]
    interface ITradingStorage {
        function callbacks() external view returns (address);
    }
}

const TRADING_STORAGE: &str = "0x8a311D7048c35985aa31C131B9A13e03a5f7422d";

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        eprintln!("usage:");
        eprintln!("  inspect_logs tx <tx_hash> [<tx_hash2> ...]");
        eprintln!("  inspect_logs range <from_block> <to_block> [trader_address]");
        std::process::exit(2);
    }

    let url = env::var("BASE_WSS_URL").map_err(|_| {
        eyre::eyre!(
            "BASE_WSS_URL not set (run via `doppler run -- cargo run --bin inspect_logs ...`)"
        )
    })?;
    let provider = ProviderBuilder::new()
        .connect_ws(WsConnect::new(url))
        .await?;

    let trading_storage = ITradingStorage::new(Address::from_str(TRADING_STORAGE)?, &provider);
    let callbacks = trading_storage.callbacks().call().await?;
    eprintln!("callbacks contract: {callbacks}");
    eprintln!(
        "MarketExecuted topic0: {:?}",
        <MarketExecuted as SolEvent>::SIGNATURE_HASH
    );
    eprintln!(
        "LimitExecuted  topic0: {:?}",
        <LimitExecuted as SolEvent>::SIGNATURE_HASH
    );

    print_header();

    match args[1].as_str() {
        "tx" => {
            for raw in &args[2..] {
                let tx_hash =
                    B256::from_str(raw).map_err(|e| eyre::eyre!("bad tx hash {raw}: {e}"))?;
                let receipt = provider
                    .get_transaction_receipt(tx_hash)
                    .await?
                    .ok_or_else(|| eyre::eyre!("tx receipt not found for {raw}"))?;
                for log in receipt.inner.logs() {
                    if log.address() != callbacks {
                        continue;
                    }
                    inspect_log(&log.clone())?;
                }
            }
        }
        "range" => {
            if args.len() < 4 {
                bail!("range mode needs <from_block> <to_block> [trader]");
            }
            let from: u64 = args[2].parse()?;
            let to: u64 = args[3].parse()?;
            let trader: Option<Address> = if args.len() >= 5 {
                Some(Address::from_str(&args[4])?)
            } else {
                None
            };
            let filter = Filter::new()
                .address(callbacks)
                .from_block(from)
                .to_block(to)
                .event_signature(vec![
                    <MarketExecuted as SolEvent>::SIGNATURE_HASH,
                    <LimitExecuted as SolEvent>::SIGNATURE_HASH,
                ]);
            let logs = provider.get_logs(&filter).await?;
            eprintln!("range {from}..={to} returned {} logs", logs.len());
            for log in &logs {
                if let Some(t) = trader
                    && log_trader(log) != Some(t)
                {
                    continue;
                }
                inspect_log(log)?;
            }
        }
        other => bail!("unknown mode: {other}"),
    }
    Ok(())
}

fn print_header() {
    println!(
        "kind            | block      | log_idx | tx                                                                  | trader                                     | pair | pos | buy   | flag           | open_price        | exec_price        | pct_profit"
    );
    println!(
        "----------------+------------+---------+---------------------------------------------------------------------+--------------------------------------------+------+-----+-------+----------------+-------------------+-------------------+----------"
    );
}

fn log_trader(log: &alloy::rpc::types::Log) -> Option<Address> {
    let topic0 = log.topic0()?;
    if *topic0 == <MarketExecuted as SolEvent>::SIGNATURE_HASH {
        MarketExecuted::decode_log(&log.inner)
            .ok()
            .map(|d| d.data.t.trader)
    } else if *topic0 == <LimitExecuted as SolEvent>::SIGNATURE_HASH {
        LimitExecuted::decode_log(&log.inner)
            .ok()
            .map(|d| d.data.t.trader)
    } else {
        None
    }
}

fn inspect_log(log: &alloy::rpc::types::Log) -> Result<()> {
    let topic0 = match log.topic0() {
        Some(t) => *t,
        None => return Ok(()),
    };
    let tx = log
        .transaction_hash
        .map(|h| format!("{h:#x}"))
        .unwrap_or_else(|| "?".into());
    let block = log.block_number.unwrap_or_default();
    let log_idx = log.log_index.unwrap_or_default();

    if topic0 == <MarketExecuted as SolEvent>::SIGNATURE_HASH {
        let decoded = MarketExecuted::decode_log(&log.inner)?;
        let ev = &decoded.data;
        let flag = format!("open={} isPnl={}", ev.open, ev.isPnl);
        println!(
            "MarketExecuted  | {:<10} | {:<7} | {:<67} | {} | {:<4} | {:<3} | {:<5} | {:<14} | {:<17} | {:<17} | {}",
            block,
            log_idx,
            tx,
            ev.t.trader,
            ev.t.pairIndex,
            ev.t.index,
            ev.t.buy,
            flag,
            ev.t.openPrice,
            ev.price,
            ev.percentProfit,
        );
    } else if topic0 == <LimitExecuted as SolEvent>::SIGNATURE_HASH {
        let decoded = LimitExecuted::decode_log(&log.inner)?;
        let ev = &decoded.data;
        // orderType meaning per Avantis: 0=TP, 1=SL, 2=LIQ, 3=LIMIT_OPEN
        // (subject to change — check Avantis docs).
        let flag = format!("isPnl={} type={}", ev.isPnl, ev.orderType);
        println!(
            "LimitExecuted   | {:<10} | {:<7} | {:<67} | {} | {:<4} | {:<3} | {:<5} | {:<14} | {:<17} | {:<17} | {}",
            block,
            log_idx,
            tx,
            ev.t.trader,
            ev.t.pairIndex,
            ev.t.index,
            ev.t.buy,
            flag,
            ev.t.openPrice,
            ev.price,
            ev.percentProfit,
        );
    } else {
        eprintln!("skip topic0={topic0:?} block={block} log_idx={log_idx}");
    }
    Ok(())
}
