//! Cross-references `config.pkl` against the live Avantis pair list and
//! Lighter market list, so a typo or unlisted token surfaces before
//! deploying — not the first time a leader trades it.
//!
//! Run via: `doppler run -- cargo run --bin validate-config`
//!
//! Exit codes:
//!   0 — every leader's `allowed_tokens` exists on Avantis (Lighter misses
//!       are reported as warnings; the executor handles them gracefully).
//!   1 — at least one `allowed_tokens` entry is not an Avantis pair. That
//!       leader can never trade that symbol via this bot — almost always
//!       a config typo.
//!
//! Requires the same Doppler env as the main binary:
//!   - BASE_WSS_URL
//!   - LIGHTER_BASE_URL (optional; defaults to mainnet)

use std::collections::HashSet;
use std::path::Path;

use alloy::providers::{ProviderBuilder, WsConnect};
use eyre::Result;
use highlane::config;
use highlane::shared::dex::avantis::contracts::{MULTICALL3, PAIR_STORAGE, parse_addr};
use highlane::shared::dex::avantis::pairs::load_pair_names;
use highlane::shared::dex::lighter::client::LighterClient;
use highlane::shared::dex::lighter::markets::{load_markets, symbol_root};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn")),
        )
        .init();

    let ws_url = std::env::var("BASE_WSS_URL").map_err(|_| {
        eyre::eyre!(
            "BASE_WSS_URL not set (run via `doppler run -- cargo run --bin validate-config`)"
        )
    })?;
    let lighter_base_url = std::env::var("LIGHTER_BASE_URL")
        .unwrap_or_else(|_| "https://mainnet.zklighter.elliot.ai".to_string());

    let cfg = config::load(Path::new("config.pkl"))?;
    println!("config.pkl: {} leader(s)", cfg.leaders.len());

    // Avantis pair set, normalized through symbol_root + uppercase (the same
    // path the watcher uses, so the equality check here mirrors runtime
    // behavior exactly).
    let provider = ProviderBuilder::new()
        .connect_ws(WsConnect::new(ws_url))
        .await?;
    let pairs =
        load_pair_names(&provider, parse_addr(PAIR_STORAGE), parse_addr(MULTICALL3)).await?;
    let avantis_roots: HashSet<String> = pairs.values().map(|name| symbol_root(name)).collect();
    println!("Avantis pairs: {}", avantis_roots.len());

    // Lighter market keys are themselves the normalized symbol_root (see
    // markets::load_markets), so `markets.contains_key(tok)` is the same
    // lookup the executor's handle_intent does.
    let client = LighterClient::new(&lighter_base_url)?;
    let markets = load_markets(&client).await?;
    println!("Lighter markets: {}", markets.len());
    println!();

    let mut any_avantis_miss = false;
    let mut any_lighter_miss = false;

    for l in &cfg.leaders {
        println!(
            "Leader {} ({:#x})  copy_ratio={:.4}",
            l.name, l.wallet, l.copy_ratio
        );
        for tok in &l.allowed_tokens {
            // `tok` was already normalized by config::validate.
            let on_avantis = avantis_roots.contains(tok);
            let on_lighter = markets.contains_key(tok);
            let a = if on_avantis { "OK " } else { "MISS" };
            let li = if on_lighter { "OK " } else { "MISS" };
            println!("  {tok:<8}  Avantis={a}  Lighter={li}");
            if !on_avantis {
                any_avantis_miss = true;
            }
            if !on_lighter {
                any_lighter_miss = true;
            }
        }
        println!();
    }

    if any_avantis_miss {
        eprintln!(
            "ERROR: one or more allowed_tokens are not listed on Avantis. The leader \
             cannot trade those symbols via this bot — fix config.pkl before deploying."
        );
        std::process::exit(1);
    }

    if any_lighter_miss {
        println!(
            "WARNING: tokens marked Lighter=MISS exist on Avantis but are not on Lighter. \
             The executor will log them and skip the copy at trade time. Re-run this \
             check after Lighter lists them, or remove the entry from allowed_tokens."
        );
    }
    println!("All allowed_tokens are tradeable on Avantis.");
    Ok(())
}
