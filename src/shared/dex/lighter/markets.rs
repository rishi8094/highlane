//! Cached map of Lighter perp markets keyed by symbol-root (e.g. "BTC", "ETH").
//!
//! Avantis pair names are like "BTC/USD". We strip the "/USD" suffix to match
//! Lighter's symbol convention (just "BTC" / "ETH" / etc.).

use std::collections::HashMap;
use std::path::Path;
use std::time::{Duration, SystemTime};

use eyre::Result;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use super::client::{LighterClient, PerpsOrderBookDetail};

const CACHE_DIR: &str = "tmp";
const CACHE_PREFIX: &str = "lighter-markets-";
const CACHE_MAX_AGE: Duration = Duration::from_secs(24 * 60 * 60);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Market {
    pub market_id: i32,
    pub symbol: String,
    pub size_decimals: i32,
    pub price_decimals: i32,
    pub min_base_amount: f64,
    pub max_leverage: u64,
}

#[derive(Serialize, Deserialize)]
struct MarketsCache {
    timestamp: u64,
    markets: HashMap<String, Market>,
}

pub async fn load_markets(client: &LighterClient) -> Result<HashMap<String, Market>> {
    if let Some(cached) = read_cache() {
        info!(count = cached.len(), "loaded Lighter markets from cache");
        return Ok(cached);
    }
    let markets = fetch(client).await?;
    write_cache(&markets);
    Ok(markets)
}

async fn fetch(client: &LighterClient) -> Result<HashMap<String, Market>> {
    let resp = client.order_book_details().await?;
    let mut out = HashMap::new();
    for d in resp.perp {
        if let Some(m) = parse_market(&d) {
            out.insert(symbol_root(&m.symbol), m);
        } else {
            warn!(symbol = %d.symbol, "skipping market with incomplete metadata");
        }
    }
    info!(count = out.len(), "fetched Lighter perp markets");
    Ok(out)
}

fn parse_market(d: &PerpsOrderBookDetail) -> Option<Market> {
    let min_base = d
        .min_base_amount
        .as_deref()
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(0.0);
    let max_lev = d
        .max_leverage
        .or_else(|| d.initial_margin_fraction.map(|imf| {
            if imf > 0.0 { 1.0 / imf } else { 0.0 }
        }))
        .map(|x| x.round() as u64)
        .unwrap_or(0);
    if d.size_decimals < 0 || d.price_decimals < 0 || max_lev == 0 {
        return None;
    }
    Some(Market {
        market_id: d.market_id,
        symbol: d.symbol.clone(),
        size_decimals: d.size_decimals,
        price_decimals: d.price_decimals,
        min_base_amount: min_base,
        max_leverage: max_lev,
    })
}

/// "BTC/USD" -> "BTC", "ETH" -> "ETH"
pub fn symbol_root(symbol: &str) -> String {
    symbol
        .split('/')
        .next()
        .unwrap_or(symbol)
        .trim()
        .to_uppercase()
}

fn find_cache_file() -> Option<(std::path::PathBuf, u64)> {
    let dir = Path::new(CACHE_DIR);
    if !dir.exists() {
        return None;
    }
    for entry in std::fs::read_dir(dir).ok()?.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if let Some(ts_str) = name
            .strip_prefix(CACHE_PREFIX)
            .and_then(|s| s.strip_suffix(".json"))
            && let Ok(ts) = ts_str.parse::<u64>()
        {
            return Some((entry.path(), ts));
        }
    }
    None
}

fn read_cache() -> Option<HashMap<String, Market>> {
    let (path, ts) = find_cache_file()?;
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .ok()?
        .as_secs();
    if now.saturating_sub(ts) > CACHE_MAX_AGE.as_secs() {
        let _ = std::fs::remove_file(&path);
        return None;
    }
    let data = std::fs::read_to_string(&path).ok()?;
    let cache: MarketsCache = serde_json::from_str(&data).ok()?;
    Some(cache.markets)
}

fn write_cache(markets: &HashMap<String, Market>) {
    let _ = std::fs::create_dir_all(CACHE_DIR);
    if let Ok(entries) = std::fs::read_dir(CACHE_DIR) {
        for entry in entries.flatten() {
            let name = entry.file_name();
            if name.to_string_lossy().starts_with(CACHE_PREFIX) {
                let _ = std::fs::remove_file(entry.path());
            }
        }
    }
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let cache = MarketsCache {
        timestamp: now,
        markets: markets.clone(),
    };
    let filename = format!("{}/{}{}.json", CACHE_DIR, CACHE_PREFIX, now);
    if let Ok(json) = serde_json::to_string_pretty(&cache) {
        let _ = std::fs::write(&filename, json);
    }
}
