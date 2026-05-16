//! Unified candle fetcher across Lighter, Avantis, Hyperliquid and Ostium.
//!
//! Defaults: all four venues, BTC+ETH, last 24h ending now, 1m resolution.
//! Run: `cargo run --bin candles -- [--venues v1,v2] [--symbols BTC,ETH]
//!                                    [--from <iso|unix>] [--to <iso|unix>]
//!                                    [--resolution 1m] [--out-dir ./tmp]
//!                                    [--force]`

use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use chrono::{DateTime, Utc};
use eyre::{Context, Result, eyre};
use futures_util::future::join_all;
use reqwest::Client as Http;
use serde_json::{Value, json};

const DEFAULT_OUT_DIR: &str = "./tmp";
const DEFAULT_RESOLUTION: &str = "1m";
const HTTP_TIMEOUT: Duration = Duration::from_secs(30);
const UA: &str = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0 Safari/537.36";
const OSTIUM_AUTH: &str =
    "Basic MHN0aXVtOnQkUlJBc05jd3kpUHhUKGg2KERhWFBzbWokZVdFVHJeVVp4K2VVQXErbmtCSw==";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Venue {
    Lighter,
    Avantis,
    Hyperliquid,
    Ostium,
    Aster,
}

impl Venue {
    fn name(&self) -> &'static str {
        match self {
            Venue::Lighter => "lighter",
            Venue::Avantis => "avantis",
            Venue::Hyperliquid => "hyperliquid",
            Venue::Ostium => "ostium",
            Venue::Aster => "aster",
        }
    }
    fn parse(s: &str) -> Result<Self> {
        match s.to_ascii_lowercase().as_str() {
            "lighter" => Ok(Venue::Lighter),
            "avantis" => Ok(Venue::Avantis),
            "hyperliquid" | "hl" => Ok(Venue::Hyperliquid),
            "ostium" => Ok(Venue::Ostium),
            "aster" => Ok(Venue::Aster),
            _ => Err(eyre!("unknown venue: {s}")),
        }
    }
    fn all() -> Vec<Venue> {
        vec![
            Venue::Lighter,
            Venue::Avantis,
            Venue::Hyperliquid,
            Venue::Ostium,
            Venue::Aster,
        ]
    }
}

#[derive(Clone, Debug)]
struct Args {
    venues: Vec<Venue>,
    symbols: Vec<String>,
    from_unix: i64,
    to_unix: i64,
    resolution: String,
    resolution_minutes: u32,
    out_dir: PathBuf,
    force: bool,
}

#[derive(Debug)]
enum Outcome {
    Cached(PathBuf),
    Fetched { path: PathBuf, bytes: usize },
    Failed(String),
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = parse_args()?;
    fs::create_dir_all(&args.out_dir).ok();
    eprintln!(
        "fetching venues={:?} symbols={:?} from={} to={} resolution={} out_dir={}",
        args.venues.iter().map(|v| v.name()).collect::<Vec<_>>(),
        args.symbols,
        args.from_unix,
        args.to_unix,
        args.resolution,
        args.out_dir.display()
    );

    let http = Http::builder()
        .timeout(HTTP_TIMEOUT)
        .user_agent(UA)
        .build()?;

    let mut handles = Vec::new();
    for v in &args.venues {
        for s in &args.symbols {
            let v = *v;
            let s = s.clone();
            let a = args.clone();
            let http = http.clone();
            handles.push(tokio::spawn(async move { fetch(v, &s, &a, &http).await }));
        }
    }
    let results = join_all(handles).await;

    let mut any_failed = false;
    for r in results {
        match r {
            Ok(Outcome::Cached(p)) => eprintln!("  cached  {}", p.display()),
            Ok(Outcome::Fetched { path, bytes }) => {
                eprintln!("  fetched {} ({} bytes)", path.display(), bytes)
            }
            Ok(Outcome::Failed(msg)) => {
                eprintln!("  FAIL    {msg}");
                any_failed = true;
            }
            Err(e) => {
                eprintln!("  PANIC   {e}");
                any_failed = true;
            }
        }
    }
    if any_failed {
        std::process::exit(1);
    }
    Ok(())
}

fn parse_args() -> Result<Args> {
    let mut venues: Option<Vec<Venue>> = None;
    let mut symbols: Option<Vec<String>> = None;
    let mut from: Option<String> = None;
    let mut to: Option<String> = None;
    let mut resolution: Option<String> = None;
    let mut out_dir: Option<PathBuf> = None;
    let mut force = false;

    let mut iter = std::env::args().skip(1).peekable();
    while let Some(arg) = iter.next() {
        let (key, val_inline): (&str, Option<String>) = if let Some(eq) = arg.find('=') {
            (&arg[..eq], Some(arg[eq + 1..].to_string()))
        } else {
            (arg.as_str(), None)
        };
        let key = key.to_string();
        let take_val =
            |k: &str, inline: Option<String>, it: &mut std::iter::Peekable<_>| -> Result<String> {
                if let Some(v) = inline {
                    return Ok(v);
                }
                it.next().ok_or_else(|| eyre!("flag {k} requires a value"))
            };
        match key.as_str() {
            "--venues" => {
                let v = take_val(&key, val_inline, &mut iter)?;
                let parsed: Result<Vec<_>> = v.split(',').map(|s| Venue::parse(s.trim())).collect();
                venues = Some(parsed?);
            }
            "--symbols" => {
                let v = take_val(&key, val_inline, &mut iter)?;
                symbols = Some(
                    v.split(',')
                        .map(|s| s.trim().to_ascii_uppercase())
                        .collect(),
                );
            }
            "--from" => from = Some(take_val(&key, val_inline, &mut iter)?),
            "--to" => to = Some(take_val(&key, val_inline, &mut iter)?),
            "--resolution" | "--res" => resolution = Some(take_val(&key, val_inline, &mut iter)?),
            "--out-dir" => out_dir = Some(PathBuf::from(take_val(&key, val_inline, &mut iter)?)),
            "--force" => force = true,
            "-h" | "--help" => {
                print_help();
                std::process::exit(0);
            }
            _ => return Err(eyre!("unknown flag: {arg}")),
        }
    }

    let resolution = resolution.unwrap_or_else(|| DEFAULT_RESOLUTION.to_string());
    let resolution_minutes = parse_resolution_minutes(&resolution)?;
    let now = Utc::now().timestamp();
    let to_unix = match to {
        Some(s) => parse_time_to_unix(&s)?,
        None => now,
    };
    let from_unix = match from {
        Some(s) => parse_time_to_unix(&s)?,
        None => to_unix - 24 * 3600,
    };
    if from_unix >= to_unix {
        return Err(eyre!(
            "--from must be < --to (got from={from_unix} to={to_unix})"
        ));
    }

    Ok(Args {
        venues: venues.unwrap_or_else(Venue::all),
        symbols: symbols.unwrap_or_else(|| vec!["BTC".to_string(), "ETH".to_string()]),
        from_unix,
        to_unix,
        resolution,
        resolution_minutes,
        out_dir: out_dir.unwrap_or_else(|| PathBuf::from(DEFAULT_OUT_DIR)),
        force,
    })
}

fn print_help() {
    eprintln!(
        "Usage: candles [options]
  --venues <list>      lighter,avantis,hyperliquid,ostium,aster  (default: all)
  --symbols <list>     BTC,ETH                              (default: BTC,ETH)
  --from <ts>          ISO8601 or unix seconds              (default: 24h ago)
  --to <ts>            ISO8601 or unix seconds              (default: now)
  --resolution <res>   1m | 5m | 15m | 1h | 4h | 1d         (default: 1m)
  --out-dir <path>     output directory                     (default: ./tmp)
  --force              bypass cache"
    );
}

/// Accept an ISO8601 string or a unix-second integer; return unix seconds.
fn parse_time_to_unix(s: &str) -> Result<i64> {
    if let Ok(n) = s.parse::<i64>() {
        // Treat very large values as ms.
        return Ok(if n > 1_000_000_000_000 { n / 1000 } else { n });
    }
    let dt: DateTime<Utc> = s
        .parse()
        .with_context(|| format!("cannot parse time: {s}"))?;
    Ok(dt.timestamp())
}

/// "1m" → 1, "5m" → 5, "1h" → 60, "4h" → 240, "1d" → 1440.
fn parse_resolution_minutes(s: &str) -> Result<u32> {
    let s = s.trim().to_ascii_lowercase();
    let (num, unit) = s.split_at(s.len() - 1);
    let n: u32 = num
        .parse()
        .with_context(|| format!("invalid resolution number in {s}"))?;
    match unit {
        "m" => Ok(n),
        "h" => Ok(n * 60),
        "d" => Ok(n * 60 * 24),
        _ => Err(eyre!("unknown resolution unit in {s}")),
    }
}

fn cache_path(args: &Args, venue: Venue, sym: &str) -> PathBuf {
    args.out_dir.join(format!(
        "candles_{}_{}_{}_{}_{}.json",
        venue.name(),
        sym,
        args.resolution,
        args.from_unix,
        args.to_unix
    ))
}

async fn fetch(venue: Venue, sym: &str, args: &Args, http: &Http) -> Outcome {
    let path = cache_path(args, venue, sym);
    if path.exists() && !args.force {
        return Outcome::Cached(path);
    }
    let res = match venue {
        Venue::Lighter => fetch_lighter(http, sym, args).await,
        Venue::Avantis => fetch_avantis(http, sym, args).await,
        Venue::Hyperliquid => fetch_hyperliquid(http, sym, args).await,
        Venue::Ostium => fetch_ostium(http, sym, args).await,
        Venue::Aster => fetch_aster(http, sym, args).await,
    };
    match res {
        Ok(body) => match fs::write(&path, &body) {
            Ok(_) => Outcome::Fetched {
                path,
                bytes: body.len(),
            },
            Err(e) => Outcome::Failed(format!("{venue:?} {sym} write {}: {e}", path.display())),
        },
        Err(e) => Outcome::Failed(format!("{venue:?} {sym}: {e}")),
    }
}

// ───────────── Lighter ─────────────────────────────────────────────────────
fn lighter_market_id(sym: &str) -> Result<i32> {
    match sym {
        "BTC" => Ok(1),
        "ETH" => Ok(0),
        _ => Err(eyre!("Lighter market_id unknown for {sym}")),
    }
}
fn lighter_resolution(args: &Args) -> String {
    let m = args.resolution_minutes;
    if m.is_multiple_of(60) {
        format!("{}h", m / 60)
    } else {
        format!("{m}m")
    }
}
async fn fetch_lighter(http: &Http, sym: &str, args: &Args) -> Result<String> {
    let mid = lighter_market_id(sym)?;
    let resolution = lighter_resolution(args);
    let lo_ms = args.from_unix * 1000;
    let hi_ms = args.to_unix * 1000;
    // Page back from `to` until we cross `from`. 500 candles/page max.
    let mut all: std::collections::BTreeMap<i64, Value> = Default::default();
    let mut cursor_end_ms = hi_ms;
    let mut page = 0;
    while cursor_end_ms > lo_ms {
        page += 1;
        if page > 100 {
            break;
        }
        let url = format!(
            "https://mainnet.zklighter.elliot.ai/api/v1/candles?market_id={mid}&resolution={resolution}&start_timestamp={lo_ms}&end_timestamp={cursor_end_ms}&count_back=500"
        );
        let body = http
            .get(&url)
            .header("Origin", "https://app.lighter.xyz")
            .header("Referer", "https://app.lighter.xyz/")
            .send()
            .await?
            .text()
            .await?;
        let v: Value = serde_json::from_str(&body)
            .with_context(|| format!("decode lighter; head: {}", &body[..body.len().min(200)]))?;
        let candles = v
            .get("c")
            .or_else(|| v.get("candlesticks"))
            .or_else(|| v.get("candles"))
            .and_then(|x| x.as_array())
            .cloned()
            .unwrap_or_default();
        if candles.is_empty() {
            break;
        }
        let mut min_t = i64::MAX;
        for c in &candles {
            if let Some(t) = c.get("t").and_then(|x| x.as_i64()) {
                if t < min_t {
                    min_t = t;
                }
                all.insert(t, c.clone());
            }
        }
        if min_t == i64::MAX || min_t <= lo_ms {
            break;
        }
        cursor_end_ms = min_t - 1;
    }
    let arr: Vec<Value> = all.into_values().collect();
    Ok(serde_json::to_string_pretty(&Value::Array(arr))?)
}

// ───────────── Avantis ─────────────────────────────────────────────────────
fn avantis_symbol(sym: &str) -> String {
    format!("Crypto.{}/USD", sym)
}
async fn fetch_avantis(http: &Http, sym: &str, args: &Args) -> Result<String> {
    let url = format!(
        "https://feed-v3.avantisfi.com/v1/shims/tradingview/history?symbol={}&resolution={}&from={}&to={}",
        avantis_symbol(sym),
        args.resolution_minutes,
        args.from_unix,
        args.to_unix,
    );
    let body = http
        .get(&url)
        .header("Origin", "https://www.avantisfi.com")
        .header("Referer", "https://www.avantisfi.com/")
        .send()
        .await?
        .text()
        .await?;
    // Validate it parses as JSON (catches HTML error pages early).
    let _: Value = serde_json::from_str(&body)
        .with_context(|| format!("decode avantis; head: {}", &body[..body.len().min(200)]))?;
    Ok(body)
}

// ───────────── Hyperliquid ─────────────────────────────────────────────────
fn hl_resolution(args: &Args) -> String {
    let m = args.resolution_minutes;
    if m.is_multiple_of(60) {
        format!("{}h", m / 60)
    } else {
        format!("{m}m")
    }
}
async fn fetch_hyperliquid(http: &Http, sym: &str, args: &Args) -> Result<String> {
    // Hyperliquid's `candleSnapshot` caps responses at ~5000 candles per
    // call. At 1m that's only ~3.5 days, so for any window wider than that
    // we have to page back from `to` toward `from`, dedup by `t`, and stitch.
    let interval = hl_resolution(args);
    let lo_ms = args.from_unix * 1000;
    let mut cursor_end_ms = args.to_unix * 1000;
    let mut all: std::collections::BTreeMap<i64, Value> = Default::default();
    let mut page = 0;
    while cursor_end_ms > lo_ms {
        page += 1;
        if page > 20 {
            break;
        }
        let body = json!({
            "type": "candleSnapshot",
            "req": {
                "coin": sym,
                "interval": &interval,
                "startTime": lo_ms,
                "endTime": cursor_end_ms,
            }
        });
        let resp = http
            .post("https://api-ui.hyperliquid.xyz/info")
            .header("content-type", "application/json")
            .header("origin", "https://app.hyperliquid.xyz")
            .header("referer", "https://app.hyperliquid.xyz/")
            .body(serde_json::to_string(&body)?)
            .send()
            .await?;
        let status = resp.status();
        let raw = resp.text().await?;
        if !status.is_success() {
            return Err(eyre!(
                "hyperliquid HTTP {status}; head: {}",
                &raw[..raw.len().min(200)]
            ));
        }
        let candles: Vec<Value> = serde_json::from_str(&raw).with_context(|| {
            format!(
                "decode hl page {page}; head: {}",
                &raw[..raw.len().min(200)]
            )
        })?;
        if candles.is_empty() {
            break;
        }
        let mut min_t = i64::MAX;
        let mut added = 0_usize;
        for c in candles {
            if let Some(t) = c.get("t").and_then(|x| x.as_i64()) {
                if t < min_t {
                    min_t = t;
                }
                if all.insert(t, c).is_none() {
                    added += 1;
                }
            }
        }
        // If a page returned only candles we already have (no progress) or
        // its earliest candle is already at/before our window start, we're done.
        if added == 0 || min_t == i64::MAX || min_t <= lo_ms {
            break;
        }
        cursor_end_ms = min_t - 1;
    }
    let arr: Vec<Value> = all.into_values().collect();
    Ok(serde_json::to_string_pretty(&Value::Array(arr))?)
}

// ───────────── Ostium ──────────────────────────────────────────────────────
fn ostium_asset(sym: &str) -> String {
    format!("{sym}USD")
}
async fn fetch_ostium(http: &Http, sym: &str, args: &Args) -> Result<String> {
    // Ostium caps the number of candles per request ("Too many candles
    // requested" 400). Chunk the request to stay well under whatever the
    // cap is — target ~1500 candles per page, which works at any resolution
    // we care about.
    let target_candles_per_page: i64 = 1500;
    let chunk_secs: i64 = target_candles_per_page * args.resolution_minutes as i64 * 60;
    let mut all: std::collections::BTreeMap<i64, Value> = Default::default();
    let mut cursor_from = args.from_unix;
    let mut page = 0;
    while cursor_from < args.to_unix {
        page += 1;
        if page > 30 {
            return Err(eyre!("ostium pagination cap (30 pages) hit"));
        }
        let cursor_to = (cursor_from + chunk_secs).min(args.to_unix);
        let body = json!({
            "asset": ostium_asset(sym),
            "resolution": args.resolution_minutes.to_string(),
            "fromTimestampSeconds": cursor_from,
            "toTimestampSeconds": cursor_to,
        });
        let resp = http
            .post("https://history.ostium.io/ohlc/getHistorical")
            .header("authorization", OSTIUM_AUTH)
            .header("content-type", "application/json")
            .header("origin", "https://app.ostium.com")
            .header("referer", "https://app.ostium.com/")
            .body(serde_json::to_string(&body)?)
            .send()
            .await?;
        let status = resp.status();
        let raw = resp.text().await?;
        if !(200..300).contains(&status.as_u16()) {
            return Err(eyre!(
                "ostium HTTP {status} page {page} (from={cursor_from} to={cursor_to}); head: {}",
                &raw[..raw.len().min(200)]
            ));
        }
        let v: Value = serde_json::from_str(&raw).with_context(|| {
            format!(
                "decode ostium page {page}; head: {}",
                &raw[..raw.len().min(200)]
            )
        })?;
        if let Some(arr) = v.get("data").and_then(|x| x.as_array()) {
            for c in arr {
                if let Some(t) = c.get("time").and_then(|x| x.as_i64()) {
                    all.insert(t, c.clone());
                }
            }
        }
        cursor_from = cursor_to;
    }
    // Re-emit in the original shape `{"data": [...]}` so downstream parsers
    // don't need to change.
    let arr: Vec<Value> = all.into_values().collect();
    Ok(serde_json::to_string(
        &json!({ "data": Value::Array(arr) }),
    )?)
}

// ───────────── Aster ───────────────────────────────────────────────────────
fn aster_symbol(sym: &str) -> String {
    format!("{sym}USDT")
}
async fn fetch_aster(http: &Http, sym: &str, args: &Args) -> Result<String> {
    // Binance-Futures-compatible klines API: limit caps at 1000 per call and
    // rows are returned ascending by openTime, so we page forward from `from`
    // toward `to` and dedup by openTime (row[0]).
    let hi_ms = args.to_unix * 1000;
    let mut cursor_start_ms = args.from_unix * 1000;
    let mut all: std::collections::BTreeMap<i64, Value> = Default::default();
    let mut page = 0;
    while cursor_start_ms < hi_ms {
        page += 1;
        if page > 20 {
            break;
        }
        let url = format!(
            "https://www.asterdex.com/fapi/v1/klines?symbol={}&interval={}&contractType=PERPETUAL&startTime={}&endTime={}&limit=1000",
            aster_symbol(sym),
            args.resolution,
            cursor_start_ms,
            hi_ms,
        );
        let resp = http
            .get(&url)
            .header("Origin", "https://www.asterdex.com")
            .header("Referer", "https://www.asterdex.com/")
            .send()
            .await?;
        let status = resp.status();
        let raw = resp.text().await?;
        if !status.is_success() {
            return Err(eyre!(
                "aster HTTP {status} page {page}; head: {}",
                &raw[..raw.len().min(200)]
            ));
        }
        let rows: Vec<Value> = serde_json::from_str(&raw).with_context(|| {
            format!(
                "decode aster page {page}; head: {}",
                &raw[..raw.len().min(200)]
            )
        })?;
        if rows.is_empty() {
            break;
        }
        let n = rows.len();
        let mut max_t = i64::MIN;
        for c in rows {
            if let Some(t) = c.get(0).and_then(|x| x.as_i64()) {
                if t > max_t {
                    max_t = t;
                }
                all.insert(t, c);
            }
        }
        if n < 1000 || max_t == i64::MIN {
            break;
        }
        cursor_start_ms = max_t + 1;
    }
    let arr: Vec<Value> = all.into_values().collect();
    Ok(serde_json::to_string_pretty(&Value::Array(arr))?)
}

#[allow(dead_code)]
fn _path_to_string(p: &Path) -> String {
    p.display().to_string()
}
