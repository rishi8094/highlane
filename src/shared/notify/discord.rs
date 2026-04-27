//! Discord webhook notifier for trade events and capital alerts.
//!
//! Sends are fire-and-forget: each public `notify_*` method spawns a tokio
//! task and returns immediately, so a slow Discord endpoint can never
//! backpressure the executor. Failures log at `error!(target = "webhook")`.

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use reqwest::Client;
use serde_json::{Value, json};
use tracing::{debug, error};

use crate::shared::intent::Side;

const COLOR_LONG: u32 = 0x2ecc71; // green
const COLOR_SHORT: u32 = 0xe74c3c; // red
const COLOR_AMBER: u32 = 0xf1c40f; // utilisation alert
const COLOR_LOSS: u32 = 0xe74c3c;
const COLOR_WIN: u32 = 0x2ecc71;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

/// Owned (so it can be moved into a spawned task) — call sites construct one
/// per fill.
#[derive(Debug, Clone)]
pub struct OpenFill {
    pub symbol: String,
    pub side: Side,
    pub leader_price: f64,
    pub our_price: f64,
    pub size: f64,
    pub notional_usd: f64,
    pub collateral_usd: f64,
    pub leverage: u32,
    pub leader_tx: String,
    pub our_tx: Option<String>,
}

#[derive(Debug, Clone)]
pub struct CloseFill {
    pub symbol: String,
    pub side: Side,
    pub leader_close_price: f64,
    pub our_close_price: f64,
    pub size: f64,
    pub our_entry_price: f64,
    pub leader_entry_price: f64,
    pub our_pnl_usd: f64,
    pub our_pnl_pct: f64,
    pub leader_pnl_pct: Option<f64>,
    pub our_tx: Option<String>,
}

#[derive(Debug, Clone)]
pub struct UtilisationAlert {
    pub utilisation_pct: f64,
    pub collateral_usd: f64,
    pub available_usd: f64,
    pub margin_used_usd: f64,
}

#[derive(Debug, Clone)]
pub struct StartupInfo {
    pub leader_address: String,
    pub follower_l1_address: String,
    pub account_index: i64,
    pub budget_usd: f64,
    pub leader_max_exposure_usd: f64,
    pub slippage_bps: u32,
    pub dry_run: bool,
}

#[derive(Clone)]
pub struct DiscordNotifier {
    inner: Arc<Inner>,
}

struct Inner {
    client: Client,
    webhook_url: Option<String>,
}

impl DiscordNotifier {
    pub fn new(webhook_url: Option<String>) -> Self {
        let client = Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()
            .unwrap_or_else(|_| Client::new());
        Self {
            inner: Arc::new(Inner {
                client,
                webhook_url,
            }),
        }
    }

    pub fn enabled(&self) -> bool {
        self.inner.webhook_url.is_some()
    }

    pub fn notify_open(&self, fill: OpenFill) {
        self.spawn_send(build_open_embed(&fill));
    }

    pub fn notify_close(&self, fill: CloseFill) {
        self.spawn_send(build_close_embed(&fill));
    }

    pub fn notify_utilisation(&self, alert: UtilisationAlert) {
        self.spawn_send(build_utilisation_embed(&alert));
    }

    pub fn notify_startup(&self, info: StartupInfo) {
        self.spawn_send(build_startup_embed(&info));
    }

    /// Send a shutdown notification synchronously and wait for it to flush.
    /// Used on the exit path where a `tokio::spawn` would be torn down by the
    /// runtime shutting down before the request completes.
    pub async fn notify_shutdown(&self, reason: &str, fatal: bool) {
        let Some(url) = self.inner.webhook_url.clone() else {
            return;
        };
        let body = build_shutdown_embed(reason, fatal);
        post(&self.inner.client, &url, body).await;
    }

    fn spawn_send(&self, body: Value) {
        let Some(url) = self.inner.webhook_url.clone() else {
            return;
        };
        let client = self.inner.client.clone();
        tokio::spawn(async move {
            post(&client, &url, body).await;
        });
    }
}

async fn post(client: &Client, url: &str, body: Value) {
    match client.post(url).json(&body).send().await {
        Ok(resp) => {
            let status = resp.status();
            if !status.is_success() {
                let text = resp.text().await.unwrap_or_default();
                error!(target: "webhook", %status, body = %text, "discord webhook non-2xx");
            } else {
                debug!(target: "webhook", "discord webhook delivered");
            }
        }
        Err(e) => {
            error!(target: "webhook", error = ?e, "discord webhook send failed");
        }
    }
}

fn build_open_embed(fill: &OpenFill) -> Value {
    let color = match fill.side {
        Side::Long => COLOR_LONG,
        Side::Short => COLOR_SHORT,
    };
    let title = format!("OPEN · {} · {}", fill.side, fill.symbol);

    // `our_price` is the IOC slippage *cap* (Lighter doesn't return per-fill
    // VWAP), so this number is the worst-case price we accepted, not the
    // realized one. Label it accordingly.
    let cap_bps = signed_slippage_bps(fill.leader_price, fill.our_price, fill.side);

    let symbol_root = fill
        .symbol
        .split('/')
        .next()
        .unwrap_or(&fill.symbol)
        .to_string();

    let mut footer_parts = Vec::new();
    if !fill.leader_tx.is_empty() && fill.leader_tx != "?" {
        footer_parts.push(format!("Avantis · {}", truncate_tx(&fill.leader_tx)));
    }
    if let Some(tx) = &fill.our_tx
        && !tx.is_empty()
    {
        footer_parts.push(format!("Lighter · {}", truncate_tx(tx)));
    }
    let footer_text = footer_parts.join("  |  ");

    let mut embed = json!({
        "title": title,
        "color": color,
        "fields": [
            { "name": "Size", "value": format!("{} {} ({})", fmt_size(fill.size), symbol_root, fmt_usd(fill.notional_usd)), "inline": true },
            { "name": "Leverage", "value": format!("{}x ({} collateral)", fill.leverage, fmt_usd(fill.collateral_usd)), "inline": true },
            { "name": "Slippage cap", "value": fmt_signed_bps(cap_bps), "inline": true },
            { "name": "Leader price", "value": fmt_usd(fill.leader_price), "inline": true },
            { "name": "Worst price", "value": fmt_usd(fill.our_price), "inline": true },
        ],
        "timestamp": Utc::now().to_rfc3339(),
    });
    if !footer_text.is_empty() {
        embed["footer"] = json!({ "text": footer_text });
    }
    json!({ "embeds": [embed] })
}

fn build_close_embed(fill: &CloseFill) -> Value {
    let color = if fill.our_pnl_usd >= 0.0 {
        COLOR_WIN
    } else {
        COLOR_LOSS
    };
    // Lighter doesn't return per-fill VWAPs, so `our_pnl_*` is computed from
    // the IOC slippage cap on entry/exit and represents a *worst-case bound*
    // on realized PnL, not the actual figure. Surface it that way.
    let title = format!(
        "CLOSE · {} · {} · ≥ {} ({})",
        fill.side,
        fill.symbol,
        fmt_signed_usd(fill.our_pnl_usd),
        fmt_signed_pct(fill.our_pnl_pct),
    );

    let symbol_root = fill
        .symbol
        .split('/')
        .next()
        .unwrap_or(&fill.symbol)
        .to_string();

    let our_line = format!(
        "≥ {} ({})",
        fmt_signed_usd(fill.our_pnl_usd),
        fmt_signed_pct(fill.our_pnl_pct)
    );
    let leader_line = match fill.leader_pnl_pct {
        Some(pct) => fmt_signed_pct(pct),
        None => "—".to_string(),
    };

    let mut fields = vec![
        json!({ "name": "Entry (cap)", "value": format!("{} (leader {})", fmt_usd(fill.our_entry_price), fmt_usd(fill.leader_entry_price)), "inline": true }),
        json!({ "name": "Exit (cap)", "value": format!("{} (leader {})", fmt_usd(fill.our_close_price), fmt_usd(fill.leader_close_price)), "inline": true }),
        json!({ "name": "Size", "value": format!("{} {}", fmt_size(fill.size), symbol_root), "inline": true }),
        json!({ "name": "Our PnL (worst-case)", "value": our_line, "inline": true }),
        json!({ "name": "Leader PnL", "value": leader_line, "inline": true }),
    ];
    if let Some(tx) = &fill.our_tx
        && !tx.is_empty()
    {
        fields.push(json!({
            "name": "Lighter tx",
            "value": format!("`{}`", truncate_tx(tx)),
            "inline": false,
        }));
    }

    let embed = json!({
        "title": title,
        "color": color,
        "fields": fields,
        "timestamp": Utc::now().to_rfc3339(),
    });
    json!({ "embeds": [embed] })
}

fn build_shutdown_embed(reason: &str, fatal: bool) -> Value {
    let title = if fatal {
        "highlane offline · CRASHED"
    } else {
        "highlane offline · stopped"
    };
    let color = if fatal { COLOR_LOSS } else { COLOR_AMBER };
    let embed = json!({
        "title": title,
        "color": color,
        "fields": [
            { "name": "Reason", "value": reason, "inline": false },
        ],
        "timestamp": Utc::now().to_rfc3339(),
    });
    json!({ "embeds": [embed] })
}

fn build_startup_embed(info: &StartupInfo) -> Value {
    let mode = if info.dry_run { "DRY-RUN" } else { "LIVE" };
    let title = format!("highlane online · {mode}");
    let embed = json!({
        "title": title,
        "color": if info.dry_run { COLOR_AMBER } else { COLOR_WIN },
        "fields": [
            { "name": "Leader", "value": format!("`{}`", truncate_addr(&info.leader_address)), "inline": true },
            { "name": "Follower", "value": format!("`{}`", truncate_addr(&info.follower_l1_address)), "inline": true },
            { "name": "Account", "value": info.account_index.to_string(), "inline": true },
            { "name": "Budget", "value": fmt_usd(info.budget_usd), "inline": true },
            { "name": "Leader cap", "value": fmt_usd(info.leader_max_exposure_usd), "inline": true },
            { "name": "Slippage", "value": format!("{} bps", info.slippage_bps), "inline": true },
        ],
        "timestamp": Utc::now().to_rfc3339(),
    });
    json!({ "embeds": [embed] })
}

fn build_utilisation_embed(a: &UtilisationAlert) -> Value {
    let title = format!("⚠ Capital utilisation {}", fmt_pct(a.utilisation_pct));
    let embed = json!({
        "title": title,
        "color": COLOR_AMBER,
        "fields": [
            { "name": "Margin used", "value": fmt_usd(a.margin_used_usd), "inline": true },
            { "name": "Available", "value": fmt_usd(a.available_usd), "inline": true },
            { "name": "Collateral", "value": fmt_usd(a.collateral_usd), "inline": true },
        ],
        "timestamp": Utc::now().to_rfc3339(),
    });
    json!({ "embeds": [embed] })
}

/// Signed slippage in bps relative to the leader's price, where `+` means
/// "worse for us" (paid more on a buy / received less on a sell).
fn signed_slippage_bps(leader: f64, ours: f64, side: Side) -> f64 {
    if leader == 0.0 {
        return 0.0;
    }
    let raw = (ours - leader) / leader * 10_000.0;
    match side {
        Side::Long => raw,   // buying — higher fill = worse
        Side::Short => -raw, // selling — lower fill = worse
    }
}

fn fmt_usd(x: f64) -> String {
    let neg = x < 0.0;
    let abs = x.abs();
    let whole = abs.trunc() as i64;
    let cents = (abs.fract() * 100.0).round() as i64;
    let whole_str = group_thousands(whole);
    let sign = if neg { "-" } else { "" };
    format!("{sign}${whole_str}.{cents:02}")
}

fn fmt_signed_usd(x: f64) -> String {
    if x >= 0.0 {
        format!("+{}", fmt_usd(x))
    } else {
        fmt_usd(x) // already has '-' from fmt_usd
    }
}

fn fmt_pct(frac: f64) -> String {
    format!("{:.1}%", frac * 100.0)
}

fn fmt_signed_pct(frac: f64) -> String {
    let v = frac * 100.0;
    if v >= 0.0 {
        format!("+{v:.2}%")
    } else {
        format!("{v:.2}%")
    }
}

fn fmt_signed_bps(bps: f64) -> String {
    if bps >= 0.0 {
        format!("+{bps:.1} bps")
    } else {
        format!("{bps:.1} bps")
    }
}

fn fmt_size(x: f64) -> String {
    // Show enough precision for crypto sizes without trailing noise.
    if x.abs() >= 1.0 {
        format!("{x:.4}")
    } else {
        format!("{x:.6}")
    }
}

fn group_thousands(n: i64) -> String {
    let s = n.abs().to_string();
    let bytes = s.as_bytes();
    let mut out = String::with_capacity(s.len() + s.len() / 3);
    for (i, b) in bytes.iter().enumerate() {
        if i > 0 && (bytes.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(*b as char);
    }
    if n < 0 {
        let mut neg = String::with_capacity(out.len() + 1);
        neg.push('-');
        neg.push_str(&out);
        neg
    } else {
        out
    }
}

fn truncate_tx(tx: &str) -> String {
    let t = tx.trim();
    if t.len() <= 14 {
        return t.to_string();
    }
    let head = &t[..10]; // "0x" + 8
    let tail = &t[t.len() - 4..];
    format!("{head}…{tail}")
}

fn truncate_addr(addr: &str) -> String {
    let a = addr.trim();
    if a.len() <= 12 {
        return a.to_string();
    }
    let head = &a[..6]; // "0x" + 4
    let tail = &a[a.len() - 4..];
    format!("{head}…{tail}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_embed_contains_expected_fields() {
        let fill = OpenFill {
            symbol: "BTC/USD".into(),
            side: Side::Long,
            leader_price: 77_000.0,
            our_price: 77_024.5,
            size: 0.0649,
            notional_usd: 5_000.0,
            collateral_usd: 200.0,
            leverage: 25,
            leader_tx: "0x1234567890abcdef1234567890abcdef12345678".into(),
            our_tx: Some("0xfedcba0987654321fedcba0987654321fedcba09".into()),
        };
        let v = build_open_embed(&fill);
        let s = serde_json::to_string(&v).unwrap();
        assert!(s.contains("OPEN · LONG · BTC/USD"), "{s}");
        assert!(s.contains("0.064900 BTC"), "{s}");
        assert!(s.contains("$5,000.00"), "{s}");
        assert!(s.contains("25x"), "{s}");
        assert!(s.contains("$200.00"), "{s}");
        assert!(s.contains("Leader price"), "{s}");
        assert!(s.contains("Worst price"), "{s}");
        // ~3.18 bps slippage cap on a long
        assert!(s.contains("Slippage cap"), "{s}");
        assert!(s.contains("bps"), "{s}");
        assert!(s.contains("0x12345678…5678"), "{s}");
    }

    #[test]
    fn close_embed_shows_pnl_in_title() {
        let fill = CloseFill {
            symbol: "BTC/USD".into(),
            side: Side::Long,
            leader_close_price: 77_700.0,
            our_close_price: 77_672.3,
            size: 0.0649,
            our_entry_price: 77_024.5,
            leader_entry_price: 77_000.0,
            our_pnl_usd: 42.18,
            our_pnl_pct: 0.0211,
            leader_pnl_pct: Some(0.0184),
            our_tx: Some("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd".into()),
        };
        let v = build_close_embed(&fill);
        let s = serde_json::to_string(&v).unwrap();
        assert!(s.contains("CLOSE · LONG · BTC/USD"), "{s}");
        assert!(s.contains("+$42.18"), "{s}");
        assert!(s.contains("+2.11%"), "{s}");
        assert!(s.contains("+1.84%"), "{s}"); // leader pnl
    }

    #[test]
    fn utilisation_embed_basic() {
        let v = build_utilisation_embed(&UtilisationAlert {
            utilisation_pct: 0.782,
            collateral_usd: 1_000.0,
            available_usd: 217.9,
            margin_used_usd: 782.1,
        });
        let s = serde_json::to_string(&v).unwrap();
        assert!(s.contains("78.2%"), "{s}");
        assert!(s.contains("$782.10"), "{s}");
        assert!(s.contains("$217.90"), "{s}");
    }

    #[test]
    fn slippage_sign_convention() {
        // Long: paid more than signal → +ve bps (bad for us).
        let bps = signed_slippage_bps(100.0, 100.5, Side::Long);
        assert!((bps - 50.0).abs() < 0.01);
        // Short: sold for less than signal → +ve bps (bad for us).
        let bps = signed_slippage_bps(100.0, 99.5, Side::Short);
        assert!((bps - 50.0).abs() < 0.01);
    }

    #[test]
    fn fmt_helpers() {
        assert_eq!(fmt_usd(1234.5), "$1,234.50");
        assert_eq!(fmt_usd(-12.0), "-$12.00");
        assert_eq!(fmt_signed_usd(0.0), "+$0.00");
        assert_eq!(fmt_signed_pct(0.0184), "+1.84%");
        assert_eq!(fmt_signed_pct(-0.005), "-0.50%");
        assert_eq!(
            truncate_tx("0x1234567890abcdef1234567890abcdef12345678"),
            "0x12345678…5678"
        );
    }
}
