//! One-shot recovery for the LIT/USD pos=0 state corrupted by the
//! `LimitExecuted.isPnl` routing bug + the orphan it left behind.
//!
//! Targets specific signal IDs (default: 423, 424, 653 — the known broken
//! set on 2026-04-29). For each:
//!   1. Look up the linked `trades` row.
//!   2. If the trade is still open on Lighter, send a reduce-only IOC at a
//!      sentinel-wide price bound (no leader anchor — it's stale by hours)
//!      and stamp `trades.exit_*` with the actual fill outcome.
//!   3. Stamp `signals.exit_*` with chain truth for the orphan (424 → SL hit
//!      at 0.96284) or a sentinel for the bogus rows.
//!
//! Run AFTER the watcher patch is deployed so newly-arriving keeper closes
//! aren't re-corrupting state while we recover.
//!
//! Default mode is dry-run. Use `--apply` to actually flatten on Lighter +
//! write the DB.
//!
//! Usage:
//!   doppler run -- cargo run --bin recover_lit_pos0
//!   doppler run -- cargo run --bin recover_lit_pos0 -- --apply
//!   doppler run -- cargo run --bin recover_lit_pos0 -- --signals 423,424,653 --apply

use std::collections::HashMap;
use std::env;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chrono::{DateTime, Utc};
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use eyre::{Context, Result};
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

use highlane::db::{self, DbPool};
use highlane::schema::{signals, trades};
use highlane::shared::dex::lighter::client::{AccountPosition, LighterClient};
use highlane::shared::dex::lighter::markets::{Market, load_markets};
use highlane::shared::dex::lighter::signer::{
    IS_ASK_BUY, IS_ASK_SELL, LighterSigner, ORDER_TYPE_MARKET, TIF_IOC,
};
use highlane::shared::intent::Side;

const TX_TYPE_CREATE_ORDER: u8 = 14;
/// Match the executor's post-fill snapshot delay; Lighter IOCs settle within
/// hundreds of ms.
const POST_FILL_DELAY: Duration = Duration::from_millis(750);

/// Sentinel `exit_tx` written to bogus signal rows so future audits can
/// distinguish recovery-stamped rows from real leader closes.
const RECOVERY_SENTINEL: &str = "recovery:isPnl-bug:2026-04-29";

#[derive(Debug, Clone, Copy)]
enum Origin {
    /// Signal row was created from a misclassified keeper close — no real
    /// leader open behind it. exit_price stamped NULL.
    Bogus,
    /// Signal row was a real leader open; the leader's matching close was
    /// missed (and instead created a bogus sibling). Stamp exit_* from chain.
    OrphanRealOpen {
        exit_price: f64,
        exit_tx: &'static str,
        exit_block: i64,
        exit_at: &'static str, // ISO 8601
    },
}

#[derive(Debug, Clone, Copy)]
struct TargetSignal {
    id: i32,
    origin: Origin,
}

/// The known-broken set on 2026-04-29. Encoded inline because the chain
/// truth (424's SL hit) is what makes correct stamping possible — querying
/// alone can't tell us which row is the orphan.
const DEFAULT_TARGETS: &[TargetSignal] = &[
    TargetSignal {
        id: 423,
        origin: Origin::Bogus,
    },
    TargetSignal {
        id: 424,
        origin: Origin::OrphanRealOpen {
            exit_price: 0.96284,
            exit_tx: "0xf7bb458f31d008d7dcd667d8bfe7c72ea7eca5e6431ab954c5f732d571e4b73c",
            exit_block: 45354078,
            exit_at: "2026-04-29T21:18:23+00:00",
        },
    },
    TargetSignal {
        id: 653,
        origin: Origin::Bogus,
    },
];

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let args: Vec<String> = env::args().collect();
    let apply = args.iter().any(|a| a == "--apply");
    let signals_override = args
        .iter()
        .position(|a| a == "--signals")
        .and_then(|i| args.get(i + 1));

    let targets: Vec<TargetSignal> = match signals_override {
        Some(spec) => spec
            .split(',')
            .map(|s| s.trim().parse::<i32>())
            .collect::<std::result::Result<Vec<_>, _>>()
            .context("parsing --signals as comma-separated i32 list")?
            .into_iter()
            // Override-only mode: assume Bogus unless the id matches a known
            // orphan in DEFAULT_TARGETS (so re-running with --signals 423,424,653
            // still stamps 424 with chain truth instead of treating it as bogus).
            .map(|id| {
                DEFAULT_TARGETS
                    .iter()
                    .find(|t| t.id == id)
                    .copied()
                    .unwrap_or(TargetSignal {
                        id,
                        origin: Origin::Bogus,
                    })
            })
            .collect(),
        None => DEFAULT_TARGETS.to_vec(),
    };

    info!(
        apply,
        target_count = targets.len(),
        "recover_lit_pos0 starting"
    );
    if !apply {
        info!("DRY RUN — pass --apply to actually flatten Lighter + write DB");
    }

    let pool = db::init().await?;

    let cfg = LighterRecoveryConfig::from_env()?;
    let client = LighterClient::new(&cfg.base_url)?;
    let account_index = resolve_account_index(&client, &cfg).await?;
    info!(account_index, "resolved Lighter account");

    let markets = load_markets(&client).await?;
    info!(market_count = markets.len(), "Lighter markets ready");

    let signer = if cfg.dry_run_lighter {
        warn!("LIGHTER_DRY_RUN=true — Lighter sendTx will be skipped even with --apply");
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

    for target in &targets {
        if let Err(e) = handle_signal(
            target,
            &pool,
            &client,
            signer.as_ref(),
            &markets,
            account_index,
            &cfg,
            apply,
        )
        .await
        {
            error!(signal_id = target.id, error = ?e, "signal recovery failed");
        }
    }

    info!("recover_lit_pos0 done");
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn handle_signal(
    target: &TargetSignal,
    pool: &DbPool,
    client: &LighterClient,
    signer: Option<&LighterSigner>,
    markets: &HashMap<String, Market>,
    account_index: i64,
    cfg: &LighterRecoveryConfig,
    apply: bool,
) -> Result<()> {
    let signal = load_signal(pool, target.id).await?;
    info!(
        signal_id = signal.id,
        symbol = %signal.symbol,
        side = ?signal.side,
        pair = signal.leader_pair_index,
        pos = signal.leader_position_index,
        entry_price = signal.entry_price,
        signal_exit_at_set = signal.exit_at_set,
        origin = ?target.origin,
        "loaded signal"
    );

    let trade = load_trade_for_signal(pool, signal.id).await?;
    let close_outcome = match trade {
        None => {
            info!(
                signal_id = signal.id,
                "no trades row for this signal — DB-only stamp"
            );
            CloseOutcome::NoTrade
        }
        Some(t) if t.exit_at_set => {
            info!(
                trade_id = t.id,
                signal_id = signal.id,
                "trades row already has exit_at — skipping Lighter close"
            );
            CloseOutcome::AlreadyClosed
        }
        Some(t) => {
            close_on_lighter(
                &t,
                &signal,
                pool,
                client,
                signer,
                markets,
                account_index,
                cfg,
                apply,
            )
            .await?
        }
    };

    stamp_signal_exit(pool, &signal, target.origin, &close_outcome, apply).await?;
    Ok(())
}

#[derive(Debug)]
#[allow(dead_code)] // fields are surfaced via Debug only
enum CloseOutcome {
    NoTrade,
    AlreadyClosed,
    Flattened {
        delta_abs: i64,
        bound_price: f64,
        our_tx: Option<String>,
    },
    ZeroFill,
}

#[allow(clippy::too_many_arguments)]
async fn close_on_lighter(
    trade: &TradeRow,
    signal: &SignalRow,
    pool: &DbPool,
    client: &LighterClient,
    signer: Option<&LighterSigner>,
    markets: &HashMap<String, Market>,
    account_index: i64,
    cfg: &LighterRecoveryConfig,
    apply: bool,
) -> Result<CloseOutcome> {
    let market = markets
        .values()
        .find(|m| m.market_id == trade.market_id)
        .ok_or_else(|| {
            eyre::eyre!(
                "no Lighter market metadata for market_id={} — cannot derive size_decimals",
                trade.market_id
            )
        })?;

    // Reduce-only counter-side IOC. Close LONG → SELL, close SHORT → BUY.
    let close_side = signal.side.flip();
    let is_ask = match close_side {
        Side::Long => IS_ASK_BUY,
        Side::Short => IS_ASK_SELL,
    };
    // Sentinel-wide price bound: no fresh leader anchor available.
    // Lighter's IOC `price` is a one-sided bound interpreted by direction:
    //   BUY  (close SHORT, close_side = Long)  → MAX price we'll pay → use 10× entry as ceiling.
    //   SELL (close LONG,  close_side = Short) → MIN price we'll accept → 1 (= 1 tick, accepts any).
    // (The reverse — bound=1 on a BUY — would never cross at $0.0001/LIT.)
    let bound_price_int: i32 = match close_side {
        Side::Long => {
            // 10× headroom is enough to swallow any realistic gap; capping
            // at i32::MAX guards against pathological entries.
            let scaled = signal.entry_price * 10f64.powi(market.price_decimals) * 10.0;
            if scaled > i32::MAX as f64 {
                i32::MAX
            } else {
                scaled.round().max(1.0) as i32
            }
        }
        Side::Short => 1,
    };
    let bound_price_human = bound_price_int as f64 / 10f64.powi(market.price_decimals);

    info!(
        signal_id = signal.id,
        trade_id = trade.id,
        market_id = market.market_id,
        size = trade.size,
        side = ?close_side,
        bound_price_int,
        bound_price_human,
        apply,
        "PLAN: reduce-only IOC to flatten"
    );

    if !apply {
        return Ok(CloseOutcome::Flattened {
            delta_abs: 0, // not yet executed
            bound_price: bound_price_human,
            our_tx: None,
        });
    }

    let Some(signer) = signer else {
        warn!(
            "LIGHTER_DRY_RUN=true — skipping sendTx; DB will not be stamped to mirror the no-op Lighter side"
        );
        return Ok(CloseOutcome::ZeroFill);
    };

    let pre_size = signed_size(client, account_index, market).await?;
    let nonce = client.next_nonce(account_index, cfg.api_key_index).await?;
    let signed = signer.sign_create_order(
        market.market_id,
        client_order_id(),
        trade.size,
        bound_price_int,
        is_ask,
        ORDER_TYPE_MARKET,
        TIF_IOC,
        true, // reduce_only
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
    let our_tx = resp.tx_hash.clone();
    info!(tx_hash = %our_tx, "Lighter accepted CLOSE");

    tokio::time::sleep(POST_FILL_DELAY).await;
    let post_size = signed_size(client, account_index, market).await?;
    let delta = post_size - pre_size;

    if delta == 0 {
        warn!(
            signal_id = signal.id,
            trade_id = trade.id,
            pre_size,
            post_size,
            "Lighter CLOSE returned 0 fill — Lighter may already be flat, or IOC didn't cross"
        );
        // Stamp the trade row anyway with NULL exit_price so reconciliation
        // can pick it up. Mirrors `fail_open_for_market` semantics.
        stamp_trade_exit(pool, trade.id, None, None).await?;
        return Ok(CloseOutcome::ZeroFill);
    }

    let delta_abs = delta.unsigned_abs() as i64;
    info!(
        signal_id = signal.id,
        trade_id = trade.id,
        delta,
        delta_abs,
        "Lighter CLOSE filled"
    );
    // Stamp exit_price = NULL: Lighter doesn't return per-fill VWAP, and our
    // sentinel-wide IOC bound (10× entry) isn't a meaningful proxy. NULL +
    // non-NULL exit_at + non-NULL exit_tx is the documented "closed but fill
    // price unknown" shape (mirrors `fail_open_for_market`).
    stamp_trade_exit(pool, trade.id, None, Some(&our_tx)).await?;
    Ok(CloseOutcome::Flattened {
        delta_abs,
        bound_price: bound_price_human,
        our_tx: Some(our_tx),
    })
}

async fn stamp_trade_exit(
    pool: &DbPool,
    trade_id: i32,
    exit_price: Option<f64>,
    exit_tx: Option<&str>,
) -> Result<()> {
    let mut conn = pool.get().await?;
    let now = Utc::now();
    diesel::update(
        trades::table
            .filter(trades::id.eq(trade_id))
            .filter(trades::exit_at.is_null()),
    )
    .set((
        trades::exit_price.eq(exit_price),
        trades::exit_tx.eq(exit_tx),
        trades::exit_at.eq(Some(now)),
    ))
    .execute(&mut conn)
    .await?;
    info!(trade_id, "stamped trades.exit_*");
    Ok(())
}

async fn stamp_signal_exit(
    pool: &DbPool,
    signal: &SignalRow,
    origin: Origin,
    outcome: &CloseOutcome,
    apply: bool,
) -> Result<()> {
    if signal.exit_at_set {
        info!(
            signal_id = signal.id,
            "signals.exit_at already set — skipping stamp"
        );
        return Ok(());
    }

    let (exit_price, exit_tx, exit_block, exit_at_ts): (
        Option<f64>,
        String,
        Option<i64>,
        DateTime<Utc>,
    ) = match origin {
        Origin::OrphanRealOpen {
            exit_price,
            exit_tx,
            exit_block,
            exit_at,
        } => (
            Some(exit_price),
            exit_tx.to_string(),
            Some(exit_block),
            DateTime::parse_from_rfc3339(exit_at)?.with_timezone(&Utc),
        ),
        Origin::Bogus => (
            // For bogus signals we don't have a real chain close — use
            // NULL exit_price + sentinel exit_tx + now() so the row is
            // unambiguously a recovery stamp on inspection. exit_block
            // is required NOT NULL only when exit_price is set (per the
            // schema's nullable semantics) — leave NULL.
            None,
            RECOVERY_SENTINEL.to_string(),
            None,
            Utc::now(),
        ),
    };

    info!(
        signal_id = signal.id,
        ?origin,
        exit_price,
        %exit_tx,
        exit_block,
        ?outcome,
        apply,
        "PLAN: stamp signals.exit_*"
    );
    if !apply {
        return Ok(());
    }

    let mut conn = pool.get().await?;
    diesel::update(
        signals::table
            .filter(signals::id.eq(signal.id))
            .filter(signals::exit_at.is_null()),
    )
    .set((
        signals::exit_price.eq(exit_price),
        signals::exit_tx.eq(Some(exit_tx)),
        signals::exit_block.eq(exit_block),
        signals::exit_at.eq(Some(exit_at_ts)),
    ))
    .execute(&mut conn)
    .await?;
    info!(signal_id = signal.id, "stamped signals.exit_*");
    Ok(())
}

#[derive(Debug)]
struct SignalRow {
    id: i32,
    symbol: String,
    side: Side,
    leader_pair_index: i64,
    leader_position_index: i64,
    entry_price: f64,
    exit_at_set: bool,
}

async fn load_signal(pool: &DbPool, id: i32) -> Result<SignalRow> {
    use highlane::schema::symbols;
    let mut conn = pool.get().await?;
    let row: (
        i32,
        String,
        Side,
        i64,
        i64,
        f64,
        Option<chrono::DateTime<Utc>>,
    ) = signals::table
        .inner_join(symbols::table.on(symbols::id.eq(signals::symbol_id)))
        .filter(signals::id.eq(id))
        .select((
            signals::id,
            symbols::name,
            signals::side,
            signals::leader_pair_index,
            signals::leader_position_index,
            signals::entry_price,
            signals::exit_at,
        ))
        .first(&mut conn)
        .await
        .with_context(|| format!("loading signals.id={id}"))?;
    Ok(SignalRow {
        id: row.0,
        symbol: row.1,
        side: row.2,
        leader_pair_index: row.3,
        leader_position_index: row.4,
        entry_price: row.5,
        exit_at_set: row.6.is_some(),
    })
}

#[derive(Debug)]
struct TradeRow {
    id: i32,
    market_id: i32,
    size: i64,
    exit_at_set: bool,
}

async fn load_trade_for_signal(pool: &DbPool, signal_id: i32) -> Result<Option<TradeRow>> {
    let mut conn = pool.get().await?;
    let row: Option<(i32, i32, i64, Option<chrono::DateTime<Utc>>)> = trades::table
        .filter(trades::signal_id.eq(signal_id))
        .order(trades::id.desc())
        .select((trades::id, trades::market_id, trades::size, trades::exit_at))
        .first(&mut conn)
        .await
        .optional()?;
    Ok(row.map(|(id, market_id, size, exit_at)| TradeRow {
        id,
        market_id,
        size,
        exit_at_set: exit_at.is_some(),
    }))
}

async fn signed_size(client: &LighterClient, account_index: i64, market: &Market) -> Result<i64> {
    let det = client.account(account_index).await?;
    Ok(signed_size_for_market(
        &det.positions,
        market.market_id,
        market.size_decimals,
    ))
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

fn client_order_id() -> i64 {
    let ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    (ms as i64) & ((1_i64 << 48) - 1)
}

struct LighterRecoveryConfig {
    base_url: String,
    chain_id: i32,
    l1_address: Option<String>,
    account_index_override: Option<i64>,
    api_key_index: i32,
    api_key_private_hex: String,
    dry_run_lighter: bool,
}

impl LighterRecoveryConfig {
    fn from_env() -> Result<Self> {
        Ok(Self {
            base_url: env::var("LIGHTER_BASE_URL")
                .unwrap_or_else(|_| "https://mainnet.zklighter.elliot.ai".into()),
            chain_id: env::var("LIGHTER_CHAIN_ID")
                .unwrap_or_else(|_| "304".into())
                .parse()?,
            l1_address: env::var("LIGHTER_L1_ADDRESS").ok(),
            account_index_override: env::var("LIGHTER_ACCOUNT_INDEX")
                .ok()
                .and_then(|s| s.parse().ok()),
            api_key_index: env::var("LIGHTER_API_KEY_INDEX")
                .unwrap_or_else(|_| "0".into())
                .parse()?,
            api_key_private_hex: env::var("LIGHTER_API_KEY_PRIVATE")
                .map_err(|_| eyre::eyre!("LIGHTER_API_KEY_PRIVATE not set"))?,
            dry_run_lighter: env::var("LIGHTER_DRY_RUN")
                .unwrap_or_default()
                .eq_ignore_ascii_case("true"),
        })
    }
}

async fn resolve_account_index(client: &LighterClient, cfg: &LighterRecoveryConfig) -> Result<i64> {
    if let Some(idx) = cfg.account_index_override {
        return Ok(idx);
    }
    let l1 = cfg.l1_address.as_ref().ok_or_else(|| {
        eyre::eyre!("LIGHTER_L1_ADDRESS not set and no LIGHTER_ACCOUNT_INDEX override")
    })?;
    let resp = client.accounts_by_l1_address(l1).await?;
    let chosen = resp
        .sub_accounts
        .iter()
        .find(|a| a.account_type == 0)
        // `diesel::prelude::*` brings `LimitDsl::first` into scope which
        // shadows `slice::first`; use the fully-qualified slice path.
        .or_else(|| <[_]>::first(&resp.sub_accounts))
        .ok_or_else(|| eyre::eyre!("no Lighter sub-account found for L1 address {l1}"))?;
    Ok(chosen.index)
}
