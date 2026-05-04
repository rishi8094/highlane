use highlane::db;
use highlane::shared::dex::Dex;
use highlane::shared::dex::avantis::{contracts::parse_addr, watcher};
use highlane::shared::dex::lighter::executor::{LighterConfig, run as run_executor};
use highlane::shared::intent::TradeIntent;
use highlane::shared::notify::DiscordNotifier;
use tokio::sync::mpsc;

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let _log_guard = highlane::shared::observability::init()?;

    let ws_url = std::env::var("BASE_WSS_URL")
        .map_err(|_| eyre::eyre!("BASE_WSS_URL not set (run via `doppler run -- cargo run`)"))?;
    let leader = parse_addr("0x3b514bCDd2E96af48374c3D2ca42736a2393212F");

    let pool = db::init().await?;
    let leader_lower = format!("{:#x}", leader);
    let trader_id = db::traders::upsert_trader(&pool, &leader_lower, Dex::Avantis).await?;
    tracing::info!(trader_id, leader = %leader_lower, "trader registered");

    let cfg = LighterConfig {
        base_url: env_or("LIGHTER_BASE_URL", "https://mainnet.zklighter.elliot.ai"),
        chain_id: env_or("LIGHTER_CHAIN_ID", "304").parse()?,
        l1_address: std::env::var("LIGHTER_L1_ADDRESS")
            .map_err(|_| eyre::eyre!("LIGHTER_L1_ADDRESS not set"))?,
        account_index_override: std::env::var("LIGHTER_ACCOUNT_INDEX")
            .ok()
            .and_then(|s| s.parse().ok()),
        api_key_index: env_or("LIGHTER_API_KEY_INDEX", "0").parse()?,
        api_key_private_hex: std::env::var("LIGHTER_API_KEY_PRIVATE")
            .map_err(|_| eyre::eyre!("LIGHTER_API_KEY_PRIVATE not set"))?,
        follower_budget_override: std::env::var("FOLLOWER_BUDGET_USD")
            .ok()
            .and_then(|s| s.parse().ok()),
        leader_max_exposure_usd: env_or("LEADER_MAX_EXPOSURE_USD", "125000").parse()?,
        dry_run: env_or("LIGHTER_DRY_RUN", "false").eq_ignore_ascii_case("true"),
        slippage_bps: env_or("LIGHTER_SLIPPAGE_BPS", "50").parse()?,
    };

    let notifier = DiscordNotifier::new(std::env::var("DISCORD_WEBHOOK_URL").ok());
    if notifier.enabled() {
        tracing::info!("Discord webhook notifier enabled");
    } else {
        tracing::info!("DISCORD_WEBHOOK_URL not set; Discord notifications disabled");
    }

    let (tx, rx) = mpsc::channel::<TradeIntent>(256);

    let watcher_pool = pool.clone();
    let watcher_notifier = notifier.clone();
    let watcher_handle = tokio::spawn(async move {
        watcher::watch_leader(
            &ws_url,
            leader,
            trader_id,
            watcher_pool,
            tx,
            watcher_notifier,
        )
        .await
    });
    let exec_handle = tokio::spawn(run_executor(rx, cfg, pool, notifier.clone(), leader_lower));

    // SIGTERM is what fly.io / docker / k8s send on graceful shutdown; SIGINT
    // is Ctrl-C in local runs. Both should webhook before we exit.
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .map_err(|e| eyre::eyre!("failed to install SIGTERM handler: {e}"))?;

    let (which, reason, err): (&str, String, Option<eyre::Report>) = tokio::select! {
        r = watcher_handle => match r {
            Ok(_never) => ("watcher", "watcher returned unexpectedly".into(), None),
            Err(join_err) => {
                let kind = if join_err.is_panic() { "panicked" } else { "aborted" };
                ("watcher", format!("watcher {kind}"), Some(eyre::eyre!("watcher {kind}: {join_err}")))
            }
        },
        r = exec_handle => match r {
            Ok(Ok(())) => ("executor", "executor returned (intent channel closed)".into(), None),
            Ok(Err(e)) => {
                let msg = format!("executor errored: {e}");
                ("executor", msg, Some(e))
            }
            Err(join_err) => {
                let kind = if join_err.is_panic() { "panicked" } else { "aborted" };
                ("executor", format!("executor {kind}"), Some(eyre::eyre!("executor {kind}: {join_err}")))
            }
        },
        _ = tokio::signal::ctrl_c() => {
            ("signal", "received SIGINT".into(), None)
        }
        _ = sigterm.recv() => {
            ("signal", "received SIGTERM".into(), None)
        }
    };

    let fatal = err.is_some();
    tracing::error!(component = which, %reason, fatal, "bot shutting down");
    notifier.notify_shutdown(&reason, fatal).await;

    match err {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}
