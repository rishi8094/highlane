mod db;
mod schema;
mod shared;

use shared::dex::Dex;
use shared::dex::avantis::{contracts::parse_addr, watcher};
use shared::dex::lighter::executor::{LighterConfig, run as run_executor};
use shared::intent::TradeIntent;
use tokio::sync::mpsc;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> eyre::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

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

    let (tx, rx) = mpsc::channel::<TradeIntent>(256);

    let watcher_pool = pool.clone();
    let watcher_fut = async move {
        watcher::watch_leader(&ws_url, leader, trader_id, watcher_pool, tx).await;
        #[allow(unreachable_code)]
        Ok::<_, eyre::Report>(())
    };
    let exec_fut = run_executor(rx, cfg, pool);

    tokio::try_join!(watcher_fut, exec_fut)?;
    Ok(())
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}
