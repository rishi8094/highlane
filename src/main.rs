mod shared;

use shared::dex::avantis::{contracts::parse_addr, watcher};
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
    let leader = parse_addr("0x2619108957623d4E8CC18663c41D822e2A30F19b"); //0x3b514bCDd2E96af48374c3D2ca42736a2393212F

    watcher::watch_leader(&ws_url, leader).await
}
