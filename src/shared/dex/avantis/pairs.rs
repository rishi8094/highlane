use alloy::{
    primitives::{Address, Bytes, U256},
    providers::Provider,
    sol_types::SolCall,
};
use eyre::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use std::time::{Duration, SystemTime};
use tracing::{info, warn};

use super::contracts::{IBatchCall, IPairStorage};

const CACHE_DIR: &str = "tmp";
const CACHE_MAX_AGE: Duration = Duration::from_secs(10 * 24 * 60 * 60); // 10 days

#[derive(Serialize, Deserialize)]
struct PairCache {
    timestamp: u64,
    pairs: HashMap<u64, String>,
}

pub async fn load_pair_names(
    provider: &(impl Provider + Clone),
    pair_storage_addr: Address,
    multicall3_addr: Address,
) -> Result<HashMap<u64, String>> {
    if let Some(cached) = read_cache() {
        info!(count = cached.len(), "loaded Avantis pairs from cache");
        return Ok(cached);
    }

    let pairs = fetch_pair_names(provider, pair_storage_addr, multicall3_addr).await?;
    write_cache(&pairs);
    Ok(pairs)
}

fn find_cache_file() -> Option<(std::path::PathBuf, u64)> {
    let dir = Path::new(CACHE_DIR);
    if !dir.exists() {
        return None;
    }

    let entries = std::fs::read_dir(dir).ok()?;
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if let Some(ts_str) = name.strip_prefix("pairs-").and_then(|s| s.strip_suffix(".json"))
            && let Ok(ts) = ts_str.parse::<u64>()
        {
            return Some((entry.path(), ts));
        }
    }
    None
}

fn read_cache() -> Option<HashMap<u64, String>> {
    let (path, ts) = find_cache_file()?;

    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .ok()?
        .as_secs();

    if now - ts > CACHE_MAX_AGE.as_secs() {
        // Stale — remove and refetch
        let _ = std::fs::remove_file(&path);
        return None;
    }

    let data = std::fs::read_to_string(&path).ok()?;
    let cache: PairCache = serde_json::from_str(&data).ok()?;
    Some(cache.pairs)
}

fn write_cache(pairs: &HashMap<u64, String>) {
    let _ = std::fs::create_dir_all(CACHE_DIR);

    // Remove any existing cache files
    if let Ok(entries) = std::fs::read_dir(CACHE_DIR) {
        for entry in entries.flatten() {
            let name = entry.file_name();
            if name.to_string_lossy().starts_with("pairs-") {
                let _ = std::fs::remove_file(entry.path());
            }
        }
    }

    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let cache = PairCache {
        timestamp: now,
        pairs: pairs.clone(),
    };

    let filename = format!("{}/pairs-{}.json", CACHE_DIR, now);
    if let Ok(json) = serde_json::to_string_pretty(&cache) {
        let _ = std::fs::write(&filename, json);
    }
}

async fn fetch_pair_names(
    provider: &(impl Provider + Clone),
    pair_storage_addr: Address,
    multicall3_addr: Address,
) -> Result<HashMap<u64, String>> {
    let pair_storage = IPairStorage::new(pair_storage_addr, provider);
    let count: u64 = pair_storage.pairsCount().call().await?.to();

    let calls: Vec<IBatchCall::Call3> = (0..count)
        .map(|i| {
            let calldata = IPairStorage::getPairDataCall {
                _pairIndex: U256::from(i),
            }
            .abi_encode();
            IBatchCall::Call3 {
                target: pair_storage_addr,
                allowFailure: true,
                callData: Bytes::from(calldata),
            }
        })
        .collect();

    let multicall = IBatchCall::new(multicall3_addr, provider);
    let results: Vec<IBatchCall::Result3> = multicall.aggregate3(calls).call().await?;

    info!(count, "fetching Avantis pairs from chain");
    let mut pair_names = HashMap::new();
    for (i, result) in results.iter().enumerate() {
        if result.success {
            if let Ok(data) =
                IPairStorage::getPairDataCall::abi_decode_returns(&result.returnData)
            {
                let name = format!("{}/{}", data.from, data.to);
                pair_names.insert(i as u64, name);
            }
        } else {
            warn!(pair_index = i, "failed to load Avantis pair");
        }
    }

    Ok(pair_names)
}
