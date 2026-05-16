//! Pkl-driven application config.
//!
//! Replaces the old `LEADER_MAX_EXPOSURE_USD` / `FOLLOWER_BUDGET_USD` env-var
//! pair. Every leader the bot follows is declared in `config.pkl` at the
//! repo root, with a per-leader `copy_ratio` (fraction of the leader's
//! collateral to copy) and `allowed_tokens` allowlist (which symbols this
//! leader is allowed to trade — keeps two leaders from fighting over the
//! same netted Lighter market).

use std::collections::HashSet;
use std::path::Path;
use std::str::FromStr;

use alloy::primitives::Address;
use eyre::{Result, WrapErr, eyre};
use serde::Deserialize;

use crate::shared::dex::Dex;
use crate::shared::dex::lighter::markets::symbol_root;

/// Wire format produced by `rpkl::from_config`. Only stdlib types so
/// deserialization is trivial; `validate` does the typed conversions and
/// safety checks.
#[derive(Debug, Clone, Deserialize)]
struct RawLeader {
    name: String,
    wallet: String,
    source_dex: String,
    copy_ratio: f64,
    allowed_tokens: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct RawConfig {
    leaders: Vec<RawLeader>,
}

/// Validated, typed form the rest of the program uses.
#[derive(Debug, Clone)]
pub struct LeaderConfig {
    pub name: String,
    pub wallet: Address,
    pub source_dex: Dex,
    pub copy_ratio: f64,
    /// Token roots (e.g. `"BTC"`, `"ETH"`) — already normalized through
    /// [`symbol_root`] + uppercase on load so runtime comparison is a direct
    /// equality check against the normalized intent symbol.
    pub allowed_tokens: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub leaders: Vec<LeaderConfig>,
}

pub fn load(path: &Path) -> Result<AppConfig> {
    // rpkl spawns the `pkl` CLI; if it's not on PATH the error surface is
    // generic ("unknown error"), so fail fast with a directly actionable
    // message when we can detect it ourselves.
    if which_pkl().is_none() {
        return Err(eyre!(
            "the `pkl` CLI is not on PATH but is required to evaluate config.pkl. \
             Install it from https://github.com/apple/pkl/releases (or run inside the \
             Docker image, which bundles pkl 0.31.1). Looked for `pkl` in PATH."
        ));
    }
    let raw: RawConfig = rpkl::from_config(path)
        .map_err(|e| eyre!("failed to evaluate pkl config at {}: {e}", path.display()))?;
    validate(raw)
}

fn which_pkl() -> Option<std::path::PathBuf> {
    let path = std::env::var_os("PATH")?;
    for dir in std::env::split_paths(&path) {
        let candidate = dir.join("pkl");
        if candidate.is_file() {
            return Some(candidate);
        }
    }
    None
}

fn validate(raw: RawConfig) -> Result<AppConfig> {
    if raw.leaders.is_empty() {
        return Err(eyre!("config.pkl has no leaders"));
    }
    let mut leaders = Vec::with_capacity(raw.leaders.len());
    for rl in raw.leaders {
        let RawLeader {
            name,
            wallet,
            source_dex,
            copy_ratio,
            allowed_tokens,
        } = rl;

        if name.trim().is_empty() {
            return Err(eyre!("leader is missing a name"));
        }
        if !(copy_ratio > 0.0 && copy_ratio <= 5.0) {
            return Err(eyre!(
                "leader {name:?}: copy_ratio must be in (0, 5]; got {copy_ratio}"
            ));
        }
        let wallet_addr: Address = wallet
            .parse()
            .with_context(|| format!("leader {name:?}: invalid wallet address {wallet:?}"))?;
        let dex = Dex::from_str(&source_dex)
            .with_context(|| format!("leader {name:?}: unknown source_dex {source_dex:?}"))?;
        // Only Avantis is wired end-to-end today. Accepting `lighter` (or any
        // other DEX `Dex::from_str` knows about) here would create a `traders`
        // row whose logs the Avantis watcher would happily process as if they
        // came from Avantis. Fail loudly until another watcher lands.
        if !matches!(dex, Dex::Avantis) {
            return Err(eyre!(
                "leader {name:?}: source_dex={source_dex:?} is not yet supported as a watch source; \
                 only \"avantis\" is wired end-to-end. Add the watcher for {source_dex:?} before \
                 declaring leaders against it."
            ));
        }
        if allowed_tokens.is_empty() {
            return Err(eyre!(
                "leader {name:?}: allowed_tokens must contain at least one symbol"
            ));
        }

        // Normalize via the same helper the executor uses so "BTC/USD",
        // "btc", and "BTC" can't sneak past the overlap check below.
        let mut normalized = Vec::with_capacity(allowed_tokens.len());
        for tok in &allowed_tokens {
            let n = symbol_root(tok);
            if n.is_empty() {
                return Err(eyre!(
                    "leader {name:?}: allowed_tokens contains an empty symbol"
                ));
            }
            normalized.push(n);
        }
        normalized.sort();
        normalized.dedup();

        leaders.push(LeaderConfig {
            name,
            wallet: wallet_addr,
            source_dex: dex,
            copy_ratio,
            allowed_tokens: normalized,
        });
    }

    // Overlap check on the normalized form: no symbol may appear in more
    // than one leader's allowed_tokens. Lighter nets positions per market,
    // so two leaders trading the same symbol could end up holding opposite
    // sides of one net position, which we cannot represent correctly.
    let mut seen: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    for l in &leaders {
        for sym in &l.allowed_tokens {
            if let Some(prev) = seen.insert(sym.clone(), l.name.clone())
                && prev != l.name
            {
                return Err(eyre!(
                    "config.pkl: symbol {sym:?} is declared in both {prev:?} and {:?}; \
                     Lighter nets positions so each symbol must belong to a single leader",
                    l.name
                ));
            }
        }
    }
    // Defensive: ensure leader names are unique too — duplicate names would
    // make the per-leader observability output ambiguous.
    let mut names_seen: HashSet<&str> = HashSet::new();
    for l in &leaders {
        if !names_seen.insert(&l.name) {
            return Err(eyre!("config.pkl: duplicate leader name {:?}", l.name));
        }
    }

    // Duplicate (source_dex, wallet) entries silently flatten in the DB
    // (`upsert_trader` is idempotent on that key) and the watcher's
    // `leaders.iter().find(...)` would always match the first declaration,
    // so a second policy (copy_ratio, allowed_tokens) for the same wallet
    // would be ignored without warning. Reject up front.
    let mut wallet_seen: std::collections::HashMap<(Dex, Address), String> =
        std::collections::HashMap::new();
    for l in &leaders {
        let key = (l.source_dex, l.wallet);
        if let Some(prev) = wallet_seen.insert(key, l.name.clone())
            && prev != l.name
        {
            return Err(eyre!(
                "config.pkl: wallet {:#x} on {:?} is declared by both {prev:?} and {:?}; \
                 the watcher only honors the first policy for a (source_dex, wallet) pair",
                l.wallet,
                l.source_dex,
                l.name
            ));
        }
    }
    Ok(AppConfig { leaders })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn raw(leaders: Vec<RawLeader>) -> RawConfig {
        RawConfig { leaders }
    }
    fn rl(name: &str, wallet: &str, dex: &str, ratio: f64, tokens: &[&str]) -> RawLeader {
        RawLeader {
            name: name.into(),
            wallet: wallet.into(),
            source_dex: dex.into(),
            copy_ratio: ratio,
            allowed_tokens: tokens.iter().map(|s| (*s).to_string()).collect(),
        }
    }
    const WALLET_A: &str = "0x3b514bCDd2E96af48374c3D2ca42736a2393212F";
    const WALLET_B: &str = "0x000000000000000000000000000000000000dEaD";

    #[test]
    fn duplicate_symbol_across_leaders_fails() {
        let cfg = raw(vec![
            rl("alice", WALLET_A, "avantis", 0.35, &["BTC", "ETH"]),
            rl("bob", WALLET_B, "avantis", 0.20, &["BTC"]),
        ]);
        let err = validate(cfg).unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("BTC"), "{msg}");
        assert!(msg.contains("alice") && msg.contains("bob"), "{msg}");
    }

    #[test]
    fn normalized_overlap_is_caught() {
        // "btc/usd" (alice) and "BTC" (bob) both normalize to "BTC".
        let cfg = raw(vec![
            rl("alice", WALLET_A, "avantis", 0.35, &["btc/usd"]),
            rl("bob", WALLET_B, "avantis", 0.20, &["BTC"]),
        ]);
        let err = validate(cfg).unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("BTC"), "{msg}");
    }

    #[test]
    fn invalid_wallet_names_offending_leader() {
        let cfg = raw(vec![rl("alice", "not-a-wallet", "avantis", 0.5, &["BTC"])]);
        let err = validate(cfg).unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("alice"), "{msg}");
        assert!(msg.contains("wallet"), "{msg}");
    }

    #[test]
    fn unknown_source_dex_names_offending_leader() {
        let cfg = raw(vec![rl("alice", WALLET_A, "hyperliquid", 0.5, &["BTC"])]);
        let err = validate(cfg).unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("alice"), "{msg}");
        assert!(msg.contains("hyperliquid"), "{msg}");
    }

    #[test]
    fn ratio_out_of_range_fails() {
        let too_big = raw(vec![rl("alice", WALLET_A, "avantis", 6.0, &["BTC"])]);
        assert!(validate(too_big).is_err());
        let zero = raw(vec![rl("alice", WALLET_A, "avantis", 0.0, &["BTC"])]);
        assert!(validate(zero).is_err());
        let negative = raw(vec![rl("alice", WALLET_A, "avantis", -0.1, &["BTC"])]);
        assert!(validate(negative).is_err());
    }

    #[test]
    fn empty_allowed_tokens_fails() {
        let cfg = raw(vec![rl("alice", WALLET_A, "avantis", 0.5, &[])]);
        assert!(validate(cfg).is_err());
    }

    #[test]
    fn happy_path_two_leaders_disjoint_tokens() {
        let cfg = raw(vec![
            rl("alice", WALLET_A, "avantis", 0.35, &["BTC/USD"]),
            rl("bob", WALLET_B, "avantis", 0.10, &["eth"]),
        ]);
        let app = validate(cfg).expect("should validate");
        assert_eq!(app.leaders.len(), 2);
        assert_eq!(app.leaders[0].allowed_tokens, vec!["BTC".to_string()]);
        assert_eq!(app.leaders[1].allowed_tokens, vec!["ETH".to_string()]);
    }

    #[test]
    fn lighter_source_dex_is_rejected_even_though_known() {
        // `Dex::from_str` accepts "lighter" but the watcher pipeline only
        // wires Avantis end-to-end. Catch this at config-load.
        let cfg = raw(vec![rl("alice", WALLET_A, "lighter", 0.5, &["BTC"])]);
        let err = validate(cfg).unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("alice"), "{msg}");
        assert!(
            msg.contains("not yet supported") || msg.contains("avantis"),
            "{msg}"
        );
    }

    #[test]
    fn duplicate_wallet_across_leaders_fails() {
        // Same wallet, different names, disjoint tokens — looks legitimate
        // but `upsert_trader` would return the same trader_id for both and
        // watcher.iter().find(...) would always pick the first, silently
        // dropping the second policy.
        let cfg = raw(vec![
            rl("alice", WALLET_A, "avantis", 0.35, &["BTC"]),
            rl("alice-clone", WALLET_A, "avantis", 0.10, &["ETH"]),
        ]);
        let err = validate(cfg).unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("alice"), "{msg}");
        assert!(msg.contains("alice-clone"), "{msg}");
    }
}
