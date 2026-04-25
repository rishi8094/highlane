//! Persistent mapping between leader positions and our follower-side fills.
//!
//! When the leader closes (specific) position `(leader, pair_idx, leader_pos_idx)`, we
//! need to know exactly which Lighter fill it corresponds to so we can place a
//! reduce-only counter-order with the same base_amount on the same market.

use std::collections::HashMap;
use std::path::Path;

use alloy::primitives::Address;
use eyre::Result;
use serde::{Deserialize, Serialize};

use crate::shared::intent::Side;

const STATE_PATH: &str = "tmp/copy-state.json";

pub type StateKey = (Address, u64, u64);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenPosition {
    pub leader: Address,
    pub leader_pair_index: u64,
    pub leader_position_index: u64,
    pub symbol: String,
    pub side: Side,
    pub market_id: i32,
    /// Base amount in scaled integer units (per market.size_decimals).
    pub base_amount: i64,
    pub opened_block: u64,
    pub opened_tx: String,
}

#[derive(Default)]
pub struct CopyState {
    map: HashMap<StateKey, OpenPosition>,
}

impl CopyState {
    pub fn load() -> Self {
        match read_file() {
            Some(positions) => {
                let map = positions.into_iter().map(|p| {
                    ((p.leader, p.leader_pair_index, p.leader_position_index), p)
                }).collect();
                Self { map }
            }
            None => Self::default(),
        }
    }

    pub fn get(&self, key: &StateKey) -> Option<&OpenPosition> {
        self.map.get(key)
    }

    pub fn insert(&mut self, pos: OpenPosition) -> Result<()> {
        let key = (pos.leader, pos.leader_pair_index, pos.leader_position_index);
        self.map.insert(key, pos);
        self.persist()
    }

    pub fn remove(&mut self, key: &StateKey) -> Result<Option<OpenPosition>> {
        let prev = self.map.remove(key);
        self.persist()?;
        Ok(prev)
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&StateKey, &OpenPosition)> {
        self.map.iter()
    }

    /// Drop every entry on a given Lighter market. Returns the dropped keys.
    pub fn drop_market(&mut self, market_id: i32) -> Result<Vec<StateKey>> {
        let to_drop: Vec<StateKey> = self
            .map
            .iter()
            .filter(|(_, v)| v.market_id == market_id)
            .map(|(k, _)| *k)
            .collect();
        for k in &to_drop {
            self.map.remove(k);
        }
        if !to_drop.is_empty() {
            self.persist()?;
        }
        Ok(to_drop)
    }

    fn persist(&self) -> Result<()> {
        let _ = std::fs::create_dir_all("tmp");
        let positions: Vec<&OpenPosition> = self.map.values().collect();
        let json = serde_json::to_string_pretty(&positions)?;
        let tmp = format!("{STATE_PATH}.tmp");
        std::fs::write(&tmp, json)?;
        std::fs::rename(&tmp, STATE_PATH)?;
        Ok(())
    }
}

fn read_file() -> Option<Vec<OpenPosition>> {
    let p = Path::new(STATE_PATH);
    if !p.exists() {
        return None;
    }
    let s = std::fs::read_to_string(p).ok()?;
    match serde_json::from_str::<Vec<OpenPosition>>(&s) {
        Ok(v) => Some(v),
        Err(e) => {
            tracing::warn!(error = ?e, "copy-state.json corrupted; starting fresh");
            None
        }
    }
}
