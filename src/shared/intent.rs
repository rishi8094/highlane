use alloy::primitives::Address;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Side {
    Long,
    Short,
}

impl Side {
    pub fn flip(self) -> Self {
        match self {
            Side::Long => Side::Short,
            Side::Short => Side::Long,
        }
    }
}

impl fmt::Display for Side {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Side::Long => "LONG",
            Side::Short => "SHORT",
        })
    }
}

#[derive(Debug, Clone)]
pub enum TradeIntent {
    Open {
        leader: Address,
        symbol: String,
        side: Side,
        leader_collateral_usd: f64,
        leader_leverage: u64,
        leader_exec_price: f64,
        leader_pair_index: u64,
        leader_position_index: u64,
        source_tx: String,
        source_block: u64,
    },
    Close {
        leader: Address,
        symbol: String,
        leader_pair_index: u64,
        leader_position_index: u64,
        leader_exec_price: f64,
        leader_pnl_pct: f64,
        source_tx: String,
        source_block: u64,
    },
}

impl TradeIntent {
    pub fn leader(&self) -> Address {
        match self {
            TradeIntent::Open { leader, .. } | TradeIntent::Close { leader, .. } => *leader,
        }
    }

    pub fn symbol(&self) -> &str {
        match self {
            TradeIntent::Open { symbol, .. } | TradeIntent::Close { symbol, .. } => symbol,
        }
    }

    pub fn key(&self) -> (Address, u64, u64) {
        match self {
            TradeIntent::Open {
                leader,
                leader_pair_index,
                leader_position_index,
                ..
            }
            | TradeIntent::Close {
                leader,
                leader_pair_index,
                leader_position_index,
                ..
            } => (*leader, *leader_pair_index, *leader_position_index),
        }
    }
}
