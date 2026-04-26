use alloy::primitives::Address;
use diesel_derive_enum::DbEnum;
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, DbEnum)]
#[ExistingTypePath = "crate::schema::sql_types::TradeSide"]
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
        source_tx: String,
        source_block: u64,
        signal_id: i32,
        symbol_id: i32,
    },
    Close {
        leader: Address,
        symbol: String,
        leader_pair_index: u64,
        leader_position_index: u64,
        /// Price at which the leader's close executed on Avantis (1e10-scaled
        /// → human float). Used to bound our reduce-only IOC on Lighter so the
        /// signer doesn't reject `price=0`.
        leader_exec_price: f64,
        signal_id: i32,
    },
}

impl TradeIntent {
    pub fn leader(&self) -> Address {
        match self {
            TradeIntent::Open { leader, .. } | TradeIntent::Close { leader, .. } => *leader,
        }
    }
}
