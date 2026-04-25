use alloy::primitives::U256;

pub fn format_1e10(raw: U256) -> f64 {
    raw.to::<u128>() as f64 / 1e10
}

pub fn format_usdc(raw: U256) -> f64 {
    raw.to::<u128>() as f64 / 1e6
}

pub fn format_leverage(raw: U256) -> u64 {
    (raw.to::<u128>() / 10_000_000_000) as u64
}
