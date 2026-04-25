use alloy::primitives::U256;

pub fn format_1e10(raw: U256) -> f64 {
    raw.to::<u128>() as f64 / 1e10
}

pub fn format_usdc(raw: U256) -> f64 {
    raw.to::<u128>() as f64 / 1e6
}
