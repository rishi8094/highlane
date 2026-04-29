use std::time::Duration;

use eyre::{Context, Result, eyre};
use reqwest::Client as Http;
use serde::{Deserialize, Serialize};

#[derive(Clone)]
pub struct LighterClient {
    base_url: String,
    http: Http,
}

impl LighterClient {
    pub fn new(base_url: impl Into<String>) -> Result<Self> {
        let http = Http::builder().timeout(Duration::from_secs(15)).build()?;
        Ok(Self {
            base_url: base_url.into().trim_end_matches('/').to_string(),
            http,
        })
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }

    pub async fn accounts_by_l1_address(&self, l1_address: &str) -> Result<AccountsByL1Address> {
        let resp = self
            .http
            .get(self.url("/api/v1/accountsByL1Address"))
            .query(&[("l1_address", l1_address)])
            .send()
            .await
            .context("GET accountsByL1Address")?;
        let status = resp.status();
        let body = resp.text().await?;
        if !status.is_success() {
            return Err(eyre!("accountsByL1Address {status}: {body}"));
        }
        serde_json::from_str(&body).with_context(|| format!("decode accountsByL1Address: {body}"))
    }

    pub async fn next_nonce(&self, account_index: i64, api_key_index: i32) -> Result<i64> {
        let resp = self
            .http
            .get(self.url("/api/v1/nextNonce"))
            .query(&[
                ("account_index", account_index.to_string()),
                ("api_key_index", api_key_index.to_string()),
            ])
            .send()
            .await
            .context("GET nextNonce")?;
        let status = resp.status();
        let body = resp.text().await?;
        if !status.is_success() {
            return Err(eyre!("nextNonce {status}: {body}"));
        }
        let v: NextNonceResp =
            serde_json::from_str(&body).with_context(|| format!("decode nextNonce: {body}"))?;
        Ok(v.nonce)
    }

    pub async fn account(&self, account_index: i64) -> Result<AccountDetails> {
        let resp = self
            .http
            .get(self.url("/api/v1/account"))
            .query(&[
                ("by", "index".to_string()),
                ("value", account_index.to_string()),
            ])
            .send()
            .await
            .context("GET account")?;
        let status = resp.status();
        let body = resp.text().await?;
        if !status.is_success() {
            return Err(eyre!("account {status}: {body}"));
        }
        let env: AccountEnvelope =
            serde_json::from_str(&body).with_context(|| format!("decode account: {body}"))?;
        Ok(env.into_details())
    }

    pub async fn order_book_details(&self) -> Result<OrderBookDetailsResp> {
        let resp = self
            .http
            .get(self.url("/api/v1/orderBookDetails"))
            .send()
            .await
            .context("GET orderBookDetails")?;
        let status = resp.status();
        let body = resp.text().await?;
        if !status.is_success() {
            return Err(eyre!("orderBookDetails {status}: {body}"));
        }
        serde_json::from_str(&body).with_context(|| format!("decode orderBookDetails: {body}"))
    }

    pub async fn send_tx(&self, tx_type: u8, tx_info: &str) -> Result<SendTxResp> {
        let form = [
            ("tx_type", tx_type.to_string()),
            ("tx_info", tx_info.to_string()),
            ("price_protection", "true".to_string()),
        ];
        let resp = self
            .http
            .post(self.url("/api/v1/sendTx"))
            .form(&form)
            .send()
            .await
            .context("POST sendTx")?;
        let status = resp.status();
        let body = resp.text().await?;
        if !status.is_success() {
            return Err(eyre!("sendTx {status}: {body}"));
        }
        serde_json::from_str(&body).with_context(|| format!("decode sendTx: {body}"))
    }
}

#[derive(Debug, Deserialize)]
pub struct AccountsByL1Address {
    #[serde(default)]
    pub sub_accounts: Vec<SubAccount>,
}

#[derive(Debug, Deserialize)]
pub struct SubAccount {
    pub index: i64,
    /// 0 = main cross account, 3 = system / special slot. Prefer 0.
    #[serde(default)]
    pub account_type: i32,
    #[serde(default)]
    pub collateral: Option<String>,
}

#[derive(Debug, Deserialize)]
struct NextNonceResp {
    #[serde(alias = "nextNonce")]
    nonce: i64,
}

#[derive(Debug, Deserialize)]
pub struct OrderBookDetailsResp {
    #[serde(default)]
    pub order_book_details: Vec<PerpsOrderBookDetail>,
}

/// Subset of fields we need from a Lighter perp orderbook entry.
/// Margin fractions are encoded in basis points × 0.01% (so `400` means
/// 4% initial margin → 25x max leverage). We derive max_leverage from
/// `min_initial_margin_fraction` (preferred) or fall back to default.
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct PerpsOrderBookDetail {
    pub market_id: i32,
    pub symbol: String,
    #[serde(default)]
    pub market_type: String,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub size_decimals: i32,
    #[serde(default)]
    pub price_decimals: i32,
    #[serde(default)]
    pub min_base_amount: Option<String>,
    #[serde(default)]
    pub min_initial_margin_fraction: Option<i32>,
    #[serde(default)]
    pub default_initial_margin_fraction: Option<i32>,
}

/// Lighter wraps the account either as { code, accounts: [...] } or returns it
/// directly; we tolerate both.
#[derive(Debug, Deserialize)]
struct AccountEnvelope {
    #[serde(default)]
    accounts: Vec<AccountDetails>,
    #[serde(default)]
    positions: Vec<AccountPosition>,
    #[serde(default, alias = "collateral", alias = "total_collateral")]
    collateral: Option<String>,
    #[serde(default, alias = "available_balance")]
    available_balance: Option<String>,
}

impl AccountEnvelope {
    fn into_details(self) -> AccountDetails {
        if let Some(d) = self.accounts.into_iter().next() {
            return d;
        }
        AccountDetails {
            positions: self.positions,
            collateral: self.collateral,
            available_balance: self.available_balance,
        }
    }
}

#[derive(Debug, Deserialize, Default)]
pub struct AccountDetails {
    #[serde(default)]
    pub positions: Vec<AccountPosition>,
    #[serde(default, alias = "total_collateral")]
    pub collateral: Option<String>,
    #[serde(default)]
    pub available_balance: Option<String>,
}

impl AccountDetails {
    /// Total USDC collateral on the account (deposited + unrealized PnL minus
    /// fees). Falls back to `available_balance` if Lighter doesn't return
    /// `collateral`. Returns `None` if neither field is parseable.
    pub fn wallet_balance(&self) -> Option<f64> {
        self.collateral
            .as_deref()
            .and_then(|s| s.parse::<f64>().ok())
            .or_else(|| {
                self.available_balance
                    .as_deref()
                    .and_then(|s| s.parse::<f64>().ok())
            })
    }
}

/// Subset of fields we need from AccountPosition. Lighter encodes direction
/// as `sign` (1 long, -1 short, 0 flat) and position size as a decimal string
/// in `position`.
#[derive(Debug, Deserialize, Clone)]
pub struct AccountPosition {
    pub market_id: i32,
    #[serde(default)]
    pub sign: i32,
    #[serde(default)]
    pub position: String,
}

impl AccountPosition {
    /// Signed integer base amount at the market's size_decimals scale.
    pub fn signed_base_int(&self, size_decimals: i32) -> i64 {
        let mag: f64 = self.position.parse().unwrap_or(0.0);
        let scaled = (mag * 10f64.powi(size_decimals)).round() as i64;
        if self.sign < 0 { -scaled } else { scaled }
    }
}

#[derive(Debug, Deserialize)]
pub struct SendTxResp {
    #[serde(default)]
    pub code: i32,
    #[serde(default)]
    pub message: String,
    #[serde(default, alias = "tx_hash")]
    pub tx_hash: String,
}
