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
        let http = Http::builder()
            .timeout(Duration::from_secs(15))
            .build()?;
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
        let v: NextNonceResp = serde_json::from_str(&body)
            .with_context(|| format!("decode nextNonce: {body}"))?;
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
    #[serde(default)]
    pub l1_address: String,
}

#[derive(Debug, Deserialize)]
struct NextNonceResp {
    #[serde(alias = "nextNonce")]
    nonce: i64,
}

#[derive(Debug, Deserialize)]
pub struct OrderBookDetailsResp {
    #[serde(default, alias = "perp_order_book_details")]
    pub perp: Vec<PerpsOrderBookDetail>,
}

/// Subset of fields we need from PerpsOrderBookDetail. Lighter returns more —
/// we pull only what we use and let the rest deserialize by ignoring unknowns.
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct PerpsOrderBookDetail {
    pub market_id: i32,
    pub symbol: String,
    /// Decimals scale for base amount (e.g. 4 means 1 unit = 1e-4 BASE)
    #[serde(default, alias = "size_decimals")]
    pub size_decimals: i32,
    /// Decimals scale for price (e.g. 2 means 1 unit = 1e-2 USD)
    #[serde(default, alias = "price_decimals")]
    pub price_decimals: i32,
    /// Min order size in base units (e.g. "0.001")
    #[serde(default, alias = "min_base_amount")]
    pub min_base_amount: Option<String>,
    /// Max leverage (e.g. 50)
    #[serde(default, alias = "max_leverage")]
    pub max_leverage: Option<f64>,
    /// Initial margin fraction (1/leverage). If max_leverage missing we derive from this.
    #[serde(default, alias = "initial_margin_fraction")]
    pub initial_margin_fraction: Option<f64>,
}

/// Lighter wraps the account either as { code, accounts: [...] } or returns it
/// directly; we tolerate both.
#[derive(Debug, Deserialize)]
struct AccountEnvelope {
    #[serde(default)]
    accounts: Vec<AccountDetails>,
    #[serde(default)]
    positions: Vec<AccountPosition>,
    #[serde(default)]
    available_balance: Option<String>,
    #[serde(default)]
    collateral: Option<String>,
}

impl AccountEnvelope {
    fn into_details(self) -> AccountDetails {
        if let Some(d) = self.accounts.into_iter().next() {
            return d;
        }
        AccountDetails {
            positions: self.positions,
            available_balance: self.available_balance,
            collateral: self.collateral,
        }
    }
}

#[derive(Debug, Deserialize, Default)]
pub struct AccountDetails {
    #[serde(default)]
    pub positions: Vec<AccountPosition>,
    #[serde(default)]
    pub available_balance: Option<String>,
    #[serde(default)]
    pub collateral: Option<String>,
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
