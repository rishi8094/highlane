//! libloading wrapper around the Go-compiled Lighter signer shared library.
//!
//! Lighter signs orders with Schnorr-over-ECgFp5/Goldilocks/Poseidon2 — a
//! ZK-rollup-native scheme with no Rust crate that matches Lighter's exact
//! parameters. The official Python SDK ships and dlopens a pre-built Go
//! shared library (`lighter-signer-*.so` / `.dylib`); we do the same here.
//!
//! Run `tools/fetch_lighter_signer.sh` once to vendor the binary for the
//! host platform under `vendor/lighter/<platform>/`.

use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_longlong, c_void};
use std::path::PathBuf;
use std::sync::Arc;

use eyre::{Context, Result, eyre};
use libloading::{Library, Symbol};
use tracing::info;

/// Mirror of the Go `SignedTxResponse` C struct. The `tx_type` byte is
/// part of the ABI layout but we don't consume it in Rust.
#[repr(C)]
#[allow(dead_code)]
struct CSignedTxResponse {
    tx_type: u8,
    tx_info: *mut c_char,
    tx_hash: *mut c_char,
    message_to_sign: *mut c_char,
    err: *mut c_char,
}

type CreateClientFn = unsafe extern "C" fn(
    url: *const c_char,
    private_key: *const c_char,
    chain_id: c_int,
    api_key_index: c_int,
    account_index: c_longlong,
) -> *mut c_char;

#[allow(clippy::too_many_arguments)]
type SignCreateOrderFn = unsafe extern "C" fn(
    market_index: c_int,
    client_order_index: c_longlong,
    base_amount: c_longlong,
    price: c_int,
    is_ask: c_int,
    order_type: c_int,
    time_in_force: c_int,
    reduce_only: c_int,
    trigger_price: c_int,
    order_expiry: c_longlong,
    integrator_account_index: c_longlong,
    integrator_taker_fee: c_int,
    integrator_maker_fee: c_int,
    skip_nonce: u8,
    nonce: c_longlong,
    api_key_index: c_int,
    account_index: c_longlong,
) -> CSignedTxResponse;

type FreeFn = unsafe extern "C" fn(*mut c_void);

pub struct LighterSigner {
    _lib: Arc<Library>,
    sign_create_order: SignCreateOrderFn,
    free: FreeFn,
    pub api_key_index: i32,
    pub account_index: i64,
}

pub const ORDER_TYPE_MARKET: i32 = 1;
pub const TIF_IOC: i32 = 0;
pub const IS_ASK_BUY: i32 = 0;
pub const IS_ASK_SELL: i32 = 1;

impl LighterSigner {
    /// Locate the platform-specific .so/.dylib under `vendor/lighter/<platform>/`.
    pub fn default_library_path() -> Option<PathBuf> {
        let (platform, ext) = match (std::env::consts::OS, std::env::consts::ARCH) {
            ("macos", "aarch64") => ("darwin-arm64", "dylib"),
            ("linux", "x86_64") => ("linux-amd64", "so"),
            ("linux", "aarch64") => ("linux-arm64", "so"),
            _ => return None,
        };
        Some(
            PathBuf::from("vendor/lighter")
                .join(platform)
                .join(format!("lighter-signer-{platform}.{ext}")),
        )
    }

    pub fn load(
        library_path: &std::path::Path,
        base_url: &str,
        chain_id: i32,
        api_key_private_hex: &str,
        api_key_index: i32,
        account_index: i64,
    ) -> Result<Self> {
        if !library_path.exists() {
            return Err(eyre!(
                "Lighter signer library not found at {}. Run tools/fetch_lighter_signer.sh first.",
                library_path.display()
            ));
        }

        let lib = unsafe { Library::new(library_path) }
            .with_context(|| format!("dlopen {}", library_path.display()))?;
        let lib = Arc::new(lib);

        let create_client: Symbol<CreateClientFn> =
            unsafe { lib.get(b"CreateClient\0") }.context("locate CreateClient symbol")?;
        let sign_create_order: Symbol<SignCreateOrderFn> =
            unsafe { lib.get(b"SignCreateOrder\0") }.context("locate SignCreateOrder symbol")?;
        let free: Symbol<FreeFn> = unsafe { lib.get(b"Free\0") }.context("locate Free symbol")?;

        let url_c = CString::new(base_url)?;
        let priv_c = CString::new(api_key_private_hex)?;
        let err_ptr = unsafe {
            create_client(
                url_c.as_ptr(),
                priv_c.as_ptr(),
                chain_id,
                api_key_index,
                account_index,
            )
        };
        if !err_ptr.is_null() {
            let err = unsafe { CStr::from_ptr(err_ptr) }
                .to_string_lossy()
                .into_owned();
            unsafe { free(err_ptr as *mut c_void) };
            return Err(eyre!("Lighter CreateClient failed: {err}"));
        }

        info!(
            chain_id,
            api_key_index, account_index, "Lighter signer loaded"
        );

        let sign_create_order = *sign_create_order;
        let free = *free;
        Ok(Self {
            _lib: lib,
            sign_create_order,
            free,
            api_key_index,
            account_index,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn sign_create_order(
        &self,
        market_index: i32,
        client_order_index: i64,
        base_amount: i64,
        price: i32,
        is_ask: i32,
        order_type: i32,
        time_in_force: i32,
        reduce_only: bool,
        trigger_price: i32,
        order_expiry: i64,
        nonce: i64,
    ) -> Result<String> {
        let resp = unsafe {
            (self.sign_create_order)(
                market_index,
                client_order_index,
                base_amount,
                price,
                is_ask,
                order_type,
                time_in_force,
                if reduce_only { 1 } else { 0 },
                trigger_price,
                order_expiry,
                0,
                0,
                0,
                0,
                nonce,
                self.api_key_index,
                self.account_index,
            )
        };
        self.consume_response(resp)
    }

    /// Drains every Go-allocated string out of the response (so the Go runtime
    /// can reclaim them) and returns only `tx_info` — the JSON payload we POST
    /// to /api/v1/sendTx.
    fn consume_response(&self, resp: CSignedTxResponse) -> Result<String> {
        let take = |p: *mut c_char| -> String {
            if p.is_null() {
                return String::new();
            }
            let s = unsafe { CStr::from_ptr(p) }.to_string_lossy().into_owned();
            unsafe { (self.free)(p as *mut c_void) };
            s
        };
        let err = take(resp.err);
        let tx_info = take(resp.tx_info);
        let _ = take(resp.tx_hash);
        let _ = take(resp.message_to_sign);
        if !err.is_empty() {
            return Err(eyre!("Lighter SignCreateOrder failed: {err}"));
        }
        Ok(tx_info)
    }
}

// SAFETY: the underlying Go library is thread-safe per its design (uses a global
// client registry keyed by api_key_index/account_index). Library is reference-counted;
// the function pointers we hold are stable for the lib's lifetime.
unsafe impl Send for LighterSigner {}
unsafe impl Sync for LighterSigner {}
