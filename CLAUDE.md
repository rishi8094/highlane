## CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A copy-trading bot. It watches on-chain activity from a configured set of "leader" wallets across supported DEXs (currently Avantis, a perpetuals DEX on Base), decodes their trades, and mirrors them — either on the same DEX the leader used, or on a different one. The architecture must therefore separate **observing** a trade from **executing** it, so any source DEX can be paired with any destination DEX.

## Project status

Early-stage Rust project. `src/main.rs` is still a `Hello, world!` stub. The substantive code lives under `src/shared/dex/avantis/` but is **not yet wired into the binary** — there is no `mod shared;` declaration in `main.rs`, and `pairs.rs` imports `eyre`, `serde`, and `serde_json` which are **not** declared in `Cargo.toml`. Those deps must be added before the `shared` tree compiles.

## Commands

Anything that touches RPCs, signing keys, or other config reads env vars from Doppler — **always prefix with `doppler run --`**:

- Run: `doppler run -- cargo run`
- Build: `cargo build` (release: `cargo build --release`) — pure compile, no env needed
- Test: `doppler run -- cargo test` — single test: `doppler run -- cargo test <name>`
- Lint: `cargo clippy --all-targets`
- Format: `cargo fmt`

Edition is `2024`, so a recent stable Rust toolchain is required.

## Architecture

Built on [`alloy`](https://docs.rs/alloy) + `tokio`. The bot decomposes into three layers; each DEX implements the first two and may implement the third:

1. **Watcher** — subscribes to a leader wallet's on-chain activity on a given DEX and emits decoded trade events (open/close, pair, side, size, leverage, prices).
2. **Normalizer** — converts a DEX-specific event into a protocol-agnostic `Trade` intent (pair symbol like `"ETH/USD"`, side, notional in USD, leverage, etc.) so it can be routed to any destination.
3. **Executor** — takes a normalized intent and submits the equivalent trade on a target DEX, translating symbols/scales/leverage to that DEX's conventions.

Code is organised under `src/shared/dex/<protocol>/`. To add a DEX, implement watcher + executor under a new `src/shared/dex/<name>/` module and register it with the router.

### `src/shared/dex/avantis/` (reference implementation)

- `contracts.rs` — single `alloy::sol!` block declaring the on-chain surface: `Trade` struct, `ICallbacks` events (`MarketExecuted`, `LimitExecuted` — these are the watcher's primary signal), `IPairStorage`, `ITradingStorage`, and a local `IBatchCall` redeclaration of Multicall3's `aggregate3`. Hardcoded Base addresses live here: `TRADING_STORAGE`, `PAIR_STORAGE`, `MULTICALL3`.
- `pairs.rs` — `load_pair_names(provider, pair_storage, multicall3)` returns `HashMap<pairIndex, "FROM/TO">`, used to translate Avantis's numeric `pairIndex` into the symbol the normalizer/router speaks. One `aggregate3` batches `getPairData` for every index; results cache to `tmp/pairs-<unix-ts>.json` with a 10-day TTL (read sweeps stale, write sweeps prior `pairs-*.json`).
- `format.rs` — fixed-point helpers for Avantis's on-chain scales: `format_1e10` (price scale) and `format_usdc` (1e6 / USDC, used for position size and PnL).
- `abis/` — Foundry-generated JSON ABIs kept for reference only. **Not loaded at runtime**; alloy's `sol!` macro generates bindings from the Solidity in `contracts.rs`. Extend the `sol!` block rather than wiring up the JSON.

The `tmp/` directory is the runtime cache root and is gitignored (only `.keep` is tracked). Treat anything under it as disposable.

## Conventions

- All on-chain reads should batch through Multicall3 (`MULTICALL3` in `contracts.rs`) using `IBatchCall::aggregate3` with `allowFailure: true`, decoding per-call via `<Call>::abi_decode_returns` and skipping failed entries — see `fetch_pair_names` in `pairs.rs` as the canonical pattern.
- Any new persistent on-chain data fetch should follow the same `tmp/<name>-<unix-ts>.json` cache scheme used by `pairs.rs` (TTL check, sweep-before-write).
- Keep DEX-specific scales (e.g. Avantis's 1e10 price / 1e6 USDC) confined to that DEX's `format.rs`. Cross-DEX code must speak in normalized units (USD notional, human-readable price, `"BASE/QUOTE"` symbol).
- Secrets (RPC URLs, leader-wallet addresses, signer keys) come from Doppler — never read them from a local `.env` or hardcode them. Use `std::env::var` after launching under `doppler run --`.
