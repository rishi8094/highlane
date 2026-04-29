// Library facade so binaries beyond the main `highlane` daemon (e.g.
// `inspect_logs`, one-shot recovery scripts) can reuse the same DB and DEX
// modules without duplicating code. The main bin re-imports these via
// `use highlane::...` rather than redeclaring `mod`.
pub mod db;
pub mod schema;
pub mod shared;
