use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use eyre::Result;

use crate::db::DbPool;
use crate::db::models::NewTrader;
use crate::schema::traders;
use crate::shared::dex::Dex;

/// Insert the trader if missing, otherwise return the existing row's id.
/// Idempotent on `(wallet_address, source_dex)`.
pub async fn upsert_trader(
    pool: &DbPool,
    wallet_address: &str,
    source_dex: Dex,
) -> Result<i32> {
    let mut conn = pool.lock().await;
    let new = NewTrader {
        wallet_address,
        source_dex,
    };
    let id: i32 = diesel::insert_into(traders::table)
        .values(&new)
        .on_conflict((traders::wallet_address, traders::source_dex))
        .do_update()
        .set(traders::wallet_address.eq(wallet_address))
        .returning(traders::id)
        .get_result(&mut *conn)
        .await?;
    Ok(id)
}
