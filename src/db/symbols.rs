use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use eyre::Result;

use crate::db::DbPool;
use crate::schema::symbols;

/// Get-or-create a symbol row by name (e.g. "ETH/USD"). Returns the id.
pub async fn upsert(pool: &DbPool, name: &str) -> Result<i32> {
    let mut conn = pool.lock().await;
    let id: i32 = diesel::insert_into(symbols::table)
        .values(symbols::name.eq(name))
        .on_conflict(symbols::name)
        // No-op update so RETURNING fires for an existing row.
        .do_update()
        .set(symbols::name.eq(name))
        .returning(symbols::id)
        .get_result(&mut *conn)
        .await?;
    Ok(id)
}
