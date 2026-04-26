use diesel::Connection;
use diesel::pg::PgConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::pooled_connection::bb8::Pool;
use diesel_migrations::{EmbeddedMigrations, MigrationHarness, embed_migrations};
use eyre::{Context, Result};
use tracing::info;

const MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations");

pub type DbPool = Pool<AsyncPgConnection>;

pub async fn init() -> Result<DbPool> {
    let url = std::env::var("DATABASE_URL")
        .map_err(|_| eyre::eyre!("DATABASE_URL not set (run via `doppler run -- cargo run`)"))?;

    run_migrations(&url).context("running embedded migrations")?;

    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    let pool = Pool::builder()
        .max_size(8)
        .build(manager)
        .await
        .context("building Postgres pool")?;
    info!("postgres pool ready");
    Ok(pool)
}

/// Migrations run on a short-lived sync connection. `diesel_migrations` is
/// sync-only; this avoids pulling another async-migration crate just for
/// startup.
fn run_migrations(url: &str) -> Result<()> {
    let mut conn = PgConnection::establish(url)
        .with_context(|| "connecting to Postgres for migrations")?;
    let applied = conn
        .run_pending_migrations(MIGRATIONS)
        .map_err(|e| eyre::eyre!("migration failure: {e}"))?;
    if applied.is_empty() {
        info!("no pending migrations");
    } else {
        for m in applied {
            info!(migration = %m, "applied migration");
        }
    }
    Ok(())
}
