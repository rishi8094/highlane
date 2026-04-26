use std::sync::Arc;

use diesel::Connection;
use diesel::pg::PgConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::pooled_connection::{AsyncDieselConnectionManager, ManagerConfig};
use diesel_async::pooled_connection::bb8::Pool;
use diesel_migrations::{EmbeddedMigrations, MigrationHarness, embed_migrations};
use eyre::{Context, Result};
use rustls::{ClientConfig, RootCertStore};
use tokio_postgres_rustls::MakeRustlsConnect;
use tracing::{error, info};

const MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations");

pub type DbPool = Pool<AsyncPgConnection>;

pub async fn init() -> Result<DbPool> {
    // rustls 0.23 requires the process-wide CryptoProvider to be installed
    // before any TLS handshake. Idempotent — install_default returns Err if
    // already set, which we ignore.
    let _ = rustls::crypto::ring::default_provider().install_default();

    let url = std::env::var("DATABASE_URL")
        .map_err(|_| eyre::eyre!("DATABASE_URL not set (run via `doppler run -- cargo run`)"))?;

    run_migrations(&url).context("running embedded migrations")?;

    let mut mgr_cfg: ManagerConfig<AsyncPgConnection> = ManagerConfig::default();
    mgr_cfg.custom_setup = Box::new(|url| Box::pin(establish_tls(url.to_string())));
    let manager =
        AsyncDieselConnectionManager::<AsyncPgConnection>::new_with_config(url, mgr_cfg);
    let pool = Pool::builder()
        .max_size(8)
        .build(manager)
        .await
        .context("building Postgres pool")?;
    info!("postgres pool ready");
    Ok(pool)
}

/// Establish a tokio-postgres connection over rustls and hand it to
/// diesel-async. tokio-postgres has no built-in TLS — without this, any
/// server that requires SSL (e.g. Fly Postgres) will hang the handshake
/// until bb8's connection_timeout fires.
async fn establish_tls(url: String) -> diesel::ConnectionResult<AsyncPgConnection> {
    let tls = MakeRustlsConnect::new((*tls_config()).clone());
    let (client, conn) = tokio_postgres::connect(&url, tls)
        .await
        .map_err(|e| diesel::ConnectionError::BadConnection(e.to_string()))?;
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            error!(error = ?e, "postgres connection driver error");
        }
    });
    AsyncPgConnection::try_from(client).await
}

fn tls_config() -> Arc<ClientConfig> {
    let mut roots = RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    Arc::new(
        ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    )
}

/// Migrations run on a short-lived sync connection. `diesel_migrations` is
/// sync-only; libpq handles SSL transparently so no extra config is needed.
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
