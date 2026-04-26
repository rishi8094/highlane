use std::sync::Arc;
use std::time::Duration;

use diesel::Connection;
use diesel::pg::PgConnection;
use diesel_async::AsyncPgConnection;
use diesel_migrations::{EmbeddedMigrations, MigrationHarness, embed_migrations};
use eyre::{Context, Result};
use rustls::{ClientConfig, RootCertStore};
use tokio::sync::Mutex;
use tokio_postgres_rustls::MakeRustlsConnect;
use tracing::{error, info};

const MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations");

/// Single shared async Postgres connection. Single-machine deployment, so
/// pooling adds no throughput. Calls serialize through the mutex; given the
/// bot's <1 query/sec rate this is fine.
pub type DbPool = Arc<Mutex<AsyncPgConnection>>;

pub async fn init() -> Result<DbPool> {
    // rustls 0.23 requires the process-wide CryptoProvider to be installed
    // before any TLS handshake. Idempotent — install_default returns Err if
    // already set, which we ignore.
    let _ = rustls::crypto::ring::default_provider().install_default();

    let url = std::env::var("DATABASE_URL")
        .map_err(|_| eyre::eyre!("DATABASE_URL not set (run via `doppler run -- cargo run`)"))?;

    run_migrations(&url).context("running embedded migrations")?;

    info!("opening async postgres connection (15s budget)");
    let conn = match tokio::time::timeout(Duration::from_secs(15), establish_tls(url)).await {
        Ok(Ok(c)) => c,
        Ok(Err(e)) => return Err(eyre::eyre!("async postgres connect failed: {e}")),
        Err(_) => {
            return Err(eyre::eyre!(
                "async postgres connect timed out after 15s — check sslmode= in DATABASE_URL \
                 or whether tokio resolves the host differently than libpq."
            ));
        }
    };
    info!("postgres connection ready");
    Ok(Arc::new(Mutex::new(conn)))
}

/// Establish a tokio-postgres connection over rustls and hand it to
/// diesel-async. tokio-postgres has no built-in TLS — without this, any
/// server that requires SSL (e.g. Fly Postgres) hangs the handshake.
async fn establish_tls(url: String) -> diesel::ConnectionResult<AsyncPgConnection> {
    info!("establish_tls: tokio_postgres::connect starting");
    let tls = MakeRustlsConnect::new((*tls_config()).clone());
    let (client, conn) = tokio_postgres::connect(&url, tls)
        .await
        .map_err(|e| {
            error!(error = %e, "tokio_postgres::connect failed");
            diesel::ConnectionError::BadConnection(e.to_string())
        })?;
    info!("establish_tls: tokio_postgres::connect ok");
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
