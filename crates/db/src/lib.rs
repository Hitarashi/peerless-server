//! Database access for the bot.

use std::{
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use diesel_async::{AsyncPgConnection, RunQueryDsl, pooled_connection::bb8::Pool};
use futures_util::FutureExt as _;
use rustls_platform_verifier::ConfigVerifierExt as _;

mod albums;
mod auth;
mod migrations;
mod models;
mod requests;
mod schema;
mod session;
mod settings;
mod tracks;
mod worker_session;

pub mod crypto;
pub mod dump;
pub mod integrations;
mod stats;

pub use albums::AlbumsRepository;
pub use auth::{Auth, AuthedPeer};
pub use crypto::CryptoCipher;
pub use dump::{DbDumpService, DumpStats, RestoreStats};
pub use engine::orchestrator::deps::{CachedTrack, RequestLog, SaveTrackInput};
pub use migrations::migrate;
pub use models::{
    Album, NewAlbum, NewUserIntegration, OneTimeAuthCode, Request, SettingsRow, TgWorkerSession,
    Track, User, UserIntegration, UserSession,
};
pub use music::{Provider, TrackKey};
pub use requests::RequestLogRepository;
pub use session::{ClientMetadata, SessionIdentity, SessionManager, SessionTokens, hash_token};
pub use settings::SettingsStore;
pub use stats::{AlacStats, StatsRepository, TopTrackStat};
pub use tracks::{AlbumArtist, TracksRepository};
pub use worker_session::WorkerSessionStore;

type DieselManager =
    diesel_async::pooled_connection::AsyncDieselConnectionManager<AsyncPgConnection>;
type DieselPool = Pool<AsyncPgConnection>;

/// A cloneable async PostgreSQL pool. The URL is retained solely so Diesel's
/// migration harness can run in its required blocking wrapper.
#[derive(Clone)]
pub struct DbPool {
    pool: DieselPool,
    database_url: Arc<str>,
}

impl DbPool {
    pub async fn connection(
        &self,
    ) -> Result<
        diesel_async::pooled_connection::bb8::PooledConnection<'_, AsyncPgConnection>,
        DbError,
    > {
        self.pool
            .get()
            .await
            .map_err(|error| DbError::Pool(error.to_string()))
    }

    /// Cheap liveness check: run `SELECT 1` on a pooled connection.
    /// Returns `Ok(())` when the database answers, `Err(DbError)` otherwise.
    ///
    /// Exists so health probes outside this crate can assert that PostgreSQL is
    /// still serving without ever taking a direct Diesel dependency: the `db`
    /// crate owns all Diesel usage, and every other crate reaches the database
    /// only through this API.
    ///
    /// Deliberately has no timeout of its own. The caller (the server's
    /// liveness probe) already bounds the future with `tokio::time::timeout`;
    /// a nested timeout here would silently discard a slow-but-healthy
    /// database and make the two bounds impossible to reason about.
    pub async fn ping(&self) -> Result<(), DbError> {
        let mut connection = self.connection().await?;
        diesel::sql_query("SELECT 1")
            .execute(&mut *connection)
            .await
            .map(|_| ())
            .map_err(DbError::from)
    }

    pub(crate) fn database_url(&self) -> &str {
        &self.database_url
    }
}

/// Open a single connection, honouring the URL's `sslmode`.
///
/// `AsyncPgConnection::establish` hardcodes `tokio_postgres::NoTls`
/// (diesel-async 0.9 `pg/mod.rs`), so it can never reach a host that requires
/// TLS -- Neon is one, and it answers a plaintext startup packet by dropping it,
/// which surfaces as a pool timeout rather than a TLS error. When `sslmode`
/// asks for TLS we do the handshake ourselves with rustls and hand the finished
/// client to diesel; otherwise we keep the plaintext path so local development
/// against a bare `localhost:5432` still works.
pub async fn establish(database_url: &str) -> Result<AsyncPgConnection, diesel::ConnectionError> {
    let config = tokio_postgres::Config::from_str(database_url)
        .map_err(|e| diesel::ConnectionError::InvalidConnectionUrl(e.to_string()))?;

    let use_tls = config.get_ssl_mode() == tokio_postgres::config::SslMode::Require;

    // The two arms produce different `Connection` stream types, so they cannot be
    // unified in one expression; each path ends with the same wrap step.
    if use_tls {
        // rustls 0.23 can only pick a provider automatically when exactly one of
        // `ring`/`aws-lc-rs` is compiled in. This workspace enables both (our own
        // dependency and reqwest's rustls path), so `ClientConfig::builder()`
        // panics with "Could not automatically determine the process-level
        // CryptoProvider". Installing ring as the process default resolves it;
        // this returns Err when a provider is already installed, which is fine.
        let _ = rustls::crypto::ring::default_provider().install_default();

        // `with_platform_verifier` is what diesel_async's own rustls examples
        // use, and reads the OS trust store.
        let tls = tokio_postgres_rustls::MakeRustlsConnect::new(
            rustls::ClientConfig::with_platform_verifier(),
        );
        let (client, connection) = config
            .connect(tls)
            .await
            .map_err(|e| diesel::ConnectionError::BadConnection(e.to_string()))?;
        return AsyncPgConnection::try_from_client_and_connection(client, connection).await;
    }

    let (client, connection) = tokio_postgres::connect(database_url, tokio_postgres::NoTls)
        .await
        .map_err(|e| diesel::ConnectionError::BadConnection(e.to_string()))?;
    AsyncPgConnection::try_from_client_and_connection(client, connection).await
}

/// Establish the shared Diesel async pool.
pub async fn connect(database_url: &str) -> Result<DbPool, DbError> {
    let mut config = diesel_async::pooled_connection::ManagerConfig::default();
    config.custom_setup = Box::new(|url| async move { establish(url).await }.boxed());
    let manager = DieselManager::new_with_config(database_url, config);
    let pool = Pool::builder()
        .build(manager)
        .await
        .map_err(|error| DbError::Pool(error.to_string()))?;
    let database = DbPool {
        pool,
        database_url: Arc::from(database_url.to_owned()),
    };
    // Fail during application bootstrap, not on the first repository call.
    // The pool still performs normal health checks for subsequent requests.
    let _ = database.connection().await?;
    Ok(database)
}

/// Connect a database integration test to its own PostgreSQL schema. Tests
/// must opt in explicitly with TEST_DATABASE_URL; production DATABASE_URL is
/// deliberately never used as a test fallback.
pub async fn connect_test_isolated() -> Result<DbPool, DbError> {
    let base_url = std::env::var("TEST_DATABASE_URL")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| DbError::Row("TEST_DATABASE_URL is required for database tests".into()))?;
    let base = connect(&base_url).await?;
    static TEST_SCHEMA_SEQUENCE: AtomicU64 = AtomicU64::new(0);
    let sequence = TEST_SCHEMA_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let schema = format!(
        "test_{}_{}_{}",
        std::process::id(),
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default(),
        sequence
    );
    let mut connection = base.connection().await?;
    diesel::sql_query(format!("CREATE SCHEMA {schema}"))
        .execute(&mut *connection)
        .await?;
    drop(connection);
    let separator = if base_url.contains('?') { '&' } else { '?' };
    let isolated_url = format!("{base_url}{separator}options=-c%20search_path%3D{schema}%2Cpublic");
    connect(&isolated_url).await
}

/// Errors returned by the persistence layer.
#[derive(Debug, thiserror::Error)]
pub enum DbError {
    #[error("database error: {0}")]
    Database(#[from] diesel::result::Error),
    #[error("database pool error: {0}")]
    Pool(String),
    #[error("migration error: {0}")]
    Migration(String),
    #[error("database row error: {0}")]
    Row(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("unauthorized: {0}")]
    Unauthorized(String),
    #[error("validation error: {0}")]
    Validation(String),
}

impl From<Box<dyn std::error::Error + Send + Sync>> for DbError {
    fn from(error: Box<dyn std::error::Error + Send + Sync>) -> Self {
        Self::Row(error.to_string())
    }
}
