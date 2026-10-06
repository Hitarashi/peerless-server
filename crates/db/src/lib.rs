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
pub use engine::orchestrator::deps::{CachedTrack, SaveTrackInput};
pub use migrations::migrate;
pub use models::{
    Album, NewAlbum, NewUserIntegration, OneTimeAuthCode, SettingsRow, TgWorkerSession, Track,
    User, UserIntegration, UserSession,
};
pub use music::Provider;
pub use session::{ClientMetadata, SessionIdentity, SessionManager, SessionTokens, hash_token};
pub use settings::SettingsStore;
pub use stats::{AlacStats, StatsRepository};
pub use tracks::TracksRepository;
pub use worker_session::WorkerSessionStore;

type DieselManager =
    diesel_async::pooled_connection::AsyncDieselConnectionManager<AsyncPgConnection>;
type DieselPool = Pool<AsyncPgConnection>;

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

pub async fn establish(database_url: &str) -> Result<AsyncPgConnection, diesel::ConnectionError> {
    let config = tokio_postgres::Config::from_str(database_url)
        .map_err(|e| diesel::ConnectionError::InvalidConnectionUrl(e.to_string()))?;

    let use_tls = config.get_ssl_mode() == tokio_postgres::config::SslMode::Require;

    if use_tls {
        let _ = rustls::crypto::ring::default_provider().install_default();

        let tls = tokio_postgres_rustls::MakeRustlsConnect::new(
            rustls::ClientConfig::with_platform_verifier()
                .map_err(|e| diesel::ConnectionError::BadConnection(e.to_string()))?,
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

    let _ = database.connection().await?;
    Ok(database)
}

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
