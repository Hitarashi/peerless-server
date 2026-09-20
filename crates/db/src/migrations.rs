use diesel::Connection;
use diesel_async::async_connection_wrapper::AsyncConnectionWrapper;
use diesel_migrations::{EmbeddedMigrations, MigrationHarness, embed_migrations};

use crate::{DbError, DbPool};

/// Embedded Diesel migrations for this crate.
pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations");

/// Apply the embedded canonical schema migrations.
pub async fn migrate(pool: &DbPool) -> Result<(), DbError> {
    let database_url = pool.database_url().to_owned();
    tokio::task::spawn_blocking(move || {
        let mut connection =
            AsyncConnectionWrapper::<diesel_async::AsyncPgConnection>::establish(&database_url)
                .map_err(|error| DbError::Migration(error.to_string()))?;
        let applied = connection
            .run_pending_migrations(MIGRATIONS)
            .map_err(|error| DbError::Migration(error.to_string()))?;
        for version in applied {
            tracing::info!("Applied database migration: {version}");
        }
        Ok(())
    })
    .await
    .map_err(|error| DbError::Migration(error.to_string()))?
}
