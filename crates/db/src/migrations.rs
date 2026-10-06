use diesel_async::async_connection_wrapper::AsyncConnectionWrapper;
use diesel_migrations::{EmbeddedMigrations, MigrationHarness, embed_migrations};

use crate::{DbError, DbPool, establish};

pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations");

pub async fn migrate(pool: &DbPool) -> Result<(), DbError> {
    let database_url = pool.database_url().to_owned();
    let connection = establish(&database_url)
        .await
        .map_err(|error| DbError::Migration(error.to_string()))?;
    tokio::task::spawn_blocking(move || {
        let mut connection: AsyncConnectionWrapper<diesel_async::AsyncPgConnection> =
            connection.into();
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
