use diesel::prelude::*;
use diesel_async::RunQueryDsl;

use crate::{
    DbError, DbPool,
    models::{NewUserIntegration, UserIntegration},
    schema::user_integrations,
};

/// Upsert a user's third-party integration credentials (e.g. Last.fm session).
pub async fn save_integration(
    pool: &DbPool,
    telegram_id: i64,
    provider: &str,
    username: &str,
    encrypted_session_key: &str,
) -> Result<(), DbError> {
    let mut conn = pool.connection().await?;
    let now = chrono::Utc::now();
    let record = NewUserIntegration {
        telegram_id,
        provider,
        username,
        encrypted_session_key,
        updated_at: now,
    };

    diesel::insert_into(user_integrations::table)
        .values(&record)
        .on_conflict((user_integrations::telegram_id, user_integrations::provider))
        .do_update()
        .set((
            user_integrations::username.eq(username),
            user_integrations::encrypted_session_key.eq(encrypted_session_key),
            user_integrations::updated_at.eq(now),
        ))
        .execute(&mut *conn)
        .await?;

    Ok(())
}

/// Retrieve integration details for a given user and provider.
pub async fn get_integration(
    pool: &DbPool,
    telegram_id: i64,
    provider: &str,
) -> Result<Option<UserIntegration>, DbError> {
    let mut conn = pool.connection().await?;
    user_integrations::table
        .filter(user_integrations::telegram_id.eq(telegram_id))
        .filter(user_integrations::provider.eq(provider))
        .select(UserIntegration::as_select())
        .first(&mut *conn)
        .await
        .optional()
        .map_err(Into::into)
}

/// Check if a user has connected a specific integration provider.
pub async fn has_integration(
    pool: &DbPool,
    telegram_id: i64,
    provider: &str,
) -> Result<bool, DbError> {
    let mut conn = pool.connection().await?;
    let count: i64 = user_integrations::table
        .filter(user_integrations::telegram_id.eq(telegram_id))
        .filter(user_integrations::provider.eq(provider))
        .count()
        .get_result(&mut *conn)
        .await?;
    Ok(count > 0)
}

/// Delete an integration record for a user and provider.
pub async fn delete_integration(
    pool: &DbPool,
    telegram_id: i64,
    provider: &str,
) -> Result<(), DbError> {
    let mut conn = pool.connection().await?;
    diesel::delete(
        user_integrations::table
            .filter(user_integrations::telegram_id.eq(telegram_id))
            .filter(user_integrations::provider.eq(provider)),
    )
    .execute(&mut *conn)
    .await?;
    Ok(())
}
