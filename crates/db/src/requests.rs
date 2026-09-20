use std::borrow::Borrow;

use diesel_async::RunQueryDsl;
use engine::orchestrator::deps::RequestLog;

use crate::{DbError, DbPool, models::NewRequest, schema::requests};

/// Append-only request log repository.
#[derive(Clone)]
pub struct RequestLogRepository {
    pool: DbPool,
}

impl RequestLogRepository {
    pub fn new(pool: DbPool) -> Self {
        Self { pool }
    }

    pub async fn log_request<I>(&self, data: I) -> Result<(), DbError>
    where
        I: Borrow<RequestLog>,
    {
        let data = data.borrow();
        let duration_ms = data
            .duration_ms
            .map(|value| i32::try_from(value).map_err(|error| DbError::Row(error.to_string())))
            .transpose()?;
        let mut connection = self.pool.connection().await?;
        diesel::insert_into(requests::table)
            .values(NewRequest {
                telegram_id: data.telegram_id,
                chat_id: data.chat_id,
                provider: data.track_key.provider,
                track_id: &data.track_key.track_id,
                is_cache_hit: data.is_cache_hit,
                duration_ms,
                status: &data.status,
                error_reason: data.error_reason.as_deref(),
            })
            .execute(&mut *connection)
            .await?;
        Ok(())
    }
}
