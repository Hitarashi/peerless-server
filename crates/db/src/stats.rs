use diesel::prelude::*;
use diesel_async::RunQueryDsl;

use crate::{DbError, DbPool, schema::tracks};

#[derive(Debug, Clone)]
pub struct AlacStats {
    pub total_cached_tracks: i64,
}

#[derive(Clone)]
pub struct StatsRepository {
    pool: DbPool,
}

impl StatsRepository {
    pub fn new(pool: DbPool) -> Self {
        Self { pool }
    }

    pub async fn get_stats(&self) -> Result<AlacStats, DbError> {
        let mut connection = self.pool.connection().await?;
        let total_cached_tracks: i64 = tracks::table.count().get_result(&mut *connection).await?;
        Ok(AlacStats {
            total_cached_tracks,
        })
    }
}
