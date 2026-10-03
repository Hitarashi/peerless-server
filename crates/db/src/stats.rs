use diesel::{
    sql_query,
    sql_types::{BigInt, Double, Text},
};
use diesel_async::RunQueryDsl;
use music::{Provider, TrackKey};

use crate::{DbError, DbPool};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopTrackStat {
    pub track_key: TrackKey,
    pub title: Option<String>,
    pub artist: Option<String>,
    pub request_count: i64,
}

#[derive(Debug, Clone)]
pub struct AlacStats {
    pub total_cached_tracks: i64,
    pub total_requests: i64,
    pub cache_hits: i64,
    pub cache_misses: i64,
    pub cache_hit_ratio: f64,
    pub avg_rip_duration_ms: i64,
    pub avg_cache_duration_ms: i64,
    pub total_failed_requests: i64,
    pub top_tracks: Vec<TopTrackStat>,
}

#[derive(diesel::QueryableByName)]
struct AggregateRow {
    #[diesel(sql_type = BigInt)]
    total_cached_tracks: i64,
    #[diesel(sql_type = BigInt)]
    total_requests: i64,
    #[diesel(sql_type = BigInt)]
    cache_hits: i64,
    #[diesel(sql_type = BigInt)]
    total_failed: i64,
    #[diesel(sql_type = diesel::sql_types::Nullable<Double>)]
    avg_rip: Option<f64>,
    #[diesel(sql_type = diesel::sql_types::Nullable<Double>)]
    avg_cache: Option<f64>,
}

#[derive(diesel::QueryableByName)]
struct TopTrackRow {
    #[diesel(sql_type = diesel::sql_types::VarChar)]
    provider: Provider,
    #[diesel(sql_type = Text)]
    track_id: String,
    #[diesel(sql_type = BigInt)]
    request_count: i64,
    #[diesel(sql_type = diesel::sql_types::Nullable<Text>)]
    title: Option<String>,
    #[diesel(sql_type = diesel::sql_types::Nullable<Text>)]
    artist: Option<String>,
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
        let row = sql_query("SELECT (SELECT COUNT(*) FROM tracks WHERE provider = 'apple') AS total_cached_tracks, (SELECT COUNT(*) FROM requests WHERE provider = 'apple') AS total_requests, (SELECT COUNT(*) FROM requests WHERE provider = 'apple' AND is_cache_hit) AS cache_hits, (SELECT COUNT(*) FROM requests WHERE provider = 'apple' AND status = 'failed') AS total_failed, (SELECT ROUND(AVG(duration_ms))::double precision FROM requests WHERE provider = 'apple' AND NOT is_cache_hit AND status = 'completed') AS avg_rip, (SELECT ROUND(AVG(duration_ms))::double precision FROM requests WHERE provider = 'apple' AND is_cache_hit AND status = 'completed') AS avg_cache")
            .load::<AggregateRow>(&mut *connection)
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| DbError::Row("stats query returned no rows".to_owned()))?;
        let cache_misses = row.total_requests - row.cache_hits;
        let cache_hit_ratio = if row.total_requests > 0 {
            (row.cache_hits as f64 / row.total_requests as f64 * 1000.0).round() / 10.0
        } else {
            0.0
        };
        let top_rows = sql_query("SELECT r.provider, r.track_id, COUNT(*) AS request_count, MAX(t.title) AS title, MAX(t.artist) AS artist FROM requests r LEFT JOIN tracks t ON t.provider = r.provider AND t.track_id = r.track_id WHERE r.provider = 'apple' AND r.status = 'completed' GROUP BY r.provider, r.track_id ORDER BY COUNT(*) DESC LIMIT 5")
            .load::<TopTrackRow>(&mut *connection)
            .await?;
        Ok(AlacStats {
            total_cached_tracks: row.total_cached_tracks,
            total_requests: row.total_requests,
            cache_hits: row.cache_hits,
            cache_misses,
            cache_hit_ratio,
            avg_rip_duration_ms: row.avg_rip.unwrap_or(0.0).round() as i64,
            avg_cache_duration_ms: row.avg_cache.unwrap_or(0.0).round() as i64,
            total_failed_requests: row.total_failed,
            top_tracks: top_rows
                .into_iter()
                .map(|row| TopTrackStat {
                    track_key: TrackKey::new(row.provider, row.track_id),
                    title: row.title,
                    artist: row.artist,
                    request_count: row.request_count,
                })
                .collect(),
        })
    }
}
