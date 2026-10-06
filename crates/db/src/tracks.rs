use std::{
    borrow::Borrow,
    collections::{HashMap, HashSet},
};

use diesel::{dsl::now, prelude::*};
use diesel_async::RunQueryDsl;
use engine::orchestrator::deps::{CachedTrack, SaveTrackInput};
use music::Codec;

use crate::{DbError, DbPool, Track, models::NewTrack, schema::tracks};

fn cached_track(track: Track) -> CachedTrack {
    CachedTrack {
        track_id: track.track_id,
        codec: track.codec,
        message_id: i64::from(track.message_id),
        file_id: track.file_id,
        file_unique_id: track.file_unique_id,
    }
}

#[derive(Clone)]
pub struct TracksRepository {
    pool: DbPool,
}

impl TracksRepository {
    pub fn new(pool: DbPool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &DbPool {
        &self.pool
    }

    pub async fn find_cached_tracks(
        &self,
        track_ids: &[String],
    ) -> Result<HashMap<(String, Codec), CachedTrack>, DbError> {
        let unique_ids: Vec<String> = track_ids
            .iter()
            .filter(|id| !id.is_empty())
            .cloned()
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        if unique_ids.is_empty() {
            return Ok(HashMap::new());
        }
        let mut connection = self.pool.connection().await?;
        let rows = tracks::table
            .filter(tracks::track_id.eq_any(unique_ids))
            .select(Track::as_select())
            .load::<Track>(&mut *connection)
            .await?;
        let mut map = HashMap::new();
        for track in rows {
            let cached = cached_track(track);
            map.insert((cached.track_id.clone(), cached.codec), cached);
        }
        Ok(map)
    }

    pub async fn find_track_by_file_unique_id(
        &self,
        file_unique_id: &str,
    ) -> Result<Option<Track>, DbError> {
        let mut connection = self.pool.connection().await?;
        Ok(tracks::table
            .filter(tracks::file_unique_id.eq(file_unique_id))
            .select(Track::as_select())
            .first::<Track>(&mut *connection)
            .await
            .optional()?)
    }

    pub async fn find_track_by_id(&self, id: i32) -> Result<Option<Track>, DbError> {
        let mut connection = self.pool.connection().await?;
        Ok(tracks::table
            .filter(tracks::id.eq(id))
            .select(Track::as_select())
            .first::<Track>(&mut *connection)
            .await
            .optional()?)
    }

    pub async fn get_track_by_apple_id(&self, track_id: &str) -> Result<Option<Track>, DbError> {
        let mut connection = self.pool.connection().await?;
        Ok(tracks::table
            .filter(tracks::track_id.eq(track_id))
            .order(tracks::id.desc())
            .select(Track::as_select())
            .first::<Track>(&mut *connection)
            .await
            .optional()?)
    }

    pub async fn find_track_formats(
        &self,
        track_ids: &[String],
    ) -> Result<Vec<(i32, String, Codec)>, DbError> {
        if track_ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut connection = self.pool.connection().await?;
        Ok(tracks::table
            .filter(tracks::track_id.eq_any(track_ids.to_vec()))
            .select((tracks::id, tracks::track_id, tracks::codec))
            .load::<(i32, String, Codec)>(&mut *connection)
            .await?)
    }

    pub async fn find_latest_track(&self) -> Result<Option<Track>, DbError> {
        let mut connection = self.pool.connection().await?;
        Ok(tracks::table
            .order(tracks::created_at.desc())
            .select(Track::as_select())
            .first::<Track>(&mut *connection)
            .await
            .optional()?)
    }

    pub async fn save_track<I>(&self, input: I) -> Result<Track, DbError>
    where
        I: Borrow<SaveTrackInput>,
    {
        let input = input.borrow();
        let message_id = i32::try_from(input.message_id)
            .map_err(|error| DbError::Row(format!("message_id out of range: {error}")))?;
        let mut connection = self.pool.connection().await?;
        let new_track = NewTrack {
            track_id: &input.track_id,
            codec: input.codec,
            message_id,
            file_id: &input.file_id,
            file_unique_id: &input.file_unique_id,
        };
        diesel::insert_into(tracks::table)
            .values(new_track)
            .on_conflict((tracks::track_id, tracks::codec))
            .do_update()
            .set((
                tracks::message_id.eq(message_id),
                tracks::file_id.eq(&input.file_id),
                tracks::file_unique_id.eq(&input.file_unique_id),
                tracks::updated_at.eq(now),
            ))
            .execute(&mut *connection)
            .await?;
        tracks::table
            .filter(tracks::track_id.eq(&input.track_id))
            .filter(tracks::codec.eq(input.codec))
            .select(Track::as_select())
            .first::<Track>(&mut *connection)
            .await
            .map_err(DbError::from)
    }

    pub async fn delete_track(
        &self,
        track_id: &str,
        codec: Option<Codec>,
    ) -> Result<bool, DbError> {
        let mut connection = self.pool.connection().await?;
        let mut query = diesel::delete(tracks::table)
            .filter(tracks::track_id.eq(track_id))
            .into_boxed();
        if let Some(codec) = codec {
            query = query.filter(tracks::codec.eq(codec));
        }
        Ok(query.execute(&mut *connection).await? > 0)
    }

    pub async fn get_all_track_ids(&self) -> Result<Vec<(String, Codec)>, DbError> {
        let mut connection = self.pool.connection().await?;
        let rows = tracks::table
            .select((tracks::track_id, tracks::codec))
            .load::<(String, Codec)>(&mut *connection)
            .await?;
        Ok(rows)
    }

    pub async fn delete_tracks_not_in(
        &self,
        valid_tracks: &[(String, Codec)],
    ) -> Result<u64, DbError> {
        let mut connection = self.pool.connection().await?;
        let valid: HashSet<_> = valid_tracks.iter().cloned().collect();
        connection
            .build_transaction()
            .run(async |transaction| -> Result<u64, diesel::result::Error> {
                let rows = tracks::table
                    .select((tracks::id, tracks::track_id, tracks::codec))
                    .load::<(i32, String, Codec)>(&mut *transaction)
                    .await?;
                let stale_ids: Vec<i32> = rows
                    .into_iter()
                    .filter_map(|(id, track_id, codec)| {
                        (!valid.contains(&(track_id, codec))).then_some(id)
                    })
                    .collect();
                if stale_ids.is_empty() {
                    return Ok(0_u64);
                }
                let deleted = diesel::delete(tracks::table.filter(tracks::id.eq_any(stale_ids)))
                    .execute(&mut *transaction)
                    .await?;
                Ok(deleted as u64)
            })
            .await
            .map_err(DbError::from)
    }

    pub async fn find_all_by_track_id(&self, track_id: &str) -> Result<Vec<Track>, DbError> {
        let mut connection = self.pool.connection().await?;
        Ok(tracks::table
            .filter(tracks::track_id.eq(track_id))
            .order(tracks::id.asc())
            .select(Track::as_select())
            .load::<Track>(&mut *connection)
            .await?)
    }
}
