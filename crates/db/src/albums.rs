use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use engine::orchestrator::deps::{
    AlbumReplacementExpectation, AlbumReplacementResult, AlbumUpload,
};
use music::{Codec, Provider};

use crate::{
    DbError, DbPool,
    models::{Album, NewAlbum},
    schema::albums,
};

/// Database repository for cached album archive parts.
#[derive(Clone)]
pub struct AlbumsRepository {
    pool: DbPool,
}

impl AlbumsRepository {
    pub fn new(pool: DbPool) -> Self {
        Self { pool }
    }

    /// Finds all archive parts for a given album, ordered by part_index.
    pub async fn find_albums(
        &self,
        provider: Provider,
        album_id: &str,
        codec: Option<Codec>,
    ) -> Result<Vec<Album>, DbError> {
        let mut connection = self.pool.connection().await?;
        let mut query = albums::table
            .filter(albums::provider.eq(provider))
            .filter(albums::album_id.eq(album_id))
            .order(albums::part_index.asc())
            .into_boxed();
        if let Some(c) = codec {
            query = query.filter(albums::codec.eq(c));
        }
        let rows = query
            .select(Album::as_select())
            .load::<Album>(&mut *connection)
            .await?;
        Ok(rows)
    }

    /// Finds a single album archive record by Telegram file unique id.
    pub async fn find_by_file_unique_id(
        &self,
        file_unique_id: &str,
    ) -> Result<Option<Album>, DbError> {
        let mut connection = self.pool.connection().await?;
        Ok(albums::table
            .filter(albums::file_unique_id.eq(file_unique_id))
            .select(Album::as_select())
            .first::<Album>(&mut *connection)
            .await
            .optional()?)
    }

    /// Inserts or updates an album archive part record.
    pub async fn save_album(&self, input: &NewAlbum<'_>) -> Result<Album, DbError> {
        let mut connection = self.pool.connection().await?;
        diesel::insert_into(albums::table)
            .values(input)
            .on_conflict((
                albums::provider,
                albums::album_id,
                albums::codec,
                albums::part_index,
            ))
            .do_update()
            .set((
                albums::total_parts.eq(input.total_parts),
                albums::message_id.eq(input.message_id),
                albums::file_id.eq(input.file_id),
                albums::file_unique_id.eq(input.file_unique_id),
                albums::file_size.eq(input.file_size),
                albums::file_name.eq(input.file_name),
                albums::generation_hash.eq(input.generation_hash),
            ))
            .execute(&mut *connection)
            .await?;

        albums::table
            .filter(albums::provider.eq(input.provider))
            .filter(albums::album_id.eq(input.album_id))
            .filter(albums::codec.eq(input.codec))
            .filter(albums::part_index.eq(input.part_index))
            .select(Album::as_select())
            .first::<Album>(&mut *connection)
            .await
            .map_err(DbError::from)
    }

    /// Replaces every archive part for one album/rendition in one database
    /// transaction. Old rows remain visible if validation or any insert
    /// fails, so callers can remove newly uploaded Telegram messages and keep
    /// serving the previous cache.
    pub async fn replace_albums(
        &self,
        provider: Provider,
        album_id: &str,
        codec: Codec,
        expected: &AlbumReplacementExpectation,
        uploads: &[AlbumUpload],
    ) -> Result<AlbumReplacementResult, DbError> {
        let new_albums = uploads
            .iter()
            .map(|upload| {
                if upload.provider != provider
                    || upload.album_id != album_id
                    || upload.codec != codec
                {
                    return Err(DbError::Row(
                        "album replacement contains a mismatched part".to_owned(),
                    ));
                }
                Ok(NewAlbum {
                    provider: upload.provider,
                    album_id: &upload.album_id,
                    codec: upload.codec,
                    part_index: upload.part_index,
                    total_parts: upload.total_parts,
                    message_id: i32::try_from(upload.message_id)
                        .map_err(|error| DbError::Row(error.to_string()))?,
                    file_id: &upload.file_id,
                    file_unique_id: &upload.file_unique_id,
                    file_size: upload.file_size,
                    file_name: &upload.file_name,
                    generation_hash: &upload.generation_hash,
                })
            })
            .collect::<Result<Vec<_>, DbError>>()?;
        let mut connection = self.pool.connection().await?;
        connection
            .build_transaction()
            .run(
                async |transaction| -> Result<AlbumReplacementResult, diesel::result::Error> {
                    // A row lock is not enough when the group is currently
                    // empty: two rebuilds could both observe no rows and then
                    // race to insert.  A transaction-scoped advisory lock gives
                    // the key a stable serialization point in both cases.
                    let group = match codec {
                        Codec::Alac | Codec::Aac => "primary",
                        Codec::Ec3 => "atmos",
                        Codec::Flac => "flac",
                    };
                    let lock_key = format!("album-zip:{}:{}:{group}", provider.as_str(), album_id);
                    diesel::sql_query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
                        .bind::<diesel::sql_types::Text, _>(lock_key)
                        .execute(&mut *transaction)
                        .await?;

                    let existing = albums::table
                        .filter(albums::provider.eq(provider))
                        .filter(albums::album_id.eq(album_id))
                        .filter(albums::codec.eq_any(match codec {
                            Codec::Alac | Codec::Aac => vec![Codec::Alac, Codec::Aac],
                            other => vec![other],
                        }))
                        .select(Album::as_select())
                        .for_update()
                        .load::<Album>(&mut *transaction)
                        .await?;

                    let matches_expected = match expected {
                        AlbumReplacementExpectation::Empty => existing.is_empty(),
                        AlbumReplacementExpectation::Generation(generation) => {
                            !existing.is_empty()
                                && existing
                                    .iter()
                                    .all(|row| row.generation_hash == *generation)
                        }
                        AlbumReplacementExpectation::Mixed => false,
                    };
                    if !matches_expected {
                        return Ok(AlbumReplacementResult::Stale);
                    }

                    let displaced_message_ids = existing
                        .iter()
                        .map(|row| i64::from(row.message_id))
                        .collect::<Vec<_>>();
                    diesel::delete(
                        albums::table
                            .filter(albums::provider.eq(provider))
                            .filter(albums::album_id.eq(album_id))
                            .filter(albums::codec.eq_any(match codec {
                                Codec::Alac | Codec::Aac => vec![Codec::Alac, Codec::Aac],
                                other => vec![other],
                            })),
                    )
                    .execute(&mut *transaction)
                    .await?;
                    if !new_albums.is_empty() {
                        diesel::insert_into(albums::table)
                            .values(&new_albums)
                            .execute(&mut *transaction)
                            .await?;
                    }
                    Ok(AlbumReplacementResult::Committed {
                        displaced_message_ids,
                    })
                },
            )
            .await
            .map_err(DbError::from)
    }

    /// Deletes all archive parts for an album, returning the deleted records.
    pub async fn delete_albums(
        &self,
        provider: Provider,
        album_id: &str,
        codec: Option<Codec>,
    ) -> Result<Vec<Album>, DbError> {
        let existing = self.find_albums(provider, album_id, codec).await?;
        if existing.is_empty() {
            return Ok(Vec::new());
        }
        let mut connection = self.pool.connection().await?;
        let mut query = diesel::delete(albums::table)
            .filter(albums::provider.eq(provider))
            .filter(albums::album_id.eq(album_id))
            .into_boxed();
        if let Some(c) = codec {
            query = query.filter(albums::codec.eq(c));
        }
        query.execute(&mut *connection).await?;
        Ok(existing)
    }

    /// Lists album archives for indexing/exporting.
    pub async fn list_albums(&self) -> Result<Vec<Album>, DbError> {
        let mut connection = self.pool.connection().await?;
        Ok(albums::table
            .order(albums::id.desc())
            .select(Album::as_select())
            .load::<Album>(&mut *connection)
            .await?)
    }
}
