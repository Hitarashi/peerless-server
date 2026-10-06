use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use music::Codec;
use peerless_core::{AlbumReplacementExpectation, AlbumReplacementResult, AlbumUpload};

use crate::{
    DbError, DbPool,
    models::{Album, NewAlbum},
    schema::albums,
};

#[derive(Clone)]
pub struct AlbumsRepository {
    pool: DbPool,
}

impl AlbumsRepository {
    pub fn new(pool: DbPool) -> Self {
        Self { pool }
    }

    pub async fn find_albums(
        &self,
        album_id: &str,
        codec: Option<Codec>,
    ) -> Result<Vec<Album>, DbError> {
        let mut connection = self.pool.connection().await?;
        let mut query = albums::table
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

    pub async fn find_album_parts_by_ids(
        &self,
        album_ids: &[String],
    ) -> Result<Vec<Album>, DbError> {
        if album_ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut connection = self.pool.connection().await?;
        Ok(albums::table
            .filter(albums::album_id.eq_any(album_ids.to_vec()))
            .order((albums::album_id.asc(), albums::part_index.asc()))
            .select(Album::as_select())
            .load::<Album>(&mut *connection)
            .await?)
    }

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

    pub async fn save_album(&self, input: &NewAlbum<'_>) -> Result<Album, DbError> {
        let mut connection = self.pool.connection().await?;
        diesel::insert_into(albums::table)
            .values(input)
            .on_conflict((albums::album_id, albums::codec, albums::part_index))
            .do_update()
            .set((
                albums::total_parts.eq(input.total_parts),
                albums::message_id.eq(input.message_id),
                albums::file_id.eq(input.file_id),
                albums::file_unique_id.eq(input.file_unique_id),
                albums::file_size.eq(input.file_size),
                albums::generation_hash.eq(input.generation_hash),
            ))
            .execute(&mut *connection)
            .await?;

        albums::table
            .filter(albums::album_id.eq(input.album_id))
            .filter(albums::codec.eq(input.codec))
            .filter(albums::part_index.eq(input.part_index))
            .select(Album::as_select())
            .first::<Album>(&mut *connection)
            .await
            .map_err(DbError::from)
    }

    pub async fn replace_albums(
        &self,
        album_id: &str,
        codec: Codec,
        expected: &AlbumReplacementExpectation,
        uploads: &[AlbumUpload],
    ) -> Result<AlbumReplacementResult, DbError> {
        let new_albums = uploads
            .iter()
            .map(|upload| {
                if upload.album_id != album_id || upload.codec != codec {
                    return Err(DbError::Row(
                        "album replacement contains a mismatched part".to_owned(),
                    ));
                }
                Ok(NewAlbum {
                    album_id: &upload.album_id,
                    codec: upload.codec,
                    part_index: upload.part_index,
                    total_parts: upload.total_parts,
                    message_id: i32::try_from(upload.message_id)
                        .map_err(|error| DbError::Row(error.to_string()))?,
                    file_id: &upload.file_id,
                    file_unique_id: &upload.file_unique_id,
                    file_size: upload.file_size,
                    generation_hash: &upload.generation_hash,
                })
            })
            .collect::<Result<Vec<_>, DbError>>()?;
        let mut connection = self.pool.connection().await?;
        connection
            .build_transaction()
            .run(
                async |transaction| -> Result<AlbumReplacementResult, diesel::result::Error> {
                    let group = match codec {
                        Codec::Alac | Codec::Aac => "primary",
                        Codec::Ec3 => "atmos",
                    };
                    let lock_key = format!("album-zip:{album_id}:{group}");
                    diesel::sql_query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
                        .bind::<diesel::sql_types::Text, _>(lock_key)
                        .execute(&mut *transaction)
                        .await?;

                    let existing = albums::table
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
                    diesel::delete(albums::table.filter(albums::album_id.eq(album_id)).filter(
                        albums::codec.eq_any(match codec {
                            Codec::Alac | Codec::Aac => vec![Codec::Alac, Codec::Aac],
                            other => vec![other],
                        }),
                    ))
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

    pub async fn delete_albums(
        &self,
        album_id: &str,
        codec: Option<Codec>,
    ) -> Result<Vec<Album>, DbError> {
        let existing = self.find_albums(album_id, codec).await?;
        if existing.is_empty() {
            return Ok(Vec::new());
        }
        let mut connection = self.pool.connection().await?;
        let mut query = diesel::delete(albums::table)
            .filter(albums::album_id.eq(album_id))
            .into_boxed();
        if let Some(c) = codec {
            query = query.filter(albums::codec.eq(c));
        }
        query.execute(&mut *connection).await?;
        Ok(existing)
    }

    pub async fn list_albums(&self) -> Result<Vec<Album>, DbError> {
        let mut connection = self.pool.connection().await?;
        Ok(albums::table
            .order(albums::id.desc())
            .select(Album::as_select())
            .load::<Album>(&mut *connection)
            .await?)
    }
}
