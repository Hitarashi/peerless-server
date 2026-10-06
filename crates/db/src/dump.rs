use std::{io::Write, time::Instant};

use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use flate2::{Compression, write::GzEncoder};
use music::Codec;
use peerless_core::MAX_DOCUMENT_BYTES;
use serde::{Deserialize, Serialize};

use crate::{
    Album, DbError, DbPool, SettingsRow, Track, User,
    schema::{albums, settings, tracks, users},
};

const ARCHIVE_VERSION: u32 = 1;
const MAX_ARCHIVE_ROWS: usize = 1_000_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DumpStats {
    pub users_count: i64,
    pub tracks_count: i64,
    pub bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoreStats {
    pub users_merged: u64,
    pub tracks_merged: u64,
    pub duration_ms: u128,
}

#[derive(Debug, Serialize, Deserialize)]
struct Archive {
    format_version: u32,
    generated_at: String,

    #[serde(default)]
    dump_channel_id: Option<i64>,
    users: Vec<UserArchive>,
    tracks: Vec<TrackArchive>,
    #[serde(default, alias = "album_zips")]
    albums: Vec<AlbumArchive>,
    settings: SettingsArchive,
}

#[derive(Debug, Serialize, Deserialize, Insertable)]
#[diesel(table_name = users)]
struct UserArchive {
    telegram_id: i64,
    name: Option<String>,
    created_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Serialize, Deserialize, Insertable)]
#[diesel(table_name = tracks)]
struct TrackArchive {
    track_id: String,
    #[serde(default)]
    codec: Codec,
    message_id: i32,
    file_id: String,
    file_unique_id: String,
    created_at: chrono::DateTime<chrono::Utc>,
    updated_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Serialize, Deserialize, Insertable)]
#[diesel(table_name = albums)]
struct AlbumArchive {
    album_id: String,
    #[serde(default)]
    codec: Codec,
    part_index: i32,
    total_parts: i32,
    message_id: i32,
    file_id: String,
    file_unique_id: String,
    file_size: i64,

    #[serde(default)]
    generation_hash: String,
    created_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Serialize, Deserialize)]
struct SettingsArchive {
    data: serde_json::Value,
    updated_at: chrono::DateTime<chrono::Utc>,
}

impl From<User> for UserArchive {
    fn from(row: User) -> Self {
        Self {
            telegram_id: row.telegram_id,
            name: row.name,
            created_at: row.created_at,
        }
    }
}

impl From<Track> for TrackArchive {
    fn from(row: Track) -> Self {
        Self {
            track_id: row.track_id,
            codec: row.codec,
            message_id: row.message_id,
            file_id: row.file_id,
            file_unique_id: row.file_unique_id,
            created_at: row.created_at,
            updated_at: row.updated_at,
        }
    }
}

impl From<Album> for AlbumArchive {
    fn from(row: Album) -> Self {
        Self {
            album_id: row.album_id,
            codec: row.codec,
            part_index: row.part_index,
            total_parts: row.total_parts,
            message_id: row.message_id,
            file_id: row.file_id,
            file_unique_id: row.file_unique_id,
            file_size: row.file_size,
            generation_hash: row.generation_hash,
            created_at: row.created_at,
        }
    }
}

impl From<SettingsRow> for SettingsArchive {
    fn from(row: SettingsRow) -> Self {
        Self {
            data: row.data,
            updated_at: row.updated_at,
        }
    }
}

pub struct DbDumpService {
    pool: DbPool,
}

impl DbDumpService {
    pub fn new(pool: DbPool) -> Self {
        Self { pool }
    }

    pub async fn export_dump(&self) -> Result<(Vec<u8>, DumpStats, String), DbError> {
        self.export_dump_with_channel(None).await
    }

    pub async fn export_dump_for_channel(
        &self,
        dump_channel_id: i64,
    ) -> Result<(Vec<u8>, DumpStats, String), DbError> {
        self.export_dump_with_channel(Some(dump_channel_id)).await
    }

    async fn export_dump_with_channel(
        &self,
        dump_channel_id: Option<i64>,
    ) -> Result<(Vec<u8>, DumpStats, String), DbError> {
        let started = Instant::now();
        let mut connection = self.pool.connection().await?;
        let users = users::table
            .select(User::as_select())
            .load::<User>(&mut *connection)
            .await?;
        let tracks = tracks::table
            .select(Track::as_select())
            .load::<Track>(&mut *connection)
            .await?;
        let albums = albums::table
            .select(Album::as_select())
            .load::<Album>(&mut *connection)
            .await?;
        let settings = settings::table
            .select(SettingsRow::as_select())
            .first::<SettingsRow>(&mut *connection)
            .await?;
        let archive = Archive {
            format_version: ARCHIVE_VERSION,
            generated_at: chrono::Utc::now().to_rfc3339(),
            dump_channel_id,
            users: users.into_iter().map(UserArchive::from).collect(),
            tracks: tracks.into_iter().map(TrackArchive::from).collect(),
            albums: albums.into_iter().map(AlbumArchive::from).collect(),
            settings: SettingsArchive::from(settings),
        };
        let json = serde_json::to_vec(&archive)
            .map_err(|error| DbError::Row(format!("archive encode failed: {error}")))?;
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder
            .write_all(&json)
            .map_err(|error| DbError::Row(format!("gzip encode failed: {error}")))?;
        let compressed = encoder
            .finish()
            .map_err(|error| DbError::Row(format!("gzip finish failed: {error}")))?;
        let filename = format!(
            "alac_dump_{}.json.gz",
            chrono::Utc::now().format("%Y-%m-%dT%H-%M-%S")
        );
        let stats = DumpStats {
            users_count: archive.users.len() as i64,
            tracks_count: archive.tracks.len() as i64,
            bytes: compressed.len(),
        };
        tracing::info!(
            users = stats.users_count,
            tracks = stats.tracks_count,
            elapsed_ms = started.elapsed().as_millis(),
            "database archive exported"
        );
        Ok((compressed, stats, filename))
    }

    pub async fn import_dump(&self, gzip_bytes: &[u8]) -> Result<RestoreStats, DbError> {
        self.import_dump_with_channel(gzip_bytes, None).await
    }

    pub async fn import_dump_for_channel(
        &self,
        gzip_bytes: &[u8],
        dump_channel_id: i64,
    ) -> Result<RestoreStats, DbError> {
        self.import_dump_with_channel(gzip_bytes, Some(dump_channel_id))
            .await
    }

    async fn import_dump_with_channel(
        &self,
        gzip_bytes: &[u8],
        expected_channel_id: Option<i64>,
    ) -> Result<RestoreStats, DbError> {
        let started = Instant::now();
        if gzip_bytes.len() as u64 > MAX_DOCUMENT_BYTES {
            return Err(DbError::Row(
                "database archive exceeds size limit".to_owned(),
            ));
        }
        let archive: Archive = serde_json::from_slice(&gunzip(gzip_bytes)?)
            .map_err(|error| DbError::Row(format!("invalid archive: {error}")))?;
        if archive.format_version != ARCHIVE_VERSION {
            return Err(DbError::Row(format!(
                "unsupported archive version {}",
                archive.format_version
            )));
        }
        if let Some(expected) = expected_channel_id
            && archive.dump_channel_id != Some(expected)
        {
            return Err(DbError::Row(
                "database archive belongs to a different dump channel".to_owned(),
            ));
        }
        validate_archive_limits(&archive)?;
        let users_merged = archive.users.len() as u64;
        let tracks_merged = archive.tracks.len() as u64;
        let mut connection = self.pool.connection().await?;
        connection
            .build_transaction()
            .run(async |transaction| {
                diesel::delete(tracks::table)
                    .execute(&mut *transaction)
                    .await?;
                diesel::delete(albums::table)
                    .execute(&mut *transaction)
                    .await?;
                diesel::delete(users::table)
                    .execute(&mut *transaction)
                    .await?;

                for row in archive.users {
                    diesel::insert_into(users::table)
                        .values(row)
                        .execute(&mut *transaction)
                        .await?;
                }
                for row in archive.tracks {
                    diesel::insert_into(tracks::table)
                        .values(row)
                        .execute(&mut *transaction)
                        .await?;
                }
                for row in archive.albums {
                    diesel::insert_into(albums::table)
                        .values(row)
                        .execute(&mut *transaction)
                        .await?;
                }
                diesel::update(settings::table.filter(settings::id.eq(1_i16)))
                    .set((
                        settings::data.eq(archive.settings.data),
                        settings::updated_at.eq(archive.settings.updated_at),
                    ))
                    .execute(&mut *transaction)
                    .await?;
                Ok::<(), DbError>(())
            })
            .await?;

        tracing::info!(
            users = users_merged,
            tracks = tracks_merged,
            elapsed_ms = started.elapsed().as_millis(),
            "database archive restored"
        );
        Ok(RestoreStats {
            users_merged,
            tracks_merged,
            duration_ms: started.elapsed().as_millis(),
        })
    }
}

fn gunzip(bytes: &[u8]) -> Result<Vec<u8>, DbError> {
    use std::io::Read;
    let mut decoder = flate2::read::GzDecoder::new(bytes);
    let mut out = Vec::new();
    decoder
        .read_to_end(&mut out)
        .map_err(|error| DbError::Row(format!("gzip decode failed: {error}")))?;
    Ok(out)
}

fn validate_archive_limits(archive: &Archive) -> Result<(), DbError> {
    let row_count = archive.users.len() + archive.tracks.len() + archive.albums.len() + 1;
    if row_count > MAX_ARCHIVE_ROWS {
        return Err(DbError::Row("archive exceeds maximum row limit".to_owned()));
    }
    Ok(())
}
