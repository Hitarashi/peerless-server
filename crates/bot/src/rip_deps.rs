//! Production composition of the engine's orchestration capabilities.

use std::{collections::HashMap, sync::Arc};

use diesel::result::{DatabaseErrorKind, Error as DieselError};
use engine::{
    Codec, Provider,
    orchestrator::deps::{
        AlbumCache, AlbumCacheError, AlbumCacheOperation, AlbumReplacementExpectation,
        AlbumReplacementResult, AlbumUpload, BoxFuture, CachedAlbum, CachedTrack, ChatDelivery,
        Delivery, DeliveryError, DumpMessageRef, DumpPublish, JobBookkeeping, JobBookkeepingError,
        JobBookkeepingOperation, OrchestratorConfig, ProviderAccess, RequestLog, SaveTrackInput,
        TrackCache, TrackCacheError, TrackCacheOperation,
    },
    ripper::RipperConfig,
    settings::BotSettings,
    types::TrackKey,
};
use lyrics::LyricsRegistry;

use crate::{providers::ProviderRegistry, telegram_sink::FerogramTelegramSink};

/// Read the retry settings shared by the ripper and the upload lane.
fn retry_values() -> (u64, u32) {
    let retry_base_ms = std::env::var("ALAC_RETRY_BASE_MS")
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value <= engine::limits::MAX_RETRY_BASE_MS)
        .unwrap_or(2000);
    let max_retries = std::env::var("ALAC_MAX_RETRIES")
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value <= engine::limits::MAX_RETRIES)
        .unwrap_or(3);
    (retry_base_ms, max_retries)
}

/// Build the engine-owned orchestrator configuration from production settings.
pub fn orchestrator_config() -> OrchestratorConfig {
    let (upload_retry_base_ms, upload_max_retries) = retry_values();
    OrchestratorConfig {
        upload_retry_base_ms,
        upload_max_retries,
        ..OrchestratorConfig::default()
    }
}

fn db_is_unavailable(error: &db::DbError) -> bool {
    matches!(
        error,
        db::DbError::Pool(_)
            | db::DbError::Database(DieselError::DatabaseError(
                DatabaseErrorKind::ClosedConnection,
                _,
            ))
    )
}

fn map_album_replacement(
    result: AlbumReplacementResult,
) -> Result<AlbumReplacementResult, AlbumCacheError> {
    match result {
        AlbumReplacementResult::Stale => Err(AlbumCacheError::conflict(
            AlbumCacheOperation::Replace,
            "album replacement lost compare-and-swap race",
        )),
        committed => Ok(committed),
    }
}

fn track_cache_error(operation: TrackCacheOperation, error: db::DbError) -> TrackCacheError {
    let detail = error.to_string();
    if db_is_unavailable(&error) {
        TrackCacheError::unavailable(operation, detail)
    } else {
        TrackCacheError::failed(operation, detail)
    }
}

fn album_cache_error(operation: AlbumCacheOperation, error: db::DbError) -> AlbumCacheError {
    let detail = error.to_string();
    if db_is_unavailable(&error) {
        AlbumCacheError::unavailable(operation, detail)
    } else {
        AlbumCacheError::failed(operation, detail)
    }
}

fn bookkeeping_error(
    operation: JobBookkeepingOperation,
    error: db::DbError,
) -> JobBookkeepingError {
    let detail = error.to_string();
    if db_is_unavailable(&error) {
        JobBookkeepingError::unavailable(operation, detail)
    } else {
        JobBookkeepingError::failed(operation, detail)
    }
}

/// All environment-owned production dependencies used by the orchestrator.
pub struct RipDeps {
    sink: FerogramTelegramSink,
    albums: db::AlbumsRepository,
    tracks: db::TracksRepository,
    requests: db::RequestLogRepository,
    settings: db::SettingsStore,
    providers: ProviderRegistry,
    /// Shared with `ripper_deps` so health probes observe the same circuit
    /// and cache state the ripper uses.
    mirror_policy: apple::MirrorPolicyManager<apple::ReqwestMirrorHttp>,
}

impl RipDeps {
    /// Build the production dependency graph and load the settings snapshot.
    pub async fn new(
        client: Arc<ferogram::Client>,
        dump_peer: ferogram::PeerRef,
        tracks: db::TracksRepository,
        requests: db::RequestLogRepository,
        settings: db::SettingsStore,
        database: db::DbPool,
    ) -> Result<Self, DeliveryError> {
        settings
            .init()
            .await
            .map_err(|error| DeliveryError::Unavailable(format!("load settings: {error}")))?;

        let apple = apple::AppleProduction::new(apple::AppleProductionConfig::default());
        let probe_policy = apple.mirror_policy().shared();
        let (retry_base_ms, max_retries) = retry_values();
        let ripper_config = RipperConfig {
            base_delay_ms: retry_base_ms,
            max_retries,
            lyrics_registry: Arc::new(LyricsRegistry::all_sources()),
            ..Default::default()
        };

        let albums = db::AlbumsRepository::new(database);
        let sink = FerogramTelegramSink::new(client, dump_peer).await?;

        let qobuz = qobuz::QobuzProduction::from_env();
        if qobuz.is_some() {
            tracing::info!("Qobuz provider initialized successfully");
        } else {
            tracing::info!(
                "Qobuz provider not configured (missing QOBUZ_BACKEND_URL or credentials)"
            );
        }

        Ok(Self {
            sink,
            albums,
            tracks,
            requests,
            settings,
            providers: ProviderRegistry::new(apple, qobuz, ripper_config),
            mirror_policy: probe_policy,
        })
    }

    /// Probe mirror health for the status dashboard. Uses a clone of the
    /// ripper's policy manager, so circuit state and the endpoint cache stay
    /// coherent between rips and probes.
    pub async fn probe_mirror_health(&self) -> crate::mirror_health::HealthReport {
        use crate::mirror_health::{MirrorHealthProbe, PolicyProbe};

        PolicyProbe::new(self.mirror_policy.shared()).probe().await
    }

    /// The mutable settings store backing the admin `/settings` panel.
    pub fn settings(&self) -> &db::SettingsStore {
        &self.settings
    }

    /// Synchronous snapshot read for panel rendering.
    pub fn settings_snapshot(&self) -> BotSettings {
        self.settings.get_settings()
    }

    /// Read-only database access for handlers that need direct queries outside
    /// the orchestrator seam.
    pub fn tracks(&self) -> &db::TracksRepository {
        &self.tracks
    }

    pub fn albums(&self) -> &db::AlbumsRepository {
        &self.albums
    }

    pub fn requests(&self) -> &db::RequestLogRepository {
        &self.requests
    }

    pub fn catalog(&self) -> &apple::Catalog<apple::ReqwestTransport> {
        self.providers.catalog()
    }

    pub fn playlist(&self) -> &apple::PlaylistClient<apple::ReqwestPlaylistHttp> {
        self.providers.playlist()
    }
}

impl ProviderAccess for RipDeps {
    type Providers = ProviderRegistry;

    fn providers(&self) -> &Self::Providers {
        &self.providers
    }
}

impl TrackCache for RipDeps {
    fn find_cached_tracks<'a>(
        &'a self,
        keys: &'a [TrackKey],
    ) -> BoxFuture<'a, Result<HashMap<TrackKey, CachedTrack>, TrackCacheError>> {
        Box::pin(async move {
            self.tracks
                .find_cached_tracks(keys)
                .await
                .map_err(|error| track_cache_error(TrackCacheOperation::Find, error))
        })
    }

    fn save_track<'a>(
        &'a self,
        input: SaveTrackInput,
    ) -> BoxFuture<'a, Result<(), TrackCacheError>> {
        Box::pin(async move {
            self.tracks
                .save_track(&input)
                .await
                .map(|_| ())
                .map_err(|error| track_cache_error(TrackCacheOperation::Save, error))
        })
    }

    fn delete_track<'a>(
        &'a self,
        track_key: &'a TrackKey,
    ) -> BoxFuture<'a, Result<bool, TrackCacheError>> {
        Box::pin(async move {
            self.tracks
                .delete_track(track_key)
                .await
                .map_err(|error| track_cache_error(TrackCacheOperation::Delete, error))
        })
    }
}

impl AlbumCache for RipDeps {
    fn save_album<'a>(&'a self, upload: AlbumUpload) -> BoxFuture<'a, Result<(), AlbumCacheError>> {
        Box::pin(async move {
            let message_id = i32::try_from(upload.message_id).map_err(|error| {
                AlbumCacheError::failed(
                    AlbumCacheOperation::Save,
                    format!("message_id out of range: {error}"),
                )
            })?;
            let new_album = db::NewAlbum {
                provider: upload.provider,
                album_id: &upload.album_id,
                codec: upload.codec,
                part_index: upload.part_index,
                total_parts: upload.total_parts,
                message_id,
                file_id: &upload.file_id,
                file_unique_id: &upload.file_unique_id,
                file_size: upload.file_size,
                file_name: &upload.file_name,
                generation_hash: &upload.generation_hash,
            };
            self.albums
                .save_album(&new_album)
                .await
                .map(|_| ())
                .map_err(|error| album_cache_error(AlbumCacheOperation::Save, error))
        })
    }

    fn replace_albums<'a>(
        &'a self,
        provider: Provider,
        album_id: &'a str,
        codec: Codec,
        expected: AlbumReplacementExpectation,
        uploads: Vec<AlbumUpload>,
    ) -> BoxFuture<'a, Result<AlbumReplacementResult, AlbumCacheError>> {
        Box::pin(async move {
            let result = self
                .albums
                .replace_albums(provider, album_id, codec, &expected, &uploads)
                .await
                .map_err(|error| album_cache_error(AlbumCacheOperation::Replace, error))?;
            map_album_replacement(result)
        })
    }

    fn find_albums<'a>(
        &'a self,
        provider: Provider,
        album_id: &'a str,
        codec: Option<Codec>,
    ) -> BoxFuture<'a, Result<Vec<CachedAlbum>, AlbumCacheError>> {
        Box::pin(async move {
            let rows = self
                .albums
                .find_albums(provider, album_id, codec)
                .await
                .map_err(|error| album_cache_error(AlbumCacheOperation::Find, error))?;
            Ok(rows
                .into_iter()
                .map(|row| CachedAlbum {
                    part_index: row.part_index,
                    total_parts: row.total_parts,
                    message_id: i64::from(row.message_id),
                    file_unique_id: row.file_unique_id,
                    generation_hash: row.generation_hash,
                    file_size: row.file_size,
                    codec: row.codec,
                })
                .collect())
        })
    }

    fn delete_albums<'a>(
        &'a self,
        provider: Provider,
        album_id: &'a str,
        codec: Option<Codec>,
    ) -> BoxFuture<'a, Result<(), AlbumCacheError>> {
        Box::pin(async move {
            self.albums
                .delete_albums(provider, album_id, codec)
                .await
                .map(|_| ())
                .map_err(|error| album_cache_error(AlbumCacheOperation::Delete, error))
        })
    }
}

impl JobBookkeeping for RipDeps {
    fn settings_snapshot(&self) -> BotSettings {
        self.settings.get_settings()
    }

    fn log_request<'a>(
        &'a self,
        log: RequestLog,
    ) -> BoxFuture<'a, Result<(), JobBookkeepingError>> {
        Box::pin(async move {
            self.requests
                .log_request(&log)
                .await
                .map_err(|error| bookkeeping_error(JobBookkeepingOperation::LogRequest, error))
        })
    }
}

impl Delivery for RipDeps {
    fn publish_to_dump<'a>(
        &'a self,
        publication: DumpPublish,
    ) -> BoxFuture<'a, Result<engine::orchestrator::deps::DumpPublication, DeliveryError>> {
        Box::pin(async move { self.sink.publish_to_dump(publication).await })
    }

    fn deliver_to_chat<'a>(
        &'a self,
        delivery: ChatDelivery,
    ) -> BoxFuture<'a, Result<engine::orchestrator::deps::DeliveryReceipt, DeliveryError>> {
        Box::pin(async move { self.sink.deliver_to_chat(delivery).await })
    }

    fn materialize_cached<'a>(
        &'a self,
        source: DumpMessageRef,
        destination: &'a std::path::Path,
        progress: Option<&'a engine::orchestrator::deps::UploadProgressCallback>,
    ) -> BoxFuture<'a, Result<(), DeliveryError>> {
        Box::pin(async move {
            self.sink
                .materialize_cached(source, destination, progress)
                .await
        })
    }

    fn retract_dump<'a>(
        &'a self,
        messages: &'a [DumpMessageRef],
    ) -> BoxFuture<'a, Result<(), DeliveryError>> {
        Box::pin(async move { self.sink.retract_dump(messages).await })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stale_album_replacement_is_a_conflict() {
        let result = map_album_replacement(AlbumReplacementResult::Stale);
        assert!(matches!(
            result,
            Err(AlbumCacheError::Conflict {
                operation: AlbumCacheOperation::Replace,
                ..
            })
        ));
    }

    #[test]
    fn closed_database_connection_is_unavailable() {
        let error = db::DbError::Database(DieselError::DatabaseError(
            DatabaseErrorKind::ClosedConnection,
            Box::new("connection closed".to_owned()),
        ));
        let mapped = track_cache_error(TrackCacheOperation::Find, error);
        assert!(matches!(
            mapped,
            TrackCacheError::Unavailable {
                operation: TrackCacheOperation::Find,
                ..
            }
        ));
    }
}
