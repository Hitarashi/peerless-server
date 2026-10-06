use std::{collections::HashMap, sync::Arc};

use diesel::result::{DatabaseErrorKind, Error as DieselError};
use engine::{
    Codec, Provider,
    orchestrator::deps::{
        AlbumCache, AlbumCacheError, AlbumCacheOperation, AlbumReplacementExpectation,
        AlbumReplacementResult, AlbumUpload, ArtworkProvider, BoxFuture, CachedAlbum, CachedTrack,
        ChatDelivery, CollectionResolver, Delivery, DeliveryError, DumpMessageRef, DumpPublish,
        OrchestratorConfig, ProviderDeps, ProviderPresentation, SaveTrackInput, Storefront,
        TaskBookkeeping, TrackAcquisition, TrackCache, TrackCacheError, TrackCacheOperation,
    },
    ripper::{RipError, RipperConfig},
    settings::BotSettings,
    types::{AlbumTracks, ArtistTracks, TrackRipResult},
};
use music::PlaylistData;

use crate::{providers::ProviderRegistry, telegram_sink::FerogramTelegramSink};

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
                DatabaseErrorKind::ClosedConnection | DatabaseErrorKind::UnableToSendCommand,
                _
            ))
    )
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

pub struct RipDeps {
    sink: FerogramTelegramSink,
    albums: db::AlbumsRepository,
    tracks: db::TracksRepository,
    settings: Arc<db::SettingsStore>,
    providers: ProviderRegistry,

    mirror_policy: apple::MirrorPolicyManager<apple::ReqwestMirrorHttp>,
}

impl RipDeps {
    pub async fn new(
        client: Arc<ferogram::Client>,
        dump_peer: ferogram::PeerRef,
        tracks: db::TracksRepository,
        settings: Arc<db::SettingsStore>,
        database: db::DbPool,
    ) -> Result<Self, DeliveryError> {
        settings
            .init()
            .await
            .map_err(|error| DeliveryError::Unavailable(format!("load settings: {error}")))?;

        let lyricsporn_api_endpoint = settings.lyricsporn_api_endpoint();
        let apple = apple::AppleProduction::with_api_endpoint(
            apple::AppleProductionConfig::default(),
            lyricsporn_api_endpoint.clone(),
        );
        let probe_policy = apple.mirror_policy().shared();
        let (retry_base_ms, max_retries) = retry_values();
        let ripper_config = RipperConfig {
            base_delay_ms: retry_base_ms,
            max_retries,
            ..RipperConfig::default()
        };
        let bot_settings = settings.get_settings();
        let default_storefront = engine::settings::resolve_default_storefront(&bot_settings);
        let sink = FerogramTelegramSink::new(client, dump_peer).await?;
        let albums = db::AlbumsRepository::new(database.clone());

        Ok(Self {
            sink,
            albums,
            tracks,
            settings,
            providers: ProviderRegistry::new(apple, ripper_config, default_storefront),
            mirror_policy: probe_policy,
        })
    }

    pub async fn probe_mirror_health(&self) -> crate::mirror_health::HealthReport {
        use crate::mirror_health::{MirrorHealthProbe, PolicyProbe};

        PolicyProbe::new(self.mirror_policy.shared()).probe().await
    }

    pub fn settings(&self) -> &db::SettingsStore {
        &self.settings
    }

    pub fn settings_snapshot(&self) -> BotSettings {
        self.settings.get_settings()
    }

    pub fn tracks(&self) -> &db::TracksRepository {
        &self.tracks
    }

    pub fn albums(&self) -> &db::AlbumsRepository {
        &self.albums
    }

    pub fn catalog(&self) -> &apple::Catalog<apple::ReqwestTransport> {
        self.providers.catalog()
    }

    pub fn playlist(&self) -> &apple::PlaylistClient<apple::ReqwestPlaylistHttp> {
        self.providers.playlist()
    }
}

impl CollectionResolver for RipDeps {
    async fn fetch_album_tracks(
        &self,
        provider: Provider,
        id: &str,
        storefront: Storefront<'_>,
    ) -> Result<AlbumTracks, String> {
        self.providers
            .fetch_album_tracks(provider, id, storefront)
            .await
    }

    async fn fetch_artist_tracks(
        &self,
        provider: Provider,
        id: &str,
        storefront: Storefront<'_>,
    ) -> Result<ArtistTracks, String> {
        self.providers
            .fetch_artist_tracks(provider, id, storefront)
            .await
    }

    async fn fetch_artist_album_ids(
        &self,
        provider: Provider,
        id: &str,
        storefront: Storefront<'_>,
    ) -> Result<Vec<String>, String> {
        self.providers
            .fetch_artist_album_ids(provider, id, storefront)
            .await
    }

    async fn fetch_playlist_tracks(
        &self,
        provider: Provider,
        id: &str,
        storefront: Storefront<'_>,
    ) -> Result<PlaylistData, String> {
        self.providers
            .fetch_playlist_tracks(provider, id, storefront)
            .await
    }
}

impl TrackAcquisition for RipDeps {
    async fn rip(
        &self,
        track_id: &str,
        options: engine::ripper::RipOptions<'_>,
    ) -> Result<TrackRipResult, RipError> {
        self.providers.rip(track_id, options).await
    }
}

impl ArtworkProvider for RipDeps {
    async fn fetch_artwork(&self, url: &str) -> Option<Vec<u8>> {
        self.providers.fetch_artwork(url).await
    }

    fn artwork_url_at_size(&self, provider: Provider, url: &str, size: u16) -> String {
        self.providers.artwork_url_at_size(provider, url, size)
    }
}

impl ProviderPresentation for RipDeps {
    fn default_job_header(&self) -> &str {
        self.providers.default_job_header()
    }

    fn album_url(
        &self,
        provider: Provider,
        album_id: &str,
        storefront: Storefront<'_>,
    ) -> Option<String> {
        self.providers.album_url(provider, album_id, storefront)
    }

    fn unavailable_track_message(&self) -> &str {
        self.providers.unavailable_track_message()
    }

    fn unavailable_track_log_message(&self) -> &str {
        self.providers.unavailable_track_log_message()
    }
}

impl ProviderDeps for RipDeps {
    fn supports_provider(&self, provider: Provider) -> bool {
        self.providers.supports_provider(provider)
    }
}

impl TrackCache for RipDeps {
    fn find_cached_tracks<'a>(
        &'a self,
        track_ids: &'a [String],
    ) -> BoxFuture<'a, Result<HashMap<(String, Codec), CachedTrack>, TrackCacheError>> {
        Box::pin(async move {
            self.tracks
                .find_cached_tracks(track_ids)
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
        track_id: &'a str,
        codec: Option<Codec>,
    ) -> BoxFuture<'a, Result<bool, TrackCacheError>> {
        Box::pin(async move {
            self.tracks
                .delete_track(track_id, codec)
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
                album_id: &upload.album_id,
                codec: upload.codec,
                part_index: upload.part_index,
                total_parts: upload.total_parts,
                message_id,
                file_id: &upload.file_id,
                file_unique_id: &upload.file_unique_id,
                file_size: upload.file_size,
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
        _provider: Provider,
        album_id: &'a str,
        codec: Codec,
        expected: AlbumReplacementExpectation,
        uploads: Vec<AlbumUpload>,
    ) -> BoxFuture<'a, Result<AlbumReplacementResult, AlbumCacheError>> {
        Box::pin(async move {
            let result = self
                .albums
                .replace_albums(album_id, codec, &expected, &uploads)
                .await
                .map_err(|error| album_cache_error(AlbumCacheOperation::Replace, error))?;
            Ok(result)
        })
    }

    fn find_albums<'a>(
        &'a self,
        _provider: Provider,
        album_id: &'a str,
        codec: Option<Codec>,
    ) -> BoxFuture<'a, Result<Vec<CachedAlbum>, AlbumCacheError>> {
        Box::pin(async move {
            let rows = self
                .albums
                .find_albums(album_id, codec)
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
        _provider: Provider,
        album_id: &'a str,
        codec: Option<Codec>,
    ) -> BoxFuture<'a, Result<(), AlbumCacheError>> {
        Box::pin(async move {
            self.albums
                .delete_albums(album_id, codec)
                .await
                .map(|_| ())
                .map_err(|error| album_cache_error(AlbumCacheOperation::Delete, error))
        })
    }
}

impl TaskBookkeeping for RipDeps {
    fn settings_snapshot(&self) -> BotSettings {
        self.settings.get_settings()
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
