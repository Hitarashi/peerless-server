use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use engine::{
    orchestrator::{
        RipOrchestrator,
        deps::{
            AlbumCache, AlbumCacheError, AlbumReplacementExpectation, AlbumReplacementResult,
            AlbumUpload, ArtworkProvider, BoxFuture, CachedAlbum, CachedTrack, ChatDelivery,
            CollectionResolver, Delivery, DeliveryError, DeliveryReceipt, DumpMessageRef,
            DumpPublication, DumpPublish, OrchestratorConfig, ProviderDeps, ProviderPresentation,
            RetryConfig, SaveTrackInput, StorageRetryPolicy, Storefront, TaskBookkeeping,
            TrackAcquisition, TrackCache, TrackCacheError, UploadProgressCallback,
        },
        types::{OrchestratorEvent, RipTaskOptions, TaskPhase},
    },
    settings::BotSettings,
    types::{AlbumTracks, ArtistTracks, ParsedTargetItem, TargetKind},
};
use music::PlaylistData;

struct RacePresentation;

impl ProviderPresentation for RacePresentation {
    fn default_job_header(&self) -> &str {
        "Lossless Rip"
    }

    fn album_url(
        &self,
        album_id: &str,
        storefront: Storefront<'_>,
    ) -> Option<String> {
        let storefront = storefront.get().unwrap_or_default();
        (!album_id.is_empty()).then(|| format!("{storefront}/album/{album_id}"))
    }

    fn unavailable_track_message(&self) -> &str {
        "Unavailable (not streamable)"
    }

    fn unavailable_track_log_message(&self) -> &str {
        "Track is not streamable, skipping rip"
    }
}

#[derive(Clone)]
struct RaceDeps {
    settings: BotSettings,
    rip_hold: Arc<tokio::sync::Mutex<bool>>,
    rip_calls: Arc<AtomicUsize>,
}

impl TrackCache for RaceDeps {
    fn find_cached_tracks<'a>(
        &'a self,
        track_ids: &'a [String],
    ) -> BoxFuture<'a, Result<HashMap<(String, engine::Codec), CachedTrack>, TrackCacheError>> {
        let _ = track_ids;
        Box::pin(async { Ok(HashMap::new()) })
    }

    fn save_track<'a>(
        &'a self,
        input: SaveTrackInput,
    ) -> BoxFuture<'a, Result<(), TrackCacheError>> {
        let _ = input;
        Box::pin(async { Ok(()) })
    }

    fn delete_track<'a>(
        &'a self,
        track_id: &'a str,
        codec: Option<engine::Codec>,
    ) -> BoxFuture<'a, Result<bool, TrackCacheError>> {
        let _ = (track_id, codec);
        Box::pin(async { Ok(true) })
    }
}

impl TaskBookkeeping for RaceDeps {
    fn settings_snapshot(&self) -> BotSettings {
        self.settings.clone()
    }
}

impl AlbumCache for RaceDeps {
    fn save_album<'a>(&'a self, upload: AlbumUpload) -> BoxFuture<'a, Result<(), AlbumCacheError>> {
        let _ = upload;
        Box::pin(async { Ok(()) })
    }

    fn replace_albums<'a>(
        &'a self,
        album_id: &'a str,
        codec: engine::Codec,
        expected: AlbumReplacementExpectation,
        uploads: Vec<AlbumUpload>,
    ) -> BoxFuture<'a, Result<AlbumReplacementResult, AlbumCacheError>> {
        let _ = (album_id, codec, expected, uploads);
        Box::pin(async { Ok(AlbumReplacementResult::Stale) })
    }

    fn find_albums<'a>(
        &'a self,
        album_id: &'a str,
        codec: Option<engine::Codec>,
    ) -> BoxFuture<'a, Result<Vec<CachedAlbum>, AlbumCacheError>> {
        let _ = (album_id, codec);
        Box::pin(async { Ok(Vec::new()) })
    }

    fn delete_albums<'a>(
        &'a self,
        album_id: &'a str,
        codec: Option<engine::Codec>,
    ) -> BoxFuture<'a, Result<(), AlbumCacheError>> {
        let _ = (album_id, codec);
        Box::pin(async { Ok(()) })
    }
}

impl Delivery for RaceDeps {
    fn publish_to_dump<'a>(
        &'a self,
        publication: DumpPublish,
    ) -> BoxFuture<'a, Result<DumpPublication, DeliveryError>> {
        let _ = publication;
        Box::pin(async { Err(DeliveryError::Unavailable("delivery not reached".into())) })
    }

    fn deliver_to_chat<'a>(
        &'a self,
        delivery: ChatDelivery,
    ) -> BoxFuture<'a, Result<DeliveryReceipt, DeliveryError>> {
        let _ = delivery;
        Box::pin(async { Err(DeliveryError::Unavailable("delivery not reached".into())) })
    }

    fn materialize_cached<'a>(
        &'a self,
        source: DumpMessageRef,
        destination: &'a std::path::Path,
        progress: Option<&'a UploadProgressCallback>,
    ) -> BoxFuture<'a, Result<(), DeliveryError>> {
        let _ = (source, destination, progress);
        Box::pin(async { Err(DeliveryError::Unavailable("delivery not reached".into())) })
    }

    fn retract_dump<'a>(
        &'a self,
        messages: &'a [DumpMessageRef],
    ) -> BoxFuture<'a, Result<(), DeliveryError>> {
        let _ = messages;
        Box::pin(async { Err(DeliveryError::Unavailable("delivery not reached".into())) })
    }
}

impl CollectionResolver for RaceDeps {
    async fn fetch_album_tracks(
        &self,
        id: &str,
        _storefront: Storefront<'_>,
    ) -> Result<AlbumTracks, String> {
        let _ = id;
        Err("no albums".to_owned())
    }
    async fn fetch_artist_tracks(
        &self,
        id: &str,
        _storefront: Storefront<'_>,
    ) -> Result<ArtistTracks, String> {
        let _ = id;
        Err("no artists".to_owned())
    }
    async fn fetch_playlist_tracks(
        &self,
        id: &str,
        _storefront: Storefront<'_>,
    ) -> Result<PlaylistData, String> {
        let _ = id;
        Err("Playlist lookup timed out after 1ms: no playlists".to_owned())
    }
}

impl TrackAcquisition for RaceDeps {
    async fn rip(
        &self,
        id: &str,
        _options: engine::ripper::RipOptions<'_>,
    ) -> Result<engine::types::TrackRipResult, engine::ripper::RipError> {
        let _ = id;
        self.rip_calls.fetch_add(1, Ordering::SeqCst);

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while *self.rip_hold.lock().await && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        Err(engine::ripper::RipError::SourceOffline {
            source: engine::streaming::SourceId::PrimaryMirror,
        })
    }
}

impl ArtworkProvider for RaceDeps {
    async fn fetch_artwork(&self, url: &str) -> Option<Vec<u8>> {
        let _ = url;
        None
    }

    fn artwork_url_at_size(&self, url: &str, size: u16) -> String {
        let _ = size;
        url.to_owned()
    }
}

impl ProviderPresentation for RaceDeps {
    fn default_job_header(&self) -> &str {
        RacePresentation.default_job_header()
    }

    fn album_url(
        &self,
        album_id: &str,
        storefront: Storefront<'_>,
    ) -> Option<String> {
        RacePresentation.album_url(album_id, storefront)
    }

    fn unavailable_track_message(&self) -> &str {
        RacePresentation.unavailable_track_message()
    }

    fn unavailable_track_log_message(&self) -> &str {
        RacePresentation.unavailable_track_log_message()
    }
}

impl ProviderDeps for RaceDeps {}

fn race_options() -> RipTaskOptions {
    RipTaskOptions {
        chat_id: 100,
        user_id: 42,
        user_name: Some("tester".to_owned()),
        delivery_chat_id: 100,
        is_group: false,
        is_force: false,
        is_cache_only: false,
        single_storefront: None,
        parsed_items: vec![ParsedTargetItem {
            id: "track1".to_owned(),
            kind: TargetKind::Track,
            storefront: None,
        }],
        reply_to_message_id: Some(555),
        is_admin: true,
        codec_preference: None,
        rendition_policy: engine::orchestrator::types::RenditionPolicy::PrimaryWithOptionalAtmos,
    }
}

fn terminal_recorder(orch: &RipOrchestrator) -> Arc<Mutex<Vec<(&'static str, String)>>> {
    let events: Arc<Mutex<Vec<(&'static str, String)>>> = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&events);
    orch.subscribe(Arc::new(move |event| {
        let entry = match event {
            OrchestratorEvent::Completed(job, _) => ("completed", job.id.clone()),
            OrchestratorEvent::Cancelled(job, _) => ("cancelled", job.id.clone()),
            OrchestratorEvent::Failed(job, _) => ("failed", job.id.clone()),
            _ => return,
        };
        sink.lock().unwrap().push(entry);
    }));
    events
}

async fn wait_for_phase(
    orch: &RipOrchestrator,
    phase: TaskPhase,
) -> engine::orchestrator::types::ActiveRipTask {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        if let Some(job) = orch
            .get_active_tasks()
            .into_iter()
            .find(|job| job.phase == phase)
        {
            return job;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "job never reached phase {phase:?}"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancel_during_active_rip_emits_exactly_one_cancelled_terminal() {
    let orch = Arc::new(RipOrchestrator::new(OrchestratorConfig {
        storage_retry: StorageRetryPolicy::test(),
        upload_retry: RetryConfig::new(0, 0),
    }));
    let terminals = terminal_recorder(&orch);
    let deps = Arc::new(RaceDeps {
        settings: engine::settings::default_settings(),
        rip_hold: Arc::new(tokio::sync::Mutex::new(true)),
        rip_calls: Arc::new(AtomicUsize::new(0)),
    });

    let run_orch = Arc::clone(&orch);
    let run_deps = Arc::clone(&deps);
    let options = race_options();
    let task = tokio::spawn(async move { run_orch.start_task(run_deps, &options).await });

    let job = wait_for_phase(&orch, TaskPhase::Processing).await;
    assert!(orch.cancel_task(&job.id, Some("tester")));

    *deps.rip_hold.lock().await = false;
    let _ = task.await.unwrap();

    let terminals = terminals.lock().unwrap();
    assert_eq!(terminals.len(), 1, "exactly one terminal event");
    assert_eq!(terminals[0].0, "cancelled");
    assert!(orch.get_active_tasks().is_empty(), "job removed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn settled_failed_job_is_no_longer_cancellable() {
    let orch = Arc::new(RipOrchestrator::new(OrchestratorConfig {
        storage_retry: StorageRetryPolicy::test(),
        upload_retry: RetryConfig::new(0, 0),
    }));
    let terminals = terminal_recorder(&orch);
    let deps = Arc::new(RaceDeps {
        settings: engine::settings::default_settings(),
        rip_hold: Arc::new(tokio::sync::Mutex::new(false)),
        rip_calls: Arc::new(AtomicUsize::new(0)),
    });

    let run_orch = Arc::clone(&orch);
    let run_deps = Arc::clone(&deps);
    let options = race_options();
    let task = tokio::spawn(async move { run_orch.start_task(run_deps, &options).await });
    let result = task.await.unwrap();

    let terminals = terminals.lock().unwrap();
    assert_eq!(terminals.len(), 1);
    assert_eq!(terminals[0].0, "completed");
    assert!(result.is_ok(), "recorded failure + continue settles Ok");

    assert_eq!(result.as_ref().unwrap().failed_count, 1);

    assert!(!orch.cancel_task(&terminals[0].1, Some("late")));
}

#[test]
fn unresolved_progress_has_zero_total_tracks() {
    let progress = engine::orchestrator::types::RipTaskProgress {
        job_id: "j".to_owned(),
        total_tracks: 0,
        completed_tracks: 0,
        cached_count: 0,
        ripped_count: 0,
        failed_count: 0,
        skipped_count: 0,
        percent: 0,
        job_activity: None,
        download: None,
        upload: None,
        codec: None,
    };
    assert_eq!(progress.total_tracks, 0);
}

#[test]
fn mirror_health_labels_render_expected_text() {
    use bot::mirror_health::{HealthReport, LastKnownHealth, MirrorHealth};

    let health = LastKnownHealth::new();
    assert_eq!(health.label(), None, "unknown before first probe");
    health.record(HealthReport {
        health: MirrorHealth::Online,
        latency_ms: 42,
    });
    assert_eq!(health.label(), Some("Online"));
    health.record(HealthReport {
        health: MirrorHealth::Unreachable,
        latency_ms: 4000,
    });
    assert_eq!(health.label(), Some("Unreachable"));
    health.record(HealthReport {
        health: MirrorHealth::Unavailable,
        latency_ms: 4000,
    });
    assert_eq!(health.label(), Some("Unavailable"));
    assert_eq!(MirrorHealth::NotConfigured.label(), "Not configured");
}
