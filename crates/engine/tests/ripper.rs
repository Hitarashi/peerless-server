//! Ripper tests — progress
//! sequences, retry semantics, stall/cancel behavior, temp cleanup, and
//! result mapping.

use std::{
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU32, Ordering},
    },
};

use bytes::Bytes;
use engine::{
    filename::MAX_FILENAME_BYTES,
    orchestrator::types::{RipActivity, TrackLabel},
    ripper::{
        AlacTrackRipper, RipError, RipOptions, RipProgressCallback, RipStage, RipperConfig,
        SourceFailureKind, fetch_artwork_bytes,
    },
    streaming::{AudioStreamSource, ByteStream, ProgressCallback},
    types::TrackMeta,
};
use futures_util::stream;
use music::{CodecPreference, Provider};
use tokio::io::AsyncWriteExt;
use tokio_util::sync::CancellationToken;

fn meta() -> TrackMeta {
    TrackMeta {
        id: "42".into(),
        title: "Title".into(),
        artist: "Artist".into(),
        album: "Album".into(),
        album_artist: "Artist".into(),
        genre: None,                 // → 'Unknown' in result mapping
        release_date: String::new(), // '' passthrough
        composer: None,
        track_number: None, // → 1
        track_count: None,  // → 1
        disc_number: None,
        disc_count: None,
        duration_secs: 240,
        explicit: false,
        content_advisory: None,
        artwork_url: String::new(), // → no artwork fetch
        album_id: None,
        artist_id: None,
        isrc: None,
        record_label: None,
        copyright: None,
        upc: None,
        is_streamable: None,
    }
}

fn fake_stream_with_length(chunks: Vec<Bytes>, content_length: Option<u64>) -> AudioStreamSource {
    AudioStreamSource {
        stream: Box::pin(stream::iter(
            chunks
                .into_iter()
                .map(Ok::<_, engine::streaming::StreamBodyError>),
        )) as ByteStream,
        source: engine::streaming::SourceId::PrimaryMirror,
        codec: "alac".into(),
        bit_depth: 24,
        sample_rate: 96_000,
        content_length,
    }
}

fn valid_stream_chunks() -> Vec<Bytes> {
    vec![Bytes::from_static(include_bytes!("fixtures/tone.m4a"))]
}

fn split_valid_stream_chunks(count: usize) -> Vec<Bytes> {
    let bytes = include_bytes!("fixtures/tone.m4a");
    let chunk_size = bytes.len().div_ceil(count);
    bytes
        .chunks(chunk_size)
        .map(Bytes::copy_from_slice)
        .collect()
}

/// Scriptable fake stage: acquisition behavior is configurable per test.
struct FakeStage {
    meta: TrackMeta,
    meta_failures: u32, // first N track_meta calls fail
    meta_calls: AtomicU32,
    stream_chunks: Vec<Bytes>,
    stream_content_length: Option<u64>,
    connect_fails: u32, // first N connect calls fail with a stall message
    connect_error: Option<RipError>,
    connect_permanent: Option<String>,
    connect_unavailable: bool,
    connect_calls: AtomicU32,
    observations: Mutex<Vec<(engine::streaming::SourceId, SourceFailureKind, String)>>,
}

impl FakeStage {
    fn ok() -> Self {
        Self {
            meta: meta(),
            meta_failures: 0,
            meta_calls: AtomicU32::new(0),
            stream_chunks: valid_stream_chunks(),
            stream_content_length: None,
            connect_fails: 0,
            connect_error: None,
            connect_permanent: None,
            connect_unavailable: false,
            connect_calls: AtomicU32::new(0),
            observations: Mutex::new(Vec::new()),
        }
    }
}

impl RipStage for FakeStage {
    async fn track_meta(&self, track_id: &str, storefront: &str) -> Result<TrackMeta, RipError> {
        let _ = (track_id, storefront);
        let n = self.meta_calls.fetch_add(1, Ordering::SeqCst);
        if n < self.meta_failures {
            return Err(RipError::Message("catalog down".into()));
        }
        Ok(self.meta.clone())
    }

    async fn connect_stream(
        &self,
        track_id: &str,
        meta: &TrackMeta,
        signal: Option<CancellationToken>,
        on_progress: Option<ProgressCallback>,
        codec_preference: CodecPreference,
    ) -> Result<AudioStreamSource, RipError> {
        let _ = (track_id, meta, signal, on_progress, codec_preference);
        let n = self.connect_calls.fetch_add(1, Ordering::SeqCst);
        if self.connect_unavailable {
            return Err(RipError::RenditionUnavailable {
                reason: "Dolby Atmos is unavailable".into(),
            });
        }
        if let Some(reason) = self.connect_permanent.clone() {
            return Err(RipError::TrackUnavailable { reason });
        }
        if let Some(err) = self.connect_error.clone() {
            return Err(err);
        }
        if n < self.connect_fails {
            return Err(RipError::Message(
                "Audio stream stalled on test: no data received for 45s".into(),
            ));
        }
        Ok(fake_stream_with_length(
            self.stream_chunks.clone(),
            self.stream_content_length,
        ))
    }

    fn observe_stream_failure(
        &self,
        source: &engine::streaming::SourceId,
        kind: SourceFailureKind,
        detail: &str,
    ) {
        self.observations
            .lock()
            .unwrap()
            .push((source.clone(), kind, detail.to_owned()));
    }

    fn track_tags(&self, meta: &TrackMeta) -> media::TrackTags {
        media::TrackTags {
            title: Some(meta.title.clone()),
            artist: Some(meta.artist.clone()),
            album: Some(meta.album.clone()),
            ..media::TrackTags::default()
        }
    }
}

fn config(dir: &Path, retries: u32, base_ms: u64) -> RipperConfig {
    RipperConfig {
        default_output_dir: dir.to_owned(),
        max_retries: retries,
        base_delay_ms: base_ms,
        ..RipperConfig::default()
    }
}

type ProgressLog = Arc<Mutex<Vec<RipActivity>>>;

fn record() -> (RipProgressCallback, ProgressLog) {
    let log: ProgressLog = Arc::new(Mutex::new(Vec::new()));
    let log2 = log.clone();
    let cb: RipProgressCallback = Arc::new(move |activity| {
        log2.lock().unwrap().push(activity);
    });
    (cb, log)
}

#[tokio::test]
async fn happy_path_progress_and_result_mapping() {
    let dir = tempfile::tempdir().unwrap();
    let deps = FakeStage::ok();
    let ripper = AlacTrackRipper::new(config(dir.path(), 3, 1));
    let (cb, log) = record();
    let result = ripper
        .rip(
            &deps,
            "42",
            RipOptions::new(Provider::Apple, "us").with_progress(&cb),
        )
        .await
        .unwrap();

    // Result mapping (JS || / ?? semantics).
    assert_eq!(result.title, "Title");
    assert_eq!(result.genre, "Unknown");
    assert_eq!(result.release_date, "");
    assert_eq!(result.track_number, 1);
    assert_eq!(result.track_count, 1);
    assert_eq!(result.codec, "alac");
    assert_eq!(result.bit_depth, 24);
    assert_eq!(result.sample_rate, 96_000);
    assert_eq!(result.duration, 240);
    assert!(result.file_path.ends_with("01. Title - Artist [ALAC].m4a"));

    // Progress sequence (TS: lastProgressUpdate=0 → the FIRST chunk always
    // emits a byte-progress update).
    let log = log.lock().unwrap();
    assert_eq!(log[0], RipActivity::ResolvingMetadata);
    assert_eq!(
        log[1],
        RipActivity::Connecting {
            track: TrackLabel::new("Title", "Artist")
        }
    );
    assert!(matches!(
        log[2],
        RipActivity::Downloading { ref track, ref progress }
            if track == &TrackLabel::new("Title", "Artist")
                && progress.completed > 0
                && progress.total.is_none()
    ));
    assert!(log.iter().any(|activity| matches!(
        activity,
        RipActivity::Tagging { track }
            if track == &TrackLabel::new("Title", "Artist")
    )));

    // Finalized output exists; temp raw cleaned up.
    assert!(Path::new(&result.file_path).is_file());
    let leftovers: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_str().is_some_and(|n| n.ends_with(".raw")))
        .collect();
    assert!(leftovers.is_empty(), "temp raw removed");
    assert!(
        std::fs::read_dir(dir.path()).unwrap().all(|entry| !entry
            .unwrap()
            .file_name()
            .to_str()
            .is_some_and(|name| name.starts_with(".track_"))),
        "successful rips remove their staging directory"
    );
}

#[tokio::test]
async fn metadata_error_cleans_staging_lane() {
    let dir = tempfile::tempdir().unwrap();
    let mut deps = FakeStage::ok();
    deps.meta_failures = 1;
    let ripper = AlacTrackRipper::new(config(dir.path(), 0, 1));

    let error = ripper
        .rip(&deps, "42", RipOptions::new(Provider::Apple, "us"))
        .await
        .unwrap_err();

    assert!(matches!(error, RipError::Message(message) if message == "catalog down"));
    assert!(std::fs::read_dir(dir.path()).unwrap().next().is_none());
}

#[tokio::test]
async fn retry_succeeds_after_two_failures() {
    let dir = tempfile::tempdir().unwrap();
    let mut deps = FakeStage::ok();
    deps.connect_fails = 2;
    let ripper = AlacTrackRipper::new(config(dir.path(), 3, 1));
    let (cb, log) = record();
    let result = ripper
        .rip(
            &deps,
            "42",
            RipOptions::new(Provider::Apple, "us").with_progress(&cb),
        )
        .await
        .unwrap();
    assert_eq!(result.title, "Title");
    assert_eq!(
        deps.connect_calls.load(Ordering::SeqCst),
        3,
        "3 attempts total"
    );
    let log = log.lock().unwrap();
    let connecting_events: Vec<_> = log
        .iter()
        .filter(|activity| matches!(activity, RipActivity::Connecting { .. }))
        .collect();
    assert_eq!(connecting_events.len(), 3);
}

#[tokio::test]
async fn exhaustion_rethrows_last_error() {
    let dir = tempfile::tempdir().unwrap();
    let mut deps = FakeStage::ok();
    deps.connect_fails = 10;
    let ripper = AlacTrackRipper::new(config(dir.path(), 2, 1));
    let error = ripper
        .rip(&deps, "42", RipOptions::new(Provider::Apple, "us"))
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        RipError::Message(message) if message == "Audio stream stalled on test: no data received for 45s"
    ));
    assert_eq!(
        deps.connect_calls.load(Ordering::SeqCst),
        3,
        "max_retries=2 → 3 attempts"
    );
}

#[tokio::test]
async fn cancelled_error_bypasses_retries() {
    let dir = tempfile::tempdir().unwrap();
    let ripper = AlacTrackRipper::new(config(dir.path(), 5, 1));
    let (cb, log) = record();

    struct CancelledStage(FakeStage);
    impl RipStage for CancelledStage {
        async fn track_meta(
            &self,
            track_id: &str,
            storefront: &str,
        ) -> Result<TrackMeta, RipError> {
            self.0.track_meta(track_id, storefront).await
        }
        async fn connect_stream(
            &self,
            track_id: &str,
            meta: &TrackMeta,
            signal: Option<CancellationToken>,
            on_progress: Option<ProgressCallback>,
            codec_preference: music::CodecPreference,
        ) -> Result<AudioStreamSource, RipError> {
            let _ = self
                .0
                .connect_stream(track_id, meta, signal, on_progress, codec_preference)
                .await;
            Err(RipError::Cancelled)
        }
        fn observe_stream_failure(
            &self,
            source: &engine::streaming::SourceId,
            kind: SourceFailureKind,
            detail: &str,
        ) {
            self.0.observe_stream_failure(source, kind, detail);
        }

        fn track_tags(&self, meta: &TrackMeta) -> media::TrackTags {
            self.0.track_tags(meta)
        }
    }

    let stage = CancelledStage(FakeStage::ok());
    let error = ripper
        .rip(
            &stage,
            "42",
            RipOptions::new(Provider::Apple, "us").with_progress(&cb),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, RipError::Cancelled));
    assert_eq!(log.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn cancellation_mid_rip_no_retry() {
    let dir = tempfile::tempdir().unwrap();
    let deps = FakeStage::ok();
    let ripper = AlacTrackRipper::new(config(dir.path(), 3, 1));
    let token = CancellationToken::new();
    token.cancel();
    let error = ripper
        .rip(
            &deps,
            "42",
            RipOptions::new(Provider::Apple, "us").with_signal(token),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, RipError::Cancelled));
    assert_eq!(deps.meta_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn artwork_empty_vec_means_no_cover() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let Ok((mut socket, _)) = listener.accept().await else {
            return;
        };
        socket
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            .await
            .ok();
    });
    let dir = tempfile::tempdir().unwrap();
    let artwork = fetch_artwork_bytes(
        &config(dir.path(), 0, 0),
        &format!("http://{address}/cover"),
    )
    .await;
    assert_eq!(artwork, Some(Vec::new()));
    server.abort();
}

#[tokio::test]
async fn stalled_stream_is_retryable() {
    // A stream that NEVER yields (pending forever): rip_once returns the
    // 45s stall error, and the retry loop retries it.
    let dir = tempfile::tempdir().unwrap();
    struct StalledStage(FakeStage);
    impl RipStage for StalledStage {
        async fn track_meta(&self, t: &str, s: &str) -> Result<TrackMeta, RipError> {
            self.0.track_meta(t, s).await
        }
        async fn connect_stream(
            &self,
            track_id: &str,
            meta: &TrackMeta,
            signal: Option<CancellationToken>,
            on_progress: Option<ProgressCallback>,
            codec_preference: music::CodecPreference,
        ) -> Result<AudioStreamSource, RipError> {
            let _ = (track_id, meta, signal, on_progress, codec_preference);
            self.0.connect_calls.fetch_add(1, Ordering::SeqCst);
            let pending: ByteStream = Box::pin(stream::pending());
            Ok(AudioStreamSource {
                stream: pending,
                source: engine::streaming::SourceId::PrimaryMirror,
                codec: "alac".into(),
                bit_depth: 24,
                sample_rate: 96_000,
                content_length: None,
            })
        }
        fn observe_stream_failure(
            &self,
            source: &engine::streaming::SourceId,
            kind: SourceFailureKind,
            detail: &str,
        ) {
            self.0.observe_stream_failure(source, kind, detail);
        }

        fn track_tags(&self, meta: &TrackMeta) -> media::TrackTags {
            self.0.track_tags(meta)
        }
    }

    let inner = FakeStage::ok();
    let connect_calls = Arc::new(AtomicU32::new(0));
    // Track connect attempts via the shared counter inside FakeStage.
    let stage = StalledStage(inner);
    let ripper = AlacTrackRipper::new(config(dir.path(), 1, 1));
    tokio::time::pause();
    let error = ripper
        .rip(&stage, "42", RipOptions::new(Provider::Apple, "us"))
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        RipError::StreamStalled {
            source: engine::streaming::SourceId::PrimaryMirror,
            secs: 45
        }
    ));
    assert_eq!(
        stage.0.connect_calls.load(Ordering::SeqCst),
        2,
        "retried once"
    );
    assert_eq!(
        stage.0.observations.lock().unwrap().len(),
        2,
        "each stalled stream is observed"
    );
    drop(connect_calls);
}

#[tokio::test]
async fn progress_totals_with_content_length() {
    let dir = tempfile::tempdir().unwrap();
    let mut deps = FakeStage::ok();
    let stream_length = include_bytes!("fixtures/tone.m4a").len() as u64;
    deps.stream_chunks = split_valid_stream_chunks(2);
    deps.stream_content_length = Some(stream_length);
    let ripper = AlacTrackRipper::new(config(dir.path(), 3, 1));
    let (cb, log) = record();
    ripper
        .rip(
            &deps,
            "42",
            RipOptions::new(Provider::Apple, "us").with_progress(&cb),
        )
        .await
        .unwrap();
    let log = log.lock().unwrap();
    let download_events: Vec<&RipActivity> = log
        .iter()
        .filter(|activity| matches!(activity, RipActivity::Downloading { .. }))
        .collect();
    assert!(!download_events.is_empty());
    // Byte progress includes the known total when content length is present.
    assert!(matches!(
        download_events[0],
        RipActivity::Downloading { progress, .. } if progress.total == Some(stream_length)
    ));
}

#[tokio::test]
async fn short_body_is_rejected_before_tagging() {
    let dir = tempfile::tempdir().unwrap();
    let mut deps = FakeStage::ok();
    deps.stream_chunks = vec![Bytes::from(vec![0u8; 10]), Bytes::from(vec![0u8; 5])];
    deps.stream_content_length = Some(20);
    let ripper = AlacTrackRipper::new(config(dir.path(), 0, 1));

    let error = ripper
        .rip(&deps, "42", RipOptions::new(Provider::Apple, "us"))
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        RipError::IncompleteBody {
            source: engine::streaming::SourceId::PrimaryMirror,
            expected: 20,
            received: 15
        }
    ));
    assert!(matches!(
        deps.observations.lock().unwrap().as_slice(),
        [(_, SourceFailureKind::IncompleteBody, _)]
    ));
    assert!(std::fs::read_dir(dir.path()).unwrap().next().is_none());
}

#[tokio::test]
async fn output_dir_override_used() {
    let base = tempfile::tempdir().unwrap();
    let other = tempfile::tempdir().unwrap();
    let deps = FakeStage::ok();
    let ripper = AlacTrackRipper::new(config(base.path(), 3, 1));
    let result = ripper
        .rip(
            &deps,
            "42",
            RipOptions::new(Provider::Apple, "us").with_output_dir(other.path()),
        )
        .await
        .unwrap();
    assert!(result.file_path.starts_with(other.path().to_str().unwrap()));
    assert!(Path::new(&result.file_path).starts_with(other.path()));
    assert!(Path::new(&result.file_path).is_file());
}

#[tokio::test]
async fn long_metadata_filename_is_bounded_and_rip_succeeds() {
    let dir = tempfile::tempdir().unwrap();
    let mut deps = FakeStage::ok();
    deps.meta.title = "曲".repeat(150);
    let ripper = AlacTrackRipper::new(config(dir.path(), 0, 1));

    let result = ripper
        .rip(&deps, "42", RipOptions::new(Provider::Apple, "us"))
        .await
        .expect("long metadata should produce a bounded output name");
    let filename = Path::new(&result.file_path)
        .file_name()
        .and_then(|name| name.to_str())
        .expect("bounded output filename is valid UTF-8");

    assert!(!filename.is_empty());
    assert!(filename.len() <= MAX_FILENAME_BYTES);
    assert!(filename.ends_with(" [ALAC].m4a"));
    assert!(Path::new(&result.file_path).is_file());
}

#[tokio::test]
async fn local_io_error_is_non_retryable() {
    let dir = tempfile::tempdir().unwrap();
    let mut deps = FakeStage::ok();
    deps.connect_error = Some(RipError::LocalIo {
        message: "local io failure: disk full".into(),
    });
    let ripper = AlacTrackRipper::new(config(dir.path(), 4, 1));
    let (cb, log) = record();

    let error = ripper
        .rip(
            &deps,
            "42",
            RipOptions::new(Provider::Apple, "us").with_progress(&cb),
        )
        .await
        .expect_err("a local io error should fail without retries");

    assert!(matches!(
        error,
        RipError::LocalIo { message } if message.contains("disk full")
    ));
    assert_eq!(deps.connect_calls.load(Ordering::SeqCst), 1);
    assert!(
        log.lock()
            .unwrap()
            .iter()
            .all(|activity| !matches!(activity, RipActivity::Downloading { .. }))
    );
}

#[tokio::test]
async fn progress_throttles_to_one_per_second() {
    let dir = tempfile::tempdir().unwrap();
    let mut deps = FakeStage::ok();
    // Many chunks arriving quickly: progress updates throttle to 1/sec.
    deps.stream_chunks = split_valid_stream_chunks(50);
    let ripper = AlacTrackRipper::new(config(dir.path(), 3, 1));
    let (cb, log) = record();
    tokio::time::pause();
    ripper
        .rip(
            &deps,
            "42",
            RipOptions::new(Provider::Apple, "us").with_progress(&cb),
        )
        .await
        .unwrap();
    let log = log.lock().unwrap();
    let download_events: Vec<_> = log
        .iter()
        .filter(|activity| matches!(activity, RipActivity::Downloading { .. }))
        .collect();
    // All chunks delivered within the same paused "second" → at most a
    // handful of updates (first fires immediately due to the backdated
    // last_update, subsequent ones only after 1s of paused time, which
    // never advances while chunks are ready).
    assert!(
        download_events.len() <= 3,
        "throttled: got {}",
        download_events.len()
    );
}

#[tokio::test]
async fn not_found_404_skips_retries_completely() {
    let dir = tempfile::tempdir().unwrap();
    let mut deps = FakeStage::ok();
    deps.connect_permanent = Some("Wrapper /m3u8 returned HTTP 404".into());
    let ripper = AlacTrackRipper::new(config(dir.path(), 4, 1000));
    let error = ripper
        .rip(&deps, "6804576275", RipOptions::new(Provider::Apple, "in"))
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        RipError::TrackUnavailable { reason } if reason == "Wrapper /m3u8 returned HTTP 404"
    ));
    assert_eq!(
        deps.connect_calls.load(Ordering::SeqCst),
        1,
        "should have stopped on attempt 0 without retrying"
    );
}

#[tokio::test]
async fn typed_unavailable_outcome_bypasses_retries() {
    let dir = tempfile::tempdir().unwrap();
    let mut deps = FakeStage::ok();
    deps.connect_unavailable = true;
    let ripper = AlacTrackRipper::new(config(dir.path(), 4, 1000));

    let error = ripper
        .rip(
            &deps,
            "42",
            RipOptions::new(Provider::Apple, "us").with_codec_preference(CodecPreference::Atmos),
        )
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        RipError::RenditionUnavailable { reason } if reason == "Dolby Atmos is unavailable"
    ));
    assert_eq!(deps.connect_calls.load(Ordering::SeqCst), 1);
}
