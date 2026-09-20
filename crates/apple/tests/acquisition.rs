use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use apple::{
    AcquisitionOutcome, AppleAcquisitionConfig, AppleStreamAcquisition, MANIFEST_URL, MirrorHttp,
    MirrorHttpError, MirrorPolicyManager, WrapperKind, map_acquisition_outcome,
};
use bytes::Bytes;
use engine::{
    orchestrator::types::{RipActivity, TrackLabel},
    ripper::{AlacTrackRipper, RipError, RipOptions, RipStage, RipperConfig, SourceFailureKind},
    streaming::{
        AudioStreamSource, ByteStream, ProgressCallback, SourceId, StreamError, StreamHttp,
        StreamHttpError, StreamHttpResponse, StreamTransport,
    },
    types::TrackMeta,
};
use futures_util::stream;
use music::Provider;
use tokio_util::sync::CancellationToken;

struct FakeMirror {
    available: bool,
}

impl MirrorHttp for FakeMirror {
    async fn get(
        &self,
        url: &str,
        headers: &[(&str, String)],
        timeout: Duration,
        signal: Option<&CancellationToken>,
    ) -> Result<String, MirrorHttpError> {
        let _ = (headers, timeout, signal);
        if !self.available {
            return Err(MirrorHttpError::Status(503));
        }
        if url == MANIFEST_URL {
            Ok(r#"{"source":{"apple":"https://mirror"},"key":"key"}"#.to_owned())
        } else {
            Ok(r#"{"wrapper_instances":[1]}"#.to_owned())
        }
    }
}

struct FakeStream {
    calls: Arc<Mutex<Vec<String>>>,
    failures: Mutex<Vec<String>>,
    success_codecs: Mutex<Vec<String>>,
    cancel_on_fetch: bool,
    cancel_after_second_candidate: bool,
}

struct CorruptMirrorStream {
    calls: Arc<Mutex<Vec<String>>>,
    wrapper_codec: String,
    wrapper_corrupt: bool,
}

impl CorruptMirrorStream {
    fn new(calls: Arc<Mutex<Vec<String>>>, wrapper_codec: &str, wrapper_corrupt: bool) -> Self {
        Self {
            calls,
            wrapper_codec: wrapper_codec.to_owned(),
            wrapper_corrupt,
        }
    }
}

impl FakeStream {
    fn new(failures: &[&str], calls: Arc<Mutex<Vec<String>>>) -> Self {
        Self {
            calls,
            failures: Mutex::new(failures.iter().map(|url| (*url).to_owned()).collect()),
            success_codecs: Mutex::new(Vec::new()),
            cancel_on_fetch: false,
            cancel_after_second_candidate: false,
        }
    }

    fn with_success_codecs(codecs: &[&str], calls: Arc<Mutex<Vec<String>>>) -> Self {
        Self {
            calls,
            failures: Mutex::new(Vec::new()),
            success_codecs: Mutex::new(codecs.iter().map(|codec| (*codec).to_owned()).collect()),
            cancel_on_fetch: false,
            cancel_after_second_candidate: false,
        }
    }
}

impl StreamHttp for FakeStream {
    async fn fetch(
        &self,
        url: &str,
        api_key: Option<&str>,
        timeout: Duration,
        signal: Option<&CancellationToken>,
    ) -> Result<StreamHttpResponse, StreamHttpError> {
        let _ = (api_key, timeout);
        self.calls.lock().unwrap().push(url.to_owned());
        if self.cancel_on_fetch {
            if let Some(token) = signal {
                token.cancel();
            }
            return Err(StreamHttpError::Cancelled);
        }
        let should_fail = self
            .failures
            .lock()
            .unwrap()
            .iter()
            .position(|failure| url.contains(failure));
        if let Some(index) = should_fail {
            self.failures.lock().unwrap().remove(index);
            return Err(StreamHttpError::Network("transient".to_owned()));
        }
        let codec = {
            let mut success_codecs = self.success_codecs.lock().unwrap();
            if success_codecs.is_empty() {
                "alac".to_owned()
            } else {
                success_codecs.remove(0)
            }
        };
        let body: ByteStream = Box::pin(stream::once(
            async move { Ok(Bytes::from_static(b"audio")) },
        ));
        if self.cancel_after_second_candidate
            && url.ends_with("/stream/42")
            && !url.contains("/api/")
            && let Some(token) = signal
        {
            token.cancel();
        }
        Ok(StreamHttpResponse {
            status: 200,
            codec: Some(codec),
            bit_depth: Some("24".to_owned()),
            sample_rate: Some("96000".to_owned()),
            content_length: Some(5),
            body: Some(body),
        })
    }
}

impl StreamHttp for CorruptMirrorStream {
    async fn fetch(
        &self,
        url: &str,
        api_key: Option<&str>,
        timeout: Duration,
        signal: Option<&CancellationToken>,
    ) -> Result<StreamHttpResponse, StreamHttpError> {
        let _ = (api_key, timeout, signal);
        self.calls.lock().unwrap().push(url.to_owned());
        let is_mirror = url.contains("https://mirror/");
        let bytes = if is_mirror || self.wrapper_corrupt {
            Bytes::copy_from_slice(&include_bytes!("../../engine/tests/fixtures/tone.m4a")[..1024])
        } else {
            Bytes::from_static(include_bytes!("../../engine/tests/fixtures/tone.m4a"))
        };
        let codec = if is_mirror {
            "alac".to_owned()
        } else {
            self.wrapper_codec.clone()
        };
        let content_length = bytes.len() as u64;
        let body: ByteStream = Box::pin(stream::once(async move { Ok(bytes) }));
        Ok(StreamHttpResponse {
            status: 200,
            codec: Some(codec),
            bit_depth: Some("24".to_owned()),
            sample_rate: Some("96000".to_owned()),
            content_length: Some(content_length),
            body: Some(body),
        })
    }
}

fn progress_log() -> (ProgressCallback, Arc<Mutex<Vec<RipActivity>>>) {
    let messages = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&messages);
    let callback: ProgressCallback = Arc::new(move |activity| {
        captured.lock().unwrap().push(activity);
    });
    (callback, messages)
}

async fn wrapper_m3u8_404_server() -> (String, Arc<AtomicUsize>, tokio::task::JoinHandle<()>) {
    use tokio::io::AsyncWriteExt;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let requests = Arc::new(AtomicUsize::new(0));
    let request_count = Arc::clone(&requests);
    let server = tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                break;
            };
            request_count.fetch_add(1, Ordering::SeqCst);
            socket
                .write_all(
                    b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .await
                .ok();
        }
    });
    (format!("http://{address}/wrapper-lite"), requests, server)
}

async fn wrapper_license_404_server() -> (String, Arc<AtomicUsize>, tokio::task::JoinHandle<()>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let license_requests = Arc::new(AtomicUsize::new(0));
    let license_request_count = Arc::clone(&license_requests);
    let server = tokio::spawn(async move {
        let master = concat!(
            "#EXTM3U\n",
            "#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"audio-stereo-256\",AUTOSELECT=YES,CHANNELS=\"2\",NAME=\"song\"\n",
            "#EXT-X-STREAM-INF:BANDWIDTH=128000,CODECS=\"mp4a.40.2\",AUDIO=\"audio-stereo-256\"\n",
            "media.m3u8\n"
        );
        let media = concat!(
            "#EXTM3U\n",
            "#EXT-X-KEY:METHOD=ISO-23001-7,URI=\"data:;base64,AAAAAAAAAAAAAAAAAAAAAA==\"\n",
            "#EXT-X-MAP:URI=\"/audio.mp4\",BYTERANGE=\"1037@0\"\n",
            "#EXTINF:1,\n",
            "#EXT-X-BYTERANGE:1@1037\n",
            "/audio.mp4\n"
        );
        let m3u8 =
            format!(r#"{{"code":0,"msg":"","data":{{"m3u8":"http://{address}/master.m3u8"}}}}"#);

        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                break;
            };
            let mut request = Vec::new();
            let mut buffer = [0; 1024];
            while let Ok(read) = socket.read(&mut buffer).await {
                if read == 0 {
                    break;
                }
                request.extend_from_slice(&buffer[..read]);
                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }
            let request = String::from_utf8_lossy(&request);
            let path = request.lines().next().unwrap_or_default();
            let (status, content_type, body) = if path.starts_with("GET /wrapper-lite/m3u8") {
                ("200 OK", "application/json", m3u8.as_str())
            } else if path.starts_with("GET /master.m3u8") {
                ("200 OK", "application/vnd.apple.mpegurl", master)
            } else if path.starts_with("GET /media.m3u8") {
                ("200 OK", "application/vnd.apple.mpegurl", media)
            } else if path.starts_with("POST /wrapper-lite/license") {
                license_request_count.fetch_add(1, Ordering::SeqCst);
                ("404 Not Found", "text/plain", "")
            } else {
                ("404 Not Found", "text/plain", "")
            };
            let response = format!(
                "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            socket.write_all(response.as_bytes()).await.ok();
        }
    });
    (
        format!("http://{address}/wrapper-lite"),
        license_requests,
        server,
    )
}

fn acquisition(
    failures: &[&str],
    mirror: FakeMirror,
    wrapper_url: Option<&str>,
    kind: WrapperKind,
    rounds: u32,
) -> (
    AppleStreamAcquisition<FakeStream, FakeMirror>,
    Arc<Mutex<Vec<String>>>,
) {
    acquisition_with_retry_config(failures, mirror, wrapper_url, kind, rounds, 0)
}

fn acquisition_with_retry_config(
    failures: &[&str],
    mirror: FakeMirror,
    wrapper_url: Option<&str>,
    kind: WrapperKind,
    rounds: u32,
    retry_base_delay_ms: u64,
) -> (
    AppleStreamAcquisition<FakeStream, FakeMirror>,
    Arc<Mutex<Vec<String>>>,
) {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let policy = MirrorPolicyManager::new(mirror, None);
    let stream = FakeStream::new(failures, Arc::clone(&calls));
    (
        AppleStreamAcquisition::with_config(
            StreamTransport::new(stream),
            policy,
            wrapper_url.map(str::to_owned),
            None,
            kind,
            AppleAcquisitionConfig {
                retry_rounds: rounds,
                retry_base_delay_ms,
            },
        ),
        calls,
    )
}

fn test_track_meta() -> TrackMeta {
    TrackMeta {
        id: "42".to_owned(),
        title: "Title".to_owned(),
        artist: "Artist".to_owned(),
        album: "Album".to_owned(),
        album_artist: "Artist".to_owned(),
        genre: None,
        release_date: String::new(),
        composer: None,
        track_number: None,
        track_count: None,
        disc_number: None,
        disc_count: None,
        duration_secs: 240,
        explicit: false,
        content_advisory: None,
        artwork_url: String::new(),
        album_id: None,
        artist_id: None,
        isrc: None,
        record_label: None,
        copyright: None,
        upc: None,
        is_streamable: None,
    }
}

struct FallbackRipStage {
    acquisition: AppleStreamAcquisition<CorruptMirrorStream, FakeMirror>,
    reports: Mutex<Vec<(SourceId, SourceFailureKind, String)>>,
}

impl RipStage for FallbackRipStage {
    async fn track_meta(&self, track_id: &str, storefront: &str) -> Result<TrackMeta, RipError> {
        let _ = (track_id, storefront);
        Ok(test_track_meta())
    }

    async fn connect_stream(
        &self,
        track_id: &str,
        meta: &TrackMeta,
        signal: Option<CancellationToken>,
        on_progress: Option<ProgressCallback>,
        codec_preference: apple::CodecPreference,
    ) -> Result<AudioStreamSource, RipError> {
        map_acquisition_outcome(
            self.acquisition
                .connect_stream(track_id, meta, signal, on_progress, codec_preference)
                .await,
        )
    }

    fn observe_stream_failure(&self, source: &SourceId, kind: SourceFailureKind, detail: &str) {
        self.reports
            .lock()
            .unwrap()
            .push((source.clone(), kind, detail.to_owned()));
        if matches!(source, SourceId::PrimaryMirror) {
            self.acquisition.mirror_policy().record_failure(detail);
        }
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

fn fallback_rip_stage(
    mirror_available: bool,
    wrapper_codec: &str,
    wrapper_corrupt: bool,
    calls: Arc<Mutex<Vec<String>>>,
) -> FallbackRipStage {
    let policy = MirrorPolicyManager::new(
        FakeMirror {
            available: mirror_available,
        },
        None,
    );
    let acquisition = AppleStreamAcquisition::with_config(
        StreamTransport::new(CorruptMirrorStream::new(
            calls,
            wrapper_codec,
            wrapper_corrupt,
        )),
        policy,
        Some("https://wrapper".to_owned()),
        None,
        WrapperKind::Endpoints,
        AppleAcquisitionConfig {
            retry_rounds: 1,
            retry_base_delay_ms: 0,
        },
    );
    FallbackRipStage {
        acquisition,
        reports: Mutex::new(Vec::new()),
    }
}

#[tokio::test]
async fn transient_mirror_failure_reuses_resolved_endpoint_on_retry_round() {
    let (acquisition, calls) = acquisition(
        &["mirror/api/stream/42"],
        FakeMirror { available: true },
        None,
        WrapperKind::Native,
        2,
    );
    let source = match acquisition
        .connect_stream(
            "42",
            &test_track_meta(),
            None,
            None,
            apple::CodecPreference::HighestQuality,
        )
        .await
        .unwrap()
    {
        AcquisitionOutcome::Stream(source) => source,
        AcquisitionOutcome::RenditionAbsent => panic!("primary stream must be available"),
    };
    assert_eq!(source.source, SourceId::PrimaryMirror);
    assert_eq!(calls.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn wrapper_candidates_are_tried_in_order_after_mirror_failure() {
    let (acquisition, calls) = acquisition(
        &["/api/stream/42"],
        FakeMirror { available: false },
        Some("https://wrapper///"),
        WrapperKind::Endpoints,
        1,
    );
    acquisition
        .connect_stream(
            "42",
            &test_track_meta(),
            None,
            None,
            apple::CodecPreference::HighestQuality,
        )
        .await
        .unwrap();
    assert_eq!(
        *calls.lock().unwrap(),
        vec![
            "https://wrapper/api/stream/42".to_owned(),
            "https://wrapper/stream/42".to_owned(),
        ]
    );
}

#[tokio::test]
async fn highest_quality_endpoint_wrapper_returns_non_ec3_stream() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let policy = MirrorPolicyManager::new(FakeMirror { available: false }, None);
    let stream = FakeStream::with_success_codecs(&["mp4a.40.2"], Arc::clone(&calls));
    let acquisition = AppleStreamAcquisition::with_config(
        StreamTransport::new(stream),
        policy,
        Some("https://wrapper".to_owned()),
        None,
        WrapperKind::Endpoints,
        AppleAcquisitionConfig {
            retry_rounds: 1,
            retry_base_delay_ms: 0,
        },
    );

    let outcome = acquisition
        .connect_stream(
            "42",
            &test_track_meta(),
            None,
            None,
            apple::CodecPreference::HighestQuality,
        )
        .await
        .expect("a non-EC-3 endpoint stream is valid for highest quality");

    match outcome {
        AcquisitionOutcome::Stream(source) => {
            assert_eq!(source.codec, "mp4a.40.2");
            assert!(matches!(
                source.source,
                SourceId::WrapperCandidate { endpoint }
                    if endpoint == "https://wrapper/api/stream/42"
            ));
        }
        AcquisitionOutcome::RenditionAbsent => {
            panic!("a primary request must not report rendition absence")
        }
    }
    assert_eq!(
        *calls.lock().unwrap(),
        vec!["https://wrapper/api/stream/42".to_owned()]
    );
}

#[tokio::test]
async fn wrapper_failures_are_aggregated_in_candidate_order() {
    let (acquisition, _) = acquisition(
        &["/api/stream/42", "/stream/42"],
        FakeMirror { available: false },
        Some("https://wrapper"),
        WrapperKind::Endpoints,
        1,
    );
    let error = acquisition
        .connect_stream(
            "42",
            &test_track_meta(),
            None,
            None,
            apple::CodecPreference::HighestQuality,
        )
        .await
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "Failed to stream audio from all sources. All streaming endpoints failed for track 42. Errors: Wrapper candidate (https://wrapper/api/stream/42) failed: transient; Wrapper candidate (https://wrapper/stream/42) failed: transient"
    );
}

#[tokio::test]
async fn cancellation_does_not_retry_or_open_primary_circuit() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let policy = MirrorPolicyManager::new(FakeMirror { available: true }, None);
    let stream = FakeStream {
        calls: Arc::clone(&calls),
        failures: Mutex::new(Vec::new()),
        success_codecs: Mutex::new(Vec::new()),
        cancel_on_fetch: true,
        cancel_after_second_candidate: false,
    };
    let acquisition = AppleStreamAcquisition::with_config(
        StreamTransport::new(stream),
        policy,
        None,
        None,
        WrapperKind::Native,
        AppleAcquisitionConfig {
            retry_rounds: 4,
            retry_base_delay_ms: 0,
        },
    );
    let signal = CancellationToken::new();
    let error = acquisition
        .connect_stream(
            "42",
            &test_track_meta(),
            Some(signal),
            None,
            apple::CodecPreference::HighestQuality,
        )
        .await
        .unwrap_err();

    assert!(error.to_string().contains("Download was cancelled"));
    assert_eq!(
        calls.lock().unwrap().len(),
        1,
        "the canceled primary is not retried"
    );
    assert!(
        !acquisition.mirror_policy().is_circuit_open(),
        "cancellation is not a mirror failure"
    );
}

#[tokio::test]
async fn primary_failure_opens_circuit_and_primary_success_clears_it() {
    let (failed_acquisition, _) = acquisition(
        &["mirror/api/stream/42"],
        FakeMirror { available: true },
        None,
        WrapperKind::Native,
        1,
    );
    failed_acquisition
        .connect_stream(
            "42",
            &test_track_meta(),
            None,
            None,
            apple::CodecPreference::HighestQuality,
        )
        .await
        .unwrap_err();
    assert!(failed_acquisition.mirror_policy().is_circuit_open());

    let calls = Arc::new(Mutex::new(Vec::new()));
    let policy = MirrorPolicyManager::new(
        FakeMirror { available: true },
        Some(("https://mirror/".to_owned(), "key".to_owned())),
    );
    policy.record_failure("previous primary failure");
    let successful_acquisition = AppleStreamAcquisition::with_config(
        StreamTransport::new(FakeStream::new(&[], Arc::clone(&calls))),
        policy,
        None,
        None,
        WrapperKind::Native,
        AppleAcquisitionConfig {
            retry_rounds: 1,
            retry_base_delay_ms: 0,
        },
    );
    successful_acquisition
        .connect_stream(
            "42",
            &test_track_meta(),
            None,
            None,
            apple::CodecPreference::HighestQuality,
        )
        .await
        .unwrap();
    assert!(!successful_acquisition.mirror_policy().is_circuit_open());
}

#[tokio::test]
async fn fallback_reports_progress_when_primary_is_unavailable() {
    let (acquisition, _) = acquisition(
        &[],
        FakeMirror { available: false },
        Some("https://wrapper"),
        WrapperKind::Endpoints,
        1,
    );
    let (callback, messages) = progress_log();
    acquisition
        .connect_stream(
            "42",
            &test_track_meta(),
            None,
            Some(callback),
            apple::CodecPreference::HighestQuality,
        )
        .await
        .unwrap();

    assert_eq!(
        *messages.lock().unwrap(),
        vec![RipActivity::Connecting {
            track: TrackLabel::new("Title", "Artist")
        }]
    );
}

#[tokio::test]
async fn corrupt_primary_body_marks_mirror_and_retries_with_wrapper() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let stage = fallback_rip_stage(true, "alac", false, Arc::clone(&calls));
    let output_dir = tempfile::tempdir().unwrap();
    let ripper = AlacTrackRipper::new(RipperConfig {
        default_output_dir: output_dir.path().to_owned(),
        max_retries: 1,
        base_delay_ms: 0,
        ..RipperConfig::default()
    });

    let result = ripper
        .rip(&stage, "42", RipOptions::new(Provider::Apple, "us"))
        .await
        .expect("wrapper should succeed after corrupt mirror output");

    assert_eq!(result.title, "Title");
    assert_eq!(
        *calls.lock().unwrap(),
        vec![
            "https://mirror/api/stream/42".to_owned(),
            "https://wrapper/api/stream/42".to_owned(),
        ]
    );
    assert!(stage.acquisition.mirror_policy().is_circuit_open());
    let reports = stage.reports.lock().unwrap();
    assert_eq!(reports.len(), 1);
    assert_eq!(reports[0].0, SourceId::PrimaryMirror);
    assert_eq!(reports[0].1, SourceFailureKind::MediaValidation);
    assert!(!reports[0].2.is_empty());
}

#[tokio::test]
async fn wrapper_corruption_does_not_fallback_or_poison_mirror() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let stage = fallback_rip_stage(true, "ec-3", true, Arc::clone(&calls));
    let output_dir = tempfile::tempdir().unwrap();
    let ripper = AlacTrackRipper::new(RipperConfig {
        default_output_dir: output_dir.path().to_owned(),
        max_retries: 1,
        base_delay_ms: 0,
        ..RipperConfig::default()
    });

    let error = ripper
        .rip(
            &stage,
            "42",
            RipOptions::new(Provider::Apple, "us")
                .with_codec_preference(apple::CodecPreference::Atmos),
        )
        .await
        .expect_err("wrapper corruption should remain a failed rip");

    assert!(matches!(
        error,
        RipError::Decode {
            source: Some(SourceId::WrapperCandidate { endpoint }),
            detail,
        } if endpoint == "https://wrapper/api/stream/42" && !detail.is_empty()
    ));
    assert!(!stage.acquisition.mirror_policy().is_circuit_open());
    assert!(
        calls
            .lock()
            .unwrap()
            .iter()
            .all(|url| url.starts_with("https://wrapper/"))
    );
    assert!(
        stage
            .reports
            .lock()
            .unwrap()
            .iter()
            .all(|(source, _, _)| { matches!(source, SourceId::WrapperCandidate { .. }) })
    );
}

#[tokio::test]
async fn atmos_wrapper_m3u8_404_returns_absence_without_retry() {
    let (wrapper_url, requests, server) = wrapper_m3u8_404_server().await;
    let (acquisition, stream_calls) = acquisition(
        &[],
        FakeMirror { available: false },
        Some(&wrapper_url),
        WrapperKind::Native,
        3,
    );
    let (callback, messages) = progress_log();
    let outcome = acquisition
        .connect_stream(
            "42",
            &test_track_meta(),
            None,
            Some(callback),
            apple::CodecPreference::Atmos,
        )
        .await
        .expect("an Atmos wrapper 404 is optional absence");

    assert!(matches!(outcome, AcquisitionOutcome::RenditionAbsent));
    assert_eq!(requests.load(Ordering::SeqCst), 1);
    assert!(stream_calls.lock().unwrap().is_empty());
    assert!(
        messages
            .lock()
            .unwrap()
            .iter()
            .all(|activity| matches!(activity, RipActivity::Connecting { .. }))
    );
    server.abort();
}

#[tokio::test]
async fn primary_wrapper_m3u8_404_is_permanent_without_retry() {
    let (wrapper_url, requests, server) = wrapper_m3u8_404_server().await;
    let (acquisition, stream_calls) = acquisition(
        &[],
        FakeMirror { available: false },
        Some(&wrapper_url),
        WrapperKind::Native,
        3,
    );
    let error = acquisition
        .connect_stream(
            "42",
            &test_track_meta(),
            None,
            None,
            apple::CodecPreference::HighestQuality,
        )
        .await
        .expect_err("a primary wrapper 404 remains a permanent failure");

    assert!(matches!(
        error,
        StreamError::Permanent(message) if message.contains("/m3u8") && message.contains("404")
    ));
    assert_eq!(requests.load(Ordering::SeqCst), 1);
    assert!(stream_calls.lock().unwrap().is_empty());
    server.abort();
}

#[tokio::test]
async fn wrapper_license_404_remains_retryable_technical_failure() {
    let (wrapper_url, license_requests, server) = wrapper_license_404_server().await;
    let (acquisition, _) = acquisition(
        &[],
        FakeMirror { available: false },
        Some(&wrapper_url),
        WrapperKind::Native,
        3,
    );
    let error = acquisition
        .connect_stream(
            "42",
            &test_track_meta(),
            None,
            None,
            apple::CodecPreference::HighestQuality,
        )
        .await
        .expect_err("a license 404 is a technical failure, not track absence");

    assert!(matches!(
        error,
        StreamError::Message(message) if message.contains("/license") && message.contains("404")
    ));
    assert_eq!(license_requests.load(Ordering::SeqCst), 3);
    server.abort();
}

#[tokio::test]
async fn wrapper_service_offline_is_typed_without_retry() {
    // Bind an ephemeral port then drop the listener: nothing is listening,
    // so the wrapper-lite connection is refused.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let wrapper_url = format!("http://{address}/wrapper-lite");
    let (acquisition, _) = acquisition(
        &[],
        FakeMirror { available: false },
        Some(&wrapper_url),
        WrapperKind::Native,
        3,
    );
    let error = acquisition
        .connect_stream(
            "42",
            &test_track_meta(),
            None,
            None,
            apple::CodecPreference::HighestQuality,
        )
        .await
        .expect_err("an unreachable wrapper service is a typed offline failure");

    assert!(
        matches!(error, StreamError::SourceOffline { source } if matches!(
            source,
            SourceId::WrapperLite { .. }
        ))
    );
}

#[tokio::test]
async fn wrapper_authentication_rejection_is_typed() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                break;
            };
            let mut request = Vec::new();
            let mut buffer = [0; 1024];
            while let Ok(read) = socket.read(&mut buffer).await {
                if read == 0 {
                    break;
                }
                request.extend_from_slice(&buffer[..read]);
                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }
            socket
                .write_all(
                    b"HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .await
                .ok();
        }
    });
    let wrapper_url = format!("http://{address}/wrapper-lite");
    let (acquisition, _) = acquisition(
        &[],
        FakeMirror { available: false },
        Some(&wrapper_url),
        WrapperKind::Native,
        3,
    );
    let error = acquisition
        .connect_stream(
            "42",
            &test_track_meta(),
            None,
            None,
            apple::CodecPreference::HighestQuality,
        )
        .await
        .expect_err("a 401 from the wrapper is a typed authentication failure");

    eprintln!("ACTUAL_ERROR={error:?}");
    assert!(
        matches!(error, StreamError::Authentication { source } if matches!(
            source,
            SourceId::WrapperLite { .. }
        ))
    );
    server.abort();
}

#[tokio::test]
async fn primary_wrapper_m3u8_404_maps_to_track_unavailable_in_rip_error() {
    let (wrapper_url, requests, server) = wrapper_m3u8_404_server().await;
    let (acquisition, _) = acquisition(
        &[],
        FakeMirror { available: false },
        Some(&wrapper_url),
        WrapperKind::Native,
        3,
    );
    let error = acquisition
        .connect_stream(
            "42",
            &test_track_meta(),
            None,
            None,
            apple::CodecPreference::HighestQuality,
        )
        .await
        .expect_err("a primary wrapper 404 is typed track absence");

    let rip_error = RipError::from(error);
    assert!(matches!(
        rip_error,
        RipError::TrackUnavailable { reason } if reason.contains("/m3u8") && reason.contains("404")
    ));
    assert_eq!(requests.load(Ordering::SeqCst), 1);
    server.abort();
}

#[tokio::test]
async fn atmos_wrapper_m3u8_404_returns_public_absence() {
    let (wrapper_url, requests, server) = wrapper_m3u8_404_server().await;
    let (acquisition, _) = acquisition(
        &[],
        FakeMirror { available: false },
        Some(&wrapper_url),
        WrapperKind::Native,
        3,
    );
    let outcome = acquisition
        .connect_stream(
            "42",
            &test_track_meta(),
            None,
            None,
            apple::CodecPreference::Atmos,
        )
        .await
        .expect("an Atmos wrapper 404 is optional absence");

    assert!(matches!(outcome, AcquisitionOutcome::RenditionAbsent));
    assert_eq!(requests.load(Ordering::SeqCst), 1);
    server.abort();
}

#[tokio::test]
async fn atmos_absence_maps_to_rendition_unavailable_in_rip_stage() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let stage = fallback_rip_stage(false, "mp4a.40.2", false, calls);
    let error = stage
        .connect_stream(
            "42",
            &test_track_meta(),
            None,
            None,
            apple::CodecPreference::Atmos,
        )
        .await
        .expect_err("Atmos absence should map to a typed rip error");

    assert!(matches!(
        error,
        RipError::RenditionUnavailable { reason }
            if reason == "Dolby Atmos is unavailable"
    ));
}

#[tokio::test]
async fn wrapper_source_id_carries_url_in_display() {
    let source = SourceId::WrapperLite {
        url: "http://127.0.0.1:12340".to_owned(),
    };
    assert_eq!(source.to_string(), "wrapper-lite (http://127.0.0.1:12340)");
    assert_eq!(
        SourceId::WrapperCandidate {
            endpoint: "https://wrapper/api/stream/42".to_owned()
        }
        .to_string(),
        "wrapper candidate (https://wrapper/api/stream/42)"
    );
    assert_eq!(SourceId::PrimaryMirror.to_string(), "primary mirror");
}

#[tokio::test]
async fn atmos_all_non_ec3_wrapper_candidates_are_unavailable_without_retry() {
    let (acquisition, calls) = acquisition(
        &[],
        FakeMirror { available: false },
        Some("https://wrapper"),
        WrapperKind::Endpoints,
        3,
    );
    let outcome = acquisition
        .connect_stream(
            "42",
            &test_track_meta(),
            None,
            None,
            apple::CodecPreference::Atmos,
        )
        .await
        .expect("all non-EC-3 candidates must be unavailable");

    assert!(matches!(outcome, AcquisitionOutcome::RenditionAbsent));
    assert_eq!(calls.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn atmos_non_ec3_candidate_does_not_hide_technical_wrapper_failure() {
    let (acquisition, calls) = acquisition(
        &["/api/stream/42", "/api/stream/42"],
        FakeMirror { available: false },
        Some("https://wrapper"),
        WrapperKind::Endpoints,
        2,
    );
    let error = acquisition
        .connect_stream(
            "42",
            &test_track_meta(),
            None,
            None,
            apple::CodecPreference::Atmos,
        )
        .await
        .expect_err("a technical candidate failure must remain retryable");

    assert!(matches!(error, StreamError::Message(message) if message.contains("transient")));
    assert_eq!(calls.lock().unwrap().len(), 4);
}

#[tokio::test]
async fn cancellation_after_non_ec3_candidate_is_not_typed_unavailable() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let policy = MirrorPolicyManager::new(FakeMirror { available: false }, None);
    let stream = FakeStream {
        calls: Arc::clone(&calls),
        failures: Mutex::new(Vec::new()),
        success_codecs: Mutex::new(vec!["mp4a.40.2".to_owned(), "mp4a.40.2".to_owned()]),
        cancel_on_fetch: false,
        cancel_after_second_candidate: true,
    };
    let acquisition = AppleStreamAcquisition::with_config(
        StreamTransport::new(stream),
        policy,
        Some("https://wrapper".to_owned()),
        None,
        WrapperKind::Endpoints,
        AppleAcquisitionConfig {
            retry_rounds: 3,
            retry_base_delay_ms: 0,
        },
    );
    let signal = CancellationToken::new();
    let error = acquisition
        .connect_stream(
            "42",
            &test_track_meta(),
            Some(signal),
            None,
            apple::CodecPreference::Atmos,
        )
        .await
        .expect_err("cancellation must not become optional absence");

    assert!(matches!(error, StreamError::Cancelled));
    assert_eq!(calls.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn atmos_non_ec3_wrapper_candidate_falls_back_to_ec3_candidate() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let policy = MirrorPolicyManager::new(FakeMirror { available: false }, None);
    let stream = FakeStream::with_success_codecs(&["mp4a.40.2", "ec-3"], Arc::clone(&calls));
    let acquisition = AppleStreamAcquisition::with_config(
        StreamTransport::new(stream),
        policy,
        Some("https://wrapper".to_owned()),
        None,
        WrapperKind::Endpoints,
        AppleAcquisitionConfig {
            retry_rounds: 1,
            retry_base_delay_ms: 0,
        },
    );

    let source = match acquisition
        .connect_stream(
            "42",
            &test_track_meta(),
            None,
            None,
            apple::CodecPreference::Atmos,
        )
        .await
        .expect("the second wrapper candidate is a valid Atmos stream")
    {
        AcquisitionOutcome::Stream(source) => source,
        AcquisitionOutcome::RenditionAbsent => panic!("the second candidate is EC-3"),
    };
    assert_eq!(source.codec, "ec-3");
    assert_eq!(calls.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn injected_retry_config_controls_exponential_backoff_and_rounds() {
    tokio::time::pause();
    let (acquisition, calls) = acquisition_with_retry_config(
        &["mirror/api/stream/42", "mirror/api/stream/42"],
        FakeMirror { available: true },
        None,
        WrapperKind::Native,
        3,
        100,
    );
    let (callback, messages) = progress_log();
    acquisition
        .connect_stream(
            "42",
            &test_track_meta(),
            None,
            Some(callback),
            apple::CodecPreference::HighestQuality,
        )
        .await
        .unwrap();

    assert_eq!(calls.lock().unwrap().len(), 3);
    assert!(messages.lock().unwrap().is_empty());
}
