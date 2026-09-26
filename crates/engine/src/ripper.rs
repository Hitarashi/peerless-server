//! Single-track ripper: metadata → stream → raw file → tagged M4A, with
//! retries, cancellation, and source-failure circuit reporting.

use std::{
    collections::BTreeMap,
    fmt,
    future::Future,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use futures_util::{FutureExt, StreamExt};
use lyrics::{LyricsHttp, LyricsLookup, LyricsRegistry};
use music::{CodecPreference, Provider};
use tokio::io::AsyncWriteExt;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::{
    filename::StandardFilename,
    orchestrator::types::{ByteProgress, RipActivity, TrackLabel},
    streaming::{AudioStreamSource, ProgressCallback, SourceId, StreamError},
    tagger,
    types::{TrackMeta, TrackRipResult},
};

#[derive(Debug, Clone)]
pub enum RipError {
    Cancelled,
    TrackUnavailable {
        reason: String,
    },
    RenditionUnavailable {
        reason: String,
    },
    LocalIo {
        message: String,
    },
    SourceOffline {
        source: SourceId,
    },
    StreamCorrupt {
        source: SourceId,
        detail: String,
    },
    IncompleteBody {
        source: SourceId,
        expected: u64,
        received: u64,
    },
    StreamStalled {
        source: SourceId,
        secs: u64,
    },
    Timeout {
        source: Option<SourceId>,
        detail: String,
    },
    Authentication {
        source: SourceId,
    },
    PlaylistParse {
        which: &'static str,
        detail: String,
    },
    License {
        detail: String,
    },
    Decrypt {
        detail: String,
    },
    Decode {
        source: Option<SourceId>,
        detail: String,
    },
    Message(String),
}

impl std::fmt::Display for RipError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Cancelled => formatter.write_str("Download was cancelled"),
            Self::TrackUnavailable { reason } => write!(formatter, "track unavailable: {reason}"),
            Self::RenditionUnavailable { reason } => {
                write!(formatter, "rendition unavailable: {reason}")
            }
            Self::LocalIo { message } => write!(formatter, "local file error: {message}"),
            Self::SourceOffline { source } => write!(formatter, "{source} is offline"),
            Self::StreamCorrupt { source, detail } => {
                write!(formatter, "stream corrupt on {source}: {detail}")
            }
            Self::IncompleteBody {
                source,
                expected,
                received,
            } => write!(
                formatter,
                "incomplete audio body from {source}: expected {expected} bytes, received {received}"
            ),
            Self::StreamStalled { source, secs } => write!(
                formatter,
                "audio stream stalled on {source}: no data received for {secs}s"
            ),
            Self::Timeout { detail, .. } => write!(formatter, "operation timed out: {detail}"),
            Self::Authentication { source } => {
                write!(formatter, "authentication failed on {source}")
            }
            Self::PlaylistParse { which, detail } => {
                write!(formatter, "{which} playlist parse failed: {detail}")
            }
            Self::License { detail } => write!(formatter, "license error: {detail}"),
            Self::Decrypt { detail } => write!(formatter, "decrypt failed: {detail}"),
            Self::Decode { detail, .. } => write!(formatter, "audio decode failed: {detail}"),
            Self::Message(message) => formatter.write_str(message),
        }
    }
}

impl std::error::Error for RipError {}

/// Failure classes that can be attributed to an acquired stream source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceFailureKind {
    Stream,
    IncompleteBody,
    MediaValidation,
}

impl From<StreamError> for RipError {
    fn from(error: StreamError) -> Self {
        match error {
            StreamError::Message(message) => RipError::Message(message),
            StreamError::Permanent(reason) => RipError::TrackUnavailable { reason },
            StreamError::Unavailable(reason) => RipError::RenditionUnavailable { reason },
            StreamError::Timeout { source, secs } => RipError::Timeout {
                source: Some(source),
                detail: format!("stream handshake timed out after {secs}s"),
            },
            StreamError::Authentication { source } => RipError::Authentication { source },
            StreamError::SourceOffline { source } => RipError::SourceOffline { source },
            StreamError::PlaylistParse { which, detail } => {
                RipError::PlaylistParse { which, detail }
            }
            StreamError::License { detail } => RipError::License { detail },
            StreamError::Decrypt { detail } => RipError::Decrypt { detail },
            StreamError::IncompleteBody {
                source,
                expected,
                received,
            } => RipError::IncompleteBody {
                source,
                expected,
                received,
            },
            StreamError::Cancelled => RipError::Cancelled,
        }
    }
}

impl From<std::io::Error> for RipError {
    fn from(error: std::io::Error) -> Self {
        RipError::LocalIo {
            message: error.to_string(),
        }
    }
}

pub type RipProgressCallback = Arc<dyn Fn(RipActivity) + Send + Sync>;

/// Configuration knobs (default retries 3, base delay 2s).
#[derive(Clone)]
pub struct RipperConfig {
    pub default_output_dir: PathBuf,
    pub max_retries: u32,
    pub base_delay_ms: u64,
    pub lyrics_registry: Arc<LyricsRegistry>,
    pub lyrics_client: reqwest::Client,
    pub lyrics_timeout: Duration,
    pub artwork_client: reqwest::Client,
    pub artwork_timeout: Duration,
}

impl fmt::Debug for RipperConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RipperConfig")
            .field("default_output_dir", &self.default_output_dir)
            .field("max_retries", &self.max_retries)
            .field("base_delay_ms", &self.base_delay_ms)
            .field("lyrics_timeout", &self.lyrics_timeout)
            .field("artwork_timeout", &self.artwork_timeout)
            .finish_non_exhaustive()
    }
}

impl Default for RipperConfig {
    fn default() -> Self {
        Self {
            default_output_dir: std::env::current_dir()
                .unwrap_or_else(|_| PathBuf::from("."))
                .join("bot-data")
                .join("downloads"),
            max_retries: 3,
            base_delay_ms: 2000,
            lyrics_registry: Arc::new(LyricsRegistry::default()),
            lyrics_client: reqwest::Client::new(),
            lyrics_timeout: Duration::from_secs(5),
            artwork_client: reqwest::Client::new(),
            artwork_timeout: Duration::from_secs(15),
        }
    }
}

/// Provider-owned acquisition and metadata stage used by the generic ripper.
pub trait RipStage: Send + Sync {
    fn track_meta(
        &self,
        track_id: &str,
        storefront: &str,
    ) -> impl Future<Output = Result<TrackMeta, RipError>> + Send;
    fn connect_stream(
        &self,
        track_id: &str,
        meta: &TrackMeta,
        signal: Option<CancellationToken>,
        on_progress: Option<ProgressCallback>,
        codec_preference: CodecPreference,
    ) -> impl Future<Output = Result<AudioStreamSource, RipError>> + Send;
    fn observe_stream_failure(&self, source: &SourceId, kind: SourceFailureKind, detail: &str);
    fn track_tags(&self, meta: &TrackMeta) -> media::TrackTags;
}

struct ReqwestLyricsHttp {
    client: reqwest::Client,
    timeout: Duration,
}

impl LyricsHttp for ReqwestLyricsHttp {
    fn get_json<'a>(&'a self, url: &'a str) -> lyrics::LyricsFuture<'a, Option<String>> {
        Box::pin(async move {
            let response = self
                .client
                .get(url)
                .header("User-Agent", "AlacBot/1.0")
                .timeout(self.timeout)
                .send()
                .await
                .ok()?;
            if !response.status().is_success() {
                return None;
            }
            response.text().await.ok()
        })
    }
}

async fn fetch_lyrics(
    client: reqwest::Client,
    timeout: Duration,
    registry: Arc<LyricsRegistry>,
    lookup: LyricsLookup,
) -> Option<String> {
    let http = ReqwestLyricsHttp { client, timeout };
    lyrics::lookup(&http, registry.as_ref(), &lookup).await
}

async fn fetch_artwork_with_client(
    client: reqwest::Client,
    timeout: Duration,
    url: &str,
) -> Option<Vec<u8>> {
    let response = client
        .get(url)
        .header("User-Agent", "AlacBot/1.0")
        .timeout(timeout)
        .send()
        .await
        .ok()?;
    if !response.status().is_success() {
        return None;
    }
    Some(response.bytes().await.ok()?.to_vec())
}

/// Fetch artwork for the orchestrator's album ZIP enrichment seam.
pub async fn fetch_artwork_bytes(config: &RipperConfig, url: &str) -> Option<Vec<u8>> {
    fetch_artwork_with_client(config.artwork_client.clone(), config.artwork_timeout, url).await
}

fn assemble_tags(
    stage: &impl RipStage,
    meta: &TrackMeta,
    cover: Option<&[u8]>,
    lyrics: Option<&str>,
) -> media::TrackTags {
    let mut tags = stage.track_tags(meta);
    tags.lyrics = lyrics.map(str::to_owned).filter(|value| !value.is_empty());
    tags.artwork_jpeg = cover.filter(|value| !value.is_empty()).map(<[u8]>::to_vec);
    tags
}

fn map_media_finalize_error(error: media::MediaError) -> RipError {
    match error {
        media::MediaError::SourceValidation(media::SourceValidationError::Decode(message)) => {
            RipError::Decode {
                source: None,
                detail: message,
            }
        }
        media::MediaError::SourceValidation(media::SourceValidationError::Invalid(message)) => {
            RipError::Decode {
                source: None,
                detail: message,
            }
        }
        media::MediaError::Io(error) => RipError::LocalIo {
            message: error.to_string(),
        },
        error => RipError::Message(error.to_string()),
    }
}

async fn finalize_m4a(
    raw_path: &Path,
    output_path: &Path,
    tags: &media::TrackTags,
) -> Result<(), RipError> {
    let cancellation = CancellationToken::new();
    media::MediaProcessor::new()
        .finalize_m4a(raw_path, output_path, tags, &cancellation)
        .await
        .map(|_| ())
        .map_err(map_media_finalize_error)
}

/// Retrying wrapper around `rip_once`.
pub struct AlacTrackRipper {
    config: RipperConfig,
}

/// Options controlling a track rip operation.
#[derive(Clone)]
pub struct RipOptions<'a> {
    pub provider: Provider,
    pub storefront: &'a str,
    pub on_progress: Option<&'a RipProgressCallback>,
    pub signal: Option<CancellationToken>,
    pub output_dir: Option<&'a Path>,
    pub codec_preference: CodecPreference,
}

impl std::fmt::Debug for RipOptions<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RipOptions")
            .field("provider", &self.provider)
            .field("storefront", &self.storefront)
            .field("signal", &self.signal)
            .field("output_dir", &self.output_dir)
            .field("codec_preference", &self.codec_preference)
            .finish_non_exhaustive()
    }
}

impl<'a> RipOptions<'a> {
    pub fn new(provider: Provider, storefront: &'a str) -> Self {
        Self {
            provider,
            storefront,
            on_progress: None,
            signal: None,
            output_dir: None,
            codec_preference: CodecPreference::HighestQuality,
        }
    }

    pub fn with_progress(mut self, on_progress: &'a RipProgressCallback) -> Self {
        self.on_progress = Some(on_progress);
        self
    }

    pub fn with_signal(mut self, signal: CancellationToken) -> Self {
        self.signal = Some(signal);
        self
    }

    pub fn with_output_dir(mut self, output_dir: &'a Path) -> Self {
        self.output_dir = Some(output_dir);
        self
    }

    pub fn with_codec_preference(mut self, preference: CodecPreference) -> Self {
        self.codec_preference = preference;
        self
    }
}
impl AlacTrackRipper {
    pub fn new(config: RipperConfig) -> Self {
        Self { config }
    }

    pub fn config(&self) -> &RipperConfig {
        &self.config
    }

    pub async fn rip<D: RipStage>(
        &self,
        stage: &D,
        track_id: &str,
        options: RipOptions<'_>,
    ) -> Result<TrackRipResult, RipError> {
        let mut attempt: u32 = 0;

        loop {
            if options.signal.as_ref().is_some_and(|t| t.is_cancelled()) {
                return Err(RipError::Cancelled);
            }

            match self.rip_once(stage, track_id, &options).await {
                Ok(result) => return Ok(result),
                Err(err) => {
                    if matches!(
                        err,
                        RipError::Cancelled
                            | RipError::TrackUnavailable { .. }
                            | RipError::RenditionUnavailable { .. }
                            | RipError::LocalIo { .. }
                            | RipError::SourceOffline { .. }
                    ) {
                        return Err(err);
                    }
                    if options.signal.as_ref().is_some_and(|t| t.is_cancelled()) {
                        return Err(RipError::Cancelled);
                    }
                    if attempt >= self.config.max_retries {
                        return Err(err);
                    }
                    attempt += 1;

                    let raw_delay = self.config.base_delay_ms * 2u64.pow(attempt - 1);
                    let jitter = 0.8 + jitter_fraction() * 0.4;
                    let delay_ms = (raw_delay as f64 * jitter).round() as u64;

                    warn!(
                        track_id,
                        attempt,
                        max_retries = self.config.max_retries,
                        delay_ms,
                        error = %err,
                        "Track rip failed, retrying"
                    );

                    // Abortable sleep: cancellation rejects immediately.
                    if let Some(token) = &options.signal {
                        tokio::select! {
                            _ = tokio::time::sleep(Duration::from_millis(delay_ms)) => {}
                            _ = token.cancelled() => return Err(RipError::Cancelled),
                        }
                    } else {
                        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                    }
                }
            }
        }
    }

    async fn rip_once<D: RipStage>(
        &self,
        stage: &D,
        track_id: &str,
        options: &RipOptions<'_>,
    ) -> Result<TrackRipResult, RipError> {
        let RipOptions {
            on_progress,
            storefront,
            signal,
            output_dir,
            codec_preference,
            provider,
        } = options;
        let on_progress = *on_progress;
        let signal = signal.as_ref();
        let output_dir = *output_dir;
        let codec_preference = *codec_preference;
        let rip_start = std::time::Instant::now();

        if signal.is_some_and(|t| t.is_cancelled()) {
            return Err(RipError::Cancelled);
        }

        let target_dir: &Path = output_dir.unwrap_or(&self.config.default_output_dir);
        tokio::fs::create_dir_all(target_dir).await?;
        let track_dir = target_dir.join(format!(".track_{}", unique_temp_suffix()));
        if let Err(error) = tokio::fs::create_dir_all(&track_dir).await {
            // `create_dir_all` can leave a partially-created directory behind
            // before reporting an error.  Do not leave that staging lane for
            // a later `/clean` invocation to discover.
            let _ = tokio::fs::remove_dir_all(&track_dir).await;
            return Err(error.into());
        }

        // Keep every operation after staging-directory creation inside one
        // result so metadata/connect/cancellation errors get the same cleanup
        // as stream and tagging errors.
        let result: Result<TrackRipResult, RipError> = async {
            emit_progress(on_progress, RipActivity::ResolvingMetadata);
            let meta = stage.track_meta(track_id, storefront).await?;
            let track = TrackLabel::from_meta(&meta);

            if signal.is_some_and(|t| t.is_cancelled()) {
                return Err(RipError::Cancelled);
            }

            emit_progress(
                on_progress,
                RipActivity::Connecting {
                    track: track.clone(),
                },
            );

            // Concurrent prefetch: lyrics + artwork run while the audio streams.
            let lyrics_lookup = LyricsLookup {
                title: meta.title.clone(),
                artists: vec![meta.artist.clone()],
                album: Some(meta.album.clone()).filter(|a| !a.is_empty()),
                duration: Some(meta.duration_secs).filter(|d| *d != 0),
                provider_ids: BTreeMap::from([(provider.as_str().to_owned(), track_id.to_owned())]),
            };
            let lyrics_task = {
                let lookup = lyrics_lookup.clone();
                let track_id = track_id.to_owned();
                let client = self.config.lyrics_client.clone();
                let timeout = self.config.lyrics_timeout;
                let registry = Arc::clone(&self.config.lyrics_registry);
                async move {
                    match fetch_lyrics(client, timeout, registry, lookup).await {
                        Some(l) => {
                            debug!(track_id, found = true, "Lyrics prefetch completed");
                            Some(l)
                        }
                        None => {
                            debug!(track_id, found = false, "Lyrics prefetch completed");
                            None
                        }
                    }
                }
            };
            let artwork_task = {
                let track_id = track_id.to_owned();
                let artwork_url = meta.artwork_url.clone();
                let client = self.config.artwork_client.clone();
                let timeout = self.config.artwork_timeout;
                async move {
                    if artwork_url.is_empty() {
                        return None;
                    }
                    let artwork = fetch_artwork_with_client(client, timeout, &artwork_url).await;
                    debug!(
                        track_id,
                        size_bytes = artwork.as_ref().map_or(0, Vec::len),
                        "Artwork prefetch completed"
                    );
                    artwork
                }
            };

            // Connect the audio stream.
            let stream_start = std::time::Instant::now();
            let stream_progress: Option<ProgressCallback> = on_progress.cloned();
            let mut stream = stage
                .connect_stream(
                    track_id,
                    &meta,
                    signal.cloned(),
                    stream_progress,
                    codec_preference,
                )
                .await?;

            debug!(
                track_id,
                source = %stream.source,
                codec = %stream.codec,
                bit_depth = stream.bit_depth,
                sample_rate = stream.sample_rate,
                "Stream audio specs received"
            );

            let unix_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis())
                .unwrap_or(0);
            let safe_track_id: String = track_id
                .chars()
                .map(|character| {
                    if character.is_ascii_alphanumeric() || matches!(character, '-' | '_') {
                        character
                    } else {
                        '_'
                    }
                })
                .collect();
            // Keep the human-readable track id for diagnostics, but always add a
            // monotonic/time component. Track ids are external input and must not
            // be allowed to select a path or collide within one job.
            let temp_raw_name = format!(
                "stream_{safe_track_id}_{unix_ms}_{}.raw",
                unique_temp_suffix()
            );
            let temp_raw_name = StandardFilename::sanitize_and_bound(&temp_raw_name, Some(".raw"));
            let temp_raw_path = track_dir.join(temp_raw_name);
            // The raw stream is staged in the private lane, but the completed
            // file must live outside it: callers consume this path after `rip`
            // returns, while the lane is removed on every outcome. Reserve the
            // normal human-readable name first, falling back to a unique name
            // rather than clobbering a concurrent rip's output.
            let base_name = tagger::build_track_filename_with_codec(&meta, &stream.codec);
            let mut final_name = base_name.clone();
            let mut final_path = target_dir.join(&final_name);
            loop {
                match tokio::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&final_path)
                    .await
                {
                    Ok(_) => break,
                    Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                        let collision_id = unique_temp_suffix().to_string();
                        final_name = base_name.with_collision_id(&collision_id);
                        final_path = target_dir.join(&final_name);
                    }
                    Err(error) => return Err(error.into()),
                }
            }

            // Detached-into-the-loop prefetch: lyrics + artwork download while
            // the audio streams (polled in the same select! as the stream so
            // failures never wait on them).
            let lyrics_fut = std::pin::pin!(lyrics_task);
            let artwork_fut = std::pin::pin!(artwork_task);
            let mut lyrics_result: Option<Option<String>> = None;
            let mut artwork_result: Option<Option<Vec<u8>>> = None;
            let mut lyrics_fut = lyrics_fut.fuse();
            let mut artwork_fut = artwork_fut.fuse();

            const CHUNK_INACTIVITY_TIMEOUT: Duration = Duration::from_secs(45);

            let result: Result<TrackRipResult, RipError> = async {
                let total = stream.content_length.filter(|len| *len > 0);
                let mut downloaded_bytes = 0u64;
                // Starting at 0 makes the first chunk always emit a progress event.
                let mut last_progress_update = std::time::Instant::now()
                    .checked_sub(Duration::from_secs(10))
                    .unwrap_or_else(std::time::Instant::now);
                let mut file = tokio::fs::File::create(&temp_raw_path).await?;

                loop {
                    if signal.is_some_and(|t| t.is_cancelled()) {
                        return Err(RipError::Cancelled);
                    }

                    let chunk: bytes::Bytes = tokio::select! {
                            lyrics = &mut lyrics_fut => {
                                lyrics_result = Some(lyrics);
                                continue;
                        }
                        artwork = &mut artwork_fut => {
                            artwork_result = Some(artwork);
                            continue;
                        }
                        _ = tokio::time::sleep(CHUNK_INACTIVITY_TIMEOUT) => {
                            if signal.is_some_and(|token| token.is_cancelled()) {
                                return Err(RipError::Cancelled);
                            }
                            let error = RipError::StreamStalled {
                                source: stream.source.clone(),
                                secs: CHUNK_INACTIVITY_TIMEOUT.as_secs(),
                            };
                            stage.observe_stream_failure(
                                &stream.source,
                                SourceFailureKind::Stream,
                                &error.to_string(),
                            );
                            return Err(error);
                        }
                        chunk = stream.stream.next() => match chunk {
                            Some(Ok(bytes)) => bytes,
                            Some(Err(error)) => {
                                if signal.is_some_and(|token| token.is_cancelled()) {
                                    return Err(RipError::Cancelled);
                                }
                                let error = RipError::StreamCorrupt {
                                    source: stream.source.clone(),
                                    detail: error.to_string(),
                                };
                                stage.observe_stream_failure(
                                    &stream.source,
                                    SourceFailureKind::Stream,
                                    &error.to_string(),
                                );
                                return Err(error);
                            }
                            None => break,
                        },
                    };

                    file.write_all(&chunk).await?;
                    downloaded_bytes += chunk.len() as u64;

                    if last_progress_update.elapsed() > Duration::from_secs(1) {
                        last_progress_update = std::time::Instant::now();
                        emit_progress(
                            on_progress,
                            RipActivity::Downloading {
                                track: track.clone(),
                                progress: ByteProgress {
                                    completed: downloaded_bytes,
                                    total,
                                },
                            },
                        );
                    }
                }

                if let Some(expected) = total
                    && downloaded_bytes != expected
                {
                    if signal.is_some_and(|token| token.is_cancelled()) {
                        return Err(RipError::Cancelled);
                    }
                    let error = RipError::IncompleteBody {
                        source: stream.source.clone(),
                        expected,
                        received: downloaded_bytes,
                    };
                    stage.observe_stream_failure(
                        &stream.source,
                        SourceFailureKind::IncompleteBody,
                        &error.to_string(),
                    );
                    return Err(error);
                }

                file.flush().await?;

                if signal.is_some_and(|t| t.is_cancelled()) {
                    return Err(RipError::Cancelled);
                }

                debug!(
                    track_id,
                    source = %stream.source,
                    downloaded_bytes,
                    stream_duration_ms = stream_start.elapsed().as_millis() as u64,
                    "Stream download completed"
                );

                emit_progress(
                    on_progress,
                    RipActivity::Tagging {
                        track: track.clone(),
                    },
                );
                let tag_start = std::time::Instant::now();

                // Reap any prefetches that outlived the stream.
                if lyrics_result.is_none() {
                    lyrics_result = Some((&mut lyrics_fut).await);
                }
                if artwork_result.is_none() {
                    artwork_result = Some((&mut artwork_fut).await);
                }
                let lyrics = lyrics_result.flatten();
                let cover = artwork_result.flatten();

                if signal.is_some_and(|t| t.is_cancelled()) {
                    return Err(RipError::Cancelled);
                }

                // Only source validation produces `Decode`: the stage cannot
                // know the stream source, so the ripper attaches it here.
                // Post-tag failures arrive as `Message`/`LocalIo` and never
                // trip the circuit.
                let tags = assemble_tags(stage, &meta, cover.as_deref(), lyrics.as_deref());
                match finalize_m4a(&temp_raw_path, &final_path, &tags).await {
                    Ok(()) => {}
                    Err(RipError::Decode { source, detail }) => {
                        if signal.is_some_and(|token| token.is_cancelled()) {
                            return Err(RipError::Cancelled);
                        }
                        let error = RipError::Decode {
                            source: source.or_else(|| Some(stream.source.clone())),
                            detail,
                        };
                        stage.observe_stream_failure(
                            &stream.source,
                            SourceFailureKind::MediaValidation,
                            &error.to_string(),
                        );
                        return Err(error);
                    }
                    Err(error) => return Err(error),
                }

                debug!(
                    track_id,
                    has_lyrics = lyrics.is_some(),
                    has_cover = cover.is_some(),
                    tag_duration_ms = tag_start.elapsed().as_millis() as u64,
                    total_duration_ms = rip_start.elapsed().as_millis() as u64,
                    "Tagging finished"
                );

                Ok(TrackRipResult {
                    file_path: final_path.to_string_lossy().into_owned(),
                    title: meta.title.clone(),
                    artist: meta.artist.clone(),
                    album: meta.album.clone(),
                    duration: meta.duration_secs,
                    bit_depth: stream.bit_depth,
                    sample_rate: stream.sample_rate,
                    codec: stream.codec.clone(),
                    genre: meta
                        .genre
                        .clone()
                        .filter(|g| !g.is_empty())
                        .unwrap_or_else(|| "Unknown".to_owned()),
                    release_date: meta.release_date.clone(),
                    track_number: meta.track_number.unwrap_or(1),
                    track_count: meta.track_count.unwrap_or(1),
                    isrc: meta.isrc.clone().filter(|s| !s.is_empty()),
                })
            }
            .await;

            // Temp raw removed on every exit path; prefetch tasks reaped.  A
            // failed finalizer may also have left a partial public output.
            let _ = tokio::fs::remove_file(&temp_raw_path).await;
            if result.is_err() {
                let _ = tokio::fs::remove_file(&final_path).await;
            }
            result
        }
        .await;

        // The staging lane is private implementation detail, not persistent
        // storage. Remove it after both success and failure (including errors
        // before the inner stream result is constructed).
        let _ = tokio::fs::remove_dir_all(&track_dir).await;
        result
    }
}

fn emit_progress(on_progress: Option<&RipProgressCallback>, activity: RipActivity) {
    if let Some(callback) = on_progress {
        callback(activity);
    }
}

/// Jitter fraction in `0.0..1.0` from a cheap time-seeded xorshift
/// (exact values are never asserted).
fn jitter_fraction() -> f64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static STATE: AtomicU64 = AtomicU64::new(0);
    let mut state = STATE.load(Ordering::Relaxed);
    if state == 0 {
        state = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0x9E3779B97F4A7C15)
            | 1;
    }
    state ^= state << 13;
    state ^= state >> 7;
    state ^= state << 17;
    STATE.store(state, Ordering::Relaxed);
    (state >> 11) as f64 / (1u64 << 53) as f64
}

fn unique_temp_suffix() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(1);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use super::{RipError, map_media_finalize_error};

    #[test]
    fn source_validation_is_attributed_to_the_acquired_stream() {
        let error = map_media_finalize_error(media::MediaError::SourceValidation(
            media::SourceValidationError::Decode("unexpected end of bitstream".to_owned()),
        ));
        assert!(matches!(
            error,
            RipError::Decode { source: None, detail }
                if detail == "unexpected end of bitstream"
        ));
    }

    #[test]
    fn post_tag_failures_do_not_look_like_source_corruption() {
        let decode = map_media_finalize_error(media::MediaError::Decode(
            "post-tag decode failed".to_owned(),
        ));
        assert!(matches!(
            decode,
            RipError::Message(message) if message == "audio decode failed: post-tag decode failed"
        ));

        let invalid = map_media_finalize_error(media::MediaError::Invalid(
            "finalized file has no duration".to_owned(),
        ));
        assert!(matches!(
            invalid,
            RipError::Message(message) if message == "invalid media: finalized file has no duration"
        ));

        let local_io =
            map_media_finalize_error(media::MediaError::Io(std::io::Error::other("disk full")));
        assert!(matches!(
            local_io,
            RipError::LocalIo { message } if message == "disk full"
        ));
    }
}
