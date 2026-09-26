//! Apple Music audio acquisition and source failover.
//!
//! This is the provider boundary used by the generic engine ripper. It owns
//! the Apple mirror policy, wrapper selection, retry ordering, and the native
//! wrapper path; the engine only sees one `connect_stream` operation.

use std::{sync::Arc, time::Duration};

use engine::{
    orchestrator::types::{RipActivity, TrackLabel},
    streaming::{
        AudioStreamSource, FetchEndpointOptions, ProgressCallback, SourceId, StreamError,
        StreamHttp, StreamTransport,
    },
    types::TrackMeta,
};
use music::CodecPreference;
use tokio_util::sync::CancellationToken;

use crate::{
    ReqwestMirrorHttp,
    mirror_http::MirrorHttp,
    mirror_policy::{MirrorEndpoint, MirrorPolicyManager},
    playlist::ReqwestPlaylistHttp,
    token::DeveloperTokenProvider,
    wrapper::{WrapperEngine, WrapperTrackOutcome, WrapperUnavailableReason},
};

pub struct AppleStreamAcquisition<S: StreamHttp, M: MirrorHttp> {
    stream_transport: StreamTransport<S>,
    mirror_policy: MirrorPolicyManager<M>,
    wrapper_url: Option<String>,
    wrapper_api_key: Option<String>,
    wrapper_kind: WrapperKind,
    retry_config: AppleAcquisitionConfig,
}

/// Deployment flavor of the configured wrapper: the native wrapper-lite
/// relay or plain candidate endpoints.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WrapperKind {
    #[default]
    Native,
    Endpoints,
}

impl WrapperKind {
    fn from_environment() -> Self {
        match std::env::var("ALAC_WRAPPER_KIND").as_deref() {
            Ok("endpoints") => WrapperKind::Endpoints,
            _ => WrapperKind::Native,
        }
    }
}

/// Outcome of one acquisition round. Keeping optional absence separate from
/// ordinary failures prevents a successful response from an untrusted/direct
/// endpoint from being mistaken for a provider-confirmed missing rendition.
enum AcquisitionAttempt {
    Source(AudioStreamSource),
    RenditionAbsent,
    Typed(StreamError),
    NoSource,
}

struct TrackRequest<'a> {
    id: &'a str,
    label: &'a TrackLabel,
}

/// Result of Apple source selection.
#[derive(Debug)]
pub enum AcquisitionOutcome {
    /// A stream was successfully selected from an available source.
    Stream(AudioStreamSource),
    /// The optional Atmos rendition was confirmed absent; primary requests
    /// return a stream or an error and never this variant.
    RenditionAbsent,
}

/// Convert an acquisition result to the generic ripper's result type.
pub fn map_acquisition_outcome(
    result: Result<AcquisitionOutcome, StreamError>,
) -> Result<AudioStreamSource, engine::ripper::RipError> {
    match result {
        Ok(AcquisitionOutcome::Stream(stream)) => Ok(stream),
        Ok(AcquisitionOutcome::RenditionAbsent) => {
            Err(engine::ripper::RipError::RenditionUnavailable {
                reason: "Dolby Atmos is unavailable".to_owned(),
            })
        }
        Err(error) => Err(engine::ripper::RipError::from(error)),
    }
}

/// Retry settings for one Apple stream acquisition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppleAcquisitionConfig {
    /// Total mirror/wrapper rounds, including the initial round.
    pub retry_rounds: u32,
    /// Delay before the second round; later delays double up to 30 seconds.
    pub retry_base_delay_ms: u64,
}

impl Default for AppleAcquisitionConfig {
    fn default() -> Self {
        Self {
            retry_rounds: 3,
            retry_base_delay_ms: 2_000,
        }
    }
}

impl AppleAcquisitionConfig {
    fn from_environment() -> Self {
        Self {
            retry_rounds: std::env::var("ALAC_STREAM_RETRIES")
                .ok()
                .and_then(|value| value.parse().ok())
                .filter(|rounds| *rounds > 0)
                .unwrap_or(3),
            retry_base_delay_ms: std::env::var("ALAC_STREAM_RETRY_BASE_MS")
                .ok()
                .and_then(|value| value.parse().ok())
                .filter(|delay| *delay > 0)
                .unwrap_or(2_000)
                .min(30_000),
        }
    }
}

impl<S: StreamHttp, M: MirrorHttp> AppleStreamAcquisition<S, M> {
    pub fn with_config(
        stream_transport: StreamTransport<S>,
        mirror_policy: MirrorPolicyManager<M>,
        wrapper_url: Option<String>,
        wrapper_api_key: Option<String>,
        wrapper_kind: WrapperKind,
        retry_config: AppleAcquisitionConfig,
    ) -> Self {
        Self {
            stream_transport,
            mirror_policy,
            wrapper_url,
            wrapper_api_key,
            wrapper_kind,
            retry_config: AppleAcquisitionConfig {
                retry_rounds: retry_config.retry_rounds.max(1),
                retry_base_delay_ms: retry_config.retry_base_delay_ms.min(30_000),
            },
        }
    }

    pub fn mirror_policy(&self) -> &MirrorPolicyManager<M> {
        &self.mirror_policy
    }

    pub async fn connect_stream(
        &self,
        track_id: &str,
        meta: &TrackMeta,
        signal: Option<CancellationToken>,
        on_progress: Option<ProgressCallback>,
        codec_preference: CodecPreference,
    ) -> Result<AcquisitionOutcome, StreamError> {
        let track = TrackLabel::from_meta(meta);
        let rounds = self.retry_config.retry_rounds;
        let base_delay = self.retry_config.retry_base_delay_ms;
        let mut all_errors = Vec::new();
        // Resolve at most once for this acquisition. A failed stream is a
        // transient request failure, not a reason to re-run endpoint health
        // discovery and let its circuit cooldown hide the same endpoint from
        // the next retry round.
        let primary = self
            .mirror_policy
            .get_endpoint(false, signal.clone())
            .await
            .ok();

        for round in 0..rounds {
            if round > 0 {
                if signal.as_ref().is_some_and(CancellationToken::is_cancelled) {
                    break;
                }
                let delay = base_delay * 2u64.pow(round - 1);
                tracing::debug!(
                    round,
                    rounds,
                    delay_ms = delay,
                    "retrying Apple stream acquisition"
                );
                if let Some(token) = signal.as_ref() {
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_millis(delay)) => {}
                        _ = token.cancelled() => break,
                    }
                } else {
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                }
            }

            let mut round_errors = Vec::new();
            let attempt = self
                .connect_once(
                    TrackRequest {
                        id: track_id,
                        label: &track,
                    },
                    signal.clone(),
                    on_progress.clone(),
                    codec_preference,
                    primary.as_ref(),
                    &mut round_errors,
                )
                .await;
            if signal.as_ref().is_some_and(CancellationToken::is_cancelled) {
                return Err(StreamError::Cancelled);
            }
            match attempt {
                AcquisitionAttempt::Source(stream) => {
                    return Ok(AcquisitionOutcome::Stream(stream));
                }
                AcquisitionAttempt::RenditionAbsent => {
                    return Ok(AcquisitionOutcome::RenditionAbsent);
                }
                AcquisitionAttempt::Typed(error) => {
                    return Err(error);
                }
                AcquisitionAttempt::NoSource => {}
            }
            for message in round_errors {
                if !all_errors.contains(&message) {
                    all_errors.push(message);
                }
            }
            if signal.as_ref().is_some_and(CancellationToken::is_cancelled) {
                break;
            }
        }

        let wrapper_missing = self
            .wrapper_url
            .as_deref()
            .is_none_or(|url| url.trim().trim_end_matches('/').is_empty());
        if signal.as_ref().is_some_and(CancellationToken::is_cancelled) {
            return Err(StreamError::Cancelled);
        }
        if wrapper_missing {
            return Err(StreamError::Message(format!(
                "Audio streaming failed and no wrapper URL is configured. Errors: {}",
                all_errors.join("; ")
            )));
        }
        Err(StreamError::Message(format!(
            "Failed to stream audio from all sources. All streaming endpoints failed for track {track_id}. Errors: {}",
            all_errors.join("; ")
        )))
    }

    async fn connect_once(
        &self,
        request: TrackRequest<'_>,
        signal: Option<CancellationToken>,
        on_progress: Option<ProgressCallback>,
        codec_preference: CodecPreference,
        primary: Option<&MirrorEndpoint>,
        errors: &mut Vec<String>,
    ) -> AcquisitionAttempt {
        if let Some(primary) = primary {
            if codec_preference == CodecPreference::Atmos {
                errors.push("Skipping primary mirror: Atmos requested".to_owned());
            } else {
                let mirror_url = primary.mirror_url.trim_end_matches('/');
                let result = self
                    .stream_transport
                    .fetch_endpoint(FetchEndpointOptions {
                        stream_url: format!("{mirror_url}/api/stream/{}", request.id),
                        api_key: Some(primary.api_key.clone()),
                        source: SourceId::PrimaryMirror,
                        signal: signal.clone(),
                        timeout: Duration::from_secs(15),
                    })
                    .await;
                match result {
                    Ok(stream) => {
                        self.mirror_policy.record_success();
                        return Self::accepted_attempt(stream, codec_preference);
                    }
                    Err(StreamError::IncompleteBody {
                        source,
                        expected,
                        received,
                    }) => {
                        let message = format!(
                            "incomplete audio body from {source}: expected {expected} bytes, received {received}"
                        );
                        errors.push(format!("Primary mirror failed: {message}"));
                        if !signal.as_ref().is_some_and(CancellationToken::is_cancelled) {
                            self.mirror_policy.record_failure(&message);
                        }
                    }
                    Err(StreamError::Timeout { source, secs }) => {
                        let message =
                            format!("stream handshake timed out after {secs}s on {source}");
                        errors.push(format!("Primary mirror failed: {message}"));
                        if !signal.as_ref().is_some_and(CancellationToken::is_cancelled) {
                            self.mirror_policy.record_failure(&message);
                        }
                    }
                    Err(StreamError::Cancelled) => {
                        errors.push("Primary mirror cancelled".to_owned());
                    }
                    Err(error) => {
                        let message = error.to_string();
                        errors.push(format!("Primary mirror failed: {message}"));
                        if !signal.as_ref().is_some_and(CancellationToken::is_cancelled) {
                            self.mirror_policy.record_failure(&message);
                        }
                    }
                }
            }
        } else {
            tracing::debug!(
                track_id = request.id,
                "Primary mirror manifest/status lookup failed, will attempt fallback"
            );
        }

        let Some(clean_wrapper) = self
            .wrapper_url
            .as_deref()
            .map(str::trim)
            .map(|url| url.trim_end_matches('/'))
            .filter(|url| !url.is_empty())
        else {
            return AcquisitionAttempt::NoSource;
        };
        if let Some(callback) = on_progress.as_ref() {
            callback(RipActivity::Connecting {
                track: request.label.clone(),
            });
        }

        // The wrapper flavor is decided once, at construction: the configured
        // deployment URL either is the native wrapper-lite relay or resolves
        // through plain candidate endpoints. The chosen flavor fixes the
        // SourceId for every error and report downstream.
        if self.wrapper_kind == WrapperKind::Native {
            let wrapper_engine = WrapperEngine::new(clean_wrapper, self.wrapper_api_key.as_deref());
            match wrapper_engine
                .rip_track_with_outcome(
                    request.id,
                    request.label,
                    signal,
                    on_progress,
                    codec_preference,
                )
                .await
            {
                Ok(WrapperTrackOutcome::Source(source)) => {
                    Self::accepted_attempt(source, codec_preference)
                }
                Ok(WrapperTrackOutcome::Unavailable(reason)) => {
                    Self::rendition_absence(reason, codec_preference)
                }
                // Terminal wrapper states surface immediately: a confirmed
                // offline service or rejected credentials cannot be repaired
                // by another round, and the ripper must see the typed
                // failure rather than an aggregated message.
                Err(
                    error
                    @ (StreamError::SourceOffline { .. } | StreamError::Authentication { .. }),
                ) => AcquisitionAttempt::Typed(error),
                Err(error) => {
                    errors.push(format!("Native wrapper engine failed: {error}"));
                    AcquisitionAttempt::NoSource
                }
            }
        } else {
            let mut only_unavailable = true;
            let mut unavailable_reason = None;
            for endpoint in [
                format!("{clean_wrapper}/api/stream/{}", request.id),
                format!("{clean_wrapper}/stream/{}", request.id),
            ] {
                if signal.as_ref().is_some_and(CancellationToken::is_cancelled) {
                    errors.push(format!(
                        "Wrapper candidate iteration cancelled before {endpoint}: Download was cancelled"
                    ));
                    return AcquisitionAttempt::NoSource;
                }
                match self
                    .stream_transport
                    .fetch_endpoint(FetchEndpointOptions {
                        stream_url: endpoint.clone(),
                        api_key: self.wrapper_api_key.clone(),
                        source: SourceId::WrapperCandidate {
                            endpoint: endpoint.clone(),
                        },
                        signal: signal.clone(),
                        timeout: Duration::from_secs(15),
                    })
                    .await
                {
                    Ok(stream) => {
                        if signal.as_ref().is_some_and(CancellationToken::is_cancelled) {
                            errors.push(format!(
                                "Wrapper candidate ({endpoint}) cancelled after resolution: Download was cancelled"
                            ));
                            return AcquisitionAttempt::NoSource;
                        }
                        match Self::accept_stream(stream, codec_preference) {
                            Ok(source) => return AcquisitionAttempt::Source(source),
                            Err(reason) => unavailable_reason = Some(reason),
                        }
                    }
                    Err(StreamError::Cancelled) => {
                        errors.push(format!(
                            "Wrapper candidate ({endpoint}) cancelled: Download was cancelled"
                        ));
                        only_unavailable = false;
                    }
                    Err(error) => {
                        only_unavailable = false;
                        errors.push(format!("Wrapper candidate ({endpoint}) failed: {error}"))
                    }
                }
            }
            if signal.as_ref().is_some_and(CancellationToken::is_cancelled) {
                errors.push(
                    "Wrapper candidate iteration cancelled: Download was cancelled".to_owned(),
                );
                return AcquisitionAttempt::NoSource;
            }
            if only_unavailable && let Some(reason) = unavailable_reason {
                return Self::rendition_absence(reason, codec_preference);
            }
            AcquisitionAttempt::NoSource
        }
    }

    fn rendition_absence(
        reason: WrapperUnavailableReason,
        codec_preference: CodecPreference,
    ) -> AcquisitionAttempt {
        if codec_preference == CodecPreference::Atmos {
            return AcquisitionAttempt::RenditionAbsent;
        }
        match reason {
            WrapperUnavailableReason::M3u8NotFound => {
                AcquisitionAttempt::Typed(StreamError::Permanent(reason.to_string()))
            }
            _ => AcquisitionAttempt::Typed(StreamError::Message(reason.to_string())),
        }
    }

    fn accepted_attempt(
        stream: AudioStreamSource,
        codec_preference: CodecPreference,
    ) -> AcquisitionAttempt {
        match Self::accept_stream(stream, codec_preference) {
            Ok(stream) => AcquisitionAttempt::Source(stream),
            Err(reason) => Self::rendition_absence(reason, codec_preference),
        }
    }

    fn accept_stream(
        stream: AudioStreamSource,
        codec_preference: CodecPreference,
    ) -> Result<AudioStreamSource, WrapperUnavailableReason> {
        if codec_preference == CodecPreference::Atmos && !stream.codec.eq_ignore_ascii_case("ec-3")
        {
            Err(WrapperUnavailableReason::NonEc3StreamForAtmos)
        } else {
            Ok(stream)
        }
    }
}

/// Production dependencies for the generic engine ripper.
pub struct AppleRipperDeps {
    catalog: crate::catalog::Catalog<crate::catalog::ReqwestTransport>,
    acquisition: AppleStreamAcquisition<engine::streaming::ReqwestHttp, ReqwestMirrorHttp>,
}

impl AppleRipperDeps {
    pub fn new(
        catalog: crate::catalog::Catalog<crate::catalog::ReqwestTransport>,
        acquisition: AppleStreamAcquisition<engine::streaming::ReqwestHttp, ReqwestMirrorHttp>,
    ) -> Self {
        Self {
            catalog,
            acquisition,
        }
    }

    pub fn mirror_policy(&self) -> &MirrorPolicyManager<ReqwestMirrorHttp> {
        self.acquisition.mirror_policy()
    }

    pub fn catalog(&self) -> &crate::catalog::Catalog<crate::catalog::ReqwestTransport> {
        &self.catalog
    }
}

/// Production settings for the complete Apple provider composition.
#[derive(Debug, Clone)]
pub struct AppleProductionConfig {
    pub wrapper_url: Option<String>,
    pub wrapper_api_key: Option<String>,
    pub wrapper_kind: WrapperKind,
    pub mirror_override: Option<(String, String)>,
    pub acquisition: AppleAcquisitionConfig,
}

impl Default for AppleProductionConfig {
    fn default() -> Self {
        Self::from_environment()
    }
}

impl AppleProductionConfig {
    /// Load Apple-specific production settings from the process environment.
    pub fn from_environment() -> Self {
        Self {
            wrapper_url: Some(
                env_option("ALAC_WRAPPER_URL")
                    .unwrap_or_else(|| "http://127.0.0.1:12340".to_owned()),
            ),
            wrapper_api_key: env_option("ALAC_WRAPPER_API_KEY"),
            wrapper_kind: WrapperKind::from_environment(),
            mirror_override: env_option("ALAC_MIRROR_URL").zip(env_option("ALAC_API_KEY")),
            acquisition: AppleAcquisitionConfig::from_environment(),
        }
    }
}

/// Fully wired Apple catalog, playlist, mirror, and stream acquisition.
///
/// The mirror manager retained by this composition is the owner of the shared
/// policy state. Clones returned by `mirror_policy()` and the acquisition use
/// that same state, so dashboard probes and rips observe one circuit/cache.
pub struct AppleProduction {
    playlist: crate::playlist::PlaylistClient<crate::playlist::ReqwestPlaylistHttp>,
    ripper_deps: AppleRipperDeps,
    mirror_policy: MirrorPolicyManager<ReqwestMirrorHttp>,
}

/// Apple-owned presentation data used by the provider-neutral orchestrator.
#[derive(Debug, Clone, Copy, Default)]
pub struct ApplePresentation;

impl engine::orchestrator::deps::ProviderPresentation for ApplePresentation {
    fn default_job_header(&self) -> &str {
        "Apple Music Lossless Rip"
    }

    fn album_url(&self, album_id: &str, storefront: &str) -> Option<String> {
        (!album_id.is_empty()).then(|| {
            let storefront = if storefront.is_empty() {
                "us"
            } else {
                storefront
            };
            format!("https://music.apple.com/{storefront}/album/{album_id}")
        })
    }

    fn unavailable_track_message(&self) -> &str {
        "Unavailable on Apple Music (not streamable)"
    }

    fn unavailable_track_log_message(&self) -> &str {
        "Track is not streamable in Apple Music catalog, skipping rip"
    }
}

impl AppleProduction {
    pub fn new(config: AppleProductionConfig) -> Self {
        let token_provider = Arc::new(DeveloperTokenProvider::new(ReqwestPlaylistHttp::new()));
        let catalog = crate::catalog::Catalog::new(crate::catalog::ReqwestTransport::new())
            .with_token_provider(token_provider.clone());
        let mirror_policy =
            MirrorPolicyManager::new(ReqwestMirrorHttp::new(), config.mirror_override);
        let stream_transport =
            engine::streaming::StreamTransport::new(engine::streaming::ReqwestHttp::new());
        let acquisition = AppleStreamAcquisition::with_config(
            stream_transport,
            mirror_policy.shared(),
            config.wrapper_url,
            config.wrapper_api_key,
            config.wrapper_kind,
            config.acquisition,
        );
        let ripper_deps = AppleRipperDeps::new(catalog, acquisition);
        Self {
            playlist: crate::playlist::PlaylistClient::with_token_provider(
                ReqwestPlaylistHttp::new(),
                token_provider,
            ),
            ripper_deps,
            mirror_policy,
        }
    }

    pub fn catalog(&self) -> &crate::catalog::Catalog<crate::catalog::ReqwestTransport> {
        self.ripper_deps.catalog()
    }

    pub fn playlist(
        &self,
    ) -> &crate::playlist::PlaylistClient<crate::playlist::ReqwestPlaylistHttp> {
        &self.playlist
    }

    pub fn ripper_deps(&self) -> &AppleRipperDeps {
        &self.ripper_deps
    }

    pub fn mirror_policy(&self) -> &MirrorPolicyManager<ReqwestMirrorHttp> {
        &self.mirror_policy
    }
}

fn env_option(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
}

impl engine::ripper::RipStage for AppleRipperDeps {
    fn observe_stream_failure(
        &self,
        source: &SourceId,
        kind: engine::ripper::SourceFailureKind,
        detail: &str,
    ) {
        // Circuit rules: only source-attributed corruption from the primary
        // mirror trips the circuit. Wrapper-attributed failures and local or
        // post-tag failures never do.
        if !matches!(source, SourceId::PrimaryMirror) {
            return;
        }
        if matches!(
            kind,
            engine::ripper::SourceFailureKind::Stream
                | engine::ripper::SourceFailureKind::IncompleteBody
                | engine::ripper::SourceFailureKind::MediaValidation
        ) {
            self.acquisition.mirror_policy().record_failure(detail);
        }
    }

    async fn track_meta(
        &self,
        track_id: &str,
        storefront: &str,
    ) -> Result<music::TrackMeta, engine::ripper::RipError> {
        self.catalog
            .fetch_track_meta(track_id, storefront)
            .await
            .map_err(|error| engine::ripper::RipError::Message(error.to_string()))
    }

    async fn connect_stream(
        &self,
        track_id: &str,
        meta: &music::TrackMeta,
        signal: Option<CancellationToken>,
        on_progress: Option<ProgressCallback>,
        codec_preference: CodecPreference,
    ) -> Result<AudioStreamSource, engine::ripper::RipError> {
        map_acquisition_outcome(
            self.acquisition
                .connect_stream(track_id, meta, signal, on_progress, codec_preference)
                .await,
        )
    }

    fn track_tags(&self, meta: &music::TrackMeta) -> media::TrackTags {
        media::TrackTags {
            title: (!meta.title.is_empty()).then(|| meta.title.clone()),
            title_sort: (!meta.title.is_empty()).then(|| meta.title.clone()),
            artist: (!meta.artist.is_empty()).then(|| meta.artist.clone()),
            artist_sort: (!meta.artist.is_empty()).then(|| meta.artist.clone()),
            album: (!meta.album.is_empty()).then(|| meta.album.clone()),
            album_sort: (!meta.album.is_empty()).then(|| meta.album.clone()),
            album_artist: (!meta.album_artist.is_empty()).then(|| meta.album_artist.clone()),
            album_artist_sort: (!meta.album_artist.is_empty()).then(|| meta.album_artist.clone()),
            release_date: (!meta.release_date.is_empty()).then(|| meta.release_date.clone()),
            genre: meta.genre.clone().filter(|value| !value.is_empty()),
            composer: meta.composer.clone().filter(|value| !value.is_empty()),
            composer_sort: meta.composer.clone().filter(|value| !value.is_empty()),
            track_number: meta
                .track_number
                .filter(|value| *value != 0)
                .and_then(|value| u16::try_from(value).ok()),
            track_count: meta
                .track_count
                .filter(|value| *value != 0)
                .and_then(|value| u16::try_from(value).ok()),
            disc_number: meta
                .disc_number
                .filter(|value| *value != 0)
                .and_then(|value| u16::try_from(value).ok()),
            disc_count: meta
                .disc_count
                .filter(|value| *value != 0)
                .and_then(|value| u16::try_from(value).ok()),
            lyrics: None,
            artwork_jpeg: None,
            isrc: meta.isrc.clone().filter(|value| !value.is_empty()),
            label: meta.record_label.clone().filter(|value| !value.is_empty()),
            copyright: meta.copyright.clone().filter(|value| !value.is_empty()),
            publisher: meta.record_label.clone().filter(|value| !value.is_empty()),
            performer: (!meta.artist.is_empty()).then(|| meta.artist.clone()),
            release_time: (!meta.release_date.is_empty()).then(|| meta.release_date.clone()),
            upc: meta.upc.clone().filter(|value| !value.is_empty()),
            song_id: meta.id.parse::<u64>().ok(),
            album_id: meta
                .album_id
                .as_deref()
                .and_then(|value| value.parse().ok()),
            artist_id: meta
                .artist_id
                .as_deref()
                .and_then(|value| value.parse().ok()),
            explicit: Some(meta.explicit),
            advisory: Some(match meta.content_advisory.as_deref() {
                Some(value) if value.eq_ignore_ascii_case("explicit") => {
                    media::AdvisoryKind::Explicit
                }
                Some(value) if value.eq_ignore_ascii_case("clean") => media::AdvisoryKind::Clean,
                Some(_) => media::AdvisoryKind::Inoffensive,
                None if meta.explicit => media::AdvisoryKind::Explicit,
                None => media::AdvisoryKind::Inoffensive,
            }),
            media_kind: Some(media::MediaKind::Music),
            compilation: Some(
                meta.album_artist.eq_ignore_ascii_case("Various Artists")
                    || meta.artist.eq_ignore_ascii_case("Various Artists"),
            ),
            gapless: Some(true),
            genre_id: None,
            storefront_id: None,
            encoder: Some("peerless".to_owned()),
            comment: None,
            description: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use engine::{
        ripper::{RipStage, SourceFailureKind},
        streaming::{ReqwestHttp, SourceId, StreamTransport},
    };

    use super::{AppleRipperDeps, AppleStreamAcquisition};
    use crate::{Catalog, MirrorPolicyManager, ReqwestMirrorHttp};

    #[test]
    fn source_failure_reporting_only_marks_primary_mirror() {
        let acquisition = AppleStreamAcquisition::with_config(
            StreamTransport::new(ReqwestHttp::new()),
            MirrorPolicyManager::new(ReqwestMirrorHttp::new(), None),
            None,
            None,
            super::WrapperKind::Native,
            super::AppleAcquisitionConfig {
                retry_rounds: 1,
                retry_base_delay_ms: 0,
            },
        );
        let deps = AppleRipperDeps::new(
            Catalog::new(crate::catalog::ReqwestTransport::new()),
            acquisition,
        );

        deps.observe_stream_failure(
            &SourceId::WrapperLite {
                url: "http://wrapper".to_owned(),
            },
            SourceFailureKind::MediaValidation,
            "audio decode failed: corrupt wrapper",
        );
        assert!(!deps.mirror_policy().is_circuit_open());

        deps.observe_stream_failure(
            &SourceId::WrapperCandidate {
                endpoint: "http://wrapper/api/stream/42".to_owned(),
            },
            SourceFailureKind::MediaValidation,
            "audio decode failed: corrupt candidate",
        );
        assert!(!deps.mirror_policy().is_circuit_open());

        deps.observe_stream_failure(
            &SourceId::PrimaryMirror,
            SourceFailureKind::MediaValidation,
            "audio decode failed: corrupt mirror",
        );
        assert!(deps.mirror_policy().is_circuit_open());
    }
}
