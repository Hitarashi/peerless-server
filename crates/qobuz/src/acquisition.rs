//! Stream acquisition and RipStage implementation for Qobuz.

use std::sync::Arc;

use engine::{
    ripper::{RipError, RipStage, SourceFailureKind},
    streaming::{AudioStreamSource, ProgressCallback, SourceId, StreamBodyError},
};
use futures_util::StreamExt;
use music::{CodecPreference, TrackMeta};
use tokio_util::sync::CancellationToken;

use crate::gateway::{QobuzError, QobuzGateway, QobuzStreamInfo, quality_ladder};

#[derive(Clone)]
pub struct QobuzAcquisition {
    primary: Arc<dyn QobuzGateway>,
    fallback: Option<Arc<dyn QobuzGateway>>,
    primary_url: String,
    http_client: reqwest::Client,
}

impl QobuzAcquisition {
    pub fn new(
        primary: Arc<dyn QobuzGateway>,
        fallback: Option<Arc<dyn QobuzGateway>>,
        primary_url: String,
    ) -> Self {
        let http_client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .unwrap_or_default();

        Self {
            primary,
            fallback,
            primary_url,
            http_client,
        }
    }

    fn backend_source(&self) -> SourceId {
        SourceId::QobuzBackend {
            url: self.primary_url.clone(),
        }
    }
}

/// Map a gateway failure onto the engine error with the right retry behavior:
/// auth failures surface as authentication errors, rate-limit/network issues
/// become retryable timeouts, and only genuine not-found/unavailable aborts
/// the rip without retries.
fn map_catalog_error(
    gateway: &str,
    track_id: &str,
    err: QobuzError,
    source: &SourceId,
) -> RipError {
    match err {
        QobuzError::Auth(detail) => {
            tracing::warn!("Qobuz {gateway} auth failure for track {track_id}: {detail}");
            RipError::Authentication {
                source: source.clone(),
            }
        }
        QobuzError::RateLimit(detail) | QobuzError::Network(detail) => RipError::Timeout {
            source: Some(source.clone()),
            detail: format!("[{gateway}] {detail}"),
        },
        other => RipError::TrackUnavailable {
            reason: format!("[{gateway}] {other}"),
        },
    }
}

fn map_stream_error(gateway: &str, track_id: &str, err: QobuzError, source: &SourceId) -> RipError {
    match err {
        QobuzError::Auth(detail) => {
            tracing::warn!("Qobuz {gateway} auth failure for track {track_id}: {detail}");
            RipError::Authentication {
                source: source.clone(),
            }
        }
        QobuzError::RateLimit(detail) | QobuzError::Network(detail) => RipError::Timeout {
            source: Some(source.clone()),
            detail: format!("[{gateway}] {detail}"),
        },
        other => RipError::RenditionUnavailable {
            reason: format!("[{gateway}] {other}"),
        },
    }
}

async fn fetch_meta_with_fallback(
    primary: &Arc<dyn QobuzGateway>,
    fallback: &Option<Arc<dyn QobuzGateway>>,
    primary_source: &SourceId,
    track_id: &str,
) -> Result<TrackMeta, RipError> {
    match primary.fetch_track_meta(track_id).await {
        Ok(meta) => Ok(meta),
        Err(primary_err) => {
            if let Some(fallback_gw) = fallback {
                let gw_name = fallback_gw.gateway_name();
                tracing::warn!(
                    "Primary Qobuz gateway failed for track {track_id} ({primary_err}), trying {gw_name}"
                );
                match fallback_gw.fetch_track_meta(track_id).await {
                    Ok(meta) => Ok(meta),
                    // Both gateways failed: keep both errors. If either side
                    // failed on credentials, surface that (it needs operator
                    // action); otherwise report unavailable without retries.
                    Err(fallback_err) => {
                        let primary_source_name = primary.gateway_name();
                        if matches!(primary_err, QobuzError::Auth(_)) {
                            Err(map_catalog_error(
                                primary_source_name,
                                track_id,
                                primary_err,
                                primary_source,
                            ))
                        } else if matches!(fallback_err, QobuzError::Auth(_)) {
                            Err(map_catalog_error(
                                gw_name,
                                track_id,
                                fallback_err,
                                &SourceId::QobuzNative,
                            ))
                        } else {
                            Err(RipError::TrackUnavailable {
                                reason: format!(
                                    "[primary {primary_source_name}: {primary_err}] [{gw_name}: {fallback_err}]"
                                ),
                            })
                        }
                    }
                }
            } else {
                Err(map_catalog_error(
                    primary.gateway_name(),
                    track_id,
                    primary_err,
                    primary_source,
                ))
            }
        }
    }
}

/// Resolution phase of `connect_stream`, split out so tests can prove the
/// ordering without touching the network: strict top-down ladder first
/// (preferred → higher → lower across both gateways), then one fail-open
/// pass. The first success is always the highest tier any gateway can serve.
async fn resolve_best_available(
    primary: &Arc<dyn QobuzGateway>,
    fallback: &Option<Arc<dyn QobuzGateway>>,
    backend_source: &SourceId,
    track_id: &str,
    preferred_format: u32,
    signal: &Option<CancellationToken>,
) -> Result<(QobuzStreamInfo, SourceId), QobuzError> {
    let mut last_error: Option<QobuzError> = None;

    for format_id in quality_ladder(preferred_format) {
        if signal.as_ref().is_some_and(|s| s.is_cancelled()) {
            return Err(QobuzError::Message("cancelled".to_owned()));
        }
        match resolve_with_gateways(primary, fallback, track_id, format_id, false).await {
            Ok((info, from_primary, gw_name)) => {
                let source = if from_primary {
                    backend_source.clone()
                } else {
                    SourceId::QobuzNative
                };
                tracing::debug!(
                    "Qobuz stream resolved for track {track_id} via {gw_name} at format {}",
                    info.format_id
                );
                return Ok((info, source));
            }
            Err(err) => {
                // Auth/rate-limit/network abort the ladder early: other tiers
                // would fail identically; the caller maps the error to the
                // retryable engine variant.
                if matches!(
                    err,
                    QobuzError::Auth(_) | QobuzError::RateLimit(_) | QobuzError::Network(_)
                ) {
                    return Err(err);
                }
                last_error = Some(err);
            }
        }
    }

    // Final fail-open pass at the requested tier (backend may substitute
    // the closest available master).
    match resolve_with_gateways(primary, fallback, track_id, preferred_format, true).await {
        Ok((info, from_primary, gw_name)) => {
            let source = if from_primary {
                backend_source.clone()
            } else {
                SourceId::QobuzNative
            };
            tracing::debug!(
                "Qobuz stream resolved for track {track_id} via {gw_name} fail-open at format {}",
                info.format_id
            );
            Ok((info, source))
        }
        Err(err) => Err(last_error.or(Some(err)).unwrap_or_else(|| {
            QobuzError::Unavailable(format!("no stream resolved for track {track_id}"))
        })),
    }
}
async fn resolve_with_gateways(
    primary: &Arc<dyn QobuzGateway>,
    fallback: &Option<Arc<dyn QobuzGateway>>,
    track_id: &str,
    format_id: u32,
    fallback_flag: bool,
) -> Result<(QobuzStreamInfo, bool, &'static str), QobuzError> {
    match primary
        .resolve_stream_url(track_id, format_id, fallback_flag)
        .await
    {
        Ok(info) => Ok((info, true, primary.gateway_name())),
        Err(primary_err) => {
            if let Some(fallback_gw) = fallback {
                match fallback_gw
                    .resolve_stream_url(track_id, format_id, fallback_flag)
                    .await
                {
                    Ok(info) => Ok((info, false, fallback_gw.gateway_name())),
                    Err(_) => Err(primary_err),
                }
            } else {
                Err(primary_err)
            }
        }
    }
}

impl RipStage for QobuzAcquisition {
    async fn track_meta(&self, track_id: &str, _storefront: &str) -> Result<TrackMeta, RipError> {
        fetch_meta_with_fallback(
            &self.primary,
            &self.fallback,
            &self.backend_source(),
            track_id,
        )
        .await
    }

    async fn connect_stream(
        &self,
        track_id: &str,
        _meta: &TrackMeta,
        signal: Option<CancellationToken>,
        _on_progress: Option<ProgressCallback>,
        codec_preference: CodecPreference,
    ) -> Result<AudioStreamSource, RipError> {
        if signal.as_ref().is_some_and(|s| s.is_cancelled()) {
            return Err(RipError::Cancelled);
        }

        // Quality ladder lives in `resolve_best_available` (codebase-only;
        // Telegram exposes no chooser): preferred tier → higher tiers →
        // lower tiers, strict first, then one fail-open pass. Since every
        // Qobuz input requests HighestQuality, the first success is always
        // the highest tier any gateway can serve for the track.
        let backend_source = self.backend_source();
        let (stream_info, source) = match resolve_best_available(
            &self.primary,
            &self.fallback,
            &backend_source,
            track_id,
            codec_preference.qobuz_format_id(),
            &signal,
        )
        .await
        {
            Ok(ok) => ok,
            Err(err) => {
                if signal.as_ref().is_some_and(|s| s.is_cancelled()) {
                    return Err(RipError::Cancelled);
                }
                let gw = self.primary.gateway_name();
                return Err(map_stream_error(gw, track_id, err, &backend_source));
            }
        };

        if signal.as_ref().is_some_and(|s| s.is_cancelled()) {
            return Err(RipError::Cancelled);
        }

        let req = self
            .http_client
            .get(&stream_info.url)
            .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)");

        let send_future = req.send();
        let res = if let Some(sig) = &signal {
            tokio::select! {
                _ = sig.cancelled() => return Err(RipError::Cancelled),
                res = send_future => res,
            }
        } else {
            send_future.await
        };

        let res = res.map_err(|err| RipError::Timeout {
            source: Some(source.clone()),
            detail: err.to_string(),
        })?;

        if !res.status().is_success() {
            return Err(RipError::StreamCorrupt {
                source,
                detail: format!("CDN returned HTTP status {}", res.status()),
            });
        }

        let content_length = res.content_length();
        let codec = if stream_info.mime_type.contains("mp3") || stream_info.format_id == 5 {
            "mp3".to_owned()
        } else {
            "flac".to_owned()
        };

        let stream = Box::pin(
            res.bytes_stream()
                .map(|chunk| chunk.map_err(|e| StreamBodyError::Network(e.to_string()))),
        );

        Ok(AudioStreamSource {
            stream,
            source,
            codec,
            bit_depth: stream_info.bit_depth,
            sample_rate: stream_info.sample_rate,
            content_length,
        })
    }

    fn observe_stream_failure(&self, source: &SourceId, kind: SourceFailureKind, detail: &str) {
        tracing::warn!(source = %source, ?kind, detail, "Qobuz stream failure observed");
    }

    fn track_tags(&self, meta: &TrackMeta) -> media::TrackTags {
        media::TrackTags {
            title: Some(meta.title.clone()),
            artist: Some(meta.artist.clone()),
            album: Some(meta.album.clone()),
            album_artist: Some(meta.album_artist.clone()),
            release_date: Some(meta.release_date.clone()),
            genre: meta.genre.clone(),
            composer: meta.composer.clone(),
            track_number: meta.track_number.and_then(|n| u16::try_from(n).ok()),
            track_count: meta.track_count.and_then(|n| u16::try_from(n).ok()),
            disc_number: meta.disc_number.and_then(|n| u16::try_from(n).ok()),
            disc_count: meta.disc_count.and_then(|n| u16::try_from(n).ok()),
            isrc: meta.isrc.clone(),
            label: meta.record_label.clone(),
            copyright: meta.copyright.clone(),
            upc: meta.upc.clone(),
            explicit: Some(meta.explicit),
            advisory: if meta.explicit {
                Some(media::AdvisoryKind::Explicit)
            } else {
                Some(media::AdvisoryKind::Clean)
            },
            media_kind: Some(media::MediaKind::Music),
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use music::{AlbumTracks, ArtistTracks, PlaylistData};

    use super::*;
    use crate::gateway::BoxFuture;

    /// Scripted gateway: records every (format, fallback) attempt and serves
    /// canned outcomes, so resolution ordering is provable without network.
    struct StubGateway {
        name: &'static str,
        /// Formats that succeed on strict passes.
        strict_ok: Vec<u32>,
        /// Succeed on the fail-open pass (with this format).
        failopen_ok: Option<u32>,
        /// Fail every attempt with an auth error instead.
        auth_error: bool,
        attempts: Mutex<Vec<(u32, bool)>>,
    }

    impl StubGateway {
        fn new(name: &'static str) -> Self {
            Self {
                name,
                strict_ok: Vec::new(),
                failopen_ok: None,
                auth_error: false,
                attempts: Mutex::new(Vec::new()),
            }
        }

        fn attempts(&self) -> Vec<(u32, bool)> {
            self.attempts.lock().unwrap().clone()
        }
    }

    impl QobuzGateway for StubGateway {
        fn gateway_name(&self) -> &'static str {
            self.name
        }

        fn fetch_track_meta<'a>(
            &'a self,
            track_id: &'a str,
        ) -> BoxFuture<'a, Result<TrackMeta, QobuzError>> {
            Box::pin(async move { Err(QobuzError::NotFound(format!("stub has no {track_id}"))) })
        }

        fn resolve_stream_url<'a>(
            &'a self,
            track_id: &'a str,
            format_id: u32,
            fallback: bool,
        ) -> BoxFuture<'a, Result<QobuzStreamInfo, QobuzError>> {
            Box::pin(async move {
                self.attempts.lock().unwrap().push((format_id, fallback));
                if self.auth_error {
                    return Err(QobuzError::Auth("stub credentials rejected".to_owned()));
                }
                let hit = if fallback {
                    self.failopen_ok
                } else if self.strict_ok.contains(&format_id) {
                    Some(format_id)
                } else {
                    None
                };
                match hit {
                    Some(format) => Ok(QobuzStreamInfo {
                        url: format!("https://cdn.stub/{track_id}/{format}"),
                        format_id: format,
                        mime_type: "audio/flac".to_owned(),
                        bit_depth: 16,
                        sample_rate: 44_100,
                    }),
                    None => Err(QobuzError::Unavailable(format!(
                        "stub has no {track_id} at {format_id}"
                    ))),
                }
            })
        }

        fn fetch_album_tracks<'a>(
            &'a self,
            album_id: &'a str,
        ) -> BoxFuture<'a, Result<AlbumTracks, QobuzError>> {
            Box::pin(async move { Err(QobuzError::NotFound(format!("stub has no {album_id}"))) })
        }

        fn fetch_artist_tracks<'a>(
            &'a self,
            artist_id: &'a str,
        ) -> BoxFuture<'a, Result<ArtistTracks, QobuzError>> {
            Box::pin(async move { Err(QobuzError::NotFound(format!("stub has no {artist_id}"))) })
        }

        fn fetch_playlist_tracks<'a>(
            &'a self,
            playlist_id: &'a str,
        ) -> BoxFuture<'a, Result<PlaylistData, QobuzError>> {
            Box::pin(async move { Err(QobuzError::NotFound(format!("stub has no {playlist_id}"))) })
        }
    }

    fn backend_source() -> SourceId {
        SourceId::QobuzBackend {
            url: "https://test.invalid".to_owned(),
        }
    }

    fn arc_stub(stub: StubGateway) -> Arc<dyn QobuzGateway> {
        Arc::new(stub)
    }

    #[tokio::test]
    async fn highest_available_tier_wins_without_chooser() {
        // Track exists at Hi-Res 96 only: 27 must be attempted first, then 7
        // succeeds — the user gets the best available master, no chooser.
        let primary = arc_stub({
            let mut s = StubGateway::new("primary");
            s.strict_ok = vec![7];
            s
        });
        let source = backend_source();
        let (info, resolved_source) =
            resolve_best_available(&primary, &None, &source, "t1", 27, &None)
                .await
                .expect("should resolve at 7");
        assert_eq!(info.format_id, 7);
        assert_eq!(resolved_source, source);
    }

    #[tokio::test]
    async fn ladder_is_strict_top_down_before_failopen() {
        let stub = Arc::new({
            let mut s = StubGateway::new("primary");
            s.failopen_ok = Some(6);
            s
        });
        let primary: Arc<dyn QobuzGateway> = stub.clone();
        let source = backend_source();
        let (info, _) = resolve_best_available(&primary, &None, &source, "t2", 27, &None)
            .await
            .expect("fail-open should resolve");
        assert_eq!(info.format_id, 6);
        // Strict 27 → 7 → 6 → 5 first, fail-open 27 last.
        assert_eq!(
            stub.attempts(),
            vec![(27, false), (7, false), (6, false), (5, false), (27, true)]
        );
    }

    #[tokio::test]
    async fn fallback_gateway_serves_when_primary_cannot() {
        let primary = arc_stub(StubGateway::new("primary"));
        let fallback = {
            let mut s = StubGateway::new("fallback");
            s.strict_ok = vec![6];
            arc_stub(s)
        };
        let source = backend_source();
        let (info, resolved_source) =
            resolve_best_available(&primary, &Some(fallback), &source, "t3", 27, &None)
                .await
                .expect("fallback should resolve");
        assert_eq!(info.format_id, 6);
        assert_eq!(resolved_source, SourceId::QobuzNative);
    }

    #[tokio::test]
    async fn auth_error_aborts_ladder_immediately() {
        let stub = Arc::new({
            let mut s = StubGateway::new("primary");
            s.auth_error = true;
            s
        });
        let primary: Arc<dyn QobuzGateway> = stub.clone();
        let source = backend_source();
        let err = resolve_best_available(&primary, &None, &source, "t4", 27, &None)
            .await
            .expect_err("auth must fail");
        assert!(matches!(err, QobuzError::Auth(_)));
        // No point trying 7/6/5 with rejected credentials.
        assert_eq!(stub.attempts(), vec![(27, false)]);
    }
}
