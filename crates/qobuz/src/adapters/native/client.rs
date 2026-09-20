//! Direct in-process Qobuz API 0.2 client implementing the QobuzGateway seam.

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use music::{AlbumTracks, ArtistTracks, PlaylistData, TrackMeta};
use tokio::sync::RwLock;

use crate::{
    adapters::native::{
        bundle::{BundleScraper, ScrapedTokens},
        signature::generate_request_signature,
    },
    gateway::{
        BoxFuture, QobuzError, QobuzGateway, QobuzStreamInfo, normalize_sample_rate,
        quality_fallback_ladder,
    },
    types::{QobuzAlbum, QobuzArtistData, QobuzPlaylist, QobuzStreamData, QobuzTrack},
};

const API_BASE: &str = "https://www.qobuz.com/api.json/0.2";
/// Scraped web-player tokens go stale when Qobuz rotates the bundle; refresh
/// at most this often (and immediately on auth failures) instead of caching
/// forever in a long-running bot process.
const TOKEN_TTL: Duration = Duration::from_secs(24 * 60 * 60);
/// Bound on parallel per-album fetches for native artist resolution.
const ARTIST_ALBUM_FANOUT: usize = 8;

#[derive(Debug, Clone, Default)]
pub struct NativeConfig {
    pub app_id: Option<String>,
    pub app_secret: Option<String>,
    pub user_auth_token: Option<String>,
}

impl NativeConfig {
    pub fn from_environment() -> Self {
        Self {
            app_id: std::env::var("QOBUZ_APP_ID")
                .ok()
                .filter(|s| !s.trim().is_empty()),
            app_secret: std::env::var("QOBUZ_APP_SECRET")
                .ok()
                .filter(|s| !s.trim().is_empty()),
            user_auth_token: std::env::var("QOBUZ_USER_AUTH_TOKEN")
                .ok()
                .filter(|s| !s.trim().is_empty()),
        }
    }
}

pub struct NativeRipperAdapter {
    config: NativeConfig,
    scraper: BundleScraper,
    cached_tokens: RwLock<Option<(ScrapedTokens, Instant)>>,
    client: reqwest::Client,
}

impl NativeRipperAdapter {
    pub fn new(config: NativeConfig) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(20))
            .build()
            .unwrap_or_default();

        Self {
            config,
            scraper: BundleScraper::new(),
            cached_tokens: RwLock::new(None),
            client,
        }
    }

    async fn ensure_tokens(&self) -> (String, Vec<String>) {
        if let (Some(app_id), Some(app_secret)) = (&self.config.app_id, &self.config.app_secret) {
            return (app_id.clone(), vec![app_secret.clone()]);
        }

        {
            let read_guard = self.cached_tokens.read().await;
            if let Some((tokens, fetched_at)) = read_guard.as_ref()
                && fetched_at.elapsed() < TOKEN_TTL
            {
                let app_id = self
                    .config
                    .app_id
                    .clone()
                    .unwrap_or_else(|| tokens.app_id.clone());
                let secrets = if let Some(sec) = &self.config.app_secret {
                    vec![sec.clone()]
                } else {
                    tokens.secrets.clone()
                };
                return (app_id, secrets);
            }
        }

        let scraped = self.scraper.get_tokens().await;
        let app_id = self
            .config
            .app_id
            .clone()
            .unwrap_or_else(|| scraped.app_id.clone());
        let secrets = if let Some(sec) = &self.config.app_secret {
            vec![sec.clone()]
        } else {
            scraped.secrets.clone()
        };

        let mut write_guard = self.cached_tokens.write().await;
        *write_guard = Some((scraped, Instant::now()));

        (app_id, secrets)
    }

    /// Drop scraped tokens so the next call re-scrapes. Invoke when the API
    /// answers with auth errors — retrying with the same stale bundle is
    /// wasted work, and the configured app id/secret (if any) still wins.
    async fn invalidate_tokens(&self) {
        let mut write_guard = self.cached_tokens.write().await;
        *write_guard = None;
    }

    fn now_ts() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    }
}

impl QobuzGateway for NativeRipperAdapter {
    fn gateway_name(&self) -> &'static str {
        "native-ripper"
    }

    fn fetch_track_meta<'a>(
        &'a self,
        track_id: &'a str,
    ) -> BoxFuture<'a, Result<TrackMeta, QobuzError>> {
        Box::pin(async move {
            let (app_id, _) = self.ensure_tokens().await;
            let url = format!("{API_BASE}/track/get?track_id={track_id}&app_id={app_id}");

            let res = self
                .client
                .get(&url)
                .send()
                .await
                .map_err(|e| QobuzError::Network(e.to_string()))?;

            match res.status() {
                s if s == reqwest::StatusCode::NOT_FOUND => {
                    return Err(QobuzError::NotFound(format!("Track {track_id} not found")));
                }
                s if s == reqwest::StatusCode::UNAUTHORIZED
                    || s == reqwest::StatusCode::FORBIDDEN =>
                {
                    self.invalidate_tokens().await;
                    return Err(QobuzError::Auth(format!(
                        "Track {track_id}: Qobuz returned {s}"
                    )));
                }
                s if s == reqwest::StatusCode::TOO_MANY_REQUESTS => {
                    return Err(QobuzError::RateLimit(format!(
                        "Track {track_id}: Qobuz returned {s}"
                    )));
                }
                s if !s.is_success() => {
                    return Err(QobuzError::Message(format!(
                        "Track {track_id}: Qobuz returned {s}"
                    )));
                }
                _ => {}
            }

            let track: QobuzTrack = res
                .json()
                .await
                .map_err(|e| QobuzError::Message(format!("JSON error: {e}")))?;

            Ok(track.to_track_meta(None))
        })
    }

    fn resolve_stream_url<'a>(
        &'a self,
        track_id: &'a str,
        requested_format: u32,
        fallback: bool,
    ) -> BoxFuture<'a, Result<QobuzStreamInfo, QobuzError>> {
        Box::pin(async move {
            let (app_id, secrets) = self.ensure_tokens().await;
            let auth_token = self.config.user_auth_token.as_deref().unwrap_or_default();

            // The acquisition stage owns the upward ladder (preferred →
            // higher → lower). Natively we can only satisfy the exact tier or
            // step *down* on a fail-open pass — never upscale here, or the
            // two ladders multiply into secrets × formats requests.
            let quality_ladder = if fallback {
                quality_fallback_ladder(requested_format)
            } else {
                vec![requested_format]
            };

            let mut saw_auth_error = false;
            for format_id in quality_ladder {
                for secret in &secrets {
                    let ts = Self::now_ts();
                    let format_str = format_id.to_string();
                    let params = [
                        ("format_id", format_str.as_str()),
                        ("intent", "stream"),
                        ("track_id", track_id),
                    ];
                    let sig = generate_request_signature("track/getFileUrl", &params, ts, secret);

                    let mut url = format!(
                        "{API_BASE}/track/getFileUrl?track_id={track_id}&format_id={format_id}&intent=stream&request_ts={ts}&request_sig={sig}&app_id={app_id}"
                    );
                    if !auth_token.is_empty() {
                        url.push_str("&user_auth_token=");
                        url.push_str(auth_token);
                    }

                    match self.client.get(&url).send().await {
                        Ok(res) => {
                            let status = res.status();
                            if status == reqwest::StatusCode::UNAUTHORIZED
                                || status == reqwest::StatusCode::FORBIDDEN
                            {
                                saw_auth_error = true;
                                continue;
                            }
                            if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                                return Err(QobuzError::RateLimit(format!(
                                    "Qobuz rate-limited native stream lookup for track {track_id}"
                                )));
                            }
                            if !status.is_success() {
                                continue;
                            }
                            if let Ok(data) = res.json::<QobuzStreamData>().await
                                && let Some(stream_url) = data.url
                            {
                                return Ok(QobuzStreamInfo {
                                    url: stream_url,
                                    format_id: data.format_id.unwrap_or(format_id),
                                    mime_type: data
                                        .mime_type
                                        .unwrap_or_else(|| "audio/flac".to_owned()),
                                    bit_depth: data.bit_depth.unwrap_or(16),
                                    sample_rate: normalize_sample_rate(data.sampling_rate),
                                });
                            }
                        }
                        Err(_) => continue,
                    }
                }
            }

            if saw_auth_error {
                // Stale scraped bundle or revoked token: force a re-scrape
                // next call instead of failing forever on cached secrets.
                self.invalidate_tokens().await;
                return Err(QobuzError::Auth(format!(
                    "Qobuz rejected native credentials for track {track_id}"
                )));
            }

            Err(QobuzError::Unavailable(format!(
                "Could not resolve stream URL natively for track {track_id}"
            )))
        })
    }

    fn fetch_album_tracks<'a>(
        &'a self,
        album_id: &'a str,
    ) -> BoxFuture<'a, Result<AlbumTracks, QobuzError>> {
        Box::pin(async move {
            let (app_id, _) = self.ensure_tokens().await;
            let url = format!("{API_BASE}/album/get?album_id={album_id}&app_id={app_id}");

            let res = self
                .client
                .get(&url)
                .send()
                .await
                .map_err(|e| QobuzError::Network(e.to_string()))?;

            match res.status() {
                s if s == reqwest::StatusCode::NOT_FOUND => {
                    return Err(QobuzError::NotFound(format!("Album {album_id} not found")));
                }
                s if s == reqwest::StatusCode::UNAUTHORIZED
                    || s == reqwest::StatusCode::FORBIDDEN =>
                {
                    self.invalidate_tokens().await;
                    return Err(QobuzError::Auth(format!(
                        "Album {album_id}: Qobuz returned {s}"
                    )));
                }
                s if s == reqwest::StatusCode::TOO_MANY_REQUESTS => {
                    return Err(QobuzError::RateLimit(format!(
                        "Album {album_id}: Qobuz returned {s}"
                    )));
                }
                s if !s.is_success() => {
                    return Err(QobuzError::Message(format!(
                        "Album {album_id}: Qobuz returned {s}"
                    )));
                }
                _ => {}
            }

            let album: QobuzAlbum = res
                .json()
                .await
                .map_err(|e| QobuzError::Message(format!("JSON error: {e}")))?;

            Ok(album.to_album_tracks())
        })
    }

    fn fetch_artist_tracks<'a>(
        &'a self,
        artist_id: &'a str,
    ) -> BoxFuture<'a, Result<ArtistTracks, QobuzError>> {
        Box::pin(async move {
            let (app_id, _) = self.ensure_tokens().await;
            let url = format!(
                "{API_BASE}/artist/get?artist_id={artist_id}&app_id={app_id}&limit=100&extra=albums"
            );

            let res = self
                .client
                .get(&url)
                .send()
                .await
                .map_err(|e| QobuzError::Network(e.to_string()))?;

            match res.status() {
                s if s == reqwest::StatusCode::NOT_FOUND => {
                    return Err(QobuzError::NotFound(format!(
                        "Artist {artist_id} not found"
                    )));
                }
                s if s == reqwest::StatusCode::UNAUTHORIZED
                    || s == reqwest::StatusCode::FORBIDDEN =>
                {
                    self.invalidate_tokens().await;
                    return Err(QobuzError::Auth(format!(
                        "Artist {artist_id}: Qobuz returned {s}"
                    )));
                }
                s if s == reqwest::StatusCode::TOO_MANY_REQUESTS => {
                    return Err(QobuzError::RateLimit(format!(
                        "Artist {artist_id}: Qobuz returned {s}"
                    )));
                }
                s if !s.is_success() => {
                    return Err(QobuzError::Message(format!(
                        "Artist {artist_id}: Qobuz returned {s}"
                    )));
                }
                _ => {}
            }

            let artist: QobuzArtistData = res
                .json()
                .await
                .map_err(|e| QobuzError::Message(format!("JSON error: {e}")))?;

            let album_ids: Vec<String> = artist
                .albums
                .map(|a| a.items.into_iter().map(|item| item.id_string()).collect())
                .unwrap_or_default();

            // The artist endpoint only lists albums — resolve each album's
            // tracklist with bounded parallelism. Partial failures are
            // tolerated (an artist with 40 albums still yields 39 on one
            // flaky page); total failure surfaces as unavailable.
            let mut tracks = Vec::new();
            if album_ids.is_empty() {
                return Ok(ArtistTracks {
                    artist_id: artist_id.to_owned(),
                    artist_name: artist.name,
                    tracks,
                });
            }
            let mut failures = 0usize;
            let mut pending = std::collections::VecDeque::from(album_ids);
            while !pending.is_empty() {
                let batch: Vec<String> = pending
                    .drain(..pending.len().min(ARTIST_ALBUM_FANOUT))
                    .collect();
                let mut batch_futs = futures_util::future::join_all(
                    batch
                        .iter()
                        .map(|album_id| self.fetch_album_tracks(album_id)),
                )
                .await
                .into_iter();
                for _ in &batch {
                    match batch_futs.next() {
                        Some(Ok(album)) => tracks.extend(album.tracks),
                        _ => failures += 1,
                    }
                }
            }

            if tracks.is_empty() && failures > 0 {
                return Err(QobuzError::Unavailable(format!(
                    "Artist {artist_id}: all {failures} album lookups failed"
                )));
            }

            Ok(ArtistTracks {
                artist_id: artist_id.to_owned(),
                artist_name: artist.name,
                tracks,
            })
        })
    }

    fn fetch_artist_album_ids<'a>(
        &'a self,
        artist_id: &'a str,
    ) -> BoxFuture<'a, Result<Vec<String>, QobuzError>> {
        Box::pin(async move {
            let (app_id, _) = self.ensure_tokens().await;
            let url =
                format!("{API_BASE}/artist/get?artist_id={artist_id}&extra=albums&app_id={app_id}");
            let res = self
                .client
                .get(&url)
                .send()
                .await
                .map_err(|e| QobuzError::Network(e.to_string()))?;

            if res.status() == reqwest::StatusCode::NOT_FOUND {
                return Err(QobuzError::NotFound(format!(
                    "Artist {artist_id} not found"
                )));
            }
            if !res.status().is_success() {
                return Err(QobuzError::Message(format!(
                    "Qobuz returned HTTP {}",
                    res.status()
                )));
            }

            let artist: QobuzArtistData = res
                .json()
                .await
                .map_err(|e| QobuzError::Message(format!("JSON error: {e}")))?;

            let album_ids: Vec<String> = artist
                .albums
                .map(|a| a.items.into_iter().map(|item| item.id_string()).collect())
                .unwrap_or_default();

            Ok(album_ids)
        })
    }

    fn fetch_playlist_tracks<'a>(
        &'a self,
        playlist_id: &'a str,
    ) -> BoxFuture<'a, Result<PlaylistData, QobuzError>> {
        Box::pin(async move {
            let (app_id, _) = self.ensure_tokens().await;
            let url = format!("{API_BASE}/playlist/get?playlist_id={playlist_id}&app_id={app_id}");

            let res = self
                .client
                .get(&url)
                .send()
                .await
                .map_err(|e| QobuzError::Network(e.to_string()))?;

            match res.status() {
                s if s == reqwest::StatusCode::NOT_FOUND => {
                    return Err(QobuzError::NotFound(format!(
                        "Playlist {playlist_id} not found"
                    )));
                }
                s if s == reqwest::StatusCode::UNAUTHORIZED
                    || s == reqwest::StatusCode::FORBIDDEN =>
                {
                    self.invalidate_tokens().await;
                    return Err(QobuzError::Auth(format!(
                        "Playlist {playlist_id}: Qobuz returned {s}"
                    )));
                }
                s if s == reqwest::StatusCode::TOO_MANY_REQUESTS => {
                    return Err(QobuzError::RateLimit(format!(
                        "Playlist {playlist_id}: Qobuz returned {s}"
                    )));
                }
                s if !s.is_success() => {
                    return Err(QobuzError::Message(format!(
                        "Playlist {playlist_id}: Qobuz returned {s}"
                    )));
                }
                _ => {}
            }

            let playlist: QobuzPlaylist = res
                .json()
                .await
                .map_err(|e| QobuzError::Message(format!("JSON error: {e}")))?;

            Ok(playlist.to_playlist_data())
        })
    }
}
