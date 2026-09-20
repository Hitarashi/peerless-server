//! Adapter communicating with the remote Qobuz Cloudflare Worker backend.

use std::time::Duration;

use music::{AlbumTracks, ArtistTracks, PlaylistData, TrackMeta};
use reqwest::header::{HeaderMap, HeaderValue};

use crate::{
    gateway::{BoxFuture, QobuzError, QobuzGateway, QobuzStreamInfo, normalize_sample_rate},
    types::{
        QobuzAlbumResponse, QobuzArtistResponse, QobuzPlaylistResponse, QobuzStreamUrlResponse,
        QobuzTrackResponse,
    },
};

#[derive(Debug, Clone)]
pub struct HostedWorkerAdapter {
    backend_url: String,
    api_key: Option<String>,
    client: reqwest::Client,
}

impl HostedWorkerAdapter {
    pub fn new(backend_url: String, api_key: Option<String>) -> Self {
        let clean_url = backend_url.trim().trim_end_matches('/').to_owned();
        let mut default_headers = HeaderMap::new();
        default_headers.insert(
            reqwest::header::USER_AGENT,
            HeaderValue::from_static("peerless/1.0 (Linux; x86_64)"),
        );
        let client = reqwest::Client::builder()
            .default_headers(default_headers)
            .timeout(Duration::from_secs(25))
            .build()
            .unwrap_or_default();

        Self {
            backend_url: clean_url,
            api_key: api_key.filter(|k| !k.trim().is_empty()),
            client,
        }
    }

    fn apply_auth(&self, mut req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        if let Some(key) = &self.api_key {
            req = req.header("X-API-Key", key);
        }
        req
    }

    /// Classify an HTTP status before touching the body: error payloads are
    /// often not JSON, and the engine needs typed failures (auth vs
    /// rate-limit vs not-found) to decide between retry and abort.
    fn status_error(&self, status: reqwest::StatusCode, what: &str) -> Option<QobuzError> {
        use reqwest::StatusCode;
        match status {
            StatusCode::NOT_FOUND => Some(QobuzError::NotFound(format!("{what} not found"))),
            StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => Some(QobuzError::Auth(format!(
                "{what}: backend returned {status}"
            ))),
            StatusCode::TOO_MANY_REQUESTS => Some(QobuzError::RateLimit(format!(
                "{what}: backend returned {status}"
            ))),
            s if s.is_success() => None,
            s => Some(QobuzError::Message(format!("{what}: backend returned {s}"))),
        }
    }
}

impl QobuzGateway for HostedWorkerAdapter {
    fn gateway_name(&self) -> &'static str {
        "hosted-worker"
    }

    fn fetch_track_meta<'a>(
        &'a self,
        track_id: &'a str,
    ) -> BoxFuture<'a, Result<TrackMeta, QobuzError>> {
        Box::pin(async move {
            let url = format!("{}/api/track/{}", self.backend_url, track_id);
            let req = self.apply_auth(self.client.get(&url));

            let res = req
                .send()
                .await
                .map_err(|e| QobuzError::Network(format!("Failed to reach hosted backend: {e}")))?;

            let what = format!("Track {track_id}");
            if let Some(err) = self.status_error(res.status(), &what) {
                return Err(err);
            }

            let body: QobuzTrackResponse = res
                .json()
                .await
                .map_err(|e| QobuzError::Message(format!("Failed to parse track JSON: {e}")))?;

            if let Some(err) = body.error {
                return Err(QobuzError::Message(err));
            }

            let track = body.track.ok_or_else(|| {
                QobuzError::NotFound(format!("No track payload for id {track_id}"))
            })?;

            Ok(track.to_track_meta(None))
        })
    }

    fn resolve_stream_url<'a>(
        &'a self,
        track_id: &'a str,
        format_id: u32,
        fallback: bool,
    ) -> BoxFuture<'a, Result<QobuzStreamInfo, QobuzError>> {
        Box::pin(async move {
            let url = format!(
                "{}/api/track/{}/url?quality={}&fallback={}",
                self.backend_url, track_id, format_id, fallback
            );
            let req = self.apply_auth(self.client.get(&url));

            let res = req.send().await.map_err(|e| {
                QobuzError::Network(format!("Failed to contact stream endpoint: {e}"))
            })?;

            let what = format!("Stream for track {track_id} (quality {format_id})");
            if let Some(err) = self.status_error(res.status(), &what) {
                return Err(err);
            }

            let body: QobuzStreamUrlResponse = res.json().await.map_err(|e| {
                QobuzError::Message(format!("Failed to parse stream URL response: {e}"))
            })?;

            if !body.success {
                let msg = body
                    .error
                    .unwrap_or_else(|| "Unknown backend error".to_owned());
                return Err(QobuzError::Unavailable(msg));
            }

            let data = body.data.ok_or_else(|| {
                QobuzError::Unavailable("Missing stream data in backend response".to_owned())
            })?;

            let stream_url = data.url.or(body.url).ok_or_else(|| {
                QobuzError::Unavailable("Missing stream url in backend response".to_owned())
            })?;

            Ok(QobuzStreamInfo {
                url: stream_url,
                format_id: data.format_id.unwrap_or(format_id),
                mime_type: data.mime_type.unwrap_or_else(|| "audio/flac".to_owned()),
                bit_depth: data.bit_depth.unwrap_or(16),
                sample_rate: normalize_sample_rate(data.sampling_rate),
            })
        })
    }

    fn fetch_album_tracks<'a>(
        &'a self,
        album_id: &'a str,
    ) -> BoxFuture<'a, Result<AlbumTracks, QobuzError>> {
        Box::pin(async move {
            let url = format!("{}/api/album/{}", self.backend_url, album_id);
            let req = self.apply_auth(self.client.get(&url));

            let res = req
                .send()
                .await
                .map_err(|e| QobuzError::Network(format!("Failed to reach hosted backend: {e}")))?;

            let what = format!("Album {album_id}");
            if let Some(err) = self.status_error(res.status(), &what) {
                return Err(err);
            }

            let body: QobuzAlbumResponse = res
                .json()
                .await
                .map_err(|e| QobuzError::Message(format!("Failed to parse album JSON: {e}")))?;

            if let Some(err) = body.error {
                return Err(QobuzError::Message(err));
            }

            let album = body.album.ok_or_else(|| {
                QobuzError::NotFound(format!("No album payload for id {album_id}"))
            })?;

            Ok(album.to_album_tracks())
        })
    }

    fn fetch_artist_tracks<'a>(
        &'a self,
        artist_id: &'a str,
    ) -> BoxFuture<'a, Result<ArtistTracks, QobuzError>> {
        Box::pin(async move {
            let url = format!("{}/api/artist/{}?smart=true", self.backend_url, artist_id);
            let req = self.apply_auth(self.client.get(&url));

            let res = req
                .send()
                .await
                .map_err(|e| QobuzError::Network(format!("Failed to reach hosted backend: {e}")))?;

            let what = format!("Artist {artist_id}");
            if let Some(err) = self.status_error(res.status(), &what) {
                return Err(err);
            }

            let body: QobuzArtistResponse = res
                .json()
                .await
                .map_err(|e| QobuzError::Message(format!("Failed to parse artist JSON: {e}")))?;

            if let Some(err) = body.error {
                return Err(QobuzError::Message(err));
            }

            let artist_data = body.artist.ok_or_else(|| {
                QobuzError::NotFound(format!("No artist payload for id {artist_id}"))
            })?;

            let tracks = artist_data
                .albums
                .map(|a| {
                    a.items
                        .into_iter()
                        .flat_map(|item| item.to_album_tracks().tracks)
                        .collect()
                })
                .unwrap_or_default();

            Ok(ArtistTracks {
                artist_id: artist_id.to_owned(),
                artist_name: artist_data.name,
                tracks,
            })
        })
    }

    fn fetch_artist_album_ids<'a>(
        &'a self,
        artist_id: &'a str,
    ) -> BoxFuture<'a, Result<Vec<String>, QobuzError>> {
        Box::pin(async move {
            let url = format!("{}/api/artist/{}?smart=true", self.backend_url, artist_id);
            let req = self.apply_auth(self.client.get(&url));

            let res = req
                .send()
                .await
                .map_err(|e| QobuzError::Network(format!("Failed to reach hosted backend: {e}")))?;

            let what = format!("Artist {artist_id}");
            if let Some(err) = self.status_error(res.status(), &what) {
                return Err(err);
            }

            let body: QobuzArtistResponse = res
                .json()
                .await
                .map_err(|e| QobuzError::Message(format!("Failed to parse artist JSON: {e}")))?;

            if let Some(err) = body.error {
                return Err(QobuzError::Message(err));
            }

            let artist_data = body.artist.ok_or_else(|| {
                QobuzError::NotFound(format!("No artist payload for id {artist_id}"))
            })?;

            let album_ids = artist_data
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
            let url = format!("{}/api/playlist/{}", self.backend_url, playlist_id);
            let req = self.apply_auth(self.client.get(&url));

            let res = req
                .send()
                .await
                .map_err(|e| QobuzError::Network(format!("Failed to reach hosted backend: {e}")))?;

            let what = format!("Playlist {playlist_id}");
            if let Some(err) = self.status_error(res.status(), &what) {
                return Err(err);
            }

            let body: QobuzPlaylistResponse = res
                .json()
                .await
                .map_err(|e| QobuzError::Message(format!("Failed to parse playlist JSON: {e}")))?;

            if let Some(err) = body.error {
                return Err(QobuzError::Message(err));
            }

            let playlist = body.playlist.ok_or_else(|| {
                QobuzError::NotFound(format!("No playlist payload for id {playlist_id}"))
            })?;

            Ok(playlist.to_playlist_data())
        })
    }
}
