//! Lyricsporn-backed playlist metadata and ISRC resolver.
//!
//! Playlist metadata and track lists are fetched through Lyricsporn. Audio
//! acquisition remains a separate Apple wrapper and mirror workflow.

use std::{
    collections::HashMap,
    future::Future,
    time::{Duration, Instant},
};

use music::{PlaylistData, PlaylistTrack, url::urlencode};
use serde_json::Value;

const LYRICSPORN_USER_AGENT: &str = "peerless-server";

/// User-facing playlist resolution failures.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PlaylistError {
    #[error("Lyricsporn playlist lookup timed out after {elapsed_ms}ms: {message}")]
    TimedOut { elapsed_ms: u64, message: String },
    #[error("Playlist {playlist_id} not found on storefront '{storefront}'")]
    NotFound {
        playlist_id: String,
        storefront: String,
    },
    #[error("Lyricsporn API returned HTTP {status}")]
    Http { status: u16 },
    #[error("No playlist found matching ID {playlist_id}")]
    NoData { playlist_id: String },
    #[error("{0}")]
    Other(String),
}

/// One header for an HTTP GET.
pub type Header = (String, String);

/// The seam every playlist fetch crosses, with status codes surfaced so
/// missing Apple Music IDs can be reported without scraping Apple auth.
pub trait PlaylistHttp: Send + Sync {
    fn get(
        &self,
        url: &str,
        headers: &[Header],
        timeout: Duration,
    ) -> impl Future<Output = Result<String, PlaylistHttpError>> + Send;
}

#[derive(Debug, thiserror::Error)]
pub enum PlaylistHttpError {
    #[error("request failed: {0}")]
    Network(String),
    #[error("HTTP {0}")]
    Status(u16),
}

#[derive(Clone, Default)]
pub struct ReqwestPlaylistHttp {
    client: reqwest::Client,
}

impl ReqwestPlaylistHttp {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }
}

impl PlaylistHttp for ReqwestPlaylistHttp {
    async fn get(
        &self,
        url: &str,
        headers: &[Header],
        timeout: Duration,
    ) -> Result<String, PlaylistHttpError> {
        let mut request = self.client.get(url).timeout(timeout);
        for (name, value) in headers {
            request = request.header(name, value);
        }
        let response = request
            .send()
            .await
            .map_err(|error| PlaylistHttpError::Network(error.to_string()))?;
        let status = response.status().as_u16();
        if !(200..300).contains(&status) {
            return Err(PlaylistHttpError::Status(status));
        }
        response
            .text()
            .await
            .map_err(|error| PlaylistHttpError::Network(error.to_string()))
    }
}

#[derive(Debug)]
pub struct PlaylistClient<H: PlaylistHttp> {
    http: H,
    api_endpoint: engine::settings::LyricspornApiEndpoint,
}

impl<H: PlaylistHttp> PlaylistClient<H> {
    pub fn new(http: H) -> Self {
        Self::with_endpoint(http, engine::settings::LyricspornApiEndpoint::default())
    }

    pub fn with_endpoint(http: H, api_endpoint: engine::settings::LyricspornApiEndpoint) -> Self {
        Self { http, api_endpoint }
    }

    fn configured_api_url(&self) -> Result<String, PlaylistError> {
        self.api_endpoint
            .get()
            .ok_or_else(|| PlaylistError::Other("Catalog API URL is not configured".to_owned()))
    }

    /// Resolve playlist metadata and all available tracks through Lyricsporn.
    /// An unavailable regional playlist is retried against the US storefront.
    pub async fn fetch_playlist_tracks(
        &self,
        playlist_id: &str,
        storefront: &str,
    ) -> Result<PlaylistData, PlaylistError> {
        let storefront = normalize_storefront(storefront);
        match self.fetch_playlist_internal(playlist_id, &storefront).await {
            Ok(data) => Ok(data),
            Err(error) if storefront != "us" => {
                tracing::debug!(
                    playlist_id,
                    "Retrying Lyricsporn playlist lookup on US storefront"
                );
                self.fetch_playlist_internal(playlist_id, "us")
                    .await
                    .or(Err(error))
            }
            Err(error) => Err(error),
        }
    }

    async fn fetch_playlist_internal(
        &self,
        playlist_id: &str,
        storefront: &str,
    ) -> Result<PlaylistData, PlaylistError> {
        let api_url = self.configured_api_url()?;
        let detail_url = format!(
            "{api_url}/playlists/{}?storefront={}&artworkSize=1000",
            urlencode(playlist_id),
            urlencode(storefront)
        );
        let start = Instant::now();
        let body = self
            .get(
                &detail_url,
                Duration::from_secs(20),
                start,
                playlist_id,
                storefront,
            )
            .await?;
        let response: Value =
            serde_json::from_str(&body).map_err(|error| PlaylistError::Other(error.to_string()))?;
        let playlist = response.get("data").ok_or_else(|| PlaylistError::NoData {
            playlist_id: playlist_id.to_owned(),
        })?;
        let title = playlist
            .get("name")
            .and_then(Value::as_str)
            .filter(|name| !name.is_empty())
            .unwrap_or("Untitled Playlist")
            .to_owned();
        let curator_name = playlist
            .get("curatorName")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned);
        let description = playlist
            .pointer("/description/standard")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned);

        let initial_url = format!(
            "{api_url}/playlists/{}/collections/tracks?storefront={}&limit=100&offset=0&artworkSize=1000",
            urlencode(playlist_id),
            urlencode(storefront)
        );
        let mut page_url = initial_url.clone();
        let mut tracks = Vec::new();
        let first_page = self
            .get_json(&page_url, Duration::from_secs(15), playlist_id, storefront)
            .await?;
        append_page_tracks(&first_page, &mut tracks);
        let mut next = first_page
            .pointer("/page/next")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned);

        for _ in 0..101 {
            let Some(next_url) = next.take() else {
                break;
            };
            let next_url = next_url_from(&page_url, &next_url, &api_url);
            let page = match self
                .get_json(&next_url, Duration::from_secs(15), playlist_id, storefront)
                .await
            {
                Ok(page) => page,
                Err(error) => {
                    tracing::warn!(playlist_id, error = %error, "Could not fetch next playlist page");
                    break;
                }
            };
            append_page_tracks(&page, &mut tracks);
            page_url = next_url;
            next = page
                .pointer("/page/next")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned);
        }

        tracing::info!(
            playlist_id,
            curator = curator_name.as_deref().unwrap_or_default(),
            track_count = tracks.len(),
            title = %title,
            "Lyricsporn playlist resolved"
        );
        Ok(PlaylistData {
            id: playlist_id.to_owned(),
            title,
            curator_name,
            description,
            tracks,
        })
    }

    /// Resolve ISRCs for a list of Apple Music song IDs via Lyricsporn.
    pub async fn fetch_songs_isrc(
        &self,
        song_ids: &[&str],
        storefront: &str,
    ) -> Result<HashMap<String, String>, PlaylistError> {
        let storefront = normalize_storefront(storefront);
        let mut isrcs = HashMap::new();
        for song_id in song_ids {
            if let Some(isrc) = self.fetch_song_isrc(song_id, &storefront).await? {
                isrcs.insert((*song_id).to_owned(), isrc);
            }
        }
        Ok(isrcs)
    }

    /// Single-song ISRC lookup convenience helper.
    pub async fn fetch_song_isrc(
        &self,
        song_id: &str,
        storefront: &str,
    ) -> Result<Option<String>, PlaylistError> {
        let storefront = normalize_storefront(storefront);
        let api_url = self.configured_api_url()?;
        let url = format!(
            "{api_url}/tracks/{}?storefront={}",
            urlencode(song_id),
            urlencode(&storefront)
        );
        let start = Instant::now();
        let body = self
            .get(&url, Duration::from_secs(20), start, song_id, &storefront)
            .await?;
        let response: Value =
            serde_json::from_str(&body).map_err(|error| PlaylistError::Other(error.to_string()))?;
        Ok(response
            .pointer("/track/isrc")
            .and_then(Value::as_str)
            .filter(|isrc| !isrc.is_empty())
            .map(ToOwned::to_owned))
    }

    async fn get(
        &self,
        url: &str,
        timeout: Duration,
        started_at: Instant,
        resource_id: &str,
        storefront: &str,
    ) -> Result<String, PlaylistError> {
        let headers = [("User-Agent".to_owned(), LYRICSPORN_USER_AGENT.to_owned())];
        self.http
            .get(url, &headers, timeout)
            .await
            .map_err(|error| map_http_error(error, started_at, resource_id, storefront))
    }

    async fn get_json(
        &self,
        url: &str,
        timeout: Duration,
        resource_id: &str,
        storefront: &str,
    ) -> Result<Value, PlaylistError> {
        let started_at = Instant::now();
        let body = self
            .get(url, timeout, started_at, resource_id, storefront)
            .await?;
        serde_json::from_str(&body).map_err(|error| PlaylistError::Other(error.to_string()))
    }
}

fn normalize_storefront(storefront: &str) -> String {
    let storefront = storefront.trim().to_ascii_lowercase();
    if storefront.is_empty() {
        "us".to_owned()
    } else {
        storefront
    }
}

fn map_http_error(
    error: PlaylistHttpError,
    started_at: Instant,
    playlist_id: &str,
    storefront: &str,
) -> PlaylistError {
    match error {
        PlaylistHttpError::Network(message) => PlaylistError::TimedOut {
            elapsed_ms: started_at.elapsed().as_millis() as u64,
            message,
        },
        PlaylistHttpError::Status(404) => PlaylistError::NotFound {
            playlist_id: playlist_id.to_owned(),
            storefront: storefront.to_owned(),
        },
        PlaylistHttpError::Status(status) => PlaylistError::Http { status },
    }
}

fn append_page_tracks(page: &Value, tracks: &mut Vec<PlaylistTrack>) {
    if let Some(items) = page.get("items").and_then(Value::as_array) {
        tracks.extend(items.iter().filter_map(map_track));
    }
}

fn map_track(item: &Value) -> Option<PlaylistTrack> {
    let id = item.get("id")?.as_str()?.to_owned();
    if id.is_empty() {
        return None;
    }
    let title = item
        .get("name")
        .and_then(Value::as_str)
        .filter(|name| !name.is_empty())
        .unwrap_or("Untitled Track")
        .to_owned();
    let artist = item
        .get("artistName")
        .and_then(Value::as_str)
        .filter(|name| !name.is_empty())
        .unwrap_or("Unknown Artist")
        .to_owned();
    let duration = item.get("durationMs").and_then(Value::as_u64).or_else(|| {
        item.get("durationMs")
            .and_then(Value::as_f64)
            .map(|duration| duration.max(0.0) as u64)
    });
    Some(PlaylistTrack {
        id,
        title,
        artist,
        duration,
    })
}

fn next_url_from(current_url: &str, next: &str, api_url: &str) -> String {
    if next.starts_with("http://") || next.starts_with("https://") {
        next.to_owned()
    } else if next.starts_with('/') {
        reqwest::Url::parse(current_url)
            .map(|url| format!("{}{next}", url.origin().ascii_serialization()))
            .unwrap_or_else(|_| format!("{api_url}{next}"))
    } else if next.starts_with('?') {
        format!(
            "{}{next}",
            current_url.split('?').next().unwrap_or(current_url)
        )
    } else {
        format!("{api_url}/{next}")
    }
}
