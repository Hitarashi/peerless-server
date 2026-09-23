//! Apple Music playlist resolver.
//!
//! Two jobs:
//!
//! 1. Scrape the Apple Music web-player developer token and cache it.
//! 2. Fetch playlist metadata/tracks from the AMP API with US-storefront
//!    fallback.
//!
//! All network I/O crosses the `PlaylistHttp` seam so tests run offline
//! against canned responses.

use std::{
    collections::HashMap,
    future::Future,
    sync::Arc,
    time::{Duration, Instant},
};

use music::{PlaylistData, PlaylistTrack};
use serde::Deserialize;

/// Chrome UA used by every request here.
pub const APPLE_USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/145.0.0.0";

/// User-facing playlist resolution failures.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PlaylistError {
    #[error("Playlist lookup timed out after {elapsed_ms}ms: {message}")]
    TimedOut { elapsed_ms: u64, message: String },
    #[error("Playlist {playlist_id} not found on storefront '{storefront}'")]
    NotFound {
        playlist_id: String,
        storefront: String,
    },
    #[error("Apple Music API returned HTTP {status}")]
    Http { status: u16 },
    #[error("No playlist found matching ID {playlist_id}")]
    NoData { playlist_id: String },
    #[error("{0}")]
    Other(String),
}

/// One header for an HTTP GET.
pub type Header = (String, String);

/// The seam every playlist fetch crosses. Like the catalog `Transport` but
/// carries full headers (the AMP API needs `Authorization` + `Origin`) and
/// surfaces HTTP status codes as errors.
pub trait PlaylistHttp: Send + Sync {
    fn get(
        &self,
        url: &str,
        headers: &[Header],
        timeout: Duration,
    ) -> impl Future<Output = Result<String, PlaylistHttpError>> + Send;
}

/// Failures of a single HTTP GET. Any `fetch()` throw (timeout or network)
/// folds into one "timed out after Xms" message; non-OK statuses are
/// checked explicitly (404 vs everything else).
#[derive(Debug, thiserror::Error)]
pub enum PlaylistHttpError {
    #[error("request failed: {0}")]
    Network(String),
    #[error("HTTP {0}")]
    Status(u16),
}

/// Production adapter over `reqwest`.
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
            .map_err(|e| PlaylistHttpError::Network(e.to_string()))?;
        let status = response.status().as_u16();
        if !(200..300).contains(&status) {
            return Err(PlaylistHttpError::Status(status));
        }
        response
            .text()
            .await
            .map_err(|e| PlaylistHttpError::Network(e.to_string()))
    }
}

pub fn ua_header() -> Header {
    ("User-Agent".to_string(), APPLE_USER_AGENT.to_string())
}

pub fn auth_headers(token: &str) -> Vec<Header> {
    vec![
        ua_header(),
        ("Authorization".to_string(), format!("Bearer {token}")),
        ("Origin".to_string(), "https://music.apple.com".to_string()),
    ]
}

pub use crate::token::DeveloperTokenProvider;

/// Playlist client with the shared token cache (module-level
/// `cachedToken` variables; one client hands every fetch the same cache).
#[derive(Debug)]
pub struct PlaylistClient<H: PlaylistHttp> {
    pub(crate) http: H,
    token_provider: Arc<DeveloperTokenProvider<H>>,
}

impl<H: PlaylistHttp + Clone> PlaylistClient<H> {
    pub fn new(http: H) -> Self {
        Self::with_token_provider(http.clone(), Arc::new(DeveloperTokenProvider::new(http)))
    }

    pub fn with_token_provider(http: H, token_provider: Arc<DeveloperTokenProvider<H>>) -> Self {
        Self {
            http,
            token_provider,
        }
    }

    pub fn token_provider(&self) -> &Arc<DeveloperTokenProvider<H>> {
        &self.token_provider
    }

    /// Retrieves and caches the Apple Music Web Client developer token.
    pub async fn get_developer_token(&self) -> Result<String, String> {
        self.token_provider.get_developer_token().await
    }

    /// Drops the cached token so the next request performs a fresh scrape.
    pub fn invalidate_developer_token(&self) {
        self.token_provider.invalidate_developer_token();
    }

    /// `fetchPlaylistTracks`: fetch with US-storefront fallback on failure.
    pub async fn fetch_playlist_tracks(
        &self,
        playlist_id: &str,
        storefront: &str,
    ) -> Result<PlaylistData, PlaylistError> {
        let sf_raw = storefront.to_ascii_lowercase();
        let sf = if sf_raw.is_empty() {
            "us".to_string()
        } else {
            sf_raw
        };

        match self.fetch_playlist_internal(playlist_id, &sf).await {
            Ok(data) => Ok(data),
            Err(err) => {
                if sf != "us" {
                    tracing::debug!("Retrying playlist lookup on US storefront fallback");
                    self.fetch_playlist_internal(playlist_id, "us").await
                } else {
                    Err(err)
                }
            }
        }
    }

    /// `fetchPlaylistInternal` — the single-storefront fetch with pagination.
    async fn fetch_playlist_internal(
        &self,
        playlist_id: &str,
        storefront: &str,
    ) -> Result<PlaylistData, PlaylistError> {
        let mut token = self
            .get_developer_token()
            .await
            .map_err(PlaylistError::Other)?;
        let initial_url = format!(
            "https://amp-api.music.apple.com/v1/catalog/{}/playlists/{}",
            url_encode(storefront),
            url_encode(playlist_id)
        );

        let start = Instant::now();
        let body = match self
            .http
            .get(&initial_url, &auth_headers(&token), Duration::from_secs(20))
            .await
        {
            Ok(body) => body,
            Err(PlaylistHttpError::Network(message)) => {
                let elapsed_ms = start.elapsed().as_millis() as u64;
                tracing::error!(
                    playlist_id,
                    elapsed_ms,
                    "Apple Music playlist request timed out or network failed"
                );
                return Err(PlaylistError::TimedOut {
                    elapsed_ms,
                    message,
                });
            }
            Err(PlaylistHttpError::Status(404)) => {
                return Err(PlaylistError::NotFound {
                    playlist_id: playlist_id.to_string(),
                    storefront: storefront.to_string(),
                });
            }
            Err(PlaylistHttpError::Status(401 | 403)) => {
                // A cached token can expire or be revoked before its local
                // TTL. Refresh once, then surface the second failure.
                self.invalidate_developer_token();
                token = self
                    .get_developer_token()
                    .await
                    .map_err(PlaylistError::Other)?;
                self.http
                    .get(&initial_url, &auth_headers(&token), Duration::from_secs(20))
                    .await
                    .map_err(|error| match error {
                        PlaylistHttpError::Network(message) => PlaylistError::TimedOut {
                            elapsed_ms: start.elapsed().as_millis() as u64,
                            message,
                        },
                        PlaylistHttpError::Status(404) => PlaylistError::NotFound {
                            playlist_id: playlist_id.to_string(),
                            storefront: storefront.to_string(),
                        },
                        PlaylistHttpError::Status(status) => PlaylistError::Http { status },
                    })?
            }
            Err(PlaylistHttpError::Status(status)) => {
                return Err(PlaylistError::Http { status });
            }
        };

        let json: RawPlaylistResponse =
            serde_json::from_str(&body).map_err(|e| PlaylistError::Other(e.to_string()))?;

        let playlist_item =
            json.data
                .as_ref()
                .and_then(|d| d.first())
                .ok_or(PlaylistError::NoData {
                    playlist_id: playlist_id.to_string(),
                })?;

        let title = playlist_item
            .attributes
            .as_ref()
            .and_then(|a| a.name.clone())
            .unwrap_or_else(|| "Untitled Playlist".to_string());
        let curator_name = playlist_item
            .attributes
            .as_ref()
            .and_then(|a| a.curator_name.clone());
        let description = playlist_item
            .attributes
            .as_ref()
            .and_then(|a| a.description.as_ref().and_then(|d| d.standard.clone()));

        let mut tracks: Vec<PlaylistTrack> = Vec::new();
        let initial_tracks = playlist_item
            .relationships
            .as_ref()
            .and_then(|r| r.tracks.as_ref())
            .and_then(|t| t.data.as_ref())
            .map(|d| d.as_slice())
            .unwrap_or_default();
        for t in initial_tracks {
            if t.id.as_deref().is_some_and(|id| !id.is_empty()) {
                tracks.push(map_track(t));
            }
        }

        // Pagination: follow `next` while present, joining relative URLs to
        // the AMP origin. Any failed page silently stops paging.
        let mut next_url = playlist_item
            .relationships
            .as_ref()
            .and_then(|r| r.tracks.as_ref())
            .and_then(|t| t.next.clone());
        while let Some(next) = next_url {
            let full_next = if next.starts_with("http") {
                next
            } else {
                format!("https://amp-api.music.apple.com{next}")
            };
            match self
                .http
                .get(&full_next, &auth_headers(&token), Duration::from_secs(15))
                .await
            {
                Ok(body) => match serde_json::from_str::<RawTracksPageResponse>(&body) {
                    Ok(page) => {
                        for t in page.data.unwrap_or_default() {
                            if t.id.as_deref().is_some_and(|id| !id.is_empty()) {
                                tracks.push(map_track(&t));
                            }
                        }
                        next_url = page.next;
                    }
                    Err(_) => break,
                },
                Err(_) => break,
            }
        }

        tracing::info!(
            playlist_id,
            curator = curator_name.as_deref().unwrap_or_default(),
            track_count = tracks.len(),
            title = %title,
            "Apple Music playlist resolved"
        );

        Ok(PlaylistData {
            id: playlist_id.to_string(),
            title,
            curator_name,
            description,
            tracks,
        })
    }

    /// Fetch ISRCs for a batch of Apple track IDs via the Apple Music Catalog API.
    pub async fn fetch_songs_isrc(
        &self,
        song_ids: &[&str],
        storefront: &str,
    ) -> Result<HashMap<String, String>, PlaylistError> {
        if song_ids.is_empty() {
            return Ok(HashMap::new());
        }

        let sf_raw = storefront.to_ascii_lowercase();
        let sf = if sf_raw.is_empty() { "us" } else { &sf_raw };

        let ids_param = song_ids.join(",");
        let url = format!(
            "https://amp-api.music.apple.com/v1/catalog/{}/songs?ids={}",
            url_encode(sf),
            ids_param
        );

        let mut token = self
            .get_developer_token()
            .await
            .map_err(PlaylistError::Other)?;

        let body = match self
            .http
            .get(&url, &auth_headers(&token), Duration::from_secs(20))
            .await
        {
            Ok(b) => b,
            Err(PlaylistHttpError::Status(401 | 403)) => {
                self.invalidate_developer_token();
                token = self
                    .get_developer_token()
                    .await
                    .map_err(PlaylistError::Other)?;
                self.http
                    .get(&url, &auth_headers(&token), Duration::from_secs(20))
                    .await
                    .map_err(|e| PlaylistError::Other(e.to_string()))?
            }
            Err(e) => return Err(PlaylistError::Other(e.to_string())),
        };

        let res: RawSongsResponse =
            serde_json::from_str(&body).map_err(|e| PlaylistError::Other(e.to_string()))?;

        let mut map = HashMap::new();
        if let Some(items) = res.data {
            for item in items {
                if let Some(attrs) = item.attributes
                    && let Some(isrc) = attrs.isrc.filter(|s| !s.is_empty())
                {
                    map.insert(item.id, isrc);
                }
            }
        }

        Ok(map)
    }

    /// Single-song ISRC lookup convenience helper.
    pub async fn fetch_song_isrc(
        &self,
        song_id: &str,
        storefront: &str,
    ) -> Result<Option<String>, PlaylistError> {
        let map = self.fetch_songs_isrc(&[song_id], storefront).await?;
        Ok(map.get(song_id).cloned())
    }
}

fn map_track(t: &RawTrack) -> PlaylistTrack {
    let raw_id = t.id.clone().unwrap_or_default();
    PlaylistTrack {
        id: raw_id.clone(),
        title: t
            .attributes
            .as_ref()
            .and_then(|a| a.name.clone())
            .filter(|n| !n.is_empty())
            .unwrap_or_else(|| format!("Track {raw_id}")),
        artist: t
            .attributes
            .as_ref()
            .and_then(|a| a.artist_name.clone())
            .filter(|n| !n.is_empty())
            .unwrap_or_else(|| "Unknown Artist".to_string()),
        duration: t
            .attributes
            .as_ref()
            .and_then(|a| a.duration_in_millis)
            // A zero or missing duration maps to `None` (not 0).
            // missing maps to None; .5 rounds half-up.
            .filter(|ms| *ms != 0)
            .map(|ms| ((ms as f64) / 1000.0).round() as u64),
    }
}

/// URL-encode a path segment.
fn url_encode(segment: &str) -> String {
    let mut out = String::new();
    for byte in segment.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char)
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

#[derive(Debug, Deserialize)]
struct RawPlaylistResponse {
    #[serde(default)]
    data: Option<Vec<RawPlaylistItem>>,
}

#[derive(Debug, Deserialize)]
struct RawPlaylistItem {
    #[serde(default)]
    attributes: Option<RawAttributes>,
    #[serde(default)]
    relationships: Option<RawRelationships>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawAttributes {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    curator_name: Option<String>,
    #[serde(default)]
    description: Option<RawDescription>,
}

#[derive(Debug, Deserialize)]
struct RawDescription {
    #[serde(default)]
    standard: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RawRelationships {
    #[serde(default)]
    tracks: Option<RawTracks>,
}

#[derive(Debug, Deserialize)]
struct RawTracks {
    #[serde(default)]
    next: Option<String>,
    #[serde(default)]
    data: Option<Vec<RawTrack>>,
}

#[derive(Debug, Deserialize)]
struct RawTracksPageResponse {
    #[serde(default)]
    next: Option<String>,
    #[serde(default)]
    data: Option<Vec<RawTrack>>,
}

#[derive(Debug, Deserialize)]
struct RawTrack {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    attributes: Option<RawTrackAttributes>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawTrackAttributes {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    artist_name: Option<String>,
    #[serde(default)]
    duration_in_millis: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct RawSongsResponse {
    #[serde(default)]
    data: Option<Vec<RawSongItem>>,
}

#[derive(Debug, Deserialize)]
struct RawSongItem {
    id: String,
    #[serde(default)]
    attributes: Option<RawSongAttributes>,
}

#[derive(Debug, Deserialize)]
struct RawSongAttributes {
    #[serde(default)]
    isrc: Option<String>,
}

#[cfg(test)]
mod tests {
    use std::{collections::VecDeque, sync::Mutex};

    use super::*;
    use crate::token::{
        find_asset_path, find_developer_token_var, find_direct_jwt, find_var_assignment,
    };

    type RequestLog = Arc<Mutex<Vec<(String, Vec<Header>)>>>;
    type ResponseQueue = Arc<Mutex<VecDeque<Result<&'static str, PlaylistHttpError>>>>;

    /// Serves queued responses strictly in request order (first pop wins);
    /// records every (url, headers) pair for assertions.
    #[derive(Clone)]
    struct FakeHttp {
        responses: ResponseQueue,
        requested: RequestLog,
    }

    impl FakeHttp {
        fn new(responses: Vec<Result<&'static str, PlaylistHttpError>>) -> Self {
            Self {
                responses: Arc::new(Mutex::new(VecDeque::from(responses))),
                requested: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn requests(&self) -> Vec<(String, Vec<Header>)> {
            self.requested.lock().unwrap().clone()
        }
    }

    impl PlaylistHttp for FakeHttp {
        async fn get(
            &self,
            url: &str,
            headers: &[Header],
            timeout: Duration,
        ) -> Result<String, PlaylistHttpError> {
            let _ = timeout;
            self.requested
                .lock()
                .unwrap()
                .push((url.to_string(), headers.to_vec()));
            match self.responses.lock().unwrap().pop_front() {
                Some(Ok(body)) => Ok(body.to_string()),
                Some(Err(e)) => Err(e),
                None => Err(PlaylistHttpError::Network("no queued response".into())),
            }
        }
    }

    const ASSET_URL: &str = "https://music.apple.com/assets/index~ab12CD.js";
    const BROWSE_HTML: &str = "<html><script src=\"/assets/index~ab12CD.js\"></script></html>";
    const JS_ASSET_VAR: &str = "var e=\"eyJhvar.okay.sig\";var t={developerToken:e};";
    const JS_ASSET_DIRECT: &str = "nothing here eyJhfirst.second.sig tail";
    const AMP_US_PL1: &str = "https://amp-api.music.apple.com/v1/catalog/us/playlists/pl.1";

    #[tokio::test]
    async fn token_scrape_via_var_assignment() {
        let http = FakeHttp::new(vec![Ok(BROWSE_HTML), Ok(JS_ASSET_VAR)]);
        let client = PlaylistClient::new(http);
        let token = client.get_developer_token().await.unwrap();
        assert_eq!(token, "eyJhvar.okay.sig");
        assert_eq!(
            client.http.requests()[0].0,
            "https://music.apple.com/us/browse"
        );
        assert_eq!(client.http.requests()[1].0, ASSET_URL);
    }

    #[tokio::test]
    async fn token_scrape_direct_jwt_fallback() {
        let http = FakeHttp::new(vec![Ok(BROWSE_HTML), Ok(JS_ASSET_DIRECT)]);
        let client = PlaylistClient::new(http);
        let token = client.get_developer_token().await.unwrap();
        assert_eq!(token, "eyJhfirst.second.sig");
    }

    #[tokio::test]
    async fn scrape_failure_is_returned_and_not_cached() {
        let http = FakeHttp::new(vec![]); // no responses: first request fails
        let client = PlaylistClient::new(http);
        assert!(client.get_developer_token().await.is_err());
        assert!(client.get_developer_token().await.is_err());
        assert_eq!(
            client.http.requests().len(),
            2,
            "failed scrapes are not cached"
        );
    }

    #[tokio::test]
    async fn successful_scrape_caches_24h() {
        let http = FakeHttp::new(vec![Ok(BROWSE_HTML), Ok(JS_ASSET_VAR)]);
        let client = PlaylistClient::new(http);
        let _ = client.get_developer_token().await.unwrap();
        let _ = client.get_developer_token().await.unwrap();
        assert_eq!(
            client.http.requests().len(),
            2,
            "token cache hit — only the initial 2 scrape requests"
        );
    }

    #[tokio::test]
    async fn fetch_playlist_sends_auth_headers() {
        let body = r#"{"data":[{"id":"pl.1","attributes":{"name":"Mix"},"relationships":{"tracks":{"data":[{"id":"100","attributes":{"name":"T1","artistName":"A1","durationInMillis":1500}}]}}}]}"#;
        let http = FakeHttp::new(vec![Ok(BROWSE_HTML), Ok(JS_ASSET_VAR), Ok(body)]);
        let client = PlaylistClient::new(http);
        let data = client.fetch_playlist_tracks("pl.1", "us").await.unwrap();
        assert_eq!(data.title, "Mix");
        assert_eq!(data.tracks.len(), 1);
        assert_eq!(data.tracks[0].duration, Some(2));

        let requests = client.http.requests();
        let amp_call = requests
            .iter()
            .find(|(url, _)| url.contains("amp-api"))
            .expect("amp call recorded");
        assert_eq!(amp_call.0, AMP_US_PL1);
        let header = |name: &str| {
            amp_call
                .1
                .iter()
                .find(|(k, _)| k == name)
                .map(|(_, v)| v.clone())
                .unwrap_or_default()
        };
        assert_eq!(header("Authorization"), "Bearer eyJhvar.okay.sig");
        assert_eq!(header("Origin"), "https://music.apple.com");
        assert_eq!(header("User-Agent"), APPLE_USER_AGENT);
    }

    #[tokio::test]
    async fn refreshes_cached_token_once_after_auth_failure() {
        let body = r#"{"data":[{"id":"pl.1","attributes":{"name":"Mix"},"relationships":{"tracks":{"data":[]}}}]}"#;
        let http = FakeHttp::new(vec![
            Ok(BROWSE_HTML),
            Ok(JS_ASSET_VAR),
            Err(PlaylistHttpError::Status(401)),
            Ok(BROWSE_HTML),
            Ok(JS_ASSET_DIRECT),
            Ok(body),
        ]);
        let client = PlaylistClient::new(http);
        let data = client.fetch_playlist_tracks("pl.1", "us").await.unwrap();
        assert_eq!(data.title, "Mix");
        assert_eq!(
            client.http.requests().len(),
            6,
            "the token is regenerated exactly once after authentication failure"
        );
    }

    #[tokio::test]
    async fn fetch_playlist_404_then_us_fallback() {
        // Storefront 'gb' 404s → retried on 'us' which succeeds.
        let us_body = r#"{"data":[{"id":"pl.1","attributes":{"name":"US"},"relationships":{"tracks":{"data":[{"id":"7"}]}}}]}"#;
        let http = FakeHttp::new(vec![
            Ok(BROWSE_HTML),
            Ok(JS_ASSET_VAR),
            Err(PlaylistHttpError::Status(404)),
            Ok(us_body),
        ]);
        let client = PlaylistClient::new(http);
        let data = client.fetch_playlist_tracks("pl.1", "gb").await.unwrap();
        assert_eq!(data.title, "US");
        assert_eq!(data.tracks.len(), 1);
        assert_eq!(data.tracks[0].title, "Track 7");
        assert_eq!(data.tracks[0].artist, "Unknown Artist");
        let requests = client.http.requests();
        assert_eq!(
            requests[2].0,
            "https://amp-api.music.apple.com/v1/catalog/gb/playlists/pl.1"
        );
        assert_eq!(requests[3].0, AMP_US_PL1);
    }

    #[tokio::test]
    async fn fetch_playlist_404_on_us_reports_not_found() {
        let http = FakeHttp::new(vec![
            Ok(BROWSE_HTML),
            Ok(JS_ASSET_VAR),
            Err(PlaylistHttpError::Status(404)),
        ]);
        let client = PlaylistClient::new(http);
        let err = client
            .fetch_playlist_tracks("pl.1", "us")
            .await
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "Playlist pl.1 not found on storefront 'us'"
        );
    }

    #[tokio::test]
    async fn fetch_playlist_http_error_string() {
        let http = FakeHttp::new(vec![
            Ok(BROWSE_HTML),
            Ok(JS_ASSET_VAR),
            Err(PlaylistHttpError::Status(503)),
        ]);
        let client = PlaylistClient::new(http);
        let err = client
            .fetch_playlist_tracks("pl.1", "us")
            .await
            .unwrap_err();
        assert_eq!(err.to_string(), "Apple Music API returned HTTP 503");
    }

    #[tokio::test]
    async fn fetch_playlist_no_data() {
        let body = r#"{"data":[]}"#;
        let http = FakeHttp::new(vec![Ok(BROWSE_HTML), Ok(JS_ASSET_VAR), Ok(body)]);
        let client = PlaylistClient::new(http);
        let err = client
            .fetch_playlist_tracks("pl.x", "us")
            .await
            .unwrap_err();
        assert_eq!(err.to_string(), "No playlist found matching ID pl.x");
    }

    #[tokio::test]
    async fn fetch_playlist_untitled_defaults() {
        let body = r#"{"data":[{"id":"pl.1","relationships":{"tracks":{"data":[{"id":"7"}]}}}]}"#;
        let http = FakeHttp::new(vec![Ok(BROWSE_HTML), Ok(JS_ASSET_VAR), Ok(body)]);
        let client = PlaylistClient::new(http);
        let data = client.fetch_playlist_tracks("pl.1", "us").await.unwrap();
        assert_eq!(data.title, "Untitled Playlist");
        assert!(data.curator_name.is_none());
        assert!(data.description.is_none());
    }

    #[tokio::test]
    async fn pagination_follows_next() {
        let first = r#"{"data":[{"id":"pl.p","relationships":{"tracks":{"next":"/v1/catalog/us/playlists/pl.p/tracks?offset=100","data":[{"id":"1"}]}}}]}"#;
        let page = r#"{"next":null,"data":[{"id":"2"},{"id":"3"}]}"#;
        let http = FakeHttp::new(vec![Ok(BROWSE_HTML), Ok(JS_ASSET_VAR), Ok(first), Ok(page)]);
        let client = PlaylistClient::new(http);
        let data = client.fetch_playlist_tracks("pl.p", "us").await.unwrap();
        assert_eq!(data.tracks.len(), 3);
        let requests = client.http.requests();
        assert_eq!(
            requests[3].0,
            "https://amp-api.music.apple.com/v1/catalog/us/playlists/pl.p/tracks?offset=100"
        );
    }

    #[tokio::test]
    async fn pagination_absolute_next_url() {
        let first = r#"{"data":[{"id":"pl.p","relationships":{"tracks":{"next":"https://amp-api.music.apple.com/v1/catalog/us/playlists/pl.p/tracks?offset=1","data":[{"id":"1"}]}}}]}"#;
        let page = r#"{"next":null,"data":[{"id":"2"}]}"#;
        let http = FakeHttp::new(vec![Ok(BROWSE_HTML), Ok(JS_ASSET_VAR), Ok(first), Ok(page)]);
        let client = PlaylistClient::new(http);
        let data = client.fetch_playlist_tracks("pl.p", "us").await.unwrap();
        assert_eq!(data.tracks.len(), 2);
    }

    #[tokio::test]
    async fn pagination_stops_on_bad_page() {
        let first = r#"{"data":[{"id":"pl.p","relationships":{"tracks":{"next":"/v1/catalog/us/playlists/pl.p/tracks?offset=100","data":[{"id":"1"}]}}}]}"#;
        let bad = "not json at all";
        let http = FakeHttp::new(vec![Ok(BROWSE_HTML), Ok(JS_ASSET_VAR), Ok(first), Ok(bad)]);
        let client = PlaylistClient::new(http);
        let data = client.fetch_playlist_tracks("pl.p", "us").await.unwrap();
        assert_eq!(data.tracks.len(), 1);
    }

    #[tokio::test]
    async fn empty_storefront_becomes_us() {
        let body = r#"{"data":[{"id":"pl.1","attributes":{"name":"US"},"relationships":{"tracks":{"data":[]}}}]}"#;
        let http = FakeHttp::new(vec![Ok(BROWSE_HTML), Ok(JS_ASSET_VAR), Ok(body)]);
        let client = PlaylistClient::new(http);
        let data = client.fetch_playlist_tracks("pl.1", "").await.unwrap();
        assert_eq!(data.title, "US");
        let requests = client.http.requests();
        assert_eq!(requests[2].0, AMP_US_PL1);
    }

    #[test]
    fn asset_path_scanner() {
        assert_eq!(
            find_asset_path("<script src=\"/assets/index~ab12CD.js\">"),
            Some("/assets/index~ab12CD.js".to_string())
        );
        assert_eq!(find_asset_path("no assets"), None);
        assert_eq!(find_asset_path("/assets/index~.js"), None);
    }

    #[test]
    fn developer_token_var_scanner() {
        assert_eq!(
            find_developer_token_var("var t={developerToken:e};"),
            Some("e".to_string())
        );
        assert_eq!(
            find_developer_token_var("developerToken:$abc_1 rest"),
            Some("$abc_1".to_string())
        );
        assert_eq!(find_developer_token_var("nothing"), None);
    }

    #[test]
    fn var_assignment_scanner() {
        assert_eq!(
            find_var_assignment("var e = \"tok\";", "e"),
            Some("tok".to_string())
        );
        assert_eq!(
            find_var_assignment("var e=\"tok\";more", "e"),
            Some("tok".to_string())
        );
        assert_eq!(
            find_var_assignment("var $e=\"t\";", "$e"),
            Some("t".to_string())
        );
        assert_eq!(find_var_assignment("var f=\"x\";", "e"), None);
    }

    #[test]
    fn direct_jwt_scanner() {
        assert_eq!(
            find_direct_jwt("x eyJhseg1.seg2.seg3 y"),
            Some("eyJhseg1.seg2.seg3".to_string())
        );
        assert_eq!(find_direct_jwt("no jwt"), None);
        assert_eq!(find_direct_jwt("eyJh only.one"), None);
    }

    #[test]
    fn url_encode_keeps_unreserved() {
        assert_eq!(url_encode("us"), "us");
        assert_eq!(url_encode("pl.1234"), "pl.1234");
        assert_eq!(url_encode("a b/c"), "a%20b%2Fc");
    }

    #[tokio::test]
    async fn fetch_songs_isrc_batch_and_single() {
        let songs_json = r#"{"data":[
            {"id":"1001","attributes":{"isrc":"USUM71703861"}},
            {"id":"1002","attributes":{"isrc":"GBAYE0601498"}},
            {"id":"1003","attributes":{"isrc":""}},
            {"id":"1004","attributes":{}}
        ]}"#;
        let http = FakeHttp::new(vec![Ok(BROWSE_HTML), Ok(JS_ASSET_VAR), Ok(songs_json)]);
        let client = PlaylistClient::new(http);
        let map = client
            .fetch_songs_isrc(&["1001", "1002", "1003", "1004"], "us")
            .await
            .unwrap();

        assert_eq!(map.get("1001").map(|s| s.as_str()), Some("USUM71703861"));
        assert_eq!(map.get("1002").map(|s| s.as_str()), Some("GBAYE0601498"));
        assert_eq!(map.get("1003"), None);
        assert_eq!(map.get("1004"), None);

        let requests = client.http.requests();
        assert_eq!(
            requests[2].0,
            "https://amp-api.music.apple.com/v1/catalog/us/songs?ids=1001,1002,1003,1004"
        );
    }
}
