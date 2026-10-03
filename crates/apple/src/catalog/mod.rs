//! Lyricsporn-backed Apple Music catalog client.
//!
//! Audio acquisition remains in the Apple wrapper and mirror modules. All
//! catalog metadata and search requests go through Lyricsporn, so this crate
//! never scrapes an Apple developer token or calls Apple's catalog endpoints.

mod cache;
mod transport;

use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use cache::Cache;
use music::{AlbumTracks, ArtistTracks, TrackMeta, url::urlencode};
use serde_json::Value;
use tracing::{debug, info, warn};
pub use transport::{ReqwestTransport, Transport, TransportError};

const LYRICSPORN_USER_AGENT: &str = "peerless-server";
const TRACK_TIMEOUT: Duration = Duration::from_secs(15);
const COLLECTION_TIMEOUT: Duration = Duration::from_secs(30);
const SEARCH_TIMEOUT: Duration = Duration::from_secs(15);
const COLLECTION_PAGE_SIZE: usize = 100;

/// Regional storefronts tried after `us` in the fallback chain, in order.
pub const REGIONAL_STOREFRONTS: [&str; 7] = ["jp", "gb", "in", "ca", "de", "fr", "au"];

/// Catalog lookup failures. `Message` carries user-facing text from the
/// provider boundary; transport and JSON errors retain their source.
#[derive(Debug, thiserror::Error)]
pub enum CatalogError {
    #[error("{0}")]
    Message(String),
    #[error("transport error: {0}")]
    Transport(#[from] TransportError),
    #[error("bad JSON: {0}")]
    Json(#[from] serde_json::Error),
}

/// Re-point an artwork URL at a different square size.
pub fn artwork_url_at_size(url: &str, size: u16) -> String {
    format_artwork_url(url, size)
}

fn format_artwork_url(url: &str, size: u16) -> String {
    let size = size.to_string();
    url.replace("{w}x{h}bb.{f}", &format!("{size}x{size}bb.jpg"))
        .replace("{w}", &size)
        .replace("{h}", &size)
        .replace("{f}", "jpg")
        .replace("100x100bb", &format!("{size}x{size}bb"))
}

fn normalize_storefront(storefront: &str) -> String {
    let storefront = storefront.trim().to_ascii_lowercase();
    if storefront.is_empty() {
        "us".to_owned()
    } else {
        storefront
    }
}

fn storefront_fallbacks(storefront: &str) -> Vec<String> {
    let mut storefronts = vec![storefront.to_owned()];
    if storefront != "us" {
        storefronts.push("us".to_owned());
    }
    storefronts.extend(
        REGIONAL_STOREFRONTS
            .into_iter()
            .filter(|region| *region != storefront && *region != "us")
            .map(str::to_owned),
    );
    storefronts
}

#[derive(Clone)]
enum CacheValue {
    Track(TrackMeta),
    Album(AlbumTracks),
    Artist(ArtistTracks),
    ArtistAlbumIds(Vec<String>),
    Search(Vec<TrackMeta>),
}

trait CachedValue: Clone {
    fn from_value(value: &CacheValue) -> Option<Self>;
    fn into_value(self) -> CacheValue;
}

impl CachedValue for TrackMeta {
    fn from_value(value: &CacheValue) -> Option<Self> {
        if let CacheValue::Track(value) = value {
            Some(value.clone())
        } else {
            None
        }
    }

    fn into_value(self) -> CacheValue {
        CacheValue::Track(self)
    }
}

impl CachedValue for AlbumTracks {
    fn from_value(value: &CacheValue) -> Option<Self> {
        if let CacheValue::Album(value) = value {
            Some(value.clone())
        } else {
            None
        }
    }

    fn into_value(self) -> CacheValue {
        CacheValue::Album(self)
    }
}

impl CachedValue for ArtistTracks {
    fn from_value(value: &CacheValue) -> Option<Self> {
        if let CacheValue::Artist(value) = value {
            Some(value.clone())
        } else {
            None
        }
    }

    fn into_value(self) -> CacheValue {
        CacheValue::Artist(self)
    }
}

impl CachedValue for Vec<String> {
    fn from_value(value: &CacheValue) -> Option<Self> {
        if let CacheValue::ArtistAlbumIds(value) = value {
            Some(value.clone())
        } else {
            None
        }
    }

    fn into_value(self) -> CacheValue {
        CacheValue::ArtistAlbumIds(self)
    }
}

struct TrackContext {
    album_id: Option<String>,
    album_name: Option<String>,
    album_artist: Option<String>,
    artist_id: Option<String>,
    track_count: Option<i64>,
    artwork_url: Option<String>,
}

impl TrackContext {
    fn from_album(album: &Value, album_id: &str, fallback_artist: Option<&str>) -> Self {
        Self {
            album_id: Some(album_id.to_owned()),
            album_name: string(album, "name").map(ToOwned::to_owned),
            album_artist: string(album, "artistName")
                .or(fallback_artist)
                .map(ToOwned::to_owned),
            artist_id: album_artist_id(album),
            track_count: integer(album, "trackCount"),
            artwork_url: artwork_url(album, 1000),
        }
    }
}

/// Catalog client over Lyricsporn, with a TTL cache and storefront fallback.
pub struct Catalog<T: Transport> {
    transport: T,
    api_endpoint: engine::settings::LyricspornApiEndpoint,
    cache: Mutex<Cache<CacheValue>>,
    max_cache: usize,
}

impl<T: Transport> Catalog<T> {
    pub fn new(transport: T) -> Self {
        Self::with_endpoint(
            transport,
            engine::settings::LyricspornApiEndpoint::default(),
        )
    }

    pub fn with_endpoint(
        transport: T,
        api_endpoint: engine::settings::LyricspornApiEndpoint,
    ) -> Self {
        Self::with_limits_and_endpoint(transport, 500, Duration::from_secs(10 * 60), api_endpoint)
    }

    pub fn with_limits(transport: T, max_cache: usize, ttl: Duration) -> Self {
        Self::with_limits_and_endpoint(
            transport,
            max_cache,
            ttl,
            engine::settings::LyricspornApiEndpoint::default(),
        )
    }

    pub fn with_limits_and_endpoint(
        transport: T,
        max_cache: usize,
        ttl: Duration,
        api_endpoint: engine::settings::LyricspornApiEndpoint,
    ) -> Self {
        Self {
            transport,
            api_endpoint,
            cache: Mutex::new(Cache::new(max_cache, ttl)),
            max_cache,
        }
    }

    /// Cache capacity.
    pub fn capacity(&self) -> usize {
        self.max_cache
    }

    pub fn clear_cache(&self) {
        self.cache.lock().expect("cache mutex poisoned").clear();
    }

    /// Shared handle to the transport — useful to inspect requests.
    pub fn transport(&self) -> &T {
        &self.transport
    }

    fn configured_api_url(&self) -> Result<String, CatalogError> {
        self.api_endpoint
            .get()
            .ok_or_else(|| CatalogError::Message("Lyricsporn API URL is not configured".to_owned()))
    }

    fn get_cached(&self, key: &str) -> Option<CacheValue> {
        self.cache
            .lock()
            .expect("cache mutex poisoned")
            .get(key, Instant::now())
    }

    fn set_cached(&self, key: &str, value: CacheValue) {
        self.cache
            .lock()
            .expect("cache mutex poisoned")
            .set(key, value, Instant::now());
    }

    async fn get_json(
        &self,
        url: &str,
        timeout: Duration,
        context: &str,
    ) -> Result<Value, CatalogError> {
        let body = self
            .transport
            .get(url, LYRICSPORN_USER_AGENT, timeout)
            .await
            .map_err(|error| match error {
                TransportError::Fetch { elapsed_ms, source } => CatalogError::Message(format!(
                    "Lyricsporn {context} request failed after {elapsed_ms}ms: {source}"
                )),
                TransportError::Status { status: 404 } => {
                    CatalogError::Message(format!("Lyricsporn {context} was not found"))
                }
                TransportError::Status { status } => CatalogError::Message(format!(
                    "Lyricsporn {context} request failed (HTTP {status})"
                )),
            })?;
        Ok(serde_json::from_str(&body)?)
    }

    async fn fetch_collection_items(
        &self,
        resource: &str,
        resource_id: &str,
        collection: &str,
        storefront: &str,
    ) -> Result<Vec<Value>, CatalogError> {
        let api_url = self.configured_api_url()?;
        let mut url = format!(
            "{api_url}/{}/{}/collections/{}?storefront={}&limit={}&offset=0&artworkSize=1000",
            resource,
            urlencode(resource_id),
            collection,
            urlencode(storefront),
            COLLECTION_PAGE_SIZE
        );
        let mut items = Vec::new();
        // The API caps collection offsets at 10,000. The page count bound also
        // protects against a malformed `next` link repeating forever.
        for _ in 0..101 {
            let body = self
                .get_json(&url, COLLECTION_TIMEOUT, "catalog collection")
                .await?;
            if let Some(page_items) = body.get("items").and_then(Value::as_array) {
                items.extend(page_items.iter().cloned());
            }
            let Some(next) = body
                .pointer("/page/next")
                .and_then(Value::as_str)
                .filter(|next| !next.is_empty())
            else {
                break;
            };
            url = collection_next_url(&url, next, &api_url);
        }
        Ok(items)
    }

    async fn do_fetch_track_meta(
        &self,
        track_id: &str,
        storefront: &str,
    ) -> Result<TrackMeta, CatalogError> {
        let api_url = self.configured_api_url()?;
        let url = format!(
            "{api_url}/tracks/{}?storefront={}&include=artwork,artists,album&artworkSize=1000",
            urlencode(track_id),
            urlencode(storefront)
        );
        let response = self.get_json(&url, TRACK_TIMEOUT, "track metadata").await?;
        let track = response.get("track").ok_or_else(|| {
            CatalogError::Message(format!(
                "Lyricsporn returned no metadata for track {track_id}"
            ))
        })?;
        let mut meta = map_track(track, None, None);
        if meta.id.is_empty() {
            meta.id = track_id.to_owned();
        }
        Ok(meta)
    }

    async fn do_fetch_album_tracks(
        &self,
        album_id: &str,
        storefront: &str,
    ) -> Result<AlbumTracks, CatalogError> {
        let api_url = self.configured_api_url()?;
        let album_url = format!(
            "{api_url}/albums/{}?storefront={}&include=artists&limit=100&artworkSize=1000",
            urlencode(album_id),
            urlencode(storefront)
        );
        let album_response = self
            .get_json(&album_url, COLLECTION_TIMEOUT, "album metadata")
            .await?;
        let album = album_response.get("data").ok_or_else(|| {
            CatalogError::Message(format!("Lyricsporn found no album matching ID {album_id}"))
        })?;
        let items = self
            .fetch_collection_items("albums", album_id, "tracks", storefront)
            .await?;
        if items.is_empty() {
            return Err(CatalogError::Message(format!(
                "Lyricsporn found no tracks for album {album_id}"
            )));
        }

        let context = TrackContext::from_album(album, album_id, None);
        let track_count = context
            .track_count
            .or_else(|| i64::try_from(items.len()).ok());
        let mut tracks: Vec<TrackMeta> = items
            .iter()
            .enumerate()
            .map(|(index, item)| {
                let mut track = map_track(item, Some(&context), Some(index as i64 + 1));
                if track.track_count.is_none() {
                    track.track_count = track_count;
                }
                track
            })
            .collect();
        let disc_count = tracks.iter().filter_map(|track| track.disc_number).max();
        for track in &mut tracks {
            track.disc_count = disc_count;
        }
        let album_meta = map_album(album, album_id, &tracks, track_count);

        info!(
            album_id,
            album = %album_meta.album,
            artist = %album_meta.artist,
            track_count = tracks.len(),
            "Lyricsporn album resolved"
        );
        Ok(AlbumTracks {
            album: album_meta,
            tracks,
        })
    }

    async fn do_fetch_artist_album_ids(
        &self,
        artist_id: &str,
        storefront: &str,
    ) -> Result<Vec<String>, CatalogError> {
        let items = self
            .fetch_collection_items("artists", artist_id, "albums", storefront)
            .await?;
        Ok(unique_ids(&items))
    }

    async fn do_fetch_artist_tracks(
        &self,
        artist_id: &str,
        storefront: &str,
    ) -> Result<ArtistTracks, CatalogError> {
        let api_url = self.configured_api_url()?;
        let artist_url = format!(
            "{api_url}/artists/{}?storefront={}&include=albums&limit=100&artworkSize=1000",
            urlencode(artist_id),
            urlencode(storefront)
        );
        let artist_response = self
            .get_json(&artist_url, COLLECTION_TIMEOUT, "artist metadata")
            .await?;
        let artist = artist_response.get("data").ok_or_else(|| {
            CatalogError::Message(format!(
                "Lyricsporn found no artist matching ID {artist_id}"
            ))
        })?;
        let artist_name = string(artist, "name")
            .unwrap_or("Unknown Artist")
            .to_owned();
        let albums = self
            .fetch_collection_items("artists", artist_id, "albums", storefront)
            .await?;
        let mut tracks = Vec::new();
        let mut seen_track_ids = std::collections::HashSet::new();
        let mut first_error = None;

        for album in &albums {
            let Some(album_id) = string(album, "id") else {
                continue;
            };
            let album_name = string(album, "name").unwrap_or_default();
            let context = TrackContext {
                album_id: Some(album_id.to_owned()),
                album_name: Some(album_name.to_owned()),
                album_artist: Some(
                    string(album, "artistName")
                        .unwrap_or(&artist_name)
                        .to_owned(),
                ),
                artist_id: Some(artist_id.to_owned()),
                track_count: integer(album, "trackCount"),
                artwork_url: artwork_url(album, 1000),
            };
            match self
                .fetch_collection_items("albums", album_id, "tracks", storefront)
                .await
            {
                Ok(album_tracks) => {
                    for (index, item) in album_tracks.iter().enumerate() {
                        let track = map_track(item, Some(&context), Some(index as i64 + 1));
                        if !track.id.is_empty() && seen_track_ids.insert(track.id.clone()) {
                            tracks.push(track);
                        }
                    }
                }
                Err(error) => {
                    first_error.get_or_insert(error);
                    debug!(album_id, error = %first_error.as_ref().expect("just inserted"), "Skipping unavailable artist album");
                }
            }
        }

        if tracks.is_empty() {
            return Err(first_error.unwrap_or_else(|| {
                CatalogError::Message(format!("Lyricsporn found no tracks for artist {artist_id}"))
            }));
        }
        if artist_name == "Unknown Artist" {
            info!(
                artist_id,
                tracks = tracks.len(),
                "Lyricsporn artist resolved"
            );
        } else {
            info!(artist_id, artist = %artist_name, tracks = tracks.len(), "Lyricsporn artist resolved");
        }
        Ok(ArtistTracks {
            artist_id: artist_id.to_owned(),
            artist_name,
            tracks,
        })
    }

    /// Resolve one Apple Music track ID through Lyricsporn.
    pub async fn fetch_track_meta(
        &self,
        track_id: &str,
        storefront: &str,
    ) -> Result<TrackMeta, CatalogError> {
        let storefront = normalize_storefront(storefront);
        let api_url = self.configured_api_url()?;
        let key = format!("{api_url}|track:{storefront}:{track_id}");
        if let Some(value) = self
            .get_cached(&key)
            .and_then(|value| TrackMeta::from_value(&value))
        {
            return Ok(value);
        }
        let mut original_error = None;
        let storefronts = storefront_fallbacks(&storefront);
        for region in &storefronts {
            match self.do_fetch_track_meta(track_id, region).await {
                Ok(meta) => {
                    self.set_cached(&key, meta.clone().into_value());
                    return Ok(meta);
                }
                Err(error) => {
                    if original_error.is_none() {
                        original_error = Some(error);
                    }
                    debug!(track_id, storefront = %region, "Lyricsporn track lookup missed storefront");
                }
            }
        }
        if let Some(error) = original_error {
            warn!(
                track_id,
                configured_api_url = %api_url,
                storefront = %storefront,
                error = %error,
                "Lyricsporn track lookup failed for all storefronts"
            );
            return Err(error);
        }
        Err(CatalogError::Message(format!(
            "Lyricsporn found no song matching track ID {track_id}"
        )))
    }

    /// Resolve an album and its ordered track list through Lyricsporn.
    pub async fn fetch_album_tracks(
        &self,
        album_id: &str,
        storefront: &str,
    ) -> Result<AlbumTracks, CatalogError> {
        let storefront = normalize_storefront(storefront);
        let api_url = self.configured_api_url()?;
        let key = format!("{api_url}|album:{storefront}:{album_id}");
        if let Some(value) = self
            .get_cached(&key)
            .and_then(|value| AlbumTracks::from_value(&value))
        {
            return Ok(value);
        }
        let mut original_error = None;
        for region in storefront_fallbacks(&storefront) {
            match self.do_fetch_album_tracks(album_id, &region).await {
                Ok(album) => {
                    self.set_cached(&key, album.clone().into_value());
                    return Ok(album);
                }
                Err(error) => {
                    if original_error.is_none() {
                        original_error = Some(error);
                    }
                    debug!(album_id, storefront = %region, "Lyricsporn album lookup missed storefront");
                }
            }
        }
        Err(original_error.unwrap_or_else(|| {
            CatalogError::Message(format!("Lyricsporn found no album matching ID {album_id}"))
        }))
    }

    /// Resolve all album tracks for an artist through Lyricsporn.
    pub async fn fetch_artist_tracks(
        &self,
        artist_id: &str,
        storefront: &str,
    ) -> Result<ArtistTracks, CatalogError> {
        let storefront = normalize_storefront(storefront);
        let api_url = self.configured_api_url()?;
        let key = format!("{api_url}|artist:{storefront}:{artist_id}");
        if let Some(value) = self
            .get_cached(&key)
            .and_then(|value| ArtistTracks::from_value(&value))
        {
            return Ok(value);
        }
        let mut original_error = None;
        for region in storefront_fallbacks(&storefront) {
            match self.do_fetch_artist_tracks(artist_id, &region).await {
                Ok(artist) => {
                    self.set_cached(&key, artist.clone().into_value());
                    return Ok(artist);
                }
                Err(error) => {
                    if original_error.is_none() {
                        original_error = Some(error);
                    }
                    debug!(artist_id, storefront = %region, "Lyricsporn artist lookup missed storefront");
                }
            }
        }
        Err(original_error.unwrap_or_else(|| {
            CatalogError::Message(format!(
                "Lyricsporn found no artist matching ID {artist_id}"
            ))
        }))
    }

    /// Resolve an artist's album IDs through Lyricsporn.
    pub async fn fetch_artist_album_ids(
        &self,
        artist_id: &str,
        storefront: &str,
    ) -> Result<Vec<String>, CatalogError> {
        let storefront = normalize_storefront(storefront);
        let api_url = self.configured_api_url()?;
        let key = format!("{api_url}|artist_albums:{storefront}:{artist_id}");
        if let Some(value) = self
            .get_cached(&key)
            .and_then(|value| Vec::<String>::from_value(&value))
        {
            return Ok(value);
        }
        let mut original_error = None;
        for region in storefront_fallbacks(&storefront) {
            match self.do_fetch_artist_album_ids(artist_id, &region).await {
                Ok(ids) if !ids.is_empty() => {
                    self.set_cached(&key, ids.clone().into_value());
                    return Ok(ids);
                }
                Ok(_) => {
                    original_error.get_or_insert_with(|| {
                        CatalogError::Message(format!(
                            "Lyricsporn found no albums for artist {artist_id}"
                        ))
                    });
                }
                Err(error) => {
                    original_error.get_or_insert(error);
                }
            }
        }
        Err(original_error.unwrap_or_else(|| {
            CatalogError::Message(format!("Lyricsporn found no albums for artist {artist_id}"))
        }))
    }

    async fn do_search_catalog(
        &self,
        term: &str,
        limit: i64,
        storefront: &str,
    ) -> Result<Vec<TrackMeta>, CatalogError> {
        let limit = limit.clamp(1, 25);
        let api_url = self.configured_api_url()?;
        let url = format!(
            "{api_url}/catalog/search?term={}&storefront={}&types=songs&limit={limit}&artworkSize=1000",
            urlencode(term),
            urlencode(storefront)
        );
        let response = match self.get_json(&url, SEARCH_TIMEOUT, "catalog search").await {
            Ok(response) => response,
            Err(error) => {
                warn!(term, storefront, error = %error, "Lyricsporn catalog search failed");
                return Ok(Vec::new());
            }
        };
        Ok(response
            .pointer("/results/songs/items")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .map(|item| map_track(item, None, None))
            .filter(|track| !track.id.is_empty())
            .collect())
    }

    /// Search songs through Lyricsporn. Provider/network failures degrade to
    /// an empty result so the Telegram command can still show cached tracks.
    pub async fn search_catalog(
        &self,
        term: &str,
        limit: i64,
        storefront: &str,
    ) -> Result<Vec<TrackMeta>, CatalogError> {
        let storefront = normalize_storefront(storefront);
        let clean_term = term.trim().to_lowercase();
        let Ok(api_url) = self.configured_api_url() else {
            return Ok(Vec::new());
        };
        let key = format!("{api_url}|search:{storefront}:{limit}:{clean_term}");
        if let Some(CacheValue::Search(tracks)) = self.get_cached(&key) {
            return Ok(tracks);
        }
        let mut results = Vec::new();
        for region in storefront_fallbacks(&storefront) {
            results = self.do_search_catalog(term, limit, &region).await?;
            if !results.is_empty() {
                break;
            }
        }
        self.set_cached(&key, CacheValue::Search(results.clone()));
        Ok(results)
    }
}

fn collection_next_url(current_url: &str, next: &str, api_url: &str) -> String {
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

fn unique_ids(items: &[Value]) -> Vec<String> {
    let mut seen = std::collections::HashSet::new();
    items
        .iter()
        .filter_map(|item| string(item, "id"))
        .filter(|id| seen.insert((*id).to_owned()))
        .map(ToOwned::to_owned)
        .collect()
}

fn string<'a>(value: &'a Value, key: &str) -> Option<&'a str> {
    value
        .get(key)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
}

fn integer(value: &Value, key: &str) -> Option<i64> {
    let number = value.get(key)?;
    number
        .as_i64()
        .or_else(|| number.as_u64().and_then(|n| i64::try_from(n).ok()))
        .or_else(|| number.as_f64().map(|n| n.round() as i64))
}

fn artwork_url(value: &Value, size: u16) -> Option<String> {
    value
        .pointer("/artwork/url")
        .and_then(Value::as_str)
        .filter(|url| !url.is_empty())
        .map(|url| format_artwork_url(url, size))
}

fn id_from_url(url: Option<&str>) -> Option<String> {
    let id = url?.trim_end_matches('/').rsplit('/').next()?;
    id.chars()
        .all(|c| c.is_ascii_digit())
        .then(|| id.to_owned())
}

fn album_artist_id(album: &Value) -> Option<String> {
    album
        .pointer("/collections/artists/items/0/id")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .or_else(|| id_from_url(string(album, "artistUrl")))
}

fn map_track(item: &Value, context: Option<&TrackContext>, position: Option<i64>) -> TrackMeta {
    let id = string(item, "id").unwrap_or_default().to_owned();
    let artist = string(item, "artist")
        .or_else(|| string(item, "artistName"))
        .unwrap_or("Unknown Artist")
        .to_owned();
    let album = string(item, "album")
        .or_else(|| string(item, "albumName"))
        .or_else(|| context.and_then(|context| context.album_name.as_deref()))
        .unwrap_or_default()
        .to_owned();
    let album_artist = string(item, "albumArtist")
        .or_else(|| string(item, "albumArtistName"))
        .or_else(|| context.and_then(|context| context.album_artist.as_deref()))
        .unwrap_or(&artist)
        .to_owned();
    let artwork = artwork_url(item, 1000).or_else(|| {
        context
            .and_then(|context| context.artwork_url.as_deref())
            .map(ToOwned::to_owned)
    });
    let content_advisory = string(item, "contentRating").map(ToOwned::to_owned);
    let artist_id = item
        .pointer("/artists/0/id")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .or_else(|| string(item, "artistId").map(ToOwned::to_owned))
        .or_else(|| context.and_then(|context| context.artist_id.clone()));
    let album_id = item
        .pointer("/albumResource/id")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .or_else(|| string(item, "albumId").map(ToOwned::to_owned))
        .or_else(|| context.and_then(|context| context.album_id.clone()));
    let genre = item
        .get("genres")
        .and_then(Value::as_array)
        .and_then(|genres| genres.first())
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    let duration_secs = integer(item, "durationMs")
        .map(|duration| (duration as f64 / 1000.0).round() as i64)
        .unwrap_or(0);
    let release_date = string(item, "releaseDate")
        .unwrap_or_default()
        .chars()
        .take(10)
        .collect();

    TrackMeta {
        id,
        title: string(item, "title")
            .or_else(|| string(item, "name"))
            .unwrap_or_default()
            .to_owned(),
        artist,
        album,
        album_artist,
        genre,
        release_date,
        composer: string(item, "composer").map(ToOwned::to_owned),
        track_number: integer(item, "trackNumber").or(position),
        track_count: integer(item, "trackCount")
            .or_else(|| context.and_then(|context| context.track_count)),
        disc_number: integer(item, "discNumber"),
        disc_count: integer(item, "discCount"),
        duration_secs,
        explicit: content_advisory
            .as_deref()
            .is_some_and(|rating| rating.eq_ignore_ascii_case("explicit")),
        content_advisory,
        artwork_url: artwork.unwrap_or_default(),
        album_id,
        artist_id,
        isrc: string(item, "isrc").map(ToOwned::to_owned),
        record_label: string(item, "recordLabel").map(ToOwned::to_owned),
        copyright: string(item, "copyright").map(ToOwned::to_owned),
        upc: string(item, "upc").map(ToOwned::to_owned),
        is_streamable: item.get("isStreamable").and_then(Value::as_bool),
    }
}

fn map_album(
    album: &Value,
    album_id: &str,
    tracks: &[TrackMeta],
    track_count: Option<i64>,
) -> TrackMeta {
    let name = string(album, "name").unwrap_or_default().to_owned();
    let artist = string(album, "artistName")
        .unwrap_or_else(|| {
            tracks
                .first()
                .map_or("Unknown Artist", |track| &track.artist)
        })
        .to_owned();
    let rating = string(album, "contentRating").map(ToOwned::to_owned);
    TrackMeta {
        id: album_id.to_owned(),
        title: name.clone(),
        artist: artist.clone(),
        album: name,
        album_artist: artist,
        genre: album
            .get("genres")
            .and_then(Value::as_array)
            .and_then(|genres| genres.first())
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        release_date: string(album, "releaseDate")
            .unwrap_or_default()
            .chars()
            .take(10)
            .collect(),
        composer: None,
        track_number: None,
        track_count,
        disc_number: None,
        disc_count: tracks.iter().filter_map(|track| track.disc_number).max(),
        duration_secs: 0,
        explicit: rating
            .as_deref()
            .is_some_and(|value| value.eq_ignore_ascii_case("explicit")),
        content_advisory: rating,
        artwork_url: artwork_url(album, 1000).unwrap_or_default(),
        album_id: Some(album_id.to_owned()),
        artist_id: album_artist_id(album),
        isrc: None,
        record_label: string(album, "recordLabel").map(ToOwned::to_owned),
        copyright: string(album, "copyright").map(ToOwned::to_owned),
        upc: string(album, "upc").map(ToOwned::to_owned),
        is_streamable: None,
    }
}

/// Production catalog handle shared across handlers/worker tasks.
pub type SharedCatalog = Arc<Catalog<ReqwestTransport>>;
