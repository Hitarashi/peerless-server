//! iTunes catalog client.
//!
//! Behavior notes:
//! - Errors are plain user-facing messages; the message IS the interface —
//!   it surfaces to users through the rip pipeline.
//! - The track HTTP-failure message intentionally omits the context word
//!   (`iTunes lookup failed (HTTP N)`) while its timeout message and the
//!   album/artist messages include it.
//! - Transport throws (timeout and network alike) produce the
//!   `iTunes <ctx> lookup timed out after Xms: <msg>` message.
//! - Storefront fallback: primary → `us` (if different) → every regional
//!   except the original. First success cached under the ORIGINAL key.
//!   All fail → the ORIGINAL primary error is rethrown.
//! - User-agent differs per endpoint family: Chrome UA everywhere except
//!   charts (`Mozilla/5.0`).
//! - Spans use `.instrument()` (not entered guards) so futures stay `Send`
//!   for later tokio spawn in the bot crate.

mod cache;
mod transport;

use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use cache::Cache;
use music::{AlbumTracks, ArtistTracks, TrackMeta};
use serde::Deserialize;
use tracing::{debug, error, info, info_span};
pub use transport::{
    CHARTS_USER_AGENT, ITUNES_USER_AGENT, ReqwestTransport, Transport, TransportError,
};

use crate::{playlist::ReqwestPlaylistHttp, token::DeveloperTokenProvider};

/// Regional storefronts tried after `us` in the fallback chain, in order.
pub const REGIONAL_STOREFRONTS: [&str; 7] = ["jp", "gb", "in", "ca", "de", "fr", "au"];

/// One album returned by an iTunes album search.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlbumSearchResult {
    pub id: String,
    pub title: String,
    pub artist: String,
    pub url: String,
    pub release_date: Option<String>,
    pub genre: Option<String>,
    pub track_count: Option<usize>,
    pub storefront: String,
}

/// An album entry returned by Apple's RSS charts feed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChartAlbum {
    pub id: String,
    pub title: String,
    pub artist: String,
    pub url: String,
    pub artwork_url: Option<String>,
    pub release_date: Option<String>,
    pub genre: Option<String>,
}

const TRACK_TIMEOUT: Duration = Duration::from_secs(15);
const ALBUM_TIMEOUT: Duration = Duration::from_secs(25);
const ARTIST_TIMEOUT: Duration = Duration::from_secs(30);
const ARTIST_BATCH_TIMEOUT: Duration = Duration::from_secs(20);
const SEARCH_TIMEOUT: Duration = Duration::from_secs(15);
const CHARTS_TIMEOUT: Duration = Duration::from_secs(15);

/// Every catalog failure. `Message` carries exact user-facing error text.
#[derive(Debug, thiserror::Error)]
pub enum CatalogError {
    /// A plain error string, surfaced verbatim to users.
    #[error("{0}")]
    Message(String),
    /// Raw transport failure (charts endpoint propagates these).
    #[error("transport error: {0}")]
    Transport(#[from] TransportError),
    /// JSON body failed to parse.
    #[error("bad JSON: {0}")]
    Json(#[from] serde_json::Error),
}

/// iTunes lookup API raw result item. Every field optional; camelCase names.
#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
struct ItunesRawItem {
    wrapper_type: Option<String>,
    kind: Option<String>,
    track_id: Option<i64>,
    collection_id: Option<i64>,
    artist_id: Option<i64>,
    collection_artist_id: Option<i64>,
    track_name: Option<String>,
    collection_name: Option<String>,
    collection_view_url: Option<String>,
    artist_name: Option<String>,
    collection_artist_name: Option<String>,
    composer_name: Option<String>,
    primary_genre_name: Option<String>,
    release_date: Option<String>,
    track_number: Option<i64>,
    track_count: Option<i64>,
    disc_number: Option<i64>,
    disc_count: Option<i64>,
    track_time_millis: Option<i64>,
    track_explicitness: Option<String>,
    #[serde(rename = "contentAdvisoryRating")]
    content_advisory_rating: Option<String>,
    collection_explicitness: Option<String>,
    artwork_url_100: Option<String>,
    isrc: Option<String>,
    record_label: Option<String>,
    copyright: Option<String>,
    upc: Option<String>,
    is_streamable: Option<bool>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(default)]
struct ItunesResponse {
    results: Vec<ItunesRawItem>,
}

/// Apple RSS charts feed item.
#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
struct ChartsRawAlbum {
    id: String,
    name: String,
    artist_name: String,
    url: String,
    artwork_url_100: Option<String>,
    release_date: Option<String>,
    genres: Vec<ChartsGenre>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
struct ChartsGenre {
    name: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(default)]
struct ChartsFeed {
    results: Option<Vec<ChartsRawAlbum>>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(default)]
struct ChartsResponse {
    feed: Option<ChartsFeed>,
}

/// Replace the first `\d+x\d+bb` artwork size with 3000x3000bb.
fn format_artwork_url(url: Option<&str>) -> String {
    let Some(url) = url else {
        return String::new();
    };
    static RE: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    let re = RE.get_or_init(|| regex::Regex::new(r"\d+x\d+bb").expect("artwork size regex"));
    re.replace(url, "3000x3000bb").into_owned()
}

/// Re-point an artwork URL at a different square size (for example 320 for
/// Telegram document thumbnails).
pub fn artwork_url_at_size(url: &str, size: u16) -> String {
    static RE: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    let re = RE.get_or_init(|| regex::Regex::new(r"\d+x\d+bb").expect("artwork size regex"));
    let replacement = format!("{size}x{size}bb");
    re.replace(url, replacement.as_str()).into_owned()
}

/// Format an AMP artwork URL template by replacing `{w}x{h}bb.{f}` with `1000x1000bb.jpg`.
pub fn format_amp_artwork_url(url: Option<&str>) -> String {
    let Some(url) = url else {
        return String::new();
    };
    url.replace("{w}x{h}bb.{f}", "1000x1000bb.jpg")
        .replace("{w}", "1000")
        .replace("{h}", "1000")
        .replace("{f}", "jpg")
}

/// Apple Music AMP API artwork object.
#[derive(Debug, Clone, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "camelCase", default)]
pub struct AmpArtwork {
    pub url: Option<String>,
    pub width: Option<u32>,
    pub height: Option<u32>,
}

/// Song attributes returned by Apple Music AMP API.
#[derive(Debug, Clone, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "camelCase", default)]
pub struct AmpSongAttributes {
    pub name: Option<String>,
    pub artist_name: Option<String>,
    pub album_name: Option<String>,
    pub album_artist_name: Option<String>,
    pub composer_name: Option<String>,
    pub genre_names: Vec<String>,
    pub release_date: Option<String>,
    pub track_number: Option<i64>,
    pub disc_number: Option<i64>,
    pub duration_in_millis: Option<u64>,
    pub isrc: Option<String>,
    pub audio_traits: Vec<String>,
    pub artwork: Option<AmpArtwork>,
    pub record_label: Option<String>,
    pub copyright: Option<String>,
    pub is_streamable: Option<bool>,
    pub content_rating: Option<String>,
    pub upc: Option<String>,
}

/// A resource reference in an AMP item relationship.
#[derive(Debug, Clone, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "camelCase", default)]
pub struct AmpResourceIdentifier {
    pub id: String,
    #[serde(rename = "type")]
    pub item_type: Option<String>,
}

/// Relationships on an AMP song item.
#[derive(Debug, Clone, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "camelCase", default)]
pub struct AmpSongRelationships {
    pub albums: Option<AmpAlbumRelationshipData>,
    pub artists: Option<AmpSongRelationshipData>,
}

#[derive(Debug, Clone, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "camelCase", default)]
pub struct AmpAlbumRelationshipData {
    pub data: Vec<AmpAlbumItem>,
}

#[derive(Debug, Clone, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "camelCase", default)]
pub struct AmpSongRelationshipData {
    pub data: Vec<AmpResourceIdentifier>,
}

/// Single song item returned by Apple Music AMP API.
#[derive(Debug, Clone, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "camelCase", default)]
pub struct AmpSongItem {
    pub id: String,
    #[serde(rename = "type")]
    pub item_type: Option<String>,
    pub attributes: Option<AmpSongAttributes>,
    pub relationships: Option<AmpSongRelationships>,
}

/// Response payload from `/v1/catalog/{storefront}/songs/{id}`.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct AmpSongResponse {
    pub data: Vec<AmpSongItem>,
}

/// Album attributes returned by Apple Music AMP API.
#[derive(Debug, Clone, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "camelCase", default)]
pub struct AmpAlbumAttributes {
    pub name: Option<String>,
    pub artist_name: Option<String>,
    pub artwork: Option<AmpArtwork>,
    pub genre_names: Vec<String>,
    pub release_date: Option<String>,
    pub track_count: Option<i64>,
    pub record_label: Option<String>,
    pub copyright: Option<String>,
    pub upc: Option<String>,
    pub content_rating: Option<String>,
}

/// Relationships on an AMP album item, containing child tracks.
#[derive(Debug, Clone, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "camelCase", default)]
pub struct AmpAlbumRelationships {
    pub tracks: Option<AmpAlbumTracksRelationship>,
}

#[derive(Debug, Clone, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "camelCase", default)]
pub struct AmpAlbumTracksRelationship {
    pub data: Vec<AmpSongItem>,
}

/// Single album item returned by Apple Music AMP API.
#[derive(Debug, Clone, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "camelCase", default)]
pub struct AmpAlbumItem {
    pub id: String,
    #[serde(rename = "type")]
    pub item_type: Option<String>,
    pub attributes: Option<AmpAlbumAttributes>,
    pub relationships: Option<AmpAlbumRelationships>,
}

/// Response payload from `/v1/catalog/{storefront}/albums/{id}`.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct AmpAlbumResponse {
    pub data: Vec<AmpAlbumItem>,
}

fn map_amp_song_item(
    item: &AmpSongItem,
    fallback_album_id: Option<&str>,
    fallback_album_title: Option<&str>,
    fallback_album_artist: Option<&str>,
    fallback_artwork_url: Option<&str>,
    fallback_track_count: Option<i64>,
    fallback_disc_count: Option<i64>,
) -> TrackMeta {
    let id = item.id.clone();
    let attrs = item.attributes.as_ref();
    let album_item = item
        .relationships
        .as_ref()
        .and_then(|r| r.albums.as_ref())
        .and_then(|a| a.data.first());
    let album_attrs = album_item.and_then(|a| a.attributes.as_ref());

    let title = attrs.and_then(|a| a.name.clone()).unwrap_or_default();
    let artist = attrs
        .and_then(|a| a.artist_name.clone())
        .unwrap_or_default();
    let album = attrs
        .and_then(|a| a.album_name.clone())
        .or_else(|| album_attrs.and_then(|a| a.name.clone()))
        .or_else(|| fallback_album_title.map(|s| s.to_string()))
        .unwrap_or_default();
    let album_artist = attrs
        .and_then(|a| a.album_artist_name.clone())
        .or_else(|| album_attrs.and_then(|a| a.artist_name.clone()))
        .or_else(|| attrs.and_then(|a| a.artist_name.clone()))
        .or_else(|| fallback_album_artist.map(|s| s.to_string()))
        .unwrap_or_default();
    let genre = attrs.and_then(|a| a.genre_names.first().cloned());
    let release_date = attrs
        .and_then(|a| a.release_date.as_deref())
        .unwrap_or("")
        .chars()
        .take(10)
        .collect();
    let composer = attrs.and_then(|a| a.composer_name.clone());
    let track_number = attrs.and_then(|a| a.track_number);
    let track_count = fallback_track_count.or_else(|| album_attrs.and_then(|a| a.track_count));
    let disc_number = attrs.and_then(|a| a.disc_number);
    let disc_count = fallback_disc_count;
    let duration_secs = attrs
        .and_then(|a| a.duration_in_millis)
        .map(|ms| ((ms as f64) / 1000.0).round() as i64)
        .unwrap_or(0);
    let explicit = attrs
        .and_then(|a| a.content_rating.as_deref())
        .map(|r| r == "explicit")
        .unwrap_or(false);
    let content_advisory = attrs.and_then(|a| a.content_rating.clone());
    let mut artwork_url = format_amp_artwork_url(
        attrs
            .and_then(|a| a.artwork.as_ref())
            .and_then(|a| a.url.as_deref()),
    );
    if artwork_url.is_empty()
        && let Some(fallback_url) = fallback_artwork_url
    {
        artwork_url = fallback_url.to_string();
    }
    let album_id = album_item
        .map(|d| d.id.clone())
        .or_else(|| fallback_album_id.map(|s| s.to_string()));
    let artist_id = item
        .relationships
        .as_ref()
        .and_then(|r| r.artists.as_ref())
        .and_then(|a| a.data.first())
        .map(|d| d.id.clone());
    let isrc = attrs.and_then(|a| a.isrc.clone()).filter(|s| !s.is_empty());
    let record_label = attrs
        .and_then(|a| a.record_label.clone())
        .filter(|s| !s.is_empty())
        .or_else(|| {
            album_attrs
                .and_then(|a| a.record_label.clone())
                .filter(|s| !s.is_empty())
        });
    let copyright = attrs
        .and_then(|a| a.copyright.clone())
        .filter(|s| !s.is_empty())
        .or_else(|| {
            album_attrs
                .and_then(|a| a.copyright.clone())
                .filter(|s| !s.is_empty())
        });
    let upc = attrs
        .and_then(|a| a.upc.clone())
        .filter(|s| !s.is_empty())
        .or_else(|| {
            album_attrs
                .and_then(|a| a.upc.clone())
                .filter(|s| !s.is_empty())
        });
    let is_streamable = attrs.and_then(|a| a.is_streamable);

    TrackMeta {
        id,
        title,
        artist,
        album,
        album_artist,
        genre,
        release_date,
        composer,
        track_number,
        track_count,
        disc_number,
        disc_count,
        duration_secs,
        explicit,
        content_advisory,
        artwork_url,
        album_id,
        artist_id,
        isrc,
        record_label,
        copyright,
        upc,
        is_streamable,
    }
}

fn map_amp_album(
    album_item: &AmpAlbumItem,
    collection_id: &str,
    tracks: &[TrackMeta],
) -> TrackMeta {
    let attrs = album_item.attributes.as_ref();
    let id = if !album_item.id.is_empty() {
        album_item.id.clone()
    } else {
        collection_id.to_string()
    };
    let title = attrs.and_then(|a| a.name.clone()).unwrap_or_default();
    let artist = attrs
        .and_then(|a| a.artist_name.clone())
        .unwrap_or_default();
    let album = title.clone();
    let album_artist = artist.clone();
    let genre = attrs.and_then(|a| a.genre_names.first().cloned());
    let release_date = attrs
        .and_then(|a| a.release_date.as_deref())
        .unwrap_or("")
        .chars()
        .take(10)
        .collect();
    let track_count = attrs
        .and_then(|a| a.track_count)
        .or_else(|| (!tracks.is_empty()).then_some(tracks.len() as i64));
    let explicit = attrs
        .and_then(|a| a.content_rating.as_deref())
        .map(|r| r == "explicit")
        .unwrap_or(false);
    let content_advisory = attrs.and_then(|a| a.content_rating.clone());
    let artwork_url = format_amp_artwork_url(
        attrs
            .and_then(|a| a.artwork.as_ref())
            .and_then(|a| a.url.as_deref()),
    );
    let record_label = attrs
        .and_then(|a| a.record_label.clone())
        .filter(|s| !s.is_empty());
    let copyright = attrs
        .and_then(|a| a.copyright.clone())
        .filter(|s| !s.is_empty());
    let upc = attrs.and_then(|a| a.upc.clone()).filter(|s| !s.is_empty());

    let max_disc = tracks.iter().filter_map(|t| t.disc_number).max();

    TrackMeta {
        id: id.clone(),
        title,
        artist,
        album,
        album_artist,
        genre,
        release_date,
        composer: None,
        track_number: None,
        track_count,
        disc_number: None,
        disc_count: max_disc,
        duration_secs: 0,
        explicit,
        content_advisory,
        artwork_url,
        album_id: Some(id),
        artist_id: None,
        isrc: None,
        record_label,
        copyright,
        upc,
        is_streamable: None,
    }
}

/// Map a raw iTunes item to [`TrackMeta`], retaining both the legacy core
/// fields and any richer provider metadata present in the response.
/// An id of `0` maps to an empty string (JS falsy coercion).
fn map_itunes_item(item: &ItunesRawItem) -> TrackMeta {
    TrackMeta {
        id: item
            .track_id
            .filter(|id| *id != 0)
            .map_or_else(String::new, |id| id.to_string()),
        title: item.track_name.clone().unwrap_or_default(),
        artist: item.artist_name.clone().unwrap_or_default(),
        album: item.collection_name.clone().unwrap_or_default(),
        album_artist: item
            .collection_artist_name
            .clone()
            .or_else(|| item.artist_name.clone())
            .unwrap_or_default(),
        genre: item.primary_genre_name.clone(),
        release_date: item
            .release_date
            .as_deref()
            .unwrap_or("")
            .chars()
            .take(10)
            .collect(),
        composer: item.composer_name.clone(),
        track_number: item.track_number,
        track_count: item.track_count,
        disc_number: item.disc_number,
        disc_count: item.disc_count,
        duration_secs: (item.track_time_millis.unwrap_or(0) as f64 / 1000.0).round() as i64,
        explicit: item
            .track_explicitness
            .as_deref()
            .or(item.content_advisory_rating.as_deref())
            .or(item.collection_explicitness.as_deref())
            == Some("explicit"),
        content_advisory: item
            .track_explicitness
            .clone()
            .or_else(|| item.content_advisory_rating.clone())
            .or_else(|| item.collection_explicitness.clone()),
        artwork_url: format_artwork_url(item.artwork_url_100.as_deref()),
        album_id: item
            .collection_id
            .filter(|id| *id != 0)
            .map(|id| id.to_string()),
        artist_id: item
            .artist_id
            .or(item.collection_artist_id)
            .filter(|id| *id != 0)
            .map(|id| id.to_string()),
        isrc: item.isrc.clone().filter(|value| !value.is_empty()),
        record_label: item.record_label.clone().filter(|value| !value.is_empty()),
        copyright: item.copyright.clone().filter(|value| !value.is_empty()),
        upc: item.upc.clone().filter(|value| !value.is_empty()),
        is_streamable: item.is_streamable,
    }
}

fn is_track_item(item: &ItunesRawItem) -> bool {
    match item.kind.as_deref() {
        Some("song") => true,
        Some("music-video")
        | Some("feature-movie")
        | Some("tv-episode")
        | Some("podcast-episode") => false,
        _ => item.wrapper_type.as_deref() == Some("track"),
    }
}

fn normalize_storefront(storefront: &str) -> String {
    let sf = storefront.to_lowercase();
    if sf.is_empty() { "us".to_owned() } else { sf }
}

/// URL-encode a query component (percent-encoding, unreserved chars kept) —
/// equivalent to JS `encodeURIComponent` for our inputs.
fn urlencode(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(byte as char)
            }
            _ => encoded.push_str(&format!("%{byte:02X}")),
        }
    }
    encoded
}

/// Storefront fallback chain shared by track/album/artist lookups, inlined
/// around a concrete fetch method call to avoid closure-HRTB gymnastics:
/// cache-check → fetch on requested sf → (if sf != us) fetch on us →
/// fetch on each regional sf except the original. First success is cached
/// under the ORIGINAL key; all-fail rethrows the ORIGINAL error.
macro_rules! with_fallback {
    ($self:ident, $key:expr, $sf:expr, $ctx:literal, $method:ident($($arg:expr),* $(,)?)) => {{
        if let Some(value) = $self.get_cached(&$key).and_then(|cached| R::from_value(&cached)) {
            return Ok(value);
        }
        let sf = normalize_storefront($sf);
        match $self.$method($($arg,)* &sf).await {
            Ok(value) => {
                $self.set_cached(&$key, value.clone().into_value());
                Ok(value)
            }
            Err(original) => {
                if sf != "us" {
                    debug!(fallback_storefront = "us", context = $ctx, "retrying lookup on US storefront");
                    if let Ok(value) = $self.$method($($arg,)* "us").await {
                        $self.set_cached(&$key, value.clone().into_value());
                        return Ok(value);
                    }
                }
                for fallback in REGIONAL_STOREFRONTS {
                    if fallback == sf {
                        continue;
                    }
                    debug!(fallback_storefront = fallback, context = $ctx, "retrying lookup on regional storefront");
                    if let Ok(value) = $self.$method($($arg,)* fallback).await {
                        $self.set_cached(&$key, value.clone().into_value());
                        return Ok(value);
                    }
                }
                Err(original)
            }
        }
    }};
}

/// Cached payloads — each cache key stores one of these shapes.
#[derive(Clone)]
enum CacheValue {
    Track(TrackMeta),
    Album(AlbumTracks),
    Artist(ArtistTracks),
    ArtistAlbumIds(Vec<String>),
    Search(Vec<TrackMeta>),
    Charts(Vec<ChartAlbum>),
}

/// Bridge between typed lookup results and the untyped cache slot.
trait CachedValue: Clone {
    fn from_value(cached: &CacheValue) -> Option<Self>;
    fn into_value(self) -> CacheValue;
}

impl CachedValue for TrackMeta {
    fn from_value(cached: &CacheValue) -> Option<Self> {
        match cached {
            CacheValue::Track(v) => Some(v.clone()),
            _ => None,
        }
    }
    fn into_value(self) -> CacheValue {
        CacheValue::Track(self)
    }
}

impl CachedValue for AlbumTracks {
    fn from_value(cached: &CacheValue) -> Option<Self> {
        match cached {
            CacheValue::Album(v) => Some(v.clone()),
            _ => None,
        }
    }
    fn into_value(self) -> CacheValue {
        CacheValue::Album(self)
    }
}

impl CachedValue for ArtistTracks {
    fn from_value(cached: &CacheValue) -> Option<Self> {
        match cached {
            CacheValue::Artist(v) => Some(v.clone()),
            _ => None,
        }
    }
    fn into_value(self) -> CacheValue {
        CacheValue::Artist(self)
    }
}

impl CachedValue for Vec<String> {
    fn from_value(cached: &CacheValue) -> Option<Self> {
        match cached {
            CacheValue::ArtistAlbumIds(v) => Some(v.clone()),
            _ => None,
        }
    }
    fn into_value(self) -> CacheValue {
        CacheValue::ArtistAlbumIds(self)
    }
}

/// Catalog client over any transport, with TTL cache and storefront fallback.
pub struct Catalog<T: Transport> {
    transport: T,
    cache: Mutex<Cache<CacheValue>>,
    max_cache: usize,
    token_provider: Option<Arc<DeveloperTokenProvider<ReqwestPlaylistHttp>>>,
}

impl<T: Transport> Catalog<T> {
    pub fn new(transport: T) -> Self {
        Self::with_limits(transport, 500, Duration::from_secs(10 * 60))
    }

    pub fn with_limits(transport: T, max_cache: usize, ttl: Duration) -> Self {
        Self {
            transport,
            cache: Mutex::new(Cache::new(max_cache, ttl)),
            max_cache,
            token_provider: None,
        }
    }

    pub fn with_token_provider(
        mut self,
        token_provider: Arc<DeveloperTokenProvider<ReqwestPlaylistHttp>>,
    ) -> Self {
        self.token_provider = Some(token_provider);
        self
    }

    pub fn token_provider(&self) -> Option<&Arc<DeveloperTokenProvider<ReqwestPlaylistHttp>>> {
        self.token_provider.as_ref()
    }

    /// Cache capacity.
    pub fn capacity(&self) -> usize {
        self.max_cache
    }

    pub fn clear_cache(&self) {
        self.cache.lock().expect("cache mutex poisoned").clear();
    }

    /// Shared handle to the transport — lets tests observe served URLs.
    pub fn transport(&self) -> &T {
        &self.transport
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

    /// Parse a body's `results` array; serde errors propagate.
    fn parse_results(body: &str) -> Result<Vec<ItunesRawItem>, CatalogError> {
        let data: ItunesResponse = serde_json::from_str(body)?;
        Ok(data.results)
    }

    /// GET an iTunes URL and map failures to the exact user-facing messages.
    /// `timeout_ctx` rides in the timeout message; `http_prefix` is the
    /// verbatim HTTP-failure prefix (track's omits the context word).
    async fn fetch_itunes_body(
        &self,
        url: &str,
        timeout: Duration,
        timeout_ctx: &'static str,
        http_prefix: &'static str,
    ) -> Result<String, CatalogError> {
        match self.transport.get(url, ITUNES_USER_AGENT, timeout).await {
            Ok(body) => Ok(body),
            Err(TransportError::Fetch { elapsed_ms, source }) => {
                error!(elapsed_ms, error = %source, "iTunes lookup timed out / network error");
                Err(CatalogError::Message(format!(
                    "iTunes {timeout_ctx} lookup timed out after {elapsed_ms}ms: {source}"
                )))
            }
            Err(TransportError::Status { status }) => {
                error!(status, "iTunes lookup HTTP failure");
                Err(CatalogError::Message(format!(
                    "{http_prefix} failed (HTTP {status})"
                )))
            }
        }
    }

    async fn do_fetch_track_meta(
        &self,
        track_id: &str,
        sf: &str,
    ) -> Result<TrackMeta, CatalogError> {
        info_span!("itunes_track", track_id, storefront = sf)
            .in_scope(|| debug!("Querying iTunes API for track..."));
        let url = format!(
            "https://itunes.apple.com/lookup?id={}&country={}",
            urlencode(track_id),
            urlencode(sf)
        );
        let start = Instant::now();

        let body = self
            .fetch_itunes_body(&url, TRACK_TIMEOUT, "track", "iTunes lookup")
            .await?;
        let results = Self::parse_results(&body)?;

        let track_item = results
            .iter()
            .find(|r| is_track_item(r) || r.track_id.is_some_and(|id| id.to_string() == track_id));
        let Some(track_item) = track_item else {
            error!(track_id, "Track not found in iTunes response");
            return Err(CatalogError::Message(format!(
                "iTunes found no song matching track ID {track_id}"
            )));
        };

        if matches!(
            track_item.kind.as_deref(),
            Some("music-video")
                | Some("feature-movie")
                | Some("tv-episode")
                | Some("podcast-episode")
        ) {
            let kind = match track_item.kind.as_deref() {
                Some("music-video") => "music video",
                Some("feature-movie") => "feature movie",
                Some("tv-episode") => "TV episode",
                Some("podcast-episode") => "podcast episode",
                other => other.unwrap_or("video"),
            };
            return Err(CatalogError::Message(format!(
                "iTunes item {track_id} is a {kind}; only audio tracks are supported"
            )));
        }

        let mut meta = map_itunes_item(track_item);
        meta.id = track_item
            .track_id
            .filter(|id| *id != 0) // id 0 is treated as absent (JS falsy)
            .map_or_else(|| track_id.to_owned(), |id| id.to_string());

        info!(
            track_id,
            artist = %meta.artist,
            title = %meta.title,
            duration = meta.duration_secs,
            elapsed_ms = start.elapsed().as_millis() as u64,
            "iTunes metadata resolved"
        );
        Ok(meta)
    }

    pub fn parse_amp_song_body(body: &str, track_id: &str) -> Result<TrackMeta, CatalogError> {
        let response: AmpSongResponse = serde_json::from_str(body)?;
        let item = response.data.into_iter().next().ok_or_else(|| {
            CatalogError::Message(format!("AMP found no song matching track ID {track_id}"))
        })?;
        let meta = map_amp_song_item(&item, None, None, None, None, None, None);
        Ok(meta)
    }

    pub fn parse_amp_album_body(
        body: &str,
        collection_id: &str,
    ) -> Result<AlbumTracks, CatalogError> {
        let response: AmpAlbumResponse = serde_json::from_str(body)?;
        let album_item = response.data.into_iter().next().ok_or_else(|| {
            CatalogError::Message(format!(
                "AMP found no album matching collection ID {collection_id}"
            ))
        })?;
        let raw_tracks = album_item
            .relationships
            .as_ref()
            .and_then(|r| r.tracks.as_ref())
            .map(|t| t.data.as_slice())
            .unwrap_or_default();

        if raw_tracks.is_empty() {
            return Err(CatalogError::Message(format!(
                "AMP found no tracks for collection {collection_id}"
            )));
        }

        let album_attrs = album_item.attributes.as_ref();
        let album_title = album_attrs.and_then(|a| a.name.as_deref());
        let album_artist = album_attrs.and_then(|a| a.artist_name.as_deref());
        let album_artwork_url = format_amp_artwork_url(
            album_attrs
                .and_then(|a| a.artwork.as_ref())
                .and_then(|a| a.url.as_deref()),
        );
        let track_count = album_attrs
            .and_then(|a| a.track_count)
            .or_else(|| (!raw_tracks.is_empty()).then_some(raw_tracks.len() as i64));
        let max_disc = raw_tracks
            .iter()
            .filter_map(|t| t.attributes.as_ref().and_then(|a| a.disc_number))
            .max();

        let tracks: Vec<TrackMeta> = raw_tracks
            .iter()
            .map(|t| {
                map_amp_song_item(
                    t,
                    Some(collection_id),
                    album_title,
                    album_artist,
                    if album_artwork_url.is_empty() {
                        None
                    } else {
                        Some(&album_artwork_url)
                    },
                    track_count,
                    max_disc,
                )
            })
            .collect();

        let album_meta = if album_attrs.is_some() {
            map_amp_album(&album_item, collection_id, &tracks)
        } else {
            tracks.first().expect("checked non-empty").clone()
        };

        Ok(AlbumTracks {
            album: album_meta,
            tracks,
        })
    }

    async fn do_fetch_track_meta_amp(
        &self,
        provider: &DeveloperTokenProvider<ReqwestPlaylistHttp>,
        track_id: &str,
        sf: &str,
    ) -> Result<TrackMeta, CatalogError> {
        info_span!("amp_track", track_id, storefront = sf)
            .in_scope(|| debug!("Querying Apple Music AMP API for song..."));
        let url = format!(
            "https://amp-api.music.apple.com/v1/catalog/{}/songs/{}?include=albums",
            urlencode(sf),
            urlencode(track_id)
        );
        let start = Instant::now();
        let body = provider
            .fetch_amp(&url, TRACK_TIMEOUT)
            .await
            .map_err(|err| CatalogError::Message(format!("AMP track fetch failed: {err}")))?;

        let meta = Self::parse_amp_song_body(&body, track_id)?;
        info!(
            track_id,
            artist = %meta.artist,
            title = %meta.title,
            duration = meta.duration_secs,
            isrc = meta.isrc.as_deref().unwrap_or(""),
            elapsed_ms = start.elapsed().as_millis() as u64,
            "AMP track metadata resolved"
        );
        Ok(meta)
    }

    async fn do_fetch_album_tracks_amp(
        &self,
        provider: &DeveloperTokenProvider<ReqwestPlaylistHttp>,
        collection_id: &str,
        sf: &str,
    ) -> Result<AlbumTracks, CatalogError> {
        info_span!("amp_album", collection_id, storefront = sf)
            .in_scope(|| debug!("Querying Apple Music AMP API for album..."));
        let url = format!(
            "https://amp-api.music.apple.com/v1/catalog/{}/albums/{}",
            urlencode(sf),
            urlencode(collection_id)
        );
        let start = Instant::now();
        let body = provider
            .fetch_amp(&url, ALBUM_TIMEOUT)
            .await
            .map_err(|err| CatalogError::Message(format!("AMP album fetch failed: {err}")))?;

        let album_tracks = Self::parse_amp_album_body(&body, collection_id)?;
        info!(
            collection_id,
            album = %album_tracks.album.album,
            artist = %album_tracks.album.artist,
            track_count = album_tracks.tracks.len(),
            elapsed_ms = start.elapsed().as_millis() as u64,
            "AMP album collection resolved"
        );
        Ok(album_tracks)
    }

    /// Fetch a single track's metadata with storefront fallback.
    pub async fn fetch_track_meta(
        &self,
        track_id: &str,
        storefront: &str,
    ) -> Result<TrackMeta, CatalogError> {
        let sf = normalize_storefront(storefront);
        let cache_key = format!("track:{sf}:{track_id}");
        type R = TrackMeta;

        if let Some(value) = self
            .get_cached(&cache_key)
            .and_then(|cached| R::from_value(&cached))
        {
            return Ok(value);
        }

        if let Some(provider) = &self.token_provider {
            match self.do_fetch_track_meta_amp(provider, track_id, &sf).await {
                Ok(mut meta) => {
                    if meta.track_count.is_none()
                        && let Ok(itunes_meta) = self.do_fetch_track_meta(track_id, &sf).await
                    {
                        meta.track_count = itunes_meta.track_count;
                    }
                    self.set_cached(&cache_key, meta.clone().into_value());
                    return Ok(meta);
                }
                Err(err) => {
                    debug!(
                        track_id,
                        storefront = %sf,
                        error = %err,
                        "AMP track lookup failed, falling back to iTunes"
                    );
                }
            }
        }

        with_fallback!(
            self,
            cache_key,
            storefront,
            "track",
            do_fetch_track_meta(track_id)
        )
    }

    async fn do_fetch_album_tracks(
        &self,
        collection_id: &str,
        sf: &str,
    ) -> Result<AlbumTracks, CatalogError> {
        info_span!("itunes_album", collection_id, storefront = sf)
            .in_scope(|| debug!("Querying iTunes API for album collection..."));
        let url = format!(
            "https://itunes.apple.com/lookup?id={}&entity=song&country={}",
            urlencode(collection_id),
            urlencode(sf)
        );
        let start = Instant::now();

        let body = self
            .fetch_itunes_body(&url, ALBUM_TIMEOUT, "album", "iTunes album lookup")
            .await?;
        let results = Self::parse_results(&body)?;

        let collection_item = results
            .iter()
            .find(|r| r.wrapper_type.as_deref() == Some("collection"));
        let tracks: Vec<TrackMeta> = results
            .iter()
            .filter(|r| is_track_item(r))
            .map(map_itunes_item)
            .collect();

        if tracks.is_empty() {
            // An empty storefront response is a normal miss while the
            // fallback chain checks other regions. Logging each attempt at
            // error level produced one noisy line per storefront; the
            // orchestrator still reports the final unresolved album once.
            debug!(
                collection_id,
                storefront = sf,
                "No tracks found for collection"
            );
            return Err(CatalogError::Message(format!(
                "iTunes found no tracks for collection {collection_id}"
            )));
        }
        let first_track = tracks.first().expect("checked non-empty").clone();

        let album_meta = match collection_item {
            Some(c) => TrackMeta {
                id: c
                    .collection_id
                    .filter(|id| *id != 0) // id 0 is treated as absent (JS falsy)
                    .map_or_else(|| collection_id.to_owned(), |id| id.to_string()),
                title: c.collection_name.clone().unwrap_or_default(),
                artist: c.artist_name.clone().unwrap_or_default(),
                album: c.collection_name.clone().unwrap_or_default(),
                album_artist: c
                    .collection_artist_name
                    .clone()
                    .or_else(|| c.artist_name.clone())
                    .unwrap_or_default(),
                genre: c.primary_genre_name.clone(),
                release_date: c
                    .release_date
                    .as_deref()
                    .unwrap_or("")
                    .chars()
                    .take(10)
                    .collect(),
                composer: c.composer_name.clone(),
                track_number: None,
                track_count: None,
                disc_number: None,
                disc_count: None,
                duration_secs: 0,
                explicit: c.collection_explicitness.as_deref() == Some("explicit"),
                content_advisory: c.collection_explicitness.clone(),
                artwork_url: format_artwork_url(c.artwork_url_100.as_deref()),
                album_id: c
                    .collection_id
                    .filter(|id| *id != 0)
                    .map(|id| id.to_string()),
                artist_id: c
                    .artist_id
                    .or(c.collection_artist_id)
                    .filter(|id| *id != 0)
                    .map(|id| id.to_string()),
                isrc: c.isrc.clone().filter(|value| !value.is_empty()),
                record_label: c.record_label.clone().filter(|value| !value.is_empty()),
                copyright: c.copyright.clone().filter(|value| !value.is_empty()),
                upc: c.upc.clone().filter(|value| !value.is_empty()),
                is_streamable: None,
            },
            None => first_track,
        };

        info!(
            collection_id,
            album = %album_meta.album,
            artist = %album_meta.artist,
            track_count = tracks.len(),
            elapsed_ms = start.elapsed().as_millis() as u64,
            "iTunes album collection resolved"
        );
        Ok(AlbumTracks {
            album: album_meta,
            tracks,
        })
    }

    /// Fetch an album's tracks with storefront fallback.
    pub async fn fetch_album_tracks(
        &self,
        collection_id: &str,
        storefront: &str,
    ) -> Result<AlbumTracks, CatalogError> {
        let sf = normalize_storefront(storefront);
        let cache_key = format!("album:{sf}:{collection_id}");
        type R = AlbumTracks;

        if let Some(value) = self
            .get_cached(&cache_key)
            .and_then(|cached| R::from_value(&cached))
        {
            return Ok(value);
        }

        if let Some(provider) = &self.token_provider {
            match self
                .do_fetch_album_tracks_amp(provider, collection_id, &sf)
                .await
            {
                Ok(album_tracks) => {
                    self.set_cached(&cache_key, album_tracks.clone().into_value());
                    return Ok(album_tracks);
                }
                Err(err) => {
                    debug!(
                        collection_id,
                        storefront = %sf,
                        error = %err,
                        "AMP album lookup failed, falling back to iTunes"
                    );
                }
            }
        }

        with_fallback!(
            self,
            cache_key,
            storefront,
            "album",
            do_fetch_album_tracks(collection_id)
        )
    }

    async fn do_fetch_artist_tracks(
        &self,
        artist_id: &str,
        sf: &str,
    ) -> Result<ArtistTracks, CatalogError> {
        info_span!("itunes_artist", artist_id, storefront = sf)
            .in_scope(|| debug!("Querying iTunes API for artist discography..."));
        let discog_url = format!(
            "https://itunes.apple.com/lookup?id={}&entity=album&limit=200&country={}",
            urlencode(artist_id),
            urlencode(sf)
        );
        let start = Instant::now();

        let body = self
            .fetch_itunes_body(
                &discog_url,
                ARTIST_TIMEOUT,
                "artist",
                "iTunes artist lookup",
            )
            .await?;
        let results = Self::parse_results(&body)?;

        let artist_item = results
            .iter()
            .find(|r| r.wrapper_type.as_deref() == Some("artist"));
        let mut artist_name = artist_item
            .and_then(|r| r.artist_name.clone())
            .unwrap_or_default();

        let mut all_tracks: Vec<TrackMeta> = Vec::new();
        let mut seen_track_ids: std::collections::HashSet<String> =
            std::collections::HashSet::new();

        let collection_ids: Vec<i64> = results
            .iter()
            .filter(|r| r.wrapper_type.as_deref() == Some("collection"))
            .filter_map(|r| r.collection_id)
            .collect();

        if !collection_ids.is_empty() {
            for chunk in collection_ids.chunks(25) {
                let ids = chunk
                    .iter()
                    .map(|id| id.to_string())
                    .collect::<Vec<_>>()
                    .join(",");
                let url = format!(
                    "https://itunes.apple.com/lookup?id={ids}&entity=song&country={}",
                    urlencode(sf)
                );
                // Batch failures (transport, HTTP, JSON) are all swallowed
                // with a debug log and the loop continues.
                match self
                    .transport
                    .get(&url, ITUNES_USER_AGENT, ARTIST_BATCH_TIMEOUT)
                    .await
                {
                    Ok(body) => {
                        if let Ok(batch) = Self::parse_results(&body) {
                            for item in &batch {
                                if is_track_item(item) {
                                    let track_id =
                                        item.track_id.map_or_else(String::new, |id| id.to_string());
                                    if !track_id.is_empty() && seen_track_ids.insert(track_id) {
                                        all_tracks.push(map_itunes_item(item));
                                    }
                                }
                            }
                        }
                    }
                    Err(err) => {
                        debug!(error = %err, "Error fetching artist collection batch, continuing...");
                    }
                }
            }
        }

        if all_tracks.is_empty() {
            // Fallback: direct song lookup for the artist (all failures ignored).
            let songs_url = format!(
                "https://itunes.apple.com/lookup?id={}&entity=song&limit=200&country={}",
                urlencode(artist_id),
                urlencode(sf)
            );
            if let Ok(body) = self
                .transport
                .get(&songs_url, ITUNES_USER_AGENT, ARTIST_BATCH_TIMEOUT)
                .await
                && let Ok(song_results) = Self::parse_results(&body)
            {
                if artist_name.is_empty() {
                    artist_name = song_results
                        .iter()
                        .find(|r| r.wrapper_type.as_deref() == Some("artist"))
                        .and_then(|r| r.artist_name.clone())
                        .unwrap_or_default();
                }
                for item in &song_results {
                    if is_track_item(item) {
                        let track_id = item.track_id.map_or_else(String::new, |id| id.to_string());
                        if !track_id.is_empty() && seen_track_ids.insert(track_id) {
                            all_tracks.push(map_itunes_item(item));
                        }
                    }
                }
            }
        }

        if all_tracks.is_empty() {
            error!(artist_id, "No tracks found for artist");
            return Err(CatalogError::Message(format!(
                "iTunes found no tracks for artist {artist_id}"
            )));
        }

        if artist_name.is_empty() {
            artist_name = all_tracks[0].artist.clone();
        }

        info!(
            artist_id,
            artist = %artist_name,
            track_count = all_tracks.len(),
            elapsed_ms = start.elapsed().as_millis() as u64,
            "iTunes artist discography resolved"
        );
        Ok(ArtistTracks {
            artist_id: artist_id.to_owned(),
            artist_name: if artist_name.is_empty() {
                "Unknown Artist".to_owned()
            } else {
                artist_name
            },
            tracks: all_tracks,
        })
    }

    /// Fetch an artist's tracks with storefront fallback.
    pub async fn fetch_artist_tracks(
        &self,
        artist_id: &str,
        storefront: &str,
    ) -> Result<ArtistTracks, CatalogError> {
        let sf = normalize_storefront(storefront);
        let cache_key = format!("artist:{sf}:{artist_id}");
        type R = ArtistTracks;
        with_fallback!(
            self,
            cache_key,
            storefront,
            "artist",
            do_fetch_artist_tracks(artist_id)
        )
    }

    async fn do_fetch_artist_album_ids(
        &self,
        artist_id: &str,
        sf: &str,
    ) -> Result<Vec<String>, CatalogError> {
        info_span!("itunes_artist_albums", artist_id, storefront = sf)
            .in_scope(|| debug!("Querying iTunes API for artist album ids..."));
        let discog_url = format!(
            "https://itunes.apple.com/lookup?id={}&entity=album&limit=200&country={}",
            urlencode(artist_id),
            urlencode(sf)
        );
        let body = self
            .fetch_itunes_body(
                &discog_url,
                ARTIST_TIMEOUT,
                "artist",
                "iTunes artist album lookup",
            )
            .await?;
        let results = Self::parse_results(&body)?;
        let collection_ids: Vec<String> = results
            .into_iter()
            .filter(|r| r.wrapper_type.as_deref() == Some("collection"))
            .filter_map(|r| r.collection_id)
            .map(|id| id.to_string())
            .collect();
        Ok(collection_ids)
    }

    /// Fetch an artist's album IDs with storefront fallback.
    pub async fn fetch_artist_album_ids(
        &self,
        artist_id: &str,
        storefront: &str,
    ) -> Result<Vec<String>, CatalogError> {
        let sf = normalize_storefront(storefront);
        let cache_key = format!("artist_albums:{sf}:{artist_id}");
        type R = Vec<String>;
        with_fallback!(
            self,
            cache_key,
            storefront,
            "artist",
            do_fetch_artist_album_ids(artist_id)
        )
    }

    /// One search attempt. Transport/HTTP failures → `[]`; JSON parse errors
    /// propagate.
    async fn do_search_catalog(
        &self,
        term: &str,
        limit: i64,
        sf: &str,
    ) -> Result<Vec<TrackMeta>, CatalogError> {
        info_span!("itunes_search", term, limit, storefront = sf)
            .in_scope(|| debug!("Querying iTunes search API..."));
        let url = format!(
            "https://itunes.apple.com/search?term={}&entity=song&limit={}&country={}",
            urlencode(term),
            urlencode(&limit.to_string()),
            urlencode(sf)
        );

        let body = match self
            .transport
            .get(&url, ITUNES_USER_AGENT, SEARCH_TIMEOUT)
            .await
        {
            Ok(body) => body,
            Err(TransportError::Fetch { elapsed_ms, source }) => {
                error!(term, elapsed_ms, error = %source, "iTunes search timed out / network error");
                return Ok(Vec::new());
            }
            Err(TransportError::Status { status }) => {
                error!(term, status, "iTunes search HTTP failure");
                return Ok(Vec::new());
            }
        };

        let tracks: Vec<TrackMeta> = Self::parse_results(&body)?
            .iter()
            .filter(|r| is_track_item(r))
            .map(map_itunes_item)
            .collect();
        info!(term, matches = tracks.len(), "iTunes search resolved");
        Ok(tracks)
    }

    /// Search iTunes albums without applying the track-search storefront
    /// fallback. This is the narrow catalog operation used by the random
    /// album explorer.
    pub async fn search_albums(
        &self,
        term: &str,
        limit: i64,
        storefront: &str,
    ) -> Result<Vec<AlbumSearchResult>, CatalogError> {
        let sf = normalize_storefront(storefront);
        let url = format!(
            "https://itunes.apple.com/search?term={}&entity=album&limit={}&country={}",
            urlencode(term),
            urlencode(&limit.to_string()),
            urlencode(&sf),
        );
        let body = self
            .transport
            .get(&url, ITUNES_USER_AGENT, Duration::from_secs(10))
            .await
            .map_err(|error| match error {
                TransportError::Fetch { source, .. } => CatalogError::Message(source.to_string()),
                TransportError::Status { status } => CatalogError::Message(format!(
                    "Failed to search iTunes catalog (HTTP {status})"
                )),
            })?;
        let results = Self::parse_results(&body)?;
        Ok(results
            .into_iter()
            .filter_map(|item| {
                let id = item.collection_id?.to_string();
                Some(AlbumSearchResult {
                    id: id.clone(),
                    title: item
                        .collection_name
                        .unwrap_or_else(|| "Unknown Album".to_owned()),
                    artist: item
                        .artist_name
                        .unwrap_or_else(|| "Unknown Artist".to_owned()),
                    url: item
                        .collection_view_url
                        .unwrap_or_else(|| format!("https://music.apple.com/{sf}/album/{id}")),
                    release_date: item.release_date,
                    genre: item.primary_genre_name,
                    track_count: item
                        .track_count
                        .and_then(|count| usize::try_from(count).ok()),
                    storefront: sf.clone(),
                })
            })
            .collect())
    }

    /// Search the catalog. Never errors on transport/HTTP failures —
    /// degrades to an empty list; empty results trigger storefront
    /// fallbacks. The final (possibly empty) result is cached.
    pub async fn search_catalog(
        &self,
        term: &str,
        limit: i64,
        storefront: &str,
    ) -> Result<Vec<TrackMeta>, CatalogError> {
        let sf = normalize_storefront(storefront);
        let clean_term = term.trim().to_lowercase();
        let cache_key = format!("search:{sf}:{limit}:{clean_term}");

        if let Some(CacheValue::Search(tracks)) = self.get_cached(&cache_key) {
            return Ok(tracks);
        }

        let mut results = self.do_search_catalog(term, limit, &sf).await?;
        if results.is_empty() && sf != "us" {
            debug!(original_storefront = %sf, "Retrying catalog search on US storefront fallback");
            results = self.do_search_catalog(term, limit, "us").await?;
        }

        if results.is_empty() {
            for fallback in REGIONAL_STOREFRONTS {
                if fallback == sf {
                    continue;
                }
                results = self.do_search_catalog(term, limit, fallback).await?;
                if !results.is_empty() {
                    break;
                }
            }
        }

        self.set_cached(&cache_key, CacheValue::Search(results.clone()));
        Ok(results)
    }

    /// Fetch Apple Music charts albums for a storefront. Transport throws
    /// propagate raw, `!ok` becomes the charts HTTP message, JSON errors
    /// propagate.
    pub async fn fetch_charts_albums(
        &self,
        storefront: &str,
        limit: i64,
    ) -> Result<Vec<ChartAlbum>, CatalogError> {
        let sf = normalize_storefront(storefront);
        let cache_key = format!("charts:{sf}:{limit}");

        if let Some(CacheValue::Charts(albums)) = self.get_cached(&cache_key) {
            return Ok(albums);
        }

        let url = format!(
            "https://rss.marketingtools.apple.com/api/v2/{}/music/most-played/{}/albums.json",
            urlencode(&sf),
            urlencode(&limit.to_string())
        );

        let body = match self
            .transport
            .get(&url, CHARTS_USER_AGENT, CHARTS_TIMEOUT)
            .await
        {
            Ok(body) => body,
            Err(TransportError::Status { status }) => {
                return Err(CatalogError::Message(format!(
                    "Failed to fetch Apple Music charts (HTTP {status})"
                )));
            }
            // Transport failures propagate raw on this path.
            Err(err) => return Err(err.into()),
        };

        let data: ChartsResponse = serde_json::from_str(&body)?;
        let raw = data.feed.and_then(|feed| feed.results).unwrap_or_default();

        let albums: Vec<ChartAlbum> = raw
            .into_iter()
            .map(|r| ChartAlbum {
                id: r.id,
                title: r.name,
                artist: r.artist_name,
                url: r.url,
                artwork_url: r
                    .artwork_url_100
                    .as_deref()
                    .map(|u| format_artwork_url(Some(u))),
                release_date: r.release_date,
                genre: r.genres.first().and_then(|g| g.name.clone()),
            })
            .collect();

        self.set_cached(&cache_key, CacheValue::Charts(albums.clone()));
        Ok(albums)
    }
}

/// Production catalog handle shared across handlers/worker tasks.
pub type SharedCatalog = Arc<Catalog<ReqwestTransport>>;

/// Fetch the legacy Marketing Tools RSS album feed used by release discovery.
/// This remains separate from [`Catalog::fetch_charts_albums`] because the bot
/// historically used this endpoint and treats all failures as a skipped feed.
pub async fn fetch_marketing_tools_albums(
    http: &reqwest::Client,
    storefront: &str,
    limit: usize,
) -> Result<Vec<ChartAlbum>, String> {
    let url = format!(
        "https://rss.applemarketingtools.com/api/v2/{storefront}/music/most-played/{limit}/albums.json"
    );
    let response = http
        .get(url)
        .timeout(Duration::from_secs(10))
        .send()
        .await
        .map_err(|error| error.to_string())?;
    if !response.status().is_success() {
        return Err(format!("HTTP {}", response.status()));
    }
    let body = response.text().await.map_err(|error| error.to_string())?;
    let data: ChartsResponse = serde_json::from_str(&body).map_err(|error| error.to_string())?;
    Ok(data
        .feed
        .and_then(|feed| feed.results)
        .unwrap_or_default()
        .into_iter()
        .map(|album| ChartAlbum {
            id: album.id,
            title: album.name,
            artist: album.artist_name,
            url: album.url,
            artwork_url: album
                .artwork_url_100
                .as_deref()
                .map(|url| format_artwork_url(Some(url))),
            release_date: album.release_date,
            genre: album.genres.first().and_then(|genre| genre.name.clone()),
        })
        .collect())
}

/// Look up the tracks for one Marketing Tools album. Release discovery uses
/// this narrow operation so its legacy eight-second request and no-fallback
/// behavior stay separate from the user-facing catalog lookup.
pub async fn fetch_discovery_album_tracks(
    http: &reqwest::Client,
    storefront: &str,
    album_id: &str,
) -> Result<Vec<TrackMeta>, String> {
    let url =
        format!("https://itunes.apple.com/lookup?id={album_id}&entity=song&country={storefront}");
    let response = http
        .get(url)
        .header("User-Agent", ITUNES_USER_AGENT)
        .timeout(Duration::from_secs(8))
        .send()
        .await
        .map_err(|error| error.to_string())?;
    if !response.status().is_success() {
        return Err(format!("HTTP {}", response.status()));
    }
    let body = response.text().await.map_err(|error| error.to_string())?;
    let results =
        Catalog::<ReqwestTransport>::parse_results(&body).map_err(|error| error.to_string())?;
    Ok(results
        .into_iter()
        .filter(is_track_item)
        .map(|item| map_itunes_item(&item))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_amp_artwork_url_replaces_placeholders() {
        let template =
            "https://is1-ssl.mzstatic.com/image/thumb/Music115/v4/xx/yy/zz/{w}x{h}bb.{f}";
        assert_eq!(
            format_amp_artwork_url(Some(template)),
            "https://is1-ssl.mzstatic.com/image/thumb/Music115/v4/xx/yy/zz/1000x1000bb.jpg"
        );
        assert_eq!(format_amp_artwork_url(None), "");
    }

    #[test]
    fn parse_amp_song_body_extracts_all_fields() {
        let json = r#"{
            "data": [{
                "id": "1440871441",
                "type": "songs",
                "attributes": {
                    "name": "Starboy (feat. Daft Punk)",
                    "artistName": "The Weeknd",
                    "albumName": "Starboy",
                    "composerName": "Abel Tesfaye, Thomas Bangalter & Guy-Manuel de Homem-Christo",
                    "genreNames": ["R&B/Soul", "Music"],
                    "releaseDate": "2016-09-21T07:00:00Z",
                    "trackNumber": 1,
                    "discNumber": 1,
                    "durationInMillis": 230453,
                    "isrc": "USUM71607007",
                    "audioTraits": ["lossless", "spatial", "atmos"],
                    "artwork": {
                        "width": 3000,
                        "height": 3000,
                        "url": "https://is1-ssl.mzstatic.com/image/thumb/Music115/v4/7c/49/a1/mzi.x/{w}x{h}bb.{f}"
                    },
                    "recordLabel": "Universal Republic Records",
                    "copyright": "℗ 2016 The Weeknd XO, Inc.",
                    "isStreamable": true,
                    "contentRating": "explicit"
                },
                "relationships": {
                    "albums": {
                        "data": [{"id": "1440871434", "type": "albums"}]
                    },
                    "artists": {
                        "data": [{"id": "1234567", "type": "artists"}]
                    }
                }
            }]
        }"#;

        let meta = Catalog::<ReqwestTransport>::parse_amp_song_body(json, "1440871441").unwrap();
        assert_eq!(meta.id, "1440871441");
        assert_eq!(meta.title, "Starboy (feat. Daft Punk)");
        assert_eq!(meta.artist, "The Weeknd");
        assert_eq!(meta.album, "Starboy");
        assert_eq!(meta.album_artist, "The Weeknd");
        assert_eq!(meta.genre.as_deref(), Some("R&B/Soul"));
        assert_eq!(meta.release_date, "2016-09-21");
        assert_eq!(
            meta.composer.as_deref(),
            Some("Abel Tesfaye, Thomas Bangalter & Guy-Manuel de Homem-Christo")
        );
        assert_eq!(meta.track_number, Some(1));
        assert_eq!(meta.disc_number, Some(1));
        assert_eq!(meta.duration_secs, 230);
        assert!(meta.explicit);
        assert_eq!(meta.content_advisory.as_deref(), Some("explicit"));
        assert_eq!(
            meta.artwork_url,
            "https://is1-ssl.mzstatic.com/image/thumb/Music115/v4/7c/49/a1/mzi.x/1000x1000bb.jpg"
        );
        assert_eq!(meta.isrc.as_deref(), Some("USUM71607007"));
        assert_eq!(
            meta.record_label.as_deref(),
            Some("Universal Republic Records")
        );
        assert_eq!(
            meta.copyright.as_deref(),
            Some("℗ 2016 The Weeknd XO, Inc.")
        );
        assert_eq!(meta.is_streamable, Some(true));
        assert_eq!(meta.album_id.as_deref(), Some("1440871434"));
        assert_eq!(meta.artist_id.as_deref(), Some("1234567"));
    }

    #[test]
    fn parse_amp_song_body_extracts_track_count_from_album_relationship() {
        let json = r#"{
            "data": [{
                "id": "1440871441",
                "type": "songs",
                "attributes": {
                    "name": "Starboy (feat. Daft Punk)",
                    "artistName": "The Weeknd"
                },
                "relationships": {
                    "albums": {
                        "data": [{
                            "id": "1440871434",
                            "type": "albums",
                            "attributes": {
                                "name": "Starboy",
                                "artistName": "The Weeknd",
                                "trackCount": 18,
                                "upc": "00602557211438",
                                "recordLabel": "Universal Republic Records",
                                "copyright": "℗ 2016 The Weeknd XO, Inc."
                            }
                        }]
                    }
                }
            }]
        }"#;

        let meta = Catalog::<ReqwestTransport>::parse_amp_song_body(json, "1440871441").unwrap();
        assert_eq!(meta.id, "1440871441");
        assert_eq!(meta.track_count, Some(18));
        assert_eq!(meta.album, "Starboy");
        assert_eq!(meta.album_artist, "The Weeknd");
        assert_eq!(meta.album_id.as_deref(), Some("1440871434"));
        assert_eq!(meta.upc.as_deref(), Some("00602557211438"));
        assert_eq!(
            meta.record_label.as_deref(),
            Some("Universal Republic Records")
        );
        assert_eq!(
            meta.copyright.as_deref(),
            Some("℗ 2016 The Weeknd XO, Inc.")
        );
    }

    #[test]
    fn parse_amp_album_body_extracts_album_and_tracks_with_isrc() {
        let json = r#"{
            "data": [{
                "id": "1499378108",
                "type": "albums",
                "attributes": {
                    "name": "After Hours",
                    "artistName": "The Weeknd",
                    "artwork": {
                        "width": 3000,
                        "height": 3000,
                        "url": "https://is1-ssl.mzstatic.com/image/thumb/Music125/v4/4a/5b/6c/mzi.y/{w}x{h}bb.{f}"
                    },
                    "genreNames": ["R&B/Soul"],
                    "releaseDate": "2020-03-20",
                    "trackCount": 2,
                    "recordLabel": "Republic Records",
                    "copyright": "℗ 2020 The Weeknd XO, Inc.",
                    "upc": "00602508824578"
                },
                "relationships": {
                    "tracks": {
                        "data": [
                            {
                                "id": "1499378607",
                                "type": "songs",
                                "attributes": {
                                    "name": "Alone Again",
                                    "artistName": "The Weeknd",
                                    "trackNumber": 1,
                                    "discNumber": 1,
                                    "durationInMillis": 250000,
                                    "isrc": "USUG12000658",
                                    "isStreamable": true
                                }
                            },
                            {
                                "id": "1499378610",
                                "type": "songs",
                                "attributes": {
                                    "name": "Too Late",
                                    "artistName": "The Weeknd",
                                    "trackNumber": 2,
                                    "discNumber": 1,
                                    "durationInMillis": 239000,
                                    "isrc": "USUG12000659",
                                    "isStreamable": true
                                }
                            }
                        ]
                    }
                }
            }]
        }"#;

        let res = Catalog::<ReqwestTransport>::parse_amp_album_body(json, "1499378108").unwrap();
        assert_eq!(res.album.id, "1499378108");
        assert_eq!(res.album.title, "After Hours");
        assert_eq!(res.album.artist, "The Weeknd");
        assert_eq!(res.album.album, "After Hours");
        assert_eq!(res.album.genre.as_deref(), Some("R&B/Soul"));
        assert_eq!(res.album.release_date, "2020-03-20");
        assert_eq!(res.album.track_count, Some(2));
        assert_eq!(res.album.upc.as_deref(), Some("00602508824578"));
        assert_eq!(res.album.record_label.as_deref(), Some("Republic Records"));
        assert_eq!(
            res.album.artwork_url,
            "https://is1-ssl.mzstatic.com/image/thumb/Music125/v4/4a/5b/6c/mzi.y/1000x1000bb.jpg"
        );

        assert_eq!(res.tracks.len(), 2);
        assert_eq!(res.tracks[0].id, "1499378607");
        assert_eq!(res.tracks[0].title, "Alone Again");
        assert_eq!(res.tracks[0].album, "After Hours");
        assert_eq!(res.tracks[0].isrc.as_deref(), Some("USUG12000658"));
        assert_eq!(res.tracks[0].track_number, Some(1));
        assert_eq!(res.tracks[0].track_count, Some(2));
        assert_eq!(res.tracks[0].duration_secs, 250);

        assert_eq!(res.tracks[1].id, "1499378610");
        assert_eq!(res.tracks[1].title, "Too Late");
        assert_eq!(res.tracks[1].album, "After Hours");
        assert_eq!(res.tracks[1].isrc.as_deref(), Some("USUG12000659"));
        assert_eq!(res.tracks[1].track_number, Some(2));
        assert_eq!(res.tracks[1].track_count, Some(2));
        assert_eq!(res.tracks[1].duration_secs, 239);
    }

    #[test]
    fn parse_amp_album_no_tracks_fails() {
        let json = r#"{
            "data": [{
                "id": "1499378108",
                "type": "albums",
                "attributes": {
                    "name": "After Hours"
                },
                "relationships": {
                    "tracks": {
                        "data": []
                    }
                }
            }]
        }"#;

        let res = Catalog::<ReqwestTransport>::parse_amp_album_body(json, "1499378108");
        assert!(res.is_err());
    }
}
