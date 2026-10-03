//! Offline catalog tests via a fake transport — field mapping, cache
//! behavior, storefront fallback order, and error semantics.

use std::{collections::HashMap, sync::Mutex, time::Duration};

use apple::catalog::{Catalog, CatalogError, Transport, TransportError};
use engine::settings::LyricspornApiEndpoint;

/// Serves canned JSON by URL substring match; records every served URL.
struct FakeTransport {
    routes: HashMap<String, Route>,
    served: Mutex<Vec<String>>,
}

enum Route {
    Json(String),
    Status(u16),
}

impl FakeTransport {
    fn new() -> Self {
        Self {
            routes: HashMap::new(),
            served: Mutex::new(Vec::new()),
        }
    }

    /// Serve `json` for any URL containing `needle`.
    fn on(&mut self, needle: &str, json: &str) -> &mut Self {
        self.routes
            .insert(needle.to_owned(), Route::Json(json.to_owned()));
        self
    }

    /// Fail with an HTTP status for any URL containing `needle`.
    fn fail_with(&mut self, needle: &str, status: u16) -> &mut Self {
        self.routes.insert(needle.to_owned(), Route::Status(status));
        self
    }

    fn served(&self) -> Vec<String> {
        self.served.lock().unwrap().clone()
    }
}

fn configured_catalog(transport: FakeTransport) -> Catalog<FakeTransport> {
    Catalog::with_endpoint(transport, test_api_endpoint())
}

fn test_api_endpoint() -> LyricspornApiEndpoint {
    LyricspornApiEndpoint::new(Some("https://catalog.example/api/v1"))
}

impl Transport for FakeTransport {
    async fn get(
        &self,
        url: &str,
        user_agent: &str,
        timeout: Duration,
    ) -> Result<String, TransportError> {
        let _ = (user_agent, timeout);
        self.served.lock().unwrap().push(url.to_owned());
        for (needle, route) in &self.routes {
            if url.contains(needle.as_str()) {
                return match route {
                    Route::Json(body) => Ok(body.clone()),
                    Route::Status(status) => Err(TransportError::Status { status: *status }),
                };
            }
        }
        Err(TransportError::Status { status: 404 })
    }
}

fn track_json() -> String {
    serde_json::json!({
        "track": {
            "id": "1440841730",
            "type": "songs",
            "albumId": "1440841723",
            "artistId": "12345",
            "name": "The Hills",
            "albumName": "Beauty Behind the Madness",
            "artistName": "The Weeknd",
            "albumArtistName": "The Weeknd",
            "composer": "Abel Tesfaye",
            "genres": ["R&B/Soul"],
            "releaseDate": "2015-05-27T07:00:00Z",
            "trackNumber": 7,
            "trackCount": 14,
            "discNumber": 1,
            "discCount": 1,
            "durationMs": 241758,
            "contentRating": "explicit",
            "isrc": "USUG11500631",
            "recordLabel": "Republic Records",
            "copyright": "2015 The Weeknd XO, Inc.",
            "upc": "602547151602",
            "artwork": {
                "url": "https://is1-ssl.mzstatic.com/image/thumb/Music/v4/99/9b/abc/xyz/{w}x{h}bb.{f}"
            }
        }
    })
    .to_string()
}

#[tokio::test]
async fn track_mapping_matches_expected_fields() {
    let mut fake = FakeTransport::new();
    fake.on("tracks/1440841730?", &track_json());
    let catalog = configured_catalog(fake);
    let meta = catalog
        .fetch_track_meta("1440841730", "us")
        .await
        .expect("track fetch");
    assert_eq!(meta.id, "1440841730");
    assert_eq!(meta.title, "The Hills");
    assert_eq!(meta.artist, "The Weeknd");
    assert_eq!(meta.album, "Beauty Behind the Madness");
    assert_eq!(meta.album_artist, "The Weeknd");
    assert_eq!(meta.genre.as_deref(), Some("R&B/Soul"));
    assert_eq!(meta.release_date, "2015-05-27"); // sliced to 10 chars
    assert_eq!(meta.composer.as_deref(), Some("Abel Tesfaye"));
    assert_eq!(meta.album_id.as_deref(), Some("1440841723"));
    assert_eq!(meta.artist_id.as_deref(), Some("12345"));
    assert_eq!(meta.isrc.as_deref(), Some("USUG11500631"));
    assert_eq!(meta.record_label.as_deref(), Some("Republic Records"));
    assert_eq!(meta.copyright.as_deref(), Some("2015 The Weeknd XO, Inc."));
    assert_eq!(meta.upc.as_deref(), Some("602547151602"));
    assert_eq!(meta.track_number, Some(7));
    assert_eq!(meta.track_count, Some(14));
    assert_eq!(meta.disc_number, Some(1));
    assert_eq!(meta.disc_count, Some(1));
    assert_eq!(meta.duration_secs, 242); // 241758ms rounds to 242
    assert!(meta.explicit);
    assert_eq!(meta.content_advisory.as_deref(), Some("explicit"));
    assert_eq!(
        meta.artwork_url,
        "https://is1-ssl.mzstatic.com/image/thumb/Music/v4/99/9b/abc/xyz/1000x1000bb.jpg"
    );
}

#[tokio::test]
async fn track_missing_fields_map_to_defaults() {
    let mut fake = FakeTransport::new();
    fake.on("tracks/1?", r#"{"track":{"id":"1","durationMs":999}}"#);
    let catalog = configured_catalog(fake);
    let meta = catalog.fetch_track_meta("1", "us").await.expect("track");
    assert_eq!(meta.title, "");
    assert_eq!(meta.genre, None);
    assert_eq!(meta.release_date, ""); // absent date → ''
    assert_eq!(meta.duration_secs, 1);
    assert_eq!(meta.artwork_url, ""); // absent artwork → ''
    assert!(!meta.explicit);
}

#[tokio::test]
async fn track_not_found_returns_expected_message() {
    let mut fake = FakeTransport::new();
    fake.fail_with("tracks/999?", 404);
    let catalog = configured_catalog(fake);
    let err = catalog
        .fetch_track_meta("999", "us")
        .await
        .expect_err("should fail");
    match err {
        CatalogError::Message(msg) => {
            assert_eq!(msg, "Lyricsporn track metadata was not found")
        }
        other => panic!("expected Message, got {other:?}"),
    }
}

#[tokio::test]
async fn track_http_error_uses_expected_message() {
    let mut fake = FakeTransport::new();
    fake.fail_with("storefront=us", 503);
    let catalog = configured_catalog(fake);
    let err = catalog
        .fetch_track_meta("1", "us")
        .await
        .expect_err("should fail");
    match err {
        CatalogError::Message(msg) => {
            assert_eq!(msg, "Lyricsporn track metadata request failed (HTTP 503)")
        }
        other => panic!("expected Message, got {other:?}"),
    }
}

#[tokio::test]
async fn cache_hit_serves_one_network_call() {
    let mut fake = FakeTransport::new();
    fake.on("tracks/1440841730?", &track_json());
    let catalog = configured_catalog(fake);
    let first = catalog
        .fetch_track_meta("1440841730", "us")
        .await
        .expect("first");
    let second = catalog
        .fetch_track_meta("1440841730", "us")
        .await
        .expect("second (cached)");
    assert_eq!(first, second);
    assert_eq!(
        catalog.transport().served().len(),
        1,
        "second call must be a cache hit"
    );
}

#[tokio::test]
async fn us_fallback_caches_under_original_key() {
    let mut fake = FakeTransport::new();
    // jp fails, us succeeds.
    fake.fail_with("storefront=jp", 404);
    fake.on("storefront=us", &track_json());
    let catalog = configured_catalog(fake);
    let meta = catalog
        .fetch_track_meta("1440841730", "jp")
        .await
        .expect("us fallback should succeed");
    assert_eq!(meta.id, "1440841730");
    // Called again: served from cache under the ORIGINAL jp key.
    let again = catalog
        .fetch_track_meta("1440841730", "jp")
        .await
        .expect("cached");
    assert_eq!(again, meta);
    let us_served = catalog
        .transport()
        .served()
        .into_iter()
        .filter(|u| u.contains("storefront=us"))
        .count();
    assert_eq!(us_served, 1, "second call must hit the jp cache key");
}

#[tokio::test]
async fn regional_chain_order() {
    let mut fake = FakeTransport::new();
    fake.fail_with("storefront=jp", 404);
    fake.fail_with("storefront=us", 404);
    fake.on("storefront=gb", &track_json());
    let catalog = configured_catalog(fake);
    let meta = catalog
        .fetch_track_meta("1440841730", "jp")
        .await
        .expect("gb fallback should succeed");
    assert_eq!(meta.id, "1440841730");
    // Attempt order: jp (primary), us, gb (first regional hit).
    let served = catalog.transport().served();
    let order: Vec<&str> = served
        .iter()
        .map(|u| {
            u.split("storefront=")
                .nth(1)
                .unwrap_or("?")
                .split('&')
                .next()
                .unwrap_or("?")
        })
        .collect();
    assert_eq!(order, vec!["jp", "us", "gb"]);
}

#[tokio::test]
async fn all_fail_rethrows_original_error() {
    let mut fake = FakeTransport::new();
    fake.fail_with("storefront=jp", 404);
    fake.fail_with("storefront=us", 503);
    for sf in ["gb", "in", "ca", "de", "fr", "au"] {
        fake.fail_with(&format!("storefront={sf}"), 404);
    }
    let catalog = configured_catalog(fake);
    let err = catalog
        .fetch_track_meta("1440841730", "jp")
        .await
        .expect_err("all storefronts fail");
    // ORIGINAL error was the jp 404, not the later us 503.
    match err {
        CatalogError::Message(msg) => {
            assert_eq!(msg, "Lyricsporn track metadata was not found")
        }
        other => panic!("expected Message, got {other:?}"),
    }
    // 1 primary + 1 us + 6 remaining regionals = 8 attempts.
    assert_eq!(catalog.transport().served().len(), 8);
}

#[tokio::test]
async fn album_mapping_and_meta_from_collection() {
    let album = serde_json::json!({
        "data": {
            "id": "1440841723",
            "type": "albums",
            "name": "Beauty Behind the Madness",
            "artistName": "The Weeknd",
            "artistUrl": "https://music.apple.com/us/artist/the-weeknd/12345",
            "contentRating": "explicit",
            "releaseDate": "2015-08-28T07:00:00Z",
            "trackCount": 2,
            "artwork": {"url": "https://x/{w}x{h}bb.{f}"}
        }
    })
    .to_string();
    let tracks = serde_json::json!({
        "type": "songs",
        "items": [
            {"id":"1","type":"songs","name":"T1","artistName":"The Weeknd","durationMs":100000},
            {"id":"2","type":"songs","name":"T2","artistName":"The Weeknd","durationMs":200000}
        ],
        "page": {"offset":0,"limit":100,"total":2,"next":null}
    })
    .to_string();
    let mut fake = FakeTransport::new();
    fake.on("albums/1440841723?", &album);
    fake.on("albums/1440841723/collections/tracks", &tracks);
    let catalog = configured_catalog(fake);
    let res = catalog
        .fetch_album_tracks("1440841723", "us")
        .await
        .expect("album");
    assert_eq!(res.album.id, "1440841723");
    assert_eq!(res.album.album, "Beauty Behind the Madness");
    assert_eq!(res.album.release_date, "2015-08-28");
    assert_eq!(res.album.duration_secs, 0);
    assert!(res.album.explicit);
    assert_eq!(res.album.artwork_url, "https://x/1000x1000bb.jpg");
    assert_eq!(res.album.artist_id.as_deref(), Some("12345"));
    assert_eq!(res.tracks.len(), 2);
    assert_eq!(res.tracks[0].title, "T1");
}

#[tokio::test]
async fn album_without_tracks_is_not_found() {
    let mut fake = FakeTransport::new();
    fake.on("albums/55?", r#"{"data":{"id":"55","name":"Empty"}}"#);
    fake.on(
        "albums/55/collections/tracks",
        r#"{"type":"songs","items":[],"page":{"offset":0,"limit":100,"total":0,"next":null}}"#,
    );
    let catalog = configured_catalog(fake);
    let err = catalog
        .fetch_album_tracks("55", "us")
        .await
        .expect_err("no tracks");
    match err {
        CatalogError::Message(msg) => {
            assert_eq!(msg, "Lyricsporn found no tracks for album 55")
        }
        other => panic!("expected Message, got {other:?}"),
    }
}

#[tokio::test]
async fn artist_album_tracks_deduplicate_and_map_name() {
    let mut fake = FakeTransport::new();
    fake.on(
        "artists/4797563?",
        r#"{"data":{"id":"4797563","name":"The Weeknd"}}"#,
    );
    fake.on(
        "artists/4797563/collections/albums",
        r#"{"type":"albums","items":[{"id":"1000","name":"Album 1","artistName":"The Weeknd"},{"id":"1001","name":"Album 2","artistName":"The Weeknd"}],"page":{"offset":0,"limit":100,"total":2,"next":null}}"#,
    );
    fake.on(
        "albums/1000/collections/tracks",
        r#"{"type":"songs","items":[{"id":"999","name":"Song 1","artistName":"The Weeknd","durationMs":60000}],"page":{"offset":0,"limit":100,"total":1,"next":null}}"#,
    );
    fake.on(
        "albums/1001/collections/tracks",
        r#"{"type":"songs","items":[{"id":"999","name":"Song 2","artistName":"The Weeknd","durationMs":60000}],"page":{"offset":0,"limit":100,"total":1,"next":null}}"#,
    );
    let catalog = configured_catalog(fake);

    let res = catalog
        .fetch_artist_tracks("4797563", "us")
        .await
        .expect("artist");
    assert_eq!(res.artist_name, "The Weeknd");
    assert_eq!(res.tracks.len(), 1, "duplicate track IDs are removed");
    assert_eq!(res.tracks[0].id, "999");
    assert_eq!(res.tracks[0].title, "Song 1");
}

#[tokio::test]
async fn artist_without_albums_returns_not_found() {
    let mut fake = FakeTransport::new();
    fake.on(
        "artists/123?",
        r#"{"data":{"id":"123","name":"No Albums"}}"#,
    );
    fake.on(
        "artists/123/collections/albums",
        r#"{"type":"albums","items":[],"page":{"offset":0,"limit":100,"total":0,"next":null}}"#,
    );
    let catalog = configured_catalog(fake);
    let err = catalog
        .fetch_artist_tracks("123", "us")
        .await
        .expect_err("empty artist");
    assert!(err.to_string().contains("no tracks for artist 123"));
}

#[tokio::test]
async fn artist_skips_an_unavailable_album_when_another_has_tracks() {
    let mut fake = FakeTransport::new();
    fake.on("artists/7?", r#"{"data":{"id":"7","name":"Solo"}}"#);
    fake.on(
        "artists/7/collections/albums",
        r#"{"type":"albums","items":[{"id":"bad","name":"Unavailable"},{"id":"good","name":"Available","artistName":"Solo"}],"page":{"offset":0,"limit":100,"total":2,"next":null}}"#,
    );
    fake.fail_with("albums/bad/collections/tracks", 503);
    fake.on(
        "albums/good/collections/tracks",
        r#"{"type":"songs","items":[{"id":"77","name":"Only Song","artistName":"Solo","durationMs":1000}],"page":{"offset":0,"limit":100,"total":1,"next":null}}"#,
    );
    let catalog = configured_catalog(fake);
    let res = catalog
        .fetch_artist_tracks("7", "us")
        .await
        .expect("artist with one available album");
    assert_eq!(res.artist_name, "Solo");
    assert_eq!(res.tracks.len(), 1);
    assert_eq!(res.tracks[0].title, "Only Song");
}

#[tokio::test]
async fn search_never_errors_and_falls_back() {
    let mut fake = FakeTransport::new();
    fake.fail_with("storefront=jp", 500); // primary fails → []
    fake.on("storefront=us", r#"{"results":{"songs":{"items":[]}}}"#);
    fake.on(
        "storefront=gb",
        r#"{"results":{"songs":{"items":[{"id":"5","type":"songs","name":"Found","artistName":"A","durationMs":1000}]}}}"#,
    );
    let catalog = configured_catalog(fake);
    let results = catalog
        .search_catalog("query", 5, "jp")
        .await
        .expect("search never errors");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].title, "Found");
    // Cached final result: second search makes no new calls.
    let served_before = catalog.transport().served().len();
    let again = catalog.search_catalog("query", 5, "jp").await.unwrap();
    assert_eq!(again, results);
    assert_eq!(catalog.transport().served().len(), served_before);
}

#[tokio::test]
async fn search_empty_results_fall_through_to_regional() {
    let mut fake = FakeTransport::new();
    fake.on("storefront=us", r#"{"results":{"songs":{"items":[]}}}"#);
    fake.on(
        "storefront=de",
        r#"{"results":{"songs":{"items":[{"id":"9","type":"songs","name":"De Hit","artistName":"B","durationMs":1000}]}}}"#,
    );
    let catalog = configured_catalog(fake);
    let results = catalog
        .search_catalog("term", 5, "us")
        .await
        .expect("search");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].title, "De Hit");
}

#[tokio::test]
async fn cache_ttl_expiry_refetches() {
    let mut fake = FakeTransport::new();
    fake.on("tracks/1440841730?", &track_json());
    let catalog =
        Catalog::with_limits_and_endpoint(fake, 10, Duration::from_millis(50), test_api_endpoint());
    let _ = catalog
        .fetch_track_meta("1440841730", "us")
        .await
        .expect("first");
    assert_eq!(catalog.transport().served().len(), 1);
    tokio::time::sleep(Duration::from_millis(120)).await;
    let _ = catalog
        .fetch_track_meta("1440841730", "us")
        .await
        .expect("refetch after expiry");
    assert_eq!(
        catalog.transport().served().len(),
        2,
        "expired entry must refetch"
    );
}

#[tokio::test]
async fn clear_cache_forces_refetch() {
    let mut fake = FakeTransport::new();
    fake.on("tracks/1440841730?", &track_json());
    let catalog = configured_catalog(fake);
    let _ = catalog
        .fetch_track_meta("1440841730", "us")
        .await
        .expect("first");
    catalog.clear_cache();
    let _ = catalog
        .fetch_track_meta("1440841730", "us")
        .await
        .expect("refetch after clear");
    assert_eq!(catalog.transport().served().len(), 2);
}

#[tokio::test]
async fn album_includes_music_videos_as_tracks() {
    let album = r#"{"data":{"id":"1753101056","name":"Four Me (Apple Music Edition) - EP","artistName":"Karan Aujla"}}"#;
    let tracks = r#"{"type":"songs","items":[{"id":"1753101068","type":"songs","name":"IDK HOW","artistName":"Karan Aujla","durationMs":180000},{"id":"1753101549","type":"music-videos","name":"Up Next: Karan Aujla (Exclusive)","artistName":"Karan Aujla","durationMs":289000}],"page":{"offset":0,"limit":100,"total":2,"next":null}}"#;
    let mut fake = FakeTransport::new();
    fake.on("albums/1753101056?", album);
    fake.on("albums/1753101056/collections/tracks", tracks);
    let catalog = configured_catalog(fake);
    let res = catalog
        .fetch_album_tracks("1753101056", "us")
        .await
        .expect("album tracks");
    assert_eq!(res.tracks.len(), 2, "both song and music-video included");
    assert_eq!(res.tracks[0].id, "1753101068");
    assert_eq!(res.tracks[0].title, "IDK HOW");
    assert_eq!(res.tracks[1].id, "1753101549");
    assert_eq!(res.tracks[1].title, "Up Next: Karan Aujla (Exclusive)");
}

#[tokio::test]
async fn fetch_track_supports_music_videos() {
    let json = r#"{"track":{"id":"1753101549","type":"music-videos","name":"Up Next: Karan Aujla (Exclusive)","artistName":"Karan Aujla","durationMs":289000}}"#;
    let mut fake = FakeTransport::new();
    fake.on("tracks/1753101549?", json);
    let catalog = configured_catalog(fake);
    let meta = catalog
        .fetch_track_meta("1753101549", "us")
        .await
        .expect("music video should be supported as track");
    assert_eq!(meta.id, "1753101549");
    assert_eq!(meta.title, "Up Next: Karan Aujla (Exclusive)");
}

#[tokio::test]
async fn fetch_track_rejects_response_without_track_data() {
    let mut fake = FakeTransport::new();
    fake.on("tracks/999999999?", r#"{"data":{"id":"999999999"}}"#);
    let catalog = configured_catalog(fake);
    let err = catalog
        .fetch_track_meta("999999999", "us")
        .await
        .expect_err("response without a track should be rejected");
    match err {
        CatalogError::Message(msg) => {
            assert_eq!(msg, "Lyricsporn returned no metadata for track 999999999");
        }
        other => panic!("expected CatalogError::Message, got {other:?}"),
    }
}
