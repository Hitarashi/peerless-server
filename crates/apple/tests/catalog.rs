//! Offline catalog tests via a fake transport — field mapping, cache
//! behavior, storefront fallback order, and error semantics.

use std::{collections::HashMap, sync::Mutex, time::Duration};

use apple::catalog::{Catalog, CatalogError, Transport, TransportError};

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
    r#"{
        "results": [{
            "wrapperType": "track",
            "kind": "song",
            "trackId": 1440841730,
            "collectionId": 1440841723,
            "artistId": 12345,
            "trackName": "The Hills",
            "collectionName": "Beauty Behind the Madness",
            "artistName": "The Weeknd",
            "collectionArtistName": "The Weeknd",
            "composerName": "Abel Tesfaye",
            "primaryGenreName": "R&B/Soul",
            "releaseDate": "2015-05-27T07:00:00Z",
            "trackNumber": 7,
            "trackCount": 14,
            "discNumber": 1,
            "discCount": 1,
            "trackTimeMillis": 241758,
            "trackExplicitness": "explicit",
            "isrc": "USUG11500631",
            "recordLabel": "Republic Records",
            "copyright": "2015 The Weeknd XO, Inc.",
            "upc": "602547151602",
            "artworkUrl100": "https://is1-ssl.mzstatic.com/image/thumb/Music/v4/99/9b/abc/xyz/100x100bb.jpg"
        }]
    }"#
    .to_owned()
}

#[tokio::test]
async fn track_mapping_matches_expected_fields() {
    let mut fake = FakeTransport::new();
    fake.on("id=1440841730", &track_json());
    let catalog = Catalog::new(fake);
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
        "https://is1-ssl.mzstatic.com/image/thumb/Music/v4/99/9b/abc/xyz/3000x3000bb.jpg"
    );
}

#[tokio::test]
async fn track_missing_fields_map_to_defaults() {
    let mut fake = FakeTransport::new();
    fake.on(
        "id=1",
        r#"{"results": [{"wrapperType": "track", "trackId": 1, "trackTimeMillis": 999}]}"#,
    );
    let catalog = Catalog::new(fake);
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
    fake.on("id=999", r#"{"results": []}"#);
    let catalog = Catalog::new(fake);
    let err = catalog
        .fetch_track_meta("999", "us")
        .await
        .expect_err("should fail");
    match err {
        CatalogError::Message(msg) => {
            assert_eq!(msg, "iTunes found no song matching track ID 999")
        }
        other => panic!("expected Message, got {other:?}"),
    }
}

#[tokio::test]
async fn track_http_error_uses_expected_message() {
    let mut fake = FakeTransport::new();
    fake.fail_with("id=1", 503);
    let catalog = Catalog::new(fake);
    let err = catalog
        .fetch_track_meta("1", "us")
        .await
        .expect_err("should fail");
    // The track HTTP message omits the "track" context word.
    match err {
        CatalogError::Message(msg) => {
            assert_eq!(msg, "iTunes lookup failed (HTTP 503)")
        }
        other => panic!("expected Message, got {other:?}"),
    }
}

#[tokio::test]
async fn cache_hit_serves_one_network_call() {
    let mut fake = FakeTransport::new();
    fake.on("id=1440841730", &track_json());
    let catalog = Catalog::new(fake);
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
    fake.fail_with("country=jp", 404);
    fake.on("country=us", &track_json());
    let catalog = Catalog::new(fake);
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
        .filter(|u| u.contains("country=us"))
        .count();
    assert_eq!(us_served, 1, "second call must hit the jp cache key");
}

#[tokio::test]
async fn regional_chain_order() {
    let mut fake = FakeTransport::new();
    fake.fail_with("country=jp", 404);
    fake.fail_with("country=us", 404);
    fake.on("country=gb", &track_json());
    let catalog = Catalog::new(fake);
    let meta = catalog
        .fetch_track_meta("1440841730", "jp")
        .await
        .expect("gb fallback should succeed");
    assert_eq!(meta.id, "1440841730");
    // Attempt order: jp (primary), us, gb (first regional hit).
    let served = catalog.transport().served();
    let order: Vec<&str> = served
        .iter()
        .map(|u| u.split("country=").nth(1).unwrap_or("?"))
        .collect();
    assert_eq!(order, vec!["jp", "us", "gb"]);
}

#[tokio::test]
async fn all_fail_rethrows_original_error() {
    let mut fake = FakeTransport::new();
    fake.fail_with("country=jp", 404);
    fake.fail_with("country=us", 503);
    for sf in ["gb", "in", "ca", "de", "fr", "au"] {
        fake.fail_with(&format!("country={sf}"), 404);
    }
    let catalog = Catalog::new(fake);
    let err = catalog
        .fetch_track_meta("1440841730", "jp")
        .await
        .expect_err("all storefronts fail");
    // ORIGINAL error was the jp 404, not the later us 503.
    match err {
        CatalogError::Message(msg) => {
            assert_eq!(msg, "iTunes lookup failed (HTTP 404)")
        }
        other => panic!("expected Message, got {other:?}"),
    }
    // 1 primary + 1 us + 6 remaining regionals = 8 attempts.
    assert_eq!(catalog.transport().served().len(), 8);
}

#[tokio::test]
async fn album_mapping_and_meta_from_collection() {
    let json = r#"{
        "results": [
            {
                "wrapperType": "collection",
                "collectionId": 1440841723,
                "collectionName": "Beauty Behind the Madness",
                "artistName": "The Weeknd",
                "collectionExplicitness": "explicit",
                "releaseDate": "2015-08-28T07:00:00Z",
                "artworkUrl100": "https://x/100x100bb.jpg"
            },
            {
                "wrapperType": "track",
                "kind": "song",
                "trackId": 1,
                "trackName": "T1",
                "collectionName": "Beauty Behind the Madness",
                "artistName": "The Weeknd",
                "trackTimeMillis": 100000
            },
            {
                "wrapperType": "track",
                "kind": "song",
                "trackId": 2,
                "trackName": "T2",
                "artistName": "The Weeknd",
                "trackTimeMillis": 200000
            }
        ]
    }"#;
    let mut fake = FakeTransport::new();
    fake.on("id=1440841723", json);
    let catalog = Catalog::new(fake);
    let res = catalog
        .fetch_album_tracks("1440841723", "us")
        .await
        .expect("album");
    assert_eq!(res.album.id, "1440841723");
    assert_eq!(res.album.album, "Beauty Behind the Madness");
    assert_eq!(res.album.release_date, "2015-08-28");
    assert_eq!(res.album.duration_secs, 0);
    assert!(res.album.explicit);
    assert_eq!(res.album.artwork_url, "https://x/3000x3000bb.jpg");
    assert_eq!(res.tracks.len(), 2);
    assert_eq!(res.tracks[0].title, "T1");
}

#[tokio::test]
async fn album_without_tracks_is_not_found() {
    let mut fake = FakeTransport::new();
    fake.on(
        "id=55",
        r#"{"results": [{"wrapperType":"collection","collectionId":55}]}"#,
    );
    let catalog = Catalog::new(fake);
    let err = catalog
        .fetch_album_tracks("55", "us")
        .await
        .expect_err("no tracks");
    match err {
        CatalogError::Message(msg) => {
            assert_eq!(msg, "iTunes found no tracks for collection 55")
        }
        other => panic!("expected Message, got {other:?}"),
    }
}

#[tokio::test]
async fn artist_chunking_dedup_and_name_fallback() {
    // 30 collections → batches of 25 + 5. Track 999 appears in every batch
    // and must dedup to one entry.
    let discog = {
        let mut results = vec![serde_json::json!({
            "wrapperType": "artist",
            "artistId": 4797563,
            "artistName": "The Weeknd"
        })];
        for i in 0..30 {
            results.push(serde_json::json!({
                "wrapperType": "collection",
                "collectionId": 1000 + i
            }));
        }
        serde_json::json!({ "results": results }).to_string()
    };
    let mut batch = Vec::new();
    for id in 0..30 {
        batch.push(serde_json::json!({
            "wrapperType": "track",
            "kind": "song",
            "trackId": if id == 0 { 999 } else { 2000 + id },
            "trackName": format!("Song {id}"),
            "artistName": "The Weeknd",
            "trackTimeMillis": 60000
        }));
    }
    let batch_json = serde_json::json!({ "results": batch }).to_string();

    let mut fake = FakeTransport::new();
    fake.on("entity=album", &discog);
    fake.on("entity=song", &batch_json);
    let catalog = Catalog::new(fake);

    let res = catalog
        .fetch_artist_tracks("4797563", "us")
        .await
        .expect("artist");
    assert_eq!(res.artist_name, "The Weeknd");
    // Unique ids: 999 plus 2001..=2029 = 30 (the per-batch duplicate 999 dedups).
    assert_eq!(res.tracks.len(), 30);
    let unique: std::collections::HashSet<&str> =
        res.tracks.iter().map(|t| t.id.as_str()).collect();
    assert_eq!(unique.len(), 30, "no duplicate track ids");
    // Two batch URLs (25 + 5 chunking).
    let batches: Vec<_> = catalog
        .transport()
        .served()
        .into_iter()
        .filter(|u| u.contains("entity=song"))
        .collect();
    assert_eq!(batches.len(), 2, "30 collections chunk into 2 batches");
    let id_count = |url: &str| {
        url.split("id=")
            .nth(1)
            .unwrap()
            .split('&')
            .next()
            .unwrap()
            .split(',')
            .count()
    };
    assert_eq!(id_count(&batches[0]), 25);
    assert_eq!(id_count(&batches[1]), 5);
}

#[tokio::test]
async fn artist_name_falls_back_to_first_track() {
    // No artist wrapper anywhere; name must come from the first track.
    let discog = serde_json::json!({
        "results": [
            {"wrapperType": "collection", "collectionId": 100},
            {"wrapperType": "track", "trackId": 42, "trackName": "S", "artistName": "Fallback Artist", "trackTimeMillis": 1000}
        ]
    })
    .to_string();
    let mut fake = FakeTransport::new();
    fake.on("entity=album", &discog);
    // Batch lookup serves the same two results.
    fake.on("entity=song", &discog);
    let catalog = Catalog::new(fake);
    let res = catalog
        .fetch_artist_tracks("123", "us")
        .await
        .expect("artist");
    assert_eq!(res.artist_name, "Fallback Artist");
    assert_eq!(res.tracks.len(), 1);
}

#[tokio::test]
async fn artist_song_fallback_when_no_collections() {
    // Discography returns no collections → entity=song fallback path.
    let discog = serde_json::json!({
        "results": [
            {"wrapperType": "artist", "artistId": 7, "artistName": "Solo"},
            {"wrapperType": "track", "trackId": 77, "trackName": "Only Song", "artistName": "Solo", "trackTimeMillis": 1000}
        ]
    })
    .to_string();
    let mut fake = FakeTransport::new();
    // entity=album request gets the artist+track payload too (no collections).
    fake.on("entity=album", &discog);
    fake.on("entity=song", &discog);
    let catalog = Catalog::new(fake);
    let res = catalog
        .fetch_artist_tracks("7", "us")
        .await
        .expect("artist via song fallback");
    assert_eq!(res.artist_name, "Solo");
    assert_eq!(res.tracks.len(), 1);
    assert_eq!(res.tracks[0].title, "Only Song");
}

#[tokio::test]
async fn search_never_errors_and_falls_back() {
    let mut fake = FakeTransport::new();
    fake.fail_with("country=jp", 500); // primary fails → []
    fake.on("country=us", r#"{"results": []}"#); // us fallback: empty
    fake.on(
        "country=gb",
        r#"{"results": [{"wrapperType":"track","kind":"song","trackId":5,"trackName":"Found","artistName":"A","trackTimeMillis":1000}]}"#,
    );
    let catalog = Catalog::new(fake);
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
    fake.on("country=us", r#"{"results": []}"#);
    fake.on(
        "country=de",
        r#"{"results": [{"wrapperType":"track","kind":"song","trackId":9,"trackName":"De Hit","artistName":"B","trackTimeMillis":1000}]}"#,
    );
    let catalog = Catalog::new(fake);
    let results = catalog
        .search_catalog("term", 5, "us")
        .await
        .expect("search");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].title, "De Hit");
}

#[tokio::test]
async fn charts_mapping_with_option_fields() {
    let json = r#"{
        "feed": {
            "results": [
                {
                    "id": "1440904761",
                    "name": "After Hours",
                    "artistName": "The Weeknd",
                    "url": "https://music.apple.com/us/album/after-hours/1440904761",
                    "artworkUrl100": "https://x/100x100bb.jpg",
                    "releaseDate": "2020-03-20",
                    "genres": [{"name": "Pop"}, {"name": "R&B/Soul"}]
                },
                {
                    "id": "1",
                    "name": "No Artwork",
                    "artistName": "B",
                    "url": "https://music.apple.com/us/album/x/1"
                }
            ]
        }
    }"#;
    let mut fake = FakeTransport::new();
    fake.on("most-played", json);
    let catalog = Catalog::new(fake);
    let albums = catalog.fetch_charts_albums("us", 50).await.expect("charts");
    assert_eq!(albums.len(), 2);
    assert_eq!(albums[0].title, "After Hours");
    assert_eq!(albums[0].genre.as_deref(), Some("Pop"));
    assert_eq!(
        albums[0].artwork_url.as_deref(),
        Some("https://x/3000x3000bb.jpg")
    );
    assert_eq!(albums[1].artwork_url, None);
    assert_eq!(albums[1].genre, None);
    assert_eq!(albums[1].release_date, None);
}

#[tokio::test]
async fn charts_http_error_message() {
    let mut fake = FakeTransport::new();
    fake.fail_with("most-played", 500);
    let catalog = Catalog::new(fake);
    let err = catalog
        .fetch_charts_albums("us", 50)
        .await
        .expect_err("charts http failure");
    match err {
        CatalogError::Message(msg) => {
            assert_eq!(msg, "Failed to fetch Apple Music charts (HTTP 500)")
        }
        other => panic!("expected Message, got {other:?}"),
    }
}

#[tokio::test]
async fn cache_ttl_expiry_refetches() {
    let mut fake = FakeTransport::new();
    fake.on("id=1440841730", &track_json());
    let catalog = Catalog::with_limits(fake, 10, Duration::from_millis(50));
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
    fake.on("id=1440841730", &track_json());
    let catalog = Catalog::new(fake);
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
    let json = r#"{
        "results": [
            {
                "wrapperType": "collection",
                "collectionId": 1753101056,
                "collectionName": "Four Me (Apple Music Edition) - EP",
                "artistName": "Karan Aujla"
            },
            {
                "wrapperType": "track",
                "kind": "song",
                "trackId": 1753101068,
                "trackName": "IDK HOW",
                "artistName": "Karan Aujla",
                "trackTimeMillis": 180000
            },
            {
                "wrapperType": "track",
                "kind": "music-video",
                "trackId": 1753101549,
                "trackName": "Up Next: Karan Aujla (Exclusive)",
                "artistName": "Karan Aujla",
                "trackTimeMillis": 289000
            }
        ]
    }"#;
    let mut fake = FakeTransport::new();
    fake.on("id=1753101056", json);
    let catalog = Catalog::new(fake);
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
    let json = r#"{
        "results": [
            {
                "wrapperType": "track",
                "kind": "music-video",
                "trackId": 1753101549,
                "trackName": "Up Next: Karan Aujla (Exclusive)",
                "artistName": "Karan Aujla",
                "trackTimeMillis": 289000
            }
        ]
    }"#;
    let mut fake = FakeTransport::new();
    fake.on("id=1753101549", json);
    let catalog = Catalog::new(fake);
    let meta = catalog
        .fetch_track_meta("1753101549", "us")
        .await
        .expect("music video should be supported as track");
    assert_eq!(meta.id, "1753101549");
    assert_eq!(meta.title, "Up Next: Karan Aujla (Exclusive)");
}

#[tokio::test]
async fn fetch_track_rejects_feature_movies() {
    let json = r#"{
        "results": [
            {
                "wrapperType": "track",
                "kind": "feature-movie",
                "trackId": 999999999,
                "trackName": "Some Movie",
                "artistName": "Some Director",
                "trackTimeMillis": 7200000
            }
        ]
    }"#;
    let mut fake = FakeTransport::new();
    fake.on("id=999999999", json);
    let catalog = Catalog::new(fake);
    let err = catalog
        .fetch_track_meta("999999999", "us")
        .await
        .expect_err("feature movie should be rejected");
    match err {
        CatalogError::Message(msg) => {
            assert_eq!(
                msg,
                "iTunes item 999999999 is a feature movie; only audio tracks are supported"
            );
        }
        other => panic!("expected CatalogError::Message, got {other:?}"),
    }
}
