use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use db::{
    AlbumsRepository, DbPool, NewAlbum, RequestLogRepository, SettingsStore, TracksRepository,
    connect_test_isolated, migrate,
};
use diesel::{sql_query, sql_types::Text};
use diesel_async::RunQueryDsl;
use engine::orchestrator::deps::{
    AlbumReplacementExpectation, AlbumReplacementResult, AlbumUpload, RequestLog, SaveTrackInput,
};
use music::{Codec, Provider, TrackKey};
use serde_json::json;

async fn client() -> DbPool {
    let client = connect_test_isolated()
        .await
        .expect("TEST_DATABASE_URL and PostgreSQL are required for db tests");
    migrate(&client).await.expect("database migrations");
    client
}

fn track(id: &str, title: &str) -> SaveTrackInput {
    SaveTrackInput {
        track_key: TrackKey::new(Provider::Apple, id).with_codec(Codec::Alac),
        codec: Codec::Alac,
        message_id: 123,
        file_id: format!("file-{id}"),
        file_unique_id: format!("unique-{id}"),
        title: title.to_owned(),
        artist: "Test Artist".to_owned(),
        album: "Test Album".to_owned(),
        duration: 180,
        bit_depth: 24,
        sample_rate: 44_100,
        genre: "Test".to_owned(),
        release_date: "2026-01-01".to_owned(),
        track_number: 1,
        track_count: 1,
        isrc: None,
    }
}

async fn clean_tracks(client: &DbPool, prefix: &str) {
    let mut connection = client.connection().await.expect("connection");
    sql_query(format!(
        "DELETE FROM tracks WHERE track_id LIKE '{}%'",
        prefix.replace('\'', "''")
    ))
    .execute(&mut *connection)
    .await
    .expect("track cleanup");
}

async fn execute(client: &DbPool, statement: &str) {
    let mut connection = client.connection().await.expect("connection");
    sql_query(statement)
        .execute(&mut *connection)
        .await
        .expect("statement execution");
}

static PREFIX_SEQUENCE: AtomicU64 = AtomicU64::new(0);

fn unique_prefix(kind: &str) -> String {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before unix epoch")
        .as_nanos();
    let sequence = PREFIX_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    format!("db-m5b-{kind}-{stamp}-{sequence}-")
}

#[tokio::test]
async fn track_cache_hit_miss_and_empty_list() {
    let client = client().await;
    let prefix = unique_prefix("cache");
    clean_tracks(&client, &prefix).await;
    let repository = TracksRepository::new(client.clone());
    let id = format!("{prefix}hit");
    repository
        .save_track(&track(&id, "Cache Hit"))
        .await
        .expect("save");

    let result = repository
        .find_cached_tracks(&[
            TrackKey::apple(id.clone()),
            TrackKey::apple(format!("{prefix}miss")),
            TrackKey::apple(String::new()),
        ])
        .await
        .expect("find cached tracks");
    assert_eq!(result.len(), 1);
    assert_eq!(result[&TrackKey::apple(id.clone())].title, "Cache Hit");
    assert!(
        repository
            .find_cached_tracks(&[])
            .await
            .expect("empty lookup")
            .is_empty()
    );
    assert!(
        repository
            .find_track_by_file_unique_id(&format!("unique-{id}"))
            .await
            .expect("file unique lookup")
            .is_some()
    );
    clean_tracks(&client, &prefix).await;
}

#[tokio::test]
async fn save_find_delete_search_and_prune_tracks() {
    let client = client().await;
    let prefix = unique_prefix("ops");
    clean_tracks(&client, &prefix).await;
    let repository = TracksRepository::new(client.clone());
    let first = format!("{prefix}first");
    let second = format!("{prefix}second");
    let unique_title = format!("{prefix} unique song");
    repository
        .save_track(&track(&first, &unique_title))
        .await
        .expect("save first");
    repository
        .save_track(&track(&second, "Another Song"))
        .await
        .expect("save second");
    assert_eq!(
        repository
            .search_cached_tracks(&prefix, 10)
            .await
            .expect("search")
            .len(),
        1
    );
    let ids = repository.get_all_track_ids().await.expect("all ids");
    assert!(
        ids.contains(&TrackKey::apple_codec(first.clone(), Codec::Alac))
            && ids.contains(&TrackKey::apple_codec(second.clone(), Codec::Alac))
    );
    // Global prune. This shared test
    // database can retain rows from interrupted earlier runs, so only assert
    // that our second row was pruned rather than an exact global count.
    assert!(
        repository
            .delete_tracks_not_in(std::slice::from_ref(&TrackKey::apple_codec(
                first.clone(),
                Codec::Alac,
            )))
            .await
            .expect("prune")
            >= 1
    );
    assert!(
        repository
            .delete_track(&TrackKey::apple_codec(first.clone(), Codec::Alac))
            .await
            .expect("delete hit")
    );
    assert!(
        !repository
            .delete_track(&TrackKey::apple_codec(first.clone(), Codec::Alac))
            .await
            .expect("delete miss")
    );
    clean_tracks(&client, &prefix).await;
}

#[tokio::test]
async fn request_log_insert() {
    let client = client().await;
    let repository = RequestLogRepository::new(client.clone());
    let id = format!("db-m5b-request-{}", std::process::id());
    execute(
        &client,
        &format!("DELETE FROM requests WHERE track_id = '{}'", id),
    )
    .await;
    repository
        .log_request(&RequestLog {
            telegram_id: 9_001,
            chat_id: -9_002,
            track_key: TrackKey::apple(id.clone()),
            is_cache_hit: false,
            duration_ms: Some(42),
            status: "ok".to_owned(),
            error_reason: None,
        })
        .await
        .expect("log request");
    #[derive(diesel::QueryableByName)]
    struct StatusRow {
        #[diesel(sql_type = Text)]
        status: String,
    }
    let mut connection = client.connection().await.expect("connection");
    let rows = sql_query("SELECT status FROM requests WHERE track_id = $1")
        .bind::<Text, _>(&id)
        .load::<StatusRow>(&mut *connection)
        .await
        .expect("request query");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].status, "ok");
    execute(
        &client,
        &format!("DELETE FROM requests WHERE track_id = '{}'", id),
    )
    .await;
}

#[tokio::test]
async fn settings_defaults_parsing_and_mutations() {
    let client = client().await;
    // Clean every key this test may write: the parsing probes below AND the
    // toggle mutations later in the test (which persist via set_setting).
    // Without this, a second run against the same DB reads the previous
    // run's toggled rows and fails.
    let store = SettingsStore::new(client.clone());
    store.init().await.expect("load settings");
    assert_eq!(store.get_settings().max_collection_tracks, 50);
    assert_eq!(store.get_settings().auto_dump_storefronts, vec!["us"]);

    assert_eq!(
        store
            .set_setting("ripping_mode", json!("not-a-mode"))
            .await
            .ripping_mode
            .as_str(),
        "live"
    );
    assert_eq!(
        store
            .set_setting("max_collection_tracks", json!(-1))
            .await
            .max_collection_tracks,
        50
    );
    assert_eq!(
        store
            .set_setting("auto_dump_storefronts", json!([]))
            .await
            .auto_dump_storefronts,
        vec!["us"]
    );
    assert!(
        !store
            .set_setting("album_rip_enabled", json!(false))
            .await
            .album_rip_enabled
    );

    assert!(store.toggle_album().await);
    assert!(!store.toggle_playlist().await);
    assert!(!store.toggle_artist().await);
    assert!(!store.toggle_txt().await);
    assert!(!store.toggle_multi_link_rip().await);
    assert_eq!(
        store
            .set_setting("max_collection_tracks", json!(12))
            .await
            .max_collection_tracks,
        12
    );
    assert_eq!(store.cycle_ripping_mode().await.as_str(), "cache_only");
    assert_eq!(store.cycle_ripping_mode().await.as_str(), "paused");
    assert_eq!(store.cycle_ripping_mode().await.as_str(), "live");
    assert_eq!(store.set_max_collection_tracks(-8).await, 0);
    assert_eq!(
        store.add_auto_dump_storefront(" CA ").await,
        vec!["us", "ca"]
    );
    assert_eq!(store.add_auto_dump_storefront("ca").await, vec!["us", "ca"]);
    assert_eq!(store.remove_auto_dump_storefront("US").await, vec!["ca"]);
    assert_eq!(store.remove_auto_dump_storefront("ca").await, vec!["us"]);
    assert_eq!(store.set_auto_dump_storefronts(&[]).await, vec!["us"]);

    store.set_setting("ripping_mode", json!("live")).await;
    store.set_setting("max_collection_tracks", json!(50)).await;
    store
        .set_setting("auto_dump_storefronts", json!(["us"]))
        .await;
    store.set_setting("album_rip_enabled", json!(true)).await;
    store.set_setting("playlist_rip_enabled", json!(true)).await;
    store.set_setting("artist_rip_enabled", json!(true)).await;
    store.set_setting("txt_rip_enabled", json!(true)).await;
    store
        .set_setting("multi_link_rip_enabled", json!(true))
        .await;
}

#[tokio::test]
async fn albums_repository_save_find_delete() {
    let client = client().await;
    let repo = AlbumsRepository::new(client.clone());
    let album_id = format!("test-alb-{}", std::process::id());

    let _ = repo.delete_albums(Provider::Apple, &album_id, None).await;

    let uid1 = format!("uniq1-{}", std::process::id());
    let uid2 = format!("uniq2-{}", std::process::id());

    let new_part1 = NewAlbum {
        provider: Provider::Apple,
        album_id: &album_id,
        codec: Codec::Alac,
        part_index: 1,
        total_parts: 2,
        message_id: 100,
        file_id: "file1",
        file_unique_id: &uid1,
        file_size: 5000,
        file_name: "album.part1.zip",
        generation_hash: "hash1",
    };
    let new_part2 = NewAlbum {
        provider: Provider::Apple,
        album_id: &album_id,
        codec: Codec::Alac,
        part_index: 2,
        total_parts: 2,
        message_id: 101,
        file_id: "file2",
        file_unique_id: &uid2,
        file_size: 6000,
        file_name: "album.part2.zip",
        generation_hash: "hash1",
    };

    repo.save_album(&new_part1).await.expect("save part 1");
    repo.save_album(&new_part2).await.expect("save part 2");

    let parts = repo
        .find_albums(Provider::Apple, &album_id, None)
        .await
        .expect("find albums");
    assert_eq!(parts.len(), 2);
    assert_eq!(parts[0].part_index, 1);
    assert_eq!(parts[1].part_index, 2);

    let deleted = repo
        .delete_albums(Provider::Apple, &album_id, None)
        .await
        .expect("delete");
    assert_eq!(deleted.len(), 2);

    let parts_after = repo
        .find_albums(Provider::Apple, &album_id, None)
        .await
        .expect("find after delete");
    assert_eq!(parts_after.len(), 0);
}

#[tokio::test]
async fn albums_repository_replacement_rolls_back_and_preserves_old_rows() {
    let client = client().await;
    let repo = AlbumsRepository::new(client.clone());
    let album_id = unique_prefix("replace");
    let old_uid = format!("{album_id}old");

    let old = NewAlbum {
        provider: Provider::Apple,
        album_id: &album_id,
        codec: Codec::Alac,
        part_index: 1,
        total_parts: 1,
        message_id: 100,
        file_id: "old-file",
        file_unique_id: &old_uid,
        file_size: 5_000,
        file_name: "old.zip",
        generation_hash: "old-generation",
    };
    repo.save_album(&old).await.expect("save old archive row");

    // The message-id check fails during the insert after the transaction has
    // deleted the old rendition. A rollback must make that delete invisible.
    let replacement = AlbumUpload {
        provider: Provider::Apple,
        album_id: album_id.clone(),
        codec: Codec::Alac,
        part_index: 1,
        total_parts: 1,
        message_id: 0,
        file_id: "new-file".into(),
        file_unique_id: format!("{album_id}new"),
        file_size: 6_000,
        file_name: "new.zip".into(),
        generation_hash: "new-generation".into(),
    };
    assert!(
        repo.replace_albums(
            Provider::Apple,
            &album_id,
            Codec::Alac,
            &AlbumReplacementExpectation::Generation("old-generation".into()),
            &[replacement],
        )
        .await
        .is_err(),
        "invalid replacement must fail"
    );

    let rows = repo
        .find_albums(Provider::Apple, &album_id, Some(Codec::Alac))
        .await
        .expect("find rolled-back archive row");
    assert_eq!(
        rows.len(),
        1,
        "the old row remains after replacement failure"
    );
    assert_eq!(rows[0].message_id, 100);
    assert_eq!(rows[0].file_unique_id, old_uid);
    assert_eq!(rows[0].generation_hash, "old-generation");
}

#[tokio::test]
async fn albums_repository_replacement_switches_primary_codec_and_keeps_atmos() {
    let client = client().await;
    let repo = AlbumsRepository::new(client.clone());
    let album_id = unique_prefix("codec-switch");
    let old_alac_uid = format!("{album_id}-old-alac");
    let old_atmos_uid = format!("{album_id}-old-atmos");

    repo.save_album(&NewAlbum {
        provider: Provider::Apple,
        album_id: &album_id,
        codec: Codec::Alac,
        part_index: 1,
        total_parts: 1,
        message_id: 201,
        file_id: "old-alac-file",
        file_unique_id: &old_alac_uid,
        file_size: 5_000,
        file_name: "old-alac.zip",
        generation_hash: "old-generation",
    })
    .await
    .expect("save ALAC row");
    repo.save_album(&NewAlbum {
        provider: Provider::Apple,
        album_id: &album_id,
        codec: Codec::Ec3,
        part_index: 1,
        total_parts: 1,
        message_id: 202,
        file_id: "old-atmos-file",
        file_unique_id: &old_atmos_uid,
        file_size: 5_000,
        file_name: "old-atmos.zip",
        generation_hash: "old-generation",
    })
    .await
    .expect("save Atmos row");

    let replacement = AlbumUpload {
        provider: Provider::Apple,
        album_id: album_id.clone(),
        codec: Codec::Aac,
        part_index: 1,
        total_parts: 1,
        message_id: 203,
        file_id: "new-aac-file".into(),
        file_unique_id: format!("{album_id}-new-aac"),
        file_size: 6_000,
        file_name: "new-aac.zip".into(),
        generation_hash: "new-generation".into(),
    };
    repo.replace_albums(
        Provider::Apple,
        &album_id,
        Codec::Aac,
        &AlbumReplacementExpectation::Generation("old-generation".into()),
        &[replacement],
    )
    .await
    .expect("replace primary codec");

    let rows = repo
        .find_albums(Provider::Apple, &album_id, None)
        .await
        .expect("find switched rows");
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().any(|row| row.codec == Codec::Aac));
    assert!(rows.iter().any(|row| row.codec == Codec::Ec3));
    assert!(!rows.iter().any(|row| row.codec == Codec::Alac));
}

#[tokio::test]
async fn albums_repository_concurrent_replacement_commits_one_generation() {
    let client = client().await;
    let repo = AlbumsRepository::new(client.clone());
    let album_id = unique_prefix("concurrent-replace");
    let old_uid = format!("{album_id}-old");
    repo.save_album(&NewAlbum {
        provider: Provider::Apple,
        album_id: &album_id,
        codec: Codec::Alac,
        part_index: 1,
        total_parts: 1,
        message_id: 300,
        file_id: "old-file",
        file_unique_id: &old_uid,
        file_size: 5_000,
        file_name: "old.zip",
        generation_hash: "base-generation",
    })
    .await
    .expect("save old archive row");

    let barrier = Arc::new(tokio::sync::Barrier::new(3));
    let first_repo = repo.clone();
    let first_barrier = Arc::clone(&barrier);
    let first_album = album_id.clone();
    let first = tokio::spawn(async move {
        let upload = AlbumUpload {
            provider: Provider::Apple,
            album_id: first_album.clone(),
            codec: Codec::Alac,
            part_index: 1,
            total_parts: 1,
            message_id: 301,
            file_id: "first-file".into(),
            file_unique_id: format!("{first_album}-first"),
            file_size: 6_000,
            file_name: "first.zip".into(),
            generation_hash: "first-generation".into(),
        };
        first_barrier.wait().await;
        first_repo
            .replace_albums(
                Provider::Apple,
                &first_album,
                Codec::Alac,
                &AlbumReplacementExpectation::Generation("base-generation".into()),
                &[upload],
            )
            .await
            .expect("first replacement transaction")
    });

    let second_repo = repo.clone();
    let second_barrier = Arc::clone(&barrier);
    let second_album = album_id.clone();
    let second = tokio::spawn(async move {
        let upload = AlbumUpload {
            provider: Provider::Apple,
            album_id: second_album.clone(),
            codec: Codec::Alac,
            part_index: 1,
            total_parts: 1,
            message_id: 302,
            file_id: "second-file".into(),
            file_unique_id: format!("{second_album}-second"),
            file_size: 6_000,
            file_name: "second.zip".into(),
            generation_hash: "second-generation".into(),
        };
        second_barrier.wait().await;
        second_repo
            .replace_albums(
                Provider::Apple,
                &second_album,
                Codec::Alac,
                &AlbumReplacementExpectation::Generation("base-generation".into()),
                &[upload],
            )
            .await
            .expect("second replacement transaction")
    });

    barrier.wait().await;
    let (first, second) = tokio::join!(first, second);
    let first = first.expect("first task");
    let second = second.expect("second task");
    let outcomes = [first, second];
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, AlbumReplacementResult::Committed { .. }))
            .count(),
        1,
        "one compare-and-replace wins and the other is stale"
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, AlbumReplacementResult::Stale))
            .count(),
        1
    );
    let committed = outcomes
        .iter()
        .find_map(|outcome| match outcome {
            AlbumReplacementResult::Committed {
                displaced_message_ids,
            } => Some(displaced_message_ids),
            AlbumReplacementResult::Stale => None,
        })
        .expect("committed outcome");
    assert_eq!(committed, &[300]);

    let rows = repo
        .find_albums(Provider::Apple, &album_id, Some(Codec::Alac))
        .await
        .expect("find winning row");
    assert_eq!(rows.len(), 1);
    assert!(matches!(rows[0].message_id, 301 | 302));
    assert!(matches!(
        rows[0].generation_hash.as_str(),
        "first-generation" | "second-generation"
    ));
}
