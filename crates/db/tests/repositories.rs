use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use db::{
    AlbumsRepository, DbPool, NewAlbum, SettingsStore, TracksRepository, connect_test_isolated,
    migrate,
};
use diesel::sql_query;
use diesel_async::RunQueryDsl;
use music::{Codec, Provider};
use peerless_core::{
    AlbumReplacementExpectation, AlbumReplacementResult, AlbumUpload, SaveTrackInput,
};
use serde_json::json;

async fn client() -> DbPool {
    let client = connect_test_isolated()
        .await
        .expect("TEST_DATABASE_URL and PostgreSQL are required for db tests");
    migrate(&client).await.expect("database migrations");
    client
}

fn track(id: &str) -> SaveTrackInput {
    SaveTrackInput {
        track_id: id.to_string(),
        codec: Codec::Alac,
        message_id: 123,
        file_id: format!("file-{id}"),
        file_unique_id: format!("unique-{id}"),
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
    repository.save_track(&track(&id)).await.expect("save");

    let result = repository
        .find_cached_tracks(&[id.clone(), format!("{prefix}miss"), String::new()])
        .await
        .expect("find cached tracks");
    assert_eq!(result.len(), 1);
    assert_eq!(result[&(id.clone(), Codec::Alac)].message_id, 123);
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
async fn save_find_delete_and_prune_tracks() {
    let client = client().await;
    let prefix = unique_prefix("ops");
    clean_tracks(&client, &prefix).await;
    let repository = TracksRepository::new(client.clone());
    let first = format!("{prefix}first");
    let second = format!("{prefix}second");
    repository
        .save_track(&track(&first))
        .await
        .expect("save first");
    repository
        .save_track(&track(&second))
        .await
        .expect("save second");
    let ids = repository.get_all_track_ids().await.expect("all ids");
    assert!(
        ids.contains(&(first.clone(), Codec::Alac)) && ids.contains(&(second.clone(), Codec::Alac))
    );

    assert!(
        repository
            .delete_tracks_not_in(std::slice::from_ref(&(first.clone(), Codec::Alac,)))
            .await
            .expect("prune")
            >= 1
    );
    assert!(
        repository
            .delete_track(&first, Some(Codec::Alac))
            .await
            .expect("delete hit")
    );
    assert!(
        !repository
            .delete_track(&first, Some(Codec::Alac))
            .await
            .expect("delete miss")
    );
    clean_tracks(&client, &prefix).await;
}

#[tokio::test]
async fn settings_defaults_parsing_and_mutations() {
    let client = client().await;

    let store = SettingsStore::new(client.clone());
    store.init().await.expect("load settings");
    assert_eq!(store.get_settings().max_collection_tracks, 50);

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
    store.set_setting("ripping_mode", json!("live")).await;
    store.set_setting("max_collection_tracks", json!(50)).await;
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

    let _ = repo.delete_albums(&album_id, None).await;

    let uid1 = format!("uniq1-{}", std::process::id());
    let uid2 = format!("uniq2-{}", std::process::id());

    let new_part1 = NewAlbum {
        album_id: &album_id,
        codec: Codec::Alac,
        part_index: 1,
        total_parts: 2,
        message_id: 100,
        file_id: "file1",
        file_unique_id: &uid1,
        file_size: 5000,
        generation_hash: "hash1",
    };
    let new_part2 = NewAlbum {
        album_id: &album_id,
        codec: Codec::Alac,
        part_index: 2,
        total_parts: 2,
        message_id: 101,
        file_id: "file2",
        file_unique_id: &uid2,
        file_size: 6000,
        generation_hash: "hash1",
    };

    repo.save_album(&new_part1).await.expect("save part 1");
    repo.save_album(&new_part2).await.expect("save part 2");

    let parts = repo
        .find_albums(&album_id, None)
        .await
        .expect("find albums");
    assert_eq!(parts.len(), 2);
    assert_eq!(parts[0].part_index, 1);
    assert_eq!(parts[1].part_index, 2);

    let deleted = repo.delete_albums(&album_id, None).await.expect("delete");
    assert_eq!(deleted.len(), 2);

    let parts_after = repo
        .find_albums(&album_id, None)
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
        album_id: &album_id,
        codec: Codec::Alac,
        part_index: 1,
        total_parts: 1,
        message_id: 100,
        file_id: "old-file",
        file_unique_id: &old_uid,
        file_size: 5_000,
        generation_hash: "old-generation",
    };
    repo.save_album(&old).await.expect("save old archive row");

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
        generation_hash: "new-generation".into(),
    };
    assert!(
        repo.replace_albums(
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
        .find_albums(&album_id, Some(Codec::Alac))
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
        album_id: &album_id,
        codec: Codec::Alac,
        part_index: 1,
        total_parts: 1,
        message_id: 201,
        file_id: "old-alac-file",
        file_unique_id: &old_alac_uid,
        file_size: 5_000,
        generation_hash: "old-generation",
    })
    .await
    .expect("save ALAC row");
    repo.save_album(&NewAlbum {
        album_id: &album_id,
        codec: Codec::Ec3,
        part_index: 1,
        total_parts: 1,
        message_id: 202,
        file_id: "old-atmos-file",
        file_unique_id: &old_atmos_uid,
        file_size: 5_000,
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
        generation_hash: "new-generation".into(),
    };
    repo.replace_albums(
        &album_id,
        Codec::Aac,
        &AlbumReplacementExpectation::Generation("old-generation".into()),
        &[replacement],
    )
    .await
    .expect("replace primary codec");

    let rows = repo
        .find_albums(&album_id, None)
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
        album_id: &album_id,
        codec: Codec::Alac,
        part_index: 1,
        total_parts: 1,
        message_id: 300,
        file_id: "old-file",
        file_unique_id: &old_uid,
        file_size: 5_000,
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
            generation_hash: "first-generation".into(),
        };
        first_barrier.wait().await;
        first_repo
            .replace_albums(
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
            generation_hash: "second-generation".into(),
        };
        second_barrier.wait().await;
        second_repo
            .replace_albums(
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
        .find_albums(&album_id, Some(Codec::Alac))
        .await
        .expect("find winning row");
    assert_eq!(rows.len(), 1);
    assert!(matches!(rows[0].message_id, 301 | 302));
    assert!(matches!(
        rows[0].generation_hash.as_str(),
        "first-generation" | "second-generation"
    ));
}
