use db::{Auth, LibraryManager, SaveTrackInput, TracksRepository, connect_test_isolated, migrate};
use music::{Codec, Provider, TrackKey};

fn test_track(id: &str, title: &str) -> SaveTrackInput {
    SaveTrackInput {
        track_key: TrackKey::new(Provider::Apple, id).with_codec(Codec::Alac),
        codec: Codec::Alac,
        message_id: 100,
        file_id: format!("file-{id}"),
        file_unique_id: format!("unique-{id}"),
        title: title.to_owned(),
        artist: "Test Artist".to_owned(),
        album: "Test Album".to_owned(),
        duration: 210,
        bit_depth: 24,
        sample_rate: 48_000,
        genre: "Pop".to_owned(),
        release_date: "2026".to_owned(),
        track_number: 1,
        track_count: 10,
        isrc: None,
    }
}

#[tokio::test]
async fn favorites_and_playlists_lifecycle() {
    let client = match connect_test_isolated().await {
        Ok(c) => c,
        Err(_) => {
            eprintln!("Skipping library test: TEST_DATABASE_URL not set");
            return;
        }
    };
    migrate(&client).await.expect("database migrations");

    let auth = Auth::new(client.clone(), 999_000_101);
    let tracks_repo = TracksRepository::new(client.clone());
    let library_mgr = LibraryManager::new(client.clone());

    let user_a = 999_000_102;
    let user_b = 999_000_103;
    auth.authorize(user_a, Some("User A"))
        .await
        .expect("auth user A");
    auth.authorize(user_b, Some("User B"))
        .await
        .expect("auth user B");

    // Seed two tracks
    let t1 = tracks_repo
        .save_track(test_track("lib_trk_1", "Song One"))
        .await
        .expect("seed track 1");
    let t2 = tracks_repo
        .save_track(test_track("lib_trk_2", "Song Two"))
        .await
        .expect("seed track 2");

    // --- Favorites ---
    // 1. Toggle favorite on track 1 -> returns true (added)
    let added = library_mgr
        .toggle_favorite(user_a, t1.id)
        .await
        .expect("add favorite");
    assert!(added);
    assert!(
        library_mgr
            .is_favorite(user_a, t1.id)
            .await
            .expect("is favorite")
    );
    assert!(
        !library_mgr
            .is_favorite(user_b, t1.id)
            .await
            .expect("user B does not have favorite")
    );

    let favs = library_mgr
        .list_favorites(user_a, 0, 10)
        .await
        .expect("list favorites");
    assert_eq!(favs.len(), 1);
    assert_eq!(favs[0].id, t1.id);

    // 2. Toggle again -> returns false (removed)
    let removed = library_mgr
        .toggle_favorite(user_a, t1.id)
        .await
        .expect("remove favorite");
    assert!(!removed);
    assert!(
        !library_mgr
            .is_favorite(user_a, t1.id)
            .await
            .expect("is favorite after removal")
    );

    // 2b. Explicit favorite methods (add_favorite, remove_favorite, list_favorite_ids)
    let explicit_added = library_mgr
        .add_favorite(user_a, t1.id)
        .await
        .expect("explicit add favorite");
    assert!(explicit_added, "Newly added favorite must return true");

    let explicit_duplicate = library_mgr
        .add_favorite(user_a, t1.id)
        .await
        .expect("explicit duplicate add");
    assert!(!explicit_duplicate, "Duplicate favorite must return false");

    let explicit_added_2 = library_mgr
        .add_favorite(user_a, t2.id)
        .await
        .expect("explicit add favorite 2");
    assert!(
        explicit_added_2,
        "Newly added second favorite must return true"
    );

    let fav_ids = library_mgr
        .list_favorite_ids(user_a)
        .await
        .expect("list favorite ids");
    assert_eq!(
        fav_ids,
        vec![t2.id, t1.id],
        "Favorite IDs should be ordered by created_at DESC"
    );

    let explicit_removed = library_mgr
        .remove_favorite(user_a, t1.id)
        .await
        .expect("explicit remove favorite");
    assert!(explicit_removed, "Removed favorite must return true");

    let explicit_remove_absent = library_mgr
        .remove_favorite(user_a, t1.id)
        .await
        .expect("explicit remove absent");
    assert!(
        !explicit_remove_absent,
        "Removing absent favorite must return false"
    );

    let fav_ids_post_remove = library_mgr
        .list_favorite_ids(user_a)
        .await
        .expect("list favorite ids post remove");
    assert_eq!(fav_ids_post_remove, vec![t2.id]);

    // Clean up t2
    library_mgr
        .remove_favorite(user_a, t2.id)
        .await
        .expect("cleanup t2");

    // --- Playlists ---
    // 3. Create playlist for user A
    let pl = library_mgr
        .create_playlist(user_a, "Night Vibes")
        .await
        .expect("create playlist");
    assert_eq!(pl.name, "Night Vibes");
    assert_eq!(pl.telegram_id, user_a);

    // 4. Add tracks
    let inserted = library_mgr
        .add_tracks_to_playlist(user_a, pl.id, &[t1.id, t2.id])
        .await
        .expect("add tracks to playlist");
    assert_eq!(inserted, 2);

    // 5. Get playlist details
    let details = library_mgr
        .get_playlist(user_a, pl.id)
        .await
        .expect("get playlist");
    assert_eq!(details.tracks.len(), 2);
    assert_eq!(details.tracks[0].id, t1.id);
    assert_eq!(details.tracks[1].id, t2.id);

    // 6. Cross-user isolation barrier: User B cannot get User A's playlist
    let breach = library_mgr.get_playlist(user_b, pl.id).await;
    assert!(
        breach.is_err(),
        "User B must not be able to read User A's playlist"
    );

    // 7. Reorder tracks: reverse order [t2, t1]
    library_mgr
        .reorder_playlist(user_a, pl.id, &[t2.id, t1.id])
        .await
        .expect("reorder playlist");

    let reordered = library_mgr
        .get_playlist(user_a, pl.id)
        .await
        .expect("get reordered playlist");
    assert_eq!(reordered.tracks[0].id, t2.id);
    assert_eq!(reordered.tracks[1].id, t1.id);

    // 8. Delete playlist
    let deleted = library_mgr
        .delete_playlist(user_a, pl.id)
        .await
        .expect("delete playlist");
    assert!(deleted);

    let post_delete = library_mgr.get_playlist(user_a, pl.id).await;
    assert!(post_delete.is_err(), "Playlist must be gone after deletion");
}
