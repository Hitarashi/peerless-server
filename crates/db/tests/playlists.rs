use db::{connect_test_isolated, get_playlist, import_playlist, list_playlists, migrate};
use diesel::{
    sql_query,
    sql_types::{BigInt, Text},
};
use diesel_async::RunQueryDsl;

#[tokio::test]
async fn import_playlist_persists_and_replaces_tracks_for_same_source() {
    let pool = connect_test_isolated()
        .await
        .expect("TEST_DATABASE_URL and PostgreSQL are required for database tests");
    migrate(&pool).await.expect("database migrations");

    let telegram_id = 701_010_001;
    let mut connection = pool.connection().await.expect("database connection");
    sql_query("INSERT INTO users (telegram_id, name) VALUES ($1, $2)")
        .bind::<BigInt, _>(telegram_id)
        .bind::<Text, _>("playlist import regression")
        .execute(&mut *connection)
        .await
        .expect("insert playlist owner");
    drop(connection);

    let source_url = "https://music.youtube.com/playlist?list=PLtest";
    let first_import = import_playlist(
        &pool,
        telegram_id,
        "Imported mix",
        source_url,
        &["1559885421".into(), "1440643709".into()],
    )
    .await
    .expect("first import");
    assert_eq!(first_import.track_count, 2);
    assert_eq!(first_import.preview_track_ids, ["1559885421", "1440643709"]);

    let second_import = import_playlist(
        &pool,
        telegram_id,
        "Updated mix",
        source_url,
        &["1559885421".into()],
    )
    .await
    .expect("reimport same source");
    assert_eq!(second_import.id, first_import.id);
    assert_eq!(second_import.name, "Updated mix");
    assert_eq!(second_import.track_count, 1);
    assert_eq!(second_import.preview_track_ids, ["1559885421"]);

    let detail = get_playlist(&pool, telegram_id, &second_import.id)
        .await
        .expect("read imported playlist");
    assert_eq!(detail.track_ids, ["1559885421"]);

    let playlists = list_playlists(&pool, telegram_id)
        .await
        .expect("list user playlists");
    assert_eq!(playlists.len(), 1);
    assert_eq!(playlists[0].id, second_import.id);
    assert!(!playlists[0].is_favorites);
}
