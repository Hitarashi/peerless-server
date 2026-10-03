use db::{DbDumpService, TracksRepository, connect_test_isolated, migrate};
use diesel::{
    QueryableByName, sql_query,
    sql_types::{Text, VarChar},
};
use diesel_async::RunQueryDsl;

#[derive(Debug, QueryableByName)]
struct StoredTrack {
    #[diesel(sql_type = VarChar)]
    provider: String,
    #[diesel(sql_type = Text)]
    track_id: String,
    #[diesel(sql_type = VarChar)]
    codec: String,
}

#[derive(Debug, QueryableByName)]
struct StoredAlbum {
    #[diesel(sql_type = VarChar)]
    provider: String,
    #[diesel(sql_type = Text)]
    album_id: String,
    #[diesel(sql_type = VarChar)]
    codec: String,
}

#[tokio::test]
async fn archive_roundtrips_legacy_provider_rows_and_codecs() {
    let pool = connect_test_isolated()
        .await
        .expect("TEST_DATABASE_URL and PostgreSQL are required for db tests");
    migrate(&pool).await.expect("database migrations");

    let mut connection = pool.connection().await.expect("connection");
    sql_query(
        "INSERT INTO tracks (provider, track_id, codec, message_id, file_id, file_unique_id, title, artist, album, duration, bit_depth, sample_rate, genre, release_date, track_number, track_count) VALUES ($1, $2, $3, 101, 'legacy-track-file', 'legacy-track-unique', 'Legacy Track', 'Legacy Artist', 'Legacy Album', 180, 24, 96000, 'Music', '2026-01-01', 1, 1)",
    )
    .bind::<VarChar, _>("qobuz")
    .bind::<Text, _>("legacy-track")
    .bind::<VarChar, _>("flac")
    .execute(&mut *connection)
    .await
    .expect("insert legacy track");
    sql_query(
        "INSERT INTO albums (provider, album_id, codec, part_index, total_parts, message_id, file_id, file_unique_id, file_size, file_name, generation_hash) VALUES ($1, $2, $3, 1, 1, 102, 'legacy-album-file', 'legacy-album-unique', 4096, 'legacy.zip', 'legacy-generation')",
    )
    .bind::<VarChar, _>("qobuz")
    .bind::<Text, _>("legacy-album")
    .bind::<VarChar, _>("flac")
    .execute(&mut *connection)
    .await
    .expect("insert legacy album");
    sql_query(
        "INSERT INTO requests (telegram_id, chat_id, provider, track_id, is_cache_hit, status) VALUES (700001, 700002, $1, $2, true, 'completed')",
    )
    .bind::<VarChar, _>("qobuz")
    .bind::<Text, _>("legacy-track")
    .execute(&mut *connection)
    .await
    .expect("insert legacy request");
    drop(connection);

    let archive_service = DbDumpService::new(pool.clone());
    let (archive, _, _) = archive_service.export_dump().await.expect("export archive");
    archive_service
        .import_dump(&archive)
        .await
        .expect("restore archive");

    let mut connection = pool.connection().await.expect("connection");
    let track =
        sql_query("SELECT provider, track_id, codec FROM tracks WHERE track_id = 'legacy-track'")
            .get_result::<StoredTrack>(&mut *connection)
            .await
            .expect("restored legacy track");
    assert_eq!(track.provider, "qobuz");
    assert_eq!(track.track_id, "legacy-track");
    assert_eq!(track.codec, "flac");

    let album =
        sql_query("SELECT provider, album_id, codec FROM albums WHERE album_id = 'legacy-album'")
            .get_result::<StoredAlbum>(&mut *connection)
            .await
            .expect("restored legacy album");
    assert_eq!(album.provider, "qobuz");
    assert_eq!(album.album_id, "legacy-album");
    assert_eq!(album.codec, "flac");
    drop(connection);

    assert!(
        TracksRepository::new(pool)
            .search_cached_tracks("Legacy Track", 10)
            .await
            .expect("Apple-only cache search")
            .is_empty()
    );
}
