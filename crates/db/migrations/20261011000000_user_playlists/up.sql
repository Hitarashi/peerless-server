DELETE FROM user_integrations WHERE provider = 'listenbrainz';

CREATE TABLE user_playlists (
    id TEXT PRIMARY KEY,
    telegram_id BIGINT NOT NULL REFERENCES users(telegram_id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    is_favorites BOOLEAN NOT NULL DEFAULT FALSE,
    source_url TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT user_playlists_name_check CHECK (length(trim(name)) BETWEEN 1 AND 80)
);
CREATE INDEX user_playlists_owner_updated_idx ON user_playlists (telegram_id, updated_at DESC);
CREATE UNIQUE INDEX user_playlists_one_favorites_idx ON user_playlists (telegram_id) WHERE is_favorites;
CREATE UNIQUE INDEX user_playlists_source_url_idx ON user_playlists (telegram_id, source_url) WHERE source_url IS NOT NULL;

CREATE TABLE user_playlist_tracks (
    playlist_id TEXT NOT NULL REFERENCES user_playlists(id) ON DELETE CASCADE,
    position INTEGER NOT NULL,
    track_id TEXT NOT NULL,
    PRIMARY KEY (playlist_id, position),
    CONSTRAINT user_playlist_tracks_position_check CHECK (position >= 0),
    CONSTRAINT user_playlist_tracks_apple_id_check CHECK (track_id ~ '^[0-9]+$')
);
