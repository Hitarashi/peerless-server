CREATE TABLE users (
    telegram_id BIGINT PRIMARY KEY,
    name TEXT,
    created_at TIMESTAMPTZ DEFAULT now() NOT NULL
);

CREATE TABLE tracks (
    id SERIAL PRIMARY KEY,
    track_id TEXT NOT NULL,
    codec VARCHAR(16) NOT NULL DEFAULT 'alac',
    message_id INTEGER NOT NULL,
    file_id TEXT NOT NULL,
    file_unique_id TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now() NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT now() NOT NULL,
    CONSTRAINT tracks_codec_check CHECK (codec IN ('alac', 'ec-3', 'aac')),
    CONSTRAINT tracks_track_id_codec_unique UNIQUE (track_id, codec)
);
CREATE INDEX tracks_codec_idx ON tracks (codec);
CREATE UNIQUE INDEX tracks_file_unique_id_idx ON tracks (file_unique_id);

CREATE TABLE settings (
    id SMALLINT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    data JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at TIMESTAMPTZ DEFAULT now() NOT NULL
);
INSERT INTO settings (id, data) VALUES (1, '{}'::jsonb);

CREATE TABLE albums (
    id SERIAL PRIMARY KEY,
    album_id TEXT NOT NULL,
    codec VARCHAR(16) NOT NULL DEFAULT 'alac',
    part_index INTEGER NOT NULL,
    total_parts INTEGER NOT NULL,
    message_id INTEGER NOT NULL,
    file_id TEXT NOT NULL,
    file_unique_id TEXT NOT NULL,
    file_size BIGINT NOT NULL,
    generation_hash VARCHAR NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_albums_album_codec_part UNIQUE (album_id, codec, part_index),
    CONSTRAINT albums_codec_check CHECK (codec IN ('alac', 'ec-3', 'aac')),
    CONSTRAINT ck_albums_part_index_positive CHECK (part_index > 0),
    CONSTRAINT ck_albums_total_parts_positive CHECK (total_parts > 0),
    CONSTRAINT ck_albums_part_index_within_total CHECK (part_index <= total_parts),
    CONSTRAINT ck_albums_message_id_positive CHECK (message_id > 0),
    CONSTRAINT ck_albums_file_size_positive CHECK (file_size > 0)
);
CREATE INDEX idx_albums_album ON albums (album_id);
CREATE INDEX idx_albums_codec ON albums (codec);
CREATE UNIQUE INDEX idx_albums_file_unique_id ON albums (file_unique_id);

CREATE TABLE user_sessions (
    id TEXT PRIMARY KEY,
    telegram_id BIGINT NOT NULL REFERENCES users(telegram_id) ON DELETE CASCADE,
    refresh_token_hash TEXT NOT NULL UNIQUE,
    client_name TEXT,
    client_version TEXT,
    device_name TEXT,
    platform TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_active_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL,
    revoked BOOLEAN NOT NULL DEFAULT FALSE
);
CREATE INDEX idx_user_sessions_telegram_id ON user_sessions(telegram_id);
CREATE INDEX idx_user_sessions_refresh_token_hash ON user_sessions(refresh_token_hash);

CREATE TABLE one_time_auth_codes (
    code VARCHAR(32) PRIMARY KEY,
    telegram_id BIGINT NOT NULL REFERENCES users(telegram_id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX idx_one_time_auth_codes_telegram_id ON one_time_auth_codes(telegram_id);

CREATE TABLE tg_worker_sessions (
    bot_token_hash VARCHAR(64) PRIMARY KEY,
    session_data TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE user_integrations (
    telegram_id BIGINT NOT NULL REFERENCES users(telegram_id) ON DELETE CASCADE,
    provider VARCHAR NOT NULL,
    username TEXT NOT NULL,
    encrypted_session_key TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (telegram_id, provider)
);
