diesel::table! {
    users (telegram_id) {
        telegram_id -> BigInt,
        name -> Nullable<Text>,
        created_at -> Timestamptz,
    }
}

diesel::table! {
    tracks (id) {
        id -> Integer,
        track_id -> Text,
        codec -> Varchar,
        message_id -> Integer,
        file_id -> Text,
        file_unique_id -> Text,
        created_at -> Timestamptz,
        updated_at -> Timestamptz,
    }
}

diesel::table! {
    settings (id) {
        id -> SmallInt,
        data -> Jsonb,
        updated_at -> Timestamptz,
    }
}

diesel::table! {
    albums (id) {
        id -> Integer,
        album_id -> Text,
        codec -> Varchar,
        part_index -> Integer,
        total_parts -> Integer,
        message_id -> Integer,
        file_id -> Text,
        file_unique_id -> Text,
        file_size -> BigInt,
        generation_hash -> Varchar,
        created_at -> Timestamptz,
    }
}

diesel::table! {
    user_sessions (id) {
        id -> Text,
        telegram_id -> BigInt,
        refresh_token_hash -> Text,
        client_name -> Nullable<Text>,
        client_version -> Nullable<Text>,
        device_name -> Nullable<Text>,
        platform -> Nullable<Text>,
        created_at -> Timestamptz,
        last_active_at -> Timestamptz,
        expires_at -> Timestamptz,
        revoked -> Bool,
    }
}

diesel::table! {
    one_time_auth_codes (code) {
        code -> Varchar,
        telegram_id -> BigInt,
        created_at -> Timestamptz,
        expires_at -> Timestamptz,
    }
}

diesel::table! {
    tg_worker_sessions (bot_token_hash) {
        bot_token_hash -> Varchar,
        session_data -> Text,
        created_at -> Timestamptz,
        updated_at -> Timestamptz,
    }
}

diesel::table! {
    user_integrations (telegram_id, provider) {
        telegram_id -> BigInt,
        provider -> Varchar,
        username -> Text,
        encrypted_session_key -> Text,
        created_at -> Timestamptz,
        updated_at -> Timestamptz,
    }
}

diesel::allow_tables_to_appear_in_same_query!(
    users,
    tracks,
    settings,
    albums,
    user_sessions,
    one_time_auth_codes,
    tg_worker_sessions,
    user_integrations
);
