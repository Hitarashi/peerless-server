use chrono::{DateTime, Utc};
use diesel::prelude::*;
use music::Codec;

use crate::schema::{
    albums, one_time_auth_codes, settings, tg_worker_sessions, tracks, user_sessions, users,
};

#[derive(Debug, Clone, Default, Queryable, Selectable)]
#[diesel(table_name = users)]
pub struct User {
    pub telegram_id: i64,
    pub name: Option<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Queryable, Selectable, QueryableByName)]
#[diesel(table_name = tracks)]
pub struct Track {
    #[diesel(sql_type = diesel::sql_types::Integer)]
    pub id: i32,
    #[diesel(sql_type = diesel::sql_types::Text)]
    pub track_id: String,
    #[diesel(sql_type = diesel::sql_types::VarChar)]
    pub codec: Codec,
    #[diesel(sql_type = diesel::sql_types::Integer)]
    pub message_id: i32,
    #[diesel(sql_type = diesel::sql_types::Text)]
    pub file_id: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    pub file_unique_id: String,
    #[diesel(sql_type = diesel::sql_types::Timestamptz)]
    pub created_at: DateTime<Utc>,
    #[diesel(sql_type = diesel::sql_types::Timestamptz)]
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Queryable, Selectable)]
#[diesel(table_name = settings)]
pub struct SettingsRow {
    pub id: i16,
    pub data: serde_json::Value,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Queryable, Selectable)]
#[diesel(table_name = albums)]
pub struct Album {
    pub id: i32,
    pub album_id: String,
    pub codec: Codec,
    pub part_index: i32,
    pub total_parts: i32,
    pub message_id: i32,
    pub file_id: String,
    pub file_unique_id: String,
    pub file_size: i64,
    pub generation_hash: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Insertable)]
#[diesel(table_name = users)]
pub struct NewUser<'a> {
    pub telegram_id: i64,
    pub name: Option<&'a str>,
}

#[derive(Debug, Clone, Insertable)]
#[diesel(table_name = tracks)]
pub struct NewTrack<'a> {
    pub track_id: &'a str,
    pub codec: Codec,
    pub message_id: i32,
    pub file_id: &'a str,
    pub file_unique_id: &'a str,
}

#[derive(Debug, Clone, Insertable)]
#[diesel(table_name = albums)]
pub struct NewAlbum<'a> {
    pub album_id: &'a str,
    pub codec: Codec,
    pub part_index: i32,
    pub total_parts: i32,
    pub message_id: i32,
    pub file_id: &'a str,
    pub file_unique_id: &'a str,
    pub file_size: i64,
    pub generation_hash: &'a str,
}

#[derive(Debug, Clone, Queryable, Selectable)]
#[diesel(table_name = user_sessions)]
pub struct UserSession {
    pub id: String,
    pub telegram_id: i64,
    pub refresh_token_hash: String,
    pub client_name: Option<String>,
    pub client_version: Option<String>,
    pub device_name: Option<String>,
    pub platform: Option<String>,
    pub created_at: DateTime<Utc>,
    pub last_active_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub revoked: bool,
}

#[derive(Debug, Clone, Insertable)]
#[diesel(table_name = user_sessions)]
pub struct NewUserSession<'a> {
    pub id: &'a str,
    pub telegram_id: i64,
    pub refresh_token_hash: &'a str,
    pub client_name: Option<&'a str>,
    pub client_version: Option<&'a str>,
    pub device_name: Option<&'a str>,
    pub platform: Option<&'a str>,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Queryable, Selectable)]
#[diesel(table_name = one_time_auth_codes)]
pub struct OneTimeAuthCode {
    pub code: String,
    pub telegram_id: i64,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Insertable)]
#[diesel(table_name = one_time_auth_codes)]
pub struct NewOneTimeAuthCode<'a> {
    pub code: &'a str,
    pub telegram_id: i64,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Queryable, Selectable)]
#[diesel(table_name = tg_worker_sessions)]
pub struct TgWorkerSession {
    pub bot_token_hash: String,
    pub session_data: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Insertable, AsChangeset)]
#[diesel(table_name = tg_worker_sessions)]
pub struct NewTgWorkerSession<'a> {
    pub bot_token_hash: &'a str,
    pub session_data: &'a str,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Queryable, Selectable, Identifiable)]
#[diesel(table_name = crate::schema::user_integrations)]
#[diesel(primary_key(telegram_id, provider))]
pub struct UserIntegration {
    pub telegram_id: i64,
    pub provider: String,
    pub username: String,
    pub encrypted_session_key: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Insertable, AsChangeset)]
#[diesel(table_name = crate::schema::user_integrations)]
pub struct NewUserIntegration<'a> {
    pub telegram_id: i64,
    pub provider: &'a str,
    pub username: &'a str,
    pub encrypted_session_key: &'a str,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}
