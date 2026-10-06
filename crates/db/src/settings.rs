use std::sync::RwLock;

use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use engine::settings::{
    BotSettings, LyricspornApiEndpoint, RippingMode, default_settings, normalize_lyricsporn_api_url,
};
use serde_json::{Value, json};

use crate::{DbError, DbPool, models::SettingsRow, schema::settings};

pub struct SettingsStore {
    pool: DbPool,
    cached_settings: RwLock<BotSettings>,
    lyricsporn_api_endpoint: LyricspornApiEndpoint,
}

impl SettingsStore {
    pub fn new(pool: DbPool) -> Self {
        Self {
            pool,
            cached_settings: RwLock::new(default_settings()),
            lyricsporn_api_endpoint: LyricspornApiEndpoint::default(),
        }
    }

    pub async fn init(&self) -> Result<(), DbError> {
        self.reload().await
    }

    pub async fn reload(&self) -> Result<(), DbError> {
        let result = self.load().await?;
        self.lyricsporn_api_endpoint
            .set(result.lyricsporn_api_url.as_deref());
        *self
            .cached_settings
            .write()
            .expect("settings lock poisoned") = result;
        Ok(())
    }

    async fn load(&self) -> Result<BotSettings, DbError> {
        let mut connection = self.pool.connection().await?;
        let row = settings::table
            .filter(settings::id.eq(1_i16))
            .select(SettingsRow::as_select())
            .first::<SettingsRow>(&mut *connection)
            .await?;
        Ok(from_row(row))
    }

    pub fn get_settings(&self) -> BotSettings {
        self.cached_settings
            .read()
            .expect("settings lock poisoned")
            .clone()
    }

    pub fn lyricsporn_api_endpoint(&self) -> LyricspornApiEndpoint {
        self.lyricsporn_api_endpoint.clone()
    }

    pub async fn set_setting(&self, key: &str, value: Value) -> BotSettings {
        let mut next = self.get_settings();
        let canonical = canonical_key(key);
        let applied = match canonical {
            Some(k) => {
                next.extra.remove(key);
                next.extra.remove(k);
                apply_value(&mut next, k, &value)
            }
            None => {
                next.extra.insert(key.to_owned(), value);
                true
            }
        };
        if !applied {
            return self.get_settings();
        }
        if let Err(error) = self.persist(&next).await {
            tracing::error!(%error, setting = key, "failed to persist setting");
            return self.get_settings();
        }
        *self
            .cached_settings
            .write()
            .expect("settings lock poisoned") = next.clone();
        self.lyricsporn_api_endpoint
            .set(next.lyricsporn_api_url.as_deref());
        next
    }

    async fn persist(&self, value: &BotSettings) -> Result<(), DbError> {
        let mut connection = self.pool.connection().await?;
        let data_json = serde_json::to_value(value).map_err(|e| DbError::Row(e.to_string()))?;
        diesel::update(settings::table.filter(settings::id.eq(1_i16)))
            .set((
                settings::data.eq(data_json),
                settings::updated_at.eq(diesel::dsl::now),
            ))
            .execute(&mut *connection)
            .await?;
        Ok(())
    }

    pub async fn cycle_ripping_mode(&self) -> RippingMode {
        let next = self.get_settings().cycled_mode();
        self.set_setting("ripping_mode", json!(next.as_str()))
            .await
            .ripping_mode
    }

    pub async fn toggle_apple(&self) -> bool {
        self.toggle("apple_rip_enabled").await
    }
    pub async fn toggle_album(&self) -> bool {
        self.toggle("album_rip_enabled").await
    }
    pub async fn toggle_playlist(&self) -> bool {
        self.toggle("playlist_rip_enabled").await
    }
    pub async fn toggle_artist(&self) -> bool {
        self.toggle("artist_rip_enabled").await
    }
    pub async fn toggle_txt(&self) -> bool {
        self.toggle("txt_rip_enabled").await
    }
    pub async fn toggle_multi_link_rip(&self) -> bool {
        self.toggle("multi_link_rip_enabled").await
    }
    async fn toggle(&self, key: &str) -> bool {
        let current = self.get_settings();
        let value = match key {
            "apple_rip_enabled" => !current.apple_rip_enabled,
            "album_rip_enabled" => !current.album_rip_enabled,
            "playlist_rip_enabled" => !current.playlist_rip_enabled,
            "artist_rip_enabled" => !current.artist_rip_enabled,
            "txt_rip_enabled" => !current.txt_rip_enabled,
            _ => !current.multi_link_rip_enabled,
        };
        let settings = self.set_setting(key, json!(value)).await;
        match key {
            "apple_rip_enabled" => settings.apple_rip_enabled,
            "album_rip_enabled" => settings.album_rip_enabled,
            "playlist_rip_enabled" => settings.playlist_rip_enabled,
            "artist_rip_enabled" => settings.artist_rip_enabled,
            "txt_rip_enabled" => settings.txt_rip_enabled,
            _ => settings.multi_link_rip_enabled,
        }
    }

    pub async fn set_max_collection_tracks(&self, limit: i64) -> u32 {
        let value = u32::try_from(limit.max(0))
            .unwrap_or(engine::limits::MAX_COLLECTION_TRACKS)
            .min(engine::limits::MAX_COLLECTION_TRACKS);
        self.set_setting("max_collection_tracks", json!(value))
            .await
            .max_collection_tracks
    }
}

fn from_row(row: SettingsRow) -> BotSettings {
    let mut settings: BotSettings = serde_json::from_value(row.data).unwrap_or_else(|error| {
        tracing::warn!(%error, "failed to decode stored settings; using defaults");
        default_settings()
    });
    if settings.stream_public_url.is_none()
        && let Some(val) = settings.extra.remove("stream_public_url")
        && let Some(s) = val.as_str()
    {
        let trimmed = s.trim().trim_end_matches('/');
        if !trimmed.is_empty() {
            settings.stream_public_url = Some(trimmed.to_string());
        }
    }
    if let Some(val) = settings.extra.remove("stream_server_port")
        && let Some(port) = val.as_u64().and_then(|v| u16::try_from(v).ok())
    {
        settings.stream_server_port = port;
    }
    settings.lyricsporn_api_url = settings
        .lyricsporn_api_url
        .as_deref()
        .and_then(normalize_lyricsporn_api_url);
    settings
}

fn canonical_key(key: &str) -> Option<&'static str> {
    match key {
        "ripping_mode" | "rippingMode" => Some("ripping_mode"),
        "apple_rip_enabled" | "appleRipEnabled" | "apple" => Some("apple_rip_enabled"),
        "album_rip_enabled" | "albumRipEnabled" => Some("album_rip_enabled"),
        "playlist_rip_enabled" | "playlistRipEnabled" => Some("playlist_rip_enabled"),
        "artist_rip_enabled" | "artistRipEnabled" => Some("artist_rip_enabled"),
        "txt_rip_enabled" | "txtRipEnabled" => Some("txt_rip_enabled"),
        "multi_link_rip_enabled" | "multiLinkRipEnabled" => Some("multi_link_rip_enabled"),
        "max_collection_tracks" | "maxCollectionTracks" => Some("max_collection_tracks"),
        "stream_public_url" | "streamPublicUrl" | "stream_url" | "streamUrl" => {
            Some("stream_public_url")
        }
        "stream_server_port" | "streamServerPort" | "stream_port" | "streamPort" => {
            Some("stream_server_port")
        }
        "lyricsporn_api_url" | "lyricspornApiUrl" | "lyricsporn_url" | "lyricspornUrl" => {
            Some("lyricsporn_api_url")
        }
        _ => None,
    }
}

fn apply_value(settings: &mut BotSettings, key: &str, value: &Value) -> bool {
    match key {
        "ripping_mode" => value
            .as_str()
            .and_then(RippingMode::parse)
            .map(|mode| settings.ripping_mode = mode)
            .is_some(),
        "apple_rip_enabled" => value
            .as_bool()
            .map(|v| settings.apple_rip_enabled = v)
            .is_some(),
        "album_rip_enabled" => value
            .as_bool()
            .map(|v| settings.album_rip_enabled = v)
            .is_some(),
        "playlist_rip_enabled" => value
            .as_bool()
            .map(|v| settings.playlist_rip_enabled = v)
            .is_some(),
        "artist_rip_enabled" => value
            .as_bool()
            .map(|v| settings.artist_rip_enabled = v)
            .is_some(),
        "txt_rip_enabled" => value
            .as_bool()
            .map(|v| settings.txt_rip_enabled = v)
            .is_some(),
        "multi_link_rip_enabled" => value
            .as_bool()
            .map(|v| settings.multi_link_rip_enabled = v)
            .is_some(),
        "max_collection_tracks" => value
            .as_u64()
            .and_then(|v| u32::try_from(v).ok())
            .filter(|v| engine::limits::validate_collection_limit(*v))
            .map(|v| settings.max_collection_tracks = v)
            .is_some(),
        "stream_public_url" => {
            if value.is_null() {
                settings.stream_public_url = None;
                true
            } else if let Some(s) = value.as_str() {
                let trimmed = s.trim().trim_end_matches('/');
                if trimmed.is_empty() {
                    settings.stream_public_url = None;
                } else {
                    settings.stream_public_url = Some(trimmed.to_string());
                }
                true
            } else {
                false
            }
        }
        "stream_server_port" => value
            .as_u64()
            .and_then(|v| u16::try_from(v).ok())
            .map(|v| settings.stream_server_port = v)
            .is_some(),
        "lyricsporn_api_url" => {
            if value.is_null() {
                settings.lyricsporn_api_url = None;
                true
            } else if let Some(url) = value.as_str().and_then(normalize_lyricsporn_api_url) {
                settings.lyricsporn_api_url = Some(url);
                true
            } else {
                false
            }
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn test_canonical_key_stream_settings() {
        assert_eq!(
            canonical_key("stream_public_url"),
            Some("stream_public_url")
        );
        assert_eq!(canonical_key("stream_url"), Some("stream_public_url"));
        assert_eq!(canonical_key("streamUrl"), Some("stream_public_url"));
        assert_eq!(
            canonical_key("stream_server_port"),
            Some("stream_server_port")
        );
        assert_eq!(canonical_key("stream_port"), Some("stream_server_port"));
    }

    #[test]
    fn test_apply_value_stream_settings() {
        let mut settings = default_settings();
        assert_eq!(settings.stream_public_url, None);

        assert!(apply_value(
            &mut settings,
            "stream_public_url",
            &json!("http://192.168.0.6:4444/")
        ));
        assert_eq!(
            settings.stream_public_url,
            Some("http://192.168.0.6:4444".to_string())
        );

        assert!(apply_value(
            &mut settings,
            "stream_public_url",
            &serde_json::Value::Null
        ));
        assert_eq!(settings.stream_public_url, None);

        assert!(apply_value(
            &mut settings,
            "stream_public_url",
            &json!("   ")
        ));
        assert_eq!(settings.stream_public_url, None);

        assert!(apply_value(
            &mut settings,
            "stream_server_port",
            &json!(8080)
        ));
        assert_eq!(settings.stream_server_port, 8080);
    }

    #[test]
    fn test_from_row_recovers_stream_url_from_extra() {
        let mut raw_data = serde_json::Map::new();
        raw_data.insert(
            "stream_public_url".to_string(),
            json!("http://192.168.0.6:4444"),
        );
        let row = SettingsRow {
            id: 1,
            data: serde_json::Value::Object(raw_data),
            updated_at: chrono::Utc::now(),
        };

        let parsed = from_row(row);
        assert_eq!(
            parsed.stream_public_url,
            Some("http://192.168.0.6:4444".to_string())
        );
    }
}
