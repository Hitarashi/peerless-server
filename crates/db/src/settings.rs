use std::sync::RwLock;

use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use engine::settings::{BotSettings, RippingMode, default_settings};
use serde_json::{Value, json};

use crate::{DbError, DbPool, models::SettingsRow, schema::settings};

/// The settings table is a one-row, JSONB-backed configuration record.
/// Unknown keys are preserved in `BotSettings.extra` to ensure future settings
/// require zero database schema migrations.
pub struct SettingsStore {
    pool: DbPool,
    cached_settings: RwLock<BotSettings>,
}

impl SettingsStore {
    pub fn new(pool: DbPool) -> Self {
        Self {
            pool,
            cached_settings: RwLock::new(default_settings()),
        }
    }

    pub async fn init(&self) -> Result<(), DbError> {
        self.reload().await
    }

    /// Reload the cache from PostgreSQL. This is intentionally fallible: a
    /// stale in-memory snapshot is unsafe after a restore or a reconnect.
    pub async fn reload(&self) -> Result<(), DbError> {
        let result = self.load().await?;
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

    /// Set one externally named setting while persisting the complete typed
    /// singleton. Unknown keys are preserved into `extra` dynamically.
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
    pub async fn toggle_qobuz(&self) -> bool {
        self.toggle("qobuz_rip_enabled").await
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
    pub async fn toggle_auto_dump(&self) -> bool {
        self.toggle("auto_dump_enabled").await
    }

    async fn toggle(&self, key: &str) -> bool {
        let current = self.get_settings();
        let value = match key {
            "apple_rip_enabled" => !current.apple_rip_enabled,
            "qobuz_rip_enabled" => !current.qobuz_rip_enabled,
            "album_rip_enabled" => !current.album_rip_enabled,
            "playlist_rip_enabled" => !current.playlist_rip_enabled,
            "artist_rip_enabled" => !current.artist_rip_enabled,
            "txt_rip_enabled" => !current.txt_rip_enabled,
            "auto_dump_enabled" => !current.auto_dump_enabled,
            _ => !current.multi_link_rip_enabled,
        };
        let settings = self.set_setting(key, json!(value)).await;
        match key {
            "apple_rip_enabled" => settings.apple_rip_enabled,
            "qobuz_rip_enabled" => settings.qobuz_rip_enabled,
            "album_rip_enabled" => settings.album_rip_enabled,
            "playlist_rip_enabled" => settings.playlist_rip_enabled,
            "artist_rip_enabled" => settings.artist_rip_enabled,
            "txt_rip_enabled" => settings.txt_rip_enabled,
            "auto_dump_enabled" => settings.auto_dump_enabled,
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

    pub async fn add_auto_dump_storefront(&self, storefront: &str) -> Vec<String> {
        let clean = storefront.to_lowercase().trim().to_owned();
        let mut values = self.get_settings().auto_dump_storefronts;
        if clean.is_empty() || values.iter().any(|value| value == &clean) {
            return values;
        }
        values.push(clean);
        self.set_setting("auto_dump_storefronts", json!(values))
            .await
            .auto_dump_storefronts
    }

    pub async fn remove_auto_dump_storefront(&self, storefront: &str) -> Vec<String> {
        let clean = storefront.to_lowercase().trim().to_owned();
        let mut values = self
            .get_settings()
            .auto_dump_storefronts
            .into_iter()
            .filter(|value| value != &clean)
            .collect::<Vec<_>>();
        if values.is_empty() {
            values.push("us".to_owned());
        }
        self.set_setting("auto_dump_storefronts", json!(values))
            .await
            .auto_dump_storefronts
    }

    pub async fn set_auto_dump_storefronts(&self, storefronts: &[String]) -> Vec<String> {
        let mut values = Vec::new();
        for storefront in storefronts {
            let clean = storefront.to_lowercase().trim().to_owned();
            if !clean.is_empty() && !values.contains(&clean) {
                values.push(clean);
            }
        }
        if values.is_empty() {
            values.push("us".to_owned());
        }
        self.set_setting("auto_dump_storefronts", json!(values))
            .await
            .auto_dump_storefronts
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
    settings
}

fn canonical_key(key: &str) -> Option<&'static str> {
    match key {
        "ripping_mode" | "rippingMode" => Some("ripping_mode"),
        "apple_rip_enabled" | "appleRipEnabled" | "apple" => Some("apple_rip_enabled"),
        "qobuz_rip_enabled" | "qobuzRipEnabled" | "qobuz" => Some("qobuz_rip_enabled"),
        "album_rip_enabled" | "albumRipEnabled" => Some("album_rip_enabled"),
        "playlist_rip_enabled" | "playlistRipEnabled" => Some("playlist_rip_enabled"),
        "artist_rip_enabled" | "artistRipEnabled" => Some("artist_rip_enabled"),
        "txt_rip_enabled" | "txtRipEnabled" => Some("txt_rip_enabled"),
        "multi_link_rip_enabled" | "multiLinkRipEnabled" => Some("multi_link_rip_enabled"),
        "max_collection_tracks" | "maxCollectionTracks" => Some("max_collection_tracks"),
        "auto_dump_enabled" | "autoDumpEnabled" => Some("auto_dump_enabled"),
        "auto_dump_storefronts" | "autoDumpStorefronts" => Some("auto_dump_storefronts"),
        "stream_public_url" | "streamPublicUrl" | "stream_url" | "streamUrl" => {
            Some("stream_public_url")
        }
        "stream_server_port" | "streamServerPort" | "stream_port" | "streamPort" => {
            Some("stream_server_port")
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
        "qobuz_rip_enabled" => value
            .as_bool()
            .map(|v| settings.qobuz_rip_enabled = v)
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
        "auto_dump_enabled" => value
            .as_bool()
            .map(|v| settings.auto_dump_enabled = v)
            .is_some(),
        "auto_dump_storefronts" => value
            .as_array()
            .map(|values| {
                let cleaned = values
                    .iter()
                    .filter_map(Value::as_str)
                    .map(|v| v.to_lowercase().trim().to_owned())
                    .filter(|v| !v.is_empty())
                    .collect::<Vec<_>>();
                if !cleaned.is_empty() {
                    settings.auto_dump_storefronts = cleaned;
                }
            })
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

        // Apply URL with trailing slash
        assert!(apply_value(
            &mut settings,
            "stream_public_url",
            &json!("http://192.168.0.6:4444/")
        ));
        assert_eq!(
            settings.stream_public_url,
            Some("http://192.168.0.6:4444".to_string())
        );

        // Clear URL with null
        assert!(apply_value(
            &mut settings,
            "stream_public_url",
            &serde_json::Value::Null
        ));
        assert_eq!(settings.stream_public_url, None);

        // Clear URL with empty string
        assert!(apply_value(
            &mut settings,
            "stream_public_url",
            &json!("   ")
        ));
        assert_eq!(settings.stream_public_url, None);

        // Apply port
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
