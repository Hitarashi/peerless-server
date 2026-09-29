//! Bot settings: ripping mode + feature flags.
//!
//! The DB-backed `SettingsStore` (typed singleton, write-through cache)
//! lives in the db crate; this module carries the domain types,
//! defaults, and permission logic.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

fn default_true() -> bool {
    true
}

fn default_50() -> u32 {
    50
}

fn default_storefronts() -> Vec<String> {
    vec!["us".to_string()]
}

/// The storefront every Apple Music catalog call falls back to when the caller
/// did not supply one.
pub const FALLBACK_STOREFRONT: &str = "in";

fn default_storefront() -> String {
    FALLBACK_STOREFRONT.to_string()
}

fn default_stream_server_port() -> u16 {
    4444
}

/// Whether the bot rips live, serves cache only, or is paused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RippingMode {
    #[default]
    Live,
    CacheOnly,
    Paused,
}

impl RippingMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            RippingMode::Live => "live",
            RippingMode::CacheOnly => "cache_only",
            RippingMode::Paused => "paused",
        }
    }

    /// Parse a stored `ripping_mode` row value; anything unrecognized falls
    /// back to the default.
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "live" => Some(RippingMode::Live),
            "cache_only" => Some(RippingMode::CacheOnly),
            "paused" => Some(RippingMode::Paused),
            _ => None,
        }
    }
}

/// The bot's runtime settings.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BotSettings {
    #[serde(default)]
    pub ripping_mode: RippingMode,
    #[serde(default = "default_true")]
    pub album_rip_enabled: bool,
    #[serde(default = "default_true")]
    pub playlist_rip_enabled: bool,
    #[serde(default = "default_true")]
    pub artist_rip_enabled: bool,
    #[serde(default = "default_true")]
    pub txt_rip_enabled: bool,
    #[serde(default = "default_true")]
    pub multi_link_rip_enabled: bool,
    #[serde(default = "default_50")]
    pub max_collection_tracks: u32,
    #[serde(default = "default_true")]
    pub auto_dump_enabled: bool,
    #[serde(default = "default_storefronts")]
    pub auto_dump_storefronts: Vec<String>,
    #[serde(default = "default_true")]
    pub apple_rip_enabled: bool,
    #[serde(default = "default_true")]
    pub qobuz_rip_enabled: bool,
    #[serde(default)]
    pub stream_public_url: Option<String>,
    #[serde(default = "default_stream_server_port")]
    pub stream_server_port: u16,
    /// Storefront used for Apple Music catalog lookups when the caller did not
    /// supply one. Distinct from `auto_dump_storefronts`, which is the list of
    /// storefronts auto-dump sweeps.
    #[serde(default = "default_storefront")]
    pub default_storefront: String,
    #[serde(flatten, default)]
    pub extra: HashMap<String, serde_json::Value>,
}

impl Default for BotSettings {
    fn default() -> Self {
        Self {
            ripping_mode: RippingMode::Live,
            album_rip_enabled: true,
            playlist_rip_enabled: true,
            artist_rip_enabled: true,
            txt_rip_enabled: true,
            multi_link_rip_enabled: true,
            max_collection_tracks: 50,
            auto_dump_enabled: true,
            auto_dump_storefronts: vec!["us".to_string()],
            apple_rip_enabled: true,
            qobuz_rip_enabled: true,
            stream_public_url: None,
            stream_server_port: 4444,
            default_storefront: default_storefront(),
            extra: HashMap::new(),
        }
    }
}

impl BotSettings {
    /// Admins can always rip live; users need mode `'live'`.
    pub fn can_rip_live(&self, is_admin: bool) -> bool {
        if is_admin {
            return true;
        }
        self.ripping_mode == RippingMode::Live
    }

    /// Check if live ripping is enabled for the specified provider.
    pub fn can_rip_provider(&self, provider: music::Provider, is_admin: bool) -> bool {
        if is_admin {
            return true;
        }
        match provider {
            music::Provider::Apple => self.apple_rip_enabled,
            music::Provider::Qobuz => self.qobuz_rip_enabled,
        }
    }

    pub fn can_rip_apple(&self, is_admin: bool) -> bool {
        self.can_rip_provider(music::Provider::Apple, is_admin)
    }

    pub fn can_rip_qobuz(&self, is_admin: bool) -> bool {
        self.can_rip_provider(music::Provider::Qobuz, is_admin)
    }

    /// Admins can always serve cache; users need mode `!= 'paused'`.
    pub fn can_serve_cache(&self, is_admin: bool) -> bool {
        if is_admin {
            return true;
        }
        self.ripping_mode != RippingMode::Paused
    }

    pub fn can_rip_album(&self, is_admin: bool) -> bool {
        is_admin || self.album_rip_enabled
    }

    pub fn can_rip_playlist(&self, is_admin: bool) -> bool {
        is_admin || self.playlist_rip_enabled
    }

    pub fn can_rip_artist(&self, is_admin: bool) -> bool {
        is_admin || self.artist_rip_enabled
    }

    pub fn can_rip_txt(&self, is_admin: bool) -> bool {
        is_admin || self.txt_rip_enabled
    }

    pub fn can_rip_multi_link(&self, is_admin: bool) -> bool {
        is_admin || self.multi_link_rip_enabled
    }

    /// Cycle: live → cache_only → paused → live.
    pub fn cycled_mode(&self) -> RippingMode {
        match self.ripping_mode {
            RippingMode::Live => RippingMode::CacheOnly,
            RippingMode::CacheOnly => RippingMode::Paused,
            RippingMode::Paused => RippingMode::Live,
        }
    }
}

/// `DEFAULT_SETTINGS` with its `autoDumpStorefronts: ['us']` — the const
/// cannot hold a `Vec<String>`, so callers use this constructor.
pub fn default_settings() -> BotSettings {
    BotSettings::default()
}

/// The configured default storefront, trimmed and guaranteed non-empty.
///
/// A stored value that is empty or whitespace-only would otherwise be handed
/// straight to the Apple API, so it falls back to [`FALLBACK_STOREFRONT`].
/// Call this wherever `default_storefront` is about to become a request
/// parameter.
pub fn resolve_default_storefront(settings: &BotSettings) -> &str {
    let trimmed = settings.default_storefront.trim();
    if trimmed.is_empty() {
        FALLBACK_STOREFRONT
    } else {
        trimmed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_ts() {
        let d = default_settings();
        assert_eq!(d.ripping_mode, RippingMode::Live);
        assert!(d.album_rip_enabled);
        assert!(d.playlist_rip_enabled);
        assert!(d.artist_rip_enabled);
        assert!(d.txt_rip_enabled);
        assert!(d.multi_link_rip_enabled);
        assert_eq!(d.max_collection_tracks, 50);
        assert!(d.auto_dump_enabled);
        assert_eq!(d.auto_dump_storefronts, vec!["us".to_string()]);
        assert!(d.apple_rip_enabled);
        assert!(d.qobuz_rip_enabled);
        assert_eq!(d.stream_public_url, None);
        assert_eq!(d.stream_server_port, 4444);
        assert_eq!(d.default_storefront, "in");
    }

    #[test]
    fn default_storefront_is_independent_of_auto_dump_storefronts() {
        let d = default_settings();
        assert_eq!(d.default_storefront, "in");
        assert_eq!(d.auto_dump_storefronts, vec!["us".to_string()]);
    }

    #[test]
    fn resolve_default_storefront_falls_back_when_blank() {
        let mut s = default_settings();
        assert_eq!(resolve_default_storefront(&s), "in");

        // A stored value wins once it is non-blank.
        s.default_storefront = "gb".to_string();
        assert_eq!(resolve_default_storefront(&s), "gb");

        // Surrounding whitespace is trimmed, not rejected.
        s.default_storefront = "  jp  ".to_string();
        assert_eq!(resolve_default_storefront(&s), "jp");

        // Empty / whitespace-only never reaches the Apple API.
        s.default_storefront = String::new();
        assert_eq!(resolve_default_storefront(&s), "in");
        s.default_storefront = "   \t ".to_string();
        assert_eq!(resolve_default_storefront(&s), "in");
    }

    #[test]
    fn missing_default_storefront_key_deserializes_to_default() {
        // Rows persisted before the field existed have no `default_storefront`
        // key; serde must fall back rather than fail the whole decode.
        let decoded: BotSettings =
            serde_json::from_str(r#"{"rippingMode":"live","albumRipEnabled":true}"#)
                .expect("deserialize legacy row");
        assert_eq!(decoded.default_storefront, "in");
        assert_eq!(resolve_default_storefront(&decoded), "in");
    }

    #[test]
    fn default_storefront_roundtrips_through_json() {
        let mut s = default_settings();
        s.default_storefront = "ca".to_string();
        let json_str = serde_json::to_string(&s).expect("serialize");
        let decoded: BotSettings = serde_json::from_str(&json_str).expect("deserialize");
        assert_eq!(decoded.default_storefront, "ca");
        // The flattened `extra` map must not shadow the typed field.
        assert!(!decoded.extra.contains_key("default_storefront"));
    }

    #[test]
    fn mode_parse_roundtrip() {
        assert_eq!(RippingMode::parse("live"), Some(RippingMode::Live));
        assert_eq!(
            RippingMode::parse("cache_only"),
            Some(RippingMode::CacheOnly)
        );
        assert_eq!(RippingMode::parse("paused"), Some(RippingMode::Paused));
        assert_eq!(RippingMode::parse("bogus"), None);
    }

    #[test]
    fn can_rip_live_admin_bypass_and_modes() {
        let live = default_settings();
        let cache = BotSettings {
            ripping_mode: RippingMode::CacheOnly,
            ..default_settings()
        };
        let paused = BotSettings {
            ripping_mode: RippingMode::Paused,
            ..default_settings()
        };

        assert!(live.can_rip_live(false));
        assert!(!cache.can_rip_live(false));
        assert!(!paused.can_rip_live(false));
        // Admins bypass everything.
        assert!(paused.can_rip_live(true));
    }

    #[test]
    fn can_rip_provider_admin_bypass() {
        let mut s = default_settings();
        s.apple_rip_enabled = false;
        s.qobuz_rip_enabled = false;

        assert!(!s.can_rip_apple(false));
        assert!(!s.can_rip_qobuz(false));
        // Admins bypass
        assert!(s.can_rip_apple(true));
        assert!(s.can_rip_qobuz(true));
    }

    #[test]
    fn json_serialization_roundtrip_with_extra() {
        let mut s = default_settings();
        s.extra
            .insert("custom_feature".to_string(), serde_json::json!(true));

        let json_str = serde_json::to_string(&s).expect("serialize");
        let deserialized: BotSettings = serde_json::from_str(&json_str).expect("deserialize");
        assert!(deserialized.apple_rip_enabled);
        assert_eq!(
            deserialized.extra.get("custom_feature"),
            Some(&serde_json::json!(true))
        );
    }

    #[test]
    fn can_serve_cache_blocks_only_when_paused() {
        let live = default_settings();
        let cache = BotSettings {
            ripping_mode: RippingMode::CacheOnly,
            ..default_settings()
        };
        let paused = BotSettings {
            ripping_mode: RippingMode::Paused,
            ..default_settings()
        };

        assert!(live.can_serve_cache(false));
        assert!(cache.can_serve_cache(false));
        assert!(!paused.can_serve_cache(false));
        assert!(paused.can_serve_cache(true));
    }

    #[test]
    fn collection_toggles_admin_bypass() {
        let disabled = BotSettings {
            album_rip_enabled: false,
            playlist_rip_enabled: false,
            artist_rip_enabled: false,
            txt_rip_enabled: false,
            multi_link_rip_enabled: false,
            ..default_settings()
        };

        assert!(!disabled.can_rip_album(false));
        assert!(disabled.can_rip_album(true));
        assert!(!disabled.can_rip_playlist(false));
        assert!(disabled.can_rip_playlist(true));
        assert!(!disabled.can_rip_artist(false));
        assert!(disabled.can_rip_artist(true));
        assert!(!disabled.can_rip_txt(false));
        assert!(disabled.can_rip_txt(true));
        assert!(!disabled.can_rip_multi_link(false));
        assert!(disabled.can_rip_multi_link(true));
    }

    #[test]
    fn cycled_mode_loops_correctly() {
        let live = default_settings();
        let cache = BotSettings {
            ripping_mode: live.cycled_mode(),
            ..default_settings()
        };
        let paused = BotSettings {
            ripping_mode: cache.cycled_mode(),
            ..default_settings()
        };
        let back_to_live = BotSettings {
            ripping_mode: paused.cycled_mode(),
            ..default_settings()
        };

        assert_eq!(cache.ripping_mode, RippingMode::CacheOnly);
        assert_eq!(paused.ripping_mode, RippingMode::Paused);
        assert_eq!(back_to_live.ripping_mode, RippingMode::Live);
    }
}
