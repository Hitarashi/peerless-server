use std::sync::Arc;

use ferogram::{
    InputMessage, PeerRef, filters,
    filters::Dispatcher,
    keyboard::{Button, InlineKeyboard},
    update::CallbackQuery,
};

use crate::{
    BotState,
    html::{escape, parse_dynamic_html},
    interaction::{SettingFeature, SettingPage, SettingsAction},
};

fn toggle_label(label: &str, enabled: bool) -> String {
    format!("{label}: {}", if enabled { "ON" } else { "OFF" })
}

fn mode_description(mode: engine::settings::RippingMode) -> &'static str {
    use engine::settings::RippingMode::{CacheOnly, Live, Paused};
    match mode {
        Live => "<b>Live ripping</b> (Cache hits and live decryption)",
        CacheOnly => "<b>Cache only</b> (Serves cached songs; live decryption blocked)",
        Paused => "! <b>Paused</b> (Ripping commands suspended for regular users)",
    }
}

fn flag(enabled: bool) -> &'static str {
    if enabled { "Enabled" } else { "Disabled" }
}

pub fn render_settings_text(settings: &engine::settings::BotSettings) -> String {
    render_catalog_text(settings)
}

pub fn render_catalog_text(settings: &engine::settings::BotSettings) -> String {
    let lyricsporn_url = settings
        .lyricsporn_api_url
        .as_deref()
        .map(escape)
        .unwrap_or_else(|| "Not configured".to_owned());
    let stream_url = settings
        .stream_public_url
        .as_deref()
        .map(escape)
        .unwrap_or_else(|| "Not configured".to_owned());
    let storefront = engine::settings::resolve_default_storefront(settings);

    format!(
        "<b>Bot settings and operation controls</b><br/><br/>\
<b>Engine & Operations:</b><br/>\
• <b>Engine Mode:</b> {}<br/>\
• <b>Apple Music Ripping:</b> {}<br/>\
• <b>Default Storefront:</b> <code>{}</code><br/><br/>\
<b>Content Scopes (User Limits):</b><br/>\
• <b>Album Ripping:</b> {}<br/>\
• <b>Playlist Ripping:</b> {}<br/>\
• <b>Artist Ripping:</b> {}<br/>\
• <b>.TXT File Ripping:</b> {}<br/>\
• <b>Multi-Link Ripping:</b> {}<br/><br/>\
<b>Integrations & Networking:</b><br/>\
• <b>Lyricsporn API URL:</b> <code>{lyricsporn_url}</code><br/>\
• <b>Stream Public URL:</b> <code>{stream_url}</code><br/>\
• <b>Stream Server Port:</b> <code>{}</code><br/><br/>\
<blockquote><i>Select a setting below to view details, learn how to configure it, or change values.</i></blockquote>",
        mode_description(settings.ripping_mode),
        flag(settings.apple_rip_enabled),
        storefront,
        flag(settings.album_rip_enabled),
        flag(settings.playlist_rip_enabled),
        flag(settings.artist_rip_enabled),
        flag(settings.txt_rip_enabled),
        flag(settings.multi_link_rip_enabled),
        settings.stream_server_port,
    )
}

fn catalog_keyboard(settings: &engine::settings::BotSettings) -> ferogram::tl::enums::ReplyMarkup {
    let mode_str = match settings.ripping_mode {
        engine::settings::RippingMode::Live => "Live",
        engine::settings::RippingMode::CacheOnly => "Cache Only",
        engine::settings::RippingMode::Paused => "Paused",
    };
    let sf = engine::settings::resolve_default_storefront(settings);
    InlineKeyboard::new()
        .row([Button::callback(
            format!("Engine Mode: {mode_str} »"),
            b"settings:view:mode",
        )])
        .row([Button::callback(
            format!(
                "Apple Music: {} »",
                if settings.apple_rip_enabled {
                    "ON"
                } else {
                    "OFF"
                }
            ),
            b"settings:view:apple",
        )])
        .row([Button::callback(
            "Content Scopes (Albums, TXT...) »",
            b"settings:view:scopes",
        )])
        .row([
            Button::callback("Lyricsporn API »", b"settings:view:lyricsporn"),
            Button::callback("Stream Server »", b"settings:view:stream"),
        ])
        .row([Button::callback(
            format!("Default Storefront: {sf} »"),
            b"settings:view:storefront",
        )])
        .row([
            Button::callback("Refresh", b"settings:refresh"),
            Button::callback("Close", b"settings:close"),
        ])
        .into_markup()
}

fn render_mode_text(settings: &engine::settings::BotSettings) -> String {
    format!(
        "<b>Engine Operating Mode</b><br/><br/>\
Current mode: {}<br/><br/>\
<b>Mode descriptions:</b><br/>\
• <b>Live:</b> Normal operations. Cache hits are served and live decryption/ripping is enabled.<br/>\
• <b>Cache Only:</b> Only serves tracks already cached in the database. New live decryptions are blocked.<br/>\
• <b>Paused:</b> All user ripping commands are temporarily suspended for regular users.<br/><br/>\
<b>How to configure via command:</b><br/>\
<code>/settings mode &lt;live|cache_only|paused&gt;</code><br/><br/>\
<blockquote><i>Select a mode below to switch immediately, or use the command.</i></blockquote>",
        mode_description(settings.ripping_mode),
    )
}

fn mode_keyboard(current: engine::settings::RippingMode) -> ferogram::tl::enums::ReplyMarkup {
    use engine::settings::RippingMode::{CacheOnly, Live, Paused};
    InlineKeyboard::new()
        .row([
            Button::callback(
                if current == Live {
                    "✓ Live Ripping"
                } else {
                    "Live Ripping"
                },
                b"settings:set_mode:live",
            ),
            Button::callback(
                if current == CacheOnly {
                    "✓ Cache Only"
                } else {
                    "Cache Only"
                },
                b"settings:set_mode:cache_only",
            ),
            Button::callback(
                if current == Paused {
                    "✓ Fully Paused"
                } else {
                    "Fully Paused"
                },
                b"settings:set_mode:paused",
            ),
        ])
        .row([
            Button::callback("« Back to Catalog", b"settings:home"),
            Button::callback("Close", b"settings:close"),
        ])
        .into_markup()
}

fn render_apple_text(settings: &engine::settings::BotSettings) -> String {
    let status = if settings.apple_rip_enabled {
        "Enabled"
    } else {
        "Disabled"
    };
    format!(
        "<b>Apple Music Ripping</b><br/><br/>\
Current status: <b>{status}</b><br/><br/>\
<b>About this setting:</b><br/>\
Controls whether Apple Music downloads are allowed for regular users. When disabled, non-admin users cannot download Apple Music songs, albums, or playlists.<br/><br/>\
<b>How to configure via command:</b><br/>\
<code>/settings apple &lt;on|off&gt;</code><br/><br/>\
<blockquote><i>Use the button below to toggle this setting.</i></blockquote>",
    )
}

fn apple_keyboard(enabled: bool) -> ferogram::tl::enums::ReplyMarkup {
    let toggle_label = if enabled { "Turn OFF" } else { "Turn ON" };
    InlineKeyboard::new()
        .row([Button::callback(toggle_label, b"settings:apple")])
        .row([
            Button::callback("« Back to Catalog", b"settings:home"),
            Button::callback("Close", b"settings:close"),
        ])
        .into_markup()
}

fn render_scopes_text(settings: &engine::settings::BotSettings) -> String {
    format!(
        "<b>Download Content Scopes</b><br/><br/>\
Controls which content types regular users are permitted to download. Admins always bypass these restrictions.<br/><br/>\
• <b>Albums:</b> {}<br/>\
• <b>Playlists:</b> {}<br/>\
• <b>Artists:</b> {}<br/>\
• <b>.TXT Batch Files:</b> {}<br/>\
• <b>Multi-Link Messages:</b> {}<br/><br/>\
<b>How to configure via command:</b><br/>\
<code>/settings &lt;album|playlist|artist|txt|multilink&gt; &lt;on|off&gt;</code><br/><br/>\
<blockquote><i>Tap any button below to toggle that scope on or off.</i></blockquote>",
        flag(settings.album_rip_enabled),
        flag(settings.playlist_rip_enabled),
        flag(settings.artist_rip_enabled),
        flag(settings.txt_rip_enabled),
        flag(settings.multi_link_rip_enabled),
    )
}

fn scopes_keyboard(settings: &engine::settings::BotSettings) -> ferogram::tl::enums::ReplyMarkup {
    InlineKeyboard::new()
        .row([
            Button::callback(
                toggle_label("Albums", settings.album_rip_enabled),
                b"settings:album",
            ),
            Button::callback(
                toggle_label("Playlists", settings.playlist_rip_enabled),
                b"settings:playlist",
            ),
        ])
        .row([
            Button::callback(
                toggle_label("Artists", settings.artist_rip_enabled),
                b"settings:artist",
            ),
            Button::callback(
                toggle_label(".TXT Batch", settings.txt_rip_enabled),
                b"settings:txt",
            ),
        ])
        .row([Button::callback(
            toggle_label("Multi-Link", settings.multi_link_rip_enabled),
            b"settings:multilink",
        )])
        .row([
            Button::callback("« Back to Catalog", b"settings:home"),
            Button::callback("Close", b"settings:close"),
        ])
        .into_markup()
}

fn render_lyricsporn_text(settings: &engine::settings::BotSettings) -> String {
    let current_url = settings
        .lyricsporn_api_url
        .as_deref()
        .map(|u| format!("<code>{}</code>", escape(u)))
        .unwrap_or_else(|| "<i>Not configured</i>".to_owned());
    format!(
        "<b>Lyricsporn API Integration</b><br/><br/>\
Endpoint: {current_url}<br/><br/>\
<b>About this setting:</b><br/>\
Connects the bot to the Lyricsporn metadata backend. Used for:<br/>\
• Synchronized lyrics (.LRC / TTML)<br/>\
• High-resolution album artwork<br/>\
• Catalog search enhancement<br/><br/>\
<b>How to set a URL:</b><br/>\
<code>/settings lyricsporn_url &lt;http(s)://host/api/v1&gt;</code><br/><br/>\
<b>Example:</b><br/>\
<code>/settings lyricsporn_url https://api.lyricsporn.com/api/v1</code><br/><br/>\
<b>How to clear:</b><br/>\
<code>/settings lyricsporn_url clear</code>",
    )
}

fn lyricsporn_keyboard(has_url: bool) -> ferogram::tl::enums::ReplyMarkup {
    let mut kb = InlineKeyboard::new();
    if has_url {
        kb = kb.row([Button::callback(
            "Clear API URL",
            b"settings:clear:lyricsporn",
        )]);
    }
    kb.row([
        Button::callback("« Back to Catalog", b"settings:home"),
        Button::callback("Close", b"settings:close"),
    ])
    .into_markup()
}

fn render_stream_text(settings: &engine::settings::BotSettings) -> String {
    let public_url = settings
        .stream_public_url
        .as_deref()
        .map(|u| format!("<code>{}</code>", escape(u)))
        .unwrap_or_else(|| "<i>Not configured</i>".to_owned());
    format!(
        "<b>Streaming Server Configuration</b><br/><br/>\
• <b>Public URL:</b> {public_url}<br/>\
• <b>Server Port:</b> <code>{}</code><br/><br/>\
<b>About this setting:</b><br/>\
Configures the built-in HTTP audio stream server for web-based track listening and preview links.<br/><br/>\
<b>How to set Public URL:</b><br/>\
<code>/settings stream_url &lt;http(s)://domain.com&gt;</code><br/>\
<i>Or clear with:</i><br/>\
<code>/settings stream_url clear</code><br/><br/>\
<b>How to set Server Port:</b><br/>\
<code>/settings stream_port &lt;1024-65535&gt;</code><br/><br/>\
<b>Examples:</b><br/>\
<code>/settings stream_url https://stream.example.com</code><br/>\
<code>/settings stream_port 4444</code>",
        settings.stream_server_port,
    )
}

fn stream_keyboard(has_url: bool) -> ferogram::tl::enums::ReplyMarkup {
    let mut kb = InlineKeyboard::new();
    if has_url {
        kb = kb.row([Button::callback(
            "Clear Stream Public URL",
            b"settings:clear:stream",
        )]);
    }
    kb.row([
        Button::callback("« Back to Catalog", b"settings:home"),
        Button::callback("Close", b"settings:close"),
    ])
    .into_markup()
}

fn render_storefront_text(settings: &engine::settings::BotSettings) -> String {
    let current_sf = engine::settings::resolve_default_storefront(settings);
    format!(
        "<b>Default Storefront</b><br/><br/>\
Current storefront: <code>{current_sf}</code><br/><br/>\
<b>About this setting:</b><br/>\
Two-letter Apple Music country storefront code used for searching and fetching songs when no storefront is specified in the request or URL.<br/><br/>\
<b>How to configure via command:</b><br/>\
<code>/settings storefront &lt;country_code&gt;</code><br/><br/>\
<b>Example:</b><br/>\
<code>/settings storefront us</code><br/><br/>\
<blockquote><i>Select a popular preset below or set any 2-letter code using the command.</i></blockquote>",
    )
}

fn storefront_keyboard(current_sf: &str) -> ferogram::tl::enums::ReplyMarkup {
    InlineKeyboard::new()
        .row([
            Button::callback(
                if current_sf == "in" {
                    "✓ IN (India)"
                } else {
                    "IN (India)"
                },
                b"settings:sf:in",
            ),
            Button::callback(
                if current_sf == "us" {
                    "✓ US (United States)"
                } else {
                    "US (United States)"
                },
                b"settings:sf:us",
            ),
        ])
        .row([
            Button::callback(
                if current_sf == "gb" {
                    "✓ GB (United Kingdom)"
                } else {
                    "GB (United Kingdom)"
                },
                b"settings:sf:gb",
            ),
            Button::callback(
                if current_sf == "jp" {
                    "✓ JP (Japan)"
                } else {
                    "JP (Japan)"
                },
                b"settings:sf:jp",
            ),
        ])
        .row([
            Button::callback("« Back to Catalog", b"settings:home"),
            Button::callback("Close", b"settings:close"),
        ])
        .into_markup()
}

pub(crate) async fn render_settings_view(
    state: &BotState,
    peer: &PeerRef,
    page: SettingPage,
    message_id: Option<i32>,
    reply_to: Option<i32>,
) {
    let settings = state.rip_deps.settings_snapshot();
    let (text, markup) = match page {
        SettingPage::Catalog => (render_catalog_text(&settings), catalog_keyboard(&settings)),
        SettingPage::Mode => (
            render_mode_text(&settings),
            mode_keyboard(settings.ripping_mode),
        ),
        SettingPage::Apple => (
            render_apple_text(&settings),
            apple_keyboard(settings.apple_rip_enabled),
        ),
        SettingPage::Scopes => (render_scopes_text(&settings), scopes_keyboard(&settings)),
        SettingPage::Lyricsporn => (
            render_lyricsporn_text(&settings),
            lyricsporn_keyboard(settings.lyricsporn_api_url.is_some()),
        ),
        SettingPage::Stream => (
            render_stream_text(&settings),
            stream_keyboard(settings.stream_public_url.is_some()),
        ),
        SettingPage::Storefront => {
            let sf = engine::settings::resolve_default_storefront(&settings);
            (render_storefront_text(&settings), storefront_keyboard(sf))
        }
    };

    let input = InputMessage::html(parse_dynamic_html(&text)).reply_markup(markup);
    if let Some(message_id) = message_id {
        let _ = state
            .client
            .edit_message(peer.clone(), message_id, input)
            .await;
    } else if let Some(reply_to) = reply_to {
        let _ = state
            .client
            .send_message(peer.clone(), input.reply_to(Some(reply_to)))
            .await;
    } else {
        let _ = state.client.send_message(peer.clone(), input).await;
    }
}

pub(crate) async fn render_settings_message(
    state: &BotState,
    peer: &PeerRef,
    message_id: Option<i32>,
    reply_to: Option<i32>,
) {
    render_settings_view(state, peer, SettingPage::Catalog, message_id, reply_to).await;
}

pub async fn command(state: Arc<BotState>, msg: ferogram::update::IncomingMessage) {
    let sender = msg.sender_user_id().unwrap_or_default();
    if !state.auth.is_admin(sender) {
        return;
    }

    let Some(peer) = msg.peer_id() else {
        return;
    };
    let peer = PeerRef::Peer(peer.clone());

    let text = msg.text().unwrap_or_default();
    let parts: Vec<&str> = text.split_whitespace().collect();

    if parts.len() >= 3 {
        let sub = parts[1].to_lowercase();
        let raw_value = parts[2..].join(" ");

        if let Some(reply) = subcommand_reply(Arc::clone(&state), &sub, &raw_value).await {
            let _ = msg
                .reply(InputMessage::html(parse_dynamic_html(&reply)))
                .await;
            return;
        }
    }

    render_settings_message(&state, &peer, None, Some(msg.id())).await;
}

async fn subcommand_reply(state: Arc<BotState>, sub: &str, raw_value: &str) -> Option<String> {
    let settings_store = state.rip_deps.settings();
    let normalized_value = raw_value.to_ascii_lowercase();

    match sub {
        "mode" => {
            let mode = match normalized_value.as_str() {
                "live" | "cache_only" | "paused" => normalized_value.as_str(),
                _ => {
                    return Some(
                        "Usage: <code>/settings mode &lt;live|cache_only|paused&gt;</code>"
                            .to_owned(),
                    );
                }
            };
            state
                .rip_deps
                .settings()
                .set_setting("ripping_mode", serde_json::json!(mode))
                .await;
            Some(format!("Engine mode set to: <b>{mode}</b>"))
        }
        "apple" | "apple_music" => {
            let val = matches!(normalized_value.as_str(), "on" | "true" | "1");
            settings_store
                .set_setting("apple_rip_enabled", serde_json::json!(val))
                .await;
            Some(format!(
                "Apple Music ripping set to: <b>{}</b>",
                if val { "ON" } else { "OFF" }
            ))
        }
        "album" | "playlist" | "artist" | "txt" | "batch_txt" | "multilink" | "multi_link" => {
            let val = matches!(normalized_value.as_str(), "on" | "true" | "1");
            let (key, label) = match sub {
                "album" => ("album_rip_enabled", "Album ripping"),
                "playlist" => ("playlist_rip_enabled", "Playlist ripping"),
                "artist" => ("artist_rip_enabled", "Artist ripping"),
                "txt" | "batch_txt" => ("txt_rip_enabled", ".TXT batch ripping"),
                _ => ("multi_link_rip_enabled", "Multi-link ripping"),
            };
            settings_store
                .set_setting(key, serde_json::json!(val))
                .await;
            Some(format!(
                "{label} set to: <b>{}</b>",
                if val { "ON" } else { "OFF" }
            ))
        }
        "storefront" | "default_storefront" => {
            let sf = normalized_value.trim().to_ascii_lowercase();
            if sf.len() == 2 && sf.chars().all(|c| c.is_ascii_alphabetic()) {
                settings_store
                    .set_setting("default_storefront", serde_json::json!(sf))
                    .await;
                Some(format!("Default storefront set to: <code>{sf}</code>"))
            } else {
                Some(
                    "Usage: <code>/settings storefront &lt;2-letter country code (e.g. us, in, jp)&gt;</code>"
                        .to_owned(),
                )
            }
        }
        "lyricsporn_api_url" | "lyricsporn_url" | "lyricsporn" => {
            let url = if matches!(
                normalized_value.as_str(),
                "clear" | "none" | "remove" | "reset" | "default"
            ) {
                None
            } else if let Some(url) = engine::settings::normalize_lyricsporn_api_url(raw_value) {
                Some(url)
            } else {
                return Some(
                    "Usage: <code>/settings lyricsporn_url &lt;http(s)://host/api/v1 | clear&gt;</code>"
                        .to_owned(),
                );
            };
            let updated = settings_store
                .set_setting(
                    "lyricsporn_api_url",
                    url.as_ref()
                        .map(|url| serde_json::json!(url))
                        .unwrap_or(serde_json::Value::Null),
                )
                .await;
            if updated.lyricsporn_api_url == url {
                match updated.lyricsporn_api_url.as_deref() {
                    Some(url) => Some(format!(
                        "Lyricsporn API URL set to: <code>{}</code>",
                        escape(url)
                    )),
                    None => Some(
                        "Lyricsporn API URL cleared; catalog, lyrics, and artwork lookups are disabled."
                            .to_owned(),
                    ),
                }
            } else {
                Some("Could not save the Lyricsporn API URL".to_owned())
            }
        }
        "stream_url" | "stream" | "url" => {
            if matches!(
                normalized_value.as_str(),
                "clear" | "none" | "remove" | "reset"
            ) {
                settings_store
                    .set_setting("stream_public_url", serde_json::Value::Null)
                    .await;
                Some("Stream public URL cleared".to_owned())
            } else if normalized_value.starts_with("http://")
                || normalized_value.starts_with("https://")
            {
                let clean = raw_value.trim_end_matches('/');
                settings_store
                    .set_setting("stream_public_url", serde_json::json!(clean))
                    .await;
                Some(format!("Stream public URL set to: <code>{clean}</code>"))
            } else {
                Some(
                    "Usage: <code>/settings stream_url &lt;http(s)://domain.com | clear&gt;</code>"
                        .to_owned(),
                )
            }
        }
        "stream_port" | "port" => {
            if let Ok(port) = raw_value.parse::<u16>() {
                settings_store
                    .set_setting("stream_server_port", serde_json::json!(port))
                    .await;
                Some(format!("Stream server port set to: <b>{port}</b>"))
            } else {
                Some("Usage: <code>/settings stream_port &lt;1024-65535&gt;</code>".to_owned())
            }
        }
        _ => None,
    }
}

pub async fn callback(state: Arc<BotState>, query: CallbackQuery, action: SettingsAction) {
    if !state.auth.is_admin(query.user_id) {
        let _ = query
            .answer()
            .alert("Unauthorized. Owner only.")
            .send(&state.client)
            .await;
        return;
    }

    let peer = query.chat_peer.clone().map(PeerRef::Peer);
    let message_id = query.message_id;

    match action {
        SettingsAction::Close => {
            let _ = query.answer().send(&state.client).await;
            if let (Some(peer), Some(id)) = (peer, message_id) {
                delete_panel_message(state, peer, id).await;
            }
        }
        SettingsAction::Refresh => {
            let _ = query
                .answer()
                .text("Settings refreshed")
                .send(&state.client)
                .await;
            if let (Some(peer), Some(id)) = (peer, message_id) {
                render_settings_view(&state, &peer, SettingPage::Catalog, Some(id), None).await;
            }
        }
        SettingsAction::View(page) => {
            let _ = query.answer().send(&state.client).await;
            if let (Some(peer), Some(id)) = (peer, message_id) {
                render_settings_view(&state, &peer, page, Some(id), None).await;
            }
        }
        SettingsAction::Mode => {
            let new_mode = state.rip_deps.settings().cycle_ripping_mode().await;
            let label = match new_mode {
                engine::settings::RippingMode::Live => "Mode: Live Ripping",
                engine::settings::RippingMode::CacheOnly => "Mode: Cache Only",
                engine::settings::RippingMode::Paused => "Mode: Fully Paused",
            };
            let _ = query.answer().text(label).send(&state.client).await;
            if let (Some(peer), Some(id)) = (peer, message_id) {
                render_settings_view(&state, &peer, SettingPage::Mode, Some(id), None).await;
            }
        }
        SettingsAction::SetMode(mode) => {
            state
                .rip_deps
                .settings()
                .set_setting("ripping_mode", serde_json::json!(mode.as_str()))
                .await;
            let label = match mode {
                engine::settings::RippingMode::Live => "Mode set to: Live Ripping",
                engine::settings::RippingMode::CacheOnly => "Mode set to: Cache Only",
                engine::settings::RippingMode::Paused => "Mode set to: Fully Paused",
            };
            let _ = query.answer().text(label).send(&state.client).await;
            if let (Some(peer), Some(id)) = (peer, message_id) {
                render_settings_view(&state, &peer, SettingPage::Mode, Some(id), None).await;
            }
        }
        SettingsAction::Toggle(feature) => {
            let settings_store = state.rip_deps.settings();
            let (enabled, label, page) = match feature {
                SettingFeature::Apple => (
                    settings_store.toggle_apple().await,
                    "Apple Music",
                    SettingPage::Apple,
                ),
                SettingFeature::Album => (
                    settings_store.toggle_album().await,
                    "Album ripping",
                    SettingPage::Scopes,
                ),
                SettingFeature::Playlist => (
                    settings_store.toggle_playlist().await,
                    "Playlist ripping",
                    SettingPage::Scopes,
                ),
                SettingFeature::Artist => (
                    settings_store.toggle_artist().await,
                    "Artist ripping",
                    SettingPage::Scopes,
                ),
                SettingFeature::Txt => (
                    settings_store.toggle_txt().await,
                    ".TXT batch ripping",
                    SettingPage::Scopes,
                ),
                SettingFeature::MultiLink => (
                    settings_store.toggle_multi_link_rip().await,
                    "Multi-link ripping",
                    SettingPage::Scopes,
                ),
            };
            let _ = query
                .answer()
                .text(format!("{label}: {}", if enabled { "ON" } else { "OFF" }))
                .send(&state.client)
                .await;
            if let (Some(peer), Some(id)) = (peer, message_id) {
                render_settings_view(&state, &peer, page, Some(id), None).await;
            }
        }
        SettingsAction::ClearLyricsporn => {
            state
                .rip_deps
                .settings()
                .set_setting("lyricsporn_api_url", serde_json::Value::Null)
                .await;
            let _ = query
                .answer()
                .text("Lyricsporn API URL cleared")
                .send(&state.client)
                .await;
            if let (Some(peer), Some(id)) = (peer, message_id) {
                render_settings_view(&state, &peer, SettingPage::Lyricsporn, Some(id), None).await;
            }
        }
        SettingsAction::ClearStreamUrl => {
            state
                .rip_deps
                .settings()
                .set_setting("stream_public_url", serde_json::Value::Null)
                .await;
            let _ = query
                .answer()
                .text("Stream public URL cleared")
                .send(&state.client)
                .await;
            if let (Some(peer), Some(id)) = (peer, message_id) {
                render_settings_view(&state, &peer, SettingPage::Stream, Some(id), None).await;
            }
        }
        SettingsAction::SetStorefront(sf) => {
            state
                .rip_deps
                .settings()
                .set_setting("default_storefront", serde_json::json!(sf))
                .await;
            let _ = query
                .answer()
                .text(format!("Storefront set to: {sf}"))
                .send(&state.client)
                .await;
            if let (Some(peer), Some(id)) = (peer, message_id) {
                render_settings_view(&state, &peer, SettingPage::Storefront, Some(id), None).await;
            }
        }
    }
}

async fn delete_panel_message(state: Arc<BotState>, peer: PeerRef, id: i32) {
    if let Ok(messages) = state.client.get_messages(peer, &[id]).await
        && let Some(message) = messages.first()
    {
        let _ = message.delete().await;
    }
}

pub fn register(dp: &mut Dispatcher, state: Arc<BotState>) {
    dp.on_message(filters::command("settings"), move |msg| {
        let state = Arc::clone(&state);
        async move {
            command(state, msg).await;
        }
    });
}
