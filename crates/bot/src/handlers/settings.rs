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
    interaction::{SettingFeature, SettingsAction},
};

const LIMIT_PRESETS: [u32; 4] = [25, 50, 100, 0];

fn mode_button_label(mode: engine::settings::RippingMode) -> &'static str {
    use engine::settings::RippingMode::{CacheOnly, Live, Paused};
    match mode {
        Live => "Mode: Live ripping",
        CacheOnly => "Mode: Cache only",
        Paused => "Mode: Fully paused",
    }
}

fn toggle_label(label: &str, enabled: bool) -> String {
    format!("{label}: {}", if enabled { "ON" } else { "OFF" })
}

fn settings_keyboard(settings: &engine::settings::BotSettings) -> ferogram::tl::enums::ReplyMarkup {
    let limit_buttons = LIMIT_PRESETS
        .iter()
        .map(|&preset| {
            let label = if preset == 0 {
                "Unlimited"
            } else {
                &preset.to_string()
            };
            let text = if settings.max_collection_tracks == preset {
                format!("Selected · {label}")
            } else {
                label.to_owned()
            };
            Button::callback(text, format!("settings:limit:{preset}").as_bytes())
        })
        .collect::<Vec<_>>();

    let mut kb = InlineKeyboard::new()
        .row([Button::callback(
            mode_button_label(settings.ripping_mode),
            b"settings:mode",
        )])
        .row([Button::callback(
            toggle_label("Apple", settings.apple_rip_enabled),
            b"settings:apple",
        )])
        .row([
            Button::callback(
                toggle_label("Albums", settings.album_rip_enabled),
                b"settings:album",
            ),
            Button::callback(
                toggle_label("Playlists", settings.playlist_rip_enabled),
                b"settings:playlist",
            ),
            Button::callback(
                toggle_label("Artists", settings.artist_rip_enabled),
                b"settings:artist",
            ),
        ])
        .row([
            Button::callback(
                toggle_label(".TXT Batch", settings.txt_rip_enabled),
                b"settings:txt",
            ),
            Button::callback(
                toggle_label("Multi-Link", settings.multi_link_rip_enabled),
                b"settings:multilink",
            ),
        ]);
    kb = kb.row(limit_buttons);
    kb.row([
        Button::callback("Refresh", b"settings:refresh"),
        Button::callback("Close", b"settings:close"),
    ])
    .into_markup()
}

fn mode_description(mode: engine::settings::RippingMode) -> &'static str {
    use engine::settings::RippingMode::{CacheOnly, Live, Paused};
    match mode {
        Live => "<b>Live ripping</b> (Cache hits and live decryption)",
        CacheOnly => "<b>Cache only</b> (Serves cached songs; live decryption blocked)",
        Paused => "! <b>Paused</b> (Ripping commands suspended for regular users)",
    }
}

pub fn render_settings_text(settings: &engine::settings::BotSettings) -> String {
    let limit_text = if settings.max_collection_tracks == 0 {
        "Unlimited".to_owned()
    } else {
        format!("{} tracks", settings.max_collection_tracks)
    };
    let lyricsporn_url = settings
        .lyricsporn_api_url
        .as_deref()
        .map(escape)
        .unwrap_or_else(|| "Not configured".to_owned());
    format!(
        "<b>Bot settings and operation controls</b><br/><br/>\
• <b>Engine Mode:</b> {}<br/>\
• <b>Apple Music Ripping:</b> {}<br/>\
• <b>Album Ripping:</b> {}<br/>\
• <b>Playlist Ripping:</b> {}<br/>\
• <b>Artist Ripping:</b> {}<br/>\
• <b>.TXT File Ripping:</b> {}<br/>\
• <b>Multi-Link Ripping:</b> {}<br/>\
• <b>Max Collection Limit:</b> <code>{limit_text}</code><br/>\
• <b>Lyricsporn API URL:</b> <code>{lyricsporn_url}</code><br/>\
• <b>Stream Public URL:</b> <code>{}</code><br/>\
• <b>Stream Server Port:</b> <code>{}</code><br/><br/>\
<blockquote><i>Use the buttons below to toggle settings. Set or clear the API URL with <code>/settings lyricsporn_url &lt;URL|clear&gt;</code>. Owner requests bypass ripping limits.</i></blockquote>",
        mode_description(settings.ripping_mode),
        flag(settings.apple_rip_enabled),
        flag(settings.album_rip_enabled),
        flag(settings.playlist_rip_enabled),
        flag(settings.artist_rip_enabled),
        flag(settings.txt_rip_enabled),
        flag(settings.multi_link_rip_enabled),
        settings
            .stream_public_url
            .as_deref()
            .unwrap_or("Not configured"),
        settings.stream_server_port,
    )
}

fn flag(enabled: bool) -> &'static str {
    if enabled { "Enabled" } else { "Disabled" }
}

pub(crate) async fn render_settings_message(
    state: &BotState,
    peer: &PeerRef,
    message_id: Option<i32>,
    reply_to: Option<i32>,
) {
    let settings = state.rip_deps.settings_snapshot();
    let input = InputMessage::html(parse_dynamic_html(&render_settings_text(&settings)))
        .reply_markup(settings_keyboard(&settings));
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
        "limit" => {
            let num: i64 = match raw_value.parse() {
                Ok(num) => num,
                Err(_) => {
                    return Some(
                        "Usage: <code>/settings limit &lt;number (0 for unlimited)&gt;</code>"
                            .to_owned(),
                    );
                }
            };
            if num >= 0 {
                let updated = settings_store.set_max_collection_tracks(num).await;
                Some(format!(
                    "Max collection limit set to: <b>{}</b>",
                    if updated == 0 {
                        "Unlimited".to_owned()
                    } else {
                        updated.to_string()
                    }
                ))
            } else {
                Some(
                    "Usage: <code>/settings limit &lt;number (0 for unlimited)&gt;</code>"
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
                render_settings_message(&state, &peer, Some(id), None).await;
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
                render_settings_message(&state, &peer, Some(id), None).await;
            }
        }
        SettingsAction::Toggle(feature) => {
            let settings_store = state.rip_deps.settings();
            let (enabled, label) = match feature {
                SettingFeature::Apple => (settings_store.toggle_apple().await, "Apple Music"),
                SettingFeature::Album => (settings_store.toggle_album().await, "Album ripping"),
                SettingFeature::Playlist => {
                    (settings_store.toggle_playlist().await, "Playlist ripping")
                }
                SettingFeature::Artist => (settings_store.toggle_artist().await, "Artist ripping"),
                SettingFeature::Txt => (settings_store.toggle_txt().await, ".TXT batch ripping"),
                SettingFeature::MultiLink => (
                    settings_store.toggle_multi_link_rip().await,
                    "Multi-link ripping",
                ),
            };
            let _ = query
                .answer()
                .text(format!("{label}: {}", if enabled { "ON" } else { "OFF" }))
                .send(&state.client)
                .await;
            if let (Some(peer), Some(id)) = (peer, message_id) {
                render_settings_message(&state, &peer, Some(id), None).await;
            }
        }
        SettingsAction::Limit(limit) => {
            state
                .rip_deps
                .settings()
                .set_max_collection_tracks(i64::from(limit))
                .await;
            let _ = query
                .answer()
                .text(format!(
                    "Collection limit: {}",
                    if limit == 0 {
                        "Unlimited".to_owned()
                    } else {
                        format!("{limit} tracks")
                    }
                ))
                .send(&state.client)
                .await;
            if let (Some(peer), Some(id)) = (peer, message_id) {
                render_settings_message(&state, &peer, Some(id), None).await;
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
