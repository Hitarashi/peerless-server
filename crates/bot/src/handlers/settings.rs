//! `/settings` — admin settings UI .
//!
//! Two surfaces:
//! - `/settings` renders the inline keyboard panel; subcommands
//!   (`/settings mode|album|...|storefronts|limit`) mutate a single key and
//!   answer with the exact confirmation text.
//! - `settings:*` callbacks drive the panel (toggle/cycle/pick) and the
//!   storefront sub-menu, always re-rendering the panel after a change.
//!
//! Authorization is admin-only on both surfaces, re-checked in every
//! callback (owner id from `BotState::auth`).

use std::sync::Arc;

use ferogram::{
    InputMessage, PeerRef, filters,
    filters::Dispatcher,
    keyboard::{Button, InlineKeyboard},
    update::CallbackQuery,
};

use crate::{
    BotState,
    html::parse_dynamic_html,
    interaction::{SettingFeature, SettingsAction},
};

/// Storefronts offered in the auto-dump picker.
pub const POPULAR_STOREFRONTS: [&str; 8] = ["us", "gb", "jp", "in", "ca", "au", "de", "fr"];

/// Collection-size limit presets.
const LIMIT_PRESETS: [u32; 4] = [25, 50, 100, 0];

/// Short label per ripping mode.
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

/// Build the main settings inline keyboard.
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
        .row([
            Button::callback(
                toggle_label("Apple", settings.apple_rip_enabled),
                b"settings:apple",
            ),
            Button::callback(
                toggle_label("Qobuz", settings.qobuz_rip_enabled),
                b"settings:qobuz",
            ),
        ])
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
        ])
        .row([
            Button::callback(
                toggle_label("Auto-Dump", settings.auto_dump_enabled),
                b"settings:autodump",
            ),
            Button::callback(
                format!("Storefronts ({})", settings.auto_dump_storefronts.len()),
                b"settings:sf_menu",
            ),
        ]);
    kb = kb.row(limit_buttons);
    kb.row([
        Button::callback("Refresh", b"settings:refresh"),
        Button::callback("Close", b"settings:close"),
    ])
    .into_markup()
}

/// Build the storefront picker inline keyboard.
fn storefronts_keyboard(
    settings: &engine::settings::BotSettings,
) -> ferogram::tl::enums::ReplyMarkup {
    let active: std::collections::HashSet<String> = settings
        .auto_dump_storefronts
        .iter()
        .map(|sf| sf.to_lowercase())
        .collect();
    let mut kb = InlineKeyboard::new();
    for chunk in POPULAR_STOREFRONTS.chunks(4) {
        let row = chunk
            .iter()
            .map(|&sf| {
                let active = active.contains(sf.to_lowercase().as_str());
                let label = if active {
                    format!("Selected · {}", sf.to_uppercase())
                } else {
                    sf.to_uppercase()
                };
                Button::callback(label, format!("settings:sf:toggle:{sf}").as_bytes())
            })
            .collect::<Vec<_>>();
        kb = kb.row(row);
    }
    kb.row([
        Button::callback("Back to settings", b"settings:refresh"),
        Button::callback("Close", b"settings:close"),
    ])
    .into_markup()
}

/// One-line description per ripping mode.
fn mode_description(mode: engine::settings::RippingMode) -> &'static str {
    use engine::settings::RippingMode::{CacheOnly, Live, Paused};
    match mode {
        Live => "<b>Live ripping</b> (Cache hits and live decryption)",
        CacheOnly => "<b>Cache only</b> (Serves cached songs; live decryption blocked)",
        Paused => "! <b>Paused</b> (Ripping commands suspended for regular users)",
    }
}

/// Render the settings panel text.
pub fn render_settings_text(settings: &engine::settings::BotSettings) -> String {
    let limit_text = if settings.max_collection_tracks == 0 {
        "Unlimited".to_owned()
    } else {
        format!("{} tracks", settings.max_collection_tracks)
    };
    let sf_list = settings
        .auto_dump_storefronts
        .iter()
        .map(|sf| sf.to_uppercase())
        .collect::<Vec<_>>()
        .join(", ");

    format!(
        "<b>Bot settings and operation controls</b><br/><br/>\
• <b>Engine Mode:</b> {}<br/>\
• <b>Apple Music Ripping:</b> {}<br/>\
• <b>Qobuz Ripping:</b> {}<br/>\
• <b>Album Ripping:</b> {}<br/>\
• <b>Playlist Ripping:</b> {}<br/>\
• <b>Artist Ripping:</b> {}<br/>\
• <b>.TXT File Ripping:</b> {}<br/>\
• <b>Multi-Link Ripping:</b> {}<br/>\
• <b>Auto-Dump New Music:</b> {}<br/>\
• <b>Auto-Dump Storefronts:</b> <code>{sf_list}</code><br/>\
• <b>Max Collection Limit:</b> <code>{limit_text}</code><br/>\
• <b>Stream Public URL:</b> <code>{}</code><br/>\
• <b>Stream Server Port:</b> <code>{}</code><br/><br/>\
<blockquote><i>Use the buttons below to toggle settings. Owner requests bypass these limits.</i></blockquote>",
        mode_description(settings.ripping_mode),
        flag(settings.apple_rip_enabled),
        flag(settings.qobuz_rip_enabled),
        flag(settings.album_rip_enabled),
        flag(settings.playlist_rip_enabled),
        flag(settings.artist_rip_enabled),
        flag(settings.txt_rip_enabled),
        flag(settings.multi_link_rip_enabled),
        if settings.auto_dump_enabled {
            "Enabled (daily)"
        } else {
            "Disabled"
        },
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

/// Render the storefronts panel text.
pub fn render_storefronts_text(settings: &engine::settings::BotSettings) -> String {
    let sf_list = settings
        .auto_dump_storefronts
        .iter()
        .map(|sf| sf.to_uppercase())
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "<b>Auto-dump storefront configuration</b><br/><br/>\
• <b>Active Storefronts:</b> <code>{sf_list}</code><br/><br/>\
Tap a country below to toggle it on or off for the daily new music auto-dump.<br/>\
<i>You can also use:</i> <code>/settings storefronts add &lt;code&gt;</code>"
    )
}

/// Edit the settings panel message in place (ignore NOT_MODIFIED),
/// place when a message id is given, else send fresh (replying when asked).
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

/// Edit the storefronts panel message in place.
pub(crate) async fn render_storefronts_message(state: &BotState, peer: &PeerRef, message_id: i32) {
    let settings = state.rip_deps.settings_snapshot();
    let input = InputMessage::html(parse_dynamic_html(&render_storefronts_text(&settings)))
        .reply_markup(storefronts_keyboard(&settings));
    let _ = state
        .client
        .edit_message(peer.clone(), message_id, input)
        .await;
}

/// `/settings [subcommand]` — registerSettingsCommands
/// admin gate, subcommand mutations, else render.
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
        let raw_value = parts[2].to_lowercase();

        if let Some(reply) = subcommand_reply(Arc::clone(&state), &sub, &raw_value, &parts).await {
            let _ = msg
                .reply(InputMessage::html(parse_dynamic_html(&reply)))
                .await;
            return;
        }
    }

    render_settings_message(&state, &peer, None, Some(msg.id())).await;
}

/// The `/settings <sub> <value>...` mutation arms. Returns the exact
/// answer text on success (Some), or None to fall through to the panel.
async fn subcommand_reply(
    state: Arc<BotState>,
    sub: &str,
    raw_value: &str,
    parts: &[&str],
) -> Option<String> {
    let settings_store = state.rip_deps.settings();

    match sub {
        "mode" => {
            let mode = match raw_value {
                "live" | "cache_only" | "paused" => raw_value,
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
            let val = matches!(raw_value, "on" | "true" | "1");
            settings_store
                .set_setting("apple_rip_enabled", serde_json::json!(val))
                .await;
            Some(format!(
                "Apple Music ripping set to: <b>{}</b>",
                if val { "ON" } else { "OFF" }
            ))
        }
        "qobuz" => {
            let val = matches!(raw_value, "on" | "true" | "1");
            settings_store
                .set_setting("qobuz_rip_enabled", serde_json::json!(val))
                .await;
            Some(format!(
                "Qobuz ripping set to: <b>{}</b>",
                if val { "ON" } else { "OFF" }
            ))
        }
        "album" | "playlist" | "artist" | "txt" | "batch_txt" | "multilink" | "multi_link" => {
            let val = matches!(raw_value, "on" | "true" | "1");
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
        "autodump" | "auto_dump" => {
            let val = matches!(raw_value, "on" | "true" | "1");
            settings_store
                .set_setting("auto_dump_enabled", serde_json::json!(val))
                .await;
            Some(format!(
                "Auto-dump new music set to: <b>{}</b>",
                if val { "ON" } else { "OFF" }
            ))
        }
        "storefronts" | "storefront" | "sf" => {
            let action = raw_value;
            let target_sf = parts.get(3).map(|s| s.to_lowercase());
            match (action, target_sf) {
                ("add", Some(target_sf)) => {
                    let list = settings_store.add_auto_dump_storefront(&target_sf).await;
                    Some(format!(
                        "Added <b>{}</b>. Storefronts: <code>{}</code>",
                        target_sf.to_uppercase(),
                        upper_join(&list)
                    ))
                }
                ("remove" | "rm" | "del", Some(target_sf)) => {
                    let list = settings_store.remove_auto_dump_storefront(&target_sf).await;
                    Some(format!(
                        "Removed <b>{}</b>. Storefronts: <code>{}</code>",
                        target_sf.to_uppercase(),
                        upper_join(&list)
                    ))
                }
                ("set", _) => {
                    let targets = parts[3..]
                        .iter()
                        .flat_map(|part| part.split(','))
                        .map(str::trim)
                        .filter(|s| !s.is_empty())
                        .map(str::to_owned)
                        .collect::<Vec<_>>();
                    let list = settings_store.set_auto_dump_storefronts(&targets).await;
                    Some(format!(
                        "Storefronts set to: <code>{}</code>",
                        upper_join(&list)
                    ))
                }
                _ => Some(
                    "Usage: <code>/settings storefronts &lt;add|rm|set&gt; &lt;code&gt;</code>"
                        .to_owned(),
                ),
            }
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
        "stream_url" | "stream" | "url" => {
            if matches!(raw_value, "clear" | "none" | "remove" | "reset") {
                settings_store
                    .set_setting("stream_public_url", serde_json::Value::Null)
                    .await;
                Some("Stream public URL cleared".to_owned())
            } else if raw_value.starts_with("http://") || raw_value.starts_with("https://") {
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

fn upper_join(list: &[String]) -> String {
    list.iter()
        .map(|sf| sf.to_uppercase())
        .collect::<Vec<_>>()
        .join(", ")
}

/// `settings:*` callback handler.
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
        SettingsAction::Storefronts => {
            let _ = query.answer().send(&state.client).await;
            if let (Some(peer), Some(id)) = (peer, message_id) {
                render_storefronts_message(&state, &peer, id).await;
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
                SettingFeature::Qobuz => (settings_store.toggle_qobuz().await, "Qobuz"),
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
                SettingFeature::AutoDump => (settings_store.toggle_auto_dump().await, "Auto-dump"),
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
        SettingsAction::ToggleStorefront(sf) => {
            let current = state.rip_deps.settings_snapshot().auto_dump_storefronts;
            if current.iter().any(|value| value.eq_ignore_ascii_case(&sf)) {
                state
                    .rip_deps
                    .settings()
                    .remove_auto_dump_storefront(&sf)
                    .await;
                let _ = query
                    .answer()
                    .text(format!("Removed {}", sf.to_uppercase()))
                    .send(&state.client)
                    .await;
            } else {
                state
                    .rip_deps
                    .settings()
                    .add_auto_dump_storefront(&sf)
                    .await;
                let _ = query
                    .answer()
                    .text(format!("Added {}", sf.to_uppercase()))
                    .send(&state.client)
                    .await;
            }
            if let (Some(peer), Some(id)) = (peer, message_id) {
                render_storefronts_message(&state, &peer, id).await;
            }
        }
    }
}

/// Channel-aware panel deletion, same pattern as the auth-list close button.
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
