use std::sync::Arc;

use engine::{
    orchestrator::deps::{ChatDelivery, ChatRef, Delivery, DumpMessageRef, TrackCache},
    types::{ParsedTargetItem, TargetKind},
};
use ferogram::{
    InputMessage, PeerRef,
    filters::{self, Dispatcher},
    keyboard::{Button, InlineKeyboard},
    update::{CallbackQuery, IncomingMessage},
};

use crate::{
    BotState,
    html::{escape, parse_dynamic_html},
    interaction::TelegramAction,
};

fn short_title(artist: &str, title: &str) -> String {
    let raw = format!("{artist} - {title}");
    if raw.chars().count() > 28 {
        let truncated: String = raw.chars().take(25).collect();
        format!("{truncated}...")
    } else {
        raw
    }
}

fn build_results_html(query: &str, results: &[music::PlaylistTrack]) -> String {
    let lines = results
        .iter()
        .enumerate()
        .map(|(index, track)| {
            format!(
                "{}. <b>{}</b> — <i>{}</i>",
                index + 1,
                escape(&track.title),
                escape(&track.artist)
            )
        })
        .collect::<Vec<_>>()
        .join("<br/>");
    format!(
        "<b>Search results for \"<i>{}</i>\":</b><br/><br/><blockquote>{lines}</blockquote><br/><br/><i>Choose a track below to play or download.</i>",
        escape(query)
    )
}

fn build_results_keyboard(results: &[music::PlaylistTrack]) -> ferogram::tl::enums::ReplyMarkup {
    let mut kb = InlineKeyboard::new();
    for (index, track) in results.iter().enumerate() {
        kb = kb.row(vec![Button::callback(
            format!(
                "{}. {}",
                index + 1,
                short_title(&track.artist, &track.title)
            ),
            crate::interaction::TelegramAction::Get {
                track_id: track.id.clone(),
            }
            .encode()
            .as_bytes(),
        )]);
    }
    kb = kb.row(vec![Button::callback("Close", b"search_close")]);
    kb.into_markup()
}

const PAUSED: &str =
    "! <b>Service is temporarily paused for maintenance.</b><br/>Please try again later.";

pub fn register(dp: &mut Dispatcher, state: Arc<BotState>) {
    let search_state = Arc::clone(&state);
    dp.on_message(filters::command("search"), move |msg| {
        search(Arc::clone(&search_state), msg)
    });
}

async fn search(state: Arc<BotState>, msg: IncomingMessage) {
    let sender = msg.sender_user_id().unwrap_or_default();
    let marked_chat = super::marked_chat_id(&msg);
    if !state
        .auth
        .is_authorized(sender, Some(marked_chat))
        .await
        .unwrap_or(false)
    {
        return;
    }
    let is_admin = state.auth.is_admin(sender);
    if !state.rip_deps.settings_snapshot().can_serve_cache(is_admin) {
        let _ = msg
            .reply(InputMessage::html(parse_dynamic_html(PAUSED)))
            .await;
        return;
    }

    let query = msg
        .text()
        .unwrap_or_default()
        .split_whitespace()
        .skip(1)
        .collect::<Vec<_>>()
        .join(" ");
    if query.is_empty() {
        let usage = "<b>Search music</b><br/><br/><blockquote><b>Usage:</b> <code>/search &lt;track title or artist&gt;</code><br/><i>Searches the Apple Music catalog.</i></blockquote>";
        let _ = msg
            .reply(InputMessage::html(parse_dynamic_html(usage)))
            .await;
        return;
    }

    let settings = state.rip_deps.settings().get_settings();
    let default_storefront = engine::settings::resolve_default_storefront(&settings);
    let results = state
        .rip_deps
        .playlist()
        .search_catalog(&query, 10, default_storefront)
        .await
        .unwrap_or_default();

    if results.is_empty() {
        let text = format!(
            "<b>No tracks found for \"{}\".</b><br/>Try a different title or artist.",
            escape(&query)
        );
        let _ = msg
            .reply(InputMessage::html(parse_dynamic_html(&text)))
            .await;
        return;
    }

    let text = build_results_html(&query, &results);
    let keyboard = build_results_keyboard(&results);

    let _ = msg
        .reply(InputMessage::html(parse_dynamic_html(&text)).reply_markup(keyboard))
        .await;
}

pub async fn callback(state: Arc<BotState>, query: CallbackQuery, action: TelegramAction) {
    match action {
        TelegramAction::SearchClose => {
            close(state, query).await;
        }
        TelegramAction::DeliverCached { track_id } | TelegramAction::Get { track_id } => {
            get(state, query, track_id).await;
        }
        _ => {
            let _ = query
                .answer()
                .alert("This search action is unavailable. Run /search again.")
                .send(&state.client)
                .await;
        }
    }
}

async fn close(state: Arc<BotState>, query: CallbackQuery) {
    let _ = query.answer().send(&state.client).await;
    delete_query_message(&state, &query).await;
}

async fn get(state: Arc<BotState>, query: CallbackQuery, track_id: String) {
    let marked_chat = query
        .chat_peer
        .as_ref()
        .map(super::marked_peer_id)
        .unwrap_or(query.user_id);
    if !state
        .auth
        .is_authorized(query.user_id, Some(marked_chat))
        .await
        .unwrap_or(false)
    {
        let _ = query
            .answer()
            .alert("Unauthorized")
            .send(&state.client)
            .await;
        return;
    }
    let is_admin = state.auth.is_admin(query.user_id);

    let cached = state
        .rip_deps
        .find_cached_tracks(std::slice::from_ref(&track_id))
        .await
        .ok()
        .and_then(|map| map.into_values().next());
    if let Some(cached) = cached {
        if !state.rip_deps.settings_snapshot().can_serve_cache(is_admin) {
            let _ = query
                .answer()
                .alert("Service is temporarily paused for maintenance.")
                .send(&state.client)
                .await;
            return;
        }
        let _ = query
            .answer()
            .text("Already cached. Delivering track.")
            .send(&state.client)
            .await;
        let _ = state
            .rip_deps
            .deliver_to_chat(ChatDelivery::DumpCopy {
                destination: ChatRef::new(query.user_id),
                source: DumpMessageRef::new(cached.message_id),
                reply_to: None,
                silent: false,
            })
            .await;
        delete_query_message(&state, &query).await;
        return;
    }

    if !state.rip_deps.settings_snapshot().can_rip_live(is_admin) {
        let _ = query
            .answer()
            .alert("Live ripping is temporarily paused. Only cached tracks can be delivered.")
            .send(&state.client)
            .await;
        return;
    }

    let _ = query
        .answer()
        .text("Queuing lossless download")
        .send(&state.client)
        .await;

    let peer = query
        .chat_peer
        .as_ref()
        .map(|peer| PeerRef::Peer(peer.clone()))
        .unwrap_or_else(|| PeerRef::from(query.user_id));
    super::ensure_dashboard(&state, marked_chat, query.user_id, is_admin, peer).await;

    let user_display =
        crate::presentation::resolve_user_display_name(&state.client, query.user_id).await;
    let options = engine::orchestrator::types::RipTaskOptions {
        provider: engine::Provider::Apple,
        chat_id: marked_chat,
        user_id: query.user_id,
        user_name: Some(user_display),
        delivery_chat_id: query.user_id,
        is_group: marked_chat != query.user_id,
        is_force: false,
        is_cache_only: false,
        single_storefront: None,
        parsed_items: vec![ParsedTargetItem {
            id: track_id.clone(),
            kind: TargetKind::Track,
            storefront: None,
        }],
        reply_to_message_id: None,
        is_admin,
        codec_preference: None,
        rendition_policy: engine::orchestrator::types::RenditionPolicy::PrimaryOnly,
    };
    match state
        .rip_orchestrator
        .start_task(Arc::clone(&state.rip_deps), &options)
        .await
    {
        Ok(_) => {
            delete_query_message(&state, &query).await;
        }
        Err(error) => {
            tracing::warn!(track_id, %error, "search get job failed");
        }
    }
}

async fn delete_query_message(state: &BotState, query: &CallbackQuery) {
    let Some(message_id) = query.message_id else {
        return;
    };
    let Some(peer) = query.chat_peer.as_ref() else {
        return;
    };
    let peer = PeerRef::Peer(peer.clone());
    if let Ok(messages) = state.client.get_messages(peer, &[message_id]).await
        && let Some(message) = messages.first()
    {
        let _ = message.delete().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn short_title_truncates_with_ellipsis() {
        assert_eq!(short_title("A", "B"), "A - B");

        let b28 = "B".repeat(20);
        assert_eq!(short_title("AAAAA", &b28), format!("AAAAA - {b28}"));

        let over = format!("AAAAA - {}", "B".repeat(21));
        assert_eq!(
            short_title("AAAAA", &"B".repeat(21)),
            format!("{}...", over.chars().take(25).collect::<String>())
        );
        let far_over = format!("AAAAA - {}", "B".repeat(50));
        assert_eq!(
            short_title("AAAAA", &"B".repeat(50)),
            format!("{}...", far_over.chars().take(25).collect::<String>())
        );
    }

    #[test]
    fn results_html_is_exact() {
        let results = vec![
            music::PlaylistTrack {
                id: "1".into(),
                title: "Track One".into(),
                artist: "Artist A".into(),
                duration: Some(180),
            },
            music::PlaylistTrack {
                id: "2".into(),
                title: "Track Two".into(),
                artist: "Artist B".into(),
                duration: Some(200),
            },
        ];
        let text = build_results_html("query", &results);
        assert_eq!(
            text,
            "<b>Search results for \"<i>query</i>\":</b><br/><br/><blockquote>1. <b>Track One</b> — <i>Artist A</i><br/>2. <b>Track Two</b> — <i>Artist B</i></blockquote><br/><br/><i>Choose a track below to play or download.</i>"
        );
    }

    #[test]
    fn results_html_escapes_special_chars() {
        let results = vec![music::PlaylistTrack {
            id: "1".into(),
            title: "Rock & Roll <3>".into(),
            artist: "AC/DC & \"Friends\"".into(),
            duration: Some(180),
        }];
        let text = build_results_html("rock & roll", &results);
        assert_eq!(
            text,
            "<b>Search results for \"<i>rock &amp; roll</i>\":</b><br/><br/><blockquote>1. <b>Rock &amp; Roll &lt;3&gt;</b> — <i>AC/DC &amp; &quot;Friends&quot;</i></blockquote><br/><br/><i>Choose a track below to play or download.</i>"
        );
    }
}
