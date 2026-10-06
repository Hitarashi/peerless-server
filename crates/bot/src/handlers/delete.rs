use std::{
    sync::{Arc, LazyLock},
    time::Duration,
};

use apple::parse_alac_input;
use ferogram::{
    InputMessage, PeerRef, filters,
    filters::Dispatcher,
    keyboard::{Button, InlineKeyboard},
    tl,
    update::CallbackQuery,
};

use crate::{BotState, html::parse_dynamic_html, pending::PendingConfirmations};

const RESTRICTED: &str =
    "! <b>Access restricted</b><br/>This command is restricted to the bot owner.";
const USAGE: &str = "<b>Delete track usage</b><br/><br/><blockquote>• <code>/delete &lt;Apple Music link&gt;</code><br/>• Reply to an Apple Music link with <code>/delete</code></blockquote>";
const CONFIRMATION_TTL: Duration = Duration::from_secs(120);

#[derive(Clone)]
struct PendingDelete {
    user_id: i64,
    track_id: String,
    message_id: i64,
}

fn pending_deletes() -> &'static PendingConfirmations<PendingDelete> {
    static PENDING: LazyLock<PendingConfirmations<PendingDelete>> =
        LazyLock::new(|| PendingConfirmations::new(CONFIRMATION_TTL));
    &PENDING
}

fn confirmation_keyboard(token: &str) -> ferogram::tl::enums::ReplyMarkup {
    InlineKeyboard::new()
        .row([
            Button::callback(
                "Delete track",
                crate::interaction::TelegramAction::ConfirmDelete {
                    token: token.to_owned(),
                }
                .encode()
                .as_bytes(),
            ),
            Button::callback(
                "Cancel",
                crate::interaction::TelegramAction::CancelDelete {
                    token: token.to_owned(),
                }
                .encode()
                .as_bytes(),
            ),
        ])
        .into_markup()
}

async fn reply_text(msg: &ferogram::update::IncomingMessage, state: &BotState) -> Option<String> {
    let reply_header = match &msg.raw {
        tl::enums::Message::Message(m) => m.reply_to.as_ref(),
        tl::enums::Message::Service(m) => m.reply_to.as_ref(),
        _ => None,
    };
    if let Some(tl::enums::MessageReplyHeader::MessageReplyHeader(h)) = reply_header
        && let Some(ref quote) = h.quote_text
        && !quote.trim().is_empty()
    {
        return Some(quote.clone());
    }
    let reply_id = msg.reply_to_message_id()?;
    let peer = msg.peer_id()?.clone();
    let messages = state
        .client
        .get_messages(PeerRef::Peer(peer), &[reply_id])
        .await
        .ok()?;
    messages
        .first()
        .and_then(|message| message.text().map(str::to_owned))
}

async fn delete(msg: ferogram::update::IncomingMessage, state: Arc<BotState>) {
    let sender = msg.sender_user_id().unwrap_or_default();
    if !state.auth.is_admin(sender) {
        let _ = msg
            .reply(InputMessage::html(parse_dynamic_html(RESTRICTED)))
            .await;
        return;
    }

    let reply = reply_text(&msg, &state).await;
    let parsed = parse_alac_input(msg.text().unwrap_or(""), reply.as_deref());
    let Some(parsed) = parsed else {
        let _ = msg
            .reply(InputMessage::html(parse_dynamic_html(USAGE)))
            .await;
        return;
    };
    let track_id = parsed.track_id;

    let cached = match state
        .rip_deps
        .tracks()
        .find_cached_tracks(std::slice::from_ref(&track_id))
        .await
    {
        Ok(tracks) => tracks.into_values().next(),
        Err(error) => {
            tracing::warn!(%error, "failed to find cached track for deletion");
            return;
        }
    };
    let Some(cached) = cached else {
        let text = format!(
            "! <b>Track not found</b><br/>ID <code>{track_id}</code> is not in the database."
        );
        let _ = msg
            .reply(InputMessage::html(parse_dynamic_html(&text)))
            .await;
        return;
    };

    let token = pending_deletes().insert(PendingDelete {
        user_id: sender,
        track_id: track_id.clone(),
        message_id: cached.message_id,
    });
    let text = format!(
        "<b>Delete this cached track?</b><br/><br/><blockquote><b>Details:</b><br/>• Apple ID: <code>{track_id}</code><br/>• Codec: <code>{}</code><br/>• Dump message: <code>#{}</code></blockquote><br/><i>This removes the dump message and database record.</i>",
        cached.codec, cached.message_id
    );
    let _ = msg
        .reply(
            InputMessage::html(parse_dynamic_html(&text))
                .reply_markup(confirmation_keyboard(&token)),
        )
        .await;
}

pub async fn callback(
    state: Arc<BotState>,
    query: CallbackQuery,
    action: crate::interaction::TelegramAction,
) {
    if !state.auth.is_admin(query.user_id) {
        let _ = query
            .answer()
            .alert("Access restricted.")
            .send(&state.client)
            .await;
        return;
    }
    let is_confirm = matches!(
        action,
        crate::interaction::TelegramAction::ConfirmDelete { .. }
    );
    let token = match action {
        crate::interaction::TelegramAction::ConfirmDelete { token }
        | crate::interaction::TelegramAction::CancelDelete { token } => token,
        _ => {
            let _ = query
                .answer()
                .alert("This delete action is unavailable.")
                .send(&state.client)
                .await;
            return;
        }
    };
    let pending = pending_deletes().take(&token);
    let Some(pending) = pending else {
        let _ = query
            .answer()
            .alert("This action has expired. Run /delete again.")
            .send(&state.client)
            .await;
        return;
    };
    if pending.user_id != query.user_id || !is_confirm {
        let _ = query.answer().send(&state.client).await;
        if !is_confirm {
            delete_query_message(&state, &query).await;
        }
        return;
    }
    let cached = state
        .rip_deps
        .tracks()
        .find_cached_tracks(std::slice::from_ref(&pending.track_id))
        .await
        .ok()
        .and_then(|tracks| tracks.into_values().next());
    let Some(cached) = cached else {
        let _ = query
            .answer()
            .alert("Track is no longer cached.")
            .send(&state.client)
            .await;
        delete_query_message(&state, &query).await;
        return;
    };
    if cached.message_id != pending.message_id {
        let _ = query
            .answer()
            .alert("This action is stale. Run /delete again.")
            .send(&state.client)
            .await;
        delete_query_message(&state, &query).await;
        return;
    }
    let _ = query
        .answer()
        .text("Deleting track")
        .send(&state.client)
        .await;
    if let Err(error) = delete_dump_message(&state, cached.message_id).await {
        tracing::warn!(%error, track_id = %pending.track_id, "failed to delete dump message; database row retained");
        edit_query(
            &state,
            &query,
            "<b>Track was not deleted</b><br/>The dump message could not be removed. Try again.",
        )
        .await;
        return;
    }
    if let Err(error) = state
        .rip_deps
        .tracks()
        .delete_track(&pending.track_id, Some(cached.codec))
        .await
    {
        tracing::warn!(%error, track_id = %pending.track_id, "failed to delete cached track");
        edit_query(
            &state,
            &query,
            "<b>Track was not deleted</b><br/>The database record could not be removed. Try again.",
        )
        .await;
        return;
    }
    let text = format!(
        "<b>Track deleted</b><br/><br/><blockquote>• Apple ID: <code>{}</code><br/>• Codec: <code>{}</code><br/>• Removed from the database and dump channel.</blockquote>",
        pending.track_id, cached.codec,
    );
    edit_query(&state, &query, &text).await;
}

async fn edit_query(state: &BotState, query: &CallbackQuery, text: &str) {
    if let (Some(peer), Some(message_id)) =
        (query.chat_peer.clone().map(PeerRef::Peer), query.message_id)
    {
        let _ = state
            .client
            .edit_message(
                peer,
                message_id,
                InputMessage::html(parse_dynamic_html(text)),
            )
            .await;
    }
}

async fn delete_query_message(state: &BotState, query: &CallbackQuery) {
    if let (Some(peer), Some(message_id)) =
        (query.chat_peer.clone().map(PeerRef::Peer), query.message_id)
        && let Ok(messages) = state.client.get_messages(peer, &[message_id]).await
        && let Some(message) = messages.first()
    {
        let _ = message.delete().await;
    }
}

async fn delete_dump_message(state: &BotState, message_id: i64) -> Result<(), String> {
    let message_id = i32::try_from(message_id)
        .map_err(|error| format!("dump message id out of range: {error}"))?;
    let messages = state
        .client
        .get_messages(state.dump_peer.clone(), &[message_id])
        .await
        .map_err(|error| error.to_string())?;
    let Some(message) = messages.first() else {
        return Ok(());
    };
    message.delete().await.map_err(|error| error.to_string())
}

pub fn register(dp: &mut Dispatcher, state: Arc<BotState>) {
    dp.on_message(filters::command("delete"), move |msg| {
        delete(msg, Arc::clone(&state))
    });
}
