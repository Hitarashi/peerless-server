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
    interaction::TelegramAction,
};

const PAGE_SIZE: usize = 20;

fn keyboard(page: usize, pages: usize) -> ferogram::tl::enums::ReplyMarkup {
    if pages <= 1 {
        return InlineKeyboard::new()
            .row([
                Button::callback("Refresh", format!("authpage:{page}").as_bytes()),
                Button::callback("Close", b"authclose"),
            ])
            .into_markup();
    }
    let mut nav = Vec::new();
    if page > 1 {
        nav.push(Button::callback(
            "< Prev",
            format!("authpage:{}", page - 1).as_bytes(),
        ));
    }
    nav.push(Button::callback(format!("{page} / {pages}"), b"noop"));
    if page < pages {
        nav.push(Button::callback(
            "Next >",
            format!("authpage:{}", page + 1).as_bytes(),
        ));
    }
    InlineKeyboard::new()
        .row(nav)
        .row([
            Button::callback("Refresh", format!("authpage:{page}").as_bytes()),
            Button::callback("Close", b"authclose"),
        ])
        .into_markup()
}

pub(crate) async fn render(
    state: &BotState,
    peer: PeerRef,
    message_id: Option<i32>,
    requested: usize,
    reply: Option<&ferogram::update::IncomingMessage>,
) {
    let Ok(items) = state.auth.list_authorized().await else {
        return;
    };
    if items.is_empty() {
        let input = InputMessage::html(parse_dynamic_html(
            "<b>No users or groups are authorized yet.</b><br/>Use <code>/auth &lt;id | @username&gt;</code> to grant access.",
        ));
        if let Some(msg) = reply {
            let _ = msg.reply(input).await;
        } else if let Some(id) = message_id {
            let _ = state.client.edit_message(peer, id, input).await;
        }
        return;
    }
    let pages = items.len().div_ceil(PAGE_SIZE);
    let page = requested.clamp(1, pages);
    let start = (page - 1) * PAGE_SIZE;
    let mut rows = Vec::new();
    for (index, item) in items.iter().skip(start).take(PAGE_SIZE).enumerate() {
        let icon = if item.telegram_id < 0 { "👥" } else { "👤" };
        let name = escape(item.name.as_deref().unwrap_or("Unknown"));
        let link = super::peer_link(item.name.as_deref().unwrap_or("Unknown"), item.telegram_id);
        rows.push(format!(
            "{}. {icon} <a href=\"{link}\">{name}</a> — <code>{}</code>",
            start + index + 1,
            item.telegram_id
        ));
    }
    let footer = if pages > 1 {
        format!("<i>Page {page}/{pages} • Total: {}</i>", items.len())
    } else {
        format!("<i>Total: {}</i>", items.len())
    };
    let text = format!(
        "👥 <b>Authorized Users & Groups</b><br/>{footer}<br/><br/><blockquote>{}</blockquote>",
        rows.join("<br/>")
    );
    let input = InputMessage::html(parse_dynamic_html(&text)).reply_markup(keyboard(page, pages));
    if let Some(message_id) = message_id {
        // Edit: swallow "message not modified" style errors.
        let _ = state.client.edit_message(peer, message_id, input).await;
    } else if let Some(msg) = reply {
        let _ = msg.reply(input).await;
    }
}

pub fn register(dp: &mut Dispatcher, state: Arc<BotState>) {
    dp.on_message(filters::command("authlist"), move |msg| {
        let state = Arc::clone(&state);
        async move {
            if state
                .auth
                .is_admin(msg.sender_user_id().unwrap_or_default())
            {
                let page = msg
                    .text()
                    .and_then(|t| t.split_whitespace().nth(1))
                    .and_then(|p| p.parse().ok())
                    .unwrap_or(1);
                if let Some(peer) = msg.peer_id() {
                    render(&state, PeerRef::Peer(peer.clone()), None, page, Some(&msg)).await;
                }
            }
        }
    });
}

pub async fn callback(state: Arc<BotState>, query: CallbackQuery, action: TelegramAction) {
    if !state.auth.is_admin(query.user_id) {
        let _ = query
            .answer()
            .alert("Unauthorized.")
            .send(&state.client)
            .await;
        return;
    }
    // The peer the button was pressed in, used for edits/deletes.
    let peer = query.chat_peer.clone().map(PeerRef::Peer);
    match action {
        TelegramAction::Noop => {
            let _ = query.answer().send(&state.client).await;
        }
        TelegramAction::AuthClose => {
            let _ = query.answer().send(&state.client).await;
            if let (Some(peer), Some(id)) = (peer, query.message_id) {
                // Channel-aware deletion: IncomingMessage::delete
                // dispatches to channels.deleteMessages for supergroups,
                // messages.deleteMessages otherwise. Fetch first, then
                // delete through the message's own peer context.
                if let Ok(messages) = state.client.get_messages(peer, &[id]).await
                    && let Some(message) = messages.first()
                {
                    let _ = message.delete().await;
                }
            }
        }
        TelegramAction::AuthPage { page } => {
            let _ = query.answer().send(&state.client).await;
            if let (Some(peer), Some(id)) = (peer, query.message_id) {
                render(&state, peer, Some(id), page, None).await;
            }
        }
        _ => {
            let _ = query
                .answer()
                .alert("This action is unavailable. Open the authorization list again.")
                .send(&state.client)
                .await;
        }
    }
}
