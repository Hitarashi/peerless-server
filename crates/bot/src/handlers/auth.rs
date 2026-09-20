use std::sync::Arc;

use ferogram::{InputMessage, PeerRef, filters, filters::Dispatcher, tl};

use crate::{
    BotState,
    html::{escape, parse_dynamic_html},
};

/// Resolve a TL peer to the Bot-API "marked" id the stores
/// (users positive, basic groups -chat_id, channels -1e12 - channel_id).
fn peer_id(peer: &tl::enums::Peer) -> (i64, bool) {
    match peer {
        tl::enums::Peer::User(p) => (p.user_id, true),
        tl::enums::Peer::Chat(p) => (-p.chat_id, false),
        tl::enums::Peer::Channel(p) => (-(1_000_000_000_000i64 + p.channel_id), false),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TargetResult {
    Ok {
        id: i64,
        name: String,
        is_user: bool,
    },
    UnresolvedReply,
    InvalidArg,
    None,
}

fn format_user_name(u: &ferogram::types::User, fallback_id: i64) -> String {
    u.first_name()
        .map(str::to_owned)
        .map(|first| match u.last_name() {
            Some(last) => format!("{first} {last}"),
            None => first,
        })
        .or_else(|| u.username().map(str::to_owned))
        .unwrap_or_else(|| format!("User {fallback_id}"))
}

async fn resolve_user_display_name(
    client: &ferogram::Client,
    id: i64,
    hint_name: Option<String>,
) -> String {
    if let Some(name) = hint_name
        && !name.trim().is_empty()
    {
        return name;
    }
    match client.get_users_by_id(&[id]).await {
        Ok(users) => users
            .into_iter()
            .next()
            .flatten()
            .map(|u| format_user_name(&u, id))
            .unwrap_or_else(|| format!("User {id}")),
        _ => format!("User {id}"),
    }
}

fn extract_reply_header(raw: &tl::enums::Message) -> Option<&tl::enums::MessageReplyHeader> {
    match raw {
        tl::enums::Message::Message(m) => m.reply_to.as_ref(),
        tl::enums::Message::Service(m) => m.reply_to.as_ref(),
        _ => None,
    }
}

pub(crate) async fn resolve_target(
    msg: &ferogram::update::IncomingMessage,
    state: &BotState,
) -> TargetResult {
    let reply_header = extract_reply_header(&msg.raw);
    let is_reply = reply_header.is_some() || msg.reply_to_message_id().is_some();

    if is_reply {
        // Fast path: check reply_from on the MessageReplyHeader.
        if let Some(tl::enums::MessageReplyHeader::MessageReplyHeader(h)) = reply_header {
            if let Some(tl::enums::MessageFwdHeader::MessageFwdHeader(fwd)) = &h.reply_from
                && let Some(peer) = &fwd.from_id
            {
                let (id, user) = peer_id(peer);
                let name = if user {
                    resolve_user_display_name(
                        &state.client,
                        id,
                        fwd.from_name.clone().or_else(|| fwd.post_author.clone()),
                    )
                    .await
                } else {
                    fwd.from_name
                        .clone()
                        .or_else(|| fwd.post_author.clone())
                        .unwrap_or_else(|| format!("Chat {id}"))
                };
                return TargetResult::Ok {
                    id,
                    name,
                    is_user: user,
                };
            }
        } else if let Some(tl::enums::MessageReplyHeader::MessageReplyStoryHeader(s)) = reply_header
        {
            let (id, user) = peer_id(&s.peer);
            let name = if user {
                resolve_user_display_name(&state.client, id, None).await
            } else {
                format!("Chat {id}")
            };
            return TargetResult::Ok {
                id,
                name,
                is_user: user,
            };
        }

        // Fetch the replied-to message if reply_from was absent or omitted from_id.
        // Prime the peer cache first so channels.getMessages has a valid access_hash.
        // PeerRef::Id resolves from cache if available, or does one cheap RPC on miss.
        if let Some(chat_peer) = msg.peer_id() {
            let marked = match chat_peer {
                tl::enums::Peer::Chat(c) => -c.chat_id,
                tl::enums::Peer::Channel(c) => -(1_000_000_000_000i64 + c.channel_id),
                tl::enums::Peer::User(u) => u.user_id,
            };
            if let Err(e) = state.client.resolve(PeerRef::Id(marked)).await {
                tracing::warn!(marked, error = %e, "auth: could not prime peer cache for chat");
            }
        }
        let reply_msg = match msg.get_reply_with(&state.client).await {
            Ok(Some(reply)) => Some(reply),
            Err(e) => {
                tracing::warn!(error = %e, "get_reply_with failed");
                None
            }
            Ok(None) => {
                let reply_id = msg.reply_to_message_id().or_else(|| {
                    if let Some(tl::enums::MessageReplyHeader::MessageReplyHeader(h)) = reply_header
                    {
                        h.reply_to_msg_id.or(h.reply_to_top_id)
                    } else {
                        None
                    }
                });
                let target_peer = if let Some(tl::enums::MessageReplyHeader::MessageReplyHeader(
                    h,
                )) = reply_header
                {
                    h.reply_to_peer_id.as_ref().or_else(|| msg.peer_id())
                } else {
                    msg.peer_id()
                };
                if let (Some(id), Some(peer)) = (reply_id, target_peer) {
                    match state
                        .client
                        .get_messages(PeerRef::Peer(peer.clone()), &[id])
                        .await
                    {
                        Ok(mut msgs) => msgs.pop(),
                        Err(e) => {
                            tracing::warn!(error = %e, "get_messages fallback failed");
                            None
                        }
                    }
                } else {
                    None
                }
            }
        };

        if let Some(reply) = reply_msg {
            let uid = reply.sender_user_id();
            let sender_peer = reply
                .sender_id()
                .cloned()
                .or_else(|| reply.effective_sender_id());
            let target_id = uid.or_else(|| sender_peer.as_ref().map(|p| peer_id(p).0));
            if let Some(id) = target_id {
                let is_user = id > 0;
                let name = match reply.sender_user().await {
                    Ok(Some(u)) => format_user_name(&u, id),
                    _ => {
                        if is_user {
                            resolve_user_display_name(&state.client, id, None).await
                        } else {
                            format!("Chat {id}")
                        }
                    }
                };
                return TargetResult::Ok { id, name, is_user };
            }
        }

        // A message intended as a reply must NEVER fall through to group authorization!
        return TargetResult::UnresolvedReply;
    }

    // 2. Explicit argument: numeric id or @username. Keep the argument
    // optional so a bare command in a group can target the group itself.
    if let Some(text) = msg.text().and_then(|text| text.split_whitespace().nth(1)) {
        if let Ok(id) = text.parse::<i64>() {
            return TargetResult::Ok {
                id,
                name: format!("User {id}"),
                is_user: id > 0,
            };
        }
        if text.starts_with('@')
            && let Ok(peer) = state
                .client
                .resolve(PeerRef::Username(text.to_owned()))
                .await
        {
            let (id, user) = peer_id(&peer);
            return TargetResult::Ok {
                id,
                name: text.to_owned(),
                is_user: user,
            };
        }
        // An invalid explicit argument must never fall through to group
        // authorization. Report invalid argument.
        return TargetResult::InvalidArg;
    }

    // 3. Bare command inside a group (not a reply, no argument): authorize
    // the group chat itself, using the marked id form the stores.
    if msg.is_any_group() {
        let marked = match msg.peer_id() {
            Some(tl::enums::Peer::Chat(c)) => -c.chat_id,
            Some(tl::enums::Peer::Channel(c)) => -(1_000_000_000_000i64 + c.channel_id),
            _ => return TargetResult::None,
        };
        return TargetResult::Ok {
            id: marked,
            name: format!("Chat {marked}"),
            is_user: false,
        };
    }

    TargetResult::None
}

pub(crate) async fn authorize(msg: ferogram::update::IncomingMessage, state: Arc<BotState>) {
    let sender = match msg.sender_user_id() {
        Some(id) => id,
        None => return,
    };
    if !state.auth.is_admin(sender) {
        return;
    }
    let (id, name, is_user) = match resolve_target(&msg, &state).await {
        TargetResult::Ok { id, name, is_user } => (id, name, is_user),
        TargetResult::UnresolvedReply => {
            let text = "⚠️ <b>Could not resolve replied-to user.</b> Telegram did not provide sender information (the message may be deleted, privacy-protected, or inaccessible).<br/><br/>Please authorize explicitly by ID or username:<br/>• <code>/auth &lt;id | @username&gt;</code>";
            let _ = msg
                .reply(InputMessage::html(parse_dynamic_html(text)))
                .await;
            return;
        }
        TargetResult::InvalidArg => {
            let text = "⚠️ <b>Invalid argument:</b> Please specify a valid numeric ID or @username.<br/><br/>• <code>/auth &lt;id | @username&gt;</code>";
            let _ = msg
                .reply(InputMessage::html(parse_dynamic_html(text)))
                .await;
            return;
        }
        TargetResult::None => {
            let text = "<b>Authorization usage</b><br/><br/><blockquote>• Reply to a message with <code>/auth</code><br/>• <code>/auth &lt;id | @username&gt;</code><br/>• Send <code>/auth</code> inside a group to authorize the whole group</blockquote>";
            let _ = msg
                .reply(InputMessage::html(parse_dynamic_html(text)))
                .await;
            return;
        }
    };
    let link = super::peer_link(&name, id);
    match state.auth.authorize(id, Some(&name)).await {
        Ok(new) => {
            let word = if new { "Authorized" } else { "Updated" };
            let kind = if is_user { "User" } else { "Group" };
            let text = format!(
                "✓ <b>{word} {kind}:</b> {} <a href=\"{link}\">{}</a> (<code>{id}</code>)",
                if is_user { "👤" } else { "👥" },
                escape(&name)
            );
            let _ = msg
                .reply(InputMessage::html(parse_dynamic_html(&text)))
                .await;
        }
        Err(error) => tracing::error!(%error, "authorization failed"),
    }
}

pub fn register(dp: &mut Dispatcher, state: Arc<BotState>) {
    let auth_state = Arc::clone(&state);
    dp.on_message(filters::command("auth"), move |msg| {
        authorize(msg, Arc::clone(&auth_state))
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_peer_id_resolution() {
        let user_peer = tl::enums::Peer::User(tl::types::PeerUser { user_id: 123456 });
        assert_eq!(peer_id(&user_peer), (123456, true));

        let chat_peer = tl::enums::Peer::Chat(tl::types::PeerChat { chat_id: 7890 });
        assert_eq!(peer_id(&chat_peer), (-7890, false));

        let channel_peer = tl::enums::Peer::Channel(tl::types::PeerChannel {
            channel_id: 3778693487,
        });
        assert_eq!(peer_id(&channel_peer), (-1003778693487, false));
    }

    #[test]
    fn test_extract_reply_header_empty() {
        let raw = tl::enums::Message::Empty(tl::types::MessageEmpty {
            id: 1,
            peer_id: None,
        });
        assert!(extract_reply_header(&raw).is_none());
    }
}
