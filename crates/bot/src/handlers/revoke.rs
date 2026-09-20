use std::sync::Arc;

use ferogram::{InputMessage, filters, filters::Dispatcher};

use super::auth::{TargetResult, resolve_target};
use crate::{
    BotState,
    html::{escape, parse_dynamic_html},
};

pub fn register(dp: &mut Dispatcher, state: Arc<BotState>) {
    dp.on_message(filters::command("revoke"), move |msg| {
        let state = Arc::clone(&state);
        async move {
                let sender = match msg.sender_user_id() { Some(id) => id, None => return };
                if !state.auth.is_admin(sender) { return; }
                let (id, name) = match resolve_target(&msg, &state).await {
                    TargetResult::Ok { id, name, .. } => (id, name),
                    TargetResult::UnresolvedReply => {
                        let text = "⚠️ <b>Could not resolve replied-to user.</b> Telegram did not provide sender information.<br/><br/>Please revoke explicitly by ID or username:<br/>• <code>/revoke &lt;id | @username&gt;</code>";
                        let _ = msg.reply(InputMessage::html(parse_dynamic_html(text))).await;
                        return;
                    }
                    TargetResult::InvalidArg => {
                        let text = "⚠️ <b>Invalid argument:</b> Please specify a valid numeric ID or @username.<br/><br/>• <code>/revoke &lt;id | @username&gt;</code>";
                        let _ = msg.reply(InputMessage::html(parse_dynamic_html(text))).await;
                        return;
                    }
                    TargetResult::None => {
                        let usage = "<b>Revocation usage</b><br/><br/><blockquote>• Reply to a message with <code>/revoke</code><br/>• <code>/revoke &lt;id | @username&gt;</code><br/>• Send <code>/revoke</code> inside a group to revoke the group</blockquote>";
                        let _ = msg.reply(InputMessage::html(parse_dynamic_html(usage))).await;
                        return;
                    }
                };
                match state.auth.revoke(id).await {
                    Ok(true) => {
                        let link = super::peer_link(&name, id);
                        let text = format!("✓ <b>Revoked access for:</b> <a href=\"{link}\">{}</a> (<code>{id}</code>)", escape(&name));
                        let _ = msg.reply(InputMessage::html(parse_dynamic_html(&text))).await;
                    }
                    Ok(false) => { let _ = msg.reply(InputMessage::html(parse_dynamic_html(&format!("! <b>Not found:</b> ID <code>{id}</code> was not in the authorized list.")))).await; }
                    Err(error) => tracing::error!(%error, "revocation failed"),
                }
        }
    });
}
