use std::sync::Arc;

use ferogram::{InputMessage, filters, filters::Dispatcher};

use crate::{BotState, command_catalog::render_help, html::parse_dynamic_html};

pub fn register(dp: &mut Dispatcher, state: Arc<BotState>) {
    dp.on_message(filters::command("help"), move |msg| {
        let state = Arc::clone(&state);
        async move {
            let sender = match msg.sender_user_id() {
                Some(id) => id,
                None => return,
            };
            let chat = msg.peer_id().map(super::marked_peer_id);
            match state.auth.is_authorized(sender, chat).await {
                Ok(true) => {
                    let text = render_help(state.auth.is_admin(sender));
                    if let Err(error) = msg
                        .reply(InputMessage::html(parse_dynamic_html(&text)))
                        .await
                    {
                        tracing::warn!(%error, "help reply failed");
                    }
                }
                Ok(false) => {}
                Err(error) => tracing::error!(%error, "authorization lookup failed"),
            }
        }
    });
}
