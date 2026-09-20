use std::{sync::Arc, time::Duration};

use ferogram::{
    ErrorKind, InputMessage, InvocationErrorExt, PeerRef,
    filters::{self, Dispatcher},
};

use crate::{
    BotState,
    dashboard::{DashboardFuture, DashboardSink, EditError},
    dashboard_manager, event_bridge,
};

struct TelegramSink {
    client: ferogram::Client,
    peer: PeerRef,
}

/// Construct the dashboard adapter used by both `/status` and get commands.
/// Keeping the adapter here ensures every chat has one consistent message
/// lifecycle regardless of which command first opens the dashboard.
pub(crate) fn dashboard_sink(client: ferogram::Client, peer: PeerRef) -> Arc<dyn DashboardSink> {
    Arc::new(TelegramSink { client, peer })
}

fn edit_error(error: ferogram::InvocationError) -> EditError {
    match error.kind() {
        ErrorKind::FloodWait(seconds) => EditError::FloodWait(Duration::from_secs(seconds)),
        ErrorKind::Rpc { name, .. } if name == "MESSAGE_NOT_MODIFIED" => EditError::NotModified,
        _ => EditError::Other(error.to_string()),
    }
}
impl DashboardSink for TelegramSink {
    fn send<'a>(
        &'a self,
        text: &'a str,
        keyboard: Option<ferogram::tl::enums::ReplyMarkup>,
    ) -> DashboardFuture<'a, i32> {
        Box::pin(async move {
            let mut input = InputMessage::html(text);
            if let Some(k) = keyboard {
                input = input.reply_markup(k);
            }
            self.client
                .send_message(self.peer.clone(), input)
                .await
                .map(|m| m.id())
                .map_err(edit_error)
        })
    }
    fn edit<'a>(
        &'a self,
        id: i32,
        text: &'a str,
        keyboard: Option<ferogram::tl::enums::ReplyMarkup>,
    ) -> DashboardFuture<'a, ()> {
        Box::pin(async move {
            let mut input = InputMessage::html(text);
            if let Some(k) = keyboard {
                input = input.reply_markup(k);
            }
            self.client
                .edit_message(self.peer.clone(), id, input)
                .await
                .map_err(edit_error)
        })
    }
    fn delete<'a>(&'a self, id: i32) -> DashboardFuture<'a, ()> {
        Box::pin(async move {
            let messages = self
                .client
                .get_messages(self.peer.clone(), &[id])
                .await
                .map_err(edit_error)?;
            if let Some(message) = messages.first() {
                message.delete().await.map_err(edit_error)?;
            }
            Ok(())
        })
    }
}

pub fn register(dp: &mut Dispatcher, state: Arc<BotState>) {
    dp.on_message(filters::command("status"), move |msg| {
        let state = Arc::clone(&state);
        async move {
            let user = msg.sender_user_id().unwrap_or_default();
            if !state
                .auth
                .is_authorized(user, Some(super::marked_chat_id(&msg)))
                .await
                .unwrap_or(false)
            {
                return;
            }
            let admin = state.auth.is_admin(user);
            let sink: Arc<dyn DashboardSink> = Arc::new(TelegramSink {
                client: state.client.clone(),
                peer: super::chat_peer_ref(&msg),
            });
            // Real engine snapshot, viewer-scoped at open time.
            let snapshot = event_bridge::current_snapshot(&state).await;
            let _ = dashboard_manager()
                .open(super::marked_chat_id(&msg), user, admin, sink, snapshot)
                .await;
        }
    });
}
