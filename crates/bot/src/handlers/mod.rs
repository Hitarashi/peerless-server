mod auth;
mod backup;
mod clean;
mod delete;
pub mod dump;
pub(crate) mod get;
mod help;
mod index;
mod info;
mod list;
mod ops;
mod random;
mod report;
mod revoke;
mod search;
pub mod settings;
mod spec;
mod start;
mod status;
pub(crate) mod stream;

#[path = "../callbacks.rs"]
mod callbacks;

use std::sync::Arc;

use ferogram::{
    filters::{self, Dispatcher},
    update::CallbackQuery,
};

use crate::{BotState, interaction::TelegramAction};

/// Bot-API marked group id (-100...) -> t.me/c/ link segment, matching the
/// string-slice semantics.
pub(crate) fn group_link_segment(marked_id: i64) -> String {
    let s = marked_id.to_string();
    s.strip_prefix("-100")
        .unwrap_or_else(|| s.trim_start_matches('-'))
        .to_owned()
}

/// Bot-API "marked" peer id for storage and lookups: users stay positive,
/// basic groups are -chat_id, channels/supergroups are -(1e12 + channel_id).
/// Matches the id the Telegram library reports as chat.id, which is what the
/// users table stores.
pub(crate) fn marked_peer_id(peer: &ferogram::tl::enums::Peer) -> i64 {
    use ferogram::tl::enums::Peer;
    match peer {
        Peer::User(p) => p.user_id,
        Peer::Chat(p) => -p.chat_id,
        Peer::Channel(p) => -(1_000_000_000_000i64 + p.channel_id),
    }
}

/// Marked chat id for a message's chat, in the storage/DB format.
/// `msg.chat_id()` returns RAW TL ids (positive for channels), which is a
/// different format — never mix the two.
pub(crate) fn marked_chat_id(msg: &ferogram::update::IncomingMessage) -> i64 {
    msg.peer_id()
        .map(marked_peer_id)
        // Peerless updates have no chat; 0 preserves prior behavior.
        .unwrap_or_default()
}

/// Chat peer for sends/edits, derived from the message's TL peer so channel
/// and group ids resolve correctly (PeerRef::from(i64) expects marked ids).
pub(crate) fn chat_peer_ref(msg: &ferogram::update::IncomingMessage) -> ferogram::PeerRef {
    msg.peer_id()
        .map(|peer| ferogram::PeerRef::Peer(peer.clone()))
        // Fallback mirrors prior behavior for peerless updates.
        .unwrap_or(ferogram::PeerRef::from(msg.chat_id()))
}

/// Open or refresh the single status dashboard owned by a chat. Get commands
/// and callback-driven deliveries share this helper so neither path can
/// accidentally create a per-job progress message or deliver into a group.
pub(crate) async fn ensure_dashboard(
    state: &Arc<BotState>,
    chat: i64,
    viewer_id: i64,
    viewer_is_admin: bool,
    peer: ferogram::PeerRef,
) {
    let snapshot = crate::event_bridge::current_snapshot(state).await;
    let manager = crate::dashboard_manager();
    if manager.contains(chat).await {
        manager.refresh_entry_from(chat, snapshot).await;
    } else {
        let sink = status::dashboard_sink(state.client.clone(), peer);
        if let Err(error) = manager
            .open(chat, viewer_id, viewer_is_admin, sink, snapshot)
            .await
        {
            tracing::warn!(chat_id = chat, error = ?error, "failed to open status dashboard");
        }
    }
}

/// Shared dashboard sink factory for event-driven recovery paths.
pub(crate) fn dashboard_sink(
    client: ferogram::Client,
    peer: ferogram::PeerRef,
) -> Arc<dyn crate::dashboard::DashboardSink> {
    status::dashboard_sink(client, peer)
}

/// Peer link: @name -> t.me link,
/// user id -> tg://user, group/channel id -> t.me/c/ link.
pub(crate) fn peer_link(name: &str, id: i64) -> String {
    if let Some(username) = name.strip_prefix('@') {
        format!("https://t.me/{username}")
    } else if id > 0 {
        format!("tg://user?id={id}")
    } else {
        format!("https://t.me/c/{}/1", group_link_segment(id))
    }
}

pub fn register(dp: &mut Dispatcher, state: Arc<BotState>) {
    start::register(dp, Arc::clone(&state));
    help::register(dp, Arc::clone(&state));
    auth::register(dp, Arc::clone(&state));
    revoke::register(dp, Arc::clone(&state));
    list::register(dp, Arc::clone(&state));
    get::register(dp, Arc::clone(&state));
    status::register(dp, Arc::clone(&state));
    settings::register(dp, Arc::clone(&state));
    ops::register(dp, Arc::clone(&state));
    info::register(dp, Arc::clone(&state));
    clean::register(dp, Arc::clone(&state));
    delete::register(dp, Arc::clone(&state));
    spec::register(dp, Arc::clone(&state));
    index::register(dp, Arc::clone(&state));
    backup::register(dp, Arc::clone(&state));
    report::register(dp, Arc::clone(&state));
    search::register(dp, Arc::clone(&state));
    random::register(dp, Arc::clone(&state));
    dump::register(dp, Arc::clone(&state));
    stream::register(dp, Arc::clone(&state));

    let callback_state = Arc::clone(&state);
    dp.on_callback_query(filters::all::<CallbackQuery>(), move |query| {
        let state = Arc::clone(&callback_state);
        async move {
            let action = query
                .data()
                .and_then(|data| TelegramAction::decode(data).ok());
            match action {
                Some(TelegramAction::Cancel { job_id }) => {
                    callbacks::dispatch_cancel(state, query, job_id).await
                }
                Some(TelegramAction::Dashboard {
                    action: dashboard_action,
                    page,
                }) => callbacks::dispatch_dashboard(state, query, dashboard_action, page).await,
                Some(TelegramAction::Settings(action)) => {
                    settings::callback(state, query, action).await
                }
                Some(TelegramAction::Report(action)) => {
                    report::callback(state, query, action).await
                }
                Some(TelegramAction::Discovery(action)) => {
                    random::callback(state, query, action).await
                }
                Some(action @ TelegramAction::ConfirmDelete { .. })
                | Some(action @ TelegramAction::CancelDelete { .. }) => {
                    delete::callback(state, query, action).await
                }
                Some(action @ TelegramAction::ConfirmImport { .. })
                | Some(action @ TelegramAction::CancelImport { .. }) => {
                    backup::callback(state, query, action).await
                }
                Some(action @ TelegramAction::DeliverCached { .. })
                | Some(action @ TelegramAction::Get { .. })
                | Some(action @ TelegramAction::SearchClose) => {
                    search::callback(state, query, action).await
                }
                Some(action @ TelegramAction::AuthPage { .. })
                | Some(action @ TelegramAction::AuthClose) => {
                    list::callback(state, query, action).await
                }
                Some(TelegramAction::Noop) => {
                    let _ = query.answer().send(&state.client).await;
                }
                None => {
                    let _ = query
                        .answer()
                        .alert("This action is unavailable. Please run the command again.")
                        .send(&state.client)
                        .await;
                }
            }
        }
    });
}
