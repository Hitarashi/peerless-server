//! Callback dispatcher for list pagination, dashboard paging, and get
//! cancellation.

use std::sync::Arc;

use ferogram::update::CallbackQuery;

use crate::{BotState, handlers::get::cancel, interaction::DashboardAction};

pub async fn dispatch_dashboard(
    state: Arc<BotState>,
    query: CallbackQuery,
    action: DashboardAction,
    current: usize,
) {
    let chat = query
        .chat_peer
        .as_ref()
        .map(super::marked_peer_id)
        .unwrap_or(query.user_id);

    let target = match action {
        DashboardAction::Previous => current.saturating_sub(1),
        DashboardAction::Next => current + 1,
        DashboardAction::Refresh => current,
    };

    if action == DashboardAction::Refresh {
        // Re-pull the latest engine snapshot rather than trusting the
        // message-local copy.
        let snapshot = crate::event_bridge::current_snapshot(&state).await;
        crate::dashboard_manager()
            .refresh_entry_from(chat, snapshot)
            .await;
    } else {
        crate::dashboard_manager().page(chat, target).await;
    }

    let _ = query.answer().send(&state.client).await;
}

pub async fn dispatch_cancel(state: Arc<BotState>, query: CallbackQuery, job_id: String) {
    let caller = query.user_id;
    let admin = state.auth.is_admin(caller);
    let result = cancel::cancel_inline(&state, &job_id, caller, admin);
    let (text, alert) = match result {
        cancel::CancelResult::Cancelled => (cancel::CALLBACK_ACK, false),
        cancel::CancelResult::Unauthorized => (cancel::CALLBACK_UNAUTHORIZED, true),
        cancel::CancelResult::Expired => (cancel::CALLBACK_EXPIRED, false),
    };
    let answer = if alert {
        query.answer().alert(text)
    } else {
        query.answer().text(text)
    };
    let _ = answer.send(&state.client).await;

    // Refresh dashboards so the row disappears promptly.
    let snapshot = crate::event_bridge::current_snapshot(&state).await;
    crate::dashboard_manager().refresh(snapshot, true).await;
}
