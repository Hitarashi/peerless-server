use std::sync::Arc;

use ferogram::{InputMessage, filters, filters::Dispatcher};

use crate::{BotState, html::parse_dynamic_html};

const RESTRICTED: &str =
    "🔒 <b>Access Restricted:</b> This command is restricted to the bot owner.";

fn format_stats_html(stats: &db::AlacStats) -> String {
    format!(
        "<b>📊 Peerless Analytics</b><br/><br/><blockquote><b>📦 Storage & Caching</b><br/>• Cached Apple Tracks: <code>{}</code></blockquote>",
        stats.total_cached_tracks,
    )
}

async fn stats(msg: ferogram::update::IncomingMessage, state: Arc<BotState>) {
    let sender = msg.sender_user_id().unwrap_or_default();
    if !state.auth.is_admin(sender) {
        let _ = msg
            .reply(InputMessage::html(parse_dynamic_html(RESTRICTED)))
            .await;
        return;
    }
    let Some(repository) = state.stats.as_ref() else {
        tracing::warn!("stats repository is unavailable");
        return;
    };
    let Ok(data) = repository.get_stats().await else {
        tracing::warn!("stats query failed");
        return;
    };
    let text = format_stats_html(&data);
    let _ = msg
        .reply(InputMessage::html(parse_dynamic_html(&text)))
        .await;
}

pub fn register(dp: &mut Dispatcher, state: Arc<BotState>) {
    dp.on_message(filters::command("stats"), move |msg| {
        stats(msg, Arc::clone(&state))
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stats_card_renders_expected_text() {
        let stats = db::AlacStats {
            total_cached_tracks: 2,
        };
        assert_eq!(
            format_stats_html(&stats),
            "<b>📊 Peerless Analytics</b><br/><br/><blockquote><b>📦 Storage & Caching</b><br/>• Cached Apple Tracks: <code>2</code></blockquote>"
        );
    }
}
