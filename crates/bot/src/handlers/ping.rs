use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use engine::orchestrator::types::TaskPhase;
use ferogram::{InputMessage, filters, filters::Dispatcher};

use crate::{BotState, html::parse_dynamic_html};

fn format_uptime(elapsed: Duration) -> String {
    let seconds = elapsed.as_secs();
    let days = seconds / 86_400;
    let hours = seconds % 86_400 / 3_600;
    let minutes = seconds % 3_600 / 60;
    let secs = seconds % 60;
    let mut parts = Vec::new();
    if days > 0 {
        parts.push(format!("{days}d"));
    }
    if hours > 0 {
        parts.push(format!("{hours}h"));
    }
    if minutes > 0 {
        parts.push(format!("{minutes}m"));
    }
    parts.push(format!("{secs}s"));
    parts.join(" ")
}

fn rss_mb() -> f64 {
    std::fs::read_to_string("/proc/self/statm")
        .ok()
        .and_then(|statm| statm.split_whitespace().nth(1)?.parse::<f64>().ok())
        .map(|pages| pages * 4096.0 / (1024.0 * 1024.0))
        .unwrap_or(0.0)
}

struct HealthCard<'a> {
    tg_latency: u128,
    db_status: &'a str,
    db_latency: u128,
    mirror_status: &'a str,
    mirror_latency: u128,
    uptime: &'a str,
    memory_mb: f64,
    queue_state: &'a str,
}

fn health_card(card: HealthCard<'_>) -> String {
    format!(
        "🏓 <b>Pong! System Health</b><br/><br/><blockquote><b>⚡ Latencies & Services:</b><br/>• Telegram API: <code>{}ms</code><br/>• Database: <b>{}</b> (<code>{}ms</code>)<br/>• ALAC Mirror: <b>{}</b> (<code>{}ms</code>)</blockquote><br/><blockquote><b>🖥️ System Metrics:</b><br/>• Uptime: <code>{}</code><br/>• RAM (RSS): <code>{:.1} MB</code><br/>• Rip Worker: <code>{}</code></blockquote>",
        card.tg_latency,
        card.db_status,
        card.db_latency,
        card.mirror_status,
        card.mirror_latency,
        card.uptime,
        card.memory_mb,
        card.queue_state,
    )
}

async fn ping(msg: ferogram::update::IncomingMessage, state: Arc<BotState>) {
    let sender = msg.sender_user_id().unwrap_or_default();
    if !state
        .auth
        .is_authorized(sender, Some(super::marked_chat_id(&msg)))
        .await
        .unwrap_or(false)
    {
        return;
    }

    let started = Instant::now();
    let reply = match msg
        .reply(InputMessage::html(parse_dynamic_html(
            "🏓 <b>Testing system health...</b>",
        )))
        .await
    {
        Ok(reply) => reply,
        Err(error) => {
            tracing::warn!(%error, "health probe reply failed");
            return;
        }
    };
    let tg_latency = started.elapsed().as_millis();

    let db_started = Instant::now();
    let db_status = match state.stats.as_ref() {
        Some(stats) => match stats.get_stats().await {
            Ok(_) => "Operational",
            Err(error) => {
                tracing::warn!(%error, "health database check failed");
                "Degraded"
            }
        },
        None => "Degraded",
    };
    let db_latency = db_started.elapsed().as_millis();

    let mirror = state.rip_deps.probe_mirror_health().await;
    let mirror_latency = u128::from(mirror.latency_ms);
    let jobs = state.rip_orchestrator.get_active_tasks();
    let processing = jobs
        .iter()
        .any(|job| job.phase == TaskPhase::Processing || job.phase == TaskPhase::Delivering);
    let pending = jobs
        .iter()
        .filter(|job| job.phase == TaskPhase::Queued || job.phase == TaskPhase::WaitingDuplicate)
        .count();
    let queue_state = if processing {
        format!("Processing ({pending} queued)")
    } else {
        "Idle".to_owned()
    };
    let uptime = format_uptime(state.started_at.elapsed());
    let card = health_card(HealthCard {
        tg_latency,
        db_status,
        db_latency,
        mirror_status: mirror.health.label(),
        mirror_latency,
        uptime: &uptime,
        memory_mb: rss_mb(),
        queue_state: &queue_state,
    });
    let _ = state
        .client
        .edit_message(
            super::chat_peer_ref(&msg),
            reply.id(),
            InputMessage::html(parse_dynamic_html(&card)),
        )
        .await;
}

pub fn register(dp: &mut Dispatcher, state: Arc<BotState>) {
    dp.on_message(filters::command("ping"), move |msg| {
        ping(msg, Arc::clone(&state))
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uptime_renders_compound_units() {
        assert_eq!(format_uptime(Duration::from_secs(90_061)), "1d 1h 1m 1s");
    }

    #[test]
    fn uptime_renders_seconds() {
        assert_eq!(format_uptime(Duration::from_secs(5)), "5s");
    }

    #[test]
    fn health_card_renders_expected_text() {
        assert_eq!(
            health_card(HealthCard {
                tg_latency: 4,
                db_status: "Operational",
                db_latency: 2,
                mirror_status: "Online",
                mirror_latency: 8,
                uptime: "5s",
                memory_mb: 12.3,
                queue_state: "Idle",
            }),
            "🏓 <b>Pong! System Health</b><br/><br/><blockquote><b>⚡ Latencies & Services:</b><br/>• Telegram API: <code>4ms</code><br/>• Database: <b>Operational</b> (<code>2ms</code>)<br/>• ALAC Mirror: <b>Online</b> (<code>8ms</code>)</blockquote><br/><blockquote><b>🖥️ System Metrics:</b><br/>• Uptime: <code>5s</code><br/>• RAM (RSS): <code>12.3 MB</code><br/>• Rip Worker: <code>Idle</code></blockquote>"
        );
    }
}
