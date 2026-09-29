//! `/stats` — renders the Peerless analytics dashboard.
//!
//! Bot-owner only. Aggregates cache/storage counters, average retrieval and
//! rip latencies, and the most-requested tracks.

use std::sync::Arc;

use ferogram::{InputMessage, filters, filters::Dispatcher};

use crate::{BotState, html::parse_dynamic_html};

const RESTRICTED: &str =
    "🔒 <b>Access Restricted:</b> This command is restricted to the bot owner.";

fn format_duration_ms(ms: i64) -> String {
    if ms <= 0 {
        return "0ms".to_owned();
    }
    if ms < 1_000 {
        return format!("{ms}ms");
    }
    if ms < 60_000 {
        let tenths = (ms + 50) / 100;
        return format!("{}.{:01}s", tenths / 10, tenths % 10);
    }
    let minutes = ms / 60_000;
    let seconds = ((ms % 60_000) as f64 / 1_000.0).round() as i64;
    format!("{minutes}m {seconds}s")
}

fn format_stats_html(stats: &db::AlacStats) -> String {
    let top_tracks = if stats.top_tracks.is_empty() {
        "<i>No completed requests yet</i>".to_owned()
    } else {
        stats
            .top_tracks
            .iter()
            .enumerate()
            .map(|(index, track)| {
                let url = match track.track_key.provider {
                    music::Provider::Apple => {
                        format!("https://music.apple.com/song/{}", track.track_key.track_id)
                    }
                    music::Provider::Qobuz => {
                        format!("https://open.qobuz.com/track/{}", track.track_key.track_id)
                    }
                };
                let display_text = match (&track.title, &track.artist) {
                    (Some(title), Some(artist)) if !title.is_empty() && !artist.is_empty() => {
                        format!(
                            "{} — {}",
                            crate::html::escape(title),
                            crate::html::escape(artist)
                        )
                    }
                    (Some(title), _) if !title.is_empty() => crate::html::escape(title),
                    _ => format!(
                        "<code>{}</code>",
                        crate::html::escape(&track.track_key.track_id)
                    ),
                };
                format!(
                    "{}. <a href=\"{url}\">{display_text}</a> — <b>{}</b> request{}",
                    index + 1,
                    track.request_count,
                    if track.request_count > 1 { "s" } else { "" }
                )
            })
            .collect::<Vec<_>>()
            .join("<br/>")
    };

    format!(
        "<b>📊 Peerless Analytics</b><br/><br/><blockquote><b>📦 Storage & Caching</b><br/>• Cached Tracks: <code>{}</code> (<code>{}</code> Apple · <code>{}</code> Qobuz)<br/>• Total Requests: <code>{}</code><br/>• Cache Hit Ratio: <b>{}%</b> (<code>{}</code> hits / <code>{}</code> rips)<br/>• Failed Requests: <code>{}</code></blockquote><br/><blockquote><b>⚡ Latency Averages</b><br/>• Cache Retrieval: <code>{}</code><br/>• Mirror Rip Time: <code>{}</code></blockquote><br/><blockquote><b>🔥 Top Requested Tracks</b><br/>{top_tracks}</blockquote>",
        stats.total_cached_tracks,
        stats.apple_cached_tracks,
        stats.qobuz_cached_tracks,
        stats.total_requests,
        stats.cache_hit_ratio,
        stats.cache_hits,
        stats.cache_misses,
        stats.total_failed_requests,
        format_duration_ms(stats.avg_cache_duration_ms),
        format_duration_ms(stats.avg_rip_duration_ms),
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
    fn duration_renders_zero_as_empty() {
        assert_eq!(format_duration_ms(0), "0ms");
    }

    #[test]
    fn duration_renders_seconds() {
        assert_eq!(format_duration_ms(1_250), "1.3s");
    }

    #[test]
    fn duration_renders_minutes() {
        assert_eq!(format_duration_ms(61_500), "1m 2s");
    }

    #[test]
    fn stats_card_renders_expected_text() {
        let stats = db::AlacStats {
            total_cached_tracks: 3,
            apple_cached_tracks: 2,
            qobuz_cached_tracks: 1,
            total_requests: 4,
            cache_hits: 2,
            cache_misses: 2,
            cache_hit_ratio: 50.0,
            avg_rip_duration_ms: 1_250,
            avg_cache_duration_ms: 12,
            total_failed_requests: 1,
            top_tracks: vec![
                db::TopTrackStat {
                    track_key: engine::TrackKey::apple("123"),
                    title: Some("Song Title".to_owned()),
                    artist: Some("Artist Name".to_owned()),
                    request_count: 2,
                },
                db::TopTrackStat {
                    track_key: engine::TrackKey::new(music::Provider::Qobuz, "456"),
                    title: None,
                    artist: None,
                    request_count: 1,
                },
            ],
        };
        assert_eq!(
            format_stats_html(&stats),
            "<b>📊 Peerless Analytics</b><br/><br/><blockquote><b>📦 Storage & Caching</b><br/>• Cached Tracks: <code>3</code> (<code>2</code> Apple · <code>1</code> Qobuz)<br/>• Total Requests: <code>4</code><br/>• Cache Hit Ratio: <b>50%</b> (<code>2</code> hits / <code>2</code> rips)<br/>• Failed Requests: <code>1</code></blockquote><br/><blockquote><b>⚡ Latency Averages</b><br/>• Cache Retrieval: <code>12ms</code><br/>• Mirror Rip Time: <code>1.3s</code></blockquote><br/><blockquote><b>🔥 Top Requested Tracks</b><br/>1. <a href=\"https://music.apple.com/song/123\">Song Title — Artist Name</a> — <b>2</b> requests<br/>2. <a href=\"https://open.qobuz.com/track/456\"><code>456</code></a> — <b>1</b> request</blockquote>"
        );
    }
}
