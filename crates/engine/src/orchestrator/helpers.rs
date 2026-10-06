//! Shared helpers: caption text shaping, diagnostics formatting, and cache storage retries.

use super::*;

/// Escapes the characters Telegram's HTML parse mode cannot carry verbatim.
pub(super) fn html_escape(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for ch in input.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            _ => out.push(ch),
        }
    }
    out
}

/// Clamps `text` to `max_utf16` UTF-16 code units, reserving one unit for the ellipsis.
pub(super) fn clamp_str_utf16(text: &str, max_utf16: usize) -> String {
    let count = text.encode_utf16().count();
    if count <= max_utf16 {
        return text.to_string();
    }
    let mut curr_len = 0;
    let mut byte_limit = text.len();
    for (idx, ch) in text.char_indices() {
        let ch_len = ch.len_utf16();
        if curr_len + ch_len > max_utf16.saturating_sub(1) {
            byte_limit = idx;
            break;
        }
        curr_len += ch_len;
    }
    let mut truncated = text[..byte_limit].to_string();
    truncated.push('…');
    truncated
}

pub(super) fn kind_str(kind: TargetKind) -> &'static str {
    match kind {
        TargetKind::Track => "track",
        TargetKind::Album => "album",
        TargetKind::Artist => "artist",
        TargetKind::Playlist => "playlist",
    }
}

pub(super) fn panic_message(panic: Box<dyn Any + Send>) -> String {
    if let Some(message) = panic.downcast_ref::<String>() {
        message.clone()
    } else if let Some(message) = panic.downcast_ref::<&str>() {
        (*message).to_owned()
    } else {
        "task panicked".to_owned()
    }
}

pub(super) async fn find_cached_tracks_with_retry<D: TrackCache>(
    cache: &D,
    track_ids: &[String],
    policy: &StorageRetryPolicy,
) -> Result<HashMap<(String, Codec), CachedTrack>, crate::orchestrator::deps::TrackCacheError> {
    let attempts = policy.total_attempts.max(1);
    let mut attempt = 0;
    loop {
        match cache.find_cached_tracks(track_ids).await {
            Ok(value) => return Ok(value),
            Err(error) if error.is_unavailable() && attempt + 1 < attempts => {
                tokio::time::sleep(policy.delay_before_retry(attempt)).await;
                attempt += 1;
            }
            Err(error) => return Err(error),
        }
    }
}

pub(super) async fn save_track_with_retry<D: TrackCache>(
    cache: &D,
    input: SaveTrackInput,
    policy: &StorageRetryPolicy,
) -> Result<(), crate::orchestrator::deps::TrackCacheError> {
    let attempts = policy.total_attempts.max(1);
    let mut attempt = 0;
    loop {
        match cache.save_track(input.clone()).await {
            Ok(()) => return Ok(()),
            Err(error) if error.is_unavailable() && attempt + 1 < attempts => {
                tokio::time::sleep(policy.delay_before_retry(attempt)).await;
                attempt += 1;
            }
            Err(error) => return Err(error),
        }
    }
}
