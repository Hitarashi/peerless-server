//! Shared Telegram presentation policy.
//!
//! Handlers provide semantic facts; this module owns the small, calm English
//! surface users see.  The policy deliberately keeps emoji out of neutral
//! content and routine navigation.

use ferogram::tl::enums::ReplyMarkup;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeedbackKind {
    Neutral,
    Success,
    Warning,
    Error,
    InProgress,
}

#[derive(Debug, Clone)]
pub struct TelegramMessage {
    pub html: String,
    pub keyboard: Option<ReplyMarkup>,
}

/// Semantic job state shared by the detailed card and dashboard adapters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RipStatusView {
    pub state: &'static str,
    pub target: String,
    pub completed: usize,
    pub total: usize,
    pub cached: usize,
    pub ripped: usize,
    pub skipped: usize,
    pub failed: usize,
    pub activity: Option<String>,
}

impl TelegramMessage {
    pub fn new(html: impl Into<String>) -> Self {
        Self {
            html: html.into(),
            keyboard: None,
        }
    }

    pub fn with_keyboard(mut self, keyboard: ReplyMarkup) -> Self {
        self.keyboard = Some(keyboard);
        self
    }
}

/// Render a heading with at most one semantic leading symbol.  Symbols are
/// intentionally absent from neutral content and all button labels.
pub fn heading(kind: FeedbackKind, title: &str) -> String {
    let symbol = match kind {
        FeedbackKind::Neutral => "",
        FeedbackKind::Success => "✓ ",
        FeedbackKind::Warning => "! ",
        FeedbackKind::Error => "× ",
        FeedbackKind::InProgress => "… ",
    };
    format!("{symbol}<b>{title}</b>")
}

pub fn escape(value: &str) -> String {
    crate::html::escape(value)
}

pub fn action_label(action: &str, target: Option<&str>) -> String {
    match target {
        Some(target) if !target.is_empty() => format!("{action} · {target}"),
        _ => action.to_owned(),
    }
}

/// Human-readable label for a delivered rendition.  Album delivery metadata
/// stores the canonical codec string so the bot can keep labels stable even
/// when the primary rendition falls back from ALAC to AAC.
pub fn rendition_label(codec: Option<&str>) -> &'static str {
    match codec {
        Some("ec-3") => "Dolby Atmos",
        Some("aac") | Some("mp4a.40.2") | Some("mp4a.40.5") => "AAC",
        Some("flac") => "FLAC",
        Some("alac") | None => "ALAC",
        Some(_) => "Audio",
    }
}

/// Humanize a byte count the way mirror-leech does: two decimals, 1024-based
/// units (B, KB, MB, GB, TB, PB).
pub fn readable_file_size(bytes: u64) -> String {
    const UNITS: [&str; 6] = ["B", "KB", "MB", "GB", "TB", "PB"];
    if bytes == 0 {
        return "0B".to_owned();
    }
    let mut value = bytes as f64;
    let mut index = 0;
    while value >= 1024.0 && index < UNITS.len() - 1 {
        value /= 1024.0;
        index += 1;
    }
    format!("{value:.2}{}", UNITS[index])
}

/// Compact elapsed time like "1d11h47m30s", "3m36s", "45s". Only periods
/// that fit are emitted, mirror-leech style.
pub fn readable_time_compact(seconds: u64) -> String {
    const PERIODS: [(&str, u64); 4] = [("d", 86_400), ("h", 3_600), ("m", 60), ("s", 1)];
    let mut remaining = seconds;
    let mut out = String::new();
    for (name, period) in PERIODS {
        if remaining >= period {
            let value = remaining / period;
            remaining -= value * period;
            out.push_str(&format!("{value}{name}"));
        }
    }
    if out.is_empty() { "0s".to_owned() } else { out }
}

/// The 12-cell box progress bar with a two-decimal percent using `■`, `▤`, `□`:
/// `[■■■■■■□□□□□□] 50.00%` or `[▤□□□□□□□□□□□] 4.17%`.
pub fn box_progress_bar(pct: f64) -> String {
    let pct = pct.clamp(0.0, 100.0);
    const LENGTH: usize = 12;
    let fraction = pct / 100.0;
    let units = (fraction * (2.0 * LENGTH as f64)).round() as usize;
    let full_count = (units / 2).min(LENGTH);
    let half_count = if units % 2 == 1 && full_count < LENGTH {
        1
    } else {
        0
    };
    let empty_count = LENGTH.saturating_sub(full_count + half_count);
    let half_str = if half_count > 0 { "▤" } else { "" };
    format!(
        "[{}{}{}] {:.2}%",
        "■".repeat(full_count),
        half_str,
        "□".repeat(empty_count),
        pct
    )
}

/// Resolve a user display name or username for mentions.
/// Prefers `@username` if present, then display name ("First Last" or "First"),
/// falling back to "User {user_id}".
pub async fn resolve_user_display_name(client: &ferogram::Client, user_id: i64) -> String {
    if user_id <= 0 {
        return "User".to_string();
    }
    if let Ok(users) = client.get_users_by_id(&[user_id]).await
        && let Some(Some(u)) = users.into_iter().next()
    {
        if let Some(username) = u.username().filter(|n| !n.trim().is_empty()) {
            return format!("@{username}");
        }
        let first = u.first_name().unwrap_or_default().trim();
        let last = u.last_name().unwrap_or_default().trim();
        let full = match (!first.is_empty(), !last.is_empty()) {
            (true, true) => format!("{first} {last}"),
            (true, false) => first.to_owned(),
            (false, true) => last.to_owned(),
            (false, false) => String::new(),
        };
        if !full.is_empty() {
            return full;
        }
    }
    format!("User {user_id}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn symbols_are_semantic_and_neutral_is_plain() {
        assert_eq!(heading(FeedbackKind::Neutral, "Search"), "<b>Search</b>");
        assert_eq!(
            heading(FeedbackKind::Success, "Complete"),
            "✓ <b>Complete</b>"
        );
        assert_eq!(heading(FeedbackKind::Warning, "Paused"), "! <b>Paused</b>");
    }

    #[test]
    fn action_labels_use_words() {
        assert_eq!(action_label("Cancel download", None), "Cancel download");
        assert_eq!(action_label("Cached", Some("Song")), "Cached · Song");
    }

    #[test]
    fn rendition_labels_keep_codec_fallbacks_visible() {
        assert_eq!(rendition_label(Some("alac")), "ALAC");
        assert_eq!(rendition_label(Some("flac")), "FLAC");
        assert_eq!(rendition_label(Some("aac")), "AAC");
        assert_eq!(rendition_label(Some("ec-3")), "Dolby Atmos");
        assert_eq!(rendition_label(None), "ALAC");
    }

    #[test]
    fn readable_sizes_match_mirror_leech() {
        assert_eq!(readable_file_size(0), "0B");
        assert_eq!(readable_file_size(1024), "1.00KB");
        assert_eq!(readable_file_size(593_000_000), "565.53MB");
        assert_eq!(readable_file_size(8_799_493_473), "8.20GB");
    }

    #[test]
    fn readable_times_compact() {
        assert_eq!(readable_time_compact(743), "12m23s");
        assert_eq!(readable_time_compact(216), "3m36s");
        assert_eq!(readable_time_compact(127_305), "1d11h21m45s");
        assert_eq!(readable_time_compact(0), "0s");
    }

    #[test]
    fn box_bar_fills_by_twelfths() {
        assert_eq!(box_progress_bar(0.0), "[□□□□□□□□□□□□] 0.00%");
        assert_eq!(box_progress_bar(100.0), "[■■■■■■■■■■■■] 100.00%");
        assert_eq!(box_progress_bar(50.0), "[■■■■■■□□□□□□] 50.00%");
        assert_eq!(box_progress_bar(4.17), "[▤□□□□□□□□□□□] 4.17%");
        assert_eq!(box_progress_bar(8.33), "[■□□□□□□□□□□□] 8.33%");
    }
}
