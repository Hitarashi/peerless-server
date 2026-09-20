//! Dump-channel caption formatting and parsing.
//!
//! The caption has two audiences: a compact operator-facing summary and a
//! canonical machine-readable JSON record consumed by the indexer. Telegram's
//! HTML parser is the equivalent here — the bot crate feeds the returned HTML
//! string to ferogram's HTML support. Captions are clean-slate: the indexer
//! accepts only the current payload shape and does not support older formats.

use crate::types::{Codec, Provider, TrackKey, TrackRipResult};

/// Maximum plain-text length allowed by Telegram for media captions (in UTF-16 code units).
pub const MAX_MEDIA_CAPTION_UTF16_LEN: usize = 1024;

/// Clamps a string to at most `max_utf16` code units, appending an ellipsis ('…') if truncated.
pub fn clamp_str_utf16(text: &str, max_utf16: usize) -> String {
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

/// Estimates the plain-text UTF-16 code unit count after Telegram processes HTML tags and entities.
pub fn estimate_html_utf16_len(html: &str) -> usize {
    let mut count = 0;
    let mut chars = html.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '<' {
            let mut tag_name = String::new();
            while let Some(&next_ch) = chars.peek() {
                if next_ch == '>' {
                    chars.next();
                    break;
                }
                tag_name.push(chars.next().unwrap());
            }
            let tag_lower = tag_name.trim().to_lowercase();
            if tag_lower.starts_with("br") {
                count += 1;
            }
        } else if ch == '&' {
            while let Some(&next_ch) = chars.peek() {
                if next_ch == ';' {
                    chars.next();
                    break;
                }
                if next_ch == ' ' || next_ch == '<' {
                    break;
                }
                chars.next();
            }
            count += 1;
        } else {
            count += ch.len_utf16();
        }
    }
    count
}

/// Everything the dump caption builder takes; `None` fields are simply
/// everything else required.
#[derive(Debug, Clone, PartialEq)]
pub struct DumpCaptionMetadata<'a> {
    pub track_key: TrackKey,
    pub title: &'a str,
    pub artist: &'a str,
    pub album: &'a str,
    pub duration: i64,
    pub bit_depth: u32,
    pub sample_rate: u32,
    pub codec: Option<&'a str>,
    pub genre: Option<&'a str>,
    pub release_date: Option<&'a str>,
    pub track_number: Option<i64>,
    pub track_count: Option<i64>,
    pub isrc: Option<&'a str>,
}

impl<'a> From<(&'a TrackRipResult, Provider, &'a str)> for DumpCaptionMetadata<'a> {
    fn from((rip, provider, track_id): (&'a TrackRipResult, Provider, &'a str)) -> Self {
        DumpCaptionMetadata {
            track_key: TrackKey::new(provider, track_id),
            title: &rip.title,
            artist: &rip.artist,
            album: &rip.album,
            duration: rip.duration,
            bit_depth: rip.bit_depth,
            sample_rate: rip.sample_rate,
            codec: Some(&rip.codec),
            genre: Some(&rip.genre),
            release_date: Some(&rip.release_date),
            track_number: Some(rip.track_number),
            track_count: Some(rip.track_count),
            isrc: rip.isrc.as_deref(),
        }
    }
}

/// Machine-readable metadata for formatting an album ZIP caption.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DumpZipCaptionMetadata<'a> {
    pub provider: Provider,
    pub album_id: &'a str,
    pub codec: Option<&'a str>,
    pub album: &'a str,
    pub artist: &'a str,
    pub filename: &'a str,
    pub part_index: i32,
    pub total_parts: i32,
    pub generation_hash: &'a str,
}

/// Metadata used to format the rich album details caption (delivered alongside the preview photo or as fallback text).
#[derive(Debug, Clone, PartialEq)]
pub struct AlbumDetailsCaptionMetadata<'a> {
    pub album: &'a str,
    pub artist: &'a str,
    pub album_url: Option<&'a str>,
    pub total_tracks: usize,
    pub delivered_tracks: Option<usize>,
    pub size_bytes: i64,
    pub total_parts: usize,
    pub release_year: &'a str,
    pub genre: Option<&'a str>,
    pub record_label: Option<&'a str>,
    pub is_partial: bool,
    pub user_name: Option<&'a str>,
    pub user_id: i64,
    /// Highest codec delivered in the archive (`alac`, `aac`, `mp4a.40.2`, `ec-3`, `flac`).
    pub codec: Option<&'a str>,
}

/// Parsed metadata extracted from an album ZIP dump caption.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedZipDumpMetadata {
    pub provider: Provider,
    pub album_id: String,
    pub codec: Codec,
    pub album: String,
    pub artist: String,
    pub part_index: i32,
    pub total_parts: i32,
    pub generation_hash: String,
}

/// Escape `& < > " '` with hex entities (mtcute-compatible).
pub fn html_escape(input: &str) -> String {
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

/// Formats a user mention with clickable link (Telegram handle or tg:// user link).
pub fn format_requester_mention(user_name: Option<&str>, user_id: i64) -> String {
    let name = user_name
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or("User");
    if let Some(handle) = name.strip_prefix('@') {
        format!(r#"<a href="https://t.me/{handle}">@{handle}</a>"#)
    } else if user_id > 0 {
        format!(
            r#"<a href="tg://user?id={user_id}">{}</a>"#,
            html_escape(name)
        )
    } else {
        html_escape(name)
    }
}

/// Formats the rich album details caption (displayed with album preview photo or fallback text).
pub fn format_album_details_caption(meta: &AlbumDetailsCaptionMetadata<'_>) -> String {
    let album_link = match meta.album_url {
        Some(url) => format!(
            r#"💿 <a href="{}"><b>{}</b></a>"#,
            html_escape(url),
            html_escape(meta.album)
        ),
        None => format!("💿 <b>{}</b>", html_escape(meta.album)),
    };

    let tracks = match meta.delivered_tracks {
        Some(delivered) if meta.is_partial => format!("{delivered}/{} tracks", meta.total_tracks),
        Some(delivered) => format!("{delivered} tracks"),
        None if meta.is_partial => format!("?/{total} tracks", total = meta.total_tracks),
        None => "unknown tracks".to_owned(),
    };

    let size = crate::progress::format_bytes(meta.size_bytes.max(0) as u64);
    let parts_info = if meta.total_parts > 1 {
        format!(" · {} parts", meta.total_parts)
    } else {
        String::new()
    };

    let mention = format_requester_mention(meta.user_name, meta.user_id);

    let mut bullets = Vec::new();
    bullets.push(format!("• <b>Tracks:</b> {tracks}"));
    bullets.push(format!("• <b>Size:</b> {size}{parts_info}"));
    if !meta.release_year.is_empty() {
        bullets.push(format!(
            "• <b>Released:</b> {}",
            html_escape(meta.release_year)
        ));
    }
    if let Some(genre) = meta.genre.filter(|s| !s.is_empty()) {
        bullets.push(format!("• <b>Genre:</b> {}", html_escape(genre)));
    }
    if let Some(label) = meta.record_label.filter(|s| !s.is_empty()) {
        bullets.push(format!("• <b>Label:</b> {}", html_escape(label)));
    }
    bullets.push(format!(
        "• <b>Quality:</b> {}",
        match meta.codec {
            Some("ec-3") => "Dolby Atmos".to_owned(),
            Some("aac") | Some("mp4a.40.2") | Some("mp4a.40.5") => "AAC 256".to_owned(),
            Some("flac") => "Lossless · FLAC".to_owned(),
            _ => "Lossless · ALAC".to_owned(),
        }
    ));
    if meta.is_partial {
        bullets.push("• ⚠️ <b>Note:</b> Partial archive".to_string());
    }
    bullets.push(format!("• <b>Requested by:</b> {mention}"));

    format!(
        "{album_link}<br/>👤 <b>Artist:</b> {}<br/><br/><blockquote>{}</blockquote>",
        html_escape(meta.artist),
        bullets.join("<br/>")
    )
}

/// Formats a compact operational caption.
///
/// The first three lines are for quick scanning in the dump channel. The
/// expandable block contains the single canonical machine record consumed by
/// the indexer; human-readable metadata is intentionally not repeated there.
/// If the rendered text exceeds Telegram's 1,024 UTF-16 code unit limit, the
/// format automatically falls back to compact JSON and abbreviated display text.
pub fn format_dump_caption(meta: &DumpCaptionMetadata<'_>) -> String {
    let track_number = meta.track_number.unwrap_or(1);
    let track_count = meta.track_count.unwrap_or(1);
    let codec_label = codec_display(meta.codec);
    let quality_line = match meta.codec {
        Some("alac") | Some("flac") | None => format!(
            "{codec_label} · {}-bit · {:.1} kHz",
            meta.bit_depth,
            meta.sample_rate as f64 / 1000.0,
        ),
        Some("ec-3") => format!(
            "{codec_label} · {:.1} kHz",
            meta.sample_rate as f64 / 1000.0,
        ),
        Some(_) => format!("{codec_label} · 256 kbps"),
    };

    let default_codec = match meta.track_key.provider {
        Provider::Qobuz => "flac",
        _ => "alac",
    };

    let canonical_codec = meta
        .codec
        .and_then(|c| c.parse::<crate::types::Codec>().ok())
        .map(|c| c.as_str())
        .unwrap_or(default_codec);

    let make_summary = |artist: &str, title: &str, album: &str| {
        format!(
            "<b>{}</b> — {}<br/><i>{}</i> · <code>{track_number}/{track_count}</code><br/><code>{quality_line}</code>",
            html_escape(title),
            html_escape(artist),
            html_escape(album),
        )
    };

    let make_payload = |artist: &str, title: &str, album: &str| {
        let mut obj = serde_json::json!({
            "provider": meta.track_key.provider,
            "track_id": meta.track_key.track_id,
            "codec": canonical_codec,
            "title": title,
            "artist": artist,
            "album": album,
            "dur": meta.duration,
            "bit": meta.bit_depth,
            "hz": meta.sample_rate,
            "genre": meta.genre.unwrap_or("Music"),
            "date": meta.release_date.unwrap_or(""),
            "trk": meta.track_number.unwrap_or(1),
            "cnt": meta.track_count.unwrap_or(1),
        });
        if let Some(isrc) = meta.isrc.filter(|s| !s.is_empty()) {
            obj["isrc"] = serde_json::Value::String(isrc.to_owned());
        }
        obj
    };

    // 1. Preferred layout: full human summary + pretty indented JSON
    let summary = make_summary(meta.artist, meta.title, meta.album);
    let payload = make_payload(meta.artist, meta.title, meta.album);
    let pretty = serde_json::to_string_pretty(&payload).expect("payload serializes");
    let payload_html = pretty
        .lines()
        .map(html_escape)
        .collect::<Vec<_>>()
        .join("<br/>");
    let candidate = format!("{summary}<br/><blockquote expandable>{payload_html}</blockquote>");

    if estimate_html_utf16_len(&candidate) <= MAX_MEDIA_CAPTION_UTF16_LEN {
        return candidate;
    }

    // 2. Compact JSON layout if pretty JSON exceeded limit
    let compact = serde_json::to_string(&payload).expect("payload serializes");
    let compact_candidate = format!(
        "{summary}<br/><blockquote expandable>{}</blockquote>",
        html_escape(&compact)
    );

    if estimate_html_utf16_len(&compact_candidate) <= MAX_MEDIA_CAPTION_UTF16_LEN {
        return compact_candidate;
    }

    // 3. Clamped layout: abbreviate long metadata strings (e.g. tracks with dozens of featured artists)
    let clamped_summary_artist = clamp_str_utf16(meta.artist, 120);
    let clamped_summary_title = clamp_str_utf16(meta.title, 120);
    let clamped_summary_album = clamp_str_utf16(meta.album, 120);
    let summary_clamped = make_summary(
        &clamped_summary_artist,
        &clamped_summary_title,
        &clamped_summary_album,
    );

    let clamped_payload_artist = clamp_str_utf16(meta.artist, 150);
    let clamped_payload_title = clamp_str_utf16(meta.title, 150);
    let clamped_payload_album = clamp_str_utf16(meta.album, 150);
    let payload_clamped = make_payload(
        &clamped_payload_artist,
        &clamped_payload_title,
        &clamped_payload_album,
    );

    let compact_clamped = serde_json::to_string(&payload_clamped).expect("payload serializes");
    let clamped_candidate = format!(
        "{summary_clamped}<br/><blockquote expandable>{}</blockquote>",
        html_escape(&compact_clamped)
    );

    if estimate_html_utf16_len(&clamped_candidate) <= MAX_MEDIA_CAPTION_UTF16_LEN {
        return clamped_candidate;
    }

    // 4. Hard safety fallback: further truncate summary strings
    let tight_summary_artist = clamp_str_utf16(meta.artist, 60);
    let tight_summary_title = clamp_str_utf16(meta.title, 60);
    let tight_summary_album = clamp_str_utf16(meta.album, 60);
    let summary_tight = make_summary(
        &tight_summary_artist,
        &tight_summary_title,
        &tight_summary_album,
    );
    format!(
        "{summary_tight}<br/><blockquote expandable>{}</blockquote>",
        html_escape(&compact_clamped)
    )
}

/// Formats an album ZIP caption.
///
/// If `total_parts <= 1`, the "part 1 of 1" line is omitted entirely.
/// If `total_parts > 1`, "Album ZIP part X of Y" is included.
/// Complete archives include an expandable blockquote with the machine-readable JSON payload.
pub fn format_zip_dump_caption(
    meta: &DumpZipCaptionMetadata<'_>,
    is_complete: bool,
    failed_count: usize,
) -> String {
    let header = format!("<b>{}</b>", html_escape(meta.filename));
    let part_line = if meta.total_parts > 1 {
        format!(
            "<br/><i>Album ZIP part {} of {}</i>",
            meta.part_index, meta.total_parts
        )
    } else {
        String::new()
    };
    let partial_note = if is_complete {
        String::new()
    } else {
        format!("<br/>Partial archive; {failed_count} track(s) failed.")
    };
    let summary = format!("{header}{part_line}{partial_note}");

    if !is_complete {
        return summary;
    }

    let default_codec = match meta.provider {
        Provider::Qobuz => "flac",
        _ => "alac",
    };

    let payload_json = serde_json::json!({
        "type": "album_zip",
        "provider": meta.provider,
        "album_id": meta.album_id,
        "codec": meta.codec.unwrap_or(default_codec),
        "album": meta.album,
        "artist": meta.artist,
        "part": meta.part_index,
        "total_parts": meta.total_parts,
        "hash": meta.generation_hash,
    });
    let pretty = serde_json::to_string_pretty(&payload_json).expect("payload serializes");
    let payload_html = pretty
        .lines()
        .map(html_escape)
        .collect::<Vec<_>>()
        .join("<br/>");

    let candidate = format!("{summary}<br/><blockquote expandable>{payload_html}</blockquote>");
    if estimate_html_utf16_len(&candidate) <= MAX_MEDIA_CAPTION_UTF16_LEN {
        return candidate;
    }

    let compact = serde_json::to_string(&payload_json).expect("payload serializes");
    let compact_candidate = format!(
        "{summary}<br/><blockquote expandable>{}</blockquote>",
        html_escape(&compact)
    );
    if estimate_html_utf16_len(&compact_candidate) <= MAX_MEDIA_CAPTION_UTF16_LEN {
        return compact_candidate;
    }

    let clamped_album = clamp_str_utf16(meta.album, 120);
    let clamped_artist = clamp_str_utf16(meta.artist, 120);
    let clamped_payload = serde_json::json!({
        "type": "album_zip",
        "provider": meta.provider,
        "album_id": meta.album_id,
        "codec": meta.codec.unwrap_or(default_codec),
        "album": clamped_album,
        "artist": clamped_artist,
        "part": meta.part_index,
        "total_parts": meta.total_parts,
        "hash": meta.generation_hash,
    });
    let compact_clamped = serde_json::to_string(&clamped_payload).expect("payload serializes");
    format!(
        "{summary}<br/><blockquote expandable>{}</blockquote>",
        html_escape(&compact_clamped)
    )
}

pub struct ParsedDumpMetadata {
    pub track_key: TrackKey,
    pub codec: Codec,
    pub title: String,
    pub artist: String,
    pub album: String,
    pub duration: i64,
    pub bit_depth: u32,
    pub sample_rate: u32,
    pub genre: String,
    pub release_date: String,
    pub track_number: i64,
    pub track_count: i64,
    pub isrc: Option<String>,
}

/// Extracts structured track metadata from a message caption. Returns
/// `None` for missing, stale, unrelated, or invalid records.
pub fn parse_dump_caption(text: Option<&str>) -> Option<ParsedDumpMetadata> {
    let text = text?;
    let payload = extract_payload(text)?;
    let parsed: serde_json::Value = serde_json::from_str(&payload).ok()?;
    let provider = parsed.get("provider")?.as_str()?.parse::<Provider>().ok()?;
    let track_id = parsed.get("track_id")?.as_str()?;
    if track_id.is_empty() {
        return None;
    }

    let default_codec = match provider {
        Provider::Qobuz => Codec::Flac,
        _ => Codec::Alac,
    };

    let codec = parsed
        .get("codec")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<Codec>().ok())
        .unwrap_or(default_codec);

    Some(ParsedDumpMetadata {
        track_key: TrackKey::new(provider, track_id).with_codec(codec),
        codec,
        title: string_field(&parsed, "title"),
        artist: string_field(&parsed, "artist"),
        album: string_field(&parsed, "album"),
        duration: number_field(&parsed, "dur", 0),
        bit_depth: number_field(&parsed, "bit", 16).max(0) as u32,
        sample_rate: number_field(&parsed, "hz", 44100).max(0) as u32,
        genre: string_field_or(&parsed, "genre", "Music"),
        release_date: string_field(&parsed, "date"),
        track_number: number_field(&parsed, "trk", 1),
        track_count: number_field(&parsed, "cnt", 1),
        isrc: parsed
            .get("isrc")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(ToOwned::to_owned),
    })
}

/// Extracts structured album ZIP metadata from a message caption.
pub fn parse_zip_dump_caption(text: Option<&str>) -> Option<ParsedZipDumpMetadata> {
    let text = text?;
    let payload = extract_zip_payload(text)?;
    let parsed: serde_json::Value = serde_json::from_str(&payload).ok()?;
    let provider = parsed.get("provider")?.as_str()?.parse::<Provider>().ok()?;
    let album_id = parsed.get("album_id")?.as_str()?;
    if album_id.is_empty() {
        return None;
    }

    let default_codec = match provider {
        Provider::Qobuz => Codec::Flac,
        _ => Codec::Alac,
    };

    let codec = parsed
        .get("codec")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<Codec>().ok())
        .unwrap_or(default_codec);

    Some(ParsedZipDumpMetadata {
        provider,
        album_id: album_id.to_owned(),
        codec,
        album: string_field(&parsed, "album"),
        artist: string_field(&parsed, "artist"),
        part_index: number_field(&parsed, "part", 1).max(1) as i32,
        total_parts: number_field(&parsed, "total_parts", 1).max(1) as i32,
        generation_hash: string_field(&parsed, "hash"),
    })
}

fn string_field(value: &serde_json::Value, key: &str) -> String {
    string_field_or(value, key, "")
}

fn string_field_or(value: &serde_json::Value, key: &str, default: &str) -> String {
    match value.get(key) {
        Some(serde_json::Value::String(s)) => s.clone(),
        _ => default.to_string(),
    }
}

fn number_field(value: &serde_json::Value, key: &str, default: i64) -> i64 {
    match value.get(key) {
        Some(v) => v.as_i64().unwrap_or(default),
        None => default,
    }
}

fn extract_payload(text: &str) -> Option<String> {
    extract_balanced_json(text, "\"track_id\"")
}

fn extract_zip_payload(text: &str) -> Option<String> {
    extract_balanced_json(text, "\"album_id\"")
}

fn extract_balanced_json(text: &str, required_key: &str) -> Option<String> {
    let normalized = text
        .replace("<br/>", "\n")
        .replace("<br>", "\n")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&amp;", "&");

    for (start, _) in normalized.match_indices('{') {
        let candidate = &normalized[start..];
        if !candidate.contains(required_key) {
            continue;
        }
        let mut depth = 0;
        let mut in_str = false;
        let mut escape = false;
        let mut end_idx = None;

        for (idx, ch) in candidate.char_indices() {
            if escape {
                escape = false;
                continue;
            }
            if ch == '\\' && in_str {
                escape = true;
                continue;
            }
            if ch == '"' {
                in_str = !in_str;
                continue;
            }
            if !in_str {
                if ch == '{' {
                    depth += 1;
                } else if ch == '}' {
                    depth -= 1;
                    if depth == 0 {
                        end_idx = Some(idx);
                        break;
                    }
                }
            }
        }

        if let Some(end) = end_idx {
            let json_str = &candidate[..=end];
            if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(json_str) {
                let stripped_key = required_key.trim_matches('"');
                if parsed.get(stripped_key).is_some() {
                    return Some(json_str.to_string());
                }
            }
        }
    }
    None
}

/// Compact codec label for captions: `ALAC`, `FLAC`, `AAC`, `Dolby Atmos`.
fn codec_display(codec: Option<&str>) -> &'static str {
    match codec {
        Some("alac") | None => "ALAC",
        Some("flac") => "FLAC",
        Some("ec-3") => "Dolby Atmos",
        Some("aac") | Some("mp4a.40.2") | Some("mp4a.40.5") => "AAC",
        Some(_) => "Audio",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_meta() -> DumpCaptionMetadata<'static> {
        DumpCaptionMetadata {
            track_key: TrackKey::apple("1440828878"),
            title: "Night Song",
            artist: "A&R <duo>",
            album: "Escapes",
            duration: 215,
            bit_depth: 24,
            sample_rate: 48000,
            codec: Some("alac"),
            genre: Some("Electronic"),
            release_date: Some("2021-06-04"),
            track_number: Some(2),
            track_count: Some(10),
            isrc: Some("USUM71703861"),
        }
    }

    #[test]
    fn format_album_details_caption_single_part() {
        let meta = AlbumDetailsCaptionMetadata {
            album: "Fossils, Vol. 1",
            artist: "Rupam Islam & Fossils",
            album_url: Some("https://music.apple.com/in/album/1440828878"),
            total_tracks: 8,
            delivered_tracks: Some(8),
            size_bytes: 289_950_924,
            total_parts: 1,
            release_year: "2001",
            genre: Some("Rock"),
            record_label: Some("Asha Audio"),
            is_partial: false,
            user_name: Some("@sayeed69"),
            user_id: 123456,
            codec: None,
        };
        let html = format_album_details_caption(&meta);
        assert!(html.contains(
            r#"💿 <a href="https://music.apple.com/in/album/1440828878"><b>Fossils, Vol. 1</b></a>"#
        ));
        assert!(html.contains("👤 <b>Artist:</b> Rupam Islam &amp; Fossils"));
        assert!(html.contains("<blockquote>"));
        assert!(html.contains("• <b>Tracks:</b> 8 tracks"));
        assert!(html.contains("• <b>Size:</b> 276.52MB"));
        // Single part: NO "parts" text
        assert!(!html.contains("parts"));
        assert!(html.contains("• <b>Released:</b> 2001"));
        assert!(html.contains("• <b>Genre:</b> Rock"));
        assert!(html.contains("• <b>Label:</b> Asha Audio"));
        assert!(html.contains("• <b>Quality:</b> Lossless · ALAC"));
        assert!(
            html.contains(
                r#"• <b>Requested by:</b> <a href="https://t.me/sayeed69">@sayeed69</a>"#
            )
        );
        assert!(html.ends_with("</blockquote>"));
    }

    #[test]
    fn format_album_details_caption_flac() {
        let meta = AlbumDetailsCaptionMetadata {
            album: "ROCKSTAR WITHOUT A GUITAR",
            artist: "UMAIR",
            album_url: None,
            total_tracks: 20,
            delivered_tracks: Some(20),
            size_bytes: 832_500_000,
            total_parts: 1,
            release_year: "2024",
            genre: Some("Hip-Hop/Rap"),
            record_label: None,
            is_partial: false,
            user_name: None,
            user_id: 0,
            codec: Some("flac"),
        };
        let html = format_album_details_caption(&meta);
        assert!(html.contains("• <b>Quality:</b> Lossless · FLAC"));
    }

    #[test]
    fn format_album_details_caption_multi_part_partial() {
        let meta = AlbumDetailsCaptionMetadata {
            album: "Greatest Hits",
            artist: "Queen",
            album_url: Some("https://music.apple.com/us/album/987654321"),
            total_tracks: 17,
            delivered_tracks: Some(15),
            size_bytes: 4_294_967_296,
            total_parts: 3,
            release_year: "1981",
            genre: None,
            record_label: None,
            is_partial: true,
            user_name: Some("John Doe"),
            user_id: 78910,
            codec: None,
        };
        let html = format_album_details_caption(&meta);
        assert!(html.contains("• <b>Tracks:</b> 15/17 tracks"));
        assert!(html.contains(" · 3 parts"));
        assert!(html.contains("• ⚠️ <b>Note:</b> Partial archive"));
        assert!(
            html.contains(r#"• <b>Requested by:</b> <a href="tg://user?id=78910">John Doe</a>"#)
        );
        assert!(html.ends_with("</blockquote>"));
    }

    #[test]
    fn format_album_details_caption_does_not_fabricate_unknown_track_count() {
        let meta = AlbumDetailsCaptionMetadata {
            album: "Legacy Atmos",
            artist: "Artist",
            album_url: None,
            total_tracks: 2,
            delivered_tracks: None,
            size_bytes: 1024,
            total_parts: 1,
            release_year: "2024",
            genre: None,
            record_label: None,
            is_partial: false,
            user_name: None,
            user_id: 0,
            codec: Some("ec-3"),
        };
        let html = format_album_details_caption(&meta);
        assert!(html.contains("• <b>Tracks:</b> unknown tracks"));
    }

    #[test]
    fn caption_is_compact_and_keeps_machine_record() {
        let html = format_dump_caption(&sample_meta());
        assert!(html.starts_with("<b>Night Song</b> — A&amp;R &lt;duo&gt;"));
        assert!(html.contains("<i>Escapes</i> · <code>2/10</code>"));
        assert!(html.contains("<code>ALAC · 24-bit · 48.0 kHz</code>"));
        assert!(html.contains("<blockquote expandable>"));
        assert!(html.ends_with("</blockquote>"));
    }

    #[test]
    fn format_dump_caption_flac() {
        let meta = DumpCaptionMetadata {
            track_key: TrackKey::new(Provider::Qobuz, "264126443"),
            title: "HEARTBREAK CITY",
            artist: "UMAIR",
            album: "ROCKSTAR WITHOUT A GUITAR",
            duration: 372,
            bit_depth: 24,
            sample_rate: 48000,
            codec: Some("flac"),
            genre: Some("Hip-Hop/Rap"),
            release_date: Some("2024-04-25"),
            track_number: Some(16),
            track_count: Some(20),
            isrc: None,
        };
        let caption = format_dump_caption(&meta);
        assert!(caption.contains("<code>FLAC · 24-bit · 48.0 kHz</code>"));
        let unescaped = caption
            .replace("&quot;", "\"")
            .replace("&amp;", "&")
            .replace("&lt;", "<")
            .replace("&gt;", ">")
            .replace("&#39;", "'")
            .replace("<br/>", "\n");
        let parsed = parse_dump_caption(Some(&unescaped)).expect("parsed flac caption");
        assert_eq!(parsed.codec, Codec::Flac);
        assert_eq!(parsed.bit_depth, 24);
        assert_eq!(parsed.sample_rate, 48000);
    }

    #[test]
    fn parse_dump_caption_extracts_all_fields() {
        let html = format_dump_caption(&sample_meta());
        let unescaped = html
            .replace("&quot;", "\"")
            .replace("&amp;", "&")
            .replace("&lt;", "<")
            .replace("&gt;", ">")
            .replace("&#39;", "'")
            .replace("<br/>", "\n");
        let parsed = parse_dump_caption(Some(&unescaped)).expect("payload parsed");
        assert_eq!(parsed.track_key.track_id, "1440828878");
        assert_eq!(parsed.codec, Codec::Alac);
        assert_eq!(parsed.title, "Night Song");
        assert_eq!(parsed.artist, "A&R <duo>");
        assert_eq!(parsed.album, "Escapes");
        assert_eq!(parsed.duration, 215);
        assert_eq!(parsed.bit_depth, 24);
        assert_eq!(parsed.sample_rate, 48000);
        assert_eq!(parsed.genre, "Electronic");
        assert_eq!(parsed.release_date, "2021-06-04");
        assert_eq!(parsed.track_number, 2);
        assert_eq!(parsed.track_count, 10);
        assert_eq!(parsed.isrc.as_deref(), Some("USUM71703861"));
    }

    #[test]
    fn parse_dump_caption_returns_none_for_garbage() {
        assert!(parse_dump_caption(None).is_none());
        assert!(parse_dump_caption(Some("just plain text with no json")).is_none());
        assert!(parse_dump_caption(Some("<b>Some Song</b> - Artist")).is_none());
        assert!(parse_dump_caption(Some(r#"{"track_id": ""}"#)).is_none());
    }

    #[test]
    fn dump_caption_falls_back_to_compact_when_pretty_exceeds_limit() {
        let long_title = "A".repeat(400);
        let long_artist_sample = "B".repeat(200);
        let long_album = "C".repeat(200);
        let meta = DumpCaptionMetadata {
            track_key: TrackKey::apple("1440828878"),
            title: &long_title,
            artist: &long_artist_sample,
            album: &long_album,
            duration: 180,
            bit_depth: 16,
            sample_rate: 44100,
            codec: Some("alac"),
            genre: Some("Pop"),
            release_date: Some("2024-01-01"),
            track_number: Some(1),
            track_count: Some(1),
            isrc: None,
        };
        let caption = format_dump_caption(&meta);
        let utf16_len = estimate_html_utf16_len(&caption);
        assert!(
            utf16_len <= MAX_MEDIA_CAPTION_UTF16_LEN,
            "caption utf16 length {utf16_len} exceeds limit {MAX_MEDIA_CAPTION_UTF16_LEN}"
        );
    }

    #[test]
    fn dump_caption_with_extremely_long_artist_fits_within_telegram_limit() {
        let long_artist = "Rochak Kohli, Jubin Nautiyal, Tulsi Kumar, Sachet-Parampara, Parampara Tandon, Neha Kakkar, Guru Randhawa, Yo Yo Honey Singh, Darshan Raval, Neeti Mohan, Tanishk Bagchi, Meet Bros., Monali Thakur, Amaal Mallik, Armaan Malik, Mithoon, Shreya Ghoshal, Akhil Sachdeva, Mansheel Gujral, Dhvani Bhanushali, B Praak, Jasleen Royal, Harshdeep Kaur, Shekhar Ravjiani, Payal Dev & Stebin Ben";
        let meta = DumpCaptionMetadata {
            track_key: TrackKey::apple("1529537935"),
            title: "Love Mashup 2020(Remix By Kedrock,Sd Style)",
            artist: long_artist,
            album: "Love Mashup 2020(Remix By Kedrock,Sd Style) - Single",
            duration: 254,
            bit_depth: 24,
            sample_rate: 44100,
            codec: Some("alac"),
            genre: Some("Bollywood"),
            release_date: Some("2020-08-25"),
            track_number: Some(1),
            track_count: Some(1),
            isrc: None,
        };
        let caption = format_dump_caption(&meta);
        let utf16_len = estimate_html_utf16_len(&caption);
        assert!(
            utf16_len <= MAX_MEDIA_CAPTION_UTF16_LEN,
            "caption utf16 length {utf16_len} exceeds limit {MAX_MEDIA_CAPTION_UTF16_LEN}"
        );
        // Verify parse_dump_caption succeeds on the resulting caption
        let unescaped = caption
            .replace("&quot;", "\"")
            .replace("&amp;", "&")
            .replace("&lt;", "<")
            .replace("&gt;", ">")
            .replace("&#39;", "'")
            .replace("<br/>", "\n");
        let parsed =
            parse_dump_caption(Some(&unescaped)).expect("payload parsed from long artist caption");
        assert_eq!(parsed.track_key.track_id, "1529537935");
        assert_eq!(parsed.codec, Codec::Alac);
        assert_eq!(parsed.duration, 254);
    }
}
