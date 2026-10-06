use crate::types::{Codec, TrackRipResult};

pub const MAX_MEDIA_CAPTION_UTF16_LEN: usize = 1024;

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DumpCaptionMetadata<'a> {
    pub track_id: &'a str,
    pub codec: Option<&'a str>,
}

impl<'a> From<(&'a TrackRipResult, &'a str)> for DumpCaptionMetadata<'a> {
    fn from((rip, track_id): (&'a TrackRipResult, &'a str)) -> Self {
        DumpCaptionMetadata {
            track_id,
            codec: Some(&rip.codec),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DumpZipCaptionMetadata<'a> {
    pub album_id: &'a str,
    pub codec: Option<&'a str>,
    pub part_index: i32,
    pub total_parts: i32,
    pub generation_hash: &'a str,
}

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

    pub codec: Option<&'a str>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedZipDumpMetadata {
    pub album_id: String,
    pub codec: Codec,
    pub part_index: i32,
    pub total_parts: i32,
    pub generation_hash: String,
}

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

pub fn format_dump_caption(meta: &DumpCaptionMetadata<'_>) -> String {
    let default_codec = "alac";

    let canonical_codec = meta
        .codec
        .and_then(|c| c.parse::<crate::types::Codec>().ok())
        .map(|c| c.as_str())
        .unwrap_or(default_codec);

    let mut payload = serde_json::json!({
        "track_id": meta.track_id,
        "codec": canonical_codec,
    });

    serialize_caption_payload(&mut payload)
}

pub fn format_zip_dump_caption(
    meta: &DumpZipCaptionMetadata<'_>,
    is_complete: bool,
    _failed_count: usize,
) -> String {
    if !is_complete {
        return String::new();
    }

    let default_codec = "alac";

    let mut payload_json = serde_json::json!({
        "type": "album_zip",
        "album_id": meta.album_id,
        "codec": meta.codec.unwrap_or(default_codec),
        "part": meta.part_index,
        "total_parts": meta.total_parts,
        "hash": meta.generation_hash,
    });
    serialize_caption_payload(&mut payload_json)
}

fn serialize_caption_payload(payload: &mut serde_json::Value) -> String {
    loop {
        let serialized = serde_json::to_string(payload).expect("payload serializes");

        let caption = serialized
            .replace('&', "\\u0026")
            .replace('<', "\\u003c")
            .replace('>', "\\u003e");
        let length = estimate_html_utf16_len(&caption);
        if length <= MAX_MEDIA_CAPTION_UTF16_LEN {
            return caption;
        }

        let longest_string = payload
            .as_object()
            .and_then(|object| {
                object
                    .iter()
                    .filter_map(|(key, value)| {
                        value.as_str().map(|text| (key.clone(), text.to_owned()))
                    })
                    .max_by_key(|(_, text)| text.encode_utf16().count())
            })
            .expect("caption payload has a string value to clamp");
        let (key, value) = longest_string;
        let value_len = value.encode_utf16().count();
        let excess = length - MAX_MEDIA_CAPTION_UTF16_LEN;
        let target_len = value_len.saturating_sub(excess.max(1));
        let shortened = if target_len == 0 {
            String::new()
        } else {
            clamp_str_utf16(&value, target_len)
        };
        payload[&key] = serde_json::Value::String(shortened);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedDumpMetadata {
    pub track_id: String,
    pub codec: Codec,
}

pub fn parse_dump_caption(text: Option<&str>) -> Option<ParsedDumpMetadata> {
    let text = text?;
    let payload = extract_payload(text)?;
    let parsed: serde_json::Value = serde_json::from_str(&payload).ok()?;
    let track_id = parsed.get("track_id")?.as_str()?;
    if track_id.is_empty() {
        return None;
    }

    let default_codec = Codec::Alac;

    let codec = parsed
        .get("codec")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<Codec>().ok())
        .unwrap_or(default_codec);

    Some(ParsedDumpMetadata {
        track_id: track_id.to_owned(),
        codec,
    })
}

pub fn parse_zip_dump_caption(text: Option<&str>) -> Option<ParsedZipDumpMetadata> {
    let text = text?;
    let payload = extract_zip_payload(text)?;
    let parsed: serde_json::Value = serde_json::from_str(&payload).ok()?;
    let album_id = parsed.get("album_id")?.as_str()?;
    if album_id.is_empty() {
        return None;
    }

    let default_codec = Codec::Alac;

    let codec = parsed
        .get("codec")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<Codec>().ok())
        .unwrap_or(default_codec);

    Some(ParsedZipDumpMetadata {
        album_id: album_id.to_owned(),
        codec,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_meta() -> DumpCaptionMetadata<'static> {
        DumpCaptionMetadata {
            track_id: "1440828878",
            codec: Some("alac"),
        }
    }

    fn sample_zip_meta() -> DumpZipCaptionMetadata<'static> {
        DumpZipCaptionMetadata {
            album_id: "1440828878",
            codec: Some("alac"),
            part_index: 1,
            total_parts: 1,
            generation_hash: "generation-123",
        }
    }

    fn assert_json_only_caption(caption: &str) -> serde_json::Value {
        assert!(
            caption.starts_with('{'),
            "caption must start with JSON: {caption}"
        );
        assert!(
            caption.ends_with('}'),
            "caption must end with JSON: {caption}"
        );
        assert!(
            estimate_html_utf16_len(caption) <= MAX_MEDIA_CAPTION_UTF16_LEN,
            "caption exceeds Telegram's UTF-16 limit"
        );
        for old_summary_marker in [
            "<b>",
            "<i>",
            "<code>",
            "<blockquote",
            "Album ZIP part",
            "Partial archive",
        ] {
            assert!(
                !caption.contains(old_summary_marker),
                "caption contains old summary marker {old_summary_marker:?}"
            );
        }
        serde_json::from_str(caption).expect("caption is valid JSON")
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
    fn format_album_details_caption_aac() {
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
            codec: Some("aac"),
        };
        let html = format_album_details_caption(&meta);
        assert!(html.contains("• <b>Quality:</b> AAC 256"));
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
        let meta = sample_meta();
        let caption = format_dump_caption(&meta);
        let json = assert_json_only_caption(&caption);
        for key in ["track_id", "codec"] {
            assert!(json.get(key).is_some(), "missing track payload key {key}");
        }

        let parsed = parse_dump_caption(Some(&caption)).expect("track payload round-trips");
        assert_eq!(parsed.track_id, meta.track_id);
        assert_eq!(parsed.codec, Codec::Alac);
    }

    #[test]
    fn format_dump_caption_codec() {
        let meta = DumpCaptionMetadata {
            track_id: "264126443",
            codec: Some("aac"),
        };
        let caption = format_dump_caption(&meta);
        let json = assert_json_only_caption(&caption);
        assert_eq!(json["codec"], "aac");
        for key in ["track_id", "codec"] {
            assert!(json.get(key).is_some(), "missing track payload key {key}");
        }
        let parsed = parse_dump_caption(Some(&caption)).expect("parsed aac caption");
        assert_eq!(parsed.track_id, "264126443");
        assert_eq!(parsed.codec, Codec::Aac);
    }

    #[test]
    fn parse_dump_caption_returns_none_for_garbage() {
        assert!(parse_dump_caption(None).is_none());
        assert!(parse_dump_caption(Some("just plain text with no json")).is_none());
        assert!(parse_dump_caption(Some("<b>Some Song</b> - Artist")).is_none());
        assert!(parse_dump_caption(Some(r#"{"track_id": ""}"#)).is_none());
    }

    #[test]
    fn zip_caption_contains_only_its_machine_record_and_round_trips() {
        let meta = sample_zip_meta();
        let caption = format_zip_dump_caption(&meta, true, 0);
        let json = assert_json_only_caption(&caption);
        for key in ["type", "album_id", "codec", "part", "total_parts", "hash"] {
            assert!(json.get(key).is_some(), "missing ZIP payload key {key}");
        }
        assert_eq!(json["type"], "album_zip");
        assert!(json.get("album").is_none());
        assert!(json.get("artist").is_none());
        assert!(json.get("filename").is_none());

        let parsed = parse_zip_dump_caption(Some(&caption)).expect("ZIP payload round-trips");
        assert_eq!(parsed.album_id, meta.album_id);
        assert_eq!(parsed.codec, Codec::Alac);
        assert_eq!(parsed.part_index, meta.part_index);
        assert_eq!(parsed.total_parts, meta.total_parts);
        assert_eq!(parsed.generation_hash, meta.generation_hash);
    }

    #[test]
    fn incomplete_zip_has_no_machine_record_or_summary_caption() {
        let caption = format_zip_dump_caption(&sample_zip_meta(), false, 3);
        assert!(caption.is_empty());
        assert!(parse_zip_dump_caption(Some(&caption)).is_none());
    }

    #[test]
    fn pathological_zip_strings_are_clamped_without_breaking_json() {
        let long_hash = "hash_token_abc_".repeat(200);
        let zip_meta = DumpZipCaptionMetadata {
            generation_hash: &long_hash,
            ..sample_zip_meta()
        };
        let zip_caption = format_zip_dump_caption(&zip_meta, true, 0);
        let zip_json = assert_json_only_caption(&zip_caption);
        assert!(zip_json["hash"].as_str().unwrap().len() < long_hash.len());
        assert!(parse_zip_dump_caption(Some(&zip_caption)).is_some());
    }
}
