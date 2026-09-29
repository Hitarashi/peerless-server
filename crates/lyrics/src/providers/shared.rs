//! Helpers shared by the provider adapters.
//!
//! Every gate below is the union of the providers that actually call the
//! item, because the workspace denies `dead_code`: an item that compiles
//! under a feature combination nobody uses would fail the build.

#[cfg(any(
    feature = "amll-ttml-db",
    feature = "binimum",
    feature = "kugou",
    feature = "musixmatch",
    feature = "netease",
    feature = "qq",
    feature = "spotify"
))]
use serde_json::Value;

#[cfg(any(
    feature = "binimum",
    feature = "kugou",
    feature = "musixmatch",
    feature = "netease",
    feature = "qq",
    feature = "spotify"
))]
pub(super) fn json(body: &str) -> Option<Value> {
    serde_json::from_str(body).ok()
}

#[cfg(any(
    feature = "amll-ttml-db",
    feature = "binimum",
    feature = "kugou",
    feature = "musixmatch",
    feature = "netease",
    feature = "qq",
    feature = "spotify"
))]
pub(super) fn value_text(value: &Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .map(str::to_owned)
}

#[cfg(any(
    feature = "binimum",
    feature = "kugou",
    feature = "musixmatch",
    feature = "netease",
    feature = "qq"
))]
pub(super) fn number(value: &Value, key: &str) -> Option<u64> {
    value.get(key).and_then(Value::as_u64)
}

#[cfg(any(
    feature = "amll-ttml-db",
    feature = "binimum",
    feature = "kugou",
    feature = "musixmatch",
    feature = "netease",
    feature = "qq",
    feature = "youtube"
))]
fn artist_parts(value: &str) -> Vec<String> {
    value
        .split([',', '/', '&', '、', ';'])
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(str::to_owned)
        .collect()
}

#[cfg(any(
    feature = "amll-ttml-db",
    feature = "binimum",
    feature = "kugou",
    feature = "musixmatch",
    feature = "netease",
    feature = "qq",
    feature = "youtube"
))]
fn artist_key(value: &str) -> String {
    let tokens: Vec<String> = value
        .split(|character: char| !character.is_alphanumeric())
        .filter(|token| !token.is_empty())
        .map(str::to_lowercase)
        .collect();
    match tokens.as_slice() {
        [] => String::new(),
        [single] => single.clone(),
        many => {
            let last = many.last().cloned().unwrap_or_default();
            let initials = many[..many.len() - 1]
                .iter()
                .filter_map(|token| token.chars().next())
                .collect::<String>();
            format!("{initials}{last}")
        }
    }
}

#[cfg(any(
    feature = "amll-ttml-db",
    feature = "binimum",
    feature = "kugou",
    feature = "musixmatch",
    feature = "netease",
    feature = "qq",
    feature = "youtube"
))]
fn title_key(value: &str) -> String {
    value
        .chars()
        .filter(|character| character.is_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect()
}

#[cfg(any(
    feature = "amll-ttml-db",
    feature = "binimum",
    feature = "kugou",
    feature = "musixmatch",
    feature = "netease",
    feature = "qq",
    feature = "youtube"
))]
fn title_matches(expected: &str, actual: &str) -> bool {
    let expected = title_key(expected);
    let actual = title_key(actual);
    !expected.is_empty()
        && !actual.is_empty()
        && (expected == actual || actual.contains(&expected) || expected.contains(&actual))
}

#[cfg(any(
    feature = "amll-ttml-db",
    feature = "binimum",
    feature = "kugou",
    feature = "musixmatch",
    feature = "netease",
    feature = "qq",
    feature = "youtube"
))]
fn artists_match(expected: &str, actual: &str) -> bool {
    artist_parts(expected).iter().any(|wanted| {
        let wanted = artist_key(wanted);
        !wanted.is_empty()
            && artist_parts(actual).iter().any(|found| {
                let found = artist_key(found);
                wanted == found
            })
    })
}

#[cfg(any(
    feature = "amll-ttml-db",
    feature = "binimum",
    feature = "kugou",
    feature = "musixmatch",
    feature = "netease",
    feature = "qq",
    feature = "youtube"
))]
pub(super) fn metadata_score(
    input: &LyricsLookup,
    title: &str,
    artist: &str,
    album: Option<&str>,
    duration: Option<u64>,
) -> Option<i64> {
    if !title_matches(&input.title, title) || !artists_match(&input.artist_string(), artist) {
        return None;
    }
    if let (Some(expected), Some(actual)) = (input.duration, duration)
        && expected > 0
        && expected.abs_diff(actual as i64) > 12
    {
        return None;
    }

    let mut score = 160;
    if title_key(&input.title) == title_key(title) {
        score += 30;
    }
    if let Some(expected) = input.album.as_deref()
        && album.is_some_and(|actual| title_matches(expected, actual))
    {
        score += 25;
    }
    if let (Some(expected), Some(actual)) = (input.duration, duration) {
        score += match expected.abs_diff(actual as i64) {
            0..=2 => 30,
            3..=5 => 20,
            _ => 10,
        };
    }
    Some(score)
}

#[cfg(any(
    feature = "amll-ttml-db",
    feature = "binimum",
    feature = "kugou",
    feature = "netease",
    feature = "qq",
    feature = "youtube"
))]
fn add_metadata_score(
    candidate: &mut LyricsCandidate,
    input: &LyricsLookup,
    title: &str,
    artist: &str,
    album: Option<&str>,
    duration: Option<u64>,
) -> bool {
    let Some(score) = metadata_score(input, title, artist, album, duration) else {
        return false;
    };
    candidate.ranking.score += score;
    true
}

#[cfg(any(
    feature = "kugou",
    feature = "musixmatch",
    feature = "netease",
    feature = "qq",
    feature = "spotify"
))]
pub(super) fn quote(value: &str) -> String {
    form_encode(value)
}

#[allow(dead_code)]
pub(super) fn form_encode(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(byte as char)
            }
            b' ' => encoded.push('+'),
            _ => encoded.push_str(&format!("%{byte:02X}")),
        }
    }
    encoded
}

#[cfg(any(
    feature = "kugou",
    feature = "musixmatch",
    feature = "netease",
    feature = "qq"
))]
pub(super) fn parsed_lyrics_to_text(raw: &str, kind: LyricsRawTypes) -> Option<String> {
    let parsed = parse_provider_lyrics(raw, kind)?;
    let lines: Vec<String> = parsed
        .lines?
        .into_iter()
        .filter_map(|line| {
            let text = line.text_from_any();
            let syllables = line.syllables().unwrap_or_default();
            let words: Vec<String> = syllables
                .iter()
                .filter_map(|syllable| {
                    let word = syllable.text();
                    if word.is_empty() || syllable.start_time() < 0 {
                        return None;
                    }
                    Some(format!(
                        "<{}>{word}",
                        crate::format_millis(syllable.start_time() as u64)
                    ))
                })
                .collect();
            let start = line
                .start_time()
                .or_else(|| syllables.first().map(|word| word.start_time()))?;
            if start < 0 || (text.trim().is_empty() && words.is_empty()) {
                return None;
            }
            if !words.is_empty() {
                Some(format!(
                    "[{}]{}",
                    crate::format_millis(start as u64),
                    words.join("")
                ))
            } else {
                Some(format!("[{}]{text}", crate::format_millis(start as u64)))
            }
        })
        .collect();
    if lines.is_empty() {
        return None;
    }
    Some(lines.join("\n"))
}

#[cfg(any(
    feature = "amll-ttml-db",
    feature = "binimum",
    feature = "kugou",
    feature = "netease",
    feature = "qq",
    feature = "youtube"
))]
pub(super) struct RecordingMetadata<'a> {
    pub(super) title: &'a str,
    pub(super) artist: &'a str,
    pub(super) album: Option<&'a str>,
    pub(super) duration: Option<u64>,
}

#[cfg(any(
    feature = "amll-ttml-db",
    feature = "binimum",
    feature = "kugou",
    feature = "netease",
    feature = "qq",
    feature = "youtube"
))]
pub(super) fn lrc_candidate(
    lyrics: &str,
    provider: &str,
    source_id: &str,
    url: &str,
    input: &LyricsLookup,
    recording: RecordingMetadata<'_>,
) -> Option<LyricsCandidate> {
    let mut candidate = crate::score_candidate(
        lyrics,
        provider,
        source_id,
        Some(url.to_owned()),
        10,
        LyricsRights::default(),
    );
    add_metadata_score(
        &mut candidate,
        input,
        recording.title,
        recording.artist,
        recording.album,
        recording.duration,
    )
    .then_some(candidate)
}

#[cfg(any(feature = "kugou", feature = "netease"))]
pub(super) fn json_array(value: &Value, key: &str) -> Vec<Value> {
    value
        .get(key)
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
}

#[cfg(any(feature = "kugou", feature = "netease"))]
pub(super) fn value_as_string(value: &Value, key: &str) -> Option<String> {
    value.get(key).and_then(|item| {
        item.as_str()
            .map(str::to_owned)
            .or_else(|| item.as_i64().map(|number| number.to_string()))
    })
}

#[cfg(any(
    feature = "lrclib",
    feature = "betterlyrics",
    feature = "paxsenix",
    feature = "unison"
))]
pub(super) fn field(value: &serde_json::Value, name: &str) -> Option<String> {
    value
        .get(name)
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
        .filter(|text| !text.trim().is_empty())
}

#[cfg(any(feature = "lrclib", feature = "betterlyrics", feature = "paxsenix"))]
pub(super) fn push_candidate(
    candidates: &mut Vec<LyricsCandidate>,
    value: &serde_json::Value,
    field_name: &str,
    provider: &str,
    source_id: &str,
    source_url: &str,
    weight: i64,
) {
    if let Some(text) = field(value, field_name) {
        candidates.push(crate::score_candidate(
            &text,
            provider,
            source_id,
            Some(source_url.to_owned()),
            weight,
            LyricsRights::default(),
        ));
    }
}

#[cfg(any(
    feature = "amll-ttml-db",
    feature = "betterlyrics",
    feature = "binimum",
    feature = "kugou",
    feature = "paxsenix"
))]
pub(super) fn encode_uri_component(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z'
            | b'a'..=b'z'
            | b'0'..=b'9'
            | b'-'
            | b'_'
            | b'.'
            | b'!'
            | b'~'
            | b'*'
            | b'\''
            | b'('
            | b')' => encoded.push(byte as char),
            _ => encoded.push_str(&format!("%{byte:02X}")),
        }
    }
    encoded
}

#[cfg(any(
    feature = "amll-ttml-db",
    feature = "binimum",
    feature = "kugou",
    feature = "netease"
))]
pub(super) fn attach_provider_timing(
    candidate: &mut LyricsCandidate,
    raw: &str,
    kind: LyricsRawTypes,
) {
    if let Some(lines) = crate::parse_provider_timed_lines(raw, kind) {
        candidate.document.word_timing = Some(lines);
    }
}

// Each import below is gated on the union of the items that use it, so a
// single-provider build neither misses a name nor compiles one it cannot
// use (the workspace denies both `unused_imports` and `dead_code`).
#[cfg(any(
    feature = "amll-ttml-db",
    feature = "binimum",
    feature = "kugou",
    feature = "musixmatch",
    feature = "netease",
    feature = "qq"
))]
use lyrics_helper::LyricsRawTypes;
#[cfg(any(
    feature = "kugou",
    feature = "musixmatch",
    feature = "netease",
    feature = "qq"
))]
use lyrics_helper::parse as parse_provider_lyrics;

#[cfg(any(
    feature = "amll-ttml-db",
    feature = "betterlyrics",
    feature = "binimum",
    feature = "kugou",
    feature = "lrclib",
    feature = "netease",
    feature = "paxsenix",
    feature = "qq",
    feature = "youtube"
))]
use crate::LyricsCandidate;
#[cfg(any(
    feature = "amll-ttml-db",
    feature = "binimum",
    feature = "kugou",
    feature = "musixmatch",
    feature = "netease",
    feature = "qq",
    feature = "youtube"
))]
use crate::LyricsLookup;
#[cfg(any(
    feature = "amll-ttml-db",
    feature = "betterlyrics",
    feature = "binimum",
    feature = "kugou",
    feature = "lrclib",
    feature = "netease",
    feature = "paxsenix",
    feature = "qq",
    feature = "youtube"
))]
use crate::LyricsRights;
