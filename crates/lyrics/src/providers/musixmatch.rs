//! Musixmatch richsync/subtitles adapter.

#[cfg(feature = "musixmatch")]
use lyrics_helper::LyricsRawTypes;
#[cfg(feature = "musixmatch")]
use serde_json::Value;

#[cfg(feature = "musixmatch")]
use super::shared::{json, metadata_score, number, parsed_lyrics_to_text, quote, value_text};
#[cfg(feature = "musixmatch")]
use crate::{
    LyricsCandidate, LyricsFuture, LyricsHttp, LyricsLookup, LyricsRights, LyricsSource,
    LyricsTimedLine, LyricsTimedWord, format_millis, score_candidate,
};

#[cfg(feature = "musixmatch")]
#[derive(Debug, Default)]
pub struct Musixmatch;

#[cfg(feature = "musixmatch")]
impl LyricsSource for Musixmatch {
    fn id(&self) -> &str {
        "musixmatch"
    }

    fn lookup<'a>(
        &'a self,
        http: &'a dyn LyricsHttp,
        input: &'a LyricsLookup,
    ) -> LyricsFuture<'a, Vec<LyricsCandidate>> {
        Box::pin(async move {
            let token_url = "https://apic-desktop.musixmatch.com/ws/1.1/token.get?format=json&app_id=web-desktop-app-v1.0";
            let headers = [
                (
                    "User-Agent",
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
                ),
                ("Cookie", "x-mxm-token-guid="),
            ];
            let Some(token_body) = http.get_with_headers(token_url, &headers).await else {
                return Vec::new();
            };
            let Some(token) = json(&token_body)
                .and_then(|answer| {
                    answer
                        .pointer("/message/body/user_token")
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                })
                .filter(|token| !token.is_empty() && !token.contains("UpgradeOnly"))
            else {
                return Vec::new();
            };

            let mut params = vec![
                "format=json".to_owned(),
                "namespace=lyrics_richsynched".to_owned(),
                "subtitle_format=mxm".to_owned(),
                "optional_calls=track.richsync".to_owned(),
                "app_id=web-desktop-app-v1.0".to_owned(),
                format!("usertoken={}", quote(&token)),
            ];
            if let Some(id) = input.provider_ids.get("spotify") {
                params.push(format!("track_spotify_id={}", quote(id)));
            } else {
                params.push(format!("q_track={}", quote(&input.title)));
                params.push(format!("q_artist={}", quote(&input.artist_string())));
                if let Some(album) = input.album.as_deref().filter(|album| !album.is_empty()) {
                    params.push(format!("q_album={}", quote(album)));
                }
                if let Some(duration) = input.duration.filter(|duration| *duration > 0) {
                    params.push(format!("q_duration={duration}"));
                }
            }
            let url = format!(
                "https://apic-desktop.musixmatch.com/ws/1.1/macro.subtitles.get?{}",
                params.join("&")
            );
            let Some(body) = http.get_with_headers(&url, &headers).await else {
                return Vec::new();
            };
            let Some(data) = json(&body) else {
                return Vec::new();
            };
            let Some(track) =
                data.pointer("/message/body/macro_calls/matcher.track.get/message/body/track")
            else {
                return Vec::new();
            };
            let title = value_text(track, "track_name").unwrap_or_default();
            let artist = value_text(track, "artist_name").unwrap_or_default();
            let album = value_text(track, "album_name");
            let duration = number(track, "track_length");
            let Some(score) = metadata_score(input, &title, &artist, album.as_deref(), duration)
            else {
                return Vec::new();
            };
            let Some(macro_calls) = data.pointer("/message/body/macro_calls") else {
                return Vec::new();
            };
            let rich = macro_calls
                .pointer("/track.richsync.get/message/body/richsync/richsync_body")
                .and_then(Value::as_str)
                .and_then(parse_musixmatch_richsync);
            let lyric_text = rich.or_else(|| parse_musixmatch_subtitles(macro_calls));
            let Some(text) = lyric_text else {
                return Vec::new();
            };
            let mut candidate = score_candidate(
                &text,
                "Musixmatch",
                "musixmatch",
                Some(url),
                20,
                LyricsRights::default(),
            );
            if let Some(timing) = parse_musixmatch_richsync_timing(macro_calls) {
                candidate.document.word_timing = Some(timing);
            }
            candidate.ranking.score += score;
            vec![candidate]
        })
    }
}

#[cfg(feature = "musixmatch")]
fn seconds_as_millis(value: &Value) -> Option<u64> {
    value
        .as_f64()
        .or_else(|| value.as_str()?.parse::<f64>().ok())
        .filter(|seconds| seconds.is_finite() && *seconds >= 0.0)
        .map(|seconds| (seconds * 1000.0).round() as u64)
}

#[cfg(feature = "musixmatch")]
fn parse_musixmatch_richsync(raw: &str) -> Option<String> {
    let verses = serde_json::from_str::<Vec<Value>>(raw).ok()?;
    let mut lines = Vec::new();
    for verse in verses {
        let start = seconds_as_millis(verse.get("ts")?)?;
        let words = verse
            .get("l")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|token| {
                let text = token.get("c")?.as_str()?.to_owned();
                let offset = seconds_as_millis(token.get("o")?)?;
                (!text.is_empty())
                    .then(|| format!("<{}>{text}", format_millis(start.saturating_add(offset))))
            })
            .collect::<Vec<_>>();
        if words.is_empty() {
            let text = value_text(&verse, "x").unwrap_or_default();
            if !text.trim().is_empty() {
                lines.push(format!("[{}]{text}", format_millis(start)));
            }
        } else {
            lines.push(format!("[{}]{}", format_millis(start), words.join("")));
        }
    }
    (!lines.is_empty()).then(|| lines.join("\n"))
}

#[cfg(feature = "musixmatch")]
fn parse_musixmatch_richsync_timing(macro_calls: &Value) -> Option<Vec<LyricsTimedLine>> {
    let raw = macro_calls
        .pointer("/track.richsync.get/message/body/richsync/richsync_body")?
        .as_str()?;
    let verses = serde_json::from_str::<Vec<Value>>(raw).ok()?;
    let lines = verses
        .into_iter()
        .filter_map(|verse| {
            let start_ms = seconds_as_millis(verse.get("ts")?)?;
            let end_ms = seconds_as_millis(verse.get("te")?)?.max(start_ms);
            let tokens = verse
                .get("l")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(|token| {
                    let text = token.get("c")?.as_str()?.to_owned();
                    if text.is_empty() {
                        return None;
                    }
                    let offset = seconds_as_millis(token.get("o")?)?;
                    Some((start_ms.saturating_add(offset), text))
                })
                .collect::<Vec<_>>();
            let mut words: Vec<LyricsTimedWord> = Vec::new();
            for (index, (word_start, text)) in tokens.iter().enumerate() {
                let word_end = tokens
                    .get(index + 1)
                    .map(|(next_start, _)| *next_start)
                    .unwrap_or(end_ms)
                    .max(*word_start);
                if text.chars().all(char::is_whitespace) {
                    if let Some(previous) = words.last_mut() {
                        previous.text.push_str(text);
                        previous.end_ms = Some(previous.end_ms.unwrap_or(word_end).max(word_end));
                    }
                } else {
                    words.push(LyricsTimedWord {
                        text: text.clone(),
                        start_ms: *word_start,
                        end_ms: Some(word_end),
                    });
                }
            }
            (!words.is_empty()).then(|| LyricsTimedLine {
                text: words.iter().map(|word| word.text.as_str()).collect(),
                start_ms,
                end_ms: Some(end_ms),
                words,
                background_words: Vec::new(),
                alignment: None,
                agent: None,
                translations: Vec::new(),
                romanization: None,
                is_instrumental: false,
            })
        })
        .collect::<Vec<_>>();
    (!lines.is_empty()).then_some(lines)
}

#[cfg(feature = "musixmatch")]
fn parse_musixmatch_subtitles(macro_calls: &Value) -> Option<String> {
    let subtitle = macro_calls
        .pointer("/track.subtitles.get/message/body/subtitle_list/0/subtitle/subtitle_body")?
        .as_str()?;
    if let Some(text) = parsed_lyrics_to_text(subtitle, LyricsRawTypes::Lrc) {
        return Some(text);
    }
    let cues = serde_json::from_str::<Vec<Value>>(subtitle).ok()?;
    let lines = cues
        .iter()
        .filter_map(|cue| {
            let start = seconds_as_millis(cue.pointer("/time/total")?)?;
            let text = value_text(cue, "text")?;
            Some(format!("[{}]{text}", format_millis(start)))
        })
        .collect::<Vec<_>>();
    (!lines.is_empty()).then(|| lines.join("\n"))
}
