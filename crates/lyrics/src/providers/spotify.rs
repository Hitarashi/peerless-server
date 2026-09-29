//! Spotify Color Lyrics adapter.

#[cfg(feature = "spotify")]
use serde_json::Value;

#[cfg(feature = "spotify")]
use super::shared::{json, quote, value_text};
#[cfg(feature = "spotify")]
use crate::{
    LyricsCandidate, LyricsFuture, LyricsHttp, LyricsLookup, LyricsRights, LyricsSource,
    LyricsTimedLine, LyricsTimedWord, format_millis, score_candidate,
};

#[cfg(feature = "spotify")]
#[derive(Debug, Default)]
pub struct Spotify {
    access_token: Option<String>,
    client_token: Option<String>,
}

#[cfg(feature = "spotify")]
impl Spotify {
    pub fn from_env() -> Self {
        Self {
            access_token: std::env::var("SPOTIFY_ACCESS_TOKEN").ok(),
            client_token: std::env::var("SPOTIFY_CLIENT_TOKEN").ok(),
        }
    }
}

#[cfg(feature = "spotify")]
impl LyricsSource for Spotify {
    fn id(&self) -> &str {
        "spotify"
    }

    fn lookup<'a>(
        &'a self,
        http: &'a dyn LyricsHttp,
        input: &'a LyricsLookup,
    ) -> LyricsFuture<'a, Vec<LyricsCandidate>> {
        Box::pin(async move {
            let (Some(access_token), Some(client_token), Some(track_id)) = (
                self.access_token.as_deref(),
                self.client_token.as_deref(),
                input.provider_ids.get("spotify"),
            ) else {
                return Vec::new();
            };
            let url = format!(
                "https://spclient.wg.spotify.com/color-lyrics/v2/track/{}?format=json&vocalRemoval=false&market=from_token",
                quote(track_id)
            );
            let auth = format!("Bearer {access_token}");
            let headers = [
                ("Accept", "application/json"),
                ("app-platform", "WebPlayer"),
                ("Authorization", auth.as_str()),
                ("client-token", client_token),
            ];
            let Some(body) = http.get_with_headers(&url, &headers).await else {
                return Vec::new();
            };
            spotify_json_candidate(&body, &url).into_iter().collect()
        })
    }
}

#[cfg(feature = "spotify")]
fn spotify_json_candidate(body: &str, url: &str) -> Option<LyricsCandidate> {
    let data = json(body)?;
    let lyrics = data.get("lyrics")?;
    let verses = lyrics.get("lines")?.as_array()?;
    let mut plain = Vec::new();
    let mut synced = Vec::new();
    let mut timed_lines = Vec::new();
    for verse in verses {
        let words = value_text(verse, "words").unwrap_or_default();
        if words.trim().is_empty() || words == "♪" {
            continue;
        }
        plain.push(words.clone());
        let start = spotify_time_ms(verse, "startTimeMs").unwrap_or_default();
        let end = spotify_time_ms(verse, "endTimeMs").filter(|end| *end > start);
        let syllables = verse
            .get("syllables")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let mut timed_words = syllables
            .iter()
            .filter_map(|syllable| {
                let text = syllable.get("text")?.as_str()?.to_owned();
                if text.is_empty() {
                    return None;
                }
                Some(LyricsTimedWord {
                    text,
                    start_ms: spotify_time_ms(syllable, "startTimeMs")?,
                    end_ms: spotify_time_ms(syllable, "endTimeMs"),
                })
            })
            .collect::<Vec<_>>();
        for index in 0..timed_words.len() {
            let inferred_end = timed_words.get(index + 1).map(|next| next.start_ms).or(end);
            if timed_words[index].end_ms.is_none() {
                timed_words[index].end_ms = inferred_end;
            }
        }
        let timed_end = end.or_else(|| timed_words.last().and_then(|word| word.end_ms));
        timed_lines.push(LyricsTimedLine {
            text: words.clone(),
            start_ms: start,
            end_ms: timed_end,
            words: timed_words.clone(),
            background_words: Vec::new(),
            alignment: None,
            agent: None,
            translations: Vec::new(),
            romanization: None,
            is_instrumental: false,
        });

        let line = if timed_words.is_empty() {
            format!("[{}]{words}", format_millis(start))
        } else {
            let parts = timed_words
                .iter()
                .map(|syllable| format!("<{}>{}", format_millis(syllable.start_ms), syllable.text))
                .collect::<Vec<_>>();
            format!("[{}]{}", format_millis(start), parts.join(""))
        };
        synced.push(line);
    }
    if plain.len() < 2 {
        return None;
    }
    let sync_type = value_text(lyrics, "syncType").unwrap_or_default();
    let text = if sync_type == "UNSYNCED" {
        plain.join("\n")
    } else {
        synced.join("\n")
    };
    let mut candidate = score_candidate(
        &text,
        "Spotify Color Lyrics",
        "spotify",
        Some(url.to_owned()),
        35,
        LyricsRights::default(),
    );
    if sync_type != "UNSYNCED" {
        candidate.document.word_timing = Some(timed_lines);
    }
    Some(candidate)
}

#[cfg(feature = "spotify")]
fn spotify_time_ms(value: &Value, key: &str) -> Option<u64> {
    value.get(key).and_then(|value| {
        value
            .as_str()
            .and_then(|value| value.parse().ok())
            .or_else(|| value.as_u64())
    })
}
