//! Unison (Baidu) adapter.

#[cfg(feature = "unison")]
use lyrics_helper::LyricsRawTypes;

#[cfg(feature = "unison")]
use super::shared::{field, form_encode};
#[cfg(feature = "unison")]
use crate::parse_provider_timed_lines;
#[cfg(feature = "unison")]
use crate::{
    LyricsCandidate, LyricsFuture, LyricsHttp, LyricsLookup, LyricsRights, LyricsSource,
    convert_ttml_to_elrc, score_candidate,
};

#[cfg(feature = "unison")]
#[derive(Debug, Default)]
pub struct Unison;

#[cfg(feature = "unison")]
async fn unison_candidate(http: &dyn LyricsHttp, url: &str) -> Vec<LyricsCandidate> {
    let Some(body) = http.get_json(url).await else {
        return Vec::new();
    };
    let Ok(response) = serde_json::from_str::<serde_json::Value>(&body) else {
        return Vec::new();
    };
    if response.get("success").and_then(serde_json::Value::as_bool) == Some(false) {
        return Vec::new();
    }
    let data = response.get("data").unwrap_or(&response);
    let Some(text) = field(data, "lyrics") else {
        return Vec::new();
    };
    let source_id = data
        .get("id")
        .and_then(|id| {
            id.as_str()
                .map(str::to_owned)
                .or_else(|| id.as_i64().map(|id| id.to_string()))
        })
        .unwrap_or_else(|| "unison".to_owned());
    let rights = LyricsRights {
        attribution: Some("Lyrics from Unison (https://unison.boidu.dev)".to_owned()),
        ..LyricsRights::default()
    };
    let format = field(data, "format").unwrap_or_default();
    let is_ttml = format.eq_ignore_ascii_case("ttml")
        || text.trim_start().starts_with("<tt")
        || text.trim_start().starts_with("<?xml");
    let mut candidates = Vec::new();
    if is_ttml {
        if let Some(converted) = convert_ttml_to_elrc(&text) {
            let mut candidate = score_candidate(
                &converted,
                "Unison (TTML)",
                &source_id,
                Some(url.to_owned()),
                22,
                rights,
            );
            #[cfg(feature = "format-parser")]
            if let Some(timing) = parse_provider_timed_lines(&text, LyricsRawTypes::Ttml) {
                candidate.document.word_timing = Some(timing);
            }
            candidates.push(candidate);
        }
    } else {
        candidates.push(score_candidate(
            &text,
            "Unison",
            &source_id,
            Some(url.to_owned()),
            22,
            rights,
        ));
    }
    candidates
}

#[cfg(feature = "unison")]
impl LyricsSource for Unison {
    fn id(&self) -> &str {
        "unison"
    }

    fn lookup<'a>(
        &'a self,
        http: &'a dyn LyricsHttp,
        input: &'a LyricsLookup,
    ) -> LyricsFuture<'a, Vec<LyricsCandidate>> {
        Box::pin(async move {
            if input.title.trim().is_empty() || input.artist_string().trim().is_empty() {
                return Vec::new();
            }
            let mut url = format!(
                "https://unison.boidu.dev/lyrics?song={}&artist={}",
                form_encode(input.title.trim()),
                form_encode(input.artist_string().trim())
            );
            if let Some(album) = input
                .album
                .as_deref()
                .filter(|album| !album.trim().is_empty())
            {
                url.push_str("&album=");
                url.push_str(&form_encode(album.trim()));
            }
            if let Some(duration) = input.duration.filter(|duration| *duration > 0) {
                url.push_str(&format!("&duration={duration}"));
            }
            let candidates = unison_candidate(http, &url).await;
            if !candidates.is_empty() {
                return candidates;
            }
            let Some(video_id) = input.provider_ids.get("youtube") else {
                return Vec::new();
            };
            let video_url = format!(
                "https://unison.boidu.dev/lyrics?v={}",
                form_encode(video_id)
            );
            unison_candidate(http, &video_url).await
        })
    }
}
