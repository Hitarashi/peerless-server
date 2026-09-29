//! Paxsenix Apple Music ELRC/LRC/TTML adapter.

#[cfg(feature = "paxsenix")]
use lyrics_helper::LyricsRawTypes;

#[cfg(feature = "paxsenix")]
use super::shared::{encode_uri_component, field, push_candidate};
#[cfg(feature = "paxsenix")]
use crate::parse_provider_timed_lines;
#[cfg(feature = "paxsenix")]
use crate::{
    LyricsCandidate, LyricsFuture, LyricsHttp, LyricsLookup, LyricsRights, LyricsSource,
    convert_ttml_to_elrc, score_candidate,
};

#[cfg(feature = "paxsenix")]
#[derive(Debug, Default)]
pub struct Paxsenix;

#[cfg(feature = "paxsenix")]
impl LyricsSource for Paxsenix {
    fn id(&self) -> &str {
        "paxsenix"
    }

    fn lookup<'a>(
        &'a self,
        http: &'a dyn LyricsHttp,
        input: &'a LyricsLookup,
    ) -> LyricsFuture<'a, Vec<LyricsCandidate>> {
        Box::pin(async move {
            let Some(track_id) = input.provider_ids.get("apple") else {
                return Vec::new();
            };
            let url = format!(
                "https://lyrics.paxsenix.org/apple-music/lyrics?id={}",
                encode_uri_component(track_id)
            );
            let Some(body) = http.get_json(&url).await else {
                return Vec::new();
            };
            let Ok(data) = serde_json::from_str::<serde_json::Value>(&body) else {
                return Vec::new();
            };
            let mut candidates = Vec::new();
            push_candidate(
                &mut candidates,
                &data,
                "elrc",
                "Paxsenix Apple Music (ELRC)",
                "paxsenix",
                &url,
                30,
            );
            if let Some(ttml) = field(&data, "ttmlContent")
                && let Some(converted) = convert_ttml_to_elrc(&ttml)
            {
                let mut candidate = score_candidate(
                    &converted,
                    "Paxsenix Apple Music (TTML-ELRC)",
                    "paxsenix",
                    Some(url.clone()),
                    30,
                    LyricsRights::default(),
                );
                #[cfg(feature = "format-parser")]
                if let Some(timing) = parse_provider_timed_lines(&ttml, LyricsRawTypes::Ttml) {
                    candidate.document.word_timing = Some(timing);
                }
                candidates.push(candidate);
            }
            push_candidate(
                &mut candidates,
                &data,
                "lrc",
                "Paxsenix Apple Music (LRC)",
                "paxsenix",
                &url,
                25,
            );
            push_candidate(
                &mut candidates,
                &data,
                "plain",
                "Paxsenix Apple Music (Plain)",
                "paxsenix",
                &url,
                20,
            );
            candidates
        })
    }
}
