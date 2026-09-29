//! BetterLyrics (Baidu) adapter.

#[cfg(feature = "betterlyrics")]
use lyrics_helper::LyricsRawTypes;

#[cfg(feature = "betterlyrics")]
use super::shared::{encode_uri_component, field, push_candidate};
#[cfg(feature = "betterlyrics")]
use crate::parse_provider_timed_lines;
#[cfg(feature = "betterlyrics")]
use crate::{
    LyricsCandidate, LyricsFuture, LyricsHttp, LyricsLookup, LyricsRights, LyricsSource,
    convert_ttml_to_elrc, score_candidate,
};

#[cfg(feature = "betterlyrics")]
#[derive(Debug, Default)]
pub struct BetterLyrics;

#[cfg(feature = "betterlyrics")]
impl LyricsSource for BetterLyrics {
    fn id(&self) -> &str {
        "betterlyrics"
    }

    fn lookup<'a>(
        &'a self,
        http: &'a dyn LyricsHttp,
        input: &'a LyricsLookup,
    ) -> LyricsFuture<'a, Vec<LyricsCandidate>> {
        Box::pin(async move {
            let primary = betterlyrics_endpoint(http, input, "getLyrics").await;
            if primary.is_empty() {
                betterlyrics_endpoint(http, input, "kugou/getLyrics").await
            } else {
                primary
            }
        })
    }
}

#[cfg(feature = "betterlyrics")]
async fn betterlyrics_endpoint(
    http: &dyn LyricsHttp,
    input: &LyricsLookup,
    endpoint: &str,
) -> Vec<LyricsCandidate> {
    let mut url = format!(
        "https://lyrics-api.boidu.dev/{endpoint}?s={}&a={}",
        encode_uri_component(&input.title),
        encode_uri_component(&input.artist_string())
    );
    if let Some(album) = input
        .album
        .as_deref()
        .filter(|album| !album.trim().is_empty())
    {
        url.push_str("&al=");
        url.push_str(&encode_uri_component(album));
    }
    if let Some(duration) = input.duration.filter(|duration| *duration > 0) {
        url.push_str(&format!("&d={duration}"));
    }
    let Some(body) = http.get_json(&url).await else {
        return Vec::new();
    };
    let Ok(data) = serde_json::from_str::<serde_json::Value>(&body) else {
        return Vec::new();
    };
    let mut candidates = Vec::new();
    if let Some(ttml) = field(&data, "ttml")
        && let Some(converted) = convert_ttml_to_elrc(&ttml)
    {
        let mut candidate = score_candidate(
            &converted,
            "BetterLyrics (Word Synced)",
            "betterlyrics",
            Some(url.clone()),
            25,
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
        "BetterLyrics (LRC)",
        "betterlyrics",
        &url,
        20,
    );
    candidates
}
