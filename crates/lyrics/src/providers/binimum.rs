//! Apple Music lyric catalogue adapter backed by Binimum.

#[cfg(feature = "binimum")]
use lyrics_helper::LyricsRawTypes;
#[cfg(feature = "binimum")]
use serde_json::Value;

#[cfg(feature = "binimum")]
use super::shared::{
    RecordingMetadata, attach_provider_timing, encode_uri_component, json, lrc_candidate,
    metadata_score, number, value_text,
};
#[cfg(feature = "binimum")]
use crate::{
    LyricsCandidate, LyricsFuture, LyricsHttp, LyricsLookup, LyricsSource, convert_ttml_to_elrc,
    convert_ttml_to_lrc,
};

/// Public Apple Music lyric catalogue used by Sonora's Binimum adapter.
#[cfg(feature = "binimum")]
#[derive(Debug, Default)]
pub struct Binimum;

#[cfg(feature = "binimum")]
impl LyricsSource for Binimum {
    fn id(&self) -> &str {
        "binimum"
    }

    fn lookup<'a>(
        &'a self,
        http: &'a dyn LyricsHttp,
        input: &'a LyricsLookup,
    ) -> LyricsFuture<'a, Vec<LyricsCandidate>> {
        Box::pin(async move {
            let mut url = format!(
                "https://lyrics-api.binimum.org/?track={}&artist={}",
                encode_uri_component(&input.title),
                encode_uri_component(&input.artist_string())
            );
            if let Some(album) = input
                .album
                .as_deref()
                .filter(|album| !album.trim().is_empty())
            {
                url.push_str("&album=");
                url.push_str(&encode_uri_component(album));
            }
            if let Some(duration) = input.duration.filter(|duration| *duration > 0) {
                url.push_str(&format!("&duration={duration}"));
            }
            let Some(body) = http.get_json(&url).await else {
                return Vec::new();
            };
            let Some(results) = json(&body)
                .and_then(|data| data.get("results").cloned())
                .and_then(|results| results.as_array().cloned())
            else {
                return Vec::new();
            };

            let mut matches: Vec<Value> = results
                .into_iter()
                .filter(|item| {
                    let title = value_text(item, "track_name").unwrap_or_default();
                    let artist = value_text(item, "artist_name").unwrap_or_default();
                    let duration = number(item, "duration");
                    metadata_score(input, &title, &artist, None, duration).is_some()
                })
                .collect();
            matches.sort_by_key(|item| {
                let kind = value_text(item, "timing_type").unwrap_or_default();
                !(kind.eq_ignore_ascii_case("word") || kind.eq_ignore_ascii_case("syllable"))
            });

            for item in matches {
                let Some(sheet_url) = value_text(&item, "lyricsUrl") else {
                    continue;
                };
                if !sheet_url.starts_with("https://") {
                    continue;
                }
                let Some(ttml) = http.get_json(&sheet_url).await else {
                    continue;
                };
                let title = value_text(&item, "track_name").unwrap_or_default();
                let artist = value_text(&item, "artist_name").unwrap_or_default();
                let album = value_text(&item, "album_name");
                let duration = number(&item, "duration");
                let candidate = convert_ttml_to_elrc(&ttml)
                    .and_then(|text| {
                        lrc_candidate(
                            &text,
                            "Apple Music (Binimum TTML)",
                            "binimum",
                            &sheet_url,
                            input,
                            RecordingMetadata {
                                title: &title,
                                artist: &artist,
                                album: album.as_deref(),
                                duration,
                            },
                        )
                    })
                    .or_else(|| {
                        convert_ttml_to_lrc(&ttml).and_then(|text| {
                            lrc_candidate(
                                &text,
                                "Apple Music (Binimum TTML)",
                                "binimum",
                                &sheet_url,
                                input,
                                RecordingMetadata {
                                    title: &title,
                                    artist: &artist,
                                    album: album.as_deref(),
                                    duration,
                                },
                            )
                        })
                    });
                if let Some(mut candidate) = candidate {
                    attach_provider_timing(&mut candidate, &ttml, LyricsRawTypes::Ttml);
                    return vec![candidate];
                }
            }
            Vec::new()
        })
    }
}
