//! Kugou KRC adapter.

#[cfg(feature = "kugou")]
use lyrics_helper::LyricsRawTypes;
#[cfg(feature = "kugou")]
use serde_json::Value;

#[cfg(feature = "kugou")]
use super::shared::{
    RecordingMetadata, attach_provider_timing, encode_uri_component, json, json_array,
    lrc_candidate, metadata_score, number, parsed_lyrics_to_text, quote, value_as_string,
    value_text,
};
#[cfg(feature = "kugou")]
use crate::LyricsTier;
#[cfg(feature = "kugou")]
use crate::{LyricsCandidate, LyricsFuture, LyricsHttp, LyricsLookup, LyricsSource};

#[cfg(feature = "kugou")]
#[derive(Debug, Default)]
pub struct Kugou;

#[cfg(feature = "kugou")]
impl LyricsSource for Kugou {
    fn id(&self) -> &str {
        "kugou"
    }

    fn lookup<'a>(
        &'a self,
        http: &'a dyn LyricsHttp,
        input: &'a LyricsLookup,
    ) -> LyricsFuture<'a, Vec<LyricsCandidate>> {
        Box::pin(async move {
            let keyword =
                encode_uri_component(&format!("{} {}", input.title, input.artist_string()));
            let search_url = format!(
                "https://mobiles.kugou.com/api/v3/search/song?format=json&keyword={keyword}&page=1&pagesize=20&showtype=1"
            );
            let Some(search_body) = http.get_json(&search_url).await else {
                return Vec::new();
            };
            let Some(data) = json(&search_body).and_then(|answer| answer.get("data").cloned())
            else {
                return Vec::new();
            };
            let mut songs: Vec<Value> = json_array(&data, "info")
                .into_iter()
                .filter(|song| {
                    let title = value_text(song, "songname").unwrap_or_default();
                    let artist = value_text(song, "singername").unwrap_or_default();
                    metadata_score(input, &title, &artist, None, number(song, "duration")).is_some()
                })
                .collect();
            songs.sort_by_key(|song| {
                let title = value_text(song, "songname").unwrap_or_default();
                let artist = value_text(song, "singername").unwrap_or_default();
                let score = metadata_score(input, &title, &artist, None, number(song, "duration"))
                    .unwrap_or_default();
                std::cmp::Reverse(score)
            });
            songs.truncate(4);

            for song in songs {
                let Some(hash) = value_text(&song, "hash") else {
                    continue;
                };
                let duration = number(&song, "duration").unwrap_or_default();
                let index_url = format!(
                    "https://lyrics.kugou.com/search?ver=1&man=yes&client=mobi&hash={}&duration={}",
                    quote(&hash),
                    duration.saturating_mul(1000)
                );
                let Some(index_body) = http.get_json(&index_url).await else {
                    continue;
                };
                let Some(answer) = json(&index_body) else {
                    continue;
                };
                let mut candidates = json_array(&answer, "candidates");
                candidates.sort_by_key(|candidate| {
                    candidate.get("krctype").and_then(Value::as_u64) != Some(2)
                });
                candidates.truncate(3);
                for lyric_candidate in candidates {
                    let Some(id) = value_as_string(&lyric_candidate, "id") else {
                        continue;
                    };
                    let Some(access_key) = value_text(&lyric_candidate, "accesskey") else {
                        continue;
                    };
                    let sheet_url = format!(
                        "https://lyrics.kugou.com/download?ver=1&client=pc&id={}&accesskey={}&fmt=krc&charset=utf8",
                        quote(&id),
                        quote(&access_key)
                    );
                    let Some(sheet_body) = http.get_json(&sheet_url).await else {
                        continue;
                    };
                    let Some(encoded) =
                        json(&sheet_body).and_then(|answer| value_text(&answer, "content"))
                    else {
                        continue;
                    };
                    let Some(krc) = lyrics_helper::decrypt_krc(&encoded) else {
                        continue;
                    };
                    let Some(text) = parsed_lyrics_to_text(&krc, LyricsRawTypes::Krc) else {
                        continue;
                    };
                    let title = value_text(&song, "songname").unwrap_or_default();
                    let artist = value_text(&song, "singername").unwrap_or_default();
                    let album = value_text(&song, "album_name");
                    if let Some(mut candidate) = lrc_candidate(
                        &text,
                        "Kugou KRC",
                        &format!("kugou:{id}"),
                        &sheet_url,
                        input,
                        RecordingMetadata {
                            title: &title,
                            artist: &artist,
                            album: album.as_deref(),
                            duration: Some(duration),
                        },
                    ) {
                        attach_provider_timing(&mut candidate, &krc, LyricsRawTypes::Krc);
                        if candidate.tier() == LyricsTier::WordSynced {
                            return vec![candidate];
                        }
                    }
                }
            }
            Vec::new()
        })
    }
}
