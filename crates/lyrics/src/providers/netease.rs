//! NetEase YRC/LRC adapter.

#[cfg(feature = "netease")]
use lyrics_helper::LyricsRawTypes;
#[cfg(feature = "netease")]
use serde_json::Value;

#[cfg(feature = "netease")]
use super::shared::{
    RecordingMetadata, attach_provider_timing, json, json_array, lrc_candidate, metadata_score,
    number, parsed_lyrics_to_text, quote, value_as_string, value_text,
};
#[cfg(feature = "netease")]
use crate::{LyricsCandidate, LyricsFuture, LyricsHttp, LyricsLookup, LyricsSource};

#[cfg(feature = "netease")]
fn artists_from_search(song: &Value) -> String {
    song.get("artists")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|artist| value_text(artist, "name"))
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(feature = "netease")]
#[derive(Debug, Default)]
pub struct NetEase;

#[cfg(feature = "netease")]
impl LyricsSource for NetEase {
    fn id(&self) -> &str {
        "netease"
    }

    fn lookup<'a>(
        &'a self,
        http: &'a dyn LyricsHttp,
        input: &'a LyricsLookup,
    ) -> LyricsFuture<'a, Vec<LyricsCandidate>> {
        Box::pin(async move {
            let query = quote(&format!("{} {}", input.title, input.artist_string()));
            let search_url =
                format!("https://music.163.com/api/search/get?s={query}&type=1&limit=8");
            let headers = [("Referer", "https://music.163.com")];
            let Some(body) = http.get_with_headers(&search_url, &headers).await else {
                return Vec::new();
            };
            let Some(mut songs) = json(&body)
                .and_then(|answer| answer.get("result").cloned())
                .map(|result| json_array(&result, "songs"))
            else {
                return Vec::new();
            };
            songs.retain(|song| {
                let title = value_text(song, "name").unwrap_or_default();
                let artist = artists_from_search(song);
                let duration = number(song, "duration").map(|millis| millis / 1000);
                metadata_score(input, &title, &artist, None, duration).is_some()
            });
            songs.sort_by_key(|song| {
                let title = value_text(song, "name").unwrap_or_default();
                let artist = artists_from_search(song);
                let duration = number(song, "duration").map(|millis| millis / 1000);
                std::cmp::Reverse(
                    metadata_score(input, &title, &artist, None, duration).unwrap_or_default(),
                )
            });
            songs.truncate(3);

            for song in songs {
                let Some(id) = value_as_string(&song, "id") else {
                    continue;
                };
                let lyric_url = format!(
                    "https://music.163.com/api/song/lyric/v1?id={id}&cp=false&lv=0&tv=0&rv=0&kv=0&yv=0&ytv=0&yrv=0"
                );
                let Some(sheet) = http.get_with_headers(&lyric_url, &headers).await else {
                    continue;
                };
                let Some(sheet) = json(&sheet) else {
                    continue;
                };
                let title = value_text(&song, "name").unwrap_or_default();
                let artist = artists_from_search(&song);
                let album = song
                    .get("album")
                    .and_then(|album| value_text(album, "name"));
                let duration = number(&song, "duration").map(|millis| millis / 1000);
                let yrc = sheet
                    .get("yrc")
                    .and_then(|value| value_text(value, "lyric"))
                    .and_then(|raw| {
                        parsed_lyrics_to_text(&raw, LyricsRawTypes::Yrc)
                            .map(|text| (text, raw, LyricsRawTypes::Yrc))
                    });
                let lrc = sheet
                    .get("lrc")
                    .and_then(|value| value_text(value, "lyric"))
                    .and_then(|raw| {
                        parsed_lyrics_to_text(&raw, LyricsRawTypes::Lrc)
                            .map(|text| (text, raw, LyricsRawTypes::Lrc))
                    });
                let Some((text, timing_source, timing_format)) = yrc.or(lrc) else {
                    continue;
                };
                if let Some(mut candidate) = lrc_candidate(
                    &text,
                    "NetEase (YRC/LRC)",
                    &format!("netease:{id}"),
                    &lyric_url,
                    input,
                    RecordingMetadata {
                        title: &title,
                        artist: &artist,
                        album: album.as_deref(),
                        duration,
                    },
                ) {
                    attach_provider_timing(&mut candidate, &timing_source, timing_format);
                    return vec![candidate];
                }
            }
            Vec::new()
        })
    }
}
