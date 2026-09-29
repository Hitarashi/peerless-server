//! QQ Music QRC adapter.

#[cfg(feature = "qq")]
use lyrics_helper::LyricsRawTypes;
#[cfg(feature = "qq")]
use serde_json::Value;

#[cfg(feature = "qq")]
use super::shared::{
    RecordingMetadata, json, lrc_candidate, metadata_score, number, parsed_lyrics_to_text, quote,
    value_text,
};
#[cfg(feature = "qq")]
use crate::parse_provider_timed_lines;
#[cfg(feature = "qq")]
use crate::{LyricsCandidate, LyricsFuture, LyricsHttp, LyricsLookup, LyricsSource};

#[cfg(feature = "qq")]
#[derive(Debug, Default)]
pub struct QqMusic;

#[cfg(feature = "qq")]
impl LyricsSource for QqMusic {
    fn id(&self) -> &str {
        "qq-music"
    }

    fn lookup<'a>(
        &'a self,
        http: &'a dyn LyricsHttp,
        input: &'a LyricsLookup,
    ) -> LyricsFuture<'a, Vec<LyricsCandidate>> {
        Box::pin(async move {
            let keyword = quote(&format!("{} {}", input.title, input.artist_string()));
            let search_url = format!(
                "https://c.y.qq.com/soso/fcgi-bin/client_search_cp?format=json&p=1&n=10&w={keyword}"
            );
            let headers = [("Referer", "https://y.qq.com/")];
            let Some(body) = http.get_with_headers(&search_url, &headers).await else {
                return Vec::new();
            };
            let Some(mut songs) = json(&body).and_then(|answer| {
                answer
                    .pointer("/data/song/list")
                    .and_then(Value::as_array)
                    .cloned()
            }) else {
                return Vec::new();
            };
            songs.retain(|song| {
                let title = value_text(song, "songname").unwrap_or_default();
                let artist = song
                    .get("singer")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .filter_map(|singer| value_text(singer, "name"))
                    .collect::<Vec<_>>()
                    .join(", ");
                let duration = number(song, "interval");
                metadata_score(input, &title, &artist, None, duration).is_some()
            });
            songs.sort_by_key(|song| {
                let title = value_text(song, "songname").unwrap_or_default();
                let artist = song
                    .get("singer")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .filter_map(|singer| value_text(singer, "name"))
                    .collect::<Vec<_>>()
                    .join(", ");
                std::cmp::Reverse(
                    metadata_score(input, &title, &artist, None, number(song, "interval"))
                        .unwrap_or_default(),
                )
            });
            songs.truncate(3);

            for song in songs {
                let Some(mid) = value_text(&song, "songmid") else {
                    continue;
                };
                let lyric_url = format!(
                    "https://c.y.qq.com/lyric/fcgi-bin/fcg_query_lyric_new.fcg?songmid={}&format=json&nobase64=0",
                    quote(&mid)
                );
                let Some(sheet) = http.get_with_headers(&lyric_url, &headers).await else {
                    continue;
                };
                let Some(sheet) = json(&sheet) else {
                    continue;
                };
                let raw = value_text(&sheet, "qrc")
                    .or_else(|| value_text(&sheet, "lyric"))
                    .and_then(decode_qq_lyric);
                let Some(raw) = raw else {
                    continue;
                };
                let raw = extract_qq_qrc(&raw).unwrap_or(raw);
                let text = lyrics_helper::decrypt_qrc(&raw)
                    .or_else(|| parsed_lyrics_to_text(&raw, LyricsRawTypes::Qrc))
                    .or_else(|| parsed_lyrics_to_text(&raw, LyricsRawTypes::Lrc));
                let Some(text) = text else {
                    continue;
                };
                let title = value_text(&song, "songname").unwrap_or_default();
                let artist = song
                    .get("singer")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .filter_map(|singer| value_text(singer, "name"))
                    .collect::<Vec<_>>()
                    .join(", ");
                let album = value_text(&song, "albumname");
                if let Some(mut candidate) = lrc_candidate(
                    &text,
                    "QQ Music (QRC)",
                    &format!("qq:{mid}"),
                    &lyric_url,
                    input,
                    RecordingMetadata {
                        title: &title,
                        artist: &artist,
                        album: album.as_deref(),
                        duration: number(&song, "interval"),
                    },
                ) {
                    if let Some(timing) = parse_provider_timed_lines(&raw, LyricsRawTypes::Qrc)
                        .or_else(|| parse_provider_timed_lines(&raw, LyricsRawTypes::Lrc))
                    {
                        candidate.document.word_timing = Some(timing);
                    }
                    return vec![candidate];
                }
            }
            Vec::new()
        })
    }
}

#[cfg(feature = "qq")]
fn decode_qq_lyric(raw: String) -> Option<String> {
    use base64::{Engine as _, engine::general_purpose::STANDARD};

    Some(
        STANDARD
            .decode(&raw)
            .ok()
            .and_then(|decoded| String::from_utf8(decoded).ok())
            .unwrap_or(raw),
    )
}

#[cfg(feature = "qq")]
fn extract_qq_qrc(raw: &str) -> Option<String> {
    let document = roxmltree::Document::parse(raw).ok()?;
    document
        .descendants()
        .find_map(|node| node.attribute("LyricContent").map(str::to_owned))
}
