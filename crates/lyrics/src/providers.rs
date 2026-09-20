//! Network adapters for lyric catalogues used by Sonora and BetterLyrics.
//!
//! Each adapter returns the same normalized LRC/ELRC document so provider
//! details stay behind the `LyricsSource` interface.

#[cfg(feature = "amll-ttml-db")]
use std::{
    collections::BTreeMap,
    sync::{Mutex, OnceLock},
};

#[cfg(any(
    feature = "amll-ttml-db",
    feature = "binimum",
    feature = "kugou",
    feature = "netease",
    feature = "musixmatch",
    feature = "qq",
    feature = "spotify"
))]
use lyrics_helper::{LyricsRawTypes, parse as parse_provider_lyrics};
use serde_json::Value;

#[cfg(feature = "kugou")]
use crate::LyricsTier;
#[cfg(any(
    feature = "amll-ttml-db",
    feature = "binimum",
    feature = "kugou",
    feature = "netease",
    feature = "qq",
    feature = "youtube"
))]
use crate::score_candidate;
use crate::{LyricsCandidate, LyricsFuture, LyricsHttp, LyricsLookup, LyricsRights, LyricsSource};

fn json(body: &str) -> Option<Value> {
    serde_json::from_str(body).ok()
}

fn value_text(value: &Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .map(str::to_owned)
}

fn number(value: &Value, key: &str) -> Option<u64> {
    value.get(key).and_then(Value::as_u64)
}

fn artist_parts(value: &str) -> Vec<String> {
    value
        .split([',', '/', '&', '、', ';'])
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(str::to_owned)
        .collect()
}

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

fn title_key(value: &str) -> String {
    value
        .chars()
        .filter(|character| character.is_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect()
}

fn title_matches(expected: &str, actual: &str) -> bool {
    let expected = title_key(expected);
    let actual = title_key(actual);
    !expected.is_empty()
        && !actual.is_empty()
        && (expected == actual || actual.contains(&expected) || expected.contains(&actual))
}

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

fn metadata_score(
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

fn quote(value: &str) -> String {
    super::form_encode(value)
}

#[cfg(any(
    feature = "kugou",
    feature = "netease",
    feature = "musixmatch",
    feature = "qq",
    feature = "spotify"
))]
fn parsed_lyrics_to_text(raw: &str, kind: LyricsRawTypes) -> Option<String> {
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
                        super::format_millis(syllable.start_time() as u64)
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
                    super::format_millis(start as u64),
                    words.join("")
                ))
            } else {
                Some(format!("[{}]{text}", super::format_millis(start as u64)))
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
struct RecordingMetadata<'a> {
    title: &'a str,
    artist: &'a str,
    album: Option<&'a str>,
    duration: Option<u64>,
}

#[cfg(any(
    feature = "amll-ttml-db",
    feature = "binimum",
    feature = "kugou",
    feature = "netease",
    feature = "qq",
    feature = "youtube"
))]
fn lrc_candidate(
    lyrics: &str,
    provider: &str,
    source_id: &str,
    url: &str,
    input: &LyricsLookup,
    recording: RecordingMetadata<'_>,
) -> Option<LyricsCandidate> {
    let mut candidate = score_candidate(
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
fn json_array(value: &Value, key: &str) -> Vec<Value> {
    value
        .get(key)
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
}

#[cfg(any(feature = "kugou", feature = "netease"))]
fn value_as_string(value: &Value, key: &str) -> Option<String> {
    value.get(key).and_then(|item| {
        item.as_str()
            .map(str::to_owned)
            .or_else(|| item.as_i64().map(|number| number.to_string()))
    })
}

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
                super::encode_uri_component(&format!("{} {}", input.title, input.artist_string()));
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
                super::encode_uri_component(&input.title),
                super::encode_uri_component(&input.artist_string())
            );
            if let Some(album) = input
                .album
                .as_deref()
                .filter(|album| !album.trim().is_empty())
            {
                url.push_str("&album=");
                url.push_str(&super::encode_uri_component(album));
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
                let candidate = super::convert_ttml_to_elrc(&ttml)
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
                        super::convert_ttml_to_lrc(&ttml).and_then(|text| {
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

#[cfg(feature = "amll-ttml-db")]
#[derive(Clone, Debug)]
struct AmllEntry {
    title: String,
    artists: Vec<String>,
    album: Option<String>,
    ncm_id: Option<String>,
    file: String,
}

#[cfg(feature = "amll-ttml-db")]
static AMLL_INDEX: OnceLock<Mutex<Option<Vec<AmllEntry>>>> = OnceLock::new();

#[cfg(feature = "amll-ttml-db")]
fn amll_index() -> &'static Mutex<Option<Vec<AmllEntry>>> {
    AMLL_INDEX.get_or_init(|| Mutex::new(None))
}

#[cfg(feature = "amll-ttml-db")]
fn parse_amll_index(text: &str) -> Vec<AmllEntry> {
    text.lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter_map(|item| {
            let file = value_text(&item, "rawLyricFile")?;
            let metadata: BTreeMap<String, Vec<String>> = item
                .get("metadata")?
                .as_array()?
                .iter()
                .filter_map(|pair| {
                    let pair = pair.as_array()?;
                    let key = pair.first()?.as_str()?.to_owned();
                    let values = pair
                        .get(1)?
                        .as_array()?
                        .iter()
                        .filter_map(Value::as_str)
                        .map(str::to_owned)
                        .collect();
                    Some((key, values))
                })
                .collect();
            Some(AmllEntry {
                title: metadata.get("musicName")?.first()?.clone(),
                artists: metadata.get("artists").cloned().unwrap_or_default(),
                album: metadata
                    .get("album")
                    .and_then(|values| values.first())
                    .cloned(),
                ncm_id: metadata
                    .get("ncmMusicId")
                    .and_then(|values| values.first())
                    .cloned(),
                file,
            })
        })
        .collect()
}

#[cfg(feature = "amll-ttml-db")]
async fn load_amll_index(http: &dyn LyricsHttp) -> Option<Vec<AmllEntry>> {
    if let Some(entries) = amll_index().lock().ok()?.clone() {
        return Some(entries);
    }
    let url = "https://raw.githubusercontent.com/amll-dev/amll-ttml-db/refs/heads/main/metadata/raw-lyrics-index.jsonl";
    let body = http.get_json(url).await?;
    let entries = parse_amll_index(&body);
    if let Ok(mut cache) = amll_index().lock() {
        *cache = Some(entries.clone());
    }
    Some(entries)
}

/// Search the AMLL TTML database by metadata and download its raw TTML sheet.
#[cfg(feature = "amll-ttml-db")]
#[derive(Debug, Default)]
pub struct AmllTtmlDb;

#[cfg(feature = "amll-ttml-db")]
impl LyricsSource for AmllTtmlDb {
    fn id(&self) -> &str {
        "amll-ttml-db"
    }

    fn lookup<'a>(
        &'a self,
        http: &'a dyn LyricsHttp,
        input: &'a LyricsLookup,
    ) -> LyricsFuture<'a, Vec<LyricsCandidate>> {
        Box::pin(async move {
            let Some(entries) = load_amll_index(http).await else {
                return Vec::new();
            };
            let best = entries
                .into_iter()
                .filter_map(|entry| {
                    let artist = entry.artists.join(", ");
                    let score =
                        metadata_score(input, &entry.title, &artist, entry.album.as_deref(), None)?;
                    Some((score, entry))
                })
                .max_by_key(|(score, _)| *score)
                .map(|(_, entry)| entry);
            let Some(entry) = best else {
                return Vec::new();
            };
            let url = format!(
                "https://raw.githubusercontent.com/amll-dev/amll-ttml-db/refs/heads/main/raw-lyrics/{}",
                super::encode_uri_component(&entry.file)
            );
            let Some(ttml) = http.get_json(&url).await else {
                return Vec::new();
            };
            let lyrics =
                super::convert_ttml_to_elrc(&ttml).or_else(|| super::convert_ttml_to_lrc(&ttml));
            lyrics
                .and_then(|text| {
                    lrc_candidate(
                        &text,
                        "AMLL TTML Database",
                        entry.ncm_id.as_deref().unwrap_or("amll-ttml-db"),
                        &url,
                        input,
                        RecordingMetadata {
                            title: &entry.title,
                            artist: &entry.artists.join(", "),
                            album: entry.album.as_deref(),
                            duration: None,
                        },
                    )
                })
                .map(|mut candidate| {
                    attach_provider_timing(&mut candidate, &ttml, LyricsRawTypes::Ttml);
                    candidate
                })
                .into_iter()
                .collect()
        })
    }
}

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
            let mut candidate = super::score_candidate(
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
                (!text.is_empty()).then(|| {
                    format!(
                        "<{}>{text}",
                        super::format_millis(start.saturating_add(offset))
                    )
                })
            })
            .collect::<Vec<_>>();
        if words.is_empty() {
            let text = value_text(&verse, "x").unwrap_or_default();
            if !text.trim().is_empty() {
                lines.push(format!("[{}]{text}", super::format_millis(start)));
            }
        } else {
            lines.push(format!(
                "[{}]{}",
                super::format_millis(start),
                words.join("")
            ));
        }
    }
    (!lines.is_empty()).then(|| lines.join("\n"))
}

#[cfg(feature = "musixmatch")]
fn parse_musixmatch_richsync_timing(macro_calls: &Value) -> Option<Vec<crate::LyricsTimedLine>> {
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
            let mut words: Vec<crate::LyricsTimedWord> = Vec::new();
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
                    words.push(crate::LyricsTimedWord {
                        text: text.clone(),
                        start_ms: *word_start,
                        end_ms: Some(word_end),
                    });
                }
            }
            (!words.is_empty()).then(|| crate::LyricsTimedLine {
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

#[cfg(feature = "format-parser")]
fn attach_provider_timing(candidate: &mut LyricsCandidate, raw: &str, kind: LyricsRawTypes) {
    if let Some(lines) = super::parse_provider_timed_lines(raw, kind) {
        candidate.document.word_timing = Some(lines);
    }
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
            Some(format!("[{}]{text}", super::format_millis(start)))
        })
        .collect::<Vec<_>>();
    (!lines.is_empty()).then(|| lines.join("\n"))
}

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
                    if let Some(timing) =
                        super::parse_provider_timed_lines(&raw, LyricsRawTypes::Qrc).or_else(|| {
                            super::parse_provider_timed_lines(&raw, LyricsRawTypes::Lrc)
                        })
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
                Some(crate::LyricsTimedWord {
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
        timed_lines.push(crate::LyricsTimedLine {
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
            format!("[{}]{words}", super::format_millis(start))
        } else {
            let parts = timed_words
                .iter()
                .map(|syllable| {
                    format!(
                        "<{}>{}",
                        super::format_millis(syllable.start_ms),
                        syllable.text
                    )
                })
                .collect::<Vec<_>>();
            format!("[{}]{}", super::format_millis(start), parts.join(""))
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
    let mut candidate = super::score_candidate(
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

#[cfg(feature = "youtube")]
#[derive(Debug, Default)]
pub struct YouTubeMusic;

#[cfg(feature = "youtube")]
impl LyricsSource for YouTubeMusic {
    fn id(&self) -> &str {
        "youtube-music"
    }

    fn lookup<'a>(
        &'a self,
        _http: &'a dyn LyricsHttp,
        input: &'a LyricsLookup,
    ) -> LyricsFuture<'a, Vec<LyricsCandidate>> {
        Box::pin(async move {
            use ytmusic::{Client, YtMusic, nav::Nav as _, parse::find_renderers};

            let client = YtMusic::anonymous();
            let query = format!("{} {}", input.title, input.artist_string());
            let mut tracks = match client.search_songs(&query).await {
                Ok(tracks) => tracks,
                Err(_) => return Vec::new(),
            };
            if let Some(id) = input.provider_ids.get("youtube") {
                tracks.sort_by_key(|track| track.video_id.as_deref() != Some(id));
            }
            tracks.retain(|track| {
                let artist = track
                    .artists
                    .iter()
                    .map(|artist| artist.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ");
                let album = track.album.as_ref().map(|album| album.name.as_str());
                let duration = track.duration.map(|duration| duration.as_secs());
                metadata_score(input, &track.title, &artist, album, duration).is_some()
                    || input
                        .provider_ids
                        .get("youtube")
                        .is_some_and(|id| track.video_id.as_deref() == Some(id.as_str()))
            });
            tracks.truncate(3);
            for track in tracks {
                let Some(id) = track.video_id.as_deref() else {
                    continue;
                };
                let next = match client
                    .execute(
                        "next",
                        Client::Music,
                        serde_json::json!({
                            "videoId": id,
                            "playlistId": format!("RDAMVM{id}"),
                            "enablePersistentPlaylistPanel": true,
                        }),
                    )
                    .await
                {
                    Ok(next) => next,
                    Err(_) => continue,
                };
                let browse = find_renderers(&next, "browseEndpoint")
                    .into_iter()
                    .find_map(|endpoint| {
                        let kind = endpoint.str_at(&[
                            "browseEndpointContextSupportedConfigs",
                            "browseEndpointContextMusicConfig",
                            "pageType",
                        ]);
                        let browse_id = endpoint.str_at(&["browseId"])?;
                        (kind == Some("MUSIC_PAGE_TYPE_TRACK_LYRICS")
                            || browse_id.starts_with("MPLYt"))
                        .then(|| browse_id.to_owned())
                    });
                let Some(browse) = browse else {
                    continue;
                };
                let Ok(response) = client
                    .execute(
                        "browse",
                        Client::Music,
                        serde_json::json!({"browseId": browse}),
                    )
                    .await
                else {
                    continue;
                };
                let Some(text) = find_renderers(&response, "musicDescriptionShelfRenderer")
                    .into_iter()
                    .filter_map(|shelf| shelf.run_text(&["description"]))
                    .find(|text| !text.trim().is_empty())
                else {
                    continue;
                };
                let artist = track
                    .artists
                    .iter()
                    .map(|artist| artist.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ");
                let album = track.album.as_ref().map(|album| album.name.as_str());
                let duration = track.duration.map(|duration| duration.as_secs());
                let Some(mut candidate) = lrc_candidate(
                    &text,
                    "YouTube Music Description",
                    &format!("youtube:{id}"),
                    &format!("https://music.youtube.com/watch?v={id}"),
                    input,
                    RecordingMetadata {
                        title: &track.title,
                        artist: &artist,
                        album,
                        duration,
                    },
                ) else {
                    continue;
                };
                candidate.ranking.score += 5;
                return vec![candidate];
            }
            Vec::new()
        })
    }
}
