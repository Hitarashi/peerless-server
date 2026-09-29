//! AMLL TTML database adapter.

#[cfg(feature = "amll-ttml-db")]
use std::{
    collections::BTreeMap,
    sync::{Mutex, OnceLock},
};

#[cfg(feature = "amll-ttml-db")]
use lyrics_helper::LyricsRawTypes;
#[cfg(feature = "amll-ttml-db")]
use serde_json::Value;

#[cfg(feature = "amll-ttml-db")]
use super::shared::{
    RecordingMetadata, attach_provider_timing, encode_uri_component, lrc_candidate, metadata_score,
    value_text,
};
#[cfg(feature = "amll-ttml-db")]
use crate::{
    LyricsCandidate, LyricsFuture, LyricsHttp, LyricsLookup, LyricsSource, convert_ttml_to_elrc,
    convert_ttml_to_lrc,
};

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
                encode_uri_component(&entry.file)
            );
            let Some(ttml) = http.get_json(&url).await else {
                return Vec::new();
            };
            let lyrics = convert_ttml_to_elrc(&ttml).or_else(|| convert_ttml_to_lrc(&ttml));
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
