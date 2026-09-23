//! Provider-agnostic lyrics lookup and rendering.
//!
//! Sources are deliberately kept behind two small seams: [`LyricsHttp`] is
//! the only network operation this crate needs, while [`LyricsSource`] lets a
//! caller choose the enabled sources and their order. The built-in adapters
//! are feature gated so applications do not have to inherit every provider.

use std::{collections::BTreeMap, future::Future, pin::Pin, sync::OnceLock};

use futures_util::future::join_all;
#[cfg(feature = "format-parser")]
use lyrics_helper::{LyricsAlignment, LyricsRawTypes, parse as parse_provider_lyrics};
use regex::Regex;
use serde::{Deserialize, Serialize};

#[cfg(any(
    feature = "amll-ttml-db",
    feature = "binimum",
    feature = "kugou",
    feature = "musixmatch",
    feature = "netease",
    feature = "qq",
    feature = "spotify",
    feature = "youtube"
))]
mod providers;

#[cfg(feature = "amll-ttml-db")]
pub use providers::AmllTtmlDb;
#[cfg(feature = "binimum")]
pub use providers::Binimum;
#[cfg(feature = "kugou")]
pub use providers::Kugou;
#[cfg(feature = "musixmatch")]
pub use providers::Musixmatch;
#[cfg(feature = "netease")]
pub use providers::NetEase;
#[cfg(feature = "qq")]
pub use providers::QqMusic;
#[cfg(feature = "spotify")]
pub use providers::Spotify;
#[cfg(feature = "youtube")]
pub use providers::YouTubeMusic;

/// A boxed future used by the object-safe HTTP and source seams.
pub type LyricsFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Canonical track information used by lyrics providers.
///
/// Provider-specific identifiers belong in [`LyricsLookup::provider_ids`],
/// keeping this input independent of any catalog or media provider.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LyricsLookup {
    pub title: String,
    pub artists: Vec<String>,
    pub album: Option<String>,
    pub duration: Option<i64>,
    pub provider_ids: BTreeMap<String, String>,
}

impl LyricsLookup {
    pub fn new<T, I>(title: impl Into<String>, artists: I) -> Self
    where
        I: IntoIterator<Item = T>,
        T: Into<String>,
    {
        Self {
            title: title.into(),
            artists: artists.into_iter().map(Into::into).collect(),
            ..Self::default()
        }
    }

    pub fn artist_string(&self) -> String {
        self.artists.join(", ")
    }

    pub fn with_album(mut self, album: impl Into<String>) -> Self {
        self.album = Some(album.into());
        self
    }

    pub fn with_duration(mut self, duration: i64) -> Self {
        self.duration = Some(duration);
        self
    }

    pub fn with_provider_id(mut self, provider: impl Into<String>, id: impl Into<String>) -> Self {
        self.provider_ids.insert(provider.into(), id.into());
        self
    }
}

/// The quality class inferred from a lyrics document.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum LyricsTier {
    WordSynced = 1000,
    LineSynced = 500,
    Plain = 100,
    None = 0,
}

impl LyricsTier {
    fn is_none(self) -> bool {
        matches!(self, Self::None)
    }
}

/// The representation carried by a document.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LyricsFormat {
    Plain,
    Lrc,
    Elrc,
}

/// A source and the URL from which a candidate was obtained.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LyricsSourceInfo {
    pub id: String,
    pub url: Option<String>,
}

/// Best-effort rights information. Missing fields are intentionally left
/// unknown rather than implying that a source grants redistribution rights.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LyricsRights {
    pub attribution: Option<String>,
    pub license: Option<String>,
    pub redistribution: Option<String>,
}

/// A normalized lyrics document, ready for richer rendering in future.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LyricsDocument {
    pub text: String,
    pub format: LyricsFormat,
    pub tier: LyricsTier,
    pub source: LyricsSourceInfo,
    pub rights: LyricsRights,
    pub language: Option<String>,
    pub translations: Vec<String>,
    pub romanization: Option<String>,
    /// Normalized line and word timing when the source provides word timing.
    /// Plain and line-only lyrics leave this unset.
    pub word_timing: Option<Vec<LyricsTimedLine>>,
}

/// Structured timing for a lyrics document.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LyricsTimedLine {
    pub text: String,
    pub start_ms: u64,
    pub end_ms: Option<u64>,
    pub words: Vec<LyricsTimedWord>,
    pub background_words: Vec<LyricsTimedWord>,
    pub alignment: Option<String>,
    /// Provider-supplied TTML vocal agent identifier, when present.
    pub agent: Option<String>,
    pub translations: Vec<LyricsTranslation>,
    pub romanization: Option<String>,
    pub is_instrumental: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LyricsTranslation {
    pub language: String,
    pub text: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LyricsTimedWord {
    pub start_ms: u64,
    pub end_ms: Option<u64>,
    pub text: String,
}

/// Preserve provider-supplied lyric structure before a source adapter
/// flattens it to display text.
#[cfg(feature = "format-parser")]
pub(crate) fn parse_provider_timed_lines(
    raw: &str,
    raw_type: LyricsRawTypes,
) -> Option<Vec<LyricsTimedLine>> {
    fn parse_words(line: &lyrics_helper::LineInfo) -> Vec<LyricsTimedWord> {
        line.syllables()
            .into_iter()
            .flatten()
            .flat_map(|syllable| syllable.parts())
            .filter_map(|part| {
                let start_ms = u64::try_from(part.start_time).ok()?;
                let text = part.text.clone();
                (!text.is_empty()).then_some(LyricsTimedWord {
                    start_ms,
                    end_ms: u64::try_from(part.end_time).ok(),
                    text,
                })
            })
            .collect()
    }

    let provider_agents = if raw_type == LyricsRawTypes::Ttml {
        roxmltree::Document::parse(raw)
            .ok()
            .map(|document| {
                document
                    .descendants()
                    .filter(|node| node.is_element() && node.tag_name().name() == "p")
                    .map(|line| {
                        line.attributes()
                            .find(|attribute| attribute.name() == "agent")
                            .map(|attribute| attribute.value().to_owned())
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default()
    } else {
        Vec::new()
    };

    let parsed = parse_provider_lyrics(raw, raw_type)?;
    let lines = parsed.lines?;
    let timed = lines
        .into_iter()
        .enumerate()
        .filter_map(|(index, line)| {
            let background_words = line.sub_line().map(parse_words).unwrap_or_default();
            let words = parse_words(&line);
            let start_ms = line
                .start_time_with_sub_line()
                .and_then(|start| u64::try_from(start).ok())
                .or_else(|| words.first().map(|word| word.start_ms))
                .or_else(|| background_words.first().map(|word| word.start_ms))?;
            let text = line.text_from_any();
            if text.trim().is_empty() && words.is_empty() && background_words.is_empty() {
                return None;
            }
            let end_ms = line
                .end_time_with_sub_line()
                .filter(|end| *end >= 0)
                .map(|end| end as u64)
                .or_else(|| words.last().and_then(|word| word.end_ms));
            let alignment = match line.alignment() {
                LyricsAlignment::Left => Some("left".to_owned()),
                LyricsAlignment::Right => Some("right".to_owned()),
                LyricsAlignment::Unspecified => None,
            };
            let mut translations = line
                .translations()
                .into_iter()
                .flat_map(|translations| translations.iter())
                .filter(|(_, text)| !text.trim().is_empty())
                .map(|(language, text)| LyricsTranslation {
                    language: language.clone(),
                    text: text.trim().to_owned(),
                })
                .collect::<Vec<_>>();
            translations.sort_by(|left, right| left.language.cmp(&right.language));
            Some(LyricsTimedLine {
                text,
                start_ms,
                end_ms,
                words,
                background_words,
                alignment,
                agent: provider_agents.get(index).cloned().flatten(),
                translations,
                romanization: line
                    .pronunciation()
                    .map(str::trim)
                    .filter(|text| !text.is_empty())
                    .map(str::to_owned),
                is_instrumental: false,
            })
        })
        .collect::<Vec<_>>();
    (!timed.is_empty()).then_some(timed)
}

/// Ranking metadata kept separate from the lyrics document.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LyricsRanking {
    pub provider: String,
    pub score: i64,
}

/// A source result with one authoritative lyrics document and separate
/// ranking metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LyricsCandidate {
    pub document: LyricsDocument,
    pub ranking: LyricsRanking,
}

impl LyricsCandidate {
    pub fn text(&self) -> &str {
        &self.document.text
    }

    pub fn tier(&self) -> LyricsTier {
        self.document.tier
    }

    pub fn source(&self) -> &LyricsSourceInfo {
        &self.document.source
    }

    pub fn rights(&self) -> &LyricsRights {
        &self.document.rights
    }

    pub fn provider(&self) -> &str {
        &self.ranking.provider
    }

    pub fn score(&self) -> i64 {
        self.ranking.score
    }

    pub fn source_id(&self) -> &str {
        &self.document.source.id
    }

    pub fn source_url(&self) -> Option<&str> {
        self.document.source.url.as_deref()
    }
}

/// Minimal HTTP operation required by all adapters. The application owns
/// clients, headers, timeout policy, retries, and status handling.
pub trait LyricsHttp: Send + Sync {
    fn get_json<'a>(&'a self, url: &'a str) -> LyricsFuture<'a, Option<String>>;

    fn get_with_headers<'a>(
        &'a self,
        url: &'a str,
        headers: &'a [(&'a str, &'a str)],
    ) -> LyricsFuture<'a, Option<String>> {
        let _ = headers;
        self.get_json(url)
    }

    fn post_with_headers<'a>(
        &'a self,
        url: &'a str,
        headers: &'a [(&'a str, &'a str)],
        body: &'a str,
    ) -> LyricsFuture<'a, Option<String>> {
        let _ = (url, headers, body);
        Box::pin(async { None })
    }
}

/// A source adapter selected by a [`LyricsRegistry`].
pub trait LyricsSource: Send + Sync {
    fn id(&self) -> &str;
    fn lookup<'a>(
        &'a self,
        http: &'a dyn LyricsHttp,
        input: &'a LyricsLookup,
    ) -> LyricsFuture<'a, Vec<LyricsCandidate>>;
}

/// Explicitly ordered source collection. Results are fetched concurrently,
/// then flattened in registry order so stable score ties are deterministic.
pub struct LyricsRegistry {
    sources: Vec<Box<dyn LyricsSource>>,
}

impl LyricsRegistry {
    pub fn new() -> Self {
        Self {
            sources: Vec::new(),
        }
    }

    pub fn add_source<S: LyricsSource + 'static>(&mut self, source: S) {
        self.sources.push(Box::new(source));
    }

    pub fn with_source<S: LyricsSource + 'static>(mut self, source: S) -> Self {
        self.add_source(source);
        self
    }

    pub fn source_ids(&self) -> impl Iterator<Item = &str> {
        self.sources.iter().map(|source| source.id())
    }

    /// Iterate over the configured sources for diagnostics and custom lookup policies.
    pub fn sources(&self) -> impl Iterator<Item = &dyn LyricsSource> + '_ {
        self.sources.iter().map(|source| source.as_ref())
    }

    /// The default registry intentionally contains only LRCLIB when that
    /// feature is enabled. Applications that want other sources should make
    /// that policy explicit with [`LyricsRegistry::all_sources`] or `add_source`.
    pub fn default_registry() -> Self {
        #[cfg(feature = "lrclib")]
        {
            Self::new().with_source(Lrclib)
        }
        #[cfg(not(feature = "lrclib"))]
        Self::new()
    }

    /// Built-in sources in the bot's historical quality/order policy.
    pub fn all_sources() -> Self {
        #[cfg(any(
            feature = "paxsenix",
            feature = "betterlyrics",
            feature = "unison",
            feature = "lrclib",
            feature = "binimum",
            feature = "amll-ttml-db",
            feature = "kugou",
            feature = "netease",
            feature = "musixmatch",
            feature = "qq",
            feature = "youtube",
            feature = "spotify"
        ))]
        {
            let mut registry = Self::new();
            #[cfg(feature = "paxsenix")]
            registry.add_source(Paxsenix);
            #[cfg(feature = "betterlyrics")]
            registry.add_source(BetterLyrics);
            #[cfg(feature = "unison")]
            registry.add_source(Unison);
            #[cfg(feature = "binimum")]
            registry.add_source(Binimum);
            #[cfg(feature = "amll-ttml-db")]
            registry.add_source(AmllTtmlDb);
            #[cfg(feature = "kugou")]
            registry.add_source(Kugou);
            #[cfg(feature = "netease")]
            registry.add_source(NetEase);
            #[cfg(feature = "musixmatch")]
            registry.add_source(Musixmatch);
            #[cfg(feature = "qq")]
            registry.add_source(QqMusic);
            #[cfg(feature = "youtube")]
            registry.add_source(YouTubeMusic);
            #[cfg(feature = "spotify")]
            registry.add_source(Spotify::from_env());
            #[cfg(feature = "lrclib")]
            registry.add_source(Lrclib);
            registry
        }
        #[cfg(not(any(
            feature = "paxsenix",
            feature = "betterlyrics",
            feature = "unison",
            feature = "lrclib",
            feature = "binimum",
            feature = "amll-ttml-db",
            feature = "kugou",
            feature = "netease",
            feature = "musixmatch",
            feature = "qq",
            feature = "youtube",
            feature = "spotify"
        )))]
        Self::new()
    }
}

impl Default for LyricsRegistry {
    fn default() -> Self {
        Self::default_registry()
    }
}

/// The crate's default source policy.
pub fn default_registry() -> LyricsRegistry {
    LyricsRegistry::default_registry()
}

/// Fetch, rank, and return all usable candidates.
pub async fn lookup_ranked(
    http: &impl LyricsHttp,
    registry: &LyricsRegistry,
    input: &LyricsLookup,
) -> Vec<LyricsCandidate> {
    let mut candidates: Vec<LyricsCandidate> = join_all(
        registry
            .sources
            .iter()
            .map(|source| source.lookup(http, input)),
    )
    .await
    .into_iter()
    .flatten()
    .filter(|candidate| !candidate.tier().is_none() && candidate.score() > 0)
    .collect();
    candidates.sort_by_key(|candidate| std::cmp::Reverse(candidate.score()));
    candidates
}

/// Convenience path returning the best renderable text.
pub async fn lookup(
    http: &impl LyricsHttp,
    registry: &LyricsRegistry,
    input: &LyricsLookup,
) -> Option<String> {
    lookup_ranked(http, registry, input)
        .await
        .into_iter()
        .next()
        .map(|candidate| candidate.text().to_owned())
}

/// Convert Apple Music TTML with word-level spans into Enhanced LRC.
pub fn convert_ttml_to_elrc(ttml: &str) -> Option<String> {
    let lines = parse_ttml_lines(ttml)?;
    if !lines.iter().any(|line| !line.words.is_empty()) {
        return None;
    }
    Some(
        lines
            .into_iter()
            .map(|line| {
                let words = line
                    .words
                    .into_iter()
                    .map(|(start_ms, text)| format!("<{}>{text}", format_millis(start_ms)))
                    .collect::<Vec<_>>();
                if words.is_empty() {
                    format!("[{}]{}", format_millis(line.start_ms), line.text)
                } else {
                    format!("[{}]{}", format_millis(line.start_ms), words.join(" "))
                }
            })
            .collect::<Vec<_>>()
            .join("\n"),
    )
}

/// Convert line-timed or word-timed TTML to LRC-compatible timing text.
pub fn convert_ttml_to_lrc(ttml: &str) -> Option<String> {
    let lines = parse_ttml_lines(ttml)?;
    let lines: Vec<String> = lines
        .into_iter()
        .filter(|line| !line.text.is_empty())
        .map(|line| format!("[{}]{}", format_millis(line.start_ms), line.text))
        .collect();
    (!lines.is_empty()).then(|| lines.join("\n"))
}

struct ParsedTtmlLine {
    start_ms: u64,
    text: String,
    words: Vec<(u64, String)>,
}

fn parse_ttml_lines(ttml: &str) -> Option<Vec<ParsedTtmlLine>> {
    fn collect(document: &roxmltree::Document<'_>) -> Vec<ParsedTtmlLine> {
        document
            .descendants()
            .filter(|node| node.is_element() && node.tag_name().name() == "p")
            .filter_map(|paragraph| {
                let words: Vec<(u64, String)> = paragraph
                    .descendants()
                    .filter(|node| node.is_element() && node.tag_name().name() == "span")
                    .filter_map(|span| {
                        let start_ms = parse_millis(span.attribute("begin")?)?;
                        let text = xml_text(span).trim().to_owned();
                        (!text.is_empty()).then_some((start_ms, text))
                    })
                    .collect();
                let start_ms = paragraph
                    .attribute("begin")
                    .and_then(parse_millis)
                    .or_else(|| words.first().map(|(start_ms, _)| *start_ms))?;
                let text = xml_text(paragraph).trim().to_owned();
                (!text.is_empty() || !words.is_empty()).then_some(ParsedTtmlLine {
                    start_ms,
                    text,
                    words,
                })
            })
            .collect()
    }

    let trimmed = ttml.trim().trim_start_matches('\u{feff}');
    let without_declaration = trimmed
        .strip_prefix("<?xml")
        .and_then(|xml| xml.find("?>").map(|end| &xml[end + 2..]))
        .unwrap_or(trimmed);
    let raw = trimmed.to_owned();
    if let Ok(document) = roxmltree::Document::parse(&raw) {
        let lines = collect(&document);
        return (!lines.is_empty()).then_some(lines);
    }

    let wrapped = format!("<root>{without_declaration}</root>");
    let document = roxmltree::Document::parse(&wrapped).ok()?;
    let lines = collect(&document);
    (!lines.is_empty()).then_some(lines)
}

/// Classify lyrics quality.
pub fn detect_lyrics_tier(lyrics: &str) -> LyricsTier {
    let trimmed = lyrics.trim();
    if trimmed.len() < 10 {
        return LyricsTier::None;
    }
    let lines: Vec<&str> = trimmed
        .split('\n')
        .filter(|line| !line.trim().is_empty())
        .collect();
    if lines.len() < 2 {
        return LyricsTier::None;
    }
    if lines.iter().any(|line| word_sync_regex().is_match(line)) {
        return LyricsTier::WordSynced;
    }
    if lines.iter().any(|line| line_sync_regex().is_match(line)) {
        return LyricsTier::LineSynced;
    }
    LyricsTier::Plain
}

/// Score a document using the historical tier and source weight model.
pub fn score_lyrics(text: &str, provider: &'static str, provider_weight: i64) -> LyricsCandidate {
    score_candidate(
        text,
        provider,
        provider,
        None,
        provider_weight,
        LyricsRights::default(),
    )
}

/// Build and score a candidate from a custom source adapter.
pub fn score_candidate(
    text: &str,
    provider: &str,
    source_id: &str,
    source_url: Option<String>,
    provider_weight: i64,
    rights: LyricsRights,
) -> LyricsCandidate {
    let text = text.trim().to_owned();
    let tier = detect_lyrics_tier(&text);
    let score = if tier.is_none() {
        0
    } else {
        tier as i64 + provider_weight
    };
    let word_timing = parse_word_timing(&text);
    let document = LyricsDocument {
        text,
        format: match tier {
            LyricsTier::WordSynced => LyricsFormat::Elrc,
            LyricsTier::LineSynced => LyricsFormat::Lrc,
            LyricsTier::Plain | LyricsTier::None => LyricsFormat::Plain,
        },
        tier,
        source: LyricsSourceInfo {
            id: source_id.to_owned(),
            url: source_url.clone(),
        },
        rights,
        language: None,
        translations: Vec::new(),
        romanization: None,
        word_timing,
    };
    LyricsCandidate {
        document,
        ranking: LyricsRanking {
            provider: provider.to_owned(),
            score,
        },
    }
}

#[cfg(any(
    feature = "lrclib",
    feature = "betterlyrics",
    feature = "paxsenix",
    feature = "unison"
))]
fn field(value: &serde_json::Value, name: &str) -> Option<String> {
    value
        .get(name)
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
        .filter(|text| !text.trim().is_empty())
}

#[cfg(any(feature = "lrclib", feature = "betterlyrics", feature = "paxsenix"))]
fn push_candidate(
    candidates: &mut Vec<LyricsCandidate>,
    value: &serde_json::Value,
    field_name: &str,
    provider: &str,
    source_id: &str,
    source_url: &str,
    weight: i64,
) {
    if let Some(text) = field(value, field_name) {
        candidates.push(score_candidate(
            &text,
            provider,
            source_id,
            Some(source_url.to_owned()),
            weight,
            LyricsRights::default(),
        ));
    }
}

#[cfg(any(
    feature = "amll-ttml-db",
    feature = "betterlyrics",
    feature = "binimum",
    feature = "paxsenix"
))]
fn encode_uri_component(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z'
            | b'a'..=b'z'
            | b'0'..=b'9'
            | b'-'
            | b'_'
            | b'.'
            | b'!'
            | b'~'
            | b'*'
            | b'\''
            | b'('
            | b')' => encoded.push(byte as char),
            _ => encoded.push_str(&format!("%{byte:02X}")),
        }
    }
    encoded
}

#[allow(dead_code)]
fn form_encode(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(byte as char)
            }
            b' ' => encoded.push('+'),
            _ => encoded.push_str(&format!("%{byte:02X}")),
        }
    }
    encoded
}

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

#[cfg(feature = "lrclib")]
#[derive(Debug, Default)]
pub struct Lrclib;

#[cfg(feature = "lrclib")]
async fn lrclib_exact(http: &dyn LyricsHttp, input: &LyricsLookup) -> Vec<LyricsCandidate> {
    let mut params = format!(
        "artist_name={}&track_name={}",
        form_encode(&input.artist_string()),
        form_encode(&input.title)
    );
    if let Some(album) = &input.album {
        params.push_str(&format!("&album_name={}", form_encode(album)));
    }
    if let Some(duration) = input.duration.filter(|duration| *duration > 0) {
        params.push_str(&format!("&duration={duration}"));
    }
    let url = format!("https://lrclib.net/api/get?{params}");
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
        "syncedLyrics",
        "LRCLIB Exact (LRC)",
        "lrclib",
        &url,
        15,
    );
    push_candidate(
        &mut candidates,
        &data,
        "plainLyrics",
        "LRCLIB Exact (Plain)",
        "lrclib",
        &url,
        10,
    );
    candidates
}

#[cfg(feature = "lrclib")]
async fn lrclib_search(http: &dyn LyricsHttp, input: &LyricsLookup) -> Vec<LyricsCandidate> {
    let url = format!(
        "https://lrclib.net/api/search?q={}",
        form_encode(&format!("{} {}", input.title, input.artist_string()))
    );
    let Some(body) = http.get_json(&url).await else {
        return Vec::new();
    };
    let Ok(mut results) = serde_json::from_str::<Vec<serde_json::Value>>(&body) else {
        return Vec::new();
    };
    results.retain(|item| lrclib_metadata_matches(input, item));
    if let Some(target) = input.duration.filter(|duration| *duration > 0) {
        results.sort_by_key(|item| {
            item.get("duration")
                .and_then(serde_json::Value::as_i64)
                .unwrap_or_default()
                .abs_diff(target)
        });
    }
    let mut candidates = Vec::new();
    for result in results {
        push_candidate(
            &mut candidates,
            &result,
            "syncedLyrics",
            "LRCLIB Search (LRC)",
            "lrclib",
            &url,
            5,
        );
        push_candidate(
            &mut candidates,
            &result,
            "plainLyrics",
            "LRCLIB Search (Plain)",
            "lrclib",
            &url,
            0,
        );
    }
    candidates
}

#[cfg(feature = "lrclib")]
fn lrclib_metadata_matches(input: &LyricsLookup, result: &serde_json::Value) -> bool {
    let Some(title) = field(result, "trackName") else {
        return false;
    };
    let Some(artist) = field(result, "artistName") else {
        return false;
    };
    let normalize = |value: &str| {
        value
            .chars()
            .filter(|character| character.is_alphanumeric())
            .flat_map(char::to_lowercase)
            .collect::<String>()
    };
    let expected_title = normalize(&input.title);
    let actual_title = normalize(&title);
    let expected_artist = normalize(&input.artist_string());
    let actual_artist = normalize(&artist);
    if expected_title.is_empty()
        || actual_title.is_empty()
        || !(actual_title == expected_title
            || actual_title.contains(&expected_title)
            || expected_title.contains(&actual_title))
        || expected_artist.is_empty()
        || actual_artist.is_empty()
        || !(actual_artist == expected_artist
            || actual_artist.contains(&expected_artist)
            || expected_artist.contains(&actual_artist))
    {
        return false;
    }
    if let Some(expected) = input.duration.filter(|duration| *duration > 0)
        && let Some(actual) = result.get("duration").and_then(serde_json::Value::as_i64)
        && expected.abs_diff(actual) > 12
    {
        return false;
    }
    true
}

#[cfg(feature = "lrclib")]
impl LyricsSource for Lrclib {
    fn id(&self) -> &str {
        "lrclib"
    }

    fn lookup<'a>(
        &'a self,
        http: &'a dyn LyricsHttp,
        input: &'a LyricsLookup,
    ) -> LyricsFuture<'a, Vec<LyricsCandidate>> {
        Box::pin(async move {
            let (exact, search) =
                futures_util::join!(lrclib_exact(http, input), lrclib_search(http, input));
            exact.into_iter().chain(search).collect()
        })
    }
}

fn word_sync_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"<\d{1,3}:\d{2}(?:[.:]\d{2,3})?>").expect("word sync regex"))
}

fn line_sync_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"\[\d{1,3}:\d{2}(?:[.:]\d{2,3})?]").expect("line sync regex"))
}

fn any_tag_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"(?s)<[^>]+>").expect("any tag regex"))
}

fn timed_line_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"^\s*\[([^]]+)\](.*)$").expect("timed line regex"))
}

fn timed_word_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"<([^>]+)>([^<]*)").expect("timed word regex"))
}

fn parse_millis(time_str: &str) -> Option<u64> {
    let time_str = time_str.trim();
    for (unit, multiplier) in [
        ("ms", 1.0),
        ("h", 3_600_000.0),
        ("m", 60_000.0),
        ("s", 1_000.0),
    ] {
        if let Some(value) = time_str.strip_suffix(unit) {
            let value = value.trim().parse::<f64>().ok()? * multiplier;
            return (value.is_finite() && value >= 0.0 && value <= u64::MAX as f64)
                .then_some(value.round() as u64);
        }
    }
    let parts: Vec<&str> = time_str.split(':').collect();
    let (hours, minutes, seconds) = match parts.as_slice() {
        [seconds] => (0, 0, *seconds),
        [minutes, seconds] => (0, minutes.parse().ok()?, *seconds),
        [hours, minutes, seconds] => (hours.parse().ok()?, minutes.parse().ok()?, *seconds),
        _ => return None,
    };
    let (whole_seconds, fraction) = seconds.split_once('.').unwrap_or((seconds, ""));
    let whole_seconds: u64 = whole_seconds.parse().ok()?;
    let mut milliseconds = fraction.as_bytes().iter().take(3).fold(0, |value, digit| {
        value * 10 + u64::from(digit.wrapping_sub(b'0'))
    });
    if !fraction.is_empty() && !fraction.bytes().all(|digit| digit.is_ascii_digit()) {
        return None;
    }
    for _ in fraction.len().min(3)..3 {
        milliseconds *= 10;
    }
    Some((hours * 3_600 + minutes * 60 + whole_seconds) * 1_000 + milliseconds)
}

fn format_millis(total_ms: u64) -> String {
    let minutes = total_ms / 60_000;
    let seconds = (total_ms / 1_000) % 60;
    let milliseconds = total_ms % 1_000;
    format!("{minutes:02}:{seconds:02}.{milliseconds:03}")
}

fn xml_text(node: roxmltree::Node<'_, '_>) -> String {
    node.descendants()
        .filter(|descendant| descendant.is_text())
        .filter_map(|descendant| descendant.text())
        .collect()
}

fn parse_word_timing(text: &str) -> Option<Vec<LyricsTimedLine>> {
    let mut lines = Vec::new();
    for line in text.lines() {
        let Some(line_caps) = timed_line_regex().captures(line) else {
            continue;
        };
        let Some(start_ms) = parse_millis(&line_caps[1]) else {
            continue;
        };
        let words: Vec<LyricsTimedWord> = timed_word_regex()
            .captures_iter(&line_caps[2])
            .filter_map(|word_caps| {
                let start_ms = parse_millis(&word_caps[1])?;
                let text = decode_entities(&strip_tags(&word_caps[2]));
                (!text.trim().is_empty()).then_some(LyricsTimedWord {
                    start_ms,
                    end_ms: None,
                    text,
                })
            })
            .collect();
        if !words.is_empty() {
            lines.push(LyricsTimedLine {
                text: words.iter().map(|word| word.text.as_str()).collect(),
                start_ms,
                end_ms: None,
                words,
                background_words: Vec::new(),
                alignment: None,
                agent: None,
                translations: Vec::new(),
                romanization: None,
                is_instrumental: false,
            });
        }
    }
    (!lines.is_empty()).then_some(lines)
}

fn strip_tags(text: &str) -> String {
    any_tag_regex().replace_all(text, "").into_owned()
}

fn decode_entities(text: &str) -> String {
    text.replace("&amp;", "&")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
}
