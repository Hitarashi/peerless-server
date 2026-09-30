use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use serde_json::Value;
use tokio::sync::Mutex;

const MIN_REQUEST_INTERVAL: Duration = Duration::from_millis(1_150);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(20);
const DURATION_TOLERANCE_MS: i64 = 5_000;

#[derive(Clone)]
pub struct RecordingMbidResolver {
    client: reqwest::Client,
    candidates_by_isrc: Arc<Mutex<HashMap<String, Arc<Vec<RecordingCandidate>>>>>,
    request_gate: Arc<Mutex<Option<Instant>>>,
}

#[derive(Debug, Clone)]
struct RecordingCandidate {
    mbid: String,
    title: String,
    artist_credit: String,
    length_ms: Option<i64>,
}

impl Default for RecordingMbidResolver {
    fn default() -> Self {
        let client = reqwest::Client::builder()
            .user_agent(concat!(
                "PeerlessServer/",
                env!("CARGO_PKG_VERSION"),
                " (https://github.com/Hitarashi/peerless-server)"
            ))
            .timeout(REQUEST_TIMEOUT)
            .build()
            .unwrap_or_default();
        Self {
            client,
            candidates_by_isrc: Arc::new(Mutex::new(HashMap::new())),
            request_gate: Arc::new(Mutex::new(None)),
        }
    }
}

impl RecordingMbidResolver {
    /// Resolves only an unambiguous MusicBrainz recording match for the supplied ISRC and metadata.
    pub async fn resolve(
        &self,
        isrc: &str,
        title: &str,
        artist: &str,
        duration_seconds: i64,
    ) -> Option<String> {
        let isrc = normalize_isrc(isrc)?;
        let title = normalize_text(title);
        let artist = normalize_text(artist);
        if title.is_empty() || artist.is_empty() {
            return None;
        }

        let candidates = self.candidates_for_isrc(&isrc).await?;
        let matches = candidates
            .iter()
            .filter(|candidate| {
                normalize_text(&candidate.title) == title
                    && normalize_text(&candidate.artist_credit) == artist
                    && duration_matches(candidate.length_ms, duration_seconds)
            })
            .map(|candidate| candidate.mbid.as_str())
            .collect::<std::collections::HashSet<_>>();

        if matches.len() == 1 {
            matches.into_iter().next().map(ToOwned::to_owned)
        } else {
            None
        }
    }

    async fn candidates_for_isrc(&self, isrc: &str) -> Option<Arc<Vec<RecordingCandidate>>> {
        if let Some(cached) = self.candidates_by_isrc.lock().await.get(isrc).cloned() {
            return Some(cached);
        }

        let mut gate = self.request_gate.lock().await;
        if let Some(cached) = self.candidates_by_isrc.lock().await.get(isrc).cloned() {
            return Some(cached);
        }
        if let Some(last_request) = *gate {
            let wait = MIN_REQUEST_INTERVAL.saturating_sub(last_request.elapsed());
            if !wait.is_zero() {
                tokio::time::sleep(wait).await;
            }
        }

        let url = format!("https://musicbrainz.org/ws/2/isrc/{isrc}?inc=artists&fmt=json");
        let response = self.client.get(&url).send().await;
        *gate = Some(Instant::now());
        let response = match response.and_then(reqwest::Response::error_for_status) {
            Ok(response) => response,
            Err(error) => {
                tracing::warn!(%isrc, %error, "MusicBrainz ISRC lookup failed");
                return None;
            }
        };
        let payload = match response.json::<Value>().await {
            Ok(payload) => payload,
            Err(error) => {
                tracing::warn!(%isrc, %error, "MusicBrainz ISRC response was not valid JSON");
                return None;
            }
        };
        let candidates = Arc::new(parse_candidates(&payload));
        self.candidates_by_isrc
            .lock()
            .await
            .insert(isrc.to_owned(), Arc::clone(&candidates));
        Some(candidates)
    }
}

fn normalize_isrc(value: &str) -> Option<String> {
    let normalized = value
        .chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .collect::<String>()
        .to_ascii_uppercase();
    (normalized.len() == 12).then_some(normalized)
}

fn normalize_text(value: &str) -> String {
    value
        .chars()
        .filter(|character| character.is_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect()
}

fn duration_matches(length_ms: Option<i64>, duration_seconds: i64) -> bool {
    match (length_ms, duration_seconds) {
        (Some(length_ms), duration_seconds) if duration_seconds > 0 => {
            (length_ms - duration_seconds.saturating_mul(1_000)).abs() <= DURATION_TOLERANCE_MS
        }
        _ => true,
    }
}

fn parse_candidates(payload: &Value) -> Vec<RecordingCandidate> {
    payload
        .get("recordings")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(parse_candidate)
        .collect()
}

fn parse_candidate(value: &Value) -> Option<RecordingCandidate> {
    let mbid = music::normalize_recording_mbid(value.get("id")?.as_str()?)?;
    let title = value.get("title")?.as_str()?.to_owned();
    let artist_credit = artist_credit_phrase(value.get("artist-credit")?)?;
    let length_ms = value.get("length").and_then(Value::as_i64);
    Some(RecordingCandidate {
        mbid,
        title,
        artist_credit,
        length_ms,
    })
}

fn artist_credit_phrase(value: &Value) -> Option<String> {
    if let Some(credit) = value.as_str() {
        return Some(credit.to_owned());
    }
    let entries = value.as_array()?;
    let mut phrase = String::new();
    for entry in entries {
        if let Some(text) = entry.as_str() {
            phrase.push_str(text);
            continue;
        }
        let name = entry.get("name").and_then(Value::as_str).or_else(|| {
            entry
                .get("artist")
                .and_then(|artist| artist.get("name"))?
                .as_str()
        })?;
        phrase.push_str(name);
        phrase.push_str(
            entry
                .get("joinphrase")
                .and_then(Value::as_str)
                .unwrap_or_default(),
        );
    }
    (!phrase.trim().is_empty()).then_some(phrase)
}
