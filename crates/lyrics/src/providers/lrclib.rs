//! LRCLIB exact-match and search adapter.

#[cfg(feature = "lrclib")]
use super::shared::{field, form_encode, push_candidate};
#[cfg(feature = "lrclib")]
use crate::{LyricsCandidate, LyricsFuture, LyricsHttp, LyricsLookup, LyricsSource};

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
