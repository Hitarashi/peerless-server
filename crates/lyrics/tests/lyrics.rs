//! Lyrics parsing and provider-scoring tests.

#[cfg(any(feature = "lrclib", feature = "betterlyrics", feature = "paxsenix"))]
use std::{collections::HashMap, sync::Mutex};

#[cfg(any(feature = "lrclib", feature = "betterlyrics", feature = "paxsenix"))]
use lyrics::{LyricsFuture, LyricsHttp, LyricsLookup, LyricsRegistry, lookup};
use lyrics::{LyricsTier, convert_ttml_to_elrc, detect_lyrics_tier, score_lyrics};

#[cfg(any(feature = "lrclib", feature = "betterlyrics", feature = "paxsenix"))]
struct FakeHttp {
    routes: Mutex<HashMap<String, String>>,
}

#[cfg(any(feature = "lrclib", feature = "betterlyrics", feature = "paxsenix"))]
impl FakeHttp {
    fn with(routes: &[(&str, &str)]) -> Self {
        Self {
            routes: Mutex::new(
                routes
                    .iter()
                    .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
                    .collect(),
            ),
        }
    }
}

#[cfg(any(feature = "lrclib", feature = "betterlyrics", feature = "paxsenix"))]
impl LyricsHttp for FakeHttp {
    fn get_json<'a>(&'a self, url: &'a str) -> LyricsFuture<'a, Option<String>> {
        Box::pin(async move {
            let routes = self.routes.lock().unwrap();
            routes
                .iter()
                .find(|(needle, _)| url.contains(needle.as_str()))
                .map(|(_, body)| body.clone())
        })
    }
}

#[test]
fn tier_detection() {
    let word_synced = "[00:01.00]<00:01.20>hello <00:02.40>world\n[00:05.00]<00:05.10>again";
    assert_eq!(detect_lyrics_tier(word_synced), LyricsTier::WordSynced);
    let line_synced = "[00:01.00]first line\n[00:05.00]second line";
    assert_eq!(detect_lyrics_tier(line_synced), LyricsTier::LineSynced);
    let plain = "first line here\nsecond line here";
    assert_eq!(detect_lyrics_tier(plain), LyricsTier::Plain);
    assert_eq!(detect_lyrics_tier("short"), LyricsTier::None);
    assert_eq!(detect_lyrics_tier("12345678901"), LyricsTier::None); // 1 line
    assert_eq!(detect_lyrics_tier(""), LyricsTier::None);
}

#[test]
fn scoring_math() {
    let candidate = score_lyrics("[00:01.00]a\n[00:02.00]b", "P", 30);
    assert_eq!(candidate.tier(), LyricsTier::LineSynced);
    assert_eq!(candidate.score(), 500 + 30);
    let none = score_lyrics("garbage", "P", 30);
    assert_eq!(none.score(), 0);
    assert_eq!(none.tier(), LyricsTier::None);
}

#[test]
fn ttml_conversion_maps_spans_and_decodes_entities() {
    let ttml = r#"<tt><body><div>
      <p begin="00:00:05.000" end="00:00:08.000">hello &amp; welcome</p>
      <p begin="00:00:10.500"><span begin="00:00:10.500">first </span><span begin="00:00:11.200">word</span></p>
    </div></body></tt>"#;
    let elrc = convert_ttml_to_elrc(ttml).unwrap();
    let lines: Vec<&str> = elrc.split('\n').collect();
    assert_eq!(lines[0], "[00:05.000]hello & welcome");
    assert_eq!(lines[1], "[00:10.500]<00:10.500>first <00:11.200>word");
}

#[test]
fn ttml_conversion_edge_cases() {
    assert!(convert_ttml_to_elrc("no tags here").is_none()); // no <span/begin
    assert!(convert_ttml_to_elrc("<p begin='1'>empty</p>").is_none()); // no spans output
    let entities = r#"<p begin="1"><span begin="1">&quot;q&quot; &#39;s</span></p>"#;
    assert_eq!(
        convert_ttml_to_elrc(entities).unwrap(),
        "[00:01.000]<00:01.000>\"q\" 's"
    );
}

#[test]
fn scored_word_synced_document_contains_structured_timing() {
    let candidate = score_lyrics(
        "[00:10.500]<00:10.500>first <00:11.200>word\n[01:02.000]<01:02.250>next",
        "provider",
        30,
    );
    assert_eq!(candidate.tier(), LyricsTier::WordSynced);
    let timing = candidate.document.word_timing.as_ref().unwrap();
    assert_eq!(timing[0].start_ms, 10_500);
    assert_eq!(timing[0].words[1].start_ms, 11_200);
    assert_eq!(timing[0].words[1].text, "word");
    assert_eq!(candidate.text(), candidate.document.text);
}

#[cfg(feature = "paxsenix")]
#[tokio::test]
async fn paxsenix_provider_parsing() {
    let body =
        r#"{"elrc":"[00:01.00]<00:01.10>a\n[00:02.00]b","lrc":"","plain":"","ttmlContent":null}"#;
    let http = FakeHttp::with(&[("paxsenix.org", body)]);
    let meta = LyricsLookup {
        title: "T".into(),
        artists: vec!["A".into()],
        provider_ids: [("apple".into(), "123".into())].into_iter().collect(),
        album: None,
        duration: None,
    };
    let result = lookup(&http, &LyricsRegistry::all_sources(), &meta).await;
    assert_eq!(
        result.unwrap(),
        "[00:01.00]<00:01.10>a\n[00:02.00]b",
        "ELRC (weight 30, tier 1000) beats everything; empty lrc/plain skipped"
    );
}

#[cfg(feature = "betterlyrics")]
#[tokio::test]
async fn better_lyrics_ttml_conversion_wins() {
    let ttml =
        r#"<p begin="1"><span begin="1">hey</span></p><p begin="2"><span begin="2">you</span></p>"#;
    let body = format!(r#"{{"ttml":{}}}"#, serde_json::json!(ttml));
    let http = FakeHttp::with(&[("boidu.dev", &body), ("lrclib.net", "{}")]);
    let meta = LyricsLookup {
        title: "T".into(),
        artists: vec!["A".into()],
        provider_ids: [("apple".into(), "123".into())].into_iter().collect(),
        album: None,
        duration: None,
    };
    let result = lookup(&http, &LyricsRegistry::all_sources(), &meta).await;
    assert!(result.unwrap().starts_with("[00:01.000]<00:01.000>hey"));
}

#[cfg(feature = "lrclib")]
#[tokio::test]
async fn lrclib_exact_param_building() {
    let body = r#"{"syncedLyrics":"[00:01.00]x\n[00:02.00]y","plainLyrics":"x\ny"}"#;
    // Route key asserts the exact query string for the get endpoint.
    let http = FakeHttp::with(&[(
        "lrclib.net/api/get?artist_name=A+artist&track_name=T+title&album_name=Al&duration=200",
        body,
    )]);
    let meta = LyricsLookup {
        title: "T title".into(),
        artists: vec!["A artist".into()],
        provider_ids: [("apple".into(), "123".into())].into_iter().collect(),
        album: Some("Al".into()),
        duration: Some(200),
    };
    let result = lookup(&http, &LyricsRegistry::all_sources(), &meta).await;
    assert_eq!(result.unwrap(), "[00:01.00]x\n[00:02.00]y");
}

#[cfg(feature = "lrclib")]
#[tokio::test]
async fn lrclib_search_duration_sort_and_form_encoding() {
    let body = r#"[
        {"syncedLyrics":"[00:01.00]far\n[00:02.00]away","duration":300},
        {"syncedLyrics":"[00:01.00]near\n[00:02.00]by","duration":201}
    ]"#;
    let http = FakeHttp::with(&[("lrclib.net/api/search?q=T+A", body)]);
    let meta = LyricsLookup {
        title: "T".into(),
        artists: vec!["A".into()],
        provider_ids: [("apple".into(), "123".into())].into_iter().collect(),
        album: None,
        duration: Some(200),
    };
    let result = lookup(&http, &LyricsRegistry::all_sources(), &meta).await;
    // duration 201 is closest to 200 → "near by" wins.
    assert_eq!(result.unwrap(), "[00:01.00]near\n[00:02.00]by");
}

#[cfg(any(feature = "lrclib", feature = "betterlyrics", feature = "paxsenix"))]
#[tokio::test]
async fn all_providers_fail_returns_none() {
    let http = FakeHttp::with(&[]);
    let meta = LyricsLookup {
        title: "T".into(),
        artists: vec!["A".into()],
        provider_ids: [("apple".into(), "123".into())].into_iter().collect(),
        album: None,
        duration: None,
    };
    assert!(
        lookup(&http, &LyricsRegistry::all_sources(), &meta)
            .await
            .is_none()
    );
}

#[cfg(all(feature = "lrclib", feature = "paxsenix"))]
#[tokio::test]
async fn stable_score_ordering_tie() {
    // Two providers with identical score: first in provider order wins
    // (paxsenix candidates come before lrclib ones).
    let body = r#"{"plain":"first provider text\nwith two lines"}"#;
    let http = FakeHttp::with(&[
        ("paxsenix.org", body),
        (
            "lrclib.net/api/get",
            r#"{"plainLyrics":"second provider text\nwith two lines"}"#,
        ),
    ]);
    let meta = LyricsLookup {
        title: "T".into(),
        artists: vec!["A".into()],
        provider_ids: [("apple".into(), "123".into())].into_iter().collect(),
        album: None,
        duration: None,
    };
    let result = lookup(&http, &LyricsRegistry::all_sources(), &meta).await;
    assert_eq!(result.unwrap(), "first provider text\nwith two lines");
}

#[cfg(feature = "lrclib")]
#[test]
fn default_registry_contains_only_lrclib() {
    let registry = LyricsRegistry::default();
    assert_eq!(registry.source_ids().collect::<Vec<_>>(), ["lrclib"]);
}

#[cfg(all(feature = "lrclib", feature = "betterlyrics", feature = "paxsenix"))]
#[test]
fn all_sources_preserves_explicit_source_order() {
    let registry = LyricsRegistry::all_sources();
    assert_eq!(
        registry.source_ids().collect::<Vec<_>>(),
        ["paxsenix", "betterlyrics", "lrclib"]
    );
}

#[cfg(feature = "lrclib")]
#[tokio::test]
async fn ranked_result_exposes_source_and_rights_metadata() {
    let body = r#"{"syncedLyrics":"[00:01.00]line one\n[00:02.00]line two","license":"CC-BY-4.0","attribution":"LRCLIB"}"#;
    let http = FakeHttp::with(&[("lrclib.net/api/get", body)]);
    let input = LyricsLookup::new("T", ["A"]).with_provider_id("catalog", "42");
    let candidates = lyrics::lookup_ranked(&http, &LyricsRegistry::default(), &input).await;
    let candidate = candidates.first().unwrap();
    assert_eq!(candidate.source_id(), "lrclib");
    assert!(
        candidate
            .source_url()
            .unwrap()
            .contains("lrclib.net/api/get")
    );
    assert_eq!(candidate.rights().license, None);
    assert_eq!(candidate.provider(), "LRCLIB Exact (LRC)");
    assert_eq!(candidate.document.source.id, "lrclib");
}
