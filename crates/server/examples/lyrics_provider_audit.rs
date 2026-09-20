use std::{collections::HashSet, time::Duration};

use futures_util::future::join_all;
use lyrics::{LyricsCandidate, LyricsFuture, LyricsHttp, LyricsLookup, LyricsRegistry, LyricsTier};

struct ProbeHttp(reqwest::Client);

impl LyricsHttp for ProbeHttp {
    fn get_json<'a>(&'a self, url: &'a str) -> LyricsFuture<'a, Option<String>> {
        Box::pin(async move {
            let response = self.0.get(url).send().await.ok()?;
            if !response.status().is_success() {
                return None;
            }
            response.text().await.ok()
        })
    }

    fn get_with_headers<'a>(
        &'a self,
        url: &'a str,
        headers: &'a [(&'a str, &'a str)],
    ) -> LyricsFuture<'a, Option<String>> {
        Box::pin(async move {
            let mut request = self.0.get(url);
            for (name, value) in headers {
                request = request.header(*name, *value);
            }
            let response = request.send().await.ok()?;
            if !response.status().is_success() {
                return None;
            }
            response.text().await.ok()
        })
    }
}

fn lyric_tokens(candidate: &LyricsCandidate) -> HashSet<String> {
    candidate
        .document
        .text
        .split(|character: char| !character.is_alphanumeric())
        .filter(|token| token.chars().any(char::is_alphabetic))
        .map(str::to_lowercase)
        .collect()
}

fn overlap_percent(left: &HashSet<String>, right: &HashSet<String>) -> u32 {
    let union = left.union(right).count();
    if union == 0 {
        return 0;
    }
    ((left.intersection(right).count() * 100) / union) as u32
}

fn script_counts(candidate: &LyricsCandidate) -> (usize, usize, usize) {
    let mut latin = 0;
    let mut devanagari = 0;
    let mut other = 0;
    for character in candidate
        .document
        .text
        .chars()
        .filter(|character| character.is_alphabetic())
    {
        if character.is_ascii_alphabetic() {
            latin += 1;
        } else if ('\u{0900}'..='\u{097f}').contains(&character) {
            devanagari += 1;
        } else {
            other += 1;
        }
    }
    (latin, devanagari, other)
}

fn screenshot_anchor_matches(candidate: &LyricsCandidate) -> (usize, usize) {
    let text = &candidate.document.text;
    let tokens = lyric_tokens(candidate);
    let devanagari = ["महबूबा", "गुलशन", "सहरा"]
        .iter()
        .filter(|anchor| text.contains(**anchor))
        .count();
    let latin = ["mehbooba", "gulshan", "sehra"]
        .iter()
        .filter(|anchor| tokens.contains(**anchor))
        .count();
    (devanagari, latin)
}

fn timing_counts(candidate: &LyricsCandidate) -> (usize, usize, usize) {
    candidate
        .document
        .word_timing
        .as_ref()
        .map(|lines| {
            (
                lines.len(),
                lines.iter().map(|line| line.words.len()).sum(),
                lines.iter().map(|line| line.background_words.len()).sum(),
            )
        })
        .unwrap_or_default()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Track: Mehbooba Mehbooba — R.D. Burman");

    let http = ProbeHttp(
        reqwest::Client::builder()
            .timeout(Duration::from_secs(20))
            .build()?,
    );
    let lookup = LyricsLookup::new("Mehbooba Mehbooba", ["R.D. Burman"])
        .with_album("Sholay (Original Motion Picture Soundtrack)")
        .with_duration(234);
    let registry = LyricsRegistry::all_sources();
    let provider_results = join_all(registry.sources().map(|source| async {
        let source_id = source.id().to_owned();
        let candidates = source.lookup(&http, &lookup).await;
        (source_id, candidates)
    }))
    .await;

    println!("\nPer-provider results:");
    for (source_id, candidates) in &provider_results {
        if candidates.is_empty() {
            println!("  {source_id}: no candidates");
            continue;
        }
        for (index, candidate) in candidates.iter().enumerate() {
            let (timed_lines, words, background_words) = timing_counts(candidate);
            let (latin, devanagari, other) = script_counts(candidate);
            let (devanagari_anchors, latin_anchors) = screenshot_anchor_matches(candidate);
            println!(
                "  {source_id}[{}]: score={} tier={:?} provider={} format={:?} timed_lines={} words={} background_words={} scripts=latin:{latin},devanagari:{devanagari},other:{other} screenshot_anchors=dev:{devanagari_anchors}/3,latin:{latin_anchors}/3 chars={}",
                index + 1,
                candidate.score(),
                candidate.tier(),
                candidate.provider(),
                candidate.document.format,
                timed_lines,
                words,
                background_words,
                candidate.document.text.chars().count(),
            );
        }
    }

    let mut ranked = provider_results
        .iter()
        .flat_map(|(source_id, candidates)| {
            candidates
                .iter()
                .filter(|candidate| candidate.tier() != LyricsTier::None && candidate.score() > 0)
                .map(move |candidate| (source_id.as_str(), candidate))
        })
        .collect::<Vec<_>>();
    ranked.sort_by_key(|(_, candidate)| std::cmp::Reverse(candidate.score()));

    println!("\nGlobal ranking (same score sort as lookup_ranked):");
    let Some((_, best)) = ranked.first() else {
        println!("  no usable candidates");
        return Ok(());
    };
    let reference_tokens = lyric_tokens(best);
    for (index, (source_id, candidate)) in ranked.iter().enumerate() {
        let (timed_lines, words, background_words) = timing_counts(candidate);
        let tokens = lyric_tokens(candidate);
        let (latin, devanagari, other) = script_counts(candidate);
        let (devanagari_anchors, latin_anchors) = screenshot_anchor_matches(candidate);
        println!(
            "  rank={} score={} tier={:?} source={} provider={} format={:?} timed_lines={} words={} background_words={} scripts=latin:{latin},devanagari:{devanagari},other:{other} screenshot_anchors=dev:{devanagari_anchors}/3,latin:{latin_anchors}/3 unique_tokens={} overlap_with_top={}%",
            index + 1,
            candidate.score(),
            candidate.tier(),
            source_id,
            candidate.provider(),
            candidate.document.format,
            timed_lines,
            words,
            background_words,
            tokens.len(),
            overlap_percent(&reference_tokens, &tokens),
        );
    }
    Ok(())
}
