//! `/dump` — on-demand + scheduled new-release archiver.
//! Discovers fresh tracks
//! across the configured storefronts ("New Music Daily" editorial playlist +
//! the Apple Marketing Tools top-albums feed, each album resolved to tracks
//! via iTunes lookup) and enqueues them through the collapsed orchestrator in
//! cache-only dump mode.

use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use engine::types::{ParsedTargetItem, TargetKind};
use ferogram::{
    InputMessage, PeerRef,
    filters::{self, Dispatcher},
    update::IncomingMessage,
};
use serde_json::Value;

use crate::{BotState, html::parse_dynamic_html};

/// Re-entry guard .
static AUTO_DUMP_RUNNING: AtomicBool = AtomicBool::new(false);

/// The "New Music Daily" editorial playlist id.
const NEW_MUSIC_DAILY_ID: &str = "pl.2b0e6e332fdf4b7a91164da3162127b5";

/// Cutoff date: today − max(1, days), formatted `YYYY-MM-DD`.
fn cutoff_date_string(days: i64) -> String {
    let days = days.max(1);
    (chrono::Utc::now() - chrono::Duration::days(days))
        .format("%Y-%m-%d")
        .to_string()
}

/// parseInt-style prefix parsing: leading ASCII digits only.
fn parse_days_prefix(raw: &str) -> Option<i64> {
    let digits: String = raw
        .chars()
        .take_while(|c| c.is_ascii_digit() || *c == '+' || *c == '-')
        .collect();
    if digits.is_empty() {
        return None;
    }
    digits.parse().ok()
}

/// Discovered track .
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoveredTrackItem {
    pub id: String,
    pub storefront: String,
}

/// Discovery outcome .
#[derive(Debug, Default)]
pub struct DiscoveredNewTracksResult {
    pub tracks: Vec<DiscoveredTrackItem>,
    pub storefront_counts: HashMap<String, usize>,
    pub cutoff_date_string: String,
    pub total_found: usize,
}

/// Source 1: the New Music Daily editorial playlist via the Catalog API with
/// the scraped developer token (errors logged and skipped).
async fn discover_from_playlist(
    http: &reqwest::Client,
    dev_token: &str,
    storefront: &str,
    cutoff: &str,
    map: &mut HashMap<String, String>,
    counts: &mut HashMap<String, usize>,
) {
    let url = format!(
        "https://api.music.apple.com/v1/catalog/{storefront}/playlists/{NEW_MUSIC_DAILY_ID}?include=tracks"
    );
    let Ok(response) = http
        .get(&url)
        .header("Authorization", format!("Bearer {dev_token}"))
        .header("Origin", "https://music.apple.com")
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await
    else {
        return;
    };
    if !response.status().is_success() {
        return;
    }
    let Ok(body) = response.text().await else {
        return;
    };
    let Ok(body) = serde_json::from_str::<Value>(&body) else {
        return;
    };
    let tracks = body
        .get("data")
        .and_then(Value::as_array)
        .and_then(|data| data.first())
        .and_then(|first| first.get("relationships"))
        .and_then(|rel| rel.get("tracks"))
        .and_then(|tracks| tracks.get("data"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    for track in tracks {
        let Some(id) = track.get("id").and_then(Value::as_str) else {
            continue;
        };
        let release_date = track
            .get("attributes")
            .and_then(|attributes| attributes.get("releaseDate"))
            .and_then(Value::as_str)
            .unwrap_or_default();
        if !id.is_empty() && !release_date.is_empty() && release_date >= cutoff {
            let inserted = map.insert(id.to_owned(), storefront.to_owned()).is_none();
            if inserted {
                *counts.entry(storefront.to_owned()).or_insert(0) += 1;
            }
        }
    }
}

/// Per-album iTunes lookup .
async fn discover_album_tracks_via_itunes(
    http: &reqwest::Client,
    storefront: &str,
    album_id: &str,
    album_release: &str,
    cutoff: &str,
    map: &mut HashMap<String, String>,
    counts: &mut HashMap<String, usize>,
) {
    let Ok(tracks) = apple::catalog::fetch_discovery_album_tracks(http, storefront, album_id).await
    else {
        return;
    };
    for track in tracks {
        let track_release = track.release_date;
        let qualifies =
            track_release.is_empty() || track_release.as_str() >= cutoff || album_release >= cutoff;
        if qualifies {
            let id = track.id;
            let inserted = map.insert(id, storefront.to_owned()).is_none();
            if inserted {
                *counts.entry(storefront.to_owned()).or_insert(0) += 1;
            }
        }
    }
}

/// Source 2: Marketing Tools top-albums feed, tracks resolved via iTunes
/// lookup (errors logged and skipped per album).
async fn discover_from_rss_albums(
    http: &reqwest::Client,
    storefront: &str,
    cutoff: &str,
    map: &mut HashMap<String, String>,
    counts: &mut HashMap<String, usize>,
) {
    let Ok(albums) = apple::catalog::fetch_marketing_tools_albums(http, storefront, 50).await
    else {
        return;
    };
    for album in albums {
        if album.id.is_empty() {
            continue;
        }
        let album_release = album.release_date.as_deref().unwrap_or_default();
        if album_release >= cutoff {
            discover_album_tracks_via_itunes(
                http,
                storefront,
                &album.id,
                album_release,
                cutoff,
                map,
                counts,
            )
            .await;
        }
    }
}

/// Discover tracks newer than the cutoff across the configured storefronts.
pub async fn discover_new_tracks(
    state: &BotState,
    storefronts: &[String],
    days: i64,
) -> DiscoveredNewTracksResult {
    let cutoff = cutoff_date_string(days);
    let mut map: HashMap<String, String> = HashMap::new();
    let mut counts: HashMap<String, usize> = HashMap::new();

    let dev_token = match state.rip_deps.playlist().get_developer_token().await {
        Ok(token) => token,
        Err(error) => {
            tracing::warn!(%error, "Apple Music developer token unavailable; skipping release discovery");
            return DiscoveredNewTracksResult::default();
        }
    };
    let http = reqwest::Client::new();

    for storefront in storefronts {
        counts.entry(storefront.clone()).or_insert(0);
        discover_from_playlist(
            &http,
            &dev_token,
            storefront,
            &cutoff,
            &mut map,
            &mut counts,
        )
        .await;
        discover_from_rss_albums(&http, storefront, &cutoff, &mut map, &mut counts).await;
    }

    let tracks = map
        .into_iter()
        .map(|(id, storefront)| DiscoveredTrackItem { id, storefront })
        .collect::<Vec<_>>();
    let total_found = tracks.len();
    tracing::info!(
        days,
        cutoff_date = %cutoff,
        total_found,
        "Autodump discovery completed"
    );
    DiscoveredNewTracksResult {
        total_found,
        cutoff_date_string: cutoff,
        tracks,
        storefront_counts: counts,
    }
}

/// Run the discovery → report → reply pipeline. Returns whether every
/// stage succeeded.
pub async fn run_auto_dump_pipeline(
    state: Arc<BotState>,
    days: i64,
    triggered_by: &str,
    chat_id: Option<i64>,
    user_id: Option<i64>,
) -> bool {
    let target_chat = chat_id.unwrap_or(state.admin_id);
    let target_user = user_id.unwrap_or(state.admin_id);

    if AUTO_DUMP_RUNNING
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_err()
    {
        tracing::warn!(
            triggered_by,
            "Auto-dump pipeline already running, skipping new trigger"
        );
        return false;
    }
    let result = run_auto_dump_pipeline_inner(
        Arc::clone(&state),
        days,
        triggered_by,
        target_chat,
        target_user,
    )
    .await;
    AUTO_DUMP_RUNNING.store(false, Ordering::SeqCst);
    result
}

async fn run_auto_dump_pipeline_inner(
    state: Arc<BotState>,
    days: i64,
    triggered_by: &str,
    target_chat: i64,
    target_user: i64,
) -> bool {
    let storefronts = state
        .rip_deps
        .settings_snapshot()
        .auto_dump_storefronts
        .clone();
    let discovery = discover_new_tracks(&state, &storefronts, days).await;

    if discovery.total_found == 0 {
        let _ = state
            .client
            .send_message(
                PeerRef::from(state.admin_id),
                InputMessage::html(parse_dynamic_html(&format!(
                    "<b>Dump complete</b><br/>No new tracks found in the last <code>{days}</code> day(s)."
                ))),
            )
            .await;
        return true;
    }

    // Discovery has completed; all live rip progress is rendered by the
    // shared dashboard in the target chat.
    super::ensure_dashboard(
        &state,
        target_chat,
        target_user,
        true,
        PeerRef::from(target_chat),
    )
    .await;

    let options = engine::orchestrator::types::RipJobOptions {
        provider: engine::Provider::Apple,
        chat_id: target_chat,
        user_id: target_user,
        user_name: Some(format!("User {target_user}")),
        delivery_chat_id: target_chat,
        is_group: target_chat != target_user,
        is_force: false,
        is_cache_only: true,
        single_storefront: None,
        parsed_items: discovery
            .tracks
            .iter()
            .map(|track| ParsedTargetItem {
                id: track.id.clone(),
                kind: TargetKind::Track,
                storefront: Some(track.storefront.clone()),
            })
            .collect(),
        reply_to_message_id: None,
        status_msg_id: 0,
        is_admin: true,
        codec_preference: None,
        rendition_policy: engine::orchestrator::types::RenditionPolicy::PrimaryOnly,
    };
    let job = state
        .rip_orchestrator
        .start_job(Arc::clone(&state.rip_deps), &options)
        .await;

    match job {
        Ok(_) => {
            // The iterates `Object.entries(storefrontCounts)`, which
            // preserves the configured storefront order (every storefront is
            // seeded with 0 before discovery).
            let sf_breakdown = storefronts
                .iter()
                .map(|sf| {
                    format!(
                        "• <b>{}:</b> <code>{}</code>",
                        sf.to_uppercase(),
                        discovery.storefront_counts.get(sf).copied().unwrap_or(0)
                    )
                })
                .collect::<Vec<_>>()
                .join("<br/>");
            let summary_html = format!(
                "📦 <b>Dump Run Completed</b><br/><br/>\
<blockquote>• <b>Total Discovered:</b> <code>{} tracks</code><br/>\
• <b>Time Window:</b> Last <code>{days}</code> day(s)<br/>\
• <b>Trigger:</b> {triggered_by}<br/><br/>\
<b>Storefront Breakdown:</b><br/>{sf_breakdown}</blockquote><br/>\
<blockquote><i>All new tracks have been archived to the dump channel.</i></blockquote>",
                discovery.total_found
            );
            let _ = state
                .client
                .send_message(
                    PeerRef::from(state.admin_id),
                    InputMessage::html(parse_dynamic_html(&summary_html)),
                )
                .await;
            true
        }
        Err(error) => {
            tracing::error!(%error, "Auto-dump pipeline execution failed");
            let _ = state
                .client
                .send_message(
                    PeerRef::from(state.admin_id),
                    InputMessage::html(parse_dynamic_html(
                        "<b>Dump failed.</b><br/>The dashboard contains the job outcome.",
                    )),
                )
                .await;
            false
        }
    }
}

pub fn register(dp: &mut Dispatcher, state: Arc<BotState>) {
    let state_dump = Arc::clone(&state);
    dp.on_message(filters::command("dump"), move |msg| {
        handle_dump_command(Arc::clone(&state_dump), msg)
    });
}

async fn handle_dump_command(state: Arc<BotState>, msg: IncomingMessage) {
    let sender = msg.sender_user_id().unwrap_or_default();
    if !state.auth.is_admin(sender) {
        tracing::debug!(user_id = sender, "Non-admin attempted dump command");
        return;
    }

    let tokens: Vec<&str> = msg.text().unwrap_or_default().split_whitespace().collect();
    let mut days: i64 = 1;
    if let Some(raw) = tokens.get(1)
        && let Some(parsed) = parse_days_prefix(raw)
        && parsed > 0
    {
        days = parsed;
    }

    if AUTO_DUMP_RUNNING.load(Ordering::SeqCst) {
        let _ = msg
            .reply(InputMessage::html(parse_dynamic_html(
                "… <b>Dump is already in progress.</b> Please wait for the current sweep to finish.",
            )))
            .await;
        return;
    }

    let marked_chat = super::marked_chat_id(&msg);
    run_auto_dump_pipeline(
        state,
        days,
        &format!("Admin Command (/dump {days})"),
        Some(marked_chat),
        Some(sender),
    )
    .await;
}

/// startAutoDumpScheduler: a 24h interval, settings-gated per tick,
/// days = 1, triggeredBy `Daily 24h Scheduler`. Spawned once from `main`.
pub async fn scheduler_loop(state: Arc<BotState>) {
    tracing::info!("Starting 24h auto-dump scheduler");
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(24 * 60 * 60));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // `setInterval` semantics: the first tick fires after the full interval,
    // but `tokio::time::interval` fires immediately — consume it.
    interval.tick().await;
    loop {
        interval.tick().await;
        if !state.rip_deps.settings_snapshot().auto_dump_enabled {
            tracing::debug!("Auto-dump scheduler tick skipped: disabled in settings");
            continue;
        }
        tracing::info!("Executing scheduled daily auto-dump sweep");
        let success =
            run_auto_dump_pipeline(state.clone(), 1, "Daily 24h Scheduler", None, None).await;
        if !success {
            tracing::error!("Scheduled auto-dump sweep encountered error");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cutoff_dates_have_expected_shape() {
        assert_eq!(cutoff_date_string(1).len(), 10);
        assert!(cutoff_date_string(1).starts_with("20"));
        assert!(cutoff_date_string(1).contains('-'));
        // max(1, days): zero and negative clamp to 1.
        assert_eq!(cutoff_date_string(0), cutoff_date_string(1));
        assert_eq!(cutoff_date_string(-5), cutoff_date_string(1));
        assert_eq!(cutoff_date_string(3), cutoff_date_string(3));
    }

    #[test]
    fn days_parsing_matches_js_parseint() {
        assert_eq!(parse_days_prefix("7"), Some(7));
        assert_eq!(parse_days_prefix("7abc"), Some(7));
        assert_eq!(parse_days_prefix("+7"), Some(7));
        assert_eq!(parse_days_prefix("-5"), Some(-5));
        assert_eq!(parse_days_prefix("0"), Some(0));
        assert_eq!(parse_days_prefix(""), None);
        assert_eq!(parse_days_prefix("abc"), None);
        // Command gate: only positive values survive.
        let days = |raw: &str| {
            let parsed = parse_days_prefix(raw);
            match parsed {
                Some(n) if n > 0 => n,
                _ => 1,
            }
        };
        assert_eq!(days("7abc"), 7);
        assert_eq!(days("-5"), 1);
        assert_eq!(days("0"), 1);
        assert_eq!(days("abc"), 1);
    }

    #[test]
    fn initial_and_progress_cards_are_exact() {
        let initial = format!(
            "… <b>Scanning Apple Music for new releases</b><br/><br/>\
<blockquote>• <b>Time Window:</b> Last <code>{days}</code> day(s)<br/>\
• <b>Storefronts:</b> <code>{sf_formatted}</code><br/>\
• <b>Triggered By:</b> {triggered_by}<br/>\
• <b>Mode:</b> Dump Channel Archiver (Cache-Only)</blockquote>",
            days = 3,
            sf_formatted = "US, GB",
            triggered_by = "Admin Command (/dump 3)",
        );
        assert_eq!(
            initial,
            "… <b>Scanning Apple Music for new releases</b><br/><br/>\
<blockquote>• <b>Time Window:</b> Last <code>3</code> day(s)<br/>\
• <b>Storefronts:</b> <code>US, GB</code><br/>\
• <b>Triggered By:</b> Admin Command (/dump 3)<br/>\
• <b>Mode:</b> Dump Channel Archiver (Cache-Only)</blockquote>"
        );

        let empty = format!(
            "✨ <b>Dump Finished: No New Tracks Found</b><br/><br/>\
<blockquote>• <b>Time Window:</b> Last <code>{days}</code> day(s)<br/>\
• <b>Cutoff Date:</b> <code>{cutoff}</code><br/>\
• <b>Storefronts Scanned:</b> <code>{sf}</code><br/>\
• <b>Discovered:</b> <code>0 tracks</code></blockquote>",
            days = 1,
            cutoff = "2026-09-08",
            sf = "US"
        );
        assert_eq!(
            empty,
            "✨ <b>Dump Finished: No New Tracks Found</b><br/><br/>\
<blockquote>• <b>Time Window:</b> Last <code>1</code> day(s)<br/>\
• <b>Cutoff Date:</b> <code>2026-09-08</code><br/>\
• <b>Storefronts Scanned:</b> <code>US</code><br/>\
• <b>Discovered:</b> <code>0 tracks</code></blockquote>"
        );
    }

    #[test]
    fn summary_card_is_exact() {
        let storefronts = ["us".to_owned(), "gb".to_owned()];
        let mut counts = HashMap::new();
        counts.insert("us".to_owned(), 4);
        counts.insert("gb".to_owned(), 2);
        let sf_breakdown = storefronts
            .iter()
            .map(|sf| {
                format!(
                    "• <b>{}:</b> <code>{}</code>",
                    sf.to_uppercase(),
                    counts.get(sf).copied().unwrap_or(0)
                )
            })
            .collect::<Vec<_>>()
            .join("<br/>");
        let summary = format!(
            "📦 <b>Dump Run Completed</b><br/><br/>\
<blockquote>• <b>Total Discovered:</b> <code>{} tracks</code><br/>\
• <b>Time Window:</b> Last <code>{days}</code> day(s)<br/>\
• <b>Trigger:</b> {triggered_by}<br/><br/>\
<b>Storefront Breakdown:</b><br/>{sf_breakdown}</blockquote><br/>\
<blockquote><i>All new tracks have been archived to the dump channel.</i></blockquote>",
            6,
            days = 1,
            triggered_by = "Daily 24h Scheduler",
        );
        assert!(summary.starts_with(
            "📦 <b>Dump Run Completed</b><br/><br/>\
<blockquote>• <b>Total Discovered:</b> <code>6 tracks</code><br/>\
• <b>Time Window:</b> Last <code>1</code> day(s)<br/>\
• <b>Trigger:</b> Daily 24h Scheduler<br/><br/>\
<b>Storefront Breakdown:</b><br/>"
        ));
        assert!(summary.contains("• <b>US:</b> <code>4</code>"));
        assert!(summary.contains("• <b>GB:</b> <code>2</code>"));
        assert!(summary.ends_with(
            "</blockquote><br/>\
<blockquote><i>All new tracks have been archived to the dump channel.</i></blockquote>"
        ));
    }

    #[test]
    fn discovery_dedupes_and_counts_per_storefront() {
        let mut map = HashMap::new();
        let mut counts = HashMap::new();
        let mut first = HashMap::new();
        let mut first_counts = HashMap::new();
        discover_from_playlist_insert(&mut first, &mut first_counts, "1", "us");
        let mut second = HashMap::new();
        let mut second_counts = HashMap::new();
        discover_from_playlist_insert(&mut second, &mut second_counts, "1", "gb");
        map.extend(first);
        counts.extend(first_counts);
        for (id, sf) in second {
            map.entry(id).or_insert(sf);
        }
        assert_eq!(map.get("1"), Some(&"us".to_owned()));
    }

    fn discover_from_playlist_insert(
        map: &mut HashMap<String, String>,
        counts: &mut HashMap<String, usize>,
        id: &str,
        storefront: &str,
    ) {
        if map.insert(id.to_owned(), storefront.to_owned()).is_none() {
            *counts.entry(storefront.to_owned()).or_insert(0) += 1;
        }
    }
}
