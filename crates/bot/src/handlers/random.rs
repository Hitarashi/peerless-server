//! `/random` — random album explorer : discovery sources, preview cards,
//! and `random:*` callbacks. Admin-only (the replies with a restricted
//! card rather than staying silent).

use std::sync::Arc;

use engine::{
    Provider,
    types::{ParsedTargetItem, TargetKind, TrackKey},
};
use ferogram::{
    InputMessage, PeerRef,
    filters::{self, Dispatcher},
    keyboard::{Button, InlineKeyboard},
    update::{CallbackQuery, IncomingMessage},
};

use crate::{
    BotState,
    html::{escape, parse_dynamic_html},
    interaction::DiscoveryAction,
};

/// WILD_SEEDS (50 words).
pub const WILD_SEEDS: &[&str] = &[
    "future",
    "midnight",
    "electric",
    "dream",
    "sunset",
    "horizon",
    "velvet",
    "echo",
    "shadow",
    "crystal",
    "neon",
    "ocean",
    "silver",
    "aurora",
    "cosmic",
    "paradise",
    "vintage",
    "solitude",
    "infinite",
    "rhythm",
    "harmony",
    "odyssey",
    "mirage",
    "serenade",
    "astral",
    "stellar",
    "cascade",
    "monochrome",
    "sanctuary",
    "voyage",
    "illusions",
    "phantom",
    "solstice",
    "vortex",
    "genesis",
    "celestial",
    "timeless",
    "radiant",
    "nostalgia",
    "euphoria",
    "zenith",
    "spectrum",
    "whisper",
    "pulse",
    "nebula",
    "labyrinth",
    "reverie",
    "resonance",
    "destiny",
    "memory",
];

/// Curated search seeds covering the Indian subcontinent:
/// India, Pakistan, Bangladesh, Afghanistan, Nepal, Bhutan, Sri Lanka.
pub const SOUTH_ASIA_SEEDS: &[&str] = &[
    // India (Bollywood, regional cinema, classical, indie, folk)
    "bollywood",
    "hindi album",
    "punjabi album",
    "tamil album",
    "telugu album",
    "malayalam album",
    "kannada album",
    "desi hip hop",
    "indian indie",
    "ghazal",
    "indian classical",
    "sufi",
    // Pakistan (Coke Studio, Qawwali, Pakistani pop/rock, Urdu)
    "pakistani pop",
    "pakistani rock",
    "coke studio",
    "qawwali",
    "sufi music",
    "urdu album",
    // Bangladesh (Bangla rock, Rabindra Sangeet, Bengali modern/folk)
    "bangla rock",
    "bengali album",
    "rabindra sangeet",
    "bangladesh music",
    // Afghanistan (Afghan folk, Pashto, Rubab)
    "afghan music",
    "pashto songs",
    // Nepal & Bhutan
    "nepali album",
    "nepali folk",
    "bhutanese music",
];

pub fn is_south_asia_source(source: &str) -> bool {
    matches!(
        source.to_lowercase().as_str(),
        "southasia"
            | "subcontinent"
            | "desi"
            | "india"
            | "pakistan"
            | "bangladesh"
            | "nepal"
            | "bhutan"
    )
}

/// SOURCE_LABELS.
pub fn source_label(source: &str) -> Option<&'static str> {
    Some(match source {
        "charts" => "Top charts",
        "wild" => "Wild search",
        s if is_south_asia_source(s) => "South Asia",
        "rock" => "Rock",
        "hiphop" => "Hip-hop",
        "pop" => "Pop",
        "electronic" => "Electronic",
        "jazz" => "Jazz",
        "indie" => "Indie",
        _ => return None,
    })
}

/// Fixed search term per source; `None` means the source takes a
/// different path (charts/wild) or is used as a raw query.
fn source_search_term(source: &str) -> Option<&'static str> {
    Some(match source {
        "rock" => "rock album",
        "hiphop" | "rap" => "hip hop album",
        "pop" => "pop album",
        "electronic" | "edm" => "electronic album",
        "jazz" => "jazz album",
        "indie" => "indie album",
        _ => return None,
    })
}

/// A discovered album candidate .
#[derive(Debug, Clone, Default)]
pub struct RandomAlbumCandidate {
    pub id: String,
    pub title: String,
    pub artist: String,
    pub url: String,
    pub release_date: Option<String>,
    pub genre: Option<String>,
    pub track_count: Option<usize>,
    pub storefront: String,
}

/// Tiny xorshift PRNG — seeded from the clock once, tested for range.
fn pick_index(rng: &mut u64, len: usize) -> usize {
    if len == 0 {
        return 0;
    }
    *rng ^= *rng << 13;
    *rng ^= *rng >> 7;
    *rng ^= *rng << 17;
    (*rng % len as u64) as usize
}

fn now_seed() -> u64 {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0x9E3779B97F4A7C15);
    nanos | 1
}

/// buildSourcesMenuText.
pub fn build_sources_menu_text() -> String {
    "<b>Apple Music random album explorer</b> (admin)<br/><br/>\
     <blockquote>Select a discovery source below to pick a random album to dump into your cache channel:</blockquote>"
        .to_owned()
}

/// buildPreviewText.
pub fn build_preview_text(candidate: &RandomAlbumCandidate) -> String {
    let release = candidate
        .release_date
        .as_deref()
        .map(|date| date.split('T').next().unwrap_or(date))
        .unwrap_or("Unknown");
    let tracks = candidate
        .track_count
        .map(|n| format!("{n} tracks"))
        .unwrap_or_else(|| "Full Album".to_owned());
    format!(
        "✓ <b>Random album selected</b><br/><br/>\
<b>Album:</b> {}<br/>\
<b>Artist:</b> {}<br/>\
<b>Tracks:</b> <code>{}</code><br/>\
<b>Released:</b> <code>{}</code><br/>\
<b>Genre:</b> <code>{}</code><br/>\
<b>Storefront:</b> <code>{}</code><br/><br/>\
<a href=\"{}\">Open in Apple Music</a>",
        escape(&candidate.title),
        escape(&candidate.artist),
        escape(&tracks),
        escape(release),
        escape(candidate.genre.as_deref().unwrap_or("Music")),
        escape(candidate.storefront.to_uppercase().as_str()),
        escape(&candidate.url),
    )
}

fn sources_keyboard() -> ferogram::tl::enums::ReplyMarkup {
    InlineKeyboard::new()
        .row(vec![
            Button::callback("Top charts", b"random:src:charts"),
            Button::callback("Wild search", b"random:src:wild"),
        ])
        .row(vec![
            Button::callback("🌏 South Asia", b"random:src:southasia:in"),
            Button::callback("Rock", b"random:src:rock"),
            Button::callback("Hip-hop", b"random:src:hiphop"),
        ])
        .row(vec![
            Button::callback("Pop", b"random:src:pop"),
            Button::callback("Electronic", b"random:src:electronic"),
            Button::callback("Jazz", b"random:src:jazz"),
        ])
        .row(vec![
            Button::callback("Indie", b"random:src:indie"),
            Button::callback("Close", b"random:close"),
        ])
        .into_markup()
}

fn preview_keyboard(
    album_id: &str,
    storefront: &str,
    source: &str,
) -> ferogram::tl::enums::ReplyMarkup {
    InlineKeyboard::new()
        .row(vec![Button::callback(
            "🚀 Start Dump",
            format!("random:dump:{album_id}:{storefront}").as_bytes(),
        )])
        .row(vec![
            Button::callback(
                "Choose another",
                format!("random:reroll:{source}:{storefront}").as_bytes(),
            ),
            Button::callback("Back to sources", b"random:menu"),
        ])
        .row(vec![Button::callback("Close", b"random:close")])
        .into_markup()
}

fn retry_keyboard(source: &str, storefront: &str) -> ferogram::tl::enums::ReplyMarkup {
    InlineKeyboard::new()
        .row(vec![
            Button::callback(
                "Try again",
                format!("random:reroll:{source}:{storefront}").as_bytes(),
            ),
            Button::callback("Back to sources", b"random:menu"),
        ])
        .row(vec![Button::callback("Close", b"random:close")])
        .into_markup()
}

/// fetchSearchAlbum (iTunes album-entity search, random pick).
async fn fetch_search_album(
    state: &BotState,
    query: &str,
    storefront: &str,
    rng: &mut u64,
) -> Result<RandomAlbumCandidate, String> {
    let sf = storefront.to_lowercase();
    let mut candidates: Vec<RandomAlbumCandidate> = state
        .rip_deps
        .catalog()
        .search_albums(query, 50, &sf)
        .await
        .map_err(|error| error.to_string())?
        .into_iter()
        .map(|item| RandomAlbumCandidate {
            id: item.id,
            title: item.title,
            artist: item.artist,
            url: item.url,
            release_date: item.release_date,
            genre: item.genre,
            track_count: item.track_count,
            storefront: item.storefront,
        })
        .collect();
    if candidates.is_empty() {
        return Err(format!(
            "No albums found matching \"{query}\" on storefront {sf}"
        ));
    }
    let index = pick_index(rng, candidates.len());
    Ok(candidates.swap_remove(index))
}

/// fetchChartsAlbum via the engine's charts feed, random pick.
async fn fetch_charts_album(
    state: &BotState,
    storefront: &str,
    rng: &mut u64,
) -> Result<RandomAlbumCandidate, String> {
    let charts = state
        .rip_deps
        .catalog()
        .fetch_charts_albums(&storefront.to_lowercase(), 50)
        .await
        .map_err(|error| format!("Failed to fetch Apple Music charts: {error}"))?;
    if charts.is_empty() {
        return Err("No albums found in Apple Music charts".to_owned());
    }
    let index = pick_index(rng, charts.len());
    let chart = &charts[index];
    Ok(RandomAlbumCandidate {
        id: chart.id.clone(),
        title: chart.title.clone(),
        artist: chart.artist.clone(),
        url: chart.url.clone(),
        release_date: chart.release_date.clone(),
        genre: chart.genre.clone(),
        track_count: None,
        storefront: storefront.to_lowercase(),
    })
}

async fn fetch_wild_album(
    state: &BotState,
    storefront: &str,
    rng: &mut u64,
) -> Result<RandomAlbumCandidate, String> {
    let seed = WILD_SEEDS[pick_index(rng, WILD_SEEDS.len())];
    fetch_search_album(state, seed, storefront, rng).await
}

async fn fetch_south_asia_album(
    state: &BotState,
    storefront: &str,
    rng: &mut u64,
) -> Result<RandomAlbumCandidate, String> {
    let sf = if storefront.is_empty() || storefront == "us" {
        "in"
    } else {
        storefront
    };

    // 25% chance to pick from top charts in India
    let pick_charts = pick_index(rng, 4) == 0;
    if pick_charts && let Ok(candidate) = fetch_charts_album(state, sf, rng).await {
        return Ok(candidate);
    }

    let seed = SOUTH_ASIA_SEEDS[pick_index(rng, SOUTH_ASIA_SEEDS.len())];
    fetch_search_album(state, seed, sf, rng).await
}

/// Fetch one candidate album for a fixed source term.
async fn fetch_candidate_by_source(
    state: &BotState,
    source: &str,
    storefront: &str,
    rng: &mut u64,
) -> Result<RandomAlbumCandidate, String> {
    let source = source.to_lowercase();
    match source.as_str() {
        "charts" | "top" => fetch_charts_album(state, storefront, rng).await,
        "wild" | "random" => fetch_wild_album(state, storefront, rng).await,
        s if is_south_asia_source(s) => fetch_south_asia_album(state, storefront, rng).await,
        _ => {
            let term = source_search_term(&source).unwrap_or(source.as_str());
            fetch_search_album(state, term, storefront, rng).await
        }
    }
}

/// Check if an album or all of its tracks have already been dumped to the database cache.
async fn is_album_already_dumped(
    state: &BotState,
    album_id: &str,
    tracks: &[music::TrackMeta],
) -> bool {
    // 1. Check if album ZIP archive parts are cached in albums table
    if let Ok(parts) = state
        .rip_deps
        .albums()
        .find_albums(Provider::Apple, album_id, None)
        .await
        && !parts.is_empty()
    {
        return true;
    }

    // 2. Check if tracks of this album are all in the tracks cache
    if !tracks.is_empty() {
        let track_keys: Vec<TrackKey> = tracks
            .iter()
            .map(|t| TrackKey::new(Provider::Apple, t.id.clone()))
            .collect();
        if let Ok(cached) = state
            .rip_deps
            .tracks()
            .find_cached_tracks(&track_keys)
            .await
            && cached.len() == tracks.len()
        {
            return true;
        }
    }

    false
}

/// Pick and validate a candidate album, ensuring it has tracks available and is not already dumped.
async fn discover_valid_candidate(
    state: &BotState,
    source: &str,
    storefront: &str,
    rng: &mut u64,
) -> Result<RandomAlbumCandidate, String> {
    const MAX_ATTEMPTS: usize = 15;
    let mut last_err = String::new();

    for attempt in 1..=MAX_ATTEMPTS {
        match fetch_candidate_by_source(state, source, storefront, rng).await {
            Ok(mut candidate) => {
                // Fast-path check: is album ZIP already cached?
                if let Ok(parts) = state
                    .rip_deps
                    .albums()
                    .find_albums(Provider::Apple, &candidate.id, None)
                    .await
                    && !parts.is_empty()
                {
                    tracing::info!(
                        album_id = %candidate.id,
                        title = %candidate.title,
                        attempt,
                        "Random album candidate ZIP is already cached in albums table, skipping..."
                    );
                    last_err = format!("Album {} is already dumped", candidate.id);
                    continue;
                }

                match state
                    .rip_deps
                    .catalog()
                    .fetch_album_tracks(&candidate.id, &candidate.storefront)
                    .await
                {
                    Ok(full) if !full.tracks.is_empty() => {
                        // Check if all tracks are already dumped in database
                        if is_album_already_dumped(state, &candidate.id, &full.tracks).await {
                            tracing::info!(
                                album_id = %candidate.id,
                                title = %candidate.title,
                                attempt,
                                "Random album candidate tracks are already dumped, skipping..."
                            );
                            last_err = format!("Album {} is already dumped", candidate.id);
                            continue;
                        }

                        candidate.track_count = Some(full.tracks.len());
                        if !full.album.title.is_empty() {
                            candidate.title = full.album.title;
                        }
                        if !full.album.artist.is_empty() {
                            candidate.artist = full.album.artist;
                        }
                        if let Some(genre) = full.album.genre.filter(|g| !g.is_empty()) {
                            candidate.genre = Some(genre);
                        }
                        if !full.album.release_date.is_empty() {
                            candidate.release_date = Some(full.album.release_date);
                        }
                        return Ok(candidate);
                    }
                    Ok(_) => {
                        tracing::warn!(
                            album_id = %candidate.id,
                            attempt,
                            "Random album candidate has 0 tracks on iTunes, retrying..."
                        );
                        last_err = format!("Album {} has 0 tracks on iTunes", candidate.id);
                    }
                    Err(err) => {
                        tracing::warn!(
                            album_id = %candidate.id,
                            attempt,
                            error = %err,
                            "Random album candidate track lookup failed, retrying..."
                        );
                        last_err = err.to_string();
                    }
                }
            }
            Err(err) => {
                last_err = err;
            }
        }
    }

    Err(format!(
        "Failed to find an undumped album with available tracks after {MAX_ATTEMPTS} attempts: {last_err}"
    ))
}

pub fn register(dp: &mut Dispatcher, state: Arc<BotState>) {
    let random_state = Arc::clone(&state);
    dp.on_message(filters::command("random"), move |msg| {
        random(Arc::clone(&random_state), msg)
    });
}

async fn random(state: Arc<BotState>, msg: IncomingMessage) {
    let sender = msg.sender_user_id().unwrap_or_default();
    let marked_chat = super::marked_chat_id(&msg);
    if !state
        .auth
        .is_authorized(sender, Some(marked_chat))
        .await
        .unwrap_or(false)
    {
        return;
    }
    if !state.auth.is_admin(sender) {
        let _ = msg
            .reply(InputMessage::html(parse_dynamic_html(
                "🔒 <b>Access Restricted:</b> Random album discovery is restricted to the bot owner.",
            )))
            .await;
        return;
    }

    let tokens: Vec<String> = msg
        .text()
        .unwrap_or_default()
        .split_whitespace()
        .skip(1)
        .map(str::to_owned)
        .collect();
    let source_arg = tokens.first().map(|s| s.to_lowercase());
    let storefront_arg = tokens.get(1).map(|s| s.to_lowercase()).unwrap_or_else(|| {
        if let Some(src) = &source_arg
            && is_south_asia_source(src)
        {
            return "in".to_owned();
        }
        "us".to_owned()
    });

    let Some(source_arg) = source_arg else {
        let _ = msg
            .reply(
                InputMessage::html(parse_dynamic_html(&build_sources_menu_text()))
                    .reply_markup(sources_keyboard()),
            )
            .await;
        return;
    };

    let loading = msg
        .reply(InputMessage::html(parse_dynamic_html(&format!(
            "… <i>Discovering a random album from {} ({})</i>",
            escape(&source_arg),
            escape(&storefront_arg.to_uppercase()),
        ))))
        .await;
    let Ok(loading) = loading else { return };

    let mut rng = now_seed();
    let peer = super::chat_peer_ref(&msg);
    let result = discover_valid_candidate(&state, &source_arg, &storefront_arg, &mut rng).await;
    match result {
        Ok(candidate) => {
            let text = build_preview_text(&candidate);
            let keyboard = preview_keyboard(&candidate.id, &candidate.storefront, &source_arg);
            let _ = state
                .client
                .edit_message(
                    peer,
                    loading.id(),
                    InputMessage::html(parse_dynamic_html(&text)).reply_markup(keyboard),
                )
                .await;
        }
        Err(error) => {
            let text = format!(
                "! <b>Could not select a random album.</b><br/><code>{}</code>",
                escape(&error)
            );
            let _ = state
                .client
                .edit_message(
                    peer,
                    loading.id(),
                    InputMessage::html(parse_dynamic_html(&text)),
                )
                .await;
        }
    }
}

pub async fn callback(state: Arc<BotState>, query: CallbackQuery, action: DiscoveryAction) {
    let marked_chat = query
        .chat_peer
        .as_ref()
        .map(super::marked_peer_id)
        .unwrap_or(query.user_id);
    if !state
        .auth
        .is_authorized(query.user_id, Some(marked_chat))
        .await
        .unwrap_or(false)
    {
        let _ = query
            .answer()
            .alert("Unauthorized")
            .send(&state.client)
            .await;
        return;
    }
    if !state.auth.is_admin(query.user_id) {
        let _ = query
            .answer()
            .alert("🔒 Access restricted to bot owner.")
            .send(&state.client)
            .await;
        return;
    }

    let peer = query
        .chat_peer
        .as_ref()
        .map(|p| PeerRef::Peer(p.clone()))
        .unwrap_or_else(|| PeerRef::from(query.user_id));
    let message_id = query.message_id.unwrap_or_default();

    match action {
        DiscoveryAction::Close => {
            let _ = query.answer().send(&state.client).await;
            delete_message(&state, &peer, message_id).await;
        }
        DiscoveryAction::Menu => {
            let _ = query.answer().send(&state.client).await;
            let text = build_sources_menu_text();
            let _ = state
                .client
                .edit_message(
                    peer,
                    message_id,
                    InputMessage::html(parse_dynamic_html(&text)).reply_markup(sources_keyboard()),
                )
                .await;
        }
        DiscoveryAction::Discover { source, storefront }
        | DiscoveryAction::Reroll { source, storefront } => {
            let _ = query
                .answer()
                .text("… Discovering a random album")
                .send(&state.client)
                .await;
            let label = source_label(&source).unwrap_or(source.as_str()).to_owned();
            let _ = state
                .client
                .edit_message(
                    peer.clone(),
                    message_id,
                    InputMessage::html(parse_dynamic_html(&format!(
                        "… <i>Discovering a random album from {}</i>",
                        escape(&label)
                    ))),
                )
                .await;

            let mut rng = now_seed();
            match discover_valid_candidate(&state, &source, &storefront, &mut rng).await {
                Ok(candidate) => {
                    let text = build_preview_text(&candidate);
                    let keyboard = preview_keyboard(&candidate.id, &candidate.storefront, &source);
                    let _ = state
                        .client
                        .edit_message(
                            peer,
                            message_id,
                            InputMessage::html(parse_dynamic_html(&text)).reply_markup(keyboard),
                        )
                        .await;
                }
                Err(error) => {
                    let text = format!(
                        "! <b>Could not discover an album.</b><br/><code>{}</code>",
                        escape(&error)
                    );
                    let _ = state
                        .client
                        .edit_message(
                            peer,
                            message_id,
                            InputMessage::html(parse_dynamic_html(&text))
                                .reply_markup(retry_keyboard(&source, &storefront)),
                        )
                        .await;
                }
            }
        }
        DiscoveryAction::Dump {
            album_id,
            storefront,
        } => {
            let _ = query
                .answer()
                .text("Queuing album dump")
                .send(&state.client)
                .await;
            delete_message(&state, &peer, message_id).await;

            // The collapsed orchestrator handles this as a cache-only job.
            let user_display =
                crate::presentation::resolve_user_display_name(&state.client, query.user_id).await;
            let options = engine::orchestrator::types::RipJobOptions {
                provider: engine::Provider::Apple,
                chat_id: marked_chat,
                user_id: query.user_id,
                user_name: Some(user_display),
                delivery_chat_id: marked_chat,
                is_group: marked_chat != query.user_id,
                is_force: false,
                is_cache_only: true,
                single_storefront: Some(storefront.clone()),
                parsed_items: vec![ParsedTargetItem {
                    id: album_id,
                    kind: TargetKind::Album,
                    storefront: Some(storefront),
                }],
                reply_to_message_id: None,
                // The shared dashboard is the only live status surface.
                status_msg_id: 0,
                is_admin: true,
                codec_preference: None,
                rendition_policy: engine::orchestrator::types::RenditionPolicy::PrimaryOnly,
            };
            super::ensure_dashboard(&state, marked_chat, query.user_id, true, peer.clone()).await;

            if let Err(error) = state
                .rip_orchestrator
                .start_job(Arc::clone(&state.rip_deps), &options)
                .await
            {
                tracing::warn!(%error, "random album dump job failed to start");
                let _ = state
                    .client
                    .send_message(
                        peer,
                        InputMessage::html(format!(
                            "! <b>Could not dump album:</b><br/><code>{}</code>",
                            crate::html::escape(&error.to_string())
                        )),
                    )
                    .await;
            }
        }
    }
}

async fn delete_message(state: &BotState, peer: &PeerRef, message_id: i32) {
    if let Ok(messages) = state.client.get_messages(peer.clone(), &[message_id]).await
        && let Some(message) = messages.first()
    {
        let _ = message.delete().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn candidate() -> RandomAlbumCandidate {
        RandomAlbumCandidate {
            id: "1".to_owned(),
            title: "Moon Dreams".to_owned(),
            artist: "Luna".to_owned(),
            url: "https://music.apple.com/us/album/x/1".to_owned(),
            release_date: Some("2024-05-01T00:00:00Z".to_owned()),
            genre: Some("Pop".to_owned()),
            track_count: Some(12),
            storefront: "us".to_owned(),
        }
    }

    #[test]
    fn preview_text_is_exact() {
        assert_eq!(
            build_preview_text(&candidate()),
            "✓ <b>Random album selected</b><br/><br/>\
<b>Album:</b> Moon Dreams<br/>\
<b>Artist:</b> Luna<br/>\
<b>Tracks:</b> <code>12 tracks</code><br/>\
<b>Released:</b> <code>2024-05-01</code><br/>\
<b>Genre:</b> <code>Pop</code><br/>\
<b>Storefront:</b> <code>US</code><br/><br/>\
<a href=\"https://music.apple.com/us/album/x/1\">Open in Apple Music</a>"
        );
    }

    #[test]
    fn preview_text_falls_back_when_fields_missing() {
        let mut c = candidate();
        c.track_count = None;
        c.release_date = None;
        c.genre = None;
        let text = build_preview_text(&c);
        assert!(text.contains("<b>Tracks:</b> <code>Full Album</code>"));
        assert!(text.contains("<b>Released:</b> <code>Unknown</code>"));
        assert!(text.contains("<b>Genre:</b> <code>Music</code>"));
    }

    #[test]
    fn sources_menu_text_is_exact() {
        assert_eq!(
            build_sources_menu_text(),
            "<b>Apple Music random album explorer</b> (admin)<br/><br/>\
<blockquote>Select a discovery source below to pick a random album to dump into your cache channel:</blockquote>"
        );
    }

    #[test]
    fn source_terms_render_expected_labels() {
        assert_eq!(source_search_term("rock"), Some("rock album"));
        assert_eq!(source_search_term("rap"), Some("hip hop album"));
        assert_eq!(source_search_term("hiphop"), Some("hip hop album"));
        assert_eq!(source_search_term("pop"), Some("pop album"));
        assert_eq!(source_search_term("edm"), Some("electronic album"));
        assert_eq!(source_search_term("electronic"), Some("electronic album"));
        assert_eq!(source_search_term("jazz"), Some("jazz album"));
        assert_eq!(source_search_term("indie"), Some("indie album"));
        assert_eq!(source_search_term("charts"), None);
        assert_eq!(source_search_term("wild"), None);
        assert_eq!(source_search_term("custom query"), None);
    }

    #[test]
    fn source_labels_render_expected_text() {
        assert_eq!(source_label("charts"), Some("Top charts"));
        assert_eq!(source_label("hiphop"), Some("Hip-hop"));
        assert_eq!(source_label("unknown"), None);
    }

    #[test]
    fn south_asia_source_and_labels_match() {
        assert!(is_south_asia_source("southasia"));
        assert!(is_south_asia_source("subcontinent"));
        assert!(is_south_asia_source("desi"));
        assert!(is_south_asia_source("india"));
        assert!(is_south_asia_source("pakistan"));
        assert!(is_south_asia_source("bangladesh"));
        assert!(is_south_asia_source("nepal"));
        assert!(is_south_asia_source("bhutan"));
        assert!(!is_south_asia_source("rock"));

        assert_eq!(source_label("southasia"), Some("South Asia"));
        assert_eq!(source_label("subcontinent"), Some("South Asia"));
        assert_eq!(source_label("desi"), Some("South Asia"));
        assert_eq!(source_label("india"), Some("South Asia"));
        assert_eq!(source_label("pakistan"), Some("South Asia"));

        assert!(!SOUTH_ASIA_SEEDS.is_empty());
        assert!(SOUTH_ASIA_SEEDS.contains(&"bollywood"));
        assert!(SOUTH_ASIA_SEEDS.contains(&"pakistani pop"));
        assert!(SOUTH_ASIA_SEEDS.contains(&"coke studio"));
        assert!(SOUTH_ASIA_SEEDS.contains(&"bangla rock"));
        assert!(SOUTH_ASIA_SEEDS.contains(&"nepali folk"));
        assert!(SOUTH_ASIA_SEEDS.contains(&"afghan music"));
    }

    #[test]
    fn prng_stays_in_range() {
        let mut rng = now_seed();
        for _ in 0..1000 {
            let index = pick_index(&mut rng, 50);
            assert!(index < 50);
        }
        // Distinct seeds spread across the range.
        let mut seen = std::collections::HashSet::new();
        for _ in 0..200 {
            let index = pick_index(&mut rng, 50);
            seen.insert(index);
        }
        assert!(seen.len() > 10, "PRNG looks degenerate: {seen:?}");
    }

    #[test]
    fn wild_seeds_match_oracle_length() {
        assert_eq!(WILD_SEEDS.len(), 50);
        assert!(WILD_SEEDS.contains(&"future"));
        assert!(WILD_SEEDS.contains(&"memory"));
    }
}
