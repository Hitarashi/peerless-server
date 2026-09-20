use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use engine::{
    orchestrator::deps::CachedTrack,
    types::{ParsedTargetItem, Provider, TargetKind, TrackKey},
};
use ferogram::{
    InputMessage, PeerRef,
    filters::{self, Dispatcher},
    keyboard::{Button, InlineKeyboard},
    update::{CallbackQuery, IncomingMessage},
};
use regex::Regex;

use crate::{
    BotState,
    html::{escape, parse_dynamic_html},
    interaction::{ReportAction, ReportReason},
};

const USER_RATE_LIMIT_MAX: usize = 5;
const USER_RATE_LIMIT_WINDOW_MS: u128 = 3_600_000;

#[derive(Debug, Clone)]
pub struct TrackReport {
    pub id: String,
    pub track_id: String,
    pub reporter_user_id: i64,
    pub reporter_chat_id: i64,
    pub reporter_name: String,
    pub reason: String,
    pub track_title: String,
    pub track_artist: String,
    pub track_album: String,
    pub dump_message_id: Option<i32>,
}

#[derive(Default)]
pub struct ReportState {
    pub reports: HashMap<String, TrackReport>,
    pub reports_by_track: HashMap<String, String>,
    pub user_timestamps: HashMap<i64, Vec<u64>>,
}

static REPORT_STATE: OnceLock<Mutex<ReportState>> = OnceLock::new();
static REPORT_COUNTER: AtomicU64 = AtomicU64::new(0);

fn report_state() -> &'static Mutex<ReportState> {
    REPORT_STATE.get_or_init(|| Mutex::new(ReportState::default()))
}

/// Checks and records a report in the sliding one-hour window.
pub fn check_user_rate_limit(state: &mut ReportState, user_id: i64, now_ms: u128) -> bool {
    let timestamps = state.user_timestamps.entry(user_id).or_default();
    let now = now_ms.min(u128::from(u64::MAX)) as u64;
    timestamps.retain(|timestamp| {
        now_ms.saturating_sub(u128::from(*timestamp)) < USER_RATE_LIMIT_WINDOW_MS
    });
    if timestamps.len() >= USER_RATE_LIMIT_MAX {
        return false;
    }
    timestamps.push(now);
    true
}

/// Parses an Apple Music track id from a provider URL.
pub fn extract_track_id_from_text(text: &str) -> Option<String> {
    if text.is_empty() {
        return None;
    }
    let song_url = Regex::new(
        r"(?i)(?:https?://)?(?:music|itunes)\.apple\.com/(?:[a-z]{2}/)?(?:song|album)/(?:[^/\s]+/)?(?:id)?(\d+)(?:\?i=(\d+))?",
    )
    .expect("report URL regex is valid");
    if let Some(captures) = song_url.captures(text) {
        return captures
            .get(2)
            .or_else(|| captures.get(1))
            .map(|capture| capture.as_str().to_owned());
    }
    None
}

pub fn clean_dump_id(id: i64) -> String {
    let value = id.unsigned_abs().to_string();
    value.strip_prefix("100").unwrap_or(&value).to_owned()
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

fn random_report_id() -> String {
    // Report ids are eight opaque lowercase base-36 characters.
    // A timestamp and process-local counter give the same shape without
    // adding a random dependency.
    let value = now_ms() as u64 ^ REPORT_COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut value = value;
    let mut result = String::with_capacity(8);
    for _ in 0..8 {
        let digit = (value % 36) as u8;
        result.push(if digit < 10 {
            (b'0' + digit) as char
        } else {
            (b'a' + digit - 10) as char
        });
        value /= 36;
    }
    result
}

fn document_file_unique_id(document: &ferogram::media::Document) -> String {
    format!("mtproto:document:{}", document.id())
}

fn cached_from_track(track: db::Track) -> CachedTrack {
    CachedTrack {
        track_key: TrackKey::new(track.provider, track.track_id.clone()).with_codec(track.codec),
        codec: track.codec,
        message_id: i64::from(track.message_id),
        file_id: track.file_id,
        file_unique_id: track.file_unique_id,
        title: track.title,
        artist: track.artist,
        album: track.album,
    }
}

async fn find_cached_track(state: &BotState, track_id: &str) -> Option<CachedTrack> {
    let key = TrackKey::new(Provider::Apple, track_id);
    let ids = [key.clone()];
    state
        .rip_deps
        .tracks()
        .find_cached_tracks(&ids)
        .await
        .ok()
        .and_then(|mut tracks| tracks.remove(&key))
}

async fn find_by_file_unique_id(state: &BotState, file_unique_id: &str) -> Option<CachedTrack> {
    state
        .rip_deps
        .tracks()
        .find_track_by_file_unique_id(file_unique_id)
        .await
        .ok()
        .flatten()
        .map(cached_from_track)
}

fn admin_keyboard(track_id: &str, report_id: &str) -> ferogram::tl::enums::ReplyMarkup {
    InlineKeyboard::new()
        .row([
            Button::callback(
                "Re-rip now (-f)",
                format!("report:act:rerip:{track_id}:{report_id}").as_bytes(),
            ),
            Button::callback(
                "Delete track",
                format!("report:act:del:{track_id}:{report_id}").as_bytes(),
            ),
        ])
        .row([Button::callback(
            "Dismiss",
            format!("report:act:dismiss:{report_id}").as_bytes(),
        )])
        .into_markup()
}

fn reason_keyboard(track_id: &str) -> ferogram::tl::enums::ReplyMarkup {
    InlineKeyboard::new()
        .row([
            Button::callback(
                "Corrupted / Won't play",
                format!("report:sub:{track_id}:corrupted").as_bytes(),
            ),
            Button::callback(
                "Incomplete / cut off",
                format!("report:sub:{track_id}:incomplete").as_bytes(),
            ),
        ])
        .row([
            Button::callback(
                "Wrong tags / metadata",
                format!("report:sub:{track_id}:metadata").as_bytes(),
            ),
            Button::callback(
                "Other (custom note)",
                format!("report:sub:{track_id}:other").as_bytes(),
            ),
        ])
        .row([Button::callback("Cancel", b"report:cancel")])
        .into_markup()
}

fn close_keyboard() -> ferogram::tl::enums::ReplyMarkup {
    InlineKeyboard::new()
        .row([Button::callback("Close", b"report:cancel")])
        .into_markup()
}

fn user_name(user: Option<ferogram::types::User>, user_id: i64) -> String {
    if let Some(user) = user {
        let first = user.first_name().unwrap_or_default().trim();
        let last = user.last_name().unwrap_or_default().trim();
        if !first.is_empty() && !last.is_empty() {
            return format!("{first} {last}");
        }
        if !first.is_empty() {
            return first.to_owned();
        }
        if !last.is_empty() {
            return last.to_owned();
        }
        if let Some(username) = user.username() {
            return format!("@{username}");
        }
    }
    format!("User {user_id}")
}

async fn dispatch_report(
    state: &BotState,
    track: &CachedTrack,
    reporter_user_id: i64,
    reporter_chat_id: i64,
    reporter_name: String,
    reason: String,
) -> TrackReport {
    let report = TrackReport {
        id: random_report_id(),
        track_id: track.track_key.track_id.clone(),
        reporter_user_id,
        reporter_chat_id,
        reporter_name,
        reason,
        track_title: track.title.clone(),
        track_artist: track.artist.clone(),
        track_album: track.album.clone(),
        dump_message_id: (track.message_id != 0)
            .then(|| i32::try_from(track.message_id).ok())
            .flatten(),
    };
    let report_id = report.id.clone();
    let admin_card = {
        let dump_link = report.dump_message_id.map_or_else(
            || "<i>Not available</i>".to_owned(),
            |message_id| {
                format!(
                    "<a href=\"https://t.me/c/{}/{message_id}\">Message #{message_id}</a>",
                    clean_dump_id(state.dump_channel_id)
                )
            },
        );
        format!(
            "🚨 <b>New Track Issue Report</b><br/><br/>👤 <b>Reported by:</b> <a href=\"tg://user?id={}\">{}</a> (<code>{}</code>)<br/>🎵 <b>Track:</b> <b>{}</b> - {}<br/>💽 <b>Album:</b> {}<br/>🆔 <b>Apple Track ID:</b> <code>{}</code><br/>🔗 <b>Dump Message:</b> {}<br/><br/>⚠️ <b>Reported Issue:</b><br/><blockquote>{}</blockquote>",
            report.reporter_user_id,
            escape(&report.reporter_name),
            report.reporter_user_id,
            escape(&report.track_title),
            escape(&report.track_artist),
            escape(&report.track_album),
            report.track_id,
            dump_link,
            escape(&report.reason),
        )
    };
    {
        let mut registry = report_state()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        registry
            .reports_by_track
            .insert(report.track_id.clone(), report_id.clone());
        registry.reports.insert(report_id.clone(), report.clone());
    }
    let admin_input = InputMessage::html(parse_dynamic_html(&admin_card))
        .reply_markup(admin_keyboard(&report.track_id, &report_id));
    let _ = state
        .client
        .send_message(PeerRef::from(state.admin_id), admin_input)
        .await;

    report
}

fn command_args(text: &str) -> String {
    text.find(char::is_whitespace)
        .map(|index| text[index..].trim().to_owned())
        .unwrap_or_default()
}

async fn handle_command(state: Arc<BotState>, msg: IncomingMessage) {
    let user_id = msg.sender_user_id().unwrap_or_default();
    let chat_id = super::marked_chat_id(&msg);
    let is_admin = state.auth.is_admin(user_id);
    if !is_admin
        && !state
            .auth
            .is_authorized(user_id, Some(chat_id))
            .await
            .unwrap_or(false)
    {
        return;
    }

    let raw_args = command_args(msg.text().unwrap_or_default());
    let reply = msg.get_reply_with(&state.client).await.ok().flatten();
    let mut target = None;
    let custom_reason;
    if let Some(reply) = reply.as_ref() {
        if reply.media().is_some() {
            if let Some(document) = reply.document() {
                target = find_by_file_unique_id(&state, &document_file_unique_id(&document)).await;
            }
            if target.is_none()
                && let Some(track_id) = reply.text().and_then(extract_track_id_from_text)
            {
                target = find_cached_track(&state, &track_id).await;
            }
            custom_reason = raw_args;
        } else if raw_args.is_empty() {
            custom_reason = String::new();
        } else {
            let mut tokens = raw_args.split_whitespace();
            let first = tokens.next().unwrap_or_default();
            if let Some(track_id) = extract_track_id_from_text(first) {
                target = find_cached_track(&state, &track_id).await;
                custom_reason = tokens.collect::<Vec<_>>().join(" ").trim().to_owned();
            } else {
                custom_reason = raw_args;
            }
        }
    } else if raw_args.is_empty() {
        custom_reason = String::new();
    } else {
        let mut tokens = raw_args.split_whitespace();
        let first = tokens.next().unwrap_or_default();
        if let Some(track_id) = extract_track_id_from_text(first) {
            target = find_cached_track(&state, &track_id).await;
            custom_reason = tokens.collect::<Vec<_>>().join(" ").trim().to_owned();
        } else {
            custom_reason = raw_args;
        }
    }

    let Some(track) = target else {
        let text = "⚠️ <b>Report a Track Issue</b><br/><br/><blockquote><b>How to report an issue:</b><br/>• <b>Reply to any song</b> sent by the bot with <code>/report</code><br/>• Or reply with your note: <code>/report &lt;description&gt;</code><br/>• Or send: <code>/report &lt;apple_music_link&gt; [description]</code><br/><br/><i>Example: Reply to a song and type <code>/report cuts off at 2:15</code></i></blockquote>";
        let _ = msg
            .reply(InputMessage::html(parse_dynamic_html(text)))
            .await;
        return;
    };

    if report_state()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .reports_by_track
        .contains_key(&track.track_key.track_id)
    {
        let text = format!(
            "ℹ️ <b>Already Under Review</b><br/><br/><blockquote>The track <b>{}</b> has already been reported and is currently under review by the administrator. Thank you for your report!</blockquote>",
            escape(&track.title)
        );
        let _ = msg
            .reply(InputMessage::html(parse_dynamic_html(&text)))
            .await;
        return;
    }
    if !is_admin {
        let allowed = {
            let mut registry = report_state()
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            check_user_rate_limit(&mut registry, user_id, now_ms())
        };
        if !allowed {
            let text = "⏳ <b>Rate Limit Reached</b><br/><br/><blockquote>You can submit a maximum of 5 reports per hour. Please wait before submitting another report.</blockquote>";
            let _ = msg
                .reply(InputMessage::html(parse_dynamic_html(text)))
                .await;
            return;
        }
    }

    if !custom_reason.is_empty() {
        let name = user_name(msg.sender_user().await.ok().flatten(), user_id);
        let report = dispatch_report(&state, &track, user_id, chat_id, name, custom_reason).await;
        let text = format!(
            "✅ <b>Report Submitted</b><br/><br/><blockquote>Thank you! Your report for <b>{}</b> has been delivered to the administrator for review.<br/><br/><b>Reason:</b> <i>{}</i></blockquote>",
            escape(&report.track_title),
            escape(&report.reason)
        );
        let _ = msg
            .reply(InputMessage::html(parse_dynamic_html(&text)))
            .await;
    } else {
        let text = format!(
            "⚠️ <b>Report Track Issue</b><br/><br/><blockquote>🎵 <b>Track:</b> <b>{}</b> - {}<br/>💽 <b>Album:</b> {}<br/><br/>Please choose the issue you experienced:</blockquote>",
            escape(&track.title),
            escape(&track.artist),
            escape(&track.album)
        );
        let _ = msg
            .reply(
                InputMessage::html(parse_dynamic_html(&text))
                    .reply_markup(reason_keyboard(&track.track_key.track_id)),
            )
            .await;
    }
}

async fn delete_message(state: &BotState, peer: PeerRef, message_id: i32) {
    if let Ok(messages) = state.client.get_messages(peer, &[message_id]).await
        && let Some(message) = messages.first()
    {
        let _ = message.delete().await;
    }
}

async fn delete_dump_message_checked(state: &BotState, message_id: i64) -> Result<(), String> {
    let message_id = i32::try_from(message_id)
        .map_err(|error| format!("dump message id out of range: {error}"))?;
    let messages = state
        .client
        .get_messages(state.dump_peer.clone(), &[message_id])
        .await
        .map_err(|error| error.to_string())?;
    let Some(message) = messages.first() else {
        return Ok(());
    };
    message.delete().await.map_err(|error| error.to_string())
}

async fn edit_query(
    state: &BotState,
    query: &CallbackQuery,
    text: &str,
    markup: Option<ferogram::tl::enums::ReplyMarkup>,
) {
    if let (Some(peer), Some(message_id)) =
        (query.chat_peer.clone().map(PeerRef::Peer), query.message_id)
    {
        let mut input = InputMessage::html(parse_dynamic_html(text));
        if let Some(markup) = markup {
            input = input.reply_markup(markup);
        }
        let _ = state.client.edit_message(peer, message_id, input).await;
    }
}

async fn callback_submission(
    state: Arc<BotState>,
    query: CallbackQuery,
    track_id: &str,
    preset: ReportReason,
) {
    if preset == ReportReason::Other {
        let _ = query.answer().send(&state.client).await;
        edit_query(
            &state,
            &query,
            "✍️ <b>Custom Report Note</b><br/><br/><blockquote>Please reply to the song with your note:<br/><code>/report &lt;your description here&gt;</code></blockquote>",
            Some(close_keyboard()),
        )
        .await;
        return;
    }
    let reason = match preset {
        ReportReason::Corrupted => "Corrupted / Won't play",
        ReportReason::Incomplete => "Incomplete / Audio cut off",
        ReportReason::Metadata => "Wrong metadata / tags / lyrics",
        ReportReason::Other => "General playback problem",
    };
    let duplicate = report_state()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .reports_by_track
        .contains_key(track_id);
    if duplicate {
        let _ = query
            .answer()
            .alert("This track is already under review.")
            .send(&state.client)
            .await;
        if let (Some(peer), Some(message_id)) =
            (query.chat_peer.clone().map(PeerRef::Peer), query.message_id)
        {
            delete_message(&state, peer, message_id).await;
        }
        return;
    }
    if !state.auth.is_admin(query.user_id) {
        let allowed = {
            let mut registry = report_state()
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            check_user_rate_limit(&mut registry, query.user_id, now_ms())
        };
        if !allowed {
            let _ = query
                .answer()
                .alert("Rate limit reached (max 5 reports/hour).")
                .send(&state.client)
                .await;
            return;
        }
    }
    let Some(track) = find_cached_track(&state, track_id).await else {
        let _ = query
            .answer()
            .alert("Track not found in database.")
            .send(&state.client)
            .await;
        return;
    };
    let report = dispatch_report(
        &state,
        &track,
        query.user_id,
        query
            .chat_peer
            .as_ref()
            .map(super::marked_peer_id)
            .unwrap_or(query.user_id),
        format!("User {}", query.user_id),
        reason.to_owned(),
    )
    .await;
    let _ = query
        .answer()
        .text("Report submitted successfully!")
        .send(&state.client)
        .await;
    let text = format!(
        "✅ <b>Report Submitted</b><br/><br/><blockquote>Thank you! Your report for <b>{}</b> has been delivered to the administrator.<br/><br/><b>Reason:</b> <i>{}</i></blockquote>",
        escape(&report.track_title),
        escape(&report.reason)
    );
    edit_query(&state, &query, &text, None).await;
}

async fn admin_callback(state: Arc<BotState>, query: CallbackQuery, action: ReportAction) {
    if !state.auth.is_admin(query.user_id) {
        let _ = query
            .answer()
            .alert("🔒 Access restricted to bot owner.")
            .send(&state.client)
            .await;
        return;
    }
    match action {
        ReportAction::Dismiss { report_id } => {
            let report = {
                let mut registry = report_state()
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                let report = registry.reports.remove(&report_id);
                if let Some(report) = &report {
                    registry.reports_by_track.remove(&report.track_id);
                }
                report
            };
            let Some(report) = report else {
                let _ = query
                    .answer()
                    .alert("This report action has expired. Open the report again.")
                    .send(&state.client)
                    .await;
                return;
            };
            let _ = query
                .answer()
                .text("Report dismissed")
                .send(&state.client)
                .await;
            let text = format!(
                "❌ <b>Report Dismissed</b><br/><br/><blockquote>The report for track <code>{}</code> was dismissed.</blockquote>",
                escape(&report.track_id)
            );
            edit_query(&state, &query, &text, None).await;
        }
        ReportAction::Delete {
            track_id,
            report_id,
        } => {
            let report = report_state()
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .reports
                .get(&report_id)
                .cloned();
            let Some(report) = report.filter(|report| report.track_id == track_id) else {
                let _ = query
                    .answer()
                    .alert("This report action has expired. Open the report again.")
                    .send(&state.client)
                    .await;
                return;
            };
            let _ = query
                .answer()
                .text("Deleting track...")
                .send(&state.client)
                .await;
            let Some(track) = find_cached_track(&state, &track_id).await else {
                let _ = query
                    .answer()
                    .alert("This track is no longer cached. Open the report again.")
                    .send(&state.client)
                    .await;
                return;
            };
            if let Err(error) = delete_dump_message_checked(&state, track.message_id).await {
                tracing::warn!(%error, track_id, "failed to delete dump message; database row retained");
                edit_query(
                    &state,
                    &query,
                    "! <b>Track was not deleted.</b><br/>The dump message could not be removed. Try again.",
                    Some(admin_keyboard(&track_id, &report_id)),
                )
                .await;
                return;
            }
            if let Err(error) = state
                .rip_deps
                .tracks()
                .delete_track(&TrackKey::new(Provider::Apple, &track_id))
                .await
            {
                tracing::warn!(%error, track_id, "failed to delete cached track after message deletion");
                edit_query(
                    &state,
                    &query,
                    "! <b>Track was not deleted completely.</b><br/>The database could not be updated. Try again.",
                    Some(admin_keyboard(&track_id, &report_id)),
                )
                .await;
                return;
            }
            {
                let mut registry = report_state()
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                registry.reports_by_track.remove(&track_id);
                registry.reports.remove(&report_id);
            }
            {
                let text = format!(
                    "ℹ️ <b>Report Update:</b><br/>The reported track <b>{}</b> has been removed from the database by the administrator.",
                    escape(&report.track_title)
                );
                let _ = state
                    .client
                    .send_message(
                        PeerRef::from(report.reporter_chat_id),
                        InputMessage::html(parse_dynamic_html(&text)),
                    )
                    .await;
            }
            let text = format!(
                "🗑️ <b>Track Deleted</b><br/><br/><blockquote>Track <code>{}</code> (<b>{}</b>) has been deleted from both the database and dump channel.</blockquote>",
                escape(&track_id),
                escape(&report.track_title)
            );
            edit_query(&state, &query, &text, None).await;
        }
        ReportAction::Rerip {
            track_id,
            report_id,
        } => {
            let report = report_state()
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .reports
                .get(&report_id)
                .cloned();
            let Some(report) = report.filter(|report| report.track_id == track_id) else {
                let _ = query
                    .answer()
                    .alert("This report action has expired. Open the report again.")
                    .send(&state.client)
                    .await;
                return;
            };
            let _ = query
                .answer()
                .text("Starting force re-rip...")
                .send(&state.client)
                .await;
            let title = escape(&report.track_title);
            let text = format!(
                "🔄 <b>Re-ripping Track...</b><br/><br/><blockquote>Re-ripping track <code>{}</code> (<b>{}</b>) in force cache-only mode. Old corrupted dump message will be replaced automatically.</blockquote>",
                escape(&track_id),
                title
            );
            edit_query(&state, &query, &text, None).await;
            let marked_chat = query
                .chat_peer
                .as_ref()
                .map(super::marked_peer_id)
                .unwrap_or(query.user_id);
            // The shared dashboard is the only live rip status surface. The
            // Created event opens/replaces it for this chat.
            super::ensure_dashboard(
                &state,
                marked_chat,
                query.user_id,
                true,
                PeerRef::from(marked_chat),
            )
            .await;
            let options = engine::orchestrator::types::RipJobOptions {
                provider: engine::Provider::Apple,
                chat_id: marked_chat,
                user_id: query.user_id,
                user_name: Some(format!("User {}", query.user_id)),
                delivery_chat_id: marked_chat,
                is_group: marked_chat != query.user_id,
                is_force: true,
                is_cache_only: true,
                single_storefront: None,
                parsed_items: vec![ParsedTargetItem {
                    id: track_id.to_owned(),
                    kind: TargetKind::Track,
                    storefront: None,
                }],
                reply_to_message_id: None,
                status_msg_id: 0,
                is_admin: true,
                codec_preference: None,
                rendition_policy: engine::orchestrator::types::RenditionPolicy::PrimaryOnly,
            };
            match state
                .rip_orchestrator
                .start_job(Arc::clone(&state.rip_deps), &options)
                .await
            {
                Ok(_) => {
                    {
                        let courtesy = format!(
                            "✅ <b>Report Update:</b><br/>The issue with <b>{}</b> has been resolved! The track was re-ripped and replaced with a healthy lossless version.",
                            escape(&report.track_title)
                        );
                        let _ = state
                            .client
                            .send_message(
                                PeerRef::from(report.reporter_chat_id),
                                InputMessage::html(parse_dynamic_html(&courtesy)),
                            )
                            .await;
                    }
                    {
                        let mut registry = report_state()
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner());
                        registry.reports_by_track.remove(&track_id);
                        registry.reports.remove(&report_id);
                    }
                    let text = format!(
                        "✅ <b>Track Re-ripped Successfully!</b><br/><br/><blockquote>Track <code>{}</code> (<b>{}</b>) was successfully re-ripped and replaced in the dump channel. Old message was deleted.</blockquote>",
                        escape(&track_id),
                        title
                    );
                    edit_query(&state, &query, &text, None).await;
                }
                Err(error) => {
                    let text = format!(
                        "❌ <b>Re-rip Failed</b><br/><br/><blockquote>Failed to re-rip track <code>{}</code>: <code>{}</code></blockquote>",
                        escape(&track_id),
                        escape(&error.to_string())
                    );
                    edit_query(
                        &state,
                        &query,
                        &text,
                        Some(admin_keyboard(&track_id, &report_id)),
                    )
                    .await;
                }
            }
        }
        ReportAction::Cancel | ReportAction::Submit { .. } => {
            let _ = query
                .answer()
                .alert("This report action is unavailable.")
                .send(&state.client)
                .await;
        }
    }
}

pub fn register(dp: &mut Dispatcher, state: Arc<BotState>) {
    dp.on_message(filters::command("report"), move |msg| {
        handle_command(Arc::clone(&state), msg)
    });
}

pub async fn callback(state: Arc<BotState>, query: CallbackQuery, action: ReportAction) {
    let chat_id = query
        .chat_peer
        .as_ref()
        .map(super::marked_peer_id)
        .unwrap_or(query.user_id);
    if !state.auth.is_admin(query.user_id)
        && !state
            .auth
            .is_authorized(query.user_id, Some(chat_id))
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
    if action == ReportAction::Cancel {
        let _ = query
            .answer()
            .text("Report cancelled")
            .send(&state.client)
            .await;
        if let (Some(peer), Some(message_id)) =
            (query.chat_peer.clone().map(PeerRef::Peer), query.message_id)
        {
            delete_message(&state, peer, message_id).await;
        }
        return;
    }
    match action {
        ReportAction::Submit { track_id, reason } => {
            callback_submission(state, query, &track_id, reason).await;
        }
        ReportAction::Dismiss { .. } | ReportAction::Delete { .. } | ReportAction::Rerip { .. } => {
            admin_callback(state, query, action).await
        }
        ReportAction::Cancel => unreachable!("cancel handled above"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_oracle_track_ids() {
        assert_eq!(
            extract_track_id_from_text("https://music.apple.com/us/album/foo/123?i=456"),
            Some("456".into())
        );
        assert_eq!(
            extract_track_id_from_text("music.apple.com/album/x/id789"),
            Some("789".into())
        );
        assert_eq!(extract_track_id_from_text("1441226844"), None);
        assert_eq!(extract_track_id_from_text("123"), None);
        assert_eq!(extract_track_id_from_text("text without ids"), None);
        assert_eq!(
            extract_track_id_from_text("https://music.apple.com/gb/song/track-name/1669552251"),
            Some("1669552251".into())
        );
    }

    #[test]
    fn rate_limit_uses_a_sliding_window() {
        let mut state = ReportState::default();
        for index in 0..5 {
            assert!(check_user_rate_limit(&mut state, 7, index));
        }
        assert!(!check_user_rate_limit(&mut state, 7, 5));
        let mut state = ReportState::default();
        state.user_timestamps.insert(7, vec![0]);
        assert!(check_user_rate_limit(&mut state, 7, 3_600_001));
    }

    #[test]
    fn cleans_marked_dump_ids() {
        assert_eq!(clean_dump_id(-1001234567890), "1234567890");
    }
}
