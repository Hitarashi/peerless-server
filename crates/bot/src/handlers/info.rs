use std::sync::Arc;

use apple::parse_alac_input;
use engine::{Provider, TrackKey};
use ferogram::{InputMessage, PeerRef, filters, filters::Dispatcher, tl};

use crate::{
    BotState,
    html::{escape, parse_dynamic_html},
};

const USAGE: &str = "<b>Track info usage</b><br/><br/><blockquote>• <code>/info &lt;Apple Music link&gt;</code><br/>• Reply to an Apple Music link with <code>/info</code></blockquote>";

async fn reply_text(msg: &ferogram::update::IncomingMessage, state: &BotState) -> Option<String> {
    let reply_header = match &msg.raw {
        tl::enums::Message::Message(m) => m.reply_to.as_ref(),
        tl::enums::Message::Service(m) => m.reply_to.as_ref(),
        _ => None,
    };
    if let Some(tl::enums::MessageReplyHeader::MessageReplyHeader(h)) = reply_header
        && let Some(ref quote) = h.quote_text
        && !quote.trim().is_empty()
    {
        return Some(quote.clone());
    }
    let reply_id = msg.reply_to_message_id()?;
    let peer = msg.peer_id()?.clone();
    let messages = state
        .client
        .get_messages(PeerRef::Peer(peer), &[reply_id])
        .await
        .ok()?;
    messages
        .first()
        .and_then(|message| message.text().map(str::to_owned))
}

fn format_duration_sec(seconds: i64) -> String {
    format!("{}:{:02}", seconds / 60, seconds % 60)
}

async fn info(msg: ferogram::update::IncomingMessage, state: Arc<BotState>) {
    let sender = msg.sender_user_id().unwrap_or_default();
    if !state
        .auth
        .is_authorized(sender, Some(super::marked_chat_id(&msg)))
        .await
        .unwrap_or(false)
    {
        return;
    }

    let reply = reply_text(&msg, &state).await;
    let parsed = parse_alac_input(msg.text().unwrap_or(""), reply.as_deref());
    let Some(parsed) = parsed else {
        let _ = msg
            .reply(InputMessage::html(parse_dynamic_html(USAGE)))
            .await;
        return;
    };
    let track_id = parsed.track_id;

    let result = async {
        let meta = state
            .rip_deps
            .catalog()
            .fetch_track_meta(&track_id, parsed.storefront.as_deref().unwrap_or("us"))
            .await
            .map_err(|error| error.to_string())?;
        let track_key = TrackKey::new(Provider::Apple, track_id.clone());
        let cached = state
            .rip_deps
            .tracks()
            .find_cached_tracks(std::slice::from_ref(&track_key))
            .await
            .map_err(|error| error.to_string())?
            .remove(&track_key);
        // The orchestration projection contains the dump id and display
        // fields; fetch the full row only to render the quality columns that
        // the info command exposes.
        let cached_row = if cached.is_some() {
            state
                .rip_deps
                .tracks()
                .search_cached_tracks(&track_id, 1)
                .await
                .ok()
                .and_then(|tracks| {
                    tracks
                        .into_iter()
                        .find(|track| track.provider == track_key.provider && track.track_id == track_key.track_id)
                })
        } else {
            None
        };

        let title = escape(&meta.title);
        let artist = escape(&meta.artist);
        let album = escape(&meta.album);
        let genre = escape(meta.genre.as_deref().unwrap_or("Music"));
        let year = if meta.release_date.is_empty() {
            "Unknown".to_owned()
        } else {
            meta.release_date.chars().take(4).collect()
        };
        let duration = format_duration_sec(meta.duration_secs);

        let (cache_status, cache_details) = match cached {
            Some(cached) => (
                "✓ <b>Cached in database</b>".to_owned(),
                {
                    let mut quality = vec!["ALAC".to_owned()];
                    if let Some(row) = cached_row.as_ref() {
                        if row.bit_depth != 0 {
                            quality.push(format!("{}-bit", row.bit_depth));
                        }
                        if row.sample_rate != 0 {
                            let tenths = (row.sample_rate + 50) / 100;
                            quality.push(format!("{}.{:01} kHz", tenths / 10, tenths % 10));
                        }
                    }
                    format!(
                        "Quality: <code>{}</code><br/>Dump Message: <code>#{}</code>",
                        quality.join(" • "),
                        cached.message_id
                    )
                },
            ),
            None => (
                "! <b>Not cached</b>".to_owned(),
                format!(
                    "Use <code>/get https://music.apple.com/song/{track_id}</code> to download the primary rendition and optional Dolby Atmos."
                ),
            ),
        };

        Ok::<_, String>(format!(
            "<b>{title}</b> — {artist}<br/><br/><blockquote><b>Metadata:</b><br/>• Album: <b>{album}</b><br/>• Track number: <code>{}</code> of <code>{}</code><br/>• Duration: <code>{duration}</code><br/>• Release year: <code>{year}</code><br/>• Genre: <code>{genre}</code><br/>• Apple track ID: <code>{track_id}</code></blockquote><br/><blockquote><b>Cache status:</b><br/>• Status: {cache_status}<br/>• {cache_details}</blockquote>",
            meta.track_number.unwrap_or(1),
            meta.track_count.unwrap_or(1),
        ))
    }
    .await;

    match result {
        Ok(text) => {
            let _ = msg
                .reply(InputMessage::html(parse_dynamic_html(&text)))
                .await;
        }
        Err(error) => {
            let text = format!(
                "! <b>Could not fetch track info.</b><br/><code>{}</code>",
                escape(&error)
            );
            let _ = msg
                .reply(InputMessage::html(parse_dynamic_html(&text)))
                .await;
        }
    }
}

pub fn register(dp: &mut Dispatcher, state: Arc<BotState>) {
    dp.on_message(filters::command("info"), move |msg| {
        info(msg, Arc::clone(&state))
    });
}
