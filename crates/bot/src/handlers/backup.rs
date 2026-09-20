//! `/export` + `/import` — gzipped typed database archive backup/restore
//! .
//!
//! Both commands are admin-only AND DM-only (the silently returns
//! outside a private chat).

use std::{
    collections::HashMap,
    fs::OpenOptions,
    io::Write,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use engine::limits::MAX_DOCUMENT_BYTES;
use ferogram::{
    InputMessage, PeerRef, filters,
    filters::Dispatcher,
    keyboard::{Button, InlineKeyboard},
    update::CallbackQuery,
};

use crate::{
    BotState,
    handlers::chat_peer_ref,
    html::{escape, parse_dynamic_html},
};

const KB: f64 = 1024.0;
const CONFIRMATION_TTL: Duration = Duration::from_secs(120);

#[derive(Clone)]
struct PendingImport {
    user_id: i64,
    media: ferogram::tl::enums::MessageMedia,
    file_name: String,
    expires_at: Instant,
}

fn pending_imports() -> &'static Mutex<HashMap<String, PendingImport>> {
    static PENDING: OnceLock<Mutex<HashMap<String, PendingImport>>> = OnceLock::new();
    PENDING.get_or_init(|| Mutex::new(HashMap::new()))
}

fn next_token() -> String {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    format!(
        "{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}

fn confirmation_keyboard(token: &str) -> ferogram::tl::enums::ReplyMarkup {
    InlineKeyboard::new()
        .row([
            Button::callback(
                "Restore database",
                crate::interaction::TelegramAction::ConfirmImport {
                    token: token.to_owned(),
                }
                .encode()
                .as_bytes(),
            ),
            Button::callback(
                "Cancel",
                crate::interaction::TelegramAction::CancelImport {
                    token: token.to_owned(),
                }
                .encode()
                .as_bytes(),
            ),
        ])
        .into_markup()
}

/// caption: "Compressed Size" in KB with one decimal.
fn export_caption(users: i64, tracks: i64, requests: i64, bytes: usize) -> String {
    format!(
        "<b>📦 Database Dump Exported</b>\n\n\
• <b>Users:</b> {users}\n\
• <b>Tracks:</b> {tracks}\n\
• <b>Requests:</b> {requests}\n\
• <b>Compressed Size:</b> {:.1} KB",
        bytes as f64 / KB
    )
}

/// Extract the `.json.gz` (or `.gz`) document from the replied-to message.
async fn replied_document(
    state: &BotState,
    msg: &ferogram::update::IncomingMessage,
) -> Option<(ferogram::tl::enums::MessageMedia, String)> {
    let reply_id = msg.reply_to_message_id()?;
    let peer = chat_peer_ref(msg);
    let messages = state.client.get_messages(peer, &[reply_id]).await.ok()?;
    let reply = messages.first()?;
    let media = reply.media()?.clone();
    let doc = ferogram::media::Document::from_media(&media)?;
    let name = doc.file_name().unwrap_or_default().to_owned();
    Some((media, name))
}

pub fn register(dp: &mut Dispatcher, state: Arc<BotState>) {
    let export_state = Arc::clone(&state);
    dp.on_message(filters::command("export"), move |msg| {
        let state = Arc::clone(&export_state);
        async move {
            export(state, msg).await;
        }
    });

    let import_state = Arc::clone(&state);
    dp.on_message(filters::command("import"), move |msg| {
        let state = Arc::clone(&import_state);
        async move {
            import(state, msg).await;
        }
    });
}

async fn export(state: Arc<BotState>, msg: ferogram::update::IncomingMessage) {
    if !state
        .auth
        .is_admin(msg.sender_user_id().unwrap_or_default())
    {
        return;
    }
    if !msg.is_private() {
        return;
    }
    let Some(peer) = msg.peer_id() else { return };
    let peer = PeerRef::Peer(peer.clone());

    let status = msg
        .reply(InputMessage::html(parse_dynamic_html(
            "📦 <i>Generating database export...</i>",
        )))
        .await
        .ok();

    let dump = db::DbDumpService::new(state.db_client.clone());
    let result = dump.export_dump_for_channel(state.dump_channel_id).await;
    let (buffer, stats, _) = match result {
        Ok(ok) => ok,
        Err(error) => {
            let text = format!(
                "× <b>Export failed</b>\n\n<code>{}</code>",
                escape(&error.to_string())
            );
            let _ = msg
                .reply(InputMessage::html(parse_dynamic_html(&text)))
                .await;
            if let Some(status) = status {
                let _ = status.delete().await;
            }
            return;
        }
    };

    // Write to a temp file, upload as a document, then clean up.
    let (tmp, mut file) = match secure_temp_file("alac_export") {
        Ok(file) => file,
        Err(error) => {
            let text = "× <b>Export failed</b><br/>Could not create a temporary archive.";
            tracing::warn!(%error, "failed to create secure export temporary file");
            let _ = msg
                .reply(InputMessage::html(parse_dynamic_html(text)))
                .await;
            return;
        }
    };
    if let Err(error) = file.write_all(&buffer) {
        let _ = std::fs::remove_file(&tmp);
        let text = format!(
            "× <b>Export failed</b>\n\n<code>{}</code>",
            escape(&error.to_string())
        );
        let _ = msg
            .reply(InputMessage::html(parse_dynamic_html(&text)))
            .await;
        return;
    }
    drop(file);

    let caption = export_caption(
        stats.users_count,
        stats.tracks_count,
        stats.requests_count,
        stats.bytes,
    );
    let send_result = match state.client.upload_file(&tmp).await {
        Ok(uploaded) => {
            let media = uploaded.as_document_media();
            let input = InputMessage::html(parse_dynamic_html(&caption)).copy_media(media);
            state
                .client
                .send_message(peer.clone(), input)
                .await
                .map(|_| ())
                .map_err(|error| error.to_string())
        }
        Err(error) => Err(error.to_string()),
    };

    let _ = std::fs::remove_file(&tmp);

    if let Err(error) = send_result {
        let text = format!("× <b>Export failed</b>\n\n<code>{}</code>", escape(&error));
        let _ = msg
            .reply(InputMessage::html(parse_dynamic_html(&text)))
            .await;
    }

    // deletes the "Generating..." status message in a finally.
    if let Some(status) = status {
        let _ = status.delete().await;
    }
}

async fn import(state: Arc<BotState>, msg: ferogram::update::IncomingMessage) {
    if !state
        .auth
        .is_admin(msg.sender_user_id().unwrap_or_default())
    {
        return;
    }
    if !msg.is_private() {
        return;
    }
    if msg.peer_id().is_none() {
        return;
    }

    let Some((doc, file_name)) = replied_document(&state, &msg).await else {
        let _ = msg
            .reply(InputMessage::html(parse_dynamic_html(
                "! Reply to a valid <code>.json.gz</code> database archive with <code>/import</code> to restore.",
            )))
            .await;
        return;
    };
    if !file_name.ends_with(".json.gz") && !file_name.ends_with(".gz") {
        let _ = msg
            .reply(InputMessage::html(parse_dynamic_html(
                "! The replied file must be a <code>.json.gz</code> database archive.",
            )))
            .await;
        return;
    }

    let token = next_token();
    {
        let mut pending = pending_imports()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let now = Instant::now();
        pending.retain(|_, value| value.expires_at > now);
        pending.insert(
            token.clone(),
            PendingImport {
                user_id: msg.sender_user_id().unwrap_or_default(),
                media: doc,
                file_name: file_name.clone(),
                expires_at: now + CONFIRMATION_TTL,
            },
        );
    }
    let text = format!(
        "<b>Restore this database archive?</b><br/><br/><blockquote>File: <code>{}</code></blockquote><br/><i>Current database records may be merged or replaced according to the archive contents.</i>",
        escape(&file_name)
    );
    let _ = msg
        .reply(
            InputMessage::html(parse_dynamic_html(&text))
                .reply_markup(confirmation_keyboard(&token)),
        )
        .await;
}

pub async fn callback(
    state: Arc<BotState>,
    query: CallbackQuery,
    action: crate::interaction::TelegramAction,
) {
    if !state.auth.is_admin(query.user_id) {
        let _ = query
            .answer()
            .alert("Access restricted.")
            .send(&state.client)
            .await;
        return;
    }
    if !matches!(query.chat_peer, Some(ferogram::tl::enums::Peer::User(_))) {
        let _ = query
            .answer()
            .alert("Database restore is available in direct messages only.")
            .send(&state.client)
            .await;
        return;
    }
    let is_confirm = matches!(
        action,
        crate::interaction::TelegramAction::ConfirmImport { .. }
    );
    let token = match action {
        crate::interaction::TelegramAction::ConfirmImport { token }
        | crate::interaction::TelegramAction::CancelImport { token } => token,
        _ => {
            let _ = query
                .answer()
                .alert("This restore action is unavailable.")
                .send(&state.client)
                .await;
            return;
        }
    };
    let pending = {
        let mut map = pending_imports()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        map.retain(|_, value| value.expires_at > Instant::now());
        map.remove(&token)
    };
    let Some(pending) = pending else {
        let _ = query
            .answer()
            .alert("This action has expired. Run /import again.")
            .send(&state.client)
            .await;
        return;
    };
    if pending.user_id != query.user_id || !is_confirm {
        let _ = query.answer().send(&state.client).await;
        if !is_confirm {
            delete_query_message(&state, &query).await;
        }
        return;
    }
    let _ = query
        .answer()
        .text("Restoring database")
        .send(&state.client)
        .await;
    let Some(peer) = query.chat_peer.clone().map(PeerRef::Peer) else {
        return;
    };
    restore_archive(&state, peer, pending.media, pending.file_name).await;
    delete_query_message(&state, &query).await;
}

async fn restore_archive(
    state: &BotState,
    peer: PeerRef,
    doc: ferogram::tl::enums::MessageMedia,
    file_name: String,
) {
    let status = state
        .client
        .send_message(
            peer.clone(),
            InputMessage::html(parse_dynamic_html(
                "… <i>Downloading and restoring database dump</i>",
            )),
        )
        .await
        .ok();
    let (tmp, file) = match secure_temp_file("alac_import") {
        Ok(file) => file,
        Err(error) => {
            tracing::warn!(%error, "failed to create secure import temporary file");
            let _ = state
                .client
                .send_message(
                    peer,
                    InputMessage::html(parse_dynamic_html(
                        "<b>Restore failed</b><br/>Could not create a temporary archive.",
                    )),
                )
                .await;
            return;
        }
    };
    drop(file);
    let restore_result = match download_document(state, &doc, &tmp).await {
        Ok(bytes) => {
            let dump = db::DbDumpService::new(state.db_client.clone());
            match dump
                .import_dump_for_channel(&bytes, state.dump_channel_id)
                .await
            {
                Ok(stats) => state
                    .rip_deps
                    .settings()
                    .reload()
                    .await
                    .map(|_| stats)
                    .map_err(|e| e.to_string()),
                Err(error) => Err(error.to_string()),
            }
        }
        Err(error) => Err(error),
    };
    let _ = std::fs::remove_file(&tmp);
    let text = match restore_result {
        Ok(stats) => format!(
            "<b>Database restored</b><br/><br/><blockquote>• Users: {}<br/>• Tracks: {}<br/>• Requests: {}<br/>• Elapsed: {}ms</blockquote>",
            stats.users_merged, stats.tracks_merged, stats.requests_merged, stats.duration_ms
        ),
        Err(error) => {
            tracing::warn!(%error, file_name, "database restore failed");
            "<b>Database restore failed</b><br/>The archive could not be applied. The database was left unchanged.".to_owned()
        }
    };
    let _ = state
        .client
        .send_message(peer, InputMessage::html(parse_dynamic_html(&text)))
        .await;
    if let Some(status) = status {
        let _ = status.delete().await;
    }
}

async fn delete_query_message(state: &BotState, query: &CallbackQuery) {
    if let (Some(peer), Some(message_id)) =
        (query.chat_peer.clone().map(PeerRef::Peer), query.message_id)
        && let Ok(messages) = state.client.get_messages(peer, &[message_id]).await
        && let Some(message) = messages.first()
    {
        let _ = message.delete().await;
    }
}

/// Download a document's bytes to a temp path and read them back.
async fn download_document(
    state: &BotState,
    media: &ferogram::tl::enums::MessageMedia,
    tmp: &Path,
) -> Result<Vec<u8>, String> {
    use ferogram::media::MediaQuality;
    state
        .client
        .download_media(media, MediaQuality::Original, tmp, None)
        .await
        .map_err(|error| error.to_string())?;
    let size = tokio::fs::metadata(tmp)
        .await
        .map_err(|error| error.to_string())?
        .len();
    if size > MAX_DOCUMENT_BYTES {
        return Err(format!(
            "database archive exceeds the {} MiB limit",
            MAX_DOCUMENT_BYTES / (1024 * 1024)
        ));
    }
    std::fs::read(tmp).map_err(|error| error.to_string())
}

fn secure_temp_file(prefix: &str) -> std::io::Result<(PathBuf, std::fs::File)> {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    let pid = std::process::id();
    for attempt in 0..16_u32 {
        let path =
            std::env::temp_dir().join(format!("{prefix}_{pid}_{timestamp}_{attempt}.json.gz"));
        match OpenOptions::new().write(true).create_new(true).open(&path) {
            Ok(file) => return Ok((path, file)),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::AlreadyExists,
        "could not allocate a unique temporary archive path",
    ))
}
