//! `/stream` command — generates single-use OTP and native app connection links.

use std::sync::Arc;

use ferogram::{
    InputMessage, filters,
    filters::Dispatcher,
    keyboard::{Button, InlineKeyboard},
};

use crate::{BotState, html::parse_dynamic_html};

pub async fn stream_command(state: Arc<BotState>, msg: ferogram::update::IncomingMessage) {
    if !msg.is_private() {
        tracing::info!(chat = ?msg.peer_id(), "stream command rejected in non-private chat");
        let text = "⚠️ <b>Direct Message Only.</b><br/>The <code>/stream</code> command can only be used in a private chat with the bot to protect your login credentials.";
        if let Err(err) = msg
            .reply(InputMessage::html(parse_dynamic_html(text)))
            .await
        {
            tracing::error!(error = %err, "failed to reply to group /stream command");
        }
        return;
    }

    let sender = match msg.sender_user_id() {
        Some(id) => id,
        None => {
            tracing::warn!("stream command received without sender_user_id");
            return;
        }
    };

    let is_admin = state.auth.is_admin(sender);
    let is_auth = is_admin
        || state
            .auth
            .is_authorized(sender, None)
            .await
            .unwrap_or(false);

    if !is_auth {
        tracing::warn!(user_id = sender, "unauthorized access attempt to /stream");
        let text = "⚠️ <b>Unauthorized.</b> You are not authorized to access the streaming server.<br/>Contact the bot administrator.";
        if let Err(err) = msg
            .reply(InputMessage::html(parse_dynamic_html(text)))
            .await
        {
            tracing::error!(error = %err, user_id = sender, "failed to reply to unauthorized /stream");
        }
        return;
    }

    // Warm Telegram's peer cache so the authenticated profile API can resolve
    // this user's current name and profile photo after the OTP is exchanged.
    if let Err(error) = msg.sender_user().await {
        tracing::warn!(user_id = sender, error = %error, "could not resolve Telegram profile for /stream");
    }

    let settings = state.rip_deps.settings_snapshot();
    let public_url = settings
        .stream_public_url
        .as_deref()
        .map(str::trim)
        .filter(|u| !u.is_empty());

    let clean_url = match public_url {
        Some(url) => url.trim_end_matches('/'),
        None => {
            if !is_admin {
                let text = "⚠️ <b>Server Not Configured.</b><br/>Please ask the bot administrator to set up the backend server URL properly before logging in.";
                let _ = msg
                    .reply(InputMessage::html(parse_dynamic_html(text)))
                    .await;
            } else {
                let text = "⚠️ <b>Streaming Server URL Not Configured.</b><br/><br/>The public backend URL is not set. Please <b>reply to this message with your server URL</b> (e.g. <code>https://stream.example.com</code>) to configure it automatically.<br/><br/>Or run <code>/settings stream_url &lt;url&gt;</code>.";
                let _ = msg
                    .reply(InputMessage::html(parse_dynamic_html(text)))
                    .await;
            }
            return;
        }
    };

    let code = match state.session_manager.create_login_code(sender).await {
        Ok(code) => code,
        Err(err) => {
            tracing::error!(error = %err, user_id = sender, "failed to generate stream login code");
            let text = "⚠️ <b>Error:</b> Failed to generate login code. Please try again later.";
            let _ = msg
                .reply(InputMessage::html(parse_dynamic_html(text)))
                .await;
            return;
        }
    };

    let open_gateway_url = format!("{clean_url}/open?code={code}");

    let text = format!(
        "🎧 <b>Lossless Audio Streaming</b><br/><br/>\
         🔑 <b>Single-Use Login Code:</b><br/>\
         <code>{code}</code><br/><br/>\
         Click the button below to connect your Peerless player automatically:"
    );

    let mut kb = InlineKeyboard::new();
    kb = kb.row([Button::url("🎵 Open & Connect Peerless", open_gateway_url)]);

    let input = InputMessage::html(parse_dynamic_html(&text)).reply_markup(kb.into_markup());

    if let Err(err) = msg.reply(input).await {
        tracing::error!(error = %err, user_id = sender, "failed to send /stream response");
    }
}

pub async fn handle_admin_url_config(
    state: Arc<BotState>,
    msg: ferogram::update::IncomingMessage,
) -> bool {
    if !msg.is_private() {
        return false;
    }

    let sender = match msg.sender_user_id() {
        Some(id) => id,
        None => return false,
    };

    if !state.auth.is_admin(sender) {
        return false;
    }

    let text = match msg.text() {
        Some(t) => t.trim(),
        None => return false,
    };

    if text.starts_with('/') {
        return false;
    }

    if text.contains("music.apple.com") || text.contains("qobuz.com") {
        return false;
    }

    let is_url = text.starts_with("http://") || text.starts_with("https://");
    let is_reply = msg.reply_to_message_id().is_some();

    if !is_url && !is_reply {
        return false;
    }

    let candidate = if is_url {
        text.to_string()
    } else if text.contains('.') || text.contains(':') {
        let trimmed = text
            .trim_start_matches("https://")
            .trim_start_matches("http://");
        format!("https://{trimmed}")
    } else {
        return false;
    };

    let clean = candidate.trim().trim_end_matches('/').to_string();
    if clean.is_empty() || (!clean.starts_with("http://") && !clean.starts_with("https://")) {
        return false;
    }

    let settings_store = state.rip_deps.settings();
    settings_store
        .set_setting("stream_public_url", serde_json::json!(clean))
        .await;

    let confirm_text =
        format!("✅ <b>Server URL Configured!</b><br/>Public URL set to: <code>{clean}</code>");

    if let Err(err) = msg
        .reply(InputMessage::html(parse_dynamic_html(&confirm_text)))
        .await
    {
        tracing::error!(
            error = %err,
            user_id = sender,
            "failed to send URL configuration confirmation"
        );
    }

    // Immediately invoke stream_command to deliver the login button!
    stream_command(state, msg).await;
    true
}

pub fn register(dp: &mut Dispatcher, state: Arc<BotState>) {
    let stream_state = Arc::clone(&state);
    dp.on_message(filters::command("stream"), move |msg| {
        let state = Arc::clone(&stream_state);
        async move {
            stream_command(state, msg).await;
        }
    });

    let auto_state = Arc::clone(&state);
    dp.on_message(
        filters::custom(|msg| {
            if let Some(t) = msg.text() {
                let trimmed = t.trim();
                !trimmed.starts_with('/')
                    && !trimmed.contains("music.apple.com")
                    && !trimmed.contains("qobuz.com")
                    && (trimmed.starts_with("http://")
                        || trimmed.starts_with("https://")
                        || msg.reply_to_message_id().is_some())
            } else {
                false
            }
        }),
        move |msg| {
            let state = Arc::clone(&auto_state);
            async move {
                handle_admin_url_config(state, msg).await;
            }
        },
    );
}
