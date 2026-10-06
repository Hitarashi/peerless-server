pub mod cancel;
pub mod gates;
pub mod input;

use std::sync::Arc;

use engine::orchestrator::deps::CollectionResolver;
use ferogram::{
    InputMessage,
    filters::{self, Dispatcher},
};

use crate::BotState;

pub fn register(dp: &mut Dispatcher, state: Arc<BotState>) {
    let get_state = Arc::clone(&state);
    dp.on_message(filters::command("get"), move |msg| {
        let state = Arc::clone(&get_state);
        async move { handle_command(state, msg).await }
    });
    let cancel_state = Arc::clone(&state);
    dp.on_message(
        filters::custom(|msg| {
            msg.text().is_some_and(|t| {
                let cmd = t.split_whitespace().next().unwrap_or("");
                let base = cmd.split('@').next().unwrap_or(cmd);
                base.starts_with("/cancel_") && base.len() > "/cancel_".len()
            })
        }),
        move |msg| {
            let state = Arc::clone(&cancel_state);
            async move { handle_cancel_id_command(state, msg).await }
        },
    );
}

async fn handle_command(state: Arc<BotState>, msg: ferogram::update::IncomingMessage) {
    let caller_id = msg.sender_user_id().unwrap_or_default();
    let chat = super::marked_chat_id(&msg);
    if !state
        .auth
        .is_authorized(caller_id, Some(chat))
        .await
        .unwrap_or(false)
    {
        return;
    }

    let caller_is_admin = state.auth.is_admin(caller_id);

    let parsed = input::parse_message(&state.client, &msg, chat, false).await;
    if parsed.items.is_empty() {
        reply(&msg, gates::usage(caller_is_admin)).await;
        return;
    }

    let (owner_id, owner_display_name, owner_is_admin) =
        if let (true, Some(id)) = (parsed.from_reply, parsed.reply_sender_id) {
            let is_admin = state.auth.is_admin(id);
            let name = parsed
                .reply_sender_name
                .unwrap_or_else(|| format!("User {id}"));
            (id, name, is_admin)
        } else {
            let user = msg.sender_user().await.ok().flatten();
            let name = user
                .as_ref()
                .and_then(|u| {
                    u.username()
                        .filter(|n| !n.trim().is_empty())
                        .map(|n| format!("@{n}"))
                })
                .or_else(|| {
                    user.as_ref().map(|u| {
                        let first = u.first_name().unwrap_or_default().trim();
                        match u.last_name().map(str::trim).filter(|l| !l.is_empty()) {
                            Some(last) if !first.is_empty() => format!("{first} {last}"),
                            Some(last) => last.to_owned(),
                            None if !first.is_empty() => first.to_owned(),
                            None => String::new(),
                        }
                    })
                })
                .filter(|name| !name.is_empty())
                .unwrap_or_else(|| format!("User {caller_id}"));
            (caller_id, name, caller_is_admin)
        };

    let is_cache_only = if parsed.from_reply {
        owner_is_admin
    } else {
        caller_is_admin
    };

    let effective_admin = caller_is_admin || owner_is_admin;
    if let Some(text) = gates::cache_gate(is_cache_only, effective_admin) {
        reply(&msg, text).await;
        return;
    }
    if let Some(text) = gates::force_gate(parsed.force, effective_admin) {
        reply(&msg, text).await;
        return;
    }
    let settings = state.rip_deps.settings_snapshot();
    if let Some(text) =
        gates::feature_gate(&settings, &parsed.items, effective_admin, parsed.document)
    {
        reply(&msg, text).await;
        return;
    }

    let is_group = chat != owner_id;

    let mut delivery_chat_id = chat;
    if is_group && !is_cache_only {
        let note = InputMessage::html(
            "<b>Download queued</b><br/>Tracks requested in this chat will be delivered to your private chat."
        )
        .silent(true);
        match state
            .client
            .send_message(ferogram::PeerRef::from(owner_id), note)
            .await
        {
            Ok(_) => delivery_chat_id = owner_id,
            Err(_) => {
                let bot_username = state
                    .client
                    .get_me()
                    .await
                    .ok()
                    .and_then(|me| me.username)
                    .unwrap_or_else(|| "peerless_bot".to_owned());
                let keyboard = ferogram::keyboard::InlineKeyboard::new()
                    .row([ferogram::keyboard::Button::url(
                        "Start bot in private chat",
                        format!("https://t.me/{bot_username}?start=start"),
                    )])
                    .into_markup();
                let text = if owner_id != caller_id {
                    format!(
                        "! <b>Private chat required for {}</b><br/><br/>Audio files are delivered to private chat to keep this group clean.<br/>Start the bot in private chat, then send your request again.",
                        crate::html::escape(&owner_display_name)
                    )
                } else {
                    "! <b>Private chat required</b><br/><br/>Audio files are delivered to your private chat to keep this group clean.<br/>Start the bot in private chat, then send your request again.".to_owned()
                };
                let _ = msg
                    .reply(InputMessage::html(text).reply_markup(keyboard))
                    .await;
                return;
            }
        }
    }

    let rendition_policy = engine::orchestrator::types::RenditionPolicy::PrimaryWithOptionalAtmos;

    let has_artist = parsed
        .items
        .iter()
        .any(|it| it.kind == engine::types::TargetKind::Artist);
    let should_fan_out = parsed.document || parsed.items.len() > 1 || has_artist;

    if should_fan_out {
        let settings = state.rip_deps.settings().get_settings();
        let default_storefront = engine::settings::resolve_default_storefront(&settings);
        let mut expanded_items = Vec::new();
        for item in &parsed.items {
            if item.kind == engine::types::TargetKind::Artist {
                let effective_sf = Some(
                    item.storefront
                        .as_deref()
                        .or(parsed.storefront.as_deref())
                        .unwrap_or(default_storefront),
                );
                match state
                    .rip_deps
                    .fetch_artist_album_ids(
                        &item.id,
                        effective_sf.map(Into::into).unwrap_or_default(),
                    )
                    .await
                {
                    Ok(album_ids) => {
                        for aid in album_ids {
                            expanded_items.push(engine::types::ParsedTargetItem {
                                id: aid,
                                kind: engine::types::TargetKind::Album,
                                storefront: effective_sf.map(str::to_owned),
                            });
                        }
                    }
                    Err(error) => {
                        tracing::error!(error = %error, "failed to resolve artist albums");
                        reply(&msg, &format!("Failed to resolve artist albums: {error}")).await;
                        return;
                    }
                }
            } else {
                expanded_items.push(item.clone());
            }
        }

        if expanded_items.is_empty() {
            reply(
                &msg,
                "No tracks, albums, playlists, or artist albums found to download.",
            )
            .await;
            return;
        }

        let max_collection_limit = settings.max_collection_tracks;
        let rip_orchestrator = Arc::clone(&state.rip_orchestrator);
        let rip_deps = Arc::clone(&state.rip_deps);
        let admission_group_id = format!("get-batch:{chat}:{}", msg.id());
        let default_storefront = default_storefront.to_owned();
        let base_options = engine::orchestrator::types::RipTaskOptions {
            chat_id: chat,
            user_id: owner_id,
            user_name: Some(owner_display_name.clone()),
            delivery_chat_id,
            is_group,
            is_force: parsed.force,
            is_cache_only,
            single_storefront: parsed.storefront.clone(),
            parsed_items: Vec::new(),
            reply_to_message_id: Some(i64::from(msg.id())),
            is_admin: owner_is_admin,
            codec_preference: parsed.codec_preference,
            rendition_policy,
        };

        tokio::spawn(async move {
            let mut selected_tracks = 0usize;
            let mut selected_items = Vec::with_capacity(expanded_items.len());
            for item in expanded_items {
                if !owner_is_admin
                    && max_collection_limit > 0
                    && selected_tracks >= max_collection_limit as usize
                {
                    tracing::info!(
                        user_id = owner_id,
                        selected_tracks,
                        max_collection_limit,
                        "batch reached collection limit, stopping"
                    );
                    break;
                }

                if !owner_is_admin && max_collection_limit > 0 {
                    let storefront = item
                        .storefront
                        .as_deref()
                        .or(base_options.single_storefront.as_deref())
                        .unwrap_or(&default_storefront);
                    let track_count = match item.kind {
                        engine::types::TargetKind::Track => Ok(1),
                        engine::types::TargetKind::Album => rip_deps
                            .fetch_album_tracks(
                                &item.id,
                                storefront.into(),
                            )
                            .await
                            .map(|album| album.tracks.len()),
                        engine::types::TargetKind::Playlist => rip_deps
                            .fetch_playlist_tracks(
                                &item.id,
                                storefront.into(),
                            )
                            .await
                            .map(|playlist| playlist.tracks.len()),
                        engine::types::TargetKind::Artist => {
                            Err("artist links must be expanded before starting album jobs"
                                .to_owned())
                        }
                    };
                    match track_count {
                        Ok(count) => {
                            selected_tracks = selected_tracks
                                .saturating_add(count.min(max_collection_limit as usize));
                        }
                        Err(error) => tracing::warn!(
                            item_id = %item.id,
                            error = %error,
                            "could not estimate collection size before scheduling its job"
                        ),
                    }
                }
                selected_items.push(item);
            }

            let mut jobs = tokio::task::JoinSet::new();
            for item in selected_items {
                let item_id = item.id.clone();
                let mut item_options = base_options.clone();
                item_options.parsed_items = vec![item];
                let rip_orchestrator = Arc::clone(&rip_orchestrator);
                let rip_deps = Arc::clone(&rip_deps);
                let admission_group_id = admission_group_id.clone();
                jobs.spawn(async move {
                    let result = rip_orchestrator
                        .start_task_in_group(rip_deps, &item_options, admission_group_id)
                        .await;
                    (item_id, result)
                });
            }

            let mut total_tracks = 0usize;
            while let Some(result) = jobs.join_next().await {
                match result {
                    Ok((item_id, Ok(summary))) => {
                        total_tracks = total_tracks.saturating_add(summary.total_tracks);
                        tracing::info!(
                            user_id = owner_id,
                            item_id,
                            tracks = summary.total_tracks,
                            "batch item job completed"
                        );
                    }
                    Ok((item_id, Err(engine::orchestrator::OrchestratorError::Cancelled))) => {
                        tracing::info!(user_id = owner_id, item_id, "batch item job cancelled");
                    }
                    Ok((item_id, Err(error))) => {
                        tracing::warn!(
                            item_id,
                            error = %error,
                            "batch item job failed"
                        );
                    }
                    Err(error) => tracing::error!(
                        user_id = owner_id,
                        error = %error,
                        "batch item task panicked"
                    ),
                }
            }
            tracing::info!(user_id = owner_id, total_tracks, "batch rip completed");
        });
        return;
    }

    let options = engine::orchestrator::types::RipTaskOptions {
        chat_id: chat,
        user_id: owner_id,
        user_name: Some(owner_display_name),
        delivery_chat_id,
        is_group,
        is_force: parsed.force,
        is_cache_only,
        single_storefront: parsed.storefront,
        parsed_items: parsed.items,
        reply_to_message_id: Some(i64::from(msg.id())),
        is_admin: owner_is_admin,
        codec_preference: parsed.codec_preference,
        rendition_policy,
    };

    if let Err(error) = state
        .rip_orchestrator
        .start_task(Arc::clone(&state.rip_deps), &options)
        .await
    {
        tracing::error!(error = %error, "get job failed");
    }
}

async fn handle_cancel_id_command(state: Arc<BotState>, msg: ferogram::update::IncomingMessage) {
    let text = msg.text().unwrap_or_default();
    let first_word = text.split_whitespace().next().unwrap_or("");
    let raw_cmd = first_word.split('@').next().unwrap_or(first_word);
    let Some(job_id) = raw_cmd.strip_prefix("/cancel_") else {
        return;
    };
    if job_id.is_empty() {
        return;
    }
    let caller = msg.sender_user_id().unwrap_or_default();
    let admin = state.auth.is_admin(caller);
    match cancel::cancel_inline(&state, job_id, caller, admin) {
        cancel::CancelResult::Cancelled => {
            reply(&msg, cancel::COMMAND_ACK).await;
        }
        cancel::CancelResult::Unauthorized => {
            reply(&msg, cancel::CALLBACK_UNAUTHORIZED).await;
        }
        cancel::CancelResult::Expired => {
            reply(&msg, cancel::CALLBACK_EXPIRED).await;
        }
    }
}

async fn reply(msg: &ferogram::update::IncomingMessage, text: &str) {
    let _ = msg.reply(InputMessage::html(text)).await;
}
