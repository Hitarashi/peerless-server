//! Finalization guards, the finalize lane marker, and ZIP build/delivery finalisation.

use super::*;

pub(super) type FinalizeResult = Result<RipTaskSummary, String>;
pub(super) type FinalizeSender = tokio::sync::oneshot::Sender<FinalizeResult>;

pub(super) struct FinalizationGuard {
    pub(super) summary_tx: Arc<Mutex<Option<FinalizeSender>>>,
    pub(super) workspace_paths: Vec<PathBuf>,
}

impl FinalizationGuard {
    pub(super) fn new(
        summary_tx: Arc<Mutex<Option<FinalizeSender>>>,
        rip_job_dir: PathBuf,
        zip_states: &[Arc<ZipState>],
    ) -> Self {
        let mut workspace_paths = Vec::with_capacity(zip_states.len() + 1);
        workspace_paths.push(rip_job_dir);
        workspace_paths.extend(zip_states.iter().map(|state| state.dir.clone()));
        Self {
            summary_tx,
            workspace_paths,
        }
    }

    pub(super) fn send(&mut self, result: FinalizeResult) {
        if let Some(tx) = self
            .summary_tx
            .lock()
            .expect("summary sender poisoned")
            .take()
        {
            let _ = tx.send(result);
        }
    }

    pub(super) async fn finish(&mut self, result: FinalizeResult) {
        for path in &self.workspace_paths {
            let _ = tokio::fs::remove_dir_all(path).await;
        }
        self.send(result);
    }
}

impl Drop for FinalizationGuard {
    fn drop(&mut self) {
        for path in &self.workspace_paths {
            let _ = std::fs::remove_dir_all(path);
        }
        self.send(Err("finalize marker stopped unexpectedly".to_owned()));
    }
}

pub(super) struct FinalizingRenditionGuard {
    pub(super) rendition: Arc<Mutex<Option<Rendition>>>,
}

impl FinalizingRenditionGuard {
    pub(super) fn new(rendition: Arc<Mutex<Option<Rendition>>>) -> Self {
        Self { rendition }
    }
}

impl Drop for FinalizingRenditionGuard {
    fn drop(&mut self) {
        if !std::thread::panicking()
            && let Ok(mut rendition) = self.rendition.lock()
        {
            *rendition = None;
        }
    }
}

pub(super) async fn enqueue_finalize_marker<D>(
    deps: Arc<D>,
    bus: EventBus,
    shared: Arc<Mutex<TaskShared>>,
    ctx: Arc<TaskContext>,
    job_controller: CancellationToken,
    upload_lane: Arc<Mutex<Option<tokio::sync::mpsc::Sender<LaneTask>>>>,
    finalization_guard: FinalizationGuard,
) where
    D: AlbumCache + Delivery + ProviderDeps + 'static,
{
    let marker_panic_shared = Arc::clone(&shared);
    let marker_panic_ctx = Arc::clone(&ctx);
    let marker_controller = job_controller.clone();
    let pushed = push_lane_task(&upload_lane, "finalize_job", None, None, None, move || {
        Box::pin(async move {
            let mut finalization_guard = finalization_guard;
            let result = futures_util::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(
                finalize_job(deps, bus, shared, ctx, marker_controller),
            ))
            .await
            .unwrap_or_else(|_| {
                let optional_atmos = marker_panic_ctx
                    .finalizing_rendition
                    .lock()
                    .ok()
                    .and_then(|rendition| *rendition)
                    == Some(Rendition::Atmos);
                if optional_atmos {
                    tracing::warn!(
                        "optional Atmos ZIP finalization panicked; keeping primary result"
                    );
                    Ok(build_job_summary(
                        &marker_panic_shared,
                        &marker_panic_ctx,
                        None,
                    ))
                } else {
                    Err("finalize marker panicked".to_owned())
                }
            });
            finalization_guard.finish(result).await;
        })
    })
    .await;
    if !pushed {
        tracing::error!(lane = "upload", "failed to enqueue finalize marker");
    }
}

pub(super) fn build_job_summary(
    shared: &Arc<Mutex<TaskShared>>,
    ctx: &TaskContext,
    zip_delivery: Option<ZipDeliveryInfo>,
) -> RipTaskSummary {
    let total_elapsed_sec = format!(
        "{:.1}",
        (now_ms().saturating_sub(ctx.queue_start_time_ms)) as f64 / 1000.0
    );
    let failed = ctx.failed_tracks.lock().expect("failures poisoned").clone();
    let first_msg_id = *ctx.first_delivered_msg_id.lock().unwrap();
    let zip_deliveries = ctx.zip_delivery_infos.lock().unwrap().clone();
    let guard = shared.lock().expect("job poisoned");
    let codec = guard
        .progress
        .codec
        .lock()
        .expect("codec poisoned")
        .clone()
        .or_else(|| zip_deliveries.first().and_then(|z| z.codec.clone()));
    RipTaskSummary {
        job_id: guard.job.id.clone(),
        job_header: guard.job.job_header.clone(),
        total_tracks: guard.job.total_tracks,
        cached_count: guard.job.cached_count,
        ripped_count: ctx.ripped_count.load(std::sync::atomic::Ordering::SeqCst),
        failed_count: failed.len(),
        failed_tracks: failed,
        skipped_uncached_tracks: Vec::new(),
        total_elapsed_sec,
        capped_count: ctx.capped_count,
        max_collection_limit: ctx.max_collection_limit,
        is_cache_only: ctx.options.is_cache_only,
        is_group: ctx.options.is_group,
        warnings: ctx
            .atmos_warning
            .lock()
            .expect("atmos poisoned")
            .clone()
            .into_iter()
            .chain(ctx.warnings.clone())
            .collect(),
        zip_delivery: zip_deliveries.first().cloned().or(zip_delivery),
        zip_deliveries,
        first_delivered_msg_id: first_msg_id,
        codec,
    }
}

pub(super) async fn finalize_job<D>(
    deps: Arc<D>,
    bus: EventBus,
    shared: Arc<Mutex<TaskShared>>,
    ctx: Arc<TaskContext>,
    job_controller: CancellationToken,
) -> FinalizeResult
where
    D: AlbumCache + Delivery + ProviderDeps,
{
    if let Some(error) = fatal_error(&ctx) {
        return Err(error);
    }
    let zip_delivery = finalize_zip(&deps, &bus, &shared, &ctx, &job_controller).await?;
    if let Some(error) = fatal_error(&ctx) {
        return Err(error);
    }
    Ok(build_job_summary(&shared, &ctx, zip_delivery))
}

pub(super) async fn finalize_zip<D>(
    deps: &Arc<D>,
    bus: &EventBus,
    shared: &Arc<Mutex<TaskShared>>,
    ctx: &Arc<TaskContext>,
    job_controller: &CancellationToken,
) -> Result<Option<ZipDeliveryInfo>, String>
where
    D: AlbumCache + Delivery + ProviderDeps,
{
    let result = finalize_zip_inner(deps, bus, shared, ctx, job_controller).await;
    let cancelled =
        shared.lock().expect("job poisoned").job.is_cancelled || job_controller.is_cancelled();
    if result.is_err() || cancelled {
        let message_ids = take_uncommitted_zip_dump_messages(ctx);
        if !message_ids.is_empty()
            && let Err(error) = deps.retract_dump(&message_ids).await
        {
            tracing::error!(%error, "failed to retract ZIP dump publication");
        }
    }
    result
}

pub(super) async fn deliver_cached_zip_rows_direct<D>(
    deps: &Arc<D>,
    shared: &Arc<Mutex<TaskShared>>,
    options: &RipTaskOptions,
    rendition: Rendition,
    rows: &[CachedAlbum],
    job_controller: &CancellationToken,
    first_delivered_msg_id: &mut Option<ChatMessageRef>,
) -> Result<(usize, i64), String>
where
    D: Delivery,
{
    let reply_to = (options.delivery_chat_id == options.chat_id)
        .then_some(options.reply_to_message_id)
        .flatten();
    let mut delivered = 0usize;
    let mut size = 0i64;
    for row in rows {
        if shared.lock().expect("job poisoned").job.is_cancelled || job_controller.is_cancelled() {
            return Ok((delivered, size));
        }
        let result = tokio::select! {
            res = deps.deliver_to_chat(ChatDelivery::DumpCopy {
                destination: ChatRef::new(options.delivery_chat_id),
                source: DumpMessageRef::new(row.message_id),
                reply_to: reply_to.map(ChatMessageRef::new),
                silent: rows.len() > 1,
            }) => res,
            _ = job_controller.cancelled() => return Ok((delivered, size)),
        };
        match result {
            Ok(DeliveryReceipt::Message(sent_id)) => {
                if shared.lock().expect("job poisoned").job.is_cancelled
                    || job_controller.is_cancelled()
                {
                    return Ok((delivered, size));
                }
                if first_delivered_msg_id.is_none() {
                    *first_delivered_msg_id = Some(sent_id);
                }
                delivered += 1;
                size += row.file_size;
                if rows.len() > 1 {
                    tokio::time::sleep(tokio::time::Duration::from_millis(120)).await;
                }
            }
            Ok(DeliveryReceipt::PreviewDelivered) if rendition == Rendition::Primary => {
                return Err("primary ZIP delivery returned a preview receipt".to_owned());
            }
            Ok(DeliveryReceipt::PreviewDelivered) => {
                tracing::warn!(
                    rendition = ?rendition,
                    "optional Atmos ZIP delivery returned a preview receipt"
                );
            }
            Err(error) if rendition == Rendition::Primary => {
                return Err(format!("primary ZIP delivery failed: {error}"));
            }
            Err(error) => {
                tracing::warn!(%error, rendition = ?rendition, "optional Atmos ZIP delivery failed");
            }
        }
    }
    Ok((delivered, size))
}

pub(super) async fn deliver_cached_zip_rows<D>(
    deps: &Arc<D>,
    shared: &Arc<Mutex<TaskShared>>,
    ctx: &Arc<TaskContext>,
    rendition: Rendition,
    rows: &[CachedAlbum],
    job_controller: &CancellationToken,
) -> Result<(usize, i64), String>
where
    D: Delivery,
{
    let options = &ctx.options;
    let reply_to = (options.delivery_chat_id == options.chat_id)
        .then_some(options.reply_to_message_id)
        .flatten();
    let mut delivered = 0usize;
    let mut size = 0i64;
    for row in rows {
        if shared.lock().expect("job poisoned").job.is_cancelled || job_controller.is_cancelled() {
            return Ok((delivered, size));
        }
        let result = deps
            .deliver_to_chat(ChatDelivery::DumpCopy {
                destination: ChatRef::new(options.delivery_chat_id),
                source: DumpMessageRef::new(row.message_id),
                reply_to: reply_to.map(ChatMessageRef::new),
                silent: rows.len() > 1,
            })
            .await;
        match result {
            Ok(DeliveryReceipt::Message(sent_id)) => {
                if shared.lock().expect("job poisoned").job.is_cancelled
                    || job_controller.is_cancelled()
                {
                    return Ok((delivered, size));
                }
                let mut first = ctx.first_delivered_msg_id.lock().unwrap();
                if first.is_none() {
                    *first = Some(sent_id);
                }
                delivered += 1;
                size += row.file_size;
            }
            Ok(DeliveryReceipt::PreviewDelivered) if rendition == Rendition::Primary => {
                return Err("primary ZIP delivery returned a preview receipt".to_owned());
            }
            Ok(DeliveryReceipt::PreviewDelivered) => {
                tracing::warn!(
                    rendition = ?rendition,
                    "optional Atmos ZIP delivery returned a preview receipt"
                );
            }
            Err(error) if rendition == Rendition::Primary => {
                return Err(format!("primary ZIP delivery failed: {error}"));
            }
            Err(error) => {
                tracing::warn!(%error, rendition = ?rendition, "optional Atmos ZIP delivery failed");
            }
        }
    }
    Ok((delivered, size))
}

pub(super) async fn finalize_zip_inner<D>(
    deps: &Arc<D>,
    bus: &EventBus,
    shared: &Arc<Mutex<TaskShared>>,
    ctx: &Arc<TaskContext>,
    job_controller: &CancellationToken,
) -> Result<Option<ZipDeliveryInfo>, String>
where
    D: AlbumCache + Delivery + ProviderDeps,
{
    let options = &ctx.options;
    if !ctx.zip_build {
        return Ok(None);
    }
    if let Some(error) = primary_zip_error(ctx) {
        return Err(error);
    }
    let finalizing_guard = FinalizingRenditionGuard::new(Arc::clone(&ctx.finalizing_rendition));
    let is_cancelled =
        || shared.lock().expect("job poisoned").job.is_cancelled || job_controller.is_cancelled();
    if is_cancelled() {
        return Ok(None);
    }
    let expected_tracks = shared.lock().expect("job poisoned").job.total_tracks;
    let failures = ctx.failed_tracks.lock().expect("failures poisoned").clone();
    let mut first_delivery = None;

    for state in &ctx.zip_states {
        *ctx.finalizing_rendition
            .lock()
            .expect("finalizing rendition poisoned") = Some(state.rendition);
        if is_cancelled() {
            return Ok(first_delivery);
        }
        if let Some(error) = primary_zip_error(ctx) {
            return Err(error);
        }
        if let Some(rows) = ctx.zip_reuse.get(&state.rendition) {
            if ctx.zip_deliver && !options.is_cache_only {
                let (delivered, size) = deliver_cached_zip_rows(
                    deps,
                    shared,
                    ctx,
                    state.rendition,
                    rows,
                    job_controller,
                )
                .await?;
                if delivered > 0 {
                    let codec = rows[0].codec.as_str().to_owned();
                    let info = ZipDeliveryInfo {
                        album: ctx.zip_album.clone(),
                        artist: ctx.zip_artist.clone(),
                        release_year: ctx.zip_release_date.chars().take(4).collect(),
                        total_tracks: expected_tracks,
                        delivered_tracks: if state.rendition == Rendition::Primary {
                            Some(expected_tracks)
                        } else {
                            ctx.zip_reuse_atmos_track_count
                        },
                        total_parts: delivered,
                        size_bytes: size,
                        is_partial: false,
                        album_id: ctx.zip_album_id.clone(),
                        album_url: ctx.zip_album_url.clone(),
                        artwork_url: ctx.zip_artwork_url.clone(),
                        genre: ctx.zip_genre.clone(),
                        record_label: ctx.zip_record_label.clone(),
                        copyright: ctx.zip_copyright.clone(),
                        photo_delivered: false,
                        codec: Some(codec),
                    };
                    if first_delivery.is_none() {
                        first_delivery = Some(info.clone());
                    }
                    ctx.zip_delivery_infos.lock().unwrap().push(info);
                }
            }
            continue;
        }
        let mut replacement_uploads = Vec::new();
        let mut rendition_dump_messages = Vec::new();
        let mut entries = state.sources.lock().expect("zip sources poisoned").clone();
        if entries.is_empty() {
            continue;
        }
        entries.sort_by(|a, b| a.archive_filename.cmp(&b.archive_filename));
        let complete = match state.rendition {
            Rendition::Primary => failures.is_empty() && entries.len() == expected_tracks,
            Rendition::Atmos => true,
        };
        let should_publish =
            complete || (ctx.zip_deliver && !options.is_cache_only && !entries.is_empty());
        if !should_publish {
            continue;
        }

        let default_codec = "alac";
        let default_enum_codec = Codec::Alac;
        let codec = state
            .codec
            .lock()
            .expect("zip codec poisoned")
            .clone()
            .or_else(|| (state.rendition == Rendition::Atmos).then(|| "ec-3".to_owned()))
            .unwrap_or_else(|| default_codec.to_owned());
        let album_codec = codec.parse::<Codec>().unwrap_or(default_enum_codec);

        if complete && !(options.is_force && options.is_admin) {
            if let Ok(existing_rows) = deps.find_albums(&ctx.zip_album_id, Some(album_codec)).await {
                let matches_hash = state
                    .generation_hash
                    .as_deref()
                    .map(|hash| existing_rows.iter().all(|r| r.generation_hash == hash))
                    .unwrap_or(true);
                let total_parts = existing_rows.first().map(|r| r.total_parts.max(1) as usize).unwrap_or(0);
                let complete_parts = !existing_rows.is_empty()
                    && existing_rows.len() == total_parts
                    && (1..=total_parts).zip(&existing_rows).all(|(n, r)| r.part_index as usize == n);
                if matches_hash && complete_parts {
                    tracing::info!(
                        album_id = %ctx.zip_album_id,
                        codec = %album_codec.as_str(),
                        "Album ZIP already exists in database cache; cancelling upload and delivering existing ZIP"
                    );
                    if ctx.zip_deliver && !options.is_cache_only {
                        let (delivered, size) = deliver_cached_zip_rows(
                            deps,
                            shared,
                            ctx,
                            state.rendition,
                            &existing_rows,
                            job_controller,
                        )
                        .await?;
                        if delivered > 0 {
                            let codec = existing_rows[0].codec.as_str().to_owned();
                            let info = ZipDeliveryInfo {
                                album: ctx.zip_album.clone(),
                                artist: ctx.zip_artist.clone(),
                                release_year: ctx.zip_release_date.chars().take(4).collect(),
                                total_tracks: expected_tracks,
                                delivered_tracks: if state.rendition == Rendition::Primary {
                                    Some(expected_tracks)
                                } else {
                                    ctx.zip_reuse_atmos_track_count
                                },
                                total_parts: delivered,
                                size_bytes: size,
                                is_partial: false,
                                album_id: ctx.zip_album_id.clone(),
                                album_url: ctx.zip_album_url.clone(),
                                artwork_url: ctx.zip_artwork_url.clone(),
                                genre: ctx.zip_genre.clone(),
                                record_label: ctx.zip_record_label.clone(),
                                copyright: ctx.zip_copyright.clone(),
                                photo_delivered: false,
                                codec: Some(codec),
                            };
                            if first_delivery.is_none() {
                                first_delivery = Some(info.clone());
                            }
                            ctx.zip_delivery_infos.lock().unwrap().push(info);
                        }
                    }
                    continue;
                }
            }
        }

        let cover_bytes = match &ctx.zip_artwork_url {
            Some(url) => deps.fetch_artwork(url).await,
            None => None,
        };
        if is_cancelled() {
            return Ok(first_delivery);
        }
        let cover_path = match &cover_bytes {
            Some(bytes) => {
                let path = state.dir.join("cover.jpg");
                tokio::fs::write(&path, bytes).await.ok().map(|_| path)
            }
            None => None,
        };
        let thumb_path = match &ctx.zip_artwork_url {
            Some(url) if !url.is_empty() => {
                let thumb_url = deps.artwork_url_at_size(url, 320);
                match deps.fetch_artwork(&thumb_url).await {
                    Some(bytes) if !bytes.is_empty() => {
                        let path = state.dir.join("cover_thumb.jpg");
                        tokio::fs::write(&path, bytes).await.ok().map(|_| path)
                    }
                    _ => None,
                }
            }
            _ => None,
        };
        if is_cancelled() {
            return Ok(first_delivery);
        }
        let thumb_path_str = thumb_path
            .as_deref()
            .map(|path| path.to_string_lossy().into_owned());
        let plans = match plan_zip_parts_with_codec(
            &ctx.zip_artist,
            &ctx.zip_album,
            &ctx.zip_release_date,
            &entries,
            cover_path,
            TELEGRAM_SPLIT_THRESHOLD_BYTES,
            &codec,
        ) {
            Ok(plans) => plans,
            Err(error) => {
                if state.rendition == Rendition::Primary {
                    return Err(format!("primary ZIP planning failed: {error}"));
                }
                tracing::warn!(%error, "optional Atmos ZIP planning failed");
                continue;
            }
        };
        let mut delivered_parts = 0usize;
        let mut delivered_size = 0i64;
        let expected_parts = plans.len();
        let mut all_parts_uploaded = true;
        enum ZipSendOutcome {
            Published(DumpPublication),
            Delivered,
        }
        for plan in plans {
            let archive_name: StandardFilename = if complete {
                plan.archive_filename.clone().into_standard()
            } else {
                plan.archive_filename.clone().into_partial()
            };
            let output = state.dir.join(&archive_name);
            let bus_clone = bus.clone();
            let shared_clone = Arc::clone(shared);
            let cancel = job_controller.clone();
            let output_clone = output.clone();
            let plan_clone = plan.clone();
            let title_clone = archive_name.as_str().to_string();
            if is_cancelled() {
                return Ok(first_delivery);
            }
            bus.set_codec(shared, Some(album_codec.as_str().to_string()));
            let build = tokio::task::spawn_blocking(move || {
                bus_clone.set_upload(
                    &shared_clone,
                    Some(UploadLane::ArchiveBuild {
                        archive: title_clone.clone(),
                        progress: ByteProgress {
                            completed: 0,
                            total: None,
                        },
                    }),
                );
                bus_clone.emit_progress(&shared_clone);
                let progress_bus = bus_clone.clone();
                let progress_shared = Arc::clone(&shared_clone);
                let progress_title = title_clone.clone();
                let on_progress = move |uploaded: u64, total: u64| {
                    progress_bus.set_upload(
                        &progress_shared,
                        Some(UploadLane::ArchiveBuild {
                            archive: progress_title.clone(),
                            progress: ByteProgress {
                                completed: uploaded,
                                total: Some(total),
                            },
                        }),
                    );
                    progress_bus.emit_progress(&progress_shared);
                };
                let result = create_zip_archive(
                    &output_clone,
                    &plan_clone,
                    Some(&on_progress),
                    Some(&cancel),
                );
                bus_clone.set_upload(&shared_clone, None);
                bus_clone.emit_progress(&shared_clone);
                result
            })
            .await;
            if is_cancelled() {
                let _ = tokio::fs::remove_file(&output).await;
                return Ok(first_delivery);
            }
            let size = match build {
                Ok(Ok(size)) => size,
                Ok(Err(error)) => {
                    let _ = tokio::fs::remove_file(&output).await;
                    if state.rendition == Rendition::Primary {
                        return Err(format!("primary ZIP build failed: {error}"));
                    }
                    all_parts_uploaded = false;
                    tracing::warn!(%error, "optional Atmos ZIP build failed");
                    continue;
                }
                Err(error) => {
                    let _ = tokio::fs::remove_file(&output).await;
                    if state.rendition == Rendition::Primary {
                        return Err(format!("primary ZIP build task failed: {error}"));
                    }
                    all_parts_uploaded = false;
                    tracing::warn!(%error, "optional Atmos ZIP build task failed");
                    continue;
                }
            };
            if size > TELEGRAM_SPLIT_THRESHOLD_BYTES {
                let _ = tokio::fs::remove_file(&output).await;
                if state.rendition == Rendition::Primary {
                    return Err("primary ZIP exceeds the upload size limit".to_owned());
                }
                all_parts_uploaded = false;
                tracing::warn!("optional Atmos ZIP exceeds the upload size limit");
                continue;
            }
            let caption = ZipCaption {
                album_id: ctx.zip_album_id.clone(),
                codec: Some(album_codec.as_str().to_owned()),
                part_index: plan.part_index as i32,
                total_parts: plan.total_parts as i32,
                generation_hash: state.generation_hash.as_deref().unwrap_or("").to_owned(),
                is_complete: complete,
            };
            let path = output.to_string_lossy().into_owned();
            if is_cancelled() {
                let _ = tokio::fs::remove_file(&output).await;
                return Ok(first_delivery);
            }
            let on_upload: UploadProgressCallback = {
                let shared = Arc::clone(shared);
                let bus = bus.clone();
                let title = archive_name.as_str().to_string();
                Arc::new(move |uploaded, total| {
                    bus.set_upload(
                        &shared,
                        Some(UploadLane::ArchiveUpload {
                            archive: title.clone(),
                            progress: ByteProgress {
                                completed: uploaded,
                                total: Some(total),
                            },
                        }),
                    );
                    bus.emit_progress(&shared);
                })
            };
            bus.set_codec(shared, Some(album_codec.as_str().to_string()));
            bus.set_upload(
                shared,
                Some(UploadLane::ArchiveUpload {
                    archive: archive_name.as_str().to_string(),
                    progress: ByteProgress {
                        completed: 0,
                        total: None,
                    },
                }),
            );
            bus.emit_progress(shared);
            let upload: Result<ZipSendOutcome, DeliveryError> = if complete {
                tokio::select! {
                    result = deps.publish_to_dump(DumpPublish::ZipDocument {
                        file_path: path.clone(),
                        thumb_path: thumb_path_str.clone(),
                        caption: caption.clone(),
                        on_upload_progress: Some(Arc::clone(&on_upload)),
                    }) => result.map(ZipSendOutcome::Published),
                    _ = job_controller.cancelled() => {
                        bus.set_upload(shared, None);
                        bus.emit_progress(shared);
                        return Ok(first_delivery);
                    }
                }
            } else {
                let chat_upload = tokio::select! {
                    result = deps.deliver_to_chat(ChatDelivery::ZipDocument {
                        destination: ChatRef::new(options.delivery_chat_id),
                        file_path: path.clone(),
                        thumb_path: thumb_path_str.clone(),
                        caption: caption.clone(),
                        on_upload_progress: Some(Arc::clone(&on_upload)),
                    }) => result,
                    _ = job_controller.cancelled() => {
                        bus.set_upload(shared, None);
                        bus.emit_progress(shared);
                        return Ok(first_delivery);
                    }
                };
                match chat_upload {
                    Ok(DeliveryReceipt::Message(sent_id)) => {
                        let mut first = ctx.first_delivered_msg_id.lock().unwrap();
                        if first.is_none() {
                            *first = Some(sent_id);
                        }
                        Ok(ZipSendOutcome::Delivered)
                    }
                    Ok(DeliveryReceipt::PreviewDelivered) => Err(DeliveryError::UnexpectedMedia),
                    Err(error) => Err(error),
                }
            };
            bus.set_upload(shared, None);
            bus.emit_progress(shared);
            match upload {
                Ok(ZipSendOutcome::Published(upload)) if complete => {
                    rendition_dump_messages.push(upload.message);
                    remember_zip_dump_message(ctx, upload.message);
                    if is_cancelled() {
                        return Ok(first_delivery);
                    }
                    if ctx.zip_deliver && !options.is_cache_only {
                        if is_cancelled() {
                            return Ok(first_delivery);
                        }
                        let reply_to = (options.delivery_chat_id == options.chat_id)
                            .then_some(options.reply_to_message_id)
                            .flatten();
                        let copy_result = tokio::select! {
                            result = deps.deliver_to_chat(ChatDelivery::DumpCopy {
                                destination: ChatRef::new(options.delivery_chat_id),
                                source: upload.message,
                                reply_to: reply_to.map(ChatMessageRef::new),
                                silent: plan.total_parts > 1,
                            }) => result,
                            _ = job_controller.cancelled() => return Ok(first_delivery),
                        };
                        match copy_result {
                            Ok(DeliveryReceipt::Message(sent_id)) => {
                                if is_cancelled() {
                                    return Ok(first_delivery);
                                }
                                let mut first = ctx.first_delivered_msg_id.lock().unwrap();
                                if first.is_none() {
                                    *first = Some(sent_id);
                                }
                                delivered_parts += 1;
                                delivered_size += size as i64;
                            }
                            Ok(DeliveryReceipt::PreviewDelivered) => {
                                if state.rendition == Rendition::Primary {
                                    return Err("primary ZIP delivery returned a preview receipt"
                                        .to_owned());
                                }
                                tracing::warn!(
                                    "optional Atmos ZIP delivery returned a preview receipt"
                                );
                            }
                            Err(error) if state.rendition == Rendition::Primary => {
                                return Err(format!("primary ZIP delivery failed: {error}"));
                            }
                            Err(error) => {
                                tracing::warn!(
                                    %error,
                                    "optional Atmos ZIP delivery failed"
                                );
                            }
                        }
                    }
                    replacement_uploads.push(AlbumUpload {
                        album_id: ctx.zip_album_id.clone(),
                        codec: album_codec,
                        part_index: plan.part_index as i32,
                        total_parts: plan.total_parts as i32,
                        message_id: upload.message.id(),
                        file_id: upload.file_id,
                        file_unique_id: upload.file_unique_id,
                        file_size: size as i64,
                        generation_hash: state.generation_hash.clone().unwrap_or_default(),
                    });
                }
                Ok(ZipSendOutcome::Delivered) if !complete && !options.is_cache_only => {
                    delivered_parts += 1;
                    delivered_size += size as i64;
                }
                Err(error) if state.rendition == Rendition::Primary => {
                    return Err(format!("primary ZIP upload failed: {error}"));
                }
                Err(error) => {
                    all_parts_uploaded = false;
                    tracing::warn!(%error, "optional Atmos ZIP upload failed");
                }
                Ok(ZipSendOutcome::Delivered) => {}
                Ok(ZipSendOutcome::Published(_)) => {
                    all_parts_uploaded = false;
                    tracing::warn!("ZIP publication was returned for a direct delivery");
                }
            }
        }
        if complete && all_parts_uploaded && replacement_uploads.len() == expected_parts {
            let new_message_ids = replacement_uploads
                .iter()
                .map(|upload| DumpMessageRef::new(upload.message_id))
                .collect::<Vec<_>>();
            let expected = ctx
                .zip_expectations
                .get(&state.rendition)
                .cloned()
                .unwrap_or(AlbumReplacementExpectation::Mixed);
            let replacement = deps
                .replace_albums(
                    &ctx.zip_album_id,
                    album_codec,
                    expected,
                    replacement_uploads,
                )
                .await;
            match replacement {
                Err(AlbumCacheError::Conflict { .. }) => {
                    if let Err(error) = deps.retract_dump(&rendition_dump_messages).await {
                        tracing::error!(
                            %error,
                            rendition = ?state.rendition,
                            "failed to retract ZIP uploads after cache conflict"
                        );
                    }
                    transfer_zip_dump_messages(ctx, &rendition_dump_messages);
                    let winner_rows = match deps
                        .find_albums(
                            &ctx.zip_album_id,
                            Some(album_codec),
                        )
                        .await
                    {
                        Ok(rows) => rows,
                        Err(error) if state.rendition == Rendition::Primary => {
                            return Err(format!(
                                "primary ZIP cache conflict winner lookup failed: {error}"
                            ));
                        }
                        Err(error) => {
                            tracing::warn!(
                                %error,
                                rendition = ?state.rendition,
                                "optional Atmos cache conflict winner lookup failed"
                            );
                            Vec::new()
                        }
                    };
                    if winner_rows.is_empty() {
                        if state.rendition == Rendition::Primary {
                            return Err(
                                "primary ZIP cache conflict had no committed winner".to_owned()
                            );
                        }
                        tracing::warn!(
                            rendition = ?state.rendition,
                            "optional Atmos cache conflict had no committed winner"
                        );
                    } else if ctx.zip_deliver && !options.is_cache_only {
                        let (winner_parts, winner_size) = deliver_cached_zip_rows(
                            deps,
                            shared,
                            ctx,
                            state.rendition,
                            &winner_rows,
                            job_controller,
                        )
                        .await?;
                        delivered_parts = winner_parts;
                        delivered_size = winner_size;
                    }
                    tracing::info!(
                        rendition = ?state.rendition,
                        "discarded ZIP uploads and reused committed cache winner"
                    );
                }
                Err(error) => {
                    if state.rendition == Rendition::Primary {
                        return Err(format!("primary ZIP cache replacement failed: {error}"));
                    }
                    tracing::warn!(%error, "optional Atmos ZIP cache replacement failed");
                    let _ = deps.retract_dump(&rendition_dump_messages).await;
                    transfer_zip_dump_messages(ctx, &rendition_dump_messages);
                }
                Ok(AlbumReplacementResult::Stale) => {
                    let _ = deps.retract_dump(&rendition_dump_messages).await;
                    transfer_zip_dump_messages(ctx, &rendition_dump_messages);
                    tracing::info!(
                        rendition = ?state.rendition,
                        "discarded stale ZIP replacement"
                    );
                }
                Ok(AlbumReplacementResult::Committed {
                    displaced_message_ids,
                }) => {
                    transfer_zip_dump_messages(ctx, &new_message_ids);
                    let old_message_ids = displaced_message_ids
                        .into_iter()
                        .map(DumpMessageRef::new)
                        .filter(|message_id| !new_message_ids.contains(message_id))
                        .collect::<Vec<_>>();
                    if !old_message_ids.is_empty() {
                        let _ = deps.retract_dump(&old_message_ids).await;
                    }
                }
            }
        } else if complete && state.rendition == Rendition::Atmos {
            let _ = deps.retract_dump(&rendition_dump_messages).await;
            transfer_zip_dump_messages(ctx, &rendition_dump_messages);
            tracing::warn!("optional Atmos ZIP publication was incomplete");
        } else if state.rendition == Rendition::Primary && complete {
            return Err("primary ZIP publication was incomplete".to_owned());
        }
        if ctx.zip_deliver && !options.is_cache_only && delivered_parts > 0 {
            let release_year = ctx.zip_release_date.chars().take(4).collect::<String>();
            let details = AlbumDetailsCaption {
                album: ctx.zip_album.clone(),
                artist: ctx.zip_artist.clone(),
                album_url: ctx.zip_album_url.clone(),
                total_tracks: expected_tracks,
                delivered_tracks: Some(entries.len()),
                size_bytes: delivered_size,
                total_parts: delivered_parts,
                release_year: release_year.clone(),
                genre: ctx.zip_genre.clone(),
                record_label: ctx.zip_record_label.clone(),
                is_partial: !complete,
                user_name: options.user_name.clone(),
                user_id: options.user_id,
                codec: Some(codec.as_str().to_owned()),
            };
            let photo_delivered = if let Some(bytes) = &cover_bytes {
                if is_cancelled() {
                    return Ok(first_delivery);
                }
                let delivered = tokio::select! {
                    result = deps.deliver_to_chat(ChatDelivery::Photo {
                        destination: ChatRef::new(options.delivery_chat_id),
                        image_bytes: bytes.clone(),
                        caption: details.clone(),
                    }) => matches!(result, Ok(DeliveryReceipt::PreviewDelivered)),
                    _ = job_controller.cancelled() => return Ok(first_delivery),
                };
                if is_cancelled() {
                    return Ok(first_delivery);
                }
                delivered
            } else {
                false
            };
            let info = ZipDeliveryInfo {
                album: ctx.zip_album.clone(),
                artist: ctx.zip_artist.clone(),
                release_year,
                total_tracks: expected_tracks,
                delivered_tracks: Some(entries.len()),
                total_parts: delivered_parts,
                size_bytes: delivered_size,
                is_partial: !complete,
                album_id: ctx.zip_album_id.clone(),
                album_url: ctx.zip_album_url.clone(),
                artwork_url: ctx.zip_artwork_url.clone(),
                genre: ctx.zip_genre.clone(),
                record_label: ctx.zip_record_label.clone(),
                copyright: ctx.zip_copyright.clone(),
                photo_delivered,
                codec: Some(codec),
            };
            if first_delivery.is_none() {
                first_delivery = Some(info.clone());
            }
            ctx.zip_delivery_infos.lock().unwrap().push(info);
        }
    }
    drop(finalizing_guard);
    Ok(first_delivery)
}
