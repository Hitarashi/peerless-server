//! Serialised upload lane: lane task plumbing, track publication, cache rollback, and ZIP staging.

use super::*;

pub(super) fn record_lane_task_panic(
    shared: &Arc<Mutex<TaskShared>>,
    ctx: &TaskContext,
    item_id: &str,
    message: String,
) {
    let error = format!("lane-2 task panicked for {item_id}: {message}");
    set_fatal_error(ctx, error.clone());
    let mut failures = ctx.failed_tracks.lock().expect("failures poisoned");
    if !failures.iter().any(|failure| failure.id == item_id) {
        failures.push(FailedTrack {
            id: item_id.to_owned(),
            error,
            kind: None,
            title: None,
            artist: None,
            storefront: None,
        });
        shared.lock().expect("job poisoned").job.failed_count = failures.len();
    }
}

pub(super) struct LaneTask {
    pub(super) run: Box<dyn FnOnce() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send>,

    pub(super) label: &'static str,

    pub(super) on_panic: Option<Box<dyn FnOnce(String) + Send>>,
}

pub(super) async fn push_lane_task(
    lane: &Arc<Mutex<Option<tokio::sync::mpsc::Sender<LaneTask>>>>,
    label: &'static str,
    on_panic: Option<Box<dyn FnOnce(String) + Send>>,
    job_cancellation: Option<&CancellationToken>,
    queue_cancellation: Option<&CancellationToken>,
    run: impl FnOnce() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + 'static,
) -> bool {
    let tx = {
        let guard = lane.lock().expect("upload lane poisoned");
        match guard.as_ref() {
            Some(tx) => tx.clone(),
            None => return false,
        }
    };
    let task = LaneTask {
        run: Box::new(run),
        label,
        on_panic,
    };
    let send_result = match (job_cancellation, queue_cancellation) {
        (Some(job_cancellation), Some(queue_cancellation)) => {
            tokio::select! {
                result = tx.send(task) => result,
                _ = job_cancellation.cancelled() => return false,
                _ = queue_cancellation.cancelled() => return false,
            }
        }
        (Some(job_cancellation), None) => {
            tokio::select! {
                result = tx.send(task) => result,
                _ = job_cancellation.cancelled() => return false,
            }
        }
        (None, Some(queue_cancellation)) => {
            tokio::select! {
                result = tx.send(task) => result,
                _ = queue_cancellation.cancelled() => return false,
            }
        }
        (None, None) => tx.send(task).await,
    };
    match send_result {
        Ok(()) => true,
        Err(_) => {
            tracing::error!(
                lane = "upload",
                item = label,
                "upload lane closed; task dropped"
            );
            false
        }
    }
}

impl RipOrchestrator {
    pub(super) fn ensure_upload_lane(&self) -> tokio::sync::mpsc::Sender<LaneTask> {
        let mut guard = self.upload_lane.lock().expect("upload lane poisoned");
        if let Some(tx) = guard.as_ref() {
            return tx.clone();
        }
        let (tx, mut rx) = tokio::sync::mpsc::channel::<LaneTask>(16);
        tokio::spawn(async move {
            while let Some(task) = rx.recv().await {
                let LaneTask {
                    run,
                    label,
                    on_panic,
                } = task;
                let result = futures_util::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(
                    async move { run().await },
                ))
                .await;
                if let Err(panic) = result {
                    let message = panic_message(panic);
                    if let Some(on_panic) = on_panic {
                        on_panic(message.clone());
                    }
                    tracing::error!(
                        lane = "upload",
                        item = label,
                        error = %message,
                        "upload lane task panicked; lane continues"
                    );
                }
            }
        });
        *guard = Some(tx.clone());
        tx
    }
}

pub(super) struct UploadLaneInput<D> {
    pub(super) deps: Arc<D>,
    pub(super) bus: EventBus,
    pub(super) shared: Arc<Mutex<TaskShared>>,
    pub(super) ctx: Arc<TaskContext>,
    pub(super) job_controller: CancellationToken,
    pub(super) queue_cancellation: Option<CancellationToken>,
    pub(super) upload_lane: Arc<Mutex<Option<tokio::sync::mpsc::Sender<LaneTask>>>>,
    pub(super) upload_item: PipelineRipResult,
}

pub(super) async fn enqueue_upload_task<D>(input: UploadLaneInput<D>) -> bool
where
    D: TrackCache + Delivery + TaskBookkeeping + 'static,
{
    let UploadLaneInput {
        deps,
        bus,
        shared,
        ctx,
        job_controller,
        queue_cancellation,
        upload_lane,
        upload_item,
    } = input;
    let panic_shared = Arc::clone(&shared);
    let panic_ctx = Arc::clone(&ctx);
    let panic_track_id = upload_item.track_id.clone();
    let panic_rendition = upload_item.rendition;
    let panic_title = upload_item.rip_result.title.clone();
    let panic_artist = upload_item.rip_result.artist.clone();
    let item_ctx = Arc::clone(&ctx);
    let task_controller = job_controller.clone();
    push_lane_task(
        &upload_lane,
        "upload_track",
        Some(Box::new(move |message| {
            record_lane_task_panic(&panic_shared, &panic_ctx, &panic_track_id, message);
            if panic_rendition == Rendition::Primary {
                let mut failures = panic_ctx.failed_tracks.lock().expect("failures poisoned");
                if let Some(failure) = failures
                    .iter_mut()
                    .find(|failure| failure.id == panic_track_id)
                {
                    failure.title = Some(panic_title.clone());
                    failure.artist = Some(panic_artist.clone());
                }
            }
        })),
        Some(&job_controller),
        queue_cancellation.as_ref(),
        move || {
            Box::pin(async move {
                run_upload_item(deps, bus, shared, item_ctx, task_controller, upload_item).await;
            })
        },
    )
    .await
}

pub(super) async fn run_upload_item<D>(
    deps: Arc<D>,
    bus: EventBus,
    shared: Arc<Mutex<TaskShared>>,
    ctx: Arc<TaskContext>,
    job_controller: CancellationToken,
    upload_item: PipelineRipResult,
) where
    D: TrackCache + Delivery + TaskBookkeeping,
{
    let uploaded_ok = upload_one(&deps, &bus, &shared, &ctx, &job_controller, &upload_item).await;

    let cancelled =
        shared.lock().expect("job poisoned").job.is_cancelled || job_controller.is_cancelled();
    if uploaded_ok
        && ctx.zip_build
        && !cancelled
        && let Some(state) = ctx.zip_state(upload_item.rendition)
    {
        let filename = build_zip_entry_filename_with_codec(
            Some(upload_item.rip_result.track_number),
            &upload_item.rip_result.title,
            &upload_item.rip_result.artist,
            &upload_item.track_id,
            &upload_item.rip_result.codec,
        );
        let destination = state.dir.join(&filename);
        if let Err(error) = tokio::fs::copy(&upload_item.rip_result.file_path, &destination).await {
            tracing::warn!(
                %error,
                track_id = %upload_item.track_id,
                rendition = ?upload_item.rendition,
                "failed to stage track for ZIP"
            );
            if upload_item.rendition == Rendition::Primary {
                set_primary_zip_error(
                    &ctx,
                    format!(
                        "primary ZIP staging failed for {}: {error}",
                        upload_item.track_id
                    ),
                );
            } else {
                *ctx.atmos_warning.lock().expect("atmos poisoned") = Some(format!(
                    "optional Atmos ZIP staging failed for {}: {error}",
                    upload_item.track_id
                ));
            }
        } else if shared.lock().expect("job poisoned").job.is_cancelled
            || job_controller.is_cancelled()
        {
            let _ = tokio::fs::remove_file(&destination).await;
        } else {
            match tokio::fs::metadata(&destination).await {
                Ok(metadata) => {
                    if let Ok(codec) = upload_item.rip_result.codec.parse::<Codec>()
                        && codec_allowed_for_rendition(upload_item.rendition, codec)
                    {
                        seed_zip_codec(state, codec);
                    }
                    state
                        .sources
                        .lock()
                        .expect("zip sources poisoned")
                        .push(ZipTrackEntry {
                            file_path: destination,
                            archive_filename: filename,
                            file_size: metadata.len(),
                        });
                }
                Err(error) => {
                    tracing::warn!(
                        %error,
                        track_id = %upload_item.track_id,
                        rendition = ?upload_item.rendition,
                        "staged ZIP source disappeared"
                    );
                    let _ = tokio::fs::remove_file(&destination).await;
                    if upload_item.rendition == Rendition::Primary {
                        set_primary_zip_error(
                            &ctx,
                            format!(
                                "primary ZIP source metadata failed for {}: {error}",
                                upload_item.track_id
                            ),
                        );
                    } else {
                        *ctx.atmos_warning.lock().expect("atmos poisoned") = Some(format!(
                            "optional Atmos ZIP source metadata failed for {}: {error}",
                            upload_item.track_id
                        ));
                    }
                }
            }
        }
    }

    delete_file_if_exists(&upload_item.rip_result.file_path).await;
}

pub(super) async fn rollback_cancelled<D>(
    deps: &Arc<D>,
    track_id: &str,
    dump_message_id: DumpMessageRef,
    delete_record: bool,
    codec: Option<Codec>,
) where
    D: TrackCache + Delivery,
{
    let dump_removed = deps.retract_dump(&[dump_message_id]).await.is_ok();
    if dump_removed && delete_record {
        let _ = deps.delete_track(track_id, codec).await;
    }
}

pub(super) async fn upload_one<D>(
    deps: &Arc<D>,
    bus: &EventBus,
    shared: &Arc<Mutex<TaskShared>>,
    ctx: &Arc<TaskContext>,
    job_controller: &CancellationToken,
    upload_item: &PipelineRipResult,
) -> bool
where
    D: TrackCache + Delivery + TaskBookkeeping,
{
    let options = &ctx.options;
    let track_id = upload_item.track_id.clone();
    let rip_result = &upload_item.rip_result;
    bus.set_codec(shared, Some(rip_result.codec.clone()));
    let track_label = TrackLabel::new(rip_result.title.clone(), rip_result.artist.clone())
        .with_artwork_url(
            (!rip_result.artwork_url.is_empty()).then(|| rip_result.artwork_url.clone()),
        )
        .with_position(
            upload_item.track_index.or_else(|| {
                u32::try_from(rip_result.track_number)
                    .ok()
                    .filter(|n| *n > 0)
            }),
            upload_item.total_tracks.or_else(|| {
                u32::try_from(rip_result.track_count)
                    .ok()
                    .filter(|n| *n > 0)
            }),
        );
    let is_cancelled =
        || shared.lock().expect("job poisoned").job.is_cancelled || job_controller.is_cancelled();

    let caption = TrackCaption::Machine {
        track_id: track_id.clone(),
        codec: rip_result.codec.clone(),
    };
    let plain_caption = format!(
        "{} - {}\n{}",
        rip_result.title, rip_result.artist, rip_result.album
    );
    let mut current_caption = caption.clone();
    let mut used_plain_caption = false;

    bus.set_upload(
        shared,
        Some(UploadLane::Track {
            track: track_label.clone(),
            progress: ByteProgress {
                completed: 0,
                total: None,
            },
        }),
    );
    bus.emit_progress(shared);

    let max_retries = ctx.config.upload_retry.retries;
    enum SendOutcome {
        Audio(crate::orchestrator::deps::DumpPublication),
    }
    let mut outcome: Option<SendOutcome> = None;
    'upload: for attempt in 0..=max_retries {
        if is_cancelled() {
            break 'upload;
        }
        let on_upload: UploadProgressCallback = {
            let shared = Arc::clone(shared);
            let bus = bus.clone();
            let label = track_label.clone();
            Arc::new(move |uploaded, total| {
                bus.set_upload(
                    &shared,
                    Some(UploadLane::Track {
                        track: label.clone(),
                        progress: ByteProgress {
                            completed: uploaded,
                            total: Some(total),
                        },
                    }),
                );
                bus.emit_progress(&shared);
            })
        };

        let upload_result = tokio::select! {
            result = deps.publish_to_dump(DumpPublish::TrackAudio {
                file_path: rip_result.file_path.clone(),
                title: rip_result.title.clone(),
                performer: rip_result.artist.clone(),
                duration: rip_result.duration,
                caption: current_caption.clone(),
                on_upload_progress: Some(Arc::clone(&on_upload)),
            }) => result,
            _ = job_controller.cancelled() => break 'upload,
        };
        match upload_result {
            Ok(upload) => {
                outcome = Some(SendOutcome::Audio(upload));

                break 'upload;
            }
            Err(upload_err) => {
                if is_cancelled() {
                    break 'upload;
                }
                if matches!(
                    upload_err,
                    DeliveryError::Rejected(
                        DeliveryRejection::EntityBoundsInvalid | DeliveryRejection::CaptionTooLong
                    )
                ) && !used_plain_caption
                {
                    used_plain_caption = true;
                    current_caption = TrackCaption::Plain(clamp_str_utf16(&plain_caption, 1024));
                    continue 'upload;
                }
                if upload_err.is_transient() && attempt < max_retries {
                    let delay = exponential_delay(ctx.config.upload_retry.base_delay_ms, attempt)
                        .as_millis() as f64
                        * jitter_multiplier();
                    tracing::warn!(
                        track_id = %track_id,
                        attempt,
                        max_retries,
                        delay_ms = delay.round() as u64,
                        error = %upload_err,
                        "Track upload to dump failed, retrying"
                    );
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_millis(delay as u64)) => {}
                        _ = job_controller.cancelled() => {}
                    }
                } else {
                    tracing::error!(
                        track_id = %track_id,
                        attempts = attempt + 1,
                        error = %upload_err,
                        "All upload retries exhausted for track"
                    );

                    if upload_item.rendition == Rendition::Primary {
                        let mut failures = ctx.failed_tracks.lock().expect("failures poisoned");
                        failures.push(FailedTrack {
                            id: track_id.clone(),
                            error: upload_err.to_string(),
                            kind: None,
                            title: Some(rip_result.title.clone()),
                            artist: Some(rip_result.artist.clone()),
                            storefront: None,
                        });
                        shared.lock().expect("job poisoned").job.failed_count = failures.len();
                    }
                    bus.set_upload(shared, None);
                    bus.emit_progress(shared);
                    return false;
                }
            }
        }
    }

    let Some(outcome) = outcome else {
        bus.set_upload(shared, None);
        bus.emit_progress(shared);
        return false;
    };

    let SendOutcome::Audio(dump_upload) = outcome;

    let post_upload: Result<i64, String> = async {
        if is_cancelled() {
            if let Err(error) = deps.retract_dump(&[dump_upload.message]).await {
                tracing::error!(%error, "failed to retract cancelled track publication");
            }
            return Err("cancelled".to_owned());
        }
        let cache_input = SaveTrackInput::from_rip_result(
            &track_id,
            rip_result,
            dump_upload.message.id(),
            &dump_upload.file_id,
            &dump_upload.file_unique_id,
        );
        if let Err(error) =
            save_track_with_retry(deps.as_ref(), cache_input, &ctx.config.storage_retry).await
        {
            if let Err(retract_error) = deps.retract_dump(&[dump_upload.message]).await {
                tracing::error!(
                    %retract_error,
                    track_id = %track_id,
                    "failed to retract track publication after cache persistence failure"
                );
            }
            return Err(format!("track cache persistence failed: {error}"));
        }

        if is_cancelled() {
            let rip_codec = rip_result.codec.parse::<Codec>().ok();
            rollback_cancelled(deps, &track_id, dump_upload.message_id(), true, rip_codec).await;
            return Err("cancelled".to_owned());
        }

        if !options.is_cache_only && !ctx.zip_deliver {
            let reply_to = (options.delivery_chat_id == options.chat_id)
                .then_some(options.reply_to_message_id)
                .flatten();
            let sent_id = deps
                .deliver_to_chat(ChatDelivery::DumpCopy {
                    destination: ChatRef::new(options.delivery_chat_id),
                    source: dump_upload.message,
                    reply_to: reply_to.map(ChatMessageRef::new),
                    silent: ctx.is_multi_track,
                })
                .await
                .map_err(|e| e.to_string())?;
            let DeliveryReceipt::Message(sent_id) = sent_id else {
                return Err("track delivery returned a preview receipt".to_owned());
            };
            let mut guard = ctx.first_delivered_msg_id.lock().unwrap();
            if guard.is_none() {
                *guard = Some(sent_id);
            }
        }

        if is_cancelled() {
            rollback_cancelled(
                deps,
                &track_id,
                dump_upload.message_id(),
                true,
                rip_result.codec.parse::<Codec>().ok(),
            )
            .await;
            return Err("cancelled".to_owned());
        }

        let total_duration_ms = (now_ms() - upload_item.start_time_ms) as i64;
        Ok(total_duration_ms)
    }
    .await;

    match post_upload {
        Ok(total_duration_ms) => {
            bus.set_upload(shared, None);
            if upload_item.rendition == Rendition::Primary {
                let new_count = ctx
                    .ripped_count
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                    + 1;
                shared.lock().expect("job poisoned").job.ripped_count = new_count;
            }

            bus.emit_progress(shared);

            tracing::info!(
                track = format!("{} - {}", rip_result.title, rip_result.artist),
                time = format!("{:.1}s", total_duration_ms as f64 / 1000.0),
                event = if options.is_cache_only {
                    "Track cached to dump"
                } else {
                    "Track completed"
                },
                "Track completed"
            );
            true
        }
        Err(err_msg) => {
            bus.set_upload(shared, None);
            if is_cancelled() {
                return false;
            }
            if upload_item.rendition == Rendition::Primary {
                record_failure(
                    shared,
                    ctx,
                    TrackFailureDetails {
                        track_id: &track_id,
                        err_msg,
                        title: Some(upload_item.rip_result.title.clone()),
                        artist: Some(upload_item.rip_result.artist.clone()),
                    },
                );
            }
            bus.emit_progress(shared);
            false
        }
    }
}

pub(super) struct TrackFailureDetails<'a> {
    pub(super) track_id: &'a str,
    pub(super) err_msg: String,
    pub(super) title: Option<String>,
    pub(super) artist: Option<String>,
}

pub(super) fn record_failure(
    shared: &Arc<Mutex<TaskShared>>,
    ctx: &TaskContext,
    details: TrackFailureDetails<'_>,
) {
    let mut failures = ctx.failed_tracks.lock().expect("failures poisoned");
    failures.push(FailedTrack {
        id: details.track_id.to_string(),
        error: details.err_msg,
        kind: None,
        title: details.title,
        artist: details.artist,
        storefront: None,
    });
    shared.lock().expect("job poisoned").job.failed_count = failures.len();
}

pub(super) async fn delete_file_if_exists(path: &str) {
    let _ = tokio::fs::remove_file(std::path::Path::new(path)).await;
}
