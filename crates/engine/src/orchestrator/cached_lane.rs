//! Cached-track lane: cache resolution outcomes and per-item cached delivery/materialisation.

use super::*;

pub(super) enum CacheResolution {
    Hit,
    Rerip,
    Cancelled,
    Failed(String),
}

pub(super) struct CacheResolutionGuard {
    pub(super) sender: Option<tokio::sync::oneshot::Sender<CacheResolution>>,
}

impl CacheResolutionGuard {
    pub(super) fn new(sender: tokio::sync::oneshot::Sender<CacheResolution>) -> Self {
        Self {
            sender: Some(sender),
        }
    }

    pub(super) fn send(&mut self, result: CacheResolution) {
        if let Some(sender) = self.sender.take() {
            let _ = sender.send(result);
        }
    }
}

impl Drop for CacheResolutionGuard {
    fn drop(&mut self) {
        if let Some(sender) = self.sender.take() {
            let _ = sender.send(CacheResolution::Failed(
                "cached lane task stopped unexpectedly".to_owned(),
            ));
        }
    }
}

pub(super) struct CachedResolutionInput<'a, D> {
    pub(super) deps: &'a Arc<D>,
    pub(super) bus: &'a EventBus,
    pub(super) shared: &'a Arc<Mutex<TaskShared>>,
    pub(super) ctx: &'a Arc<TaskContext>,
    pub(super) job_controller: &'a CancellationToken,
    pub(super) queue_signal: &'a CancellationToken,
    pub(super) item: &'a PipelineItem,
    pub(super) cached: &'a CachedTrack,
}

pub(super) async fn resolve_cached_item<D>(input: CachedResolutionInput<'_, D>) -> CacheResolution
where
    D: TrackCache + Delivery + TaskBookkeeping,
{
    let CachedResolutionInput {
        deps,
        bus,
        shared,
        ctx,
        job_controller,
        queue_signal,
        item,
        cached,
    } = input;
    let is_cancelled = || {
        shared.lock().expect("job poisoned").job.is_cancelled
            || job_controller.is_cancelled()
            || queue_signal.is_cancelled()
    };
    let track_id = cached.track_id.clone();
    let codec = cached.codec;
    if is_cancelled() {
        return CacheResolution::Cancelled;
    }
    if !codec_allowed_for_rendition(item.rendition, cached.codec) {
        tracing::warn!(
            track_id = %item.track_id,
            codec = %cached.codec.as_str(),
            "cached track codec does not match rendition; reripping"
        );
        let _ = deps.delete_track(&track_id, Some(codec)).await;
        return CacheResolution::Rerip;
    }

    bus.set_job_activity(shared, None);
    bus.set_download(
        shared,
        Some(DownloadLane::CachedDelivery {
            track: TrackLabel::new(
                item.meta_title.clone().unwrap_or_default(),
                item.meta_artist.clone().unwrap_or_default(),
            )
            .with_artwork_url(item.artwork_url.clone())
            .with_position(item.track_index, item.total_tracks),
        }),
    );
    bus.emit_progress(shared);
    let progress_guard = DownloadProgressGuard {
        bus: bus.clone(),
        shared: Arc::clone(shared),
    };
    let _ = &progress_guard;

    if !ctx.options.is_cache_only && !ctx.zip_deliver {
        let reply_to = (ctx.options.delivery_chat_id == ctx.options.chat_id)
            .then_some(ctx.options.reply_to_message_id)
            .flatten();
        let copy_result = tokio::select! {
            result = deps.deliver_to_chat(ChatDelivery::DumpCopy {
                destination: ChatRef::new(ctx.options.delivery_chat_id),
                source: DumpMessageRef::new(cached.message_id),
                reply_to: reply_to.map(ChatMessageRef::new),
                silent: ctx.is_multi_track,
            }) => result,
            _ = job_controller.cancelled() => return CacheResolution::Cancelled,
            _ = queue_signal.cancelled() => return CacheResolution::Cancelled,
        };
        let sent_id = match copy_result {
            Ok(DeliveryReceipt::Message(sent_id)) => sent_id,
            Ok(DeliveryReceipt::PreviewDelivered) => {
                tracing::warn!(track_id = %item.track_id, "cached track delivery returned a preview receipt");
                let _ = deps.delete_track(&track_id, Some(codec)).await;
                return CacheResolution::Rerip;
            }
            Err(_) => {
                tracing::warn!(track_id = %item.track_id, "cached track delivery failed; reripping");
                let _ = deps.delete_track(&track_id, Some(codec)).await;
                return CacheResolution::Rerip;
            }
        };
        if is_cancelled() {
            return CacheResolution::Cancelled;
        }
        if ctx
            .first_delivered_msg_id
            .lock()
            .expect("first message poisoned")
            .is_none()
        {
            *ctx.first_delivered_msg_id
                .lock()
                .expect("first message poisoned") = Some(sent_id);
        }
    }

    if ctx.zip_build
        && !ctx.zip_reuse.contains_key(&item.rendition)
        && let Some(state) = ctx.zip_state(item.rendition)
    {
        let title = item.meta_title.as_deref().unwrap_or("");
        let artist = item.meta_artist.as_deref().unwrap_or("");
        let filename = build_zip_entry_filename_with_codec(
            None,
            title,
            artist,
            &item.track_id,
            cached.codec.as_str(),
        );
        let destination = state.dir.join(&filename);
        let track = TrackLabel::new(
            item.meta_title.clone().unwrap_or_default(),
            item.meta_artist.clone().unwrap_or_default(),
        )
        .with_artwork_url(item.artwork_url.clone())
        .with_position(item.track_index, item.total_tracks);
        bus.set_download(
            shared,
            Some(DownloadLane::Rip(RipActivity::MaterializingCachedMedia {
                track: track.clone(),
                progress: ByteProgress {
                    completed: 0,
                    total: None,
                },
            })),
        );
        bus.emit_progress(shared);
        let progress_bus = bus.clone();
        let progress_shared = Arc::clone(shared);
        let materialization_progress: UploadProgressCallback = Arc::new(move |completed, total| {
            progress_bus.set_download(
                &progress_shared,
                Some(DownloadLane::Rip(RipActivity::MaterializingCachedMedia {
                    track: track.clone(),
                    progress: ByteProgress {
                        completed,
                        total: (total > 0).then_some(total),
                    },
                })),
            );
            progress_bus.emit_progress(&progress_shared);
        });
        let download_result = tokio::select! {
            result = deps.materialize_cached(
                DumpMessageRef::new(cached.message_id),
                &destination,
                Some(&materialization_progress),
            ) => result,
            _ = job_controller.cancelled() => {
                let _ = tokio::fs::remove_file(&destination).await;
                return CacheResolution::Cancelled;
            }
            _ = queue_signal.cancelled() => {
                let _ = tokio::fs::remove_file(&destination).await;
                return CacheResolution::Cancelled;
            }
        };
        if let Err(error) = download_result {
            tracing::warn!(
                track_id = %item.track_id,
                %error,
                "cached ZIP source unavailable; reripping"
            );
            let _ = tokio::fs::remove_file(&destination).await;
            let _ = deps.delete_track(&track_id, Some(codec)).await;
            return CacheResolution::Rerip;
        }
        if is_cancelled() {
            let _ = tokio::fs::remove_file(&destination).await;
            return CacheResolution::Cancelled;
        }
        match tokio::fs::metadata(&destination).await {
            Ok(metadata) => {
                seed_zip_codec(state, cached.codec);
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
                    track_id = %item.track_id,
                    %error,
                    "cached ZIP source disappeared; reripping"
                );
                let _ = tokio::fs::remove_file(&destination).await;
                let _ = deps.delete_track(&track_id, Some(codec)).await;
                return CacheResolution::Rerip;
            }
        }
    }
    if is_cancelled() {
        return CacheResolution::Cancelled;
    }
    if item.rendition == Rendition::Primary {
        shared.lock().expect("job poisoned").job.cached_count += 1;
    }
    bus.set_job_activity(shared, Some(TaskActivity::CachedDelivered));
    CacheResolution::Hit
}

pub(super) struct CachedLaneInput<D> {
    pub(super) deps: Arc<D>,
    pub(super) bus: EventBus,
    pub(super) shared: Arc<Mutex<TaskShared>>,
    pub(super) ctx: Arc<TaskContext>,
    pub(super) job_controller: CancellationToken,
    pub(super) queue_signal: CancellationToken,
    pub(super) upload_lane: Arc<Mutex<Option<tokio::sync::mpsc::Sender<LaneTask>>>>,
    pub(super) item: PipelineItem,
    pub(super) cached: CachedTrack,
}

pub(super) struct PendingCachedItem {
    pub(super) resolution: tokio::sync::oneshot::Receiver<CacheResolution>,
}

pub(super) enum CacheEnqueueResult {
    Pending(Box<PendingCachedItem>),
    Cancelled,
    Failed(String),
}

pub(super) async fn enqueue_cached_lane_task<D>(input: CachedLaneInput<D>) -> CacheEnqueueResult
where
    D: TrackCache + Delivery + TaskBookkeeping + 'static,
{
    let CachedLaneInput {
        deps,
        bus,
        shared,
        ctx,
        job_controller,
        queue_signal,
        upload_lane,
        item,
        cached,
    } = input;
    let (resolution_tx, resolution_rx) = tokio::sync::oneshot::channel();
    let resolution_guard = CacheResolutionGuard::new(resolution_tx);
    let cache_deps = Arc::clone(&deps);
    let cache_bus = bus.clone();
    let cache_shared = Arc::clone(&shared);
    let cache_ctx = Arc::clone(&ctx);
    let cache_controller = job_controller.clone();
    let cache_queue_signal = queue_signal.clone();
    let cache_item = item.clone();
    let cache_value = cached.clone();
    let panic_shared = Arc::clone(&shared);
    let panic_ctx = Arc::clone(&ctx);
    let panic_track_id = item.track_id.clone();
    let pushed = push_lane_task(
        &upload_lane,
        "cached_item",
        Some(Box::new(move |message| {
            record_lane_task_panic(&panic_shared, &panic_ctx, &panic_track_id, message);
        })),
        Some(&job_controller),
        Some(&queue_signal),
        move || {
            Box::pin(async move {
                let mut resolution = resolution_guard;
                let result = resolve_cached_item(CachedResolutionInput {
                    deps: &cache_deps,
                    bus: &cache_bus,
                    shared: &cache_shared,
                    ctx: &cache_ctx,
                    job_controller: &cache_controller,
                    queue_signal: &cache_queue_signal,
                    item: &cache_item,
                    cached: &cache_value,
                })
                .await;
                resolution.send(result);
            })
        },
    )
    .await;
    if !pushed {
        return if shared.lock().expect("job poisoned").job.is_cancelled
            || job_controller.is_cancelled()
            || queue_signal.is_cancelled()
        {
            CacheEnqueueResult::Cancelled
        } else {
            CacheEnqueueResult::Failed("cached lane task could not be queued".to_owned())
        };
    }
    CacheEnqueueResult::Pending(Box::new(PendingCachedItem {
        resolution: resolution_rx,
    }))
}
