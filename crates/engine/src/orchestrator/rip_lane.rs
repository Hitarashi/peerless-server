//! Fresh rip lane: per-item ripping, lane cancellation checks, and ordered fallback re-rip.

use super::*;

pub(super) enum RipLaneOutcome {
    Ripped(Box<PipelineRipResult>),
    Finished,
    Cancelled,
    Stop,
}

pub(super) fn is_lane_cancelled(
    shared: &Arc<Mutex<TaskShared>>,
    job_controller: &CancellationToken,
    queue_signal: &CancellationToken,
) -> bool {
    shared.lock().expect("job poisoned").job.is_cancelled
        || job_controller.is_cancelled()
        || queue_signal.is_cancelled()
}

pub(super) struct RipFreshInput<'a, D> {
    pub(super) deps: &'a Arc<D>,
    pub(super) bus: &'a EventBus,
    pub(super) shared: &'a Arc<Mutex<TaskShared>>,
    pub(super) ctx: &'a Arc<TaskContext>,
    pub(super) job_controller: &'a CancellationToken,
    pub(super) queue_signal: &'a CancellationToken,
    pub(super) item: PipelineItem,
    pub(super) rip_job_dir: &'a Path,
}

pub(super) async fn rip_fresh_item<D>(input: RipFreshInput<'_, D>) -> RipLaneOutcome
where
    D: ProviderDeps + TaskBookkeeping + 'static,
{
    let RipFreshInput {
        deps,
        bus,
        shared,
        ctx,
        job_controller,
        queue_signal,
        item,
        rip_job_dir,
    } = input;
    if is_lane_cancelled(shared, job_controller, queue_signal) {
        return RipLaneOutcome::Cancelled;
    }
    bus.set_job_activity(shared, None);
    bus.set_download(shared, None);
    bus.emit_progress(shared);
    if item.is_streamable == Some(false) {
        if item.rendition == Rendition::Atmos {
            return RipLaneOutcome::Finished;
        }
        let err_msg = deps.unavailable_track_message().to_owned();
        {
            let mut failures = ctx.failed_tracks.lock().expect("failures poisoned");
            failures.push(FailedTrack {
                id: item.track_id.clone(),
                error: err_msg.clone(),
                kind: Some(FailedTrackKind::TrackUnavailable),
                title: item.meta_title.clone(),
                artist: item.meta_artist.clone(),
                storefront: item.storefront.clone(),
            });
            shared.lock().expect("job poisoned").job.failed_count = failures.len();
        }
        bus.set_job_activity(shared, Some(TaskActivity::ProcessingNext));
        bus.emit_progress(shared);
        return RipLaneOutcome::Finished;
    }

    let track_start_time = now_ms();
    let update_single_track_header =
        !ctx.is_multi_track && item.meta_title.is_none() && item.meta_artist.is_none();
    let item_track_index = item.track_index;
    let item_total_tracks = item.total_tracks;

    let on_progress: RipProgressCallback = {
        let shared = Arc::clone(shared);
        let bus = bus.clone();
        Arc::new(move |mut activity| {
            match &mut activity {
                RipActivity::Connecting { track }
                | RipActivity::Downloading { track, .. }
                | RipActivity::MaterializingCachedMedia { track, .. }
                | RipActivity::Decrypting { track }
                | RipActivity::Tagging { track } => {
                    if track.track_index.is_none() {
                        track.track_index = item_track_index;
                    }
                    if track.total_tracks.is_none() {
                        track.total_tracks = item_total_tracks;
                    }
                }
                RipActivity::ResolvingMetadata => {}
            }
            if update_single_track_header {
                let track = match &activity {
                    RipActivity::Connecting { track }
                    | RipActivity::Downloading { track, .. }
                    | RipActivity::MaterializingCachedMedia { track, .. }
                    | RipActivity::Decrypting { track }
                    | RipActivity::Tagging { track } => Some(track),
                    RipActivity::ResolvingMetadata => None,
                };
                if let Some(track) = track {
                    let label = if track.artist.is_empty() {
                        track.title.clone()
                    } else if track.title.is_empty() {
                        track.artist.clone()
                    } else {
                        format!("{} - {}", track.title, track.artist)
                    };
                    if !label.is_empty() {
                        shared.lock().expect("job poisoned").job.job_header =
                            format!("<b>{}</b>", html_escape(&label));
                    }
                }
            }
            bus.set_download(&shared, Some(DownloadLane::Rip(activity)));
            bus.emit_progress(&shared);
        })
    };

    let default_sf = resolve_default_storefront(&deps.settings_snapshot()).to_owned();
    let storefront = item.storefront.clone().unwrap_or(default_sf);
    let codec_preference = if item.rendition == Rendition::Atmos {
        music::CodecPreference::Atmos
    } else {
        ctx.options
            .codec_preference
            .unwrap_or_else(|| item.rendition.codec_preference())
    };
    let rip_options = RipOptions {
        storefront: &storefront,
        on_progress: Some(&on_progress),
        signal: Some(queue_signal.clone()),
        output_dir: Some(rip_job_dir),
        codec_preference,
    };
    let initial_codec = match item.rendition {
        Rendition::Atmos => "ec-3",
        Rendition::Primary => match codec_preference {
            music::CodecPreference::HighestQuality
            | music::CodecPreference::HiRes192
            | music::CodecPreference::HiRes96
            | music::CodecPreference::LosslessCd => "alac",
            music::CodecPreference::Atmos => "ec-3",
        },
    };
    bus.set_codec(shared, Some(initial_codec.to_string()));
    let rip_result = match deps.rip(&item.track_id, rip_options).await {
        Ok(rip_result) => {
            bus.set_codec(shared, Some(rip_result.codec.clone()));
            rip_result
        }
        Err(error) => {
            bus.set_download(shared, None);
            if is_lane_cancelled(shared, job_controller, queue_signal) {
                bus.emit_progress(shared);
                return RipLaneOutcome::Cancelled;
            }
            if item.rendition == Rendition::Atmos
                && matches!(&error, RipError::RenditionUnavailable { .. })
            {
                tracing::debug!(track_id = %item.track_id, "Atmos rendition unavailable");
                bus.emit_progress(shared);
                return RipLaneOutcome::Finished;
            }

            let err_msg = error.to_string();
            let duration_ms = (now_ms() - track_start_time) as i64;
            if item.rendition == Rendition::Primary {
                let mut failures = ctx.failed_tracks.lock().expect("failures poisoned");
                failures.push(FailedTrack {
                    id: item.track_id.clone(),
                    error: err_msg.clone(),
                    kind: FailedTrackKind::of(&error),
                    title: item.meta_title.clone(),
                    artist: item.meta_artist.clone(),
                    storefront: item.storefront.clone(),
                });
                shared.lock().expect("job poisoned").job.failed_count = failures.len();
            }
            tracing::error!(
                track_id = %item.track_id,
                duration_ms,
                error = %err_msg,
                "Rip job failed"
            );

            bus.set_job_activity(shared, Some(TaskActivity::ProcessingNext));
            bus.emit_progress(shared);

            if matches!(error, RipError::SourceOffline { .. })
                && item.rendition == Rendition::Primary
            {
                tracing::error!(
                    track_id = %item.track_id,
                    error = %err_msg,
                    "Mirror source offline; skipping track and continuing batch"
                );
            }
            return RipLaneOutcome::Finished;
        }
    };

    bus.set_download(shared, None);
    bus.emit_progress(shared);
    if is_lane_cancelled(shared, job_controller, queue_signal) {
        return RipLaneOutcome::Cancelled;
    }
    RipLaneOutcome::Ripped(Box::new(PipelineRipResult {
        track_id: item.track_id,
        rip_result,
        start_time_ms: track_start_time,
        rendition: item.rendition,
        track_index: item.track_index,
        total_tracks: item.total_tracks,
    }))
}

pub(super) struct OrderedReripInput<D> {
    pub(super) deps: Arc<D>,
    pub(super) bus: EventBus,
    pub(super) shared: Arc<Mutex<TaskShared>>,
    pub(super) ctx: Arc<TaskContext>,
    pub(super) job_controller: CancellationToken,
    pub(super) queue: SequentialRipQueue,
    pub(super) item: PipelineItem,
    pub(super) rip_job_dir: PathBuf,
}

pub(super) fn submit_ordered_rerip_item<D>(
    input: OrderedReripInput<D>,
) -> crate::queue::TaskReceiver
where
    D: ProviderDeps + TaskBookkeeping + 'static,
{
    let OrderedReripInput {
        deps,
        bus,
        shared,
        ctx,
        job_controller,
        queue,
        item,
        rip_job_dir,
    } = input;
    let task_deps = Arc::clone(&deps);
    let task_bus = bus.clone();
    let task_shared = Arc::clone(&shared);
    let task_ctx = Arc::clone(&ctx);
    let task_controller = job_controller.clone();
    let task_rip_job_dir = rip_job_dir;
    queue.submit(
        move |queue_signal| {
            Box::pin(async move {
                rip_fresh_item(RipFreshInput {
                    deps: &task_deps,
                    bus: &task_bus,
                    shared: &task_shared,
                    ctx: &task_ctx,
                    job_controller: &task_controller,
                    queue_signal: &queue_signal,
                    item,
                    rip_job_dir: &task_rip_job_dir,
                })
                .await
            })
        },
        Some(EnqueueOptions {
            on_position_change: None,
            on_start: None,
            signal: Some(job_controller),
        }),
    )
}

pub(super) fn ordered_rerip_result(
    result: Result<crate::queue::TaskResult, tokio::sync::oneshot::error::RecvError>,
    shared: &Arc<Mutex<TaskShared>>,
    ctx: &Arc<TaskContext>,
    job_controller: &CancellationToken,
) -> RipLaneOutcome {
    let cancelled =
        || shared.lock().expect("job poisoned").job.is_cancelled || job_controller.is_cancelled();
    match result {
        Ok(Ok(value)) => match value.downcast::<RipLaneOutcome>() {
            Ok(outcome) => *outcome,
            Err(_) => {
                set_fatal_error(
                    ctx,
                    "ordered fallback rip returned an invalid result".to_owned(),
                );
                RipLaneOutcome::Stop
            }
        },
        Ok(Err(error)) => {
            if cancelled() {
                RipLaneOutcome::Cancelled
            } else {
                set_fatal_error(ctx, format!("ordered fallback rip failed: {error}"));
                RipLaneOutcome::Stop
            }
        }
        Err(_) => {
            if cancelled() {
                RipLaneOutcome::Cancelled
            } else {
                set_fatal_error(
                    ctx,
                    "ordered fallback rip queue stopped unexpectedly".to_owned(),
                );
                RipLaneOutcome::Stop
            }
        }
    }
}
