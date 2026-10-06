//! Ordered dispatch: slot buffering, cached/upload hand-off, and drain waits.

use super::*;

pub(super) struct LaneOneContext<'a, D> {
    pub(super) deps: Arc<D>,
    pub(super) bus: EventBus,
    pub(super) shared: Arc<Mutex<TaskShared>>,
    pub(super) uncached_items: &'a [PipelineItem],
    pub(super) job_controller: CancellationToken,
    pub(super) queue_signal: CancellationToken,
    pub(super) ctx: Arc<TaskContext>,
    pub(super) upload_lane: Arc<Mutex<Option<tokio::sync::mpsc::Sender<LaneTask>>>>,
    pub(super) queue: SequentialRipQueue,
    pub(super) workspace_guard: WorkspaceGuard,
    pub(super) summary_tx: Arc<Mutex<Option<FinalizeSender>>>,
    pub(super) rip_job_dir: PathBuf,
}

pub(super) enum OrderedSlot {
    Cached {
        item: PipelineItem,
        cached: CachedTrack,
    },
    Fresh(PipelineRipResult),
    Finished,
}

pub(super) const ORDERED_SLOT_CAPACITY: usize = 16;

pub(super) fn ordered_slot_capacity(item_count: usize) -> usize {
    item_count.saturating_add(1).max(ORDERED_SLOT_CAPACITY)
}

pub(super) struct OrderedDispatchInput<D> {
    pub(super) deps: Arc<D>,
    pub(super) bus: EventBus,
    pub(super) shared: Arc<Mutex<TaskShared>>,
    pub(super) ctx: Arc<TaskContext>,
    pub(super) job_controller: CancellationToken,
    pub(super) upload_lane: Arc<Mutex<Option<tokio::sync::mpsc::Sender<LaneTask>>>>,
    pub(super) queue: SequentialRipQueue,
    pub(super) slots: tokio::sync::mpsc::Receiver<OrderedSlot>,
    pub(super) ordered_capacity: usize,
    pub(super) rip_job_dir: PathBuf,
    pub(super) finalization_guard: FinalizationGuard,
}

pub(super) async fn run_lane_one<D>(input: LaneOneContext<'_, D>)
where
    D: TrackCache + AlbumCache + ProviderDeps + Delivery + TaskBookkeeping + 'static,
{
    let LaneOneContext {
        deps,
        bus,
        shared,
        uncached_items,
        job_controller,
        queue_signal,
        ctx,
        upload_lane,
        queue,
        mut workspace_guard,
        summary_tx,
        rip_job_dir,
    } = input;
    tracing::debug!("Rip job started from queue");

    let ordered_capacity = ordered_slot_capacity(uncached_items.len());
    let (slot_tx, slot_rx) = tokio::sync::mpsc::channel(ordered_capacity);
    let finalization_guard = FinalizationGuard::new(
        Arc::clone(&summary_tx),
        rip_job_dir.clone(),
        &ctx.zip_states,
    );

    workspace_guard.disarm();
    tokio::spawn(run_ordered_dispatch(OrderedDispatchInput {
        deps: Arc::clone(&deps),
        bus: bus.clone(),
        shared: Arc::clone(&shared),
        ctx: Arc::clone(&ctx),
        job_controller: job_controller.clone(),
        upload_lane: Arc::clone(&upload_lane),
        queue,
        slots: slot_rx,
        ordered_capacity,
        rip_job_dir: rip_job_dir.clone(),
        finalization_guard,
    }));

    let is_cancelled = || {
        shared.lock().expect("job poisoned").job.is_cancelled
            || job_controller.is_cancelled()
            || queue_signal.is_cancelled()
    };

    for item in uncached_items {
        if is_cancelled() {
            break;
        }
        let slot = if let Some(cached) = item.cached.clone() {
            bus.set_codec(&shared, Some(cached.codec.as_str().to_string()));
            OrderedSlot::Cached {
                item: item.clone(),
                cached,
            }
        } else {
            match rip_fresh_item(RipFreshInput {
                deps: &deps,
                bus: &bus,
                shared: &shared,
                ctx: &ctx,
                job_controller: &job_controller,
                queue_signal: &queue_signal,
                item: item.clone(),
                rip_job_dir: &rip_job_dir,
            })
            .await
            {
                RipLaneOutcome::Ripped(upload_item) => OrderedSlot::Fresh(*upload_item),
                RipLaneOutcome::Finished => continue,
                RipLaneOutcome::Cancelled | RipLaneOutcome::Stop => break,
            }
        };

        let sent = tokio::select! {
            result = slot_tx.send(slot) => result.is_ok(),
            _ = job_controller.cancelled() => false,
            _ = queue_signal.cancelled() => false,
        };
        if !sent {
            break;
        }
    }

    let _ = slot_tx.send(OrderedSlot::Finished).await;
}

pub(super) struct OrderedSlotDrain<'a> {
    pub(super) slots: &'a mut tokio::sync::mpsc::Receiver<OrderedSlot>,
    pub(super) buffered: &'a mut VecDeque<OrderedSlot>,
    pub(super) received_finished: &'a mut bool,
    pub(super) ordered_capacity: usize,
}

pub(super) async fn wait_for_cache_resolution(
    mut resolution: tokio::sync::oneshot::Receiver<CacheResolution>,
    drain: &mut OrderedSlotDrain<'_>,
    job_controller: &CancellationToken,
    ctx: &Arc<TaskContext>,
) -> CacheResolution {
    loop {
        tokio::select! {
            result = &mut resolution => {
                return result.unwrap_or_else(|_| CacheResolution::Failed(
                    "cached result channel closed unexpectedly".to_owned(),
                ));
            }
            slot = drain.slots.recv(), if !*drain.received_finished => {
                match slot {
                    Some(OrderedSlot::Finished) => *drain.received_finished = true,
                    Some(slot) => {
                        if drain.buffered.len() < drain.ordered_capacity {
                            drain.buffered.push_back(slot);
                        } else {
                            set_fatal_error(
                                ctx,
                                "ordered dispatcher buffer exhausted while resolving cache".to_owned(),
                            );
                        }
                    }
                    None => {
                        return CacheResolution::Failed(
                            "ordered dispatcher stopped before its finish marker".to_owned(),
                        );
                    }
                }
            }
            _ = job_controller.cancelled() => return CacheResolution::Cancelled,
        }
    }
}

pub(super) async fn wait_for_ordered_rerip(
    mut completion: crate::queue::TaskReceiver,
    drain: &mut OrderedSlotDrain<'_>,
    job_controller: &CancellationToken,
    shared: &Arc<Mutex<TaskShared>>,
    ctx: &Arc<TaskContext>,
) -> RipLaneOutcome {
    let mut cancelled = false;
    loop {
        tokio::select! {
            result = &mut completion => {
                let outcome = ordered_rerip_result(result, shared, ctx, job_controller);
                if cancelled {
                    return RipLaneOutcome::Cancelled;
                }
                return outcome;
            }
            slot = drain.slots.recv(), if !*drain.received_finished => {
                match slot {
                    Some(OrderedSlot::Finished) => *drain.received_finished = true,
                    Some(slot) => {
                        if drain.buffered.len() < drain.ordered_capacity {
                            drain.buffered.push_back(slot);
                        } else {
                            set_fatal_error(
                                ctx,
                                "ordered dispatcher buffer exhausted while reripping".to_owned(),
                            );
                        }
                    }
                    None => {
                        return if cancelled {
                            RipLaneOutcome::Cancelled
                        } else {
                            set_fatal_error(
                                ctx,
                                "ordered dispatcher stopped before its finish marker".to_owned(),
                            );
                            RipLaneOutcome::Stop
                        };
                    }
                }
            }
            _ = job_controller.cancelled(), if !cancelled => {

                cancelled = true;
            }
        }
    }
}

pub(super) async fn run_ordered_dispatch<D>(input: OrderedDispatchInput<D>)
where
    D: TrackCache + AlbumCache + ProviderDeps + Delivery + TaskBookkeeping + 'static,
{
    let OrderedDispatchInput {
        deps,
        bus,
        shared,
        ctx,
        job_controller,
        upload_lane,
        queue,
        mut slots,
        ordered_capacity,
        rip_job_dir,
        mut finalization_guard,
    } = input;
    let mut received_finished = false;
    let mut stop_dispatch = false;
    let mut buffered = VecDeque::with_capacity(ordered_capacity);

    loop {
        let slot = if let Some(slot) = buffered.pop_front() {
            slot
        } else if received_finished {
            break;
        } else {
            match slots.recv().await {
                Some(slot) => slot,
                None => break,
            }
        };
        if matches!(&slot, OrderedSlot::Finished) {
            received_finished = true;
            continue;
        }
        if stop_dispatch
            || shared.lock().expect("job poisoned").job.is_cancelled
            || job_controller.is_cancelled()
        {
            stop_dispatch = true;
            continue;
        }

        match slot {
            OrderedSlot::Fresh(upload_item) => {
                let pushed = enqueue_upload_task(UploadLaneInput {
                    deps: Arc::clone(&deps),
                    bus: bus.clone(),
                    shared: Arc::clone(&shared),
                    ctx: Arc::clone(&ctx),
                    job_controller: job_controller.clone(),
                    queue_cancellation: None,
                    upload_lane: Arc::clone(&upload_lane),
                    upload_item,
                })
                .await;
                if !pushed {
                    if !shared.lock().expect("job poisoned").job.is_cancelled
                        && !job_controller.is_cancelled()
                    {
                        set_fatal_error(&ctx, "ordered upload could not be queued".to_owned());
                    }
                    stop_dispatch = true;
                }
            }
            OrderedSlot::Cached { item, cached } => {
                let cache_result = enqueue_cached_lane_task(CachedLaneInput {
                    deps: Arc::clone(&deps),
                    bus: bus.clone(),
                    shared: Arc::clone(&shared),
                    ctx: Arc::clone(&ctx),
                    job_controller: job_controller.clone(),

                    queue_signal: job_controller.clone(),
                    upload_lane: Arc::clone(&upload_lane),
                    item: item.clone(),
                    cached,
                })
                .await;
                let cache_result = match cache_result {
                    CacheEnqueueResult::Pending(pending) => {
                        let mut drain = OrderedSlotDrain {
                            slots: &mut slots,
                            buffered: &mut buffered,
                            received_finished: &mut received_finished,
                            ordered_capacity,
                        };
                        wait_for_cache_resolution(
                            pending.resolution,
                            &mut drain,
                            &job_controller,
                            &ctx,
                        )
                        .await
                    }
                    CacheEnqueueResult::Cancelled => CacheResolution::Cancelled,
                    CacheEnqueueResult::Failed(error) => CacheResolution::Failed(error),
                };

                match cache_result {
                    CacheResolution::Hit => {}
                    CacheResolution::Failed(error) => set_fatal_error(&ctx, error),
                    CacheResolution::Cancelled => stop_dispatch = true,
                    CacheResolution::Rerip => {
                        let completion = submit_ordered_rerip_item(OrderedReripInput {
                            deps: Arc::clone(&deps),
                            bus: bus.clone(),
                            shared: Arc::clone(&shared),
                            ctx: Arc::clone(&ctx),
                            job_controller: job_controller.clone(),
                            queue: queue.clone(),
                            item: PipelineItem {
                                cached: None,
                                ..item
                            },
                            rip_job_dir: rip_job_dir.clone(),
                        });
                        let mut drain = OrderedSlotDrain {
                            slots: &mut slots,
                            buffered: &mut buffered,
                            received_finished: &mut received_finished,
                            ordered_capacity,
                        };
                        match wait_for_ordered_rerip(
                            completion,
                            &mut drain,
                            &job_controller,
                            &shared,
                            &ctx,
                        )
                        .await
                        {
                            RipLaneOutcome::Ripped(upload_item) => {
                                if !enqueue_upload_task(UploadLaneInput {
                                    deps: Arc::clone(&deps),
                                    bus: bus.clone(),
                                    shared: Arc::clone(&shared),
                                    ctx: Arc::clone(&ctx),
                                    job_controller: job_controller.clone(),
                                    queue_cancellation: None,
                                    upload_lane: Arc::clone(&upload_lane),
                                    upload_item: *upload_item,
                                })
                                .await
                                {
                                    if !shared.lock().expect("job poisoned").job.is_cancelled
                                        && !job_controller.is_cancelled()
                                    {
                                        set_fatal_error(
                                            &ctx,
                                            "ordered fallback upload could not be queued"
                                                .to_owned(),
                                        );
                                    }
                                    stop_dispatch = true;
                                }
                            }
                            RipLaneOutcome::Finished => {}
                            RipLaneOutcome::Cancelled | RipLaneOutcome::Stop => {
                                stop_dispatch = true;
                            }
                        }
                    }
                }
            }
            OrderedSlot::Finished => unreachable!("finished slot handled above"),
        }
    }

    if !received_finished
        && !shared.lock().expect("job poisoned").job.is_cancelled
        && !job_controller.is_cancelled()
    {
        finalization_guard
            .finish(Err("ordered dispatcher stopped unexpectedly".to_owned()))
            .await;
        return;
    }

    enqueue_finalize_marker(
        deps,
        bus,
        shared,
        ctx,
        job_controller,
        upload_lane,
        finalization_guard,
    )
    .await;
}
