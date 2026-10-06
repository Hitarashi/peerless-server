//! Job resolution, the rip job body, and the job-table/workspace cleanup guards.

use super::*;

#[derive(Debug, Clone)]
pub(super) struct ResolvedTrackItem {
    pub(super) id: String,
    pub(super) title: Option<String>,
    pub(super) artist: Option<String>,
    pub(super) artwork_url: Option<String>,
    pub(super) storefront: Option<String>,
    pub(super) is_streamable: Option<bool>,
}

#[derive(Clone)]
pub(super) struct PipelineItem {
    pub(super) track_id: String,
    pub(super) storefront: Option<String>,
    pub(super) meta_title: Option<String>,
    pub(super) meta_artist: Option<String>,
    pub(super) artwork_url: Option<String>,
    pub(super) is_streamable: Option<bool>,
    pub(super) rendition: Rendition,
    pub(super) cached: Option<CachedTrack>,
    pub(super) track_index: Option<u32>,
    pub(super) total_tracks: Option<u32>,
}

pub(super) struct PipelineRipResult {
    pub(super) track_id: String,
    pub(super) rip_result: TrackRipResult,
    pub(super) start_time_ms: u64,
    pub(super) rendition: Rendition,
    pub(super) track_index: Option<u32>,
    pub(super) total_tracks: Option<u32>,
}

pub(super) struct JobTableGuard {
    pub(super) jobs: Arc<Mutex<HashMap<String, Arc<Mutex<TaskShared>>>>>,
    pub(super) job_id: String,
    pub(super) armed: bool,
}

impl JobTableGuard {
    pub(super) fn new(
        jobs: Arc<Mutex<HashMap<String, Arc<Mutex<TaskShared>>>>>,
        job_id: String,
    ) -> Self {
        Self {
            jobs,
            job_id,
            armed: true,
        }
    }

    pub(super) fn remove(&mut self) {
        if self.armed {
            self.jobs
                .lock()
                .expect("jobs poisoned")
                .remove(&self.job_id);
            self.armed = false;
        }
    }
}

impl Drop for JobTableGuard {
    fn drop(&mut self) {
        self.remove();
    }
}

pub(super) struct WorkspaceGuard {
    pub(super) paths: Vec<PathBuf>,
    pub(super) armed: bool,
}

impl WorkspaceGuard {
    pub(super) fn new() -> Self {
        Self {
            paths: Vec::new(),
            armed: true,
        }
    }

    pub(super) fn add(&mut self, path: PathBuf) {
        self.paths.push(path);
    }

    pub(super) fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for WorkspaceGuard {
    fn drop(&mut self) {
        if self.armed {
            for path in &self.paths {
                let _ = std::fs::remove_dir_all(path);
            }
        }
    }
}

impl RipOrchestrator {
    pub(super) async fn run_job<D: TaskDeps>(
        &self,
        deps: Arc<D>,
        options: &RipTaskOptions,
        shared: Arc<Mutex<TaskShared>>,
        job_controller: CancellationToken,
        settings: BotSettings,
    ) -> Result<RipTaskSummary, OrchestratorError> {
        self.set_phase(&shared, TaskPhase::Resolving);
        self.bus
            .set_job_activity(&shared, Some(TaskActivity::Resolving));
        self.bus.emit_progress(&shared);

        let mut resolved_tracks: Vec<ResolvedTrackItem> = Vec::new();
        let mut album_name: Option<String> = None;
        let mut album_artist: Option<String> = None;
        let mut album_artwork_url: Option<String> = None;
        let mut album_release_date: Option<String> = None;
        let mut album_genre: Option<String> = None;
        let mut album_record_label: Option<String> = None;
        let mut album_copyright: Option<String> = None;
        let mut album_id: Option<String> = None;
        let mut album_sf: Option<String> = None;
        let mut resolution_failures = Vec::new();

        for item in &options.parsed_items {
            if job_controller.is_cancelled() {
                return Err(OrchestratorError::Cancelled);
            }

            let effective_sf = item
                .storefront
                .clone()
                .or_else(|| options.single_storefront.clone())
                .unwrap_or_else(|| resolve_default_storefront(&settings).to_owned());

            let resolution: Result<(), String> = match item.kind {
                TargetKind::Track => {
                    resolved_tracks.push(ResolvedTrackItem {
                        id: item.id.clone(),
                        title: None,
                        artist: None,
                        artwork_url: None,
                        storefront: Some(effective_sf.clone()),
                        is_streamable: None,
                    });
                    Ok(())
                }
                TargetKind::Album => match deps
                    .fetch_album_tracks(
                        &item.id,
                        effective_sf.as_str().into(),
                    )
                    .await
                {
                    Ok(AlbumTracks { album, tracks }) => {
                        album_name = Some(album.album.clone());
                        album_artist = Some(album.artist.clone());
                        album_artwork_url = Some(album.artwork_url.clone());
                        album_release_date = Some(album.release_date.clone());
                        album_genre = album.genre.clone();
                        album_record_label = album.record_label.clone();
                        album_copyright = album.copyright.clone();
                        album_id = Some(item.id.clone());
                        album_sf = Some(effective_sf.clone());
                        for t in tracks {
                            resolved_tracks.push(ResolvedTrackItem {
                                id: t.id.clone(),
                                title: Some(t.title.clone()),
                                artist: Some(t.artist.clone()),
                                artwork_url: if t.artwork_url.is_empty() {
                                    (!album.artwork_url.is_empty())
                                        .then(|| album.artwork_url.clone())
                                } else {
                                    Some(t.artwork_url.clone())
                                },
                                storefront: Some(effective_sf.clone()),
                                is_streamable: t.is_streamable,
                            });
                        }
                        Ok(())
                    }
                    Err(e) => Err(e),
                },
                TargetKind::Artist => {
                    match deps
                        .fetch_artist_tracks(
                            &item.id,
                            effective_sf.as_str().into(),
                        )
                        .await
                    {
                        Ok(ArtistTracks {
                            artist_name,
                            tracks,
                            ..
                        }) => {
                            album_artist = Some(artist_name.clone());
                            for t in tracks {
                                resolved_tracks.push(ResolvedTrackItem {
                                    id: t.id.clone(),
                                    title: Some(t.title.clone()),
                                    artist: Some(t.artist.clone()),
                                    artwork_url: (!t.artwork_url.is_empty())
                                        .then(|| t.artwork_url.clone()),
                                    storefront: Some(effective_sf.clone()),
                                    is_streamable: None,
                                });
                            }
                            Ok(())
                        }
                        Err(e) => Err(e),
                    }
                }
                TargetKind::Playlist => {
                    match deps
                        .fetch_playlist_tracks(
                            &item.id,
                            effective_sf.as_str().into(),
                        )
                        .await
                    {
                        Ok(data) => {
                            for t in data.tracks {
                                resolved_tracks.push(ResolvedTrackItem {
                                    id: t.id.clone(),
                                    title: Some(t.title.clone()),
                                    artist: Some(t.artist.clone()),
                                    artwork_url: None,
                                    storefront: Some(effective_sf.clone()),
                                    is_streamable: None,
                                });
                            }
                            Ok(())
                        }
                        Err(e) => Err(e),
                    }
                }
            };

            if let Err(err_msg) = resolution {
                tracing::error!(
                    kind = kind_str(item.kind),
                    id = %item.id,
                    error = %err_msg,
                    "Failed to resolve target item"
                );
                resolution_failures.push(ResolutionFailure {
                    kind: item.kind,
                    id: item.id.clone(),
                    error: err_msg,
                });
            }
        }

        if resolved_tracks.is_empty() {
            if resolution_failures.is_empty() {
                resolution_failures.push(ResolutionFailure {
                    kind: TargetKind::Track,
                    id: String::new(),
                    error: "No valid tracks found to process.".to_string(),
                });
            }
            return Err(OrchestratorError::ResolutionFailed {
                failures: resolution_failures,
            });
        }

        let mut seen_ids: HashSet<String> = HashSet::new();
        let unique_tracks: Vec<ResolvedTrackItem> = resolved_tracks
            .into_iter()
            .filter(|t| seen_ids.insert(t.id.clone()))
            .collect();

        let mut capped_count = 0usize;
        let max_collection_limit = settings.max_collection_tracks;
        let mut tracks_to_process = unique_tracks;
        if !options.is_admin
            && max_collection_limit > 0
            && tracks_to_process.len() > max_collection_limit as usize
        {
            capped_count = tracks_to_process.len() - max_collection_limit as usize;
            let original = tracks_to_process.len();
            tracks_to_process.truncate(max_collection_limit as usize);
            tracing::warn!(
                user_id = options.user_id,
                original,
                capped = max_collection_limit,
                "Collection capped for non-admin user"
            );
        }

        pub(super) fn non_empty(s: &Option<String>) -> Option<&str> {
            s.as_deref().filter(|s| !s.is_empty())
        }
        let header = match (non_empty(&album_name), non_empty(&album_artist)) {
            (Some(name), Some(artist)) => {
                if let (Some(id), Some(sf)) = (&album_id, &album_sf) {
                    let album_url =
                        deps.album_url(id, sf.as_str().into());
                    if let Some(album_url) = album_url {
                        format!(
                            "Album: <a href=\"{album_url}\"><b>{}</b></a> by <b>{}</b>",
                            html_escape(name),
                            html_escape(artist)
                        )
                    } else {
                        format!(
                            "Album: <b>{}</b> by <b>{}</b>",
                            html_escape(name),
                            html_escape(artist)
                        )
                    }
                } else {
                    format!(
                        "Album: <b>{}</b> by <b>{}</b>",
                        html_escape(name),
                        html_escape(artist)
                    )
                }
            }
            _ if tracks_to_process.len() == 1 => {
                let first = &tracks_to_process[0];
                match (non_empty(&first.title), non_empty(&first.artist)) {
                    (Some(title), Some(artist)) => {
                        format!(
                            "<b>{}</b> - <b>{}</b>",
                            html_escape(title),
                            html_escape(artist)
                        )
                    }
                    _ if options.is_cache_only => {
                        format!("Track Cache: <code>{}</code>", html_escape(&first.id))
                    }
                    _ => format!("Track ID: <code>{}</code>", html_escape(&first.id)),
                }
            }
            _ if options.is_cache_only => {
                format!("Batch Cache: <b>{} tracks</b>", tracks_to_process.len())
            }
            _ => format!("Batch: <b>{} tracks</b>", tracks_to_process.len()),
        };
        {
            let mut guard = shared.lock().expect("job poisoned");
            guard.job.job_header = header;
            guard.job.total_tracks = tracks_to_process.len();
        }

        let is_album_job = options.parsed_items.len() == 1
            && options.parsed_items[0].kind == TargetKind::Album
            && tracks_to_process.len() > 1;
        let zip_build = is_album_job;

        let zip_deliver = is_album_job && !options.is_cache_only;
        let mut warnings = Vec::new();

        let zip_generation_hash = zip_build.then(|| {
            let ids: Vec<&str> = tracks_to_process.iter().map(|t| t.id.as_str()).collect();
            album_generation_hash("apple", &options.parsed_items[0].id, &ids)
        });
        let mut workspace_guard = WorkspaceGuard::new();
        let mut zip_states: Vec<Arc<ZipState>> = Vec::new();
        if zip_build {
            for rendition in options.rendition_policy.renditions() {
                let dir = std::env::temp_dir().join(format!(
                    "zip_job_{}_{}",
                    cuid2::create_id(),
                    match rendition {
                        Rendition::Primary => "primary",
                        Rendition::Atmos => "atmos",
                    }
                ));
                workspace_guard.add(dir.clone());
                if let Err(error) = tokio::fs::create_dir_all(&dir).await {
                    return Err(OrchestratorError::Message(format!(
                        "create ZIP workspace: {error}"
                    )));
                }
                zip_states.push(Arc::new(ZipState {
                    rendition: *rendition,
                    dir,
                    sources: Arc::new(Mutex::new(Vec::new())),
                    codec: Arc::new(Mutex::new(None)),
                    generation_hash: zip_generation_hash.clone(),
                }));
            }
        }

        self.set_phase(&shared, TaskPhase::CheckingCache);
        let check_item = { shared.lock().expect("job poisoned").job.job_header.clone() };
        self.bus.set_job_activity(
            &shared,
            Some(TaskActivity::CheckingCache { item: check_item }),
        );
        self.bus.emit_progress(&shared);
        let requested_ids: Vec<String> = tracks_to_process
            .iter()
            .map(|track| track.id.clone())
            .collect();
        let mut existing_tracks_map = match find_cached_tracks_with_retry(
            deps.as_ref(),
            &requested_ids,
            &self.config.storage_retry,
        )
        .await
        {
            Ok(existing) => existing,
            Err(error) => {
                tracing::error!(%error, "track cache lookup failed; refusing to start media work");
                return Err(OrchestratorError::Message(error.to_string()));
            }
        };

        self.bus.set_job_activity(&shared, None);

        if options.is_force && options.is_admin {
            let mut old_message_ids: Vec<DumpMessageRef> = Vec::new();
            for item in &tracks_to_process {
                for rendition in options.rendition_policy.renditions() {
                    for codec in rendition.accepted_cache_codecs() {
                        let lookup_key = (item.id.clone(), *codec);
                        if let Some(cached) = existing_tracks_map.remove(&lookup_key) {
                            old_message_ids.push(DumpMessageRef::new(cached.message_id));
                            let _ = deps.delete_track(&item.id, Some(*codec)).await;
                        }
                    }
                }
            }
            if !old_message_ids.is_empty() {
                tracing::debug!(
                    count = old_message_ids.len(),
                    "Deleting old dump messages on force re-rip prior to queue"
                );
                let _ = deps.retract_dump(&old_message_ids).await;
            }
        }

        let mut pipeline_items: Vec<PipelineItem> = Vec::new();
        let cached_count = 0usize;
        let is_multi_track = tracks_to_process.len() > 1;
        let first_delivered_msg_id: Option<ChatMessageRef> = None;

        let existing_album_rows = if zip_build {
            match deps
                .find_albums(&options.parsed_items[0].id, None)
                .await
            {
                Ok(rows) => rows,
                Err(error) => {
                    return Err(OrchestratorError::Message(format!(
                        "album ZIP cache lookup failed: {error}"
                    )));
                }
            }
        } else {
            Vec::new()
        };

        let zip_expectations: HashMap<Rendition, AlbumReplacementExpectation> = if zip_build {
            options
                .rendition_policy
                .renditions()
                .iter()
                .map(|rendition| {
                    let replacement_codec = match rendition {
                        Rendition::Primary => Codec::Alac,
                        Rendition::Atmos => Codec::Ec3,
                    };
                    let rows = existing_album_rows
                        .iter()
                        .filter(|row| archive_codec_replaced(replacement_codec, row.codec))
                        .collect::<Vec<_>>();
                    let expectation = match rows.first() {
                        None => AlbumReplacementExpectation::Empty,
                        Some(first)
                            if rows
                                .iter()
                                .all(|row| row.generation_hash == first.generation_hash) =>
                        {
                            AlbumReplacementExpectation::Generation(first.generation_hash.clone())
                        }
                        Some(_) => AlbumReplacementExpectation::Mixed,
                    };
                    (*rendition, expectation)
                })
                .collect()
        } else {
            HashMap::new()
        };

        let mut zip_reuse: HashMap<Rendition, Vec<CachedAlbum>> = HashMap::new();
        if let Some(hash) = &zip_generation_hash
            && !options.is_force
        {
            for rendition in options.rendition_policy.renditions() {
                let codecs: &[Codec] = match rendition {
                    Rendition::Primary => &[Codec::Alac, Codec::Aac],
                    Rendition::Atmos => &[Codec::Ec3],
                };
                for codec in codecs {
                    let rows = existing_album_rows
                        .iter()
                        .filter(|row| row.codec == *codec)
                        .cloned()
                        .collect::<Vec<_>>();
                    if !rows.is_empty()
                        && rows.iter().all(|row| row.generation_hash == *hash)
                        && rows.len() == rows[0].total_parts.max(1) as usize
                        && (1..=rows.len())
                            .zip(&rows)
                            .all(|(n, row)| row.part_index as usize == n)
                    {
                        zip_reuse.insert(*rendition, rows);
                        break;
                    }
                }
            }
        }

        let is_multi = tracks_to_process.len() > 1;
        let total_count = is_multi.then_some(tracks_to_process.len() as u32);
        for (index, item) in tracks_to_process.iter().enumerate() {
            if job_controller.is_cancelled() {
                return Err(OrchestratorError::Cancelled);
            }
            let track_idx = is_multi.then_some((index + 1) as u32);
            for rendition in options.rendition_policy.renditions() {
                let cached = rendition
                    .accepted_cache_codecs()
                    .iter()
                    .find_map(|codec| existing_tracks_map.get(&(item.id.clone(), *codec)).cloned());
                pipeline_items.push(PipelineItem {
                    track_id: item.id.clone(),
                    storefront: item.storefront.clone(),
                    meta_title: item.title.clone(),
                    meta_artist: item.artist.clone(),
                    artwork_url: item.artwork_url.clone(),
                    is_streamable: item.is_streamable,
                    rendition: *rendition,
                    cached,
                    track_index: track_idx,
                    total_tracks: total_count,
                });
            }
        }
        for rendition in options.rendition_policy.renditions() {
            let reuse_valid = zip_reuse.contains_key(rendition)
                && (*rendition == Rendition::Atmos
                    || pipeline_items
                        .iter()
                        .filter(|item| item.rendition == *rendition)
                        .all(|item| item.cached.is_some()));
            if !reuse_valid {
                zip_reuse.remove(rendition);
            }
        }

        let zip_reuse_atmos_track_count = if zip_reuse.contains_key(&Rendition::Atmos) {
            let count = tracks_to_process
                .iter()
                .filter(|item| {
                    existing_tracks_map
                        .get(&(item.id.clone(), Codec::Ec3))
                        .is_some_and(|cached| cached.codec == Codec::Ec3)
                })
                .count();
            (count > 0).then_some(count)
        } else {
            None
        };
        if zip_reuse.contains_key(&Rendition::Atmos) {
            pipeline_items.retain(|item| item.rendition != Rendition::Atmos);
        }
        let mut uncached_items = pipeline_items;
        let has_fresh = uncached_items.iter().any(|item| item.cached.is_none());
        let cache_hits_present = uncached_items.iter().any(|item| item.cached.is_some());
        let can_rip_live = settings.can_rip_live(options.is_admin)
            && settings.can_rip_apple(options.is_admin);

        if !can_rip_live
            && !settings.can_rip_apple(options.is_admin)
            && (has_fresh || !cache_hits_present)
        {
            warnings.push(format!(
                "{} live ripping is currently disabled by administrator.",
                "Apple Music"
            ));
        }

        let zip_delivery: Option<ZipDeliveryInfo> = None;

        let summary = |cached_count: usize,
                       ripped_count: usize,
                       failed: Vec<FailedTrack>,
                       skipped: Vec<String>,
                       elapsed: &str,
                       zip_delivery: Option<ZipDeliveryInfo>,
                       first_msg_id: Option<ChatMessageRef>| {
            let guard = shared.lock().expect("job poisoned");
            RipTaskSummary {
                job_id: guard.job.id.clone(),
                job_header: guard.job.job_header.clone(),
                total_tracks: guard.job.total_tracks,
                cached_count,
                ripped_count,
                failed_count: failed.len(),
                failed_tracks: failed,
                skipped_uncached_tracks: skipped,
                total_elapsed_sec: elapsed.to_string(),
                capped_count,
                max_collection_limit,
                is_cache_only: options.is_cache_only,
                is_group: options.is_group,
                warnings: warnings.clone(),
                zip_delivery: zip_delivery.clone(),
                zip_deliveries: zip_delivery.clone().into_iter().collect(),
                first_delivered_msg_id: first_msg_id,
                codec: zip_delivery.as_ref().and_then(|z| z.codec.clone()),
            }
        };

        if !can_rip_live && (!zip_build || has_fresh) && !cache_hits_present {
            let skipped: Vec<String> = uncached_items
                .iter()
                .filter(|item| item.cached.is_none() && item.rendition == Rendition::Primary)
                .map(|i| i.track_id.clone())
                .collect();
            {
                let mut guard = shared.lock().expect("job poisoned");
                guard.job.skipped_count = skipped.len();
            }
            self.bus
                .set_job_activity(&shared, Some(TaskActivity::SkippingUncached));
            self.bus.emit_progress(&shared);
            let elapsed = format!(
                "{:.1}",
                (now_ms().saturating_sub(shared.lock().expect("job poisoned").job.start_time_ms)
                    as f64)
                    / 1000.0
            );

            return Ok(summary(
                cached_count,
                0,
                Vec::new(),
                skipped,
                &elapsed,
                zip_delivery,
                first_delivered_msg_id,
            ));
        }
        if !can_rip_live && has_fresh && cache_hits_present {
            let skipped: Vec<String> = uncached_items
                .iter()
                .filter(|item| item.cached.is_none() && item.rendition == Rendition::Primary)
                .map(|item| item.track_id.clone())
                .collect();
            shared.lock().expect("job poisoned").job.skipped_count = skipped.len();
            uncached_items.retain(|item| item.cached.is_some());
        }

        let all_tracks_cached = uncached_items.iter().all(|item| item.cached.is_some());
        let album_zip_reusable = !zip_build || zip_reuse.contains_key(&Rendition::Primary);

        if all_tracks_cached && album_zip_reusable {
            let current_job_id = shared.lock().expect("job poisoned").job.id.clone();
            tracing::info!(
                job_id = %current_job_id,
                tracks_count = uncached_items.len(),
                is_album = is_album_job,
                "Job is 100% cached; executing priority cache delivery bypassing rip queue"
            );
            self.set_phase(&shared, TaskPhase::Delivering);
            self.bus
                .set_job_activity(&shared, Some(TaskActivity::CachedDelivered));
            self.bus.emit_progress(&shared);

            let _permit = self
                .cache_delivery_semaphore
                .acquire()
                .await
                .map_err(|_| OrchestratorError::Cancelled)?;

            if job_controller.is_cancelled() {
                let elapsed = format!(
                    "{:.1}",
                    (now_ms().saturating_sub(shared.lock().expect("job poisoned").job.start_time_ms)
                        as f64)
                        / 1000.0
                );
                return Ok(summary(0, 0, Vec::new(), Vec::new(), &elapsed, None, None));
            }

            {
                let guard = shared.lock().expect("job poisoned");
                self.bus.emit(&OrchestratorEvent::Started(&guard.job));
            }

            let mut cached_count = 0usize;
            let mut first_delivered_msg_id: Option<ChatMessageRef> = None;
            let should_pace = uncached_items.len() > 1;
            let mut delivery_failed = false;

            if !options.is_cache_only && !zip_deliver {
                for item in uncached_items.iter_mut() {
                    if job_controller.is_cancelled() {
                        let elapsed = format!(
                            "{:.1}",
                            (now_ms().saturating_sub(
                                shared.lock().expect("job poisoned").job.start_time_ms
                            ) as f64)
                                / 1000.0
                        );
                        return Ok(summary(
                            cached_count,
                            0,
                            Vec::new(),
                            Vec::new(),
                            &elapsed,
                            None,
                            first_delivered_msg_id,
                        ));
                    }
                    if let Some(cached) = &item.cached {
                        let cache_codec = cached.codec;
                        self.bus.set_download(
                            &shared,
                            Some(DownloadLane::CachedDelivery {
                                track: TrackLabel::new(
                                    item.meta_title.clone().unwrap_or_default(),
                                    item.meta_artist.clone().unwrap_or_default(),
                                )
                                .with_artwork_url(item.artwork_url.clone())
                                .with_position(item.track_index, item.total_tracks),
                            }),
                        );
                        self.bus.emit_progress(&shared);

                        let reply_to = (options.delivery_chat_id == options.chat_id)
                            .then_some(options.reply_to_message_id)
                            .flatten();

                        let copy_result = tokio::select! {
                            res = deps.deliver_to_chat(ChatDelivery::DumpCopy {
                                destination: ChatRef::new(options.delivery_chat_id),
                                source: DumpMessageRef::new(cached.message_id),
                                reply_to: reply_to.map(ChatMessageRef::new),
                                silent: is_multi_track,
                            }) => res,
                            _ = job_controller.cancelled() => {
                                let elapsed = format!(
                                    "{:.1}",
                                    (now_ms().saturating_sub(
                                        shared.lock().expect("job poisoned").job.start_time_ms
                                    ) as f64)
                                        / 1000.0
                                );
                                return Ok(summary(
                                    cached_count,
                                    0,
                                    Vec::new(),
                                    Vec::new(),
                                    &elapsed,
                                    None,
                                    first_delivered_msg_id,
                                ));
                            }
                        };

                        match copy_result {
                            Ok(DeliveryReceipt::Message(sent_id)) => {
                                if first_delivered_msg_id.is_none() {
                                    first_delivered_msg_id = Some(sent_id);
                                }

                                cached_count += 1;
                                {
                                    let mut guard = shared.lock().expect("job poisoned");
                                    guard.job.cached_count = cached_count;
                                }
                                self.bus.set_download(&shared, None);
                                self.bus
                                    .set_job_activity(&shared, Some(TaskActivity::CachedDelivered));
                                self.bus.emit_progress(&shared);

                                if should_pace {
                                    tokio::time::sleep(tokio::time::Duration::from_millis(120))
                                        .await;
                                }
                            }
                            Ok(DeliveryReceipt::PreviewDelivered) | Err(_) => {
                                tracing::warn!(
                                    track_id = %item.track_id,
                                    "cached track delivery failed; deleting cache and falling back to rip queue"
                                );
                                let _ = deps.delete_track(&item.track_id, Some(cache_codec)).await;
                                item.cached = None;
                                self.bus.set_download(&shared, None);
                                delivery_failed = true;
                                break;
                            }
                        }
                    }
                }
            } else if zip_deliver && !options.is_cache_only {
                let mut zip_deliveries = Vec::new();
                for state in &zip_states {
                    if let Some(rows) = zip_reuse.get(&state.rendition) {
                        let direct_res = deliver_cached_zip_rows_direct(
                            &deps,
                            &shared,
                            options,
                            state.rendition,
                            rows,
                            &job_controller,
                            &mut first_delivered_msg_id,
                        )
                        .await;

                        match direct_res {
                            Ok((delivered, size)) => {
                                if delivered > 0 {
                                    let codec = rows[0].codec.as_str().to_owned();
                                    let info = ZipDeliveryInfo {
                                        album: album_name
                                            .clone()
                                            .unwrap_or_else(|| "Album".to_owned()),
                                        artist: album_artist
                                            .clone()
                                            .unwrap_or_else(|| "Unknown Artist".to_owned()),
                                        release_year: album_release_date
                                            .clone()
                                            .unwrap_or_default()
                                            .chars()
                                            .take(4)
                                            .collect(),
                                        total_tracks: tracks_to_process.len(),
                                        delivered_tracks: if state.rendition == Rendition::Primary {
                                            Some(tracks_to_process.len())
                                        } else {
                                            zip_reuse_atmos_track_count
                                        },
                                        total_parts: delivered,
                                        size_bytes: size,
                                        is_partial: false,
                                        album_id: options
                                            .parsed_items
                                            .first()
                                            .map(|i| i.id.clone())
                                            .unwrap_or_default(),
                                        album_url: match (&album_id, &album_sf) {
                                            (Some(id), Some(storefront)) => deps.album_url(
                                                id,
                                                storefront.as_str().into(),
                                            ),
                                            _ => None,
                                        },
                                        artwork_url: album_artwork_url
                                            .clone()
                                            .filter(|url| !url.is_empty()),
                                        genre: album_genre.clone(),
                                        record_label: album_record_label.clone(),
                                        copyright: album_copyright.clone(),
                                        photo_delivered: false,
                                        codec: Some(codec),
                                    };
                                    zip_deliveries.push(info);
                                }
                            }
                            Err(err) => {
                                tracing::warn!(
                                    error = %err,
                                    "cached ZIP delivery failed; falling back to rip queue"
                                );
                                delivery_failed = true;
                                break;
                            }
                        }
                    }
                }
                if !delivery_failed {
                    cached_count = tracks_to_process.len();
                    {
                        let mut guard = shared.lock().expect("job poisoned");
                        guard.job.cached_count = cached_count;
                    }
                    self.bus
                        .set_job_activity(&shared, Some(TaskActivity::CachedDelivered));
                    self.bus.emit_progress(&shared);
                    let first_zip = zip_deliveries.first().cloned();
                    let elapsed = format!(
                        "{:.1}",
                        (now_ms()
                            .saturating_sub(shared.lock().expect("job poisoned").job.start_time_ms)
                            as f64)
                            / 1000.0
                    );
                    let mut res = summary(
                        cached_count,
                        0,
                        Vec::new(),
                        Vec::new(),
                        &elapsed,
                        first_zip,
                        first_delivered_msg_id,
                    );
                    res.zip_deliveries = zip_deliveries;
                    return Ok(res);
                }
            } else {
                cached_count = tracks_to_process.len();
                {
                    let mut guard = shared.lock().expect("job poisoned");
                    guard.job.cached_count = cached_count;
                }
                self.bus
                    .set_job_activity(&shared, Some(TaskActivity::CachedDelivered));
                self.bus.emit_progress(&shared);
            }

            if !delivery_failed {
                let elapsed = format!(
                    "{:.1}",
                    (now_ms().saturating_sub(shared.lock().expect("job poisoned").job.start_time_ms)
                        as f64)
                        / 1000.0
                );

                return Ok(summary(
                    cached_count,
                    0,
                    Vec::new(),
                    Vec::new(),
                    &elapsed,
                    None,
                    first_delivered_msg_id,
                ));
            }
        }

        self.set_phase(&shared, TaskPhase::Queued);
        self.bus
            .set_job_activity(&shared, Some(TaskActivity::Queued { position: 1 }));
        self.bus.emit_progress(&shared);

        let job_id = shared.lock().expect("job poisoned").job.id.clone();
        tracing::info!(
            job_id = %job_id,
            tracks_count = uncached_items.len(),
            force = options.is_force,
            is_group = options.is_group,
            is_cache_only = options.is_cache_only,
            delivery_chat_id = options.delivery_chat_id,
            "Rip job queued"
        );

        let queue_start_time = shared.lock().expect("job poisoned").job.start_time_ms;

        let _ = self.ensure_upload_lane();

        let job_ctx = Arc::new(TaskContext {
            config: self.config.clone(),
            options: options.clone(),
            zip_build,
            zip_deliver,
            zip_states: zip_states.clone(),
            zip_reuse: zip_reuse.clone(),
            zip_expectations: zip_expectations.clone(),
            zip_reuse_atmos_track_count,
            zip_album: album_name.clone().unwrap_or_else(|| "Album".to_owned()),
            zip_artist: album_artist
                .clone()
                .unwrap_or_else(|| "Unknown Artist".to_owned()),
            zip_album_id: options
                .parsed_items
                .first()
                .map(|item| item.id.clone())
                .unwrap_or_default(),
            zip_album_url: match (&album_id, &album_sf) {
                (Some(id), Some(storefront)) => {
                    deps.album_url(id, storefront.as_str().into())
                }
                _ => None,
            },
            zip_genre: album_genre.clone(),
            zip_record_label: album_record_label.clone(),
            zip_copyright: album_copyright.clone(),
            zip_artwork_url: album_artwork_url.clone().filter(|url| !url.is_empty()),
            zip_release_date: album_release_date.clone().unwrap_or_default(),
            warnings: warnings.clone(),
            atmos_warning: Arc::new(std::sync::Mutex::new(None)),
            finalizing_rendition: Arc::new(Mutex::new(None)),
            fatal_error: Arc::new(Mutex::new(None)),
            primary_zip_error: Arc::new(Mutex::new(None)),
            zip_new_dump_messages: Arc::new(Mutex::new(Vec::new())),
            is_multi_track,
            max_collection_limit,
            capped_count,
            queue_start_time_ms: queue_start_time,
            ripped_count: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            failed_tracks: Arc::new(Mutex::new(Vec::new())),
            first_delivered_msg_id: Arc::new(Mutex::new(None)),
            zip_delivery_infos: Arc::new(Mutex::new(Vec::new())),
        });

        let rip_job_dir =
            std::env::temp_dir().join(format!("rip_job_{id}", id = cuid2::create_id()));
        workspace_guard.add(rip_job_dir.clone());
        if let Err(error) = tokio::fs::create_dir_all(&rip_job_dir).await {
            return Err(OrchestratorError::Message(format!(
                "create rip workspace: {error}"
            )));
        }

        let (summary_tx, summary_rx) = tokio::sync::oneshot::channel::<FinalizeResult>();
        let summary_tx = Arc::new(Mutex::new(Some(summary_tx)));

        let task_deps = Arc::clone(&deps);
        let task_shared = Arc::clone(&shared);
        let task_items = uncached_items;
        let task_controller = job_controller.clone();
        let task_bus = self.bus.clone();
        let task_job_ctx = Arc::clone(&job_ctx);
        let task_upload_lane = self.upload_lane.clone();
        let task_queue = self.queue.clone();
        let task_workspace_guard = workspace_guard;
        let task_summary_tx = Arc::clone(&summary_tx);
        let task_rip_job_dir = rip_job_dir.clone();

        let callback_shared = Arc::clone(&shared);
        let callback_bus = self.bus.clone();
        let on_position_change = Arc::new(move |position: u64| {
            callback_shared
                .lock()
                .expect("job poisoned")
                .job
                .queue_position = Some(position);
            callback_bus.set_job_activity(
                &callback_shared,
                Some(TaskActivity::Queued {
                    position: u32::try_from(position).unwrap_or(u32::MAX),
                }),
            );
            callback_bus.emit_progress(&callback_shared);
        });
        let callback_shared = Arc::clone(&shared);
        let callback_bus = self.bus.clone();
        let on_start = Arc::new(move || {
            {
                let mut guard = callback_shared.lock().expect("job poisoned");
                guard.job.phase = TaskPhase::Processing;
                guard.job.queue_position = Some(0);
            }
            let guard = callback_shared.lock().expect("job poisoned");
            callback_bus.emit(&OrchestratorEvent::Started(&guard.job));
            drop(guard);
            callback_bus.set_job_activity(&callback_shared, None);
            callback_bus.emit_progress(&callback_shared);
        });

        let task = move |queue_signal: CancellationToken| {
            Box::pin(async move {
                run_lane_one(LaneOneContext {
                    deps: task_deps,
                    bus: task_bus,
                    shared: task_shared,
                    uncached_items: &task_items,
                    job_controller: task_controller,
                    queue_signal,
                    ctx: task_job_ctx,
                    upload_lane: task_upload_lane,
                    queue: task_queue,
                    workspace_guard: task_workspace_guard,
                    summary_tx: task_summary_tx,
                    rip_job_dir: task_rip_job_dir,
                })
                .await
            }) as std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>
        };

        match self
            .queue
            .enqueue(
                task,
                Some(EnqueueOptions {
                    signal: Some(job_controller.clone()),
                    on_position_change: Some(on_position_change),
                    on_start: Some(on_start),
                }),
            )
            .await
        {
            Ok(_) => {}
            Err(error) => {
                let _ = tokio::fs::remove_dir_all(&rip_job_dir).await;
                for state in &zip_states {
                    let _ = tokio::fs::remove_dir_all(&state.dir).await;
                }
                return Err(error.into());
            }
        }

        match summary_rx.await {
            Ok(Ok(summary)) => Ok(summary),
            Ok(Err(error)) => Err(OrchestratorError::Message(error)),
            Err(_) => {
                let _ = tokio::fs::remove_dir_all(&rip_job_dir).await;
                for state in &zip_states {
                    let _ = tokio::fs::remove_dir_all(&state.dir).await;
                }
                Err(OrchestratorError::Message(
                    "finalize marker stopped unexpectedly".to_owned(),
                ))
            }
        }
    }
}
