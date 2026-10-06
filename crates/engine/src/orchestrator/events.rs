//! Event bus: subscriber fan-out, download/upload lane snapshots, and task terminalisation.

use super::*;

#[derive(Clone)]
pub(super) struct EventBus {
    pub(super) subscribers: Arc<Mutex<Vec<EventCallback>>>,
}

impl EventBus {
    pub(super) fn new() -> Self {
        Self {
            subscribers: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub(super) fn emit(&self, event: &OrchestratorEvent<'_>) {
        for cb in self
            .subscribers
            .lock()
            .expect("subscribers poisoned")
            .iter()
        {
            let callback_result =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| cb(event)));
            if callback_result.is_err() {
                tracing::error!("orchestrator event subscriber panicked");
            }
        }
    }

    pub(super) fn set_job_activity(
        &self,
        shared: &Arc<Mutex<TaskShared>>,
        activity: Option<TaskActivity>,
    ) {
        let guard = shared.lock().expect("job poisoned");
        *guard
            .progress
            .job_activity
            .lock()
            .expect("job activity poisoned") = activity;
    }

    pub(super) fn set_download(&self, shared: &Arc<Mutex<TaskShared>>, lane: Option<DownloadLane>) {
        let guard = shared.lock().expect("job poisoned");
        *guard
            .progress
            .download
            .lock()
            .expect("download progress poisoned") = lane;
    }

    pub(super) fn set_upload(&self, shared: &Arc<Mutex<TaskShared>>, lane: Option<UploadLane>) {
        let guard = shared.lock().expect("job poisoned");
        *guard
            .progress
            .upload
            .lock()
            .expect("upload progress poisoned") = lane;
    }

    pub(super) fn set_codec(&self, shared: &Arc<Mutex<TaskShared>>, codec: Option<String>) {
        if codec.is_none() {
            return;
        }
        let guard = shared.lock().expect("job poisoned");
        *guard.progress.codec.lock().expect("codec poisoned") = codec;
    }

    pub(super) fn emit_progress(&self, shared: &Arc<Mutex<TaskShared>>) {
        let (job, progress) = {
            let guard = shared.lock().expect("job poisoned");
            let completed_tracks = guard.job.cached_count
                + guard.job.ripped_count
                + guard.job.failed_count
                + guard.job.skipped_count;
            let percent = if guard.job.total_tracks > 0 {
                ((completed_tracks as f64 / guard.job.total_tracks as f64) * 100.0).round() as u32
            } else {
                0
            };
            let progress = RipTaskProgress {
                job_id: guard.job.id.clone(),
                total_tracks: guard.job.total_tracks,
                completed_tracks,
                cached_count: guard.job.cached_count,
                ripped_count: guard.job.ripped_count,
                failed_count: guard.job.failed_count,
                skipped_count: guard.job.skipped_count,
                percent,
                job_activity: guard
                    .progress
                    .job_activity
                    .lock()
                    .expect("job activity poisoned")
                    .clone(),
                download: guard
                    .progress
                    .download
                    .lock()
                    .expect("download progress poisoned")
                    .clone(),
                upload: guard
                    .progress
                    .upload
                    .lock()
                    .expect("upload progress poisoned")
                    .clone(),
                codec: guard.progress.codec.lock().expect("codec poisoned").clone(),
            };
            (guard.job.clone(), progress)
        };
        self.emit(&OrchestratorEvent::Progress(&job, &progress));
    }
}

pub(super) struct DownloadProgressGuard {
    pub(super) bus: EventBus,
    pub(super) shared: Arc<Mutex<TaskShared>>,
}

impl Drop for DownloadProgressGuard {
    fn drop(&mut self) {
        self.bus.set_download(&self.shared, None);
        self.bus.emit_progress(&self.shared);
    }
}

impl RipOrchestrator {
    pub(super) fn set_phase(&self, shared: &Arc<Mutex<TaskShared>>, phase: TaskPhase) {
        shared.lock().expect("job poisoned").job.phase = phase;
    }
}

impl RipOrchestrator {
    pub(super) fn terminalize(
        &self,
        shared: &Arc<Mutex<TaskShared>>,
        state: TerminalTaskState,
        summary: Option<&RipTaskSummary>,
        error: Option<&str>,
    ) -> bool {
        let (job, cancelled_by, state) = {
            let mut guard = shared.lock().expect("job poisoned");
            if guard.job.terminal_state.is_some() {
                return false;
            }

            let state = if state != TerminalTaskState::Cancelled
                && (guard.job.is_cancelled || guard.job.controller.is_cancelled())
            {
                TerminalTaskState::Cancelled
            } else {
                state
            };
            guard.job.terminal_state = Some(state);
            guard.job.completed = true;
            (guard.job.clone(), guard.job.cancelled_by.clone(), state)
        };
        match state {
            TerminalTaskState::Completed => {
                if let Some(summary) = summary {
                    self.bus.emit(&OrchestratorEvent::Completed(&job, summary));
                }
            }
            TerminalTaskState::Cancelled => {
                self.bus
                    .emit(&OrchestratorEvent::Cancelled(&job, &cancelled_by));
            }
            TerminalTaskState::Failed => {
                if let Some(error) = error {
                    self.bus.emit(&OrchestratorEvent::Failed(&job, error));
                }
            }
        }
        true
    }
}
