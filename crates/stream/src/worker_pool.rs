use std::{
    sync::{
        Arc,
        atomic::{AtomicI32, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use bytes::Bytes;
pub use db::hash_token as hash_bot_token;
use ferogram::{ErrorKind, InvocationErrorExt, tl};
use tokio::sync::{Mutex, Semaphore};

use crate::{
    StreamError,
    circuit_breaker::CircuitBreaker,
    metrics::{RetryReason, StreamMetrics},
};

const MAX_FETCH_ATTEMPTS: usize = 4;
pub(crate) const CHUNK_FETCH_DEADLINE: Duration = Duration::from_secs(25);
const RPC_TIMEOUT: Duration = Duration::from_secs(12);
const MAX_RETRY_BACKOFF: Duration = Duration::from_secs(2);

fn is_stale_file_reference_error(error: &ferogram::InvocationError) -> bool {
    matches!(
        error,
        ferogram::InvocationError::Rpc(rpc)
            if rpc.name == "FILE_REFERENCE_EXPIRED" || rpc.name == "FILE_REFERENCE_INVALID"
    )
}

struct InFlightGuard<'a>(&'a AtomicUsize);

impl<'a> InFlightGuard<'a> {
    fn new(counter: &'a AtomicUsize) -> Self {
        counter.fetch_add(1, Ordering::SeqCst);
        Self(counter)
    }
}

impl<'a> Drop for InFlightGuard<'a> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

/// A single worker instance in the pool.
pub struct WorkerInstance {
    pub id: usize,
    pub client: ferogram::Client,
    pub in_flight: AtomicUsize,
    pub username: Option<String>,
    pub dc_lock: Arc<Mutex<()>>,
}

/// Manages a pool of auxiliary Telegram bot clients for high-throughput media streaming.
pub struct StreamWorkerPool {
    workers: Vec<WorkerInstance>,
    circuit_breaker: CircuitBreaker,
    rr_cursor: AtomicUsize,
    primary_fallback: Option<WorkerInstance>,
    metrics: Arc<StreamMetrics>,
    rpc_slots: Semaphore,
}

impl StreamWorkerPool {
    /// Create an empty worker pool (e.g. for testing environments).
    pub fn empty() -> Arc<Self> {
        Arc::new(Self {
            workers: Vec::new(),
            circuit_breaker: CircuitBreaker::new(0),
            rr_cursor: AtomicUsize::new(0),
            primary_fallback: None,
            metrics: Arc::new(StreamMetrics::default()),
            rpc_slots: Semaphore::new(1),
        })
    }

    ///
    /// If `tokens` is empty or all blank, falls back to wrapping `primary_fallback` if provided.
    pub async fn new(
        primary_fallback: Option<ferogram::Client>,
        tokens: &[String],
        api_id: i32,
        api_hash: &str,
        session_store: Option<db::WorkerSessionStore>,
    ) -> Result<Arc<Self>, StreamError> {
        let valid_tokens: Vec<&str> = tokens
            .iter()
            .map(|t| t.trim())
            .filter(|t| !t.is_empty())
            .collect();

        let (workers, primary_worker) = if valid_tokens.is_empty() {
            if let Some(primary) = primary_fallback {
                tracing::warn!(
                    "No STREAM_WORKER_BOT_TOKENS configured. Falling back to primary bot client for streaming."
                );
                let me = primary.get_me().await.ok();
                (
                    vec![WorkerInstance {
                        id: 0,
                        client: primary,
                        in_flight: AtomicUsize::new(0),
                        username: me.and_then(|u| u.username),
                        dc_lock: Arc::new(Mutex::new(())),
                    }],
                    None,
                )
            } else {
                return Err(StreamError::AllWorkersUnavailable);
            }
        } else {
            let primary_worker = if let Some(ref primary) = primary_fallback {
                let me = primary.get_me().await.ok();
                Some(WorkerInstance {
                    id: usize::MAX,
                    client: primary.clone(),
                    in_flight: AtomicUsize::new(0),
                    username: me.and_then(|u| u.username),
                    dc_lock: Arc::new(Mutex::new(())),
                })
            } else {
                None
            };

            let mut workers = Vec::new();
            tracing::info!(
                count = valid_tokens.len(),
                "Initializing dedicated MTProto stream worker pool..."
            );
            for (idx, token) in valid_tokens.into_iter().enumerate() {
                let token_hash = hash_bot_token(token);
                let saved_session = if let Some(ref store) = session_store {
                    match store.get_session(&token_hash).await {
                        Ok(session) => session,
                        Err(error) => {
                            tracing::warn!(worker_id = idx, %error, "Failed to check session store for worker");
                            None
                        }
                    }
                } else {
                    None
                };

                let client = if let Some(ref session_data) = saved_session {
                    let connect_res = ferogram::Client::builder()
                        .api_id(api_id)
                        .api_hash(api_hash)
                        .catch_up(false)
                        .session_string(session_data)
                        .connect()
                        .await;

                    match connect_res {
                        Ok((connected, _shutdown)) => match connected.get_me().await {
                            Ok(_) => connected,
                            Err(err) => {
                                tracing::warn!(
                                    worker_id = idx,
                                    %err,
                                    "Saved worker session not authorized; falling back to fresh bot_sign_in"
                                );
                                create_fresh_client_and_sign_in(
                                    api_id,
                                    api_hash,
                                    token,
                                    &token_hash,
                                    session_store.as_ref(),
                                    idx,
                                )
                                .await?
                            }
                        },
                        Err(err) => {
                            tracing::warn!(
                                worker_id = idx,
                                %err,
                                "Saved worker session invalid or failed connect; falling back to bot_sign_in"
                            );
                            create_fresh_client_and_sign_in(
                                api_id,
                                api_hash,
                                token,
                                &token_hash,
                                session_store.as_ref(),
                                idx,
                            )
                            .await?
                        }
                    }
                } else {
                    create_fresh_client_and_sign_in(
                        api_id,
                        api_hash,
                        token,
                        &token_hash,
                        session_store.as_ref(),
                        idx,
                    )
                    .await?
                };

                let me = client.get_me().await.ok();
                let username = me.and_then(|u| u.username);
                tracing::info!(
                    worker_id = idx,
                    username = ?username,
                    "Auxiliary stream worker initialized"
                );

                workers.push(WorkerInstance {
                    id: idx,
                    client,
                    in_flight: AtomicUsize::new(0),
                    username,
                    dc_lock: Arc::new(Mutex::new(())),
                });
            }

            (workers, primary_worker)
        };

        let circuit_breaker = CircuitBreaker::new(workers.len());
        let primary_slot = if primary_worker.is_some() { 1 } else { 0 };
        let max_parallel_rpcs = (workers.len() + primary_slot).max(1);

        Ok(Arc::new(Self {
            workers,
            circuit_breaker,
            rr_cursor: AtomicUsize::new(0),
            primary_fallback: primary_worker,
            metrics: Arc::new(StreamMetrics::default()),
            rpc_slots: Semaphore::new(max_parallel_rpcs),
        }))
    }

    /// Total number of auxiliary workers configured in the pool.
    pub fn worker_count(&self) -> usize {
        self.workers.len()
    }

    /// Number of healthy workers currently available in the pool.
    pub fn available_worker_count(&self) -> usize {
        self.circuit_breaker.healthy_worker_count()
    }

    /// Access process-local streaming counters and latency summaries.
    pub fn metrics(&self) -> &Arc<StreamMetrics> {
        &self.metrics
    }

    /// Select the healthy worker with the fewest active in-flight chunk downloads,
    /// breaking ties using a round-robin cursor.
    pub fn pick_least_loaded(&self) -> Result<usize, StreamError> {
        let n = self.workers.len();
        if n == 0 {
            return Err(StreamError::AllWorkersUnavailable);
        }

        let start = self.rr_cursor.fetch_add(1, Ordering::Relaxed) % n;
        let mut best_worker = None;
        let mut min_in_flight = usize::MAX;

        for offset in 0..n {
            let idx = (start + offset) % n;
            let worker = &self.workers[idx];
            if self.circuit_breaker.is_available(worker.id) {
                let in_flight = worker.in_flight.load(Ordering::Relaxed);
                if in_flight < min_in_flight {
                    min_in_flight = in_flight;
                    best_worker = Some(worker.id);
                }
            }
        }

        best_worker.ok_or(StreamError::AllWorkersUnavailable)
    }

    /// Fetch a single MTProto file chunk with least-loaded dispatch and failover retries.
    pub async fn fetch_chunk(
        &self,
        location: &tl::enums::InputFileLocation,
        dc_id: &AtomicI32,
        offset: i64,
        limit: i32,
    ) -> Result<Bytes, StreamError> {
        self.fetch_chunk_until(
            location,
            dc_id,
            offset,
            limit,
            tokio::time::Instant::now() + CHUNK_FETCH_DEADLINE,
        )
        .await
    }

    pub(crate) async fn fetch_chunk_until(
        &self,
        location: &tl::enums::InputFileLocation,
        dc_id: &AtomicI32,
        offset: i64,
        limit: i32,
        deadline: tokio::time::Instant,
    ) -> Result<Bytes, StreamError> {
        let started = Instant::now();
        let result = self
            .fetch_chunk_with_budget(location, dc_id, offset, limit, deadline)
            .await;
        self.metrics
            .record_chunk_fetch(started.elapsed(), result.is_ok());
        result
    }

    async fn fetch_chunk_with_budget(
        &self,
        location: &tl::enums::InputFileLocation,
        dc_id: &AtomicI32,
        offset: i64,
        limit: i32,
        deadline: tokio::time::Instant,
    ) -> Result<Bytes, StreamError> {
        let worker_attempt_limit = if self.primary_fallback.is_some() && !self.workers.is_empty() {
            MAX_FETCH_ATTEMPTS - 1
        } else {
            MAX_FETCH_ATTEMPTS
        };
        let mut attempts = 0;
        let mut target_dc = dc_id.load(Ordering::Relaxed);
        let mut primary_attempted = false;
        let mut last_error = None;
        let mut last_flood_wait = None;

        while attempts < worker_attempt_limit && tokio::time::Instant::now() < deadline {
            let slot_wait_started = Instant::now();
            let mut slot_permit = Some(
                match tokio::time::timeout_at(deadline, self.rpc_slots.acquire()).await {
                    Ok(Ok(permit)) => permit,
                    Ok(Err(_)) => return Err(StreamError::AllWorkersUnavailable),
                    Err(_) => {
                        self.metrics
                            .record_rpc_slot_wait(slot_wait_started.elapsed());
                        last_error = Some("timed out waiting for an RPC slot".to_owned());
                        break;
                    }
                },
            );
            self.metrics
                .record_rpc_slot_wait(slot_wait_started.elapsed());

            let (worker, is_primary) = match self.pick_least_loaded() {
                Ok(worker_id) => (&self.workers[worker_id], false),
                Err(StreamError::AllWorkersUnavailable)
                    if (!primary_attempted || self.workers.is_empty())
                        && let Some(ref primary) = self.primary_fallback =>
                {
                    primary_attempted = true;
                    (primary, true)
                }
                Err(StreamError::AllWorkersUnavailable) if !self.workers.is_empty() => {
                    drop(slot_permit.take());
                    let wait = self
                        .workers
                        .iter()
                        .filter_map(|worker| self.circuit_breaker.quarantine_remaining(worker.id))
                        .min()
                        .unwrap_or(Duration::from_millis(500));
                    tracing::debug!(
                        wait_ms = wait.as_millis(),
                        "All stream workers are quarantined; waiting for the next worker"
                    );
                    if !sleep_before_retry(wait, deadline).await {
                        break;
                    }
                    continue;
                }
                Err(StreamError::AllWorkersUnavailable) => {
                    return Err(StreamError::AllWorkersUnavailable);
                }
                Err(err) => return Err(err),
            };
            let slot_permit = slot_permit.expect("RPC slot permit is held after worker selection");

            if is_primary {
                primary_attempted = true;
            }
            let guard = InFlightGuard::new(&worker.in_flight);
            let lock_wait_started = Instant::now();
            let dc_guard = match tokio::time::timeout_at(deadline, worker.dc_lock.lock()).await {
                Ok(guard) => guard,
                Err(_) => {
                    drop(guard);
                    drop(slot_permit);
                    self.metrics
                        .record_worker_lock_wait(lock_wait_started.elapsed());
                    last_error = Some("timed out waiting for the worker lock".to_owned());
                    break;
                }
            };
            self.metrics
                .record_worker_lock_wait(lock_wait_started.elapsed());
            let req = tl::functions::upload::GetFile {
                precise: true,
                cdn_supported: false,
                location: location.clone(),
                offset,
                limit,
            };

            let invoke_fut = worker.client.invoke_on_dc(target_dc, &req);
            attempts += 1;
            self.metrics.record_rpc_attempt();
            let request_deadline = (tokio::time::Instant::now() + RPC_TIMEOUT).min(deadline);
            let result = match tokio::time::timeout_at(request_deadline, invoke_fut).await {
                Ok(res) => res,
                Err(_) => {
                    drop(dc_guard);
                    drop(guard);
                    drop(slot_permit);
                    last_error = Some("upload.getFile request timed out".to_owned());
                    last_flood_wait = None;
                    tracing::debug!(
                        attempts,
                        max_attempts = MAX_FETCH_ATTEMPTS,
                        offset,
                        "Telegram upload.getFile timed out"
                    );
                    if attempts >= worker_attempt_limit
                        || !sleep_before_retry(retry_backoff(attempts), deadline).await
                    {
                        break;
                    }
                    self.metrics.record_retry(RetryReason::Timeout);
                    continue;
                }
            };
            drop(dc_guard);
            drop(guard);
            drop(slot_permit);

            match result {
                Ok(tl::enums::upload::File::File(f)) => {
                    self.metrics.record_telegram_bytes(f.bytes.len());
                    if !is_primary {
                        self.circuit_breaker.record_success(worker.id);
                    }
                    return Ok(Bytes::from(f.bytes));
                }
                Ok(tl::enums::upload::File::CdnRedirect(_)) => {
                    return Err(StreamError::UnsupportedCdnRedirect);
                }
                Err(err) if is_stale_file_reference_error(&err) => {
                    return Err(StreamError::FileReferenceExpired);
                }
                Err(err) => {
                    let kind = err.kind();
                    match kind {
                        ErrorKind::FloodWait(secs) => {
                            last_error = Some(err.to_string());
                            last_flood_wait = Some(secs);
                            tracing::debug!(
                                secs,
                                worker_id = worker.id,
                                "MTProto upload.getFile returned FloodWait"
                            );
                            if !is_primary {
                                self.circuit_breaker.quarantine(
                                    worker.id,
                                    Duration::from_secs(secs + 1),
                                    format!("FloodWait({secs}s)"),
                                );
                            }
                            if attempts >= worker_attempt_limit {
                                break;
                            }
                            if self.circuit_breaker.available_count() == 0 {
                                if !sleep_before_retry(
                                    Duration::from_secs(secs.saturating_add(1)),
                                    deadline,
                                )
                                .await
                                {
                                    break;
                                }
                                if !is_primary {
                                    self.circuit_breaker.record_success(worker.id);
                                }
                            }
                            self.metrics.record_retry(RetryReason::FloodWait);
                            continue;
                        }
                        ErrorKind::Rpc { ref name, .. } if name == "CONNECTION_NOT_INITED" => {
                            last_error = Some(err.to_string());
                            last_flood_wait = None;
                            tracing::debug!(
                                attempts,
                                max_attempts = MAX_FETCH_ATTEMPTS,
                                offset,
                                target_dc,
                                "Telegram CONNECTION_NOT_INITED during fetch_chunk"
                            );
                            if attempts >= worker_attempt_limit
                                || !sleep_before_retry(retry_backoff(attempts), deadline).await
                            {
                                break;
                            }
                            self.metrics.record_retry(RetryReason::ConnectionNotInited);
                            continue;
                        }
                        ErrorKind::Migration(new_dc) => {
                            last_error = Some(err.to_string());
                            last_flood_wait = None;
                            tracing::info!(
                                from_dc = target_dc,
                                to_dc = new_dc,
                                "Telegram DC migration redirect"
                            );
                            target_dc = new_dc;
                            dc_id.store(new_dc, Ordering::Relaxed);
                            if attempts >= worker_attempt_limit {
                                break;
                            }
                            self.metrics.record_retry(RetryReason::DcMigration);
                            continue;
                        }
                        ErrorKind::Network => {
                            last_error = Some(err.to_string());
                            last_flood_wait = None;
                            tracing::debug!(attempts, max_attempts = MAX_FETCH_ATTEMPTS, %err, "Transient network error during fetch_chunk");
                            if attempts >= worker_attempt_limit
                                || !sleep_before_retry(retry_backoff(attempts), deadline).await
                            {
                                break;
                            }
                            self.metrics.record_retry(RetryReason::Network);
                            continue;
                        }
                        ErrorKind::Transfer => return Err(StreamError::Telegram(err)),
                        _ => {
                            tracing::warn!(offset, limit, target_dc, %err, "Unhandled Telegram error during fetch_chunk");
                            return Err(StreamError::Telegram(err));
                        }
                    }
                }
            }
        }

        // Reserve the final attempt for the primary client when it was not already used.
        if let Some(ref primary) = self.primary_fallback
            && !primary_attempted
            && attempts < MAX_FETCH_ATTEMPTS
            && tokio::time::Instant::now() < deadline
        {
            let slot_wait_started = Instant::now();
            let slot_permit =
                match tokio::time::timeout_at(deadline, self.rpc_slots.acquire()).await {
                    Ok(Ok(permit)) => permit,
                    Ok(Err(_)) => return Err(StreamError::AllWorkersUnavailable),
                    Err(_) => {
                        self.metrics
                            .record_rpc_slot_wait(slot_wait_started.elapsed());
                        last_error = Some("timed out waiting for an RPC slot".to_owned());
                        return Err(StreamError::ChunkFetchFailed(retry_summary(
                            attempts, last_error,
                        )));
                    }
                };
            self.metrics
                .record_rpc_slot_wait(slot_wait_started.elapsed());
            let guard = InFlightGuard::new(&primary.in_flight);
            let lock_wait_started = Instant::now();
            let dc_guard = match tokio::time::timeout_at(deadline, primary.dc_lock.lock()).await {
                Ok(guard) => guard,
                Err(_) => {
                    drop(guard);
                    drop(slot_permit);
                    self.metrics
                        .record_worker_lock_wait(lock_wait_started.elapsed());
                    last_error = Some("timed out waiting for the primary worker lock".to_owned());
                    return Err(StreamError::ChunkFetchFailed(retry_summary(
                        attempts, last_error,
                    )));
                }
            };
            self.metrics
                .record_worker_lock_wait(lock_wait_started.elapsed());
            let req = tl::functions::upload::GetFile {
                precise: true,
                cdn_supported: false,
                location: location.clone(),
                offset,
                limit,
            };

            let invoke_fut = primary.client.invoke_on_dc(target_dc, &req);
            if attempts > 0 {
                self.metrics.record_retry(RetryReason::PrimaryFallback);
            }
            attempts += 1;
            self.metrics.record_rpc_attempt();
            let request_deadline = (tokio::time::Instant::now() + RPC_TIMEOUT).min(deadline);
            let result = match tokio::time::timeout_at(request_deadline, invoke_fut).await {
                Ok(res) => res,
                Err(_) => {
                    drop(dc_guard);
                    drop(guard);
                    last_error = Some("primary upload.getFile request timed out".to_owned());
                    return Err(StreamError::ChunkFetchFailed(retry_summary(
                        attempts, last_error,
                    )));
                }
            };
            drop(dc_guard);
            drop(guard);
            drop(slot_permit);

            match result {
                Ok(tl::enums::upload::File::File(f)) => {
                    self.metrics.record_telegram_bytes(f.bytes.len());
                    return Ok(Bytes::from(f.bytes));
                }
                Ok(tl::enums::upload::File::CdnRedirect(_)) => {
                    return Err(StreamError::UnsupportedCdnRedirect);
                }
                Err(err) if is_stale_file_reference_error(&err) => {
                    return Err(StreamError::FileReferenceExpired);
                }
                Err(err) => match err.kind() {
                    ErrorKind::FloodWait(secs) => return Err(StreamError::FloodWait(secs)),
                    ErrorKind::Transfer => return Err(StreamError::Telegram(err)),
                    ErrorKind::Network => {
                        last_flood_wait = None;
                        last_error = Some(err.to_string());
                    }
                    _ => return Err(StreamError::Telegram(err)),
                },
            }
        }

        if let Some(secs) = last_flood_wait {
            return Err(StreamError::FloodWait(secs));
        }
        Err(StreamError::ChunkFetchFailed(retry_summary(
            attempts, last_error,
        )))
    }
}

fn retry_summary(attempts: usize, last_error: Option<String>) -> String {
    match last_error {
        Some(error) => format!("{attempts} RPC attempts; last error: {error}"),
        None => format!("{attempts} RPC attempts; deadline or attempt limit reached"),
    }
}

fn retry_backoff(attempt: usize) -> Duration {
    let exponent = attempt.saturating_sub(1).min(4) as u32;
    let base_ms = 200_u64.saturating_mul(1_u64 << exponent);
    let capped_ms = base_ms.min(MAX_RETRY_BACKOFF.as_millis() as u64);
    let jitter_ms = rand::random::<u64>() % (capped_ms / 2 + 1);
    Duration::from_millis(capped_ms / 2 + jitter_ms)
}

async fn sleep_before_retry(delay: Duration, deadline: tokio::time::Instant) -> bool {
    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
    if delay >= remaining || remaining.is_zero() {
        return false;
    }
    tokio::time::sleep(delay).await;
    true
}

async fn create_fresh_client_and_sign_in(
    api_id: i32,
    api_hash: &str,
    token: &str,
    token_hash: &str,
    store: Option<&db::WorkerSessionStore>,
    idx: usize,
) -> Result<ferogram::Client, StreamError> {
    let (client, _shutdown) = ferogram::Client::builder()
        .api_id(api_id)
        .api_hash(api_hash)
        .catch_up(false)
        .session_string("")
        .connect()
        .await?;
    client.bot_sign_in(token).await?;
    if let Some(store) = store {
        match client.export_session_string().await {
            Ok(session_str) => {
                if let Err(e) = store.save_session(token_hash, &session_str).await {
                    tracing::warn!(worker_id = idx, error = %e, "Failed to persist worker session");
                } else {
                    tracing::info!(worker_id = idx, "Saved worker session to database");
                }
            }
            Err(e) => {
                tracing::warn!(worker_id = idx, error = %e, "Failed to export session string");
            }
        }
    }
    Ok(client)
}
