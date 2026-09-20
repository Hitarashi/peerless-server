use std::{
    sync::{
        Arc,
        atomic::{AtomicI32, AtomicUsize, Ordering},
    },
    time::Duration,
};

use bytes::Bytes;
pub use db::hash_token as hash_bot_token;
use ferogram::{ErrorKind, InvocationErrorExt, tl};
use tokio::sync::Mutex;

use crate::{StreamError, circuit_breaker::CircuitBreaker};

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
}

impl StreamWorkerPool {
    /// Create an empty worker pool (e.g. for testing environments).
    pub fn empty() -> Arc<Self> {
        Arc::new(Self {
            workers: Vec::new(),
            circuit_breaker: CircuitBreaker::new(0),
            rr_cursor: AtomicUsize::new(0),
            primary_fallback: None,
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

        Ok(Arc::new(Self {
            workers,
            circuit_breaker,
            rr_cursor: AtomicUsize::new(0),
            primary_fallback: primary_worker,
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
        let mut attempts = 0;
        let mut target_dc = dc_id.load(Ordering::Relaxed);
        let max_attempts = self.workers.len().max(3);

        while attempts < max_attempts {
            let (worker, is_primary) = match self.pick_least_loaded() {
                Ok(worker_id) => (&self.workers[worker_id], false),
                Err(StreamError::AllWorkersUnavailable) => {
                    if let Some(ref primary) = self.primary_fallback {
                        (primary, true)
                    } else if !self.workers.is_empty() {
                        // All pool workers temporarily quarantined; wait briefly and fall back to first worker
                        tracing::warn!(
                            "All workers quarantined in circuit breaker; waiting 500ms before retry"
                        );
                        tokio::time::sleep(Duration::from_millis(500)).await;
                        (&self.workers[0], false)
                    } else {
                        return Err(StreamError::AllWorkersUnavailable);
                    }
                }
                Err(err) => return Err(err),
            };

            let guard = InFlightGuard::new(&worker.in_flight);
            let dc_guard = worker.dc_lock.lock().await;
            let req = tl::functions::upload::GetFile {
                precise: true,
                cdn_supported: false,
                location: location.clone(),
                offset,
                limit,
            };

            let invoke_fut = worker.client.invoke_on_dc(target_dc, &req);
            let result = match tokio::time::timeout(Duration::from_secs(15), invoke_fut).await {
                Ok(res) => res,
                Err(_) => {
                    drop(dc_guard);
                    drop(guard);
                    attempts += 1;
                    tracing::warn!(
                        attempts,
                        max_attempts,
                        offset,
                        "Telegram invoke_on_dc timed out after 15s; retrying"
                    );
                    tokio::time::sleep(Duration::from_millis(200 * attempts as u64)).await;
                    if attempts >= max_attempts {
                        break;
                    }
                    continue;
                }
            };
            drop(dc_guard);
            drop(guard);

            match result {
                Ok(tl::enums::upload::File::File(f)) => {
                    if !is_primary {
                        self.circuit_breaker.record_success(worker.id);
                    }
                    return Ok(Bytes::from(f.bytes));
                }
                Ok(tl::enums::upload::File::CdnRedirect(_)) => {
                    return Err(StreamError::UnsupportedCdnRedirect);
                }
                Err(err) => match err.kind() {
                    ErrorKind::FloodWait(secs) => {
                        attempts += 1;
                        tracing::warn!(
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
                        if self.workers.len() <= 1 || self.circuit_breaker.available_count() == 0 {
                            tracing::info!(
                                secs,
                                "Single worker or all workers quarantined; sleeping through FloodWait"
                            );
                            tokio::time::sleep(Duration::from_secs(secs + 1)).await;
                            if !is_primary {
                                self.circuit_breaker.record_success(worker.id);
                            }
                            continue;
                        }
                        if attempts >= max_attempts {
                            break;
                        }
                        continue;
                    }
                    ErrorKind::Rpc { ref name, .. } if name == "FILE_REFERENCE_EXPIRED" => {
                        return Err(StreamError::FileReferenceExpired);
                    }
                    ErrorKind::Rpc { ref name, .. } if name == "CONNECTION_NOT_INITED" => {
                        attempts += 1;
                        tracing::warn!(
                            attempts,
                            max_attempts,
                            offset,
                            target_dc,
                            "Telegram CONNECTION_NOT_INITED during fetch_chunk; waiting 500ms before retry"
                        );
                        tokio::time::sleep(Duration::from_millis(500)).await;
                        if attempts >= max_attempts {
                            break;
                        }
                        continue;
                    }
                    ErrorKind::Migration(new_dc) => {
                        tracing::info!(
                            from_dc = target_dc,
                            to_dc = new_dc,
                            "Telegram DC migration redirect"
                        );
                        target_dc = new_dc;
                        dc_id.store(new_dc, Ordering::Relaxed);
                        continue;
                    }
                    ErrorKind::Network | ErrorKind::Transfer => {
                        attempts += 1;
                        tracing::warn!(attempts, max_attempts, %err, "Transient network/transfer error during fetch_chunk");
                        tokio::time::sleep(Duration::from_millis(200 * attempts as u64)).await;
                        if attempts >= max_attempts {
                            break;
                        }
                        continue;
                    }
                    _ => {
                        tracing::warn!(offset, limit, target_dc, %err, "Unhandled Telegram error during fetch_chunk");
                        return Err(StreamError::Telegram(err));
                    }
                },
            }
        }

        // If retry loop finishes without success and primary_fallback is present, attempt a final fallback with primary
        if let Some(ref primary) = self.primary_fallback {
            let guard = InFlightGuard::new(&primary.in_flight);
            let dc_guard = primary.dc_lock.lock().await;
            let req = tl::functions::upload::GetFile {
                precise: true,
                cdn_supported: false,
                location: location.clone(),
                offset,
                limit,
            };

            let invoke_fut = primary.client.invoke_on_dc(target_dc, &req);
            let result = match tokio::time::timeout(Duration::from_secs(15), invoke_fut).await {
                Ok(res) => res,
                Err(_) => {
                    drop(dc_guard);
                    drop(guard);
                    return Err(StreamError::AllWorkersUnavailable);
                }
            };
            drop(dc_guard);
            drop(guard);

            match result {
                Ok(tl::enums::upload::File::File(f)) => return Ok(Bytes::from(f.bytes)),
                Ok(tl::enums::upload::File::CdnRedirect(_)) => {
                    return Err(StreamError::UnsupportedCdnRedirect);
                }
                Err(err) => match err.kind() {
                    ErrorKind::Rpc { ref name, .. } if name == "FILE_REFERENCE_EXPIRED" => {
                        return Err(StreamError::FileReferenceExpired);
                    }
                    ErrorKind::FloodWait(secs) => return Err(StreamError::FloodWait(secs)),
                    _ => return Err(StreamError::Telegram(err)),
                },
            }
        }

        Err(StreamError::AllWorkersUnavailable)
    }
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
