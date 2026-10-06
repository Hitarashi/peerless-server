use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

#[derive(Clone, Copy)]
pub(crate) enum RetryReason {
    Network,
    Timeout,
    FloodWait,
    ConnectionNotInited,
    DcMigration,
    PrimaryFallback,
}

#[derive(Default)]
pub struct StreamMetrics {
    streams_started: AtomicU64,
    streams_with_first_chunk: AtomicU64,
    first_chunk_latency_micros_total: AtomicU64,
    first_chunk_latency_micros_max: AtomicU64,
    chunk_fetches: AtomicU64,
    chunk_fetch_failures: AtomicU64,
    chunk_fetch_latency_micros_total: AtomicU64,
    chunk_fetch_latency_micros_max: AtomicU64,
    rpc_attempts: AtomicU64,
    retries: AtomicU64,
    network_retries: AtomicU64,
    timeout_retries: AtomicU64,
    flood_wait_retries: AtomicU64,
    connection_not_inited_retries: AtomicU64,
    dc_migration_retries: AtomicU64,
    primary_fallback_retries: AtomicU64,
    telegram_bytes_received: AtomicU64,
    bytes_enqueued: AtomicU64,
    file_reference_refreshes: AtomicU64,
    worker_lock_waits: AtomicU64,
    worker_lock_wait_micros_total: AtomicU64,
    worker_lock_wait_micros_max: AtomicU64,
    rpc_slot_waits: AtomicU64,
    rpc_slot_wait_micros_total: AtomicU64,
    rpc_slot_wait_micros_max: AtomicU64,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct StreamMetricsSnapshot {
    pub streams_started: u64,
    pub streams_with_first_chunk: u64,
    pub first_chunk_latency_avg_ms: Option<u64>,
    pub first_chunk_latency_max_ms: Option<u64>,
    pub chunk_fetches: u64,
    pub chunk_fetch_failures: u64,
    pub chunk_fetch_latency_avg_ms: Option<u64>,
    pub chunk_fetch_latency_max_ms: Option<u64>,
    pub rpc_attempts: u64,
    pub retries: u64,
    pub network_retries: u64,
    pub timeout_retries: u64,
    pub flood_wait_retries: u64,
    pub connection_not_inited_retries: u64,
    pub dc_migration_retries: u64,
    pub primary_fallback_retries: u64,
    pub telegram_bytes_received: u64,
    pub bytes_enqueued: u64,
    pub file_reference_refreshes: u64,
    pub worker_lock_waits: u64,
    pub worker_lock_wait_avg_ms: Option<u64>,
    pub worker_lock_wait_max_ms: Option<u64>,
    pub rpc_slot_waits: u64,
    pub rpc_slot_wait_avg_ms: Option<u64>,
    pub rpc_slot_wait_max_ms: Option<u64>,
}

impl StreamMetrics {
    pub(crate) fn record_stream_started(&self) {
        self.streams_started.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_first_chunk(&self, elapsed: Duration) {
        let micros = duration_micros(elapsed);
        self.streams_with_first_chunk
            .fetch_add(1, Ordering::Relaxed);
        self.first_chunk_latency_micros_total
            .fetch_add(micros, Ordering::Relaxed);
        self.first_chunk_latency_micros_max
            .fetch_max(micros, Ordering::Relaxed);
    }

    pub(crate) fn record_chunk_fetch(&self, elapsed: Duration, succeeded: bool) {
        let micros = duration_micros(elapsed);
        self.chunk_fetches.fetch_add(1, Ordering::Relaxed);
        if !succeeded {
            self.chunk_fetch_failures.fetch_add(1, Ordering::Relaxed);
        }
        self.chunk_fetch_latency_micros_total
            .fetch_add(micros, Ordering::Relaxed);
        self.chunk_fetch_latency_micros_max
            .fetch_max(micros, Ordering::Relaxed);
    }

    pub(crate) fn record_rpc_attempt(&self) {
        self.rpc_attempts.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_retry(&self, reason: RetryReason) {
        self.retries.fetch_add(1, Ordering::Relaxed);
        let counter = match reason {
            RetryReason::Network => &self.network_retries,
            RetryReason::Timeout => &self.timeout_retries,
            RetryReason::FloodWait => &self.flood_wait_retries,
            RetryReason::ConnectionNotInited => &self.connection_not_inited_retries,
            RetryReason::DcMigration => &self.dc_migration_retries,
            RetryReason::PrimaryFallback => &self.primary_fallback_retries,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_telegram_bytes(&self, bytes: usize) {
        self.telegram_bytes_received
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }

    pub(crate) fn record_bytes_enqueued(&self, bytes: usize) {
        self.bytes_enqueued
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }

    pub(crate) fn record_file_reference_refresh(&self) {
        self.file_reference_refreshes
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_worker_lock_wait(&self, elapsed: Duration) {
        let micros = duration_micros(elapsed);
        self.worker_lock_waits.fetch_add(1, Ordering::Relaxed);
        self.worker_lock_wait_micros_total
            .fetch_add(micros, Ordering::Relaxed);
        self.worker_lock_wait_micros_max
            .fetch_max(micros, Ordering::Relaxed);
    }

    pub(crate) fn record_rpc_slot_wait(&self, elapsed: Duration) {
        let micros = duration_micros(elapsed);
        self.rpc_slot_waits.fetch_add(1, Ordering::Relaxed);
        self.rpc_slot_wait_micros_total
            .fetch_add(micros, Ordering::Relaxed);
        self.rpc_slot_wait_micros_max
            .fetch_max(micros, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> StreamMetricsSnapshot {
        let first_chunks = self.streams_with_first_chunk.load(Ordering::Relaxed);
        let fetches = self.chunk_fetches.load(Ordering::Relaxed);
        let lock_waits = self.worker_lock_waits.load(Ordering::Relaxed);
        let slot_waits = self.rpc_slot_waits.load(Ordering::Relaxed);

        StreamMetricsSnapshot {
            streams_started: self.streams_started.load(Ordering::Relaxed),
            streams_with_first_chunk: first_chunks,
            first_chunk_latency_avg_ms: average_millis(
                self.first_chunk_latency_micros_total
                    .load(Ordering::Relaxed),
                first_chunks,
            ),
            first_chunk_latency_max_ms: nonzero_millis(
                self.first_chunk_latency_micros_max.load(Ordering::Relaxed),
                first_chunks,
            ),
            chunk_fetches: fetches,
            chunk_fetch_failures: self.chunk_fetch_failures.load(Ordering::Relaxed),
            chunk_fetch_latency_avg_ms: average_millis(
                self.chunk_fetch_latency_micros_total
                    .load(Ordering::Relaxed),
                fetches,
            ),
            chunk_fetch_latency_max_ms: nonzero_millis(
                self.chunk_fetch_latency_micros_max.load(Ordering::Relaxed),
                fetches,
            ),
            rpc_attempts: self.rpc_attempts.load(Ordering::Relaxed),
            retries: self.retries.load(Ordering::Relaxed),
            network_retries: self.network_retries.load(Ordering::Relaxed),
            timeout_retries: self.timeout_retries.load(Ordering::Relaxed),
            flood_wait_retries: self.flood_wait_retries.load(Ordering::Relaxed),
            connection_not_inited_retries: self
                .connection_not_inited_retries
                .load(Ordering::Relaxed),
            dc_migration_retries: self.dc_migration_retries.load(Ordering::Relaxed),
            primary_fallback_retries: self.primary_fallback_retries.load(Ordering::Relaxed),
            telegram_bytes_received: self.telegram_bytes_received.load(Ordering::Relaxed),
            bytes_enqueued: self.bytes_enqueued.load(Ordering::Relaxed),
            file_reference_refreshes: self.file_reference_refreshes.load(Ordering::Relaxed),
            worker_lock_waits: lock_waits,
            worker_lock_wait_avg_ms: average_millis(
                self.worker_lock_wait_micros_total.load(Ordering::Relaxed),
                lock_waits,
            ),
            worker_lock_wait_max_ms: nonzero_millis(
                self.worker_lock_wait_micros_max.load(Ordering::Relaxed),
                lock_waits,
            ),
            rpc_slot_waits: slot_waits,
            rpc_slot_wait_avg_ms: average_millis(
                self.rpc_slot_wait_micros_total.load(Ordering::Relaxed),
                slot_waits,
            ),
            rpc_slot_wait_max_ms: nonzero_millis(
                self.rpc_slot_wait_micros_max.load(Ordering::Relaxed),
                slot_waits,
            ),
        }
    }
}

fn duration_micros(duration: Duration) -> u64 {
    duration.as_micros().min(u128::from(u64::MAX)) as u64
}

fn average_millis(total_micros: u64, count: u64) -> Option<u64> {
    (count > 0).then(|| total_micros / count / 1_000)
}

fn nonzero_millis(micros: u64, count: u64) -> Option<u64> {
    (count > 0).then_some(micros / 1_000)
}
