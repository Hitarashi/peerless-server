use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, atomic::AtomicI32},
    task::{Context, Poll},
};

use bytes::Bytes;
use ferogram::tl;
use futures_util::{Stream, StreamExt, stream::FuturesOrdered};
use tokio::sync::{RwLock, mpsc};

use crate::{
    StreamError,
    cache::{CHUNK_SIZE, ChunkCache},
    metrics::StreamMetrics,
    worker_pool::{CHUNK_FETCH_DEADLINE, StreamWorkerPool},
};

const STREAM_PREFETCH_CHUNKS: usize = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ByteRange {
    pub start: u64,
    pub end: u64,
}

impl ByteRange {
    pub fn new(start: u64, end: u64) -> Self {
        Self { start, end }
    }

    pub fn length(&self) -> u64 {
        self.end.saturating_sub(self.start) + 1
    }

    pub fn parse(header: &str, file_size: u64) -> Result<Self, StreamError> {
        let s = header.trim();
        let s = s.strip_prefix("bytes=").unwrap_or(s);
        let parts: Vec<&str> = s.split('-').collect();
        if parts.len() != 2 {
            return Err(StreamError::InvalidRange(format!(
                "Malformed range: {header}"
            )));
        }

        let parse_num = |val: &str| -> Result<u64, StreamError> {
            val.parse()
                .map_err(|_| StreamError::InvalidRange(header.to_string()))
        };

        let (start, end) = match (parts[0].trim(), parts[1].trim()) {
            ("", end_str) => {
                let suffix = parse_num(end_str)?;
                (
                    file_size.saturating_sub(suffix),
                    file_size.saturating_sub(1),
                )
            }
            (start_str, "") => (parse_num(start_str)?, file_size.saturating_sub(1)),
            (start_str, end_str) => (parse_num(start_str)?, parse_num(end_str)?),
        };

        if start > end || start >= file_size {
            return Err(StreamError::InvalidRange(format!(
                "Range out of bounds: start={start}, end={end}, file_size={file_size}"
            )));
        }

        let clamped_end = end.min(file_size.saturating_sub(1));
        Ok(Self {
            start,
            end: clamped_end,
        })
    }
}

pub struct ChunkStream {
    receiver: mpsc::Receiver<Result<Bytes, std::io::Error>>,
    abort_handle: tokio::task::AbortHandle,
}

impl Drop for ChunkStream {
    fn drop(&mut self) {
        self.abort_handle.abort();
    }
}

impl Stream for ChunkStream {
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.receiver.poll_recv(cx)
    }
}

pub type LocationRefresher = Arc<
    dyn Fn() -> Pin<
            Box<dyn Future<Output = Result<tl::enums::InputFileLocation, StreamError>> + Send>,
        > + Send
        + Sync,
>;

#[derive(Clone)]
pub struct StreamPipeParams {
    pub range: ByteRange,
    pub document_id: i64,
    pub location: Arc<RwLock<tl::enums::InputFileLocation>>,
    pub worker_id: usize,
    pub dc_id: Arc<AtomicI32>,
    pub refresh_location: Option<LocationRefresher>,
}

pub fn create_stream_pipe(
    params: StreamPipeParams,
    worker_pool: Arc<StreamWorkerPool>,
    cache: Arc<ChunkCache>,
) -> ChunkStream {
    let (tx, rx) = mpsc::channel(2);
    let metrics = Arc::clone(worker_pool.metrics());
    metrics.record_stream_started();

    let join_handle = tokio::spawn(async move {
        let chunk_size_u64 = CHUNK_SIZE as u64;
        let start_chunk = params.range.start / chunk_size_u64;
        let end_chunk = params.range.end / chunk_size_u64;
        let stream_started = std::time::Instant::now();
        let mut first_chunk = true;
        let mut next_chunk = start_chunk;
        let mut pending = FuturesOrdered::new();

        while next_chunk <= end_chunk || !pending.is_empty() {
            while next_chunk <= end_chunk && pending.len() < STREAM_PREFETCH_CHUNKS {
                let chunk_idx = next_chunk;
                let params = params.clone();
                let worker_pool = Arc::clone(&worker_pool);
                let cache = Arc::clone(&cache);
                let metrics = Arc::clone(&metrics);
                pending.push_back(async move {
                    let result = cache
                        .get_or_fetch(
                            params.document_id,
                            chunk_idx,
                            fetch_chunk_with_refresh(
                                chunk_idx,
                                params.clone(),
                                worker_pool,
                                metrics,
                            ),
                        )
                        .await;
                    (chunk_idx, result)
                });
                next_chunk += 1;
            }

            let Some((chunk_idx, chunk_result)) = pending.next().await else {
                break;
            };
            if tx.is_closed() {
                break;
            }

            let chunk_data = match chunk_result {
                Ok(bytes) => bytes,
                Err(err) => {
                    tracing::warn!(
                        worker_id = params.worker_id,
                        document_id = params.document_id,
                        chunk_idx,
                        %err,
                        "Stream chunk fetch failed"
                    );
                    let io_err = std::io::Error::other(format!(
                        "Failed to fetch stream chunk {chunk_idx}: {err}"
                    ));
                    let _ = tx.send(Err(io_err)).await;
                    break;
                }
            };

            let chunk_offset_start = chunk_idx * chunk_size_u64;
            let slice_start = if chunk_idx == start_chunk {
                (params.range.start.saturating_sub(chunk_offset_start)) as usize
            } else {
                0
            };

            let slice_end = if chunk_idx == end_chunk {
                let end_offset_in_chunk = params.range.end.saturating_sub(chunk_offset_start) + 1;
                end_offset_in_chunk as usize
            } else {
                chunk_data.len()
            };

            if slice_start < chunk_data.len() {
                let actual_end = slice_end.min(chunk_data.len());
                let sliced = chunk_data.slice(slice_start..actual_end);
                let bytes_enqueued = sliced.len();

                if tx.send(Ok(sliced)).await.is_err() {
                    break;
                }
                metrics.record_bytes_enqueued(bytes_enqueued);
                if first_chunk {
                    metrics.record_first_chunk(stream_started.elapsed());
                    first_chunk = false;
                }
            }
        }
    });

    ChunkStream {
        receiver: rx,
        abort_handle: join_handle.abort_handle(),
    }
}

async fn fetch_chunk_with_refresh(
    chunk_idx: u64,
    params: StreamPipeParams,
    worker_pool: Arc<StreamWorkerPool>,
    metrics: Arc<StreamMetrics>,
) -> Result<Bytes, StreamError> {
    let chunk_size_u64 = CHUNK_SIZE as u64;
    let offset = (chunk_idx * chunk_size_u64) as i64;
    let limit = CHUNK_SIZE as i32;
    let deadline = tokio::time::Instant::now() + CHUNK_FETCH_DEADLINE;
    let current_location = params.location.read().await.clone();

    match worker_pool
        .fetch_chunk_until_on_worker(
            params.worker_id,
            &current_location,
            &params.dc_id,
            offset,
            limit,
            deadline,
        )
        .await
    {
        Err(StreamError::FileReferenceExpired) => {
            let Some(refresher) = params.refresh_location else {
                return Err(StreamError::FileReferenceExpired);
            };

            tracing::debug!(
                worker_id = params.worker_id,
                document_id = params.document_id,
                chunk_idx,
                "Refreshing Telegram file reference with the selected stream worker"
            );
            metrics.record_file_reference_refresh();
            let new_location = match tokio::time::timeout_at(deadline, refresher()).await {
                Ok(result) => result?,
                Err(_) => {
                    return Err(StreamError::ChunkFetchFailed(
                        "timed out refreshing the Telegram file reference".to_owned(),
                    ));
                }
            };
            *params.location.write().await = new_location.clone();
            worker_pool
                .fetch_chunk_until_on_worker(
                    params.worker_id,
                    &new_location,
                    &params.dc_id,
                    offset,
                    limit,
                    deadline,
                )
                .await
        }
        result => result,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_valid_ranges() {
        let size = 10_000_000;
        assert_eq!(
            ByteRange::parse("bytes=0-499", size).unwrap(),
            ByteRange::new(0, 499)
        );
        assert_eq!(
            ByteRange::parse("bytes=500-", size).unwrap(),
            ByteRange::new(500, size - 1)
        );
        assert_eq!(
            ByteRange::parse("bytes=-1000", size).unwrap(),
            ByteRange::new(size - 1000, size - 1)
        );
    }

    #[test]
    fn parse_invalid_ranges() {
        let size = 1000;
        assert!(ByteRange::parse("bytes=2000-", size).is_err());
        assert!(ByteRange::parse("bytes=500-200", size).is_err());
        assert!(ByteRange::parse("garbage", size).is_err());
    }
}
