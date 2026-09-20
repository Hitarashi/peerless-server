use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, atomic::AtomicI32},
    task::{Context, Poll},
};

use bytes::Bytes;
use ferogram::tl;
use futures_util::Stream;
use tokio::sync::{RwLock, mpsc};

use crate::{
    StreamError,
    cache::{CHUNK_SIZE, ChunkCache},
    worker_pool::StreamWorkerPool,
};

/// Parsed HTTP byte range [start, end] (inclusive).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ByteRange {
    pub start: u64,
    pub end: u64,
}

impl ByteRange {
    pub fn new(start: u64, end: u64) -> Self {
        Self { start, end }
    }

    /// Length in bytes.
    pub fn length(&self) -> u64 {
        self.end.saturating_sub(self.start) + 1
    }

    /// Parse HTTP `Range` header value (e.g. `bytes=0-1048575` or `bytes=1048576-`).
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

/// Backpressure-regulated async byte stream wrapping a bounded prefetch channel.
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

/// Parameters for spawning a stream pipe.
#[derive(Clone)]
pub struct StreamPipeParams {
    pub range: ByteRange,
    pub document_id: i64,
    pub location: Arc<RwLock<tl::enums::InputFileLocation>>,
    pub dc_id: Arc<AtomicI32>,
    pub refresh_location: Option<LocationRefresher>,
}

/// Spawns the prefetch pipeline and produces a `ChunkStream`.
pub fn create_stream_pipe(
    params: StreamPipeParams,
    worker_pool: Arc<StreamWorkerPool>,
    cache: Arc<ChunkCache>,
) -> ChunkStream {
    // 2-chunk prefetch window (~1 MB memory buffer)
    let (tx, rx) = mpsc::channel(2);

    let join_handle = tokio::spawn(async move {
        let chunk_size_u64 = CHUNK_SIZE as u64;
        let start_chunk = params.range.start / chunk_size_u64;
        let end_chunk = params.range.end / chunk_size_u64;

        for chunk_idx in start_chunk..=end_chunk {
            if tx.is_closed() {
                break;
            }

            // 1. Check in-memory chunk cache
            let chunk_data = if let Some(cached) = cache.get(params.document_id, chunk_idx).await {
                cached
            } else {
                if tx.is_closed() {
                    break;
                }
                // Fetch from MTProto worker pool with retries
                let offset = (chunk_idx * chunk_size_u64) as i64;
                let limit = CHUNK_SIZE as i32;

                let mut fetched = None;
                for attempt in 0..3 {
                    if tx.is_closed() {
                        break;
                    }
                    let current_loc = params.location.read().await.clone();
                    match worker_pool
                        .fetch_chunk(&current_loc, &params.dc_id, offset, limit)
                        .await
                    {
                        Ok(bytes) => {
                            cache
                                .insert(params.document_id, chunk_idx, bytes.clone())
                                .await;
                            fetched = Some(bytes);
                            break;
                        }
                        Err(StreamError::FileReferenceExpired)
                            if params.refresh_location.is_some() =>
                        {
                            let refresher = params.refresh_location.as_ref().unwrap();
                            match refresher().await {
                                Ok(new_loc) => {
                                    *params.location.write().await = new_loc.clone();
                                    match worker_pool
                                        .fetch_chunk(&new_loc, &params.dc_id, offset, limit)
                                        .await
                                    {
                                        Ok(bytes) => {
                                            cache
                                                .insert(
                                                    params.document_id,
                                                    chunk_idx,
                                                    bytes.clone(),
                                                )
                                                .await;
                                            fetched = Some(bytes);
                                            break;
                                        }
                                        Err(err) => {
                                            tracing::warn!(chunk_idx, attempt, %err, "Failed to fetch chunk after location refresh; retrying");
                                            tokio::time::sleep(std::time::Duration::from_millis(
                                                300 * (attempt + 1),
                                            ))
                                            .await;
                                        }
                                    }
                                }
                                Err(err) => {
                                    tracing::warn!(chunk_idx, attempt, %err, "Failed to refresh file location; retrying");
                                    tokio::time::sleep(std::time::Duration::from_millis(
                                        300 * (attempt + 1),
                                    ))
                                    .await;
                                }
                            }
                        }
                        Err(err) => {
                            tracing::warn!(chunk_idx, attempt, %err, "Transient error fetching chunk; retrying");
                            tokio::time::sleep(std::time::Duration::from_millis(
                                300 * (attempt + 1),
                            ))
                            .await;
                        }
                    }
                }

                match fetched {
                    Some(bytes) => bytes,
                    None => {
                        let io_err = std::io::Error::other(format!(
                            "Failed to fetch chunk {chunk_idx} after retries"
                        ));
                        let _ = tx.send(Err(io_err)).await;
                        break;
                    }
                }
            };

            // 2. Slice chunk according to range boundaries
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

                // 3. Send into bounded prefetch queue; halts if player paused or disconnected
                if tx.send(Ok(sliced)).await.is_err() {
                    // Receiver dropped (client closed connection or seeked away)
                    break;
                }
            }
        }
    });

    ChunkStream {
        receiver: rx,
        abort_handle: join_handle.abort_handle(),
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
