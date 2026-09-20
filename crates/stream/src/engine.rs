use std::sync::Arc;

use ferogram::{PeerRef, tl};
use moka::future::Cache;
use music::Codec;

use crate::{
    StreamError,
    cache::ChunkCache,
    pipe::{ByteRange, ChunkStream, create_stream_pipe},
    worker_pool::StreamWorkerPool,
};

/// Track media document location and attributes cached in memory.
#[derive(Debug, Clone)]
pub struct TrackMediaMetadata {
    pub document_id: i64,
    pub access_hash: i64,
    pub file_reference: Vec<u8>,
    pub dc_id: i32,
    pub file_size: u64,
    pub mime_type: String,
    pub codec: Codec,
}

impl TrackMediaMetadata {
    /// Build `InputFileLocation` required for `upload.getFile`.
    pub fn input_location(&self) -> tl::enums::InputFileLocation {
        tl::enums::InputFileLocation::InputDocumentFileLocation(
            tl::types::InputDocumentFileLocation {
                id: self.document_id,
                access_hash: self.access_hash,
                file_reference: self.file_reference.clone(),
                thumb_size: String::new(),
            },
        )
    }
}

/// HTTP streaming response payload.
pub struct AudioStreamResponse {
    pub status: u16,
    pub content_type: String,
    pub content_length: u64,
    pub content_range: Option<String>,
    pub accept_ranges: &'static str,
    pub stream: ChunkStream,
}

/// Deep streaming module coordinating MTProto workers, chunk cache, and HTTP range translation.
#[derive(Clone)]
pub struct StreamEngine {
    worker_pool: Arc<StreamWorkerPool>,
    cache: Arc<ChunkCache>,
    metadata_cache: Cache<i32, Arc<TrackMediaMetadata>>,
    tracks_repo: db::TracksRepository,
    primary_client: Option<ferogram::Client>,
    dump_peer: PeerRef,
}

impl StreamEngine {
    pub fn new(
        worker_pool: Arc<StreamWorkerPool>,
        cache: Arc<ChunkCache>,
        tracks_repo: db::TracksRepository,
        primary_client: Option<ferogram::Client>,
        dump_peer: PeerRef,
    ) -> Self {
        let metadata_cache = Cache::builder()
            .max_capacity(1000)
            .time_to_live(std::time::Duration::from_secs(3600 * 24))
            .build();

        Self {
            worker_pool,
            cache,
            metadata_cache,
            tracks_repo,
            primary_client,
            dump_peer,
        }
    }

    /// Access the underlying MTProto worker pool.
    pub fn worker_pool(&self) -> &Arc<StreamWorkerPool> {
        &self.worker_pool
    }

    /// Access the chunk cache.
    pub fn cache(&self) -> &Arc<ChunkCache> {
        &self.cache
    }

    /// Resolve track document metadata from Dump Channel, with caching and on-demand refresh.
    pub async fn resolve_track_media(
        &self,
        track_id: i32,
        force_refresh: bool,
    ) -> Result<Arc<TrackMediaMetadata>, StreamError> {
        if !force_refresh && let Some(cached) = self.metadata_cache.get(&track_id).await {
            return Ok(cached);
        }

        let track = self
            .tracks_repo
            .find_track_by_id(track_id)
            .await?
            .ok_or(StreamError::TrackNotFound(track_id))?;

        let client = self
            .primary_client
            .as_ref()
            .ok_or(StreamError::AllWorkersUnavailable)?;

        let messages = client
            .get_messages(self.dump_peer.clone(), &[track.message_id])
            .await
            .map_err(StreamError::Telegram)?;

        let message = messages
            .into_iter()
            .next()
            .ok_or(StreamError::NoMediaDocument(track_id))?;

        let document = message
            .document()
            .ok_or(StreamError::NoMediaDocument(track_id))?;

        let mime_type = if document.mime_type().is_empty() {
            track.codec.mime_type().to_string()
        } else {
            document.mime_type().to_string()
        };

        let metadata = Arc::new(TrackMediaMetadata {
            document_id: document.id(),
            access_hash: document.access_hash(),
            file_reference: document.raw.file_reference.clone(),
            dc_id: document.raw.dc_id,
            file_size: document.size() as u64,
            mime_type,
            codec: track.codec,
        });

        self.metadata_cache
            .insert(track_id, Arc::clone(&metadata))
            .await;
        Ok(metadata)
    }

    /// Open an audio byte stream for the given track and HTTP Range header.
    pub async fn open_stream(
        &self,
        track_id: i32,
        range_header: Option<&str>,
    ) -> Result<AudioStreamResponse, StreamError> {
        let meta = self.resolve_track_media(track_id, false).await?;

        let (range, status, content_range) = if let Some(header) = range_header {
            let parsed_range = ByteRange::parse(header, meta.file_size)?;
            let range_str = format!(
                "bytes {}-{}/{}",
                parsed_range.start, parsed_range.end, meta.file_size
            );
            (parsed_range, 206, Some(range_str))
        } else {
            (
                ByteRange::new(0, meta.file_size.saturating_sub(1)),
                200,
                None,
            )
        };

        let engine = self.clone();
        let refresher: crate::pipe::LocationRefresher = Arc::new(move || {
            let engine = engine.clone();
            Box::pin(async move {
                let fresh_meta = engine.resolve_track_media(track_id, true).await?;
                Ok(fresh_meta.input_location())
            })
        });

        let params = crate::pipe::StreamPipeParams {
            range,
            document_id: meta.document_id,
            location: Arc::new(tokio::sync::RwLock::new(meta.input_location())),
            dc_id: Arc::new(std::sync::atomic::AtomicI32::new(meta.dc_id)),
            refresh_location: Some(refresher),
        };
        let stream = create_stream_pipe(
            params,
            Arc::clone(&self.worker_pool),
            Arc::clone(&self.cache),
        );

        Ok(AudioStreamResponse {
            status,
            content_type: meta.mime_type.clone(),
            content_length: range.length(),
            content_range,
            accept_ranges: "bytes",
            stream,
        })
    }
}
