use std::sync::Arc;

use ferogram::{PeerRef, tl};
use moka::future::Cache;
use music::Codec;
use tokio::sync::Mutex;

use crate::{
    StreamError,
    cache::ChunkCache,
    pipe::{ByteRange, ChunkStream, create_stream_pipe},
    worker_pool::StreamWorkerPool,
};

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

pub struct AudioStreamResponse {
    pub status: u16,
    pub content_type: String,
    pub content_length: u64,
    pub content_range: Option<String>,
    pub accept_ranges: &'static str,
    pub stream: ChunkStream,
}

#[derive(Debug, Clone)]
pub struct AudioStreamHeaders {
    pub status: u16,
    pub content_type: String,
    pub content_length: u64,
    pub content_range: Option<String>,
    pub accept_ranges: &'static str,
}

#[derive(Clone)]
pub struct StreamEngine {
    worker_pool: Arc<StreamWorkerPool>,
    cache: Arc<ChunkCache>,
    metadata_cache: Cache<(i32, usize), Arc<TrackMediaMetadata>>,
    metadata_refresh_locks: Cache<(i32, usize), Arc<Mutex<()>>>,
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
        let metadata_refresh_locks = Cache::builder()
            .max_capacity(1000)
            .time_to_live(std::time::Duration::from_secs(3600 * 24))
            .build();

        Self {
            worker_pool,
            cache,
            metadata_cache,
            metadata_refresh_locks,
            tracks_repo,
            primary_client,
            dump_peer,
        }
    }

    pub fn worker_pool(&self) -> &Arc<StreamWorkerPool> {
        &self.worker_pool
    }

    pub fn cache(&self) -> &Arc<ChunkCache> {
        &self.cache
    }

    pub async fn resolve_track_media(
        &self,
        db_track_id: i32,
        force_refresh: bool,
    ) -> Result<Arc<TrackMediaMetadata>, StreamError> {
        self.resolve_track_media_for_worker(db_track_id, usize::MAX, force_refresh)
            .await
    }

    async fn resolve_track_media_for_worker(
        &self,
        db_track_id: i32,
        worker_id: usize,
        force_refresh: bool,
    ) -> Result<Arc<TrackMediaMetadata>, StreamError> {
        let cache_key = (db_track_id, worker_id);
        if !force_refresh && let Some(cached) = self.metadata_cache.get(&cache_key).await {
            return Ok(cached);
        }

        self.load_track_media(db_track_id, worker_id).await
    }

    async fn load_track_media(
        &self,
        db_track_id: i32,
        worker_id: usize,
    ) -> Result<Arc<TrackMediaMetadata>, StreamError> {
        let track = self
            .tracks_repo
            .find_track_by_id(db_track_id)
            .await?
            .ok_or(StreamError::TrackNotFound(db_track_id))?;

        let messages = if worker_id == usize::MAX {
            let client = self
                .primary_client
                .as_ref()
                .ok_or(StreamError::AllWorkersUnavailable)?;
            client
                .get_messages(self.dump_peer.clone(), &[track.message_id])
                .await
                .map_err(StreamError::Telegram)?
        } else {
            self.worker_pool
                .get_messages_for_worker(worker_id, self.dump_peer.clone(), &[track.message_id])
                .await?
        };

        let message = messages
            .into_iter()
            .next()
            .ok_or(StreamError::NoMediaDocument(db_track_id))?;

        let document = message
            .document()
            .ok_or(StreamError::NoMediaDocument(db_track_id))?;

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
            .insert((db_track_id, worker_id), Arc::clone(&metadata))
            .await;
        Ok(metadata)
    }

    async fn refresh_track_media_if_stale(
        &self,
        db_track_id: i32,
        worker_id: usize,
        stale_file_reference: &[u8],
    ) -> Result<Arc<TrackMediaMetadata>, StreamError> {
        let cache_key = (db_track_id, worker_id);
        let refresh_lock = self
            .metadata_refresh_locks
            .get_with(cache_key, async { Arc::new(Mutex::new(())) })
            .await;
        let _guard = refresh_lock.lock().await;

        if let Some(cached) = self.metadata_cache.get(&cache_key).await
            && cached.file_reference.as_slice() != stale_file_reference
        {
            return Ok(cached);
        }

        self.load_track_media(db_track_id, worker_id).await
    }

    async fn prepare_stream(
        &self,
        db_track_id: i32,
        range_header: Option<&str>,
        worker_id: usize,
    ) -> Result<(Arc<TrackMediaMetadata>, AudioStreamHeaders, ByteRange), StreamError> {
        let meta = self
            .resolve_track_media_for_worker(db_track_id, worker_id, false)
            .await?;

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

        let headers = AudioStreamHeaders {
            status,
            content_type: meta.mime_type.clone(),
            content_length: range.length(),
            content_range,
            accept_ranges: "bytes",
        };
        Ok((meta, headers, range))
    }

    pub async fn open_stream_headers(
        &self,
        db_track_id: i32,
        range_header: Option<&str>,
    ) -> Result<AudioStreamHeaders, StreamError> {
        let (_, headers, _) = self
            .prepare_stream(db_track_id, range_header, usize::MAX)
            .await?;
        Ok(headers)
    }

    pub async fn open_stream(
        &self,
        db_track_id: i32,
        range_header: Option<&str>,
    ) -> Result<AudioStreamResponse, StreamError> {
        let worker_id = self.worker_pool.select_worker_for_stream()?;
        let (meta, headers, range) = self
            .prepare_stream(db_track_id, range_header, worker_id)
            .await?;
        let engine = self.clone();
        let stale_file_reference = meta.file_reference.clone();
        let refresher: crate::pipe::LocationRefresher = Arc::new(move || {
            let engine = engine.clone();
            let stale_file_reference = stale_file_reference.clone();
            Box::pin(async move {
                let fresh_meta = engine
                    .refresh_track_media_if_stale(db_track_id, worker_id, &stale_file_reference)
                    .await?;
                Ok(fresh_meta.input_location())
            })
        });

        let params = crate::pipe::StreamPipeParams {
            range,
            document_id: meta.document_id,
            location: Arc::new(tokio::sync::RwLock::new(meta.input_location())),
            worker_id,
            dc_id: Arc::new(std::sync::atomic::AtomicI32::new(meta.dc_id)),
            refresh_location: Some(refresher),
        };
        let stream = create_stream_pipe(
            params,
            Arc::clone(&self.worker_pool),
            Arc::clone(&self.cache),
        );

        Ok(AudioStreamResponse {
            status: headers.status,
            content_type: headers.content_type,
            content_length: headers.content_length,
            content_range: headers.content_range,
            accept_ranges: headers.accept_ranges,
            stream,
        })
    }
}
