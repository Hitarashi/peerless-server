use std::{path::Path, sync::Arc, time::Duration};

use engine::orchestrator::deps::{
    AlbumDetailsCaption, BoxFuture, ChatDelivery, ChatMessageRef, Delivery, DeliveryError,
    DeliveryReceipt, DeliveryRejection, DumpMessageRef, DumpPublication, DumpPublish, TrackCaption,
    UploadProgressCallback, ZipCaption,
};
use ferogram::{
    ErrorKind, InputMessage, InvocationError, InvocationErrorExt, PeerRef, TransferHandle,
};

const MAX_CAPTION_UTF16_LEN: usize = 1024;

const MAX_TELEGRAM_TRACK_BYTES: u64 = 2_000_000_000;

const MAX_UPLOAD_ATTEMPTS: usize = 3;
const UPLOAD_STALL_TIMEOUT: Duration = Duration::from_secs(45);
const TELEGRAM_REQUEST_TIMEOUT: Duration = Duration::from_secs(90);

// Unlike a detached watchdog, this deadline drops the pending upload future.
// Keep watching until the future completes, even if its byte counter says 100%.
async fn monitor_upload<T>(
    upload: impl std::future::Future<Output = Result<T, DeliveryError>>,
    mut progress: impl FnMut() -> (u64, u64),
    callback: Option<&UploadProgressCallback>,
    worker_id: Option<usize>,
    dc_id: Option<i32>,
    stall_timeout: Duration,
) -> Result<T, DeliveryError> {
    tokio::pin!(upload);
    let mut interval = tokio::time::interval(Duration::from_millis(500));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut last_done = progress().0;
    let deadline = tokio::time::sleep(stall_timeout);
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            biased;
            result = &mut upload => {
                if result.is_ok() {
                    let (done, total) = progress();
                    if total > 0 && done > total {
                        return Err(DeliveryError::Transient(format!(
                            "invalid upload progress: {done} exceeds {total} bytes"
                        )));
                    }
                    if let Some(callback) = callback {
                        callback(done, total, worker_id, dc_id);
                    }
                }
                return result;
            }
            _ = &mut deadline => {
                let (done, total) = progress();
                // An ACK may arrive between the last sample and this deadline.
                if done != last_done {
                    last_done = done;
                    deadline.as_mut().reset(tokio::time::Instant::now() + stall_timeout);
                    continue;
                }
                tracing::warn!(worker_id, dc_id, done, total,
                    stall_seconds = stall_timeout.as_secs(),
                    "Upload stalled: dropping pending transfer and retrying");
                return Err(DeliveryError::Transient(format!(
                    "upload made no progress for {} seconds ({done}/{total} bytes)",
                    stall_timeout.as_secs()
                )));
            }
            _ = interval.tick() => {
                let (done, total) = progress();
                if total > 0 && done > total {
                    return Err(DeliveryError::Transient(format!(
                        "invalid upload progress: {done} exceeds {total} bytes"
                    )));
                }
                if done != last_done {
                    last_done = done;
                    deadline.as_mut().reset(tokio::time::Instant::now() + stall_timeout);
                }
                if let Some(callback) = callback {
                    callback(done, total, worker_id, dc_id);
                }
            }
        }
    }
}

// Cancelling a job drops its transfer as well as its semaphore permits.
struct TransferGuard(TransferHandle);

impl Drop for TransferGuard {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

async fn telegram_request<T>(
    stage: &str,
    request: impl std::future::IntoFuture<Output = Result<T, InvocationError>>,
) -> Result<T, DeliveryError> {
    tokio::time::timeout(TELEGRAM_REQUEST_TIMEOUT, request.into_future())
        .await
        .map_err(|_| DeliveryError::Transient(format!("{stage} timed out after 90 seconds")))?
        .map_err(map_invocation)
}

fn clamp_plain_text(text: &str, max_utf16: usize) -> String {
    let mut curr_len = 0;
    let mut byte_limit = text.len();
    for (idx, ch) in text.char_indices() {
        let ch_len = ch.len_utf16();
        if curr_len + ch_len > max_utf16.saturating_sub(1) {
            byte_limit = idx;
            break;
        }
        curr_len += ch_len;
    }
    let mut truncated = text[..byte_limit].to_string();
    truncated.push('…');
    truncated
}

fn render_track_caption(caption: &TrackCaption) -> String {
    match caption {
        TrackCaption::Machine { track_id, codec } => {
            crate::caption::format_dump_caption(&crate::caption::DumpCaptionMetadata {
                track_id,
                codec: Some(codec),
            })
        }
        TrackCaption::Plain(text) => text.clone(),
    }
}

fn render_zip_caption(caption: &ZipCaption) -> String {
    crate::caption::format_zip_dump_caption(
        &crate::caption::DumpZipCaptionMetadata {
            album_id: &caption.album_id,
            codec: caption.codec.as_deref(),
            part_index: caption.part_index,
            total_parts: caption.total_parts,
            generation_hash: &caption.generation_hash,
        },
        caption.is_complete,
        0,
    )
}

fn render_album_details_caption(caption: &AlbumDetailsCaption) -> String {
    crate::caption::format_album_details_caption(&crate::caption::AlbumDetailsCaptionMetadata {
        album: &caption.album,
        artist: &caption.artist,
        album_url: caption.album_url.as_deref(),
        total_tracks: caption.total_tracks,
        delivered_tracks: caption.delivered_tracks,
        size_bytes: caption.size_bytes,
        total_parts: caption.total_parts,
        release_year: &caption.release_year,
        genre: caption.genre.as_deref(),
        record_label: caption.record_label.as_deref(),
        is_partial: caption.is_partial,
        user_name: caption.user_name.as_deref(),
        user_id: caption.user_id,
        codec: caption.codec.as_deref(),
    })
}

fn prepare_media_caption(caption_html: &str) -> InputMessage {
    let msg = InputMessage::html(caption_html);
    let utf16_count = msg.text.encode_utf16().count();
    if utf16_count <= MAX_CAPTION_UTF16_LEN {
        return msg;
    }
    tracing::warn!(
        utf16_count,
        max = MAX_CAPTION_UTF16_LEN,
        "Media caption exceeds 1024 UTF-16 code units; clamping to plain text"
    );
    let clamped = clamp_plain_text(&msg.text, MAX_CAPTION_UTF16_LEN);
    InputMessage::text(clamped)
}

pub struct FerogramTelegramSink {
    client: Arc<ferogram::Client>,
    dump_peer: PeerRef,
    dump_peer_native_id: i64,
    worker_pool: Option<Arc<stream::StreamWorkerPool>>,
    upload_slots: tokio::sync::Semaphore,
    primary_upload_slot: Arc<tokio::sync::Semaphore>,
}

fn map_invocation(error: InvocationError) -> DeliveryError {
    let detail = error.to_string();
    match error {
        InvocationError::StaleHash | InvocationError::PeerNotCached(_) => {
            DeliveryError::Transient(detail)
        }
        error => match error.kind() {
            ErrorKind::FloodWait(_)
            | ErrorKind::Network
            | ErrorKind::Migration(_)
            | ErrorKind::Transfer => DeliveryError::Transient(detail),

            ErrorKind::FileReferenceExpired => DeliveryError::Transient(detail),
            ErrorKind::Rpc { code, .. } if code >= 500 => DeliveryError::Transient(detail),
            ErrorKind::Rpc { name, .. } if name == "ENTITY_BOUNDS_INVALID" => {
                DeliveryError::Rejected(DeliveryRejection::EntityBoundsInvalid)
            }
            ErrorKind::Rpc { name, .. } if name == "MEDIA_CAPTION_TOO_LONG" => {
                DeliveryError::Rejected(DeliveryRejection::CaptionTooLong)
            }
            ErrorKind::Rpc { .. } => DeliveryError::Rejected(DeliveryRejection::Other(detail)),
            ErrorKind::Auth | ErrorKind::Cancelled | ErrorKind::Other => {
                DeliveryError::Unavailable(detail)
            }
            _ => DeliveryError::Unavailable(detail),
        },
    }
}

fn progress_task(
    handle: &TransferHandle,
    callback: Option<&UploadProgressCallback>,
    worker_id: Option<usize>,
    dc_id: Option<i32>,
) -> ProgressTaskGuard {
    ProgressTaskGuard(callback.map(|callback| {
        let callback = Arc::clone(callback);
        let handle = handle.clone();
        tokio::spawn(async move {
            loop {
                let progress = handle.progress();
                callback(progress.done, progress.total, worker_id, dc_id);
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        })
    }))
}

struct ProgressTaskGuard(Option<tokio::task::JoinHandle<()>>);

impl Drop for ProgressTaskGuard {
    fn drop(&mut self) {
        if let Some(task) = &self.0 {
            task.abort();
        }
    }
}

impl FerogramTelegramSink {
    pub async fn new(
        client: Arc<ferogram::Client>,
        dump_peer: PeerRef,
    ) -> Result<Self, DeliveryError> {
        let resolved = dump_peer
            .clone()
            .resolve(&client)
            .await
            .map_err(map_invocation)?;
        let dump_peer_native_id = ferogram::PeerExt::bare_id(&resolved);
        let resolvable_dump_peer = match &resolved {
            ferogram::tl::enums::Peer::Channel(c) => {
                PeerRef::from(-1_000_000_000_000 - c.channel_id)
            }
            _ => dump_peer,
        };
        Ok(Self {
            client,
            dump_peer: resolvable_dump_peer,
            dump_peer_native_id,
            worker_pool: None,
            upload_slots: tokio::sync::Semaphore::new(1),
            primary_upload_slot: Arc::new(tokio::sync::Semaphore::new(1)),
        })
    }

    pub fn with_worker_pool(mut self, pool: Arc<stream::StreamWorkerPool>) -> Self {
        self.upload_slots = tokio::sync::Semaphore::new(pool.upload_session_count());
        self.primary_upload_slot = pool.primary_upload_slot();
        self.worker_pool = Some(pool);
        self
    }

    async fn upload_file(
        client: &ferogram::Client,
        file_path: &str,
        callback: Option<&UploadProgressCallback>,
        worker_id: Option<usize>,
        dc_id: Option<i32>,
    ) -> Result<ferogram::media::UploadedFile, DeliveryError> {
        let handle = TransferHandle::new();
        let _guard = TransferGuard(handle.clone());
        tracing::info!(file_path, worker_id, dc_id, "Upload attempt started");
        let uploaded = monitor_upload(
            async {
                client
                    .upload_file(file_path)
                    .handle(&handle)
                    .await
                    .map_err(map_invocation)
            },
            || {
                let p = handle.progress();
                (p.done, p.total)
            },
            callback,
            worker_id,
            dc_id,
            UPLOAD_STALL_TIMEOUT,
        )
        .await?;
        tracing::info!(
            file_path,
            worker_id,
            dc_id,
            bytes = handle.progress().done,
            "Upload bytes acknowledged; publishing message"
        );
        Ok(uploaded)
    }

    fn file_ids(document: &ferogram::media::Document) -> (String, String) {
        (
            format!(
                "mtproto:v1:{}:{}:{}",
                document.raw.dc_id,
                document.id(),
                document.access_hash()
            ),
            format!("mtproto:document:{}", document.id()),
        )
    }

    async fn upload_thumbnail_with_client(
        client: &ferogram::Client,
        thumb_path: &str,
    ) -> Result<ferogram::tl::enums::InputFile, DeliveryError> {
        let uploaded = telegram_request("thumbnail upload", client.upload_file(thumb_path)).await?;
        let ferogram::tl::enums::InputMedia::UploadedPhoto(photo) = uploaded.as_photo_media()
        else {
            return Err(DeliveryError::UnexpectedMedia);
        };
        Ok(photo.file)
    }

    async fn upload_thumbnail(
        &self,
        thumb_path: &str,
    ) -> Result<ferogram::tl::enums::InputFile, DeliveryError> {
        Self::upload_thumbnail_with_client(&self.client, thumb_path).await
    }

    async fn upload_zip_media(
        &self,
        file_path: &str,
        thumb_path: Option<&str>,
        on_upload_progress: Option<&UploadProgressCallback>,
    ) -> Result<ferogram::tl::enums::InputMedia, DeliveryError> {
        let uploaded =
            Self::upload_file(&self.client, file_path, on_upload_progress, None, None).await?;
        let mut media = uploaded.as_document_media();
        if let Some(thumb_path) = thumb_path {
            match self.upload_thumbnail(thumb_path).await {
                Ok(thumb) => {
                    if let ferogram::tl::enums::InputMedia::UploadedDocument(document) = &mut media
                    {
                        document.thumb = Some(thumb);
                    }
                }
                Err(error) => {
                    tracing::warn!(%error, "ZIP thumbnail upload failed; sending without");
                }
            }
        }
        Ok(media)
    }
}

fn force_audio_document(
    document: &mut ferogram::tl::types::InputMediaUploadedDocument,
    file_path: &std::path::Path,
    duration: i32,
    title: &str,
    performer: &str,
) {
    use ferogram::tl::enums::DocumentAttribute;

    if !document.mime_type.starts_with("audio/") {
        document.mime_type = audio_mime_for(file_path).to_string();
    }

    let filename = document
        .attributes
        .iter()
        .find_map(|attribute| match attribute {
            DocumentAttribute::Filename(name) => Some(name.file_name.clone()),
            _ => None,
        });
    document.attributes.clear();
    document.attributes.push(DocumentAttribute::Audio(
        ferogram::tl::types::DocumentAttributeAudio {
            voice: false,
            duration,
            title: Some(title.to_string()),
            performer: Some(performer.to_string()),
            waveform: None,
        },
    ));
    if let Some(name) = filename {
        document.attributes.push(DocumentAttribute::Filename(
            ferogram::tl::types::DocumentAttributeFilename { file_name: name },
        ));
    }
}

fn audio_mime_for(file_path: &std::path::Path) -> &'static str {
    match file_path
        .extension()
        .and_then(|value| value.to_str())
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("flac") => "audio/flac",
        Some("m4a" | "mp4" | "m4b") => "audio/mp4",
        Some("ogg" | "oga") => "audio/ogg",
        Some("opus") => "audio/opus",
        Some("mp3") => "audio/mpeg",
        _ => "audio/mp4",
    }
}

impl Delivery for FerogramTelegramSink {
    fn publish_to_dump<'a>(
        &'a self,
        publication: DumpPublish,
    ) -> BoxFuture<'a, Result<DumpPublication, DeliveryError>> {
        Box::pin(async move {
            let _upload_slot = self
                .upload_slots
                .acquire()
                .await
                .map_err(|_| DeliveryError::Unavailable("upload limiter closed".into()))?;
            match publication {
                DumpPublish::TrackAudio {
                    file_path,
                    title,
                    performer,
                    duration,
                    caption,
                    on_upload_progress,
                } => {
                    let caption_html = render_track_caption(&caption);
                    let size = tokio::fs::metadata(&file_path)
                        .await
                        .map_err(|error| DeliveryError::LocalIo(error.to_string()))?
                        .len();
                    if size > MAX_TELEGRAM_TRACK_BYTES {
                        return Err(DeliveryError::Rejected(DeliveryRejection::Other(format!(
                            "track exceeds Telegram's 2000 MB per-track upload limit ({size} bytes)"
                        ))));
                    }
                    let duration = i32::try_from(duration).map_err(|error| {
                        DeliveryError::LocalIo(format!("duration out of range: {error}"))
                    })?;

                    let mut last_error = None;

                    for attempt in 0..MAX_UPLOAD_ATTEMPTS {
                        let lease = if attempt == MAX_UPLOAD_ATTEMPTS - 1 {
                            None
                        } else {
                            self.worker_pool
                                .as_ref()
                                .and_then(|pool| pool.lease_upload_worker().ok())
                        };
                        let worker_client =
                            lease.as_ref().map(|l| &l.client).unwrap_or(&self.client);
                        let _primary_slot = if lease.is_none() {
                            Some(self.primary_upload_slot.acquire().await.map_err(|_| {
                                DeliveryError::Unavailable("primary upload limiter closed".into())
                            })?)
                        } else {
                            None
                        };

                        let worker_id = lease.as_ref().map(|l| l.worker_id);
                        let dc_id = lease.as_ref().map(|l| l.home_dc);
                        let upload_result = Self::upload_file(
                            worker_client,
                            &file_path,
                            on_upload_progress.as_ref(),
                            worker_id,
                            dc_id,
                        )
                        .await;

                        let uploaded = match upload_result {
                            Ok(res) => res,
                            Err(err) => {
                                let mapped = err;
                                if let (Some(pool), Some(lease)) = (&self.worker_pool, &lease) {
                                    pool.mark_worker_slow(
                                        lease.worker_id,
                                        Duration::from_secs(120),
                                    );
                                }
                                tracing::warn!(attempt = attempt + 1, worker_id, dc_id, %mapped, "Track audio upload failed");
                                last_error = Some(mapped);
                                if attempt + 1 < MAX_UPLOAD_ATTEMPTS {
                                    continue;
                                } else {
                                    return Err(last_error.unwrap());
                                }
                            }
                        };

                        let mut media = uploaded.as_auto_media();
                        if let ferogram::tl::enums::InputMedia::UploadedDocument(document) =
                            &mut media
                        {
                            force_audio_document(
                                document,
                                std::path::Path::new(&file_path),
                                duration,
                                &title,
                                &performer,
                            );
                        }
                        let send_res = telegram_request(
                            "message publication",
                            worker_client.send_message(
                                self.dump_peer.clone(),
                                prepare_media_caption(&caption_html)
                                    .silent(true)
                                    .copy_media(media),
                            ),
                        )
                        .await;

                        let message = match send_res {
                            Ok(msg) => msg,
                            Err(err) => {
                                let mapped = err;
                                tracing::warn!(
                                    attempt = attempt + 1,
                                    max_attempts = MAX_UPLOAD_ATTEMPTS,
                                    %mapped,
                                    "Track audio send_message to dump failed; retrying"
                                );
                                last_error = Some(mapped);
                                if attempt + 1 < MAX_UPLOAD_ATTEMPTS {
                                    continue;
                                } else {
                                    return Err(last_error.unwrap());
                                }
                            }
                        };

                        if message.peer_id().map(ferogram::PeerExt::bare_id)
                            != Some(self.dump_peer_native_id)
                        {
                            return Err(DeliveryError::Unavailable(format!(
                                "dump upload landed in unexpected peer (expected {}, got {:?})",
                                self.dump_peer_native_id,
                                message.peer_id().map(ferogram::PeerExt::bare_id)
                            )));
                        }
                        tracing::info!(
                            attempt = attempt + 1,
                            worker_id,
                            dc_id,
                            message_id = message.id(),
                            file_path,
                            "Upload published to dump"
                        );
                        let Some(document) = message.document() else {
                            return Err(DeliveryError::UnexpectedMedia);
                        };
                        let is_audio = document.raw.attributes.iter().any(|attribute| {
                            matches!(attribute, ferogram::tl::enums::DocumentAttribute::Audio(_))
                        });
                        if !is_audio {
                            return Err(DeliveryError::UnexpectedMedia);
                        }
                        let (file_id, file_unique_id) = Self::file_ids(&document);
                        return Ok(DumpPublication {
                            message: DumpMessageRef::new(i64::from(message.id())),
                            file_id,
                            file_unique_id,
                        });
                    }

                    Err(last_error.unwrap_or_else(|| {
                        DeliveryError::Transient("All worker upload attempts failed".to_string())
                    }))
                }
                DumpPublish::ZipDocument {
                    file_path,
                    thumb_path,
                    caption,
                    on_upload_progress,
                } => {
                    let caption_html = render_zip_caption(&caption);
                    let mut last_error = None;

                    for attempt in 0..MAX_UPLOAD_ATTEMPTS {
                        let lease = if attempt == MAX_UPLOAD_ATTEMPTS - 1 {
                            None
                        } else {
                            self.worker_pool
                                .as_ref()
                                .and_then(|pool| pool.lease_upload_worker().ok())
                        };
                        let worker_client =
                            lease.as_ref().map(|l| &l.client).unwrap_or(&self.client);
                        let _primary_slot = if lease.is_none() {
                            Some(self.primary_upload_slot.acquire().await.map_err(|_| {
                                DeliveryError::Unavailable("primary upload limiter closed".into())
                            })?)
                        } else {
                            None
                        };

                        let worker_id = lease.as_ref().map(|l| l.worker_id);
                        let dc_id = lease.as_ref().map(|l| l.home_dc);
                        let upload_result = Self::upload_file(
                            worker_client,
                            &file_path,
                            on_upload_progress.as_ref(),
                            worker_id,
                            dc_id,
                        )
                        .await;

                        let uploaded = match upload_result {
                            Ok(res) => res,
                            Err(err) => {
                                let mapped = err;
                                if let (Some(pool), Some(lease)) = (&self.worker_pool, &lease) {
                                    pool.mark_worker_slow(
                                        lease.worker_id,
                                        Duration::from_secs(120),
                                    );
                                }
                                tracing::warn!(attempt = attempt + 1, worker_id, dc_id, %mapped, "ZIP upload failed");
                                last_error = Some(mapped);
                                if attempt + 1 < MAX_UPLOAD_ATTEMPTS {
                                    continue;
                                } else {
                                    return Err(last_error.unwrap());
                                }
                            }
                        };

                        let mut media = uploaded.as_document_media();
                        if let Some(thumb_path) = thumb_path.as_deref() {
                            match Self::upload_thumbnail_with_client(worker_client, thumb_path)
                                .await
                            {
                                Ok(thumb) => {
                                    if let ferogram::tl::enums::InputMedia::UploadedDocument(
                                        document,
                                    ) = &mut media
                                    {
                                        document.thumb = Some(thumb);
                                    }
                                }
                                Err(error) => {
                                    tracing::warn!(%error, "ZIP thumbnail upload failed; sending without");
                                }
                            }
                        }

                        let send_res = telegram_request(
                            "message publication",
                            worker_client.send_message(
                                self.dump_peer.clone(),
                                prepare_media_caption(&caption_html)
                                    .silent(true)
                                    .copy_media(media),
                            ),
                        )
                        .await;

                        let message = match send_res {
                            Ok(msg) => msg,
                            Err(err) => {
                                let mapped = err;
                                tracing::warn!(
                                    attempt = attempt + 1,
                                    max_attempts = MAX_UPLOAD_ATTEMPTS,
                                    %mapped,
                                    "ZIP send_message to dump failed; retrying"
                                );
                                last_error = Some(mapped);
                                if attempt + 1 < MAX_UPLOAD_ATTEMPTS {
                                    continue;
                                } else {
                                    return Err(last_error.unwrap());
                                }
                            }
                        };

                        tracing::info!(
                            attempt = attempt + 1,
                            worker_id,
                            dc_id,
                            message_id = message.id(),
                            file_path,
                            "Upload published to dump"
                        );
                        let Some(document) = message.document() else {
                            return Err(DeliveryError::UnexpectedMedia);
                        };
                        let (file_id, file_unique_id) = Self::file_ids(&document);
                        return Ok(DumpPublication {
                            message: DumpMessageRef::new(i64::from(message.id())),
                            file_id,
                            file_unique_id,
                        });
                    }

                    Err(last_error.unwrap_or_else(|| {
                        DeliveryError::Transient("All worker upload attempts failed".to_string())
                    }))
                }
            }
        })
    }

    fn deliver_to_chat<'a>(
        &'a self,
        delivery: ChatDelivery,
    ) -> BoxFuture<'a, Result<DeliveryReceipt, DeliveryError>> {
        Box::pin(async move {
            match delivery {
                ChatDelivery::DumpCopy {
                    destination,
                    source,
                    reply_to,
                    silent,
                } => {
                    if destination.id() == 0 {
                        return Err(DeliveryError::Unavailable(
                            "destination chat id cannot be 0".to_string(),
                        ));
                    }
                    let source_id = i32::try_from(source.id()).map_err(|error| {
                        DeliveryError::LocalIo(format!("source message id out of range: {error}"))
                    })?;
                    let reply_to = reply_to
                        .map(|message| i32::try_from(message.id()))
                        .transpose()
                        .map_err(|error| {
                            DeliveryError::LocalIo(format!(
                                "reply message id out of range: {error}"
                            ))
                        })?;
                    let source = telegram_request(
                        "dump message lookup",
                        self.client
                            .get_messages(self.dump_peer.clone(), &[source_id]),
                    )
                    .await?
                    .into_iter()
                    .next()
                    .ok_or(DeliveryError::UnexpectedMedia)?;
                    let media: ferogram::tl::enums::InputMedia = if let Some(photo) = source.photo()
                    {
                        photo.to_input_media().into()
                    } else if let Some(document) = source.document() {
                        document.to_input_media().into()
                    } else {
                        return Err(DeliveryError::UnexpectedMedia);
                    };
                    let message = telegram_request(
                        "message publication",
                        self.client.send_message(
                            PeerRef::from(destination.id()),
                            InputMessage::text("")
                                .reply_to(reply_to)
                                .silent(silent)
                                .copy_media(media),
                        ),
                    )
                    .await?;
                    Ok(DeliveryReceipt::Message(ChatMessageRef::new(i64::from(
                        message.id(),
                    ))))
                }
                ChatDelivery::ZipDocument {
                    destination,
                    file_path,
                    thumb_path,
                    caption,
                    on_upload_progress,
                } => {
                    let caption_html = render_zip_caption(&caption);
                    if destination.id() == 0 {
                        return Err(DeliveryError::Unavailable(
                            "destination chat id cannot be 0".to_string(),
                        ));
                    }
                    let _upload_slot =
                        self.upload_slots.acquire().await.map_err(|_| {
                            DeliveryError::Unavailable("upload limiter closed".into())
                        })?;
                    let _primary_slot = self.primary_upload_slot.acquire().await.map_err(|_| {
                        DeliveryError::Unavailable("primary upload limiter closed".into())
                    })?;
                    let media = self
                        .upload_zip_media(
                            &file_path,
                            thumb_path.as_deref(),
                            on_upload_progress.as_ref(),
                        )
                        .await?;
                    let message = telegram_request(
                        "message publication",
                        self.client.send_message(
                            PeerRef::from(destination.id()),
                            prepare_media_caption(&caption_html).copy_media(media),
                        ),
                    )
                    .await?;
                    Ok(DeliveryReceipt::Message(ChatMessageRef::new(i64::from(
                        message.id(),
                    ))))
                }
                ChatDelivery::Photo {
                    destination,
                    image_bytes,
                    caption,
                } => {
                    let caption_html = render_album_details_caption(&caption);
                    if destination.id() == 0 {
                        return Err(DeliveryError::Unavailable(
                            "destination chat id cannot be 0".to_string(),
                        ));
                    }
                    let _upload_slot =
                        self.upload_slots.acquire().await.map_err(|_| {
                            DeliveryError::Unavailable("upload limiter closed".into())
                        })?;
                    let _primary_slot = self.primary_upload_slot.acquire().await.map_err(|_| {
                        DeliveryError::Unavailable("primary upload limiter closed".into())
                    })?;
                    let uploaded = telegram_request(
                        "cover upload",
                        self.client
                            .upload(std::io::Cursor::new(image_bytes), "cover.jpg"),
                    )
                    .await?;
                    let media = uploaded.as_photo_media();
                    telegram_request(
                        "message publication",
                        self.client.send_message(
                            PeerRef::from(destination.id()),
                            prepare_media_caption(&caption_html).copy_media(media),
                        ),
                    )
                    .await?;
                    Ok(DeliveryReceipt::PreviewDelivered)
                }
            }
        })
    }

    fn materialize_cached<'a>(
        &'a self,
        source: DumpMessageRef,
        destination: &'a Path,
        progress: Option<&'a UploadProgressCallback>,
    ) -> BoxFuture<'a, Result<(), DeliveryError>> {
        Box::pin(async move {
            let source_id = i32::try_from(source.id()).map_err(|error| {
                DeliveryError::LocalIo(format!("source message id out of range: {error}"))
            })?;
            let message = self
                .client
                .get_messages(self.dump_peer.clone(), &[source_id])
                .await
                .map_err(map_invocation)?
                .into_iter()
                .next()
                .ok_or(DeliveryError::UnexpectedMedia)?;
            let document = message.document().ok_or(DeliveryError::UnexpectedMedia)?;
            let handle = TransferHandle::new();
            let _transfer_guard = TransferGuard(handle.clone());
            let progress_task = progress_task(&handle, progress, None, None);
            let download_result = self
                .client
                .download_file(&document, destination)
                .handle(&handle)
                .await;
            drop(progress_task);
            download_result.map(|_| ()).map_err(map_invocation)
        })
    }

    fn retract_dump<'a>(
        &'a self,
        messages: &'a [DumpMessageRef],
    ) -> BoxFuture<'a, Result<(), DeliveryError>> {
        Box::pin(async move {
            let ids: Vec<i32> = messages
                .iter()
                .filter_map(|message| match i32::try_from(message.id()) {
                    Ok(id) => Some(id),
                    Err(error) => {
                        tracing::warn!(
                            message_id = message.id(),
                            %error,
                            "skipping out-of-range dump message id"
                        );
                        None
                    }
                })
                .collect();
            if ids.is_empty() {
                return Ok(());
            }
            let messages = self
                .client
                .get_messages(self.dump_peer.clone(), &ids)
                .await
                .map_err(map_invocation)?;
            let mut first_error = None;
            for message in messages {
                if let Err(error) = message.delete_with(&self.client).await {
                    let error = map_invocation(error);
                    tracing::warn!(%error, message_id = message.id(), "failed to delete dump message");
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                }
            }
            first_error.map_or(Ok(()), Err)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stale_hash_and_peer_not_cached_are_transient() {
        assert!(matches!(
            map_invocation(InvocationError::StaleHash),
            DeliveryError::Transient(_)
        ));
        assert!(matches!(
            map_invocation(InvocationError::PeerNotCached("test channel".into())),
            DeliveryError::Transient(_)
        ));
    }

    #[test]
    fn server_rpc_errors_are_transient() {
        let error = InvocationError::Rpc(ferogram::RpcError {
            code: 500,
            name: "INTERNAL".into(),
            value: None,
        });
        assert!(matches!(map_invocation(error), DeliveryError::Transient(_)));
    }

    #[test]
    fn client_rpc_errors_are_rejected() {
        let error = InvocationError::Rpc(ferogram::RpcError {
            code: 400,
            name: "CHAT_WRITE_FORBIDDEN".into(),
            value: None,
        });
        assert!(matches!(
            map_invocation(error),
            DeliveryError::Rejected(DeliveryRejection::Other(_))
        ));
    }

    #[test]
    fn caption_too_long_rpc_error_is_typed() {
        let error = InvocationError::Rpc(ferogram::RpcError {
            code: 400,
            name: "MEDIA_CAPTION_TOO_LONG".into(),
            value: None,
        });
        assert_eq!(
            map_invocation(error),
            DeliveryError::Rejected(DeliveryRejection::CaptionTooLong)
        );
    }

    #[test]
    fn prepare_media_caption_clamps_long_text() {
        let long_str = "a".repeat(1100);
        let msg = prepare_media_caption(&long_str);
        assert!(msg.text.encode_utf16().count() <= 1024);
        assert!(msg.text.ends_with('…'));
    }

    #[test]
    fn file_ids_match_mtproto_format() {
        let raw = ferogram::tl::types::Document {
            id: 7,
            access_hash: 11,
            file_reference: Vec::new(),
            date: 0,
            mime_type: "audio/mp4".into(),
            size: 0,
            dc_id: 2,
            attributes: Vec::new(),
            thumbs: None,
            video_thumbs: None,
        };
        let (file_id, unique_id) =
            FerogramTelegramSink::file_ids(&ferogram::media::Document::from_raw(raw));
        assert_eq!(file_id, "mtproto:v1:2:7:11");
        assert_eq!(unique_id, "mtproto:document:7");
    }

    fn video_classified_document() -> ferogram::tl::types::InputMediaUploadedDocument {
        ferogram::tl::types::InputMediaUploadedDocument {
            nosound_video: false,
            force_file: false,
            spoiler: false,
            file: ferogram::tl::enums::InputFile::Big(ferogram::tl::types::InputFileBig {
                id: 1,
                parts: 1,
                name: "probe.m4a".into(),
            }),
            thumb: None,
            mime_type: "video/mp4".into(),
            attributes: vec![
                ferogram::tl::enums::DocumentAttribute::Video(
                    ferogram::tl::types::DocumentAttributeVideo {
                        round_message: false,
                        supports_streaming: true,
                        nosound: false,
                        duration: 0.0,
                        w: 320,
                        h: 320,
                        preload_prefix_size: None,
                        video_start_ts: None,
                        video_codec: None,
                    },
                ),
                ferogram::tl::enums::DocumentAttribute::Filename(
                    ferogram::tl::types::DocumentAttributeFilename {
                        file_name: "05. Song [Atmos].m4a".into(),
                    },
                ),
            ],
            stickers: None,
            ttl_seconds: None,
            video_cover: None,
            video_timestamp: None,
        }
    }

    #[test]
    fn video_classified_upload_is_forced_back_to_audio() {
        let mut document = video_classified_document();
        force_audio_document(
            &mut document,
            std::path::Path::new("/tmp/05. Song [Atmos].m4a"),
            196,
            "The Maari Swag",
            "Anirudh Ravichander",
        );

        assert_eq!(document.mime_type, "audio/mp4");
        assert!(document.mime_type.starts_with("audio/"));

        let audio = document
            .attributes
            .iter()
            .find_map(|attribute| match attribute {
                ferogram::tl::enums::DocumentAttribute::Audio(audio) => Some(audio),
                _ => None,
            })
            .expect("an audio attribute must exist");
        assert!(!audio.voice);
        assert_eq!(audio.duration, 196);
        assert_eq!(audio.title.as_deref(), Some("The Maari Swag"));
        assert_eq!(audio.performer.as_deref(), Some("Anirudh Ravichander"));

        assert!(
            !document.attributes.iter().any(|attribute| matches!(
                attribute,
                ferogram::tl::enums::DocumentAttribute::Video(_)
            )),
            "a video attribute would make Telegram render this as media"
        );
    }

    #[test]
    fn already_audio_upload_keeps_its_filename_and_gains_real_duration() {
        let mut document = video_classified_document();
        document.mime_type = "audio/m4a".into();
        document.attributes = vec![ferogram::tl::enums::DocumentAttribute::Audio(
            ferogram::tl::types::DocumentAttributeAudio {
                voice: false,
                duration: 0,
                title: Some("stale".into()),
                performer: None,
                waveform: None,
            },
        )];

        force_audio_document(
            &mut document,
            std::path::Path::new("/tmp/track.flac"),
            130,
            "Heartbreak Hotel",
            "Elvis Presley",
        );

        assert_eq!(document.mime_type, "audio/m4a");
        let audio = document
            .attributes
            .iter()
            .find_map(|attribute| match attribute {
                ferogram::tl::enums::DocumentAttribute::Audio(audio) => Some(audio),
                _ => None,
            })
            .expect("audio attribute");
        assert_eq!(audio.duration, 130);
        assert_eq!(audio.title.as_deref(), Some("Heartbreak Hotel"));
    }

    #[test]
    fn filename_survives_the_rewrite() {
        let mut document = video_classified_document();
        force_audio_document(
            &mut document,
            std::path::Path::new("/tmp/05. Song [Atmos].m4a"),
            196,
            "The Maari Swag",
            "Anirudh Ravichander",
        );
        assert!(
            document.attributes.iter().any(|attribute| matches!(
                attribute,
                ferogram::tl::enums::DocumentAttribute::Filename(name)
                    if name.file_name == "05. Song [Atmos].m4a"
            )),
            "the displayed filename must be preserved"
        );
    }

    #[test]
    fn mime_follows_the_ripped_container() {
        assert_eq!(audio_mime_for(std::path::Path::new("a.m4a")), "audio/mp4");
        assert_eq!(audio_mime_for(std::path::Path::new("a.FLAC")), "audio/flac");
        assert_eq!(audio_mime_for(std::path::Path::new("a.opus")), "audio/opus");
        assert_eq!(audio_mime_for(std::path::Path::new("a.mp3")), "audio/mpeg");

        assert!(audio_mime_for(std::path::Path::new("a.bin")).starts_with("audio/"));
    }

    #[tokio::test(start_paused = true)]
    async fn active_upload_can_take_longer_than_the_stall_deadline() {
        let done = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let upload_done = Arc::clone(&done);
        let result = monitor_upload(
            async move {
                for value in 1..=4 {
                    tokio::time::sleep(Duration::from_secs(30)).await;
                    upload_done.store(value, std::sync::atomic::Ordering::SeqCst);
                }
                Ok(())
            },
            || (done.load(std::sync::atomic::Ordering::SeqCst), 4),
            None,
            None,
            None,
            UPLOAD_STALL_TIMEOUT,
        )
        .await;
        assert!(result.is_ok());
        assert_eq!(done.load(std::sync::atomic::Ordering::SeqCst), 4);
    }

    #[tokio::test(start_paused = true)]
    async fn acknowledgement_at_the_deadline_extends_the_upload() {
        let done = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let upload_done = Arc::clone(&done);
        let result = monitor_upload(
            async move {
                tokio::time::sleep(UPLOAD_STALL_TIMEOUT).await;
                upload_done.store(1, std::sync::atomic::Ordering::SeqCst);
                tokio::time::sleep(Duration::from_secs(40)).await;
                upload_done.store(2, std::sync::atomic::Ordering::SeqCst);
                Ok(())
            },
            || (done.load(std::sync::atomic::Ordering::SeqCst), 2),
            None,
            None,
            None,
            UPLOAD_STALL_TIMEOUT,
        )
        .await;
        assert!(result.is_ok());
    }

    #[tokio::test(start_paused = true)]
    async fn callbacks_stop_when_an_upload_is_cancelled() {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let callback_calls = Arc::clone(&calls);
        let callback: UploadProgressCallback = Arc::new(move |_, _, _, _| {
            callback_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        });
        let handle = TransferHandle::new();
        let upload = async {
            let _guard = TransferGuard(handle.clone());
            monitor_upload(
                std::future::pending::<Result<(), DeliveryError>>(),
                || (0, 100),
                Some(&callback),
                None,
                None,
                UPLOAD_STALL_TIMEOUT,
            )
            .await
        };
        let result = tokio::time::timeout(Duration::from_secs(1), upload).await;
        assert!(result.is_err());
        assert!(handle.is_cancelled());
        let stopped_at = calls.load(std::sync::atomic::Ordering::SeqCst);
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), stopped_at);
    }

    #[tokio::test(start_paused = true)]
    async fn cached_transfer_progress_task_stops_when_its_guard_is_dropped() {
        let handle = TransferHandle::new();
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let callback_calls = Arc::clone(&calls);
        let callback: UploadProgressCallback = Arc::new(move |_, _, _, _| {
            callback_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        });
        let guard = progress_task(&handle, Some(&callback), None, None);
        tokio::task::yield_now().await;
        assert!(calls.load(std::sync::atomic::Ordering::SeqCst) > 0);
        drop(guard);
        let stopped_at = calls.load(std::sync::atomic::Ordering::SeqCst);
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), stopped_at);
    }

    #[tokio::test(start_paused = true)]
    async fn thumbnail_and_publication_requests_have_a_deadline() {
        for stage in ["thumbnail upload", "message publication"] {
            let result =
                telegram_request(stage, std::future::pending::<Result<(), InvocationError>>())
                    .await;
            assert!(
                matches!(result, Err(DeliveryError::Transient(message)) if message.contains(stage))
            );
        }
    }

    struct PendingUpload(Arc<std::sync::atomic::AtomicBool>);

    impl std::future::Future for PendingUpload {
        type Output = Result<(), DeliveryError>;

        fn poll(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Self::Output> {
            std::task::Poll::Pending
        }
    }

    impl Drop for PendingUpload {
        fn drop(&mut self) {
            self.0.store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn stalled_upload_drops_pending_rpc_even_at_completion() {
        // Unknown size (setup), final chunk, and inflated/complete counters must
        // all terminate rather than disabling the inactivity deadline.
        for (done, total) in [(0, 0), (999, 1000), (1000, 1000), (1999, 1000)] {
            let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let result = tokio::time::timeout(
                UPLOAD_STALL_TIMEOUT + Duration::from_secs(1),
                monitor_upload(
                    PendingUpload(Arc::clone(&dropped)),
                    || (done, total),
                    None,
                    Some(0),
                    Some(5),
                    UPLOAD_STALL_TIMEOUT,
                ),
            )
            .await
            .expect("the upload itself must terminate before the outer deadline");
            assert!(matches!(result, Err(DeliveryError::Transient(_))));
            assert!(dropped.load(std::sync::atomic::Ordering::SeqCst));
        }
    }
}
