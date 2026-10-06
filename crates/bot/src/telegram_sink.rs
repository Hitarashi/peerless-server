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
}

fn map_invocation(error: InvocationError) -> DeliveryError {
    let detail = error.to_string();
    match error.kind() {
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
    }
}

fn progress_task(
    handle: &TransferHandle,
    callback: Option<&UploadProgressCallback>,
) -> Option<tokio::task::JoinHandle<()>> {
    callback.map(|callback| {
        let callback = Arc::clone(callback);
        let handle = handle.clone();
        tokio::spawn(async move {
            loop {
                let progress = handle.progress();
                callback(progress.done, progress.total);
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        })
    })
}

fn stop_progress(task: Option<tokio::task::JoinHandle<()>>) {
    if let Some(task) = task {
        task.abort();
    }
}

impl FerogramTelegramSink {
    pub async fn new(
        client: Arc<ferogram::Client>,
        dump_peer: PeerRef,
    ) -> Result<Self, DeliveryError> {
        let dump_peer = dump_peer.resolve(&client).await.map_err(map_invocation)?;
        let dump_peer_native_id = ferogram::PeerExt::bare_id(&dump_peer);
        Ok(Self {
            client,
            dump_peer: PeerRef::from(dump_peer),
            dump_peer_native_id,
        })
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

    async fn upload_thumbnail(
        &self,
        thumb_path: &str,
    ) -> Result<ferogram::tl::enums::InputFile, DeliveryError> {
        let uploaded = self
            .client
            .upload_file(thumb_path)
            .await
            .map_err(map_invocation)?;
        let ferogram::tl::enums::InputMedia::UploadedPhoto(photo) = uploaded.as_photo_media()
        else {
            return Err(DeliveryError::UnexpectedMedia);
        };
        Ok(photo.file)
    }

    async fn upload_zip_media(
        &self,
        file_path: &str,
        thumb_path: Option<&str>,
        on_upload_progress: Option<&UploadProgressCallback>,
    ) -> Result<ferogram::tl::enums::InputMedia, DeliveryError> {
        let handle = TransferHandle::new();
        let progress = progress_task(&handle, on_upload_progress);
        let upload_result = self.client.upload_file(file_path).handle(&handle).await;
        stop_progress(progress);
        let uploaded = upload_result.map_err(map_invocation)?;
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
                    let handle = TransferHandle::new();
                    let progress = progress_task(&handle, on_upload_progress.as_ref());
                    let upload_result = self.client.upload_file(&file_path).handle(&handle).await;
                    stop_progress(progress);
                    let uploaded = upload_result.map_err(map_invocation)?;
                    let duration = i32::try_from(duration).map_err(|error| {
                        DeliveryError::LocalIo(format!("duration out of range: {error}"))
                    })?;
                    let mut media = uploaded.as_auto_media();
                    if let ferogram::tl::enums::InputMedia::UploadedDocument(document) = &mut media
                    {
                        force_audio_document(
                            document,
                            std::path::Path::new(&file_path),
                            duration,
                            &title,
                            &performer,
                        );
                    }
                    let message = self
                        .client
                        .send_message(
                            self.dump_peer.clone(),
                            prepare_media_caption(&caption_html)
                                .silent(true)
                                .copy_media(media),
                        )
                        .await
                        .map_err(map_invocation)?;
                    if message.peer_id().map(ferogram::PeerExt::bare_id)
                        != Some(self.dump_peer_native_id)
                    {
                        return Err(DeliveryError::Unavailable(format!(
                            "dump upload landed in unexpected peer (expected {}, got {:?})",
                            self.dump_peer_native_id,
                            message.peer_id().map(ferogram::PeerExt::bare_id)
                        )));
                    }
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
                    Ok(DumpPublication {
                        message: DumpMessageRef::new(i64::from(message.id())),
                        file_id,
                        file_unique_id,
                    })
                }
                DumpPublish::ZipDocument {
                    file_path,
                    thumb_path,
                    caption,
                    on_upload_progress,
                } => {
                    let caption_html = render_zip_caption(&caption);
                    let media = self
                        .upload_zip_media(
                            &file_path,
                            thumb_path.as_deref(),
                            on_upload_progress.as_ref(),
                        )
                        .await?;
                    let message = self
                        .client
                        .send_message(
                            self.dump_peer.clone(),
                            prepare_media_caption(&caption_html)
                                .silent(true)
                                .copy_media(media),
                        )
                        .await
                        .map_err(map_invocation)?;
                    let Some(document) = message.document() else {
                        return Err(DeliveryError::UnexpectedMedia);
                    };
                    let (file_id, file_unique_id) = Self::file_ids(&document);
                    Ok(DumpPublication {
                        message: DumpMessageRef::new(i64::from(message.id())),
                        file_id,
                        file_unique_id,
                    })
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
                    let source = self
                        .client
                        .get_messages(self.dump_peer.clone(), &[source_id])
                        .await
                        .map_err(map_invocation)?
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
                    let message = self
                        .client
                        .send_message(
                            PeerRef::from(destination.id()),
                            InputMessage::text("")
                                .reply_to(reply_to)
                                .silent(silent)
                                .copy_media(media),
                        )
                        .await
                        .map_err(map_invocation)?;
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
                    let media = self
                        .upload_zip_media(
                            &file_path,
                            thumb_path.as_deref(),
                            on_upload_progress.as_ref(),
                        )
                        .await?;
                    let message = self
                        .client
                        .send_message(
                            PeerRef::from(destination.id()),
                            prepare_media_caption(&caption_html).copy_media(media),
                        )
                        .await
                        .map_err(map_invocation)?;
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
                    let uploaded = self
                        .client
                        .upload(std::io::Cursor::new(image_bytes), "cover.jpg")
                        .await
                        .map_err(map_invocation)?;
                    let media = uploaded.as_photo_media();
                    self.client
                        .send_message(
                            PeerRef::from(destination.id()),
                            prepare_media_caption(&caption_html).copy_media(media),
                        )
                        .await
                        .map_err(map_invocation)?;
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
            let progress_task = progress_task(&handle, progress);
            let download_result = self
                .client
                .download_file(&document, destination)
                .handle(&handle)
                .await;
            stop_progress(progress_task);
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
}
