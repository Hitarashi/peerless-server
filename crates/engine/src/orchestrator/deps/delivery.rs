use std::{fmt, path::Path, sync::Arc};

use super::BoxFuture;

pub type UploadProgressCallback = Arc<dyn Fn(u64, u64) + Send + Sync>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ChatRef(pub i64);

impl ChatRef {
    pub fn new(id: i64) -> Self {
        Self(id)
    }

    pub fn id(self) -> i64 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DumpMessageRef(pub i64);

impl DumpMessageRef {
    pub fn new(id: i64) -> Self {
        Self(id)
    }

    pub fn id(self) -> i64 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ChatMessageRef(pub i64);

impl ChatMessageRef {
    pub fn new(id: i64) -> Self {
        Self(id)
    }

    pub fn id(self) -> i64 {
        self.0
    }
}

/// Caption inputs for a track-audio publication. The delivery sink renders the markup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TrackCaption {
    /// Machine-readable caption identifying the track.
    Machine { track_id: String, codec: String },
    /// Plain-text fallback, used when the machine caption is rejected as too long.
    Plain(String),
}

/// Caption inputs for a ZIP publication. The delivery sink renders the markup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ZipCaption {
    pub album_id: String,
    pub codec: Option<String>,
    pub part_index: i32,
    pub total_parts: i32,
    pub generation_hash: String,
    pub is_complete: bool,
}

/// Caption inputs for an album-details photo. The delivery sink renders the markup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlbumDetailsCaption {
    pub album: String,
    pub artist: String,
    pub album_url: Option<String>,
    pub total_tracks: usize,
    pub delivered_tracks: Option<usize>,
    pub size_bytes: i64,
    pub total_parts: usize,
    pub release_year: String,
    pub genre: Option<String>,
    pub record_label: Option<String>,
    pub is_partial: bool,
    pub user_name: Option<String>,
    pub user_id: i64,
    pub codec: Option<String>,
}

#[derive(Clone)]
pub enum DumpPublish {
    TrackAudio {
        file_path: String,
        title: String,
        performer: String,
        duration: i64,
        caption: TrackCaption,
        on_upload_progress: Option<UploadProgressCallback>,
    },
    ZipDocument {
        file_path: String,
        thumb_path: Option<String>,
        caption: ZipCaption,
        on_upload_progress: Option<UploadProgressCallback>,
    },
}

impl fmt::Debug for DumpPublish {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TrackAudio {
                file_path,
                title,
                performer,
                duration,
                caption,
                ..
            } => formatter
                .debug_struct("TrackAudio")
                .field("file_path", file_path)
                .field("title", title)
                .field("performer", performer)
                .field("duration", duration)
                .field("caption", caption)
                .finish_non_exhaustive(),
            Self::ZipDocument {
                file_path,
                thumb_path,
                caption,
                ..
            } => formatter
                .debug_struct("ZipDocument")
                .field("file_path", file_path)
                .field("thumb_path", thumb_path)
                .field("caption", caption)
                .finish_non_exhaustive(),
        }
    }
}

#[derive(Clone)]
pub enum ChatDelivery {
    DumpCopy {
        destination: ChatRef,
        source: DumpMessageRef,
        reply_to: Option<ChatMessageRef>,
        silent: bool,
    },
    ZipDocument {
        destination: ChatRef,
        file_path: String,
        thumb_path: Option<String>,
        caption: ZipCaption,
        on_upload_progress: Option<UploadProgressCallback>,
    },
    Photo {
        destination: ChatRef,
        image_bytes: Vec<u8>,
        caption: AlbumDetailsCaption,
    },
}

impl fmt::Debug for ChatDelivery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DumpCopy {
                destination,
                source,
                reply_to,
                silent,
            } => formatter
                .debug_struct("DumpCopy")
                .field("destination", destination)
                .field("source", source)
                .field("reply_to", reply_to)
                .field("silent", silent)
                .finish(),
            Self::ZipDocument {
                destination,
                file_path,
                thumb_path,
                caption,
                ..
            } => formatter
                .debug_struct("ZipDocument")
                .field("destination", destination)
                .field("file_path", file_path)
                .field("thumb_path", thumb_path)
                .field("caption", caption)
                .finish_non_exhaustive(),
            Self::Photo {
                destination,
                image_bytes,
                caption,
            } => formatter
                .debug_struct("Photo")
                .field("destination", destination)
                .field("image_bytes", &image_bytes.len())
                .field("caption", caption)
                .finish(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DumpPublication {
    pub message: DumpMessageRef,
    pub file_id: String,
    pub file_unique_id: String,
}

impl DumpPublication {
    pub fn message_id(&self) -> DumpMessageRef {
        self.message
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeliveryReceipt {
    Message(ChatMessageRef),
    PreviewDelivered,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeliveryRejection {
    EntityBoundsInvalid,
    CaptionTooLong,
    Other(String),
}

impl fmt::Display for DeliveryRejection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EntityBoundsInvalid => formatter.write_str("entity bounds invalid"),
            Self::CaptionTooLong => formatter.write_str("caption too long"),
            Self::Other(detail) => formatter.write_str(detail),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DeliveryError {
    #[error("transient delivery failure: {0}")]
    Transient(String),
    #[error("delivery rejected: {0}")]
    Rejected(DeliveryRejection),
    #[error("delivery returned unexpected media")]
    UnexpectedMedia,
    #[error("local delivery I/O failed: {0}")]
    LocalIo(String),
    #[error("delivery unavailable: {0}")]
    Unavailable(String),
}

impl DeliveryError {
    pub fn is_transient(&self) -> bool {
        matches!(self, Self::Transient(_))
    }
}

pub trait Delivery: Send + Sync {
    fn publish_to_dump<'a>(
        &'a self,
        publication: DumpPublish,
    ) -> BoxFuture<'a, Result<DumpPublication, DeliveryError>>;

    fn deliver_to_chat<'a>(
        &'a self,
        delivery: ChatDelivery,
    ) -> BoxFuture<'a, Result<DeliveryReceipt, DeliveryError>>;

    fn materialize_cached<'a>(
        &'a self,
        source: DumpMessageRef,
        destination: &'a Path,
        progress: Option<&'a UploadProgressCallback>,
    ) -> BoxFuture<'a, Result<(), DeliveryError>>;

    fn retract_dump<'a>(
        &'a self,
        messages: &'a [DumpMessageRef],
    ) -> BoxFuture<'a, Result<(), DeliveryError>>;
}

pub fn track_audio_publish(
    file_path: impl Into<String>,
    title: impl Into<String>,
    performer: impl Into<String>,
    duration: i64,
    caption: TrackCaption,
    on_upload_progress: Option<UploadProgressCallback>,
) -> DumpPublish {
    DumpPublish::TrackAudio {
        file_path: file_path.into(),
        title: title.into(),
        performer: performer.into(),
        duration,
        caption,
        on_upload_progress,
    }
}

pub fn zip_document_publish(
    file_path: impl Into<String>,
    thumb_path: Option<String>,
    caption: ZipCaption,
    on_upload_progress: Option<UploadProgressCallback>,
) -> DumpPublish {
    DumpPublish::ZipDocument {
        file_path: file_path.into(),
        thumb_path,
        caption,
        on_upload_progress,
    }
}

pub fn dump_copy_delivery(
    destination: ChatRef,
    source: DumpMessageRef,
    reply_to: Option<ChatMessageRef>,
    silent: bool,
) -> ChatDelivery {
    ChatDelivery::DumpCopy {
        destination,
        source,
        reply_to,
        silent,
    }
}

pub fn zip_chat_delivery(
    destination: ChatRef,
    file_path: impl Into<String>,
    thumb_path: Option<String>,
    caption: ZipCaption,
    on_upload_progress: Option<UploadProgressCallback>,
) -> ChatDelivery {
    ChatDelivery::ZipDocument {
        destination,
        file_path: file_path.into(),
        thumb_path,
        caption,
        on_upload_progress,
    }
}

pub fn photo_delivery(
    destination: ChatRef,
    image_bytes: Vec<u8>,
    caption: AlbumDetailsCaption,
) -> ChatDelivery {
    ChatDelivery::Photo {
        destination,
        image_bytes,
        caption,
    }
}
