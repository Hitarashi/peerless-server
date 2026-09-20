//! Integration tests for the rip orchestrator (offline — every dependency
//! is a fake).

use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
};

use engine::{
    orchestrator::{
        OrchestratorError, RipOrchestrator,
        deps::{
            AlbumCache, AlbumCacheError, AlbumCacheOperation, AlbumReplacementExpectation,
            AlbumReplacementResult, AlbumUpload, ArtworkProvider, CachedAlbum, CachedTrack,
            ChatDelivery, ChatMessageRef, CollectionResolver, Delivery, DeliveryError,
            DeliveryReceipt, DumpMessageRef, DumpPublication, DumpPublish, JobBookkeeping,
            JobBookkeepingError, JobBookkeepingOperation, OrchestratorConfig, ProviderAccess,
            ProviderComposition, ProviderPresentation, RequestLog, SaveTrackInput,
            StorageRetryPolicy, TrackAcquisition, TrackCache, TrackCacheError, TrackCacheOperation,
            UploadProgressCallback,
        },
        types::{
            DownloadLane, JobActivity, JobPhase, OrchestratorEvent, RipJobOptions, RipJobProgress,
            RipJobSummary, UploadLane,
        },
    },
    ripper::RipError,
    settings::{BotSettings, RippingMode, default_settings},
    types::{
        AlbumTracks, ArtistTracks, ParsedTargetItem, Provider, TargetKind, TrackKey, TrackMeta,
        TrackRipResult,
    },
};
use music::{PlaylistData, PlaylistTrack};

// fakes
struct FakePresentation;

impl ProviderPresentation for FakePresentation {
    fn default_job_header(&self) -> &str {
        "Apple Music Lossless Rip"
    }
    fn album_url(&self, album_id: &str, storefront: &str) -> Option<String> {
        (!album_id.is_empty())
            .then(|| format!("https://music.apple.com/{storefront}/album/{album_id}"))
    }

    fn unavailable_track_message(&self) -> &str {
        "Unavailable on Apple Music (not streamable)"
    }

    fn unavailable_track_log_message(&self) -> &str {
        "Track is not streamable in Apple Music catalog, skipping rip"
    }
}

/// What the fake ripper should do for a given track id.
#[derive(Clone)]
enum RipScript {
    Ok,
    /// Rip succeeds and the fake writes a real file (for ZIP staging tests).
    OkWithFile(Vec<u8>),
    /// Rip succeeds with a caller-selected primary codec.
    OkWithCodec(Vec<u8>, &'static str),
    /// Primary succeeds, while the optional Atmos request has no variant.
    AtmosUnavailable(Vec<u8>),
    Fail(RipError),
}

#[derive(Default)]
struct DepsState {
    saved_tracks: Vec<SaveTrackInput>,
    /// When set, `save_track` fails with this message.
    save_track_error: Option<String>,
    save_track_unavailable_attempts: usize,
    cache_unavailable_attempts: usize,
    request_log_error: Option<String>,
    request_logs: Vec<RequestLog>,
    deleted_tracks: Vec<String>,
    deleted_message_batches: Vec<Vec<i64>>,
    /// Send-audio results, one per call.
    send_audio_results: VecDeque<Result<DumpPublication, DeliveryError>>,
    sent_audio: Vec<(String, String, String)>, // (file, title, performer)
    copies: Vec<(i64, i64, Option<i64>, bool)>, // (to, msg, replyTo, silent)
    /// Message ids whose dump copy fails (once).
    copies_fail_ids: Vec<i64>,
    /// Message ids whose dump-file download fails (once).
    download_fail_ids: Vec<i64>,
    rip_calls: Vec<String>,
    // album ZIP
    saved_albums: Vec<AlbumUpload>,
    zip_dump_results: VecDeque<Result<DumpPublication, DeliveryError>>,
    zip_dump_panics: bool,
    zip_dump_calls: usize,
    zip_dump_panic_on_call: Option<usize>,
    zip_chat_results: VecDeque<Result<DeliveryReceipt, DeliveryError>>,
    save_album_error: Option<String>,
    replace_conflict: bool,
    deleted_album_zip_ids: Vec<String>,
    found_albums: HashMap<String, Vec<CachedAlbum>>,
    sent_documents: Vec<String>, // dump + direct document upload paths
    sent_document_captions: Vec<String>,
    uploaded_document_bytes: Vec<u8>, // captured at upload time (workspace is deleted after)
    /// Thumbnail paths passed to document sends (may repeat per part).
    sent_thumbs: Vec<String>,
    sent_photos: Vec<(i64, usize, String)>, // (chat, byte len, caption)
    fetch_artwork_urls: Vec<String>,
    artwork_bytes: Option<Vec<u8>>,
    /// When set, `send_audio_to_dump` waits for this token before returning
    /// — used to hold lane 2 open deterministically (cancel-during-upload
    /// tests).
    gate_uploads: Option<tokio_util::sync::CancellationToken>,
    /// Panic from a track dump upload; used to verify lane-2 settlement and
    /// workspace cleanup.
    send_audio_panics: bool,
    cached_copy_started: bool,
    replacement_committed: bool,
    replacement_gate: Option<tokio_util::sync::CancellationToken>,
    fallback_rip_track: Option<String>,
    fallback_rip_gate: Option<tokio_util::sync::CancellationToken>,
    fallback_rip_started: bool,
    initial_rip_track: Option<String>,
    initial_rip_gate: Option<tokio_util::sync::CancellationToken>,
    initial_rip_started: bool,
    cache_copy_gate: Option<tokio_util::sync::CancellationToken>,
}

impl DepsState {
    fn clear_sink_results(&mut self) {
        self.send_audio_results.clear();
    }
}

struct FakeDeps {
    state: Arc<Mutex<DepsState>>,
    settings: Mutex<BotSettings>,
    cache: Mutex<HashMap<TrackKey, CachedTrack>>,
    rip_scripts: Mutex<HashMap<String, RipScript>>,
    albums: Mutex<HashMap<String, AlbumTracks>>,
    artists: Mutex<HashMap<String, ArtistTracks>>,
    playlists: Mutex<HashMap<String, PlaylistData>>,
    rip_delay_ms: Mutex<u64>,
    cache_delay_ms: Mutex<u64>,
    rip_notify: Arc<tokio::sync::Notify>,
    copy_notify: Arc<tokio::sync::Notify>,
    replacement_notify: Arc<tokio::sync::Notify>,
    sink: FakeSink,
}

impl FakeDeps {
    fn new() -> (Arc<Self>, Arc<Mutex<DepsState>>) {
        let state = Arc::new(Mutex::new(DepsState::default()));
        let rip_notify = Arc::new(tokio::sync::Notify::new());
        let copy_notify = Arc::new(tokio::sync::Notify::new());
        let replacement_notify = Arc::new(tokio::sync::Notify::new());
        let deps = Arc::new(Self {
            state: Arc::clone(&state),
            settings: Mutex::new(default_settings()),
            cache: Mutex::new(HashMap::new()),
            rip_scripts: Mutex::new(HashMap::new()),
            albums: Mutex::new(HashMap::new()),
            artists: Mutex::new(HashMap::new()),
            playlists: Mutex::new(HashMap::new()),
            rip_delay_ms: Mutex::new(0),
            cache_delay_ms: Mutex::new(0),
            rip_notify,
            copy_notify: Arc::clone(&copy_notify),
            replacement_notify: Arc::clone(&replacement_notify),
            sink: FakeSink {
                state: Arc::clone(&state),
                copy_notify,
            },
        });
        (deps, state)
    }

    fn cache_track_with_codec(&self, id: &str, message_id: i64, codec: engine::Codec) {
        let key = TrackKey::apple(id).with_codec(codec);
        let track = CachedTrack {
            track_key: key.clone(),
            codec,
            message_id,
            file_id: format!("file_{id}"),
            file_unique_id: format!("uniq_{id}"),
            title: format!("T{id}"),
            artist: "Cached Artist".into(),
            album: "Cached Album".into(),
        };
        self.cache.lock().unwrap().insert(key, track.clone());
        if codec == engine::Codec::Alac {
            self.cache
                .lock()
                .unwrap()
                .insert(TrackKey::apple(id), track);
        }
    }

    fn cache_track(&self, id: &str, message_id: i64) {
        self.cache_track_with_codec(id, message_id, engine::Codec::Alac);
    }

    fn set_settings(&self, f: impl FnOnce(&mut BotSettings)) {
        f(&mut self.settings.lock().unwrap());
    }

    fn track_meta(id: &str, title: &str, artist: &str) -> TrackMeta {
        TrackMeta {
            id: id.into(),
            title: title.into(),
            artist: artist.into(),
            album: "Album".into(),
            album_artist: artist.into(),
            genre: None,
            release_date: "2021-06-04".into(),
            composer: None,
            track_number: Some(1),
            track_count: Some(10),
            disc_number: None,
            disc_count: None,
            duration_secs: 215,
            explicit: false,
            content_advisory: None,
            artwork_url: String::new(),
            album_id: None,
            artist_id: None,
            isrc: None,
            record_label: None,
            copyright: None,
            upc: None,
            is_streamable: None,
        }
    }

    fn album(tracks: Vec<TrackMeta>) -> AlbumTracks {
        let first = tracks
            .first()
            .cloned()
            .unwrap_or_else(|| Self::track_meta("0", "", ""));
        AlbumTracks {
            album: first,
            tracks,
        }
    }

    fn rip_result(id: &str) -> TrackRipResult {
        TrackRipResult {
            file_path: format!("/tmp/does-not-exist-{id}.m4a"),
            title: "Night Song".into(),
            artist: "A&R <duo>".into(),
            album: "Escapes".into(),
            duration: 215,
            bit_depth: 24,
            sample_rate: 48000,
            codec: "alac".into(),
            genre: "Electronic".into(),
            release_date: "2021-06-04".into(),
            track_number: 2,
            track_count: 10,
            isrc: None,
        }
    }

    fn upload_ok() -> Result<DumpPublication, DeliveryError> {
        Ok(DumpPublication {
            message: DumpMessageRef::new(777),
            file_id: "dump_file".into(),
            file_unique_id: "dump_uniq".into(),
        })
    }
}

struct FakeSink {
    state: Arc<Mutex<DepsState>>,
    copy_notify: Arc<tokio::sync::Notify>,
}

fn archive_codec_replaced(replacement: engine::Codec, existing: engine::Codec) -> bool {
    match replacement {
        engine::Codec::Alac | engine::Codec::Aac => {
            matches!(existing, engine::Codec::Alac | engine::Codec::Aac)
        }
        other => existing == other,
    }
}

impl Delivery for FakeSink {
    fn publish_to_dump<'a>(
        &'a self,
        publication: DumpPublish,
    ) -> engine::orchestrator::deps::BoxFuture<'a, Result<DumpPublication, DeliveryError>> {
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            match publication {
                DumpPublish::TrackAudio {
                    file_path,
                    title,
                    performer,
                    ..
                } => {
                    let gate = state.lock().unwrap().gate_uploads.clone();
                    if let Some(gate) = gate {
                        gate.cancelled().await;
                    }
                    let mut st = state.lock().unwrap();
                    st.sent_audio.push((file_path, title, performer));
                    if st.send_audio_panics {
                        drop(st);
                        panic!("track sink panic");
                    }
                    match st.send_audio_results.pop_front() {
                        Some(result) => result,
                        None => FakeDeps::upload_ok(),
                    }
                }
                DumpPublish::ZipDocument {
                    file_path,
                    thumb_path,
                    caption_html,
                    on_upload_progress,
                } => {
                    let bytes = std::fs::read(&file_path).unwrap_or_default();
                    if let Some(callback) = on_upload_progress {
                        callback(0, bytes.len() as u64);
                        callback(bytes.len() as u64, bytes.len() as u64);
                    }
                    let mut st = state.lock().unwrap();
                    st.sent_documents.push(file_path);
                    st.sent_document_captions.push(caption_html);
                    st.uploaded_document_bytes = bytes;
                    if let Some(thumb_path) = thumb_path {
                        st.sent_thumbs.push(thumb_path);
                    }
                    st.zip_dump_calls += 1;
                    let should_panic =
                        st.zip_dump_panics || st.zip_dump_panic_on_call == Some(st.zip_dump_calls);
                    drop(st);
                    if should_panic {
                        panic!("ZIP sink panic");
                    }
                    let mut st = state.lock().unwrap();
                    match st.zip_dump_results.pop_front() {
                        Some(result) => result,
                        None => Ok(DumpPublication {
                            message: DumpMessageRef::new(900),
                            file_id: "zip_file".into(),
                            file_unique_id: "zip_uniq".into(),
                        }),
                    }
                }
            }
        })
    }

    fn deliver_to_chat<'a>(
        &'a self,
        delivery: ChatDelivery,
    ) -> engine::orchestrator::deps::BoxFuture<'a, Result<DeliveryReceipt, DeliveryError>> {
        let state = Arc::clone(&self.state);
        let copy_notify = Arc::clone(&self.copy_notify);
        Box::pin(async move {
            match delivery {
                ChatDelivery::DumpCopy {
                    destination,
                    source,
                    reply_to,
                    silent,
                } => {
                    let message_id = source.id();
                    let cached_gate = {
                        let st = state.lock().unwrap();
                        if message_id == 4242 {
                            st.cache_copy_gate
                                .clone()
                                .or_else(|| st.gate_uploads.clone())
                        } else {
                            None
                        }
                    };
                    if message_id == 4242 {
                        state.lock().unwrap().cached_copy_started = true;
                        copy_notify.notify_waiters();
                    }
                    if let Some(gate) = cached_gate {
                        gate.cancelled().await;
                    }
                    let mut st = state.lock().unwrap();
                    st.copies.push((
                        destination.id(),
                        message_id,
                        reply_to.map(ChatMessageRef::id),
                        silent,
                    ));
                    drop(st);
                    copy_notify.notify_waiters();
                    let mut st = state.lock().unwrap();
                    if st.copies_fail_ids.contains(&message_id) {
                        st.copies_fail_ids.retain(|id| *id != message_id);
                        return Err(DeliveryError::Unavailable("copy failed".into()));
                    }
                    Ok(DeliveryReceipt::Message(ChatMessageRef::new(message_id)))
                }
                ChatDelivery::ZipDocument {
                    destination,
                    file_path,
                    caption_html,
                    on_upload_progress,
                    ..
                } => {
                    let bytes = std::fs::read(&file_path).unwrap_or_default();
                    if let Some(callback) = on_upload_progress {
                        callback(0, bytes.len() as u64);
                        callback(bytes.len() as u64, bytes.len() as u64);
                    }
                    let mut st = state.lock().unwrap();
                    st.sent_documents.push(file_path);
                    st.sent_document_captions.push(caption_html);
                    st.uploaded_document_bytes = bytes;
                    let result = st
                        .zip_chat_results
                        .pop_front()
                        .unwrap_or_else(|| Ok(DeliveryReceipt::Message(ChatMessageRef::new(200))));
                    let _ = destination;
                    result
                }
                ChatDelivery::Photo {
                    destination,
                    image_bytes,
                    caption_html,
                } => {
                    state.lock().unwrap().sent_photos.push((
                        destination.id(),
                        image_bytes.len(),
                        caption_html,
                    ));
                    Ok(DeliveryReceipt::PreviewDelivered)
                }
            }
        })
    }

    fn materialize_cached<'a>(
        &'a self,
        source: DumpMessageRef,
        destination: &'a std::path::Path,
        progress: Option<&'a UploadProgressCallback>,
    ) -> engine::orchestrator::deps::BoxFuture<'a, Result<(), DeliveryError>> {
        let state = Arc::clone(&self.state);
        let should_fail = state
            .lock()
            .unwrap()
            .download_fail_ids
            .contains(&source.id());
        Box::pin(async move {
            if should_fail {
                return Err(DeliveryError::Unavailable("download failed".into()));
            }
            let bytes = b"cached-audio-bytes";
            std::fs::write(destination, bytes)
                .map_err(|error| DeliveryError::LocalIo(error.to_string()))?;
            if let Some(callback) = progress {
                callback(bytes.len() as u64, bytes.len() as u64);
            }
            Ok(())
        })
    }

    fn retract_dump<'a>(
        &'a self,
        messages: &'a [DumpMessageRef],
    ) -> engine::orchestrator::deps::BoxFuture<'a, Result<(), DeliveryError>> {
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            state
                .lock()
                .unwrap()
                .deleted_message_batches
                .push(messages.iter().map(|message| message.id()).collect());
            Ok(())
        })
    }
}

impl CollectionResolver for FakeDeps {
    async fn fetch_album_tracks(&self, id: &str, storefront: &str) -> Result<AlbumTracks, String> {
        let _ = storefront;
        self.albums
            .lock()
            .unwrap()
            .get(id)
            .cloned()
            .ok_or_else(|| format!("Album {id} not found"))
    }

    async fn fetch_artist_tracks(
        &self,
        id: &str,
        storefront: &str,
    ) -> Result<ArtistTracks, String> {
        let _ = storefront;
        self.artists
            .lock()
            .unwrap()
            .get(id)
            .cloned()
            .ok_or_else(|| format!("Artist {id} not found"))
    }

    async fn fetch_playlist_tracks(
        &self,
        id: &str,
        storefront: &str,
    ) -> Result<PlaylistData, String> {
        let _ = storefront;
        self.playlists
            .lock()
            .unwrap()
            .get(id)
            .cloned()
            .ok_or_else(|| format!("Playlist {id} not found on storefront 'us'"))
    }
}

impl TrackAcquisition for FakeDeps {
    async fn rip(
        &self,
        track_id: &str,
        options: engine::ripper::RipOptions<'_>,
    ) -> Result<TrackRipResult, RipError> {
        let output_dir = options.output_dir;
        self.state
            .lock()
            .unwrap()
            .rip_calls
            .push(track_id.to_string());
        self.rip_notify.notify_waiters();
        let script = self.rip_scripts.lock().unwrap().get(track_id).cloned();
        let id = track_id.to_string();
        let delay_ms = *self.rip_delay_ms.lock().unwrap();
        let fallback_gate = {
            let mut state = self.state.lock().unwrap();
            if state.fallback_rip_track.as_deref() == Some(track_id) {
                state.fallback_rip_started = true;
                state.fallback_rip_gate.clone()
            } else {
                None
            }
        };
        let initial_gate = {
            let mut state = self.state.lock().unwrap();
            if state.initial_rip_track.as_deref() == Some(track_id) {
                state.initial_rip_started = true;
                state.initial_rip_gate.clone()
            } else {
                None
            }
        };
        if fallback_gate.is_some() {
            self.rip_notify.notify_waiters();
        }
        if initial_gate.is_some() {
            self.rip_notify.notify_waiters();
        }
        if delay_ms > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
        }
        if let Some(gate) = fallback_gate {
            gate.cancelled().await;
        }
        if let Some(gate) = initial_gate {
            gate.cancelled().await;
        }
        match script {
            Some(RipScript::Fail(error)) => Err(error.clone()),
            Some(RipScript::AtmosUnavailable(_))
                if options.codec_preference == music::CodecPreference::Atmos =>
            {
                Err(RipError::RenditionUnavailable {
                    reason: "No Dolby Atmos stream variant found".into(),
                })
            }
            Some(RipScript::OkWithCodec(bytes, codec)) => {
                let dir = output_dir
                    .map(std::path::Path::to_path_buf)
                    .unwrap_or_else(|| std::env::temp_dir().join(format!("fake_rip_{id}")));
                let rendition = if options.codec_preference == music::CodecPreference::Atmos {
                    "atmos"
                } else {
                    "primary"
                };
                let path = dir.join(format!("{id}-{rendition}.m4a"));
                std::fs::write(&path, &bytes)
                    .map_err(|error| RipError::Message(error.to_string()))?;
                let mut result = Self::rip_result(&id);
                result.file_path = path.to_string_lossy().into_owned();
                result.codec = if options.codec_preference == music::CodecPreference::Atmos {
                    "ec-3".to_owned()
                } else {
                    codec.to_owned()
                };
                Ok(result)
            }
            Some(RipScript::OkWithFile(bytes)) | Some(RipScript::AtmosUnavailable(bytes)) => {
                let dir = output_dir
                    .map(std::path::Path::to_path_buf)
                    .unwrap_or_else(|| std::env::temp_dir().join(format!("fake_rip_{id}")));
                let rendition = if options.codec_preference == music::CodecPreference::Atmos {
                    "atmos"
                } else {
                    "primary"
                };
                let path = dir.join(format!("{id}-{rendition}.m4a"));
                std::fs::write(&path, &bytes).map_err(|e| RipError::Message(e.to_string()))?;
                let mut result = TrackRipResult {
                    file_path: path.to_string_lossy().into_owned(),
                    ..Self::rip_result(&id)
                };
                if options.codec_preference == music::CodecPreference::Atmos {
                    result.codec = "ec-3".into();
                }
                Ok(result)
            }
            _ => {
                let result = Self::rip_result(&id);
                std::fs::write(&result.file_path, b"fake-ripped-audio")
                    .map_err(|error| RipError::Message(error.to_string()))?;
                Ok(result)
            }
        }
    }
}

impl ArtworkProvider for FakeDeps {
    async fn fetch_artwork(&self, url: &str) -> Option<Vec<u8>> {
        self.state
            .lock()
            .unwrap()
            .fetch_artwork_urls
            .push(url.to_owned());
        self.state.lock().unwrap().artwork_bytes.clone()
    }

    fn artwork_url_at_size(&self, url: &str, size: u16) -> String {
        let _ = size;
        url.to_owned()
    }
}

impl ProviderComposition for FakeDeps {
    type Collections = Self;
    type Acquisition = Self;
    type Artwork = Self;
    type Presentation = FakePresentation;

    fn provider(&self) -> Provider {
        Provider::Apple
    }

    fn collections(&self) -> &Self::Collections {
        self
    }

    fn acquisition(&self) -> &Self::Acquisition {
        self
    }

    fn artwork(&self) -> &Self::Artwork {
        self
    }

    fn presentation(&self) -> &Self::Presentation {
        static PRESENTATION: FakePresentation = FakePresentation;
        &PRESENTATION
    }
}

impl ProviderAccess for FakeDeps {
    type Providers = Self;

    fn providers(&self) -> &Self::Providers {
        self
    }
}

impl TrackCache for FakeDeps {
    fn find_cached_tracks<'a>(
        &'a self,
        keys: &'a [TrackKey],
    ) -> engine::orchestrator::deps::BoxFuture<
        'a,
        Result<HashMap<TrackKey, CachedTrack>, TrackCacheError>,
    > {
        let cache: HashMap<TrackKey, CachedTrack> = self
            .cache
            .lock()
            .unwrap()
            .iter()
            .filter(|(key, _)| keys.contains(key))
            .map(|(id, track)| (id.clone(), track.clone()))
            .collect();
        let delay_ms = *self.cache_delay_ms.lock().unwrap();
        let unavailable = {
            let mut state = self.state.lock().unwrap();
            if state.cache_unavailable_attempts > 0 {
                state.cache_unavailable_attempts -= 1;
                true
            } else {
                false
            }
        };
        Box::pin(async move {
            if delay_ms > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
            }
            if unavailable {
                Err(TrackCacheError::unavailable(
                    TrackCacheOperation::Find,
                    "cache unavailable",
                ))
            } else {
                Ok(cache)
            }
        })
    }

    fn save_track<'a>(
        &'a self,
        input: SaveTrackInput,
    ) -> engine::orchestrator::deps::BoxFuture<'a, Result<(), TrackCacheError>> {
        let (error, unavailable) = {
            let mut state = self.state.lock().unwrap();
            let unavailable = if state.save_track_unavailable_attempts > 0 {
                state.save_track_unavailable_attempts -= 1;
                true
            } else {
                false
            };
            (state.save_track_error.clone(), unavailable)
        };
        self.cache_track_with_codec(&input.track_key.track_id, input.message_id, input.codec);
        self.state.lock().unwrap().saved_tracks.push(input);
        Box::pin(async move {
            if unavailable {
                Err(TrackCacheError::unavailable(
                    TrackCacheOperation::Save,
                    "cache unavailable",
                ))
            } else {
                error.map_or(Ok(()), |detail| {
                    Err(TrackCacheError::failed(TrackCacheOperation::Save, detail))
                })
            }
        })
    }

    fn delete_track<'a>(
        &'a self,
        track_key: &'a TrackKey,
    ) -> engine::orchestrator::deps::BoxFuture<'a, Result<bool, TrackCacheError>> {
        self.state
            .lock()
            .unwrap()
            .deleted_tracks
            .push(track_key.track_id.clone());
        self.cache.lock().unwrap().remove(track_key);
        Box::pin(async { Ok(true) })
    }
}

impl JobBookkeeping for FakeDeps {
    fn settings_snapshot(&self) -> BotSettings {
        self.settings.lock().unwrap().clone()
    }

    fn log_request<'a>(
        &'a self,
        log: RequestLog,
    ) -> engine::orchestrator::deps::BoxFuture<'a, Result<(), JobBookkeepingError>> {
        let error = {
            let mut state = self.state.lock().unwrap();
            state.request_logs.push(log);
            state.request_log_error.clone()
        };
        Box::pin(async move {
            error.map_or(Ok(()), |detail| {
                Err(JobBookkeepingError::failed(
                    JobBookkeepingOperation::LogRequest,
                    detail,
                ))
            })
        })
    }
}

impl AlbumCache for FakeDeps {
    fn save_album<'a>(
        &'a self,
        upload: AlbumUpload,
    ) -> engine::orchestrator::deps::BoxFuture<'a, Result<(), AlbumCacheError>> {
        let mut state = self.state.lock().unwrap();
        state.saved_albums.push(upload);
        let error = state.save_album_error.clone();
        Box::pin(async move {
            error.map_or(Ok(()), |detail| {
                Err(AlbumCacheError::failed(AlbumCacheOperation::Save, detail))
            })
        })
    }

    fn replace_albums<'a>(
        &'a self,
        _: engine::types::Provider,
        album_id: &'a str,
        codec: engine::Codec,
        expected: AlbumReplacementExpectation,
        uploads: Vec<AlbumUpload>,
    ) -> engine::orchestrator::deps::BoxFuture<'a, Result<AlbumReplacementResult, AlbumCacheError>>
    {
        let mut state = self.state.lock().unwrap();
        let error = state.save_album_error.clone();
        let replacement = if state.replace_conflict {
            Err(AlbumCacheError::conflict(
                AlbumCacheOperation::Replace,
                "replacement conflict",
            ))
        } else if let Some(detail) = error {
            Err(AlbumCacheError::failed(
                AlbumCacheOperation::Replace,
                detail,
            ))
        } else {
            let rows = state.found_albums.entry(album_id.to_owned()).or_default();
            let matches_expected = match &expected {
                AlbumReplacementExpectation::Empty => rows
                    .iter()
                    .all(|row| !archive_codec_replaced(codec, row.codec)),
                AlbumReplacementExpectation::Generation(generation) => {
                    let group = rows
                        .iter()
                        .filter(|row| archive_codec_replaced(codec, row.codec))
                        .collect::<Vec<_>>();
                    !group.is_empty() && group.iter().all(|row| row.generation_hash == *generation)
                }
                AlbumReplacementExpectation::Mixed => false,
            };
            if !matches_expected {
                Ok(AlbumReplacementResult::Stale)
            } else {
                let displaced_message_ids = rows
                    .iter()
                    .filter(|row| archive_codec_replaced(codec, row.codec))
                    .map(|row| row.message_id)
                    .collect::<Vec<_>>();
                rows.retain(|row| !archive_codec_replaced(codec, row.codec));
                rows.extend(uploads.iter().map(|upload| CachedAlbum {
                    part_index: upload.part_index,
                    total_parts: upload.total_parts,
                    message_id: upload.message_id,
                    file_unique_id: upload.file_unique_id.clone(),
                    generation_hash: upload.generation_hash.clone(),
                    file_size: upload.file_size,
                    codec: upload.codec,
                }));
                state.deleted_album_zip_ids.push(album_id.to_owned());
                state.saved_albums.extend(uploads.iter().cloned());
                state.replacement_committed = true;
                Ok(AlbumReplacementResult::Committed {
                    displaced_message_ids,
                })
            }
        };
        let replacement_gate = state.replacement_gate.clone();
        let replacement_notify = Arc::clone(&self.replacement_notify);
        drop(state);
        replacement_notify.notify_waiters();
        Box::pin(async move {
            if let Some(gate) = replacement_gate {
                gate.cancelled().await;
            }
            replacement
        })
    }

    fn find_albums<'a>(
        &'a self,
        provider: engine::types::Provider,
        album_id: &'a str,
        codec: Option<engine::Codec>,
    ) -> engine::orchestrator::deps::BoxFuture<'a, Result<Vec<CachedAlbum>, AlbumCacheError>> {
        let _ = provider;
        let rows = self
            .state
            .lock()
            .unwrap()
            .found_albums
            .get(album_id)
            .cloned()
            .unwrap_or_default();
        let rows = rows
            .into_iter()
            .filter(|row| codec.is_none_or(|expected| row.codec == expected))
            .collect();
        Box::pin(async move { Ok(rows) })
    }

    fn delete_albums<'a>(
        &'a self,
        provider: engine::types::Provider,
        album_id: &'a str,
        codec: Option<engine::Codec>,
    ) -> engine::orchestrator::deps::BoxFuture<'a, Result<(), AlbumCacheError>> {
        let _ = (provider, codec);
        self.state
            .lock()
            .unwrap()
            .deleted_album_zip_ids
            .push(album_id.to_owned());
        Box::pin(async { Ok(()) })
    }
}

impl Delivery for FakeDeps {
    fn publish_to_dump<'a>(
        &'a self,
        publication: DumpPublish,
    ) -> engine::orchestrator::deps::BoxFuture<'a, Result<DumpPublication, DeliveryError>> {
        self.sink.publish_to_dump(publication)
    }

    fn deliver_to_chat<'a>(
        &'a self,
        delivery: ChatDelivery,
    ) -> engine::orchestrator::deps::BoxFuture<'a, Result<DeliveryReceipt, DeliveryError>> {
        self.sink.deliver_to_chat(delivery)
    }

    fn materialize_cached<'a>(
        &'a self,
        source: DumpMessageRef,
        destination: &'a std::path::Path,
        progress: Option<&'a UploadProgressCallback>,
    ) -> engine::orchestrator::deps::BoxFuture<'a, Result<(), DeliveryError>> {
        self.sink.materialize_cached(source, destination, progress)
    }

    fn retract_dump<'a>(
        &'a self,
        messages: &'a [DumpMessageRef],
    ) -> engine::orchestrator::deps::BoxFuture<'a, Result<(), DeliveryError>> {
        self.sink.retract_dump(messages)
    }
}

// helpers
#[derive(Clone)]
struct EventLog {
    records: Arc<Mutex<Vec<String>>>,
    progress: Arc<Mutex<Vec<RipJobProgress>>>,
    upload_progress: Arc<Mutex<Vec<UploadLane>>>,
}

impl EventLog {
    fn attach(orch: &RipOrchestrator) -> Self {
        let log = Self {
            records: Arc::new(Mutex::new(Vec::new())),
            progress: Arc::new(Mutex::new(Vec::new())),
            upload_progress: Arc::new(Mutex::new(Vec::new())),
        };
        let records = Arc::clone(&log.records);
        let progress = Arc::clone(&log.progress);
        let upload_progress = Arc::clone(&log.upload_progress);
        orch.subscribe(Arc::new(move |event: &OrchestratorEvent<'_>| {
            let mut v = records.lock().unwrap();
            match event {
                OrchestratorEvent::Created(_) => v.push("created".into()),
                OrchestratorEvent::Started(_) => v.push("started".into()),
                OrchestratorEvent::Completed(_, _) => v.push("completed".into()),
                OrchestratorEvent::Cancelled(_, by) => {
                    v.push(format!("cancelled:{:?}", by));
                }
                OrchestratorEvent::Failed(_, msg) => v.push(format!("failed:{msg}")),
                OrchestratorEvent::Progress(_, p) => {
                    progress.lock().unwrap().push((*p).clone());
                    if let Some(upload) = &p.upload {
                        upload_progress.lock().unwrap().push(upload.clone());
                    }
                }
            }
        }));
        log
    }

    fn snapshot(&self) -> Vec<String> {
        self.records.lock().unwrap().clone()
    }

    fn upload_progress_snapshot(&self) -> Vec<UploadLane> {
        self.upload_progress.lock().unwrap().clone()
    }

    fn progress_snapshot(&self) -> Vec<RipJobProgress> {
        self.progress.lock().unwrap().clone()
    }
}

fn options(items: Vec<ParsedTargetItem>, is_admin: bool) -> RipJobOptions {
    RipJobOptions {
        provider: engine::types::Provider::Apple,
        chat_id: 100,
        user_id: 42,
        user_name: Some("tester".into()),
        delivery_chat_id: 100,
        is_group: false,
        is_force: false,
        is_cache_only: false,
        single_storefront: None,
        parsed_items: items,
        reply_to_message_id: Some(555),
        status_msg_id: 999,
        is_admin,
        codec_preference: None,
        rendition_policy: engine::orchestrator::types::RenditionPolicy::PrimaryOnly,
    }
}

fn track_item(id: &str) -> ParsedTargetItem {
    ParsedTargetItem {
        id: id.into(),
        kind: TargetKind::Track,
        storefront: None,
    }
}

fn album_item(id: &str) -> ParsedTargetItem {
    ParsedTargetItem {
        id: id.into(),
        kind: TargetKind::Album,
        storefront: None,
    }
}

fn playlist_item(id: &str) -> ParsedTargetItem {
    ParsedTargetItem {
        id: id.into(),
        kind: TargetKind::Playlist,
        storefront: None,
    }
}

fn setup_with_upload_retries(
    upload_max_retries: u32,
) -> (
    RipOrchestrator,
    Arc<FakeDeps>,
    Arc<Mutex<DepsState>>,
    EventLog,
) {
    let (deps, state) = FakeDeps::new();
    let orch = RipOrchestrator::new(OrchestratorConfig {
        storage_retry: StorageRetryPolicy::test(),
        upload_retry_base_ms: 0,
        upload_max_retries,
    });
    let events = EventLog::attach(&orch);
    (orch, deps, state, events)
}

fn setup() -> (
    RipOrchestrator,
    Arc<FakeDeps>,
    Arc<Mutex<DepsState>>,
    EventLog,
) {
    setup_with_upload_retries(3)
}

async fn run_async(
    orch: &RipOrchestrator,
    deps: &Arc<FakeDeps>,
    opts: &RipJobOptions,
) -> Result<RipJobSummary, OrchestratorError> {
    orch.start_job(Arc::clone(deps), opts).await
}

// tests
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn happy_path_single_track() {
    let (orch, deps, state, events) = setup();
    deps.cache.lock().unwrap().clear();

    let summary = run_async(&orch, &deps, &options(vec![track_item("1440828878")], true))
        .await
        .expect("job succeeds");

    assert_eq!(summary.total_tracks, 1);
    assert_eq!(summary.ripped_count, 1);
    assert_eq!(summary.failed_count, 0);
    assert_eq!(summary.cached_count, 0);
    assert_eq!(summary.job_header, "Track ID: <code>1440828878</code>");
    assert_eq!(summary.skipped_uncached_tracks.len(), 0);
    assert!(!summary.total_elapsed_sec.is_empty());

    let st = state.lock().unwrap();
    assert_eq!(st.rip_calls, vec!["1440828878".to_string()]);
    assert_eq!(st.saved_tracks.len(), 1);
    assert_eq!(
        st.sent_audio,
        vec![(
            "/tmp/does-not-exist-1440828878.m4a".to_string(),
            "Night Song".to_string(),
            "A&R <duo>".to_string(),
        )],
        "a live miss must upload to the dump before delivering the DM copy"
    );
    let saved = &st.saved_tracks[0];
    assert_eq!(
        saved.track_key,
        TrackKey::apple("1440828878").with_codec(engine::Codec::Alac)
    );
    assert_eq!(saved.codec, engine::Codec::Alac);
    assert_eq!(saved.message_id, 777);
    assert_eq!(saved.title, "Night Song");
    assert_eq!(saved.bit_depth, 24);
    assert_eq!(st.request_logs.len(), 1);
    assert_eq!(st.request_logs[0].status, "completed");
    assert!(!st.request_logs[0].is_cache_hit);
    assert_eq!(
        st.copies,
        vec![(100, 777, Some(555), false)],
        "reply_to passed when delivery==chat; silent=false for single track"
    );
    drop(st);

    let ev = events.snapshot();
    assert_eq!(ev[0], "created");
    assert!(ev.contains(&"started".to_string()));
    assert_eq!(*ev.last().unwrap(), "completed");
    // No cancelled/failed.
    assert!(
        ev.iter()
            .all(|e| !e.starts_with("cancelled") && !e.starts_with("failed"))
    );

    // Job is gone from the map after completion.
    assert!(orch.get_active_jobs().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dual_rendition_single_track_delivers_primary_then_atmos() {
    let (orch, deps, state, _) = setup();
    deps.rip_scripts
        .lock()
        .unwrap()
        .insert("dual.single".into(), RipScript::OkWithFile(vec![1, 2, 3]));
    {
        let mut st = state.lock().unwrap();
        st.send_audio_results.push_back(FakeDeps::upload_ok());
        st.send_audio_results.push_back(FakeDeps::upload_ok());
    }

    let mut opts = options(vec![track_item("dual.single")], false);
    opts.rendition_policy = music::RenditionPolicy::PrimaryWithOptionalAtmos;
    let summary = run_async(&orch, &deps, &opts)
        .await
        .expect("dual single-track job succeeds");

    assert_eq!(summary.total_tracks, 1);
    assert_eq!(
        summary.ripped_count, 1,
        "track progress counts required work"
    );
    assert_eq!(summary.failed_count, 0);
    let st = state.lock().unwrap();
    assert_eq!(st.sent_audio.len(), 2, "both renditions reach the dump");
    assert!(st.sent_audio[0].0.contains("dual.single-primary"));
    assert!(st.sent_audio[1].0.contains("dual.single-atmos"));
    assert_eq!(st.saved_tracks.len(), 2);
    assert_eq!(st.saved_tracks[0].codec, engine::Codec::Alac);
    assert_eq!(st.saved_tracks[1].codec, engine::Codec::Ec3);
    assert_eq!(
        st.copies,
        vec![(100, 777, Some(555), false), (100, 777, Some(555), false)],
        "primary copy precedes the optional Atmos copy"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn missing_atmos_is_silent_and_primary_still_delivers() {
    let (orch, deps, state, events) = setup();
    deps.rip_scripts.lock().unwrap().insert(
        "dual.missing".into(),
        RipScript::AtmosUnavailable(vec![4, 5, 6]),
    );
    state
        .lock()
        .unwrap()
        .send_audio_results
        .push_back(FakeDeps::upload_ok());

    let mut opts = options(vec![track_item("dual.missing")], false);
    opts.rendition_policy = music::RenditionPolicy::PrimaryWithOptionalAtmos;
    let summary = run_async(&orch, &deps, &opts)
        .await
        .expect("missing Atmos does not fail the job");

    assert_eq!(summary.ripped_count, 1);
    assert_eq!(summary.failed_count, 0);
    assert!(summary.failed_tracks.is_empty());
    let st = state.lock().unwrap();
    assert_eq!(st.sent_audio.len(), 1, "unavailable Atmos is not uploaded");
    assert_eq!(st.saved_tracks.len(), 1, "unavailable Atmos is not cached");
    assert_eq!(st.copies.len(), 1, "the primary copy is retained");
    assert!(
        st.request_logs.iter().all(|log| log.status != "failed"),
        "unavailable Atmos must not create a failed request log"
    );
    assert!(
        !events
            .snapshot()
            .iter()
            .any(|event| event.starts_with("failed:"))
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn header_from_rip_metadata_for_single_track() {
    let (orch, deps, _, _) = setup();
    // Track items never carry metadata → header stays `Track {id}`.
    let summary = run_async(
        &orch,
        &deps,
        &options(vec![track_item("123"), track_item("456")], true),
    )
    .await
    .expect("job succeeds");
    // Multi-link: initial header is `Batch (2 links)` — never refined since
    // plain tracks resolve without titles.
    assert_eq!(summary.job_header, "Batch: <b>2 tracks</b>");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn album_resolution_refines_header_and_lists_tracks() {
    let (orch, deps, state, _) = setup();
    let meta = |id: &str, title: &str| FakeDeps::track_meta(id, title, "Nils Frahm");
    deps.albums.lock().unwrap().insert(
        "alb.1".into(),
        FakeDeps::album(vec![meta("t1", "Says"), meta("t2", "Some")]),
    );
    deps.rip_scripts
        .lock()
        .unwrap()
        .insert("t1".into(), RipScript::OkWithFile(vec![1, 2, 3]));
    deps.rip_scripts
        .lock()
        .unwrap()
        .insert("t2".into(), RipScript::OkWithFile(vec![4, 5, 6]));

    let summary = run_async(&orch, &deps, &options(vec![album_item("alb.1")], true))
        .await
        .expect("job succeeds");

    assert_eq!(summary.total_tracks, 2);
    assert_eq!(
        summary.job_header,
        "Album: <a href=\"https://music.apple.com/us/album/alb.1\"><b>Album</b></a> by <b>Nils Frahm</b>"
    );
    let st = state.lock().unwrap();
    assert_eq!(st.rip_calls.len(), 2);
    assert_eq!(st.sent_documents.len(), 1, "the album is archived");
    assert_eq!(st.copies, vec![(100, 900, Some(555), false)]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn all_cached_uses_ordered_pipeline_once() {
    let (orch, deps, state, events) = setup();
    deps.cache_track("1440828878", 4242);
    *deps.cache_delay_ms.lock().unwrap() = 120;

    let summary = run_async(
        &orch,
        &deps,
        &options(vec![track_item("1440828878")], false),
    )
    .await
    .expect("job succeeds");

    assert_eq!(summary.cached_count, 1);
    assert_eq!(summary.ripped_count, 0);
    assert_eq!(summary.total_tracks, 1);
    assert_ne!(summary.total_elapsed_sec, "0.0");
    assert_eq!(summary.failed_tracks.len(), 0);

    let st = state.lock().unwrap();
    assert!(st.rip_calls.is_empty(), "no rip on cache hit");
    assert_eq!(st.copies, vec![(100, 4242, Some(555), false)]);
    assert_eq!(st.request_logs.len(), 1);
    assert!(st.request_logs[0].is_cache_hit);
    assert_eq!(st.request_logs[0].duration_ms, Some(0));
    drop(st);

    let progress = events.progress_snapshot();
    assert!(progress.iter().any(|snapshot| {
        snapshot.completed_tracks == 1
            && snapshot.percent == 100
            && matches!(
                snapshot.job_activity.as_ref(),
                Some(JobActivity::CachedDelivered)
            )
            && snapshot.download.is_none()
    }));
    assert!(progress.iter().any(|snapshot| matches!(
        snapshot.download.as_ref(),
        Some(DownloadLane::CachedDelivery { track })
            if track.title == "T1440828878"
    )));
    let ev = events.snapshot();
    assert_eq!(*ev.last().unwrap(), "completed");
    assert!(
        ev.contains(&"started".to_string()),
        "cache hits use the ordered pipeline"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cache_lookup_unavailability_retries_before_serving_hit() {
    let (orch, deps, state, _) = setup();
    deps.cache_track("cache-retry", 4242);
    state.lock().unwrap().cache_unavailable_attempts = 2;

    let summary = run_async(
        &orch,
        &deps,
        &options(vec![track_item("cache-retry")], false),
    )
    .await
    .expect("cache lookup retries then succeeds");

    assert_eq!(summary.cached_count, 1);
    let st = state.lock().unwrap();
    assert!(
        st.rip_calls.is_empty(),
        "a recovered cache lookup avoids ripping"
    );
    assert_eq!(st.copies, vec![(100, 4242, Some(555), false)]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cache_lookup_failure_stops_before_media_work() {
    let (orch, deps, state, events) = setup();
    state.lock().unwrap().cache_unavailable_attempts = 3;

    let error = run_async(&orch, &deps, &options(vec![track_item("cache-down")], true))
        .await
        .expect_err("cache failure must refuse to start media work");

    assert_eq!(
        error.to_string(),
        "find track unavailable: cache unavailable"
    );
    let st = state.lock().unwrap();
    assert!(st.rip_calls.is_empty());
    assert!(st.sent_audio.is_empty());
    assert!(st.copies.is_empty());
    assert!(st.request_logs.is_empty());
    drop(st);
    assert!(
        events
            .snapshot()
            .iter()
            .any(|event| event == "failed:find track unavailable: cache unavailable")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cache_only_marks_cached_without_delivery() {
    let (orch, deps, state, _) = setup();
    deps.cache_track("1440828878", 4242);

    let mut opts = options(vec![track_item("1440828878")], false);
    opts.is_cache_only = true;

    let summary = run_async(&orch, &deps, &opts).await.expect("job succeeds");

    assert_eq!(summary.cached_count, 1);
    assert!(summary.is_cache_only);
    let st = state.lock().unwrap();
    assert!(st.copies.is_empty(), "cache-only never delivers copies");
    assert!(
        st.request_logs.is_empty(),
        "cache-only does not log requests"
    );
    drop(st);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cache_only_serves_hits_and_seeds_misses() {
    let (orch, deps, state, _) = setup();
    deps.cache_track("hit", 4242);
    let mut opts = options(vec![track_item("hit"), track_item("miss")], false);
    opts.is_cache_only = true;

    let summary = run_async(&orch, &deps, &opts)
        .await
        .expect("cache job succeeds");
    assert_eq!(summary.cached_count, 1);
    assert_eq!(summary.ripped_count, 1);
    assert!(summary.skipped_uncached_tracks.is_empty());
    let st = state.lock().unwrap();
    assert_eq!(st.rip_calls, vec!["miss"]);
    assert_eq!(st.saved_tracks.len(), 1);
    assert!(st.copies.is_empty(), "cache-only never delivers copies");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn maintenance_mode_skips_uncached_tracks() {
    let (orch, deps, _, events) = setup();
    deps.set_settings(|s| s.ripping_mode = RippingMode::Paused);

    let summary = run_async(&orch, &deps, &options(vec![track_item("1")], false))
        .await
        .expect("maintenance mode completes with a skipped miss");
    assert_eq!(summary.skipped_uncached_tracks, vec!["1"]);
    assert!(events.snapshot().contains(&"completed".to_string()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn provider_disabled_skips_uncached_and_adds_warning() {
    let (orch, deps, _, events) = setup();
    deps.set_settings(|s| s.apple_rip_enabled = false);

    let summary = run_async(&orch, &deps, &options(vec![track_item("1")], false))
        .await
        .expect("provider disabled completes with skipped uncached");
    assert_eq!(summary.skipped_uncached_tracks, vec!["1"]);
    assert_eq!(summary.cached_count, 0);
    assert_eq!(summary.ripped_count, 0);
    assert!(
        summary
            .warnings
            .iter()
            .any(|w| w.contains("Apple Music live ripping is currently disabled"))
    );
    assert!(events.snapshot().contains(&"completed".to_string()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn provider_disabled_serves_cached_track() {
    let (orch, deps, _, _) = setup();
    deps.set_settings(|s| s.apple_rip_enabled = false);
    deps.cache_track("1", 99);

    let summary = run_async(&orch, &deps, &options(vec![track_item("1")], false))
        .await
        .expect("cached delivery succeeds when provider live ripping is disabled");
    assert_eq!(summary.cached_count, 1);
    assert_eq!(summary.ripped_count, 0);
    assert_eq!(summary.skipped_uncached_tracks.len(), 0);
    assert_eq!(summary.warnings.len(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn provider_disabled_admin_bypasses() {
    let (orch, deps, _, _) = setup();
    deps.set_settings(|s| s.apple_rip_enabled = false);
    deps.rip_scripts
        .lock()
        .unwrap()
        .insert("1".to_owned(), RipScript::OkWithFile(vec![1, 2, 3]));

    let summary = run_async(&orch, &deps, &options(vec![track_item("1")], true))
        .await
        .expect("admin bypasses provider disabled gate");
    assert_eq!(summary.ripped_count, 1);
    assert_eq!(summary.skipped_uncached_tracks.len(), 0);
    assert_eq!(summary.warnings.len(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn artist_album_ids_resolution() {
    let (_, deps, _, _) = setup();
    let mut t1 = FakeDeps::track_meta("t1", "Track 1", "Artist");
    t1.album_id = Some("alb.1".into());
    let mut t2 = FakeDeps::track_meta("t2", "Track 2", "Artist");
    t2.album_id = Some("alb.1".into());
    let mut t3 = FakeDeps::track_meta("t3", "Track 3", "Artist");
    t3.album_id = Some("alb.2".into());

    deps.artists.lock().unwrap().insert(
        "art.1".into(),
        music::ArtistTracks {
            artist_id: "art.1".into(),
            artist_name: "Artist".into(),
            tracks: vec![t1, t2, t3],
        },
    );

    let album_ids = deps
        .fetch_artist_album_ids("art.1", "us")
        .await
        .expect("artist album ids resolved");
    assert_eq!(album_ids, vec!["alb.1".to_string(), "alb.2".to_string()]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn maintenance_gate_allows_admin_and_cache_only() {
    let (orch, deps, _, _) = setup();
    deps.set_settings(|s| s.ripping_mode = RippingMode::Paused);
    deps.cache_track("1", 9);

    let mut opts = options(vec![track_item("1")], false);
    opts.is_cache_only = true;
    run_async(&orch, &deps, &opts)
        .await
        .expect("cache-only passes the gate");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn non_admin_collection_cap() {
    let (orch, deps, _, _) = setup();
    let meta = |id: &str| FakeDeps::track_meta(id, "Says", "Nils Frahm");
    deps.albums.lock().unwrap().insert(
        "alb.1".into(),
        FakeDeps::album(vec![meta("t1"), meta("t2"), meta("t3")]),
    );
    for id in ["t1", "t2"] {
        deps.rip_scripts
            .lock()
            .unwrap()
            .insert(id.to_owned(), RipScript::OkWithFile(vec![1, 2, 3]));
    }
    deps.set_settings(|s| s.max_collection_tracks = 2);

    let summary = run_async(&orch, &deps, &options(vec![album_item("alb.1")], false))
        .await
        .expect("job succeeds");

    assert_eq!(summary.total_tracks, 2, "only the first 2 are processed");
    assert_eq!(summary.capped_count, 1);
    assert_eq!(summary.max_collection_limit, 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dedup_preserves_first_occurrence_order() {
    let (orch, deps, state, _) = setup();
    let meta = |id: &str| FakeDeps::track_meta(id, "Says", "Nils Frahm");
    deps.albums.lock().unwrap().insert(
        "alb.1".into(),
        FakeDeps::album(vec![meta("t2"), meta("t1"), meta("t2")]),
    );
    for id in ["t1", "t2"] {
        deps.rip_scripts
            .lock()
            .unwrap()
            .insert(id.to_owned(), RipScript::OkWithFile(vec![1, 2, 3]));
    }

    let summary = run_async(&orch, &deps, &options(vec![album_item("alb.1")], true))
        .await
        .expect("job succeeds");

    assert_eq!(summary.total_tracks, 2, "duplicate t2 collapsed");
    assert_eq!(
        state.lock().unwrap().rip_calls,
        vec!["t2".to_string(), "t1".to_string()]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn force_purge_deletes_cached_before_queue() {
    let (orch, deps, state, _) = setup();
    deps.cache_track("1440828878", 4242);

    let mut opts = options(vec![track_item("1440828878")], true);
    opts.is_force = true;

    let summary = run_async(&orch, &deps, &opts).await.expect("job succeeds");

    assert_eq!(summary.cached_count, 0, "purged cache is not served");
    let st = state.lock().unwrap();
    assert_eq!(st.deleted_tracks, vec!["1440828878".to_string()]);
    assert_eq!(
        st.deleted_message_batches,
        vec![vec![4242]],
        "old dump messages deleted in one batch"
    );
    assert_eq!(st.rip_calls, vec!["1440828878".to_string()], "re-ripped");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dual_force_refreshes_primary_and_atmos_independently() {
    let (orch, deps, state, _) = setup();
    deps.cache_track_with_codec("dual.force", 440, engine::Codec::Alac);
    deps.cache_track_with_codec("dual.force", 441, engine::Codec::Ec3);
    deps.rip_scripts
        .lock()
        .unwrap()
        .insert("dual.force".into(), RipScript::OkWithFile(vec![7, 8, 9]));
    {
        let mut st = state.lock().unwrap();
        st.send_audio_results.push_back(FakeDeps::upload_ok());
        st.send_audio_results.push_back(FakeDeps::upload_ok());
    }

    let mut opts = options(vec![track_item("dual.force")], true);
    opts.is_force = true;
    opts.rendition_policy = music::RenditionPolicy::PrimaryWithOptionalAtmos;
    let summary = run_async(&orch, &deps, &opts)
        .await
        .expect("dual force job succeeds");

    assert_eq!(summary.cached_count, 0);
    assert_eq!(summary.ripped_count, 1);
    let st = state.lock().unwrap();
    assert_eq!(st.rip_calls, vec!["dual.force", "dual.force"]);
    assert_eq!(st.deleted_tracks, vec!["dual.force", "dual.force"]);
    assert_eq!(st.deleted_message_batches, vec![vec![440, 441]]);
    assert_eq!(st.saved_tracks.len(), 2);
    assert_eq!(st.saved_tracks[0].codec, engine::Codec::Alac);
    assert_eq!(st.saved_tracks[1].codec, engine::Codec::Ec3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn force_without_admin_keeps_cache() {
    let (orch, deps, state, _) = setup();
    deps.cache_track("1440828878", 4242);

    let mut opts = options(vec![track_item("1440828878")], false);
    opts.is_force = true;

    let summary = run_async(&orch, &deps, &opts).await.expect("job succeeds");

    assert_eq!(summary.cached_count, 1, "non-admin force does not purge");
    let st = state.lock().unwrap();
    assert!(st.deleted_tracks.is_empty());
    assert!(st.deleted_message_batches.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resolution_error_bubbles_with_prefix() {
    let (orch, deps, _, events) = setup();

    let err = run_async(&orch, &deps, &options(vec![album_item("missing")], true))
        .await
        .expect_err("resolution fails");

    assert_eq!(
        err.to_string(),
        "Failed to resolve any tracks: album missing: Album missing not found"
    );
    let ev = events.snapshot();
    assert_eq!(
        *ev.last().unwrap(),
        "failed:Failed to resolve any tracks: album missing: Album missing not found"
    );
    assert!(orch.get_active_jobs().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partial_resolution_continues_successful_tracks() {
    let (orch, deps, state, _) = setup();
    let summary = run_async(
        &orch,
        &deps,
        &options(vec![album_item("missing"), track_item("good")], true),
    )
    .await
    .expect("a resolvable item keeps the job alive");

    assert_eq!(summary.total_tracks, 1);
    assert_eq!(summary.ripped_count, 1);
    assert_eq!(state.lock().unwrap().rip_calls, vec!["good"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn all_resolution_failures_are_structured() {
    let (orch, deps, _, _) = setup();
    let err = run_async(
        &orch,
        &deps,
        &options(vec![album_item("a"), playlist_item("p")], true),
    )
    .await
    .expect_err("nothing resolved");

    match err {
        OrchestratorError::ResolutionFailed { failures } => {
            assert_eq!(failures.len(), 2);
            assert_eq!(failures[0].id, "a");
            assert_eq!(failures[1].id, "p");
        }
        other => panic!("expected structured resolution failure, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admin_can_rip_when_live_mode_is_paused() {
    let (orch, deps, state, _) = setup();
    deps.set_settings(|settings| settings.ripping_mode = RippingMode::Paused);

    let summary = run_async(&orch, &deps, &options(vec![track_item("admin")], true))
        .await
        .expect("admin live override");
    assert_eq!(summary.ripped_count, 1);
    assert_eq!(state.lock().unwrap().rip_calls, vec!["admin"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_resolution_fails() {
    let (orch, deps, _, _) = setup();
    // A playlist that resolves to zero tracks.
    deps.playlists.lock().unwrap().insert(
        "pl.empty".into(),
        PlaylistData {
            id: "pl.empty".into(),
            title: "Empty".into(),
            curator_name: None,
            description: None,
            tracks: vec![],
        },
    );

    let err = run_async(
        &orch,
        &deps,
        &options(vec![playlist_item("pl.empty")], true),
    )
    .await
    .expect_err("no tracks");

    assert_eq!(
        err.to_string(),
        "Failed to resolve any tracks: track : No valid tracks found to process."
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn playlist_resolution_uses_seam() {
    let (orch, deps, _, _) = setup();
    deps.playlists.lock().unwrap().insert(
        "pl.1".into(),
        PlaylistData {
            id: "pl.1".into(),
            title: "Mix".into(),
            curator_name: None,
            description: None,
            tracks: vec![PlaylistTrack {
                id: "pt1".into(),
                title: "Track One".into(),
                artist: "Artist".into(),
                duration: Some(122),
            }],
        },
    );

    let summary = run_async(&orch, &deps, &options(vec![playlist_item("pl.1")], true))
        .await
        .expect("job succeeds");
    assert_eq!(summary.total_tracks, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rip_failure_logs_and_continues() {
    let (orch, deps, state, _) = setup();
    deps.rip_scripts.lock().unwrap().insert(
        "bad".into(),
        RipScript::Fail(RipError::Message("CDN error".into())),
    );
    deps.rip_scripts
        .lock()
        .unwrap()
        .insert("good".into(), RipScript::Ok);

    let summary = run_async(
        &orch,
        &deps,
        &options(vec![track_item("bad"), track_item("good")], true),
    )
    .await
    .expect("job completes despite one failure");

    assert_eq!(summary.failed_tracks.len(), 1);
    assert_eq!(summary.failed_tracks[0].id, "bad");
    assert_eq!(summary.failed_tracks[0].error, "CDN error");
    assert_eq!(summary.ripped_count, 1);
    let st = state.lock().unwrap();
    let failed_logs: Vec<_> = st
        .request_logs
        .iter()
        .filter(|l| l.status == "failed")
        .collect();
    assert_eq!(failed_logs.len(), 1);
    assert_eq!(failed_logs[0].track_key, TrackKey::apple("bad"));
    assert_eq!(failed_logs[0].error_reason.as_deref(), Some("CDN error"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn source_offline_skips_track_and_continues_batch() {
    let (orch, deps, state, events) = setup();
    for id in ["t1", "t2", "t3"] {
        deps.rip_scripts.lock().unwrap().insert(
            id.into(),
            RipScript::Fail(RipError::SourceOffline {
                source: engine::streaming::SourceId::PrimaryMirror,
            }),
        );
    }

    let summary = run_async(
        &orch,
        &deps,
        &options(
            vec![track_item("t1"), track_item("t2"), track_item("t3")],
            true,
        ),
    )
    .await
    .expect("job completes with per-track failures");

    // SourceOffline marks each track failed and the batch continues.
    let st = state.lock().unwrap();
    assert_eq!(
        st.rip_calls,
        vec!["t1".to_string(), "t2".to_string(), "t3".to_string()]
    );
    drop(st);
    assert_eq!(summary.failed_tracks.len(), 3);
    assert_eq!(summary.failed_tracks[0].id, "t1");
    assert_eq!(summary.failed_tracks[1].id, "t2");
    assert_eq!(summary.failed_tracks[2].id, "t3");
    let _ = events;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn generic_timeout_and_status_errors_do_not_break_batch() {
    let (orch, deps, state, _) = setup();
    for id in ["t1", "t2", "t3"] {
        deps.rip_scripts.lock().unwrap().insert(
            id.into(),
            RipScript::Fail(RipError::Message("request timed out with HTTP 503".into())),
        );
    }

    let summary = run_async(
        &orch,
        &deps,
        &options(
            vec![track_item("t1"), track_item("t2"), track_item("t3")],
            true,
        ),
    )
    .await
    .expect("generic errors are ordinary track failures");
    assert_eq!(state.lock().unwrap().rip_calls.len(), 3);
    assert_eq!(summary.failed_tracks.len(), 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upload_retry_then_success() {
    let (orch, deps, state, _) = setup();
    {
        let mut st = state.lock().unwrap();
        st.clear_sink_results();
        st.send_audio_results
            .push_back(Err(DeliveryError::Transient("flood".into())));
        st.send_audio_results.push_back(FakeDeps::upload_ok());
    }

    let summary = run_async(&orch, &deps, &options(vec![track_item("t1")], true))
        .await
        .expect("second attempt succeeds");

    assert_eq!(summary.ripped_count, 1);
    assert_eq!(summary.failed_count, 0);
    let st = state.lock().unwrap();
    assert_eq!(st.sent_audio.len(), 2, "retried exactly once");
    assert_eq!(st.saved_tracks.len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unavailable_delivery_is_not_retried() {
    let (orch, deps, state, _) = setup_with_upload_retries(3);
    {
        let mut st = state.lock().unwrap();
        st.clear_sink_results();
        st.send_audio_results
            .push_back(Err(DeliveryError::Unavailable("dump offline".into())));
        st.send_audio_results.push_back(FakeDeps::upload_ok());
    }

    let summary = run_async(
        &orch,
        &deps,
        &options(vec![track_item("delivery-down")], true),
    )
    .await
    .expect("non-transient delivery failure is recorded");

    assert_eq!(summary.failed_tracks.len(), 1);
    assert_eq!(
        summary.failed_tracks[0].error,
        "delivery unavailable: dump offline"
    );
    let st = state.lock().unwrap();
    assert_eq!(
        st.sent_audio.len(),
        1,
        "unavailable delivery is not retried"
    );
    assert!(st.saved_tracks.is_empty());
    assert!(st.copies.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn configured_upload_retry_count_controls_calls() {
    let (orch, deps, state, _) = setup_with_upload_retries(1);
    {
        let mut st = state.lock().unwrap();
        st.clear_sink_results();
        st.send_audio_results
            .push_back(Err(DeliveryError::Transient("flood".into())));
        st.send_audio_results
            .push_back(Err(DeliveryError::Transient("flood".into())));
    }

    let summary = run_async(&orch, &deps, &options(vec![track_item("t1")], true))
        .await
        .expect("exhaustion is recorded, not propagated");
    assert_eq!(summary.failed_tracks.len(), 1);
    assert_eq!(state.lock().unwrap().sent_audio.len(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exhausted_upload_continues_next_track() {
    let (orch, deps, state, _) = setup_with_upload_retries(0);
    {
        let mut st = state.lock().unwrap();
        st.clear_sink_results();
        st.send_audio_results
            .push_back(Err(DeliveryError::Unavailable("first failed".into())));
        st.send_audio_results.push_back(FakeDeps::upload_ok());
    }

    let summary = run_async(
        &orch,
        &deps,
        &options(vec![track_item("bad"), track_item("good")], true),
    )
    .await
    .expect("later tracks continue after exhaustion");
    assert_eq!(summary.failed_tracks.len(), 1);
    assert_eq!(summary.ripped_count, 1);
    assert_eq!(state.lock().unwrap().saved_tracks.len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upload_retries_exhausted_records_failure() {
    let (orch, deps, state, _) = setup();
    {
        let mut st = state.lock().unwrap();
        st.clear_sink_results();
        for _ in 0..4 {
            st.send_audio_results
                .push_back(Err(DeliveryError::Transient("flood".into())));
        }
    }

    let summary = run_async(&orch, &deps, &options(vec![track_item("t1")], true))
        .await
        .expect("deviation: job survives exhausted uploads");

    assert_eq!(summary.ripped_count, 0);
    assert_eq!(summary.failed_tracks.len(), 1);
    assert_eq!(summary.failed_tracks[0].id, "t1");
    assert_eq!(
        summary.failed_tracks[0].error,
        "transient delivery failure: flood"
    );
    let st = state.lock().unwrap();
    assert_eq!(st.sent_audio.len(), 4, "max_retries=4 attempts");
    // No request log is written for an upload-exhausted track.
    assert!(st.request_logs.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn panicking_track_upload_fails_once_and_cleans_workspace() {
    let (orch, deps, state, events) = setup();
    deps.rip_scripts
        .lock()
        .unwrap()
        .insert("panic.track".into(), RipScript::OkWithFile(vec![1, 2, 3]));
    state.lock().unwrap().send_audio_panics = true;

    let error = run_async(
        &orch,
        &deps,
        &options(vec![track_item("panic.track")], true),
    )
    .await
    .expect_err("a track upload panic must fail rather than complete");
    assert_eq!(
        error.to_string(),
        "lane-2 task panicked for panic.track: track sink panic"
    );

    let st = state.lock().unwrap();
    let uploaded_path = st
        .sent_audio
        .first()
        .map(|(path, _, _)| path.clone())
        .expect("the panicking upload was attempted");
    assert!(
        !std::path::Path::new(&uploaded_path).exists(),
        "the marker cleans the ripped file after a lane-2 panic"
    );
    drop(st);

    let terminal_events = events
        .snapshot()
        .into_iter()
        .filter(|event| event == "completed" || event.starts_with("failed:"))
        .collect::<Vec<_>>();
    assert_eq!(terminal_events.len(), 1);
    assert_eq!(
        terminal_events[0],
        "failed:lane-2 task panicked for panic.track: track sink panic"
    );
    assert!(orch.get_active_jobs().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn non_audio_media_records_no_log() {
    let (orch, deps, state, _) = setup();
    {
        let mut st = state.lock().unwrap();
        st.clear_sink_results();
        st.send_audio_results
            .push_back(Err(DeliveryError::UnexpectedMedia)); // not audio
    }

    let summary = run_async(&orch, &deps, &options(vec![track_item("t1")], true))
        .await
        .expect("job continues");

    assert_eq!(summary.failed_tracks.len(), 1);
    assert_eq!(
        summary.failed_tracks[0].error,
        "delivery returned unexpected media"
    );
    let st = state.lock().unwrap();
    assert!(st.request_logs.is_empty(), "no request log for non-audio");
    assert!(st.saved_tracks.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn post_upload_save_failure_records_and_logs() {
    let (orch, deps, state, _) = setup();
    state.lock().unwrap().save_track_error = Some("db down".into());

    let summary = run_async(&orch, &deps, &options(vec![track_item("t1")], true))
        .await
        .expect("job completes (failure recorded, not thrown)");

    assert_eq!(summary.ripped_count, 0);
    assert_eq!(summary.failed_tracks.len(), 1);
    assert_eq!(summary.failed_tracks[0].id, "t1");
    assert_eq!(
        summary.failed_tracks[0].error,
        "track cache persistence failed: save track failed: db down"
    );
    let st = state.lock().unwrap();
    // The post-upload catch logs a failed request (save → copy → log share
    // the try/catch: the copy never happens because save failed first).
    assert_eq!(st.request_logs.len(), 1);
    assert_eq!(st.request_logs[0].status, "failed");
    assert_eq!(
        st.request_logs[0].error_reason.as_deref(),
        Some("track cache persistence failed: save track failed: db down")
    );
    assert!(st.copies.is_empty(), "copy skipped after save failure");
    assert_eq!(
        st.deleted_message_batches,
        vec![vec![777]],
        "a failed cache write retracts only the new dump publication"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unavailable_cache_write_retries_before_non_transient_failure() {
    let (orch, deps, state, _) = setup();
    {
        let mut st = state.lock().unwrap();
        st.save_track_unavailable_attempts = 2;
        st.save_track_error = Some("db row rejected".into());
    }

    let summary = run_async(&orch, &deps, &options(vec![track_item("save-retry")], true))
        .await
        .expect("cache-write failure is recorded without failing the job");

    assert_eq!(summary.failed_tracks.len(), 1);
    assert_eq!(
        summary.failed_tracks[0].error,
        "track cache persistence failed: save track failed: db row rejected"
    );
    let st = state.lock().unwrap();
    assert_eq!(
        st.saved_tracks.len(),
        3,
        "two unavailable attempts plus one final failure"
    );
    assert_eq!(st.deleted_message_batches, vec![vec![777]]);
    assert!(st.copies.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn request_log_failure_is_best_effort() {
    let (orch, deps, state, _) = setup();
    state.lock().unwrap().request_log_error = Some("audit store unavailable".into());

    let summary = run_async(&orch, &deps, &options(vec![track_item("log-down")], true))
        .await
        .expect("request-log failure must not fail a delivered track");

    assert_eq!(summary.ripped_count, 1);
    assert!(summary.failed_tracks.is_empty());
    let st = state.lock().unwrap();
    assert_eq!(st.copies, vec![(100, 777, Some(555), false)]);
    assert_eq!(st.request_logs.len(), 1);
    assert_eq!(st.request_logs[0].status, "completed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cache_copy_failure_marks_re_rip() {
    let (orch, deps, state, _) = setup();
    deps.cache_track("t1", 4242);
    // Make the cache-copy fail for message 4242 (and later copies succeed).
    {
        let mut st = state.lock().unwrap();
        st.clear_sink_results();
        // send_audio isn't used on the cache path; copies fail via a
        // dedicated flag instead.
        st.copies_fail_ids.push(4242);
    }

    let summary = run_async(&orch, &deps, &options(vec![track_item("t1")], false))
        .await
        .expect("job succeeds");
    assert_eq!(summary.ripped_count, 1);
    let st = state.lock().unwrap();
    assert_eq!(st.rip_calls, vec!["t1".to_string()]);
    assert_eq!(st.deleted_tracks, vec!["t1".to_string()]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_first_cache_slot_preserves_order_while_other_job_uploads() {
    let (orch, deps, state, _) = setup();
    deps.cache_track("ordered-cache", 4242);
    deps.rip_scripts
        .lock()
        .unwrap()
        .insert("ordered-cache".into(), RipScript::OkWithFile(vec![1, 2]));
    deps.rip_scripts
        .lock()
        .unwrap()
        .insert("ordered-after".into(), RipScript::OkWithFile(vec![3, 4]));
    deps.rip_scripts
        .lock()
        .unwrap()
        .insert("other-job".into(), RipScript::OkWithFile(vec![5, 6]));

    let fallback_gate = tokio_util::sync::CancellationToken::new();
    let initial_gate = tokio_util::sync::CancellationToken::new();
    let cache_copy_gate = tokio_util::sync::CancellationToken::new();
    {
        let mut st = state.lock().unwrap();
        st.copies_fail_ids.push(4242);
        st.fallback_rip_track = Some("ordered-cache".into());
        st.fallback_rip_gate = Some(fallback_gate.clone());
        st.initial_rip_track = Some("ordered-after".into());
        st.initial_rip_gate = Some(initial_gate.clone());
        st.cache_copy_gate = Some(cache_copy_gate.clone());
        st.clear_sink_results();
        for message_id in [880, 881, 882] {
            st.send_audio_results.push_back(Ok(DumpPublication {
                message: DumpMessageRef::new(message_id),
                file_id: format!("file-{message_id}"),
                file_unique_id: format!("unique-{message_id}"),
            }));
        }
    }

    let orch = Arc::new(orch);
    let first_orch = Arc::clone(&orch);
    let first_deps = Arc::clone(&deps);
    let first = tokio::spawn(async move {
        run_async(
            &first_orch,
            &first_deps,
            &options(
                vec![track_item("ordered-cache"), track_item("ordered-after")],
                true,
            ),
        )
        .await
    });

    // The first job is still ripping its later item while the cache copy is
    // held. Queue the independent job before the ordered fallback can claim
    // the single rip worker.
    loop {
        let notified = deps.rip_notify.notified();
        if state
            .lock()
            .unwrap()
            .rip_calls
            .iter()
            .any(|track_id| track_id == "ordered-after")
        {
            break;
        }
        notified.await;
    }
    let second_orch = Arc::clone(&orch);
    let second_deps = Arc::clone(&deps);
    let second = tokio::spawn(async move {
        run_async(
            &second_orch,
            &second_deps,
            &options(vec![track_item("other-job")], true),
        )
        .await
    });

    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            if orch
                .get_active_jobs()
                .iter()
                .any(|job| job.phase == JobPhase::Queued && job.queue_position == Some(1))
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("independent job is admitted before the initial rip releases");
    loop {
        let notified = deps.copy_notify.notified();
        if state.lock().unwrap().cached_copy_started {
            break;
        }
        notified.await;
    }
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            if orch
                .get_active_jobs()
                .iter()
                .any(|job| job.phase == JobPhase::Queued && job.queue_position == Some(1))
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("independent job is queued before the fallback is released");
    cache_copy_gate.cancel();
    initial_gate.cancel();

    loop {
        let notified = deps.rip_notify.notified();
        if state.lock().unwrap().fallback_rip_started {
            break;
        }
        notified.await;
    }
    loop {
        let notified = deps.copy_notify.notified();
        if state
            .lock()
            .unwrap()
            .copies
            .iter()
            .any(|(_, message_id, _, _)| *message_id == 880)
        {
            break;
        }
        notified.await;
    }

    let before_fallback = state.lock().unwrap().copies.clone();
    assert_eq!(
        before_fallback
            .iter()
            .map(|(_, message_id, _, _)| *message_id)
            .collect::<Vec<_>>(),
        vec![4242, 880],
        "the independent job uploads while this job's fallback rip is gated"
    );

    fallback_gate.cancel();
    first.await.unwrap().expect("ordered job succeeds");
    second.await.unwrap().expect("independent job succeeds");
    let copies = state
        .lock()
        .unwrap()
        .copies
        .iter()
        .map(|(_, message_id, _, _)| *message_id)
        .collect::<Vec<_>>();
    assert_eq!(copies, vec![4242, 880, 881, 882]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cache_failure_hands_rerip_to_active_lane_without_deadlock() {
    let (orch, deps, state, _) = setup();
    let first_items = (0..16)
        .map(|index| track_item(&format!("lane-fill-{index}")))
        .collect::<Vec<_>>();
    for index in 0..16 {
        deps.rip_scripts.lock().unwrap().insert(
            format!("lane-fill-{index}"),
            RipScript::OkWithFile(vec![index as u8]),
        );
    }
    deps.cache_track("cached-fallback", 4242);
    deps.rip_scripts
        .lock()
        .unwrap()
        .insert("cached-fallback".into(), RipScript::OkWithFile(vec![1, 2]));
    deps.rip_scripts
        .lock()
        .unwrap()
        .insert("after-fallback".into(), RipScript::OkWithFile(vec![3, 4]));
    let gate = tokio_util::sync::CancellationToken::new();
    {
        let mut st = state.lock().unwrap();
        st.gate_uploads = Some(gate.clone());
        st.copies_fail_ids.push(4242);
    }

    let orch = Arc::new(orch);
    let first_orch = Arc::clone(&orch);
    let first_deps = Arc::clone(&deps);
    let first = tokio::spawn(async move {
        run_async(&first_orch, &first_deps, &options(first_items, true)).await
    });

    loop {
        let notified = deps.rip_notify.notified();
        if state.lock().unwrap().rip_calls.len() == 16 {
            break;
        }
        notified.await;
    }

    let second_orch = Arc::clone(&orch);
    let second_deps = Arc::clone(&deps);
    let second_items = vec![track_item("cached-fallback"), track_item("after-fallback")];
    let second = tokio::spawn(async move {
        run_async(&second_orch, &second_deps, &options(second_items, true)).await
    });
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    assert!(
        !second.is_finished(),
        "the gated lane must still be backpressured"
    );

    gate.cancel();
    let result = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        let first_result = first.await.unwrap().expect("lane-fill job succeeds");
        let second_result = second.await.unwrap().expect("fallback job succeeds");
        (first_result, second_result)
    })
    .await
    .expect("cache fallback must not deadlock behind the active lane-1 job");

    assert_eq!(result.1.ripped_count, 2);
    let st = state.lock().unwrap();
    assert_eq!(
        st.rip_calls
            .iter()
            .filter(|track_id| track_id.as_str() == "cached-fallback")
            .count(),
        1,
        "the failed cache hit is reripped exactly once"
    );
    assert!(
        st.rip_calls
            .iter()
            .any(|track_id| track_id == "after-fallback")
    );
    assert_eq!(st.deleted_tracks, vec!["cached-fallback".to_owned()]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn early_cached_rerip_drains_seventeen_later_items_in_order() {
    let (orch, deps, state, _) = setup();
    let cached_id = "cached-first";
    deps.cache_track(cached_id, 4242);
    deps.rip_scripts
        .lock()
        .unwrap()
        .insert(cached_id.to_owned(), RipScript::OkWithFile(vec![0, 1]));

    let later_ids = (0..17)
        .map(|index| format!("fresh-{index:02}"))
        .collect::<Vec<_>>();
    for (index, track_id) in later_ids.iter().enumerate() {
        deps.rip_scripts.lock().unwrap().insert(
            track_id.clone(),
            RipScript::OkWithFile(vec![index as u8, index as u8 + 1]),
        );
    }

    let fallback_gate = tokio_util::sync::CancellationToken::new();
    {
        let mut st = state.lock().unwrap();
        st.copies_fail_ids.push(4242);
        st.fallback_rip_track = Some(cached_id.to_owned());
        st.fallback_rip_gate = Some(fallback_gate.clone());
        for message_id in 1000..1018 {
            st.send_audio_results.push_back(Ok(DumpPublication {
                message: DumpMessageRef::new(message_id),
                file_id: format!("file-{message_id}"),
                file_unique_id: format!("unique-{message_id}"),
            }));
        }
    }

    let mut requested_items = vec![track_item(cached_id)];
    requested_items.extend(later_ids.iter().map(|track_id| track_item(track_id)));
    let task = tokio::spawn({
        let deps = Arc::clone(&deps);
        async move { run_async(&orch, &deps, &options(requested_items, true)).await }
    });

    let fallback_started = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let notified = deps.rip_notify.notified();
            if state.lock().unwrap().fallback_rip_started {
                break;
            }
            notified.await;
        }
    })
    .await;
    assert!(
        fallback_started.is_ok(),
        "the fallback must start after lane 1 drains all later completions"
    );
    assert_eq!(
        state
            .lock()
            .unwrap()
            .copies
            .iter()
            .map(|copy| copy.1)
            .collect::<Vec<_>>(),
        vec![4242],
        "later delivery remains held behind the missing ordinal"
    );

    fallback_gate.cancel();
    let summary = tokio::time::timeout(std::time::Duration::from_secs(2), task)
        .await
        .expect("ordered fallback job must not deadlock")
        .unwrap()
        .expect("ordered fallback job succeeds");
    assert_eq!(summary.ripped_count, 18);

    let delivered = state
        .lock()
        .unwrap()
        .copies
        .iter()
        .skip(1)
        .map(|copy| copy.1)
        .collect::<Vec<_>>();
    assert_eq!(delivered, (1000..1018).collect::<Vec<_>>());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_cached_wait_drops_gated_lane_two_send() {
    let (orch, deps, state, events) = setup();
    deps.cache_track("cancelled-cache", 4242);
    let gate = tokio_util::sync::CancellationToken::new();
    state.lock().unwrap().gate_uploads = Some(gate.clone());

    let orch = Arc::new(orch);
    let run_orch = Arc::clone(&orch);
    let run_deps = Arc::clone(&deps);
    let task = tokio::spawn(async move {
        run_async(
            &run_orch,
            &run_deps,
            &options(vec![track_item("cancelled-cache")], false),
        )
        .await
    });
    loop {
        let notified = deps.copy_notify.notified();
        if state.lock().unwrap().cached_copy_started {
            break;
        }
        notified.await;
    }
    let job = orch
        .get_active_jobs()
        .into_iter()
        .next()
        .expect("cached job remains active while the send is gated");
    assert!(orch.cancel_job(&job.id, Some("tester")));

    let result = tokio::time::timeout(std::time::Duration::from_secs(2), task)
        .await
        .expect("cancellation must release the cached wait")
        .unwrap()
        .expect("cancelled job still returns its summary");
    assert_eq!(result.ripped_count, 0);
    assert!(state.lock().unwrap().copies.is_empty());
    assert!(
        events
            .snapshot()
            .iter()
            .any(|event| event.starts_with("cancelled:"))
    );
    assert!(
        !gate.is_cancelled(),
        "the fake gate was not needed to settle"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gated_cached_work_does_not_hold_another_jobs_lane_one() {
    let (orch, deps, state, _) = setup();
    deps.cache_track("cached", 4242);
    let gate = tokio_util::sync::CancellationToken::new();
    state.lock().unwrap().gate_uploads = Some(gate.clone());

    let orch = Arc::new(orch);
    let cached_orch = Arc::clone(&orch);
    let cached_deps = Arc::clone(&deps);
    let cached_options = options(vec![track_item("cached")], true);
    let cached_job =
        tokio::spawn(async move { run_async(&cached_orch, &cached_deps, &cached_options).await });

    loop {
        let notified = deps.copy_notify.notified();
        if state.lock().unwrap().cached_copy_started {
            break;
        }
        notified.await;
    }

    let fresh_orch = Arc::clone(&orch);
    let fresh_deps = Arc::clone(&deps);
    let fresh_options = options(vec![track_item("fresh")], true);
    let fresh_job =
        tokio::spawn(async move { run_async(&fresh_orch, &fresh_deps, &fresh_options).await });

    loop {
        let notified = deps.rip_notify.notified();
        if state.lock().unwrap().rip_calls == vec!["fresh".to_owned()] {
            break;
        }
        notified.await;
    }
    assert!(!fresh_job.is_finished(), "fresh job still awaits lane two");
    assert_eq!(orch.get_active_jobs().len(), 2);

    gate.cancel();
    cached_job
        .await
        .unwrap()
        .expect("cached job completes after gate release");
    fresh_job
        .await
        .unwrap()
        .expect("fresh job completes after gate release");
    let copies: Vec<i64> = state
        .lock()
        .unwrap()
        .copies
        .iter()
        .map(|copy| copy.1)
        .collect();
    assert!(
        copies == vec![4242, 777] || copies == vec![777, 4242],
        "both cached and fresh deliveries succeed concurrently: {copies:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancel_job_semantics() {
    let (orch, deps, _, _) = setup();

    assert!(!orch.cancel_job("missing", None), "unknown id → false");

    // Run a quick job to completion.
    run_async(&orch, &deps, &options(vec![track_item("t1")], true))
        .await
        .expect("job succeeds");
    // The job map is emptied after completion — nothing to cancel.
    assert!(!orch.cancel_job("whatever", None));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queue_position_field_defaults_none() {
    let (orch, deps, _, _) = setup();
    run_async(&orch, &deps, &options(vec![track_item("t1")], true))
        .await
        .expect("job succeeds");
    // The command handler sets queue_position; the job flow never does,
    // and the job map is empty after completion.
    assert!(orch.get_active_jobs().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queued_position_and_pending_cancel_are_terminally_safe() {
    let (deps, _) = FakeDeps::new();
    *deps.rip_delay_ms.lock().unwrap() = 100;
    let orch = Arc::new(RipOrchestrator::new(OrchestratorConfig::test()));
    let terminal_events = Arc::new(Mutex::new(Vec::<(String, &'static str)>::new()));
    let phase_snapshots = Arc::new(Mutex::new(Vec::<(JobPhase, Option<u64>)>::new()));
    let terminals = Arc::clone(&terminal_events);
    let phases = Arc::clone(&phase_snapshots);
    orch.subscribe(Arc::new(move |event: &OrchestratorEvent<'_>| {
        let job = match event {
            OrchestratorEvent::Created(job)
            | OrchestratorEvent::Started(job)
            | OrchestratorEvent::Progress(job, _) => job,
            OrchestratorEvent::Completed(job, _) => job,
            OrchestratorEvent::Cancelled(job, _) => job,
            OrchestratorEvent::Failed(job, _) => job,
        };
        match event {
            OrchestratorEvent::Started(_) => {
                phases.lock().unwrap().push((job.phase, job.queue_position))
            }
            OrchestratorEvent::Completed(_, _) => terminals
                .lock()
                .unwrap()
                .push((job.id.clone(), "completed")),
            OrchestratorEvent::Cancelled(_, _) => terminals
                .lock()
                .unwrap()
                .push((job.id.clone(), "cancelled")),
            OrchestratorEvent::Failed(_, _) => {
                terminals.lock().unwrap().push((job.id.clone(), "failed"))
            }
            _ => {}
        }
    }));

    let first_orch = Arc::clone(&orch);
    let first_deps = Arc::clone(&deps);
    let first_options = options(vec![track_item("first")], true);
    let first = tokio::spawn(async move { first_orch.start_job(first_deps, &first_options).await });
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;

    let second_orch = Arc::clone(&orch);
    let second_deps = Arc::clone(&deps);
    let second_options = options(vec![track_item("second")], true);
    let second =
        tokio::spawn(async move { second_orch.start_job(second_deps, &second_options).await });
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;

    let queued = orch
        .get_active_jobs()
        .into_iter()
        .find(|job| job.phase == JobPhase::Queued)
        .expect("second job is queued");
    assert_eq!(queued.queue_position, Some(1));
    assert!(orch.cancel_job(&queued.id, Some("tester")));

    let second_result = second.await.unwrap();
    assert!(second_result.is_err());
    first.await.unwrap().expect("first job completes");

    let terminal_events = terminal_events.lock().unwrap();
    let second_terminals: Vec<_> = terminal_events
        .iter()
        .filter(|(id, _)| id == &queued.id)
        .collect();
    assert_eq!(second_terminals.len(), 1);
    assert_eq!(second_terminals[0].1, "cancelled");
    assert!(
        phase_snapshots
            .lock()
            .unwrap()
            .iter()
            .any(|(phase, position)| *phase == JobPhase::Processing && *position == Some(0))
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn active_cancel_emits_only_cancelled_terminal_event() {
    let (orch, deps, _, events) = setup();
    *deps.rip_delay_ms.lock().unwrap() = 100;
    let orch = Arc::new(orch);
    let run_orch = Arc::clone(&orch);
    let run_deps = Arc::clone(&deps);
    let run_options = options(vec![track_item("cancel")], true);
    let task = tokio::spawn(async move { run_orch.start_job(run_deps, &run_options).await });
    tokio::time::sleep(std::time::Duration::from_millis(15)).await;
    let job = orch
        .get_active_jobs()
        .into_iter()
        .next()
        .expect("active job");
    assert!(orch.cancel_job(&job.id, Some("tester")));
    let _ = task.await.unwrap();
    let terminal_count = events
        .snapshot()
        .iter()
        .filter(|event| {
            event.starts_with("completed")
                || event.starts_with("cancelled")
                || event.starts_with("failed")
        })
        .count();
    assert_eq!(terminal_count, 1);
    assert!(
        events
            .snapshot()
            .iter()
            .any(|event| event.starts_with("cancelled"))
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn progress_percent_math() {
    let (orch, deps, _, events) = setup();
    let meta = |id: &str| FakeDeps::track_meta(id, "Says", "Nils Frahm");
    deps.albums.lock().unwrap().insert(
        "alb.prog".into(),
        FakeDeps::album(vec![meta("prog_t1"), meta("prog_t2")]),
    );

    let summary = run_async(&orch, &deps, &options(vec![album_item("alb.prog")], true))
        .await
        .expect("job succeeds");
    assert_eq!(summary.ripped_count, 2);

    let progress = events.progress_snapshot();
    let last_progress = progress.last().expect("progress events exist");
    assert_eq!(last_progress.percent, 100);
    assert!(last_progress.upload.is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_tracks_after_cap_edge() {
    // max_collection_tracks = 0 means "no cap" (limit > 0 check).
    let (orch, deps, _, _) = setup();
    let meta = |id: &str| FakeDeps::track_meta(id, "Says", "Nils Frahm");
    deps.albums.lock().unwrap().insert(
        "alb.1".into(),
        FakeDeps::album(vec![meta("t1"), meta("t2")]),
    );
    deps.set_settings(|s| s.max_collection_tracks = 0);

    let summary = run_async(&orch, &deps, &options(vec![album_item("alb.1")], false))
        .await
        .expect("job succeeds");
    assert_eq!(summary.total_tracks, 2, "0 disables the cap");
    assert_eq!(summary.capped_count, 0);
}

// album ZIP: generation-hash reuse and cover handling
/// Builds RipJobOptions for a single album item.
fn album_options(album: &str, cache_only: bool, force: bool) -> RipJobOptions {
    RipJobOptions {
        provider: engine::types::Provider::Apple,
        chat_id: 100,
        user_id: 42,
        user_name: Some("tester".into()),
        delivery_chat_id: 100,
        is_group: false,
        is_force: force,
        is_cache_only: cache_only,
        single_storefront: None,
        parsed_items: vec![album_item(album)],
        reply_to_message_id: Some(555),
        status_msg_id: 999,
        is_admin: true,
        codec_preference: None,
        rendition_policy: engine::orchestrator::types::RenditionPolicy::PrimaryOnly,
    }
}

fn dual_zip_options(album: &str) -> RipJobOptions {
    let mut opts = album_options(album, false, false);
    opts.rendition_policy = music::RenditionPolicy::PrimaryWithOptionalAtmos;
    opts
}

/// Computes the generation hash the orchestrator will derive for a faked
/// album, matching `album_generation_hash("apple", album, ids)`.
fn expected_generation_hash(album: &str, ids: &[&str]) -> String {
    engine::zip::album_generation_hash("apple", album, ids)
}

fn cached_zip_row(album: &str, part: i32, total: i32, hash: &str) -> CachedAlbum {
    cached_zip_row_with_codec(album, part, total, hash, engine::Codec::Alac)
}

fn cached_zip_row_with_codec(
    album: &str,
    part: i32,
    total: i32,
    hash: &str,
    codec: engine::Codec,
) -> CachedAlbum {
    CachedAlbum {
        part_index: part,
        total_parts: total,
        message_id: 5000 + i64::from(part),
        file_unique_id: format!("zip_uniq_{album}_{part}"),
        generation_hash: hash.to_owned(),
        file_size: 512,
        codec,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn zip_only_delivery_skips_individual_track_copies() {
    // Ripped track on a user ZIP job: staged for the archive, dumped for the
    // cache, but never copied to the requester as an individual file.
    let (orch, deps, state, _) = setup();
    let meta = |id: &str| FakeDeps::track_meta(id, "Track", "Artist");
    deps.albums.lock().unwrap().insert(
        "alb.rip".into(),
        FakeDeps::album(vec![meta("t1"), meta("t2")]),
    );
    deps.rip_scripts
        .lock()
        .unwrap()
        .insert("t1".into(), RipScript::OkWithFile(vec![1, 2, 3]));
    deps.rip_scripts
        .lock()
        .unwrap()
        .insert("t2".into(), RipScript::OkWithFile(vec![4, 5, 6]));
    {
        let mut st = state.lock().unwrap();
        for _ in 0..2 {
            st.send_audio_results.push_back(FakeDeps::upload_ok());
        }
    }

    let summary = run_async(&orch, &deps, &album_options("alb.rip", false, false))
        .await
        .expect("job succeeds");

    let st = state.lock().unwrap();
    // Both tracks were ripped and cached to the dump.
    assert_eq!(summary.ripped_count, 2);
    assert_eq!(st.sent_audio.len(), 2, "audio cached to dump");
    // ZIP-only: copies to the requester are the ZIP part alone (msg 900 is
    // the fake dump-upload message id), never the individual tracks.
    let per_track_copies: Vec<i64> = st
        .copies
        .iter()
        .filter(|(to, _, _, _)| *to == 100)
        .map(|(_, msg, _, _)| *msg)
        .collect();
    assert!(
        per_track_copies.iter().all(|msg| *msg == 900),
        "individual tracks must not be delivered on ZIP jobs: {per_track_copies:?}"
    );
    assert_eq!(st.sent_documents.len(), 1, "complete archive published");
    let delivery = summary
        .zip_delivery
        .as_ref()
        .expect("rebuild delivery metadata present");
    assert_eq!(delivery.delivered_tracks, Some(2));
    assert_eq!(delivery.total_parts, 1);
    assert!(!delivery.is_partial);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn primary_zip_upload_failure_fails_after_marker_cleanup() {
    let (orch, deps, state, events) = setup();
    let meta = |id: &str| FakeDeps::track_meta(id, "Track", "Artist");
    deps.albums.lock().unwrap().insert(
        "alb.zip.upload-failure".into(),
        FakeDeps::album(vec![meta("u1"), meta("u2")]),
    );
    for id in ["u1", "u2"] {
        deps.rip_scripts
            .lock()
            .unwrap()
            .insert(id.into(), RipScript::OkWithFile(vec![1, 2, 3]));
    }
    {
        let mut st = state.lock().unwrap();
        st.send_audio_results.push_back(FakeDeps::upload_ok());
        st.send_audio_results.push_back(FakeDeps::upload_ok());
        st.zip_dump_results
            .push_back(Err(DeliveryError::Unavailable("ZIP service down".into())));
    }

    let error = run_async(
        &orch,
        &deps,
        &album_options("alb.zip.upload-failure", false, false),
    )
    .await
    .expect_err("a required primary ZIP upload must fail the job");
    assert_eq!(
        error.to_string(),
        "primary ZIP upload failed: delivery unavailable: ZIP service down"
    );
    let st = state.lock().unwrap();
    let uploaded_path = st
        .sent_documents
        .first()
        .expect("ZIP upload was attempted")
        .clone();
    assert!(!std::path::Path::new(&uploaded_path).exists());
    drop(st);
    let records = events.snapshot();
    assert_eq!(
        records
            .iter()
            .filter(|event| event.starts_with("completed") || event.starts_with("failed:"))
            .count(),
        1
    );
    assert!(records.iter().any(|event| {
        event == "failed:primary ZIP upload failed: delivery unavailable: ZIP service down"
    }));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn panicking_zip_marker_settles_once_and_cleans_workspace() {
    let (orch, deps, state, events) = setup();
    let meta = |id: &str| FakeDeps::track_meta(id, "Track", "Artist");
    deps.albums.lock().unwrap().insert(
        "alb.zip.panic".into(),
        FakeDeps::album(vec![meta("p1"), meta("p2")]),
    );
    for id in ["p1", "p2"] {
        deps.rip_scripts
            .lock()
            .unwrap()
            .insert(id.into(), RipScript::OkWithFile(vec![1, 2, 3]));
    }
    {
        let mut st = state.lock().unwrap();
        for _ in 0..2 {
            st.send_audio_results.push_back(FakeDeps::upload_ok());
        }
        st.zip_dump_panics = true;
    }

    let error = run_async(&orch, &deps, &album_options("alb.zip.panic", false, false))
        .await
        .expect_err("a marker panic must fail rather than hang");
    assert_eq!(error.to_string(), "finalize marker panicked");
    let records = events.snapshot();
    assert_eq!(
        records
            .iter()
            .filter(|event| event.starts_with("completed") || event.starts_with("failed:"))
            .count(),
        1
    );
    assert!(
        records
            .iter()
            .any(|event| event == "failed:finalize marker panicked")
    );
    assert!(
        state
            .lock()
            .unwrap()
            .sent_documents
            .first()
            .is_some_and(|path| !std::path::Path::new(path).exists())
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn primary_zip_delivery_failure_is_not_reported_completed() {
    let (orch, deps, state, events) = setup();
    let meta = |id: &str| FakeDeps::track_meta(id, "Track", "Artist");
    deps.albums.lock().unwrap().insert(
        "alb.zip.delivery-failure".into(),
        FakeDeps::album(vec![meta("d1"), meta("d2")]),
    );
    for id in ["d1", "d2"] {
        deps.rip_scripts
            .lock()
            .unwrap()
            .insert(id.into(), RipScript::OkWithFile(vec![1, 2, 3]));
    }
    {
        let mut st = state.lock().unwrap();
        for _ in 0..2 {
            st.send_audio_results.push_back(FakeDeps::upload_ok());
        }
        st.copies_fail_ids.push(900);
    }

    let error = run_async(
        &orch,
        &deps,
        &album_options("alb.zip.delivery-failure", false, false),
    )
    .await
    .expect_err("a required primary ZIP copy must fail the job");
    assert_eq!(
        error.to_string(),
        "primary ZIP delivery failed: delivery unavailable: copy failed"
    );
    let records = events.snapshot();
    assert!(records.iter().any(|event| {
        event == "failed:primary ZIP delivery failed: delivery unavailable: copy failed"
    }));
    assert!(!records.iter().any(|event| event == "completed"));
    assert_eq!(
        state.lock().unwrap().deleted_message_batches,
        vec![vec![900]],
        "failed primary ZIP is rolled back from the dump"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn optional_atmos_zip_failure_keeps_primary_successful() {
    let (orch, deps, state, events) = setup();
    let meta = |id: &str| FakeDeps::track_meta(id, "Track", "Artist");
    deps.albums.lock().unwrap().insert(
        "alb.zip.optional-failure".into(),
        FakeDeps::album(vec![meta("o1"), meta("o2")]),
    );
    for id in ["o1", "o2"] {
        deps.rip_scripts
            .lock()
            .unwrap()
            .insert(id.into(), RipScript::OkWithFile(vec![1, 2, 3]));
    }
    {
        let mut st = state.lock().unwrap();
        for _ in 0..4 {
            st.send_audio_results.push_back(FakeDeps::upload_ok());
        }
        st.zip_dump_results.push_back(FakeDeps::upload_ok());
        st.zip_dump_panic_on_call = Some(2);
    }

    let mut opts = dual_zip_options("alb.zip.optional-failure");
    opts.is_admin = true;
    let summary = run_async(&orch, &deps, &opts)
        .await
        .expect("optional Atmos ZIP failure is best effort");
    assert_eq!(summary.zip_deliveries.len(), 1);
    assert_eq!(summary.zip_deliveries[0].codec.as_deref(), Some("alac"));
    assert_eq!(state.lock().unwrap().saved_albums.len(), 1);
    assert!(events.snapshot().iter().any(|event| event == "completed"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn zip_primary_and_atmos_progress_reaches_event_consumers() {
    let (orch, deps, state, events) = setup();
    let meta = |id: &str| FakeDeps::track_meta(id, "Track", "Artist");
    deps.albums.lock().unwrap().insert(
        "alb.progress".into(),
        FakeDeps::album(vec![meta("t1"), meta("t2")]),
    );
    for id in ["t1", "t2"] {
        deps.rip_scripts
            .lock()
            .unwrap()
            .insert(id.into(), RipScript::OkWithFile(vec![1, 2, 3]));
    }
    {
        let mut st = state.lock().unwrap();
        for _ in 0..4 {
            st.send_audio_results.push_back(FakeDeps::upload_ok());
        }
    }

    let mut opts = album_options("alb.progress", false, false);
    opts.rendition_policy = music::RenditionPolicy::PrimaryWithOptionalAtmos;
    run_async(&orch, &deps, &opts)
        .await
        .expect("dual-rendition ZIP job succeeds");

    let st = state.lock().unwrap();
    assert_eq!(
        st.sent_documents.len(),
        2,
        "primary and Atmos archives upload"
    );
    assert_eq!(
        st.saved_albums.len(),
        2,
        "primary and Atmos archive rows persist"
    );
    assert!(
        st.saved_albums
            .iter()
            .any(|album| album.codec == engine::Codec::Alac)
    );
    assert!(
        st.saved_albums
            .iter()
            .any(|album| album.codec == engine::Codec::Ec3)
    );
    drop(st);

    let progress = events.upload_progress_snapshot();
    let build_progress = progress
        .iter()
        .filter(|activity| matches!(activity, UploadLane::ArchiveBuild { .. }))
        .collect::<Vec<_>>();
    let upload_progress = progress
        .iter()
        .filter(|activity| matches!(activity, UploadLane::ArchiveUpload { .. }))
        .collect::<Vec<_>>();
    assert_eq!(
        build_progress.len(),
        6,
        "both archives emit build status and bytes"
    );
    assert_eq!(
        upload_progress.len(),
        6,
        "both archives emit upload status and bytes"
    );
    assert!(build_progress.iter().any(|activity| matches!(
        activity,
        UploadLane::ArchiveBuild { progress, .. } if progress.completed > 0
    )));
    assert!(upload_progress.iter().any(|activity| matches!(
        activity,
        UploadLane::ArchiveUpload { progress, .. } if progress.completed > 0
    )));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sparse_dual_zip_delivers_primary_and_available_atmos_parts_in_order() {
    let (orch, deps, state, _) = setup();
    let meta = |id: &str| FakeDeps::track_meta(id, "Track", "Artist");
    deps.albums.lock().unwrap().insert(
        "alb.sparse".into(),
        FakeDeps::album(vec![meta("s1"), meta("s2")]),
    );
    deps.rip_scripts
        .lock()
        .unwrap()
        .insert("s1".into(), RipScript::OkWithFile(vec![1, 2, 3]));
    deps.rip_scripts
        .lock()
        .unwrap()
        .insert("s2".into(), RipScript::AtmosUnavailable(vec![4, 5, 6]));
    {
        let mut st = state.lock().unwrap();
        for _ in 0..3 {
            st.send_audio_results.push_back(FakeDeps::upload_ok());
        }
    }

    let summary = run_async(&orch, &deps, &dual_zip_options("alb.sparse"))
        .await
        .expect("sparse dual ZIP job succeeds");

    assert_eq!(summary.failed_count, 0);
    assert_eq!(summary.zip_deliveries.len(), 2);
    assert_eq!(
        summary
            .zip_deliveries
            .iter()
            .map(|info| info.codec.as_deref())
            .collect::<Vec<_>>(),
        vec![Some("alac"), Some("ec-3")],
        "delivery metadata remains primary then Atmos"
    );
    assert_eq!(summary.zip_deliveries[0].delivered_tracks, Some(2));
    assert_eq!(summary.zip_deliveries[1].delivered_tracks, Some(1));

    let st = state.lock().unwrap();
    assert_eq!(
        st.sent_audio.len(),
        3,
        "only the available Atmos track is ripped"
    );
    assert_eq!(
        st.sent_documents.len(),
        2,
        "both non-empty archives are published"
    );
    assert_eq!(
        st.saved_albums.len(),
        2,
        "both archive identities are cached"
    );
    assert_eq!(st.saved_albums[0].codec, engine::Codec::Alac);
    assert_eq!(st.saved_albums[1].codec, engine::Codec::Ec3);
    assert_eq!(
        st.copies.iter().map(|copy| copy.1).collect::<Vec<_>>(),
        vec![900, 900],
        "ZIP delivery replaces all individual track copies"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn zip_cached_without_reuse_rows_stages_and_rebuilds() {
    // Fully cached album, no reusable ZIP rows: staging downloads run, the
    // archive is rebuilt from the cached files, and no individual track
    // copies are delivered.
    let (orch, deps, state, _) = setup();
    let meta = |id: &str| FakeDeps::track_meta(id, "Track", "Artist");
    deps.albums.lock().unwrap().insert(
        "alb.stage".into(),
        FakeDeps::album(vec![meta("t1"), meta("t2")]),
    );
    deps.cache_track("t1", 101);
    deps.cache_track("t2", 102);

    let summary = run_async(&orch, &deps, &album_options("alb.stage", false, false))
        .await
        .expect("job succeeds");

    let st = state.lock().unwrap();
    assert_eq!(summary.cached_count, 2, "both tracks served from cache");
    assert!(st.rip_calls.is_empty(), "nothing re-ripped");
    assert_eq!(
        st.sent_documents.len(),
        1,
        "rebuilt from staged cache files"
    );
    // Request log still written for ZIP jobs' cached hits.
    assert_eq!(
        st.request_logs
            .iter()
            .filter(|log| log.is_cache_hit && log.status == "completed")
            .count(),
        2,
        "cache-hit requests logged for staged tracks: {:?}",
        st.request_logs
    );
    // No individual track copies.
    let per_track_copies: Vec<i64> = st
        .copies
        .iter()
        .filter(|(to, _, _, _)| *to == 100)
        .map(|(_, msg, _, _)| *msg)
        .collect();
    assert!(
        per_track_copies.iter().all(|msg| *msg == 900),
        "no individual copies on ZIP jobs: {per_track_copies:?}"
    );
    assert!(
        summary
            .zip_delivery
            .as_ref()
            .is_some_and(|d| !d.is_partial && d.delivered_tracks == Some(2)),
        "delivery metadata present"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cached_aac_zip_uses_aac_identity_everywhere() {
    let (orch, deps, state, _) = setup();
    let meta = |id: &str| FakeDeps::track_meta(id, "Track", "Artist");
    deps.albums.lock().unwrap().insert(
        "alb.aac".into(),
        FakeDeps::album(vec![meta("a1"), meta("a2")]),
    );
    deps.cache_track_with_codec("a1", 201, engine::Codec::Aac);
    deps.cache_track_with_codec("a2", 202, engine::Codec::Aac);

    let summary = run_async(&orch, &deps, &album_options("alb.aac", false, false))
        .await
        .expect("job succeeds");

    let st = state.lock().unwrap();
    assert_eq!(st.saved_albums.len(), 1);
    assert_eq!(st.saved_albums[0].codec, engine::Codec::Aac);
    assert!(st.sent_document_captions[0].contains("[AAC]"));
    assert_eq!(
        summary
            .zip_delivery
            .as_ref()
            .and_then(|info| info.codec.as_deref()),
        Some("aac")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn zip_replacement_cleans_old_document_but_keeps_committed_new_document() {
    let (orch, deps, state, _) = setup();
    let album_id = "alb.zip.old-cleanup";
    let meta = |id: &str| FakeDeps::track_meta(id, "Track", "Artist");
    deps.albums.lock().unwrap().insert(
        album_id.into(),
        FakeDeps::album(vec![meta("old-1"), meta("old-2")]),
    );
    for id in ["old-1", "old-2"] {
        deps.rip_scripts
            .lock()
            .unwrap()
            .insert(id.into(), RipScript::OkWithFile(vec![1, 2, 3]));
    }
    let old_hash = expected_generation_hash(album_id, &["old-1", "old-2"]);
    let mut old_row = cached_zip_row(album_id, 1, 1, &old_hash);
    old_row.message_id = 4001;
    state
        .lock()
        .unwrap()
        .found_albums
        .insert(album_id.into(), vec![old_row]);

    run_async(&orch, &deps, &album_options(album_id, false, true))
        .await
        .expect("replacement succeeds");

    let st = state.lock().unwrap();
    assert_eq!(st.deleted_message_batches, vec![vec![4001]]);
    assert_eq!(st.saved_albums.len(), 1);
    assert_eq!(st.saved_albums[0].message_id, 900);
    assert_eq!(
        st.found_albums
            .get(album_id)
            .unwrap()
            .iter()
            .map(|row| row.message_id)
            .collect::<Vec<_>>(),
        vec![900]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn zip_cancel_after_replacement_commit_never_deletes_new_document() {
    let (orch, deps, state, _) = setup();
    let album_id = "alb.zip.cancel-after-commit";
    let meta = |id: &str| FakeDeps::track_meta(id, "Track", "Artist");
    deps.albums.lock().unwrap().insert(
        album_id.into(),
        FakeDeps::album(vec![meta("commit-1"), meta("commit-2")]),
    );
    for id in ["commit-1", "commit-2"] {
        deps.rip_scripts
            .lock()
            .unwrap()
            .insert(id.into(), RipScript::OkWithFile(vec![1, 2, 3]));
    }
    let mut old_row = cached_zip_row(album_id, 1, 1, "old-generation");
    old_row.message_id = 4101;
    state
        .lock()
        .unwrap()
        .found_albums
        .insert(album_id.into(), vec![old_row]);
    let replacement_gate = tokio_util::sync::CancellationToken::new();
    state.lock().unwrap().replacement_gate = Some(replacement_gate.clone());

    let orch = Arc::new(orch);
    let run_orch = Arc::clone(&orch);
    let run_deps = Arc::clone(&deps);
    let task = tokio::spawn(async move {
        run_async(&run_orch, &run_deps, &album_options(album_id, false, true)).await
    });
    loop {
        let notified = deps.replacement_notify.notified();
        if state.lock().unwrap().replacement_committed {
            break;
        }
        notified.await;
    }
    let job = orch
        .get_active_jobs()
        .into_iter()
        .next()
        .expect("job remains active while replacement future is gated");
    assert!(orch.cancel_job(&job.id, Some("tester")));
    replacement_gate.cancel();

    tokio::time::timeout(std::time::Duration::from_secs(2), task)
        .await
        .expect("cancelled replacement settles")
        .unwrap()
        .expect("cancelled job returns its summary");
    assert_eq!(
        state.lock().unwrap().deleted_message_batches,
        vec![vec![4101]],
        "only the old document is cleaned after commit"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn zip_primary_codec_transition_removes_stale_alac_and_reuses_aac() {
    let (orch, deps, state, _) = setup();
    let album_id = "alb.zip.codec-transition";
    let meta = |id: &str| FakeDeps::track_meta(id, "Track", "Artist");
    deps.albums.lock().unwrap().insert(
        album_id.into(),
        FakeDeps::album(vec![meta("codec-1"), meta("codec-2")]),
    );
    for id in ["codec-1", "codec-2"] {
        deps.rip_scripts
            .lock()
            .unwrap()
            .insert(id.into(), RipScript::OkWithCodec(vec![1, 2, 3], "aac"));
    }
    let mut old_primary =
        cached_zip_row_with_codec(album_id, 1, 1, "old-generation", engine::Codec::Alac);
    old_primary.message_id = 4201;
    let mut old_atmos =
        cached_zip_row_with_codec(album_id, 1, 1, "atmos-generation", engine::Codec::Ec3);
    old_atmos.message_id = 4202;
    state
        .lock()
        .unwrap()
        .found_albums
        .insert(album_id.into(), vec![old_primary, old_atmos]);

    run_async(&orch, &deps, &album_options(album_id, false, true))
        .await
        .expect("AAC replacement succeeds");
    {
        let st = state.lock().unwrap();
        assert_eq!(st.deleted_message_batches, vec![vec![4201]]);
        let rows = st.found_albums.get(album_id).unwrap();
        assert!(rows.iter().any(|row| row.codec == engine::Codec::Aac));
        assert!(rows.iter().any(|row| row.codec == engine::Codec::Ec3));
        assert!(!rows.iter().any(|row| row.codec == engine::Codec::Alac));
    }

    // Seed only the AAC per-track alternatives for a second request. Reuse
    // must find the AAC archive; a stale ALAC row would win the old codec
    // preference and force a needless rebuild.
    deps.cache_track_with_codec("codec-1", 4301, engine::Codec::Aac);
    deps.cache_track_with_codec("codec-2", 4302, engine::Codec::Aac);
    let document_count = state.lock().unwrap().sent_documents.len();
    let summary = run_async(&orch, &deps, &album_options(album_id, false, false))
        .await
        .expect("AAC archive is reusable");
    let st = state.lock().unwrap();
    assert_eq!(
        st.sent_documents.len(),
        document_count,
        "reuse skips rebuild"
    );
    assert_eq!(summary.cached_count, 2);
    assert_eq!(
        summary
            .zip_delivery
            .as_ref()
            .and_then(|info| info.codec.as_deref()),
        Some("aac")
    );
    assert_eq!(
        st.copies.last().map(|copy| copy.1),
        Some(900),
        "the AAC archive row is delivered on reuse"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn zip_partial_delivery_marks_zip_delivery_partial() {
    // One of two tracks fails to rip → user gets a partial archive and the
    // delivery metadata reflects it.
    let (orch, deps, state, _) = setup();
    let meta = |id: &str| FakeDeps::track_meta(id, "Track", "Artist");
    deps.albums.lock().unwrap().insert(
        "alb.part".into(),
        FakeDeps::album(vec![meta("t1"), meta("t2")]),
    );
    deps.rip_scripts
        .lock()
        .unwrap()
        .insert("t1".into(), RipScript::OkWithFile(vec![1, 2, 3]));
    deps.rip_scripts.lock().unwrap().insert(
        "t2".into(),
        RipScript::Fail(RipError::Message("mirror exploded".into())),
    );
    {
        let mut st = state.lock().unwrap();
        st.send_audio_results.push_back(FakeDeps::upload_ok());
    }

    let summary = run_async(&orch, &deps, &album_options("alb.part", false, false))
        .await
        .expect("job succeeds");

    assert_eq!(summary.failed_count, 1);
    let delivery = summary
        .zip_delivery
        .as_ref()
        .expect("partial delivery still reports metadata");
    assert!(delivery.is_partial, "delivery flagged partial");
    assert_eq!(
        delivery.delivered_tracks,
        Some(1),
        "one track in the archive"
    );
    assert_eq!(delivery.total_tracks, 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn zip_reuse_delivers_cached_parts_without_rebuild() {
    let (orch, deps, state, _) = setup();
    let meta = |id: &str| FakeDeps::track_meta(id, "Track", "Artist");
    deps.albums.lock().unwrap().insert(
        "alb.zip".into(),
        FakeDeps::album(vec![meta("t1"), meta("t2")]),
    );
    deps.cache_track("t1", 101);
    deps.cache_track("t2", 102);
    let hash = expected_generation_hash("alb.zip", &["t1", "t2"]);
    state.lock().unwrap().found_albums.insert(
        "alb.zip".into(),
        vec![
            cached_zip_row("alb.zip", 1, 2, &hash),
            cached_zip_row("alb.zip", 2, 2, &hash),
        ],
    );

    let summary = run_async(&orch, &deps, &album_options("alb.zip", false, false))
        .await
        .expect("job succeeds");

    let st = state.lock().unwrap();
    // ZIP-only delivery: the requester gets the cached parts alone — the
    // individual track copies must NOT be sent for ZIP jobs.
    let zip_copies: Vec<i64> = st
        .copies
        .iter()
        .filter(|(to, _, _, _)| *to == 100)
        .map(|(_, msg, _, _)| *msg)
        .collect();
    assert_eq!(zip_copies, vec![5001, 5002]);
    // No rebuild: no document uploads, no new ZIP rows, no purges.
    assert!(st.sent_documents.is_empty());
    assert!(st.saved_albums.is_empty());
    assert!(st.deleted_album_zip_ids.is_empty());
    // The per-track staging downloads never ran (reuse skips them).
    assert!(st.rip_calls.is_empty());
    assert_eq!(summary.cached_count, 2);
    assert!(summary.warnings.is_empty());
    let delivery = summary
        .zip_delivery
        .as_ref()
        .expect("reuse delivery metadata present");
    assert_eq!(delivery.total_parts, 2);
    assert!(!delivery.is_partial);
    assert_eq!(delivery.size_bytes, 2 * 512);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn zip_replacement_conflict_retracts_loser_and_reuses_winner() {
    let (orch, deps, state, _) = setup();
    let album_id = "alb.zip.conflict";
    let meta = |id: &str| FakeDeps::track_meta(id, "Track", "Artist");
    deps.albums.lock().unwrap().insert(
        album_id.into(),
        FakeDeps::album(vec![meta("conflict-1"), meta("conflict-2")]),
    );
    for id in ["conflict-1", "conflict-2"] {
        deps.rip_scripts
            .lock()
            .unwrap()
            .insert(id.into(), RipScript::OkWithFile(vec![1, 2, 3]));
    }
    let mut winner = cached_zip_row(album_id, 1, 1, "committed-winner");
    winner.message_id = 7001;
    {
        let mut st = state.lock().unwrap();
        st.replace_conflict = true;
        st.found_albums.insert(album_id.into(), vec![winner]);
    }

    let summary = run_async(&orch, &deps, &album_options(album_id, false, true))
        .await
        .expect("a cache replacement conflict reuses the committed winner");

    assert_eq!(summary.failed_count, 0);
    let st = state.lock().unwrap();
    assert!(
        st.saved_albums.is_empty(),
        "the losing generation is not cached"
    );
    assert_eq!(
        st.deleted_message_batches,
        vec![vec![900]],
        "only the losing publication is retracted"
    );
    assert_eq!(
        st.copies.last().map(|copy| copy.1),
        Some(7001),
        "the committed winner is delivered after the conflict"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sparse_atmos_zip_reuse_reports_tracks_not_archive_parts() {
    let (orch, deps, state, _) = setup();
    let meta = |id: &str| FakeDeps::track_meta(id, "Track", "Artist");
    deps.albums.lock().unwrap().insert(
        "alb.sparse.reuse".into(),
        FakeDeps::album(vec![meta("s1"), meta("s2")]),
    );
    deps.cache_track("s1", 101);
    deps.cache_track("s2", 102);
    // Both tracks have persisted EC-3 cache rows, while the sparse Atmos
    // archive itself still has a single part. This distinguishes delivered
    // tracks from archive parts and would fail if reuse used rows.len().
    deps.cache_track_with_codec("s1", 201, engine::Codec::Ec3);
    deps.cache_track_with_codec("s2", 202, engine::Codec::Ec3);
    let hash = expected_generation_hash("alb.sparse.reuse", &["s1", "s2"]);
    let mut atmos_row =
        cached_zip_row_with_codec("alb.sparse.reuse", 1, 1, &hash, engine::Codec::Ec3);
    atmos_row.message_id = 6001;
    state.lock().unwrap().found_albums.insert(
        "alb.sparse.reuse".into(),
        vec![cached_zip_row("alb.sparse.reuse", 1, 1, &hash), atmos_row],
    );

    let summary = run_async(&orch, &deps, &dual_zip_options("alb.sparse.reuse"))
        .await
        .expect("sparse reused dual ZIP succeeds");

    let st = state.lock().unwrap();
    assert!(
        st.sent_documents.is_empty(),
        "both archive renditions reused"
    );
    assert_eq!(
        st.copies.iter().map(|copy| copy.1).collect::<Vec<_>>(),
        vec![5001, 6001],
        "primary and sparse Atmos parts are delivered in order"
    );
    assert_eq!(summary.zip_deliveries.len(), 2);
    assert_eq!(summary.zip_deliveries[0].delivered_tracks, Some(2));
    assert_eq!(summary.zip_deliveries[1].delivered_tracks, Some(2));
    assert_eq!(summary.zip_deliveries[1].total_parts, 1);
    assert!(
        summary.zip_deliveries[1].delivered_tracks.unwrap() > summary.zip_deliveries[1].total_parts,
        "sparse Atmos track count must exceed its single archive part"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn zip_hash_mismatch_rebuilds() {
    let (orch, deps, state, _) = setup();
    let meta = |id: &str| FakeDeps::track_meta(id, "Track", "Artist");
    deps.albums.lock().unwrap().insert(
        "alb.zip".into(),
        FakeDeps::album(vec![meta("t1"), meta("t2")]),
    );
    deps.cache_track("t1", 101);
    deps.cache_track("t2", 102);
    // Rows exist but under a stale generation hash.
    state.lock().unwrap().found_albums.insert(
        "alb.zip".into(),
        vec![cached_zip_row("alb.zip", 1, 1, "deadbeef")],
    );

    let summary = run_async(&orch, &deps, &album_options("alb.zip", true, false))
        .await
        .expect("job succeeds");

    let st = state.lock().unwrap();
    // Stale rows purged before republishing the fresh complete set.
    assert!(st.deleted_album_zip_ids.contains(&"alb.zip".to_owned()));
    assert_eq!(
        st.sent_documents.len(),
        1,
        "rebuilt archive published to dump"
    );
    assert_eq!(st.saved_albums.len(), 1, "fresh row persisted");
    assert_eq!(
        st.saved_albums[0].generation_hash,
        expected_generation_hash("alb.zip", &["t1", "t2"]),
        "republished under the current hash"
    );
    assert_eq!(summary.cached_count, 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn zip_force_disables_reuse() {
    let (orch, deps, state, _) = setup();
    let meta = |id: &str| FakeDeps::track_meta(id, "Track", "Artist");
    deps.albums.lock().unwrap().insert(
        "alb.zip".into(),
        FakeDeps::album(vec![meta("t1"), meta("t2")]),
    );
    deps.cache_track("t1", 101);
    deps.cache_track("t2", 102);
    let hash = expected_generation_hash("alb.zip", &["t1", "t2"]);
    state.lock().unwrap().found_albums.insert(
        "alb.zip".into(),
        vec![cached_zip_row("alb.zip", 1, 1, &hash)],
    );

    let summary = run_async(&orch, &deps, &album_options("alb.zip", false, true))
        .await
        .expect("job succeeds");

    let st = state.lock().unwrap();
    // Force purges the cache (delete_track) and re-rips; matching cached
    // rows must NOT short-circuit delivery of a rebuilt archive set.
    assert!(!st.deleted_tracks.is_empty(), "force purges cached tracks");
    assert_eq!(st.rip_calls.len(), 2, "both tracks re-ripped");
    assert!(summary.warnings.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_track_album_delivers_individual_track_without_archive() {
    let (orch, deps, state, _) = setup();
    let meta = |id: &str| FakeDeps::track_meta(id, "Solo", "Artist");
    deps.albums
        .lock()
        .unwrap()
        .insert("alb.solo".into(), FakeDeps::album(vec![meta("t1")]));

    let summary = run_async(&orch, &deps, &album_options("alb.solo", false, false))
        .await
        .expect("job succeeds");

    assert_eq!(summary.total_tracks, 1);
    assert!(summary.warnings.is_empty());
    assert!(summary.zip_deliveries.is_empty());
    let state = state.lock().unwrap();
    assert!(state.sent_documents.is_empty());
    assert_eq!(state.copies, vec![(100, 777, Some(555), false)]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multi_track_playlist_delivers_individual_tracks_without_archive() {
    let (orch, deps, state, _) = setup();
    deps.playlists.lock().unwrap().insert(
        "pl.multi".into(),
        PlaylistData {
            id: "pl.multi".into(),
            title: "Mix".into(),
            curator_name: None,
            description: None,
            tracks: vec![
                PlaylistTrack {
                    id: "p1".into(),
                    title: "One".into(),
                    artist: "Artist".into(),
                    duration: Some(120),
                },
                PlaylistTrack {
                    id: "p2".into(),
                    title: "Two".into(),
                    artist: "Artist".into(),
                    duration: Some(130),
                },
            ],
        },
    );

    let summary = run_async(
        &orch,
        &deps,
        &options(vec![playlist_item("pl.multi")], false),
    )
    .await
    .expect("job succeeds");

    assert_eq!(summary.total_tracks, 2);
    assert_eq!(summary.ripped_count, 2);
    assert!(summary.zip_deliveries.is_empty());
    let state = state.lock().unwrap();
    assert!(state.sent_documents.is_empty());
    assert_eq!(state.copies, vec![(100, 777, Some(555), true); 2]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn zip_build_includes_cover_and_real_generation_hash() {
    let (orch, deps, state, _) = setup();
    let meta = |id: &str| FakeDeps::track_meta(id, "Track", "Artist");
    deps.albums.lock().unwrap().insert(
        "alb.zip".into(),
        FakeDeps::album(vec![meta("t1"), meta("t2")]),
    );
    // Album artwork present → cover fetch + preview + cover.jpg entry.
    {
        let mut albums = deps.albums.lock().unwrap();
        let mut album = albums.get("alb.zip").cloned().unwrap();
        album.album.artwork_url = "https://example.test/cover/3000x3000bb.jpg".into();
        album.album.release_date = "2021-06-04".into();
        albums.insert("alb.zip".into(), album);
    }
    deps.rip_scripts
        .lock()
        .unwrap()
        .insert("t1".into(), RipScript::OkWithFile(vec![1, 2, 3]));
    deps.rip_scripts
        .lock()
        .unwrap()
        .insert("t2".into(), RipScript::OkWithFile(vec![4, 5, 6]));
    {
        let mut st = state.lock().unwrap();
        st.artwork_bytes = Some(vec![9, 9, 9]);
        for _ in 0..2 {
            st.send_audio_results.push_back(FakeDeps::upload_ok());
        }
    }

    let summary = run_async(&orch, &deps, &album_options("alb.zip", false, false))
        .await
        .expect("job succeeds");

    let st = state.lock().unwrap();
    // Complete rebuild published the archive to the dump.
    assert_eq!(st.sent_documents.len(), 1, "one archive part uploaded");
    assert_eq!(st.saved_albums.len(), 1, "one row persisted");
    let saved = &st.saved_albums[0];
    assert_eq!(saved.part_index, 1);
    assert_eq!(
        saved.generation_hash,
        expected_generation_hash("alb.zip", &["t1", "t2"]),
        "real generation hash persisted"
    );
    // ZIP-only delivery: no per-track dump copies for the individual
    // tracks (only the ZIP part DM copy may occur).
    let per_track_copies: Vec<i64> = st
        .copies
        .iter()
        .filter(|(to, _, _, _)| *to == 100)
        .map(|(_, msg, _, _)| *msg)
        .collect();
    assert!(
        per_track_copies.iter().all(|msg| *msg == 900),
        "only the ZIP part copy is delivered, got {per_track_copies:?}"
    );
    // Cover + thumbnail fetched, preview sent to the requester.
    assert_eq!(
        st.fetch_artwork_urls.len(),
        2,
        "cover + 320px thumbnail fetched: {:?}",
        st.fetch_artwork_urls
    );
    assert_eq!(st.sent_photos.len(), 1);
    assert_eq!(st.sent_photos[0].0, 100);
    assert_eq!(st.sent_thumbs.len(), 1, "thumbnail attached to the part");
    // Filename carries the release year.
    assert!(
        saved.file_name.contains("(2021)"),
        "file was {}",
        saved.file_name
    );
    // The uploaded archive really contains cover.jpg + hashed manifest.
    // Bytes were captured at upload time — the workspace is gone now.
    let archive_dir = std::env::temp_dir().join(format!("zip_assert_{}", std::process::id()));
    std::fs::create_dir_all(&archive_dir).unwrap();
    let archive_path = archive_dir.join("uploaded.zip");
    std::fs::write(&archive_path, &st.uploaded_document_bytes).unwrap();
    let file = std::fs::File::open(&archive_path).unwrap();
    let mut archive = zip::ZipArchive::new(file).unwrap();
    assert!(archive.by_name("cover.jpg").is_ok());
    assert!(archive.by_name("manifest.json").is_ok());
    let mut manifest_file = archive.by_name("manifest.json").unwrap();
    let mut manifest_bytes = Vec::new();
    std::io::Read::read_to_end(&mut manifest_file, &mut manifest_bytes).unwrap();
    let manifest: serde_json::Value = serde_json::from_slice(&manifest_bytes).unwrap();
    assert!(manifest["cover_sha256"].is_string());
    assert!(manifest["tracks"][0]["sha256"].is_string());
    let _ = std::fs::remove_dir_all(&archive_dir);
    assert!(summary.warnings.is_empty());
}

// two-lane pipeline (lane 1 rip ∥ lane 2 upload)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn get_album_job_delivers_only_zip_archives() {
    // Multi-track album requests use the `/get` path and deliver archives,
    // never individual track copies.
    let (orch, deps, state, _) = setup();
    let meta = |id: &str| FakeDeps::track_meta(id, "Track", "Artist");
    deps.albums.lock().unwrap().insert(
        "alb.auto".into(),
        FakeDeps::album(vec![meta("t1"), meta("t2")]),
    );
    deps.rip_scripts
        .lock()
        .unwrap()
        .insert("t1".into(), RipScript::OkWithFile(vec![1, 2, 3]));
    deps.rip_scripts
        .lock()
        .unwrap()
        .insert("t2".into(), RipScript::OkWithFile(vec![4, 5, 6]));
    {
        let mut st = state.lock().unwrap();
        for _ in 0..4 {
            st.send_audio_results.push_back(FakeDeps::upload_ok());
        }
    }

    let summary = run_async(&orch, &deps, &dual_zip_options("alb.auto"))
        .await
        .expect("job succeeds");

    let st = state.lock().unwrap();
    assert_eq!(summary.ripped_count, 2);
    assert_eq!(
        st.sent_audio.len(),
        4,
        "both requested renditions are cached"
    );
    // Individual tracks are not delivered to the user; only archive parts
    // (message 900) are copied from the dump.
    let delivered: Vec<i64> = st
        .copies
        .iter()
        .filter(|(to, _, _, _)| *to == 100)
        .map(|(_, msg, _, _)| *msg)
        .collect();
    assert_eq!(delivered, vec![900, 900], "only ZIPs are delivered");
    assert_eq!(st.sent_documents.len(), 2, "both archives uploaded to dump");
    assert_eq!(st.saved_albums.len(), 2, "both archive rows saved");
    assert_eq!(summary.zip_deliveries.len(), 2);
    assert!(
        st.sent_photos.is_empty(),
        "artwork is absent in this fixture"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_lanes_interleave_job_b_rips_while_job_a_uploads() {
    // Lane 1 must free the rip slot when job A's last RIP is done, even
    // while its uploads continue on lane 2 — so job B starts ripping
    // before A's summary resolves.
    let (orch, deps, state, _) = setup();
    let meta = |id: &str| FakeDeps::track_meta(id, "Track", "Artist");
    deps.albums.lock().unwrap().insert(
        "alb.a".into(),
        FakeDeps::album(vec![meta("a1"), meta("a2")]),
    );
    deps.albums.lock().unwrap().insert(
        "alb.b".into(),
        FakeDeps::album(vec![meta("b1"), meta("b2")]),
    );
    for id in ["a1", "a2", "b1", "b2"] {
        deps.rip_scripts
            .lock()
            .unwrap()
            .insert(id.into(), RipScript::OkWithFile(vec![1]));
    }
    {
        let mut st = state.lock().unwrap();
        for _ in 0..4 {
            st.send_audio_results.push_back(FakeDeps::upload_ok());
        }
    }

    let orch = Arc::new(orch);
    let a_deps = Arc::clone(&deps);
    let a_options = options(vec![album_item("alb.a")], false);
    let b_deps = Arc::clone(&deps);
    let b_options = options(vec![album_item("alb.b")], false);

    let a_orch = Arc::clone(&orch);
    let a_task = tokio::spawn(async move { run_async(&a_orch, &a_deps, &a_options).await });
    // Ensure A is admitted and starts lane 1 first.
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let b_orch = Arc::clone(&orch);
    let b_task = tokio::spawn(async move { run_async(&b_orch, &b_deps, &b_options).await });
    let a_summary = a_task.await.unwrap().expect("job A succeeds");
    let b_summary = b_task.await.unwrap().expect("job B succeeds");
    assert_eq!(a_summary.ripped_count, 2);
    assert_eq!(b_summary.ripped_count, 2);

    // Both jobs' four tracks reached the dump through lane 2.
    let st = state.lock().unwrap();
    assert_eq!(st.sent_audio.len(), 4);
    assert_eq!(st.saved_albums.len(), 2, "both archives cached");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gated_upload_does_not_hold_lane_one_or_terminalize_job_early() {
    let (orch, deps, state, events) = setup();
    let meta = |id: &str| FakeDeps::track_meta(id, "Track", "Artist");
    deps.albums.lock().unwrap().insert(
        "alb.gated.a".into(),
        FakeDeps::album(vec![meta("ga1"), meta("ga2")]),
    );
    deps.albums
        .lock()
        .unwrap()
        .insert("alb.gated.b".into(), FakeDeps::album(vec![meta("gb1")]));
    for id in ["ga1", "ga2", "gb1"] {
        deps.rip_scripts
            .lock()
            .unwrap()
            .insert(id.into(), RipScript::OkWithFile(vec![1]));
    }
    {
        let mut st = state.lock().unwrap();
        for _ in 0..3 {
            st.send_audio_results.push_back(FakeDeps::upload_ok());
        }
    }
    let gate = tokio_util::sync::CancellationToken::new();
    state.lock().unwrap().gate_uploads = Some(gate.clone());

    let orch = Arc::new(orch);
    let a_orch = Arc::clone(&orch);
    let a_deps = Arc::clone(&deps);
    let a_options = options(vec![album_item("alb.gated.a")], true);
    let a = tokio::spawn(async move { run_async(&a_orch, &a_deps, &a_options).await });

    let wait_for_rips = |count: usize, deps: Arc<FakeDeps>| async move {
        loop {
            let notified = deps.rip_notify.notified();
            if deps.state.lock().unwrap().rip_calls.len() >= count {
                break;
            }
            notified.await;
        }
    };
    wait_for_rips(2, Arc::clone(&deps)).await;

    let b_orch = Arc::clone(&orch);
    let b_deps = Arc::clone(&deps);
    let b_options = options(vec![album_item("alb.gated.b")], true);
    let b = tokio::spawn(async move { run_async(&b_orch, &b_deps, &b_options).await });
    wait_for_rips(3, Arc::clone(&deps)).await;

    // A's lane-1 task has returned and B has ripped while A's first upload is
    // still gated. A remains active because its FIFO marker has not settled.
    assert!(!a.is_finished(), "A must await its lane-2 marker");
    assert_eq!(orch.get_active_jobs().len(), 2);
    assert!(!events.snapshot().iter().any(|event| event == "completed"));

    gate.cancel();
    a.await.unwrap().expect("A succeeds after gate release");
    b.await.unwrap().expect("B succeeds after gate release");
    assert!(orch.get_active_jobs().is_empty());
    assert_eq!(
        events
            .snapshot()
            .iter()
            .filter(|event| event.starts_with("completed"))
            .count(),
        2
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_during_lane_two_upload_still_resolves() {
    // Cancelling after lane 1 finished its rips (uploads gated open on
    // lane 2) must not hang start_job: the finalize marker still runs,
    // cleans the workspaces, and resolves the summary.
    let (orch, deps, state, events) = setup();
    let meta = |id: &str| FakeDeps::track_meta(id, "Track", "Artist");
    deps.albums.lock().unwrap().insert(
        "alb.cancel".into(),
        FakeDeps::album(vec![meta("c1"), meta("c2")]),
    );
    for id in ["c1", "c2"] {
        deps.rip_scripts
            .lock()
            .unwrap()
            .insert(id.into(), RipScript::OkWithFile(vec![1]));
    }
    {
        let mut st = state.lock().unwrap();
        for _ in 0..2 {
            st.send_audio_results.push_back(FakeDeps::upload_ok());
        }
    }
    // Hold every lane-2 upload open until the test releases it.
    let gate = tokio_util::sync::CancellationToken::new();
    state.lock().unwrap().gate_uploads = Some(gate.clone());

    let orch = Arc::new(orch);
    let run_orch = Arc::clone(&orch);
    let run_deps = Arc::clone(&deps);
    let run_options = options(vec![album_item("alb.cancel")], false);
    let task = tokio::spawn(async move { run_async(&run_orch, &run_deps, &run_options).await });
    // Wait until lane 1 is done (both rip calls recorded) — its slot frees
    // while lane 2 is gated mid-upload.
    let rip_done = async {
        loop {
            let done = {
                let st = state.lock().unwrap();
                st.rip_calls.len() == 2
            };
            if done {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    };
    rip_done.await;
    let job = orch
        .get_active_jobs()
        .into_iter()
        .next()
        .expect("active job");
    assert!(orch.cancel_job(&job.id, Some("tester")));
    // Release the gated uploads; the marker then runs and resolves.
    gate.cancel();
    let summary = task.await.unwrap().expect("summary still resolves");
    // The gated uploads completed as Telegram sends but the job was
    // already cancelled: the post-upload rollback deleted the dump rows,
    // so nothing counts as ripped.
    assert_eq!(summary.ripped_count, 0, "uploads rolled back post-cancel");
    let terminal_count = events
        .snapshot()
        .iter()
        .filter(|event| {
            event.starts_with("completed")
                || event.starts_with("cancelled")
                || event.starts_with("failed")
        })
        .count();
    assert_eq!(terminal_count, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn staging_failure_queues_rip_for_cached_track() {
    // A cache row whose dump message cannot be downloaded is re-ripped on
    // lane 1 instead of silently breaking the archive.
    let (orch, deps, state, _) = setup();
    let meta = |id: &str| FakeDeps::track_meta(id, "Track", "Artist");
    deps.albums.lock().unwrap().insert(
        "alb.stage".into(),
        FakeDeps::album(vec![meta("s1"), meta("s2")]),
    );
    // s1 is cached with a dead dump row (staging download fails), s2 is a
    // plain miss. Both end up ripped.
    deps.cache_track("s1", 424242);
    {
        let mut st = state.lock().unwrap();
        st.download_fail_ids.push(424242);
    }
    deps.rip_scripts
        .lock()
        .unwrap()
        .insert("s1".into(), RipScript::OkWithFile(vec![9]));
    deps.rip_scripts
        .lock()
        .unwrap()
        .insert("s2".into(), RipScript::OkWithFile(vec![8]));
    {
        let mut st = state.lock().unwrap();
        for _ in 0..2 {
            st.send_audio_results.push_back(FakeDeps::upload_ok());
        }
    }

    let summary = run_async(&orch, &deps, &options(vec![album_item("alb.stage")], false))
        .await
        .expect("job succeeds");

    // s1 was re-ripped (stale row + dead dump) and s2 ripped fresh.
    let st = state.lock().unwrap();
    assert!(
        st.rip_calls.contains(&"s1".to_string()),
        "stale s1 re-ripped"
    );
    assert!(st.rip_calls.contains(&"s2".to_string()), "s2 ripped");
    assert_eq!(summary.ripped_count, 2);
    assert_eq!(st.saved_albums.len(), 1, "archive complete and cached");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fully_cached_multi_track_album_bypasses_rip_queue_while_rip_queue_is_occupied() {
    let (orch, deps, state, _) = setup();
    let meta = |id: &str| FakeDeps::track_meta(id, "Track", "Artist");
    deps.albums.lock().unwrap().insert(
        "alb.bypass".into(),
        FakeDeps::album(vec![meta("byp_t1"), meta("byp_t2")]),
    );
    deps.cache_track("byp_t1", 101);
    deps.cache_track("byp_t2", 102);
    let hash = expected_generation_hash("alb.bypass", &["byp_t1", "byp_t2"]);
    state.lock().unwrap().found_albums.insert(
        "alb.bypass".into(),
        vec![
            cached_zip_row("alb.bypass", 1, 2, &hash),
            cached_zip_row("alb.bypass", 2, 2, &hash),
        ],
    );

    // Job A holds the rip queue with a gate
    let rip_gate = tokio_util::sync::CancellationToken::new();
    state.lock().unwrap().gate_uploads = None;
    deps.rip_scripts
        .lock()
        .unwrap()
        .insert("blocking_rip".into(), RipScript::OkWithFile(vec![1]));
    state.lock().unwrap().initial_rip_track = Some("blocking_rip".into());
    state.lock().unwrap().initial_rip_gate = Some(rip_gate.clone());

    let orch = Arc::new(orch);
    let first_orch = Arc::clone(&orch);
    let first_deps = Arc::clone(&deps);
    let first_task = tokio::spawn(async move {
        run_async(
            &first_orch,
            &first_deps,
            &options(vec![track_item("blocking_rip")], true),
        )
        .await
    });

    // Wait until Job A has started ripping and is holding the queue
    loop {
        let notified = deps.rip_notify.notified();
        if state.lock().unwrap().initial_rip_started {
            break;
        }
        notified.await;
    }

    // Now submit Job B (the 100% cached album). It must NOT wait behind Job A!
    let second_orch = Arc::clone(&orch);
    let second_deps = Arc::clone(&deps);
    let summary = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        run_async(
            &second_orch,
            &second_deps,
            &album_options("alb.bypass", false, false),
        ),
    )
    .await
    .expect("cached album job must not wait behind Job A in sequential rip queue")
    .expect("job succeeds");

    assert_eq!(summary.cached_count, 2);
    assert_eq!(summary.ripped_count, 0);

    // Release Job A so test finishes cleanly
    rip_gate.cancel();
    let _ = first_task.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inflight_duplicate_job_waits_for_primary_and_delivers_from_cache() {
    let (deps, state) = FakeDeps::new();
    let orch = Arc::new(RipOrchestrator::new(OrchestratorConfig::test()));

    let rip_gate = tokio_util::sync::CancellationToken::new();
    deps.rip_scripts
        .lock()
        .unwrap()
        .insert("shared_dup".into(), RipScript::OkWithFile(vec![1, 2]));
    state.lock().unwrap().initial_rip_track = Some("shared_dup".into());
    state.lock().unwrap().initial_rip_gate = Some(rip_gate.clone());
    state
        .lock()
        .unwrap()
        .send_audio_results
        .push_back(FakeDeps::upload_ok());

    let o1 = Arc::clone(&orch);
    let d1 = Arc::clone(&deps);
    let opts1 = options(vec![track_item("shared_dup")], true);
    let task1 = tokio::spawn(async move { o1.start_job(d1, &opts1).await });

    // Wait until job 1 starts ripping
    loop {
        let notified = deps.rip_notify.notified();
        if state.lock().unwrap().initial_rip_started {
            break;
        }
        notified.await;
    }

    // Now job 2 requests the same item
    let o2 = Arc::clone(&orch);
    let d2 = Arc::clone(&deps);
    let opts2 = options(vec![track_item("shared_dup")], false);
    let task2 = tokio::spawn(async move { o2.start_job(d2, &opts2).await });

    // Wait a brief moment and verify job 2 is WaitingDuplicate
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let active_jobs = orch.get_active_jobs();
    assert_eq!(active_jobs.len(), 2);
    let job2 = active_jobs
        .iter()
        .find(|j| j.phase == JobPhase::WaitingDuplicate)
        .expect("job 2 in WaitingDuplicate");
    assert_eq!(job2.phase, JobPhase::WaitingDuplicate);

    // Release job 1's rip gate
    rip_gate.cancel();
    let res1 = task1.await.unwrap().expect("job 1 succeeds");
    assert_eq!(res1.ripped_count, 1);

    // Job 2 should now awaken and complete from cache!
    let res2 = tokio::time::timeout(std::time::Duration::from_secs(3), task2)
        .await
        .expect("job 2 must complete quickly after job 1 finishes")
        .unwrap()
        .expect("job 2 succeeds from cache");
    assert_eq!(res2.cached_count, 1);
    assert_eq!(res2.ripped_count, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inflight_duplicate_job_can_be_cancelled_independently() {
    let (deps, state) = FakeDeps::new();
    let orch = Arc::new(RipOrchestrator::new(OrchestratorConfig::test()));

    let rip_gate = tokio_util::sync::CancellationToken::new();
    deps.rip_scripts
        .lock()
        .unwrap()
        .insert("cancel_dup".into(), RipScript::OkWithFile(vec![1, 2]));
    state.lock().unwrap().initial_rip_track = Some("cancel_dup".into());
    state.lock().unwrap().initial_rip_gate = Some(rip_gate.clone());
    state
        .lock()
        .unwrap()
        .send_audio_results
        .push_back(FakeDeps::upload_ok());

    let o1 = Arc::clone(&orch);
    let d1 = Arc::clone(&deps);
    let opts1 = options(vec![track_item("cancel_dup")], true);
    let task1 = tokio::spawn(async move { o1.start_job(d1, &opts1).await });

    loop {
        let notified = deps.rip_notify.notified();
        if state.lock().unwrap().initial_rip_started {
            break;
        }
        notified.await;
    }

    let o2 = Arc::clone(&orch);
    let d2 = Arc::clone(&deps);
    let opts2 = options(vec![track_item("cancel_dup")], false);
    let task2 = tokio::spawn(async move { o2.start_job(d2, &opts2).await });

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let active_jobs = orch.get_active_jobs();
    let job2 = active_jobs
        .iter()
        .find(|j| j.phase == JobPhase::WaitingDuplicate)
        .expect("job 2 in WaitingDuplicate");
    let job2_id = job2.id.clone();

    // Cancel job 2 independently
    assert!(orch.cancel_job(&job2_id, Some("tester")));

    let res2 = task2.await.unwrap();
    assert!(
        res2.is_err(),
        "cancelled duplicate returns error / cancelled"
    );

    // Job 1 should still be running and completes when unblocked
    rip_gate.cancel();
    let res1 = task1.await.unwrap().expect("job 1 completes unaffected");
    assert_eq!(res1.ripped_count, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inflight_duplicate_job_takes_over_rip_if_primary_fails() {
    let (deps, state) = FakeDeps::new();
    let orch = Arc::new(RipOrchestrator::new(OrchestratorConfig::test()));

    let rip_gate = tokio_util::sync::CancellationToken::new();
    deps.rip_scripts
        .lock()
        .unwrap()
        .insert("fail_dup".into(), RipScript::OkWithFile(vec![1, 2]));
    state.lock().unwrap().initial_rip_track = Some("fail_dup".into());
    state.lock().unwrap().initial_rip_gate = Some(rip_gate.clone());
    state
        .lock()
        .unwrap()
        .send_audio_results
        .push_back(FakeDeps::upload_ok());

    let o1 = Arc::clone(&orch);
    let d1 = Arc::clone(&deps);
    let opts1 = options(vec![track_item("fail_dup")], true);
    let task1 = tokio::spawn(async move { o1.start_job(d1, &opts1).await });

    loop {
        let notified = deps.rip_notify.notified();
        if state.lock().unwrap().initial_rip_started {
            break;
        }
        notified.await;
    }

    let o2 = Arc::clone(&orch);
    let d2 = Arc::clone(&deps);
    let opts2 = options(vec![track_item("fail_dup")], false);
    let task2 = tokio::spawn(async move { o2.start_job(d2, &opts2).await });

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let active_jobs = orch.get_active_jobs();
    let job1 = active_jobs
        .iter()
        .find(|j| j.phase != JobPhase::WaitingDuplicate)
        .expect("job 1 active");
    let job1_id = job1.id.clone();

    // Cancel job 1 (the primary)
    assert!(orch.cancel_job(&job1_id, Some("tester")));
    rip_gate.cancel();
    let _ = task1.await;

    // Job 2 should take over, rip "fail_dup", and complete!
    let res2 = tokio::time::timeout(std::time::Duration::from_secs(3), task2)
        .await
        .expect("job 2 must take over and complete")
        .unwrap()
        .expect("job 2 succeeds as active ripper");
    assert_eq!(res2.ripped_count, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_track_cached_album_bypasses_rip_queue_while_rip_queue_is_occupied() {
    let (orch, deps, state, _) = setup();
    let rip_gate = tokio_util::sync::CancellationToken::new();
    state.lock().unwrap().initial_rip_track = Some("blocking_rip_single".into());
    state.lock().unwrap().initial_rip_gate = Some(rip_gate.clone());

    let orch = Arc::new(orch);

    // Job 1 occupies the serial rip queue
    let o1 = Arc::clone(&orch);
    let d1 = Arc::clone(&deps);
    let opts1 = options(vec![track_item("blocking_rip_single")], false);
    let task1 = tokio::spawn(async move { o1.start_job(d1, &opts1).await });

    // Wait until Job 1 has actually entered lane one (ripping)
    loop {
        let notified = deps.rip_notify.notified();
        if state.lock().unwrap().initial_rip_started {
            break;
        }
        notified.await;
    }

    // Job 2 is a single-track album that is already cached
    deps.albums.lock().unwrap().insert(
        "alb.single_cached".into(),
        FakeDeps::album(vec![FakeDeps::track_meta(
            "single_t1",
            "Ausiko Raat",
            "Studio King",
        )]),
    );
    deps.cache_track("single_t1", 9999);

    let o2 = Arc::clone(&orch);
    let d2 = Arc::clone(&deps);
    let mut opts2 = album_options("alb.single_cached", false, false);
    opts2.delivery_chat_id = 777;
    let task2 = tokio::spawn(async move { o2.start_job(d2, &opts2).await });

    // Job 2 MUST complete immediately via the cache delivery lane
    let res2 = tokio::time::timeout(std::time::Duration::from_secs(2), task2)
        .await
        .expect("single-track cached album must bypass rip queue without waiting")
        .unwrap()
        .expect("cached single-track album succeeds");

    assert_eq!(res2.cached_count, 1);
    assert_eq!(res2.ripped_count, 0);

    // Verify copy was delivered to chat 777
    assert!(
        state
            .lock()
            .unwrap()
            .copies
            .iter()
            .any(|(chat, msg, _, _)| *chat == 777 && *msg == 9999),
        "cached track 9999 must have been delivered to chat 777"
    );

    // Unblock Job 1 so it finishes
    rip_gate.cancel();
    let res1 = task1.await.unwrap().expect("job 1 completes");
    assert_eq!(res1.ripped_count, 1);
}
