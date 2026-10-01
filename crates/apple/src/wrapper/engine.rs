//! High-level native wrapper-lite engine.
//!
//! Coordinates:
//! 1. Fetching HLS playlist from wrapper-lite
//! 2. Parsing ALAC stream variant and segment byte ranges
//! 3. Fetching FairPlay key templates from wrapper-lite
//! 4. Downloading fragmented MP4 from Apple CDN
//! 5. Decrypting audio samples using Temari
//! 6. Returning an `AudioStreamSource` compatible with the ripper pipeline.

use std::{collections::HashMap, sync::Arc, time::Duration};

use bytes::Bytes;
use engine::{
    orchestrator::types::{ByteProgress, RipActivity, TrackLabel},
    streaming::{AudioStreamSource, ProgressCallback, SourceId, StreamError},
};
use music::CodecPreference;
use tokio_util::sync::CancellationToken;
use tracing::debug;

use super::{
    client::{WrapperError, WrapperLiteClient, WrapperUnavailableReason},
    decryptor::{decrypt_fragment, transform_init_segment},
    playlist::{AlacStreamInfo, MediaPlaylistInfo, parse_master_playlist, parse_media_playlist},
};

pub struct WrapperEngine {
    client: WrapperLiteClient,
    http_client: reqwest::Client,
    source: SourceId,
}

/// Result of wrapper acquisition while retaining the only provenance that can
/// justify an optional-unavailable outcome.
#[derive(Debug)]
pub enum WrapperTrackOutcome {
    Source(AudioStreamSource),
    Unavailable(WrapperUnavailableReason),
}

impl WrapperEngine {
    pub fn new(wrapper_url: &str, api_key: Option<&str>) -> Self {
        let client = WrapperLiteClient::new(wrapper_url, api_key);
        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(60))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        let source = SourceId::WrapperLite {
            url: client.base_url().to_owned(),
        };
        Self {
            client,
            http_client,
            source,
        }
    }

    pub fn client(&self) -> &WrapperLiteClient {
        &self.client
    }

    /// Primary entry point: rips/decrypts track from wrapper-lite, returning an `AudioStreamSource`.
    ///
    /// `preference` selects the variant when the master playlist offers
    /// several (ALAC vs AAC vs Atmos).
    pub async fn rip_track(
        &self,
        track_id: &str,
        signal: Option<CancellationToken>,
        on_progress: Option<ProgressCallback>,
        preference: CodecPreference,
    ) -> Result<AudioStreamSource, StreamError> {
        let track = TrackLabel::new(format!("Track {track_id}"), String::new());
        match self
            .rip_track_with_outcome(track_id, &track, signal, on_progress, preference)
            .await?
        {
            WrapperTrackOutcome::Source(source) => Ok(source),
            WrapperTrackOutcome::Unavailable(reason) => {
                Err(StreamError::Unavailable(reason.to_string()))
            }
        }
    }

    /// Acquire a stream without erasing playlist-selection provenance.
    pub(crate) async fn rip_track_with_outcome(
        &self,
        track_id: &str,
        track: &TrackLabel,
        signal: Option<CancellationToken>,
        on_progress: Option<ProgressCallback>,
        preference: CodecPreference,
    ) -> Result<WrapperTrackOutcome, StreamError> {
        if signal.as_ref().is_some_and(|s| s.is_cancelled()) {
            return Err(StreamError::Cancelled);
        }

        report(
            on_progress.as_ref(),
            RipActivity::Connecting {
                track: track.clone(),
            },
        );

        let master_url = match self.client.fetch_m3u8_url(track_id).await {
            Ok(url) => url,
            Err(WrapperError::Unavailable(reason)) => {
                return Ok(WrapperTrackOutcome::Unavailable(reason));
            }
            Err(error) => return Err(self.map_client_error("fetch m3u8 url", error)),
        };

        debug!(track_id = %track_id, master_url = %master_url, "Fetched master m3u8 URL");

        let master_resp = self
            .http_client
            .get(&master_url)
            .send()
            .await
            .map_err(|e| self.map_reqwest_error("Fetch master playlist", e))?;
        let content_type = master_resp
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_owned();
        // Some music-video catalog entries resolve to a protected direct
        // media file rather than a playlist. Resolve their web playback audio
        // stream; the normal HLS decryption path handles it.
        let (stream_info, direct_media_info) =
            if is_direct_media_response(&content_type, &master_url) {
                self.webplayback_fallback(track_id, track, signal.as_ref(), on_progress.as_ref())
                    .await?
            } else {
                let master_text = master_resp
                    .text()
                    .await
                    .map_err(|e| self.map_reqwest_error("Read master playlist", e))?;

                // Stores with no lossless HLS may return a direct file or a
                // non-master response from /m3u8. Resolve the web playback
                // audio stream in those cases.
                match parse_master_playlist(&master_text, &master_url, preference) {
                    Ok(info) => (info, None),
                    Err(WrapperError::Unavailable(reason)) => {
                        return Ok(WrapperTrackOutcome::Unavailable(reason));
                    }
                    Err(error) => {
                        if !looks_like_master_playlist(&master_text) {
                            self.webplayback_fallback(
                                track_id,
                                track,
                                signal.as_ref(),
                                on_progress.as_ref(),
                            )
                            .await?
                        } else {
                            return Err(StreamError::PlaylistParse {
                                which: "master",
                                detail: error.to_string(),
                            });
                        }
                    }
                }
            };
        debug!(
            codec = %stream_info.codec,
            sample_rate = stream_info.sample_rate,
            bit_depth = stream_info.bit_depth,
            media_url = %stream_info.stream_url,
            "Selected audio stream playlist"
        );

        report(
            on_progress.as_ref(),
            RipActivity::Connecting {
                track: track.clone(),
            },
        );

        let media_info = if let Some(media_info) = direct_media_info {
            media_info
        } else {
            let media_resp = self
                .http_client
                .get(&stream_info.stream_url)
                .send()
                .await
                .map_err(|e| self.map_reqwest_error("Fetch media playlist", e))?;
            let media_text = media_resp
                .text()
                .await
                .map_err(|e| self.map_reqwest_error("Read media playlist", e))?;

            parse_media_playlist(&media_text, &stream_info.stream_url).map_err(|e| {
                StreamError::PlaylistParse {
                    which: "media",
                    detail: e.to_string(),
                }
            })?
        };

        // 5b. CENC (Widevine) playlists — the webplayback AAC path — take a
        // different decryption route than the FairPlay master variants.
        if media_info.key_method.as_deref() == Some("ISO-23001-7") {
            let kid_b64 =
                media_info
                    .cenc_kid_b64
                    .clone()
                    .ok_or_else(|| StreamError::PlaylistParse {
                        which: "media",
                        detail: "CENC playlist has no key id".to_owned(),
                    })?;
            return self
                .rip_cenc_stream(
                    track_id,
                    &media_info,
                    &kid_b64,
                    track,
                    signal.as_ref(),
                    on_progress.as_ref(),
                )
                .await
                .map(WrapperTrackOutcome::Source);
        }

        let mut key_templates: HashMap<String, Arc<temari::rounds::Template>> = HashMap::new();
        for seg in &media_info.segments {
            if let Some(key_uri) = &seg.key_uri
                && !key_templates.contains_key(key_uri)
            {
                let adam_for_key = if key_uri.contains("P000000000") || key_uri.ends_with("/s1/e1")
                {
                    "0"
                } else {
                    track_id
                };
                let tmpl = self
                    .client
                    .fetch_template(adam_for_key, key_uri)
                    .await
                    .map_err(|e| StreamError::Decrypt {
                        detail: format!("fetch template for {key_uri}: {e}"),
                    })?;
                key_templates.insert(key_uri.clone(), Arc::new(tmpl));
            }
        }

        report(
            on_progress.as_ref(),
            RipActivity::Downloading {
                track: track.clone(),
                progress: ByteProgress {
                    completed: 0,
                    total: None,
                },
            },
        );

        // In Apple Music, media_info.single_file_url is almost always present
        let (decrypted_bytes, sample_rate) = if let Some(single_url) = &media_info.single_file_url {
            let resp = self
                .http_client
                .get(single_url)
                .send()
                .await
                .map_err(|e| self.map_reqwest_error("Download audio stream", e))?;
            let raw_data = resp
                .bytes()
                .await
                .map_err(|e| self.map_reqwest_error("Read audio stream bytes", e))?;

            report(
                on_progress.as_ref(),
                RipActivity::Decrypting {
                    track: track.clone(),
                },
            );

            self.decrypt_single_file_stream(&raw_data, &media_info, &key_templates, track_id)?
        } else {
            // Segment-by-segment download fallback
            self.decrypt_multi_segment_stream(
                &media_info,
                &key_templates,
                track_id,
                track,
                on_progress.as_ref(),
            )
            .await?
        };

        let total_size = decrypted_bytes.len() as u64;
        let stream = Box::pin(futures_util::stream::once(async move {
            Ok(Bytes::from(decrypted_bytes))
        }));

        Ok(WrapperTrackOutcome::Source(AudioStreamSource {
            stream,
            source: self.source.clone(),
            codec: stream_info.codec,
            bit_depth: stream_info.bit_depth,
            sample_rate: sample_rate.unwrap_or(stream_info.sample_rate),
            content_length: Some(total_size),
        }))
    }

    fn map_client_error(&self, what: &str, error: WrapperError) -> StreamError {
        match error {
            WrapperError::Http(error) => {
                if error.is_timeout() {
                    return StreamError::Timeout {
                        source: self.source.clone(),
                        secs: 30,
                    };
                }
                if error.is_connect() {
                    // The wrapper-lite relay itself is unreachable: the
                    // service is offline, not flaky.
                    return StreamError::SourceOffline {
                        source: self.source.clone(),
                    };
                }
                StreamError::Message(format!("{what}: {error}"))
            }
            WrapperError::Auth { .. } => StreamError::Authentication {
                source: self.source.clone(),
            },
            WrapperError::Api { code, message } => StreamError::Message(format!(
                "{what}: wrapper API error (code {code}): {message}"
            )),
            other => StreamError::Message(format!("{what}: {other}")),
        }
    }

    fn map_reqwest_error(&self, what: &str, error: reqwest::Error) -> StreamError {
        if error.is_timeout() {
            return StreamError::Timeout {
                source: self.source.clone(),
                secs: 60,
            };
        }
        if error.is_connect() {
            return StreamError::Message(format!("{what}: {error}"));
        }
        if let Some(status) = error.status() {
            let code = status.as_u16();
            if code == 401 || code == 403 {
                return StreamError::Authentication {
                    source: self.source.clone(),
                };
            }
        }
        StreamError::Message(format!("{what}: {error}"))
    }

    /// Resolve the web playback audio when `/m3u8` returns a protected direct
    /// file or no usable audio master. Web playback may return either a master
    /// playlist or the selected AAC media playlist directly.
    async fn webplayback_fallback(
        &self,
        track_id: &str,
        track: &TrackLabel,
        signal: Option<&CancellationToken>,
        on_progress: Option<&ProgressCallback>,
    ) -> Result<(AlacStreamInfo, Option<MediaPlaylistInfo>), StreamError> {
        if signal.is_some_and(|t| t.is_cancelled()) {
            return Err(StreamError::Cancelled);
        }
        report(
            on_progress,
            RipActivity::Connecting {
                track: track.clone(),
            },
        );
        let master_url = self
            .client
            .fetch_webplayback(track_id)
            .await
            .map_err(|e| self.map_client_error("fetch web playback", e))?;
        debug!(track_id = %track_id, master_url = %master_url, "Using web playback fallback");
        let response = self
            .http_client
            .get(&master_url)
            .send()
            .await
            .map_err(|error| self.map_reqwest_error("Fetch web playback master", error))?
            .error_for_status()
            .map_err(|error| self.map_reqwest_error("Fetch web playback master", error))?;
        let master_text = response
            .text()
            .await
            .map_err(|error| self.map_reqwest_error("Read web playback master", error))?;
        match parse_master_playlist(&master_text, &master_url, CodecPreference::HighestQuality) {
            Ok(info) => Ok((info, None)),
            Err(error) if !looks_like_master_playlist(&master_text) => {
                let media_info = parse_media_playlist(&master_text, &master_url).map_err(
                    |media_error| StreamError::PlaylistParse {
                        which: "web playback playlist",
                        detail: format!(
                            "response is neither a master playlist ({error}) nor a media playlist ({media_error})"
                        ),
                    },
                )?;
                if media_info.segments.is_empty() {
                    return Err(StreamError::PlaylistParse {
                        which: "web playback playlist",
                        detail: "media playlist contains no audio segments".to_owned(),
                    });
                }
                debug!(
                    track_id = %track_id,
                    segments = media_info.segments.len(),
                    "Using direct web playback media playlist"
                );
                Ok((
                    AlacStreamInfo {
                        stream_url: master_url,
                        codec: "mp4a.40.2".to_owned(),
                        sample_rate: 44_100,
                        bit_depth: 16,
                    },
                    Some(media_info),
                ))
            }
            Err(error) => Err(StreamError::PlaylistParse {
                which: "web playback master",
                detail: error.to_string(),
            }),
        }
    }

    /// The CENC (Widevine) route: fetch a license from wrapper-lite,
    /// unwrap the content key, download the whole media file, decrypt each
    /// fragment with AES-CTR and return the reassembled stream.
    async fn rip_cenc_stream(
        &self,
        track_id: &str,
        media_info: &super::playlist::MediaPlaylistInfo,
        kid_b64: &str,
        track: &TrackLabel,
        signal: Option<&CancellationToken>,
        on_progress: Option<&ProgressCallback>,
    ) -> Result<AudioStreamSource, StreamError> {
        use base64::{Engine, engine::general_purpose::STANDARD as B64};

        if signal.is_some_and(|t| t.is_cancelled()) {
            return Err(StreamError::Cancelled);
        }
        report(
            on_progress,
            RipActivity::Decrypting {
                track: track.clone(),
            },
        );

        let kid = B64.decode(kid_b64).map_err(|e| StreamError::Decrypt {
            detail: format!("decode CENC key id: {e}"),
        })?;
        let mut cdm = super::widevine::Cdm::new(&kid).map_err(|e| StreamError::Decrypt {
            detail: format!("init Widevine CDM: {e}"),
        })?;
        let challenge = cdm.license_request().map_err(|e| StreamError::Decrypt {
            detail: format!("build license request: {e}"),
        })?;

        // wrapper-lite forwards the playlist's original EXT-X-KEY URI
        // ("data:;base64,<kid>") verbatim to Apple; the PSSH built above
        // travels only inside the challenge.
        let key_uri = media_info
            .segments
            .iter()
            .find_map(|seg| seg.key_uri.clone())
            .unwrap_or_else(|| format!("data:;base64,{kid_b64}"));

        let license_b64 = self
            .client
            .fetch_license(track_id, &challenge, &key_uri)
            .await
            .map_err(|e| self.map_license_error(e))?;

        let content_keys = cdm
            .content_keys(&license_b64)
            .map_err(|e| StreamError::License {
                detail: format!("unwrap license keys: {e}"),
            })?;
        let content_key = content_keys
            .iter()
            .find(|k| k.key_id == kid)
            .map(|k| k.value)
            .or_else(|| content_keys.first().map(|k| k.value))
            .ok_or_else(|| StreamError::License {
                detail: "license contained no content key".to_owned(),
            })?;

        if signal.is_some_and(|t| t.is_cancelled()) {
            return Err(StreamError::Cancelled);
        }
        report(
            on_progress,
            RipActivity::Downloading {
                track: track.clone(),
                progress: ByteProgress {
                    completed: 0,
                    total: None,
                },
            },
        );

        // Whole-file layout: init (EXT-X-MAP byterange) + fragments
        // (EXT-X-BYTERANGE) all reference one file.
        let single_url = media_info
            .single_file_url
            .as_deref()
            .or(media_info.segments.first().map(|s| s.uri.as_str()))
            .ok_or_else(|| StreamError::PlaylistParse {
                which: "media",
                detail: "CENC playlist has no segments".to_owned(),
            })?;
        let resp = self
            .http_client
            .get(single_url)
            .send()
            .await
            .map_err(|e| self.map_reqwest_error("Download audio stream", e))?;
        let expected_len = resp.content_length();
        let raw_data = resp
            .bytes()
            .await
            .map_err(|e| self.map_reqwest_error("Read audio stream bytes", e))?;
        if let Some(expected) = expected_len
            && (raw_data.len() as u64) < expected
        {
            return Err(StreamError::Decrypt {
                detail: format!(
                    "audio stream truncated: got {} of {} advertised bytes ({} missing)",
                    raw_data.len(),
                    expected,
                    expected - raw_data.len() as u64
                ),
            });
        }

        report(
            on_progress,
            RipActivity::Decrypting {
                track: track.clone(),
            },
        );

        let (init_offset, init_len) = media_info.init_byte_range.unwrap_or((0, 1037));
        if raw_data.len() < (init_offset + init_len) as usize {
            return Err(StreamError::Decrypt {
                detail: "raw stream shorter than init segment".to_owned(),
            });
        }
        let init_raw = &raw_data[init_offset as usize..(init_offset + init_len) as usize];
        let transformed_init = super::decryptor::transform_init_segment(init_raw).map_err(|e| {
            StreamError::Decrypt {
                detail: format!("transform init segment: {e}"),
            }
        })?;

        let mut output = Vec::with_capacity(raw_data.len());
        output.extend_from_slice(&transformed_init);

        for (i, seg) in media_info.segments.iter().enumerate() {
            if signal.is_some_and(|t| t.is_cancelled()) {
                return Err(StreamError::Cancelled);
            }
            let (off, len) = seg.byte_range.ok_or_else(|| StreamError::Decrypt {
                detail: format!("segment {i} missing byte range"),
            })?;
            let start = off as usize;
            let end = (off + len) as usize;
            if end > raw_data.len() {
                return Err(StreamError::Decrypt {
                    detail: format!(
                        "segment {i} range {start}..{end} out of bounds ({})",
                        raw_data.len()
                    ),
                });
            }
            let mut frag = raw_data[start..end].to_vec();
            super::cenc::decrypt_cenc_fragment(&mut frag, &content_key).map_err(|e| {
                StreamError::Decrypt {
                    detail: format!("decrypt segment {i}: {e}"),
                }
            })?;
            super::cenc::strip_encryption_boxes(&mut frag).map_err(|e| StreamError::Decrypt {
                detail: format!("strip segment {i} boxes: {e}"),
            })?;
            output.extend_from_slice(&frag);
        }

        let stamped = stamp_fragment_duration(&mut output, track_id);
        let total_size = output.len() as u64;
        // Diagnostics: the size of the assembled stream, how it compares to the
        // source and to the sum of the playlist's declared segment ranges. A
        // mismatch here is the earliest signal that assembly dropped or
        // duplicated audio, well before tagging reports a missing duration.
        tracing::debug!(
            track_id,
            source_bytes = raw_data.len(),
            assembled_bytes = output.len(),
            segments = media_info.segments.len(),
            declared_segment_bytes = media_info
                .segments
                .iter()
                .filter_map(|s| s.byte_range.map(|(_, len)| len))
                .sum::<u64>(),
            init_bytes = transformed_init.len(),
            final_bytes = total_size,
            duration_secs = stamped,
            "apple stream assembly sizes"
        );
        let stream = Box::pin(futures_util::stream::once(async move {
            Ok(Bytes::from(output))
        }));

        Ok(AudioStreamSource {
            stream,
            source: self.source.clone(),
            codec: "mp4a.40.2".to_owned(),
            bit_depth: 16,
            sample_rate: 44_100,
            content_length: Some(total_size),
        })
    }

    fn map_license_error(&self, error: WrapperError) -> StreamError {
        match error {
            WrapperError::Http(error) => {
                if error.is_timeout() {
                    return StreamError::Timeout {
                        source: self.source.clone(),
                        secs: 30,
                    };
                }
                StreamError::License {
                    detail: format!("fetch license: {error}"),
                }
            }
            WrapperError::Auth { status } => StreamError::License {
                detail: format!("license rejected (HTTP {status})"),
            },
            WrapperError::Api { code, message } => StreamError::License {
                detail: format!("license error (code {code}): {message}"),
            },
            other => StreamError::License {
                detail: other.to_string(),
            },
        }
    }

    fn decrypt_single_file_stream(
        &self,
        raw_data: &[u8],
        media_info: &super::playlist::MediaPlaylistInfo,
        key_templates: &HashMap<String, Arc<temari::rounds::Template>>,
        track_id: &str,
    ) -> Result<(Vec<u8>, Option<u32>), StreamError> {
        // Extract and transform init segment
        let (init_offset, init_len) = media_info.init_byte_range.unwrap_or((0, 1037));
        if raw_data.len() < (init_offset + init_len) as usize {
            return Err(StreamError::Decrypt {
                detail: "raw stream shorter than init segment".to_owned(),
            });
        }
        let init_raw = &raw_data[init_offset as usize..(init_offset + init_len) as usize];
        let sample_rate = super::decryptor::audio_sample_rate(init_raw);
        let transformed_init =
            transform_init_segment(init_raw).map_err(|e| StreamError::Decrypt {
                detail: format!("transform init segment: {e}"),
            })?;

        let mut output = Vec::with_capacity(raw_data.len());
        output.extend_from_slice(&transformed_init);

        // Track default template if available
        let default_template = key_templates
            .iter()
            .find(|(k, _)| !k.contains("P000000000"))
            .map(|(_, v)| v.clone())
            .or_else(|| key_templates.values().next().cloned())
            .ok_or_else(|| StreamError::Decrypt {
                detail: "no FairPlay decryption template available".to_owned(),
            })?;

        // Process each media fragment
        for (i, seg) in media_info.segments.iter().enumerate() {
            let (off, len) = seg.byte_range.ok_or_else(|| StreamError::Decrypt {
                detail: format!("segment {i} missing byte range"),
            })?;
            let start = off as usize;
            let end = (off + len) as usize;
            if end > raw_data.len() {
                return Err(StreamError::Decrypt {
                    detail: format!(
                        "segment {i} range {start}..{end} out of bounds ({})",
                        raw_data.len()
                    ),
                });
            }
            let frag_raw = &raw_data[start..end];

            let tmpl = seg
                .key_uri
                .as_ref()
                .and_then(|uri| key_templates.get(uri))
                .unwrap_or(&default_template);

            let decrypted_frag =
                decrypt_fragment(frag_raw, tmpl).map_err(|e| StreamError::Decrypt {
                    detail: format!("decrypt fragment {i}: {e}"),
                })?;

            output.extend_from_slice(&decrypted_frag);
        }

        let _ = super::decryptor::normalize_fragment_start_time(&mut output);
        let _ = stamp_fragment_duration(&mut output, track_id);
        Ok((output, sample_rate))
    }

    async fn decrypt_multi_segment_stream(
        &self,
        media_info: &super::playlist::MediaPlaylistInfo,
        key_templates: &HashMap<String, Arc<temari::rounds::Template>>,
        track_id: &str,
        track: &TrackLabel,
        on_progress: Option<&ProgressCallback>,
    ) -> Result<(Vec<u8>, Option<u32>), StreamError> {
        report(
            on_progress,
            RipActivity::Decrypting {
                track: track.clone(),
            },
        );
        let init_resp = self
            .http_client
            .get(&media_info.init_uri)
            .send()
            .await
            .map_err(|e| self.map_reqwest_error("Fetch init segment", e))?;
        let init_bytes = init_resp
            .bytes()
            .await
            .map_err(|e| self.map_reqwest_error("Read init bytes", e))?;
        let sample_rate = super::decryptor::audio_sample_rate(&init_bytes);
        let transformed_init =
            transform_init_segment(&init_bytes).map_err(|e| StreamError::Decrypt {
                detail: format!("transform init segment: {e}"),
            })?;

        let mut output = Vec::new();
        output.extend_from_slice(&transformed_init);

        let default_template = key_templates
            .iter()
            .find(|(k, _)| !k.contains("P000000000"))
            .map(|(_, v)| v.clone())
            .or_else(|| key_templates.values().next().cloned())
            .ok_or_else(|| StreamError::Decrypt {
                detail: "no FairPlay template available".to_owned(),
            })?;

        for (i, seg) in media_info.segments.iter().enumerate() {
            let seg_resp = self
                .http_client
                .get(&seg.uri)
                .send()
                .await
                .map_err(|e| self.map_reqwest_error("Fetch segment", e))?;
            let seg_bytes = seg_resp
                .bytes()
                .await
                .map_err(|e| self.map_reqwest_error("Read segment bytes", e))?;

            let tmpl = seg
                .key_uri
                .as_ref()
                .and_then(|uri| key_templates.get(uri))
                .unwrap_or(&default_template);

            let dec = decrypt_fragment(&seg_bytes, tmpl).map_err(|e| StreamError::Decrypt {
                detail: format!("decrypt segment {i}: {e}"),
            })?;
            output.extend_from_slice(&dec);
        }

        let _ = super::decryptor::normalize_fragment_start_time(&mut output);
        let _ = stamp_fragment_duration(&mut output, track_id);
        Ok((output, sample_rate))
    }
}

fn report(callback: Option<&ProgressCallback>, activity: RipActivity) {
    if let Some(callback) = callback {
        callback(activity);
    }
}

fn is_direct_media_response(content_type: &str, media_url: &str) -> bool {
    let media_type = content_type
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    if media_type.starts_with("video/")
        || media_type.starts_with("audio/")
        || matches!(media_type.as_str(), "application/mp4" | "application/x-m4v")
    {
        return true;
    }

    let path = media_url
        .split(['?', '#'])
        .next()
        .unwrap_or(media_url)
        .to_ascii_lowercase();
    [".m4v", ".mp4", ".mov", ".m4a", ".mp3"]
        .iter()
        .any(|extension| path.ends_with(extension))
}

/// True when the response is a master playlist rather than a direct
/// media file. A master starts with `#EXTM3U` and carries
/// `#EXT-X-STREAM-INF` lines.
fn looks_like_master_playlist(text: &str) -> bool {
    let head = text.trim_start();
    head.starts_with("#EXTM3U") && text.contains("#EXT-X-STREAM-INF")
}

/// Give the assembled stream a real duration, in place.
///
/// Apple's HLS output is a *fragmented* MP4: an initial `moov` followed by
/// many `moof`/`mdat` pairs. A fragmented container carries no duration — the
/// `mvhd` and `mdhd` fields are present but zero — so every reader reports a
/// track of length zero, and the rip pipeline rejects it as having no duration
/// even though the audio is perfect.
///
/// This used to shell out to MP4Box or ffmpeg to remux, and fell back to
/// returning the fragmented bytes untouched when neither was installed. That
/// fallback was silent at `debug` level and is what made this failure so hard
/// to see: the pipeline produced a file it could not itself read. Recovering
/// the duration from the fragments is exact, needs no external binary, and
/// fixes the container header rather than papering over it.
fn stamp_fragment_duration(assembled: &mut [u8], track_id: &str) -> Option<f64> {
    match media::stamp_fragmented_duration(assembled) {
        Some(duration) => {
            debug!(
                track_id,
                duration_secs = duration,
                "recovered fragmented MP4 duration"
            );
            Some(duration)
        }
        None => {
            debug!(
                track_id,
                bytes = assembled.len(),
                "stream carries no fragment metadata; leaving duration as-is"
            );
            None
        }
    }
}
