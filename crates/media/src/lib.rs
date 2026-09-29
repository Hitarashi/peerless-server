//! Native media operations used by the bot.
//!
//! This crate owns decoder, FFT, image, and container implementation details.
//! Callers receive domain values and committed output paths only.

use std::{
    path::{Path, PathBuf},
    sync::{Arc, OnceLock},
};

use tokio::{sync::Semaphore, task::spawn_blocking};
use tokio_util::sync::CancellationToken;

mod decode;
mod fragmented;
mod spectrogram;
mod tags;

pub use fragmented::stamp_fragmented_duration;

static MEDIA_SLOTS: OnceLock<Arc<Semaphore>> = OnceLock::new();

fn media_slot() -> Arc<Semaphore> {
    MEDIA_SLOTS
        .get_or_init(|| Arc::new(Semaphore::new(2)))
        .clone()
}

#[derive(Debug, thiserror::Error)]
pub enum MediaError {
    #[error("operation cancelled")]
    Cancelled,
    #[error("unsupported audio format")]
    UnsupportedFormat,
    #[error("audio decode failed: {0}")]
    Decode(String),
    #[error("spectrogram render failed: {0}")]
    Render(String),
    #[error("invalid media: {0}")]
    Invalid(String),
    /// Decode/structure failure while validating the original downloaded
    /// source, before any metadata is written. This provenance lets callers
    /// distinguish source corruption from a later finalization failure.
    #[error(transparent)]
    SourceValidation(SourceValidationError),
    #[error("metadata update failed: {0}")]
    Metadata(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

/// Failures that prove the original downloaded media is corrupt.
#[derive(Debug, thiserror::Error)]
pub enum SourceValidationError {
    #[error("audio decode failed: {0}")]
    Decode(String),
    #[error("invalid media: {0}")]
    Invalid(String),
}

#[derive(Clone, Debug, PartialEq)]
pub struct AudioInfo {
    pub codec: String,
    pub sample_rate: u32,
    pub channels: u32,
    pub bit_depth: Option<u32>,
    pub duration_secs: f64,
    pub title: Option<String>,
    pub artist: Option<String>,
    pub album: Option<String>,
}

#[derive(Clone, Debug)]
pub struct SpectrogramOptions {
    pub title: Option<String>,
    pub comment: Option<String>,
    pub width: u32,
    pub height: u32,
    pub dynamic_range_db: f32,
    pub max_duration_secs: Option<f64>,
}

impl Default for SpectrogramOptions {
    fn default() -> Self {
        Self {
            title: None,
            comment: None,
            width: 1200,
            height: 551,
            dynamic_range_db: 120.0,
            max_duration_secs: None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct SpectrogramReport {
    pub info: AudioInfo,
    pub output: PathBuf,
}

/// Semantic metadata accepted by native M4A/FLAC/MP3 finalization.
///
/// The fields intentionally mirror the useful metadata fields while
/// remaining provider-neutral. Empty strings are ignored.
#[derive(Clone, Debug, Default)]
pub struct TrackTags {
    pub title: Option<String>,
    pub title_sort: Option<String>,
    pub artist: Option<String>,
    pub artist_sort: Option<String>,
    pub album: Option<String>,
    pub album_sort: Option<String>,
    pub album_artist: Option<String>,
    pub album_artist_sort: Option<String>,
    pub release_date: Option<String>,
    pub genre: Option<String>,
    pub composer: Option<String>,
    pub composer_sort: Option<String>,
    pub track_number: Option<u16>,
    pub track_count: Option<u16>,
    pub disc_number: Option<u16>,
    pub disc_count: Option<u16>,
    pub lyrics: Option<String>,
    /// Embedded cover bytes. JPEG and PNG are detected automatically.
    pub artwork_jpeg: Option<Vec<u8>>,
    pub isrc: Option<String>,
    pub label: Option<String>,
    pub copyright: Option<String>,
    pub publisher: Option<String>,
    pub performer: Option<String>,
    pub release_time: Option<String>,
    pub upc: Option<String>,
    pub song_id: Option<u64>,
    pub album_id: Option<u64>,
    pub artist_id: Option<u64>,
    pub explicit: Option<bool>,
    pub advisory: Option<AdvisoryKind>,
    pub media_kind: Option<MediaKind>,
    pub compilation: Option<bool>,
    pub gapless: Option<bool>,
    pub genre_id: Option<u32>,
    pub storefront_id: Option<u32>,
    pub encoder: Option<String>,
    pub comment: Option<String>,
    pub description: Option<String>,
}

/// Parental-control rating represented by the iTunes `rtng` atom.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AdvisoryKind {
    Explicit,
    Clean,
    Inoffensive,
}

/// Media type written to the iTunes `stik` atom without exposing
/// `mp4ameta`'s representation through the media seam.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MediaKind {
    Music,
    Audiobook,
    MusicVideo,
    Movie,
}

#[derive(Clone, Debug)]
pub struct ValidatedM4a {
    path: PathBuf,
    info: AudioInfo,
}

impl ValidatedM4a {
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn info(&self) -> &AudioInfo {
        &self.info
    }
}

/// Deep native media interface. Decoder and renderer details remain private.
#[derive(Clone, Default)]
pub struct MediaProcessor;

impl MediaProcessor {
    pub fn new() -> Self {
        Self
    }

    pub async fn inspect(
        &self,
        source: &Path,
        cancellation: &CancellationToken,
    ) -> Result<AudioInfo, MediaError> {
        let permit = media_slot()
            .acquire_owned()
            .await
            .map_err(|_| MediaError::Decode("media worker unavailable".into()))?;
        let source = source.to_owned();
        let cancellation = cancellation.clone();
        let result = spawn_blocking(move || decode::inspect_sync(&source, &cancellation))
            .await
            .map_err(|error| MediaError::Decode(error.to_string()))?;
        drop(permit);
        result
    }

    pub async fn render_spectrogram(
        &self,
        source: &Path,
        destination: &Path,
        options: &SpectrogramOptions,
        cancellation: &CancellationToken,
    ) -> Result<SpectrogramReport, MediaError> {
        let permit = media_slot()
            .acquire_owned()
            .await
            .map_err(|_| MediaError::Render("media worker unavailable".into()))?;
        let source = source.to_owned();
        let destination = destination.to_owned();
        let options = options.clone();
        let cancellation = cancellation.clone();
        let result = spawn_blocking(move || {
            spectrogram::render_spectrogram_sync(&source, &destination, &options, &cancellation)
        })
        .await
        .map_err(|error| MediaError::Render(error.to_string()))?;
        drop(permit);
        result
    }

    /// Validate, tag, and commit an audio rip. The pipeline selects the
    /// best available codec (ALAC, ec-3, AAC, FLAC, MP3); finalize never gates on the
    /// codec itself — it validates container integrity and packet boundaries
    /// end-to-end before and after tagging.
    pub async fn finalize_m4a(
        &self,
        source: &Path,
        destination: &Path,
        tags: &TrackTags,
        cancellation: &CancellationToken,
    ) -> Result<ValidatedM4a, MediaError> {
        let permit = media_slot()
            .acquire_owned()
            .await
            .map_err(|_| MediaError::Metadata("media worker unavailable".into()))?;
        let source = source.to_owned();
        let destination = destination.to_owned();
        let tags = tags.clone();
        let cancellation = cancellation.clone();
        let result = spawn_blocking(move || {
            tags::finalize_m4a_sync(&source, &destination, &tags, &cancellation)
        })
        .await
        .map_err(|error| MediaError::Metadata(error.to_string()))?;
        drop(permit);
        result
    }
}

fn mark_source_validation(error: MediaError) -> MediaError {
    match error {
        MediaError::Decode(message) => {
            MediaError::SourceValidation(SourceValidationError::Decode(message))
        }
        MediaError::Invalid(message) => {
            MediaError::SourceValidation(SourceValidationError::Invalid(message))
        }
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wav_fixture(path: &Path) {
        let sample_rate = 8_000u32;
        let samples: Vec<i16> = (0..sample_rate)
            .map(|index| {
                let phase = std::f32::consts::TAU * 440.0 * index as f32 / sample_rate as f32;
                (phase.sin() * 12_000.0) as i16
            })
            .collect();
        let data_len = (samples.len() * 2) as u32;
        let mut bytes = Vec::with_capacity(44 + data_len as usize);
        bytes.extend_from_slice(b"RIFF");
        bytes.extend_from_slice(&(36 + data_len).to_le_bytes());
        bytes.extend_from_slice(b"WAVEfmt ");
        bytes.extend_from_slice(&16u32.to_le_bytes());
        bytes.extend_from_slice(&1u16.to_le_bytes());
        bytes.extend_from_slice(&1u16.to_le_bytes());
        bytes.extend_from_slice(&sample_rate.to_le_bytes());
        bytes.extend_from_slice(&(sample_rate * 2).to_le_bytes());
        bytes.extend_from_slice(&2u16.to_le_bytes());
        bytes.extend_from_slice(&16u16.to_le_bytes());
        bytes.extend_from_slice(b"data");
        bytes.extend_from_slice(&data_len.to_le_bytes());
        for sample in samples {
            bytes.extend_from_slice(&sample.to_le_bytes());
        }
        std::fs::write(path, bytes).unwrap();
    }

    #[tokio::test]
    async fn native_probe_and_spectrogram_work_for_wav() {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("tone.wav");
        let output = dir.path().join("tone.png");
        wav_fixture(&input);
        let processor = MediaProcessor::new();
        let token = CancellationToken::new();
        let info = processor.inspect(&input, &token).await.unwrap();
        assert_eq!(info.sample_rate, 8_000);
        assert_eq!(info.channels, 1);
        assert!(info.duration_secs > 0.9);
        let report = processor
            .render_spectrogram(&input, &output, &SpectrogramOptions::default(), &token)
            .await
            .unwrap();
        assert_eq!(report.output, output);
        assert_eq!(std::fs::read(&output).unwrap()[..8], *b"\x89PNG\r\n\x1a\n");
    }

    #[tokio::test]
    async fn cancelled_native_operation_returns_cancelled() {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("missing.wav");
        let token = CancellationToken::new();
        token.cancel();
        assert!(matches!(
            MediaProcessor::new().inspect(&input, &token).await,
            Err(MediaError::Cancelled)
        ));
    }

    #[test]
    fn source_validation_provenance_is_distinct_from_post_tag_failures() {
        let source_decode =
            mark_source_validation(MediaError::Decode("unexpected end of bitstream".to_owned()));
        assert!(matches!(
            source_decode,
            MediaError::SourceValidation(SourceValidationError::Decode(message))
                if message == "unexpected end of bitstream"
        ));

        let source_invalid = mark_source_validation(MediaError::Invalid("no audio track".into()));
        assert!(matches!(
            source_invalid,
            MediaError::SourceValidation(SourceValidationError::Invalid(message))
                if message == "no audio track"
        ));

        let post_tag_decode = MediaError::Decode("finalized file could not be decoded".into());
        let post_tag_invalid = MediaError::Invalid("finalized file has no duration".into());
        assert!(!matches!(post_tag_decode, MediaError::SourceValidation(_)));
        assert!(!matches!(post_tag_invalid, MediaError::SourceValidation(_)));
    }

    #[tokio::test]
    async fn inspect_does_not_require_pcm_sample_decoding() {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("tone.wav");
        wav_fixture(&input);
        let processor = MediaProcessor::new();
        let token = CancellationToken::new();
        let info = processor.inspect(&input, &token).await.unwrap();
        assert_eq!(info.sample_rate, 8_000);
        assert_eq!(info.channels, 1);
        assert!(info.duration_secs > 0.9);
    }
}
