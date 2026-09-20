//! Native media operations used by the bot.
//!
//! This crate owns decoder, FFT, image, and container implementation details.
//! Callers receive domain values and committed output paths only.

use std::{
    fs::File,
    path::{Path, PathBuf},
    sync::{Arc, OnceLock},
};

use font8x8::UnicodeFonts;
use id3::TagLike;
use realfft::RealFftPlanner;
use symphonia::core::{
    audio::sample::Sample,
    codecs::audio::{
        AudioCodecId, AudioDecoderOptions,
        well_known::{
            CODEC_ID_AAC, CODEC_ID_ALAC, CODEC_ID_EAC3, CODEC_ID_FLAC, CODEC_ID_MP3, CODEC_ID_OPUS,
            CODEC_ID_VORBIS,
        },
    },
    errors::Error as SymphoniaError,
    formats::{FormatOptions, TrackType, probe::Hint},
    io::MediaSourceStream,
    meta::MetadataOptions,
};
use tokio::{sync::Semaphore, task::spawn_blocking};
use tokio_util::sync::CancellationToken;

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

const MAX_DECODED_FRAMES: usize = 30_000_000;

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
        let result = spawn_blocking(move || inspect_sync(&source, &cancellation))
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
            render_spectrogram_sync(&source, &destination, &options, &cancellation)
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
        let result =
            spawn_blocking(move || finalize_m4a_sync(&source, &destination, &tags, &cancellation))
                .await
                .map_err(|error| MediaError::Metadata(error.to_string()))?;
        drop(permit);
        result
    }
}

fn inspect_sync(source: &Path, cancellation: &CancellationToken) -> Result<AudioInfo, MediaError> {
    if cancellation.is_cancelled() {
        return Err(MediaError::Cancelled);
    }
    let decoded = decode_sync(source, cancellation, false)?;
    Ok(decoded.info)
}

fn render_spectrogram_sync(
    source: &Path,
    destination: &Path,
    options: &SpectrogramOptions,
    cancellation: &CancellationToken,
) -> Result<SpectrogramReport, MediaError> {
    if cancellation.is_cancelled() {
        return Err(MediaError::Cancelled);
    }
    let decoded = decode_sync(source, cancellation, true)?;
    let info = decoded.info.clone();
    let rendered = render_image(&decoded.samples, info.sample_rate, options, cancellation)?;
    let file = File::create(destination)?;
    let mut encoder = png::Encoder::new(file, rendered.width, rendered.height);
    encoder.set_color(png::ColorType::Rgb);
    encoder.set_depth(png::BitDepth::Eight);
    let mut writer = encoder
        .write_header()
        .map_err(|error| MediaError::Render(error.to_string()))?;
    writer
        .write_image_data(&rendered.pixels)
        .map_err(|error| MediaError::Render(error.to_string()))?;
    Ok(SpectrogramReport {
        info,
        output: destination.to_owned(),
    })
}

struct DecodedAudio {
    info: AudioInfo,
    samples: Vec<Vec<f32>>,
}

fn decode_sync(
    source: &Path,
    cancellation: &CancellationToken,
    collect_samples: bool,
) -> Result<DecodedAudio, MediaError> {
    let file = File::open(source)?;
    let mss = MediaSourceStream::new(Box::new(file), Default::default());
    let mut hint = Hint::new();
    if let Some(extension) = source.extension().and_then(|value| value.to_str()) {
        hint.with_extension(extension);
    }
    let mut format = symphonia::default::get_probe()
        .probe(
            &hint,
            mss,
            FormatOptions::default(),
            MetadataOptions::default(),
        )
        .map_err(|error| match error {
            SymphoniaError::Unsupported(_) => MediaError::UnsupportedFormat,
            other => MediaError::Decode(other.to_string()),
        })?;
    let track = format
        .default_track(TrackType::Audio)
        .ok_or_else(|| MediaError::Invalid("no audio track".into()))?;
    let codec_params = track
        .codec_params
        .as_ref()
        .ok_or_else(|| MediaError::Invalid("codec parameters missing".into()))?;
    let params = codec_params
        .audio()
        .ok_or_else(|| MediaError::Invalid("audio parameters missing".into()))?;
    let sample_rate = params
        .sample_rate
        .ok_or_else(|| MediaError::Invalid("sample rate missing".into()))?;
    let probed_channels = params.channels.as_ref().map(|layout| layout.count() as u32);
    let duration_secs = track
        .duration
        .and_then(|duration| {
            track
                .time_base
                .and_then(|base| base.calc_duration(duration))
        })
        .map(|time| time.as_secs_f64())
        .unwrap_or_else(|| {
            track
                .num_frames
                .map(|frames| frames as f64 / sample_rate as f64)
                .unwrap_or(0.0)
        });
    let codec = codec_name(params.codec);
    let track_id = track.id;
    let decoder = symphonia::default::get_codecs()
        .make_audio_decoder(params, &AudioDecoderOptions::default());
    let can_decode = decoder.is_ok();
    let channels = if can_decode {
        probed_channels.ok_or_else(|| MediaError::Invalid("channel count missing".into()))?
    } else if collect_samples {
        return Err(MediaError::UnsupportedFormat);
    } else {
        probed_channels.unwrap_or(2)
    };
    let bit_depth = params
        .bits_per_sample
        .or_else(|| alac_bit_depth(params.codec, params.extra_data.as_deref()));

    let mut decoder = if collect_samples {
        match decoder {
            Ok(decoder) => Some(decoder),
            Err(error) => {
                return Err(MediaError::UnsupportedFormat.or_decode(error.to_string()));
            }
        }
    } else {
        None
    };
    let mut samples = if collect_samples {
        vec![Vec::new(); channels as usize]
    } else {
        Vec::new()
    };
    let mut decoded_packets = 0usize;
    let mut first_decode_error = None;
    while let Some(packet) = format
        .next_packet()
        .map_err(|error| MediaError::Decode(error.to_string()))?
    {
        if cancellation.is_cancelled() {
            return Err(MediaError::Cancelled);
        }
        if packet.track_id != track_id {
            continue;
        }
        let Some(decoder) = decoder.as_mut() else {
            continue;
        };
        match decoder.decode(&packet) {
            Ok(audio_buf) => {
                decoded_packets += 1;
                let count = audio_buf.frames();
                if samples.len().saturating_add(count) > MAX_DECODED_FRAMES {
                    return Err(MediaError::Invalid(
                        "decoded audio exceeds frame limit".into(),
                    ));
                }
                let mut interleaved = vec![f32::MID; audio_buf.samples_interleaved()];
                audio_buf.copy_to_slice_interleaved(&mut interleaved);
                let channel_count = channels as usize;
                for (channel, sample) in interleaved.into_iter().enumerate() {
                    samples[channel % channel_count].push(sample);
                }
            }
            Err(SymphoniaError::DecodeError(message)) => {
                if first_decode_error.is_none() {
                    first_decode_error = Some(message.to_string());
                }
                let pad_frames = if !packet.dur.is_zero() {
                    packet.dur.get() as usize
                } else {
                    4096
                };
                for channel in &mut samples {
                    channel.extend(std::iter::repeat_n(f32::MID, pad_frames));
                }
            }
            Err(other) => {
                return Err(MediaError::Decode(other.to_string()));
            }
        }
    }
    if collect_samples
        && decoded_packets == 0
        && let Some(err) = first_decode_error
    {
        return Err(MediaError::Decode(err));
    }
    Ok(DecodedAudio {
        info: AudioInfo {
            codec,
            sample_rate,
            channels,
            bit_depth,
            duration_secs,
            title: None,
            artist: None,
            album: None,
        },
        samples,
    })
}

fn codec_name(codec: AudioCodecId) -> String {
    match codec {
        CODEC_ID_ALAC => "alac",
        CODEC_ID_AAC => "aac",
        CODEC_ID_EAC3 => "eac3",
        CODEC_ID_FLAC => "flac",
        CODEC_ID_MP3 => "mp3",
        CODEC_ID_OPUS => "opus",
        CODEC_ID_VORBIS => "vorbis",
        _ => "unknown",
    }
    .to_owned()
}

fn alac_bit_depth(codec: AudioCodecId, extra_data: Option<&[u8]>) -> Option<u32> {
    if codec != CODEC_ID_ALAC {
        return None;
    }
    extra_data
        .and_then(|data| data.get(5).copied())
        .map(u32::from)
        .filter(|bits| (1..=64).contains(bits))
}

trait DecodeErrorExt {
    fn or_decode(self, message: String) -> MediaError;
}

impl DecodeErrorExt for MediaError {
    fn or_decode(self, message: String) -> MediaError {
        match self {
            MediaError::UnsupportedFormat => MediaError::Decode(message),
            other => other,
        }
    }
}

struct RenderedImage {
    pixels: Vec<u8>,
    width: u32,
    height: u32,
}

fn render_image(
    samples: &[Vec<f32>],
    sample_rate: u32,
    options: &SpectrogramOptions,
    cancellation: &CancellationToken,
) -> Result<RenderedImage, MediaError> {
    if samples.is_empty() || samples.iter().all(Vec::is_empty) {
        return Err(MediaError::Invalid("audio contains no samples".into()));
    }
    if options.width == 0 || options.height == 0 {
        return Err(MediaError::Invalid(
            "spectrogram dimensions must be non-zero".into(),
        ));
    }
    let channels = samples.len().min(2);
    let frame_count = samples
        .iter()
        .take(channels)
        .map(Vec::len)
        .min()
        .unwrap_or_default();
    let max_frames = options
        .max_duration_secs
        .filter(|seconds| *seconds > 0.0)
        .map(|seconds| (seconds * sample_rate as f64) as usize)
        .unwrap_or(frame_count)
        .min(frame_count);
    let fft_size = 2048usize;
    let mut planner = RealFftPlanner::<f32>::new();
    let fft = planner.plan_fft_forward(fft_size);
    let mut input = vec![0.0; fft_size];
    let mut spectrum = fft.make_output_vec();
    let window: Vec<f32> = (0..fft_size)
        .map(|index| {
            let phase = std::f32::consts::TAU * index as f32 / (fft_size - 1) as f32;
            0.5 - 0.5 * phase.cos()
        })
        .collect();
    let plot_width = options.width as usize;
    let plot_height = options.height as usize;
    let left = 58usize;
    let right = 86usize;
    let top = 49usize;
    let bottom = 49usize;
    let panel_gap = 1usize;
    let canvas_width = left + plot_width + right;
    let canvas_height =
        top + channels * plot_height + (channels.saturating_sub(1) * panel_gap) + bottom;
    let mut pixels = vec![0u8; canvas_width * canvas_height * 3];
    let window_sum = window.iter().sum::<f32>().max(1.0);
    let max_start = max_frames.saturating_sub(fft_size);

    for (channel, channel_samples) in samples.iter().take(channels).enumerate() {
        let plot_top = top + channel * (plot_height + panel_gap);
        for x in 0..plot_width {
            if cancellation.is_cancelled() {
                return Err(MediaError::Cancelled);
            }
            let start = if plot_width == 1 {
                0
            } else {
                x * max_start / (plot_width - 1)
            };
            for (index, value) in input.iter_mut().enumerate() {
                *value = channel_samples.get(start + index).copied().unwrap_or(0.0) * window[index];
            }
            fft.process(&mut input, &mut spectrum)
                .map_err(|error| MediaError::Render(error.to_string()))?;
            for y in 0..plot_height {
                let bin = (plot_height - 1 - y) * (spectrum.len() - 1) / plot_height;
                let amplitude = (spectrum[bin].norm() * 2.0 / window_sum).max(1.0e-9);
                let db = 20.0 * amplitude.log10() + 18.0;
                let level =
                    ((db + options.dynamic_range_db) / options.dynamic_range_db).clamp(0.0, 1.0);
                set_pixel(
                    &mut pixels,
                    canvas_width,
                    left + x,
                    plot_top + y,
                    heat_color(level),
                );
            }
        }
    }

    let layout = Layout {
        width: canvas_width,
        canvas_height,
        left,
        plot_width,
        plot_height,
        channels,
        sample_rate,
        frame_count: max_frames,
    };
    draw_layout(&mut pixels, &layout, options);
    Ok(RenderedImage {
        pixels,
        width: canvas_width as u32,
        height: canvas_height as u32,
    })
}

fn heat_color(level: f32) -> [u8; 3] {
    const STOPS: &[(f32, [u8; 3])] = &[
        (0.00, [0, 0, 0]),
        (0.10, [0, 0, 35]),
        (0.24, [0, 0, 125]),
        (0.40, [80, 0, 150]),
        (0.54, [175, 0, 90]),
        (0.64, [235, 0, 15]),
        (0.73, [255, 95, 0]),
        (0.82, [255, 180, 0]),
        (0.91, [255, 245, 40]),
        (0.98, [255, 255, 180]),
        (1.00, [255, 255, 255]),
    ];
    let level = level.clamp(0.0, 1.0);
    let pair = STOPS
        .windows(2)
        .find(|pair| level <= pair[1].0)
        .unwrap_or_else(|| &STOPS[STOPS.len() - 2..]);
    let (low, high) = (pair[0], pair[1]);
    let fraction = ((level - low.0) / (high.0 - low.0)).clamp(0.0, 1.0);
    std::array::from_fn(|index| {
        (low.1[index] as f32 + fraction * (high.1[index] as f32 - low.1[index] as f32)) as u8
    })
}

fn set_pixel(pixels: &mut [u8], width: usize, x: usize, y: usize, color: [u8; 3]) {
    let offset = (y * width + x) * 3;
    if let Some(pixel) = pixels.get_mut(offset..offset + 3) {
        pixel.copy_from_slice(&color);
    }
}

struct Layout {
    width: usize,
    canvas_height: usize,
    left: usize,
    plot_width: usize,
    plot_height: usize,
    channels: usize,
    sample_rate: u32,
    frame_count: usize,
}

fn draw_layout(pixels: &mut [u8], layout: &Layout, options: &SpectrogramOptions) {
    let width = layout.width;
    let canvas_height = layout.canvas_height;
    let left = layout.left;
    let plot_width = layout.plot_width;
    let plot_height = layout.plot_height;
    let channels = layout.channels;
    let plot_right = left + plot_width - 1;
    let plot_bottom = 49 + channels * plot_height + channels.saturating_sub(1) - 1;
    let axis = [145, 145, 145];
    for channel in 0..channels {
        let top = 49 + channel * (plot_height + 1);
        let bottom = top + plot_height - 1;
        for x in left..=plot_right {
            set_pixel(pixels, width, x, top, axis);
            set_pixel(pixels, width, x, bottom, axis);
        }
        for y in top..=bottom {
            set_pixel(pixels, width, left, y, axis);
            set_pixel(pixels, width, plot_right, y, axis);
        }
        let nyquist_khz = layout.sample_rate as f32 / 2000.0;
        let tick_step = 1.0;
        let mut tick = 0.0;
        while tick <= nyquist_khz + 0.01 {
            let tick_y =
                bottom.saturating_sub((tick / nyquist_khz.max(0.1) * plot_height as f32) as usize);
            draw_text(
                pixels,
                width,
                38,
                tick_y.saturating_sub(4),
                &format!("{tick:.0}"),
                axis,
            );
            draw_text(
                pixels,
                width,
                plot_right + 6,
                tick_y.saturating_sub(4),
                &format!("{tick:.0}"),
                axis,
            );
            tick += tick_step;
        }
    }
    let duration = layout.frame_count as f32 / layout.sample_rate.max(1) as f32;
    let time_step = if duration <= 12.0 {
        2.0
    } else if duration <= 90.0 {
        5.0
    } else {
        10.0
    };
    let mut seconds = 0.0;
    while seconds <= duration + 0.01 {
        let x = left + ((seconds / duration.max(0.01)) * (plot_width - 1) as f32) as usize;
        let label = format_time(seconds);
        draw_text(
            pixels,
            width,
            x.saturating_sub(label.len() * 4),
            34,
            &label,
            axis,
        );
        let bottom_y = plot_bottom + 6;
        draw_text(
            pixels,
            width,
            x.saturating_sub(label.len() * 4),
            bottom_y,
            &label,
            axis,
        );
        seconds += time_step;
    }
    if let Some(title) = options.title.as_deref() {
        draw_text(
            pixels,
            width,
            left + plot_width / 2 - title.len() * 4,
            10,
            title,
            [220, 220, 220],
        );
    }
    draw_text(
        pixels,
        width,
        left + plot_width / 2 - 24,
        canvas_height - 26,
        "Time (s)",
        axis,
    );
    draw_text_vertical(
        pixels,
        width,
        8,
        49 + plot_height / 2,
        "Frequency (kHz)",
        axis,
    );
    if channels > 1 {
        draw_text_vertical(
            pixels,
            width,
            8,
            49 + plot_height + 1 + plot_height / 2,
            "Frequency (kHz)",
            axis,
        );
    }
    let palette_x = plot_right + 37;
    let palette_top = 49 + plot_height / 2;
    let palette_height = plot_height.min(canvas_height.saturating_sub(palette_top + 50));
    for y in 0..palette_height {
        let level = 1.0 - y as f32 / palette_height.max(1) as f32;
        for x in palette_x..palette_x + 14 {
            set_pixel(pixels, width, x, palette_top + y, heat_color(level));
        }
    }
    for index in 0..=12 {
        let label_y = palette_top + index * palette_height / 12;
        draw_text(
            pixels,
            width,
            palette_x + 16,
            label_y.saturating_sub(4),
            &format!("-{}", index * 10),
            axis,
        );
    }
    if let Some(comment) = options.comment.as_deref() {
        draw_text(
            pixels,
            width,
            2,
            canvas_height - 16,
            comment,
            [175, 175, 175],
        );
    }
}

fn format_time(seconds: f32) -> String {
    let minutes = (seconds / 60.0).floor() as u32;
    let secs = (seconds % 60.0).round() as u32;
    format!("{minutes}:{secs:02}")
}

fn draw_text(pixels: &mut [u8], width: usize, x: usize, y: usize, text: &str, color: [u8; 3]) {
    let mut cursor = x;
    for character in text.chars() {
        let fallback = match character {
            '•' => Some('x'),
            '–' | '—' => Some('-'),
            _ => None,
        };
        if let Some(glyph) = font8x8::BASIC_FONTS
            .get(character)
            .or_else(|| fallback.and_then(|value| font8x8::BASIC_FONTS.get(value)))
        {
            for (row, bits) in glyph.iter().enumerate() {
                for column in 0..8 {
                    if bits & (1 << column) != 0 {
                        set_pixel(pixels, width, cursor + column, y + row, color);
                    }
                }
            }
        }
        cursor += 8;
    }
}

fn draw_text_vertical(
    pixels: &mut [u8],
    width: usize,
    x: usize,
    center_y: usize,
    text: &str,
    color: [u8; 3],
) {
    let height = text.chars().count() * 8;
    let start_y = center_y.saturating_sub(height / 2);
    for (index, character) in text.chars().enumerate() {
        let Some(glyph) = font8x8::BASIC_FONTS.get(character) else {
            continue;
        };
        for (row, bits) in glyph.iter().enumerate() {
            for column in 0..8 {
                if bits & (1 << column) != 0 {
                    set_pixel(
                        pixels,
                        width,
                        x + row,
                        start_y + (height - (index + 1) * 8) + column,
                        color,
                    );
                }
            }
        }
    }
}

fn tag_flac(path: &Path, tags: &TrackTags) -> Result<(), MediaError> {
    let mut tag = metaflac::Tag::read_from_path(path)
        .map_err(|error| MediaError::Invalid(error.to_string()))?;
    let comments = tag.vorbis_comments_mut();
    if let Some(value) = non_empty(tags.title.as_deref()) {
        comments.set_title(vec![value]);
    }
    if let Some(value) = non_empty(tags.artist.as_deref()) {
        comments.set_artist(vec![value]);
    }
    if let Some(value) = non_empty(tags.album.as_deref()) {
        comments.set_album(vec![value]);
    }
    if let Some(value) = non_empty(tags.album_artist.as_deref()) {
        comments.set_album_artist(vec![value]);
    }
    if let Some(value) = non_empty(tags.release_date.as_deref()) {
        comments.set("DATE", vec![value]);
    }
    if let Some(value) = non_empty(tags.genre.as_deref()) {
        comments.set_genre(vec![value]);
    }
    if let Some(value) = non_empty(tags.composer.as_deref()) {
        comments.set("COMPOSER", vec![value]);
    }
    if let Some(value) = tags.track_number {
        comments.set_track(value as u32);
    }
    if let Some(value) = tags.track_count {
        comments.set_total_tracks(value as u32);
    }
    if let Some(value) = tags.disc_number {
        comments.set("DISCNUMBER", vec![value.to_string()]);
    }
    if let Some(value) = tags.disc_count {
        comments.set("DISCTOTAL", vec![value.to_string()]);
    }
    if let Some(value) = non_empty(tags.lyrics.as_deref()) {
        comments.set_lyrics(vec![value]);
    }
    if let Some(value) = non_empty(tags.isrc.as_deref()) {
        comments.set("ISRC", vec![value]);
    }
    if let Some(value) = non_empty(tags.copyright.as_deref()) {
        comments.set("COPYRIGHT", vec![value]);
    }
    if let Some(image) = tags
        .artwork_jpeg
        .as_deref()
        .filter(|image| !image.is_empty())
    {
        let mime_type = if image.starts_with(b"\x89PNG\r\n\x1a\n") {
            "image/png"
        } else {
            "image/jpeg"
        };
        tag.add_picture(
            mime_type,
            metaflac::block::PictureType::CoverFront,
            image.to_vec(),
        );
    }
    tag.save()
        .map_err(|error| MediaError::Metadata(error.to_string()))?;
    Ok(())
}

fn tag_mp3(path: &Path, tags: &TrackTags) -> Result<(), MediaError> {
    let mut tag = id3::Tag::read_from_path(path).unwrap_or_default();
    if let Some(value) = non_empty(tags.title.as_deref()) {
        tag.set_title(value);
    }
    if let Some(value) = non_empty(tags.artist.as_deref()) {
        tag.set_artist(value);
    }
    if let Some(value) = non_empty(tags.album.as_deref()) {
        tag.set_album(value);
    }
    if let Some(value) = non_empty(tags.album_artist.as_deref()) {
        tag.set_album_artist(value);
    }
    if let Some(value) = non_empty(tags.genre.as_deref()) {
        tag.set_genre(value);
    }
    if let Some(value) = tags.track_number {
        tag.set_track(value as u32);
    }
    if let Some(value) = tags.track_count {
        tag.set_total_tracks(value as u32);
    }
    if let Some(value) = tags.disc_number {
        tag.set_disc(value as u32);
    }
    if let Some(value) = tags.disc_count {
        tag.set_total_discs(value as u32);
    }
    if let Some(image) = tags
        .artwork_jpeg
        .as_deref()
        .filter(|image| !image.is_empty())
    {
        let mime_type = if image.starts_with(b"\x89PNG\r\n\x1a\n") {
            "image/png".to_string()
        } else {
            "image/jpeg".to_string()
        };
        tag.add_frame(id3::frame::Picture {
            mime_type,
            picture_type: id3::frame::PictureType::CoverFront,
            description: String::new(),
            data: image.to_vec(),
        });
    }
    tag.write_to_path(path, id3::Version::Id3v24)
        .map_err(|error| MediaError::Metadata(error.to_string()))?;
    Ok(())
}

fn tag_m4a(path: &Path, tags: &TrackTags) -> Result<(), MediaError> {
    let mut tag = mp4ameta::Tag::read_from_path(path)
        .map_err(|error| MediaError::Invalid(error.to_string()))?;
    if let Some(value) = non_empty(tags.title.as_deref()) {
        tag.set_title(value);
    }
    if let Some(value) = non_empty(tags.title_sort.as_deref()) {
        tag.set_title_sort_order(value);
    }
    if let Some(value) = non_empty(tags.artist.as_deref()) {
        tag.set_artist(value);
    }
    if let Some(value) = non_empty(tags.artist_sort.as_deref()) {
        tag.set_artist_sort_order(value);
    }
    if let Some(value) = non_empty(tags.album.as_deref()) {
        tag.set_album(value);
    }
    if let Some(value) = non_empty(tags.album_sort.as_deref()) {
        tag.set_album_sort_order(value);
    }
    if let Some(value) = non_empty(tags.album_artist.as_deref()) {
        tag.set_album_artist(value);
    }
    if let Some(value) = non_empty(tags.album_artist_sort.as_deref()) {
        tag.set_album_artist_sort_order(value);
    }
    if let Some(value) = non_empty(tags.release_date.as_deref()) {
        tag.set_year(value);
    }
    if let Some(value) = non_empty(tags.genre.as_deref()) {
        tag.set_genre(value);
    }
    if let Some(value) = non_empty(tags.composer.as_deref()) {
        tag.set_composer(value);
    }
    if let Some(value) = non_empty(tags.composer_sort.as_deref()) {
        tag.set_composer_sort_order(value);
    }
    if let Some(value) = tags.track_number {
        tag.set_track(value, tags.track_count.unwrap_or(0));
    }
    if let Some(value) = tags.disc_number {
        tag.set_disc(value, tags.disc_count.unwrap_or(0));
    }
    if let Some(value) = non_empty(tags.lyrics.as_deref()) {
        tag.set_lyrics(value);
    }
    if let Some(image) = tags
        .artwork_jpeg
        .as_deref()
        .filter(|image| !image.is_empty())
    {
        if image.starts_with(b"\x89PNG\r\n\x1a\n") {
            tag.set_artwork(mp4ameta::Img::png(image));
        } else {
            tag.set_artwork(mp4ameta::Img::jpeg(image));
        }
    }
    write_extended_metadata(&mut tag, tags);
    tag.write_to_path(path)
        .map_err(|error| MediaError::Metadata(error.to_string()))?;
    Ok(())
}

fn finalize_m4a_sync(
    source: &Path,
    destination: &Path,
    tags: &TrackTags,
    cancellation: &CancellationToken,
) -> Result<ValidatedM4a, MediaError> {
    if cancellation.is_cancelled() {
        return Err(MediaError::Cancelled);
    }
    let info = inspect_sync(source, cancellation).map_err(mark_source_validation)?;

    let ext = destination
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("m4a");
    let part = destination.with_extension(format!("{ext}.part"));
    let part = match part.file_name().and_then(|value| value.to_str()) {
        Some(name) if name.len() <= 255 => part,
        _ => destination.with_extension("p"),
    };
    if part.exists() {
        std::fs::remove_file(&part)?;
    }
    std::fs::copy(source, &part)?;
    let result = (|| {
        if cancellation.is_cancelled() {
            return Err(MediaError::Cancelled);
        }

        let is_flac = ext == "flac" || info.codec == "flac";
        let is_mp3 = ext == "mp3" || info.codec == "mp3";

        if is_flac {
            tag_flac(&part, tags)?;
        } else if is_mp3 {
            tag_mp3(&part, tags)?;
        } else {
            tag_m4a(&part, tags)?;
        }

        if cancellation.is_cancelled() {
            return Err(MediaError::Cancelled);
        }
        let validated = inspect_sync(&part, cancellation)?;
        if validated.codec != info.codec {
            return Err(MediaError::Invalid(format!(
                "finalized file codec changed: expected {}, found {}",
                info.codec, validated.codec
            )));
        }
        if validated.duration_secs == 0.0 {
            return Err(MediaError::Invalid("finalized file has no duration".into()));
        }
        std::fs::rename(&part, destination)?;
        Ok(ValidatedM4a {
            path: destination.to_owned(),
            info: validated,
        })
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&part);
    }
    result
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

fn non_empty(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

/// Write provider and Apple-specific metadata while keeping `mp4ameta` out of
/// the public media interface. Optional values are omitted rather than writing
/// empty atoms, which keeps files compact and avoids misleading player output.
fn write_extended_metadata(tag: &mut mp4ameta::Tag, tags: &TrackTags) {
    if let Some(value) = non_empty(tags.isrc.as_deref()) {
        tag.set_isrc(value);
        set_freeform_text(tag, "ISRC", value);
    }
    if let Some(value) = non_empty(tags.label.as_deref()) {
        tag.set_label(value);
        set_freeform_text(tag, "LABEL", value);
    }
    if let Some(value) = non_empty(tags.copyright.as_deref()) {
        tag.set_copyright(value);
    }
    if let Some(value) = non_empty(tags.publisher.as_deref()) {
        set_freeform_text(tag, "PUBLISHER", value);
    }
    if let Some(value) = non_empty(tags.performer.as_deref()) {
        set_freeform_text(tag, "PERFORMER", value);
    }
    if let Some(value) = non_empty(tags.release_time.as_deref()) {
        set_freeform_text(tag, "RELEASETIME", value);
    }
    if let Some(value) = non_empty(tags.upc.as_deref()) {
        set_freeform_text(tag, "UPC", value);
    }

    if let Some(value) = tags.song_id.and_then(to_u32) {
        set_numeric_atom(tag, *b"cnID", value);
    }
    if let Some(value) = tags.album_id.and_then(to_u32) {
        set_numeric_atom(tag, *b"plID", value);
    }
    if let Some(value) = tags.artist_id.and_then(to_u32) {
        set_numeric_atom(tag, *b"atID", value);
    }
    if let Some(value) = tags.genre_id {
        set_numeric_atom(tag, *b"geID", value);
    }
    if let Some(value) = tags.storefront_id {
        set_numeric_atom(tag, *b"sfID", value);
    }

    if tags.compilation == Some(true) {
        tag.set_data(
            mp4ameta::ident::COMPILATION,
            mp4ameta::Data::BeSigned(vec![1]),
        );
    }
    if tags.gapless == Some(true) {
        tag.set_data(
            mp4ameta::Fourcc(*b"pgap"),
            mp4ameta::Data::BeSigned(vec![1]),
        );
    }
    if let Some(value) = non_empty(tags.encoder.as_deref()) {
        tag.set_data(
            mp4ameta::Fourcc(*b"\xa9too"),
            mp4ameta::Data::Utf8(value.to_owned()),
        );
        tag.set_data(
            mp4ameta::ident::ENCODER,
            mp4ameta::Data::Utf8(value.to_owned()),
        );
    }
    if let Some(value) = non_empty(tags.comment.as_deref()) {
        tag.set_comment(value);
    }
    if let Some(value) = non_empty(tags.description.as_deref()) {
        tag.set_description(value);
    }

    if let Some(advisory) = tags.advisory.or_else(|| {
        tags.explicit.map(|explicit| {
            if explicit {
                AdvisoryKind::Explicit
            } else {
                AdvisoryKind::Inoffensive
            }
        })
    }) {
        let rating = match advisory {
            AdvisoryKind::Explicit => mp4ameta::AdvisoryRating::Explicit,
            AdvisoryKind::Clean => mp4ameta::AdvisoryRating::Clean,
            AdvisoryKind::Inoffensive => mp4ameta::AdvisoryRating::Inoffensive,
        };
        tag.set_data(
            mp4ameta::ident::ADVISORY_RATING,
            mp4ameta::Data::BeSigned(vec![rating.code()]),
        );
    }
    if let Some(kind) = tags.media_kind {
        let media_type = match kind {
            MediaKind::Music => mp4ameta::MediaType::Normal,
            MediaKind::Audiobook => mp4ameta::MediaType::AudioBook,
            MediaKind::MusicVideo => mp4ameta::MediaType::MusicVideo,
            MediaKind::Movie => mp4ameta::MediaType::Movie,
        };
        tag.set_data(
            mp4ameta::ident::MEDIA_TYPE,
            mp4ameta::Data::BeSigned(vec![media_type.code()]),
        );
    }
}

fn to_u32(value: u64) -> Option<u32> {
    u32::try_from(value).ok()
}

fn set_numeric_atom(tag: &mut mp4ameta::Tag, atom: [u8; 4], value: u32) {
    tag.set_data(
        mp4ameta::Fourcc(atom),
        mp4ameta::Data::BeSigned(value.to_be_bytes().to_vec()),
    );
}

fn set_freeform_text(tag: &mut mp4ameta::Tag, name: &'static str, value: &str) {
    let ident = mp4ameta::FreeformIdent::new_static("com.apple.iTunes", name);
    tag.set_data(ident, mp4ameta::Data::Utf8(value.to_owned()));
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
    fn stereo_layout_matches_reference_canvas() {
        let samples = vec![vec![0.0; 2_048], vec![0.0; 2_048]];
        let rendered = render_image(
            &samples,
            44_100,
            &SpectrogramOptions::default(),
            &CancellationToken::new(),
        )
        .unwrap();
        assert_eq!((rendered.width, rendered.height), (1_344, 1_201));
    }

    #[test]
    fn codec_names_are_stable_for_native_validation() {
        use symphonia::core::codecs::audio::well_known::{CODEC_ID_AAC, CODEC_ID_ALAC};

        assert_eq!(codec_name(CODEC_ID_ALAC), "alac");
        assert_eq!(codec_name(CODEC_ID_AAC), "aac");
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

    #[test]
    fn alac_bit_depth_uses_magic_cookie_when_params_omit_it() {
        use symphonia::core::codecs::audio::well_known::CODEC_ID_ALAC;

        assert_eq!(
            alac_bit_depth(CODEC_ID_ALAC, Some(&[0, 0, 16, 0, 0, 24])),
            Some(24)
        );
        assert_eq!(alac_bit_depth(CODEC_ID_ALAC, Some(&[0; 5])), None);
    }

    #[test]
    fn extended_metadata_maps_to_itunes_atoms() {
        let mut tag = mp4ameta::Tag::default();
        let tags = TrackTags {
            title: Some("Title".into()),
            title_sort: Some("Title, The".into()),
            artist: Some("Artist".into()),
            artist_sort: Some("Artist, The".into()),
            album: Some("Album".into()),
            album_sort: Some("Album, The".into()),
            album_artist: Some("Album Artist".into()),
            album_artist_sort: Some("Artist, Album".into()),
            composer: Some("Composer".into()),
            composer_sort: Some("Composer, The".into()),
            isrc: Some("US-AAA-24-00001".into()),
            label: Some("Example Records".into()),
            copyright: Some("2024 Example Records".into()),
            publisher: Some("Example Publishing".into()),
            performer: Some("Featured Performer".into()),
            release_time: Some("2024-01-02T03:04:05Z".into()),
            upc: Some("123456789012".into()),
            song_id: Some(42),
            album_id: Some(43),
            artist_id: Some(44),
            explicit: Some(true),
            media_kind: Some(MediaKind::Music),
            ..TrackTags::default()
        };

        if let Some(value) = tags.title.as_deref() {
            tag.set_title(value);
        }
        if let Some(value) = non_empty(tags.title_sort.as_deref()) {
            tag.set_title_sort_order(value);
        }
        write_extended_metadata(&mut tag, &tags);

        assert_eq!(tag.title(), Some("Title"));
        assert_eq!(tag.title_sort_order(), Some("Title, The"));
        assert_eq!(tag.isrc(), Some("US-AAA-24-00001"));
        assert_eq!(tag.label(), Some("Example Records"));
        assert_eq!(tag.copyright(), Some("2024 Example Records"));
        assert_eq!(tag.media_type(), Some(mp4ameta::MediaType::Normal));
        assert_eq!(
            tag.advisory_rating(),
            Some(mp4ameta::AdvisoryRating::Explicit)
        );
        assert!(matches!(
            tag.data_of(&mp4ameta::ident::ADVISORY_RATING).next(),
            Some(mp4ameta::Data::BeSigned(bytes)) if bytes == &[4]
        ));
        assert!(matches!(
            tag.data_of(&mp4ameta::ident::MEDIA_TYPE).next(),
            Some(mp4ameta::Data::BeSigned(bytes)) if bytes == &[1]
        ));
        assert_eq!(
            tag.bytes_of(&mp4ameta::Fourcc(*b"cnID")).next(),
            Some(&42u32.to_be_bytes()[..])
        );
        let publisher = mp4ameta::FreeformIdent::new_borrowed("com.apple.iTunes", "PUBLISHER");
        assert_eq!(
            tag.strings_of(&publisher).next(),
            Some("Example Publishing")
        );
        let performer = mp4ameta::FreeformIdent::new_borrowed("com.apple.iTunes", "PERFORMER");
        assert_eq!(
            tag.strings_of(&performer).next(),
            Some("Featured Performer")
        );
        let isrc_freeform = mp4ameta::FreeformIdent::new_borrowed("com.apple.iTunes", "ISRC");
        assert_eq!(
            tag.strings_of(&isrc_freeform).next(),
            Some("US-AAA-24-00001")
        );
        let label_freeform = mp4ameta::FreeformIdent::new_borrowed("com.apple.iTunes", "LABEL");
        assert_eq!(
            tag.strings_of(&label_freeform).next(),
            Some("Example Records")
        );
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
