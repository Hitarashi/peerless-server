//! Audio inspection and decoding.
//!
//! Every entry point here touches the filesystem and blocks. The `_sync`
//! suffix marks that contract: callers must reach them through
//! `spawn_blocking`, never directly from an async task.

use std::{fs::File, path::Path};

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
use tokio_util::sync::CancellationToken;

use crate::{AudioInfo, MediaError};

const MAX_DECODED_FRAMES: usize = 30_000_000;

/// Decoded PCM planes plus the probed stream description.
pub(super) struct DecodedAudio {
    pub(super) info: AudioInfo,
    pub(super) samples: Vec<Vec<f32>>,
}

pub(super) fn inspect_sync(
    source: &Path,
    cancellation: &CancellationToken,
) -> Result<AudioInfo, MediaError> {
    if cancellation.is_cancelled() {
        return Err(MediaError::Cancelled);
    }
    let decoded = decode_sync(source, cancellation, false)?;
    Ok(decoded.info)
}

pub(super) fn decode_sync(
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codec_names_are_stable_for_native_validation() {
        use symphonia::core::codecs::audio::well_known::{CODEC_ID_AAC, CODEC_ID_ALAC};

        assert_eq!(codec_name(CODEC_ID_ALAC), "alac");
        assert_eq!(codec_name(CODEC_ID_AAC), "aac");
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
}
