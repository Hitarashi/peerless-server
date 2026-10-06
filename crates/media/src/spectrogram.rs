use std::{fs::File, path::Path};

use font8x8::UnicodeFonts;
use realfft::RealFftPlanner;
use tokio_util::sync::CancellationToken;

use crate::{MediaError, SpectrogramOptions, SpectrogramReport};

struct RenderedImage {
    pixels: Vec<u8>,
    width: u32,
    height: u32,
}

pub(super) fn render_spectrogram_sync(
    source: &Path,
    destination: &Path,
    options: &SpectrogramOptions,
    cancellation: &CancellationToken,
) -> Result<SpectrogramReport, MediaError> {
    if cancellation.is_cancelled() {
        return Err(MediaError::Cancelled);
    }
    let decoded = crate::decode::decode_sync(source, cancellation, true)?;
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
