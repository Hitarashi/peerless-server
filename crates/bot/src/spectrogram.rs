use std::path::Path;

#[derive(Clone, Debug, PartialEq)]
pub struct AudioProbeResult {
    pub title: Option<String>,
    pub artist: Option<String>,
    pub album: Option<String>,
    pub codec: String,
    pub sample_rate: u32,
    pub bit_depth: Option<u32>,
    pub channels: u32,
    pub bit_rate: Option<u64>,
    pub duration: f64,
}

pub const AUDIO_EXTENSIONS: [&str; 12] = [
    ".m4a", ".flac", ".mp3", ".wav", ".wave", ".aac", ".alac", ".ogg", ".oga", ".opus", ".aiff",
    ".aif",
];

pub async fn probe_audio(file: &Path) -> Result<AudioProbeResult, String> {
    let info = media::MediaProcessor::new()
        .inspect(file, &tokio_util::sync::CancellationToken::new())
        .await
        .map_err(|error| error.to_string())?;
    Ok(AudioProbeResult {
        title: info.title,
        artist: info.artist,
        album: info.album,
        codec: info.codec,
        sample_rate: info.sample_rate,
        bit_depth: info.bit_depth,
        channels: info.channels,
        bit_rate: None,
        duration: info.duration_secs,
    })
}

pub async fn generate_native_spectrogram(
    input: &Path,
    output: &Path,
    title: Option<&str>,
    comment: Option<&str>,
    duration_secs: f64,
) -> Result<(), String> {
    let options = media::SpectrogramOptions {
        title: title.map(str::to_owned),
        comment: comment.map(str::to_owned),
        max_duration_secs: (duration_secs > 0.0).then_some(duration_secs),
        ..media::SpectrogramOptions::default()
    };
    media::MediaProcessor::new()
        .render_spectrogram(
            input,
            output,
            &options,
            &tokio_util::sync::CancellationToken::new(),
        )
        .await
        .map(|_| ())
        .map_err(|error| error.to_string())
}

pub fn format_duration(seconds: f64) -> String {
    let mins = (seconds / 60.0).floor() as u64;
    let secs = (seconds % 60.0).floor() as u64;
    if mins >= 60 {
        format!("{}:{:02}:{:02}", mins / 60, mins % 60, secs)
    } else {
        format!("{mins}:{secs:02}")
    }
}

pub fn thousands_separator(value: u32) -> String {
    let digits = value.to_string();
    let first = digits.len() % 3;
    if first == 0 {
        digits
            .as_bytes()
            .chunks(3)
            .map(|chunk| std::str::from_utf8(chunk).unwrap())
            .collect::<Vec<_>>()
            .join(",")
    } else {
        let mut result = digits[..first].to_owned();
        for chunk in digits.as_bytes()[first..].chunks(3) {
            result.push(',');
            result.push_str(std::str::from_utf8(chunk).unwrap());
        }
        result
    }
}

pub fn build_caption(probe: &AudioProbeResult, song_title: &str, codec: &str) -> String {
    let mut caption = format!(
        "<b>Audio spectrogram analysis</b><br/><br/>• <b>Track:</b> <code>{}</code><br/>",
        crate::html::escape(song_title)
    );
    if let Some(artist) = &probe.artist {
        caption.push_str(&format!(
            "• <b>Artist:</b> <code>{}</code><br/>",
            crate::html::escape(artist)
        ));
    }
    if let Some(album) = &probe.album {
        caption.push_str(&format!(
            "• <b>Album:</b> <code>{}</code><br/>",
            crate::html::escape(album)
        ));
    }
    caption.push_str(&format!(
        "• <b>Codec:</b> <code>{}</code><br/>• <b>Sample Rate:</b> <code>{} Hz ({} kHz)</code><br/>",
        crate::html::escape(codec),
        thousands_separator(probe.sample_rate),
        (f64::from(probe.sample_rate) / 100.0).round() / 10.0
    ));
    if let Some(bit_depth) = probe.bit_depth {
        caption.push_str(&format!(
            "• <b>Bit Depth:</b> <code>{bit_depth}-bit</code><br/>"
        ));
    }
    let channels = match probe.channels {
        1 => "Mono".to_owned(),
        2 => "Stereo (2.0)".to_owned(),
        channels => format!("{channels} channels"),
    };
    caption.push_str(&format!("• <b>Channels:</b> <code>{channels}</code><br/>"));
    if let Some(bit_rate) = probe.bit_rate {
        caption.push_str(&format!(
            "• <b>Bitrate:</b> <code>{} kbps</code><br/>",
            (bit_rate as f64 / 1000.0).round()
        ));
    }
    if probe.duration > 0.0 {
        caption.push_str(&format!(
            "• <b>Duration:</b> <code>{}</code><br/>",
            format_duration(probe.duration)
        ));
    }
    caption
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_durations() {
        assert_eq!(format_duration(0.0), "0:00");
        assert_eq!(format_duration(65.0), "1:05");
        assert_eq!(format_duration(3610.0), "1:00:10");
        assert_eq!(format_duration(59.0), "0:59");
    }

    #[test]
    fn has_all_audio_extensions() {
        assert!(AUDIO_EXTENSIONS.contains(&".m4a"));
        assert!(AUDIO_EXTENSIONS.contains(&".flac"));
        assert_eq!(AUDIO_EXTENSIONS.len(), 12);
    }

    #[test]
    fn separates_thousands() {
        assert_eq!(thousands_separator(44_100), "44,100");
    }

    #[test]
    fn builds_oracle_caption() {
        let probe = AudioProbeResult {
            title: Some("Song".into()),
            artist: None,
            album: None,
            codec: "ALAC".into(),
            sample_rate: 44_100,
            bit_depth: Some(24),
            channels: 2,
            bit_rate: Some(900_000),
            duration: 180.5,
        };
        let caption = build_caption(&probe, "Song", "ALAC");
        assert!(caption.contains("• <b>Track:</b> <code>Song</code><br/>"));
        assert!(caption.contains("• <b>Codec:</b> <code>ALAC</code><br/>"));
        assert!(caption.contains("• <b>Sample Rate:</b> <code>44,100 Hz (44.1 kHz)</code><br/>"));
        assert!(caption.contains("• <b>Bit Depth:</b> <code>24-bit</code><br/>"));
        assert!(caption.contains("• <b>Channels:</b> <code>Stereo (2.0)</code><br/>"));
        assert!(caption.contains("• <b>Bitrate:</b> <code>900 kbps</code><br/>"));
        assert!(caption.contains("• <b>Duration:</b> <code>3:00</code><br/>"));
    }
}
