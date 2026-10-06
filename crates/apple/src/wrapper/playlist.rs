use music::CodecPreference;

use super::client::{WrapperError, WrapperUnavailableReason};

#[derive(Debug, Clone)]
pub struct StreamVariant {
    pub uri: String,
    pub codec: String,

    pub group_id: String,
    pub bandwidth: u64,
}

#[derive(Debug, Clone)]
pub struct AlacStreamInfo {
    pub stream_url: String,
    pub codec: String,
    pub sample_rate: u32,
    pub bit_depth: u32,
}

#[derive(Debug, Clone)]
pub struct MediaSegmentRef {
    pub uri: String,
    pub byte_range: Option<(u64, u64)>,
    pub key_uri: Option<String>,
}

#[derive(Debug, Clone)]
pub struct MediaPlaylistInfo {
    pub init_uri: String,
    pub init_byte_range: Option<(u64, u64)>,
    pub segments: Vec<MediaSegmentRef>,
    pub single_file_url: Option<String>,

    pub key_method: Option<String>,

    pub cenc_kid_b64: Option<String>,
}

pub fn resolve_url(base: &str, uri: &str) -> String {
    if uri.starts_with("http://") || uri.starts_with("https://") {
        return uri.to_owned();
    }
    let base = base.split('?').next().unwrap_or(base);
    if let Some(pos) = base.rfind('/') {
        format!("{}/{}", &base[..pos], uri)
    } else {
        uri.to_owned()
    }
}

fn parse_byte_range(val: &str) -> Option<(u64, u64)> {
    let val = val.trim_matches('"').trim();
    if let Some((len_str, off_str)) = val.split_once('@') {
        let len: u64 = len_str.parse().ok()?;
        let off: u64 = off_str.parse().ok()?;
        Some((off, len))
    } else if let Ok(len) = val.parse::<u64>() {
        Some((0, len))
    } else {
        None
    }
}

fn split_attributes(s: &str) -> Result<Vec<&str>, WrapperError> {
    if s.trim().is_empty() {
        return Err(malformed_attribute_error());
    }

    let mut parts = Vec::new();
    let mut in_quotes = false;
    let mut start = 0;
    for (i, c) in s.char_indices() {
        match c {
            '"' => in_quotes = !in_quotes,
            ',' if !in_quotes => {
                let part = s[start..i].trim();
                validate_attribute_component(part)?;
                parts.push(part);
                start = i + 1;
            }
            _ => {}
        }
    }
    if in_quotes {
        return Err(malformed_attribute_error());
    }
    let tail = s[start..].trim();
    validate_attribute_component(tail)?;
    parts.push(tail);
    Ok(parts)
}

fn malformed_attribute_error() -> WrapperError {
    WrapperError::Message("Playlist has a malformed HLS attribute component".into())
}

fn validate_attribute_component(part: &str) -> Result<(), WrapperError> {
    let Some((key, value)) = part.split_once('=') else {
        return Err(malformed_attribute_error());
    };
    let key = key.trim();
    let value = value.trim();
    if key.is_empty() || value.is_empty() {
        return Err(malformed_attribute_error());
    }

    if value.starts_with('"') {
        if value.len() < 2 || !value.ends_with('"') {
            return Err(malformed_attribute_error());
        }
        let quoted = &value[1..value.len() - 1];
        if quoted.is_empty() || quoted.contains('"') {
            return Err(malformed_attribute_error());
        }
    } else if value.contains('"') {
        return Err(malformed_attribute_error());
    }
    Ok(())
}

pub fn parse_master_playlist(
    content: &str,
    master_url: &str,
    preference: CodecPreference,
) -> Result<AlacStreamInfo, WrapperError> {
    if !is_master_playlist(content) {
        return Err(WrapperError::Message(
            "Wrapper response is not a master playlist".into(),
        ));
    }

    validate_media_numeric_attributes(content)?;
    let mut variants = collect_variants(content, master_url)?;
    variants.extend(collect_audio_renditions(content, master_url)?);
    if variants.is_empty() {
        return Err(WrapperError::Message(
            "Master playlist contains no stream variants".into(),
        ));
    }
    select_variant(&variants, preference, content)
}

fn collect_audio_renditions(
    content: &str,
    master_url: &str,
) -> Result<Vec<StreamVariant>, WrapperError> {
    let mut renditions = Vec::new();
    for line in content.lines() {
        let Some(attrs) = line.trim().strip_prefix("#EXT-X-MEDIA:") else {
            continue;
        };
        let mut media_type = None;
        let mut group_id = None;
        let mut uri = None;
        for part in split_attributes(attrs)? {
            let (key, value) = part.split_once('=').expect("validated attribute component");
            let value = value.trim().trim_matches('"');
            match key.trim() {
                "TYPE" => media_type = Some(value),
                "GROUP-ID" => group_id = Some(value),
                "URI" => uri = Some(value),
                _ => {}
            }
        }
        if media_type != Some("AUDIO") {
            continue;
        }
        let (Some(group_id), Some(uri)) = (group_id, uri) else {
            continue;
        };
        let (codec, bitrate_prefix) = if group_id.starts_with("audio-stereo-") {
            ("mp4a.40.2", "audio-stereo-")
        } else if group_id.starts_with("audio-HE-stereo-") {
            ("mp4a.40.5", "audio-HE-stereo-")
        } else if group_id.starts_with("audio-atmos-") {
            ("ec-3", "audio-atmos-")
        } else {
            continue;
        };
        let bitrate = group_bitrate(group_id, bitrate_prefix)? as u64;
        renditions.push(StreamVariant {
            uri: resolve_url(master_url, uri),
            codec: codec.to_owned(),
            group_id: group_id.to_owned(),
            bandwidth: bitrate,
        });
    }
    Ok(renditions)
}

fn collect_variants(content: &str, master_url: &str) -> Result<Vec<StreamVariant>, WrapperError> {
    let mut variants = Vec::new();
    let mut pending: Option<StreamVariant> = None;
    for line in content.lines() {
        let line = line.trim();
        if let Some(attrs) = line.strip_prefix("#EXT-X-STREAM-INF:") {
            if pending.is_some() {
                return Err(WrapperError::Message(
                    "Master playlist has a stream variant without a URI".into(),
                ));
            }
            if attrs.trim().is_empty() {
                return Err(WrapperError::Message(
                    "Master playlist has an empty stream variant tag".into(),
                ));
            }
            let mut variant = StreamVariant {
                uri: String::new(),
                codec: String::new(),
                group_id: String::new(),
                bandwidth: 0,
            };
            let mut bandwidth = None;
            for part in split_attributes(attrs)? {
                let Some((k, v)) = part.split_once('=') else {
                    return Err(WrapperError::Message(
                        "Master playlist has a malformed stream variant tag".into(),
                    ));
                };
                let k = k.trim();
                let v = v.trim().trim_matches('"');
                if k.is_empty() || v.is_empty() {
                    return Err(WrapperError::Message(
                        "Master playlist has a malformed stream variant tag".into(),
                    ));
                }
                if k == "CODECS" {
                    variant.codec = v.to_owned();
                } else if k == "AUDIO" {
                    variant.group_id = v.to_owned();
                } else if k == "BANDWIDTH" {
                    bandwidth = Some(parse_positive_u64(v, "BANDWIDTH")?);
                } else if k == "AVERAGE-BANDWIDTH" {
                    let average = parse_positive_u64(v, "AVERAGE-BANDWIDTH")?;

                    if bandwidth.is_none() {
                        bandwidth = Some(average);
                    }
                }
            }
            variant.bandwidth = bandwidth.ok_or_else(|| {
                WrapperError::Message("Master playlist stream variant is missing BANDWIDTH".into())
            })?;
            if variant.codec.is_empty() || variant.group_id.is_empty() {
                return Err(WrapperError::Message(
                    "Master playlist stream variant is missing CODECS or AUDIO".into(),
                ));
            }
            validate_variant_group(&variant)?;
            pending = Some(variant);
        } else if pending.is_some() && !line.is_empty() && !line.starts_with('#') {
            let mut variant = pending.take().unwrap();
            variant.uri = resolve_url(master_url, line);
            variants.push(variant);
        }
    }
    if pending.is_some() {
        return Err(WrapperError::Message(
            "Master playlist has a stream variant without a URI".into(),
        ));
    }
    Ok(variants)
}

fn is_master_playlist(content: &str) -> bool {
    content.lines().map(str::trim).find(|line| !line.is_empty()) == Some("#EXTM3U")
        && content
            .lines()
            .any(|line| line.trim().starts_with("#EXT-X-STREAM-INF:"))
}

fn validate_media_numeric_attributes(content: &str) -> Result<(), WrapperError> {
    for line in content.lines() {
        let Some(attrs) = line.trim().strip_prefix("#EXT-X-MEDIA:") else {
            continue;
        };
        for part in split_attributes(attrs)? {
            let (key, value) = part.split_once('=').expect("validated attribute component");
            let value = value.trim().trim_matches('"');
            match key.trim() {
                "SAMPLE-RATE" => {
                    parse_positive_u32(value, "SAMPLE-RATE")?;
                }
                "BIT-DEPTH" => {
                    parse_positive_u32(value, "BIT-DEPTH")?;
                }
                _ => {}
            }
        }
    }
    Ok(())
}

fn validate_variant_group(variant: &StreamVariant) -> Result<(), WrapperError> {
    match variant.codec.as_str() {
        "alac" => {
            alac_group_specs(&variant.group_id)?;
        }
        "ec-3" if variant.group_id.starts_with("audio-atmos") => {
            group_bitrate(&variant.group_id, "audio-atmos-")?;
        }
        "mp4a.40.2" if variant.group_id.starts_with("audio-stereo") => {
            group_bitrate(&variant.group_id, "audio-stereo-")?;
        }
        "mp4a.40.5" if variant.group_id.starts_with("audio-HE-stereo") => {
            group_bitrate(&variant.group_id, "audio-HE-stereo-")?;
        }
        _ => {}
    }
    Ok(())
}

fn group_rank(
    group_id: &str,
    codec: &str,
    preference: CodecPreference,
) -> Result<Option<i64>, WrapperError> {
    let is_atmos_group = group_id.starts_with("audio-atmos");
    match (codec, preference) {
        ("ec-3", CodecPreference::Atmos) if is_atmos_group => {
            Ok(Some(20_000 + group_bitrate(group_id, "audio-atmos-")?))
        }
        ("alac", p) if p != CodecPreference::Atmos => {
            Ok(Some(10_000_000 + alac_group_specs(group_id)?.0 as i64))
        }
        ("mp4a.40.2", p) if p != CodecPreference::Atmos => {
            Ok(Some(5_000 + group_bitrate(group_id, "audio-stereo-")?))
        }
        ("mp4a.40.5", p) if p != CodecPreference::Atmos => {
            Ok(Some(4_000 + group_bitrate(group_id, "audio-HE-stereo-")?))
        }
        _ => Ok(None),
    }
}

fn group_bitrate(group_id: &str, prefix: &str) -> Result<i64, WrapperError> {
    let rest = group_id.strip_prefix(prefix).ok_or_else(|| {
        WrapperError::Message(format!(
            "Master playlist has invalid audio group: {group_id}"
        ))
    })?;

    let bitrate_token = rest.split_once('-').map_or(rest, |(bitrate, _)| bitrate);
    let bitrate = parse_positive_u64(bitrate_token, "audio group bitrate")?;
    i64::try_from(bitrate).map_err(|_| {
        WrapperError::Message(format!(
            "Master playlist has invalid audio group: {group_id}"
        ))
    })
}

fn parse_positive_u64(value: &str, attribute: &str) -> Result<u64, WrapperError> {
    let parsed = value.parse::<u64>().map_err(|_| {
        WrapperError::Message(format!("Master playlist has invalid {attribute}: {value}"))
    })?;
    if parsed == 0 {
        return Err(WrapperError::Message(format!(
            "Master playlist has invalid {attribute}: {value}"
        )));
    }
    Ok(parsed)
}

fn parse_positive_u32(value: &str, attribute: &str) -> Result<u32, WrapperError> {
    let parsed = value.parse::<u32>().map_err(|_| {
        WrapperError::Message(format!("Master playlist has invalid {attribute}: {value}"))
    })?;
    if parsed == 0 {
        return Err(WrapperError::Message(format!(
            "Master playlist has invalid {attribute}: {value}"
        )));
    }
    Ok(parsed)
}

fn alac_group_specs(group_id: &str) -> Result<(u32, u32), WrapperError> {
    let parts: Vec<&str> = group_id.split('-').collect();
    if parts.len() >= 2 {
        let rate = parse_positive_u32(parts[parts.len() - 2], "ALAC sample rate")?;
        let depth = parse_positive_u32(parts[parts.len() - 1], "ALAC bit depth")?;
        return Ok((rate, depth));
    }
    Err(WrapperError::Message(format!(
        "Master playlist has invalid ALAC audio group: {group_id}"
    )))
}

fn select_variant(
    variants: &[StreamVariant],
    preference: CodecPreference,
    content: &str,
) -> Result<AlacStreamInfo, WrapperError> {
    let mut best: Option<(i64, &StreamVariant, u32, u32)> = None;
    for variant in variants {
        let Some(rank) = group_rank(&variant.group_id, &variant.codec, preference)? else {
            continue;
        };
        let (sample_rate, bit_depth) = if variant.codec == "alac" {
            alac_group_specs(&variant.group_id)
        } else {
            media_line_specs(content, &variant.group_id)
        }?;
        let better = match best {
            None => true,
            Some((best_rank, _, _, _)) => rank > best_rank,
        };
        if better {
            best = Some((rank, variant, sample_rate, bit_depth));
        }
    }

    let (_, variant, sample_rate, bit_depth) = best.ok_or_else(|| {
        let what = match preference {
            CodecPreference::Atmos => "Dolby Atmos",
            _ => "no audio",
        };
        let message = format!("No {what} stream variant found in master playlist");
        if preference == CodecPreference::Atmos {
            WrapperError::Unavailable(WrapperUnavailableReason::NoAtmosVariantInValidMaster)
        } else {
            WrapperError::Message(message)
        }
    })?;

    Ok(AlacStreamInfo {
        stream_url: variant.uri.clone(),
        codec: variant.codec.clone(),
        sample_rate,
        bit_depth,
    })
}

fn media_line_specs(content: &str, group_id: &str) -> Result<(u32, u32), WrapperError> {
    let is_atmos = group_id.starts_with("audio-atmos");
    let mut sample_rate = None;
    let mut bit_depth = None;
    for line in content.lines() {
        let Some(attrs) = line.strip_prefix("#EXT-X-MEDIA:") else {
            continue;
        };
        let mut matches_group = false;
        let mut rate = None;
        let mut depth = None;
        for part in split_attributes(attrs)? {
            let (k, v) = part.split_once('=').expect("validated attribute component");
            let k = k.trim();
            let v = v.trim().trim_matches('"');
            if k == "GROUP-ID" && v == group_id {
                matches_group = true;
            } else if k == "SAMPLE-RATE" {
                rate = Some(parse_positive_u32(v, "SAMPLE-RATE")?);
            } else if k == "BIT-DEPTH" {
                depth = Some(parse_positive_u32(v, "BIT-DEPTH")?);
            }
        }
        if matches_group {
            sample_rate = rate.or(sample_rate);
            bit_depth = depth.or(bit_depth);
        }
    }
    if is_atmos {
        Ok((sample_rate.unwrap_or(48_000), bit_depth.unwrap_or(16)))
    } else {
        Ok((sample_rate.unwrap_or(44_100), bit_depth.unwrap_or(16)))
    }
}

pub fn parse_media_playlist(
    content: &str,
    media_url: &str,
) -> Result<MediaPlaylistInfo, WrapperError> {
    let mut init_uri = None;
    let mut init_byte_range = None;
    let mut segments = Vec::new();
    let mut current_key_uri = None;
    let mut next_range = None;
    let mut key_method = None;
    let mut cenc_kid_b64 = None;

    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        if let Some(attrs) = line.strip_prefix("#EXT-X-KEY:") {
            let mut key_uri = None;
            let mut method = None;
            let mut key_format = None;
            for part in split_attributes(attrs)? {
                let (k, v) = part.split_once('=').expect("validated attribute component");
                let k = k.trim();
                let v = v.trim().trim_matches('"');
                if k == "METHOD" {
                    method = Some(v.to_owned());
                } else if k == "URI" {
                    key_uri = Some(v.to_owned());
                } else if k == "KEYFORMAT" {
                    key_format = Some(v.to_owned());
                }
            }
            match method.as_deref() {
                Some("SAMPLE-AES")
                    if key_format
                        .as_deref()
                        .is_none_or(|format| format == "com.apple.streamingkeydelivery") =>
                {
                    current_key_uri = key_uri;
                }

                Some("ISO-23001-7") => {
                    current_key_uri = key_uri.clone();
                    if let Some(uri) = &key_uri {
                        cenc_kid_b64 = uri.rsplit(',').next().map(str::to_owned);
                    }
                }
                _ => {}
            }
            key_method = method;
            continue;
        }

        if let Some(attrs) = line.strip_prefix("#EXT-X-MAP:") {
            for part in split_attributes(attrs)? {
                let (k, v) = part.split_once('=').expect("validated attribute component");
                let k = k.trim();
                let v = v.trim().trim_matches('"');
                if k == "URI" {
                    init_uri = Some(resolve_url(media_url, v));
                } else if k == "BYTERANGE" {
                    init_byte_range = parse_byte_range(v);
                }
            }
            continue;
        }

        if let Some(range_str) = line.strip_prefix("#EXT-X-BYTERANGE:") {
            next_range = parse_byte_range(range_str);
            continue;
        }

        if !line.starts_with('#') {
            let seg_url = resolve_url(media_url, line);
            segments.push(MediaSegmentRef {
                uri: seg_url,
                byte_range: next_range.take(),
                key_uri: current_key_uri.clone(),
            });
        }
    }

    let init_uri = init_uri.ok_or_else(|| {
        WrapperError::Message("No #EXT-X-MAP init segment found in media playlist".into())
    })?;

    let single_file = if segments.iter().all(|s| s.uri == init_uri) {
        Some(init_uri.clone())
    } else {
        None
    };

    Ok(MediaPlaylistInfo {
        init_uri,
        init_byte_range,
        segments,
        single_file_url: single_file,
        key_method,
        cenc_kid_b64,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_master_and_media_playlist() {
        let master = r#"#EXTM3U
#EXT-X-VERSION:7
#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="audio-alac-stereo-44100-16",AUTOSELECT=YES,CHANNELS="2",NAME="songEnhanced",SAMPLE-RATE=44100,BIT-DEPTH=16
#EXT-X-STREAM-INF:AVERAGE-BANDWIDTH=828199,CODECS="alac",AUDIO="audio-alac-stereo-44100-16"
P290437790_A1529441498_audio_en_gr1411.m3u8"#;

        let base = "https://aod.itunes.apple.com/assets/master.m3u8";
        let info = parse_master_playlist(master, base, CodecPreference::HighestQuality)
            .expect("master playlist parse");
        assert_eq!(info.sample_rate, 44100);
        assert_eq!(info.bit_depth, 16);
        assert_eq!(
            info.stream_url,
            "https://aod.itunes.apple.com/assets/P290437790_A1529441498_audio_en_gr1411.m3u8"
        );

        let media = r#"#EXTM3U
#EXT-X-VERSION:7
#EXT-X-KEY:METHOD=SAMPLE-AES,URI="skd://itunes.apple.com/P000000000/s1/e1",KEYFORMAT="com.apple.streamingkeydelivery",KEYFORMATVERSIONS="1"
#EXT-X-MAP:URI="audio.mp4",BYTERANGE="1037@0"
#EXTINF:14.95365,
#EXT-X-BYTERANGE:1300622@1037
audio.mp4
#EXT-X-KEY:METHOD=SAMPLE-AES,URI="skd://itunes.apple.com/P290437790/c6",KEYFORMAT="com.apple.streamingkeydelivery",KEYFORMATVERSIONS="1"
#EXTINF:14.95365,
#EXT-X-BYTERANGE:1532212@1301659
audio.mp4"#;

        let media_info =
            parse_media_playlist(media, &info.stream_url).expect("media playlist parse");
        assert_eq!(
            media_info.init_uri,
            "https://aod.itunes.apple.com/assets/audio.mp4"
        );
        assert_eq!(media_info.init_byte_range, Some((0, 1037)));
        assert_eq!(media_info.segments.len(), 2);
        assert_eq!(media_info.segments[0].byte_range, Some((1037, 1300622)));
        assert_eq!(
            media_info.segments[0].key_uri.as_deref(),
            Some("skd://itunes.apple.com/P000000000/s1/e1")
        );
        assert_eq!(media_info.segments[1].byte_range, Some((1301659, 1532212)));
        assert_eq!(
            media_info.segments[1].key_uri.as_deref(),
            Some("skd://itunes.apple.com/P290437790/c6")
        );
        assert_eq!(
            media_info.single_file_url,
            Some("https://aod.itunes.apple.com/assets/audio.mp4".to_string())
        );
    }

    fn full_master() -> String {
        r#"#EXTM3U
#EXT-X-VERSION:7
#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="audio-stereo-256",AUTOSELECT=YES,CHANNELS="2",NAME="songEnhanced"
#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="audio-alac-stereo-96000-24",AUTOSELECT=YES,CHANNELS="2",NAME="songEnhanced",SAMPLE-RATE=96000,BIT-DEPTH=24
#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="audio-alac-stereo-48000-24",AUTOSELECT=YES,CHANNELS="2",NAME="songEnhanced",SAMPLE-RATE=48000,BIT-DEPTH=24
#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="audio-alac-stereo-192000-24",AUTOSELECT=YES,CHANNELS="2",NAME="songEnhanced",SAMPLE-RATE=192000,BIT-DEPTH=24
#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="audio-atmos-2768",AUTOSELECT=YES,CHANNELS="16/JOC",NAME="songEnhanced"
#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="audio-atmos-2448",AUTOSELECT=YES,CHANNELS="16/JOC",NAME="songEnhanced"
#EXT-X-STREAM-INF:AVERAGE-BANDWIDTH=263605,BANDWIDTH=270020,CODECS="mp4a.40.2",AUDIO="audio-stereo-256"
A_track_gr256.m3u8
#EXT-X-STREAM-INF:AVERAGE-BANDWIDTH=2858574,BANDWIDTH=3018699,CODECS="alac",AUDIO="audio-alac-stereo-96000-24"
A_track_alac_96.m3u8
#EXT-X-STREAM-INF:AVERAGE-BANDWIDTH=1751770,BANDWIDTH=1850877,CODECS="alac",AUDIO="audio-alac-stereo-48000-24"
A_track_alac_48.m3u8
#EXT-X-STREAM-INF:AVERAGE-BANDWIDTH=2858574,BANDWIDTH=3018699,CODECS="alac",AUDIO="audio-alac-stereo-192000-24"
A_track_alac_192.m3u8
#EXT-X-STREAM-INF:AVERAGE-BANDWIDTH=770158,BANDWIDTH=771138,CODECS="ec-3",AUDIO="audio-atmos-2768"
A_track_atmos_2768.m3u8
#EXT-X-STREAM-INF:AVERAGE-BANDWIDTH=450158,BANDWIDTH=450839,CODECS="ec-3",AUDIO="audio-atmos-2448"
A_track_atmos_2448.m3u8
#EXT-X-STREAM-INF:AVERAGE-BANDWIDTH=133444,BANDWIDTH=137870,CODECS="mp4a.40.2",AUDIO="audio-stereo-128"
A_track_gr128.m3u8
#EXT-X-STREAM-INF:AVERAGE-BANDWIDTH=69882,BANDWIDTH=78238,CODECS="mp4a.40.5",AUDIO="audio-HE-stereo-64"
A_track_gr64.m3u8"#
            .to_owned()
    }

    #[test]
    fn highest_quality_prefers_hi_res_alac() {
        let info = parse_master_playlist(
            &full_master(),
            "https://aod.itunes.apple.com/assets/master.m3u8",
            CodecPreference::HighestQuality,
        )
        .expect("parse");
        assert_eq!(info.codec, "alac");
        assert_eq!(info.sample_rate, 192_000);
        assert_eq!(info.bit_depth, 24);
        assert!(info.stream_url.ends_with("A_track_alac_192.m3u8"));
    }

    #[test]
    fn atmos_preference_picks_highest_atmos() {
        let info = parse_master_playlist(
            &full_master(),
            "https://aod.itunes.apple.com/assets/master.m3u8",
            CodecPreference::Atmos,
        )
        .expect("parse");
        assert_eq!(info.codec, "ec-3");
        assert_eq!(info.sample_rate, 48_000);
        assert!(info.stream_url.ends_with("A_track_atmos_2768.m3u8"));
    }

    #[test]
    fn atmos_selection_ignores_binaural_stereo_group_suffix() {
        let master = r#"#EXTM3U
#EXT-X-VERSION:7
#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="audio-stereo-256-binaural",AUTOSELECT=YES,CHANNELS="2",NAME="songEnhanced"
#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="audio-atmos-2768",AUTOSELECT=YES,CHANNELS="16/JOC",NAME="songEnhanced"
#EXT-X-STREAM-INF:BANDWIDTH=270020,CODECS="mp4a.40.2",AUDIO="audio-stereo-256-binaural"
A_track_gr256.m3u8
#EXT-X-STREAM-INF:BANDWIDTH=771138,CODECS="ec-3",AUDIO="audio-atmos-2768"
A_track_atmos_2768.m3u8"#;

        let info = parse_master_playlist(
            master,
            "https://aod.itunes.apple.com/assets/master.m3u8",
            CodecPreference::Atmos,
        )
        .expect("binaural suffix is valid provider metadata");
        assert_eq!(info.codec, "ec-3");
        assert!(info.stream_url.ends_with("A_track_atmos_2768.m3u8"));
    }

    #[test]
    fn aac_falls_back_when_no_alac() {
        let master = r#"#EXTM3U
#EXT-X-VERSION:7
#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="audio-stereo-256",AUTOSELECT=YES,CHANNELS="2",NAME="songEnhanced"
#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="audio-stereo-128",AUTOSELECT=YES,CHANNELS="2",NAME="songEnhanced"
#EXT-X-STREAM-INF:AVERAGE-BANDWIDTH=263605,BANDWIDTH=270020,CODECS="mp4a.40.2",AUDIO="audio-stereo-256"
A_track_gr256.m3u8
#EXT-X-STREAM-INF:AVERAGE-BANDWIDTH=133444,BANDWIDTH=137870,CODECS="mp4a.40.2",AUDIO="audio-stereo-128"
A_track_gr128.m3u8"#;
        let info = parse_master_playlist(
            master,
            "https://aod.itunes.apple.com/assets/master.m3u8",
            CodecPreference::HighestQuality,
        )
        .expect("parse");
        assert_eq!(info.codec, "mp4a.40.2");
        assert!(info.stream_url.ends_with("A_track_gr256.m3u8"));
    }

    #[test]
    fn atmos_preference_without_atmos_is_an_error() {
        let master = r#"#EXTM3U
#EXT-X-VERSION:7
#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="audio-stereo-256",AUTOSELECT=YES,CHANNELS="2",NAME="songEnhanced"
#EXT-X-STREAM-INF:AVERAGE-BANDWIDTH=263605,BANDWIDTH=270020,CODECS="mp4a.40.2",AUDIO="audio-stereo-256"
A_track_gr256.m3u8"#;
        let error = parse_master_playlist(
            master,
            "https://aod.itunes.apple.com/assets/master.m3u8",
            CodecPreference::Atmos,
        )
        .expect_err("no atmos variant");
        assert!(matches!(
            error,
            WrapperError::Unavailable(WrapperUnavailableReason::NoAtmosVariantInValidMaster)
        ));
    }

    #[test]
    fn malformed_media_attributes_are_retryable_errors() {
        let cases = [
            r#"#EXTM3U
#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="audio-atmos-2768",BROKEN,NAME="songEnhanced"
#EXT-X-STREAM-INF:BANDWIDTH=128000,CODECS="ec-3",AUDIO="audio-atmos-2768"
audio.m3u8"#,
            r#"#EXTM3U
#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="audio-atmos-2768",NAME="songEnhanced
#EXT-X-STREAM-INF:BANDWIDTH=128000,CODECS="ec-3",AUDIO="audio-atmos-2768"
audio.m3u8"#,
        ];

        for content in cases {
            let error = parse_master_playlist(
                content,
                "https://aod.itunes.apple.com/assets/master.m3u8",
                CodecPreference::Atmos,
            )
            .expect_err("malformed media attributes must be retryable");
            assert!(matches!(error, WrapperError::Message(_)), "{error:?}");
        }
    }

    #[test]
    fn malformed_non_master_and_empty_playlists_are_retryable_errors() {
        let cases = [
            "not an m3u8",
            "#EXTM3U\n#EXT-X-VERSION:7",
            "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=128000",
            "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=128000,broken\naudio.m3u8",
            "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=not-a-number,CODECS=\"mp4a.40.2\",AUDIO=\"audio-stereo-128\"\naudio.m3u8",
            "#EXTM3U\n#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"audio-stereo-128\",SAMPLE-RATE=not-a-number\n#EXT-X-STREAM-INF:BANDWIDTH=128000,CODECS=\"mp4a.40.2\",AUDIO=\"audio-stereo-128\"\naudio.m3u8",
            "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=128000,CODECS=\"ec-3\",AUDIO=\"audio-atmos-invalid\"\naudio.m3u8",
            "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=128000,CODECS=\"mp4a.40.2\",AUDIO=\"audio-stereo-not-a-number-binaural\"\naudio.m3u8",
        ];

        for content in cases {
            let error = parse_master_playlist(
                content,
                "https://aod.itunes.apple.com/assets/master.m3u8",
                CodecPreference::Atmos,
            )
            .expect_err("invalid master responses must not mean Atmos absence");
            assert!(matches!(error, WrapperError::Message(_)), "{error:?}");
        }
    }
}
