//! Native lossless conversion of fragmented MP4 audio into progressive MP4.
//!
//! The remuxer copies encoded samples and the original sample entry unchanged.
//! It supports one continuous audio track, which matches the fragmented music
//! files handled by the rip pipeline.

use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
    path::Path,
};

use mp4_track::{
    Codec, Error as Mp4Error, FourCc, Mp4Reader, Mp4Writer, SampleInfo, SampleInput, TrackConfig,
    TrackKind, WriterConfig,
};
use tokio_util::sync::CancellationToken;

use crate::MediaError;

const MAX_SAMPLE_ENTRY_BYTES: u64 = 16 * 1024 * 1024;

#[derive(Clone, Copy)]
struct BoxSpan {
    start: u64,
    size: u64,
    header_size: u64,
    kind: [u8; 4],
}

impl BoxSpan {
    fn end(self) -> Result<u64, MediaError> {
        self.start
            .checked_add(self.size)
            .ok_or_else(|| invalid("MP4 box end offset overflow"))
    }

    fn body(self) -> Result<u64, MediaError> {
        self.start
            .checked_add(self.header_size)
            .ok_or_else(|| invalid("MP4 box body offset overflow"))
    }

    fn is(self, kind: &[u8; 4]) -> bool {
        &self.kind == kind
    }
}

/// Converts a fragmented M4A source into progressive M4A at `destination`.
/// Returns `false` for an already progressive source so the caller can copy it.
pub(super) fn remux_if_fragmented(
    source: &Path,
    destination: &Path,
    cancellation: &CancellationToken,
) -> Result<bool, MediaError> {
    if cancellation.is_cancelled() {
        return Err(MediaError::Cancelled);
    }

    let mut source_file = File::open(source)?;
    let top_level = top_level_boxes(&mut source_file)?;
    if !top_level.iter().any(|box_| box_.is(b"moof")) {
        return Ok(false);
    }

    let result = remux_fragmented(
        source,
        &mut source_file,
        &top_level,
        destination,
        cancellation,
    );
    if result.is_err() {
        let _ = std::fs::remove_file(destination);
    }
    result.map(|()| true)
}

fn remux_fragmented(
    source: &Path,
    source_file: &mut File,
    top_level: &[BoxSpan],
    destination: &Path,
    cancellation: &CancellationToken,
) -> Result<(), MediaError> {
    let mut reader = Mp4Reader::open(File::open(source)?)
        .map_err(|error| remux_error("fragmented MP4 parse failed", error))?;
    if cancellation.is_cancelled() {
        return Err(MediaError::Cancelled);
    }
    if !reader.is_fragmented() {
        return Err(invalid(
            "MP4 has moof boxes but was not parsed as fragmented",
        ));
    }
    if reader.tracks().len() != 1 {
        return Err(invalid(format!(
            "fragmented audio remux requires one track, found {}",
            reader.tracks().len()
        )));
    }

    let track = reader
        .tracks()
        .first()
        .cloned()
        .ok_or_else(|| invalid("fragmented MP4 contains no tracks"))?;
    if track.kind != TrackKind::Audio {
        return Err(invalid("fragmented MP4 track is not audio"));
    }
    if !track.enabled {
        return Err(invalid("fragmented audio track is disabled"));
    }
    if track.timescale == 0 || reader.movie_timescale() == 0 {
        return Err(invalid("fragmented audio has a zero timescale"));
    }
    let moov = unique_box(top_level.iter().copied(), b"moov", "top level")?;
    let (entry, raw_sample_entry) = first_track_sample_entry(source_file, moov)?;
    reject_encrypted_input(source_file, top_level, entry, &raw_sample_entry)?;

    let sample_infos: Vec<SampleInfo> = reader
        .samples(track.id)
        .map_err(|error| remux_error("fragmented MP4 sample table failed", error))?
        .collect();
    if sample_infos.is_empty() {
        return Err(invalid("fragmented audio track contains no samples"));
    }

    let mut expected_dts = 0u64;
    for (index, sample) in sample_infos.iter().enumerate() {
        if usize::try_from(sample.index).ok() != Some(index) {
            return Err(invalid("fragmented audio samples are not in track order"));
        }
        if sample.dts != expected_dts {
            return Err(invalid(format!(
                "fragmented audio timeline is discontinuous at sample {}: expected DTS {expected_dts}, found {}",
                sample.index, sample.dts
            )));
        }
        if sample.duration == 0 || sample.size == 0 {
            return Err(invalid(format!(
                "fragmented audio sample {} has an empty duration or payload",
                sample.index
            )));
        }
        if sample.description_index != 1 {
            return Err(invalid(format!(
                "fragmented audio switches sample descriptions at sample {}",
                sample.index
            )));
        }
        i32::try_from(sample.cts_offset).map_err(|_| {
            invalid(format!(
                "fragmented audio composition offset is out of range at sample {}",
                sample.index
            ))
        })?;
        expected_dts = expected_dts
            .checked_add(u64::from(sample.duration))
            .ok_or_else(|| invalid("fragmented audio duration overflow"))?;
    }

    let track_config = TrackConfig {
        kind: track.kind,
        timescale: track.timescale,
        language: track.language.clone(),
        handler_name: track.handler_name.clone(),
        // The raw sample entry carries ALAC, AAC, EC3, and other codec boxes
        // verbatim, including codec-specific boxes this crate does not model.
        codec: Codec::Other {
            entry,
            raw: raw_sample_entry,
        },
        width: track.width,
        height: track.height,
        edit_list: track.edit_list.clone(),
    };
    let config = WriterConfig {
        movie_timescale: reader.movie_timescale(),
        ..WriterConfig::default()
    };
    let mut writer = Mp4Writer::new(File::create(destination)?, config)
        .map_err(|error| remux_error("progressive MP4 writer failed", error))?;
    let output_track = writer
        .add_track(track_config)
        .map_err(|error| remux_error("progressive MP4 track setup failed", error))?;

    let mut sample_buffer = Vec::new();
    for sample_info in sample_infos {
        if cancellation.is_cancelled() {
            return Err(MediaError::Cancelled);
        }
        let sample = reader
            .read_sample_into(track.id, sample_info.index, &mut sample_buffer)
            .map_err(|error| {
                remux_error(
                    &format!(
                        "could not read fragmented audio sample {}",
                        sample_info.index
                    ),
                    error,
                )
            })?;
        if sample.size != sample_info.size {
            return Err(invalid(format!(
                "fragmented audio sample {} changed size while reading",
                sample_info.index
            )));
        }
        writer
            .write_sample(
                output_track,
                &SampleInput {
                    data: &sample_buffer,
                    duration: sample_info.duration,
                    cts_offset: i32::try_from(sample_info.cts_offset).map_err(|_| {
                        invalid("fragmented audio composition offset is out of range")
                    })?,
                    is_sync: sample_info.is_sync,
                },
            )
            .map_err(|error| remux_error("progressive MP4 sample write failed", error))?;
    }
    if cancellation.is_cancelled() {
        return Err(MediaError::Cancelled);
    }
    writer
        .finish()
        .map_err(|error| remux_error("progressive MP4 finalization failed", error))?;
    Ok(())
}

fn top_level_boxes(file: &mut File) -> Result<Vec<BoxSpan>, MediaError> {
    let file_len = file.metadata()?.len();
    let mut boxes = Vec::new();
    let mut offset = 0u64;
    while offset < file_len {
        let box_ = read_box(file, offset, file_len)?;
        offset = box_.end()?;
        boxes.push(box_);
    }
    if offset != file_len {
        return Err(invalid("MP4 top-level boxes do not cover the file"));
    }
    Ok(boxes)
}

fn child_boxes(file: &mut File, parent: BoxSpan) -> Result<Vec<BoxSpan>, MediaError> {
    let mut boxes = Vec::new();
    let mut offset = parent.body()?;
    let end = parent.end()?;
    while offset < end {
        let child = read_box(file, offset, end)?;
        offset = child.end()?;
        boxes.push(child);
    }
    if offset != end {
        return Err(invalid("nested MP4 boxes do not cover their parent"));
    }
    Ok(boxes)
}

fn read_box(file: &mut File, start: u64, parent_end: u64) -> Result<BoxSpan, MediaError> {
    if start >= parent_end || parent_end.saturating_sub(start) < 8 {
        return Err(invalid(format!(
            "truncated MP4 box header at offset {start}"
        )));
    }
    file.seek(SeekFrom::Start(start))?;
    let mut header = [0u8; 16];
    file.read_exact(&mut header[..8])?;
    let size32 = u32::from_be_bytes([header[0], header[1], header[2], header[3]]);
    let kind = [header[4], header[5], header[6], header[7]];
    let (size, header_size) = match size32 {
        0 => (parent_end - start, 8),
        1 => {
            if parent_end.saturating_sub(start) < 16 {
                return Err(invalid(format!(
                    "truncated large MP4 box at offset {start}"
                )));
            }
            file.read_exact(&mut header[8..16])?;
            (
                u64::from_be_bytes([
                    header[8], header[9], header[10], header[11], header[12], header[13],
                    header[14], header[15],
                ]),
                16,
            )
        }
        other => (u64::from(other), 8),
    };
    let end = start
        .checked_add(size)
        .ok_or_else(|| invalid(format!("MP4 box size overflow at offset {start}")))?;
    if size < header_size || end > parent_end {
        return Err(invalid(format!("invalid MP4 box bounds at offset {start}")));
    }
    Ok(BoxSpan {
        start,
        size,
        header_size,
        kind,
    })
}

fn unique_box(
    boxes: impl IntoIterator<Item = BoxSpan>,
    kind: &[u8; 4],
    parent: &str,
) -> Result<BoxSpan, MediaError> {
    let mut matches = boxes.into_iter().filter(|box_| box_.is(kind));
    let found = matches.next().ok_or_else(|| {
        invalid(format!(
            "missing {} box in {parent}",
            printable_fourcc(kind)
        ))
    })?;
    if matches.next().is_some() {
        return Err(invalid(format!(
            "duplicate {} boxes in {parent}",
            printable_fourcc(kind)
        )));
    }
    Ok(found)
}

fn child_box(file: &mut File, parent: BoxSpan, kind: &[u8; 4]) -> Result<BoxSpan, MediaError> {
    unique_box(
        child_boxes(file, parent)?,
        kind,
        &printable_fourcc(&parent.kind),
    )
}

fn first_track_sample_entry(
    file: &mut File,
    moov: BoxSpan,
) -> Result<(FourCc, Vec<u8>), MediaError> {
    let tracks: Vec<_> = child_boxes(file, moov)?
        .into_iter()
        .filter(|box_| box_.is(b"trak"))
        .collect();
    if tracks.len() != 1 {
        return Err(invalid(format!(
            "fragmented audio remux requires one track, found {}",
            tracks.len()
        )));
    }
    let track = tracks
        .first()
        .copied()
        .ok_or_else(|| invalid("fragmented audio has no track"))?;
    let mdia = child_box(file, track, b"mdia")?;
    let minf = child_box(file, mdia, b"minf")?;
    let stbl = child_box(file, minf, b"stbl")?;
    let stsd = child_box(file, stbl, b"stsd")?;
    let stsd_body = stsd.body()?;
    let entries_start = stsd_body
        .checked_add(8)
        .ok_or_else(|| invalid("stsd sample entry offset overflow"))?;
    let stsd_end = stsd.end()?;
    if entries_start > stsd_end {
        return Err(invalid("truncated stsd sample description"));
    }
    let entry_count_offset = stsd_body
        .checked_add(4)
        .ok_or_else(|| invalid("stsd sample entry count offset overflow"))?;
    let entry_count = read_u32(file, entry_count_offset)?;
    if entry_count == 0 {
        return Err(invalid("fragmented audio has no sample descriptions"));
    }
    let mut offset = entries_start;
    let mut first = None;
    for index in 0..entry_count {
        let entry = read_box(file, offset, stsd_end)?;
        if index == 0 {
            let raw_size = entry
                .size
                .checked_sub(entry.header_size)
                .ok_or_else(|| invalid("sample entry size underflow"))?;
            if raw_size > MAX_SAMPLE_ENTRY_BYTES {
                return Err(invalid("sample entry exceeds the 16 MiB safety limit"));
            }
            let raw_len = usize::try_from(raw_size)
                .map_err(|_| invalid("sample entry size does not fit memory"))?;
            let mut raw = vec![0u8; raw_len];
            file.seek(SeekFrom::Start(entry.body()?))?;
            file.read_exact(&mut raw)?;
            first = Some((FourCc::new(entry.kind), raw));
        }
        offset = entry.end()?;
    }
    if offset != stsd_end {
        return Err(invalid(
            "stsd entries do not cover the sample description box",
        ));
    }
    first.ok_or_else(|| invalid("fragmented audio has no sample entry"))
}

fn reject_encrypted_input(
    file: &mut File,
    top_level: &[BoxSpan],
    sample_entry: FourCc,
    raw_sample_entry: &[u8],
) -> Result<(), MediaError> {
    if top_level.iter().any(|box_| box_.is(b"pssh")) {
        return Err(invalid(
            "encrypted MP4 protection data remains after decryption",
        ));
    }
    let moov = unique_box(top_level.iter().copied(), b"moov", "top level")?;
    for child in child_boxes(file, moov)? {
        if child.is(b"pssh") {
            return Err(invalid(
                "encrypted MP4 protection data remains after decryption",
            ));
        }
    }
    if sample_entry == FourCc::new(*b"enca") || sample_entry == FourCc::new(*b"encv") {
        return Err(invalid("encrypted sample entry remains after decryption"));
    }
    if contains_sinf(raw_sample_entry)? {
        return Err(invalid("encrypted sample entry remains after decryption"));
    }
    for moof in top_level.iter().copied().filter(|box_| box_.is(b"moof")) {
        for traf in child_boxes(file, moof)?
            .into_iter()
            .filter(|box_| box_.is(b"traf"))
        {
            for child in child_boxes(file, traf)? {
                if child.is(b"senc") || child.is(b"saiz") || child.is(b"saio") {
                    return Err(invalid(
                        "fragment encryption auxiliary data remains after decryption",
                    ));
                }
                if (child.is(b"sgpd") || child.is(b"sbgp"))
                    && fourcc_at(file, child.body()?.saturating_add(4))? == *b"seig"
                {
                    return Err(invalid("sample encryption groups remain after decryption"));
                }
            }
        }
    }
    Ok(())
}

fn contains_sinf(raw_sample_entry: &[u8]) -> Result<bool, MediaError> {
    if raw_sample_entry.len() < 28 {
        return Err(invalid("truncated audio sample entry"));
    }
    let version = u16::from_be_bytes([raw_sample_entry[8], raw_sample_entry[9]]);
    let child_offset = match version {
        0 => 28,
        1 => 44,
        2 => 64,
        _ => return Err(invalid("unsupported audio sample entry version")),
    };
    let mut offset = child_offset;
    while offset < raw_sample_entry.len() {
        let remaining = raw_sample_entry.len().saturating_sub(offset);
        if remaining < 8 {
            return Err(invalid("truncated sample entry child box"));
        }
        let size32 = u32::from_be_bytes([
            raw_sample_entry[offset],
            raw_sample_entry[offset + 1],
            raw_sample_entry[offset + 2],
            raw_sample_entry[offset + 3],
        ]);
        let kind = [
            raw_sample_entry[offset + 4],
            raw_sample_entry[offset + 5],
            raw_sample_entry[offset + 6],
            raw_sample_entry[offset + 7],
        ];
        let (size, header_size) = if size32 == 1 {
            if remaining < 16 {
                return Err(invalid("truncated large sample entry child box"));
            }
            let size = u64::from_be_bytes([
                raw_sample_entry[offset + 8],
                raw_sample_entry[offset + 9],
                raw_sample_entry[offset + 10],
                raw_sample_entry[offset + 11],
                raw_sample_entry[offset + 12],
                raw_sample_entry[offset + 13],
                raw_sample_entry[offset + 14],
                raw_sample_entry[offset + 15],
            ]);
            (size, 16usize)
        } else if size32 == 0 {
            (u64::try_from(remaining).unwrap_or(u64::MAX), 8usize)
        } else {
            (u64::from(size32), 8usize)
        };
        if size < header_size as u64 || size > u64::try_from(remaining).unwrap_or(u64::MAX) {
            return Err(invalid("invalid sample entry child box bounds"));
        }
        if kind == *b"sinf" {
            return Ok(true);
        }
        offset = offset
            .checked_add(
                usize::try_from(size).map_err(|_| invalid("sample entry child too large"))?,
            )
            .ok_or_else(|| invalid("sample entry child offset overflow"))?;
    }
    Ok(false)
}

fn read_u32(file: &mut File, offset: u64) -> Result<u32, MediaError> {
    file.seek(SeekFrom::Start(offset))?;
    let mut bytes = [0u8; 4];
    file.read_exact(&mut bytes)?;
    Ok(u32::from_be_bytes(bytes))
}

fn fourcc_at(file: &mut File, offset: u64) -> Result<[u8; 4], MediaError> {
    file.seek(SeekFrom::Start(offset))?;
    let mut bytes = [0u8; 4];
    file.read_exact(&mut bytes)?;
    Ok(bytes)
}

fn printable_fourcc(kind: &[u8; 4]) -> String {
    String::from_utf8_lossy(kind).into_owned()
}

fn invalid(message: impl Into<String>) -> MediaError {
    MediaError::Invalid(message.into())
}

fn remux_error(context: &str, error: Mp4Error) -> MediaError {
    match error {
        Mp4Error::Io(error) => MediaError::Io(error),
        other => invalid(format!("{context}: {other}")),
    }
}
