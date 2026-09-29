//! Duration recovery for fragmented MP4 (fMP4) sources.
//!
//! Apple's HLS output is a fragmented MP4: an initial `moov` followed by many
//! `moof`/`mdat` pairs. Such a file carries **no** duration — the `mvhd` and
//! `mdhd` fields exist but are zero, because the duration is only knowable once
//! every fragment has been seen. Symphonia, like most readers, trusts those
//! fields and reports `0`.
//!
//! That `0` used to be fatal: the rip pipeline validates each file before and
//! after tagging and rejects anything without a duration, so every fragmented
//! track failed with "finalized file has no duration" however perfectly the
//! audio had downloaded.
//!
//! The duration *is* recoverable from the fragments. Each `moof` carries a
//! `tfdt` (base media decode time) plus its samples' durations, taken from
//! `trun` entries, else the `tfhd` default, else the `trex` default in `mvex`.
//! The track ends at the largest `tfdt + fragment_duration` across fragments.
//!
//! [`stamp_fragmented_duration`] also writes that value back into the `mvhd`
//! and `mdhd`, so the file stops lying to every other reader — players, taggers
//! and `ffprobe` included.

use std::{
    io::{Cursor, Read, Seek, SeekFrom},
    path::Path,
};

/// A parsed box header.
struct BoxHeader {
    start: u64,
    size: u64,
    kind: [u8; 4],
    /// Length of the size-and-type prefix, 8 or 16 for 64-bit sizes.
    header: u64,
}

impl BoxHeader {
    fn body(&self) -> u64 {
        self.start + self.header
    }

    fn end(&self) -> u64 {
        self.start + self.size
    }

    fn is(&self, kind: &[u8; 4]) -> bool {
        &self.kind == kind
    }
}

/// The version/flags word every full box carries before its payload.
struct FullBox {
    version: u8,
    /// First payload byte after version and flags.
    payload: u64,
}

impl FullBox {
    /// Bytes of `creation_time` plus `modification_time` before the timescale:
    /// 8 for version 0, 16 for version 1.
    fn time_prefix(&self) -> u64 {
        if self.version == 1 { 16 } else { 8 }
    }

    /// Offset of the duration field, which follows creation, modification and
    /// timescale.
    fn duration_offset(&self, header: &BoxHeader) -> (u64, bool) {
        (
            header.body() + 4 + self.time_prefix() + 4,
            self.version == 1,
        )
    }
}

/// The `mvhd` or `mdhd` duration field: where it lives and how wide it is.
#[derive(Clone, Copy)]
struct DurationField {
    offset: u64,
    wide: bool,
}

impl DurationField {
    /// Width in bytes: 4 unless the box is version 1.
    fn width(&self) -> usize {
        if self.wide { 8 } else { 4 }
    }

    /// Encode a tick count right-aligned at this field's width, so the caller
    /// takes the trailing `width()` bytes rather than the leading ones.
    fn encode(&self, ticks: u64) -> [u8; 8] {
        let bytes = ticks.to_be_bytes();
        let mut out = [0u8; 8];
        out[8 - self.width()..].copy_from_slice(&bytes[8 - self.width()..]);
        out
    }
}

/// What a scan learned about a file's fragment layout.
#[derive(Default)]
struct FragmentScan {
    fragments: usize,
    /// Media timescale, from `mdhd`, falling back to `mvhd`.
    timescale: u64,
    /// Largest `tfdt + fragment duration`, in media timescale units.
    end_ticks: u64,
    /// Sum of fragment durations, used only when no fragment carries a `tfdt`.
    sum_ticks: u64,
    trex_default: Option<u32>,
    mvhd: Option<DurationField>,
    mdhd: Option<DurationField>,
}

impl FragmentScan {
    /// The duration in seconds, or `None` when this is not a fragmented file
    /// or the fragments do not resolve to a usable timescale.
    fn duration_secs(&self) -> Option<f64> {
        if self.fragments == 0 || self.timescale == 0 {
            return None;
        }
        let ticks = if self.end_ticks > 0 {
            self.end_ticks
        } else {
            self.sum_ticks
        };
        if ticks == 0 {
            return None;
        }
        let duration = ticks as f64 / self.timescale as f64;
        (duration > 0.0).then_some(duration)
    }
}

fn read_header<R: Read + Seek>(reader: &mut R, offset: u64, limit: u64) -> Option<BoxHeader> {
    if offset.checked_add(8)? > limit {
        return None;
    }
    reader.seek(SeekFrom::Start(offset)).ok()?;
    let mut head = [0u8; 8];
    reader.read_exact(&mut head).ok()?;
    let mut size = u32::from_be_bytes([head[0], head[1], head[2], head[3]]) as u64;
    let mut header = 8u64;

    match size {
        // A size of 1 means a 64-bit size follows the type.
        1 => {
            let mut wide = [0u8; 8];
            reader.read_exact(&mut wide).ok()?;
            size = u64::from_be_bytes(wide);
            header = 16;
        }
        // A size of 0 means the box runs to the end of its container.
        0 => size = limit - offset,
        _ => {}
    }

    if size < header || offset.checked_add(size)? > limit {
        return None;
    }
    Some(BoxHeader {
        start: offset,
        size,
        kind: [head[4], head[5], head[6], head[7]],
        header,
    })
}

/// Every child box between `start` and `end`.
///
/// Collected rather than streamed so nested walks do not have to thread a
/// reborrow of the reader through a closure. A fragment has a handful of
/// children at each level, so this stays tiny.
fn children<R: Read + Seek>(reader: &mut R, start: u64, end: u64) -> Vec<BoxHeader> {
    let mut found = Vec::new();
    let mut offset = start;
    while let Some(header) = read_header(reader, offset, end) {
        offset = header.end();
        found.push(header);
    }
    found
}

fn u8_at<R: Read + Seek>(reader: &mut R, offset: u64) -> Option<u8> {
    reader.seek(SeekFrom::Start(offset)).ok()?;
    let mut buf = [0u8; 1];
    reader.read_exact(&mut buf).ok()?;
    Some(buf[0])
}

fn u32_at<R: Read + Seek>(reader: &mut R, offset: u64) -> Option<u32> {
    reader.seek(SeekFrom::Start(offset)).ok()?;
    let mut buf = [0u8; 4];
    reader.read_exact(&mut buf).ok()?;
    Some(u32::from_be_bytes(buf))
}

fn u64_at<R: Read + Seek>(reader: &mut R, offset: u64) -> Option<u64> {
    reader.seek(SeekFrom::Start(offset)).ok()?;
    let mut buf = [0u8; 8];
    reader.read_exact(&mut buf).ok()?;
    Some(u64::from_be_bytes(buf))
}

/// The 24-bit flag word that follows a full box's version byte.
///
/// Read as the three bytes after the version, so the first payload byte is
/// shifted out rather than masked off — masking would discard the top flag
/// byte, which is where flags like `default-base-is-moof` live.
fn flags_at<R: Read + Seek>(reader: &mut R, body: u64) -> Option<u32> {
    Some((u32_at(reader, body + 1)? >> 8) & 0x00ff_ffff)
}

fn full_box<R: Read + Seek>(reader: &mut R, header: &BoxHeader) -> Option<FullBox> {
    Some(FullBox {
        version: u8_at(reader, header.body())?,
        payload: header.body() + 4,
    })
}

/// Locate `mvhd`, the audio `mdhd`, and the `trex` sample-duration default.
fn scan_moov<R: Read + Seek>(reader: &mut R, moov: &BoxHeader, scan: &mut FragmentScan) {
    for child in children(reader, moov.body(), moov.end()) {
        if child.is(b"mvhd") {
            scan_full_header(reader, &child, scan, false);
        } else if child.is(b"trak") {
            scan_mdia(reader, &child, scan);
        } else if child.is(b"mvex") {
            for grand in children(reader, child.body(), child.end()) {
                if grand.is(b"trex") {
                    // version/flags, track_ID, description index, then duration.
                    scan.trex_default = u32_at(reader, grand.body() + 12);
                }
            }
        }
    }
}

fn scan_mdia<R: Read + Seek>(reader: &mut R, trak: &BoxHeader, scan: &mut FragmentScan) {
    for child in children(reader, trak.body(), trak.end()) {
        if !child.is(b"mdia") {
            continue;
        }
        for grand in children(reader, child.body(), child.end()) {
            if grand.is(b"mdhd") {
                scan_full_header(reader, &grand, scan, true);
            }
        }
    }
}

/// `mvhd` and `mdhd` share a layout: version/flags, creation, modification,
/// timescale, duration. The media timescale is authoritative over the movie one.
fn scan_full_header<R: Read + Seek>(
    reader: &mut R,
    header: &BoxHeader,
    scan: &mut FragmentScan,
    is_media: bool,
) {
    let Some(full) = full_box(reader, header) else {
        return;
    };
    let Some(timescale) = u32_at(reader, full.payload + full.time_prefix()) else {
        return;
    };
    if is_media || scan.timescale == 0 {
        scan.timescale = u64::from(timescale);
    }
    let (offset, wide) = full.duration_offset(header);
    let field = Some(DurationField { offset, wide });
    if is_media {
        scan.mdhd = field;
    } else {
        scan.mvhd = field;
    }
}

/// Sum one fragment's sample durations in media timescale units.
///
/// `None` means some sample's duration could not be resolved, so this fragment
/// cannot contribute to the total.
fn fragment_ticks<R: Read + Seek>(
    reader: &mut R,
    moof: &BoxHeader,
    trex_default: Option<u32>,
) -> Option<u64> {
    let mut total: u64 = 0;
    let mut resolved = false;

    for traf in children(reader, moof.body(), moof.end()) {
        if !traf.is(b"traf") {
            continue;
        }
        // `tfhd` precedes `trun` in a well-formed traf, but reading every box
        // first keeps the order irrelevant.
        let declared = children(reader, traf.body(), traf.end())
            .iter()
            .find(|box_| box_.is(b"tfhd"))
            .and_then(|box_| tfhd_default_duration(reader, box_));
        let default_duration = declared.or_else(|| trex_default.filter(|v| *v > 0));

        for child in children(reader, traf.body(), traf.end()) {
            if child.is(b"trun") {
                total = total.checked_add(run_ticks(reader, &child, default_duration)?)?;
                resolved = true;
            }
        }
    }

    resolved.then_some(total)
}

/// The `tfhd` default sample duration, when the box declares one.
fn tfhd_default_duration<R: Read + Seek>(reader: &mut R, header: &BoxHeader) -> Option<u32> {
    let flags = flags_at(reader, header.body())?;
    // `track_ID` is unconditional; every field after it is flag-gated.
    let mut offset = header.body() + 8;
    if flags & 0x01 != 0 {
        offset += 8;
    }
    if flags & 0x02 != 0 {
        offset += 4;
    }
    if flags & 0x08 == 0 {
        return None;
    }
    u32_at(reader, offset)
}

/// One `trun`'s total duration: its samples' durations, either carried inline
/// or taken from the applicable default.
fn run_ticks<R: Read + Seek>(
    reader: &mut R,
    header: &BoxHeader,
    default_duration: Option<u32>,
) -> Option<u64> {
    let flags = flags_at(reader, header.body())?;
    let count = u32_at(reader, header.body() + 4)?;
    let version = u8_at(reader, header.body())?;
    let mut offset = header.body() + 8;
    if flags & 0x01 != 0 {
        offset += 4;
    }
    if flags & 0x04 != 0 {
        offset += 4;
    }

    // Bytes each sample occupies in a `trun`, past the per-run fields.
    let stride = 4 * u64::from(flags & 0x100 != 0)
        + 4 * u64::from(flags & 0x200 != 0)
        + 4 * u64::from(flags & 0x400 != 0)
        + if flags & 0x800 != 0 {
            // Composition offsets are signed and widen in version 1.
            if version == 1 { 8 } else { 4 }
        } else {
            0
        };

    if flags & 0x100 == 0 {
        return u64::from(count).checked_mul(u64::from(default_duration?));
    }

    let mut total: u64 = 0;
    for index in 0..u64::from(count) {
        let at = offset.checked_add(index.checked_mul(stride)?)?;
        total = total.checked_add(u64::from(u32_at(reader, at)?))?;
    }
    Some(total)
}

/// A `moof`'s base media decode time, when it declares one.
fn base_decode_time<R: Read + Seek>(reader: &mut R, moof: &BoxHeader) -> Option<u64> {
    for traf in children(reader, moof.body(), moof.end()) {
        if !traf.is(b"traf") {
            continue;
        }
        for child in children(reader, traf.body(), traf.end()) {
            if !child.is(b"tfdt") {
                continue;
            }
            return match u8_at(reader, child.body())? {
                1 => u64_at(reader, child.body() + 4),
                _ => u32_at(reader, child.body() + 4).map(u64::from),
            };
        }
    }
    None
}

fn scan<R: Read + Seek>(reader: &mut R, limit: u64) -> FragmentScan {
    let mut scan = FragmentScan::default();

    for header in children(reader, 0, limit) {
        if header.is(b"moov") {
            scan_moov(reader, &header, &mut scan);
        } else if header.is(b"moof") {
            scan.fragments += 1;
            let Some(ticks) = fragment_ticks(reader, &header, scan.trex_default) else {
                continue;
            };
            scan.sum_ticks = scan.sum_ticks.saturating_add(ticks);
            if let Some(base) = base_decode_time(reader, &header) {
                scan.end_ticks = scan.end_ticks.max(base.saturating_add(ticks));
            }
        }
    }

    scan
}

/// Recover the duration of a fragmented MP4 on disk.
///
/// Returns `None` for a progressive file, an unreadable file, or one whose
/// fragments do not resolve, so the caller keeps whatever the container said.
pub(super) fn fragmented_duration_secs(source: &Path) -> Option<f64> {
    let mut file = std::fs::File::open(source).ok()?;
    let limit = file.metadata().ok()?.len();
    scan(&mut file, limit).duration_secs()
}

/// Write the real duration into a fragmented MP4's `mvhd` and `mdhd`, and
/// return it in seconds.
///
/// Apple's HLS output is fragmented and reports a duration of zero, which makes
/// it unusable downstream: players show no seek bar and validation rejects it
/// outright. Writing the recovered value back into those two fields makes the
/// container self-describing for every reader, with no remux and no external
/// binary.
///
/// Both fields are fixed-width, so this never resizes the buffer. A progressive
/// file — one with no fragments — is left untouched.
pub fn stamp_fragmented_duration(fmp4: &mut [u8]) -> Option<f64> {
    let scan = scan(&mut Cursor::new(&fmp4[..]), fmp4.len() as u64);
    let duration = scan.duration_secs()?;
    let ticks = if scan.end_ticks > 0 {
        scan.end_ticks
    } else {
        scan.sum_ticks
    };

    for field in [scan.mvhd, scan.mdhd].into_iter().flatten() {
        let width = field.width();
        let at = field.offset as usize;
        let end = at.checked_add(width)?;
        if end > fmp4.len() {
            return None;
        }
        fmp4[at..end].copy_from_slice(&field.encode(ticks)[8 - width..]);
    }

    Some(duration)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Minimal box: size, type, body.
    fn box_of(kind: &[u8; 4], body: &[u8]) -> Vec<u8> {
        let mut out = ((body.len() + 8) as u32).to_be_bytes().to_vec();
        out.extend_from_slice(kind);
        out.extend_from_slice(body);
        out
    }

    /// A box whose payload is preceded by a version/flags word.
    fn full(kind: &[u8; 4], version: u8, payload: &[u8]) -> Vec<u8> {
        let mut body = vec![version, 0, 0, 0];
        body.extend_from_slice(payload);
        box_of(kind, &body)
    }

    /// A box whose flags word is set explicitly.
    fn full_with_flags(kind: &[u8; 4], flags: u32, payload: &[u8]) -> Vec<u8> {
        // version occupies one byte, then 24 bits of flags.
        let mut body = vec![0u8, (flags >> 16) as u8, (flags >> 8) as u8, flags as u8];
        body.extend_from_slice(payload);
        box_of(kind, &body)
    }

    /// A `moov` whose duration fields are zero, exactly as Apple emits it.
    fn zeroed_moov(timescale: u32, trex_default: Option<u32>) -> Vec<u8> {
        let media_times = {
            let mut payload = Vec::new();
            payload.extend_from_slice(&0u32.to_be_bytes()); // creation
            payload.extend_from_slice(&0u32.to_be_bytes()); // modification
            payload.extend_from_slice(&timescale.to_be_bytes());
            payload.extend_from_slice(&0u32.to_be_bytes()); // duration
            payload
        };

        let mut movie_times = media_times.clone();
        if trex_default.is_some() {
            movie_times[8..12].copy_from_slice(&0u32.to_be_bytes());
        }

        let mut moov = full(b"mvhd", 0, &movie_times);
        let trak = box_of(b"trak", &box_of(b"mdia", &full(b"mdhd", 0, &media_times)));
        moov.extend_from_slice(&trak);

        if let Some(default) = trex_default {
            let mut trex = Vec::new();
            trex.extend_from_slice(&1u32.to_be_bytes()); // track_ID
            trex.extend_from_slice(&1u32.to_be_bytes()); // description index
            trex.extend_from_slice(&default.to_be_bytes());
            trex.extend_from_slice(&0u32.to_be_bytes()); // default sample size
            trex.extend_from_slice(&0u32.to_be_bytes()); // default sample flags
            moov.extend_from_slice(&box_of(b"mvex", &full(b"trex", 0, &trex)));
        }

        box_of(b"moov", &moov)
    }

    /// A `tfhd` declaring a default sample duration.
    fn tfhd(default_sample_duration: u32) -> Vec<u8> {
        let mut payload = Vec::new();
        payload.extend_from_slice(&1u32.to_be_bytes()); // track_ID
        payload.extend_from_slice(&default_sample_duration.to_be_bytes());
        // default-base-is-moof | default-sample-duration-present
        full_with_flags(b"tfhd", 0x02_0008, &payload)
    }

    fn tfdt(version: u8, value: u64) -> Vec<u8> {
        let payload = if version == 1 {
            value.to_be_bytes().to_vec()
        } else {
            (value as u32).to_be_bytes().to_vec()
        };
        full(b"tfdt", version, &payload)
    }

    /// A `trun` with no inline durations, so the default applies.
    fn trun_default(sample_count: u32) -> Vec<u8> {
        let mut payload = Vec::new();
        payload.extend_from_slice(&sample_count.to_be_bytes());
        payload.extend_from_slice(&0u32.to_be_bytes()); // data offset
        for _ in 0..sample_count {
            payload.extend_from_slice(&2u32.to_be_bytes()); // sample size
        }
        // data-offset-present | sample-size-present
        full_with_flags(b"trun", 0x0201, &payload)
    }

    /// A `trun` carrying its own sample durations.
    fn trun_durations(durations: &[u32]) -> Vec<u8> {
        let mut payload = Vec::new();
        payload.extend_from_slice(&(durations.len() as u32).to_be_bytes());
        payload.extend_from_slice(&0u32.to_be_bytes()); // data offset
        for value in durations {
            payload.extend_from_slice(&value.to_be_bytes());
            payload.extend_from_slice(&2u32.to_be_bytes()); // sample size
        }
        // data-offset-present | sample-size-present | sample-duration-present
        full_with_flags(b"trun", 0x0301, &payload)
    }

    fn fragment(
        tfdt_version: u8,
        base: u64,
        tfhd_default: Option<u32>,
        runs: Vec<Vec<u8>>,
    ) -> Vec<u8> {
        let mut traf = Vec::new();
        if let Some(default) = tfhd_default {
            traf.extend_from_slice(&tfhd(default));
        }
        traf.extend_from_slice(&tfdt(tfdt_version, base));
        for run in runs {
            traf.extend_from_slice(&run);
        }

        let mut mfhd = vec![0u8, 0, 0, 0];
        mfhd.extend_from_slice(&1u32.to_be_bytes());
        let mut moof = box_of(b"mfhd", &mfhd);
        moof.extend_from_slice(&box_of(b"traf", &traf));

        let mut out = moov_prefixed(&moof);
        out.extend_from_slice(&box_of(b"mdat", &[0u8; 16]));
        out
    }

    /// Re-label a child-only buffer as a `moof`.
    fn moov_prefixed(child: &[u8]) -> Vec<u8> {
        let mut out = ((child.len() + 8) as u32).to_be_bytes().to_vec();
        out.extend_from_slice(b"moof");
        out.extend_from_slice(child);
        out
    }

    fn build(fragments: Vec<Vec<u8>>, trex_default: Option<u32>) -> Vec<u8> {
        let mut out = box_of(b"ftyp", b"iso5m4a");
        out.extend_from_slice(&zeroed_moov(44_100, trex_default));
        for fragment in fragments {
            out.extend_from_slice(&fragment);
        }
        out
    }

    fn stamp(bytes: &mut [u8]) -> Option<f64> {
        stamp_fragmented_duration(bytes)
    }

    #[test]
    fn sums_fragments_via_tfhd_default_duration() {
        let mut bytes = build(
            vec![
                fragment(0, 0, Some(4096), vec![trun_default(161)]),
                fragment(0, 161 * 4096, Some(4096), vec![trun_default(161)]),
                fragment(0, 2 * 161 * 4096, Some(4096), vec![trun_default(52)]),
            ],
            None,
        );
        let expected = (2.0 * 161.0 + 52.0) * 4096.0 / 44_100.0;
        let got = stamp(&mut bytes).expect("duration");
        assert!(
            (got - expected).abs() < 0.001,
            "expected {expected}, got {got}"
        );
    }

    #[test]
    fn honours_per_sample_durations_in_trun() {
        let mut bytes = build(
            vec![fragment(
                0,
                0,
                Some(4096),
                vec![trun_durations(&[1000, 2000, 3000])],
            )],
            None,
        );
        let got = stamp(&mut bytes).expect("duration");
        assert!((got - 6000.0 / 44_100.0).abs() < 0.0001, "got {got}");
    }

    #[test]
    fn falls_back_to_trex_default_duration() {
        // No `tfhd`, so the `trex` default is the only duration source.
        let mut bytes = build(
            vec![fragment(0, 0, None, vec![trun_default(10)])],
            Some(441),
        );
        let got = stamp(&mut bytes).expect("duration");
        assert!((got - 4410.0 / 44_100.0).abs() < 0.0001, "got {got}");
    }

    #[test]
    fn tfhd_default_outranks_trex_default() {
        let mut bytes = build(
            vec![fragment(0, 0, Some(4096), vec![trun_default(10)])],
            Some(441),
        );
        let got = stamp(&mut bytes).expect("duration");
        assert!((got - 10.0 * 4096.0 / 44_100.0).abs() < 0.0001, "got {got}");
    }

    #[test]
    fn version_one_tfdt_is_read_as_64_bit() {
        let mut bytes = build(
            vec![fragment(
                1,
                5_000_000_000,
                Some(4096),
                vec![trun_default(10)],
            )],
            None,
        );
        let got = stamp(&mut bytes).expect("duration");
        let expected = (5_000_000_000.0 + 10.0 * 4096.0) / 44_100.0;
        assert!(
            (got - expected).abs() < 0.001,
            "expected {expected}, got {got}"
        );
    }

    #[test]
    fn writes_the_duration_into_mvhd_and_mdhd() {
        let mut bytes = build(
            vec![fragment(0, 0, Some(4096), vec![trun_default(100)])],
            None,
        );
        let stamped = stamp(&mut bytes).expect("duration");
        let expected_ticks = 100u64 * 4096;

        // Both headers start at zero; that zero is exactly what made every
        // downstream reader treat the track as having no duration at all.
        let fields = scan(&mut Cursor::new(&bytes[..]), bytes.len() as u64);
        let mvhd = fields.mvhd.expect("mvhd duration field");
        let mdhd = fields.mdhd.expect("mdhd duration field");
        assert_eq!(read_field(&bytes, mvhd), expected_ticks, "mvhd");
        assert_eq!(read_field(&bytes, mdhd), expected_ticks, "mdhd");

        assert!(
            (stamped - expected_ticks as f64 / 44_100.0).abs() < 0.0001,
            "got {stamped}"
        );
    }

    /// Read back a duration field the way another container parser would.
    fn read_field(bytes: &[u8], field: DurationField) -> u64 {
        let at = field.offset as usize;
        if field.wide {
            u64::from_be_bytes(bytes[at..at + 8].try_into().expect("64-bit field"))
        } else {
            u64::from(u32::from_be_bytes(
                bytes[at..at + 4].try_into().expect("32-bit field"),
            ))
        }
    }

    #[test]
    fn stamped_file_reports_its_duration_when_read_back() {
        // Round-trip through the on-disk reader so the public entry points are
        // both covered against the same bytes.
        let mut bytes = build(
            vec![fragment(0, 0, Some(4096), vec![trun_default(100)])],
            None,
        );
        let stamped = stamp(&mut bytes).expect("duration");
        let path = std::env::temp_dir().join("fragmented_roundtrip_test.m4a");
        std::fs::write(&path, &bytes).expect("write fixture");
        let read_back = fragmented_duration_secs(&path);
        let _ = std::fs::remove_file(&path);
        assert!(read_back.is_some(), "stamped file lost its duration");
        assert!((read_back.expect("duration") - stamped).abs() < 0.0001);
    }

    #[test]
    fn progressive_file_is_left_alone() {
        let mut bytes = box_of(b"ftyp", b"iso5m4a");
        bytes.extend_from_slice(&zeroed_moov(44_100, None));
        bytes.extend_from_slice(&box_of(b"mdat", &[0u8; 32]));
        assert_eq!(
            stamp(&mut bytes),
            None,
            "a progressive file has no fragments"
        );
    }

    #[test]
    fn garbage_is_not_panicked_on() {
        let mut bytes = vec![0xAB; 512];
        assert_eq!(stamp(&mut bytes), None);
        assert_eq!(
            fragmented_duration_secs(Path::new("/nonexistent/x.m4a")),
            None
        );
        assert_eq!(stamp(&mut []), None);
    }

    #[test]
    fn truncated_file_stops_without_overrunning() {
        let full = build(
            vec![fragment(0, 0, Some(4096), vec![trun_default(100)])],
            None,
        );
        for cut in [1usize, 9, 40, 100, full.len() - 1] {
            let mut bytes = full[..cut.min(full.len())].to_vec();
            let _ = stamp(&mut bytes);
        }
    }
}
