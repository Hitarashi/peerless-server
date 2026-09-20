//! ISO-BMFF (MP4 / fMP4) parsing and FairPlay SAMPLE-AES decryption.
//!
//! Ported and adapted from `apple-music-downloader/internal/fairplay-rip/runv4.go`.
//!
//! Handles:
//! 1. Transforming the init segment (`ftyp` + `moov`):
//!    - Unprotecting `stsd` audio sample entry (`enca` -> `alac`).
//!    - Removing `sinf` box.
//!    - Stripping `sbgp`/`sgpd` grouping boxes (`seam`, `seig`).
//!    - Sanitizing `stsd` to single entry.
//! 2. Decrypting each media fragment (`moof` + `mdat`):
//!    - Reading subsample patterns from `senc`.
//!    - Reading sample sizes and data offset from `trun`.
//!    - Decrypting 16-byte blocks using `temari::rounds::decrypt`.
//!    - Removing `senc`, `saiz`, `saio`, `pssh` boxes.
//!    - Adjusting `trun.data_offset` and updating box lengths.

use super::client::WrapperError;

#[inline]
fn read_u16_be(buf: &[u8]) -> u16 {
    u16::from_be_bytes(buf[..2].try_into().unwrap())
}

#[inline]
fn read_u24_be(buf: &[u8]) -> u32 {
    ((buf[0] as u32) << 16) | ((buf[1] as u32) << 8) | (buf[2] as u32)
}

#[inline]
fn read_u32_be(buf: &[u8]) -> u32 {
    u32::from_be_bytes(buf[..4].try_into().unwrap())
}

#[inline]
fn write_u32_be(buf: &mut [u8], val: u32) {
    buf[..4].copy_from_slice(&val.to_be_bytes());
}

#[inline]
fn read_i32_be(buf: &[u8]) -> i32 {
    i32::from_be_bytes(buf[..4].try_into().unwrap())
}

#[inline]
fn write_i32_be(buf: &mut [u8], val: i32) {
    buf[..4].copy_from_slice(&val.to_be_bytes());
}

#[inline]
fn read_u64_be(buf: &[u8]) -> u64 {
    u64::from_be_bytes(buf[..8].try_into().unwrap())
}

#[derive(Debug, Clone)]
pub struct SubSample {
    pub clear_bytes: u16,
    pub protected_bytes: u32,
}

#[derive(Debug, Clone)]
pub struct SampleEncryptionInfo {
    pub subsamples: Vec<SubSample>,
}

/// Read a box header from `data` at `offset`.
/// Returns `(box_size, box_type, header_len)`.
pub fn read_box_header(data: &[u8], offset: usize) -> Option<(usize, [u8; 4], usize)> {
    if offset + 8 > data.len() {
        return None;
    }
    let size32 = read_u32_be(&data[offset..offset + 4]) as usize;
    let mut box_type = [0u8; 4];
    box_type.copy_from_slice(&data[offset + 4..offset + 8]);

    if size32 == 1 {
        // 64-bit large size
        if offset + 16 > data.len() {
            return None;
        }
        let size64 = read_u64_be(&data[offset + 8..offset + 16]) as usize;
        Some((size64, box_type, 16))
    } else if size32 == 0 {
        // Extends to EOF
        Some((data.len() - offset, box_type, 8))
    } else {
        Some((size32, box_type, 8))
    }
}

/// Helper to find a child box of given type inside a parent box.
pub fn find_child_box(
    data: &[u8],
    parent_start: usize,
    parent_end: usize,
    target_type: &[u8; 4],
) -> Option<(usize, usize)> {
    let mut cur = parent_start;
    while cur + 8 <= parent_end && cur + 8 <= data.len() {
        let (box_len, box_type, _) = read_box_header(data, cur)?;
        if box_len < 8 || cur + box_len > parent_end || cur + box_len > data.len() {
            break;
        }
        if &box_type == target_type {
            return Some((cur, box_len));
        }
        cur += box_len;
    }
    None
}

/// Find all child boxes of given type inside a parent box.
pub fn find_all_child_boxes(
    data: &[u8],
    parent_start: usize,
    parent_end: usize,
    target_type: &[u8; 4],
) -> Vec<(usize, usize)> {
    let mut res = Vec::new();
    let mut cur = parent_start;
    while cur + 8 <= parent_end && cur + 8 <= data.len() {
        let Some((box_len, box_type, _)) = read_box_header(data, cur) else {
            break;
        };
        if box_len < 8 || cur + box_len > parent_end || cur + box_len > data.len() {
            break;
        }
        if &box_type == target_type {
            res.push((cur, box_len));
        }
        cur += box_len;
    }
    res
}

/// Transforms the init segment (`ftyp` and `moov` boxes).
/// Changes sample entry `enca` to `alac`, strips `sinf`, strips `sbgp`/`sgpd` if seam/seig,
/// and updates box lengths.
pub fn transform_init_segment(init_data: &[u8]) -> Result<Vec<u8>, WrapperError> {
    let mut out = init_data.to_vec();
    let total_len = out.len();

    // Find moov box
    let (moov_off, moov_len) = find_child_box(&out, 0, total_len, b"moov")
        .ok_or_else(|| WrapperError::Message("Missing moov box in init segment".into()))?;

    // Inside moov -> trak -> mdia -> minf -> stbl
    let (trak_off, trak_len) = find_child_box(&out, moov_off + 8, moov_off + moov_len, b"trak")
        .ok_or_else(|| WrapperError::Message("Missing trak box in moov".into()))?;
    let (mdia_off, mdia_len) = find_child_box(&out, trak_off + 8, trak_off + trak_len, b"mdia")
        .ok_or_else(|| WrapperError::Message("Missing mdia box in trak".into()))?;
    let (minf_off, minf_len) = find_child_box(&out, mdia_off + 8, mdia_off + mdia_len, b"minf")
        .ok_or_else(|| WrapperError::Message("Missing minf box in mdia".into()))?;
    let (stbl_off, stbl_len) = find_child_box(&out, minf_off + 8, minf_off + minf_len, b"stbl")
        .ok_or_else(|| WrapperError::Message("Missing stbl box in minf".into()))?;

    // Inside stbl: find stsd
    let (stsd_off, stsd_len) = find_child_box(&out, stbl_off + 8, stbl_off + stbl_len, b"stsd")
        .ok_or_else(|| WrapperError::Message("Missing stsd box in stbl".into()))?;

    // stsd format:
    // 4 bytes size, 4 bytes 'stsd'
    // 1 byte version, 3 bytes flags
    // 4 bytes entry count
    if stsd_len < 16 {
        return Err(WrapperError::Message("stsd box too small".into()));
    }
    let stsd_entry_count = read_u32_be(&out[stsd_off + 12..stsd_off + 16]) as usize;
    let entry_start = stsd_off + 16;

    // Read first entry
    let (entry_len, entry_type, _) = read_box_header(&out, entry_start)
        .ok_or_else(|| WrapperError::Message("Cannot read first stsd entry".into()))?;

    let sinf_removed = if &entry_type == b"enca" {
        // AudioSampleEntry fixed fields before child boxes = 36 bytes:
        // 4 size, 4 'enca', 6 reserved, 2 data_reference_index,
        // 8 reserved, 2 channel_count, 2 sample_size, 4 predefined/reserved, 4 sample_rate
        let enca_children_start = entry_start + 36;
        let enca_end = entry_start + entry_len;

        let (sinf_off, sinf_len) = find_child_box(&out, enca_children_start, enca_end, b"sinf")
            .ok_or_else(|| WrapperError::Message("Missing sinf in enca entry".into()))?;

        // In sinf, find frma to get original codec (should be 'alac')
        let fmt = if let Some((frma_off, _)) =
            find_child_box(&out, sinf_off + 8, sinf_off + sinf_len, b"frma")
        {
            let mut f = [0u8; 4];
            f.copy_from_slice(&out[frma_off + 8..frma_off + 12]);
            f
        } else {
            *b"alac"
        };

        // 1. Rename entry type from 'enca' to original format (e.g. 'alac')
        out[entry_start + 4..entry_start + 8].copy_from_slice(&fmt);

        // 2. Remove sinf box from enca
        out.drain(sinf_off..sinf_off + sinf_len);

        // 3. Update entry size
        let new_entry_len = entry_len - sinf_len;
        write_u32_be(&mut out[entry_start..entry_start + 4], new_entry_len as u32);

        sinf_len
    } else {
        0
    };

    // Sanitize stsd to 1 entry (matches apple-music-downloader sanitizeInit and Symphonia requirement)
    let mut extra_entries_removed = 0;
    if stsd_entry_count > 1 {
        let first_entry_size = read_u32_be(&out[entry_start..entry_start + 4]) as usize;
        let second_entry_start = entry_start + first_entry_size;
        let cur_stsd_end = stsd_off + stsd_len - sinf_removed;
        if second_entry_start < cur_stsd_end {
            let excess = cur_stsd_end - second_entry_start;
            out.drain(second_entry_start..cur_stsd_end);
            extra_entries_removed = excess;
        }
        write_u32_be(&mut out[stsd_off + 12..stsd_off + 16], 1);
    }

    let total_stsd_removed = sinf_removed + extra_entries_removed;

    // Filter sbgp and sgpd in stbl if grouping type is 'seam' or 'seig'
    let cur_stbl_len = read_u32_be(&out[stbl_off..stbl_off + 4]) as usize - total_stsd_removed;
    let mut sbgp_removed = 0;
    let mut check_pos = stbl_off + 8;
    while check_pos + 8 <= stbl_off + cur_stbl_len - sbgp_removed {
        let Some((b_len, b_type, _)) = read_box_header(&out, check_pos) else {
            break;
        };
        if b_len < 8 || check_pos + b_len > out.len() {
            break;
        }
        if &b_type == b"sbgp" || &b_type == b"sgpd" {
            // Check grouping type at check_pos + 12 (FullBox version 1 + flags 3 + grouping_type 4)
            if check_pos + 16 <= out.len() {
                let grp = &out[check_pos + 12..check_pos + 16];
                if grp == b"seam" || grp == b"seig" {
                    out.drain(check_pos..check_pos + b_len);
                    sbgp_removed += b_len;
                    continue;
                }
            }
        }
        check_pos += b_len;
    }

    let total_removed = total_stsd_removed + sbgp_removed;

    // Update parent box sizes: stsd, stbl, minf, mdia, trak, moov
    if total_stsd_removed > 0 {
        let cur = read_u32_be(&out[stsd_off..stsd_off + 4]);
        write_u32_be(
            &mut out[stsd_off..stsd_off + 4],
            cur - total_stsd_removed as u32,
        );
    }
    if total_removed > 0 {
        let cur_stbl = read_u32_be(&out[stbl_off..stbl_off + 4]);
        write_u32_be(
            &mut out[stbl_off..stbl_off + 4],
            cur_stbl - total_removed as u32,
        );

        let cur_minf = read_u32_be(&out[minf_off..minf_off + 4]);
        write_u32_be(
            &mut out[minf_off..minf_off + 4],
            cur_minf - total_removed as u32,
        );

        let cur_mdia = read_u32_be(&out[mdia_off..mdia_off + 4]);
        write_u32_be(
            &mut out[mdia_off..mdia_off + 4],
            cur_mdia - total_removed as u32,
        );

        let cur_trak = read_u32_be(&out[trak_off..trak_off + 4]);
        write_u32_be(
            &mut out[trak_off..trak_off + 4],
            cur_trak - total_removed as u32,
        );

        let cur_moov = read_u32_be(&out[moov_off..moov_off + 4]);
        write_u32_be(
            &mut out[moov_off..moov_off + 4],
            cur_moov - total_removed as u32,
        );
    }

    Ok(out)
}

/// Parsed `trun` box information.
#[derive(Debug, Clone)]
pub(crate) struct TrunInfo {
    pub(crate) sample_count: usize,
    pub(crate) data_offset_field_pos: Option<usize>,
    pub(crate) data_offset: i32,
    pub(crate) sample_sizes: Vec<usize>,
}

/// `tfhd` default sample values used when `trun` omits per-sample fields.
#[derive(Debug, Clone, Default)]
pub(crate) struct TfhdDefaults {
    pub(crate) default_sample_duration: Option<u32>,
    pub(crate) default_sample_size: Option<u32>,
}

/// Parse the `tfhd` box inside `traf` for its default sample fields.
pub(crate) fn parse_tfhd_defaults(
    data: &[u8],
    traf_start: usize,
    traf_len: usize,
) -> Option<TfhdDefaults> {
    let (tfhd_off, tfhd_len) =
        find_child_box(data, traf_start + 8, traf_start + traf_len, b"tfhd")?;
    // FullBox: size/type (8) + version/flags (4), then fields.
    if tfhd_len < 16 || tfhd_off + tfhd_len > data.len() {
        return None;
    }
    let flags = read_u24_be(&data[tfhd_off + 9..tfhd_off + 12]);
    let mut cur = tfhd_off + 12;
    if flags & 0x1 != 0 {
        cur += 8; // base_data_offset
    }
    if flags & 0x2 != 0 {
        cur += 4; // sample_description_index
    }
    cur += 4; // track_id
    let mut defaults = TfhdDefaults::default();
    if flags & 0x8 != 0 {
        if cur + 4 > data.len() {
            return None;
        }
        defaults.default_sample_duration = Some(read_u32_be(&data[cur..cur + 4]));
        cur += 4;
    }
    if flags & 0x10 != 0 {
        if cur + 4 > data.len() {
            return None;
        }
        defaults.default_sample_size = Some(read_u32_be(&data[cur..cur + 4]));
    }
    Some(defaults)
}

/// Parse `trun` box inside `traf` (public for the CENC path).
///
/// Missing per-sample sizes (trun flags without 0x200) are resolved from
/// `tfhd.default_sample_size` when present, mirroring mp4ff's
/// `AddSampleDefaultValues` fallback chain.
pub(crate) fn parse_trun_pub(
    data: &[u8],
    traf_start: usize,
    traf_len: usize,
    trun_start: usize,
    trun_len: usize,
) -> Option<TrunInfo> {
    parse_trun(data, traf_start, traf_len, trun_start, trun_len)
}

/// Parse `trun` box inside `traf`.
fn parse_trun(
    data: &[u8],
    traf_start: usize,
    traf_len: usize,
    trun_start: usize,
    trun_len: usize,
) -> Option<TrunInfo> {
    if trun_len < 16 || trun_start + trun_len > data.len() {
        return None;
    }
    let flags = read_u24_be(&data[trun_start + 9..trun_start + 12]);
    let sample_count = read_u32_be(&data[trun_start + 12..trun_start + 16]) as usize;
    let mut cur = trun_start + 16;

    let data_offset_field_pos;
    let data_offset;
    if flags & 0x000001 != 0 {
        if cur + 4 > data.len() {
            return None;
        }
        data_offset_field_pos = Some(cur);
        data_offset = read_i32_be(&data[cur..cur + 4]);
        cur += 4;
    } else {
        data_offset_field_pos = None;
        data_offset = 0;
    }

    if flags & 0x000004 != 0 {
        cur += 4; // first_sample_flags
    }

    let has_duration = flags & 0x000100 != 0;
    let has_size = flags & 0x000200 != 0;
    let has_flags = flags & 0x000400 != 0;
    let has_ctts = flags & 0x000800 != 0;

    // mp4ff fallback: missing per-sample sizes come from tfhd defaults.
    let default_size = if has_size {
        None
    } else {
        parse_tfhd_defaults(data, traf_start, traf_len).and_then(|d| d.default_sample_size)
    };

    let mut sample_sizes = Vec::with_capacity(sample_count);
    for _ in 0..sample_count {
        if has_duration {
            cur += 4;
        }
        if has_size {
            if cur + 4 > data.len() {
                return None;
            }
            sample_sizes.push(read_u32_be(&data[cur..cur + 4]) as usize);
            cur += 4;
        } else {
            sample_sizes.push(default_size.unwrap_or(0) as usize);
        }
        if has_flags {
            cur += 4;
        }
        if has_ctts {
            cur += 4;
        }
    }

    Some(TrunInfo {
        sample_count,
        data_offset_field_pos,
        data_offset,
        sample_sizes,
    })
}

/// Parse `senc` box inside `traf`.
/// Returns list of subsamples for each sample.
fn parse_senc(
    data: &[u8],
    senc_start: usize,
    senc_len: usize,
) -> Option<Vec<SampleEncryptionInfo>> {
    if senc_len < 16 || senc_start + senc_len > data.len() {
        return None;
    }
    let flags = read_u24_be(&data[senc_start + 9..senc_start + 12]);
    let has_subsamples = flags & 0x000002 != 0;
    let sample_count = read_u32_be(&data[senc_start + 12..senc_start + 16]) as usize;

    let mut cur = senc_start + 16;
    let mut res = Vec::with_capacity(sample_count);

    for _ in 0..sample_count {
        if has_subsamples {
            if cur + 2 > data.len() {
                return None;
            }
            let sub_count = read_u16_be(&data[cur..cur + 2]) as usize;
            cur += 2;
            let mut subs = Vec::with_capacity(sub_count);
            for _ in 0..sub_count {
                if cur + 6 > data.len() {
                    return None;
                }
                let clear_bytes = read_u16_be(&data[cur..cur + 2]);
                let protected_bytes = read_u32_be(&data[cur + 2..cur + 6]);
                cur += 6;
                subs.push(SubSample {
                    clear_bytes,
                    protected_bytes,
                });
            }
            res.push(SampleEncryptionInfo { subsamples: subs });
        } else {
            res.push(SampleEncryptionInfo {
                subsamples: Vec::new(),
            });
        }
    }

    Some(res)
}

/// Decrypt one fragmented MP4 segment (`moof` + `mdat`) using Temari.
/// Modifies audio data in-place and removes encryption boxes (`senc`, `saiz`, `saio`, `pssh`).
pub fn decrypt_fragment(
    fragment_data: &[u8],
    template: &temari::rounds::Template,
) -> Result<Vec<u8>, WrapperError> {
    let mut out = fragment_data.to_vec();
    let total_len = out.len();

    let (moof_off, moof_len) = find_child_box(&out, 0, total_len, b"moof")
        .ok_or_else(|| WrapperError::Message("Missing moof box in fragment".into()))?;
    let (mdat_off, _) = find_child_box(&out, 0, total_len, b"mdat")
        .ok_or_else(|| WrapperError::Message("Missing mdat box in fragment".into()))?;

    let (traf_off, traf_len) = find_child_box(&out, moof_off + 8, moof_off + moof_len, b"traf")
        .ok_or_else(|| WrapperError::Message("Missing traf box in moof".into()))?;

    let trun_boxes = find_all_child_boxes(&out, traf_off + 8, traf_off + traf_len, b"trun");
    if trun_boxes.is_empty() {
        return Err(WrapperError::Message("No trun boxes in traf".into()));
    }
    let mut truns = Vec::with_capacity(trun_boxes.len());
    for (t_off, t_len) in trun_boxes {
        let trun = parse_trun(&out, traf_off, traf_len, t_off, t_len)
            .ok_or_else(|| WrapperError::Message("Failed to parse trun box".into()))?;
        truns.push(trun);
    }

    let senc_box = find_child_box(&out, traf_off + 8, traf_off + traf_len, b"senc");
    let enc_info = if let Some((s_off, s_len)) = senc_box {
        parse_senc(&out, s_off, s_len)
    } else {
        None
    };

    // 4b. Size fallback when trun and tfhd both omit per-sample sizes:
    // derive each sample's size from its senc subsample entries
    // (clear + protected bytes). Apple ec-3 packs one subsample per sample.
    if let Some(ref encs) = enc_info {
        for (trun_idx, trun) in truns.iter_mut().enumerate() {
            if trun.sample_count == 0 || !trun.sample_sizes.iter().all(|&s| s == 0) {
                continue;
            }
            if trun_idx > 0 {
                // Only resolve the leading size-less trun; multi-trun
                // size inference is ambiguous without full-sample data.
                continue;
            }
            let start_idx = 0;
            let end_idx = (start_idx + trun.sample_count).min(encs.len());
            let mut derived = Vec::with_capacity(trun.sample_count);
            for enc in &encs[start_idx..end_idx] {
                let size: usize = enc
                    .subsamples
                    .iter()
                    .map(|s| s.clear_bytes as usize + s.protected_bytes as usize)
                    .sum();
                derived.push(size);
            }
            if derived.iter().all(|&s| s > 0) && derived.len() == trun.sample_count {
                trun.sample_sizes = derived;
            }
        }
    }

    // 4c. Guard: without usable sizes there is nothing to decrypt.
    let any_size = truns
        .iter()
        .flat_map(|t| t.sample_sizes.iter())
        .any(|&s| s > 0);
    if !any_size && truns.iter().any(|t| t.sample_count > 0) {
        return Err(WrapperError::Message(
            "No sample sizes available in trun/tfhd/senc".into(),
        ));
    }

    let mut global_sample_idx = 0;
    let mut prev_sample_end = mdat_off + 8;

    for trun in &truns {
        let mut sample_offset = if trun.data_offset > 0 {
            moof_off + trun.data_offset as usize
        } else {
            prev_sample_end
        };

        for &sample_size in &trun.sample_sizes {
            if sample_offset + sample_size > out.len() {
                return Err(WrapperError::Message(format!(
                    "Sample {} offset {} exceeds fragment boundary {}",
                    global_sample_idx,
                    sample_offset + sample_size,
                    out.len()
                )));
            }

            let sample_slice = &mut out[sample_offset..sample_offset + sample_size];

            if let Some(ref encs) = enc_info {
                if let Some(enc) = encs.get(global_sample_idx) {
                    if !enc.subsamples.is_empty() {
                        let mut pos = 0;
                        for sub in &enc.subsamples {
                            pos += sub.clear_bytes as usize;
                            let prot = sub.protected_bytes as usize;
                            if pos + prot > sample_slice.len() {
                                break;
                            }
                            let head = (prot / 16) * 16;
                            if head > 0 {
                                let ct = &sample_slice[pos..pos + head];
                                let pt = temari::rounds::decrypt(template, ct);
                                sample_slice[pos..pos + head].copy_from_slice(&pt[..head]);
                            }
                            pos += prot;
                        }
                    } else {
                        let head = (sample_slice.len() / 16) * 16;
                        if head > 0 {
                            let pt = temari::rounds::decrypt(template, &sample_slice[..head]);
                            sample_slice[..head].copy_from_slice(&pt[..head]);
                        }
                    }
                }
            } else {
                let head = (sample_slice.len() / 16) * 16;
                if head > 0 {
                    let pt = temari::rounds::decrypt(template, &sample_slice[..head]);
                    sample_slice[..head].copy_from_slice(&pt[..head]);
                }
            }

            sample_offset += sample_size;
            global_sample_idx += 1;
        }
        prev_sample_end = sample_offset;
    }

    let mut boxes_to_remove: Vec<(usize, usize)> = Vec::new();
    let mut bytes_removed_from_traf = 0;

    for box_type in [b"senc", b"saiz", b"saio"] {
        for b in find_all_child_boxes(&out, traf_off + 8, traf_off + traf_len, box_type) {
            bytes_removed_from_traf += b.1;
            boxes_to_remove.push(b);
        }
    }
    // Drop sample-encryption group metadata (`seam`) left over in traf.
    {
        let mut cur = traf_off + 8;
        let traf_end = traf_off + traf_len;
        while cur + 8 <= traf_end && cur + 8 <= out.len() {
            let Some((b_len, b_type, _)) = read_box_header(&out, cur) else {
                break;
            };
            if b_len < 8 || cur + b_len > traf_end || cur + b_len > out.len() {
                break;
            }
            if (&b_type == b"sgpd" || &b_type == b"sbgp")
                && cur + 16 <= out.len()
                && &out[cur + 12..cur + 16] == b"seam"
            {
                bytes_removed_from_traf += b_len;
                boxes_to_remove.push((cur, b_len));
            }
            cur += b_len;
        }
    }
    for b in find_all_child_boxes(&out, traf_off + 8, traf_off + traf_len, b"uuid") {
        bytes_removed_from_traf += b.1;
        boxes_to_remove.push(b);
    }

    let mut bytes_removed_from_moof = bytes_removed_from_traf;
    for b in find_all_child_boxes(&out, moof_off + 8, moof_off + moof_len, b"pssh") {
        bytes_removed_from_moof += b.1;
        boxes_to_remove.push(b);
    }

    if bytes_removed_from_moof > 0 {
        for trun in &truns {
            if let Some(pos) = trun.data_offset_field_pos {
                let new_data_offset = trun.data_offset - bytes_removed_from_moof as i32;
                write_i32_be(&mut out[pos..pos + 4], new_data_offset);
            }
        }

        let cur_traf_size = read_u32_be(&out[traf_off..traf_off + 4]);
        write_u32_be(
            &mut out[traf_off..traf_off + 4],
            cur_traf_size - bytes_removed_from_traf as u32,
        );

        let cur_moof_size = read_u32_be(&out[moof_off..moof_off + 4]);
        write_u32_be(
            &mut out[moof_off..moof_off + 4],
            cur_moof_size - bytes_removed_from_moof as u32,
        );

        // Sort descending by start offset so removing earlier boxes doesn't invalidate subsequent offsets
        boxes_to_remove.sort_by_key(|a| std::cmp::Reverse(a.0));
        for (b_off, b_len) in boxes_to_remove {
            if b_off + b_len <= out.len() {
                out.drain(b_off..b_off + b_len);
            }
        }
    }

    normalize_fragment(&mut out);

    Ok(out)
}

/// Rebuild `trun` boxes that omit per-sample fields (sizes from tfhd
/// defaults) into standard truns with explicit per-sample duration and
/// size (flags 0x301). MP4Box/ffmpeg cannot resolve size-less truns,
/// so the decrypted fragment would demux as ~9 packets without this.
pub fn normalize_fragment(fragment: &mut Vec<u8>) {
    let total_len = fragment.len();
    let Some((moof_off, moof_len)) = find_child_box(fragment, 0, total_len, b"moof") else {
        return;
    };
    let Some((traf_off, traf_len)) =
        find_child_box(fragment, moof_off + 8, moof_off + moof_len, b"traf")
    else {
        return;
    };
    let Some(tfhd) = parse_tfhd_defaults(fragment, traf_off, traf_len) else {
        return;
    };

    // The sanitized init keeps a single stsd entry, but source fragments
    // may reference sample_description_index = 2 (Apple ships two entries:
    // main + Atmos variant). Demuxers drop every fragment whose index
    // exceeds the stsd entry count, so drop the tfhd flag + field.
    let mut sdi_shrink: i64 = 0;
    if let Some((tfhd_off, tfhd_len)) =
        find_child_box(fragment, traf_off + 8, traf_off + traf_len, b"tfhd")
        && tfhd_len >= 16
        && tfhd_off + tfhd_len <= fragment.len()
    {
        let flags = read_u24_be(&fragment[tfhd_off + 9..tfhd_off + 12]);
        if flags & 0x2 != 0 {
            let mut cur = tfhd_off + 12;
            if flags & 0x1 != 0 {
                cur += 8; // base_data_offset
            }
            cur += 4; // track_id precedes sample_description_index
            let sdi_pos = cur; // sample_description_index field
            if sdi_pos + 4 <= fragment.len() {
                fragment.drain(sdi_pos..sdi_pos + 4);
                let new_flags = flags & !0x2;
                fragment[tfhd_off + 9..tfhd_off + 12]
                    .copy_from_slice(&new_flags.to_be_bytes()[1..4]);
                write_u32_be(&mut fragment[tfhd_off..tfhd_off + 4], (tfhd_len - 4) as u32);
                let traf_size = read_u32_be(&fragment[traf_off..traf_off + 4]);
                write_u32_be(&mut fragment[traf_off..traf_off + 4], traf_size - 4);
                let moof_size = read_u32_be(&fragment[moof_off..moof_off + 4]);
                write_u32_be(&mut fragment[moof_off..moof_off + 4], moof_size - 4);
                sdi_shrink = -4;
            }
        }
    }

    // Collect rebuild candidates first, then splice back-to-front so
    // earlier offsets stay valid without unstable index shifting.
    let mut rebuilds: Vec<(usize, usize, i32, Vec<u8>)> = Vec::new();
    let mut cur = traf_off + 8;
    let traf_end = (traf_off as i64 + traf_len as i64 + sdi_shrink) as usize;
    while cur + 8 <= traf_end && cur + 8 <= fragment.len() {
        let Some((b_len, b_type, _)) = read_box_header(fragment, cur) else {
            break;
        };
        if b_len < 8 || cur + b_len > traf_end || cur + b_len > fragment.len() {
            break;
        }
        if &b_type == b"trun" {
            let Some(trun) = parse_trun_pub(fragment, traf_off, traf_len, cur, b_len) else {
                break;
            };
            let flags = read_u24_be(&fragment[cur + 9..cur + 12]);
            let needs_size = flags & 0x000200 == 0;
            // Only rebuild size-less truns: remuxers fail on missing sizes,
            // but duration-less truns with explicit sizes (ALAC VBR) are
            // already resolvable via tfhd defaults by MP4Box/ffmpeg.
            if needs_size && !trun.sample_sizes.iter().all(|&s| s == 0) {
                // Rebuild with explicit duration + size per sample.
                let count = trun.sample_count;
                let mut new_trun = Vec::with_capacity(8 + 4 + 4 + 4 + count * 8);
                let body_len = 4 + 4 + 4 + count * 8;
                new_trun.extend_from_slice(&((8 + body_len) as u32).to_be_bytes());
                new_trun.extend_from_slice(b"trun");
                new_trun.extend_from_slice(&[0, 0, 0x03, 0x01]); // data_offset + duration + size
                new_trun.extend_from_slice(&(count as u32).to_be_bytes());
                // data_offset patched after total growth is known.
                new_trun.extend_from_slice(&trun.data_offset.to_be_bytes());
                let duration = tfhd.default_sample_duration.unwrap_or(0);
                for &size in &trun.sample_sizes {
                    new_trun.extend_from_slice(&duration.to_be_bytes());
                    new_trun.extend_from_slice(&(size as u32).to_be_bytes());
                }
                rebuilds.push((cur, b_len, trun.data_offset, new_trun));
            }
        }
        cur += b_len;
    }

    // Rebuilt truns sit before mdat, so mdat (and every trun's data
    // target inside it) shifts by the total delta (including the tfhd
    // shrink). Patch each trun's data_offset when the fragment shrank
    // but no trun is being rebuilt.
    if rebuilds.is_empty() {
        if sdi_shrink != 0 {
            // No rebuild candidates: still patch data offsets of ALL
            // trun boxes for the tfhd shrink, since mdat moved.
            let mut cur = traf_off + 8;
            let traf_end = (traf_off as i64 + traf_len as i64 + sdi_shrink) as usize;
            while cur + 8 <= traf_end && cur + 8 <= fragment.len() {
                let Some((b_len, b_type, _)) = read_box_header(fragment, cur) else {
                    break;
                };
                if b_len < 8 || cur + b_len > traf_end || cur + b_len > fragment.len() {
                    break;
                }
                if &b_type == b"trun" {
                    let flags = read_u24_be(&fragment[cur + 9..cur + 12]);
                    if flags & 0x1 != 0 && cur + 20 <= fragment.len() {
                        let doff_pos = cur + 16;
                        let doff = i32::from_be_bytes(
                            fragment[doff_pos..doff_pos + 4].try_into().unwrap(),
                        );
                        let new_doff = doff + sdi_shrink as i32;
                        fragment[doff_pos..doff_pos + 4].copy_from_slice(&new_doff.to_be_bytes());
                    }
                }
                cur += b_len;
            }
        }
        return;
    }

    // Rebuilt truns sit before mdat, so mdat (and every trun's data
    // target inside it) shifts by the total growth (including the tfhd
    // shrink). Patch each rebuilt trun's data_offset, then splice.
    let total_trun_delta: i64 = rebuilds
        .iter()
        .map(|(_, old_len, _, new_trun)| new_trun.len() as i64 - *old_len as i64)
        .sum::<i64>();
    let total_delta: i64 = sdi_shrink + total_trun_delta;

    // Sibling truns keep their original boxes, but their samples live in
    // the same mdat, which moved by the total growth. A fragment mixing a
    // leading size-less trun with size-full siblings (96 kHz ALAC ships
    // this layout) would otherwise read every sibling sample
    // `total_delta` bytes early. Patch siblings before splicing, while
    // their box offsets are still the collection-time ones.
    {
        let mut cur = traf_off + 8;
        while cur + 8 <= traf_end && cur + 8 <= fragment.len() {
            let Some((b_len, b_type, _)) = read_box_header(fragment, cur) else {
                break;
            };
            if b_len < 8 || cur + b_len > traf_end || cur + b_len > fragment.len() {
                break;
            }
            if &b_type == b"trun" && !rebuilds.iter().any(|(off, _, _, _)| *off == cur) {
                let flags = read_u24_be(&fragment[cur + 9..cur + 12]);
                if flags & 0x1 != 0 && cur + 20 <= fragment.len() {
                    let doff_pos = cur + 16;
                    let doff =
                        i32::from_be_bytes(fragment[doff_pos..doff_pos + 4].try_into().unwrap());
                    let new_doff = doff + total_delta as i32;
                    fragment[doff_pos..doff_pos + 4].copy_from_slice(&new_doff.to_be_bytes());
                }
            }
            cur += b_len;
        }
    }

    // Splice candidates strictly back-to-front so earlier offsets stay invariant.
    for (off, old_len, old_data_offset, mut new_trun) in rebuilds.into_iter().rev() {
        if off + old_len > fragment.len() {
            continue;
        }
        // data_offset field: size(4)+type(4)+verflags(4)+count(4) = offset 16.
        if new_trun.len() >= 20 {
            new_trun[16..20]
                .copy_from_slice(&((old_data_offset as i64 + total_delta) as i32).to_be_bytes());
        }
        fragment.splice(off..off + old_len, new_trun);
    }

    // Grow parent sizes by the accumulated delta (moof, traf). The traf
    // size already absorbed sdi_shrink above; add only the trun delta.
    if traf_off + 4 <= fragment.len() {
        let traf_size = read_u32_be(&fragment[traf_off..traf_off + 4]);
        write_u32_be(
            &mut fragment[traf_off..traf_off + 4],
            (traf_size as i64 + total_trun_delta) as u32,
        );
    }
    if moof_off + 4 <= fragment.len() {
        let moof_size = read_u32_be(&fragment[moof_off..moof_off + 4]);
        write_u32_be(
            &mut fragment[moof_off..moof_off + 4],
            (moof_size as i64 + total_trun_delta) as u32,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tfhd(flags: u32, body: &[u8]) -> Vec<u8> {
        let mut b = Vec::new();
        b.extend_from_slice(&(8 + 4 + body.len() as u32).to_be_bytes());
        b.extend_from_slice(b"tfhd");
        b.extend_from_slice(&flags.to_be_bytes());
        b.extend_from_slice(body);
        b
    }

    fn trun(flags: u32, count: u32, data_offset: i32, entries: &[u32]) -> Vec<u8> {
        let entry_len = if flags & 0x100 != 0 { 4 } else { 0 }
            + if flags & 0x200 != 0 { 4 } else { 0 }
            + if flags & 0x400 != 0 { 4 } else { 0 }
            + if flags & 0x800 != 0 { 4 } else { 0 };
        let mut b = Vec::new();
        b.extend_from_slice(
            &(8 + 4
                + 4
                + (if flags & 0x1 != 0 { 4 } else { 0 })
                + (if flags & 0x4 != 0 { 4 } else { 0 })
                + (count as usize * entry_len) as u32)
                .to_be_bytes(),
        );
        b.extend_from_slice(b"trun");
        b.extend_from_slice(&flags.to_be_bytes());
        b.extend_from_slice(&count.to_be_bytes());
        if flags & 0x1 != 0 {
            b.extend_from_slice(&data_offset.to_be_bytes());
        }
        if flags & 0x4 != 0 {
            b.extend_from_slice(&0u32.to_be_bytes());
        }
        for e in entries {
            b.extend_from_slice(&e.to_be_bytes());
        }
        b
    }

    fn wrap_traf(children: &[Vec<u8>]) -> (Vec<u8>, usize) {
        let total: usize = children.iter().map(|c| c.len()).sum();
        let mut b = Vec::new();
        b.extend_from_slice(&((8 + total) as u32).to_be_bytes());
        b.extend_from_slice(b"traf");
        let traf_off = 0usize;
        for c in children {
            b.extend_from_slice(c);
        }
        (b, traf_off)
    }

    /// tfhd with default duration + size (Apple ec-3 layout: flags 0x20018).
    #[test]
    fn parse_trun_resolves_missing_sizes_from_tfhd() {
        // tfhd body must include default duration + size
        let mut tfhd_body = Vec::new();
        tfhd_body.extend_from_slice(&1u32.to_be_bytes()); // track_id
        tfhd_body.extend_from_slice(&1536u32.to_be_bytes()); // default duration
        tfhd_body.extend_from_slice(&3072u32.to_be_bytes()); // default size
        let tfhd_box = tfhd(0x020018, &tfhd_body);
        let trun = trun(0x1, 468, 3977, &[]);
        let (traf, traf_off) = wrap_traf(&[tfhd_box, trun.clone()]);
        let trun_start = traf.len() - trun.len();
        let info =
            parse_trun_pub(&traf, traf_off, traf.len(), trun_start, trun.len()).expect("parse");
        assert_eq!(info.sample_count, 468);
        assert_eq!(info.sample_sizes.len(), 468);
        assert!(info.sample_sizes.iter().all(|&s| s == 3072));
        assert_eq!(info.data_offset, 3977);
    }

    /// trun with explicit per-sample sizes (ALAC flags 0x201) ignores tfhd.
    #[test]
    fn parse_trun_prefers_explicit_sizes() {
        let mut tfhd_body = Vec::new();
        tfhd_body.extend_from_slice(&1u32.to_be_bytes());
        tfhd_body.extend_from_slice(&4096u32.to_be_bytes());
        tfhd_body.extend_from_slice(&12349u32.to_be_bytes());
        let tfhd_box = tfhd(0x020018, &tfhd_body);
        let trun = trun(0x201, 3, 100, &[11, 22, 33]);
        let (traf, traf_off) = wrap_traf(&[tfhd_box, trun.clone()]);
        let trun_start = traf.len() - trun.len();
        let info =
            parse_trun_pub(&traf, traf_off, traf.len(), trun_start, trun.len()).expect("parse");
        assert_eq!(info.sample_sizes, vec![11, 22, 33]);
    }

    /// normalize_fragment rebuilds a size-less trun with per-sample
    /// duration + size and patches data_offset for the growth.
    #[test]
    fn normalize_fragment_rebuilds_sizeless_trun() {
        // moof { mfhd, traf { tfhd(0x20018: dur 1536, size 3072), trun(0x1, 2, doff) } } + mdat
        let mut tfhd_body = Vec::new();
        tfhd_body.extend_from_slice(&1u32.to_be_bytes());
        tfhd_body.extend_from_slice(&1536u32.to_be_bytes());
        tfhd_body.extend_from_slice(&3072u32.to_be_bytes());
        let tfhd_box = tfhd(0x020018, &tfhd_body);
        let trun = trun(0x1, 2, 44, &[]);
        let (traf, _) = wrap_traf(&[tfhd_box, trun.clone()]);
        let mut mfhd = Vec::new();
        mfhd.extend_from_slice(&16u32.to_be_bytes());
        mfhd.extend_from_slice(b"mfhd");
        mfhd.extend_from_slice(&0u32.to_be_bytes());
        mfhd.extend_from_slice(&1u32.to_be_bytes());
        let moof_children_len = mfhd.len() + traf.len();
        let mut moof = Vec::new();
        moof.extend_from_slice(&((8 + moof_children_len) as u32).to_be_bytes());
        moof.extend_from_slice(b"moof");
        moof.extend_from_slice(&mfhd);
        moof.extend_from_slice(&traf);
        let payload = vec![0xABu8; 2 * 3072];
        let mut mdat = Vec::new();
        mdat.extend_from_slice(&((8 + payload.len()) as u32).to_be_bytes());
        mdat.extend_from_slice(b"mdat");
        mdat.extend_from_slice(&payload);
        let mut frag = moof;
        frag.extend_from_slice(&mdat);

        normalize_fragment(&mut frag);

        // Locate the rebuilt trun by its box type; box start = type - 4.
        let trun_type = frag.windows(4).position(|w| w == b"trun").expect("trun");
        let trun_off = trun_type - 4; // box start
        let size = read_u32_be(&frag[trun_off..trun_off + 4]) as usize;
        let flags = read_u24_be(&frag[trun_off + 9..trun_off + 12]);
        assert_eq!(flags, 0x301);
        let count = read_u32_be(&frag[trun_off + 12..trun_off + 16]);
        assert_eq!(count, 2);
        assert_eq!(size, 8 + 4 + 4 + 4 + 2 * 8);
        // doff patched by the trun growth: 44 + 16 = 60. The fixture's
        // original 44 was arbitrary, so assert the delta, not the landing.
        let doff = read_i32_be(&frag[trun_off + 16..trun_off + 20]);
        assert_eq!(doff, 60);
        // moof grew by the same 16 bytes; mdat sits right after it.
        let moof_size = read_u32_be(&frag[0..4]) as usize;
        assert_eq!(frag[moof_size + 4..moof_size + 8], *b"mdat");
        // sample 0: duration then size
        let d0 = read_u32_be(&frag[trun_off + 20..trun_off + 24]);
        let s0 = read_u32_be(&frag[trun_off + 24..trun_off + 28]);
        assert_eq!(d0, 1536);
        assert_eq!(s0, 3072);
    }

    /// normalize_fragment drops tfhd sample_description_index so the
    /// sanitized single-entry stsd stays valid.
    #[test]
    fn normalize_fragment_drops_tfhd_sdi() {
        let mut tfhd_body = Vec::new();
        tfhd_body.extend_from_slice(&1u32.to_be_bytes()); // track_id
        tfhd_body.extend_from_slice(&2u32.to_be_bytes()); // sample_description_index = 2
        tfhd_body.extend_from_slice(&1536u32.to_be_bytes()); // duration
        tfhd_body.extend_from_slice(&3072u32.to_be_bytes()); // size
        let tfhd_box = tfhd(0x02001a, &tfhd_body);
        let trun = trun(0x201, 1, 60, &[3072]);
        let (traf, _) = wrap_traf(&[tfhd_box, trun.clone()]);
        let mut mfhd = Vec::new();
        mfhd.extend_from_slice(&16u32.to_be_bytes());
        mfhd.extend_from_slice(b"mfhd");
        mfhd.extend_from_slice(&0u32.to_be_bytes());
        mfhd.extend_from_slice(&1u32.to_be_bytes());
        let mut moof = Vec::new();
        moof.extend_from_slice(&((8 + mfhd.len() + traf.len()) as u32).to_be_bytes());
        moof.extend_from_slice(b"moof");
        moof.extend_from_slice(&mfhd);
        moof.extend_from_slice(&traf);
        let mut mdat = Vec::new();
        mdat.extend_from_slice(&(8u32 + 3072).to_be_bytes());
        mdat.extend_from_slice(b"mdat");
        mdat.extend_from_slice(&vec![0u8; 3072]);
        let mut frag = moof;
        frag.extend_from_slice(&mdat);
        let len_before = frag.len();

        normalize_fragment(&mut frag);

        // tfhd shrank by 4; trun untouched (has sizes); data_offset patched -4.
        assert_eq!(frag.len(), len_before - 4);
        let tfhd_type = frag.windows(4).position(|w| w == b"tfhd").expect("tfhd");
        let tfhd_off = tfhd_type - 4;
        let tfhd_size = read_u32_be(&frag[tfhd_off..tfhd_off + 4]);
        assert_eq!(tfhd_size, 24);
        let flags = read_u24_be(&frag[tfhd_off + 9..tfhd_off + 12]);
        assert_eq!(flags, 0x20018, "sdi flag cleared");
        // track_id preserved.
        let track_id = read_u32_be(&frag[tfhd_off + 12..tfhd_off + 16]);
        assert_eq!(track_id, 1);
        let trun_type = frag.windows(4).position(|w| w == b"trun").expect("trun");
        let trun_off = trun_type - 4;
        let doff = read_i32_be(&frag[trun_off + 16..trun_off + 20]);
        // The tfhd drain shifts mdat 4 bytes earlier; the patch reflects
        // that: original 60 - 4 = 56.
        assert_eq!(doff, 56);
        // And the shifted target is consistent: mdat payload sits at
        // (moof size after shrink) + 8 within the fragment.
        let moof_size = read_u32_be(&frag[0..4]) as usize;
        assert_eq!(frag[moof_size + 4..moof_size + 8], *b"mdat");
    }

    /// A leading size-less trun followed by size-full sibling truns (the
    /// 96 kHz ALAC segment layout): rebuilding the leading trun grows
    /// moof and shifts mdat, so every sibling's `data_offset` must shift
    /// by the same total delta. Unpatched siblings read their samples
    /// from the wrong mdat position and decode as garbage.
    #[test]
    fn normalize_fragment_patches_sibling_trun_offsets() {
        let mut tfhd_body = Vec::new();
        tfhd_body.extend_from_slice(&1u32.to_be_bytes()); // track_id
        tfhd_body.extend_from_slice(&4096u32.to_be_bytes()); // duration
        tfhd_body.extend_from_slice(&8224u32.to_be_bytes()); // size
        let tfhd_box = tfhd(0x020018, &tfhd_body);
        // Layout: moof { mfhd, traf { tfhd, trun(0x1, 2), trun(0x201, 1) } },
        // mdat payload of 3 * 8224 bytes starting at moof-relative 108.
        let sizeless = trun(0x1, 2, 108, &[]);
        let sibling = trun(0x201, 1, 108 + 2 * 8224, &[8224]);
        let (traf, _) = wrap_traf(&[tfhd_box, sizeless.clone(), sibling.clone()]);
        let mut mfhd = Vec::new();
        mfhd.extend_from_slice(&16u32.to_be_bytes());
        mfhd.extend_from_slice(b"mfhd");
        mfhd.extend_from_slice(&0u32.to_be_bytes());
        mfhd.extend_from_slice(&1u32.to_be_bytes());
        let mut moof = Vec::new();
        moof.extend_from_slice(&((8 + mfhd.len() + traf.len()) as u32).to_be_bytes());
        moof.extend_from_slice(b"moof");
        moof.extend_from_slice(&mfhd);
        moof.extend_from_slice(&traf);
        let payload = vec![0xCDu8; 3 * 8224];
        let mut mdat = Vec::new();
        mdat.extend_from_slice(&((8 + payload.len()) as u32).to_be_bytes());
        mdat.extend_from_slice(b"mdat");
        mdat.extend_from_slice(&payload);
        let mut frag = moof;
        frag.extend_from_slice(&mdat);

        normalize_fragment(&mut frag);

        let (moof_off, moof_size) = find_child_box(&frag, 0, frag.len(), b"moof").expect("moof");
        let (traf_off, traf_len) =
            find_child_box(&frag, moof_off + 8, moof_off + moof_size, b"traf").expect("traf");
        let trun_boxes = find_all_child_boxes(&frag, traf_off + 8, traf_off + traf_len, b"trun");
        assert_eq!(trun_boxes.len(), 2, "both truns survive the rebuild");
        let (rebuilt_off, rebuilt_len) = trun_boxes[0];
        let (sibling_off, _) = trun_boxes[1];

        // Rebuilt leading trun: flags 0x301, 2 samples, doff shifted by
        // the rebuild growth (36 - 20 = 16 bytes).
        assert_eq!(read_u24_be(&frag[rebuilt_off + 9..rebuilt_off + 12]), 0x301);
        assert_eq!(read_u32_be(&frag[rebuilt_off + 12..rebuilt_off + 16]), 2);
        assert_eq!(
            rebuilt_len, 36,
            "leading trun rebuilt with per-sample sizes"
        );
        let rebuilt_doff = read_i32_be(&frag[rebuilt_off + 16..rebuilt_off + 20]);
        assert_eq!(rebuilt_doff, 108 + 16);

        // Sibling keeps its shape, but its data_offset must follow mdat.
        assert_eq!(read_u24_be(&frag[sibling_off + 9..sibling_off + 12]), 0x201);
        let sibling_doff = read_i32_be(&frag[sibling_off + 16..sibling_off + 20]);
        assert_eq!(sibling_doff, 108 + 2 * 8224 + 16, "sibling tracks mdat");

        // Both offsets land on sample boundaries inside the shifted mdat.
        let mdat_payload = moof_off + moof_size + 8;
        assert_eq!(rebuilt_doff as usize, mdat_payload);
        assert_eq!(sibling_doff as usize, mdat_payload + 2 * 8224);
    }

    /// A fragment with multiple sizeless truns (common in Atmos / multi-channel streams).
    /// Tests back-to-front splicing and ensures no slice index out of range panic.
    #[test]
    fn normalize_fragment_rebuilds_multiple_sizeless_truns() {
        let mut tfhd_body = Vec::new();
        tfhd_body.extend_from_slice(&1u32.to_be_bytes()); // track_id
        tfhd_body.extend_from_slice(&1536u32.to_be_bytes()); // duration
        tfhd_body.extend_from_slice(&3072u32.to_be_bytes()); // size
        let tfhd_box = tfhd(0x020018, &tfhd_body);
        let sizeless1 = trun(0x1, 2, 100, &[]);
        let sizeless2 = trun(0x1, 3, 100 + 2 * 3072, &[]);
        let (traf, _) = wrap_traf(&[tfhd_box, sizeless1, sizeless2]);
        let mut mfhd = Vec::new();
        mfhd.extend_from_slice(&16u32.to_be_bytes());
        mfhd.extend_from_slice(b"mfhd");
        mfhd.extend_from_slice(&0u32.to_be_bytes());
        mfhd.extend_from_slice(&1u32.to_be_bytes());
        let mut moof = Vec::new();
        moof.extend_from_slice(&((8 + mfhd.len() + traf.len()) as u32).to_be_bytes());
        moof.extend_from_slice(b"moof");
        moof.extend_from_slice(&mfhd);
        moof.extend_from_slice(&traf);
        let payload = vec![0xEEu8; 5 * 3072];
        let mut mdat = Vec::new();
        mdat.extend_from_slice(&((8 + payload.len()) as u32).to_be_bytes());
        mdat.extend_from_slice(b"mdat");
        mdat.extend_from_slice(&payload);
        let mut frag = moof;
        frag.extend_from_slice(&mdat);

        normalize_fragment(&mut frag);

        let (moof_off, moof_size) = find_child_box(&frag, 0, frag.len(), b"moof").expect("moof");
        let (traf_off, traf_len) =
            find_child_box(&frag, moof_off + 8, moof_off + moof_size, b"traf").expect("traf");
        let trun_boxes = find_all_child_boxes(&frag, traf_off + 8, traf_off + traf_len, b"trun");
        assert_eq!(trun_boxes.len(), 2, "both truns rebuilt");

        // trun 1 grew by (8+4+4+4+2*8) - 20 = 36 - 20 = 16 bytes
        // trun 2 grew by (8+4+4+4+3*8) - 20 = 44 - 20 = 24 bytes
        // total delta = 40 bytes
        let (trun1_off, trun1_len) = trun_boxes[0];
        let (trun2_off, trun2_len) = trun_boxes[1];

        assert_eq!(trun1_len, 36);
        assert_eq!(trun2_len, 44);
        assert_eq!(read_u24_be(&frag[trun1_off + 9..trun1_off + 12]), 0x301);
        assert_eq!(read_u24_be(&frag[trun2_off + 9..trun2_off + 12]), 0x301);

        let doff1 = read_i32_be(&frag[trun1_off + 16..trun1_off + 20]);
        let doff2 = read_i32_be(&frag[trun2_off + 16..trun2_off + 20]);
        assert_eq!(doff1, 100 + 40);
        assert_eq!(doff2, 100 + 2 * 3072 + 40);
    }

    /// Truncated or malformed inputs return safely without panicking.
    #[test]
    fn normalize_fragment_truncated_input_does_not_panic() {
        let mut frag = vec![0u8; 32];
        normalize_fragment(&mut frag);

        // Valid moof header but truncated payload
        let mut frag2 = Vec::new();
        frag2.extend_from_slice(&64u32.to_be_bytes());
        frag2.extend_from_slice(b"moof");
        frag2.extend_from_slice(&[0u8; 16]);
        normalize_fragment(&mut frag2);
    }
}
