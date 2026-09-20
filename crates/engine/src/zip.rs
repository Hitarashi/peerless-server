//! Album ZIP planning and creation.
//!
//! This module owns archive details. Callers provide source files and receive
//! a completed, atomically published archive; partial files are never treated
//! as successful output.

use std::{
    fs::{self, File, OpenOptions},
    io::{BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
};

use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use zip::{CompressionMethod, ZipWriter, write::SimpleFileOptions};

pub use crate::filename::MAX_ZIP_ENTRY_FILENAME_BYTES;
use crate::filename::{ArchiveFilename, StandardFilename, ZipEntryName};

/// Conservative ceiling for the current bot-only uploader.
pub const TELEGRAM_SPLIT_THRESHOLD_BYTES: u64 = 1_900_000_000;
const ENTRY_OVERHEAD_BYTES: u64 = 512;
const ARCHIVE_HEADROOM_BYTES: u64 = 64 * 1024;
/// Keep generated ZIP entry names below the existing validation ceiling.

#[derive(Debug, Clone)]
pub struct ZipTrackEntry {
    pub file_path: PathBuf,
    pub archive_filename: ZipEntryName,
    pub file_size: u64,
}

#[derive(Debug, Clone)]
pub struct ZipPartPlan {
    pub part_index: usize,
    pub total_parts: usize,
    pub tracks: Vec<ZipTrackEntry>,
    pub cover_path: Option<PathBuf>,
    pub archive_filename: ArchiveFilename,
}

#[derive(Debug, Error)]
pub enum ZipError {
    #[error("ZIP contains no tracks")]
    Empty,
    #[error("ZIP size limit must be greater than zero")]
    InvalidLimit,
    #[error("archive entry name is invalid: {0}")]
    InvalidEntryName(String),
    #[error("archive entry name is duplicated: {0}")]
    DuplicateEntry(String),
    #[error("track {name} is larger than the ZIP part limit")]
    TrackTooLarge { name: String },
    #[error("source file {path:?} is missing or unreadable: {source}")]
    Source {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("ZIP build cancelled")]
    Cancelled,
    #[error("ZIP I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("ZIP writer failed: {0}")]
    Writer(String),
}

/// Sanitizes a display name for use as an archive filename.
pub fn sanitize_archive_filename(name: &str) -> String {
    let sanitized: String = name
        .chars()
        .map(|c| match c {
            '<' | '>' | ':' | '"' | '/' | '\\' | '|' | '?' | '*' => '_',
            _ => c,
        })
        .collect();
    let trimmed = sanitized.trim().trim_end_matches('.').to_owned();
    if trimmed.is_empty() {
        "album".to_owned()
    } else {
        trimmed
    }
}

/// Builds a bounded ZIP entry filename, safely formatted with track number, track ID, and codec extension.
pub fn build_zip_entry_filename_with_codec(
    track_number: Option<i64>,
    title: &str,
    artist: &str,
    track_id: &str,
    codec: &str,
) -> ZipEntryName {
    let ext = match codec {
        "flac" => "flac",
        "mp3" => "mp3",
        _ => "m4a",
    };
    let suffix = format!(" [{track_id}].{ext}");
    let name = match track_number {
        Some(num) => format!("{num:02} - {title} - {artist}{suffix}"),
        None => format!("{title} - {artist}{suffix}"),
    };
    ZipEntryName::sanitize_and_bound(&name, Some(&suffix))
}

/// Builds a bounded ZIP entry filename, safely formatted with track number and track ID.
pub fn build_zip_entry_filename(
    track_number: Option<i64>,
    title: &str,
    artist: &str,
    track_id: &str,
) -> ZipEntryName {
    build_zip_entry_filename_with_codec(track_number, title, artist, track_id, "alac")
}

/// Builds the base archive filename without the `.zip` extension.
pub fn build_album_archive_base_name(
    artist: &str,
    album: &str,
    release_date: &str,
) -> ArchiveFilename {
    build_album_archive_base_name_with_codec(artist, album, release_date, "alac")
}

/// Same as [`build_album_archive_base_name`], labeled with the highest
/// codec delivered in the archive (`alac`, `aac`, `mp4a.40.2`, `ec-3`, `flac`).
pub fn build_album_archive_base_name_with_codec(
    artist: &str,
    album: &str,
    release_date: &str,
    codec: &str,
) -> ArchiveFilename {
    let year = release_date.chars().take(4).collect::<String>();
    let year_part = if year.len() == 4 && year.chars().all(|c| c.is_ascii_digit()) {
        format!(" ({year})")
    } else {
        String::new()
    };
    let label = archive_codec_label(codec);
    let suffix = format!(" [{label}]");
    let name = format!("{artist} - {album}{year_part}{suffix}");
    ArchiveFilename::sanitize_and_bound(&name, Some(&suffix))
}

fn archive_codec_label(codec: &str) -> &'static str {
    match codec {
        "ec-3" => "Atmos",
        "aac" | "mp4a.40.2" | "mp4a.40.5" => "AAC",
        "flac" => "FLAC",
        _ => "ALAC",
    }
}

/// Deterministic identity of the resolved track set a ZIP was built from.
/// Inputs are NUL-delimited; none of them may contain NUL, so the framing is
/// unambiguous.
pub fn album_generation_hash(provider: &str, album_id: &str, ordered_track_ids: &[&str]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"alac-zip-gen-v1");
    hasher.update([0]);
    hasher.update(provider);
    hasher.update([0]);
    hasher.update(album_id);
    for id in ordered_track_ids {
        hasher.update([0]);
        hasher.update(id);
    }
    hex_digest(hasher)
}

fn hex_digest(hasher: Sha256) -> String {
    let digest = hasher.finalize();
    let mut hex = String::with_capacity(64);
    for byte in digest {
        hex.push_str(&format!("{byte:02x}"));
    }
    hex
}

/// Streams a file's contents through SHA-256, returning the lowercase hex
/// digest. Sync I/O is deliberate: callers run this inside `spawn_blocking`.
fn hash_file(path: &Path) -> Result<String, ZipError> {
    let mut source = BufReader::with_capacity(
        64 * 1024,
        File::open(path).map_err(|source| ZipError::Source {
            path: path.to_owned(),
            source,
        })?,
    );
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let count = source
            .read(&mut buffer)
            .map_err(|source| ZipError::Source {
                path: path.to_owned(),
                source,
            })?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
    }
    Ok(hex_digest(hasher))
}

fn validate_entry_name(name: &str) -> Result<(), ZipError> {
    if name.is_empty()
        || name.len() > MAX_ZIP_ENTRY_FILENAME_BYTES
        || name.bytes().any(|b| b == 0 || b.is_ascii_control())
        || Path::new(name).is_absolute()
        || Path::new(name).components().any(|c| {
            matches!(
                c,
                std::path::Component::ParentDir | std::path::Component::RootDir
            )
        })
    {
        return Err(ZipError::InvalidEntryName(name.to_owned()));
    }
    Ok(())
}

/// Plans sequential ZIP partitions with checked size accounting.
pub fn plan_zip_parts(
    artist: &str,
    album: &str,
    release_date: &str,
    tracks: &[ZipTrackEntry],
    cover_path: Option<PathBuf>,
    max_part_bytes: u64,
) -> Result<Vec<ZipPartPlan>, ZipError> {
    plan_zip_parts_with_codec(
        artist,
        album,
        release_date,
        tracks,
        cover_path,
        max_part_bytes,
        "alac",
    )
}

/// Codec-aware [`plan_zip_parts`]; the label lands in the archive name.
pub fn plan_zip_parts_with_codec(
    artist: &str,
    album: &str,
    release_date: &str,
    tracks: &[ZipTrackEntry],
    cover_path: Option<PathBuf>,
    max_part_bytes: u64,
    codec: &str,
) -> Result<Vec<ZipPartPlan>, ZipError> {
    if tracks.is_empty() {
        return Err(ZipError::Empty);
    }
    if max_part_bytes == 0 {
        return Err(ZipError::InvalidLimit);
    }

    let mut names = std::collections::HashSet::new();
    names.insert("manifest.json".to_owned());
    for track in tracks {
        validate_entry_name(&track.archive_filename)?;
        if !names.insert(track.archive_filename.to_string()) {
            return Err(ZipError::DuplicateEntry(track.archive_filename.to_string()));
        }
        let source_size = fs::metadata(&track.file_path)
            .map_err(|source| ZipError::Source {
                path: track.file_path.clone(),
                source,
            })?
            .len();
        if source_size != track.file_size {
            return Err(ZipError::Source {
                path: track.file_path.clone(),
                source: std::io::Error::new(std::io::ErrorKind::InvalidData, "file size changed"),
            });
        }
    }

    let cover_size = match &cover_path {
        Some(path) => fs::metadata(path)
            .map_err(|source| ZipError::Source {
                path: path.clone(),
                source,
            })?
            .len(),
        None => 0,
    };
    let entry_count = tracks.len() as u64 + u64::from(cover_path.is_some()) + 1;
    let overhead = entry_count
        .checked_mul(ENTRY_OVERHEAD_BYTES)
        .and_then(|n| n.checked_add(ARCHIVE_HEADROOM_BYTES))
        .ok_or(ZipError::InvalidLimit)?;
    let usable = max_part_bytes.saturating_sub(overhead);
    if usable == 0 || cover_size > usable {
        return Err(ZipError::InvalidLimit);
    }

    let mut groups: Vec<Vec<ZipTrackEntry>> = Vec::new();
    let mut current = Vec::new();
    let mut current_size = cover_size;
    for track in tracks {
        if track.file_size > usable {
            return Err(ZipError::TrackTooLarge {
                name: track.archive_filename.to_string(),
            });
        }
        let next = current_size
            .checked_add(track.file_size)
            .ok_or(ZipError::InvalidLimit)?;
        if !current.is_empty() && next > usable {
            groups.push(std::mem::take(&mut current));
            current_size = cover_size;
        }
        current_size = current_size
            .checked_add(track.file_size)
            .ok_or(ZipError::InvalidLimit)?;
        current.push(track.clone());
    }
    if !current.is_empty() {
        groups.push(current);
    }

    let total_parts = groups.len();
    let year = release_date.chars().take(4).collect::<String>();
    let year_part = if year.len() == 4 && year.chars().all(|c| c.is_ascii_digit()) {
        format!(" ({year})")
    } else {
        String::new()
    };
    let label = archive_codec_label(codec);

    Ok(groups
        .into_iter()
        .enumerate()
        .map(|(index, tracks)| {
            let part_index = index + 1;
            let suffix = if total_parts == 1 {
                format!(" [{label}].zip")
            } else {
                format!(" [{label}] (Part {part_index} of {total_parts}).zip")
            };
            let name = format!("{artist} - {album}{year_part}{suffix}");
            let archive_filename = ArchiveFilename::sanitize_and_bound(&name, Some(&suffix));
            ZipPartPlan {
                part_index,
                total_parts,
                tracks,
                cover_path: cover_path.clone(),
                archive_filename,
            }
        })
        .collect())
}

/// Creates one archive atomically. `cancel` is checked between source chunks.
pub fn create_zip_archive(
    output_path: &Path,
    plan: &ZipPartPlan,
    progress_callback: Option<&dyn Fn(u64, u64)>,
    cancel: Option<&CancellationToken>,
) -> Result<u64, ZipError> {
    if plan.tracks.is_empty() {
        return Err(ZipError::Empty);
    }
    let mut names = std::collections::HashSet::new();
    names.insert("manifest.json".to_owned());
    if plan.cover_path.is_some() {
        names.insert("cover.jpg".to_owned());
    }
    for track in &plan.tracks {
        validate_entry_name(&track.archive_filename)?;
        if !names.insert(track.archive_filename.to_string()) {
            return Err(ZipError::DuplicateEntry(track.archive_filename.to_string()));
        }
    }

    let cover_size = plan
        .cover_path
        .as_ref()
        .map(|path| fs::metadata(path).map(|m| m.len()))
        .transpose()
        .map_err(|source| ZipError::Source {
            path: plan.cover_path.clone().unwrap(),
            source,
        })?
        .unwrap_or(0);
    // The manifest is the first archive entry, so content hashes must be
    // computed before any bytes are written. An unreadable source fails
    // here exactly as it would in the copy loop below — no half-hashed
    // archive is ever published.
    let cover_sha256 = match &plan.cover_path {
        Some(path) => Some(hash_file(path)?),
        None => None,
    };
    let track_hashes: Vec<String> = plan
        .tracks
        .iter()
        .map(|track| hash_file(&track.file_path))
        .collect::<Result<_, _>>()?;
    let manifest = serde_json::to_vec(&serde_json::json!({
        "format_version": 1,
        "part": plan.part_index,
        "total_parts": plan.total_parts,
        "cover_sha256": cover_sha256,
        "tracks": plan.tracks.iter().enumerate().map(|(index, track)| serde_json::json!({
            "name": track.archive_filename,
            "size": track.file_size,
            "sha256": track_hashes[index],
        })).collect::<Vec<_>>(),
    }))
    .map_err(|error| ZipError::Writer(error.to_string()))?;
    let total = plan
        .tracks
        .iter()
        .try_fold(cover_size, |sum, track| sum.checked_add(track.file_size))
        .ok_or(ZipError::InvalidLimit)?;
    let total = total
        .checked_add(manifest.len() as u64)
        .ok_or(ZipError::InvalidLimit)?;

    let parent = output_path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)?;
    let output_name = output_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("archive");
    let temp_suffix = format!(".{}.part", cuid2::create_id());
    let temp_name = format!(".{output_name}{temp_suffix}");
    let temp_name = StandardFilename::sanitize_and_bound(&temp_name, Some(&temp_suffix));
    let temp_path = parent.join(temp_name);
    let result = (|| {
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp_path)?;
        let writer = BufWriter::with_capacity(1024 * 1024, file);
        let mut zip = ZipWriter::new(writer);
        // ALAC/M4A is already compressed; storing avoids CPU spent on a
        // second compression pass and keeps size accounting predictable.
        let options = SimpleFileOptions::default().compression_method(CompressionMethod::Stored);
        let mut written = 0u64;
        let mut buffer = [0u8; 64 * 1024];

        zip.start_file("manifest.json", options)
            .map_err(|error| ZipError::Writer(error.to_string()))?;
        zip.write_all(&manifest)?;
        written = written
            .checked_add(manifest.len() as u64)
            .ok_or(ZipError::InvalidLimit)?;

        let mut copy_entry = |name: &str, path: &Path| -> Result<(), ZipError> {
            if cancel.is_some_and(|token| token.is_cancelled()) {
                return Err(ZipError::Cancelled);
            }
            let mut source =
                BufReader::new(File::open(path).map_err(|source| ZipError::Source {
                    path: path.to_owned(),
                    source,
                })?);
            zip.start_file(name, options)
                .map_err(|error| ZipError::Writer(error.to_string()))?;
            loop {
                if cancel.is_some_and(|token| token.is_cancelled()) {
                    return Err(ZipError::Cancelled);
                }
                let count = source.read(&mut buffer)?;
                if count == 0 {
                    break;
                }
                zip.write_all(&buffer[..count])?;
                written = written
                    .checked_add(count as u64)
                    .ok_or(ZipError::InvalidLimit)?;
                if let Some(callback) = progress_callback {
                    callback(written, total);
                }
            }
            Ok(())
        };

        if let Some(cover) = &plan.cover_path {
            copy_entry("cover.jpg", cover)?;
        }
        for track in &plan.tracks {
            copy_entry(&track.archive_filename, &track.file_path)?;
        }
        let mut writer = zip
            .finish()
            .map_err(|error| ZipError::Writer(error.to_string()))?;
        writer.flush()?;
        writer.get_ref().sync_all()?;
        Ok::<u64, ZipError>(fs::metadata(&temp_path)?.len())
    })();

    match result {
        Ok(size) => match fs::rename(&temp_path, output_path) {
            Ok(()) => Ok(size),
            Err(error) => {
                let _ = fs::remove_file(&temp_path);
                Err(ZipError::Io(error))
            }
        },
        Err(error) => {
            let _ = fs::remove_file(&temp_path);
            Err(error)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filename::{MAX_ARCHIVE_FILENAME_BYTES, MAX_FILENAME_BYTES};

    fn entry(path: &Path, name: &str, size: u64) -> ZipTrackEntry {
        ZipTrackEntry {
            file_path: path.to_owned(),
            archive_filename: ZipEntryName::sanitize_and_bound(name, None),
            file_size: size,
        }
    }

    #[test]
    fn naming_and_sanitization() {
        assert_eq!(
            build_album_archive_base_name("AC/DC", "Back in Black: Live?", "1980"),
            "AC_DC - Back in Black_ Live_ (1980) [ALAC]"
        );
    }

    #[test]
    fn naming_flac_archive_and_entries() {
        assert_eq!(
            build_album_archive_base_name_with_codec("Artist", "Album", "2024", "flac"),
            "Artist - Album (2024) [FLAC]"
        );
        assert_eq!(
            build_zip_entry_filename_with_codec(Some(1), "Song", "Artist", "123", "flac").as_str(),
            "01 - Song - Artist [123].flac"
        );
    }

    #[test]
    fn long_archive_names_are_bounded_with_codec_suffix() {
        let artist = "é".repeat(200);
        let base = build_album_archive_base_name_with_codec(&artist, "Album", "", "ec-3");
        assert!(base.len() <= MAX_ARCHIVE_FILENAME_BYTES);
        assert!(base.ends_with(" [Atmos]"));
        assert!(base.into_standard().len() <= MAX_FILENAME_BYTES);

        let root = std::env::temp_dir().join(format!("zip-test-{}", cuid2::create_id()));
        fs::create_dir_all(&root).unwrap();
        let track_path = root.join("track.m4a");
        fs::write(&track_path, b"audio-bytes").unwrap();
        let tracks = [entry(&track_path, "01 - Track.m4a", 11)];
        let plan = plan_zip_parts_with_codec(&artist, "Album", "", &tracks, None, 100_000, "ec-3")
            .unwrap();
        assert!(plan[0].archive_filename.len() <= MAX_ARCHIVE_FILENAME_BYTES);
        assert!(plan[0].archive_filename.ends_with(" [Atmos].zip"));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn rejects_invalid_limit_and_duplicate_names() {
        let path = std::env::temp_dir().join(format!("zip-test-{}", cuid2::create_id()));
        fs::write(&path, b"x").unwrap();
        let tracks = [entry(&path, "same.m4a", 1), entry(&path, "same.m4a", 1)];
        assert!(matches!(
            plan_zip_parts("a", "b", "", &tracks, None, 0),
            Err(ZipError::InvalidLimit)
        ));
        assert!(matches!(
            plan_zip_parts("a", "b", "", &tracks, None, 100_000),
            Err(ZipError::DuplicateEntry(_))
        ));
        let _ = fs::remove_file(path);
    }

    #[test]
    fn creates_atomic_archive_with_manifest() {
        let root = std::env::temp_dir().join(format!("zip-test-{}", cuid2::create_id()));
        fs::create_dir_all(&root).unwrap();
        let track_path = root.join("track.m4a");
        fs::write(&track_path, b"audio-bytes").unwrap();
        let tracks = [entry(&track_path, "01 - Track.m4a", 11)];
        let plan = plan_zip_parts("Artist", "Album", "2024", &tracks, None, 100_000).unwrap();
        let output = root.join(&plan[0].archive_filename);
        let size = create_zip_archive(&output, &plan[0], None, None).unwrap();
        assert_eq!(size, fs::metadata(&output).unwrap().len());
        let file = File::open(&output).unwrap();
        let mut archive = zip::ZipArchive::new(file).unwrap();
        assert!(archive.by_name("manifest.json").is_ok());
        assert!(archive.by_name("01 - Track.m4a").is_ok());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn manifest_contains_sha256_hashes() {
        let root = std::env::temp_dir().join(format!("zip-test-{}", cuid2::create_id()));
        fs::create_dir_all(&root).unwrap();
        let track_path = root.join("track.m4a");
        fs::write(&track_path, b"audio-bytes").unwrap();
        let cover_path = root.join("cover.jpg");
        fs::write(&cover_path, b"cover-bytes").unwrap();
        let tracks = [entry(&track_path, "01 - Track.m4a", 11)];
        let plan = plan_zip_parts(
            "Artist",
            "Album",
            "2024",
            &tracks,
            Some(cover_path.clone()),
            100_000,
        )
        .unwrap();
        let output = root.join(&plan[0].archive_filename);
        create_zip_archive(&output, &plan[0], None, None).unwrap();

        let mut track_hasher = Sha256::new();
        track_hasher.update(b"audio-bytes");
        let expected_track_digest = hex_digest(track_hasher);
        let mut cover_hasher = Sha256::new();
        cover_hasher.update(b"cover-bytes");
        let expected_cover_digest = hex_digest(cover_hasher);

        let file = File::open(&output).unwrap();
        let mut archive = zip::ZipArchive::new(file).unwrap();
        assert!(archive.by_name("cover.jpg").is_ok());
        let mut manifest_file = archive.by_name("manifest.json").unwrap();
        let mut manifest_bytes = Vec::new();
        manifest_file.read_to_end(&mut manifest_bytes).unwrap();
        let manifest: serde_json::Value = serde_json::from_slice(&manifest_bytes).unwrap();
        assert_eq!(manifest["format_version"], serde_json::json!(1));
        assert_eq!(
            manifest["cover_sha256"],
            serde_json::json!(expected_cover_digest)
        );
        let track = &manifest["tracks"][0];
        assert_eq!(track["name"], serde_json::json!("01 - Track.m4a"));
        assert_eq!(track["size"], serde_json::json!(11));
        assert_eq!(track["sha256"], serde_json::json!(expected_track_digest));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn manifest_omits_cover_hash_without_cover() {
        let root = std::env::temp_dir().join(format!("zip-test-{}", cuid2::create_id()));
        fs::create_dir_all(&root).unwrap();
        let track_path = root.join("track.m4a");
        fs::write(&track_path, b"audio-bytes").unwrap();
        let tracks = [entry(&track_path, "01 - Track.m4a", 11)];
        let plan = plan_zip_parts("Artist", "Album", "2024", &tracks, None, 100_000).unwrap();
        let output = root.join(&plan[0].archive_filename);
        create_zip_archive(&output, &plan[0], None, None).unwrap();
        let file = File::open(&output).unwrap();
        let mut archive = zip::ZipArchive::new(file).unwrap();
        let mut manifest_file = archive.by_name("manifest.json").unwrap();
        let mut manifest_bytes = Vec::new();
        manifest_file.read_to_end(&mut manifest_bytes).unwrap();
        let manifest: serde_json::Value = serde_json::from_slice(&manifest_bytes).unwrap();
        assert_eq!(manifest["cover_sha256"], serde_json::json!(null));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn generation_hash_is_deterministic_and_sensitive() {
        let ids = ["1", "2", "3"];
        let base = album_generation_hash("apple", "alb.1", &ids);
        // Deterministic.
        assert_eq!(base, album_generation_hash("apple", "alb.1", &ids));
        // Order-sensitive.
        let swapped = ["2", "1", "3"];
        assert_ne!(base, album_generation_hash("apple", "alb.1", &swapped));
        // Provider-sensitive.
        assert_ne!(base, album_generation_hash("spotify", "alb.1", &ids));
        // Album-sensitive.
        assert_ne!(base, album_generation_hash("apple", "alb.2", &ids));
        // Pinned framing: exact digest for a fixed input.
        let mut expected = Sha256::new();
        expected.update(b"alac-zip-gen-v1\x00apple\x00alb.1\x001\x002\x003");
        assert_eq!(base, hex_digest(expected));
    }

    #[test]
    fn cancellation_does_not_publish_output() {
        let root = std::env::temp_dir().join(format!("zip-test-{}", cuid2::create_id()));
        fs::create_dir_all(&root).unwrap();
        let track_path = root.join("track.m4a");
        fs::write(&track_path, b"audio-bytes").unwrap();
        let tracks = [entry(&track_path, "01 - Track.m4a", 11)];
        let plan = plan_zip_parts("Artist", "Album", "2024", &tracks, None, 100_000).unwrap();
        let output = root.join(&plan[0].archive_filename);
        let token = CancellationToken::new();
        token.cancel();
        assert!(matches!(
            create_zip_archive(&output, &plan[0], None, Some(&token)),
            Err(ZipError::Cancelled)
        ));
        assert!(!output.exists());
        let _ = fs::remove_dir_all(root);
    }
}
