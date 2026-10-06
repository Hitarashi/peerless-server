use std::{borrow::Borrow, fmt, ops::Deref, path::Path};

use crate::types::TrackMeta;

pub const MAX_FILENAME_BYTES: usize = 255;

pub const MAX_ARCHIVE_FILENAME_BYTES: usize = 245;

pub const MAX_ZIP_ENTRY_FILENAME_BYTES: usize = 240;

pub type TrackFilename = BoundedName<MAX_FILENAME_BYTES>;

pub type StandardFilename = BoundedName<MAX_FILENAME_BYTES>;

pub type ArchiveFilename = BoundedName<MAX_ARCHIVE_FILENAME_BYTES>;

pub type ZipEntryName = BoundedName<MAX_ZIP_ENTRY_FILENAME_BYTES>;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum FilenameError {
    #[error("filename is empty")]
    Empty,
    #[error("filename length {len} exceeds capacity {max}")]
    TooLong { len: usize, max: usize },
    #[error("filename contains invalid character '{0}'")]
    InvalidCharacter(char),
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BoundedName<const MAX: usize>(String);

impl<const MAX: usize> BoundedName<MAX> {
    pub fn try_new(name: impl Into<String>) -> Result<Self, FilenameError> {
        let s = name.into();
        let trimmed = s.trim();
        if trimmed.is_empty() {
            return Err(FilenameError::Empty);
        }
        if trimmed == "." || trimmed == ".." {
            return Err(FilenameError::InvalidCharacter('.'));
        }
        if trimmed.len() > MAX {
            return Err(FilenameError::TooLong {
                len: trimmed.len(),
                max: MAX,
            });
        }
        for c in trimmed.chars() {
            if is_forbidden_char(c) {
                return Err(FilenameError::InvalidCharacter(c));
            }
        }
        Ok(Self(trimmed.to_owned()))
    }

    pub fn sanitize_and_bound(name: &str, suffix: Option<&str>) -> Self {
        let sanitized: String = name
            .chars()
            .map(|c| if is_forbidden_char(c) { '_' } else { c })
            .collect();
        let trimmed = sanitized.trim();
        let candidate = if trimmed.is_empty() || trimmed == "." || trimmed == ".." {
            "track"
        } else {
            trimmed
        };

        let bounded = match suffix {
            Some(sfx) if candidate.ends_with(sfx) => {
                if sfx.len() >= MAX {
                    bound_utf8_prefix(candidate, MAX).to_owned()
                } else {
                    let prefix = candidate.strip_suffix(sfx).unwrap_or(candidate);
                    let prefix = bound_utf8_prefix(prefix, MAX - sfx.len());
                    format!("{prefix}{sfx}")
                }
            }
            _ => bound_utf8_prefix(candidate, MAX).to_owned(),
        };

        let final_str = if bounded.is_empty() || bounded == "." || bounded == ".." {
            "track".to_owned()
        } else {
            bounded
        };

        Self(final_str)
    }

    pub fn with_collision_id(&self, collision_id: &str) -> Self {
        let s = &self.0;
        let (stem, ext) = match s.rfind('.') {
            Some(idx) if idx > 0 => (&s[..idx], &s[idx..]),
            _ => (s.as_str(), ""),
        };

        let sanitized_id: String = collision_id
            .chars()
            .map(|c| if is_forbidden_char(c) { '_' } else { c })
            .collect();
        let collision_suffix = format!("_{sanitized_id}{ext}");
        if collision_suffix.len() >= MAX {
            let combined = format!("{stem}{collision_suffix}");
            Self::sanitize_and_bound(&combined, Some(ext))
        } else {
            let prefix = bound_utf8_prefix(stem, MAX - collision_suffix.len());
            Self(format!("{prefix}{collision_suffix}"))
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_inner(self) -> String {
        self.0
    }
}

impl ArchiveFilename {
    pub const PARTIAL_SUFFIX: &'static str = " [Partial]";

    pub fn into_standard(self) -> StandardFilename {
        BoundedName(self.0)
    }

    pub fn into_partial(self) -> StandardFilename {
        let s = self.0;
        let partial_name = if let Some(stem) = s.strip_suffix(".zip") {
            format!("{stem}{}.zip", Self::PARTIAL_SUFFIX)
        } else {
            format!("{s}{}", Self::PARTIAL_SUFFIX)
        };
        BoundedName(partial_name)
    }
}

pub fn bound_utf8_prefix(s: &str, max_bytes: usize) -> &str {
    if s.len() <= max_bytes {
        return s;
    }
    let mut end = max_bytes.min(s.len());
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

pub fn is_forbidden_char(c: char) -> bool {
    matches!(
        c,
        '<' | '>' | ':' | '"' | '/' | '\\' | '|' | '?' | '*' | '\0'
    ) || c.is_control()
}

pub fn build_track_filename(meta: &TrackMeta) -> TrackFilename {
    build_track_filename_with_codec(meta, "alac")
}

pub fn build_track_filename_with_codec(meta: &TrackMeta, codec: &str) -> TrackFilename {
    let number = meta.track_number.filter(|number| *number != 0).unwrap_or(1);
    let suffix = track_filename_suffix(meta.explicit, codec);
    let combined = format!("{number:02}. {} - {}{suffix}", meta.title, meta.artist);
    TrackFilename::sanitize_and_bound(&combined, Some(&suffix))
}

pub(crate) fn track_filename_suffix(explicit: bool, codec: &str) -> String {
    let explicit = if explicit { " [E]" } else { "" };
    let (label, ext) = match codec {
        "ec-3" => ("Atmos", "m4a"),
        "aac" | "mp4a.40.2" | "mp4a.40.5" => ("AAC", "m4a"),
        "flac" => ("FLAC", "flac"),
        "mp3" => ("MP3", "mp3"),
        _ => ("ALAC", "m4a"),
    };
    format!("{explicit} [{label}].{ext}")
}

impl<const MAX: usize> Deref for BoundedName<MAX> {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<const MAX: usize> AsRef<str> for BoundedName<MAX> {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl<const MAX: usize> AsRef<Path> for BoundedName<MAX> {
    fn as_ref(&self) -> &Path {
        Path::new(&self.0)
    }
}

impl<const MAX: usize> Borrow<str> for BoundedName<MAX> {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl<const MAX: usize> fmt::Display for BoundedName<MAX> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl<const MAX: usize> PartialEq<str> for BoundedName<MAX> {
    fn eq(&self, other: &str) -> bool {
        self.0 == other
    }
}

impl<const MAX: usize> PartialEq<&str> for BoundedName<MAX> {
    fn eq(&self, other: &&str) -> bool {
        self.0 == *other
    }
}

impl<const MAX: usize> PartialEq<String> for BoundedName<MAX> {
    fn eq(&self, other: &String) -> bool {
        self.0 == *other
    }
}

impl<const MAX: usize> PartialEq<BoundedName<MAX>> for str {
    fn eq(&self, other: &BoundedName<MAX>) -> bool {
        self == other.0.as_str()
    }
}

impl<const MAX: usize> PartialEq<BoundedName<MAX>> for &str {
    fn eq(&self, other: &BoundedName<MAX>) -> bool {
        *self == other.0.as_str()
    }
}

impl<const MAX: usize> PartialEq<BoundedName<MAX>> for String {
    fn eq(&self, other: &BoundedName<MAX>) -> bool {
        self == &other.0
    }
}

impl<const MAX: usize> TryFrom<String> for BoundedName<MAX> {
    type Error = FilenameError;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        Self::try_new(s)
    }
}

impl<const MAX: usize> TryFrom<&str> for BoundedName<MAX> {
    type Error = FilenameError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        Self::try_new(s)
    }
}

impl<const MAX: usize> serde::Serialize for BoundedName<MAX> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.serialize(serializer)
    }
}

impl<'de, const MAX: usize> serde::Deserialize<'de> for BoundedName<MAX> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Self::try_new(s).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn try_new_validates_length_and_characters() {
        assert_eq!(
            BoundedName::<10>::try_new("valid.m4a").unwrap().as_str(),
            "valid.m4a"
        );
        assert_eq!(
            BoundedName::<5>::try_new("toolong.m4a"),
            Err(FilenameError::TooLong { len: 11, max: 5 })
        );
        assert_eq!(
            BoundedName::<10>::try_new("bad/path"),
            Err(FilenameError::InvalidCharacter('/'))
        );
        assert_eq!(BoundedName::<10>::try_new("   "), Err(FilenameError::Empty));
    }

    #[test]
    fn sanitize_and_bound_replaces_forbidden_and_preserves_suffix() {
        let name = "Artist: Song / Remix [ALAC].m4a";
        let bounded = TrackFilename::sanitize_and_bound(name, Some(" [ALAC].m4a"));
        assert_eq!(bounded.as_str(), "Artist_ Song _ Remix [ALAC].m4a");

        let long_title = "a".repeat(300);
        let long_name = format!("{long_title} [ALAC].m4a");
        let bounded = TrackFilename::sanitize_and_bound(&long_name, Some(" [ALAC].m4a"));
        assert!(bounded.len() <= MAX_FILENAME_BYTES);
        assert!(bounded.ends_with(" [ALAC].m4a"));
    }

    #[test]
    fn utf8_boundaries_are_never_split() {
        let note = "🎵";
        let prefix = "a".repeat(253);
        let name = format!("{prefix}{note}.m4a");
        let bounded = TrackFilename::sanitize_and_bound(&name, Some(".m4a"));
        assert!(bounded.len() <= MAX_FILENAME_BYTES);
        assert!(bounded.ends_with(".m4a"));

        std::str::from_utf8(bounded.as_bytes()).expect("valid utf-8");
    }

    #[test]
    fn into_partial_transforms_archive_safely() {
        let base = "A".repeat(240);
        let archive = format!("{base}.zip");
        let planned = ArchiveFilename::sanitize_and_bound(&archive, Some(".zip"));
        assert!(planned.len() <= MAX_ARCHIVE_FILENAME_BYTES);

        let partial = planned.into_partial();
        assert!(partial.len() <= MAX_FILENAME_BYTES);
        assert!(partial.ends_with(" [Partial].zip"));
    }

    #[test]
    fn collision_id_preserves_extension_and_bound() {
        let long_base = "B".repeat(250);
        let track = TrackFilename::sanitize_and_bound(&format!("{long_base}.m4a"), Some(".m4a"));
        let collided = track.with_collision_id("xyz999");
        assert!(collided.len() <= MAX_FILENAME_BYTES);
        assert!(collided.ends_with("_xyz999.m4a"));
    }

    #[test]
    fn relative_path_components_are_rejected_or_sanitized() {
        assert!(BoundedName::<10>::try_new(".").is_err());
        assert!(BoundedName::<10>::try_new("..").is_err());

        assert_eq!(
            TrackFilename::sanitize_and_bound(".", None).as_str(),
            "track"
        );
        assert_eq!(
            TrackFilename::sanitize_and_bound("..", None).as_str(),
            "track"
        );
    }

    #[test]
    fn oversized_suffix_and_collision_suffixes_are_handled() {
        let long_suffix = ".abcdefghijklmnop";
        let bounded =
            BoundedName::<10>::sanitize_and_bound("test.abcdefghijklmnop", Some(long_suffix));
        assert!(bounded.len() <= 10);

        let base = BoundedName::<10>::sanitize_and_bound("track.m4a", Some(".m4a"));
        let collided = base.with_collision_id("verylongcollisionidexceedingtenbytes");
        assert!(collided.len() <= 10);
    }
}
