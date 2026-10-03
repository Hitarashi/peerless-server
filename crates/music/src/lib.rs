//! Provider-neutral music domain types shared by catalog, persistence, and engine.
//!
//! These values form the stable contract between provider adapters and the
//! application layers. It is also the lowest-level crate in the workspace, so
//! it hosts cross-cutting helpers (see [`url`] and [`time`]) that every other
//! crate can depend on without pulling in a provider adapter or the engine.

#[cfg(feature = "diesel")]
use diesel::{deserialize::FromSql, pg::Pg, serialize::ToSql};
use serde::{Deserialize, Serialize};

pub mod time;
pub mod url;

/// Returns a lowercase canonical form when `value` is a MusicBrainz UUID.
pub fn normalize_recording_mbid(value: &str) -> Option<String> {
    let value = value.trim();
    if value.len() != 36 {
        return None;
    }
    for (index, byte) in value.bytes().enumerate() {
        if matches!(index, 8 | 13 | 18 | 23) {
            if byte != b'-' {
                return None;
            }
        } else if !byte.is_ascii_hexdigit() {
            return None;
        }
    }
    Some(value.to_ascii_lowercase())
}

/// A catalog/cache provider identifier.
///
/// Apple is the only provider handled by the bot. Other identifiers stay
/// opaque so legacy database rows can be read and backed up unchanged.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "diesel", derive(diesel::AsExpression, diesel::FromSqlRow))]
#[cfg_attr(feature = "diesel", diesel(sql_type = diesel::sql_types::VarChar))]
pub enum Provider {
    Apple,
    Other(String),
}

impl Provider {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Apple => "apple",
            Self::Other(value) => value,
        }
    }

    pub fn is_apple(&self) -> bool {
        matches!(self, Self::Apple)
    }
}

impl std::fmt::Display for Provider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for Provider {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let value = value.trim();
        if value.is_empty() {
            return Err("provider identifier cannot be empty".to_owned());
        }
        if value.eq_ignore_ascii_case("apple") {
            Ok(Self::Apple)
        } else {
            Ok(Self::Other(value.to_owned()))
        }
    }
}

impl Serialize for Provider {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for Provider {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}

#[cfg(feature = "diesel")]
impl ToSql<diesel::sql_types::VarChar, Pg> for Provider {
    fn to_sql<'b>(
        &'b self,
        out: &mut diesel::serialize::Output<'b, '_, Pg>,
    ) -> diesel::serialize::Result {
        <str as ToSql<diesel::sql_types::Text, Pg>>::to_sql(self.as_str(), out)
    }
}

#[cfg(feature = "diesel")]
impl FromSql<diesel::sql_types::VarChar, Pg> for Provider {
    fn from_sql(
        bytes: <Pg as diesel::backend::Backend>::RawValue<'_>,
    ) -> diesel::deserialize::Result<Self> {
        <String as FromSql<diesel::sql_types::Text, Pg>>::from_sql(bytes)?
            .parse()
            .map_err(Into::into)
    }
}

/// An audio codec / encoding format supported by the bot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[cfg_attr(feature = "diesel", derive(diesel::AsExpression, diesel::FromSqlRow))]
#[cfg_attr(feature = "diesel", diesel(sql_type = diesel::sql_types::VarChar))]
#[serde(rename_all = "lowercase")]
pub enum Codec {
    #[default]
    Alac,
    #[serde(rename = "ec-3")]
    Ec3,
    Aac,
    Flac,
}

/// Audio variant selected when a provider offers more than one representation.
/// Used for highest-quality ALAC/FLAC, resolution tiers, and Dolby Atmos streams.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
pub enum CodecPreference {
    #[default]
    HighestQuality,
    HiRes192,
    HiRes96,
    LosslessCd,
    Mp3_320,
    Atmos,
}

impl CodecPreference {
    pub fn parse(s: &str) -> Option<Self> {
        let clean = s.trim().trim_start_matches('-').to_ascii_lowercase();
        match clean.as_str() {
            "highest" | "max" | "best" => Some(Self::HighestQuality),
            "hires" | "hires192" | "24-192" | "192" => Some(Self::HiRes192),
            "hires96" | "24-96" | "96" => Some(Self::HiRes96),
            "lossless" | "cd" | "16-44" | "flac" => Some(Self::LosslessCd),
            "mp3" | "320" => Some(Self::Mp3_320),
            "atmos" => Some(Self::Atmos),
            _ => None,
        }
    }
}

/// Renditions requested by one orchestration job. Atmos is deliberately an
/// optional addition to the required primary rendition; there is no
/// Atmos-only policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RenditionPolicy {
    #[default]
    PrimaryOnly,
    PrimaryWithOptionalAtmos,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Rendition {
    Primary,
    Atmos,
}

impl Rendition {
    pub const fn codec_preference(self) -> CodecPreference {
        match self {
            Self::Primary => CodecPreference::HighestQuality,
            Self::Atmos => CodecPreference::Atmos,
        }
    }

    pub const fn required(self) -> bool {
        matches!(self, Self::Primary)
    }

    pub const fn accepted_cache_codecs(self) -> &'static [Codec] {
        match self {
            Self::Primary => &[Codec::Alac, Codec::Aac, Codec::Flac],
            Self::Atmos => &[Codec::Ec3],
        }
    }
}

impl RenditionPolicy {
    pub const fn renditions(self) -> &'static [Rendition] {
        match self {
            Self::PrimaryOnly => &[Rendition::Primary],
            Self::PrimaryWithOptionalAtmos => &[Rendition::Primary, Rendition::Atmos],
        }
    }
}

/// One ordered acquisition unit for an orchestration request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenditionWorkUnit {
    track_id: String,
    rendition: Rendition,
}

impl RenditionWorkUnit {
    pub fn track_id(&self) -> &str {
        &self.track_id
    }

    pub const fn rendition(&self) -> Rendition {
        self.rendition
    }

    pub const fn codec_preference(&self) -> CodecPreference {
        self.rendition.codec_preference()
    }

    pub const fn required(&self) -> bool {
        self.rendition.required()
    }

    pub const fn accepted_cache_codecs(&self) -> &'static [Codec] {
        self.rendition.accepted_cache_codecs()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenditionWorkPlan {
    units: Vec<RenditionWorkUnit>,
}

impl RenditionPolicy {
    pub fn work_plan<I, S>(self, track_ids: I) -> RenditionWorkPlan
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let mut units = Vec::new();
        for track_id in track_ids {
            let track_id = track_id.into();
            for &rendition in self.renditions() {
                units.push(RenditionWorkUnit {
                    track_id: track_id.clone(),
                    rendition,
                });
            }
        }
        RenditionWorkPlan { units }
    }
}

impl RenditionWorkPlan {
    pub fn units(&self) -> &[RenditionWorkUnit] {
        &self.units
    }

    pub fn into_units(self) -> Vec<RenditionWorkUnit> {
        self.units
    }
}

impl Codec {
    pub const fn mime_type(self) -> &'static str {
        match self {
            Self::Alac | Self::Aac | Self::Ec3 => "audio/mp4",
            Self::Flac => "audio/flac",
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Alac => "alac",
            Self::Ec3 => "ec-3",
            Self::Aac => "aac",
            Self::Flac => "flac",
        }
    }

    pub const fn display_name(self) -> &'static str {
        match self {
            Self::Alac => "ALAC",
            Self::Ec3 => "Dolby Atmos",
            Self::Aac => "AAC",
            Self::Flac => "FLAC",
        }
    }
}

impl std::fmt::Display for Codec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for Codec {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "alac" => Ok(Self::Alac),
            "ec-3" | "ec3" | "atmos" | "dolby" | "dolby atmos" | "eac3" => Ok(Self::Ec3),
            "aac" | "mp4a.40.2" | "mp4a.40.5" | "heaac" => Ok(Self::Aac),
            "flac" => Ok(Self::Flac),
            other => Err(format!("unknown codec: {other}")),
        }
    }
}

#[cfg(feature = "diesel")]
impl ToSql<diesel::sql_types::VarChar, Pg> for Codec {
    fn to_sql<'b>(
        &'b self,
        out: &mut diesel::serialize::Output<'b, '_, Pg>,
    ) -> diesel::serialize::Result {
        <str as ToSql<diesel::sql_types::Text, Pg>>::to_sql(self.as_str(), out)
    }
}

#[cfg(feature = "diesel")]
impl FromSql<diesel::sql_types::VarChar, Pg> for Codec {
    fn from_sql(
        bytes: <Pg as diesel::backend::Backend>::RawValue<'_>,
    ) -> diesel::deserialize::Result<Self> {
        <String as FromSql<diesel::sql_types::Text, Pg>>::from_sql(bytes)?
            .parse()
            .map_err(Into::into)
    }
}

/// A globally unique track identity. The provider and optional codec are part of the identity.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TrackKey {
    pub provider: Provider,
    pub track_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub codec: Option<Codec>,
}

impl TrackKey {
    pub fn new(provider: Provider, track_id: impl Into<String>) -> Self {
        Self {
            provider,
            track_id: track_id.into(),
            codec: None,
        }
    }

    pub fn with_codec(mut self, codec: Codec) -> Self {
        self.codec = Some(codec);
        self
    }

    pub fn apple(track_id: impl Into<String>) -> Self {
        Self::new(Provider::Apple, track_id)
    }

    pub fn apple_codec(track_id: impl Into<String>, codec: Codec) -> Self {
        Self::new(Provider::Apple, track_id).with_codec(codec)
    }
}

/// The kind of music target a parsed link/id refers to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TargetKind {
    Track,
    Album,
    Playlist,
    Artist,
}

/// One parsed link/id token, e.g. `?i=` track, album, `pl.` playlist, artist.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ParsedTargetItem {
    pub id: String,
    pub kind: TargetKind,
    /// Lowercased two-letter storefront captured from the URL, when present.
    pub storefront: Option<String>,
}

/// Full result of parsing a user command (or batch file line set).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedAlacInput {
    pub items: Vec<ParsedTargetItem>,
    pub track_id: String,
    pub force: bool,
    pub is_album: bool,
    pub is_playlist: bool,
    pub is_artist: bool,
    pub storefront: Option<String>,
}

/// Canonical track metadata resolved from a music provider catalog.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrackMeta {
    pub id: String,
    pub title: String,
    pub artist: String,
    pub album: String,
    pub album_artist: String,
    pub genre: Option<String>,
    /// First 10 chars of the raw release date, or `''` when absent
    /// (always a string, like `artwork_url`).
    pub release_date: String,
    /// Composer when the provider exposes it; otherwise omitted.
    pub composer: Option<String>,
    pub track_number: Option<i64>,
    pub track_count: Option<i64>,
    pub disc_number: Option<i64>,
    pub disc_count: Option<i64>,
    /// Duration in seconds (milliseconds / 1000, rounded).
    pub duration_secs: i64,
    pub explicit: bool,
    /// Provider advisory value (`explicit`, `clean`, or another inoffensive
    /// classification), retained so the media layer can write the precise
    /// provider rating metadata.
    pub content_advisory: Option<String>,
    /// Missing artwork maps to `''` (never absent).
    pub artwork_url: String,
    /// Provider-native album and artist identifiers, when available.
    pub album_id: Option<String>,
    pub artist_id: Option<String>,
    /// Recording and release metadata exposed by the provider.
    pub isrc: Option<String>,
    pub record_label: Option<String>,
    pub copyright: Option<String>,
    pub upc: Option<String>,
    pub is_streamable: Option<bool>,
}

/// An album plus its ordered track list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlbumTracks {
    pub album: TrackMeta,
    pub tracks: Vec<TrackMeta>,
}

/// An artist's resolved discography tracks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtistTracks {
    pub artist_id: String,
    pub artist_name: String,
    pub tracks: Vec<TrackMeta>,
}

/// One track inside a playlist.
#[derive(Debug, Clone, PartialEq)]
pub struct PlaylistTrack {
    pub id: String,
    pub title: String,
    pub artist: String,
    pub duration: Option<u64>,
}

/// Playlist metadata and its ordered track list.
#[derive(Debug, Clone, PartialEq)]
pub struct PlaylistData {
    pub id: String,
    pub title: String,
    pub curator_name: Option<String>,
    pub description: Option<String>,
    pub tracks: Vec<PlaylistTrack>,
}

/// Result of a completed single-track rip.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrackRipResult {
    pub file_path: String,
    pub title: String,
    pub artist: String,
    pub album: String,
    pub duration: i64,
    pub bit_depth: u32,
    pub sample_rate: u32,
    pub codec: String,
    pub genre: String,
    pub release_date: String,
    pub track_number: i64,
    pub track_count: i64,
    pub isrc: Option<String>,
    pub recording_mbid: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_wire_format_preserves_apple_and_opaque_ids() {
        assert_eq!(Provider::Apple.as_str(), "apple");
        assert_eq!("apple".parse::<Provider>(), Ok(Provider::Apple));
        let provider = "Legacy-Store".parse::<Provider>().unwrap();
        assert_eq!(provider.as_str(), "Legacy-Store");
        assert_eq!(
            serde_json::to_string(&provider).unwrap(),
            "\"Legacy-Store\""
        );
        assert_eq!("Legacy-Store".parse::<Provider>(), Ok(provider));
        assert!("  ".parse::<Provider>().is_err());
    }

    #[test]
    fn track_key_keeps_codec_out_of_the_base_identity() {
        let key = TrackKey::apple("123");
        assert_eq!(key.codec, None);
        assert_eq!(key.clone().with_codec(Codec::Alac).track_id, "123");
    }

    #[test]
    fn rendition_policy_constrains_atmos_to_an_optional_ec3_unit() {
        assert_eq!(
            RenditionPolicy::PrimaryOnly.renditions(),
            &[Rendition::Primary]
        );
        assert_eq!(
            RenditionPolicy::PrimaryWithOptionalAtmos.renditions(),
            &[Rendition::Primary, Rendition::Atmos]
        );
        assert!(Rendition::Primary.required());
        assert!(!Rendition::Atmos.required());
        assert_eq!(
            Rendition::Primary.accepted_cache_codecs(),
            &[Codec::Alac, Codec::Aac, Codec::Flac]
        );
        assert_eq!(Rendition::Atmos.accepted_cache_codecs(), &[Codec::Ec3]);
    }

    #[test]
    fn work_unit_derives_policy_from_rendition_identity() {
        let units = RenditionPolicy::PrimaryWithOptionalAtmos
            .work_plan(["track"])
            .into_units();

        assert_eq!(units[0].track_id(), "track");
        assert_eq!(units[0].rendition(), Rendition::Primary);
        assert_eq!(units[0].codec_preference(), CodecPreference::HighestQuality);
        assert!(units[0].required());
        assert_eq!(
            units[0].accepted_cache_codecs(),
            &[Codec::Alac, Codec::Aac, Codec::Flac]
        );
        assert_eq!(units[1].rendition(), Rendition::Atmos);
        assert_eq!(units[1].codec_preference(), CodecPreference::Atmos);
        assert!(!units[1].required());
        assert_eq!(units[1].accepted_cache_codecs(), &[Codec::Ec3]);
    }

    #[test]
    fn codec_preference_options() {
        assert_eq!(
            CodecPreference::parse("hires"),
            Some(CodecPreference::HiRes192)
        );
        assert_eq!(
            CodecPreference::parse("cd"),
            Some(CodecPreference::LosslessCd)
        );
    }

    #[test]
    fn test_codec_mime_types() {
        assert_eq!(Codec::Alac.mime_type(), "audio/mp4");
        assert_eq!(Codec::Aac.mime_type(), "audio/mp4");
        assert_eq!(Codec::Ec3.mime_type(), "audio/mp4");
        assert_eq!(Codec::Flac.mime_type(), "audio/flac");
    }
}
