//! Rip orchestration, track ripping, ZIP assembly, streaming, and tagging/filename
//! policy. Provider implementations live in separate crates.

pub mod filename;
pub mod limits;
pub use lyrics;
pub mod orchestrator;
pub mod progress;
pub mod queue;
pub mod ripper;
pub mod settings;
pub mod streaming;
pub mod types;
pub mod zip;

pub use filename::{ArchiveFilename, BoundedName, StandardFilename, TrackFilename, ZipEntryName};
pub use music::{Rendition, RenditionPolicy, RenditionWorkPlan, RenditionWorkUnit};
pub use types::{
    AlbumTracks, ArtistTracks, Codec, ParsedAlacInput, ParsedTargetItem, Provider, TargetKind,
    TrackKey, TrackMeta,
};
