//! Native wrapper-lite engine module for Apple Music ALAC decryption.

pub mod cenc;
pub mod client;
pub mod decryptor;
pub mod engine;
pub mod playlist;
pub mod widevine;

pub use client::{WrapperError, WrapperLiteClient, WrapperUnavailableReason};
pub use decryptor::{decrypt_fragment, transform_init_segment};
pub use engine::{WrapperEngine, WrapperTrackOutcome};
pub use music::CodecPreference;
pub use playlist::{
    AlacStreamInfo, MediaPlaylistInfo, parse_master_playlist, parse_media_playlist,
};
