//! Network adapters for lyric catalogues used by Sonora and BetterLyrics.
//!
//! Each adapter returns the same normalized LRC/ELRC document so provider
//! details stay behind the `LyricsSource` interface. Every provider lives in
//! its own file next to this one; [`shared`] holds the JSON, scoring, and
//! encoding helpers they have in common.

mod shared;

#[cfg(feature = "amll-ttml-db")]
mod amll_ttml_db;
#[cfg(feature = "betterlyrics")]
mod betterlyrics;
#[cfg(feature = "binimum")]
mod binimum;
#[cfg(feature = "kugou")]
mod kugou;
#[cfg(feature = "lrclib")]
mod lrclib;
#[cfg(feature = "musixmatch")]
mod musixmatch;
#[cfg(feature = "netease")]
mod netease;
#[cfg(feature = "paxsenix")]
mod paxsenix;
#[cfg(feature = "qq")]
mod qq_music;
#[cfg(feature = "spotify")]
mod spotify;
#[cfg(feature = "unison")]
mod unison;
#[cfg(feature = "youtube")]
mod youtube_music;

#[cfg(feature = "amll-ttml-db")]
pub use amll_ttml_db::AmllTtmlDb;
#[cfg(feature = "betterlyrics")]
pub use betterlyrics::BetterLyrics;
#[cfg(feature = "binimum")]
pub use binimum::Binimum;
#[cfg(feature = "kugou")]
pub use kugou::Kugou;
#[cfg(feature = "lrclib")]
pub use lrclib::Lrclib;
#[cfg(feature = "musixmatch")]
pub use musixmatch::Musixmatch;
#[cfg(feature = "netease")]
pub use netease::NetEase;
#[cfg(feature = "paxsenix")]
pub use paxsenix::Paxsenix;
#[cfg(feature = "qq")]
pub use qq_music::QqMusic;
#[cfg(feature = "spotify")]
pub use spotify::Spotify;
#[cfg(feature = "unison")]
pub use unison::Unison;
#[cfg(feature = "youtube")]
pub use youtube_music::YouTubeMusic;
