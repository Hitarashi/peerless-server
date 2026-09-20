//! Gateway trait defining the boundary for Qobuz catalog queries and stream acquisition.

use std::{fmt, future::Future, pin::Pin};

use music::{AlbumTracks, ArtistTracks, PlaylistData, TrackMeta};

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Qobuz format tiers, ascending. Kept in one place so every adapter and the
/// acquisition stage agree on the ladder (no per-adapter drift).
pub const QUALITY_MP3_320: u32 = 5;
pub const QUALITY_CD_LOSSLESS: u32 = 6;
pub const QUALITY_HI_RES_96: u32 = 7;
pub const QUALITY_MAX_HI_RES: u32 = 27;

/// Ascending tier order, shared by the ladder builder.
const TIERS_ASCENDING: [u32; 4] = [
    QUALITY_MP3_320,
    QUALITY_CD_LOSSLESS,
    QUALITY_HI_RES_96,
    QUALITY_MAX_HI_RES,
];

/// Resolution order for a requested format: the preferred tier first, then
/// the tiers *above* it (a higher master always satisfies a lower request),
/// then the tiers below it. Unknown ids degrade to top-down.
pub fn quality_ladder(preferred: u32) -> Vec<u32> {
    let Some(index) = TIERS_ASCENDING.iter().position(|&t| t == preferred) else {
        return vec![
            QUALITY_MAX_HI_RES,
            QUALITY_HI_RES_96,
            QUALITY_CD_LOSSLESS,
            QUALITY_MP3_320,
        ];
    };
    let mut order = vec![TIERS_ASCENDING[index]];
    order.extend(TIERS_ASCENDING[index + 1..].iter().copied());
    order.extend(TIERS_ASCENDING[..index].iter().rev().copied());
    order
}

/// Downward-only ladder used for fail-open attempts: the requested tier first,
/// then strictly lower tiers. Never upscales on a fallback pass.
pub fn quality_fallback_ladder(preferred: u32) -> Vec<u32> {
    let Some(index) = TIERS_ASCENDING.iter().position(|&t| t == preferred) else {
        return vec![QUALITY_MP3_320];
    };
    let mut order = vec![TIERS_ASCENDING[index]];
    order.extend(TIERS_ASCENDING[..index].iter().rev().copied());
    order
}

/// Qobuz reports sampling rates in kHz below 1000 (44.1) and Hz above
/// (44100). The engine works in Hz — normalize at the gateway boundary so
/// adapters never duplicate the conversion.
pub fn normalize_sample_rate(sampling_rate: Option<f64>) -> u32 {
    match sampling_rate {
        Some(sr) if sr < 1000.0 => (sr * 1000.0).round() as u32,
        Some(sr) => sr.round() as u32,
        None => 44_100,
    }
}

#[derive(Debug, Clone)]
pub struct QobuzStreamInfo {
    pub url: String,
    pub format_id: u32,
    pub mime_type: String,
    pub bit_depth: u32,
    pub sample_rate: u32,
}

#[derive(Debug, thiserror::Error)]
pub enum QobuzError {
    #[error("Qobuz message: {0}")]
    Message(String),
    #[error("Qobuz entity not found: {0}")]
    NotFound(String),
    #[error("Qobuz stream unavailable: {0}")]
    Unavailable(String),
    #[error("Qobuz authentication failed: {0}")]
    Auth(String),
    #[error("Qobuz rate limit exceeded: {0}")]
    RateLimit(String),
    #[error("Qobuz network error: {0}")]
    Network(String),
}

pub trait QobuzGateway: Send + Sync {
    fn gateway_name(&self) -> &'static str;

    fn fetch_track_meta<'a>(
        &'a self,
        track_id: &'a str,
    ) -> BoxFuture<'a, Result<TrackMeta, QobuzError>>;

    fn resolve_stream_url<'a>(
        &'a self,
        track_id: &'a str,
        format_id: u32,
        // Fail-open pass: the adapter may return the closest available tier
        // at or below the request instead of only the exact format.
        // Strict (`false`) first, fail-open (`true`) last.
        fallback: bool,
    ) -> BoxFuture<'a, Result<QobuzStreamInfo, QobuzError>>;

    fn fetch_album_tracks<'a>(
        &'a self,
        album_id: &'a str,
    ) -> BoxFuture<'a, Result<AlbumTracks, QobuzError>>;

    fn fetch_artist_tracks<'a>(
        &'a self,
        artist_id: &'a str,
    ) -> BoxFuture<'a, Result<ArtistTracks, QobuzError>>;

    fn fetch_artist_album_ids<'a>(
        &'a self,
        artist_id: &'a str,
    ) -> BoxFuture<'a, Result<Vec<String>, QobuzError>> {
        Box::pin(async move {
            let res = self.fetch_artist_tracks(artist_id).await?;
            let mut album_ids = Vec::new();
            for t in res.tracks {
                if let Some(aid) = t.album_id
                    && !album_ids.contains(&aid)
                {
                    album_ids.push(aid);
                }
            }
            Ok(album_ids)
        })
    }

    fn fetch_playlist_tracks<'a>(
        &'a self,
        playlist_id: &'a str,
    ) -> BoxFuture<'a, Result<PlaylistData, QobuzError>>;
}

impl fmt::Display for QobuzStreamInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "format: {}, {}/{}Hz, mime: {}",
            self.format_id, self.bit_depth, self.sample_rate, self.mime_type
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ladder_prefers_requested_then_upgrades_then_downgrades() {
        assert_eq!(
            quality_ladder(QUALITY_CD_LOSSLESS),
            vec![
                QUALITY_CD_LOSSLESS,
                QUALITY_HI_RES_96,
                QUALITY_MAX_HI_RES,
                QUALITY_MP3_320
            ]
        );
        assert_eq!(
            quality_ladder(QUALITY_HI_RES_96),
            vec![
                QUALITY_HI_RES_96,
                QUALITY_MAX_HI_RES,
                QUALITY_CD_LOSSLESS,
                QUALITY_MP3_320
            ]
        );
        assert_eq!(
            quality_ladder(QUALITY_MAX_HI_RES),
            vec![
                QUALITY_MAX_HI_RES,
                QUALITY_HI_RES_96,
                QUALITY_CD_LOSSLESS,
                QUALITY_MP3_320
            ]
        );
        assert_eq!(
            quality_ladder(QUALITY_MP3_320),
            vec![
                QUALITY_MP3_320,
                QUALITY_CD_LOSSLESS,
                QUALITY_HI_RES_96,
                QUALITY_MAX_HI_RES
            ]
        );
    }

    #[test]
    fn ladder_unknown_format_degrades_top_down() {
        assert_eq!(
            quality_ladder(999),
            vec![
                QUALITY_MAX_HI_RES,
                QUALITY_HI_RES_96,
                QUALITY_CD_LOSSLESS,
                QUALITY_MP3_320
            ]
        );
    }

    #[test]
    fn fallback_ladder_never_upscales() {
        assert_eq!(
            quality_fallback_ladder(QUALITY_CD_LOSSLESS),
            vec![QUALITY_CD_LOSSLESS, QUALITY_MP3_320]
        );
        assert_eq!(
            quality_fallback_ladder(QUALITY_MAX_HI_RES),
            vec![
                QUALITY_MAX_HI_RES,
                QUALITY_HI_RES_96,
                QUALITY_CD_LOSSLESS,
                QUALITY_MP3_320
            ]
        );
    }

    #[test]
    fn sample_rate_handles_khz_and_hz() {
        assert_eq!(normalize_sample_rate(Some(44.1)), 44_100);
        assert_eq!(normalize_sample_rate(Some(96.0)), 96_000);
        assert_eq!(normalize_sample_rate(Some(44100.0)), 44_100);
        assert_eq!(normalize_sample_rate(None), 44_100);
    }
}
