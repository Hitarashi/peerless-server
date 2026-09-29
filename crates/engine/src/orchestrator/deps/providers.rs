use std::future::Future;

use music::PlaylistData;

use crate::{
    ripper::{RipError, RipOptions},
    types::{AlbumTracks, ArtistTracks, Provider, TrackRipResult},
};

/// A storefront is a **market region** ("in", "us", "gb"), never a provider.
///
/// Which adapter runs is decided by [`Provider`]; a region only tells a
/// *regional* catalog which market to address. Adapters with no notion of a
/// market (Qobuz) ignore it entirely. `None`/empty means "use the adapter's
/// own configured default", so a caller never has to invent a region.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Storefront<'a>(Option<&'a str>);

impl<'a> Storefront<'a> {
    pub const fn new(value: Option<&'a str>) -> Self {
        Self(value)
    }

    /// The requested region, or `None` when the adapter's own default applies.
    pub fn get(self) -> Option<&'a str> {
        self.0.filter(|region| !region.is_empty())
    }
}

impl<'a> From<&'a str> for Storefront<'a> {
    fn from(value: &'a str) -> Self {
        Self(Some(value))
    }
}

/// Resolve collections from one specific provider. `provider` is the routing
/// key; `storefront` is only the market the resolved catalog is addressed in.
pub trait CollectionResolver: Send + Sync {
    fn fetch_album_tracks(
        &self,
        provider: Provider,
        id: &str,
        storefront: Storefront<'_>,
    ) -> impl Future<Output = Result<AlbumTracks, String>> + Send;

    fn fetch_artist_tracks(
        &self,
        provider: Provider,
        id: &str,
        storefront: Storefront<'_>,
    ) -> impl Future<Output = Result<ArtistTracks, String>> + Send;

    fn fetch_artist_album_ids(
        &self,
        provider: Provider,
        id: &str,
        storefront: Storefront<'_>,
    ) -> impl Future<Output = Result<Vec<String>, String>> + Send {
        async move {
            let res = self.fetch_artist_tracks(provider, id, storefront).await?;
            let mut album_ids = Vec::new();
            for t in res.tracks {
                if let Some(aid) = t.album_id
                    && !album_ids.contains(&aid)
                {
                    album_ids.push(aid);
                }
            }
            Ok(album_ids)
        }
    }

    fn fetch_playlist_tracks(
        &self,
        provider: Provider,
        id: &str,
        storefront: Storefront<'_>,
    ) -> impl Future<Output = Result<PlaylistData, String>> + Send;
}

pub trait TrackAcquisition: Send + Sync {
    fn rip(
        &self,
        track_id: &str,
        options: RipOptions<'_>,
    ) -> impl Future<Output = Result<TrackRipResult, RipError>> + Send;
}

pub trait ArtworkProvider: Send + Sync {
    fn fetch_artwork(&self, url: &str) -> impl Future<Output = Option<Vec<u8>>> + Send;
    fn artwork_url_at_size(&self, provider: Provider, url: &str, size: u16) -> String;
}

pub trait ProviderPresentation: Send + Sync {
    fn default_job_header(&self) -> &str;
    fn album_url(
        &self,
        provider: Provider,
        album_id: &str,
        storefront: Storefront<'_>,
    ) -> Option<String>;
    fn unavailable_track_message(&self) -> &str;
    fn unavailable_track_log_message(&self) -> &str;
}

/// Every provider capability the orchestrator needs.
///
/// The four leaf traits above are the real seams: the production registry
/// implements all four and tests wire fakes. This trait adds only the one
/// question the orchestrator asks before starting a job — is this provider
/// usable at all? — which previously lived behind a `provider()` accessor
/// that every adapter had to keep consistent with `supports_provider`.
pub trait ProviderDeps:
    CollectionResolver
    + TrackAcquisition
    + ArtworkProvider
    + ProviderPresentation
    + Send
    + Sync
    + 'static
{
    fn supports_provider(&self, provider: Provider) -> bool;
}
