//! Statically compiled provider registry for the bot.

use apple::{ApplePresentation, AppleProduction};
use engine::{
    orchestrator::deps::{
        ArtworkProvider, CollectionResolver, ProviderDeps, ProviderPresentation, Storefront,
        TrackAcquisition,
    },
    ripper::{AlacTrackRipper, RipError, RipperConfig},
    types::{AlbumTracks, ArtistTracks, Provider, TrackRipResult},
};
use music::PlaylistData;

/// The bot's provider registry is deliberately closed and compiled in. A job
/// names the provider it wants; this is the only place that maps a
/// [`Provider`] onto an adapter, and the match is exhaustive, so a new
/// provider cannot be silently routed to the wrong catalog.
pub struct ProviderRegistry {
    apple: AppleProduction,
    qobuz: Option<qobuz::QobuzProduction>,
    ripper: AlacTrackRipper,
    /// Market used when a caller did not name one. A region, never a provider.
    default_storefront: String,
}

impl ProviderRegistry {
    pub fn new(
        apple: AppleProduction,
        qobuz: Option<qobuz::QobuzProduction>,
        ripper_config: RipperConfig,
        default_storefront: impl Into<String>,
    ) -> Self {
        Self {
            apple,
            qobuz,
            ripper: AlacTrackRipper::new(ripper_config),
            default_storefront: default_storefront.into(),
        }
    }

    pub fn catalog(&self) -> &apple::Catalog<apple::ReqwestTransport> {
        self.apple.catalog()
    }

    pub fn playlist(&self) -> &apple::PlaylistClient<apple::ReqwestPlaylistHttp> {
        self.apple.playlist()
    }

    pub fn qobuz(&self) -> Option<&qobuz::QobuzProduction> {
        self.qobuz.as_ref()
    }

    /// Qobuz is optional at build time, so "configured" is the only thing
    /// `supports_provider` can meaningfully answer for it.
    fn qobuz_catalog(&self) -> Result<&qobuz::QobuzCatalog, &'static str> {
        self.qobuz
            .as_ref()
            .map(qobuz::QobuzProduction::catalog)
            .ok_or("Qobuz provider is not configured")
    }

    /// Resolve the market for one catalog call. A caller that named a region
    /// wins; everyone else gets the configured default.
    fn storefront<'a>(&'a self, requested: Storefront<'a>) -> &'a str {
        requested.get().unwrap_or(self.default_storefront.as_str())
    }
}

impl CollectionResolver for ProviderRegistry {
    async fn fetch_album_tracks(
        &self,
        provider: Provider,
        id: &str,
        storefront: Storefront<'_>,
    ) -> Result<AlbumTracks, String> {
        match provider {
            Provider::Apple => self
                .catalog()
                .fetch_album_tracks(id, self.storefront(storefront))
                .await
                .map_err(|error| error.to_string()),
            Provider::Qobuz => {
                self.qobuz_catalog()?
                    .fetch_album_tracks(provider, id, storefront)
                    .await
            }
        }
    }

    async fn fetch_artist_tracks(
        &self,
        provider: Provider,
        id: &str,
        storefront: Storefront<'_>,
    ) -> Result<ArtistTracks, String> {
        match provider {
            Provider::Apple => self
                .catalog()
                .fetch_artist_tracks(id, self.storefront(storefront))
                .await
                .map_err(|error| error.to_string()),
            Provider::Qobuz => {
                self.qobuz_catalog()?
                    .fetch_artist_tracks(provider, id, storefront)
                    .await
            }
        }
    }

    async fn fetch_artist_album_ids(
        &self,
        provider: Provider,
        id: &str,
        storefront: Storefront<'_>,
    ) -> Result<Vec<String>, String> {
        match provider {
            Provider::Apple => self
                .catalog()
                .fetch_artist_album_ids(id, self.storefront(storefront))
                .await
                .map_err(|error| error.to_string()),
            Provider::Qobuz => {
                self.qobuz_catalog()?
                    .fetch_artist_album_ids(provider, id, storefront)
                    .await
            }
        }
    }

    async fn fetch_playlist_tracks(
        &self,
        provider: Provider,
        id: &str,
        storefront: Storefront<'_>,
    ) -> Result<PlaylistData, String> {
        match provider {
            Provider::Apple => self
                .playlist()
                .fetch_playlist_tracks(id, self.storefront(storefront))
                .await
                .map_err(|error| error.to_string()),
            Provider::Qobuz => {
                self.qobuz_catalog()?
                    .fetch_playlist_tracks(provider, id, storefront)
                    .await
            }
        }
    }
}

impl TrackAcquisition for ProviderRegistry {
    async fn rip(
        &self,
        track_id: &str,
        options: engine::ripper::RipOptions<'_>,
    ) -> Result<TrackRipResult, RipError> {
        match options.provider {
            Provider::Apple => {
                self.ripper
                    .rip(self.apple.ripper_deps(), track_id, options)
                    .await
            }
            Provider::Qobuz => {
                if let Some(qobuz) = &self.qobuz {
                    self.ripper
                        .rip(qobuz.acquisition(), track_id, options)
                        .await
                } else {
                    Err(RipError::TrackUnavailable {
                        reason: "Qobuz provider is not configured".to_string(),
                    })
                }
            }
        }
    }
}

impl ArtworkProvider for ProviderRegistry {
    async fn fetch_artwork(&self, url: &str) -> Option<Vec<u8>> {
        engine::ripper::fetch_artwork_bytes(self.ripper.config(), url).await
    }

    fn artwork_url_at_size(&self, provider: Provider, url: &str, size: u16) -> String {
        match provider {
            // Qobuz serves a fixed-size CDN image; there is no Apple-style
            // size parameter to rewrite.
            Provider::Apple => apple::catalog::artwork_url_at_size(url, size),
            Provider::Qobuz => url.to_string(),
        }
    }
}

impl ProviderPresentation for ProviderRegistry {
    fn default_job_header(&self) -> &str {
        "ALAC Lossless Rip"
    }

    fn album_url(
        &self,
        provider: Provider,
        album_id: &str,
        storefront: Storefront<'_>,
    ) -> Option<String> {
        match provider {
            Provider::Apple => {
                ApplePresentation.album_url(provider, album_id, self.storefront(storefront).into())
            }
            // Qobuz album links carry no market, so the region is dropped.
            Provider::Qobuz => qobuz::QobuzPresentation.album_url(provider, album_id, storefront),
        }
    }

    fn unavailable_track_message(&self) -> &str {
        "Unavailable on music provider (not streamable or georestricted)"
    }

    fn unavailable_track_log_message(&self) -> &str {
        "Track is not streamable in provider catalog, skipping rip"
    }
}

impl ProviderDeps for ProviderRegistry {
    fn supports_provider(&self, provider: Provider) -> bool {
        match provider {
            Provider::Apple => true,
            Provider::Qobuz => self.qobuz.is_some(),
        }
    }
}
