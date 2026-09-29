//! Qobuz catalog queries and collection resolution.

use std::sync::Arc;

use engine::{
    orchestrator::deps::{CollectionResolver, Storefront},
    types::{AlbumTracks, ArtistTracks, Provider},
};
use music::{PlaylistData, TrackMeta};

use crate::gateway::QobuzGateway;

#[derive(Clone)]
pub struct QobuzCatalog {
    primary: Arc<dyn QobuzGateway>,
    fallback: Option<Arc<dyn QobuzGateway>>,
}

impl QobuzCatalog {
    pub fn new(primary: Arc<dyn QobuzGateway>, fallback: Option<Arc<dyn QobuzGateway>>) -> Self {
        Self { primary, fallback }
    }

    pub async fn fetch_track_meta(&self, id: &str) -> Result<TrackMeta, String> {
        match self.primary.fetch_track_meta(id).await {
            Ok(track) => Ok(track),
            Err(err) => {
                if let Some(fallback) = &self.fallback {
                    tracing::warn!("Primary Qobuz catalog failed ({err}), trying fallback");
                    fallback
                        .fetch_track_meta(id)
                        .await
                        .map_err(|e| e.to_string())
                } else {
                    Err(err.to_string())
                }
            }
        }
    }
}

impl CollectionResolver for QobuzCatalog {
    async fn fetch_album_tracks(
        &self,
        _provider: Provider,
        id: &str,
        _storefront: Storefront<'_>,
    ) -> Result<AlbumTracks, String> {
        match self.primary.fetch_album_tracks(id).await {
            Ok(album) => Ok(album),
            Err(err) => {
                if let Some(fallback) = &self.fallback {
                    tracing::warn!("Primary Qobuz catalog failed ({err}), trying fallback");
                    fallback
                        .fetch_album_tracks(id)
                        .await
                        .map_err(|e| e.to_string())
                } else {
                    Err(err.to_string())
                }
            }
        }
    }

    async fn fetch_artist_tracks(
        &self,
        _provider: Provider,
        id: &str,
        _storefront: Storefront<'_>,
    ) -> Result<ArtistTracks, String> {
        match self.primary.fetch_artist_tracks(id).await {
            Ok(artist) => Ok(artist),
            Err(err) => {
                if let Some(fallback) = &self.fallback {
                    tracing::warn!("Primary Qobuz catalog failed ({err}), trying fallback");
                    fallback
                        .fetch_artist_tracks(id)
                        .await
                        .map_err(|e| e.to_string())
                } else {
                    Err(err.to_string())
                }
            }
        }
    }

    async fn fetch_artist_album_ids(
        &self,
        _provider: Provider,
        id: &str,
        _storefront: Storefront<'_>,
    ) -> Result<Vec<String>, String> {
        match self.primary.fetch_artist_album_ids(id).await {
            Ok(album_ids) => Ok(album_ids),
            Err(err) => {
                if let Some(fallback) = &self.fallback {
                    tracing::warn!("Primary Qobuz catalog failed ({err}), trying fallback");
                    fallback
                        .fetch_artist_album_ids(id)
                        .await
                        .map_err(|e| e.to_string())
                } else {
                    Err(err.to_string())
                }
            }
        }
    }

    async fn fetch_playlist_tracks(
        &self,
        _provider: Provider,
        id: &str,
        _storefront: Storefront<'_>,
    ) -> Result<PlaylistData, String> {
        match self.primary.fetch_playlist_tracks(id).await {
            Ok(playlist) => Ok(playlist),
            Err(err) => {
                if let Some(fallback) = &self.fallback {
                    tracing::warn!("Primary Qobuz catalog failed ({err}), trying fallback");
                    fallback
                        .fetch_playlist_tracks(id)
                        .await
                        .map_err(|e| e.to_string())
                } else {
                    Err(err.to_string())
                }
            }
        }
    }
}
