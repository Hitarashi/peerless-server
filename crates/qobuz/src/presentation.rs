//! Presentation metadata and formatting for Qobuz.

use engine::orchestrator::deps::{ProviderPresentation, Storefront};

#[derive(Debug, Clone, Copy, Default)]
pub struct QobuzPresentation;

impl ProviderPresentation for QobuzPresentation {
    fn default_job_header(&self) -> &str {
        "Qobuz Lossless Rip"
    }

    fn album_url(
        &self,
        _provider: engine::types::Provider,
        album_id: &str,
        _storefront: Storefront<'_>,
    ) -> Option<String> {
        (!album_id.is_empty()).then(|| format!("https://open.qobuz.com/album/{album_id}"))
    }

    fn unavailable_track_message(&self) -> &str {
        "Unavailable on Qobuz (not streamable or georestricted)"
    }

    fn unavailable_track_log_message(&self) -> &str {
        "Track is not streamable in Qobuz catalog, skipping rip"
    }
}
