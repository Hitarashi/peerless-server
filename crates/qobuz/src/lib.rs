//! Deep Qobuz integration crate: catalog, hosted worker adapter, native ripper, and stream acquisition.

pub mod acquisition;
pub mod adapters;
pub mod catalog;
pub mod gateway;
pub mod parser;
pub mod presentation;
pub mod types;

use std::sync::Arc;

pub use acquisition::QobuzAcquisition;
pub use adapters::{
    hosted::HostedWorkerAdapter,
    native::{NativeConfig, NativeRipperAdapter},
};
pub use catalog::QobuzCatalog;
pub use gateway::{QobuzError, QobuzGateway, QobuzStreamInfo};
pub use parser::{QobuzEntity, QobuzKind, parse_qobuz_url};
pub use presentation::QobuzPresentation;

#[derive(Clone)]
pub struct QobuzProduction {
    catalog: QobuzCatalog,
    acquisition: QobuzAcquisition,
    presentation: QobuzPresentation,
}

impl QobuzProduction {
    pub fn new(
        primary_url: String,
        primary_key: Option<String>,
        native_config: NativeConfig,
    ) -> Self {
        let primary = Arc::new(HostedWorkerAdapter::new(primary_url.clone(), primary_key));
        let fallback: Option<Arc<dyn QobuzGateway>> =
            Some(Arc::new(NativeRipperAdapter::new(native_config)));

        let catalog = QobuzCatalog::new(primary.clone(), fallback.clone());
        let acquisition = QobuzAcquisition::new(primary, fallback, primary_url);
        let presentation = QobuzPresentation;

        Self {
            catalog,
            acquisition,
            presentation,
        }
    }

    pub fn from_env() -> Option<Self> {
        let backend_url = std::env::var("QOBUZ_BACKEND_URL")
            .ok()
            .filter(|u| !u.trim().is_empty());
        let backend_key = std::env::var("QOBUZ_BACKEND_KEY")
            .ok()
            .filter(|k| !k.trim().is_empty());
        let native_config = NativeConfig::from_environment();

        let has_backend = backend_url.is_some();
        let has_native = native_config.user_auth_token.is_some() || native_config.app_id.is_some();

        if has_backend {
            let url = backend_url.unwrap();
            Some(Self::new(url, backend_key, native_config))
        } else if has_native {
            let primary: Arc<dyn QobuzGateway> = Arc::new(NativeRipperAdapter::new(native_config));
            let catalog = QobuzCatalog::new(primary.clone(), None);
            let acquisition =
                QobuzAcquisition::new(primary, None, "https://www.qobuz.com".to_string());
            let presentation = QobuzPresentation;
            Some(Self {
                catalog,
                acquisition,
                presentation,
            })
        } else {
            None
        }
    }

    pub fn catalog(&self) -> &QobuzCatalog {
        &self.catalog
    }

    pub fn acquisition(&self) -> &QobuzAcquisition {
        &self.acquisition
    }

    pub fn presentation(&self) -> &QobuzPresentation {
        &self.presentation
    }

    pub fn ripper_deps(&self) -> QobuzAcquisition {
        self.acquisition.clone()
    }
}
