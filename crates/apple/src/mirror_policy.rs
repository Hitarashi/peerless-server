//! Mirror discovery, health verification, caching, and circuit breaking.

use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use serde::Deserialize;
use tokio_util::sync::CancellationToken;

use crate::mirror_http::{CHROME_USER_AGENT, MirrorHttp, MirrorHttpError};

/// The decoded manifest URL used by the TypeScript implementation.
pub const MANIFEST_URL: &str =
    "https://gist.githubusercontent.com/ManOfInfinity/ec6db79f031d58640c84b225c4c78cab/raw";

pub const USER_AGENT: &str = CHROME_USER_AGENT;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MirrorEndpoint {
    pub mirror_url: String,
    pub api_key: String,
}

#[derive(Debug, thiserror::Error)]
pub enum MirrorError {
    #[error("{0}")]
    Message(String),
    #[error("{0}")]
    Json(#[from] serde_json::Error),
}

/// The synchronous part of mirror policy used by stream failover.
pub trait MirrorPolicy: Send + Sync {
    fn record_failure(&self, error: &str);
    fn record_success(&self);
}

struct PolicyState {
    cached: Option<CachedEndpoint>,
    last_failure: Option<Failure>,
}

struct CachedEndpoint {
    endpoint: MirrorEndpoint,
    expires_at: Instant,
}

struct Failure {
    time: Instant,
    error: String,
}

/// Mirror policy over an injectable text HTTP adapter.
pub struct MirrorPolicyManager<H: MirrorHttp> {
    http: H,
    env_override: Option<(String, String)>,
    failure_cooldown: Duration,
    cache_ttl: Duration,
    health_timeout: Duration,
    /// Cloned managers share circuit/cache state: a health probe observes
    /// the same endpoint the ripper resolves (and vice versa).
    state: Arc<Mutex<PolicyState>>,
}

impl<H: MirrorHttp> MirrorPolicyManager<H> {
    /// Clone that shares circuit/cache state with the original. Config and
    /// the HTTP adapter are copied; they are immutable after construction.
    pub fn shared(&self) -> Self
    where
        H: Clone,
    {
        Self {
            http: self.http.clone(),
            env_override: self.env_override.clone(),
            failure_cooldown: self.failure_cooldown,
            cache_ttl: self.cache_ttl,
            health_timeout: self.health_timeout,
            state: Arc::clone(&self.state),
        }
    }

    pub fn new(http: H, env_override: Option<(String, String)>) -> Self {
        Self::with_config(
            http,
            env_override,
            Duration::from_secs(30),
            Duration::from_secs(60 * 60),
            Duration::from_secs(8),
        )
    }

    pub fn with_config(
        http: H,
        env_override: Option<(String, String)>,
        failure_cooldown: Duration,
        cache_ttl: Duration,
        health_timeout: Duration,
    ) -> Self {
        Self {
            http,
            env_override,
            failure_cooldown,
            cache_ttl,
            health_timeout,
            state: Arc::new(Mutex::new(PolicyState {
                cached: None,
                last_failure: None,
            })),
        }
    }

    /// The adapter is exposed for offline tests and diagnostics.
    pub fn http(&self) -> &H {
        &self.http
    }

    pub fn clear_cache(&self) {
        let mut state = self.state.lock().expect("mirror policy mutex poisoned");
        state.cached = None;
        state.last_failure = None;
    }

    pub fn is_circuit_open(&self) -> bool {
        let state = self.state.lock().expect("mirror policy mutex poisoned");
        state
            .last_failure
            .as_ref()
            .is_some_and(|failure| failure.time.elapsed() < self.failure_cooldown)
    }

    pub fn record_failure(&self, error: &str) {
        let mut state = self.state.lock().expect("mirror policy mutex poisoned");
        state.last_failure = Some(Failure {
            time: Instant::now(),
            error: error.to_owned(),
        });
    }

    pub fn record_success(&self) {
        let mut state = self.state.lock().expect("mirror policy mutex poisoned");
        state.last_failure = None;
    }

    pub async fn get_endpoint(
        &self,
        force_refresh: bool,
        signal: Option<CancellationToken>,
    ) -> Result<MirrorEndpoint, MirrorError> {
        // An empty mirror URL or key means "not configured".
        if let Some((mirror_url, api_key)) = &self.env_override
            && !mirror_url.is_empty()
            && !api_key.is_empty()
        {
            return Ok(MirrorEndpoint {
                mirror_url: mirror_url.trim_end_matches('/').to_owned(),
                api_key: api_key.clone(),
            });
        }

        if !force_refresh {
            let state = self.state.lock().expect("mirror policy mutex poisoned");
            if let Some(failure) = &state.last_failure
                && failure.time.elapsed() < self.failure_cooldown
            {
                return Err(MirrorError::Message(failure.error.clone()));
            }
            if let Some(cached) = &state.cached
                && cached.expires_at > Instant::now()
            {
                return Ok(cached.endpoint.clone());
            }
        }

        let started = Instant::now();
        let manifest_result = self
            .http
            .get(
                MANIFEST_URL,
                &[("User-Agent", USER_AGENT.to_owned())],
                self.health_timeout,
                signal.as_ref(),
            )
            .await;
        let manifest_body = match manifest_result {
            Ok(body) => body,
            Err(MirrorHttpError::Status(status)) => {
                let message = format!("Failed to fetch mirror manifest (HTTP {status})");
                self.record_failure(&message);
                return Err(MirrorError::Message(message));
            }
            Err(error) => {
                let message = format!(
                    "Mirror manifest lookup timed out after {}ms: {}",
                    adapter_elapsed(&error, started),
                    adapter_message(&error)
                );
                self.record_failure(&message);
                return Err(MirrorError::Message(message));
            }
        };

        // Deliberately outside the request error mapping: malformed manifest
        // JSON propagates and, as in TS, does not open the circuit.
        let manifest: Manifest = serde_json::from_str(&manifest_body)?;
        let mirror = manifest
            .source
            .as_ref()
            .and_then(|source| source.apple.as_deref())
            .filter(|value| !value.is_empty())
            .or_else(|| {
                manifest
                    .mirrors
                    .as_ref()
                    .and_then(|mirrors| mirrors.apple.as_deref())
                    .filter(|value| !value.is_empty())
            })
            .unwrap_or("")
            .trim_end_matches('/')
            .to_owned();
        let api_key = manifest
            .key
            .as_deref()
            .filter(|value| !value.is_empty())
            .or(manifest.api_key.as_deref())
            .unwrap_or("")
            .trim()
            .to_owned();
        if mirror.is_empty() || api_key.is_empty() {
            let message = "Mirror manifest returned empty apple endpoint or api key".to_owned();
            self.record_failure(&message);
            return Err(MirrorError::Message(message));
        }

        let status_started = Instant::now();
        let status_result = self
            .http
            .get(
                &format!("{mirror}/status"),
                &[
                    ("User-Agent", USER_AGENT.to_owned()),
                    ("X-API-Key", api_key.clone()),
                ],
                self.health_timeout,
                signal.as_ref(),
            )
            .await;
        let status_body = match status_result {
            Ok(body) => body,
            Err(MirrorHttpError::Status(status)) => {
                let message = format!("Mirror /status check failed (HTTP {status})");
                self.record_failure(&message);
                return Err(MirrorError::Message(message));
            }
            Err(error) => {
                let message = format!(
                    "Mirror /status check timed out after {}ms: {}",
                    adapter_elapsed(&error, status_started),
                    adapter_message(&error)
                );
                self.record_failure(&message);
                return Err(MirrorError::Message(message));
            }
        };

        // /status has the opposite JSON behavior from the manifest: malformed
        // JSON is treated exactly like an empty object.
        let status: Status = serde_json::from_str(&status_body).unwrap_or_default();
        if status.wrapper_lossless_available == Some(false)
            || status.wrapper_instances.as_ref().is_some_and(Vec::is_empty)
        {
            let message = "Lossless wrapper is currently offline on mirror".to_owned();
            self.record_failure(&message);
            return Err(MirrorError::Message(message));
        }

        let endpoint = MirrorEndpoint {
            mirror_url: mirror,
            api_key,
        };
        let mut state = self.state.lock().expect("mirror policy mutex poisoned");
        state.cached = Some(CachedEndpoint {
            endpoint: endpoint.clone(),
            expires_at: Instant::now() + self.cache_ttl,
        });
        state.last_failure = None;
        Ok(endpoint)
    }
}

impl<H: MirrorHttp> MirrorPolicy for MirrorPolicyManager<H> {
    fn record_failure(&self, error: &str) {
        self.record_failure(error);
    }

    fn record_success(&self) {
        self.record_success();
    }
}

#[derive(Debug, Deserialize, Default)]
struct Manifest {
    source: Option<ManifestSource>,
    mirrors: Option<ManifestSource>,
    key: Option<String>,
    api_key: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
struct ManifestSource {
    apple: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
struct Status {
    wrapper_lossless_available: Option<bool>,
    wrapper_instances: Option<Vec<serde_json::Value>>,
}

fn adapter_elapsed(error: &MirrorHttpError, started: Instant) -> u64 {
    match error {
        MirrorHttpError::Timeout { elapsed_ms } => *elapsed_ms,
        _ => started.elapsed().as_millis() as u64,
    }
}

fn adapter_message(error: &MirrorHttpError) -> String {
    match error {
        MirrorHttpError::Timeout { .. } => "request timed out".to_owned(),
        MirrorHttpError::Cancelled => "request cancelled".to_owned(),
        MirrorHttpError::Network(message) => message.clone(),
        MirrorHttpError::Status(status) => format!("HTTP {status}"),
    }
}
