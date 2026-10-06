use std::{sync::Arc, time::Duration};

use moka::future::Cache;
use serde::Serialize;
use utoipa::ToSchema;

use crate::ServerState;

pub const SUBSYSTEM_DATABASE: &str = "database";

pub const SUBSYSTEM_STREAM_WORKERS: &str = "stream_workers";

pub const SUBSYSTEM_CACHE: &str = "cache";

pub const SUBSYSTEM_APPLE_WRAPPER: &str = "apple_wrapper";

const PROBE_TTL: Duration = Duration::from_secs(5);

const DB_PROBE_TIMEOUT: Duration = Duration::from_secs(2);

const HTTP_PROBE_TIMEOUT: Duration = Duration::from_secs(3);

const DEFAULT_APPLE_WRAPPER_URL: &str = "http://127.0.0.1:12340";

const APPLE_WRAPPER_STATUS_PATH: &str = "/status";

const MAX_DETAIL_LEN: usize = 160;

const STATUS_KEY: &str = "status";

const LIVENESS_KEY: &str = "liveness";

static STATUS_CACHE: std::sync::LazyLock<Cache<&'static str, Arc<StatusReport>>> =
    std::sync::LazyLock::new(|| {
        Cache::builder()
            .max_capacity(8)
            .time_to_live(PROBE_TTL)
            .build()
    });

static LIVENESS_CACHE: std::sync::LazyLock<Cache<&'static str, Arc<LivenessReport>>> =
    std::sync::LazyLock::new(|| {
        Cache::builder()
            .max_capacity(8)
            .time_to_live(PROBE_TTL)
            .build()
    });

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum SubsystemState {
    Ok,

    Degraded,

    Unavailable,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct SubsystemReport {
    #[schema(example = "database")]
    pub name: &'static str,

    pub state: SubsystemState,

    #[schema(example = "reachable")]
    pub detail: String,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct StatusReport {
    #[schema(example = "healthy")]
    pub status: &'static str,

    #[schema(example = 3600)]
    pub uptime_seconds: u64,

    pub subsystems: Vec<SubsystemReport>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct LivenessReport {
    pub live: bool,

    pub subsystems: Vec<SubsystemReport>,
}

pub async fn collect_status(state: &ServerState) -> StatusReport {
    let cached = STATUS_CACHE.get_with(STATUS_KEY, probe_all(state)).await;
    (*cached).clone()
}

pub async fn collect_liveness(state: &ServerState) -> LivenessReport {
    let cached = LIVENESS_CACHE
        .get_with(LIVENESS_KEY, probe_liveness_inner(state))
        .await;
    (*cached).clone()
}

async fn probe_all(state: &ServerState) -> Arc<StatusReport> {
    let (database, workers, cache, apple) = tokio::join!(
        probe_database(state),
        async { probe_stream_workers(state) },
        async { probe_cache(state) },
        probe_apple_wrapper(state),
    );
    let subsystems = vec![database, workers, cache, apple];
    let status = if subsystems
        .iter()
        .all(|subsystem| subsystem.state == SubsystemState::Ok)
    {
        "healthy"
    } else {
        "degraded"
    };
    Arc::new(StatusReport {
        status,
        uptime_seconds: state.started_at.elapsed().as_secs(),
        subsystems,
    })
}

async fn probe_liveness_inner(state: &ServerState) -> Arc<LivenessReport> {
    let database = probe_database(state).await;
    let workers = probe_stream_workers(state);

    let live = database.state == SubsystemState::Ok && workers.state != SubsystemState::Unavailable;
    Arc::new(LivenessReport {
        live,
        subsystems: vec![database, workers],
    })
}

async fn probe_database(state: &ServerState) -> SubsystemReport {
    match tokio::time::timeout(DB_PROBE_TIMEOUT, check_database(state)).await {
        Ok(Ok(())) => SubsystemReport {
            name: SUBSYSTEM_DATABASE,
            state: SubsystemState::Ok,
            detail: "reachable".to_owned(),
        },
        Ok(Err(detail)) => SubsystemReport {
            name: SUBSYSTEM_DATABASE,
            state: SubsystemState::Unavailable,
            detail,
        },
        Err(_) => SubsystemReport {
            name: SUBSYSTEM_DATABASE,
            state: SubsystemState::Unavailable,
            detail: format!("no answer within {}s", DB_PROBE_TIMEOUT.as_secs()),
        },
    }
}

async fn check_database(state: &ServerState) -> Result<(), String> {
    state
        .db
        .ping()
        .await
        .map_err(|error| scrub(&error.to_string()))
}

fn probe_stream_workers(state: &ServerState) -> SubsystemReport {
    let pool = state.stream_engine.worker_pool();
    let total = pool.worker_count();
    let available = pool.available_worker_count();
    let (subsystem_state, detail) = if available > 0 {
        (
            SubsystemState::Ok,
            format!("{available} of {total} workers available"),
        )
    } else if total > 0 {
        (
            SubsystemState::Unavailable,
            format!("pool fully drained: 0 of {total} workers available"),
        )
    } else {
        (
            SubsystemState::Degraded,
            "no stream workers configured".to_owned(),
        )
    };
    SubsystemReport {
        name: SUBSYSTEM_STREAM_WORKERS,
        state: subsystem_state,
        detail,
    }
}

fn probe_cache(state: &ServerState) -> SubsystemReport {
    let cache = state.stream_engine.cache();
    SubsystemReport {
        name: SUBSYSTEM_CACHE,
        state: SubsystemState::Ok,
        detail: format!(
            "{} entries, {} bytes resident",
            cache.entry_count(),
            cache.weighted_size()
        ),
    }
}

async fn probe_apple_wrapper(state: &ServerState) -> SubsystemReport {
    let Some(raw_url) = apple_wrapper_url() else {
        return SubsystemReport {
            name: SUBSYSTEM_APPLE_WRAPPER,
            state: SubsystemState::Degraded,
            detail: "not configured (ALAC_WRAPPER_URL is blank)".to_owned(),
        };
    };
    let endpoint = format!(
        "{}{APPLE_WRAPPER_STATUS_PATH}",
        raw_url.trim().trim_end_matches('/')
    );
    probe_endpoint(
        state,
        SUBSYSTEM_APPLE_WRAPPER,
        &endpoint,
        "ALAC_WRAPPER_URL",
    )
    .await
}

fn apple_wrapper_url() -> Option<String> {
    match std::env::var("ALAC_WRAPPER_URL") {
        Ok(value) if value.trim().is_empty() => None,
        Ok(value) => Some(value),
        Err(_) => Some(DEFAULT_APPLE_WRAPPER_URL.to_owned()),
    }
}

async fn probe_endpoint(
    state: &ServerState,
    name: &'static str,
    endpoint: &str,
    env_var: &str,
) -> SubsystemReport {
    let label = endpoint_label(endpoint, env_var);
    let response = state
        .http_client
        .get(endpoint)
        .timeout(HTTP_PROBE_TIMEOUT)
        .send()
        .await;
    let (subsystem_state, detail) = match response {
        Ok(response) => {
            let code = response.status().as_u16();
            let state = if response.status().is_success() {
                SubsystemState::Ok
            } else {
                SubsystemState::Degraded
            };
            (state, format!("{label} answered HTTP {code}"))
        }
        Err(error) => {
            tracing::debug!(subsystem = name, endpoint = %label, %error, "health probe failed");
            (
                SubsystemState::Unavailable,
                format!("{label} unreachable ({})", error_kind(&error)),
            )
        }
    };
    SubsystemReport {
        name,
        state: subsystem_state,
        detail,
    }
}

fn error_kind(error: &reqwest::Error) -> &'static str {
    if error.is_timeout() {
        "timeout"
    } else if error.is_connect() {
        "connection refused"
    } else if error.is_redirect() {
        "redirect loop"
    } else if error.is_body() || error.is_decode() {
        "malformed response"
    } else {
        "request failed"
    }
}

fn endpoint_label(endpoint: &str, env_var: &str) -> String {
    match reqwest::Url::parse(endpoint) {
        Ok(parsed) => {
            let host = parsed.host_str().unwrap_or("unknown");
            match parsed.port_or_known_default() {
                Some(port) => format!("{host}:{port}"),
                None => host.to_owned(),
            }
        }
        Err(_) => format!("{env_var} endpoint"),
    }
}

fn scrub(input: &str) -> String {
    let flattened = input.split_whitespace().collect::<Vec<_>>().join(" ");
    let mut out = String::with_capacity(flattened.len());
    let mut rest = flattened.as_str();
    while let Some(scheme_end) = rest.find("://") {
        out.push_str(&rest[..scheme_end]);
        out.push_str("://");
        rest = &rest[scheme_end + 3..];

        let authority_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
        let authority = &rest[..authority_end];
        rest = &rest[authority_end..];

        out.push_str(authority.rsplit('@').next().unwrap_or(authority));

        let url_end = rest.find(' ').unwrap_or(rest.len());
        rest = &rest[url_end..];
    }
    out.push_str(rest);
    if out.chars().count() > MAX_DETAIL_LEN {
        let truncated: String = out.chars().take(MAX_DETAIL_LEN).collect();
        return format!("{truncated}...");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scrub_strips_credentials_paths_and_queries() {
        assert_eq!(
            scrub(
                "failed to connect to postgres://user:hunter2@db.internal:5432/music?sslmode=require"
            ),
            "failed to connect to postgres://db.internal:5432"
        );
        assert_eq!(
            scrub("https://user:pw@example.com/v1/status?token=abcd"),
            "https://example.com"
        );
    }

    #[test]
    fn scrub_keeps_plain_text_and_truncates() {
        assert_eq!(scrub("connection refused"), "connection refused");
        let scrubbed = scrub(&"x".repeat(MAX_DETAIL_LEN + 50));
        assert_eq!(scrubbed.chars().count(), MAX_DETAIL_LEN + 3);
        assert!(scrubbed.ends_with("..."));
    }

    #[test]
    fn scrub_flattens_newlines() {
        assert_eq!(scrub("line one\n  line two"), "line one line two");
    }

    #[test]
    fn scrub_strips_a_url_embedded_in_surrounding_text() {
        assert_eq!(
            scrub("wrapped http://127.0.0.1:12340/status then more text"),
            "wrapped http://127.0.0.1:12340 then more text"
        );
    }

    #[test]
    fn endpoint_label_reports_host_and_port_only() {
        assert_eq!(
            endpoint_label(
                "https://api.example.com/v1/track?key=secret",
                "ALAC_WRAPPER_URL"
            ),
            "api.example.com:443"
        );
        assert_eq!(
            endpoint_label("http://127.0.0.1:12340/status", "ALAC_WRAPPER_URL"),
            "127.0.0.1:12340"
        );
        assert_eq!(
            endpoint_label("not a url", "ALAC_WRAPPER_URL"),
            "ALAC_WRAPPER_URL endpoint"
        );
    }
}
