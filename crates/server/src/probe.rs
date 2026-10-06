//! Subsystem probing shared by the HTTP liveness and diagnostics endpoints.
//!
//! The two concerns are deliberately separate:
//!
//! * **Liveness** ([`collect_liveness`]) answers only "should this process be
//!   restarted?". It probes the database and the stream worker pool -- both
//!   cheap and in-process. The Apple wrapper is
//!   deliberately *excluded*: a dead wrapper breaks new rips, but restarting
//!   the container does not fix it, and it would tear down streaming,
//!   catalog, lyrics, and already-ripped playback that are still working.
//! * **Diagnostics** ([`collect_status`]) reports every subsystem with an
//!   explicit state and always answers 200, so it still works when everything
//!   is broken.
//!
//! Both results are cached for [`PROBE_TTL`], so a burst of polls never fans
//! out into an outbound HTTP request per call. Liveness additionally bounds
//! the database probe with [`DB_PROBE_TIMEOUT`], so a hung PostgreSQL cannot
//! stall the Docker liveness probe.

use std::{sync::Arc, time::Duration};

use moka::future::Cache;
use serde::Serialize;
use utoipa::ToSchema;

use crate::ServerState;

/// Subsystem identifier: the PostgreSQL pool backing every request.
pub const SUBSYSTEM_DATABASE: &str = "database";
/// Subsystem identifier: the MTProto worker pool backing `/api/v1/tracks/{id}/stream`.
pub const SUBSYSTEM_STREAM_WORKERS: &str = "stream_workers";
/// Subsystem identifier: the in-memory media chunk cache.
pub const SUBSYSTEM_CACHE: &str = "cache";
/// Subsystem identifier: the Apple ALAC wrapper used for new rips.
pub const SUBSYSTEM_APPLE_WRAPPER: &str = "apple_wrapper";
/// How long a collected probe result is reused before the next poll re-probes.
const PROBE_TTL: Duration = Duration::from_secs(5);

/// Upper bound on the `SELECT 1` liveness query. Generous for a healthy
/// database, and far below the 5s the Docker liveness probe waits for a
/// response.
const DB_PROBE_TIMEOUT: Duration = Duration::from_secs(2);

/// Upper bound on an outbound HTTP probe of the Apple wrapper. Only ever
/// reached from `/api/v1/status`, never from liveness.
const HTTP_PROBE_TIMEOUT: Duration = Duration::from_secs(3);

/// Loopback address the Apple acquisition path falls back to when
/// `ALAC_WRAPPER_URL` is absent. Mirrors `AppleProductionConfig::from_environment`.
const DEFAULT_APPLE_WRAPPER_URL: &str = "http://127.0.0.1:12340";

/// Cheap wrapper-lite endpoint that answers without doing any ripping work.
const APPLE_WRAPPER_STATUS_PATH: &str = "/status";

/// Longest `detail` string published in a report, after scrubbing.
const MAX_DETAIL_LEN: usize = 160;

/// Cache key for the wide diagnostic report.
const STATUS_KEY: &str = "status";

/// Cache key for the narrow liveness report.
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

/// Verdict for a single subsystem.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum SubsystemState {
    /// The subsystem answered and is fully operational.
    Ok,
    /// The subsystem is reachable but not fully usable, or it is not
    /// configured. Not a fault, and never a reason to restart the process.
    Degraded,
    /// The subsystem is required by liveness and is not serving.
    Unavailable,
}

/// State of one probed subsystem.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct SubsystemReport {
    /// Stable subsystem identifier, e.g. `database` or `apple_wrapper`.
    #[schema(example = "database")]
    pub name: &'static str,
    /// Probe outcome for this subsystem.
    pub state: SubsystemState,
    /// Human-readable explanation. Scrubbed: never contains credentials,
    /// query strings, or a full URL.
    #[schema(example = "reachable")]
    pub detail: String,
}

/// Wide diagnostic view of every subsystem. Served by `/api/v1/status`.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct StatusReport {
    /// `healthy` only when every probed subsystem is `ok`.
    #[schema(example = "healthy")]
    pub status: &'static str,
    /// Seconds since process start.
    #[schema(example = 3600)]
    pub uptime_seconds: u64,
    /// Per-subsystem breakdown, one entry per subsystem this build probes.
    pub subsystems: Vec<SubsystemReport>,
}

/// Narrow liveness view. Served by `/api/v1/health` to decide 200 vs 503.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct LivenessReport {
    /// True when the process, the database, and the stream worker pool are
    /// all serving. This -- and only this -- decides the liveness status code.
    pub live: bool,
    /// The two subsystems liveness depends on. The Apple wrapper is excluded
    /// because a wrapper outage does not stop existing streams.
    pub subsystems: Vec<SubsystemReport>,
}

/// Probe every subsystem and report an overall verdict.
///
/// Backs `GET /api/v1/status`, which always answers 200: this is the endpoint
/// an operator reads precisely *because* something is wrong.
///
/// The four probes run concurrently, so a cold cache costs the slowest single
/// probe rather than their sum. Results are cached for [`PROBE_TTL`].
pub async fn collect_status(state: &ServerState) -> StatusReport {
    let cached = STATUS_CACHE.get_with(STATUS_KEY, probe_all(state)).await;
    (*cached).clone()
}

/// Probe only what liveness depends on: the database and the stream workers.
///
/// Backs `GET /api/v1/health`. Neither probe leaves the process, and both are
/// cached for [`PROBE_TTL`], so this path never issues an outbound HTTP
/// request.
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

    // Liveness is process + database + stream workers, nothing else. A
    // *drained* worker pool is fatal because the process cannot stream; an
    // unconfigured one is only Degraded, because that is a legitimate
    // configuration rather than a fault.
    let live = database.state == SubsystemState::Ok && workers.state != SubsystemState::Unavailable;
    Arc::new(LivenessReport {
        live,
        subsystems: vec![database, workers],
    })
}

/// Cheapest possible database check: a single `SELECT 1` over a pooled
/// connection, bounded by [`DB_PROBE_TIMEOUT`].
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
    // The `db` crate owns all Diesel usage, so the liveness query is
    // `DbPool::ping` rather than a raw `sql_query` here. `ping` has no timeout
    // of its own; the caller bounds it.
    state
        .db
        .ping()
        .await
        .map_err(|error| scrub(&error.to_string()))
}

/// A pool with workers configured but none left to serve is `Unavailable`: the
/// process cannot stream, and restarting is the operator's call. A pool with
/// no workers configured at all is `Degraded` -- catalog-only and
/// diagnostics-only deployments legitimately run that way.
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

/// Purely informational: the cache is in-process, so it cannot be down.
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

/// Probe the configured Apple wrapper. Never affects liveness: a dead wrapper
/// only stops *new* rips, and the process is still serving everything else.
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

/// Resolve the Apple wrapper address the same way `AppleProductionConfig`
/// does, so the probe follows the real acquisition path.
fn apple_wrapper_url() -> Option<String> {
    match std::env::var("ALAC_WRAPPER_URL") {
        // A blank value is an explicit opt-out: report it as not configured
        // rather than probing an address the operator never enabled.
        Ok(value) if value.trim().is_empty() => None,
        Ok(value) => Some(value),
        Err(_) => Some(DEFAULT_APPLE_WRAPPER_URL.to_owned()),
    }
}

/// Issue one cheap, short-timeout request and classify the answer.
///
/// Any HTTP response at all proves the endpoint is reachable, so a 4xx/5xx is
/// `Degraded` rather than `Unavailable`: only a transport-level failure (DNS,
/// refused connection, timeout) means the backend is not serving.
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

/// Classify a transport error without echoing the URL, the hostname, or any
/// credential back into an operator-visible body.
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

/// Render an endpoint as `host:port` for display. Never includes userinfo,
/// the path, or the query string.
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

/// Reduce a diagnostic string to something safe to publish: single-line, at
/// most [`MAX_DETAIL_LEN`] characters, and with every URL collapsed to
/// `scheme://host:port` so a connection string, an API key, or a signed URL can
/// never reach the response body.
fn scrub(input: &str) -> String {
    let flattened = input.split_whitespace().collect::<Vec<_>>().join(" ");
    let mut out = String::with_capacity(flattened.len());
    let mut rest = flattened.as_str();
    while let Some(scheme_end) = rest.find("://") {
        out.push_str(&rest[..scheme_end]);
        out.push_str("://");
        rest = &rest[scheme_end + 3..];
        // The authority runs until the first path, query, or fragment
        // delimiter; everything after it is dropped outright rather than
        // re-scanned, because a token can live in any of those segments.
        let authority_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
        let authority = &rest[..authority_end];
        rest = &rest[authority_end..];
        // `rsplit` keeps only the host: userinfo carries the password.
        out.push_str(authority.rsplit('@').next().unwrap_or(authority));
        // Skip the discarded path/query/fragment.
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
