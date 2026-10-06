use std::{
    sync::{Arc, OnceLock, RwLock},
    time::{Duration, Instant},
};

use apple::{MirrorEndpoint, MirrorError, MirrorHttp, MirrorPolicyManager};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MirrorHealth {
    Online,
    Unreachable,
    Unavailable,

    NotConfigured,
}

impl MirrorHealth {
    pub fn label(&self) -> &'static str {
        match self {
            Self::Online => "Online",
            Self::Unreachable => "Unreachable",
            Self::Unavailable => "Unavailable",
            Self::NotConfigured => "Not configured",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HealthReport {
    pub health: MirrorHealth,
    pub latency_ms: u64,
}

pub trait MirrorHealthProbe: Send + Sync {
    fn probe(&self) -> futures_util::future::BoxFuture<'_, HealthReport>;
}

pub struct PolicyProbe<H: MirrorHttp> {
    policy: MirrorPolicyManager<H>,
}

impl<H: MirrorHttp> PolicyProbe<H> {
    pub fn new(policy: MirrorPolicyManager<H>) -> Self {
        Self { policy }
    }
}

impl<H: MirrorHttp> MirrorHealthProbe for PolicyProbe<H> {
    fn probe(&self) -> futures_util::future::BoxFuture<'_, HealthReport> {
        Box::pin(async move {
            let started = Instant::now();
            let endpoint: Result<MirrorEndpoint, MirrorError> =
                self.policy.get_endpoint(false, None).await;
            let resolve_ms = started.elapsed().as_millis() as u64;
            match endpoint {
                Ok(_) => {
                    self.policy.record_success();
                    HealthReport {
                        health: MirrorHealth::Online,
                        latency_ms: resolve_ms,
                    }
                }
                Err(error) => {
                    self.policy.record_failure(&error.to_string());
                    let health = match &error {
                        MirrorError::Message(message) if message.contains("not configured") => {
                            MirrorHealth::NotConfigured
                        }

                        MirrorError::Message(message)
                            if message.contains("timed out")
                                || message.contains("HTTP")
                                || message.contains("network") =>
                        {
                            MirrorHealth::Unreachable
                        }
                        _ => MirrorHealth::Unavailable,
                    };
                    HealthReport {
                        health,
                        latency_ms: resolve_ms,
                    }
                }
            }
        })
    }
}

#[derive(Debug, Default)]
pub struct LastKnownHealth {
    report: RwLock<Option<HealthReport>>,
    probed_at: RwLock<Option<Instant>>,
}

const FRESH_WINDOW: Duration = Duration::from_secs(30);

impl LastKnownHealth {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record(&self, report: HealthReport) {
        *self.report.write().expect("health poisoned") = Some(report);
        *self.probed_at.write().expect("health poisoned") = Some(Instant::now());
    }

    pub fn label(&self) -> Option<&'static str> {
        self.report
            .read()
            .expect("health poisoned")
            .as_ref()
            .map(|report| report.health.label())
    }

    pub fn is_fresh(&self) -> bool {
        self.probed_at
            .read()
            .expect("health poisoned")
            .is_some_and(|at| at.elapsed() < FRESH_WINDOW)
    }
}

static HEALTH: OnceLock<Arc<LastKnownHealth>> = OnceLock::new();

pub fn last_known_health() -> Arc<LastKnownHealth> {
    HEALTH
        .get_or_init(|| Arc::new(LastKnownHealth::new()))
        .clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn labels_match_oracle_strings() {
        assert_eq!(MirrorHealth::Online.label(), "Online");
        assert_eq!(MirrorHealth::Unreachable.label(), "Unreachable");
        assert_eq!(MirrorHealth::Unavailable.label(), "Unavailable");
        assert_eq!(MirrorHealth::NotConfigured.label(), "Not configured");
    }

    #[test]
    fn last_known_health_starts_unknown_and_sticks() {
        let health = LastKnownHealth::new();
        assert_eq!(health.label(), None);
        health.record(HealthReport {
            health: MirrorHealth::Online,
            latency_ms: 12,
        });
        assert_eq!(health.label(), Some("Online"));
        health.record(HealthReport {
            health: MirrorHealth::Unreachable,
            latency_ms: 4001,
        });
        assert_eq!(health.label(), Some("Unreachable"));
    }

    #[test]
    fn freshness_window_elapses() {
        let health = LastKnownHealth::new();
        assert!(!health.is_fresh());
        health.record(HealthReport {
            health: MirrorHealth::Online,
            latency_ms: 1,
        });
        assert!(health.is_fresh());
    }
}
