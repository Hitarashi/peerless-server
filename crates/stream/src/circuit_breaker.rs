use std::{
    sync::RwLock,
    time::{Duration, Instant},
};

#[derive(Debug, Clone)]
enum WorkerState {
    Healthy,
    Quarantined { until: Instant },
}

/// Circuit breaker managing auxiliary worker health and FloodWait quarantines.
#[derive(Debug)]
pub struct CircuitBreaker {
    worker_states: RwLock<Vec<WorkerState>>,
}

impl CircuitBreaker {
    pub fn new(worker_count: usize) -> Self {
        Self {
            worker_states: RwLock::new(vec![WorkerState::Healthy; worker_count]),
        }
    }

    /// Check if a worker is healthy and eligible to receive chunk requests.
    pub fn is_available(&self, worker_id: usize) -> bool {
        let states = self.worker_states.read().unwrap();
        match states.get(worker_id) {
            Some(WorkerState::Healthy) => true,
            Some(WorkerState::Quarantined { until }) => Instant::now() >= *until,
            None => false,
        }
    }

    /// Place a worker in quarantine for the specified duration.
    pub fn quarantine(&self, worker_id: usize, duration: Duration, reason: impl Into<String>) {
        let reason = reason.into();
        let until = Instant::now() + duration;
        let mut states = self.worker_states.write().unwrap();
        if let Some(slot) = states.get_mut(worker_id) {
            tracing::warn!(
                worker_id,
                duration_secs = duration.as_secs_f32(),
                reason = %reason,
                "Quarantining stream worker"
            );
            *slot = WorkerState::Quarantined { until };
        }
    }

    /// Mark a worker as healthy after a successful operation if it was previously quarantined.
    pub fn record_success(&self, worker_id: usize) {
        let mut states = self.worker_states.write().unwrap();
        if let Some(slot) = states.get_mut(worker_id)
            && matches!(slot, WorkerState::Quarantined { .. })
        {
            *slot = WorkerState::Healthy;
        }
    }

    /// Remaining quarantine duration, or None if healthy/expired.
    pub fn quarantine_remaining(&self, worker_id: usize) -> Option<Duration> {
        let states = self.worker_states.read().unwrap();
        match states.get(worker_id) {
            Some(WorkerState::Quarantined { until, .. }) => {
                let now = Instant::now();
                if *until > now {
                    Some(*until - now)
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    /// Number of currently healthy and available workers.
    pub fn available_count(&self) -> usize {
        let states = self.worker_states.read().unwrap();
        let now = Instant::now();
        states
            .iter()
            .filter(|s| match s {
                WorkerState::Healthy => true,
                WorkerState::Quarantined { until } => now >= *until,
            })
            .count()
    }

    /// Alias for healthy worker count.
    pub fn healthy_worker_count(&self) -> usize {
        self.available_count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn circuit_breaker_initial_state_healthy() {
        let cb = CircuitBreaker::new(3);
        assert!(cb.is_available(0));
        assert!(cb.is_available(1));
        assert!(cb.is_available(2));
        assert!(!cb.is_available(3)); // Out of bounds
    }

    #[test]
    fn circuit_breaker_quarantine_and_expiry() {
        let cb = CircuitBreaker::new(2);
        cb.quarantine(0, Duration::from_millis(50), "FloodWait(50ms)");
        assert!(!cb.is_available(0));
        assert!(cb.quarantine_remaining(0).is_some());

        std::thread::sleep(Duration::from_millis(60));
        assert!(cb.is_available(0));
        assert_eq!(cb.quarantine_remaining(0), None);
    }

    #[test]
    fn circuit_breaker_manual_recovery() {
        let cb = CircuitBreaker::new(2);
        cb.quarantine(1, Duration::from_secs(60), "Error");
        assert!(!cb.is_available(1));
        cb.record_success(1);
        assert!(cb.is_available(1));
    }
}
