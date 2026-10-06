use std::{ops::ControlFlow, time::Duration};

use ferogram::{InvocationError, RetryContext, RetryPolicy};

#[derive(Debug, Clone, Copy)]
pub struct BoundedTelegramRetry {
    pub flood_threshold: Duration,
    pub max_attempts: u32,
    pub max_total_wait: Duration,
    pub io_retry_delay: Duration,
}

impl Default for BoundedTelegramRetry {
    fn default() -> Self {
        Self {
            flood_threshold: Duration::from_secs(60),
            max_attempts: 5,
            max_total_wait: Duration::from_secs(300),
            io_retry_delay: Duration::from_secs(1),
        }
    }
}

impl RetryPolicy for BoundedTelegramRetry {
    fn should_retry(&self, context: &RetryContext) -> ControlFlow<(), Duration> {
        let delay = match &context.error {
            InvocationError::Rpc(rpc)
                if rpc.code == 420
                    && matches!(
                        rpc.name.as_str(),
                        "FLOOD_WAIT" | "FLOOD_PREMIUM_WAIT" | "SLOWMODE_WAIT"
                    ) =>
            {
                Duration::from_secs(rpc.value.unwrap_or_default() as u64)
            }
            InvocationError::Io(_) if context.fail_count.get() == 1 => self.io_retry_delay,
            _ => return ControlFlow::Break(()),
        };

        if context.fail_count.get() > self.max_attempts
            || delay > self.flood_threshold
            || context.slept_so_far.saturating_add(delay) > self.max_total_wait
        {
            return ControlFlow::Break(());
        }

        tracing::debug!(
            attempt = context.fail_count.get(),
            ?delay,
            slept = ?context.slept_so_far,
            "retrying Telegram request after rate limit or transient I/O"
        );
        ControlFlow::Continue(delay)
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use super::*;

    fn context(error: InvocationError, attempt: u32, slept: u64) -> RetryContext {
        RetryContext {
            error,
            fail_count: NonZeroU32::new(attempt).expect("test attempts are non-zero"),
            slept_so_far: Duration::from_secs(slept),
        }
    }

    #[test]
    fn premium_wait_is_retried_within_budget() {
        let error = InvocationError::Rpc(ferogram::RpcError {
            code: 420,
            name: "FLOOD_PREMIUM_WAIT".into(),
            value: Some(3),
        });
        assert_eq!(
            BoundedTelegramRetry::default().should_retry(&context(error, 1, 0)),
            ControlFlow::Continue(Duration::from_secs(3))
        );
    }

    #[test]
    fn retries_stop_at_attempt_and_wait_budgets() {
        let error = || {
            InvocationError::Rpc(ferogram::RpcError {
                code: 420,
                name: "FLOOD_WAIT".into(),
                value: Some(3),
            })
        };
        let policy = BoundedTelegramRetry::default();
        assert_eq!(
            policy.should_retry(&context(error(), 6, 0)),
            ControlFlow::Break(())
        );
        assert_eq!(
            policy.should_retry(&context(error(), 1, 300)),
            ControlFlow::Break(())
        );
    }
}
