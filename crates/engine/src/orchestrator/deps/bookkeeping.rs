use super::BoxFuture;
use crate::{settings::BotSettings, types::TrackKey};

#[derive(Debug, Clone, PartialEq)]
pub struct RequestLog {
    pub telegram_id: i64,
    pub chat_id: i64,
    pub track_key: TrackKey,
    pub is_cache_hit: bool,
    pub duration_ms: Option<i64>,
    pub status: String,
    pub error_reason: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskBookkeepingOperation {
    LogRequest,
}

impl std::fmt::Display for TaskBookkeepingOperation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("log request")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TaskBookkeepingError {
    #[error("{operation} unavailable: {detail}")]
    Unavailable {
        operation: TaskBookkeepingOperation,
        detail: String,
    },
    #[error("{operation} failed: {detail}")]
    Failed {
        operation: TaskBookkeepingOperation,
        detail: String,
    },
}

impl TaskBookkeepingError {
    pub fn unavailable(operation: TaskBookkeepingOperation, detail: impl Into<String>) -> Self {
        Self::Unavailable {
            operation,
            detail: detail.into(),
        }
    }

    pub fn failed(operation: TaskBookkeepingOperation, detail: impl Into<String>) -> Self {
        Self::Failed {
            operation,
            detail: detail.into(),
        }
    }
}

pub trait TaskBookkeeping: Send + Sync {
    fn settings_snapshot(&self) -> BotSettings;

    fn log_request<'a>(
        &'a self,
        log: RequestLog,
    ) -> BoxFuture<'a, Result<(), TaskBookkeepingError>>;
}
