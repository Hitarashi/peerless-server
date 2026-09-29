//! Dynamic `/cancel_<cuid>` and inline cancel authorization.
//!
//! M5c: the engine orchestrator owns job state. This module only performs
//! authorization and selects which engine job to cancel.

use crate::BotState;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelResult {
    Cancelled,
    Unauthorized,
    Expired,
}

/// Inline-button cancel: authorize against the engine job's requester.
pub fn cancel_inline(
    state: &BotState,
    job_id: &str,
    caller_id: i64,
    is_admin: bool,
) -> CancelResult {
    let Some(job) = state.rip_orchestrator.get_task(job_id) else {
        return CancelResult::Expired;
    };
    if job.completed || job.terminal_state.is_some() {
        return CancelResult::Expired;
    }
    if !is_admin && job.user_id != caller_id {
        return CancelResult::Unauthorized;
    }
    let caller_name = if is_admin { "Admin" } else { "User" };
    if state
        .rip_orchestrator
        .cancel_task(job_id, Some(caller_name))
    {
        CancelResult::Cancelled
    } else {
        CancelResult::Expired
    }
}

pub const COMMAND_ACK: &str = "✓ <b>Download cancelled.</b>";
pub const CALLBACK_ACK: &str = "Download cancelled.";
pub const CALLBACK_UNAUTHORIZED: &str =
    "⛔ Only the person who requested this download or an admin can cancel it.";
pub const CALLBACK_EXPIRED: &str = "This download has already completed or expired.";
