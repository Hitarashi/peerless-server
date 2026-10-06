//! Inflight target deduplication: keys, entries, and the releasing guard.

use super::*;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct InflightTargetKey {
    pub provider: Provider,
    pub kind: TargetKind,
    pub id: String,
    pub storefront: Option<String>,
}

pub struct InflightEntry {
    pub job_id: String,
    pub notify: Arc<tokio::sync::Notify>,
    pub success: Arc<std::sync::atomic::AtomicBool>,
}

pub(super) struct InflightGuard {
    pub(super) table: Arc<Mutex<HashMap<InflightTargetKey, Arc<InflightEntry>>>>,
    pub(super) keys: Vec<InflightTargetKey>,
    pub(super) entry: Arc<InflightEntry>,
    pub(super) armed: bool,
}

impl InflightGuard {
    pub(super) fn new(
        table: Arc<Mutex<HashMap<InflightTargetKey, Arc<InflightEntry>>>>,
        keys: Vec<InflightTargetKey>,
        entry: Arc<InflightEntry>,
    ) -> Self {
        Self {
            table,
            keys,
            entry,
            armed: true,
        }
    }

    pub(super) fn finish(mut self, success: bool) {
        if success {
            self.entry
                .success
                .store(true, std::sync::atomic::Ordering::Release);
        }
        self.cleanup();
    }

    pub(super) fn cleanup(&mut self) {
        if self.armed {
            {
                let mut guard = self.table.lock().expect("inflight poisoned");
                for key in &self.keys {
                    guard.remove(key);
                }
            }
            self.entry.notify.notify_waiters();
            self.armed = false;
        }
    }
}

impl Drop for InflightGuard {
    fn drop(&mut self) {
        self.cleanup();
    }
}
