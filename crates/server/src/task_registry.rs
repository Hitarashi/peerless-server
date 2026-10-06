use std::{collections::HashMap, sync::Arc};

use crate::rip_tasks::ServerTaskMeta;

/// Returned by [`TaskRegistry::remove_if`] when the task exists but the caller is not
/// allowed to remove it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TaskRemovalRejected;

/// Shared registry of in-flight rip tasks, keyed by task id.
///
/// The task map is private: every read and write goes through the methods below, so no
/// caller can hold the lock while doing unrelated work. Cloning a registry clones the
/// handle, not the tasks.
#[derive(Clone, Default)]
pub struct TaskRegistry {
    inner: Arc<parking_lot::RwLock<HashMap<String, ServerTaskMeta>>>,
}

impl TaskRegistry {
    /// Register `meta` under `task_id`, replacing any task already registered there.
    pub fn insert(&self, task_id: String, meta: ServerTaskMeta) {
        self.inner.write().insert(task_id, meta);
    }

    /// Remove and return the task registered under `task_id`.
    pub fn remove(&self, task_id: &str) -> Option<ServerTaskMeta> {
        self.inner.write().remove(task_id)
    }

    /// Remove the task registered under `task_id` only when `allow` accepts it.
    ///
    /// `Ok(None)` means no such task is registered, `Err(TaskRemovalRejected)` means it
    /// exists but the caller may not remove it. The authorization check and the removal
    /// happen under a single write lock, so a rejected removal leaves the task intact.
    pub fn remove_if(
        &self,
        task_id: &str,
        allow: impl FnOnce(&ServerTaskMeta) -> bool,
    ) -> Result<Option<ServerTaskMeta>, TaskRemovalRejected> {
        let mut tasks = self.inner.write();
        let Some(task) = tasks.get(task_id) else {
            return Ok(None);
        };
        if !allow(task) {
            return Err(TaskRemovalRejected);
        }
        Ok(tasks.remove(task_id))
    }

    /// Register `meta` unless an equivalent task is already in flight.
    ///
    /// Returns the id of the newly registered task, or `Err(existing_task_id)` when
    /// `equivalent(&existing, &candidate)` matched a task that is already registered. The
    /// lookup and the insert share one write lock, so concurrent callers can never reserve
    /// the same track twice.
    pub fn insert_unique(
        &self,
        meta: ServerTaskMeta,
        equivalent: impl Fn(&ServerTaskMeta, &ServerTaskMeta) -> bool,
    ) -> Result<String, String> {
        let mut tasks = self.inner.write();
        if let Some(existing) = tasks.values().find(|task| equivalent(task, &meta)) {
            return Err(existing.task_id.clone());
        }

        let task_id = meta.task_id.clone();
        tasks.insert(task_id.clone(), meta);
        Ok(task_id)
    }

    /// A copy of the task registered under `task_id`.
    pub fn get(&self, task_id: &str) -> Option<ServerTaskMeta> {
        self.inner.read().get(task_id).cloned()
    }

    pub fn contains_key(&self, task_id: &str) -> bool {
        self.inner.read().contains_key(task_id)
    }

    pub fn len(&self) -> usize {
        self.inner.read().len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.read().is_empty()
    }

    /// Copies of every registered task, in unspecified order.
    pub fn all(&self) -> Vec<ServerTaskMeta> {
        self.inner.read().values().cloned().collect()
    }

    /// Mutate the task registered under `task_id` in place; reports whether it existed.
    ///
    /// Nothing is copied, so this is the cheap path for hot updates such as progress.
    pub fn update(&self, task_id: &str, update: impl FnOnce(&mut ServerTaskMeta)) -> bool {
        let mut tasks = self.inner.write();
        let Some(task) = tasks.get_mut(task_id) else {
            return false;
        };
        update(task);
        true
    }

    /// Mutate the task registered under `task_id` and return its updated copy.
    pub fn update_and_get(
        &self,
        task_id: &str,
        update: impl FnOnce(&mut ServerTaskMeta),
    ) -> Option<ServerTaskMeta> {
        let mut tasks = self.inner.write();
        let task = tasks.get_mut(task_id)?;
        update(task);
        Some(task.clone())
    }

    /// Mutate the first task matching `matches` and return its updated copy.
    ///
    /// The scan and the mutation share one write lock, so callers only ever see a task
    /// that was already matching when it was updated.
    pub fn update_first_matching(
        &self,
        mut matches: impl FnMut(&ServerTaskMeta) -> bool,
        update: impl FnOnce(&mut ServerTaskMeta),
    ) -> Option<ServerTaskMeta> {
        let mut tasks = self.inner.write();
        let task = tasks.values_mut().find(|task| matches(task))?;
        update(task);
        Some(task.clone())
    }
}
