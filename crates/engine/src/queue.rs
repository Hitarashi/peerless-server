use std::{
    any::Any,
    collections::VecDeque,
    future::Future,
    panic::AssertUnwindSafe,
    pin::Pin,
    sync::{Arc, Mutex},
};

use futures_util::FutureExt;
use tokio::{sync::oneshot, task::JoinHandle};
use tokio_util::sync::CancellationToken;

pub struct EnqueueOptions {
    pub on_position_change: Option<Arc<dyn Fn(u64) + Send + Sync>>,
    pub on_start: Option<Arc<dyn Fn() + Send + Sync>>,
    pub signal: Option<CancellationToken>,
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum QueueError {
    #[error("Job was aborted")]
    Aborted,
    #[error("Queue cleared")]
    Cleared,
    #[doc(hidden)]
    #[error("Task panic: {0}")]
    Task(String),
}

pub type ErasedValue = Box<dyn Any + Send>;
pub type TaskResult = Result<ErasedValue, QueueError>;
pub type TaskReceiver = oneshot::Receiver<TaskResult>;
type ErasedTask =
    Box<dyn FnOnce(CancellationToken) -> Pin<Box<dyn Future<Output = ErasedValue> + Send>> + Send>;

struct QueuedItem {
    id: u64,
    task: Option<ErasedTask>,
    child: CancellationToken,
    completion: oneshot::Sender<Result<ErasedValue, QueueError>>,
    on_position_change: Option<Arc<dyn Fn(u64) + Send + Sync>>,
    on_start: Option<Arc<dyn Fn() + Send + Sync>>,
    stop_watcher: Arc<tokio::sync::Notify>,
}

struct QueueState {
    pending: VecDeque<QueuedItem>,
    active: Option<u64>,
    processing: bool,
    next_id: u64,
}

struct Inner {
    state: Mutex<QueueState>,
}

#[derive(Clone)]
pub struct SequentialRipQueue {
    inner: Arc<Inner>,
}

impl Default for SequentialRipQueue {
    fn default() -> Self {
        Self::new()
    }
}

impl SequentialRipQueue {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Inner {
                state: Mutex::new(QueueState {
                    pending: VecDeque::new(),
                    active: None,
                    processing: false,
                    next_id: 1,
                }),
            }),
        }
    }

    pub fn get_pending_count(&self) -> usize {
        self.inner
            .state
            .lock()
            .expect("queue mutex poisoned")
            .pending
            .len()
    }

    pub fn is_processing(&self) -> bool {
        self.inner
            .state
            .lock()
            .expect("queue mutex poisoned")
            .processing
    }

    pub fn clear(&self) {
        let pending = {
            let mut state = self.inner.state.lock().expect("queue mutex poisoned");
            state.pending.drain(..).collect::<Vec<_>>()
        };
        for item in pending {
            item.stop_watcher.notify_one();
            item.child.cancel();
            let _ = item.completion.send(Err(QueueError::Cleared));
        }
    }

    pub async fn enqueue<T: Send + 'static>(
        &self,
        task: impl FnOnce(CancellationToken) -> Pin<Box<dyn Future<Output = T> + Send>> + Send + 'static,
        options: Option<EnqueueOptions>,
    ) -> Result<T, QueueError> {
        let completion_rx = self.submit(task, options);
        let result = completion_rx.await.unwrap_or_else(|_| {
            Err(QueueError::Task(
                "queue worker stopped unexpectedly".to_owned(),
            ))
        })?;
        result
            .downcast::<T>()
            .map(|value| *value)
            .map_err(|_| QueueError::Task("queue result type mismatch".to_owned()))
    }

    pub fn submit<T: Send + 'static>(
        &self,
        task: impl FnOnce(CancellationToken) -> Pin<Box<dyn Future<Output = T> + Send>> + Send + 'static,
        options: Option<EnqueueOptions>,
    ) -> TaskReceiver {
        let options = options.unwrap_or(EnqueueOptions {
            on_position_change: None,
            on_start: None,
            signal: None,
        });
        if options
            .signal
            .as_ref()
            .is_some_and(CancellationToken::is_cancelled)
        {
            let (completion_tx, completion_rx) = oneshot::channel();
            let _ = completion_tx.send(Err(QueueError::Aborted));
            return completion_rx;
        }

        let child = CancellationToken::new();
        let (completion_tx, completion_rx) = oneshot::channel();
        let task: ErasedTask = Box::new(move |token| {
            Box::pin(async move { Box::new(task(token).await) as ErasedValue })
        });
        let stop_watcher = Arc::new(tokio::sync::Notify::new());
        let outer = options.signal.clone();
        let (id, start_worker, position_callback) = {
            let mut state = self.inner.state.lock().expect("queue mutex poisoned");
            let id = state.next_id;
            state.next_id += 1;
            let start_worker = !state.processing;
            state.processing = true;
            let position = state.pending.len() + 1;
            state.pending.push_back(QueuedItem {
                id,
                task: Some(task),
                child: child.clone(),
                completion: completion_tx,
                on_position_change: options.on_position_change.clone(),
                on_start: options.on_start.clone(),
                stop_watcher: stop_watcher.clone(),
            });
            let callback = if start_worker {
                None
            } else {
                options
                    .on_position_change
                    .map(|callback| (callback, position as u64))
            };
            (id, start_worker, callback)
        };

        if let Some(callback) = position_callback {
            callback.0(callback.1);
        }
        if let Some(outer) = outer {
            spawn_abort_watcher(
                self.inner.clone(),
                id,
                child.clone(),
                outer,
                stop_watcher.clone(),
            );
        }
        if start_worker {
            tokio::spawn(run_worker(self.inner.clone()));
        }

        completion_rx
    }
}

fn spawn_abort_watcher(
    inner: Arc<Inner>,
    id: u64,
    child: CancellationToken,
    outer: CancellationToken,
    stop: Arc<tokio::sync::Notify>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        tokio::select! {
            _ = outer.cancelled() => abort_item(&inner, id, &child),
            _ = stop.notified() => {},
        }
    })
}

fn abort_item(inner: &Arc<Inner>, id: u64, child: &CancellationToken) {
    let (removed, callbacks) = {
        let mut state = inner.state.lock().expect("queue mutex poisoned");
        if let Some(index) = state.pending.iter().position(|item| item.id == id) {
            let item = state.pending.remove(index).expect("queue item disappeared");
            let callbacks = state
                .pending
                .iter()
                .skip(index)
                .enumerate()
                .filter_map(|(offset, item)| {
                    item.on_position_change
                        .clone()
                        .map(|callback| (callback, (index + offset + 1) as u64))
                })
                .collect::<Vec<_>>();
            (Some(item), callbacks)
        } else {
            (None, Vec::new())
        }
    };

    if let Some(item) = removed {
        item.stop_watcher.notify_one();
        child.cancel();
        let _ = item.completion.send(Err(QueueError::Aborted));
        for (callback, position) in callbacks {
            callback(position);
        }
    } else {
        let is_active = inner.state.lock().expect("queue mutex poisoned").active == Some(id);
        if is_active {
            child.cancel();
        }
    }
}

async fn run_worker(inner: Arc<Inner>) {
    loop {
        let (mut item, callbacks) = {
            let mut state = inner.state.lock().expect("queue mutex poisoned");
            let Some(item) = state.pending.pop_front() else {
                state.active = None;
                state.processing = false;
                return;
            };
            state.active = Some(item.id);
            let callbacks = state
                .pending
                .iter()
                .enumerate()
                .filter_map(|(index, item)| {
                    item.on_position_change
                        .clone()
                        .map(|callback| (callback, (index + 1) as u64))
                })
                .collect::<Vec<_>>();
            (item, callbacks)
        };

        for (callback, position) in callbacks {
            callback(position);
        }
        if let Some(on_start) = &item.on_start {
            on_start();
        }

        let child = item.child.clone();
        let task = item.task.take().expect("queued task missing");
        let result = AssertUnwindSafe(async move { task(child).await })
            .catch_unwind()
            .await
            .map_err(|panic| QueueError::Task(panic_message(panic)));
        let _ = item.completion.send(result);
        item.stop_watcher.notify_one();

        let mut state = inner.state.lock().expect("queue mutex poisoned");
        state.active = None;
        if state.pending.is_empty() {
            state.processing = false;
            return;
        }
    }
}

fn panic_message(panic: Box<dyn Any + Send>) -> String {
    if let Some(message) = panic.downcast_ref::<String>() {
        message.clone()
    } else if let Some(message) = panic.downcast_ref::<&str>() {
        (*message).to_owned()
    } else {
        "task panicked".to_owned()
    }
}
