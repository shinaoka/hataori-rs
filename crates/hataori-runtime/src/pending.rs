use crate::{
    action::{ActionValue, WireValue},
    error::{ResourceKind, RuntimeError},
};
use hataori_runtime_foundation::protocol::{ActionId, DomainId, LocalityId, RequestId, TraceId};
use std::{
    collections::HashMap,
    future::Future,
    marker::PhantomData,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    task::{Context, Poll, Waker},
    time::{Duration, Instant},
};

struct PromiseState {
    result: Option<Result<ActionValue, RuntimeError>>,
    waker: Option<Waker>,
}

pub(crate) struct Promise {
    state: Mutex<PromiseState>,
    done: AtomicBool,
}

impl Promise {
    pub(crate) fn new() -> Self {
        Self {
            state: Mutex::new(PromiseState {
                result: None,
                waker: None,
            }),
            done: AtomicBool::new(false),
        }
    }

    pub(crate) fn complete(&self, result: Result<ActionValue, RuntimeError>) {
        let mut state = self.state.lock().unwrap();
        if state.result.is_none() && !self.done.load(Ordering::Acquire) {
            state.result = Some(result);
            self.done.store(true, Ordering::Release);
            if let Some(waker) = state.waker.take() {
                waker.wake();
            }
        }
    }

    fn poll(&self, context: &mut Context<'_>) -> Poll<Result<ActionValue, RuntimeError>> {
        let mut state = self.state.lock().unwrap();
        if let Some(result) = state.result.take() {
            Poll::Ready(result)
        } else {
            state.waker = Some(context.waker().clone());
            Poll::Pending
        }
    }

    fn done(&self) -> bool {
        self.done.load(Ordering::Acquire)
    }
}

pub(crate) struct PendingEntry {
    pub promise: Arc<Promise>,
    pub destination: LocalityId,
    pub action: ActionId,
    pub domain: DomainId,
    pub trace_id: Option<TraceId>,
    pub expires_at: Instant,
    pub deadline: Duration,
    pub cancel_token: Option<Arc<AtomicBool>>,
}

#[derive(Default)]
struct PendingState {
    entries: HashMap<RequestId, PendingEntry>,
}

pub(crate) struct PendingTable {
    state: Mutex<PendingState>,
    limit: usize,
}

impl PendingTable {
    pub(crate) fn new(limit: usize) -> Self {
        Self {
            state: Mutex::new(PendingState::default()),
            limit,
        }
    }

    pub(crate) fn insert(
        &self,
        request: RequestId,
        entry: PendingEntry,
    ) -> Result<(), RuntimeError> {
        let mut state = self.state.lock().unwrap();
        if state.entries.len() >= self.limit {
            return Err(RuntimeError::ResourceExhausted {
                resource: ResourceKind::PendingCalls,
                limit: self.limit,
            });
        }
        state.entries.insert(request, entry);
        Ok(())
    }

    pub(crate) fn take_checked(
        &self,
        request: RequestId,
        source: LocalityId,
        action: ActionId,
    ) -> Result<Option<PendingEntry>, RuntimeError> {
        let mut state = self.state.lock().unwrap();
        let Some(entry) = state.entries.get(&request) else {
            return Ok(None);
        };
        if entry.destination != source || entry.action != action {
            return Err(RuntimeError::Protocol(
                "result source or action does not match pending request".into(),
            ));
        }
        Ok(state.entries.remove(&request))
    }

    pub(crate) fn remove(&self, request: RequestId) -> Option<PendingEntry> {
        self.state.lock().unwrap().entries.remove(&request)
    }

    pub(crate) fn expired(&self, now: Instant) -> Vec<(RequestId, PendingEntry)> {
        let mut state = self.state.lock().unwrap();
        let expired: Vec<_> = state
            .entries
            .iter()
            .filter_map(|(request, entry)| (entry.expires_at <= now).then_some(*request))
            .collect();
        expired
            .into_iter()
            .filter_map(|request| state.entries.remove(&request).map(|entry| (request, entry)))
            .collect()
    }

    pub(crate) fn drain(&self) -> Vec<(RequestId, PendingEntry)> {
        self.state.lock().unwrap().entries.drain().collect()
    }

    pub(crate) fn len(&self) -> usize {
        self.state.lock().unwrap().entries.len()
    }
}

#[must_use = "dropping a remote future cancels its pending request"]
pub struct RemoteFuture<T: WireValue> {
    pub(crate) request: RequestId,
    promise: Arc<Promise>,
    cancel: Arc<dyn Fn(RequestId) + Send + Sync>,
    finished: bool,
    marker: PhantomData<T>,
}

impl<T: WireValue> std::fmt::Debug for RemoteFuture<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RemoteFuture")
            .field("request", &self.request)
            .field("finished", &self.finished)
            .finish()
    }
}

impl<T: WireValue> RemoteFuture<T> {
    pub(crate) fn new(
        request: RequestId,
        promise: Arc<Promise>,
        cancel: Arc<dyn Fn(RequestId) + Send + Sync>,
    ) -> Self {
        Self {
            request,
            promise,
            cancel,
            finished: false,
            marker: PhantomData,
        }
    }

    pub const fn request_id(&self) -> RequestId {
        self.request
    }
}

impl<T: WireValue> Unpin for RemoteFuture<T> {}

impl<T: WireValue> Future for RemoteFuture<T> {
    type Output = Result<T, RuntimeError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if this.finished {
            panic!("RemoteFuture polled after completion");
        }
        match this.promise.poll(context) {
            Poll::Ready(Ok(ActionValue::Encoded(segments))) => {
                this.finished = true;
                Poll::Ready(T::decode(segments).map_err(|error| {
                    RuntimeError::Protocol(format!("action output decode failed: {error}"))
                }))
            }
            Poll::Ready(Ok(ActionValue::Typed(value))) => {
                this.finished = true;
                Poll::Ready(
                    value
                        .downcast::<T>()
                        .map(|value| *value)
                        .map_err(|_| RuntimeError::Protocol("typed action output mismatch".into())),
                )
            }
            Poll::Ready(Err(error)) => {
                this.finished = true;
                Poll::Ready(Err(error))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<T: WireValue> Drop for RemoteFuture<T> {
    fn drop(&mut self) {
        if !self.finished && !self.promise.done() {
            (self.cancel)(self.request);
        }
    }
}

#[cfg(test)]
mod tests;
