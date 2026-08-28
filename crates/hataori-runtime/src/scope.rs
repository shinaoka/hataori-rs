use crate::{Action, Place, RemoteFuture, Runtime, RuntimeError, WireValue};
use hataori_runtime_foundation::protocol::RequestId;
use std::{
    cell::RefCell,
    collections::HashSet,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

pub struct RuntimeScope<'runtime> {
    runtime: RefCell<&'runtime mut Runtime>,
    tracked: RefCell<HashSet<RequestId>>,
}

impl std::fmt::Debug for RuntimeScope<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RuntimeScope")
            .field("tracked", &self.tracked.borrow().len())
            .finish_non_exhaustive()
    }
}

impl RuntimeScope<'_> {
    pub fn spawn_on<'scope, A: Action>(
        &'scope self,
        place: Place,
        action: A,
    ) -> Result<ScopedFuture<'scope, A::Output>, RuntimeError> {
        let future = self.runtime.borrow().spawn_on(place, action)?;
        let request = future.request_id();
        self.tracked.borrow_mut().insert(request);
        Ok(ScopedFuture {
            inner: future,
            tracked: &self.tracked,
        })
    }

    pub fn block_on<T: WireValue>(&self, future: ScopedFuture<'_, T>) -> Result<T, RuntimeError> {
        self.runtime.borrow_mut().block_on(future)
    }
}

#[must_use = "a scoped future must be joined or dropped before the scope exits"]
pub struct ScopedFuture<'scope, T: WireValue> {
    inner: RemoteFuture<T>,
    tracked: &'scope RefCell<HashSet<RequestId>>,
}

impl<T: WireValue> Future for ScopedFuture<'_, T> {
    type Output = Result<T, RuntimeError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.inner).poll(context) {
            Poll::Ready(result) => {
                self.tracked.borrow_mut().remove(&self.inner.request_id());
                Poll::Ready(result)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<T: WireValue> Drop for ScopedFuture<'_, T> {
    fn drop(&mut self) {
        self.tracked.borrow_mut().remove(&self.inner.request_id());
    }
}

impl Runtime {
    pub fn scope<R>(
        &mut self,
        function: impl for<'scope> FnOnce(&'scope RuntimeScope<'_>) -> Result<R, RuntimeError>,
    ) -> Result<R, RuntimeError> {
        let scope = RuntimeScope {
            runtime: RefCell::new(self),
            tracked: RefCell::new(HashSet::new()),
        };
        let result = function(&scope);
        let requests: Vec<_> = scope.tracked.borrow().iter().copied().collect();
        let client = scope.runtime.borrow().client();
        for request in &requests {
            client.cancel(*request);
        }
        if result.is_ok() && !requests.is_empty() {
            scope.runtime.borrow_mut().progress(requests.len())?;
        }
        result
    }
}
