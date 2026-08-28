use crate::AlgorithmError;
use hataori_runtime::{
    Action, ActionError, DomainId, Place, RemoteFuture, Runtime, RuntimeBuilder, RuntimeError,
    Segments, WireValue,
};
use std::{
    future::Future,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};
#[derive(Clone, Copy, Debug, Default)]
pub struct CollectiveToken<T>(PhantomData<fn() -> T>);
impl<T> CollectiveToken<T> {
    pub const fn new() -> Self {
        Self(PhantomData)
    }
}
pub struct CollectiveEcho<T>(T);
impl<T: WireValue> WireValue for CollectiveEcho<T> {
    const SCHEMA_ID: u64 = T::SCHEMA_ID ^ 0xe504_0000_0000_0001;
    fn encode(self) -> Result<Segments, ActionError> {
        self.0.encode()
    }
    fn decode(s: Segments) -> Result<Self, ActionError> {
        Ok(Self(T::decode(s)?))
    }
}
impl<T: WireValue> Action for CollectiveEcho<T> {
    const ID: u128 = 0xe504_0000_0000_0000_0000_0000_0000_0001 ^ T::SCHEMA_ID as u128;
    type Output = T;
}
pub trait CollectiveRegistryExt {
    fn register_collective<T: WireValue>(&mut self) -> Result<CollectiveToken<T>, RuntimeError>;
}
impl CollectiveRegistryExt for RuntimeBuilder {
    fn register_collective<T: WireValue>(&mut self) -> Result<CollectiveToken<T>, RuntimeError> {
        self.register::<CollectiveEcho<T>, _>(|v| Ok(v.0))?;
        Ok(CollectiveToken::new())
    }
}
pub struct GatherFuture<T: WireValue> {
    futures: Vec<Option<RemoteFuture<T>>>,
    values: Vec<Option<T>>,
}
impl<T: WireValue> Unpin for GatherFuture<T> {}
impl<T: WireValue> Future for GatherFuture<T> {
    type Output = Result<Vec<T>, AlgorithmError>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        let mut pending = false;
        for i in 0..this.futures.len() {
            if let Some(f) = &mut this.futures[i] {
                match Pin::new(f).poll(cx) {
                    Poll::Pending => pending = true,
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e.into())),
                    Poll::Ready(Ok(v)) => {
                        this.futures[i] = None;
                        this.values[i] = Some(v)
                    }
                }
            }
        }
        if pending {
            return Poll::Pending;
        }
        Poll::Ready(Ok(this
            .values
            .iter_mut()
            .map(|v| v.take().unwrap())
            .collect()))
    }
}
pub fn gather<T: WireValue>(futures: Vec<RemoteFuture<T>>) -> GatherFuture<T> {
    let n = futures.len();
    GatherFuture {
        futures: futures.into_iter().map(Some).collect(),
        values: (0..n).map(|_| None).collect(),
    }
}
pub fn broadcast<T: WireValue + Clone>(
    runtime: &Runtime,
    domain: DomainId,
    value: T,
    _: CollectiveToken<T>,
) -> Result<GatherFuture<T>, AlgorithmError> {
    let futures = runtime
        .localities()
        .iter()
        .map(|&locality| {
            runtime.spawn_on(Place::new(locality, domain), CollectiveEcho(value.clone()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(gather(futures))
}
pub fn scatter<T: WireValue>(
    runtime: &Runtime,
    domain: DomainId,
    values: Vec<T>,
    _: CollectiveToken<T>,
) -> Result<GatherFuture<T>, AlgorithmError> {
    if values.len() != runtime.localities().len() {
        return Err(AlgorithmError::InputShape(
            "scatter requires one value per locality",
        ));
    }
    let futures = values
        .into_iter()
        .zip(runtime.localities())
        .map(|(value, &locality)| {
            runtime.spawn_on(Place::new(locality, domain), CollectiveEcho(value))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(gather(futures))
}
