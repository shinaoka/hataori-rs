//! Long-lived bounded runtime and typed structured remote execution.
//!
//! Phase B structured execution and Phase C fixed-placement remote objects
//! build on `hataori-runtime-foundation` without exposing backend types.

mod action;
mod dedup;
mod domain;
mod error;
mod object;
mod pending;
mod runtime;
mod scope;
mod wire;

pub use action::{Action, Segments, WireValue};
pub use domain::{DomainConfig, DomainStats};
pub use error::{ActionError, ResourceKind, RuntimeError, RuntimeState};
pub use object::{
    ColocatedFuture, CreateFuture, DistributedObject, LeaseTransfer, Mobility, ObjectAccess,
    ObjectCallFuture, ObjectConcurrency, ObjectLimits, ObjectReadAction, ObjectRoot, ObjectStats,
    ObjectWriteAction, PlacementFallback, PlacementFuture, Remote, RootedCreateFuture,
    TransferFuture, UpgradeFuture, WeakRemote,
};
pub use pending::RemoteFuture;
pub use runtime::{
    Place, Runtime, RuntimeBuilder, RuntimeClient, RuntimeLimits, RuntimeProgress, RuntimeStats,
    ShutdownReport, SpawnOptions,
};
pub use scope::{RuntimeScope, ScopedFuture};
