//! Long-lived bounded runtime and typed structured remote execution.
//!
//! Phase B builds on `hataori-runtime-foundation` without exposing transport
//! backend types through actions, futures, scopes, or runtime statistics.

mod action;
mod dedup;
mod domain;
mod error;
mod pending;
mod runtime;
mod scope;
mod wire;

pub use action::{Action, Segments, WireValue};
pub use domain::{DomainConfig, DomainStats};
pub use error::{ActionError, ResourceKind, RuntimeError, RuntimeState};
pub use pending::RemoteFuture;
pub use runtime::{
    Place, Runtime, RuntimeBuilder, RuntimeClient, RuntimeLimits, RuntimeProgress, RuntimeStats,
    ShutdownReport, SpawnOptions,
};
pub use scope::{RuntimeScope, ScopedFuture};
