//! Bounded initiator-side algorithms built directly on Hataori actions and futures.
mod batch;
mod collectives;
mod error;
mod options;
pub use batch::{
    pmap, pmap_at, pmap_colocated, pmap_preferred_colocated, AlgorithmRegistryExt,
    BatchActionToken, ControllerStats, PmapFuture,
};
pub use collectives::{
    broadcast, gather, scatter, CollectiveRegistryExt, CollectiveToken, GatherFuture,
};
pub use error::AlgorithmError;
pub use options::{LocalMode, PmapOptions};
