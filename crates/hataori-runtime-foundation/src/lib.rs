//! Internal Phase A protocol and transport foundation for Hataori.
//!
//! This unpublished crate keeps the future runtime work isolated from the
//! implemented P0/P1 facade while its transport contract is established.

#[doc(hidden)]
pub mod conformance;
#[doc(hidden)]
pub mod memory;
#[cfg(feature = "mpi")]
#[doc(hidden)]
pub mod mpi;
#[doc(hidden)]
pub mod protocol;
#[doc(hidden)]
pub mod tcp;
#[doc(hidden)]
pub mod transport;
