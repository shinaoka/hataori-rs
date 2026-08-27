use hataori_runtime_foundation::{
    protocol::{ActionId, DomainId, LocalityId, ObjectId, ObjectLocation, ObjectTypeId, RequestId},
    transport::TransportError,
};
use std::{fmt, time::Duration};

pub const MAX_ERROR_BYTES: usize = 4096;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeState {
    Created,
    Bootstrapping,
    Running,
    Draining,
    Stopped,
    Failed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResourceKind {
    PendingCalls,
    ActionQueue,
    DedupEntries,
    DedupBytes,
    SendTickets,
    Objects,
    ObjectMailbox,
    ResolverEntries,
    LocalityLeases,
    ObjectRoots,
    PlacementTickets,
    LeaseTransfers,
    Migrations,
    PreparedMigrations,
    Forwarders,
    SnapshotBytes,
    SnapshotSegments,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ActionError {
    Codec(String),
    User(String),
    Panic,
}

impl ActionError {
    pub fn codec(message: impl Into<String>) -> Self {
        Self::Codec(cap(message.into()))
    }

    pub fn user(message: impl Into<String>) -> Self {
        Self::User(cap(message.into()))
    }

    pub(crate) fn message(&self) -> String {
        match self {
            Self::Codec(message) | Self::User(message) => message.clone(),
            Self::Panic => "action handler panicked".into(),
        }
    }
}

impl fmt::Display for ActionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "action error: {self:?}")
    }
}

impl std::error::Error for ActionError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RuntimeError {
    InvalidLimits(&'static str),
    BuilderSealed,
    HandshakeNotPrepared,
    InvalidState {
        expected: RuntimeState,
        actual: RuntimeState,
    },
    DuplicateAction(ActionId),
    UnknownAction(ActionId),
    UnknownDomain(DomainId),
    DuplicateObjectType(ObjectTypeId),
    UnknownObjectType(ObjectTypeId),
    UnknownObject(ObjectId),
    ObjectCollected(ObjectId),
    StaleObjectLocation(ObjectId),
    ObjectAction(String),
    InvalidObjectTypeId,
    InvalidObjectConcurrency,
    PinnedObjectType(ObjectTypeId),
    MigrationInProgress(ObjectId),
    MigrationConflict(ObjectId),
    Migration {
        request: RequestId,
        message: String,
    },
    MigrationRolledBack(ObjectId),
    MigrationCommittedFailure {
        object: ObjectId,
        message: String,
    },
    SnapshotTooLarge {
        bytes: usize,
        limit: usize,
    },
    Moved {
        object: ObjectId,
        location: ObjectLocation,
    },
    RedirectLimit(ObjectId),
    InvalidActionId,
    InvalidSchema,
    InvalidDeadline,
    InvalidMembership(LocalityId),
    ResourceExhausted {
        resource: ResourceKind,
        limit: usize,
    },
    DeadlineExceeded {
        request: RequestId,
        deadline: Duration,
    },
    Cancelled(RequestId),
    RemoteAction {
        request: RequestId,
        message: String,
    },
    RemoteObject {
        request: RequestId,
        message: String,
    },
    Placement {
        request: RequestId,
        message: String,
    },
    Lease {
        request: RequestId,
        message: String,
    },
    LeaseTransferExpired(ObjectId),
    DuplicateResultUnavailable(RequestId),
    Protocol(String),
    Transport(TransportError),
    PeerFailed(LocalityId),
    Shutdown,
    ShutdownTimeout,
    RetainedResources,
}

impl fmt::Display for RuntimeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "runtime error: {self:?}")
    }
}

impl std::error::Error for RuntimeError {}

impl From<TransportError> for RuntimeError {
    fn from(error: TransportError) -> Self {
        Self::Transport(error)
    }
}

pub(crate) fn cap(mut message: String) -> String {
    if message.len() <= MAX_ERROR_BYTES {
        return message;
    }
    let mut end = MAX_ERROR_BYTES;
    while !message.is_char_boundary(end) {
        end -= 1;
    }
    message.truncate(end);
    message
}
