//! Backend-independent bounded transport contract.

use crate::protocol::{Channel, LocalityId, Parcel, ProtocolError};
use std::{
    collections::VecDeque,
    fmt,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        mpsc::{self, Receiver, SyncSender},
        Mutex, MutexGuard,
    },
    time::Duration,
};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct SendTicket(u64);

impl SendTicket {
    pub(crate) const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TransportEvent {
    LocalSendComplete {
        ticket: SendTicket,
    },
    Incoming {
        parcel: Parcel,
    },
    SendFailed {
        ticket: SendTicket,
        error: TransportError,
    },
    PeerFailed {
        peer: LocalityId,
        error: TransportError,
    },
    ShutdownComplete,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Progress {
    pub events: usize,
    pub made_progress: bool,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TransportStats {
    pub submitted_parcels: u64,
    pub submitted_bytes: u64,
    pub received_parcels: u64,
    pub received_bytes: u64,
    pub queued_control: usize,
    pub queued_action: usize,
    pub queued_bulk: usize,
    pub queued_bytes: usize,
    pub retained_read_bytes: usize,
    pub retained_write_bytes: usize,
    pub retained_reassembly_bytes: usize,
    pub peer_failures: u64,
    pub pending_events: usize,
    pub pending_event_bytes: usize,
}

impl TransportStats {
    pub const fn queued_for(self, channel: Channel) -> usize {
        match channel {
            Channel::Control => self.queued_control,
            Channel::Action => self.queued_action,
            Channel::Bulk => self.queued_bulk,
        }
    }

    pub const fn retained_bytes(self) -> usize {
        self.queued_bytes
            .saturating_add(self.retained_read_bytes)
            .saturating_add(self.retained_write_bytes)
            .saturating_add(self.retained_reassembly_bytes)
            .saturating_add(self.pending_event_bytes)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TransportError {
    Protocol(ProtocolError),
    InvalidDestination(LocalityId),
    QueueFull { channel: Channel, limit: usize },
    ByteLimit { actual: usize, limit: usize },
    Disconnected(LocalityId),
    Peer(String),
    Io(String),
    Timeout(&'static str),
    Shutdown,
    WrongThread,
}

impl fmt::Display for TransportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "transport error: {self:?}")
    }
}

impl std::error::Error for TransportError {}

impl From<ProtocolError> for TransportError {
    fn from(error: ProtocolError) -> Self {
        Self::Protocol(error)
    }
}

pub trait TransportHandle: Send + Sync {
    fn try_send(&self, parcel: Parcel) -> Result<SendTicket, TransportError>;
}

pub trait TransportDriver {
    fn local_id(&self) -> LocalityId;
    fn members(&self) -> &[LocalityId];
    fn capabilities(&self) -> u64;
    fn progress(
        &mut self,
        events: &mut Vec<TransportEvent>,
        max_events: usize,
    ) -> Result<Progress, TransportError>;
    fn flush(&mut self, timeout: Duration) -> Result<(), TransportError>;
    fn shutdown(
        &mut self,
        events: &mut Vec<TransportEvent>,
        timeout: Duration,
    ) -> Result<(), TransportError>;
    fn stats(&self) -> TransportStats;
}

#[derive(Debug)]
pub(crate) struct Submission {
    pub(crate) ticket: SendTicket,
    pub(crate) parcel: Parcel,
    pub(crate) reserved_bytes: usize,
}

#[derive(Debug)]
pub(crate) struct SubmissionState {
    limits: crate::protocol::ProtocolLimits,
    next_ticket: AtomicU64,
    closed: AtomicBool,
    gate: Mutex<()>,
    counts: Vec<AtomicUsize>,
    bytes: Vec<AtomicUsize>,
    submitted_parcels: AtomicU64,
    submitted_bytes: AtomicU64,
    received_parcels: AtomicU64,
    received_bytes: AtomicU64,
    peer_failures: AtomicU64,
}

impl SubmissionState {
    pub(crate) fn new(members: usize, limits: crate::protocol::ProtocolLimits) -> Self {
        Self {
            limits,
            next_ticket: AtomicU64::new(1),
            closed: AtomicBool::new(false),
            gate: Mutex::new(()),
            counts: (0..members * 3).map(|_| AtomicUsize::new(0)).collect(),
            bytes: (0..members).map(|_| AtomicUsize::new(0)).collect(),
            submitted_parcels: AtomicU64::new(0),
            submitted_bytes: AtomicU64::new(0),
            received_parcels: AtomicU64::new(0),
            received_bytes: AtomicU64::new(0),
            peer_failures: AtomicU64::new(0),
        }
    }

    pub(crate) fn reserve(
        &self,
        peer: usize,
        channel: Channel,
        bytes: usize,
    ) -> Result<SendPermit<'_>, TransportError> {
        let gate = self.gate.lock().unwrap();
        if self.closed.load(Ordering::Acquire) {
            return Err(TransportError::Shutdown);
        }
        let slot = Self::slot(peer, channel);
        let count = self.counts[slot].fetch_add(1, Ordering::AcqRel) + 1;
        if count > self.limits.max_queued_parcels_per_peer {
            self.counts[slot].fetch_sub(1, Ordering::AcqRel);
            return Err(TransportError::QueueFull {
                channel,
                limit: self.limits.max_queued_parcels_per_peer,
            });
        }
        let total = self.bytes[peer].fetch_add(bytes, Ordering::AcqRel) + bytes;
        let byte_limit = if channel == Channel::Control {
            self.limits.max_inflight_bytes_per_peer
        } else {
            self.limits.max_inflight_bytes_per_peer - self.limits.control_reserved_bytes_per_peer
        };
        if total > byte_limit {
            self.bytes[peer].fetch_sub(bytes, Ordering::AcqRel);
            self.counts[slot].fetch_sub(1, Ordering::AcqRel);
            return Err(TransportError::ByteLimit {
                actual: total,
                limit: byte_limit,
            });
        }
        let raw = self.next_ticket.load(Ordering::Relaxed);
        if raw == 0 || raw == u64::MAX {
            self.release(peer, channel, bytes);
            return Err(TransportError::Peer("send ticket overflow".into()));
        }
        self.next_ticket.store(raw + 1, Ordering::Relaxed);
        Ok(SendPermit {
            state: self,
            _gate: gate,
            peer,
            channel,
            bytes,
            ticket: SendTicket::new(raw),
            committed: false,
        })
    }

    pub(crate) fn close(&self) {
        let _gate = self.gate.lock().unwrap();
        self.closed.store(true, Ordering::Release);
    }

    pub(crate) fn release(&self, peer: usize, channel: Channel, bytes: usize) {
        let slot = Self::slot(peer, channel);
        self.bytes[peer].fetch_sub(bytes, Ordering::AcqRel);
        self.counts[slot].fetch_sub(1, Ordering::AcqRel);
    }

    pub(crate) fn record_received(&self, bytes: usize) {
        self.received_parcels.fetch_add(1, Ordering::Relaxed);
        self.received_bytes
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }

    pub(crate) fn record_peer_failure(&self) {
        self.peer_failures.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn outstanding_count(&self) -> usize {
        self.counts
            .iter()
            .map(|count| count.load(Ordering::Acquire))
            .sum()
    }

    pub(crate) fn stats(&self) -> TransportStats {
        let mut stats = TransportStats {
            submitted_parcels: self.submitted_parcels.load(Ordering::Relaxed),
            submitted_bytes: self.submitted_bytes.load(Ordering::Relaxed),
            received_parcels: self.received_parcels.load(Ordering::Relaxed),
            received_bytes: self.received_bytes.load(Ordering::Relaxed),
            peer_failures: self.peer_failures.load(Ordering::Relaxed),
            ..TransportStats::default()
        };
        for peer in 0..self.counts.len() / 3 {
            stats.queued_control +=
                self.counts[Self::slot(peer, Channel::Control)].load(Ordering::Acquire);
            stats.queued_action +=
                self.counts[Self::slot(peer, Channel::Action)].load(Ordering::Acquire);
            stats.queued_bulk +=
                self.counts[Self::slot(peer, Channel::Bulk)].load(Ordering::Acquire);
            stats.queued_bytes += self.bytes[peer].load(Ordering::Acquire);
        }
        stats
    }

    const fn slot(peer: usize, channel: Channel) -> usize {
        peer * 3 + channel as usize
    }
}

pub(crate) struct SendPermit<'a> {
    state: &'a SubmissionState,
    _gate: MutexGuard<'a, ()>,
    peer: usize,
    channel: Channel,
    bytes: usize,
    ticket: SendTicket,
    committed: bool,
}

impl SendPermit<'_> {
    pub(crate) const fn ticket(&self) -> SendTicket {
        self.ticket
    }

    pub(crate) fn commit(mut self) {
        self.state.submitted_parcels.fetch_add(1, Ordering::Relaxed);
        self.state
            .submitted_bytes
            .fetch_add(self.bytes as u64, Ordering::Relaxed);
        self.committed = true;
    }
}

impl Drop for SendPermit<'_> {
    fn drop(&mut self) {
        if !self.committed {
            self.state.release(self.peer, self.channel, self.bytes);
        }
    }
}

pub(crate) fn add_pending_event_stats(
    stats: &mut TransportStats,
    events: &VecDeque<TransportEvent>,
) {
    stats.pending_events = events.len();
    stats.pending_event_bytes = events.iter().fold(0, |total, event| {
        total.saturating_add(match event {
            TransportEvent::Incoming { parcel } => parcel.payload_len().unwrap_or(usize::MAX),
            _ => 0,
        })
    });
}

pub(crate) fn submission_channels<T>(capacity: usize) -> ([SyncSender<T>; 3], [Receiver<T>; 3]) {
    let [(control_tx, control_rx), (action_tx, action_rx), (bulk_tx, bulk_rx)] =
        std::array::from_fn(|_| mpsc::sync_channel(capacity));
    (
        [control_tx, action_tx, bulk_tx],
        [control_rx, action_rx, bulk_rx],
    )
}

#[cfg(test)]
mod tests;
