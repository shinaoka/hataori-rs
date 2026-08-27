//! Deterministic bounded in-memory transport and fault injection.

use crate::{
    protocol::{Channel, LocalityId, Parcel, ProtocolLimits, RunId},
    transport::{
        Progress, SendTicket, TransportDriver, TransportError, TransportEvent, TransportHandle,
        TransportStats,
    },
};
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MemoryFault {
    Pass,
    Delay { progress_ticks: u64 },
    Duplicate,
    Loss,
    Reorder,
    Saturate,
    Disconnect,
}

#[derive(Debug)]
pub struct MemoryNetwork;

impl MemoryNetwork {
    pub fn build(
        locality_count: usize,
        run_id: RunId,
        limits: ProtocolLimits,
        faults: impl IntoIterator<Item = MemoryFault>,
    ) -> Result<Vec<(MemoryHandle, MemoryDriver)>, TransportError> {
        limits.validate()?;
        if locality_count == 0 {
            return Err(TransportError::Peer("membership is empty".into()));
        }
        let members: Vec<_> = (0..locality_count as u64).map(LocalityId::new).collect();
        let state = Arc::new(Mutex::new(State {
            run_id,
            limits,
            tick: 0,
            next_ticket: 1,
            faults: faults.into_iter().collect(),
            delayed: VecDeque::new(),
            nodes: (0..locality_count).map(|_| Node::default()).collect(),
        }));
        Ok(members
            .iter()
            .copied()
            .map(|local_id| {
                (
                    MemoryHandle {
                        local_id,
                        state: Arc::clone(&state),
                    },
                    MemoryDriver {
                        local_id,
                        members: members.clone(),
                        state: Arc::clone(&state),
                        shutdown: false,
                    },
                )
            })
            .collect())
    }
}

#[derive(Clone)]
pub struct MemoryHandle {
    local_id: LocalityId,
    state: Arc<Mutex<State>>,
}

impl std::fmt::Debug for MemoryHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MemoryHandle")
            .field("local_id", &self.local_id)
            .finish_non_exhaustive()
    }
}

impl TransportHandle for MemoryHandle {
    fn try_send(&self, parcel: Parcel) -> Result<SendTicket, TransportError> {
        let mut state = self.state.lock().unwrap();
        if !state.node(self.local_id)?.active {
            return Err(TransportError::Shutdown);
        }
        if parcel.run_id != state.run_id {
            return Err(TransportError::Protocol(
                crate::protocol::ProtocolError::RunMismatch,
            ));
        }
        if parcel.source != self.local_id {
            return Err(TransportError::Peer(
                "parcel source does not match handle".into(),
            ));
        }
        parcel.validate(state.limits)?;
        state.node(parcel.destination)?;
        if state.node(self.local_id)?.system.len() >= state.limits.max_queued_parcels_per_peer {
            return Err(TransportError::QueueFull {
                channel: Channel::Control,
                limit: state.limits.max_queued_parcels_per_peer,
            });
        }
        let bytes = parcel.payload_len()?;
        let ticket = SendTicket::new(state.next_ticket);
        state.next_ticket = state
            .next_ticket
            .checked_add(1)
            .ok_or_else(|| TransportError::Peer("send ticket overflow".into()))?;
        let fault = state.faults.pop_front().unwrap_or(MemoryFault::Pass);
        match fault {
            MemoryFault::Saturate => {
                return Err(TransportError::QueueFull {
                    channel: parcel.channel,
                    limit: state.limits.max_queued_parcels_per_peer,
                });
            }
            MemoryFault::Disconnect => {
                state
                    .node_mut(self.local_id)?
                    .system
                    .push_back(TransportEvent::PeerFailed {
                        peer: parcel.destination,
                        error: TransportError::Disconnected(parcel.destination),
                    });
                state.node_mut(self.local_id)?.stats.peer_failures += 1;
            }
            MemoryFault::Loss => {}
            MemoryFault::Delay { progress_ticks } => {
                state.reserve(&parcel, bytes, 1)?;
                let destination = parcel.destination;
                let channel = parcel.channel as usize;
                let node = state.node_mut(destination)?;
                node.delayed_counts[channel] += 1;
                node.queued_bytes += bytes;
                let release_at = state.tick.saturating_add(progress_ticks.max(1));
                state.delayed.push_back(Delayed {
                    release_at,
                    destination: parcel.destination,
                    parcel,
                    bytes,
                });
            }
            MemoryFault::Duplicate => {
                state.reserve(&parcel, bytes, 2)?;
                state.enqueue(parcel.clone(), bytes, false)?;
                state.enqueue(parcel, bytes, false)?;
            }
            MemoryFault::Reorder => {
                state.reserve(&parcel, bytes, 1)?;
                state.enqueue(parcel, bytes, true)?;
            }
            MemoryFault::Pass => {
                state.reserve(&parcel, bytes, 1)?;
                state.enqueue(parcel, bytes, false)?;
            }
        }
        let source = state.node_mut(self.local_id)?;
        source.stats.submitted_parcels += 1;
        source.stats.submitted_bytes += bytes as u64;
        source
            .system
            .push_back(TransportEvent::LocalSendComplete { ticket });
        Ok(ticket)
    }
}

pub struct MemoryDriver {
    local_id: LocalityId,
    members: Vec<LocalityId>,
    state: Arc<Mutex<State>>,
    shutdown: bool,
}

impl std::fmt::Debug for MemoryDriver {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MemoryDriver")
            .field("local_id", &self.local_id)
            .field("members", &self.members)
            .field("stats", &self.stats())
            .field("shutdown", &self.shutdown)
            .finish()
    }
}

impl TransportDriver for MemoryDriver {
    fn local_id(&self) -> LocalityId {
        self.local_id
    }

    fn members(&self) -> &[LocalityId] {
        &self.members
    }

    fn capabilities(&self) -> u64 {
        0
    }

    fn progress(
        &mut self,
        events: &mut Vec<TransportEvent>,
        max_events: usize,
    ) -> Result<Progress, TransportError> {
        if self.shutdown {
            return Err(TransportError::Shutdown);
        }
        let mut state = self.state.lock().unwrap();
        state.tick = state.tick.saturating_add(1);
        state.release_delayed()?;
        let start = events.len();
        let node = state.node_mut(self.local_id)?;
        while events.len() - start < max_events {
            let event = node
                .system
                .pop_front()
                .or_else(|| node.incoming[Channel::Control as usize].pop_front())
                .or_else(|| node.incoming[Channel::Action as usize].pop_front())
                .or_else(|| node.incoming[Channel::Bulk as usize].pop_front());
            let Some(event) = event else { break };
            if let TransportEvent::Incoming { parcel } = &event {
                let bytes = parcel.payload_len()?;
                node.release(parcel.channel, bytes);
                node.stats.received_parcels += 1;
                node.stats.received_bytes += bytes as u64;
            }
            events.push(event);
        }
        let count = events.len() - start;
        Ok(Progress {
            events: count,
            made_progress: count > 0,
        })
    }

    fn flush(&mut self, timeout: Duration) -> Result<(), TransportError> {
        let deadline = Instant::now() + timeout;
        loop {
            let mut state = self.state.lock().unwrap();
            state.tick = state.tick.saturating_add(1);
            state.release_delayed()?;
            let pending = state
                .delayed
                .iter()
                .any(|item| item.parcel.source == self.local_id);
            if !pending {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(TransportError::Timeout("memory flush"));
            }
            drop(state);
            std::thread::yield_now();
        }
    }

    fn shutdown(
        &mut self,
        events: &mut Vec<TransportEvent>,
        timeout: Duration,
    ) -> Result<(), TransportError> {
        self.state.lock().unwrap().node_mut(self.local_id)?.active = false;
        self.flush(timeout)?;
        let mut state = self.state.lock().unwrap();
        state.cancel_locality(self.local_id)?;
        let node = state.node_mut(self.local_id)?;
        node.active = false;
        node.clear();
        self.shutdown = true;
        events.push(TransportEvent::ShutdownComplete);
        Ok(())
    }

    fn stats(&self) -> TransportStats {
        self.state
            .lock()
            .ok()
            .and_then(|state| state.node(self.local_id).ok().map(|node| node.stats()))
            .unwrap_or_default()
    }
}

#[derive(Debug)]
struct Delayed {
    release_at: u64,
    destination: LocalityId,
    parcel: Parcel,
    bytes: usize,
}

#[derive(Debug)]
struct State {
    run_id: RunId,
    limits: ProtocolLimits,
    tick: u64,
    next_ticket: u64,
    faults: VecDeque<MemoryFault>,
    delayed: VecDeque<Delayed>,
    nodes: Vec<Node>,
}

impl State {
    fn index(&self, locality: LocalityId) -> Result<usize, TransportError> {
        let index: usize = locality
            .get()
            .try_into()
            .map_err(|_| TransportError::InvalidDestination(locality))?;
        (index < self.nodes.len())
            .then_some(index)
            .ok_or(TransportError::InvalidDestination(locality))
    }

    fn node(&self, locality: LocalityId) -> Result<&Node, TransportError> {
        Ok(&self.nodes[self.index(locality)?])
    }

    fn node_mut(&mut self, locality: LocalityId) -> Result<&mut Node, TransportError> {
        let index = self.index(locality)?;
        Ok(&mut self.nodes[index])
    }

    fn reserve(&self, parcel: &Parcel, bytes: usize, copies: usize) -> Result<(), TransportError> {
        let node = self.node(parcel.destination)?;
        if !node.active {
            return Err(TransportError::Disconnected(parcel.destination));
        }
        let channel = parcel.channel as usize;
        let count = node.incoming[channel]
            .len()
            .checked_add(node.delayed_counts[channel])
            .and_then(|value| value.checked_add(copies))
            .ok_or(TransportError::ByteLimit {
                actual: usize::MAX,
                limit: self.limits.max_queued_parcels_per_peer,
            })?;
        if count > self.limits.max_queued_parcels_per_peer {
            return Err(TransportError::QueueFull {
                channel: parcel.channel,
                limit: self.limits.max_queued_parcels_per_peer,
            });
        }
        let added = bytes.checked_mul(copies).ok_or(TransportError::ByteLimit {
            actual: usize::MAX,
            limit: self.limits.max_inflight_bytes_per_peer,
        })?;
        let total = node
            .queued_bytes
            .checked_add(added)
            .ok_or(TransportError::ByteLimit {
                actual: usize::MAX,
                limit: self.limits.max_inflight_bytes_per_peer,
            })?;
        let byte_limit = if parcel.channel == Channel::Control {
            self.limits.max_inflight_bytes_per_peer
        } else {
            self.limits.max_inflight_bytes_per_peer - self.limits.control_reserved_bytes_per_peer
        };
        if total > byte_limit {
            return Err(TransportError::ByteLimit {
                actual: total,
                limit: byte_limit,
            });
        }
        Ok(())
    }

    fn enqueue(&mut self, parcel: Parcel, bytes: usize, front: bool) -> Result<(), TransportError> {
        let node = self.node_mut(parcel.destination)?;
        node.queued_bytes += bytes;
        let queue = &mut node.incoming[parcel.channel as usize];
        let event = TransportEvent::Incoming { parcel };
        if front {
            queue.push_front(event);
        } else {
            queue.push_back(event);
        }
        Ok(())
    }

    fn cancel_locality(&mut self, locality: LocalityId) -> Result<(), TransportError> {
        let mut retained = VecDeque::with_capacity(self.delayed.len());
        while let Some(item) = self.delayed.pop_front() {
            if item.parcel.source == locality || item.destination == locality {
                if item.destination != locality {
                    let node = self.node_mut(item.destination)?;
                    node.delayed_counts[item.parcel.channel as usize] -= 1;
                    node.queued_bytes -= item.bytes;
                }
            } else {
                retained.push_back(item);
            }
        }
        self.delayed = retained;
        Ok(())
    }

    fn release_delayed(&mut self) -> Result<(), TransportError> {
        let mut retained = VecDeque::with_capacity(self.delayed.len());
        while let Some(item) = self.delayed.pop_front() {
            if item.release_at <= self.tick {
                let node = self.node_mut(item.destination)?;
                node.delayed_counts[item.parcel.channel as usize] -= 1;
                let queue = &mut node.incoming[item.parcel.channel as usize];
                queue.push_back(TransportEvent::Incoming {
                    parcel: item.parcel,
                });
            } else {
                retained.push_back(item);
            }
        }
        self.delayed = retained;
        Ok(())
    }
}

#[derive(Debug)]
struct Node {
    active: bool,
    system: VecDeque<TransportEvent>,
    incoming: [VecDeque<TransportEvent>; 3],
    delayed_counts: [usize; 3],
    queued_bytes: usize,
    stats: TransportStats,
}

impl Default for Node {
    fn default() -> Self {
        Self {
            active: true,
            system: VecDeque::new(),
            incoming: std::array::from_fn(|_| VecDeque::new()),
            delayed_counts: [0; 3],
            queued_bytes: 0,
            stats: TransportStats::default(),
        }
    }
}

impl Node {
    fn release(&mut self, _channel: Channel, bytes: usize) {
        self.queued_bytes -= bytes;
        self.stats.queued_bytes = self.queued_bytes;
    }

    fn stats(&self) -> TransportStats {
        let mut stats = self.stats;
        stats.queued_control = self.incoming[Channel::Control as usize].len()
            + self.delayed_counts[Channel::Control as usize];
        stats.queued_action = self.incoming[Channel::Action as usize].len()
            + self.delayed_counts[Channel::Action as usize];
        stats.queued_bulk = self.incoming[Channel::Bulk as usize].len()
            + self.delayed_counts[Channel::Bulk as usize];
        stats.queued_bytes = self.queued_bytes;
        stats.pending_events = self.system.len();
        stats
    }

    fn clear(&mut self) {
        self.system.clear();
        for queue in &mut self.incoming {
            queue.clear();
        }
        self.delayed_counts = [0; 3];
        self.queued_bytes = 0;
        self.stats.queued_control = 0;
        self.stats.queued_action = 0;
        self.stats.queued_bulk = 0;
        self.stats.queued_bytes = 0;
    }
}

#[cfg(test)]
mod tests;
