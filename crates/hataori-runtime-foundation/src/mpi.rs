//! MPI transport with backend-private ranks, communicator, tags, and chunking.

use crate::{
    protocol::{
        decode_hello, decode_parcel, encode_hello, encode_parcel, Channel, Hello, LocalityId,
        Parcel, ProtocolLimits,
    },
    transport::{
        add_pending_event_stats, submission_channels, Progress, SendTicket, Submission,
        SubmissionState, TransportDriver, TransportError, TransportEvent, TransportHandle,
        TransportStats,
    },
};
use mpi::{
    collective::CommunicatorCollectives,
    point_to_point::{Destination, Source},
    topology::{Communicator, SimpleCommunicator},
};
use std::{
    collections::VecDeque,
    sync::{
        mpsc::{Receiver, SyncSender, TryRecvError, TrySendError},
        Arc,
    },
    time::{Duration, Instant},
};

const CHUNK_MAGIC: [u8; 4] = *b"HMPC";
const CHUNK_VERSION: u8 = 1;
const CHUNK_HEADER_BYTES: usize = 29;
const CHUNK_PAYLOAD_BYTES: usize = 4 * 1024;
const CONTROL_TAG: i32 = 41;
const ACTION_TAG: i32 = 42;
const BULK_TAG: i32 = 43;

#[derive(Debug)]
pub struct MpiTransport;

impl MpiTransport {
    pub fn connect<C: Communicator>(
        world: &C,
        hello: Hello,
    ) -> Result<(MpiHandle, MpiDriver), TransportError> {
        require_main_thread()?;
        let local_limits = hello.limits.validate()?;
        let communicator = world.duplicate();
        let size: usize = communicator
            .size()
            .try_into()
            .map_err(|_| TransportError::Peer("invalid MPI world size".into()))?;
        let rank: usize = communicator
            .rank()
            .try_into()
            .map_err(|_| TransportError::Peer("invalid MPI rank".into()))?;
        if size == 0 || rank >= size {
            return Err(TransportError::Peer("invalid MPI membership".into()));
        }
        let encoded = encode_hello(&hello)?;
        let mut gathered = vec![0_u8; encoded.len() * size];
        communicator.all_gather_into(encoded.as_slice(), gathered.as_mut_slice());
        let mut limits = local_limits;
        let mut capabilities = hello.capabilities;
        for bytes in gathered.chunks_exact(encoded.len()) {
            let peer = decode_hello(bytes)?;
            let negotiated = Hello {
                limits,
                ..hello.clone()
            }
            .validate_peer(&peer)?;
            limits = negotiated.limits;
            capabilities &= negotiated.capabilities;
        }
        let members: Vec<_> = (0..size as u64).map(LocalityId::new).collect();
        let shared = Arc::new(SubmissionState::new(size, limits));
        let mut senders = Vec::with_capacity(size);
        let mut receivers = Vec::with_capacity(size);
        for _ in 0..size {
            let (peer_senders, peer_receivers) =
                submission_channels(limits.max_queued_parcels_per_peer);
            senders.push(peer_senders);
            receivers.push(peer_receivers);
        }
        let local_id = members[rank];
        Ok((
            MpiHandle {
                local_id,
                run_id: hello.run_id,
                members: members.clone(),
                limits,
                senders,
                shared: Arc::clone(&shared),
            },
            MpiDriver {
                local_id,
                run_id: hello.run_id,
                members,
                capabilities,
                limits,
                communicator,
                receivers,
                outbound: (0..size).map(|_| OutboundPeer::default()).collect(),
                reassembly: (0..size).map(|_| ReassemblyPeer::default()).collect(),
                pending_events: VecDeque::new(),
                shared,
                failed: None,
                shutdown: false,
            },
        ))
    }
}

#[derive(Clone)]
pub struct MpiHandle {
    local_id: LocalityId,
    run_id: crate::protocol::RunId,
    members: Vec<LocalityId>,
    limits: ProtocolLimits,
    senders: Vec<[SyncSender<Submission>; 3]>,
    shared: Arc<SubmissionState>,
}

impl std::fmt::Debug for MpiHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MpiHandle")
            .field("local_id", &self.local_id)
            .field("members", &self.members)
            .finish_non_exhaustive()
    }
}

impl TransportHandle for MpiHandle {
    fn try_send(&self, parcel: Parcel) -> Result<SendTicket, TransportError> {
        if parcel.run_id != self.run_id {
            return Err(TransportError::Protocol(
                crate::protocol::ProtocolError::RunMismatch,
            ));
        }
        if parcel.source != self.local_id {
            return Err(TransportError::Peer(
                "parcel source does not match handle".into(),
            ));
        }
        let peer = member_index(parcel.destination, self.members.len())?;
        if parcel.destination == self.local_id {
            return Err(TransportError::InvalidDestination(parcel.destination));
        }
        parcel.validate(self.limits)?;
        let reserved_bytes = parcel.encoded_len()?;
        let channel = parcel.channel;
        let permit = self.shared.reserve(peer, channel, reserved_bytes)?;
        let ticket = permit.ticket();
        match self.senders[peer][channel as usize].try_send(Submission {
            ticket,
            parcel,
            reserved_bytes,
        }) {
            Ok(()) => {
                permit.commit();
                Ok(ticket)
            }
            Err(TrySendError::Full(_)) => Err(TransportError::QueueFull {
                channel,
                limit: self.limits.max_queued_parcels_per_peer,
            }),
            Err(TrySendError::Disconnected(_)) => {
                Err(TransportError::Disconnected(LocalityId::new(peer as u64)))
            }
        }
    }
}

pub struct MpiDriver {
    local_id: LocalityId,
    run_id: crate::protocol::RunId,
    members: Vec<LocalityId>,
    capabilities: u64,
    limits: ProtocolLimits,
    communicator: SimpleCommunicator,
    receivers: Vec<[Receiver<Submission>; 3]>,
    outbound: Vec<OutboundPeer>,
    reassembly: Vec<ReassemblyPeer>,
    pending_events: VecDeque<TransportEvent>,
    shared: Arc<SubmissionState>,
    failed: Option<TransportError>,
    shutdown: bool,
}

impl std::fmt::Debug for MpiDriver {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MpiDriver")
            .field("local_id", &self.local_id)
            .field("members", &self.members)
            .field("capabilities", &self.capabilities)
            .field("stats", &self.stats())
            .field("failed", &self.failed)
            .field("shutdown", &self.shutdown)
            .finish()
    }
}

impl MpiDriver {
    fn drive(
        &mut self,
        events: &mut Vec<TransportEvent>,
        max_events: usize,
    ) -> Result<bool, TransportError> {
        require_main_thread()?;
        let start = events.len();
        let local = self.local_id.get() as usize;
        let mut moved = false;
        for channel in [Channel::Control, Channel::Action, Channel::Bulk] {
            while events.len() - start < max_events {
                let Some(status) = self
                    .communicator
                    .any_process()
                    .immediate_probe_with_tag(tag(channel))
                else {
                    break;
                };
                let source_rank = status.source_rank();
                let source: usize = source_rank
                    .try_into()
                    .map_err(|_| TransportError::Peer("invalid MPI source rank".into()))?;
                let (chunk, _) = self
                    .communicator
                    .process_at_rank(source_rank)
                    .receive_vec_with_tag::<u8>(tag(channel));
                moved = true;
                if let Some(parcel) = accept_chunk(
                    &mut self.reassembly[source].channels[channel as usize],
                    &chunk,
                    self.limits,
                )? {
                    if parcel.run_id != self.run_id {
                        return Err(TransportError::Protocol(
                            crate::protocol::ProtocolError::RunMismatch,
                        ));
                    }
                    if parcel.source != self.members[source] || parcel.destination != self.local_id
                    {
                        return Err(TransportError::Peer(
                            "MPI parcel identity does not match rank mapping".into(),
                        ));
                    }
                    let bytes = parcel.payload_len()?;
                    self.shared.record_received(bytes);
                    events.push(TransportEvent::Incoming { parcel });
                }
            }
            for peer in 0..self.members.len() {
                if peer == local {
                    continue;
                }
                if self.outbound[peer].channels[channel as usize].is_none() {
                    match self.receivers[peer][channel as usize].try_recv() {
                        Ok(submission) => match Outbound::new(submission, self.limits) {
                            Ok(outbound) => {
                                self.outbound[peer].channels[channel as usize] = Some(outbound);
                            }
                            Err((ticket, reserved_bytes, error)) => {
                                self.shared.release(peer, channel, reserved_bytes);
                                if events.len() - start < max_events {
                                    events.push(TransportEvent::SendFailed { ticket, error });
                                } else {
                                    self.pending_events
                                        .push_back(TransportEvent::SendFailed { ticket, error });
                                }
                            }
                        },
                        Err(TryRecvError::Empty | TryRecvError::Disconnected) => {}
                    }
                }
                if let Some(outbound) = self.outbound[peer].channels[channel as usize].as_mut() {
                    let chunk = outbound.next_chunk()?;
                    self.communicator
                        .process_at_rank(peer as i32)
                        .send_with_tag(chunk.as_slice(), tag(channel));
                    moved = true;
                    if outbound.complete() {
                        let outbound = self.outbound[peer].channels[channel as usize]
                            .take()
                            .unwrap();
                        self.shared.release(peer, channel, outbound.reserved_bytes);
                        if events.len() - start < max_events {
                            events.push(TransportEvent::LocalSendComplete {
                                ticket: outbound.ticket,
                            });
                        } else {
                            self.pending_events
                                .push_back(TransportEvent::LocalSendComplete {
                                    ticket: outbound.ticket,
                                });
                        }
                    }
                }
            }
        }
        Ok(moved)
    }

    fn pending_capacity(&self) -> usize {
        self.limits
            .max_queued_parcels_per_peer
            .saturating_mul(self.members.len().saturating_sub(1))
            .saturating_mul(3)
            .saturating_mul(2)
    }

    fn outbound_empty(&self) -> bool {
        self.shared.outstanding_count() == 0
            && self
                .outbound
                .iter()
                .all(|peer| peer.channels.iter().all(Option::is_none))
    }

    fn fail(&mut self, error: TransportError) {
        for peer in 0..self.receivers.len() {
            for channel in [Channel::Control, Channel::Action, Channel::Bulk] {
                while let Ok(submission) = self.receivers[peer][channel as usize].try_recv() {
                    self.shared
                        .release(peer, channel, submission.reserved_bytes);
                }
                if let Some(outbound) = self.outbound[peer].channels[channel as usize].take() {
                    self.shared.release(peer, channel, outbound.reserved_bytes);
                }
                self.reassembly[peer].channels[channel as usize] = None;
            }
        }
        self.shared.record_peer_failure();
        self.shared.close();
        self.failed = Some(error);
    }
}

impl TransportDriver for MpiDriver {
    fn local_id(&self) -> LocalityId {
        self.local_id
    }

    fn members(&self) -> &[LocalityId] {
        &self.members
    }

    fn capabilities(&self) -> u64 {
        self.capabilities
    }

    fn progress(
        &mut self,
        events: &mut Vec<TransportEvent>,
        max_events: usize,
    ) -> Result<Progress, TransportError> {
        if self.shutdown {
            return Err(TransportError::Shutdown);
        }
        if let Some(error) = &self.failed {
            return Err(error.clone());
        }
        let start = events.len();
        while events.len() - start < max_events {
            let Some(event) = self.pending_events.pop_front() else {
                break;
            };
            events.push(event);
        }
        let moved = if events.len() - start < max_events {
            match self.drive(events, max_events - (events.len() - start)) {
                Ok(moved) => moved,
                Err(error) => {
                    self.fail(error.clone());
                    return Err(error);
                }
            }
        } else {
            false
        };
        let count = events.len() - start;
        Ok(Progress {
            events: count,
            made_progress: moved || count > 0,
        })
    }

    fn flush(&mut self, timeout: Duration) -> Result<(), TransportError> {
        if let Some(error) = &self.failed {
            return Err(error.clone());
        }
        let deadline = Instant::now() + timeout;
        while !self.outbound_empty() {
            let mut generated = Vec::new();
            if let Err(error) = self.drive(&mut generated, self.pending_capacity()) {
                self.fail(error.clone());
                return Err(error);
            }
            if self.pending_events.len() + generated.len() > self.pending_capacity() {
                return Err(TransportError::QueueFull {
                    channel: Channel::Control,
                    limit: self.pending_capacity(),
                });
            }
            self.pending_events.extend(generated);
            if Instant::now() >= deadline {
                return Err(TransportError::Timeout("MPI flush"));
            }
            std::thread::yield_now();
        }
        Ok(())
    }

    fn shutdown(
        &mut self,
        events: &mut Vec<TransportEvent>,
        timeout: Duration,
    ) -> Result<(), TransportError> {
        require_main_thread()?;
        self.shared.close();
        if self.failed.is_none() {
            self.flush(timeout)?;
        }
        self.communicator.barrier();
        let partial_reassembly = self
            .reassembly
            .iter()
            .any(|peer| peer.channels.iter().any(Option::is_some));
        for peer in &mut self.reassembly {
            peer.channels = std::array::from_fn(|_| None);
        }
        if partial_reassembly {
            let error = TransportError::Peer("MPI shutdown found partial reassembly".into());
            self.failed = Some(error.clone());
            events.extend(self.pending_events.drain(..));
            self.shutdown = true;
            return Err(error);
        }
        events.extend(self.pending_events.drain(..));
        events.push(TransportEvent::ShutdownComplete);
        self.shared.close();
        self.shutdown = true;
        Ok(())
    }

    fn stats(&self) -> TransportStats {
        let mut stats = self.shared.stats();
        stats.retained_reassembly_bytes = self
            .reassembly
            .iter()
            .flat_map(|peer| peer.channels.iter().flatten())
            .map(|value| value.bytes.len())
            .sum();
        add_pending_event_stats(&mut stats, &self.pending_events);
        stats
    }
}

#[derive(Debug, Default)]
struct OutboundPeer {
    channels: [Option<Outbound>; 3],
}

#[derive(Debug)]
struct Outbound {
    ticket: SendTicket,
    bytes: Vec<u8>,
    offset: usize,
    chunk_index: u32,
    chunk_count: u32,
    reserved_bytes: usize,
}

impl Outbound {
    fn new(
        submission: Submission,
        limits: ProtocolLimits,
    ) -> Result<Self, (SendTicket, usize, TransportError)> {
        let ticket = submission.ticket;
        let reserved_bytes = submission.reserved_bytes;
        let bytes = encode_parcel(&submission.parcel, limits)
            .map_err(|error| (ticket, reserved_bytes, error.into()))?;
        let chunk_count_usize = bytes.len().div_ceil(CHUNK_PAYLOAD_BYTES);
        let chunk_count: u32 = chunk_count_usize.try_into().map_err(|_| {
            (
                ticket,
                reserved_bytes,
                TransportError::ByteLimit {
                    actual: chunk_count_usize,
                    limit: u32::MAX as usize,
                },
            )
        })?;
        Ok(Self {
            ticket: submission.ticket,
            bytes,
            offset: 0,
            chunk_index: 0,
            chunk_count,
            reserved_bytes: submission.reserved_bytes,
        })
    }

    fn next_chunk(&mut self) -> Result<Vec<u8>, TransportError> {
        let end = self
            .offset
            .checked_add(CHUNK_PAYLOAD_BYTES)
            .map(|end| end.min(self.bytes.len()))
            .ok_or(TransportError::ByteLimit {
                actual: usize::MAX,
                limit: self.bytes.len(),
            })?;
        let total: u64 = self
            .bytes
            .len()
            .try_into()
            .map_err(|_| TransportError::ByteLimit {
                actual: self.bytes.len(),
                limit: u64::MAX as usize,
            })?;
        let mut chunk = Vec::with_capacity(CHUNK_HEADER_BYTES + end - self.offset);
        chunk.extend_from_slice(&CHUNK_MAGIC);
        chunk.push(CHUNK_VERSION);
        chunk.extend_from_slice(&self.ticket.get().to_le_bytes());
        chunk.extend_from_slice(&total.to_le_bytes());
        chunk.extend_from_slice(&self.chunk_index.to_le_bytes());
        chunk.extend_from_slice(&self.chunk_count.to_le_bytes());
        chunk.extend_from_slice(&self.bytes[self.offset..end]);
        self.offset = end;
        self.chunk_index += 1;
        Ok(chunk)
    }

    fn complete(&self) -> bool {
        self.offset == self.bytes.len()
    }
}

#[derive(Debug, Default)]
struct ReassemblyPeer {
    channels: [Option<Reassembly>; 3],
}

#[derive(Debug)]
struct Reassembly {
    transfer: u64,
    total: usize,
    next_chunk: u32,
    chunk_count: u32,
    bytes: Vec<u8>,
}

fn accept_chunk(
    slot: &mut Option<Reassembly>,
    chunk: &[u8],
    limits: ProtocolLimits,
) -> Result<Option<Parcel>, TransportError> {
    if chunk.len() < CHUNK_HEADER_BYTES {
        return Err(TransportError::Peer("MPI chunk header is truncated".into()));
    }
    if chunk[..4] != CHUNK_MAGIC || chunk[4] != CHUNK_VERSION {
        return Err(TransportError::Peer(
            "MPI chunk magic/version mismatch".into(),
        ));
    }
    let transfer = u64::from_le_bytes(chunk[5..13].try_into().unwrap());
    let total: usize = u64::from_le_bytes(chunk[13..21].try_into().unwrap())
        .try_into()
        .map_err(|_| TransportError::ByteLimit {
            actual: usize::MAX,
            limit: limits.max_frame_bytes,
        })?;
    let index = u32::from_le_bytes(chunk[21..25].try_into().unwrap());
    let count = u32::from_le_bytes(chunk[25..29].try_into().unwrap());
    let expected_count_usize = total.div_ceil(CHUNK_PAYLOAD_BYTES);
    let expected_count: u32 =
        expected_count_usize
            .try_into()
            .map_err(|_| TransportError::ByteLimit {
                actual: expected_count_usize,
                limit: u32::MAX as usize,
            })?;
    if transfer == 0
        || count == 0
        || count != expected_count
        || index >= count
        || total > limits.max_frame_bytes
    {
        return Err(TransportError::Peer("MPI chunk metadata is invalid".into()));
    }
    let expected_payload = if index + 1 == count {
        total - index as usize * CHUNK_PAYLOAD_BYTES
    } else {
        CHUNK_PAYLOAD_BYTES
    };
    let actual_payload = chunk.len() - CHUNK_HEADER_BYTES;
    if actual_payload != expected_payload {
        return Err(TransportError::ByteLimit {
            actual: actual_payload,
            limit: expected_payload,
        });
    }
    if slot.is_none() {
        if index != 0 {
            return Err(TransportError::Peer(
                "MPI chunk sequence does not start at zero".into(),
            ));
        }
        *slot = Some(Reassembly {
            transfer,
            total,
            next_chunk: 0,
            chunk_count: count,
            bytes: Vec::with_capacity(total),
        });
    }
    let state = slot.as_mut().unwrap();
    if state.transfer != transfer
        || state.total != total
        || state.chunk_count != count
        || state.next_chunk != index
    {
        return Err(TransportError::Peer("MPI chunk sequence mismatch".into()));
    }
    state.bytes.extend_from_slice(&chunk[CHUNK_HEADER_BYTES..]);
    if state.bytes.len() > state.total {
        return Err(TransportError::ByteLimit {
            actual: state.bytes.len(),
            limit: state.total,
        });
    }
    state.next_chunk += 1;
    if state.next_chunk == state.chunk_count {
        let completed = slot.take().unwrap();
        if completed.bytes.len() != completed.total {
            return Err(TransportError::Peer(
                "MPI reassembly length mismatch".into(),
            ));
        }
        Ok(Some(decode_parcel(&completed.bytes, limits)?))
    } else {
        Ok(None)
    }
}

fn tag(channel: Channel) -> i32 {
    match channel {
        Channel::Control => CONTROL_TAG,
        Channel::Action => ACTION_TAG,
        Channel::Bulk => BULK_TAG,
    }
}

fn member_index(locality: LocalityId, count: usize) -> Result<usize, TransportError> {
    let index: usize = locality
        .get()
        .try_into()
        .map_err(|_| TransportError::InvalidDestination(locality))?;
    (index < count)
        .then_some(index)
        .ok_or(TransportError::InvalidDestination(locality))
}

fn require_main_thread() -> Result<(), TransportError> {
    let mut flag = 0;
    // SAFETY: MPI is initialized by the caller, `flag` points to writable `i32`,
    // and `MPI_Is_thread_main` neither retains the pointer nor crosses the call.
    let code = unsafe { mpi::ffi::MPI_Is_thread_main(&mut flag) };
    if code != mpi::ffi::MPI_SUCCESS as i32 || flag == 0 {
        Err(TransportError::WrongThread)
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests;
