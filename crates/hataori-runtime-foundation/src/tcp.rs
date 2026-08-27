//! Fixed-membership TCP transport with one bounded stream per logical channel.

mod rendezvous;
pub use rendezvous::{TcpMembership, TcpRendezvous, TcpRendezvousConfig};

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
use std::{
    collections::VecDeque,
    io::{self, Read, Write},
    net::{Shutdown, SocketAddr, TcpListener, TcpStream},
    sync::{
        mpsc::{Receiver, SyncSender, TryRecvError, TrySendError},
        Arc,
    },
    thread,
    time::{Duration, Instant},
};

const LENGTH_BYTES: usize = 8;
const BOOTSTRAP_HEADER_BYTES: usize = 13;
const IO_CHUNK_BYTES: usize = 64 * 1024;

#[derive(Clone, Debug)]
pub struct TcpConfig {
    pub local_id: LocalityId,
    pub endpoints: Vec<SocketAddr>,
    pub hello: Hello,
    pub bootstrap_timeout: Duration,
    pub io_timeout: Duration,
}

#[derive(Debug)]
pub struct TcpTransport;

impl TcpTransport {
    pub fn connect(config: TcpConfig) -> Result<(TcpHandle, TcpDriver), TransportError> {
        let limits = config.hello.limits.validate()?;
        let local_index = member_index(config.local_id, config.endpoints.len())?;
        if config.endpoints.is_empty() {
            return Err(TransportError::Peer("TCP membership is empty".into()));
        }
        let mut unique = config.endpoints.clone();
        unique.sort_unstable();
        unique.dedup();
        if unique.len() != config.endpoints.len() {
            return Err(TransportError::Peer("TCP endpoints must be unique".into()));
        }
        let listener = TcpListener::bind(config.endpoints[local_index]).map_err(io_error)?;
        listener.set_nonblocking(true).map_err(io_error)?;
        let deadline = Instant::now() + config.bootstrap_timeout;
        let members: Vec<_> = (0..config.endpoints.len() as u64)
            .map(LocalityId::new)
            .collect();
        let mut peer_io: Vec<PeerIo> = (0..members.len()).map(|_| PeerIo::default()).collect();
        let mut capabilities = config.hello.capabilities;

        for peer_index in 0..local_index {
            let peer = members[peer_index];
            for channel in [Channel::Control, Channel::Action, Channel::Bulk] {
                let mut stream = connect_until(config.endpoints[peer_index], deadline)?;
                capabilities &= bootstrap_exchange(
                    &mut stream,
                    config.local_id,
                    peer,
                    channel,
                    &config.hello,
                    config.io_timeout,
                    true,
                )?;
                peer_io[peer_index].channels[channel as usize] = Some(StreamIo::new(stream)?);
            }
        }

        let expected_accepts = (members.len() - local_index - 1) * 3;
        let mut accepted = 0;
        while accepted < expected_accepts {
            if Instant::now() >= deadline {
                return Err(TransportError::Timeout("TCP bootstrap accept"));
            }
            match listener.accept() {
                Ok((mut stream, _)) => {
                    let (peer, channel, peer_capabilities) = bootstrap_exchange_accept(
                        &mut stream,
                        config.local_id,
                        &config.hello,
                        config.io_timeout,
                    )?;
                    capabilities &= peer_capabilities;
                    let peer_index = member_index(peer, members.len())?;
                    if peer_index <= local_index
                        || peer_io[peer_index].channels[channel as usize].is_some()
                    {
                        return Err(TransportError::Peer(
                            "unexpected or duplicate TCP bootstrap peer".into(),
                        ));
                    }
                    peer_io[peer_index].channels[channel as usize] = Some(StreamIo::new(stream)?);
                    accepted += 1;
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(1));
                }
                Err(error) => return Err(io_error(error)),
            }
        }
        drop(listener);

        for (index, peer) in peer_io.iter().enumerate() {
            if index != local_index && peer.channels.iter().any(Option::is_none) {
                return Err(TransportError::Peer(
                    "TCP bootstrap did not establish every channel".into(),
                ));
            }
        }

        let shared = Arc::new(SubmissionState::new(members.len(), limits));
        let mut senders = Vec::with_capacity(members.len());
        let mut receivers = Vec::with_capacity(members.len());
        for _ in 0..members.len() {
            let (peer_senders, peer_receivers) =
                submission_channels(limits.max_queued_parcels_per_peer);
            senders.push(peer_senders);
            receivers.push(peer_receivers);
        }
        let handle = TcpHandle {
            local_id: config.local_id,
            run_id: config.hello.run_id,
            members: members.clone(),
            limits,
            senders,
            shared: Arc::clone(&shared),
        };
        let driver = TcpDriver {
            local_id: config.local_id,
            run_id: config.hello.run_id,
            members,
            capabilities,
            limits,
            receivers,
            peers: peer_io,
            pending_events: VecDeque::new(),
            shared,
            failed: None,
            shutdown: false,
        };
        Ok((handle, driver))
    }
}

#[derive(Clone)]
pub struct TcpHandle {
    local_id: LocalityId,
    run_id: crate::protocol::RunId,
    members: Vec<LocalityId>,
    limits: ProtocolLimits,
    senders: Vec<[SyncSender<Submission>; 3]>,
    shared: Arc<SubmissionState>,
}

impl std::fmt::Debug for TcpHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TcpHandle")
            .field("local_id", &self.local_id)
            .field("members", &self.members)
            .finish_non_exhaustive()
    }
}

impl TransportHandle for TcpHandle {
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
        let peer_index = member_index(parcel.destination, self.members.len())?;
        if parcel.destination == self.local_id {
            return Err(TransportError::InvalidDestination(parcel.destination));
        }
        parcel.validate(self.limits)?;
        let reserved_bytes =
            parcel
                .encoded_len()?
                .checked_add(LENGTH_BYTES)
                .ok_or(TransportError::ByteLimit {
                    actual: usize::MAX,
                    limit: self.limits.max_inflight_bytes_per_peer,
                })?;
        let channel = parcel.channel;
        let permit = self.shared.reserve(peer_index, channel, reserved_bytes)?;
        let ticket = permit.ticket();
        let submission = Submission {
            ticket,
            parcel,
            reserved_bytes,
        };
        match self.senders[peer_index][channel as usize].try_send(submission) {
            Ok(()) => {
                permit.commit();
                Ok(ticket)
            }
            Err(TrySendError::Full(_)) => Err(TransportError::QueueFull {
                channel,
                limit: self.limits.max_queued_parcels_per_peer,
            }),
            Err(TrySendError::Disconnected(_)) => {
                Err(TransportError::Disconnected(parcel_destination(peer_index)))
            }
        }
    }
}

pub struct TcpDriver {
    local_id: LocalityId,
    run_id: crate::protocol::RunId,
    members: Vec<LocalityId>,
    capabilities: u64,
    limits: ProtocolLimits,
    receivers: Vec<[Receiver<Submission>; 3]>,
    peers: Vec<PeerIo>,
    pending_events: VecDeque<TransportEvent>,
    shared: Arc<SubmissionState>,
    failed: Option<TransportError>,
    shutdown: bool,
}

impl std::fmt::Debug for TcpDriver {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TcpDriver")
            .field("local_id", &self.local_id)
            .field("members", &self.members)
            .field("capabilities", &self.capabilities)
            .field("stats", &self.stats())
            .field("failed", &self.failed)
            .field("shutdown", &self.shutdown)
            .finish()
    }
}

impl TcpDriver {
    fn drive(
        &mut self,
        events: &mut Vec<TransportEvent>,
        max_events: usize,
    ) -> Result<bool, TransportError> {
        let start = events.len();
        let local_index = member_index(self.local_id, self.members.len())?;
        let mut moved = false;
        for channel in [Channel::Control, Channel::Action, Channel::Bulk] {
            for peer_index in 0..self.members.len() {
                if peer_index == local_index {
                    continue;
                }
                let stream = self.peers[peer_index].channels[channel as usize]
                    .as_mut()
                    .ok_or_else(|| TransportError::Peer("missing TCP channel".into()))?;
                if stream.writes.len() < self.limits.max_queued_parcels_per_peer {
                    match self.receivers[peer_index][channel as usize].try_recv() {
                        Ok(submission) => match encode_submission(submission, self.limits) {
                            Ok(frame) => {
                                stream.writes.push_back(frame);
                                moved = true;
                            }
                            Err((ticket, reserved_bytes, error)) => {
                                self.shared.release(peer_index, channel, reserved_bytes);
                                if events.len() - start < max_events {
                                    events.push(TransportEvent::SendFailed { ticket, error });
                                } else {
                                    self.pending_events
                                        .push_back(TransportEvent::SendFailed { ticket, error });
                                }
                            }
                        },
                        Err(TryRecvError::Empty) => {}
                        Err(TryRecvError::Disconnected) => {}
                    }
                }
                if events.len() - start < max_events {
                    moved |= service_write(stream, peer_index, channel, &self.shared, events)?;
                }
                if events.len() - start < max_events {
                    let remaining = max_events - (events.len() - start);
                    let event_limit = events.len() + remaining;
                    moved |= service_read(
                        stream,
                        ConnectionIdentity {
                            local: self.local_id,
                            peer: self.members[peer_index],
                            run_id: self.run_id,
                        },
                        self.limits,
                        &self.shared,
                        events,
                        event_limit,
                    )?;
                }
            }
        }
        Ok(moved)
    }

    fn fail(&mut self, error: TransportError) {
        for peer_index in 0..self.receivers.len() {
            for channel in [Channel::Control, Channel::Action, Channel::Bulk] {
                while let Ok(submission) = self.receivers[peer_index][channel as usize].try_recv() {
                    self.shared
                        .release(peer_index, channel, submission.reserved_bytes);
                }
                if let Some(stream) = self.peers[peer_index].channels[channel as usize].as_mut() {
                    while let Some(frame) = stream.writes.pop_front() {
                        self.shared
                            .release(peer_index, channel, frame.reserved_bytes);
                    }
                    stream.read.clear();
                }
            }
        }
        self.shared.record_peer_failure();
        self.shared.close();
        self.failed = Some(error);
    }

    fn pending_capacity(&self) -> usize {
        self.limits
            .max_queued_parcels_per_peer
            .saturating_mul(self.members.len().saturating_sub(1))
            .saturating_mul(3)
            .saturating_mul(2)
    }

    fn outbound_empty(&self) -> bool {
        let local_index = self.local_id.get() as usize;
        self.peers.iter().enumerate().all(|(peer, io)| {
            peer == local_index
                || io
                    .channels
                    .iter()
                    .flatten()
                    .all(|stream| stream.writes.is_empty())
        }) && self.shared.outstanding_count() == 0
    }
}

impl TransportDriver for TcpDriver {
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
                return Err(TransportError::Timeout("TCP flush"));
            }
            thread::yield_now();
        }
        Ok(())
    }

    fn shutdown(
        &mut self,
        events: &mut Vec<TransportEvent>,
        timeout: Duration,
    ) -> Result<(), TransportError> {
        self.shared.close();
        if self.failed.is_none() {
            self.flush(timeout)?;
        }
        let partial_read = self.peers.iter().any(|peer| {
            peer.channels
                .iter()
                .flatten()
                .any(|stream| !stream.read.is_empty())
        });
        for peer in &mut self.peers {
            for stream in peer.channels.iter_mut().flatten() {
                let _ = stream.stream.shutdown(Shutdown::Both);
                stream.read.clear();
                stream.writes.clear();
            }
        }
        if partial_read {
            let error = TransportError::Peer("TCP shutdown found a partial frame".into());
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
        for peer in &self.peers {
            for stream in peer.channels.iter().flatten() {
                stats.retained_read_bytes += stream.read.len();
            }
        }
        add_pending_event_stats(&mut stats, &self.pending_events);
        stats
    }
}

#[derive(Debug)]
struct WriteFrame {
    bytes: Vec<u8>,
    offset: usize,
    ticket: SendTicket,
    reserved_bytes: usize,
}

#[derive(Debug, Default)]
struct PeerIo {
    channels: [Option<StreamIo>; 3],
}

#[derive(Debug)]
struct StreamIo {
    stream: TcpStream,
    writes: VecDeque<WriteFrame>,
    read: Vec<u8>,
}

impl StreamIo {
    fn new(stream: TcpStream) -> Result<Self, TransportError> {
        stream.set_nonblocking(true).map_err(io_error)?;
        stream.set_nodelay(true).map_err(io_error)?;
        Ok(Self {
            stream,
            writes: VecDeque::new(),
            read: Vec::new(),
        })
    }
}

fn encode_submission(
    submission: Submission,
    limits: ProtocolLimits,
) -> Result<WriteFrame, (SendTicket, usize, TransportError)> {
    let ticket = submission.ticket;
    let reserved_bytes = submission.reserved_bytes;
    let encoded = encode_parcel(&submission.parcel, limits)
        .map_err(|error| (ticket, reserved_bytes, TransportError::Protocol(error)))?;
    let length: u64 = encoded.len().try_into().map_err(|_| {
        (
            ticket,
            reserved_bytes,
            TransportError::ByteLimit {
                actual: usize::MAX,
                limit: limits.max_frame_bytes,
            },
        )
    })?;
    let mut bytes = Vec::with_capacity(LENGTH_BYTES + encoded.len());
    bytes.extend_from_slice(&length.to_le_bytes());
    bytes.extend_from_slice(&encoded);
    Ok(WriteFrame {
        bytes,
        offset: 0,
        ticket,
        reserved_bytes,
    })
}

fn service_write(
    stream: &mut StreamIo,
    peer: usize,
    channel: Channel,
    shared: &SubmissionState,
    events: &mut Vec<TransportEvent>,
) -> Result<bool, TransportError> {
    let Some(frame) = stream.writes.front_mut() else {
        return Ok(false);
    };
    match stream.stream.write(&frame.bytes[frame.offset..]) {
        Ok(0) => Err(TransportError::Disconnected(parcel_destination(peer))),
        Ok(written) => {
            frame.offset += written;
            if frame.offset == frame.bytes.len() {
                let frame = stream.writes.pop_front().unwrap();
                shared.release(peer, channel, frame.reserved_bytes);
                events.push(TransportEvent::LocalSendComplete {
                    ticket: frame.ticket,
                });
            }
            Ok(true)
        }
        Err(error) if error.kind() == io::ErrorKind::WouldBlock => Ok(false),
        Err(error) => Err(io_error(error)),
    }
}

#[derive(Clone, Copy)]
struct ConnectionIdentity {
    local: LocalityId,
    peer: LocalityId,
    run_id: crate::protocol::RunId,
}

fn service_read(
    stream: &mut StreamIo,
    identity: ConnectionIdentity,
    limits: ProtocolLimits,
    shared: &SubmissionState,
    events: &mut Vec<TransportEvent>,
    max_events: usize,
) -> Result<bool, TransportError> {
    let mut moved = false;
    let max_retained =
        limits
            .max_frame_bytes
            .checked_add(LENGTH_BYTES)
            .ok_or(TransportError::ByteLimit {
                actual: usize::MAX,
                limit: limits.max_frame_bytes,
            })?;
    let mut chunk = [0_u8; IO_CHUNK_BYTES];
    loop {
        match stream.stream.read(&mut chunk) {
            Ok(0) => return Err(TransportError::Disconnected(identity.peer)),
            Ok(read) => {
                if stream.read.len() + read > max_retained {
                    return Err(TransportError::ByteLimit {
                        actual: stream.read.len() + read,
                        limit: max_retained,
                    });
                }
                stream.read.extend_from_slice(&chunk[..read]);
                moved = true;
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
            Err(error) => return Err(io_error(error)),
        }
    }
    while events.len() < max_events {
        if stream.read.len() < LENGTH_BYTES {
            break;
        }
        let frame_len: usize = u64::from_le_bytes(stream.read[..8].try_into().unwrap())
            .try_into()
            .map_err(|_| TransportError::ByteLimit {
                actual: usize::MAX,
                limit: limits.max_frame_bytes,
            })?;
        if frame_len > limits.max_frame_bytes {
            return Err(TransportError::ByteLimit {
                actual: frame_len,
                limit: limits.max_frame_bytes,
            });
        }
        let total = LENGTH_BYTES
            .checked_add(frame_len)
            .ok_or(TransportError::ByteLimit {
                actual: usize::MAX,
                limit: limits.max_frame_bytes,
            })?;
        if stream.read.len() < total {
            break;
        }
        let parcel = decode_parcel(&stream.read[LENGTH_BYTES..total], limits)?;
        stream.read.drain(..total);
        if parcel.run_id != identity.run_id {
            return Err(TransportError::Protocol(
                crate::protocol::ProtocolError::RunMismatch,
            ));
        }
        if parcel.source != identity.peer || parcel.destination != identity.local {
            return Err(TransportError::Peer(
                "TCP parcel source/destination does not match connection".into(),
            ));
        }
        let bytes = parcel.payload_len()?;
        shared.record_received(bytes);
        events.push(TransportEvent::Incoming { parcel });
    }
    Ok(moved)
}

fn connect_until(endpoint: SocketAddr, deadline: Instant) -> Result<TcpStream, TransportError> {
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(TransportError::Timeout("TCP bootstrap connect"));
        }
        match TcpStream::connect_timeout(&endpoint, remaining.min(Duration::from_millis(100))) {
            Ok(stream) => return Ok(stream),
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::ConnectionRefused
                        | io::ErrorKind::TimedOut
                        | io::ErrorKind::AddrNotAvailable
                ) =>
            {
                thread::sleep(Duration::from_millis(1));
            }
            Err(error) => return Err(io_error(error)),
        }
    }
}

fn bootstrap_exchange(
    stream: &mut TcpStream,
    local: LocalityId,
    peer: LocalityId,
    channel: Channel,
    hello: &Hello,
    timeout: Duration,
    initiator: bool,
) -> Result<u64, TransportError> {
    stream.set_read_timeout(Some(timeout)).map_err(io_error)?;
    stream.set_write_timeout(Some(timeout)).map_err(io_error)?;
    if initiator {
        write_bootstrap(stream, local, channel, hello)?;
        let (actual_peer, actual_channel, peer_hello) = read_bootstrap(stream)?;
        if actual_peer != peer || actual_channel != channel {
            return Err(TransportError::Peer(
                "TCP handshake identity mismatch".into(),
            ));
        }
        return Ok(hello.validate_peer(&peer_hello)?.capabilities);
    }
    Ok(hello.capabilities)
}

fn bootstrap_exchange_accept(
    stream: &mut TcpStream,
    local: LocalityId,
    hello: &Hello,
    timeout: Duration,
) -> Result<(LocalityId, Channel, u64), TransportError> {
    stream.set_read_timeout(Some(timeout)).map_err(io_error)?;
    stream.set_write_timeout(Some(timeout)).map_err(io_error)?;
    let (peer, channel, peer_hello) = read_bootstrap(stream)?;
    let capabilities = hello.validate_peer(&peer_hello)?.capabilities;
    write_bootstrap(stream, local, channel, hello)?;
    Ok((peer, channel, capabilities))
}

fn write_bootstrap(
    stream: &mut TcpStream,
    local: LocalityId,
    channel: Channel,
    hello: &Hello,
) -> Result<(), TransportError> {
    let encoded = encode_hello(hello)?;
    let length: u32 = encoded
        .len()
        .try_into()
        .map_err(|_| TransportError::Peer("hello is too large".into()))?;
    let mut frame = Vec::with_capacity(BOOTSTRAP_HEADER_BYTES + encoded.len());
    frame.extend_from_slice(&local.get().to_le_bytes());
    frame.push(channel as u8);
    frame.extend_from_slice(&length.to_le_bytes());
    frame.extend_from_slice(&encoded);
    stream.write_all(&frame).map_err(io_error)
}

fn read_bootstrap(stream: &mut TcpStream) -> Result<(LocalityId, Channel, Hello), TransportError> {
    let mut header = [0_u8; BOOTSTRAP_HEADER_BYTES];
    stream.read_exact(&mut header).map_err(io_error)?;
    let peer = LocalityId::new(u64::from_le_bytes(header[..8].try_into().unwrap()));
    let channel = Channel::try_from(header[8])?;
    let length = u32::from_le_bytes(header[9..13].try_into().unwrap()) as usize;
    if length > 4096 {
        return Err(TransportError::ByteLimit {
            actual: length,
            limit: 4096,
        });
    }
    let mut encoded = vec![0_u8; length];
    stream.read_exact(&mut encoded).map_err(io_error)?;
    Ok((peer, channel, decode_hello(&encoded)?))
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

fn parcel_destination(index: usize) -> LocalityId {
    LocalityId::new(index as u64)
}

fn io_error(error: io::Error) -> TransportError {
    TransportError::Io(error.to_string())
}

#[cfg(test)]
mod tests;
