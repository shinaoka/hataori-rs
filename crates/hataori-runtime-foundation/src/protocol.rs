//! Transport-independent identities, handshake records, parcels, and framing.

use std::{fmt, num::TryFromIntError};

const HELLO_MAGIC: [u8; 4] = *b"HTHL";
const PARCEL_MAGIC: [u8; 4] = *b"HTPR";
pub const PROTOCOL_VERSION: u32 = 1;
const HELLO_BYTES: usize = 142;
const PARCEL_FIXED_BYTES: usize = 87;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RunId(u128);

impl RunId {
    pub fn new(value: u128) -> Result<Self, ProtocolError> {
        (value != 0)
            .then_some(Self(value))
            .ok_or(ProtocolError::ZeroId("RunId"))
    }

    pub const fn get(self) -> u128 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct LocalityId(u64);

impl LocalityId {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

macro_rules! nonzero_u128_id {
    ($name:ident) => {
        #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name(u128);

        impl $name {
            pub fn new(value: u128) -> Result<Self, ProtocolError> {
                (value != 0)
                    .then_some(Self(value))
                    .ok_or(ProtocolError::ZeroId(stringify!($name)))
            }

            pub const fn get(self) -> u128 {
                self.0
            }
        }
    };
}

nonzero_u128_id!(MessageId);
nonzero_u128_id!(TaskId);
nonzero_u128_id!(ActionId);
nonzero_u128_id!(ObjectTypeId);
nonzero_u128_id!(TraceId);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DomainId(u32);

impl DomainId {
    pub const DEFAULT: Self = Self(0);

    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u32 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RequestId {
    pub origin: LocalityId,
    pub sequence: u64,
}

impl RequestId {
    pub fn new(origin: LocalityId, sequence: u64) -> Result<Self, ProtocolError> {
        (sequence != 0)
            .then_some(Self { origin, sequence })
            .ok_or(ProtocolError::ZeroId("RequestId.sequence"))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RuntimeVersion {
    pub major: u16,
    pub minor: u16,
    pub patch: u16,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum Channel {
    Control = 0,
    Action = 1,
    Bulk = 2,
}

impl TryFrom<u8> for Channel {
    type Error = ProtocolError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Control),
            1 => Ok(Self::Action),
            2 => Ok(Self::Bulk),
            _ => Err(ProtocolError::InvalidChannel(value)),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum ParcelKind {
    Control = 0,
    Data = 1,
    Shutdown = 2,
}

impl TryFrom<u8> for ParcelKind {
    type Error = ProtocolError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Control),
            1 => Ok(Self::Data),
            2 => Ok(Self::Shutdown),
            _ => Err(ProtocolError::InvalidKind(value)),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProtocolLimits {
    pub max_frame_bytes: usize,
    pub max_payload_bytes: usize,
    pub max_segments: usize,
    pub max_queued_parcels_per_peer: usize,
    pub max_inflight_bytes_per_peer: usize,
    pub control_reserved_bytes_per_peer: usize,
}

impl ProtocolLimits {
    pub fn validate(self) -> Result<Self, ProtocolError> {
        if self.max_frame_bytes < PARCEL_FIXED_BYTES
            || self.max_payload_bytes == 0
            || self.max_segments == 0
            || self.max_queued_parcels_per_peer == 0
            || self.max_inflight_bytes_per_peer == 0
            || self.control_reserved_bytes_per_peer == 0
            || self.control_reserved_bytes_per_peer >= self.max_inflight_bytes_per_peer
        {
            return Err(ProtocolError::InvalidLimits);
        }
        Ok(self)
    }

    pub fn negotiate(self, peer: Self) -> Result<Self, ProtocolError> {
        self.validate()?;
        peer.validate()?;
        Self {
            max_frame_bytes: self.max_frame_bytes.min(peer.max_frame_bytes),
            max_payload_bytes: self.max_payload_bytes.min(peer.max_payload_bytes),
            max_segments: self.max_segments.min(peer.max_segments),
            max_queued_parcels_per_peer: self
                .max_queued_parcels_per_peer
                .min(peer.max_queued_parcels_per_peer),
            max_inflight_bytes_per_peer: self
                .max_inflight_bytes_per_peer
                .min(peer.max_inflight_bytes_per_peer),
            control_reserved_bytes_per_peer: self
                .control_reserved_bytes_per_peer
                .min(peer.control_reserved_bytes_per_peer),
        }
        .validate()
    }
}

impl Default for ProtocolLimits {
    fn default() -> Self {
        Self {
            max_frame_bytes: 16 * 1024 * 1024,
            max_payload_bytes: 8 * 1024 * 1024,
            max_segments: 1024,
            max_queued_parcels_per_peer: 256,
            max_inflight_bytes_per_peer: 16 * 1024 * 1024,
            control_reserved_bytes_per_peer: 64 * 1024,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Hello {
    pub run_id: RunId,
    pub runtime_version: RuntimeVersion,
    pub action_registry_hash: [u8; 32],
    pub object_registry_hash: [u8; 32],
    pub capabilities: u64,
    pub limits: ProtocolLimits,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NegotiatedHello {
    pub capabilities: u64,
    pub limits: ProtocolLimits,
}

impl Hello {
    pub fn validate_peer(&self, peer: &Self) -> Result<NegotiatedHello, ProtocolError> {
        if self.run_id != peer.run_id {
            return Err(ProtocolError::RunMismatch);
        }
        if self.runtime_version != peer.runtime_version {
            return Err(ProtocolError::RuntimeVersionMismatch);
        }
        if self.action_registry_hash != peer.action_registry_hash {
            return Err(ProtocolError::ActionRegistryMismatch);
        }
        if self.object_registry_hash != peer.object_registry_hash {
            return Err(ProtocolError::ObjectRegistryMismatch);
        }
        Ok(NegotiatedHello {
            capabilities: self.capabilities & peer.capabilities,
            limits: self.limits.negotiate(peer.limits)?,
        })
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct Parcel {
    pub run_id: RunId,
    pub message_id: MessageId,
    pub channel: Channel,
    pub kind: ParcelKind,
    pub source: LocalityId,
    pub destination: LocalityId,
    pub trace_id: Option<TraceId>,
    pub segments: Vec<Vec<u8>>,
}

impl fmt::Debug for Parcel {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Parcel")
            .field("run_id", &self.run_id)
            .field("message_id", &self.message_id)
            .field("channel", &self.channel)
            .field("kind", &self.kind)
            .field("source", &self.source)
            .field("destination", &self.destination)
            .field("trace_id", &self.trace_id)
            .field("segments", &self.segments.len())
            .field("payload_bytes", &self.payload_len().unwrap_or(usize::MAX))
            .finish()
    }
}

impl Parcel {
    pub fn payload_len(&self) -> Result<usize, ProtocolError> {
        self.segments.iter().try_fold(0_usize, |total, segment| {
            total
                .checked_add(segment.len())
                .ok_or(ProtocolError::LengthOverflow)
        })
    }

    pub fn encoded_len(&self) -> Result<usize, ProtocolError> {
        let payload = self.payload_len()?;
        let table = self
            .segments
            .len()
            .checked_mul(8)
            .ok_or(ProtocolError::LengthOverflow)?;
        PARCEL_FIXED_BYTES
            .checked_add(table)
            .and_then(|value| value.checked_add(payload))
            .ok_or(ProtocolError::LengthOverflow)
    }

    pub fn validate(&self, limits: ProtocolLimits) -> Result<(), ProtocolError> {
        limits.validate()?;
        if self.segments.len() > limits.max_segments {
            return Err(ProtocolError::TooManySegments {
                actual: self.segments.len(),
                limit: limits.max_segments,
            });
        }
        let payload = self.payload_len()?;
        if payload > limits.max_payload_bytes {
            return Err(ProtocolError::PayloadTooLarge {
                actual: payload,
                limit: limits.max_payload_bytes,
            });
        }
        let frame = self.encoded_len()?;
        if frame > limits.max_frame_bytes {
            return Err(ProtocolError::FrameTooLarge {
                actual: frame,
                limit: limits.max_frame_bytes,
            });
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProtocolError {
    InvalidMagic,
    InvalidVersion { actual: u32 },
    InvalidLength { expected: usize, actual: usize },
    LengthOverflow,
    ZeroId(&'static str),
    InvalidChannel(u8),
    InvalidKind(u8),
    InvalidTraceFlag(u8),
    InvalidLimits,
    RunMismatch,
    RuntimeVersionMismatch,
    ActionRegistryMismatch,
    ObjectRegistryMismatch,
    TooManySegments { actual: usize, limit: usize },
    PayloadTooLarge { actual: usize, limit: usize },
    FrameTooLarge { actual: usize, limit: usize },
    PayloadLengthMismatch { declared: usize, actual: usize },
    TrailingBytes { remaining: usize },
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "protocol error: {self:?}")
    }
}

impl std::error::Error for ProtocolError {}

impl From<TryFromIntError> for ProtocolError {
    fn from(_: TryFromIntError) -> Self {
        Self::LengthOverflow
    }
}

pub fn encode_hello(hello: &Hello) -> Result<Vec<u8>, ProtocolError> {
    hello.limits.validate()?;
    let mut output = Vec::with_capacity(HELLO_BYTES);
    output.extend_from_slice(&HELLO_MAGIC);
    push_u32(&mut output, PROTOCOL_VERSION);
    push_u128(&mut output, hello.run_id.get());
    push_u16(&mut output, hello.runtime_version.major);
    push_u16(&mut output, hello.runtime_version.minor);
    push_u16(&mut output, hello.runtime_version.patch);
    output.extend_from_slice(&hello.action_registry_hash);
    output.extend_from_slice(&hello.object_registry_hash);
    push_u64(&mut output, hello.capabilities);
    push_u64(&mut output, hello.limits.max_frame_bytes.try_into()?);
    push_u64(&mut output, hello.limits.max_payload_bytes.try_into()?);
    push_u32(&mut output, hello.limits.max_segments.try_into()?);
    push_u32(
        &mut output,
        hello.limits.max_queued_parcels_per_peer.try_into()?,
    );
    push_u64(
        &mut output,
        hello.limits.max_inflight_bytes_per_peer.try_into()?,
    );
    push_u64(
        &mut output,
        hello.limits.control_reserved_bytes_per_peer.try_into()?,
    );
    debug_assert_eq!(output.len(), HELLO_BYTES);
    Ok(output)
}

pub fn decode_hello(bytes: &[u8]) -> Result<Hello, ProtocolError> {
    if bytes.len() != HELLO_BYTES {
        return Err(ProtocolError::InvalidLength {
            expected: HELLO_BYTES,
            actual: bytes.len(),
        });
    }
    let mut input = Reader::new(bytes);
    if input.take(4)? != HELLO_MAGIC {
        return Err(ProtocolError::InvalidMagic);
    }
    let version = input.u32()?;
    if version != PROTOCOL_VERSION {
        return Err(ProtocolError::InvalidVersion { actual: version });
    }
    let run_id = RunId::new(input.u128()?)?;
    let runtime_version = RuntimeVersion {
        major: input.u16()?,
        minor: input.u16()?,
        patch: input.u16()?,
    };
    let action_registry_hash = input.array_32()?;
    let object_registry_hash = input.array_32()?;
    let capabilities = input.u64()?;
    let limits = ProtocolLimits {
        max_frame_bytes: input.u64()?.try_into()?,
        max_payload_bytes: input.u64()?.try_into()?,
        max_segments: input.u32()?.try_into()?,
        max_queued_parcels_per_peer: input.u32()?.try_into()?,
        max_inflight_bytes_per_peer: input.u64()?.try_into()?,
        control_reserved_bytes_per_peer: input.u64()?.try_into()?,
    }
    .validate()?;
    input.finish()?;
    Ok(Hello {
        run_id,
        runtime_version,
        action_registry_hash,
        object_registry_hash,
        capabilities,
        limits,
    })
}

pub fn encode_parcel(parcel: &Parcel, limits: ProtocolLimits) -> Result<Vec<u8>, ProtocolError> {
    parcel.validate(limits)?;
    let payload_len = parcel.payload_len()?;
    let table_len = parcel
        .segments
        .len()
        .checked_mul(8)
        .ok_or(ProtocolError::LengthOverflow)?;
    let capacity = PARCEL_FIXED_BYTES
        .checked_add(table_len)
        .and_then(|value| value.checked_add(payload_len))
        .ok_or(ProtocolError::LengthOverflow)?;
    let mut output = Vec::with_capacity(capacity);
    output.extend_from_slice(&PARCEL_MAGIC);
    push_u32(&mut output, PROTOCOL_VERSION);
    push_u128(&mut output, parcel.run_id.get());
    push_u128(&mut output, parcel.message_id.get());
    output.push(parcel.channel as u8);
    output.push(parcel.kind as u8);
    push_u64(&mut output, parcel.source.get());
    push_u64(&mut output, parcel.destination.get());
    match parcel.trace_id {
        Some(trace_id) => {
            output.push(1);
            push_u128(&mut output, trace_id.get());
        }
        None => {
            output.push(0);
            push_u128(&mut output, 0);
        }
    }
    push_u64(&mut output, payload_len.try_into()?);
    push_u32(&mut output, parcel.segments.len().try_into()?);
    for segment in &parcel.segments {
        push_u64(&mut output, segment.len().try_into()?);
    }
    for segment in &parcel.segments {
        output.extend_from_slice(segment);
    }
    debug_assert_eq!(output.len(), capacity);
    Ok(output)
}

pub fn decode_parcel(bytes: &[u8], limits: ProtocolLimits) -> Result<Parcel, ProtocolError> {
    limits.validate()?;
    if bytes.len() > limits.max_frame_bytes {
        return Err(ProtocolError::FrameTooLarge {
            actual: bytes.len(),
            limit: limits.max_frame_bytes,
        });
    }
    let mut input = Reader::new(bytes);
    if input.take(4)? != PARCEL_MAGIC {
        return Err(ProtocolError::InvalidMagic);
    }
    let version = input.u32()?;
    if version != PROTOCOL_VERSION {
        return Err(ProtocolError::InvalidVersion { actual: version });
    }
    let run_id = RunId::new(input.u128()?)?;
    let message_id = MessageId::new(input.u128()?)?;
    let channel = Channel::try_from(input.u8()?)?;
    let kind = ParcelKind::try_from(input.u8()?)?;
    let source = LocalityId::new(input.u64()?);
    let destination = LocalityId::new(input.u64()?);
    let trace_flag = input.u8()?;
    let trace_value = input.u128()?;
    let trace_id = match trace_flag {
        0 if trace_value == 0 => None,
        1 => Some(TraceId::new(trace_value)?),
        value => return Err(ProtocolError::InvalidTraceFlag(value)),
    };
    let declared_payload: usize = input.u64()?.try_into()?;
    let segment_count: usize = input.u32()?.try_into()?;
    if segment_count > limits.max_segments {
        return Err(ProtocolError::TooManySegments {
            actual: segment_count,
            limit: limits.max_segments,
        });
    }
    if declared_payload > limits.max_payload_bytes {
        return Err(ProtocolError::PayloadTooLarge {
            actual: declared_payload,
            limit: limits.max_payload_bytes,
        });
    }
    let mut lengths = Vec::with_capacity(segment_count);
    let mut total = 0_usize;
    for _ in 0..segment_count {
        let length: usize = input.u64()?.try_into()?;
        total = total
            .checked_add(length)
            .ok_or(ProtocolError::LengthOverflow)?;
        if total > limits.max_payload_bytes {
            return Err(ProtocolError::PayloadTooLarge {
                actual: total,
                limit: limits.max_payload_bytes,
            });
        }
        lengths.push(length);
    }
    if total != declared_payload {
        return Err(ProtocolError::PayloadLengthMismatch {
            declared: declared_payload,
            actual: total,
        });
    }
    let mut segments = Vec::with_capacity(segment_count);
    for length in lengths {
        segments.push(input.take(length)?.to_vec());
    }
    input.finish()?;
    let parcel = Parcel {
        run_id,
        message_id,
        channel,
        kind,
        source,
        destination,
        trace_id,
        segments,
    };
    parcel.validate(limits)?;
    Ok(parcel)
}

fn push_u16(output: &mut Vec<u8>, value: u16) {
    output.extend_from_slice(&value.to_le_bytes());
}

fn push_u32(output: &mut Vec<u8>, value: u32) {
    output.extend_from_slice(&value.to_le_bytes());
}

fn push_u64(output: &mut Vec<u8>, value: u64) {
    output.extend_from_slice(&value.to_le_bytes());
}

fn push_u128(output: &mut Vec<u8>, value: u128) {
    output.extend_from_slice(&value.to_le_bytes());
}

struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], ProtocolError> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or(ProtocolError::LengthOverflow)?;
        if end > self.bytes.len() {
            return Err(ProtocolError::InvalidLength {
                expected: end,
                actual: self.bytes.len(),
            });
        }
        let output = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(output)
    }

    fn u8(&mut self) -> Result<u8, ProtocolError> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, ProtocolError> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }

    fn u32(&mut self) -> Result<u32, ProtocolError> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }

    fn u64(&mut self) -> Result<u64, ProtocolError> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }

    fn u128(&mut self) -> Result<u128, ProtocolError> {
        Ok(u128::from_le_bytes(self.take(16)?.try_into().unwrap()))
    }

    fn array_32(&mut self) -> Result<[u8; 32], ProtocolError> {
        Ok(self.take(32)?.try_into().unwrap())
    }

    fn finish(self) -> Result<(), ProtocolError> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(ProtocolError::TrailingBytes {
                remaining: self.bytes.len() - self.offset,
            })
        }
    }
}

#[cfg(test)]
mod tests;
