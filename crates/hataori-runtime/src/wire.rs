use crate::{action::Segments, error::MAX_ERROR_BYTES};
use hataori_runtime_foundation::protocol::{
    ActionId, Channel, DomainId, Parcel, ParcelKind, RequestId,
};

const MAGIC: [u8; 4] = *b"HRTA";
const VERSION: u8 = 1;
const HEADER_BYTES: usize = 52;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub(crate) enum RuntimeMessageKind {
    Request = 1,
    Success = 2,
    Failure = 3,
    Cancel = 4,
    Cancelled = 5,
    DuplicateResultUnavailable = 6,
}

impl TryFrom<u8> for RuntimeMessageKind {
    type Error = WireError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Request),
            2 => Ok(Self::Success),
            3 => Ok(Self::Failure),
            4 => Ok(Self::Cancel),
            5 => Ok(Self::Cancelled),
            6 => Ok(Self::DuplicateResultUnavailable),
            _ => Err(WireError::InvalidKind(value)),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RuntimeMessage {
    pub kind: RuntimeMessageKind,
    pub request: RequestId,
    pub action: ActionId,
    pub domain: DomainId,
    pub deadline_ms: u64,
    pub payload: Segments,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum WireError {
    HeaderLength(usize),
    Magic,
    Version(u8),
    InvalidKind(u8),
    Reserved,
    ZeroRequest,
    ZeroAction,
    Channel,
    Identity,
    UnexpectedDeadline,
    UnexpectedPayload,
    ErrorPayload,
}

impl std::fmt::Display for WireError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "runtime wire error: {self:?}")
    }
}

impl std::error::Error for WireError {}

pub(crate) fn encode(message: RuntimeMessage) -> Segments {
    let mut header = Vec::with_capacity(HEADER_BYTES);
    header.extend_from_slice(&MAGIC);
    header.push(VERSION);
    header.push(message.kind as u8);
    header.extend_from_slice(&[0; 2]);
    header.extend_from_slice(&message.request.origin.get().to_le_bytes());
    header.extend_from_slice(&message.request.sequence.to_le_bytes());
    header.extend_from_slice(&message.action.get().to_le_bytes());
    header.extend_from_slice(&message.domain.get().to_le_bytes());
    header.extend_from_slice(&message.deadline_ms.to_le_bytes());
    debug_assert_eq!(header.len(), HEADER_BYTES);
    let mut segments = Vec::with_capacity(message.payload.len() + 1);
    segments.push(header);
    segments.extend(message.payload);
    segments
}

pub(crate) fn decode(parcel: Parcel) -> Result<RuntimeMessage, WireError> {
    let shape = ParcelShape {
        channel: parcel.channel,
        kind: parcel.kind,
        source: parcel.source,
        destination: parcel.destination,
    };
    let mut segments = parcel.segments.into_iter();
    let header = segments.next().ok_or(WireError::HeaderLength(0))?;
    if header.len() != HEADER_BYTES {
        return Err(WireError::HeaderLength(header.len()));
    }
    if header[..4] != MAGIC {
        return Err(WireError::Magic);
    }
    if header[4] != VERSION {
        return Err(WireError::Version(header[4]));
    }
    let kind = RuntimeMessageKind::try_from(header[5])?;
    if header[6..8] != [0; 2] {
        return Err(WireError::Reserved);
    }
    let origin = u64::from_le_bytes(header[8..16].try_into().unwrap());
    let sequence = u64::from_le_bytes(header[16..24].try_into().unwrap());
    if sequence == 0 {
        return Err(WireError::ZeroRequest);
    }
    let action = u128::from_le_bytes(header[24..40].try_into().unwrap());
    let action = ActionId::new(action).map_err(|_| WireError::ZeroAction)?;
    let domain = DomainId::new(u32::from_le_bytes(header[40..44].try_into().unwrap()));
    let deadline_ms = u64::from_le_bytes(header[44..52].try_into().unwrap());
    let request = RequestId {
        origin: hataori_runtime_foundation::protocol::LocalityId::new(origin),
        sequence,
    };
    let payload: Segments = segments.collect();
    validate_shape(kind, shape, request, deadline_ms, &payload)?;
    Ok(RuntimeMessage {
        kind,
        request,
        action,
        domain,
        deadline_ms,
        payload,
    })
}

#[derive(Clone, Copy)]
struct ParcelShape {
    channel: Channel,
    kind: ParcelKind,
    source: hataori_runtime_foundation::protocol::LocalityId,
    destination: hataori_runtime_foundation::protocol::LocalityId,
}

fn validate_shape(
    kind: RuntimeMessageKind,
    shape: ParcelShape,
    request: RequestId,
    deadline_ms: u64,
    payload: &Segments,
) -> Result<(), WireError> {
    let (expected_channel, expected_parcel_kind) = match kind {
        RuntimeMessageKind::Cancel => (Channel::Control, ParcelKind::Control),
        _ => (Channel::Action, ParcelKind::Data),
    };
    if shape.channel != expected_channel || shape.kind != expected_parcel_kind {
        return Err(WireError::Channel);
    }
    match kind {
        RuntimeMessageKind::Request | RuntimeMessageKind::Cancel => {
            if shape.source != request.origin {
                return Err(WireError::Identity);
            }
        }
        _ => {
            if shape.destination != request.origin {
                return Err(WireError::Identity);
            }
        }
    }
    if kind == RuntimeMessageKind::Request {
        if deadline_ms == 0 {
            return Err(WireError::UnexpectedDeadline);
        }
    } else if deadline_ms != 0 {
        return Err(WireError::UnexpectedDeadline);
    }
    match kind {
        RuntimeMessageKind::Cancel
        | RuntimeMessageKind::Cancelled
        | RuntimeMessageKind::DuplicateResultUnavailable
            if !payload.is_empty() =>
        {
            Err(WireError::UnexpectedPayload)
        }
        RuntimeMessageKind::Failure
            if payload.len() != 1
                || payload[0].len() > MAX_ERROR_BYTES
                || std::str::from_utf8(&payload[0]).is_err() =>
        {
            Err(WireError::ErrorPayload)
        }
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests;
