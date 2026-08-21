use crate::map::truncate_message;
use bincode::config::{self, Configuration};
use serde::{de::DeserializeOwned, Serialize};
use std::fmt;

pub(crate) const PROTOCOL_VERSION: u16 = 1;
pub(crate) const MAX_WIRE_BYTES: u64 = i32::MAX as u64;
pub(crate) const HEADER_LEN: usize = 28;
pub(crate) const NO_ERROR_KEY: i64 = i64::MAX;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub(crate) enum MessageKind {
    Ready,
    Task,
    Result,
    Stop,
    Drain,
}

impl MessageKind {
    pub(crate) const fn code(self) -> u8 {
        match self {
            Self::Ready => 0,
            Self::Task => 1,
            Self::Result => 2,
            Self::Stop => 3,
            Self::Drain => 4,
        }
    }
}

impl TryFrom<u8> for MessageKind {
    type Error = WireError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Ready),
            1 => Ok(Self::Task),
            2 => Ok(Self::Result),
            3 => Ok(Self::Stop),
            4 => Ok(Self::Drain),
            value => Err(WireError::InvalidKind(value)),
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub(crate) enum MessageStatus {
    None,
    Ok,
    Error,
}

impl MessageStatus {
    pub(crate) const fn code(self) -> u8 {
        match self {
            Self::None => 0,
            Self::Ok => 1,
            Self::Error => 2,
        }
    }
}

impl TryFrom<u8> for MessageStatus {
    type Error = WireError;

    fn try_from(value: u8) -> Result<Self, WireError> {
        match value {
            0 => Ok(Self::None),
            1 => Ok(Self::Ok),
            2 => Ok(Self::Error),
            value => Err(WireError::InvalidStatus(value)),
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub(crate) struct Header {
    version: u16,
    kind: MessageKind,
    status: MessageStatus,
    batch_id: u64,
    item_count: u64,
    payload_len: u64,
}

impl Header {
    pub(crate) fn new(
        kind: MessageKind,
        status: MessageStatus,
        batch_id: u64,
        item_count: u64,
        payload_len: u64,
    ) -> Result<Self, WireError> {
        validate_status(kind, status)?;
        validate_payload_len(payload_len)?;
        Ok(Self {
            version: PROTOCOL_VERSION,
            kind,
            status,
            batch_id,
            item_count,
            payload_len,
        })
    }

    pub(crate) fn encode(self) -> [u8; HEADER_LEN] {
        let mut bytes = [0; HEADER_LEN];
        bytes[0..2].copy_from_slice(&self.version.to_le_bytes());
        bytes[2] = self.kind.code();
        bytes[3] = self.status.code();
        bytes[4..12].copy_from_slice(&self.batch_id.to_le_bytes());
        bytes[12..20].copy_from_slice(&self.item_count.to_le_bytes());
        bytes[20..28].copy_from_slice(&self.payload_len.to_le_bytes());
        bytes
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, WireError> {
        if bytes.len() != HEADER_LEN {
            return Err(WireError::InvalidHeaderSize {
                actual: bytes.len(),
            });
        }
        let version = u16::from_le_bytes([bytes[0], bytes[1]]);
        if version != PROTOCOL_VERSION {
            return Err(WireError::InvalidVersion {
                expected: PROTOCOL_VERSION,
                actual: version,
            });
        }
        let kind = MessageKind::try_from(bytes[2])?;
        let status = MessageStatus::try_from(bytes[3])?;
        validate_status(kind, status)?;
        let batch_id = u64::from_le_bytes(bytes[4..12].try_into().unwrap());
        let item_count = u64::from_le_bytes(bytes[12..20].try_into().unwrap());
        let payload_len = u64::from_le_bytes(bytes[20..28].try_into().unwrap());
        validate_payload_len(payload_len)?;
        Ok(Self {
            version,
            kind,
            status,
            batch_id,
            item_count,
            payload_len,
        })
    }

    #[cfg(test)]
    pub(crate) const fn version(self) -> u16 {
        self.version
    }
    pub(crate) const fn kind(self) -> MessageKind {
        self.kind
    }
    pub(crate) const fn status(self) -> MessageStatus {
        self.status
    }
    pub(crate) const fn batch_id(self) -> u64 {
        self.batch_id
    }
    pub(crate) const fn item_count(self) -> u64 {
        self.item_count
    }
    pub(crate) const fn payload_len(self) -> u64 {
        self.payload_len
    }
}

fn validate_status(kind: MessageKind, status: MessageStatus) -> Result<(), WireError> {
    let valid = match kind {
        MessageKind::Ready | MessageKind::Task | MessageKind::Stop | MessageKind::Drain => {
            status == MessageStatus::None
        }
        MessageKind::Result => matches!(status, MessageStatus::Ok | MessageStatus::Error),
    };
    valid
        .then_some(())
        .ok_or(WireError::StatusKindMismatch { kind, status })
}

fn validate_payload_len(payload_len: u64) -> Result<(), WireError> {
    (payload_len <= MAX_WIRE_BYTES)
        .then_some(())
        .ok_or(WireError::PayloadTooLarge { bytes: payload_len })
}

fn wire_config() -> Configuration<config::LittleEndian, config::Fixint> {
    config::standard()
        .with_fixed_int_encoding()
        .with_little_endian()
}

pub(crate) fn encode_payload<T: Serialize>(value: &T) -> Result<Vec<u8>, WireError> {
    let bytes =
        bincode::serde::encode_to_vec(value, wire_config()).map_err(|error| WireError::Encode {
            detail: truncate_message(error.to_string()),
        })?;
    validate_payload_len(bytes.len() as u64)?;
    Ok(bytes)
}

pub(crate) fn decode_payload<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, WireError> {
    validate_payload_len(bytes.len() as u64)?;
    let (value, used) =
        bincode::serde::decode_from_slice(bytes, wire_config()).map_err(|error| {
            WireError::Decode {
                detail: truncate_message(error.to_string()),
            }
        })?;
    if used != bytes.len() {
        return Err(WireError::TrailingBytes {
            used,
            total: bytes.len(),
        });
    }
    Ok(value)
}

pub(crate) fn checked_mpi_count(value: u64) -> Result<i32, WireError> {
    (value <= i32::MAX as u64)
        .then_some(value as i32)
        .ok_or(WireError::CountOverflow { count: value })
}

pub(crate) fn checked_usize_length(value: u64) -> Result<usize, WireError> {
    usize::try_from(value).map_err(|_| WireError::UsizeOverflow { length: value })
}

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) enum ErrorClass {
    WireProtocol,
    Callback,
    #[allow(dead_code, reason = "Task #19 drain-fault convergence uses this class")]
    Drain,
}

impl ErrorClass {
    pub(crate) const COUNT: u8 = 3;

    pub(crate) const fn code(self) -> u8 {
        match self {
            Self::WireProtocol => 0,
            Self::Callback => 1,
            Self::Drain => 2,
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) struct ErrorKey(i64);

impl ErrorKey {
    pub(crate) const NO_ERROR_KEY: i64 = NO_ERROR_KEY;

    pub(crate) fn new(
        task_key: u64,
        class: ErrorClass,
        reporting_rank: i32,
        world_size: i32,
    ) -> Result<Self, WireError> {
        validate_world_rank(reporting_rank, world_size)?;
        let value = ((task_key as u128 * ErrorClass::COUNT as u128 + class.code() as u128)
            * world_size as u128)
            + reporting_rank as u128;
        if value >= NO_ERROR_KEY as u128 {
            return Err(WireError::ErrorKeyOverflow);
        }
        Ok(Self(value as i64))
    }

    pub(crate) const fn get(self) -> i64 {
        self.0
    }

    #[cfg(test)]
    pub(crate) fn checked_reporting_rank(self, world_size: i32) -> Result<i32, WireError> {
        if world_size <= 0 {
            return Err(WireError::InvalidWorldSize { world_size });
        }
        if self.0 == NO_ERROR_KEY {
            return Err(WireError::NoErrorKey);
        }
        Ok((self.0 % world_size as i64) as i32)
    }
}

fn validate_world_rank(rank: i32, world_size: i32) -> Result<(), WireError> {
    if world_size <= 0 {
        return Err(WireError::InvalidWorldSize { world_size });
    }
    if rank < 0 || rank >= world_size {
        return Err(WireError::InvalidRank { rank, world_size });
    }
    Ok(())
}

pub(crate) fn preflight_candidate(
    failed: bool,
    reporting_rank: i32,
    world_size: i32,
) -> Result<i32, WireError> {
    validate_world_rank(reporting_rank, world_size)?;
    Ok(if failed { reporting_rank } else { world_size })
}

pub(crate) enum WireError {
    InvalidHeaderSize {
        actual: usize,
    },
    InvalidVersion {
        expected: u16,
        actual: u16,
    },
    InvalidKind(u8),
    InvalidStatus(u8),
    StatusKindMismatch {
        kind: MessageKind,
        status: MessageStatus,
    },
    PayloadTooLarge {
        bytes: u64,
    },
    CountOverflow {
        count: u64,
    },
    UsizeOverflow {
        length: u64,
    },
    Encode {
        detail: String,
    },
    Decode {
        detail: String,
    },
    TrailingBytes {
        used: usize,
        total: usize,
    },
    InvalidWorldSize {
        world_size: i32,
    },
    InvalidRank {
        rank: i32,
        world_size: i32,
    },
    ErrorKeyOverflow,
    #[cfg(test)]
    NoErrorKey,
}

impl fmt::Debug for WireError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.to_string())
    }
}

impl fmt::Display for WireError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidHeaderSize { actual } => {
                write!(formatter, "invalid wire header size: {actual}")
            }
            Self::InvalidVersion { expected, actual } => write!(
                formatter,
                "invalid wire version: expected {expected}, got {actual}"
            ),
            Self::InvalidKind(value) => write!(formatter, "invalid wire message kind: {value}"),
            Self::InvalidStatus(value) => write!(formatter, "invalid wire message status: {value}"),
            Self::StatusKindMismatch { kind, status } => write!(
                formatter,
                "wire status is incompatible with message kind: {kind:?}/{status:?}"
            ),
            Self::PayloadTooLarge { bytes } => {
                write!(formatter, "wire payload is too large: {bytes} bytes")
            }
            Self::CountOverflow { count } => write!(formatter, "MPI count overflows i32: {count}"),
            Self::UsizeOverflow { length } => {
                write!(formatter, "wire length overflows usize: {length}")
            }
            Self::Encode { detail } => write!(
                formatter,
                "wire payload encoding failed: {}",
                truncate_message(detail.clone())
            ),
            Self::Decode { detail } => write!(
                formatter,
                "wire payload decoding failed: {}",
                truncate_message(detail.clone())
            ),
            Self::TrailingBytes { used, total } => write!(
                formatter,
                "wire payload has trailing bytes: used {used} of {total}"
            ),
            Self::InvalidWorldSize { world_size } => {
                write!(formatter, "invalid MPI world size: {world_size}")
            }
            Self::InvalidRank { rank, world_size } => write!(
                formatter,
                "invalid MPI rank {rank} for world size {world_size}"
            ),
            Self::ErrorKeyOverflow => formatter.write_str("deterministic error key overflows i64"),
            #[cfg(test)]
            Self::NoErrorKey => formatter.write_str("no-error key has no reporting rank"),
        }
    }
}

impl std::error::Error for WireError {}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[test]
    fn header_golden_and_roundtrip() {
        let header = Header::new(
            MessageKind::Result,
            MessageStatus::Error,
            0x0102_0304_0506_0708,
            0x1112_1314_1516_1718,
            123,
        )
        .unwrap();
        assert_eq!(
            header.encode(),
            [
                1, 0, 2, 2, 8, 7, 6, 5, 4, 3, 2, 1, 0x18, 0x17, 0x16, 0x15, 0x14, 0x13, 0x12, 0x11,
                123, 0, 0, 0, 0, 0, 0, 0,
            ]
        );
        assert_eq!(Header::decode(&header.encode()).unwrap(), header);
        assert_eq!(header.version(), PROTOCOL_VERSION);
        assert_eq!(header.kind(), MessageKind::Result);
        assert_eq!(header.status(), MessageStatus::Error);
        assert_eq!(header.batch_id(), 0x0102_0304_0506_0708);
        assert_eq!(header.item_count(), 0x1112_1314_1516_1718);
        assert_eq!(header.payload_len(), 123);
    }

    #[test]
    fn header_rejects_bad_size_fields_pairs_and_payload_cap() {
        assert!(matches!(
            Header::decode(&[0; HEADER_LEN - 1]),
            Err(WireError::InvalidHeaderSize { .. })
        ));
        assert!(matches!(
            Header::decode(&[0; HEADER_LEN + 1]),
            Err(WireError::InvalidHeaderSize { .. })
        ));

        let mut bytes = [0; HEADER_LEN];
        bytes[0] = 2;
        assert!(matches!(
            Header::decode(&bytes),
            Err(WireError::InvalidVersion { .. })
        ));
        bytes[0] = 1;
        bytes[2] = 99;
        assert!(matches!(
            Header::decode(&bytes),
            Err(WireError::InvalidKind(99))
        ));
        bytes[2] = 0;
        bytes[3] = 99;
        assert!(matches!(
            Header::decode(&bytes),
            Err(WireError::InvalidStatus(99))
        ));

        assert!(matches!(
            Header::new(MessageKind::Ready, MessageStatus::Ok, 0, 0, 0),
            Err(WireError::StatusKindMismatch { .. })
        ));
        assert!(matches!(
            Header::new(MessageKind::Result, MessageStatus::None, 0, 0, 0),
            Err(WireError::StatusKindMismatch { .. })
        ));
        bytes[2] = MessageKind::Ready.code();
        bytes[3] = MessageStatus::Ok.code();
        assert!(matches!(
            Header::decode(&bytes),
            Err(WireError::StatusKindMismatch { .. })
        ));
        assert!(Header::new(MessageKind::Stop, MessageStatus::None, 0, 0, MAX_WIRE_BYTES).is_ok());
        assert!(matches!(
            Header::new(
                MessageKind::Stop,
                MessageStatus::None,
                0,
                0,
                MAX_WIRE_BYTES + 1
            ),
            Err(WireError::PayloadTooLarge { .. })
        ));
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct Pair {
        left: u16,
        right: String,
    }

    #[test]
    fn codec_uses_fixed_little_endian_and_roundtrips() {
        assert_eq!(encode_payload(&0x1234_u16).unwrap(), vec![0x34, 0x12]);
        let value = Pair {
            left: 7,
            right: "wire".into(),
        };
        let bytes = encode_payload(&value).unwrap();
        assert_eq!(decode_payload::<Pair>(&bytes).unwrap(), value);
    }

    #[test]
    fn codec_rejects_malformed_trailing_and_oversized_payloads() {
        assert!(matches!(
            decode_payload::<u16>(&[1]),
            Err(WireError::Decode { .. })
        ));
        let mut bytes = encode_payload(&1_u16).unwrap();
        bytes.push(0);
        assert!(matches!(
            decode_payload::<u16>(&bytes),
            Err(WireError::TrailingBytes { .. })
        ));
    }

    #[test]
    fn conversion_boundaries_are_checked_without_large_allocations() {
        assert_eq!(checked_mpi_count(i32::MAX as u64).unwrap(), i32::MAX);
        assert!(matches!(
            checked_mpi_count(i32::MAX as u64 + 1),
            Err(WireError::CountOverflow { .. })
        ));
        assert_eq!(checked_usize_length(usize::MAX as u64).unwrap(), usize::MAX);
        #[cfg(target_pointer_width = "32")]
        assert!(matches!(
            checked_usize_length(u64::from(u32::MAX) + 1),
            Err(WireError::UsizeOverflow { .. })
        ));
    }

    #[test]
    fn error_keys_order_and_rank_decode_are_signed() {
        let world = 4;
        let a = ErrorKey::new(2, ErrorClass::WireProtocol, 3, world).unwrap();
        let b = ErrorKey::new(2, ErrorClass::Callback, 0, world).unwrap();
        let c = ErrorKey::new(2, ErrorClass::Callback, 1, world).unwrap();
        assert!(a < b && b < c);
        assert_eq!(c.checked_reporting_rank(world).unwrap(), 1);
        assert_eq!(ErrorKey::NO_ERROR_KEY, i64::MAX);
        assert_eq!(std::cmp::min(a, ErrorKey(ErrorKey::NO_ERROR_KEY)), a);
        assert!(
            ErrorKey::new(0, ErrorClass::WireProtocol, 0, world)
                .unwrap()
                .get()
                >= 0
        );
    }

    #[test]
    fn error_keys_validate_world_rank_and_overflow() {
        assert!(matches!(
            ErrorKey::new(0, ErrorClass::WireProtocol, 0, 0),
            Err(WireError::InvalidWorldSize { .. })
        ));
        assert!(matches!(
            ErrorKey::new(0, ErrorClass::WireProtocol, -1, 2),
            Err(WireError::InvalidRank { .. })
        ));
        assert!(matches!(
            ErrorKey::new(0, ErrorClass::WireProtocol, 2, 2),
            Err(WireError::InvalidRank { .. })
        ));
        assert!(matches!(
            ErrorKey::new(i64::MAX as u64, ErrorClass::WireProtocol, 0, i32::MAX),
            Err(WireError::ErrorKeyOverflow)
        ));
        let world = 2;
        let max_task =
            ((i64::MAX as u128 - 1 - (ErrorClass::Drain.code() as u128 * world as u128 + 1))
                / (ErrorClass::COUNT as u128 * world as u128)) as u64;
        let largest = ErrorKey::new(max_task, ErrorClass::Drain, 1, world).unwrap();
        assert!(largest.get() < i64::MAX);
        assert!(ErrorKey::new(max_task + 1, ErrorClass::Drain, 1, world).is_err());
        assert!(matches!(
            ErrorKey(i64::MAX).checked_reporting_rank(2),
            Err(WireError::NoErrorKey)
        ));
        assert!(matches!(
            ErrorKey::new(u64::MAX, ErrorClass::Drain, 1, 2),
            Err(WireError::ErrorKeyOverflow)
        ));
    }

    #[test]
    fn preflight_candidate_uses_signed_rank_or_world_sentinel() {
        assert_eq!(preflight_candidate(false, 1, 4).unwrap(), 4);
        assert_eq!(preflight_candidate(true, 1, 4).unwrap(), 1);
        assert!(matches!(
            preflight_candidate(true, -1, 4),
            Err(WireError::InvalidRank { .. })
        ));
        assert!(matches!(
            preflight_candidate(false, 0, 0),
            Err(WireError::InvalidWorldSize { .. })
        ));
    }

    #[test]
    fn error_display_is_bounded_and_accessors_are_available() {
        let detail = "é".repeat(5000);
        let error = WireError::Decode {
            detail: truncate_message(detail),
        };
        let WireError::Decode { detail } = &error else {
            unreachable!()
        };
        assert!(detail.len() <= 4096);
        assert!(detail.is_char_boundary(detail.len()));
        assert!(std::error::Error::source(&error).is_none());
        assert_eq!(MessageKind::Task.code(), 1);
        assert_eq!(MessageStatus::Ok.code(), 1);
        assert_eq!(ErrorClass::Drain.code(), 2);
    }
}
