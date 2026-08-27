use super::*;
use crate::protocol::{MessageId, ParcelKind, RunId};

fn parcel(size: usize) -> Parcel {
    Parcel {
        run_id: RunId::new(1).unwrap(),
        message_id: MessageId::new(1).unwrap(),
        channel: Channel::Bulk,
        kind: ParcelKind::Data,
        source: LocalityId::new(0),
        destination: LocalityId::new(1),
        trace_id: None,
        segments: (0..16).map(|index| vec![index as u8; size / 16]).collect(),
    }
}

#[test]
fn chunk_reassembly_round_trips_one_mebibyte() {
    let limits = ProtocolLimits::default();
    let submission = Submission {
        ticket: SendTicket::new(1),
        parcel: parcel(1024 * 1024),
        reserved_bytes: 1024 * 1024,
    };
    let mut outbound = Outbound::new(submission, limits).unwrap();
    assert!(outbound.chunk_count > 1);
    let mut reassembly = None;
    let mut completed = None;
    while !outbound.complete() {
        completed = accept_chunk(&mut reassembly, &outbound.next_chunk().unwrap(), limits).unwrap();
    }
    let completed = completed.unwrap();
    assert_eq!(completed.payload_len().unwrap(), 1024 * 1024);
    for (index, segment) in completed.segments.iter().enumerate() {
        assert!(segment.iter().all(|byte| *byte == index as u8));
    }
    assert!(reassembly.is_none());
}

#[test]
fn outbound_encoding_failure_preserves_release_metadata() {
    let limits = ProtocolLimits {
        max_payload_bytes: 1,
        ..ProtocolLimits::default()
    };
    let submission = Submission {
        ticket: SendTicket::new(7),
        parcel: Parcel {
            segments: vec![vec![0; 2]],
            ..parcel(0)
        },
        reserved_bytes: 77,
    };
    let (ticket, reserved_bytes, error) = Outbound::new(submission, limits).unwrap_err();
    assert_eq!(ticket.get(), 7);
    assert_eq!(reserved_bytes, 77);
    assert!(matches!(error, TransportError::Protocol(_)));
}

#[test]
fn malformed_chunks_fail_before_reassembly_growth() {
    let limits = ProtocolLimits::default();
    let mut slot = None;
    assert!(matches!(
        accept_chunk(&mut slot, &[0; CHUNK_HEADER_BYTES - 1], limits),
        Err(TransportError::Peer(_))
    ));
    let submission = Submission {
        ticket: SendTicket::new(2),
        parcel: parcel(1024),
        reserved_bytes: 1024,
    };
    let mut outbound = Outbound::new(submission, limits).unwrap();
    let mut chunk = outbound.next_chunk().unwrap();
    chunk[0] ^= 1;
    assert!(matches!(
        accept_chunk(&mut slot, &chunk, limits),
        Err(TransportError::Peer(_))
    ));

    let submission = Submission {
        ticket: SendTicket::new(3),
        parcel: parcel(16 * 1024),
        reserved_bytes: 16 * 1024,
    };
    let mut outbound = Outbound::new(submission, limits).unwrap();
    let _first = outbound.next_chunk().unwrap();
    let second = outbound.next_chunk().unwrap();
    assert!(matches!(
        accept_chunk(&mut slot, &second, limits),
        Err(TransportError::Peer(_))
    ));
    assert!(slot.is_none());
}
