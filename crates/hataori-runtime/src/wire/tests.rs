use super::*;
use hataori_runtime_foundation::protocol::{LocalityId, MessageId, RunId, TraceId};

fn message(kind: RuntimeMessageKind) -> RuntimeMessage {
    RuntimeMessage {
        kind,
        request: RequestId::new(LocalityId::new(1), 2).unwrap(),
        action: ActionId::new(3).unwrap(),
        domain: DomainId::new(4),
        deadline_ms: if kind == RuntimeMessageKind::Request {
            100
        } else {
            0
        },
        payload: if kind == RuntimeMessageKind::Request {
            vec![b"input".to_vec()]
        } else {
            Vec::new()
        },
    }
}

fn parcel(message: RuntimeMessage) -> Parcel {
    let source = if matches!(
        message.kind,
        RuntimeMessageKind::Request | RuntimeMessageKind::Cancel
    ) {
        message.request.origin
    } else {
        LocalityId::new(0)
    };
    let destination = if matches!(
        message.kind,
        RuntimeMessageKind::Request | RuntimeMessageKind::Cancel
    ) {
        LocalityId::new(0)
    } else {
        message.request.origin
    };
    let control = message.kind == RuntimeMessageKind::Cancel;
    Parcel {
        run_id: RunId::new(1).unwrap(),
        message_id: MessageId::new(1).unwrap(),
        channel: if control {
            Channel::Control
        } else {
            Channel::Action
        },
        kind: if control {
            ParcelKind::Control
        } else {
            ParcelKind::Data
        },
        source,
        destination,
        trace_id: Some(TraceId::new(9).unwrap()),
        segments: encode(message),
    }
}

#[test]
fn runtime_header_round_trips_every_kind() {
    for kind in [
        RuntimeMessageKind::Request,
        RuntimeMessageKind::Success,
        RuntimeMessageKind::Failure,
        RuntimeMessageKind::Cancel,
        RuntimeMessageKind::Cancelled,
        RuntimeMessageKind::DuplicateResultUnavailable,
        RuntimeMessageKind::Moved,
    ] {
        let mut expected = message(kind);
        if kind == RuntimeMessageKind::Failure {
            expected.payload = vec![b"failed".to_vec()];
        } else if kind == RuntimeMessageKind::Moved {
            expected.payload = vec![vec![0; 32], vec![0; 32]];
        }
        assert_eq!(decode(parcel(expected.clone())).unwrap(), expected);
    }
}

#[test]
fn malformed_headers_channels_and_payloads_fail_closed() {
    let valid = message(RuntimeMessageKind::Request);
    let mut cases = Vec::new();
    let mut short = parcel(valid.clone());
    short.segments[0].pop();
    cases.push(short);
    let mut magic = parcel(valid.clone());
    magic.segments[0][0] ^= 1;
    cases.push(magic);
    let mut version = parcel(valid.clone());
    version.segments[0][4] = 2;
    cases.push(version);
    let mut kind = parcel(valid.clone());
    kind.segments[0][5] = 99;
    cases.push(kind);
    let mut reserved = parcel(valid.clone());
    reserved.segments[0][6] = 1;
    cases.push(reserved);
    let mut sequence = parcel(valid.clone());
    sequence.segments[0][16..24].fill(0);
    cases.push(sequence);
    let mut action = parcel(valid.clone());
    action.segments[0][24..40].fill(0);
    cases.push(action);
    let mut channel = parcel(valid.clone());
    channel.channel = Channel::Bulk;
    cases.push(channel);
    let mut identity = parcel(valid);
    identity.source = LocalityId::new(9);
    cases.push(identity);
    for case in cases {
        assert!(decode(case).is_err());
    }

    let mut cancel = message(RuntimeMessageKind::Cancel);
    cancel.payload = vec![vec![1]];
    assert!(matches!(
        decode(parcel(cancel)),
        Err(WireError::UnexpectedPayload)
    ));
}
