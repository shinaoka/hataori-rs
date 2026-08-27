use super::*;

fn hello() -> Hello {
    Hello {
        run_id: RunId::new(7).unwrap(),
        runtime_version: RuntimeVersion {
            major: 1,
            minor: 2,
            patch: 3,
        },
        action_registry_hash: [0x11; 32],
        object_registry_hash: [0x22; 32],
        capabilities: 5,
        limits: ProtocolLimits::default(),
    }
}

fn parcel() -> Parcel {
    Parcel {
        run_id: RunId::new(7).unwrap(),
        message_id: MessageId::new(9).unwrap(),
        channel: Channel::Bulk,
        kind: ParcelKind::Data,
        source: LocalityId::new(1),
        destination: LocalityId::new(2),
        trace_id: Some(TraceId::new(10).unwrap()),
        segments: vec![vec![1, 2, 3], vec![], vec![4, 5]],
    }
}

#[test]
fn hello_round_trip_and_negotiation() {
    let local = hello();
    let decoded = decode_hello(&encode_hello(&local).unwrap()).unwrap();
    assert_eq!(decoded, local);
    let mut peer = local.clone();
    peer.limits.max_payload_bytes /= 2;
    peer.capabilities = 0b0110;
    let negotiated = local.validate_peer(&peer).unwrap();
    assert_eq!(
        negotiated.limits.max_payload_bytes,
        peer.limits.max_payload_bytes
    );
    assert_eq!(
        negotiated.capabilities,
        local.capabilities & peer.capabilities
    );
}

#[test]
fn hello_rejects_identity_registry_version_and_limits_mismatch() {
    let local = hello();
    let mut peer = local.clone();
    peer.run_id = RunId::new(8).unwrap();
    assert_eq!(local.validate_peer(&peer), Err(ProtocolError::RunMismatch));
    peer = local.clone();
    peer.runtime_version.patch += 1;
    assert_eq!(
        local.validate_peer(&peer),
        Err(ProtocolError::RuntimeVersionMismatch)
    );
    peer = local.clone();
    peer.action_registry_hash[0] ^= 1;
    assert_eq!(
        local.validate_peer(&peer),
        Err(ProtocolError::ActionRegistryMismatch)
    );
    peer = local.clone();
    peer.object_registry_hash[0] ^= 1;
    assert_eq!(
        local.validate_peer(&peer),
        Err(ProtocolError::ObjectRegistryMismatch)
    );
    peer = local;
    peer.limits.max_segments = 0;
    assert_eq!(peer.limits.validate(), Err(ProtocolError::InvalidLimits));
    peer.limits = ProtocolLimits::default();
    peer.limits.control_reserved_bytes_per_peer = peer.limits.max_inflight_bytes_per_peer;
    assert_eq!(peer.limits.validate(), Err(ProtocolError::InvalidLimits));
}

#[test]
fn hello_decode_checks_magic_version_size_and_zero_run() {
    let encoded = encode_hello(&hello()).unwrap();
    let mut bad = encoded.clone();
    bad[0] ^= 1;
    assert_eq!(decode_hello(&bad), Err(ProtocolError::InvalidMagic));
    bad = encoded.clone();
    bad[4..8].copy_from_slice(&(PROTOCOL_VERSION + 1).to_le_bytes());
    assert_eq!(
        decode_hello(&bad),
        Err(ProtocolError::InvalidVersion {
            actual: PROTOCOL_VERSION + 1
        })
    );
    bad = encoded.clone();
    bad[8..24].fill(0);
    assert_eq!(decode_hello(&bad), Err(ProtocolError::ZeroId("RunId")));
    assert!(matches!(
        decode_hello(&encoded[..encoded.len() - 1]),
        Err(ProtocolError::InvalidLength { .. })
    ));
}

#[test]
fn segmented_parcel_round_trip_preserves_boundaries() {
    let value = parcel();
    let encoded = encode_parcel(&value, ProtocolLimits::default()).unwrap();
    let decoded = decode_parcel(&encoded, ProtocolLimits::default()).unwrap();
    assert_eq!(decoded, value);
    assert_eq!(decoded.payload_len().unwrap(), 5);
    let debug = format!("{decoded:?}");
    assert!(debug.contains("segments: 3"));
    assert!(debug.contains("payload_bytes: 5"));
    assert!(!debug.contains("[1, 2, 3]"));
}

#[test]
fn parcel_rejects_checked_limits_before_allocation() {
    let value = parcel();
    let limits = ProtocolLimits {
        max_segments: 2,
        ..ProtocolLimits::default()
    };
    assert_eq!(
        value.validate(limits),
        Err(ProtocolError::TooManySegments {
            actual: 3,
            limit: 2
        })
    );
    let limits = ProtocolLimits {
        max_payload_bytes: 4,
        ..ProtocolLimits::default()
    };
    assert_eq!(
        value.validate(limits),
        Err(ProtocolError::PayloadTooLarge {
            actual: 5,
            limit: 4
        })
    );
    let limits = ProtocolLimits {
        max_frame_bytes: PARCEL_FIXED_BYTES + 3 * 8 + 4,
        ..ProtocolLimits::default()
    };
    assert!(matches!(
        value.validate(limits),
        Err(ProtocolError::FrameTooLarge { .. })
    ));
}

#[test]
fn parcel_decode_rejects_every_discriminant_and_length_boundary() {
    let encoded = encode_parcel(&parcel(), ProtocolLimits::default()).unwrap();
    let mut bad = encoded.clone();
    bad[0] ^= 1;
    assert_eq!(
        decode_parcel(&bad, ProtocolLimits::default()),
        Err(ProtocolError::InvalidMagic)
    );
    bad = encoded.clone();
    bad[4..8].copy_from_slice(&(PROTOCOL_VERSION + 1).to_le_bytes());
    assert!(matches!(
        decode_parcel(&bad, ProtocolLimits::default()),
        Err(ProtocolError::InvalidVersion { .. })
    ));
    bad = encoded.clone();
    bad[40] = 99;
    assert_eq!(
        decode_parcel(&bad, ProtocolLimits::default()),
        Err(ProtocolError::InvalidChannel(99))
    );
    bad = encoded.clone();
    bad[41] = 99;
    assert_eq!(
        decode_parcel(&bad, ProtocolLimits::default()),
        Err(ProtocolError::InvalidKind(99))
    );
    bad = encoded.clone();
    bad[58] = 2;
    assert_eq!(
        decode_parcel(&bad, ProtocolLimits::default()),
        Err(ProtocolError::InvalidTraceFlag(2))
    );
    bad = encoded.clone();
    bad[75..83].copy_from_slice(&6_u64.to_le_bytes());
    assert_eq!(
        decode_parcel(&bad, ProtocolLimits::default()),
        Err(ProtocolError::PayloadLengthMismatch {
            declared: 6,
            actual: 5
        })
    );
    assert!(matches!(
        decode_parcel(&encoded[..encoded.len() - 1], ProtocolLimits::default()),
        Err(ProtocolError::InvalidLength { .. })
    ));
    bad = encoded;
    bad.push(0);
    assert_eq!(
        decode_parcel(&bad, ProtocolLimits::default()),
        Err(ProtocolError::TrailingBytes { remaining: 1 })
    );
}

#[test]
fn logical_id_construction_is_stable_and_checked() {
    assert_eq!(TaskId::new(1).unwrap().get(), 1);
    assert_eq!(ActionId::new(2).unwrap().get(), 2);
    assert_eq!(ObjectTypeId::new(3).unwrap().get(), 3);
    assert_eq!(TraceId::new(4).unwrap().get(), 4);
    assert_eq!(DomainId::DEFAULT.get(), 0);
    assert_eq!(DomainId::new(9).get(), 9);
    assert_eq!(RequestId::new(LocalityId::new(7), 8).unwrap().sequence, 8);
    assert_eq!(TaskId::new(0), Err(ProtocolError::ZeroId("TaskId")));
    assert_eq!(
        RequestId::new(LocalityId::new(7), 0),
        Err(ProtocolError::ZeroId("RequestId.sequence"))
    );
}

#[test]
fn one_mebibyte_segmented_payload_round_trips() {
    let mut value = parcel();
    value.segments = (0..16)
        .map(|segment| vec![segment as u8; 64 * 1024])
        .collect();
    let encoded = encode_parcel(&value, ProtocolLimits::default()).unwrap();
    let decoded = decode_parcel(&encoded, ProtocolLimits::default()).unwrap();
    assert_eq!(decoded.payload_len().unwrap(), 1024 * 1024);
    assert_eq!(decoded.segments, value.segments);
}
