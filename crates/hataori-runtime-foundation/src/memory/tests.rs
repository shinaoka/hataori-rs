use super::*;
use crate::{
    conformance::{verify_completions, verify_incoming},
    protocol::{MessageId, ParcelKind},
    transport::{TransportDriver, TransportHandle},
};

fn parcel(source: u64, destination: u64, channel: Channel, value: u8) -> Parcel {
    Parcel {
        run_id: RunId::new(1).unwrap(),
        message_id: MessageId::new(u128::from(value) + 1).unwrap(),
        channel,
        kind: ParcelKind::Data,
        source: LocalityId::new(source),
        destination: LocalityId::new(destination),
        trace_id: None,
        segments: vec![vec![value; 8]],
    }
}

fn incoming_values(events: &[TransportEvent]) -> Vec<u8> {
    events
        .iter()
        .filter_map(|event| match event {
            TransportEvent::Incoming { parcel } => Some(parcel.segments[0][0]),
            _ => None,
        })
        .collect()
}

#[test]
fn contract_delivers_and_reports_local_completion() {
    let mut nodes =
        MemoryNetwork::build(2, RunId::new(1).unwrap(), ProtocolLimits::default(), []).unwrap();
    let sent = parcel(0, 1, Channel::Action, 7);
    let ticket = nodes[0].0.try_send(sent.clone()).unwrap();
    let mut source_events = Vec::new();
    nodes[0].1.progress(&mut source_events, usize::MAX).unwrap();
    verify_completions(&[ticket], &source_events).unwrap();
    let mut destination_events = Vec::new();
    nodes[1]
        .1
        .progress(&mut destination_events, usize::MAX)
        .unwrap();
    verify_incoming(&[sent], &destination_events).unwrap();
    assert_eq!(incoming_values(&destination_events), vec![7]);
    assert_eq!(nodes[1].1.stats().retained_bytes(), 0);
}

#[test]
fn control_progresses_before_earlier_bulk() {
    let mut nodes =
        MemoryNetwork::build(2, RunId::new(1).unwrap(), ProtocolLimits::default(), []).unwrap();
    nodes[0].0.try_send(parcel(0, 1, Channel::Bulk, 1)).unwrap();
    nodes[0]
        .0
        .try_send(parcel(0, 1, Channel::Control, 2))
        .unwrap();
    let mut events = Vec::new();
    nodes[1].1.progress(&mut events, 1).unwrap();
    assert_eq!(incoming_values(&events), vec![2]);
    nodes[1].1.progress(&mut events, 1).unwrap();
    assert_eq!(incoming_values(&events), vec![2, 1]);
}

#[test]
fn deterministic_faults_cover_delay_duplicate_loss_and_reordering() {
    let faults = [
        MemoryFault::Delay { progress_ticks: 2 },
        MemoryFault::Duplicate,
        MemoryFault::Loss,
        MemoryFault::Pass,
        MemoryFault::Reorder,
    ];
    let mut nodes =
        MemoryNetwork::build(2, RunId::new(1).unwrap(), ProtocolLimits::default(), faults).unwrap();
    for value in 1..=5 {
        nodes[0]
            .0
            .try_send(parcel(0, 1, Channel::Action, value))
            .unwrap();
    }
    let mut events = Vec::new();
    nodes[1].1.progress(&mut events, 1).unwrap();
    assert_eq!(incoming_values(&events), vec![5]);
    nodes[1].1.progress(&mut events, usize::MAX).unwrap();
    assert_eq!(incoming_values(&events), vec![5, 2, 2, 4, 1]);
}

#[test]
fn saturation_disconnect_and_wrong_run_are_visible() {
    let saturated = MemoryNetwork::build(
        2,
        RunId::new(1).unwrap(),
        ProtocolLimits::default(),
        [MemoryFault::Saturate],
    )
    .unwrap();
    assert!(matches!(
        saturated[0].0.try_send(parcel(0, 1, Channel::Action, 1)),
        Err(TransportError::QueueFull { .. })
    ));

    let mut disconnected = MemoryNetwork::build(
        2,
        RunId::new(1).unwrap(),
        ProtocolLimits::default(),
        [MemoryFault::Disconnect],
    )
    .unwrap();
    disconnected[0]
        .0
        .try_send(parcel(0, 1, Channel::Action, 1))
        .unwrap();
    let mut events = Vec::new();
    disconnected[0].1.progress(&mut events, usize::MAX).unwrap();
    assert!(matches!(events[0], TransportEvent::PeerFailed { .. }));

    let mut wrong = parcel(0, 1, Channel::Action, 1);
    wrong.run_id = RunId::new(2).unwrap();
    assert!(matches!(
        disconnected[0].0.try_send(wrong),
        Err(TransportError::Protocol(_))
    ));
}

#[test]
fn count_and_byte_limits_fail_without_retention() {
    let limits = ProtocolLimits {
        max_queued_parcels_per_peer: 1,
        max_inflight_bytes_per_peer: 10,
        control_reserved_bytes_per_peer: 2,
        ..ProtocolLimits::default()
    };
    let nodes = MemoryNetwork::build(2, RunId::new(1).unwrap(), limits, []).unwrap();
    nodes[0]
        .0
        .try_send(parcel(0, 1, Channel::Action, 1))
        .unwrap();
    assert!(matches!(
        nodes[0].0.try_send(parcel(0, 1, Channel::Action, 2)),
        Err(TransportError::QueueFull { .. })
    ));
    assert_eq!(nodes[1].1.stats().queued_action, 1);
}

#[test]
fn control_reservation_survives_bulk_byte_saturation() {
    let limits = ProtocolLimits {
        max_inflight_bytes_per_peer: 10,
        control_reserved_bytes_per_peer: 2,
        ..ProtocolLimits::default()
    };
    let nodes = MemoryNetwork::build(2, RunId::new(1).unwrap(), limits, []).unwrap();
    nodes[0].0.try_send(parcel(0, 1, Channel::Bulk, 1)).unwrap();
    let mut control = parcel(0, 1, Channel::Control, 2);
    control.segments = vec![vec![2]];
    nodes[0].0.try_send(control).unwrap();
    let mut another_bulk = parcel(0, 1, Channel::Bulk, 3);
    another_bulk.segments = vec![vec![3]];
    assert!(matches!(
        nodes[0].0.try_send(another_bulk),
        Err(TransportError::ByteLimit { .. })
    ));
}

#[test]
fn clean_shutdown_clears_delayed_and_queued_resources() {
    let mut nodes = MemoryNetwork::build(
        2,
        RunId::new(1).unwrap(),
        ProtocolLimits::default(),
        [MemoryFault::Delay {
            progress_ticks: 100,
        }],
    )
    .unwrap();
    nodes[0].0.try_send(parcel(0, 1, Channel::Bulk, 1)).unwrap();
    let mut events = Vec::new();
    nodes[0]
        .1
        .shutdown(&mut events, Duration::from_millis(50))
        .unwrap();
    nodes[1]
        .1
        .shutdown(&mut events, Duration::from_millis(50))
        .unwrap();
    assert_eq!(nodes[0].1.stats().retained_bytes(), 0);
    assert_eq!(nodes[1].1.stats().retained_bytes(), 0);
    assert_eq!(
        nodes[0].0.try_send(parcel(0, 1, Channel::Control, 9)),
        Err(TransportError::Shutdown)
    );
    assert!(events.contains(&TransportEvent::ShutdownComplete));
}
