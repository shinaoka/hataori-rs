use super::*;
use crate::{
    conformance::{verify_completions, verify_incoming},
    protocol::{MessageId, ParcelKind, RunId, RuntimeVersion},
    transport::{TransportDriver, TransportHandle},
};
use std::net::TcpListener;

fn hello(run: u128, limits: ProtocolLimits) -> Hello {
    Hello {
        run_id: RunId::new(run).unwrap(),
        runtime_version: RuntimeVersion {
            major: 0,
            minor: 1,
            patch: 0,
        },
        action_registry_hash: [1; 32],
        object_registry_hash: [2; 32],
        capabilities: 0b0101,
        limits,
    }
}

fn reserve_endpoints(count: usize) -> Vec<SocketAddr> {
    (0..count)
        .map(|_| {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            listener.local_addr().unwrap()
        })
        .collect()
}

fn connect_group(
    runs: &[u128],
    limits: ProtocolLimits,
) -> Vec<Result<(TcpHandle, TcpDriver), TransportError>> {
    let endpoints = reserve_endpoints(runs.len());
    thread::scope(|scope| {
        let mut joins = Vec::new();
        for (local, run_id) in runs.iter().copied().enumerate() {
            let endpoints = endpoints.clone();
            joins.push(scope.spawn(move || {
                TcpTransport::connect(TcpConfig {
                    local_id: LocalityId::new(local as u64),
                    endpoints,
                    hello: hello(run_id, limits),
                    bootstrap_timeout: Duration::from_secs(5),
                    io_timeout: Duration::from_secs(2),
                })
            }));
        }
        joins.into_iter().map(|join| join.join().unwrap()).collect()
    })
}

fn connect_pair(
    runs: [u128; 2],
    limits: ProtocolLimits,
) -> Vec<Result<(TcpHandle, TcpDriver), TransportError>> {
    connect_group(&runs, limits)
}

fn parcel_between(
    source: u64,
    destination: u64,
    channel: Channel,
    value: u8,
    segments: Vec<Vec<u8>>,
) -> Parcel {
    Parcel {
        run_id: RunId::new(1).unwrap(),
        message_id: MessageId::new(u128::from(value) + 1).unwrap(),
        channel,
        kind: ParcelKind::Data,
        source: LocalityId::new(source),
        destination: LocalityId::new(destination),
        trace_id: None,
        segments,
    }
}

fn parcel(channel: Channel, value: u8, segments: Vec<Vec<u8>>) -> Parcel {
    parcel_between(0, 1, channel, value, segments)
}

fn pump(
    source: &mut TcpDriver,
    destination: &mut TcpDriver,
    source_events: &mut Vec<TransportEvent>,
    destination_events: &mut Vec<TransportEvent>,
    incoming: usize,
) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while destination_events
        .iter()
        .filter(|event| matches!(event, TransportEvent::Incoming { .. }))
        .count()
        < incoming
    {
        source.progress(source_events, 64).unwrap();
        destination.progress(destination_events, 64).unwrap();
        assert!(Instant::now() < deadline, "TCP progress timed out");
        thread::yield_now();
    }
}

#[test]
fn bounded_rendezvous_assigns_one_fixed_membership() {
    let rendezvous_endpoint = reserve_endpoints(1)[0];
    let transport_endpoints = reserve_endpoints(4);
    let results = thread::scope(|scope| {
        let mut joins = Vec::new();
        for (index, local_endpoint) in transport_endpoints.iter().copied().enumerate() {
            joins.push(scope.spawn(move || {
                TcpRendezvous::join(TcpRendezvousConfig {
                    run_id: RunId::new(1).unwrap(),
                    rendezvous_endpoint,
                    local_endpoint,
                    expected_localities: 4,
                    coordinator: index == 0,
                    timeout: Duration::from_secs(5),
                })
            }));
        }
        joins
            .into_iter()
            .map(|join| join.join().unwrap().unwrap())
            .collect::<Vec<_>>()
    });
    let membership = results[0].endpoints.clone();
    assert_eq!(membership.len(), 4);
    let mut ids: Vec<_> = results.iter().map(|result| result.local_id.get()).collect();
    ids.sort_unstable();
    assert_eq!(ids, vec![0, 1, 2, 3]);
    for (index, result) in results.into_iter().enumerate() {
        assert_eq!(result.endpoints, membership);
        assert_eq!(
            result.endpoints[result.local_id.get() as usize],
            transport_endpoints[index]
        );
    }
}

#[test]
fn rendezvous_rejects_wrong_run() {
    let rendezvous_endpoint = reserve_endpoints(1)[0];
    let transport_endpoints = reserve_endpoints(2);
    let results = thread::scope(|scope| {
        [1_u128, 2]
            .into_iter()
            .enumerate()
            .map(|(index, run)| {
                let local_endpoint = transport_endpoints[index];
                scope.spawn(move || {
                    TcpRendezvous::join(TcpRendezvousConfig {
                        run_id: RunId::new(run).unwrap(),
                        rendezvous_endpoint,
                        local_endpoint,
                        expected_localities: 2,
                        coordinator: index == 0,
                        timeout: Duration::from_secs(2),
                    })
                })
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|join| join.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert!(results.into_iter().all(|result| result.is_err()));
}

#[test]
fn handshake_rejects_wrong_run() {
    let results = connect_pair([1, 2], ProtocolLimits::default());
    assert!(results.into_iter().all(|result| result.is_err()));
}

#[test]
fn contract_delivers_all_channels_with_control_priority() {
    let mut pair = connect_pair([1, 1], ProtocolLimits::default())
        .into_iter()
        .map(Result::unwrap)
        .collect::<Vec<_>>();
    let sent = vec![
        parcel(Channel::Bulk, 1, vec![vec![1; 8]]),
        parcel(Channel::Control, 2, vec![vec![2; 8]]),
        parcel(Channel::Action, 3, vec![vec![3; 8]]),
    ];
    let tickets: Vec<_> = sent
        .iter()
        .cloned()
        .map(|parcel| pair[0].0.try_send(parcel).unwrap())
        .collect();
    assert_eq!(pair[0].1.capabilities(), 0b0101);
    assert_eq!(pair[1].1.capabilities(), 0b0101);
    let mut source_events = Vec::new();
    let mut destination_events = Vec::new();
    let (source, destination) = pair.split_at_mut(1);
    pump(
        &mut source[0].1,
        &mut destination[0].1,
        &mut source_events,
        &mut destination_events,
        3,
    );
    let channels: Vec<_> = destination_events
        .iter()
        .filter_map(|event| match event {
            TransportEvent::Incoming { parcel } => Some(parcel.channel),
            _ => None,
        })
        .collect();
    assert_eq!(
        channels,
        vec![Channel::Control, Channel::Action, Channel::Bulk]
    );
    verify_completions(&tickets, &source_events).unwrap();
    verify_incoming(&sent, &destination_events).unwrap();
}

#[test]
fn flush_drives_pending_io_without_losing_events() {
    let mut pair = connect_pair([1, 1], ProtocolLimits::default())
        .into_iter()
        .map(Result::unwrap)
        .collect::<Vec<_>>();
    let sent = parcel(Channel::Action, 6, vec![vec![6; 64 * 1024]]);
    let ticket = pair[0].0.try_send(sent.clone()).unwrap();
    let (source, destination) = pair.split_at_mut(1);
    let destination_events = thread::scope(|scope| {
        let join = scope.spawn(|| {
            let deadline = Instant::now() + Duration::from_secs(5);
            let mut events = Vec::new();
            while !events
                .iter()
                .any(|event| matches!(event, TransportEvent::Incoming { .. }))
            {
                destination[0].1.progress(&mut events, 8).unwrap();
                assert!(Instant::now() < deadline, "TCP flush receive timed out");
                thread::yield_now();
            }
            events
        });
        source[0].1.flush(Duration::from_secs(5)).unwrap();
        join.join().unwrap()
    });
    let mut source_events = Vec::new();
    source[0].1.progress(&mut source_events, 8).unwrap();
    verify_completions(&[ticket], &source_events).unwrap();
    verify_incoming(&[sent], &destination_events).unwrap();
}

#[test]
fn one_mebibyte_segmented_payload_transfers_and_cleans_up() {
    let mut pair = connect_pair([1, 1], ProtocolLimits::default())
        .into_iter()
        .map(Result::unwrap)
        .collect::<Vec<_>>();
    let segments: Vec<_> = (0..16).map(|index| vec![index as u8; 64 * 1024]).collect();
    pair[0]
        .0
        .try_send(parcel(Channel::Bulk, 9, segments.clone()))
        .unwrap();
    let mut source_events = Vec::new();
    let mut destination_events = Vec::new();
    let (source, destination) = pair.split_at_mut(1);
    pump(
        &mut source[0].1,
        &mut destination[0].1,
        &mut source_events,
        &mut destination_events,
        1,
    );
    let received = destination_events
        .iter()
        .find_map(|event| match event {
            TransportEvent::Incoming { parcel } => Some(parcel),
            _ => None,
        })
        .unwrap();
    assert_eq!(received.segments, segments);
    pair[0]
        .1
        .shutdown(&mut source_events, Duration::from_secs(2))
        .unwrap();
    pair[1]
        .1
        .shutdown(&mut destination_events, Duration::from_secs(2))
        .unwrap();
    assert_eq!(pair[0].1.stats().retained_bytes(), 0);
    assert_eq!(pair[1].1.stats().retained_bytes(), 0);
    assert_eq!(
        pair[0]
            .0
            .try_send(parcel(Channel::Control, 7, vec![vec![7; 8]])),
        Err(TransportError::Shutdown)
    );
}

#[test]
fn bounded_submission_and_peer_failure_are_visible() {
    let limits = ProtocolLimits {
        max_queued_parcels_per_peer: 1,
        max_inflight_bytes_per_peer: 1024,
        control_reserved_bytes_per_peer: 256,
        ..ProtocolLimits::default()
    };
    let mut pair = connect_pair([1, 1], limits)
        .into_iter()
        .map(Result::unwrap)
        .collect::<Vec<_>>();
    let mut wrong_run = parcel(Channel::Action, 9, vec![vec![9; 8]]);
    wrong_run.run_id = RunId::new(2).unwrap();
    assert!(matches!(
        pair[0].0.try_send(wrong_run),
        Err(TransportError::Protocol(_))
    ));
    assert!(matches!(
        pair[0]
            .0
            .try_send(parcel(Channel::Bulk, 8, vec![vec![8; 1000]])),
        Err(TransportError::ByteLimit { .. })
    ));
    pair[0]
        .0
        .try_send(parcel(Channel::Bulk, 7, vec![vec![7; 400]]))
        .unwrap();
    assert!(matches!(
        pair[0]
            .0
            .try_send(parcel(Channel::Control, 6, vec![vec![6; 600]])),
        Err(TransportError::ByteLimit { .. })
    ));
    pair[0]
        .0
        .try_send(parcel(Channel::Action, 1, vec![vec![1; 8]]))
        .unwrap();
    assert!(matches!(
        pair[0]
            .0
            .try_send(parcel(Channel::Action, 2, vec![vec![2; 8]])),
        Err(TransportError::QueueFull { .. })
    ));
    pair[0]
        .0
        .try_send(parcel(Channel::Control, 5, vec![vec![5; 100]]))
        .unwrap();
    drop(pair.remove(1));
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        match pair[0].1.progress(&mut Vec::new(), 8) {
            Err(TransportError::Disconnected(_)) | Err(TransportError::Io(_)) => break,
            Ok(_) if Instant::now() < deadline => thread::yield_now(),
            other => panic!("peer failure was not visible: {other:?}"),
        }
    }
    assert_eq!(pair[0].1.stats().retained_bytes(), 0);
    pair[0]
        .1
        .shutdown(&mut Vec::new(), Duration::from_secs(1))
        .unwrap();
}

#[test]
fn oversized_tcp_frame_is_rejected_before_allocation() {
    let mut pair = connect_pair([1, 1], ProtocolLimits::default())
        .into_iter()
        .map(Result::unwrap)
        .collect::<Vec<_>>();
    let declared = (ProtocolLimits::default().max_frame_bytes as u64 + 1).to_le_bytes();
    pair[0].1.peers[1].channels[Channel::Control as usize]
        .as_mut()
        .unwrap()
        .stream
        .write_all(&declared)
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        match pair[1].1.progress(&mut Vec::new(), 8) {
            Err(TransportError::ByteLimit { .. }) => break,
            Ok(_) if Instant::now() < deadline => thread::yield_now(),
            other => panic!("oversized TCP frame was not rejected: {other:?}"),
        }
    }
    assert_eq!(pair[1].1.stats().retained_bytes(), 0);
    pair[1]
        .1
        .shutdown(&mut Vec::new(), Duration::from_secs(1))
        .unwrap();
}

#[test]
fn four_locality_fixed_membership_delivers_a_ring() {
    let mut group = connect_group(&[1, 1, 1, 1], ProtocolLimits::default())
        .into_iter()
        .map(Result::unwrap)
        .collect::<Vec<_>>();
    let mut expected = Vec::new();
    for source in 0..4 {
        let destination = (source + 1) % 4;
        let value = parcel_between(
            source,
            destination,
            Channel::Control,
            source as u8 + 10,
            vec![vec![source as u8; 8]],
        );
        group[source as usize].0.try_send(value).unwrap();
        let previous = (source + 3) % 4;
        expected.push(parcel_between(
            previous,
            source,
            Channel::Control,
            previous as u8 + 10,
            vec![vec![previous as u8; 8]],
        ));
    }
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut events: Vec<Vec<TransportEvent>> = (0..4).map(|_| Vec::new()).collect();
    loop {
        for index in 0..4 {
            group[index].1.progress(&mut events[index], 8).unwrap();
        }
        if events.iter().all(|events| {
            events
                .iter()
                .any(|event| matches!(event, TransportEvent::Incoming { .. }))
        }) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "four-locality TCP ring timed out"
        );
        thread::yield_now();
    }
    for index in 0..4 {
        verify_incoming(&expected[index..=index], &events[index]).unwrap();
        assert_eq!(group[index].1.members().len(), 4);
    }
    for index in 0..4 {
        group[index]
            .1
            .shutdown(&mut events[index], Duration::from_secs(2))
            .unwrap();
    }
}

#[test]
fn process_can_construct_a_fresh_transport_after_clean_shutdown() {
    for _ in 0..2 {
        let mut pair = connect_pair([1, 1], ProtocolLimits::default())
            .into_iter()
            .map(Result::unwrap)
            .collect::<Vec<_>>();
        let mut events = Vec::new();
        pair[0]
            .1
            .shutdown(&mut events, Duration::from_secs(2))
            .unwrap();
        pair[1]
            .1
            .shutdown(&mut events, Duration::from_secs(2))
            .unwrap();
    }
}
