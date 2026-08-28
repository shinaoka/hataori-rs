use hataori_runtime_foundation::{
    conformance::{verify_completions, verify_incoming},
    mpi::MpiTransport,
    protocol::{
        Channel, Hello, LocalityId, MessageId, Parcel, ParcelKind, ProtocolLimits, RunId,
        RuntimeVersion,
    },
    transport::{TransportDriver, TransportEvent, TransportHandle},
};
use mpi::topology::Communicator;
use std::time::{Duration, Instant};

fn hello() -> Hello {
    Hello {
        run_id: RunId::new(1).unwrap(),
        runtime_version: RuntimeVersion {
            major: 0,
            minor: 1,
            patch: 0,
        },
        action_registry_hash: [1; 32],
        object_registry_hash: [2; 32],
        capabilities: 0b0101,
        limits: ProtocolLimits::default(),
    }
}

fn parcel(
    rank: i32,
    destination: i32,
    id: u128,
    channel: Channel,
    segments: Vec<Vec<u8>>,
) -> Parcel {
    Parcel {
        run_id: RunId::new(1).unwrap(),
        message_id: MessageId::new(id).unwrap(),
        channel,
        kind: ParcelKind::Data,
        source: LocalityId::new(rank as u64),
        destination: LocalityId::new(destination as u64),
        trace_id: None,
        segments,
    }
}

fn run_once<C: Communicator>(world: &C, large: bool) {
    let rank = world.rank();
    let size = world.size();
    let (handle, mut driver) = MpiTransport::connect(world, hello()).unwrap();
    assert_eq!(driver.capabilities(), 0b0101);
    let mut tickets = Vec::new();
    let mut expected = Vec::new();
    if size > 1 {
        let destination = (rank + 1) % size;
        for (offset, channel) in [Channel::Bulk, Channel::Action, Channel::Control]
            .into_iter()
            .enumerate()
        {
            let value = parcel(
                rank,
                destination,
                rank as u128 * 100 + offset as u128 + 1,
                channel,
                vec![vec![rank as u8, channel as u8]],
            );
            tickets.push(handle.try_send(value).unwrap());
        }
        let previous = (rank + size - 1) % size;
        expected.extend(
            [Channel::Bulk, Channel::Action, Channel::Control]
                .into_iter()
                .enumerate()
                .map(|(offset, channel)| {
                    parcel(
                        previous,
                        rank,
                        previous as u128 * 100 + offset as u128 + 1,
                        channel,
                        vec![vec![previous as u8, channel as u8]],
                    )
                }),
        );
        if large && rank == 0 {
            let value = parcel(
                rank,
                1,
                9999,
                Channel::Bulk,
                (0..16)
                    .map(|segment| vec![segment as u8; 64 * 1024])
                    .collect(),
            );
            tickets.push(handle.try_send(value).unwrap());
        }
        if large && rank == 1 {
            expected.push(parcel(
                0,
                1,
                9999,
                Channel::Bulk,
                (0..16)
                    .map(|segment| vec![segment as u8; 64 * 1024])
                    .collect(),
            ));
        }
    }

    let deadline = Instant::now() + Duration::from_secs(30);
    let mut events = Vec::new();
    loop {
        driver.progress(&mut events, 64).unwrap();
        let incoming = events
            .iter()
            .filter(|event| matches!(event, TransportEvent::Incoming { .. }))
            .count();
        let completions = events
            .iter()
            .filter(|event| matches!(event, TransportEvent::LocalSendComplete { .. }))
            .count();
        if incoming == expected.len() && completions == tickets.len() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "MPI transport progress timed out"
        );
        std::thread::yield_now();
    }

    verify_completions(&tickets, &events).unwrap();
    verify_incoming(&expected, &events).unwrap();
    if size > 1 {
        let incoming: Vec<_> = events
            .iter()
            .filter_map(|event| match event {
                TransportEvent::Incoming { parcel } => Some(parcel),
                _ => None,
            })
            .collect();
        assert!(incoming
            .iter()
            .any(|parcel| parcel.channel == Channel::Control));
        assert!(incoming
            .iter()
            .any(|parcel| parcel.channel == Channel::Action));
        assert!(incoming
            .iter()
            .any(|parcel| parcel.channel == Channel::Bulk));
        if large && rank == 1 {
            let control_position = incoming
                .iter()
                .position(|parcel| parcel.channel == Channel::Control)
                .unwrap();
            let large_position = incoming
                .iter()
                .position(|parcel| parcel.message_id.get() == 9999)
                .unwrap();
            assert!(control_position < large_position);
            let large_parcel = incoming[large_position];
            assert_eq!(large_parcel.payload_len().unwrap(), 1024 * 1024);
            for (index, segment) in large_parcel.segments.iter().enumerate() {
                assert!(segment.iter().all(|byte| *byte == index as u8));
            }
        }
    }

    driver.flush(Duration::from_secs(30)).unwrap();
    driver
        .shutdown(&mut events, Duration::from_secs(30))
        .unwrap();
    assert_eq!(driver.stats().retained_bytes(), 0);
    if size > 1 {
        assert_eq!(
            handle.try_send(parcel(
                rank,
                (rank + 1) % size,
                100_000 + rank as u128,
                Channel::Control,
                vec![vec![0]],
            )),
            Err(hataori_runtime_foundation::transport::TransportError::Shutdown)
        );
    }
}

fn run_flush<C: Communicator>(world: &C) {
    let rank = world.rank();
    let size = world.size();
    let (handle, mut driver) = MpiTransport::connect(world, hello()).unwrap();
    assert_eq!(driver.capabilities(), 0b0101);
    let mut tickets = Vec::new();
    let mut expected = Vec::new();
    if size > 1 {
        let destination = (rank + 1) % size;
        let value = parcel(
            rank,
            destination,
            200_000 + rank as u128,
            Channel::Control,
            vec![vec![rank as u8; 32]],
        );
        tickets.push(handle.try_send(value).unwrap());
        let previous = (rank + size - 1) % size;
        expected.push(parcel(
            previous,
            rank,
            200_000 + previous as u128,
            Channel::Control,
            vec![vec![previous as u8; 32]],
        ));
    }
    driver.flush(Duration::from_secs(30)).unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut events = Vec::new();
    loop {
        driver.progress(&mut events, 8).unwrap();
        let incoming = events
            .iter()
            .filter(|event| matches!(event, TransportEvent::Incoming { .. }))
            .count();
        if incoming == expected.len() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "MPI post-flush progress timed out"
        );
        std::thread::yield_now();
    }
    verify_completions(&tickets, &events).unwrap();
    verify_incoming(&expected, &events).unwrap();
    driver
        .shutdown(&mut events, Duration::from_secs(30))
        .unwrap();
}

fn main() {
    let universe = mpi::initialize().expect("MPI must not already be initialized or finalized");
    let world = universe.world();
    run_once(&world, true);
    run_flush(&world);
    run_once(&world, false);
}
