use hataori_runtime_foundation::{
    protocol::{
        Channel, Hello, MessageId, Parcel, ParcelKind, ProtocolLimits, RunId, RuntimeVersion,
    },
    tcp::{TcpRendezvous, TcpRendezvousConfig, TcpTransport},
    transport::{TransportDriver, TransportEvent, TransportHandle},
};
use std::{
    net::{SocketAddr, TcpListener},
    thread,
    time::{Duration, Instant},
};

fn reserve_endpoint() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}

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
        capabilities: 1,
        limits: ProtocolLimits::default(),
    }
}

fn main() {
    let rendezvous_endpoint = reserve_endpoint();
    let endpoints = [reserve_endpoint(), reserve_endpoint()];
    thread::scope(|scope| {
        let joins: Vec<_> = endpoints
            .into_iter()
            .enumerate()
            .map(|(index, local_endpoint)| {
                scope.spawn(move || {
                    let membership = TcpRendezvous::join(TcpRendezvousConfig {
                        run_id: RunId::new(1).unwrap(),
                        rendezvous_endpoint,
                        local_endpoint,
                        expected_localities: 2,
                        coordinator: index == 0,
                        timeout: Duration::from_secs(5),
                    })
                    .unwrap();
                    let local_id = membership.local_id;
                    let (handle, mut driver) = TcpTransport::connect(
                        hataori_runtime_foundation::tcp::TcpConfig {
                            local_id,
                            endpoints: membership.endpoints,
                            hello: hello(),
                            bootstrap_timeout: Duration::from_secs(5),
                            io_timeout: Duration::from_secs(2),
                        },
                    )
                    .unwrap();
                    let peer = hataori_runtime_foundation::protocol::LocalityId::new(
                        1 - local_id.get(),
                    );
                    let mut expected_completions = 0;
                    if local_id.get() == 0 {
                        handle
                            .try_send(Parcel {
                                run_id: RunId::new(1).unwrap(),
                                message_id: MessageId::new(1).unwrap(),
                                channel: Channel::Bulk,
                                kind: ParcelKind::Data,
                                source: local_id,
                                destination: peer,
                                trace_id: None,
                                segments: (0..16)
                                    .map(|segment| vec![segment as u8; 64 * 1024])
                                    .collect(),
                            })
                            .unwrap();
                        expected_completions = 1;
                    }
                    let deadline = Instant::now() + Duration::from_secs(10);
                    let mut events = Vec::new();
                    let mut ack_sent = false;
                    loop {
                        driver.progress(&mut events, 64).unwrap();
                        if local_id.get() == 1 && !ack_sent {
                            if let Some(parcel) = events.iter().find_map(|event| match event {
                                TransportEvent::Incoming { parcel }
                                    if parcel.message_id.get() == 1 => Some(parcel),
                                _ => None,
                            }) {
                                assert_eq!(parcel.payload_len().unwrap(), 1024 * 1024);
                                handle
                                    .try_send(Parcel {
                                        run_id: RunId::new(1).unwrap(),
                                        message_id: MessageId::new(2).unwrap(),
                                        channel: Channel::Control,
                                        kind: ParcelKind::Control,
                                        source: local_id,
                                        destination: peer,
                                        trace_id: None,
                                        segments: vec![b"ack".to_vec()],
                                    })
                                    .unwrap();
                                expected_completions = 1;
                                ack_sent = true;
                            }
                        }
                        let completions = events
                            .iter()
                            .filter(|event| {
                                matches!(event, TransportEvent::LocalSendComplete { .. })
                            })
                            .count();
                        let received_ack = events.iter().any(|event| {
                            matches!(event, TransportEvent::Incoming { parcel } if parcel.message_id.get() == 2)
                        });
                        let done = completions == expected_completions
                            && ((local_id.get() == 0 && received_ack)
                                || (local_id.get() == 1 && ack_sent));
                        if done {
                            break;
                        }
                        assert!(Instant::now() < deadline, "TCP smoke timed out");
                        thread::yield_now();
                    }
                    driver
                        .shutdown(&mut events, Duration::from_secs(5))
                        .unwrap();
                    assert_eq!(driver.stats().retained_bytes(), 0);
                })
            })
            .collect();
        for join in joins {
            join.join().unwrap();
        }
    });
}
