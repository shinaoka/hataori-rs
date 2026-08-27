use super::*;
use std::sync::{atomic::AtomicBool, Arc, Barrier};

#[derive(Debug)]
struct Counter(u64);

impl WireValue for Counter {
    const SCHEMA_ID: u64 = 100;

    fn encode(self) -> Result<Segments, ActionError> {
        self.0.encode()
    }

    fn decode(segments: Segments) -> Result<Self, ActionError> {
        Ok(Self(u64::decode(segments)?))
    }
}

impl DistributedObject for Counter {
    const TYPE_ID: u128 = 100;
}

struct Add(u64);

impl WireValue for Add {
    const SCHEMA_ID: u64 = 101;

    fn encode(self) -> Result<Segments, ActionError> {
        self.0.encode()
    }

    fn decode(segments: Segments) -> Result<Self, ActionError> {
        Ok(Self(u64::decode(segments)?))
    }
}

impl ObjectWriteAction<Counter> for Add {
    const ID: u128 = 101;
    type Output = u64;

    fn execute(self, state: &mut Counter) -> Result<Self::Output, ActionError> {
        state.0 += self.0;
        Ok(state.0)
    }
}

struct Get;

impl WireValue for Get {
    const SCHEMA_ID: u64 = 102;

    fn encode(self) -> Result<Segments, ActionError> {
        Ok(Vec::new())
    }

    fn decode(segments: Segments) -> Result<Self, ActionError> {
        if segments.is_empty() {
            Ok(Self)
        } else {
            Err(ActionError::codec("Get has no payload"))
        }
    }
}

impl ObjectReadAction<Counter> for Get {
    const ID: u128 = 102;
    type Output = u64;

    fn execute(self, state: &Counter) -> Result<Self::Output, ActionError> {
        Ok(state.0)
    }
}

fn service() -> (Arc<ObjectService>, ObjectId, ObjectTypeId, ObjectLocation) {
    let mut registry = ObjectRegistry::default();
    registry.register_read_write::<Counter>().unwrap();
    registry.register_read::<Counter, Get>().unwrap();
    registry.register_write::<Counter, Add>().unwrap();
    let run = RunId::new(9).unwrap();
    let service = Arc::new(ObjectService::new(
        run,
        LocalityId::new(0),
        ObjectLimits {
            lease_renew_interval: Duration::from_millis(1),
            lease_ttl: Duration::from_millis(2),
            lease_grace: Duration::from_millis(2),
            ..ObjectLimits::default()
        },
        registry,
    ));
    let object = ObjectId::new(run, 1).unwrap();
    let object_type = ObjectTypeId::new(Counter::TYPE_ID).unwrap();
    let location_bytes = service
        .create_handler(object_type)
        .unwrap()
        .execute(
            ObjectEnvelope {
                object,
                object_type,
                epoch: 1,
                lease_locality: LocalityId::new(0),
                domain: DomainId::DEFAULT,
                rooted: false,
            }
            .encode(Counter(4).encode().unwrap()),
        )
        .unwrap()
        .pop()
        .unwrap();
    let location = decode_location(location_bytes).unwrap();
    (service, object, object_type, location)
}

#[test]
fn directory_resolver_typed_calls_and_collection_are_bounded() {
    let (service, object, object_type, location) = service();
    assert_eq!(service.resolve(object), Some(location));
    let add = service
        .action_handler(ActionId::new(Add::ID).unwrap())
        .unwrap();
    let output = add
        .execute(
            ObjectEnvelope {
                object,
                object_type,
                epoch: location.epoch(),
                lease_locality: LocalityId::new(0),
                domain: location.domain(),
                rooted: false,
            }
            .encode(Add(3).encode().unwrap()),
        )
        .unwrap();
    assert_eq!(u64::decode(output).unwrap(), 7);
    assert_eq!(service.stats(Instant::now()).in_flight_calls, 0);
    assert!(service.release(object, LocalityId::new(0)));
    assert_eq!(service.stats(Instant::now()).live_objects, 0);
}

#[test]
fn stale_epoch_and_busy_writer_fail_without_blocking() {
    let (service, object, object_type, location) = service();
    let get = service
        .action_handler(ActionId::new(Get::ID).unwrap())
        .unwrap();
    let stale = get.execute(
        ObjectEnvelope {
            object,
            object_type,
            epoch: 2,
            lease_locality: LocalityId::new(0),
            domain: location.domain(),
            rooted: false,
        }
        .encode(Vec::new()),
    );
    assert!(matches!(stale, Err(ActionError::User(message)) if message.contains("stale")));

    let entry = service
        .entries
        .lock()
        .unwrap()
        .get(&object)
        .unwrap()
        .clone();
    let erased = entry.resident_state().unwrap();
    let _write = match &*erased {
        ErasedState::ReadWrite(state) => state.write().unwrap(),
        ErasedState::Exclusive(_) => unreachable!(),
    };
    let busy = get.execute(
        ObjectEnvelope {
            object,
            object_type,
            epoch: 1,
            lease_locality: LocalityId::new(0),
            domain: location.domain(),
            rooted: false,
        }
        .encode(Vec::new()),
    );
    assert!(matches!(busy, Err(ActionError::User(message)) if message.contains("saturated")));
}

#[test]
fn read_write_mode_allows_parallel_read_guards() {
    let (service, object, _, _) = service();
    let entry = service
        .entries
        .lock()
        .unwrap()
        .get(&object)
        .unwrap()
        .clone();
    let barrier = Arc::new(Barrier::new(3));
    let mut threads = Vec::new();
    for _ in 0..2 {
        let entry = Arc::clone(&entry);
        let barrier = Arc::clone(&barrier);
        threads.push(std::thread::spawn(move || {
            let erased = entry.resident_state().unwrap();
            let ErasedState::ReadWrite(state) = &*erased else {
                unreachable!()
            };
            let _read = state.read().unwrap();
            barrier.wait();
            barrier.wait();
        }));
    }
    barrier.wait();
    barrier.wait();
    for thread in threads {
        thread.join().unwrap();
    }
}

#[test]
fn admission_runs_parallel_readers_then_writer_then_later_reader() {
    let (service, object, object_type, location) = service();
    let make_job = |action: ActionId, sequence: u64| ActionJob {
        requester: LocalityId::new(1),
        request: hataori_runtime_foundation::protocol::RequestId::new(LocalityId::new(1), sequence)
            .unwrap(),
        action_id: action,
        domain: location.domain(),
        trace_id: None,
        input: ObjectEnvelope {
            object,
            object_type,
            epoch: 1,
            lease_locality: LocalityId::new(1),
            domain: location.domain(),
            rooted: false,
        }
        .encode(if action.get() == Add::ID {
            Add(1).encode().unwrap()
        } else {
            Vec::new()
        }),
        handler: service.action_handler(action).unwrap(),
        cancelled: Arc::new(AtomicBool::new(false)),
        local: false,
        submitted_at: Instant::now(),
        object: None,
    };
    let read = ActionId::new(Get::ID).unwrap();
    let write = ActionId::new(Add::ID).unwrap();
    let first = service
        .admit(read, make_job(read, 1), true)
        .unwrap()
        .unwrap();
    let second = service
        .admit(read, make_job(read, 2), true)
        .unwrap()
        .unwrap();
    assert!(service
        .admit(write, make_job(write, 3), true)
        .unwrap()
        .is_none());
    assert!(service
        .admit(read, make_job(read, 4), true)
        .unwrap()
        .is_none());
    assert!(service.complete(first.object.unwrap()).is_empty());
    assert!(service.retry_ready(8, |_| true).is_empty());
    assert!(service.complete(second.object.unwrap()).is_empty());
    let ready = service.retry_ready(8, |_| true);
    assert_eq!(ready.len(), 1);
    assert!(!ready[0].object.unwrap().read);
    assert!(service.complete(ready[0].object.unwrap()).is_empty());
    let ready = service.retry_ready(8, |_| true);
    assert_eq!(ready.len(), 1);
    assert!(ready[0].object.unwrap().read);
}

#[test]
fn transfer_pin_is_bounded_until_ack_or_release() {
    let (service, object, _, _) = service();
    assert!(service.begin_transfer(object, LocalityId::new(3), Instant::now()));
    assert_eq!(service.stats(Instant::now()).transfers, 1);
    assert!(service.ack_transfer(object, LocalityId::new(3)));
    assert_eq!(service.stats(Instant::now()).transfers, 0);
}

#[test]
fn migration_packet_round_trips_segments_and_rejects_bad_headers() {
    let run = RunId::new(5).unwrap();
    let packet = MigrationPacket {
        migration: RequestId::new(LocalityId::new(1), 9).unwrap(),
        object: ObjectId::new(run, 7).unwrap(),
        object_type: ObjectTypeId::new(8).unwrap(),
        authority: LocalityId::new(1),
        from: ObjectLocation::new(LocalityId::new(1), DomainId::DEFAULT, 2, 1, 3).unwrap(),
        destination: Place::new(LocalityId::new(2), DomainId::new(4)),
        snapshot_version: 6,
        snapshot_schema: 7,
        snapshot: vec![vec![1, 2], vec![3]],
    };
    let encoded = packet.clone().encode().unwrap();
    let decoded = MigrationPacket::decode(encoded.clone()).unwrap();
    assert_eq!(decoded.migration, packet.migration);
    assert_eq!(decoded.object, packet.object);
    assert_eq!(decoded.from, packet.from);
    assert_eq!(decoded.destination, packet.destination);
    assert_eq!(decoded.snapshot, packet.snapshot);
    let mut bad = encoded;
    bad[0][4] = 9;
    assert!(MigrationPacket::decode(bad).is_err());
}

#[test]
fn migration_snapshot_limits_fail_before_retention() {
    let (service, _, _, _) = service();
    let too_many = vec![Vec::new(); service.limits.max_snapshot_segments + 1];
    assert!(matches!(
        service.validate_snapshot(&too_many),
        Err(ActionError::User(message)) if message.contains("segment")
    ));
    let too_large = vec![vec![0; service.limits.max_snapshot_bytes + 1]];
    assert!(matches!(
        service.validate_snapshot(&too_large),
        Err(ActionError::User(message)) if message.contains("byte")
    ));
}

#[test]
fn lease_expiry_uses_suspect_grace_before_collection() {
    let (service, _object, _, _) = service();
    let now = Instant::now();
    service.expire(now + Duration::from_millis(3));
    assert_eq!(
        service.stats(now + Duration::from_millis(3)).suspect_leases,
        1
    );
    service.expire(now + Duration::from_millis(6));
    assert_eq!(
        service.stats(now + Duration::from_millis(6)).live_objects,
        0
    );
}
