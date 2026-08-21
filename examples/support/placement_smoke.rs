use super::mpi_api::traits::*;
use hataori::{broadcast, gather, scatter, PlacementErrorKind};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::rc::Rc;

#[derive(Debug, Eq, PartialEq, Deserialize)]
struct EncodeFails(i32);

impl Serialize for EncodeFails {
    fn serialize<S: Serializer>(&self, _serializer: S) -> Result<S::Ok, S::Error> {
        Err(serde::ser::Error::custom(
            "expected placement encode failure",
        ))
    }
}

#[derive(Debug, Eq, PartialEq)]
struct LocalOnly(Rc<i32>);

impl Serialize for LocalOnly {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.as_ref().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for LocalOnly {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        i32::deserialize(deserializer).map(|value| Self(Rc::new(value)))
    }
}

#[derive(Debug, Eq, PartialEq, Serialize)]
struct DecodeFails(i32);

impl<'de> Deserialize<'de> for DecodeFails {
    fn deserialize<D: Deserializer<'de>>(_deserializer: D) -> Result<Self, D::Error> {
        Err(serde::de::Error::custom(
            "expected placement decode failure",
        ))
    }
}

pub fn run<C: Communicator>(world: &C) {
    let rank = world.rank();
    let size = world.size();
    let last = size - 1;

    let copied = broadcast(
        world,
        last,
        (rank == last).then(|| "broadcast-値".to_owned()),
    )
    .unwrap();
    assert_eq!(copied, "broadcast-値");

    let shard = scatter(
        world,
        last,
        (rank == last).then(|| {
            (0..size)
                .map(|target| format!("shard-{target}-値"))
                .collect()
        }),
    )
    .unwrap();
    assert_eq!(shard, format!("shard-{rank}-値"));

    let gather_root = if size > 1 { 1 } else { 0 };
    let gathered = gather(world, gather_root, format!("value-{rank}-値")).unwrap();
    if rank == gather_root {
        assert_eq!(
            gathered.unwrap(),
            (0..size)
                .map(|source| format!("value-{source}-値"))
                .collect::<Vec<_>>()
        );
    } else {
        assert!(gathered.is_none());
    }

    let empty = broadcast(world, 0, (rank == 0).then(String::new)).unwrap();
    assert!(empty.is_empty());

    let large = broadcast(
        world,
        0,
        (rank == 0).then(|| vec![0x5a_u8; 2 * 1024 * 1024]),
    )
    .unwrap();
    assert_eq!(large.len(), 2 * 1024 * 1024);
    assert!(large.iter().all(|byte| *byte == 0x5a));

    let large_shard = scatter(
        world,
        0,
        (rank == 0).then(|| {
            (0..size)
                .map(|target| vec![target as u8; 1024 * 1024])
                .collect()
        }),
    )
    .unwrap();
    assert_eq!(large_shard.len(), 1024 * 1024);
    assert!(large_shard.iter().all(|byte| *byte == rank as u8));

    let large_gather = gather(world, last, vec![rank as u8; 256 * 1024]).unwrap();
    if rank == last {
        for (source, value) in large_gather.unwrap().into_iter().enumerate() {
            assert_eq!(value.len(), 256 * 1024);
            assert!(value.iter().all(|byte| *byte == source as u8));
        }
    }

    for round in 0..3 {
        let value = broadcast(world, 0, (rank == 0).then_some(round)).unwrap();
        assert_eq!(value, round);
    }

    let invalid_root = broadcast(world, size, None::<i32>).unwrap_err();
    assert_eq!(invalid_root.kind(), PlacementErrorKind::Preflight);
    assert_eq!(invalid_root.rank(), Some(0));

    let missing_root = broadcast(world, last, None::<i32>).unwrap_err();
    assert_eq!(missing_root.kind(), PlacementErrorKind::Preflight);
    assert_eq!(missing_root.rank(), Some(last));

    let wrong_shards = scatter(
        world,
        0,
        (rank == 0).then(|| vec![0_i32; size.saturating_sub(1) as usize]),
    )
    .unwrap_err();
    assert_eq!(wrong_shards.kind(), PlacementErrorKind::Preflight);
    assert_eq!(wrong_shards.rank(), Some(0));

    if size > 1 {
        let root_mismatch = if rank == 0 { 0 } else { last };
        let mismatch = broadcast(
            world,
            root_mismatch,
            (rank == root_mismatch).then_some(1_i32),
        )
        .unwrap_err();
        assert_eq!(mismatch.kind(), PlacementErrorKind::Preflight);
        assert_eq!(mismatch.rank(), Some(0));

        let operation_mismatch = if rank == 0 {
            broadcast(world, 0, Some(1_i32)).map(|_| ())
        } else {
            gather(world, 0, rank).map(|_| ())
        }
        .unwrap_err();
        assert_eq!(operation_mismatch.kind(), PlacementErrorKind::Preflight);
        assert_eq!(operation_mismatch.rank(), Some(0));

        let encode_error =
            broadcast(world, last, (rank == last).then_some(EncodeFails(rank))).unwrap_err();
        assert_eq!(encode_error.kind(), PlacementErrorKind::Wire);
        assert_eq!(encode_error.rank(), Some(last));
        assert!(encode_error
            .message()
            .contains("expected placement encode failure"));

        let decode_error =
            broadcast(world, 0, (rank == 0).then_some(DecodeFails(rank))).unwrap_err();
        assert_eq!(decode_error.kind(), PlacementErrorKind::Wire);
        assert_eq!(decode_error.rank(), Some(1));
        assert!(decode_error
            .message()
            .contains("expected placement decode failure"));

        if rank == 0 {
            world.process_at_rank(1).send_with_tag(&[91_u8], 2);
        }
        let isolated = broadcast(world, 0, (rank == 0).then_some(7_i32)).unwrap();
        assert_eq!(isolated, 7);
        if rank == 1 {
            let (caller_message, _) = world.process_at_rank(0).receive_vec_with_tag::<u8>(2);
            assert_eq!(caller_message, vec![91]);
        }
        world.barrier();
    } else {
        assert_eq!(broadcast(world, 0, Some(EncodeFails(1))).unwrap().0, 1);
        assert_eq!(scatter(world, 0, Some(vec![EncodeFails(2)])).unwrap().0, 2);
        assert_eq!(gather(world, 0, EncodeFails(3)).unwrap().unwrap()[0].0, 3);

        assert_eq!(
            *broadcast(world, 0, Some(LocalOnly(Rc::new(4)))).unwrap().0,
            4
        );
        assert_eq!(
            *scatter(world, 0, Some(vec![LocalOnly(Rc::new(5))]))
                .unwrap()
                .0,
            5
        );
        assert_eq!(
            *gather(world, 0, LocalOnly(Rc::new(6))).unwrap().unwrap()[0].0,
            6
        );
    }
}
