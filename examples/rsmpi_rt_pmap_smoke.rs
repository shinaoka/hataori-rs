use mpi_runtime as mpi_api;
#[cfg(feature = "rayon")]
#[path = "support/hybrid_smoke.rs"]
mod hybrid_smoke;

#[cfg(not(feature = "rayon"))]
use hataori::{pmap, Domain, LocalMode, PmapErrorKind, PmapOptions};
#[cfg(not(feature = "rayon"))]
use mpi_runtime::traits::*;
#[cfg(not(feature = "rayon"))]
use std::num::NonZeroUsize;

#[cfg(not(feature = "rayon"))]
fn options(batch_size: usize) -> PmapOptions {
    PmapOptions {
        root: 0,
        batch_size: NonZeroUsize::new(batch_size).unwrap(),
        local_mode: LocalMode::Sequential,
        prefetch: false,
    }
}

#[cfg(feature = "rayon")]
fn main() {
    use mpi_runtime::environment::Threading;
    use std::path::Path;

    let library =
        std::env::var("MPI_RT_LIB").expect("MPI_RT_LIB must be set before MPI initialization");
    assert!(Path::new(&library).is_absolute());
    let (universe, provided) = mpi_runtime::initialize_with_threading(Threading::Funneled)
        .expect("MPI must not already be initialized or finalized");
    assert!(provided >= Threading::Funneled);
    hybrid_smoke::run(&universe.world());
}

#[cfg(not(feature = "rayon"))]
fn main() {
    let mpi_rt_lib =
        std::env::var("MPI_RT_LIB").expect("MPI_RT_LIB must be set before MPI initialization");
    assert!(
        std::path::Path::new(&mpi_rt_lib).is_absolute(),
        "MPI_RT_LIB must be an absolute path"
    );
    println!("MPI_RT_LIB={mpi_rt_lib}");

    let universe = mpi_runtime::initialize().expect("MPI must not already be finalized");
    let world = universe.world();
    let rank = world.rank();
    assert_eq!(
        std::env::var("MPI_RT_LIB").as_deref(),
        Ok(mpi_rt_lib.as_str()),
        "MPI_RT_LIB changed before pmap"
    );
    println!("rank {rank}: MPI_RT_LIB={mpi_rt_lib}");
    let domain = Domain::sequential();

    let empty = pmap(
        &world,
        &domain,
        options(1),
        (rank == 0).then(Vec::<i32>::new),
        Ok::<_, String>,
    )
    .unwrap();
    assert_eq!(empty.is_some(), rank == 0);
    if let Some(values) = empty {
        assert!(values.is_empty());
    }

    let rejected_calls = std::cell::Cell::new(0_i32);
    let rejected = pmap(
        &world,
        &domain,
        PmapOptions {
            prefetch: true,
            ..options(1)
        },
        (rank == 0).then(|| vec![1_i32]),
        |item| {
            rejected_calls.set(rejected_calls.get() + 1);
            Ok::<_, String>(item)
        },
    )
    .unwrap_err();
    assert_eq!(rejected.kind(), PmapErrorKind::Preflight);
    let mut total_rejected_calls = 0_i32;
    world.all_reduce_into(
        &rejected_calls.get(),
        &mut total_rejected_calls,
        mpi_api::collective::SystemOperation::sum(),
    );
    assert_eq!(total_rejected_calls, 0);

    let one_calls = std::cell::Cell::new(0_i32);
    let one = pmap(
        &world,
        &domain,
        options(1),
        (rank == 0).then(|| vec![41_i32]),
        |item| {
            one_calls.set(one_calls.get() + 1);
            Ok::<_, String>(item + 1)
        },
    )
    .unwrap();
    let mut total_one_calls = 0_i32;
    world.all_reduce_into(
        &one_calls.get(),
        &mut total_one_calls,
        mpi_api::collective::SystemOperation::sum(),
    );
    assert_eq!(total_one_calls, 1);
    if rank == 0 {
        assert_eq!(one.unwrap(), vec![42]);
    }

    let delayed_calls = std::cell::Cell::new(0_i32);
    let delayed = pmap(
        &world,
        &domain,
        options(1),
        (rank == 0).then(|| (0..12).collect::<Vec<i32>>()),
        |item| {
            delayed_calls.set(delayed_calls.get() + 1);
            std::thread::sleep(std::time::Duration::from_millis((12 - item) as u64 * 2));
            Ok::<_, String>(item * 3)
        },
    )
    .unwrap();
    let mut total_delayed_calls = 0_i32;
    world.all_reduce_into(
        &delayed_calls.get(),
        &mut total_delayed_calls,
        mpi_api::collective::SystemOperation::sum(),
    );
    assert_eq!(total_delayed_calls, 12);
    if rank == 0 {
        assert_eq!(
            delayed.unwrap(),
            (0..12).map(|item| item * 3).collect::<Vec<_>>()
        );
    }

    let last = world.size() - 1;
    let last_root = pmap(
        &world,
        &domain,
        PmapOptions {
            root: last,
            ..options(2)
        },
        (rank == last).then(|| vec![2_i32, 7]),
        |item| Ok::<_, String>(item * 5),
    )
    .unwrap();
    if rank == last {
        assert_eq!(last_root.unwrap(), vec![10, 35]);
    } else {
        assert!(last_root.is_none());
    }

    let input = (rank == 0).then(|| (0..9).collect::<Vec<i32>>());
    let ordered = pmap(&world, &domain, options(2), input, |x| {
        Ok::<_, String>(x * 2)
    })
    .unwrap();
    if rank == 0 {
        assert_eq!(ordered.unwrap(), (0..9).map(|x| x * 2).collect::<Vec<_>>());
    } else {
        assert!(ordered.is_none());
    }

    let input = (rank == 0).then(|| (0..8).collect::<Vec<i32>>());
    let error = pmap(&world, &domain, options(1), input, |x| {
        if x == 3 {
            Err::<i32, _>("expected user failure".to_owned())
        } else {
            Ok(x)
        }
    })
    .unwrap_err();
    assert_eq!(error.kind(), PmapErrorKind::User);
    assert_eq!(error.message(), "expected user failure");
    assert!(error.key().is_some());

    let input = (rank == 0).then(|| vec![3_i32, 1, 4, 1, 5]);
    let reused = pmap(&world, &domain, options(1), input, |x| {
        Ok::<_, String>(x + 10)
    })
    .unwrap();
    if rank == 0 {
        assert_eq!(reused.unwrap(), vec![13, 11, 14, 11, 15]);
    } else {
        assert!(reused.is_none());
    }
}
