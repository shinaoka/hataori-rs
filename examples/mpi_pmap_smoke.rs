#[cfg(feature = "rayon")]
use mpi_upstream as mpi_api;
#[cfg(feature = "rayon")]
#[path = "support/hybrid_smoke.rs"]
mod hybrid_smoke;

#[cfg(not(feature = "rayon"))]
use hataori::{pmap, Domain, LocalMode, PmapErrorKind, PmapOptions};
#[cfg(not(feature = "rayon"))]
use mpi_upstream::traits::*;
#[cfg(not(feature = "rayon"))]
use std::num::NonZeroUsize;

#[cfg(not(feature = "rayon"))]
fn options(batch_size: usize) -> PmapOptions {
    PmapOptions {
        root: 0,
        batch_size: NonZeroUsize::new(batch_size).unwrap(),
        local_mode: LocalMode::Sequential,
    }
}

#[cfg(feature = "rayon")]
fn main() {
    use mpi_upstream::environment::Threading;
    let (universe, provided) = mpi_upstream::initialize_with_threading(Threading::Funneled)
        .expect("MPI must not already be initialized or finalized");
    assert!(provided >= Threading::Funneled);
    hybrid_smoke::run(&universe.world());
}

#[cfg(not(feature = "rayon"))]
fn main() {
    let universe = mpi_upstream::initialize().expect("MPI must not already be finalized");
    let world = universe.world();
    let rank = world.rank();
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
    assert!(error.message().contains("expected user failure"));
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
