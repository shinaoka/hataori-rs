//! Hybrid MPI + Rayon execution: `hataori::pmap` with a Rayon domain on every
//! rank.
//!
//! Build and run:
//!
//! ```sh
//! cargo build -p hataori-tutorial-code --features mpi,rayon --bin hybrid_pmap
//! mpiexec -n 2 target/debug/hybrid_pmap
//! ```

#[cfg(all(any(feature = "mpi", feature = "rsmpi-rt"), feature = "rayon"))]
#[path = "../support/mpi_backend.rs"]
mod mpi_backend;

#[cfg(not(all(any(feature = "mpi", feature = "rsmpi-rt"), feature = "rayon")))]
fn main() {
    println!("HATAORI_TUTORIAL_SKIP: hybrid_pmap needs --features mpi,rayon or rsmpi-rt,rayon");
}

#[cfg(all(any(feature = "mpi", feature = "rsmpi-rt"), feature = "rayon"))]
fn main() {
    tutorial::run();
}

#[cfg(all(any(feature = "mpi", feature = "rsmpi-rt"), feature = "rayon"))]
mod tutorial {
    use super::mpi_backend::{describe_backend, mpi_api};
    use hataori::{pmap, Domain, LocalMode, PmapOptions};
    use mpi_api::environment::Threading;
    use mpi_api::traits::*;
    use std::num::NonZeroUsize;

    // snippet-start:hybrid-init
    /// Hybrid `pmap` keeps every MPI call on the thread that initialized MPI
    /// while Rayon workers evaluate callbacks, so MPI must be initialized with
    /// at least `MPI_THREAD_FUNNELED`.
    fn initialize_funneled() -> mpi_api::environment::Universe {
        let (universe, provided) = mpi_api::initialize_with_threading(Threading::Funneled)
            .expect("MPI must not already be initialized");
        assert!(
            provided >= Threading::Funneled,
            "hybrid pmap requires MPI_THREAD_FUNNELED or stronger"
        );
        universe
    }
    // snippet-end:hybrid-init

    // snippet-start:hybrid-call
    fn heavy(item: u64) -> Result<u64, String> {
        // A callback that is worth spreading across threads and ranks.
        let mut acc = 0_u64;
        for k in 0..(1_000 + item * 100) {
            acc = acc.wrapping_add(k * k % 7);
        }
        Ok(acc)
    }

    /// Every rank builds a Rayon domain and passes it to the collective. The
    /// root hands out batches to itself and to remote ranks; each rank runs
    /// its batch through its own pool with the requested `LocalMode`.
    fn run_hybrid<C: Communicator>(world: &C, workers: usize, prefetch: bool) -> Option<Vec<u64>> {
        let rank = world.rank();
        let size = world.size();
        let cpu_set: Vec<usize> = (0..workers).collect();
        let domain = Domain::managed(cpu_set, workers).expect("rayon domain");

        let items = (rank == 0).then(|| (0..64_u64).collect::<Vec<_>>());
        // Batches should hold at least `workers` items so that `Outer` can
        // spread one batch over the whole pool.
        let batch_size = NonZeroUsize::new((64 / (size as usize * 4)).max(workers)).unwrap();
        let options = PmapOptions {
            root: 0,
            batch_size,
            local_mode: LocalMode::Outer,
            // When true, each remote rank may hold one extra batch so that the
            // next transfer overlaps the current computation.
            prefetch,
        };

        pmap(world, &domain, options, items, heavy).expect("hybrid pmap must succeed")
    }
    // snippet-end:hybrid-call

    pub fn run() {
        let universe = initialize_funneled();
        let world = universe.world();
        let rank = world.rank();
        let workers = 2;

        let expected: Vec<u64> = (0..64_u64).map(|item| heavy(item).unwrap()).collect();
        for prefetch in [false, true] {
            let results = run_hybrid(&world, workers, prefetch);
            if rank == 0 {
                assert_eq!(results.expect("root receives results"), expected);
            } else {
                assert!(results.is_none());
            }
        }

        if rank == 0 {
            println!(
                "hybrid_pmap: {} ranks x {} workers, backend {}, prefetch on/off verified",
                world.size(),
                workers,
                describe_backend()
            );
        }
    }
}
