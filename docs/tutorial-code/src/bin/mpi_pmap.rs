//! MPI-only distributed execution: `hataori::pmap` with a sequential domain.
//!
//! Build and run with the link-time backend:
//!
//! ```sh
//! cargo build -p hataori-tutorial-code --features mpi --bin mpi_pmap
//! mpiexec -n 4 target/debug/mpi_pmap
//! ```
//!
//! or with the runtime-loaded backend:
//!
//! ```sh
//! cargo build -p hataori-tutorial-code --features rsmpi-rt --bin mpi_pmap
//! MPI_RT_LIB=/abs/path/to/libmpiwrapper.so mpiexec -n 4 target/debug/mpi_pmap
//! ```

//! This binary demonstrates the MPI-only entry point, which exists only when
//! the `rayon` feature is disabled. Enabling `rayon` switches `hataori::pmap`
//! to the hybrid entry point, which requires a domain with a Rayon pool; see
//! `hybrid_pmap.rs` for that mode.

#[cfg(all(any(feature = "mpi", feature = "rsmpi-rt"), not(feature = "rayon")))]
#[path = "../support/mpi_backend.rs"]
mod mpi_backend;

#[cfg(not(any(feature = "mpi", feature = "rsmpi-rt")))]
fn main() {
    println!("HATAORI_TUTORIAL_SKIP: mpi_pmap needs --features mpi or --features rsmpi-rt");
}

#[cfg(all(any(feature = "mpi", feature = "rsmpi-rt"), feature = "rayon"))]
fn main() {
    println!(
        "HATAORI_TUTORIAL_SKIP: mpi_pmap shows the MPI-only pmap; \
         with `rayon` enabled pmap is the hybrid entry point (see hybrid_pmap)"
    );
}

#[cfg(all(any(feature = "mpi", feature = "rsmpi-rt"), not(feature = "rayon")))]
fn main() {
    tutorial::run();
}

#[cfg(all(any(feature = "mpi", feature = "rsmpi-rt"), not(feature = "rayon")))]
mod tutorial {
    use super::mpi_backend::{describe_backend, mpi_api};
    use hataori::{pmap, Domain, LocalMode, PmapErrorKind, PmapOptions};
    use mpi_api::traits::*;
    use serde::{Deserialize, Serialize};
    use std::num::NonZeroUsize;

    // snippet-start:mpi-pmap-types
    /// Task inputs and outputs cross rank boundaries, so they must be serde
    /// serializable. Plain numbers work too; a struct keeps the example clear.
    #[derive(Debug, Serialize, Deserialize)]
    struct Task {
        id: u32,
        cost: u64,
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Outcome {
        id: u32,
        value: u64,
    }

    fn evaluate(task: Task) -> Result<Outcome, String> {
        // Simulate uneven work so that dynamic scheduling matters.
        std::thread::sleep(std::time::Duration::from_millis(task.cost));
        Ok(Outcome {
            id: task.id,
            value: task.cost * 2,
        })
    }
    // snippet-end:mpi-pmap-types

    // snippet-start:mpi-pmap-call
    /// One collective `pmap` call. Every rank enters this function; only the
    /// root supplies the items and only the root receives the results.
    fn run_pmap<C: Communicator>(world: &C, root: i32) -> Option<Vec<Outcome>> {
        let rank = world.rank();
        let items = (rank == root).then(|| {
            (0..24_u32)
                .map(|id| Task {
                    id,
                    cost: u64::from((id * 7) % 5),
                })
                .collect::<Vec<_>>()
        });

        // The MPI-only entry point always executes callbacks sequentially in
        // a one-worker domain; `batch_size` controls how many items travel in
        // one message.
        let options = PmapOptions {
            root,
            batch_size: NonZeroUsize::new(4).unwrap(),
            local_mode: LocalMode::Sequential,
            prefetch: false,
        };

        pmap(world, &Domain::sequential(), options, items, evaluate).expect("pmap must succeed")
    }
    // snippet-end:mpi-pmap-call

    // snippet-start:mpi-pmap-errors
    /// A callback error on any rank converges to one `PmapError` on every
    /// rank, so all ranks can agree on the failure and keep using MPI.
    fn run_failing_pmap<C: Communicator>(world: &C) {
        let rank = world.rank();
        let items = (rank == 0).then(|| (0..8_u32).collect::<Vec<_>>());
        let error = pmap(
            world,
            &Domain::sequential(),
            PmapOptions::default(),
            items,
            |item| {
                if item == 5 {
                    Err(format!("item {item} is not allowed"))
                } else {
                    Ok(item + 1)
                }
            },
        )
        .expect_err("item 5 fails on whichever rank evaluates it");
        assert_eq!(error.kind(), PmapErrorKind::User);
        assert!(error.message().contains("item 5 is not allowed"));
    }
    // snippet-end:mpi-pmap-errors

    pub fn run() {
        let universe = mpi_api::initialize().expect("MPI must not already be initialized");
        let world = universe.world();
        let rank = world.rank();
        let size = world.size();

        let results = run_pmap(&world, 0);
        match results {
            Some(outcomes) => {
                assert_eq!(rank, 0);
                assert_eq!(outcomes.len(), 24);
                for (index, outcome) in outcomes.iter().enumerate() {
                    assert_eq!(outcome.id as usize, index);
                    assert_eq!(outcome.value, u64::from((outcome.id * 7) % 5) * 2);
                }
                println!(
                    "mpi_pmap: {} ranks, backend {}, {} ordered results",
                    size,
                    describe_backend(),
                    outcomes.len()
                );
            }
            None => assert_ne!(rank, 0),
        }

        // Any rank can be the root; the last rank also works.
        let last = size - 1;
        let from_last = run_pmap(&world, last);
        assert_eq!(from_last.is_some(), rank == last);

        run_failing_pmap(&world);
    }
}
