use hataori::{pmap, Domain, PmapOptions};
use mpi_upstream as mpi_api;
use mpi_upstream::traits::*;

#[path = "support/pi_common.rs"]
mod pi_common;

fn run_one<C: Communicator>(world: &C, n: i64) -> i64 {
    let rank = world.rank();
    let input = (rank == 0).then(|| (1..=n).map(|a| a as u64).collect::<Vec<u64>>());
    let result = pmap(
        world,
        &Domain::sequential(),
        PmapOptions::default(),
        input,
        |a| Ok::<_, String>(pi_common::count_for_a(a, n)),
    )
    .expect("pmap must succeed");

    result.map_or(0_i64, |values| values.iter().sum::<i64>())
}

fn main() {
    let universe = mpi_api::initialize().expect("MPI must not already be initialized or finalized");
    let world = universe.world();
    let rank = world.rank();

    if rank == 0 {
        pi_common::print_info("Start processing...", "");
        pi_common::print_info("MPI size", world.size());
    }

    // Warm up
    let n_warm = 100_i64;
    if rank == 0 {
        pi_common::print_info("Warm up", "");
        pi_common::print_info("N", n_warm);
    }
    let _ = pi_common::timed(rank, "warmup", || {
        let total = run_one(&world, n_warm);
        pi_common::estimate_pi(total, n_warm)
    });

    // Benchmarks
    for n in [100_i64, 1000, 10_000, 50_000, 100_000] {
        if rank == 0 {
            pi_common::print_info("N", n);
        }
        let _ = pi_common::timed(rank, "benchmark", || {
            let total = run_one(&world, n);
            pi_common::estimate_pi(total, n)
        });
    }
}
