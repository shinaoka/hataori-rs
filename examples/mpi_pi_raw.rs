use mpi_upstream as mpi_api;
use mpi_upstream::traits::*;

#[path = "support/pi_common.rs"]
mod pi_common;

fn compute_pi_raw<C: Communicator>(world: &C, rank: i32, size: i32, n: i64) -> f64 {
    let local = pi_common::count_coprime_pairs(rank as i64 + 1, size as i64, n);
    let mut total = 0_i64;
    world.all_reduce_into(
        &local,
        &mut total,
        mpi_api::collective::SystemOperation::sum(),
    );
    pi_common::estimate_pi(total, n)
}

fn main() {
    let universe =
        mpi_api::initialize().expect("MPI must not already be initialized or finalized");
    let world = universe.world();
    let rank = world.rank();
    let size = world.size();

    if rank == 0 {
        pi_common::print_info("Start processing...", "");
        pi_common::print_info("MPI size", size);
    }

    // Warm up
    let n_warm = 100_i64;
    if rank == 0 {
        pi_common::print_info("Warm up", "");
        pi_common::print_info("N", n_warm);
    }
    let _ = pi_common::timed(rank, "warmup", || compute_pi_raw(&world, rank, size, n_warm));

    // Benchmarks
    for n in [100_i64, 1000, 10_000, 50_000, 100_000] {
        if rank == 0 {
            pi_common::print_info("N", n);
        }
        let _ = pi_common::timed(rank, "benchmark", || compute_pi_raw(&world, rank, size, n));
    }
}
