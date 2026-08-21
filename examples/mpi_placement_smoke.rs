use mpi_upstream as mpi_api;
#[path = "support/placement_smoke.rs"]
mod placement_smoke;

fn main() {
    let universe = mpi_upstream::initialize().expect("MPI must not already be initialized");
    placement_smoke::run(&universe.world());
}
