use mpi_runtime as mpi_api;
#[path = "support/placement_smoke.rs"]
mod placement_smoke;

fn main() {
    let library =
        std::env::var("MPI_RT_LIB").expect("MPI_RT_LIB must be set before MPI initialization");
    assert!(std::path::Path::new(&library).is_absolute());
    let universe = mpi_runtime::initialize().expect("MPI must not already be initialized");
    placement_smoke::run(&universe.world());
}
