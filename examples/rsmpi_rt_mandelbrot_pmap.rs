//! `mpi_mandelbrot_pmap` built against the runtime-loaded `rsmpi-rt` backend.
//!
//! The source is shared; only the crate alias selected in
//! `support/mandelbrot_common.rs` differs. Build and run with:
//!
//! ```sh
//! cargo build --release --no-default-features --features rsmpi-rt,tenferro \
//!     --example rsmpi_rt_mandelbrot_pmap
//! MPI_RT_LIB=/abs/path/libmpiwrapper.so \
//!     mpiexec -n 4 target/release/examples/rsmpi_rt_mandelbrot_pmap
//! ```
#[path = "mpi_mandelbrot_pmap.rs"]
mod shared;

fn main() {
    shared::main();
}
