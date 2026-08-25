//! `mpi_mandelbrot_hybrid` built against the runtime-loaded `rsmpi-rt`
//! backend; see `rsmpi_rt_mandelbrot_pmap.rs`. Requires the `rayon` feature
//! as well: `--features rsmpi-rt,rayon,tenferro`.
#[path = "mpi_mandelbrot_hybrid.rs"]
mod shared;

fn main() {
    shared::main();
}
