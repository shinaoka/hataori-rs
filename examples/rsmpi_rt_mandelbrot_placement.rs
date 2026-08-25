//! `mpi_mandelbrot_placement` built against the runtime-loaded `rsmpi-rt`
//! backend; see `rsmpi_rt_mandelbrot_pmap.rs`.
#[path = "mpi_mandelbrot_placement.rs"]
mod shared;

fn main() {
    shared::main();
}
