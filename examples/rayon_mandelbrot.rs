//! Mandelbrot set, Rayon-only dynamic scheduling (no MPI).
//!
//! This is the Rayon-only counterpart of `mpi_mandelbrot_pmap.rs`: the same
//! 4096-column Mandelbrot workload, dynamically scheduled by Rayon's
//! work-stealing `par_iter` inside a single process via [`hataori::map_in`]
//! with [`LocalMode::Outer`]. There is no MPI and no rank coordination here;
//! the pool owns `--threads` worker threads and the call preserves input order.
//!
//! Run with:
//!
//! ```sh
//! cargo run --release --no-default-features --features rayon,tenferro \
//!     --example rayon_mandelbrot -- --threads 10
//! ```
use hataori::{map_in, Domain, LocalMode};
use mandelbrot_common::{tensor_from_columns, Param};
use std::env;
use std::time::Instant;

#[path = "support/mandelbrot_common.rs"]
mod mandelbrot_common;

/// Parse an optional `--threads N` command-line argument.
///
/// Defaults to all logical cores reported by the OS.
fn threads_from_args() -> usize {
    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--threads" {
            if let Some(value) = args.next() {
                return value.parse().unwrap_or_else(|_| {
                    eprintln!("Invalid --threads value: {value}");
                    std::process::exit(1);
                });
            }
        }
    }
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

fn main() {
    let threads = threads_from_args();
    let param = Param::default();
    let (x, y) = param.make_axes();
    let width = param.width;
    let height = param.height;

    // macOS does not enforce affinity, but the cpu_set must be non-empty and
    // at least as large as the worker count.
    let cpu_set: Vec<usize> = (0..threads).collect();
    let domain = Domain::managed(cpu_set, threads).expect("rayon domain");

    mandelbrot_common::print_info("Start processing Mandelbrot set...", "");
    mandelbrot_common::print_info("Backend", "Rayon only (no MPI)");
    mandelbrot_common::print_info("Threads", threads);
    mandelbrot_common::print_info("Parameters", format!("{:?}", param));
    mandelbrot_common::print_info("Computing...", "");

    let items: Vec<usize> = (0..width).collect();
    let total_start = Instant::now();
    let columns = map_in(&domain, LocalMode::Outer, items, |col_idx| {
        Ok::<_, String>(mandelbrot_common::compute_column(&param, x[col_idx], &y))
    })
    .expect("map_in must succeed");
    let elapsed = total_start.elapsed();
    mandelbrot_common::print_info("Total time", format!("{:.3} s", elapsed.as_secs_f64()));

    let full_data: Vec<i64> = columns.concat();
    let tensor = tensor_from_columns(width, height, full_data);
    let png_path = "mandelbrot_rayon.png";
    mandelbrot_common::save_png(&tensor, png_path);
    mandelbrot_common::print_info("Saved", png_path);
}
