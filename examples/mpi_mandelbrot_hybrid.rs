//! Mandelbrot set, symmetric hybrid execution: one MPI rank per node, with a
//! rank-local Rayon pool for the compute.
//!
//! This is the hybrid counterpart of `mpi_mandelbrot_pmap.rs` (sequential
//! pmap) and `rayon_mandelbrot.rs` (single-process Rayon). Each MPI rank owns
//! a Rayon pool of `--workers` threads; pmap hands batches to the root and to
//! every remote rank, and each rank computes its assigned batches inside its
//! pool while the rank's main thread keeps serving MPI messages. In a
//! homogeneous cluster this runs `--workers` compute threads per node.
//!
//! Requires `MPI_THREAD >= Funneled` (Open MPI provides it by default).
//!
//! Build and run:
//!
//! ```sh
//! cargo build --release --no-default-features --features mpi,rayon,tenferro \
//!     --example mpi_mandelbrot_hybrid
//! mpiexec -n 4 target/release/examples/mpi_mandelbrot_hybrid --workers 2
//! ```
use hataori::{pmap, Domain, LocalMode, PmapOptions};
use mandelbrot_common::{tensor_from_columns, Param};
use mpi_upstream as mpi_api;
use mpi_upstream::environment::Threading;
use mpi_upstream::traits::Communicator;
use std::env;
use std::num::NonZeroUsize;
use std::time::Instant;

#[path = "support/mandelbrot_common.rs"]
mod mandelbrot_common;

/// Smallest allowed batch, in columns.
///
/// A column is `height` i64 values (32 KiB at 4096 rows), so a 16-column
/// batch is a 512 KiB message. The same floor as `mpi_mandelbrot_pmap.rs`.
const MIN_BATCH_COLUMNS: usize = 16;

/// Parse an optional `--batch-factor N` command-line argument.
///
/// The batch size is `width / (size * workers * factor)`, clamped to at least
/// [`MIN_BATCH_COLUMNS`] columns. The factor is relative to the total number
/// of compute threads (`size * workers`) because in hybrid mode every batch is
/// internally subdivided by Rayon.
fn batch_factor_from_args() -> usize {
    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--batch-factor" {
            if let Some(value) = args.next() {
                return value.parse().unwrap_or_else(|_| {
                    eprintln!("Invalid --batch-factor value: {value}");
                    std::process::exit(1);
                });
            }
        }
    }
    32
}

/// Parse an optional `--workers N` command-line argument.
///
/// The number of Rayon compute threads per rank. Defaults to all logical
/// cores reported by the OS (the node's core count in a node-per-rank
/// deployment). For local benchmarks pass an explicit count so that
/// `size * workers` stays within the machine's cores.
fn workers_from_args() -> usize {
    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--workers" {
            if let Some(value) = args.next() {
                return value.parse().unwrap_or_else(|_| {
                    eprintln!("Invalid --workers value: {value}");
                    std::process::exit(1);
                });
            }
        }
    }
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

fn run_one<C: Communicator>(
    world: &C,
    param: &Param,
    size: i32,
    workers: usize,
) -> Option<tenferro_tensor::Tensor> {
    let rank = world.rank();
    let (x, y) = param.make_axes();
    let width = param.width;
    let height = param.height;

    // macOS does not enforce affinity, but the cpu_set must be non-empty and
    // at least as large as the worker count.
    let cpu_set: Vec<usize> = (0..workers).collect();
    let domain = Domain::managed(cpu_set, workers).expect("rayon domain");

    let factor = batch_factor_from_args();
    let batch_size = NonZeroUsize::new(
        (width / ((size as usize).max(1) * workers.max(1) * factor.max(1)))
            .max(MIN_BATCH_COLUMNS)
            .min(width),
    )
    .unwrap();
    if rank == 0 {
        mandelbrot_common::print_info("Batch factor", factor);
        mandelbrot_common::print_info("Batch size", batch_size.get());
    }
    let input = (rank == 0).then(|| (0..width).collect::<Vec<usize>>());
    let columns = pmap(
        world,
        &domain,
        PmapOptions {
            batch_size,
            local_mode: LocalMode::Outer,
            ..PmapOptions::default()
        },
        input,
        |col_idx| Ok::<_, String>(mandelbrot_common::compute_column(param, x[col_idx], &y)),
    )
    .expect("pmap must succeed");

    columns.map(|col_data| {
        let mut full_data = vec![0_i64; width * height];
        for (col_idx, col) in (0..width).zip(col_data) {
            full_data[col_idx * height..(col_idx + 1) * height].copy_from_slice(&col);
        }
        tensor_from_columns(width, height, full_data)
    })
}

fn main() {
    let (universe, provided) = mpi_api::initialize_with_threading(Threading::Funneled)
        .expect("MPI must not already be initialized or finalized");
    assert!(
        provided >= Threading::Funneled,
        "hybrid pmap requires MPI_THREAD >= Funneled"
    );
    let world = universe.world();
    let rank = world.rank();
    let size = world.size();
    let workers = workers_from_args();

    if rank == 0 {
        mandelbrot_common::print_info("Start processing Mandelbrot set...", "");
        mandelbrot_common::print_info("MPI size", size);
        mandelbrot_common::print_info("Workers per rank", workers);
    }

    let param = Param::default();
    if rank == 0 {
        mandelbrot_common::print_info("Parameters", format!("{:?}", param));
        mandelbrot_common::print_info("Computing...", "");
    }

    let total_start = Instant::now();
    let result = run_one(&world, &param, size, workers);
    let elapsed = total_start.elapsed();

    if rank == 0 {
        mandelbrot_common::print_info("Total time", format!("{:.3} s", elapsed.as_secs_f64()));
    }

    if let Some(tensor) = result {
        let png_path = "mandelbrot_hybrid.png";
        mandelbrot_common::save_png(&tensor, png_path);
        mandelbrot_common::print_info("Saved", png_path);
    }
}
