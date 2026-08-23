use hataori::{pmap, Domain, PmapOptions};
use mandelbrot_common::{tensor_from_columns, Param};
use mpi_upstream as mpi_api;
use mpi_upstream::traits::Communicator;
use std::env;
use std::num::NonZeroUsize;
use std::time::Instant;

#[path = "support/mandelbrot_common.rs"]
mod mandelbrot_common;

/// Parse an optional `--batch-factor N` command-line argument.
///
/// The batch size used by pmap is `width / (world_size * factor)`, clamped to
/// at least [`MIN_BATCH_COLUMNS`] columns. A larger factor produces smaller
/// batches and therefore finer load balancing, but increases
/// scheduling/communication overhead. The default of 32 measured fastest on a
/// 4096-column image across 2-8 ranks; larger factors start to pay per-batch
/// protocol overhead, smaller factors coarsen the scheduling granularity.
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

/// Smallest allowed batch, in columns.
///
/// A column is `height` i64 values (32 KiB at 4096 rows), so a 16-column
/// batch is a 512 KiB message: large enough to amortize the per-batch
/// handshake and serialization. Without this floor the formula collapses to a
/// single column per batch once `world_size > width / factor`, turning every
/// column into a full request/response round trip and breaking scaling with
/// `-n`.
const MIN_BATCH_COLUMNS: usize = 16;

fn run_one<C: mpi_api::traits::Communicator>(
    world: &C,
    param: &Param,
    size: i32,
) -> Option<tenferro_tensor::Tensor> {
    let rank = world.rank();
    let (x, y) = param.make_axes();
    let width = param.width;
    let height = param.height;

    // Rank 0 owns the list of column indices; pmap distributes them across all ranks.
    // Target `--batch-factor` (default 32) batches per rank so the dynamic
    // scheduler can rebalance uneven column costs, while MIN_BATCH_COLUMNS
    // bounds the total number of batches at large job counts.
    let factor = batch_factor_from_args();
    let batch_size = NonZeroUsize::new(
        (width / ((size as usize).max(1) * factor.max(1)))
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
        &Domain::sequential(),
        PmapOptions {
            batch_size,
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
    let universe = mpi_api::initialize().expect("MPI must not already be initialized or finalized");
    let world = universe.world();
    let rank = world.rank();
    let size = world.size();

    if rank == 0 {
        mandelbrot_common::print_info("Start processing Mandelbrot set...", "");
        mandelbrot_common::print_info("MPI size", size);
    }

    let param = Param::default();
    if rank == 0 {
        mandelbrot_common::print_info("Parameters", format!("{:?}", param));
        mandelbrot_common::print_info("Computing...", "");
    }

    let total_start = Instant::now();
    let result = run_one(&world, &param, size);
    let total_time = total_start.elapsed().as_secs_f64();

    if rank == 0 {
        let result = result.expect("rank 0 must receive the full result");
        let slice = result.as_slice::<i64>().expect("result must be i64");
        let max_val = slice.iter().max().copied().unwrap_or(0);
        assert!(max_val > 0, "result must contain non-zero iteration counts");

        mandelbrot_common::print_info("Performance metrics", "");
        mandelbrot_common::print_info("  Total time", format!("{total_time} seconds"));
        mandelbrot_common::print_info("  Transfer ratio", "N/A (included in total)");
        mandelbrot_common::print_info("  Max iteration count", max_val);

        let png_path = "mandelbrot_pmap.png";
        mandelbrot_common::print_info("Saving PNG", png_path);
        mandelbrot_common::save_png(&result, png_path);

        mandelbrot_common::print_info("Done!", "");
    }
}
