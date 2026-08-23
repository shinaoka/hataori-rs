use hataori::{pmap, Domain, PmapOptions};
use mandelbrot_common::{tensor_from_columns, Param};
use mpi_upstream as mpi_api;
use mpi_upstream::traits::Communicator;
use std::num::NonZeroUsize;
use std::time::Instant;

#[path = "support/mandelbrot_common.rs"]
mod mandelbrot_common;

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
    // Use a batch size that creates several batches per rank to amortize wire overhead.
    let batch_size = NonZeroUsize::new((width / ((size as usize).max(1) * 8)).max(1)).unwrap();
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
