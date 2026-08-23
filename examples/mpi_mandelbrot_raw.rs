use mandelbrot_common::{tensor_from_columns, Param};
use mpi_upstream as mpi_api;
use mpi_upstream::traits::*;
use std::time::Instant;
use tenferro_tensor::Tensor;

#[path = "support/mandelbrot_common.rs"]
mod mandelbrot_common;

const TAG_NUM_COLS: mpi_api::Tag = 0;
const TAG_INDICES: mpi_api::Tag = 1;
const TAG_COLUMN: mpi_api::Tag = 2;

fn compute_mandelbrot_raw<C: Communicator>(
    world: &C,
    rank: i32,
    size: i32,
    param: &Param,
) -> (Option<Tensor>, f64, f64) {
    let (x, y) = param.make_axes();
    let width = param.width;
    let height = param.height;

    // Each rank computes its assigned columns using round-robin distribution.
    let mut local_results: Vec<Vec<i64>> = Vec::new();
    let mut local_col_indices: Vec<usize> = Vec::new();

    let compute_start = Instant::now();
    let mut col_idx = rank as usize;
    while col_idx < width {
        local_results.push(mandelbrot_common::compute_column(param, x[col_idx], &y));
        local_col_indices.push(col_idx);
        col_idx += size as usize;
    }
    let compute_time = compute_start.elapsed().as_secs_f64();

    // Gather all results to rank 0.
    let transfer_start = Instant::now();
    if rank == 0 {
        let mut full_data = vec![0_i64; width * height];

        // Place rank 0's own results.
        for (i, &col_idx) in local_col_indices.iter().enumerate() {
            full_data[col_idx * height..(col_idx + 1) * height].copy_from_slice(&local_results[i]);
        }

        // Receive results from other ranks.
        for src_rank in 1..size {
            let src = world.process_at_rank(src_rank);

            let mut num_cols_buf = [0_i64];
            src.receive_into_with_tag(&mut num_cols_buf, TAG_NUM_COLS);
            let num_cols = num_cols_buf[0] as usize;

            let mut col_indices = vec![0_i64; num_cols];
            src.receive_into_with_tag(&mut col_indices, TAG_INDICES);

            for &col_idx_i64 in &col_indices {
                let col_idx = col_idx_i64 as usize;
                let mut col_data = vec![0_i64; height];
                src.receive_into_with_tag(&mut col_data, TAG_COLUMN);
                full_data[col_idx * height..(col_idx + 1) * height].copy_from_slice(&col_data);
            }
        }

        let transfer_time = transfer_start.elapsed().as_secs_f64();
        (
            Some(tensor_from_columns(width, height, full_data)),
            compute_time,
            transfer_time,
        )
    } else {
        let dst = world.process_at_rank(0);
        let num_cols = local_col_indices.len() as i64;
        dst.send_with_tag(&[num_cols], TAG_NUM_COLS);

        let indices_i64: Vec<i64> = local_col_indices.iter().map(|&i| i as i64).collect();
        dst.send_with_tag(&indices_i64, TAG_INDICES);

        for col_data in &local_results {
            dst.send_with_tag(col_data, TAG_COLUMN);
        }

        let transfer_time = transfer_start.elapsed().as_secs_f64();
        (None, compute_time, transfer_time)
    }
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
    let (result, compute_time, transfer_time) = compute_mandelbrot_raw(&world, rank, size, &param);
    let total_time = total_start.elapsed().as_secs_f64();

    if rank == 0 {
        let result = result.expect("rank 0 must own the full result");
        let slice = result.as_slice::<i64>().expect("result must be i64");
        let max_val = slice.iter().max().copied().unwrap_or(0);
        assert!(max_val > 0, "result must contain non-zero iteration counts");

        mandelbrot_common::print_info("Performance metrics", "");
        mandelbrot_common::print_info("  Total time", format!("{total_time} seconds"));
        mandelbrot_common::print_info("  Computation time", format!("{compute_time} seconds"));
        mandelbrot_common::print_info("  Data transfer time", format!("{transfer_time} seconds"));
        mandelbrot_common::print_info(
            "  Transfer ratio",
            format!("{}%", (transfer_time / total_time * 100.0).round()),
        );
        mandelbrot_common::print_info("  Max iteration count", max_val);
        mandelbrot_common::print_info("Done!", "");
    } else {
        mandelbrot_common::print_info(
            &format!("Rank {rank} - Computation time"),
            format!("{compute_time} seconds"),
        );
        mandelbrot_common::print_info(
            &format!("Rank {rank} - Transfer time"),
            format!("{transfer_time} seconds"),
        );
    }
}
