//! Mandelbrot set with static distribution through Hataori's placement
//! collectives: `broadcast` the parameters, `scatter` one contiguous block of
//! column indices to every rank, compute locally, and `gather` the blocks on
//! the root.
//!
//! This is the collective counterpart of `mpi_mandelbrot_raw.rs`: the same
//! static split, but every transfer is one typed collective over serde
//! values instead of hand-written tags, counts, and receive loops. Unlike
//! `pmap`, nothing rebalances the uneven column costs, so it is also a
//! baseline for what dynamic scheduling buys.
//!
//! Build and run:
//!
//! ```sh
//! cargo build --release --no-default-features --features mpi,tenferro \
//!     --example mpi_mandelbrot_placement
//! mpiexec -n 4 target/release/examples/mpi_mandelbrot_placement
//! ```
use hataori::{broadcast, gather, scatter};
use mandelbrot_common::mpi_api::traits::Communicator;
use mandelbrot_common::{print_info, Param};
use serde::{Deserialize, Serialize};
use std::time::Instant;
use tenferro_tensor::Tensor;

#[path = "support/mandelbrot_common.rs"]
mod mandelbrot_common;

// snippet-start:placement-types
/// Values that cross rank boundaries need serde. `Param` itself lives in the
/// shared example module and derives nothing, so wrap the fields the ranks
/// need.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
struct Region {
    xmin: f64,
    xmax: f64,
    width: usize,
    ymin: f64,
    ymax: f64,
    height: usize,
    max_iter: i64,
}

impl From<Param> for Region {
    fn from(param: Param) -> Self {
        Self {
            xmin: param.xmin,
            xmax: param.xmax,
            width: param.width,
            ymin: param.ymin,
            ymax: param.ymax,
            height: param.height,
            max_iter: param.max_iter,
        }
    }
}

impl From<Region> for Param {
    fn from(region: Region) -> Self {
        Self {
            xmin: region.xmin,
            xmax: region.xmax,
            width: region.width,
            ymin: region.ymin,
            ymax: region.ymax,
            height: region.height,
            max_iter: region.max_iter,
        }
    }
}

/// One rank's share of the image: a contiguous block of column indices.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Block {
    first_column: usize,
    columns: Vec<Vec<i64>>,
}
// snippet-end:placement-types

// snippet-start:placement-call
/// Static distribution with the three collectives. Every rank calls each
/// collective in the same order with the same root.
fn render<C: Communicator>(world: &C, root_param: Option<Param>) -> Option<Tensor> {
    let rank = world.rank();
    let size = world.size() as usize;
    let root = 0;

    // broadcast: the root supplies `Some`, every rank receives an owned copy.
    let region: Region =
        broadcast(world, root, root_param.map(Region::from)).expect("broadcast must succeed");
    let param = Param::from(region);
    let (x, y) = param.make_axes();

    // scatter: the root supplies exactly one shard per rank; each rank
    // receives the shard at its own index. Here a shard is a contiguous
    // range of column indices, so neighbouring ranks get neighbouring
    // columns — and, near the set, very different amounts of work.
    let shards = (rank == root).then(|| {
        (0..size)
            .map(|target| {
                let first = target * param.width / size;
                let last = (target + 1) * param.width / size;
                (first..last).collect::<Vec<usize>>()
            })
            .collect::<Vec<_>>()
    });
    let my_columns: Vec<usize> = scatter(world, root, shards).expect("scatter must succeed");

    // Compute locally; no Hataori call is involved in this step.
    let block = Block {
        first_column: my_columns.first().copied().unwrap_or(0),
        columns: my_columns
            .iter()
            .map(|&col_idx| mandelbrot_common::compute_column(&param, x[col_idx], &y))
            .collect(),
    };

    // gather: every rank supplies one value; the root receives them in rank
    // order and every other rank receives `None`.
    let blocks = gather(world, root, block).expect("gather must succeed");
    blocks.map(|blocks| {
        let mut full_data = vec![0_i64; param.width * param.height];
        for block in blocks {
            for (offset, column) in block.columns.iter().enumerate() {
                let col_idx = block.first_column + offset;
                mandelbrot_common::place_column(&mut full_data, param.height, col_idx, column);
            }
        }
        mandelbrot_common::tensor_from_columns(param.width, param.height, full_data)
    })
}
// snippet-end:placement-call

/// `pub` so that the `rsmpi_rt_*` wrapper example can reuse this file.
pub fn main() {
    let universe = mandelbrot_common::mpi_api::initialize()
        .expect("MPI must not already be initialized or finalized");
    let world = universe.world();
    let rank = world.rank();
    let size = world.size();

    // Only the root parses the parameters; `broadcast` delivers them.
    let root_param = (rank == 0).then(Param::from_args);
    if let Some(param) = root_param {
        print_info("Start processing Mandelbrot set...", "");
        print_info("Backend", mandelbrot_common::describe_backend());
        print_info("MPI size", size);
        print_info("Parameters", format!("{:?}", param));
        print_info("Computing...", "");
    }

    let total_start = Instant::now();
    let result = render(&world, root_param);
    let total_time = total_start.elapsed().as_secs_f64();

    if rank == 0 {
        let result = result.expect("rank 0 must receive the full result");
        print_info("Performance metrics", "");
        print_info("  Total time", format!("{total_time} seconds"));
        print_info(
            "  Max iteration count",
            mandelbrot_common::max_iteration(&result),
        );

        let png_path = mandelbrot_common::output_path("mandelbrot_placement.png");
        print_info("Saving PNG", &png_path);
        mandelbrot_common::save_png(&result, &png_path);
        print_info("Done!", "");
    } else {
        assert!(result.is_none(), "only the root receives the result");
    }
}
