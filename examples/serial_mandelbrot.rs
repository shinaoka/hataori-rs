//! Mandelbrot set on one thread with [`hataori::map`].
//!
//! This is the semantic baseline for every other `*_mandelbrot*` example:
//! the same columns, the same kernel, the same ordered result — but computed
//! one column at a time on the calling thread, with no pool, no MPI, and no
//! `Send`/`Sync`/serde requirements on the callback.
//!
//! Run with:
//!
//! ```sh
//! cargo run --release --no-default-features --features tenferro \
//!     --example serial_mandelbrot -- --width 1024 --height 1024
//! ```
use hataori::{map, MapError};
use mandelbrot_common::{print_info, Param};
use std::time::Instant;

#[path = "support/mandelbrot_common.rs"]
mod mandelbrot_common;

// snippet-start:serial-map
/// Render every column in input order on the calling thread.
///
/// `map` takes a `Vec<T>` and a fallible callback and returns a `Vec<U>` in
/// the same order, or the first error together with the failing index. The
/// callback borrows `param`, `x`, and `y` from the caller's stack.
fn render(param: &Param) -> Result<Vec<Vec<i64>>, MapError> {
    let (x, y) = param.make_axes();
    let columns: Vec<usize> = (0..param.width).collect();
    map(columns, |col_idx| {
        Ok::<_, String>(mandelbrot_common::compute_column(param, x[col_idx], &y))
    })
}
// snippet-end:serial-map

// snippet-start:serial-errors
/// `map` stops at the first error and reports its zero-based input index.
///
/// The callback error type only needs `Display`; `MapError` keeps the
/// index and the `Display` output (truncated to 4096 bytes).
fn reject_columns_outside(param: &Param, limit: usize) -> MapError {
    let (x, y) = param.make_axes();
    let mut evaluated = 0_usize;
    let error = map((0..param.width).collect::<Vec<usize>>(), |col_idx| {
        evaluated += 1;
        if col_idx >= limit {
            return Err(format!("column {col_idx} is outside the requested range"));
        }
        Ok(mandelbrot_common::compute_column(param, x[col_idx], &y))
    })
    .expect_err("column `limit` must fail");
    assert_eq!(error.index(), limit);
    // Columns after the failing one were never evaluated.
    assert_eq!(evaluated, limit + 1);
    error
}
// snippet-end:serial-errors

fn main() {
    let param = Param::from_args();

    print_info("Start processing Mandelbrot set...", "");
    print_info("Backend", "serial (one thread, no MPI)");
    print_info("Parameters", format!("{:?}", param));
    print_info("Computing...", "");

    let total_start = Instant::now();
    let columns = render(&param).expect("map must succeed");
    let elapsed = total_start.elapsed();
    print_info("Total time", format!("{:.3} s", elapsed.as_secs_f64()));

    let tensor = mandelbrot_common::tensor_from_ordered_columns(param.width, param.height, columns);
    print_info(
        "Max iteration count",
        mandelbrot_common::max_iteration(&tensor),
    );

    let error = reject_columns_outside(&param, param.width / 2);
    print_info(
        "First error",
        format!("index {}: {}", error.index(), error.message()),
    );

    let png_path = mandelbrot_common::output_path("mandelbrot_serial.png");
    mandelbrot_common::save_png(&tensor, &png_path);
    print_info("Saved", png_path);
}
