//! Shared pieces of the Mandelbrot examples.
//!
//! Every `*_mandelbrot*` example renders the same image: `width` columns of
//! `height` escape-iteration counts, computed one column at a time by
//! [`compute_column`]. The examples differ only in how the columns are
//! distributed — serial `map`, Rayon `map_in`, MPI `pmap`, the placement
//! collectives, or hybrid `pmap` — so this module holds everything that does
//! not depend on the execution model: the parameters, the kernel, the
//! command-line parsing, the tensor assembly, and the PNG output.
#![allow(dead_code)]

use std::env;
use std::time::Instant;
use tenferro_tensor::Tensor;

// snippet-start:backend
// The MPI crate alias shared by the MPI examples.
//
// The `mpi` feature links the upstream rsmpi crate at build time; the
// `rsmpi-rt` feature loads an MPIABI-compatible runtime at process start.
// Both expose the same rsmpi API, so the examples only need one alias and
// the same source builds against either backend.
#[cfg(all(feature = "rsmpi-rt", not(feature = "mpi")))]
#[allow(unused_imports)]
pub use mpi_runtime as mpi_api;
#[cfg(all(feature = "mpi", not(feature = "rsmpi-rt")))]
#[allow(unused_imports)]
pub use mpi_upstream as mpi_api;

/// Names the MPI backend the example was compiled with.
pub fn describe_backend() -> &'static str {
    if cfg!(feature = "rsmpi-rt") {
        "rsmpi-rt (runtime-loaded MPI)"
    } else {
        "mpi (link-time MPI)"
    }
}
// snippet-end:backend

// snippet-start:param
/// Parameters describing the Mandelbrot image region and resolution.
#[derive(Debug, Clone, Copy)]
pub struct Param {
    pub xmin: f64,
    pub xmax: f64,
    pub width: usize,
    pub ymin: f64,
    pub ymax: f64,
    pub height: usize,
    pub max_iter: i64,
}

impl Default for Param {
    fn default() -> Self {
        Self {
            xmin: -1.75,
            xmax: 0.75,
            width: 4096,
            ymin: -1.25,
            ymax: 1.25,
            height: 4096,
            max_iter: 500,
        }
    }
}
// snippet-end:param

impl Param {
    /// The default parameters, overridden by `--width N`, `--height N`, and
    /// `--max-iter N` on the command line. The tutorial checks pass small
    /// values so that every example finishes in seconds.
    pub fn from_args() -> Self {
        let default = Self::default();
        Self {
            width: arg_usize("--width", default.width),
            height: arg_usize("--height", default.height),
            max_iter: arg_usize("--max-iter", default.max_iter as usize) as i64,
            ..default
        }
    }

    /// Build the per-pixel coordinate axes, matching Julia's
    /// `range(start, stop, length + 1)[begin:end-1]`.
    pub fn make_axes(&self) -> (Vec<f64>, Vec<f64>) {
        let x = linspace(self.xmin, self.xmax, self.width);
        let y = linspace(self.ymin, self.ymax, self.height);
        (x, y)
    }
}

fn linspace(start: f64, end: f64, count: usize) -> Vec<f64> {
    if count == 0 {
        return Vec::new();
    }
    let step = (end - start) / count as f64;
    (0..count).map(|i| start + step * i as f64).collect()
}

/// The value following `name` on the command line, if any.
pub fn arg_value(name: &str) -> Option<String> {
    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == name {
            return args.next();
        }
    }
    None
}

/// A positive integer option, or `default` when absent. Exits on a value
/// that does not parse or is zero.
pub fn arg_usize(name: &str, default: usize) -> usize {
    match arg_value(name) {
        None => default,
        Some(value) => match value.parse::<usize>() {
            Ok(parsed) if parsed > 0 => parsed,
            _ => {
                eprintln!("Invalid {name} value: {value}");
                std::process::exit(1);
            }
        },
    }
}

/// A thread-count option (`--threads` or `--workers`), defaulting to all
/// logical cores reported by the OS.
pub fn arg_thread_count(name: &str) -> usize {
    let all_cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    arg_usize(name, all_cores)
}

/// The PNG path from `--output PATH`, or `default` when absent.
pub fn output_path(default: &str) -> String {
    arg_value("--output").unwrap_or_else(|| default.to_owned())
}

// snippet-start:kernel
/// Number of iterations until escape for a single complex coordinate.
pub fn mandelbrot_kernel(param: &Param, c_re: f64, c_im: f64) -> i64 {
    let mut z_re = c_re;
    let mut z_im = c_im;
    for i in 0..param.max_iter {
        let z_re_sq = z_re * z_re;
        let z_im_sq = z_im * z_im;
        if z_re_sq + z_im_sq > 4.0 {
            return i;
        }
        z_im = 2.0 * z_re * z_im + c_im;
        z_re = z_re_sq - z_im_sq + c_re;
    }
    param.max_iter
}

/// Compute one full column of the Mandelbrot set.
///
/// This is the unit of work every example distributes: columns near the
/// set's boundary need far more iterations than columns outside it, so the
/// cost per column is very uneven and dynamic scheduling pays off.
pub fn compute_column(param: &Param, x: f64, y: &[f64]) -> Vec<i64> {
    let mut col = Vec::with_capacity(y.len());
    for &y_val in y {
        col.push(mandelbrot_kernel(param, x, y_val));
    }
    col
}
// snippet-end:kernel

/// Wrap a column-major data vector into a tenferro tensor.
pub fn tensor_from_columns(width: usize, height: usize, data: Vec<i64>) -> Tensor {
    Tensor::from_vec_col_major(vec![width, height], data).expect("tensor shape must match data")
}

/// Assemble columns that are already in column order (as every Hataori
/// `map`-style call returns them) into a `[width, height]` tensor.
pub fn tensor_from_ordered_columns(width: usize, height: usize, columns: Vec<Vec<i64>>) -> Tensor {
    assert_eq!(columns.len(), width, "one entry per column");
    tensor_from_columns(width, height, columns.concat())
}

/// Copy `column` into slot `col_idx` of a column-major image buffer.
pub fn place_column(full_data: &mut [i64], height: usize, col_idx: usize, column: &[i64]) {
    full_data[col_idx * height..(col_idx + 1) * height].copy_from_slice(column);
}

/// Sanity-check a rendered image: somewhere the kernel must have run to the
/// iteration limit, otherwise the region or the parameters are wrong.
pub fn max_iteration(tensor: &Tensor) -> i64 {
    let slice = tensor
        .as_slice::<i64>()
        .expect("tensor must be contiguous i64");
    let max_val = slice.iter().copied().max().unwrap_or(0);
    assert!(max_val > 0, "result must contain non-zero iteration counts");
    max_val
}

/// Save a tenferro tensor as a grayscale PNG image.
///
/// The tensor is assumed to have shape `[width, height]` in column-major order.
/// The result is transposed to row-major image layout and normalized by the
/// maximum value, matching Julia's `gray = colorview(Gray, normalized')`.
pub fn save_png(tensor: &Tensor, path: &str) {
    let slice = tensor
        .as_slice::<i64>()
        .expect("tensor must be contiguous i64");
    let shape = tensor.shape();
    assert_eq!(shape.len(), 2, "tensor must be 2-D");
    let width = shape[0];
    let height = shape[1];

    let max_val = slice.iter().copied().max().unwrap_or(1).max(1);

    // Convert column-major tensor data to row-major grayscale image data.
    let mut img_data = vec![0_u8; width * height];
    for x in 0..width {
        for y in 0..height {
            let src = x * height + y;
            let dst = y * width + x;
            let norm = (slice[src] as f64 / max_val as f64 * 255.0) as u8;
            img_data[dst] = norm;
        }
    }

    let img = image::GrayImage::from_raw(width as u32, height as u32, img_data)
        .expect("image dimensions must match data length");
    img.save(path).expect("PNG write must succeed");
}

/// Print an informational line, matching Julia's `@info` style.
pub fn print_info<N: std::fmt::Display>(label: &str, value: N) {
    println!("[ Info: {label} ] {value}");
}

/// Time a computation and, on rank 0, print the elapsed time.
pub fn timed<F: FnOnce() -> T, T: std::fmt::Debug>(rank: i32, label: &str, f: F) -> T {
    let start = Instant::now();
    let output = f();
    let elapsed = start.elapsed();
    if rank == 0 {
        println!("[ Info: {label} ] elapsed = {elapsed:?}, output = {output:?}");
    }
    output
}
