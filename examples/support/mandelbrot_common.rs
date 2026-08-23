#![allow(dead_code)]

use std::time::Instant;
use tenferro_tensor::Tensor;

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

impl Param {
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
pub fn compute_column(param: &Param, x: f64, y: &[f64]) -> Vec<i64> {
    let mut col = Vec::with_capacity(y.len());
    for &y_val in y {
        col.push(mandelbrot_kernel(param, x, y_val));
    }
    col
}

/// Wrap a column-major data vector into a tenferro tensor.
pub fn tensor_from_columns(width: usize, height: usize, data: Vec<i64>) -> Tensor {
    Tensor::from_vec_col_major(vec![width, height], data).expect("tensor shape must match data")
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
