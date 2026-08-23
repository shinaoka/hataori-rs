#![allow(dead_code)]

use std::time::Instant;

/// Euclidean algorithm for the greatest common divisor.
pub fn gcd(mut a: u64, mut b: u64) -> u64 {
    while b != 0 {
        let tmp = a % b;
        a = b;
        b = tmp;
    }
    a
}

/// Count coprime pairs `(a, b)` where `a` follows the round-robin sequence
/// `start, start + step, ...` and `b ∈ 1..=n`.
pub fn count_coprime_pairs(start: i64, step: i64, n: i64) -> i64 {
    let mut local_cnt = 0_i64;
    for a in (start..=n).step_by(step as usize) {
        let a = a as u64;
        for b in 1..=n {
            if gcd(a, b as u64) == 1 {
                local_cnt += 1;
            }
        }
    }
    local_cnt
}

/// Count coprime pairs for a single value of `a`.
pub fn count_for_a(a: u64, n: i64) -> i64 {
    let mut cnt = 0_i64;
    for b in 1..=n {
        if gcd(a, b as u64) == 1 {
            cnt += 1;
        }
    }
    cnt
}

/// Estimate π from the coprime probability.
pub fn estimate_pi(total_coprime: i64, n: i64) -> f64 {
    let prob = total_coprime as f64 / n as f64 / n as f64;
    (6.0 / prob).sqrt()
}

/// Print an informational line, matching Julia's `@info` style.
pub fn print_info<N: std::fmt::Display>(label: &str, value: N) {
    println!("[ Info: {label} ] {value}");
}

/// Time a computation and print the elapsed time and result.
pub fn timed<F: FnOnce() -> f64>(label: &str, f: F) -> f64 {
    let start = Instant::now();
    let output = f();
    let elapsed = start.elapsed();
    println!("[ Info: {label} ] elapsed = {elapsed:?}, output = {output}");
    output
}
