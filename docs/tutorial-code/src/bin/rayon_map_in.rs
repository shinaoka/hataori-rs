//! Rank-local thread parallelism: `hataori::map_in` on a Rayon domain.
//!
//! Run with:
//!
//! ```sh
//! cargo run -p hataori-tutorial-code --features rayon --bin rayon_map_in
//! ```

use hataori::{map_in, Domain, LocalMode, MapInError, PlacementStatus, PoolOwnership};
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use std::sync::Arc;

// snippet-start:managed-domain
/// Builds a Hataori-owned Rayon pool with `workers` threads.
///
/// The CPU set must be non-empty and must not be smaller than the worker
/// count. On Linux every worker is pinned to its CPU and the placement is
/// verified at startup; on other platforms the set is only a declaration.
fn managed_domain(workers: usize) -> Domain {
    let cpu_set: Vec<usize> = (0..workers).collect();
    let domain = Domain::managed(cpu_set, workers).expect("managed Rayon domain");
    assert_eq!(domain.worker_count(), workers);
    assert_eq!(domain.pool_ownership(), Some(PoolOwnership::Managed));
    domain
}
// snippet-end:managed-domain

// snippet-start:external-domain
/// Wraps a pool the application already owns.
///
/// Hataori neither re-pins the workers nor shuts the pool down when the
/// domain is dropped; the declared CPU set is recorded as `CallerDeclared`.
fn external_domain(pool: Arc<rayon::ThreadPool>) -> Domain {
    let workers = pool.current_num_threads();
    let cpu_set: Vec<usize> = (0..workers).collect();
    let domain = Domain::external(pool, cpu_set, workers).expect("external Rayon domain");
    assert_eq!(domain.pool_ownership(), Some(PoolOwnership::External));
    assert_eq!(
        domain.placement_status(),
        Some(PlacementStatus::CallerDeclared)
    );
    domain
}
// snippet-end:external-domain

// snippet-start:local-modes
fn slow_square(item: u64) -> Result<u64, String> {
    // Reverse the cost so that out-of-order completion is likely under `Outer`.
    std::thread::sleep(std::time::Duration::from_millis(8 - item));
    Ok(item * item)
}

fn demonstrate_local_modes(domain: &Domain) {
    let items: Vec<u64> = (0..8).collect();
    let expected: Vec<u64> = items.iter().map(|item| item * item).collect();

    // Sequential: one callback at a time inside the pool, stop at first error.
    let sequential = map_in(domain, LocalMode::Sequential, items.clone(), slow_square).unwrap();
    assert_eq!(sequential, expected);

    // Outer: every item runs in parallel across the pool's workers; the
    // result vector still follows the input order.
    let outer = map_in(domain, LocalMode::Outer, items.clone(), slow_square).unwrap();
    assert_eq!(outer, expected);

    // Inner: callbacks run one at a time, but each callback may use nested
    // Rayon parallelism in the same pool (for example a `par_iter`).
    let inner = map_in(domain, LocalMode::Inner, items, |item| {
        let partial: u64 = (0..item).into_par_iter().map(|_| item).sum();
        Ok::<_, String>(partial)
    })
    .unwrap();
    assert_eq!(inner, expected);
}
// snippet-end:local-modes

// snippet-start:outer-errors
fn demonstrate_outer_errors(domain: &Domain) {
    let failing = |item: u64| {
        if item % 3 == 2 {
            Err(format!("item {item} rejected"))
        } else {
            Ok(item)
        }
    };

    // Sequential stops at the first failing input.
    let error = map_in(domain, LocalMode::Sequential, (0..9).collect(), failing).unwrap_err();
    match error {
        MapInError::Callback(inner) => assert_eq!(inner.index(), 2),
        other => panic!("unexpected error: {other:?}"),
    }

    // Outer evaluates every input exactly once, then reports the lowest
    // failing index even if a later failure finished first.
    let error = map_in(domain, LocalMode::Outer, (0..9).collect(), failing).unwrap_err();
    match error {
        MapInError::Callback(inner) => {
            assert_eq!(inner.index(), 2);
            assert_eq!(inner.message(), "item 2 rejected");
        }
        other => panic!("unexpected error: {other:?}"),
    }
}
// snippet-end:outer-errors

fn main() {
    let workers = 2;
    let managed = managed_domain(workers);
    demonstrate_local_modes(&managed);
    demonstrate_outer_errors(&managed);

    let pool = Arc::new(
        ThreadPoolBuilder::new()
            .num_threads(workers)
            .build()
            .unwrap(),
    );
    let external = external_domain(Arc::clone(&pool));
    demonstrate_local_modes(&external);
    demonstrate_outer_errors(&external);

    // A domain admits one coarse operation at a time; calling `map_in` from
    // inside another pool is rejected before the callback runs.
    let nested = pool.install(|| map_in(&managed, LocalMode::Outer, vec![1_u64], Ok::<_, String>));
    assert!(matches!(nested, Err(MapInError::ForeignPool)));

    println!("rayon_map_in: managed and external domains verified with {workers} workers");
}
