# 1. Serial map

**Model:** one thread, one process. **Features:** none.

`hataori::map` is the semantic baseline for every other model: it applies a
fallible callback to a `Vec<T>` in order and returns a `Vec<U>` in the same
order. There are no `Send`, `Sync`, `'static`, or serde requirements, so the
callback can borrow anything, including `Rc` or `RefCell` state.

## Code

<!-- snippet-source: docs/tutorial-code/src/bin/serial_map.rs#serial-map -->
```rust
use hataori::{map, MapError};

/// A callback error type: anything that implements `Display` works.
#[derive(Debug)]
struct NegativeInput(i64);

impl std::fmt::Display for NegativeInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "negative input: {}", self.0)
    }
}

fn checked_square(item: i64) -> Result<i64, NegativeInput> {
    if item < 0 {
        return Err(NegativeInput(item));
    }
    Ok(item * item)
}

fn main() {
    // `map` applies the callback one item at a time, in order, on the calling
    // thread. Nothing here needs `Send`, `Sync`, `'static`, or serde.
    let squares = map(vec![1_i64, 2, 3, 4], checked_square).expect("all inputs are non-negative");
    assert_eq!(squares, vec![1, 4, 9, 16]);

    // The callback may borrow local state; a counter shows the exactly-once
    // evaluation and the stop-at-first-error rule.
    let mut calls = 0_usize;
    let error: MapError = map(vec![5_i64, -1, 7], |item| {
        calls += 1;
        checked_square(item)
    })
    .expect_err("the second item fails");
    assert_eq!(error.index(), 1);
    assert_eq!(error.message(), "negative input: -1");
    // Evaluation stopped at the failing item: `7` was never visited.
    assert_eq!(calls, 2);

    println!(
        "serial_map: {squares:?}; first error at index {}",
        error.index()
    );
}
```
<!-- end-snippet-source -->

Source: [`docs/tutorial-code/src/bin/serial_map.rs`](https://github.com/shinaoka/hataori-rs/blob/main/docs/tutorial-code/src/bin/serial_map.rs)

## What to notice

- The error type only needs `Display`. `MapError` stores the failing input's
  index and the `Display` output, truncated to 4096 bytes at a UTF-8
  character boundary.
- `map` **stops at the first error**. The counter shows that the third item
  was never evaluated, and inputs after the failure are simply dropped.
- The callback is `FnMut`, so it can mutate captured state such as `calls`.

## Run

```bash
cargo run -p hataori-tutorial-code --bin serial_map
```

Next: [2. Rayon map_in](rayon-map-in.md) keeps this contract and adds a
thread pool.
