use super::mpi_api::collective::SystemOperation;
use super::mpi_api::traits::*;
use hataori::{pmap, Domain, LocalMode, PmapErrorKind, PmapOptions};
use rayon::ThreadPoolBuilder;
use serde::{Deserialize, Serialize};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[derive(Serialize, Deserialize)]
struct RendezvousValue {
    bytes: Vec<u8>,
    mark_on_drop: bool,
    trace_path: String,
}

impl Drop for RendezvousValue {
    fn drop(&mut self) {
        if self.mark_on_drop {
            let _ = std::fs::write(&self.trace_path, b"remote-result-send-completed");
        }
    }
}

pub fn run<C: Communicator>(world: &C) {
    let rank = world.rank();
    let main_thread = std::thread::current().id();

    static GLOBAL_WORKERS: AtomicUsize = AtomicUsize::new(0);
    ThreadPoolBuilder::new()
        .num_threads(1)
        .start_handler(|_| {
            GLOBAL_WORKERS.fetch_add(1, Ordering::SeqCst);
        })
        .build_global()
        .expect("global Rayon pool must be unused before the hybrid smoke");
    let pool = Arc::new(ThreadPoolBuilder::new().num_threads(1).build().unwrap());
    let domain = Domain::external(Arc::clone(&pool), vec![0], 1).unwrap();
    let bias = 7_i32;

    if std::env::var_os("HATAORI_HYBRID_PANIC").is_some() {
        let _ = pmap(
            world,
            &domain,
            PmapOptions::default(),
            (rank == 0).then(|| vec![0_i32]),
            |_| -> Result<i32, String> { panic!("expected hybrid callback panic") },
        );
        panic!("hybrid callback panic did not abort the MPI job");
    }

    for mode in [LocalMode::Sequential, LocalMode::Outer, LocalMode::Inner] {
        let calls = AtomicUsize::new(0);
        let root_participated = AtomicBool::new(false);
        let callback_pool = Arc::clone(&pool);
        let input = (rank == 0).then(|| (0..16).collect::<Vec<i32>>());
        let result = pmap(
            world,
            &domain,
            PmapOptions {
                root: 0,
                batch_size: NonZeroUsize::new(3).unwrap(),
                local_mode: mode,
            },
            input,
            |item| {
                assert!(callback_pool.current_thread_index().is_some());
                calls.fetch_add(1, Ordering::Relaxed);
                if rank == 0 && item == 0 {
                    root_participated.store(true, Ordering::Relaxed);
                }
                if mode == LocalMode::Inner {
                    let (_, in_pool) = rayon::join(
                        || item + bias,
                        || callback_pool.current_thread_index().is_some(),
                    );
                    assert!(in_pool);
                }
                Ok::<_, String>(item + bias)
            },
        )
        .unwrap();
        let local_calls = calls.load(Ordering::Relaxed) as i64;
        let mut total_calls = 0_i64;
        world.all_reduce_into(&local_calls, &mut total_calls, SystemOperation::sum());
        assert_eq!(total_calls, 16);
        if rank == 0 {
            assert_eq!(
                result.unwrap(),
                (0..16).map(|item| item + bias).collect::<Vec<_>>()
            );
            assert!(root_participated.load(Ordering::Relaxed));
        } else {
            assert!(result.is_none());
        }
    }

    let error_calls = AtomicUsize::new(0);
    let error = pmap(
        world,
        &domain,
        PmapOptions {
            root: 0,
            batch_size: NonZeroUsize::new(8).unwrap(),
            local_mode: LocalMode::Outer,
        },
        (rank == 0).then(|| (0..8).collect::<Vec<i32>>()),
        |item| {
            error_calls.fetch_add(1, Ordering::Relaxed);
            match item {
                1 => Err::<i32, _>("first hybrid failure".to_owned()),
                4 => Err("later hybrid failure".to_owned()),
                _ => Ok(item),
            }
        },
    )
    .unwrap_err();
    let local_error_calls = error_calls.load(Ordering::Relaxed) as i64;
    let mut total_error_calls = 0_i64;
    world.all_reduce_into(
        &local_error_calls,
        &mut total_error_calls,
        SystemOperation::sum(),
    );
    assert_eq!(total_error_calls, 8);
    assert_eq!(error.kind(), PmapErrorKind::User);
    assert_eq!(error.message(), "first hybrid failure");

    let missing = Domain::sequential();
    let missing_error = pmap(
        world,
        &missing,
        PmapOptions::default(),
        (rank == 0).then(|| vec![1_i32]),
        Ok::<_, String>,
    )
    .unwrap_err();
    assert_eq!(missing_error.kind(), PmapErrorKind::Preflight);

    let busy = domain.try_admit().unwrap();
    let busy_error = pmap(
        world,
        &domain,
        PmapOptions::default(),
        (rank == 0).then(|| vec![2_i32]),
        Ok::<_, String>,
    )
    .unwrap_err();
    assert_eq!(busy_error.kind(), PmapErrorKind::Preflight);
    drop(busy);

    let reused = pmap(
        world,
        &domain,
        PmapOptions::default(),
        (rank == 0).then(|| vec![3_i32, 1, 4]),
        |item| Ok::<_, String>(item * 2),
    )
    .unwrap();
    if rank == 0 {
        assert_eq!(reused.unwrap(), vec![6, 2, 8]);
    }

    if world.size() > 1 {
        if let Ok(trace_path) = std::env::var("HATAORI_HYBRID_TRACE") {
            if rank == 0 {
                let _ = std::fs::remove_file(&trace_path);
            }
            world.barrier();
            let callback_trace = trace_path.clone();
            let result = pmap(
                world,
                &domain,
                PmapOptions::default(),
                (rank == 0).then(|| vec![false, true]),
                |remote_item| {
                    if remote_item {
                        Ok::<_, String>(RendezvousValue {
                            bytes: vec![0x5a; 2 * 1024 * 1024],
                            mark_on_drop: true,
                            trace_path: callback_trace.clone(),
                        })
                    } else {
                        let observed = (0..500).any(|_| {
                            if std::path::Path::new(&callback_trace).exists() {
                                true
                            } else {
                                std::thread::sleep(Duration::from_millis(10));
                                false
                            }
                        });
                        assert!(
                            observed,
                            "remote rendezvous result was not serviced before root completion"
                        );
                        Ok(RendezvousValue {
                            bytes: vec![0x11; 16],
                            mark_on_drop: false,
                            trace_path: callback_trace.clone(),
                        })
                    }
                },
            )
            .unwrap();
            if rank == 0 {
                let mut values = result.unwrap();
                assert_eq!(values[0].bytes, vec![0x11; 16]);
                assert_eq!(values[1].bytes.len(), 2 * 1024 * 1024);
                assert!(values[1].bytes.iter().all(|byte| *byte == 0x5a));
                values[1].mark_on_drop = false;
                drop(values);
            }
            world.barrier();
            if rank == 0 {
                std::fs::remove_file(&trace_path).unwrap();
            }
        }
    }

    assert_eq!(std::thread::current().id(), main_thread);
    assert_eq!(GLOBAL_WORKERS.load(Ordering::SeqCst), 1);
}
