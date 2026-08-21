use hataori::{map_in, Domain, LocalMode, MapInError};
use hataori_tenferro::{TenferroAdapterError, TenferroDomain};
use std::collections::BTreeSet;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::time::{Duration, Instant};
use tenferro_cpu::{
    CpuAdmissionMode, CpuBackendKind, CpuExecutionMode, CpuExecutorAffinity, CpuExecutorShutdown,
};
use tenferro_tensor::{Tensor, TensorElementwise};

fn wait_for_workers(started: &AtomicUsize, expected: usize) {
    let deadline = Instant::now() + Duration::from_secs(2);
    while started.load(Ordering::Acquire) != expected {
        assert!(Instant::now() < deadline, "Rayon workers did not start");
        std::thread::yield_now();
    }
}

#[test]
fn caller_managed_adapter_obeys_hataori_domain_contract() {
    static GLOBAL_STARTED: AtomicUsize = AtomicUsize::new(0);
    rayon::ThreadPoolBuilder::new()
        .num_threads(1)
        .start_handler(|_| {
            GLOBAL_STARTED.fetch_add(1, Ordering::Release);
        })
        .build_global()
        .expect("integration test owns global Rayon initialization");
    wait_for_workers(&GLOBAL_STARTED, 1);
    let global_before = GLOBAL_STARTED.load(Ordering::Acquire);

    let pool_started = Arc::new(AtomicUsize::new(0));
    let started = Arc::clone(&pool_started);
    let pool = Arc::new(
        rayon::ThreadPoolBuilder::new()
            .num_threads(2)
            .thread_name(|index| format!("hataori-tenferro-{index}"))
            .start_handler(move |_| {
                started.fetch_add(1, Ordering::Release);
            })
            .build()
            .unwrap(),
    );
    wait_for_workers(&pool_started, 2);
    let domain = Domain::external(Arc::clone(&pool), vec![0, 1], 2).unwrap();
    let adapter = TenferroDomain::new(domain).unwrap();

    let called = AtomicBool::new(false);
    let outside = adapter.with_backend(|_| called.store(true, Ordering::Relaxed));
    assert!(matches!(
        outside,
        Err(TenferroAdapterError::OutsideDomainPool)
    ));
    assert!(!called.load(Ordering::Relaxed));

    let worker_names = Arc::new(Mutex::new(BTreeSet::new()));
    let observed = Arc::clone(&worker_names);
    let values = map_in(adapter.domain(), LocalMode::Inner, vec![()], |_| {
        adapter
            .with_backend(|backend| {
                let info = backend.execution_info();
                assert_eq!(backend.kind(), CpuBackendKind::Faer);
                assert_eq!(info.execution_mode(), CpuExecutionMode::CallerManaged);
                assert_eq!(info.admission_mode(), CpuAdmissionMode::CallerManaged);
                assert_eq!(info.worker_count(), 2);
                assert_eq!(info.thread_budget(), 2);
                assert_eq!(info.executor_affinity(), CpuExecutorAffinity::None);
                assert_eq!(info.executor_shutdown(), CpuExecutorShutdown::CallerOwned);
                assert!(info.resolved_placement().is_none());
                assert!(info.domain_cpus().is_none());

                let lhs = Tensor::from_vec_col_major(vec![2], vec![1.0_f64, 2.0])?;
                let rhs = Tensor::from_vec_col_major(vec![2], vec![3.0_f64, 4.0])?;
                let sum = backend.add(&lhs, &rhs)?;
                let values = sum.as_slice::<f64>().unwrap().to_vec();

                backend.with_linalg_pool(|_, _| {
                    rayon::broadcast(|_| {
                        observed
                            .lock()
                            .unwrap()
                            .insert(std::thread::current().name().unwrap_or("").to_owned());
                    });
                    Ok(())
                })?;
                Ok::<_, tenferro_tensor::Error>(values)
            })
            .map_err(|error| error.to_string())?
            .map_err(|error| error.to_string())
    })
    .unwrap();
    assert_eq!(values, vec![vec![4.0, 6.0]]);
    let names = worker_names.lock().unwrap();
    assert_eq!(names.len(), 2);
    assert!(names
        .iter()
        .all(|name| name.starts_with("hataori-tenferro-")));
    drop(names);
    assert_eq!(pool_started.load(Ordering::Acquire), 2);
    assert_eq!(GLOBAL_STARTED.load(Ordering::Acquire), global_before);

    let borrowed = String::from("borrowed");
    let output = map_in(
        adapter.domain(),
        LocalMode::Inner,
        vec![borrowed.as_str()],
        |value| {
            adapter
                .with_backend(|backend| {
                    assert_eq!(backend.execution_info().thread_budget(), 2);
                    value
                })
                .map_err(|error| error.to_string())
        },
    )
    .unwrap();
    assert_eq!(output, vec!["borrowed"]);

    let admission = adapter.domain().try_admit().unwrap();
    assert!(matches!(
        map_in(adapter.domain(), LocalMode::Inner, vec![()], |_| Ok::<
            _,
            &str,
        >(
            ()
        )),
        Err(MapInError::DomainBusy(_))
    ));
    drop(admission);

    let recursive = map_in(adapter.domain(), LocalMode::Inner, vec![()], |_| {
        adapter
            .with_backend(|_| {
                matches!(
                    adapter.with_backend(|_| ()),
                    Err(TenferroAdapterError::ConcurrentEntry)
                )
            })
            .map_err(|error| error.to_string())
    })
    .unwrap();
    assert_eq!(recursive, vec![true]);

    let callback_error = map_in(adapter.domain(), LocalMode::Inner, vec![()], |_| {
        adapter
            .with_backend(|backend| {
                backend
                    .with_linalg_pool(|_, _| Ok(()))
                    .map_err(|error| error.to_string())?;
                Err::<(), _>("callback failure".to_owned())
            })
            .map_err(|error| error.to_string())?
    });
    assert!(matches!(callback_error, Err(MapInError::Callback(_))));
    assert!(map_in(adapter.domain(), LocalMode::Inner, vec![()], |_| {
        adapter
            .with_backend(|backend| backend.with_linalg_pool(|_, _| Ok(())))
            .map_err(|error| error.to_string())?
            .map_err(|error| error.to_string())
    })
    .is_ok());

    let panic = catch_unwind(AssertUnwindSafe(|| {
        let _ = map_in(adapter.domain(), LocalMode::Inner, vec![()], |_| {
            adapter
                .with_backend(|backend| {
                    backend.with_linalg_pool(|_, _| -> tenferro_tensor::Result<()> {
                        panic!("fixture panic")
                    })
                })
                .map_err(|error| error.to_string())?
                .map_err(|error| error.to_string())
        });
    }));
    assert!(panic.is_err());
    assert!(map_in(adapter.domain(), LocalMode::Inner, vec![()], |_| {
        adapter
            .with_backend(|backend| backend.with_linalg_pool(|_, _| Ok(())))
            .map_err(|error| error.to_string())?
            .map_err(|error| error.to_string())
    })
    .is_ok());

    let simultaneous = Arc::new(Barrier::new(2));
    let entered = AtomicUsize::new(0);
    let rejected = AtomicUsize::new(0);
    let outer = map_in(adapter.domain(), LocalMode::Outer, vec![(), ()], |_| {
        simultaneous.wait();
        match adapter.with_backend(|backend| {
            backend.with_linalg_pool(|_, _| {
                entered.fetch_add(1, Ordering::AcqRel);
                std::thread::sleep(Duration::from_millis(100));
                Ok(())
            })
        }) {
            Err(TenferroAdapterError::ConcurrentEntry) => {
                rejected.fetch_add(1, Ordering::AcqRel);
                Err("concurrent adapter entry".to_owned())
            }
            Err(error) => Err(error.to_string()),
            Ok(result) => result.map_err(|error| error.to_string()),
        }
    });
    assert!(matches!(outer, Err(MapInError::Callback(_))));
    assert_eq!(entered.load(Ordering::Acquire), 1);
    assert_eq!(rejected.load(Ordering::Acquire), 1);
    assert!(map_in(adapter.domain(), LocalMode::Inner, vec![()], |_| {
        adapter
            .with_backend(|backend| backend.with_linalg_pool(|_, _| Ok(())))
            .map_err(|error| error.to_string())?
            .map_err(|error| error.to_string())
    })
    .is_ok());

    assert_eq!(pool_started.load(Ordering::Acquire), 2);
    assert_eq!(GLOBAL_STARTED.load(Ordering::Acquire), global_before);
}
