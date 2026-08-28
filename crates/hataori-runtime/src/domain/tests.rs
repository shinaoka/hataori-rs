use super::*;
use crate::Segments;
use crate::{action::ActionRegistry, Action, WireValue};
use std::{
    sync::{Condvar, Mutex},
    time::Instant,
};

struct Block;
impl WireValue for Block {
    const SCHEMA_ID: u64 = 99;
    fn encode(self) -> Result<Segments, ActionError> {
        Ok(Vec::new())
    }
    fn decode(segments: Segments) -> Result<Self, ActionError> {
        if segments.is_empty() {
            Ok(Self)
        } else {
            Err(ActionError::codec("Block has no payload"))
        }
    }
}
impl Action for Block {
    const ID: u128 = 99;
    type Output = ();
}

fn job(handler: RegisteredAction, sequence: u64) -> ActionJob {
    ActionJob {
        requester: LocalityId::new(0),
        request: RequestId::new(LocalityId::new(0), sequence).unwrap(),
        action_id: ActionId::new(Block::ID).unwrap(),
        domain: DomainId::DEFAULT,
        trace_id: None,
        input: ActionValue::Encoded(Vec::new()),
        handler,
        cancelled: Arc::new(AtomicBool::new(false)),
        local: true,
        submitted_at: Instant::now(),
        object: None,
    }
}

#[test]
fn bounded_domain_queue_rejects_third_job_without_retention() {
    let gate = Arc::new((Mutex::new((false, false)), Condvar::new()));
    let worker_gate = Arc::clone(&gate);
    let mut actions = ActionRegistry::default();
    actions
        .register::<Block, _>(move |_| {
            let (lock, ready) = &*worker_gate;
            let mut state = lock.lock().unwrap();
            state.0 = true;
            ready.notify_one();
            while !state.1 {
                state = ready.wait(state).unwrap();
            }
            Ok(())
        })
        .unwrap();
    let handler = actions
        .get(ActionId::new(Block::ID).unwrap())
        .unwrap()
        .clone();
    let mut domains = DomainRegistry::new(
        &[DomainConfig {
            id: DomainId::DEFAULT,
            workers: 1,
            queue_capacity: 1,
        }],
        3,
    )
    .unwrap();
    domains.submit(job(handler.clone(), 1)).unwrap();
    {
        let (lock, ready) = &*gate;
        let mut state = lock.lock().unwrap();
        while !state.0 {
            state = ready.wait(state).unwrap();
        }
    }
    domains.submit(job(handler.clone(), 2)).unwrap();
    assert!(matches!(
        domains.submit(job(handler, 3)),
        Err(RuntimeError::ResourceExhausted {
            resource: ResourceKind::ActionQueue,
            limit: 1
        })
    ));
    {
        let (lock, ready) = &*gate;
        let mut state = lock.lock().unwrap();
        state.1 = true;
        ready.notify_one();
    }
    for _ in 0..10_000 {
        if domains.stats()[0].1.completed == 2 {
            break;
        }
        std::thread::yield_now();
    }
    assert_eq!(domains.stats()[0].1.completed, 2);
    assert!(!domains.idle());
    while domains.try_completion().is_some() {}
    assert!(domains.idle());
    domains.stop();
}
