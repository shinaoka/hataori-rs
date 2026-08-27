use super::*;
use hataori_runtime_foundation::protocol::LocalityId;

fn ids() -> (RequestId, ActionId, DomainId) {
    (
        RequestId::new(LocalityId::new(1), 1).unwrap(),
        ActionId::new(2).unwrap(),
        DomainId::new(3),
    )
}

#[test]
fn byte_exhaustion_keeps_completion_marker_without_reexecution() {
    let now = Instant::now();
    let (request, action, domain) = ids();
    let mut table = DedupTable::new(2, 1, Duration::from_secs(1));
    assert!(matches!(
        table.begin(request, action, domain, now).unwrap(),
        DedupDisposition::New(_)
    ));
    table
        .complete(
            RuntimeMessage {
                kind: RuntimeMessageKind::Success,
                request,
                action,
                domain,
                deadline_ms: 0,
                payload: vec![vec![1, 2]],
            },
            now,
        )
        .unwrap();
    assert_eq!(table.retained_bytes(), 0);
    assert!(matches!(
        table.begin(request, action, domain, now).unwrap(),
        DedupDisposition::Unavailable(_)
    ));
}

#[test]
fn cancellation_tombstone_and_metadata_checks_are_bounded() {
    let now = Instant::now();
    let (request, action, domain) = ids();
    let mut table = DedupTable::new(1, 8, Duration::from_millis(1));
    table.cancel(request, action, domain, now).unwrap();
    assert!(matches!(
        table.begin(request, action, domain, now).unwrap(),
        DedupDisposition::Cancelled(_)
    ));
    assert!(matches!(
        table.begin(request, ActionId::new(9).unwrap(), domain, now),
        Err(RuntimeError::Protocol(_))
    ));
    table.expire(now + Duration::from_millis(2));
    assert_eq!(table.len(), 0);
}
