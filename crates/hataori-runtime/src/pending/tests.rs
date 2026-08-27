use super::*;

fn entry(sequence: u64, expires_at: Instant) -> (RequestId, PendingEntry) {
    let origin = LocalityId::new(0);
    let request = RequestId::new(origin, sequence).unwrap();
    (
        request,
        PendingEntry {
            promise: Arc::new(Promise::new()),
            destination: LocalityId::new(1),
            action: ActionId::new(1).unwrap(),
            domain: DomainId::DEFAULT,
            trace_id: None,
            expires_at,
            deadline: Duration::from_millis(1),
            cancel_token: None,
        },
    )
}

#[test]
fn pending_capacity_deadline_and_checked_result_identity_are_mechanical() {
    let now = Instant::now();
    let table = PendingTable::new(1);
    let (first, first_entry) = entry(1, now + Duration::from_millis(1));
    table.insert(first, first_entry).unwrap();
    let (second, second_entry) = entry(2, now + Duration::from_millis(2));
    assert!(matches!(
        table.insert(second, second_entry),
        Err(RuntimeError::ResourceExhausted {
            resource: ResourceKind::PendingCalls,
            limit: 1
        })
    ));
    assert!(matches!(
        table.take_checked(first, LocalityId::new(9), ActionId::new(1).unwrap()),
        Err(RuntimeError::Protocol(_))
    ));
    assert_eq!(table.len(), 1);
    let expired = table.expired(now + Duration::from_millis(2));
    assert_eq!(expired.len(), 1);
    assert_eq!(table.len(), 0);
}
