use super::*;
use crate::protocol::ProtocolLimits;

fn limits() -> ProtocolLimits {
    ProtocolLimits {
        max_queued_parcels_per_peer: 2,
        max_inflight_bytes_per_peer: 1000,
        control_reserved_bytes_per_peer: 200,
        ..ProtocolLimits::default()
    }
}

#[test]
fn permit_rollback_and_commit_account_exactly_once() {
    let state = SubmissionState::new(2, limits());
    let ticket = {
        let permit = state.reserve(1, Channel::Bulk, 700).unwrap();
        let ticket = permit.ticket();
        permit.commit();
        ticket
    };
    assert_eq!(ticket.get(), 1);
    assert!(matches!(
        state.reserve(1, Channel::Action, 101),
        Err(TransportError::ByteLimit { limit: 800, .. })
    ));
    {
        let rollback = state.reserve(1, Channel::Control, 100).unwrap();
        assert_eq!(rollback.ticket().get(), 2);
    }
    let stats = state.stats();
    assert_eq!(stats.queued_bulk, 1);
    assert_eq!(stats.queued_control, 0);
    assert_eq!(stats.queued_bytes, 700);
    assert_eq!(stats.submitted_parcels, 1);
    state.release(1, Channel::Bulk, 700);
    assert_eq!(state.stats().retained_bytes(), 0);
}

#[test]
fn control_reservation_and_atomic_close_are_enforced() {
    let state = SubmissionState::new(2, limits());
    state.reserve(1, Channel::Bulk, 800).unwrap().commit();
    state.reserve(1, Channel::Control, 200).unwrap().commit();
    assert!(matches!(
        state.reserve(1, Channel::Control, 1),
        Err(TransportError::ByteLimit { limit: 1000, .. })
    ));
    state.release(1, Channel::Bulk, 800);
    state.release(1, Channel::Control, 200);
    state.close();
    assert!(matches!(
        state.reserve(1, Channel::Control, 1),
        Err(TransportError::Shutdown)
    ));
}
