//! Shared backend-independent conformance assertions used by Phase A tests.

use crate::{
    protocol::Parcel,
    transport::{SendTicket, TransportEvent},
};
use std::collections::BTreeMap;

pub fn verify_completions(
    expected: &[SendTicket],
    events: &[TransportEvent],
) -> Result<(), String> {
    let mut actual: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            TransportEvent::LocalSendComplete { ticket } => Some(ticket.get()),
            _ => None,
        })
        .collect();
    let mut expected: Vec<_> = expected.iter().map(|ticket| ticket.get()).collect();
    actual.sort_unstable();
    expected.sort_unstable();
    if actual != expected {
        return Err(format!(
            "local completion mismatch: expected {expected:?}, got {actual:?}"
        ));
    }
    if events.iter().any(|event| {
        matches!(
            event,
            TransportEvent::SendFailed { .. } | TransportEvent::PeerFailed { .. }
        )
    }) {
        return Err("successful conformance exchange reported a failure".into());
    }
    Ok(())
}

pub fn verify_incoming(expected: &[Parcel], events: &[TransportEvent]) -> Result<(), String> {
    let expected = by_message_id(expected.iter().cloned())?;
    let actual = by_message_id(events.iter().filter_map(|event| match event {
        TransportEvent::Incoming { parcel } => Some(parcel.clone()),
        _ => None,
    }))?;
    if actual != expected {
        return Err(format!(
            "incoming parcel mismatch: expected {expected:?}, got {actual:?}"
        ));
    }
    Ok(())
}

fn by_message_id(
    parcels: impl IntoIterator<Item = Parcel>,
) -> Result<BTreeMap<u128, Parcel>, String> {
    let mut output = BTreeMap::new();
    for parcel in parcels {
        let id = parcel.message_id.get();
        if output.insert(id, parcel).is_some() {
            return Err(format!(
                "duplicate message ID in conformance evidence: {id}"
            ));
        }
    }
    Ok(output)
}
