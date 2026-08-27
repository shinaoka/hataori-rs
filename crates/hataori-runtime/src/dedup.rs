use crate::{
    action::Segments,
    error::{ResourceKind, RuntimeError},
    wire::{RuntimeMessage, RuntimeMessageKind},
};
use hataori_runtime_foundation::protocol::{ActionId, DomainId, RequestId};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

pub(crate) enum DedupDisposition {
    New(Arc<AtomicBool>),
    Running,
    Replay(RuntimeMessage),
    Unavailable(RuntimeMessage),
    Cancelled(RuntimeMessage),
}

enum DedupState {
    Running {
        action: ActionId,
        domain: DomainId,
        cancelled: Arc<AtomicBool>,
    },
    Completed {
        action: ActionId,
        domain: DomainId,
        response: Option<(RuntimeMessageKind, Segments)>,
        retained_bytes: usize,
        expires_at: Instant,
    },
    Cancelled {
        action: ActionId,
        domain: DomainId,
        expires_at: Instant,
    },
}

pub(crate) struct DedupTable {
    entries: HashMap<RequestId, DedupState>,
    max_entries: usize,
    max_bytes: usize,
    retained_bytes: usize,
    ttl: Duration,
}

impl DedupTable {
    pub(crate) fn new(max_entries: usize, max_bytes: usize, ttl: Duration) -> Self {
        Self {
            entries: HashMap::new(),
            max_entries,
            max_bytes,
            retained_bytes: 0,
            ttl,
        }
    }

    pub(crate) fn begin(
        &mut self,
        request: RequestId,
        action: ActionId,
        domain: DomainId,
        now: Instant,
    ) -> Result<DedupDisposition, RuntimeError> {
        self.expire(now);
        if let Some(state) = self.entries.get(&request) {
            let (stored_action, stored_domain) = match state {
                DedupState::Running { action, domain, .. }
                | DedupState::Completed { action, domain, .. }
                | DedupState::Cancelled { action, domain, .. } => (*action, *domain),
            };
            if stored_action != action || stored_domain != domain {
                return Err(RuntimeError::Protocol(
                    "duplicate request changed action or domain".into(),
                ));
            }
            return Ok(match state {
                DedupState::Running { .. } => DedupDisposition::Running,
                DedupState::Completed {
                    action,
                    domain,
                    response: Some((kind, payload)),
                    ..
                } => DedupDisposition::Replay(RuntimeMessage {
                    kind: *kind,
                    request,
                    action: *action,
                    domain: *domain,
                    deadline_ms: 0,
                    payload: payload.clone(),
                }),
                DedupState::Completed {
                    action,
                    domain,
                    response: None,
                    ..
                } => DedupDisposition::Unavailable(RuntimeMessage {
                    kind: RuntimeMessageKind::DuplicateResultUnavailable,
                    request,
                    action: *action,
                    domain: *domain,
                    deadline_ms: 0,
                    payload: Vec::new(),
                }),
                DedupState::Cancelled { action, domain, .. } => {
                    DedupDisposition::Cancelled(RuntimeMessage {
                        kind: RuntimeMessageKind::Cancelled,
                        request,
                        action: *action,
                        domain: *domain,
                        deadline_ms: 0,
                        payload: Vec::new(),
                    })
                }
            });
        }
        if self.entries.len() >= self.max_entries {
            return Err(RuntimeError::ResourceExhausted {
                resource: ResourceKind::DedupEntries,
                limit: self.max_entries,
            });
        }
        let cancelled = Arc::new(AtomicBool::new(false));
        self.entries.insert(
            request,
            DedupState::Running {
                action,
                domain,
                cancelled: Arc::clone(&cancelled),
            },
        );
        Ok(DedupDisposition::New(cancelled))
    }

    pub(crate) fn cancel(
        &mut self,
        request: RequestId,
        action: ActionId,
        domain: DomainId,
        now: Instant,
    ) -> Result<(), RuntimeError> {
        self.expire(now);
        if let Some(state) = self.entries.get_mut(&request) {
            let (stored_action, stored_domain) = match state {
                DedupState::Running { action, domain, .. }
                | DedupState::Completed { action, domain, .. }
                | DedupState::Cancelled { action, domain, .. } => (*action, *domain),
            };
            if stored_action != action || stored_domain != domain {
                return Err(RuntimeError::Protocol(
                    "cancellation changed request action or domain".into(),
                ));
            }
            if let DedupState::Running { cancelled, .. } = state {
                cancelled.store(true, Ordering::Release);
            }
        } else if self.entries.len() < self.max_entries {
            self.entries.insert(
                request,
                DedupState::Cancelled {
                    action,
                    domain,
                    expires_at: now + self.ttl,
                },
            );
        }
        Ok(())
    }

    pub(crate) fn complete(
        &mut self,
        message: RuntimeMessage,
        now: Instant,
    ) -> Result<(), RuntimeError> {
        let state = self.entries.get(&message.request).ok_or_else(|| {
            RuntimeError::Protocol("action completed without a dedup entry".into())
        })?;
        let (action, domain) = match state {
            DedupState::Running { action, domain, .. }
            | DedupState::Cancelled { action, domain, .. } => (*action, *domain),
            DedupState::Completed { .. } => {
                return Err(RuntimeError::Protocol(
                    "action completed more than once".into(),
                ));
            }
        };
        if action != message.action || domain != message.domain {
            return Err(RuntimeError::Protocol(
                "action completion changed request metadata".into(),
            ));
        }
        let payload_bytes = message
            .payload
            .iter()
            .try_fold(0_usize, |total, segment| total.checked_add(segment.len()));
        let payload_bytes = payload_bytes.ok_or(RuntimeError::ResourceExhausted {
            resource: ResourceKind::DedupBytes,
            limit: self.max_bytes,
        })?;
        let response = if self
            .retained_bytes
            .checked_add(payload_bytes)
            .is_some_and(|bytes| bytes <= self.max_bytes)
        {
            self.retained_bytes += payload_bytes;
            Some((message.kind, message.payload))
        } else {
            None
        };
        let retained_bytes = response.as_ref().map_or(0, |_| payload_bytes);
        let previous = self.entries.insert(
            message.request,
            DedupState::Completed {
                action: message.action,
                domain: message.domain,
                response,
                retained_bytes,
                expires_at: now + self.ttl,
            },
        );
        if let Some(DedupState::Completed { retained_bytes, .. }) = previous {
            self.retained_bytes = self.retained_bytes.saturating_sub(retained_bytes);
        }
        Ok(())
    }

    pub(crate) fn expire(&mut self, now: Instant) {
        let expired: Vec<_> = self
            .entries
            .iter()
            .filter_map(|(request, state)| match state {
                DedupState::Completed { expires_at, .. }
                | DedupState::Cancelled { expires_at, .. }
                    if *expires_at <= now =>
                {
                    Some(*request)
                }
                _ => None,
            })
            .collect();
        for request in expired {
            if let Some(DedupState::Completed { retained_bytes, .. }) =
                self.entries.remove(&request)
            {
                self.retained_bytes = self.retained_bytes.saturating_sub(retained_bytes);
            }
        }
    }

    pub(crate) fn clear(&mut self) {
        self.entries.clear();
        self.retained_bytes = 0;
    }

    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    pub(crate) const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }
}

#[cfg(test)]
mod tests;
