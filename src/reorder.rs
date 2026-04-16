// SPDX-FileCopyrightText: 2026 Matt Curfman
// SPDX-License-Identifier: Apache-2.0

#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use tokio::time::Instant;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SequenceGap {
    pub expected: u8,
    pub observed: u8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReorderEvent {
    GapDetected(SequenceGap),
    GapResolved(SequenceGap),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ReorderKey {
    pub group_id: String,
    pub edge_node_id: String,
}

impl ReorderKey {
    pub fn new(group_id: impl Into<String>, edge_node_id: impl Into<String>) -> Self {
        Self {
            group_id: group_id.into(),
            edge_node_id: edge_node_id.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReorderRequest {
    pub key: ReorderKey,
    pub gap: SequenceGap,
}

#[derive(Debug, Clone)]
pub struct ReorderTracker {
    timeout: Duration,
    nodes: BTreeMap<ReorderKey, EdgeNodeReorderState>,
}

impl ReorderTracker {
    pub fn new(timeout: Duration) -> Self {
        Self {
            timeout,
            nodes: BTreeMap::new(),
        }
    }

    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    pub fn reset(&mut self, key: ReorderKey, sequence: u8) {
        self.nodes.entry(key).or_default().reset(sequence);
    }

    pub fn clear(&mut self, key: &ReorderKey) {
        self.nodes.remove(key);
    }

    pub fn observe(&mut self, key: ReorderKey, observed: u8, now: Instant) -> Option<ReorderEvent> {
        self.nodes
            .entry(key)
            .or_default()
            .observe(observed, now, self.timeout)
    }

    pub fn next_deadline(&self) -> Option<Instant> {
        self.nodes
            .values()
            .filter_map(|state| state.next_deadline())
            .min()
    }

    pub fn due_rebirth_requests(&mut self, now: Instant) -> Vec<ReorderRequest> {
        self.nodes
            .iter_mut()
            .filter_map(|(key, state)| {
                state
                    .take_due_rebirth_request(now)
                    .map(|gap| ReorderRequest {
                        key: key.clone(),
                        gap,
                    })
            })
            .collect()
    }
}

#[derive(Debug, Clone, Default)]
struct EdgeNodeReorderState {
    last_in_order: Option<u8>,
    pending_gap: Option<PendingGap>,
}

impl EdgeNodeReorderState {
    fn reset(&mut self, sequence: u8) {
        self.last_in_order = Some(sequence);
        self.pending_gap = None;
    }

    fn observe(&mut self, observed: u8, now: Instant, timeout: Duration) -> Option<ReorderEvent> {
        let Some(last_in_order) = self.last_in_order else {
            self.reset(observed);
            return None;
        };

        let distance = forward_distance(last_in_order, observed);
        if distance == 0 || !is_forward_progress(distance) {
            return None;
        }

        if let Some(pending_gap) = &mut self.pending_gap {
            let max_distance = forward_distance(last_in_order, pending_gap.gap.observed);
            if distance > max_distance {
                insert_missing_sequences(
                    next_sequence(pending_gap.gap.observed),
                    observed,
                    &mut pending_gap.missing,
                );
                pending_gap.gap.observed = observed;
                return None;
            }

            if !pending_gap.missing.remove(&observed) {
                return None;
            }

            if pending_gap.missing.is_empty() {
                let gap = pending_gap.gap.clone();
                self.last_in_order = Some(gap.observed);
                self.pending_gap = None;
                return Some(ReorderEvent::GapResolved(gap));
            }

            return None;
        }

        if distance == 1 {
            self.last_in_order = Some(observed);
            return None;
        }

        let expected = next_sequence(last_in_order);
        let mut missing = BTreeSet::new();
        insert_missing_sequences(expected, observed, &mut missing);

        let gap = SequenceGap { expected, observed };
        self.pending_gap = Some(PendingGap {
            gap: gap.clone(),
            missing,
            deadline: now + timeout,
            rebirth_requested: false,
        });

        Some(ReorderEvent::GapDetected(gap))
    }

    fn next_deadline(&self) -> Option<Instant> {
        self.pending_gap
            .as_ref()
            .filter(|pending_gap| !pending_gap.rebirth_requested)
            .map(|pending_gap| pending_gap.deadline)
    }

    fn take_due_rebirth_request(&mut self, now: Instant) -> Option<SequenceGap> {
        let pending_gap = self.pending_gap.as_mut()?;
        if pending_gap.rebirth_requested || pending_gap.deadline > now {
            return None;
        }

        pending_gap.rebirth_requested = true;
        Some(pending_gap.gap.clone())
    }
}

#[derive(Debug, Clone)]
struct PendingGap {
    gap: SequenceGap,
    missing: BTreeSet<u8>,
    deadline: Instant,
    rebirth_requested: bool,
}

fn next_sequence(previous: u8) -> u8 {
    previous.wrapping_add(1)
}

fn forward_distance(from: u8, to: u8) -> u16 {
    to.wrapping_sub(from) as u16
}

fn is_forward_progress(distance: u16) -> bool {
    (1..=127).contains(&distance)
}

fn insert_missing_sequences(start: u8, end_exclusive: u8, missing: &mut BTreeSet<u8>) {
    let mut current = start;
    while current != end_exclusive {
        missing.insert(current);
        current = next_sequence(current);
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::Instant;

    use super::{ReorderEvent, ReorderKey, ReorderTracker, SequenceGap};

    #[test]
    fn resolves_gap_when_missing_messages_arrive_before_timeout() {
        let mut tracker = ReorderTracker::new(Duration::from_secs(2));
        let key = ReorderKey::new("G1", "E1");
        let now = Instant::now();

        tracker.reset(key.clone(), 0);
        assert_eq!(
            tracker.observe(key.clone(), 2, now),
            Some(ReorderEvent::GapDetected(SequenceGap {
                expected: 1,
                observed: 2,
            }))
        );
        assert!(tracker.next_deadline().is_some());
        assert_eq!(
            tracker.observe(key.clone(), 1, now + Duration::from_millis(500)),
            Some(ReorderEvent::GapResolved(SequenceGap {
                expected: 1,
                observed: 2,
            }))
        );
        assert_eq!(tracker.next_deadline(), None);
    }

    #[test]
    fn emits_one_rebirth_request_after_timeout() {
        let timeout = Duration::from_secs(2);
        let mut tracker = ReorderTracker::new(timeout);
        let key = ReorderKey::new("G1", "E1");
        let now = Instant::now();

        tracker.reset(key.clone(), 0);
        tracker.observe(key.clone(), 3, now);

        assert!(
            tracker
                .due_rebirth_requests(now + Duration::from_secs(1))
                .is_empty()
        );

        let requests = tracker.due_rebirth_requests(now + timeout);
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].key, key);
        assert_eq!(
            requests[0].gap,
            SequenceGap {
                expected: 1,
                observed: 3,
            }
        );
        assert!(
            tracker
                .due_rebirth_requests(now + timeout + Duration::from_secs(1))
                .is_empty()
        );
    }
}
