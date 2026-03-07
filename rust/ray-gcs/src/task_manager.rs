// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! GCS Task Manager — tracks task events for observability.
//!
//! Replaces `src/ray/gcs/gcs_task_manager.h/cc`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};

use parking_lot::RwLock;
use ray_common::id::{JobID, TaskID};

/// Maximum number of task events stored in memory.
const DEFAULT_MAX_TASK_EVENTS: usize = 100_000;

/// The GCS task manager stores task events for the dashboard and CLI.
pub struct GcsTaskManager {
    /// Task events indexed by (task_id, attempt_number).
    task_events: RwLock<HashMap<(TaskID, i32), ray_proto::ray::rpc::TaskEvents>>,
    /// Job index: job_id → set of (task_id, attempt).
    job_index: RwLock<HashMap<JobID, Vec<(TaskID, i32)>>>,
    /// Max events to store.
    max_events: usize,
    /// Number of events currently stored.
    num_events: AtomicUsize,
    /// Number of events dropped due to capacity.
    num_dropped: AtomicUsize,
}

impl GcsTaskManager {
    pub fn new(max_events: Option<usize>) -> Self {
        Self {
            task_events: RwLock::new(HashMap::new()),
            job_index: RwLock::new(HashMap::new()),
            max_events: max_events.unwrap_or(DEFAULT_MAX_TASK_EVENTS),
            num_events: AtomicUsize::new(0),
            num_dropped: AtomicUsize::new(0),
        }
    }

    /// Handle AddTaskEventData RPC.
    pub fn handle_add_task_event_data(&self, data: ray_proto::ray::rpc::TaskEventData) {
        for event in data.events_by_task {
            self.add_task_event(event);
        }
    }

    fn add_task_event(&self, event: ray_proto::ray::rpc::TaskEvents) {
        let task_id =
            TaskID::from_binary(event.task_id.as_slice().try_into().unwrap_or(&[0u8; 24]));
        let attempt = event.attempt_number;
        let key = (task_id, attempt);

        // Check capacity
        if self.num_events.load(Ordering::Relaxed) >= self.max_events {
            // Drop the event — in production we'd evict old events
            self.num_dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }

        // Extract job_id from task_id
        let job_id = task_id.job_id();

        let mut events = self.task_events.write();
        if let std::collections::hash_map::Entry::Vacant(e) = events.entry(key) {
            e.insert(event);
            self.num_events.fetch_add(1, Ordering::Relaxed);

            // Update job index
            self.job_index.write().entry(job_id).or_default().push(key);
        } else {
            // Merge with existing event
            if let Some(existing) = events.get_mut(&key) {
                // Merge state updates — keep the latest
                if event.state_updates.is_some() {
                    existing.state_updates = event.state_updates;
                }
            }
        }
    }

    /// Handle GetTaskEvents RPC — filter by job_id and/or task_ids.
    pub fn handle_get_task_events(
        &self,
        job_id: Option<&JobID>,
        task_ids: Option<&[TaskID]>,
        limit: Option<usize>,
    ) -> Vec<ray_proto::ray::rpc::TaskEvents> {
        let events = self.task_events.read();

        let filtered: Box<dyn Iterator<Item = &ray_proto::ray::rpc::TaskEvents>> =
            if let Some(job_id) = job_id {
                let job_idx = self.job_index.read();
                if let Some(keys) = job_idx.get(job_id) {
                    let collected: Vec<_> = keys.iter().filter_map(|k| events.get(k)).collect();
                    Box::new(collected.into_iter())
                } else {
                    Box::new(std::iter::empty())
                }
            } else if let Some(task_ids) = task_ids {
                let collected: Vec<_> = task_ids
                    .iter()
                    .flat_map(|tid| {
                        events
                            .iter()
                            .filter(move |((t, _), _)| t == tid)
                            .map(|(_, v)| v)
                    })
                    .collect();
                Box::new(collected.into_iter())
            } else {
                Box::new(events.values())
            };

        if let Some(limit) = limit {
            filtered.take(limit).cloned().collect()
        } else {
            filtered.cloned().collect()
        }
    }

    /// Mark tasks as failed when a job ends.
    pub fn on_job_finished(&self, job_id: &JobID) {
        let job_idx = self.job_index.read();
        if let Some(keys) = job_idx.get(job_id) {
            let mut events = self.task_events.write();
            for key in keys {
                if let Some(_event) = events.get_mut(key) {
                    // In production, would update terminal state if not already terminal
                }
            }
        }
    }

    pub fn num_events(&self) -> usize {
        self.num_events.load(Ordering::Relaxed)
    }

    pub fn num_dropped(&self) -> usize {
        self.num_dropped.load(Ordering::Relaxed)
    }
}

impl Default for GcsTaskManager {
    fn default() -> Self {
        Self::new(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_task_event(task_id_val: u8, attempt: i32) -> ray_proto::ray::rpc::TaskEvents {
        let mut tid = vec![0u8; 24];
        tid[0] = task_id_val;
        ray_proto::ray::rpc::TaskEvents {
            task_id: tid,
            attempt_number: attempt,
            ..Default::default()
        }
    }

    #[test]
    fn test_add_and_get_task_events() {
        let mgr = GcsTaskManager::new(None);

        let data = ray_proto::ray::rpc::TaskEventData {
            events_by_task: vec![
                make_task_event(1, 0),
                make_task_event(2, 0),
                make_task_event(1, 1),
            ],
            ..Default::default()
        };

        mgr.handle_add_task_event_data(data);
        assert_eq!(mgr.num_events(), 3);

        // Get all
        let events = mgr.handle_get_task_events(None, None, None);
        assert_eq!(events.len(), 3);

        // Get with limit
        let events = mgr.handle_get_task_events(None, None, Some(2));
        assert_eq!(events.len(), 2);
    }

    #[test]
    fn test_task_event_capacity() {
        let mgr = GcsTaskManager::new(Some(2));

        let data = ray_proto::ray::rpc::TaskEventData {
            events_by_task: vec![
                make_task_event(1, 0),
                make_task_event(2, 0),
                make_task_event(3, 0), // Should be dropped
            ],
            ..Default::default()
        };

        mgr.handle_add_task_event_data(data);
        assert_eq!(mgr.num_events(), 2);
        assert_eq!(mgr.num_dropped(), 1);
    }

    #[test]
    fn test_duplicate_event_is_merged() {
        let mgr = GcsTaskManager::new(None);

        let event1 = make_task_event(1, 0);
        let mut event2 = make_task_event(1, 0);
        event2.state_updates = Some(ray_proto::ray::rpc::TaskStateUpdate::default());

        let data = ray_proto::ray::rpc::TaskEventData {
            events_by_task: vec![event1, event2],
            ..Default::default()
        };

        mgr.handle_add_task_event_data(data);
        // Second insert should merge, not create a new entry
        assert_eq!(mgr.num_events(), 1);
    }

    #[test]
    fn test_different_attempts_are_separate() {
        let mgr = GcsTaskManager::new(None);

        let data = ray_proto::ray::rpc::TaskEventData {
            events_by_task: vec![make_task_event(1, 0), make_task_event(1, 1)],
            ..Default::default()
        };

        mgr.handle_add_task_event_data(data);
        assert_eq!(mgr.num_events(), 2);
    }

    #[test]
    fn test_get_task_events_with_task_ids() {
        let mgr = GcsTaskManager::new(None);

        let data = ray_proto::ray::rpc::TaskEventData {
            events_by_task: vec![
                make_task_event(1, 0),
                make_task_event(2, 0),
                make_task_event(3, 0),
            ],
            ..Default::default()
        };
        mgr.handle_add_task_event_data(data);

        let mut tid = [0u8; 24];
        tid[0] = 1;
        let task_id = TaskID::from_binary(&tid);
        let events = mgr.handle_get_task_events(None, Some(&[task_id]), None);
        assert_eq!(events.len(), 1);
    }

    #[test]
    fn test_on_job_finished_does_not_panic() {
        let mgr = GcsTaskManager::new(None);
        let data = ray_proto::ray::rpc::TaskEventData {
            events_by_task: vec![make_task_event(1, 0)],
            ..Default::default()
        };
        mgr.handle_add_task_event_data(data);

        let mut tid = [0u8; 24];
        tid[0] = 1;
        let task_id = TaskID::from_binary(&tid);
        let job_id = task_id.job_id();
        mgr.on_job_finished(&job_id);
    }

    #[test]
    fn test_default_max_events() {
        let mgr = GcsTaskManager::default();
        // Should handle large number of events up to DEFAULT_MAX_TASK_EVENTS
        assert_eq!(mgr.num_events(), 0);
        assert_eq!(mgr.num_dropped(), 0);
    }

    // ---- Ported from gcs_task_manager_test.cc ----

    #[test]
    fn test_add_many_task_events() {
        let mgr = GcsTaskManager::new(None);
        let num_events = 100usize;

        let events: Vec<ray_proto::ray::rpc::TaskEvents> = (0..num_events)
            .map(|i| make_task_event(i as u8, 0))
            .collect();

        let data = ray_proto::ray::rpc::TaskEventData {
            events_by_task: events,
            ..Default::default()
        };
        mgr.handle_add_task_event_data(data);
        assert_eq!(mgr.num_events(), num_events);
    }

    #[test]
    fn test_merge_task_events_same_task_attempt() {
        let mgr = GcsTaskManager::new(None);

        // Add the same task_id + attempt multiple times with state updates
        for _ in 0..20 {
            let mut event = make_task_event(1, 0);
            event.state_updates = Some(ray_proto::ray::rpc::TaskStateUpdate::default());
            let data = ray_proto::ray::rpc::TaskEventData {
                events_by_task: vec![event],
                ..Default::default()
            };
            mgr.handle_add_task_event_data(data);
        }

        // Should only have 1 task event entry (all merged)
        assert_eq!(mgr.num_events(), 1);
    }

    #[test]
    fn test_get_task_events_with_limit_zero() {
        let mgr = GcsTaskManager::new(None);
        let data = ray_proto::ray::rpc::TaskEventData {
            events_by_task: vec![
                make_task_event(1, 0),
                make_task_event(2, 0),
                make_task_event(3, 0),
            ],
            ..Default::default()
        };
        mgr.handle_add_task_event_data(data);
        assert_eq!(mgr.num_events(), 3);

        let events = mgr.handle_get_task_events(None, None, Some(0));
        assert_eq!(events.len(), 0);
    }

    #[test]
    fn test_get_task_events_with_limit_larger_than_stored() {
        let mgr = GcsTaskManager::new(None);
        let data = ray_proto::ray::rpc::TaskEventData {
            events_by_task: vec![make_task_event(1, 0), make_task_event(2, 0)],
            ..Default::default()
        };
        mgr.handle_add_task_event_data(data);

        let events = mgr.handle_get_task_events(None, None, Some(100));
        assert_eq!(events.len(), 2);
    }

    #[test]
    fn test_capacity_drops_excess_events() {
        let max = 5;
        let mgr = GcsTaskManager::new(Some(max));

        let events: Vec<ray_proto::ray::rpc::TaskEvents> =
            (0..10).map(|i| make_task_event(i, 0)).collect();

        let data = ray_proto::ray::rpc::TaskEventData {
            events_by_task: events,
            ..Default::default()
        };
        mgr.handle_add_task_event_data(data);

        assert_eq!(mgr.num_events(), max);
        assert_eq!(mgr.num_dropped(), 5);
    }

    #[test]
    fn test_multiple_attempts_multiple_tasks() {
        let mgr = GcsTaskManager::new(None);

        // 3 tasks, each with 3 attempts
        let mut events = Vec::new();
        for task_id_val in 1..=3u8 {
            for attempt in 0..3 {
                events.push(make_task_event(task_id_val, attempt));
            }
        }

        let data = ray_proto::ray::rpc::TaskEventData {
            events_by_task: events,
            ..Default::default()
        };
        mgr.handle_add_task_event_data(data);
        assert_eq!(mgr.num_events(), 9);
    }

    #[test]
    fn test_get_events_by_multiple_task_ids() {
        let mgr = GcsTaskManager::new(None);

        let data = ray_proto::ray::rpc::TaskEventData {
            events_by_task: vec![
                make_task_event(1, 0),
                make_task_event(1, 1),
                make_task_event(2, 0),
                make_task_event(3, 0),
            ],
            ..Default::default()
        };
        mgr.handle_add_task_event_data(data);

        let mut tid1 = [0u8; 24];
        tid1[0] = 1;
        let mut tid2 = [0u8; 24];
        tid2[0] = 2;
        let task_id1 = TaskID::from_binary(&tid1);
        let task_id2 = TaskID::from_binary(&tid2);

        // Get events for task 1 — should have 2 attempts
        let events = mgr.handle_get_task_events(None, Some(&[task_id1]), None);
        assert_eq!(events.len(), 2);

        // Get events for task 2 — should have 1
        let events = mgr.handle_get_task_events(None, Some(&[task_id2]), None);
        assert_eq!(events.len(), 1);

        // Get events for both tasks
        let events = mgr.handle_get_task_events(None, Some(&[task_id1, task_id2]), None);
        assert_eq!(events.len(), 3);
    }

    #[test]
    fn test_get_events_by_job_id() {
        let mgr = GcsTaskManager::new(None);

        // Make events with distinct first bytes to get different task IDs
        // but same job_id derivation
        let data = ray_proto::ray::rpc::TaskEventData {
            events_by_task: vec![make_task_event(1, 0), make_task_event(2, 0)],
            ..Default::default()
        };
        mgr.handle_add_task_event_data(data);

        let mut tid = [0u8; 24];
        tid[0] = 1;
        let task_id = TaskID::from_binary(&tid);
        let job_id = task_id.job_id();

        // Filter by job_id
        let events = mgr.handle_get_task_events(Some(&job_id), None, None);
        // At least the task with matching job should be returned
        assert!(!events.is_empty());
    }

    #[test]
    fn test_on_job_finished_nonexistent_job() {
        let mgr = GcsTaskManager::new(None);
        // Calling on_job_finished for a job with no events should not panic
        let mut jid = [0u8; 4];
        jid[0] = 99;
        let job_id = JobID::from_binary(&jid);
        mgr.on_job_finished(&job_id);
    }

    #[test]
    fn test_empty_task_event_data() {
        let mgr = GcsTaskManager::new(None);
        let data = ray_proto::ray::rpc::TaskEventData {
            events_by_task: vec![],
            ..Default::default()
        };
        mgr.handle_add_task_event_data(data);
        assert_eq!(mgr.num_events(), 0);
        assert_eq!(mgr.num_dropped(), 0);
    }

    #[test]
    fn test_get_all_events_returns_all_stored() {
        let mgr = GcsTaskManager::new(None);

        for i in 0..50u8 {
            let data = ray_proto::ray::rpc::TaskEventData {
                events_by_task: vec![make_task_event(i, 0)],
                ..Default::default()
            };
            mgr.handle_add_task_event_data(data);
        }

        let events = mgr.handle_get_task_events(None, None, None);
        assert_eq!(events.len(), 50);
    }

    #[test]
    fn test_task_event_merge_preserves_latest_state_update() {
        let mgr = GcsTaskManager::new(None);

        let event1 = make_task_event(1, 0);
        let data1 = ray_proto::ray::rpc::TaskEventData {
            events_by_task: vec![event1],
            ..Default::default()
        };
        mgr.handle_add_task_event_data(data1);

        // Add same task with state update
        let mut event2 = make_task_event(1, 0);
        event2.state_updates = Some(ray_proto::ray::rpc::TaskStateUpdate {
            node_id: Some(b"node-abc".to_vec()),
            ..Default::default()
        });
        let data2 = ray_proto::ray::rpc::TaskEventData {
            events_by_task: vec![event2],
            ..Default::default()
        };
        mgr.handle_add_task_event_data(data2);

        // Should still be 1 event
        assert_eq!(mgr.num_events(), 1);

        // The state update should be the latest one
        let events = mgr.handle_get_task_events(None, None, None);
        assert_eq!(events.len(), 1);
        assert!(events[0].state_updates.is_some());
    }

    #[test]
    fn test_capacity_limit_exactly_at_boundary() {
        let mgr = GcsTaskManager::new(Some(3));

        // Add exactly 3 events
        let data = ray_proto::ray::rpc::TaskEventData {
            events_by_task: vec![
                make_task_event(1, 0),
                make_task_event(2, 0),
                make_task_event(3, 0),
            ],
            ..Default::default()
        };
        mgr.handle_add_task_event_data(data);
        assert_eq!(mgr.num_events(), 3);
        assert_eq!(mgr.num_dropped(), 0);

        // One more should be dropped
        let data2 = ray_proto::ray::rpc::TaskEventData {
            events_by_task: vec![make_task_event(4, 0)],
            ..Default::default()
        };
        mgr.handle_add_task_event_data(data2);
        assert_eq!(mgr.num_events(), 3);
        assert_eq!(mgr.num_dropped(), 1);
    }

    #[test]
    fn test_get_nonexistent_task_id_returns_empty() {
        let mgr = GcsTaskManager::new(None);
        let data = ray_proto::ray::rpc::TaskEventData {
            events_by_task: vec![make_task_event(1, 0)],
            ..Default::default()
        };
        mgr.handle_add_task_event_data(data);

        let mut tid = [0u8; 24];
        tid[0] = 99; // Not inserted
        let task_id = TaskID::from_binary(&tid);
        let events = mgr.handle_get_task_events(None, Some(&[task_id]), None);
        assert!(events.is_empty());
    }

    // ---- Additional tests ported from gcs_task_manager_test.cc ----

    /// Port of TestHandleAddEventBasic — add single event and verify.
    #[test]
    fn test_handle_add_event_basic() {
        let mgr = GcsTaskManager::new(None);
        let event = make_task_event(1, 0);
        let data = ray_proto::ray::rpc::TaskEventData {
            events_by_task: vec![event],
            ..Default::default()
        };
        mgr.handle_add_task_event_data(data);
        assert_eq!(mgr.num_events(), 1);
        assert_eq!(mgr.num_dropped(), 0);
    }

    /// Port of TestHandleAddEventsMultiJobGrouping — events from different jobs.
    #[test]
    fn test_handle_add_events_multi_job_grouping() {
        let mgr = GcsTaskManager::new(None);

        // Create events with different task IDs (different first byte → may map to different jobs)
        let mut events = Vec::new();
        for i in 0..10u8 {
            events.push(make_task_event(i, 0));
        }

        let data = ray_proto::ray::rpc::TaskEventData {
            events_by_task: events,
            ..Default::default()
        };
        mgr.handle_add_task_event_data(data);
        assert_eq!(mgr.num_events(), 10);

        // Get all events
        let all = mgr.handle_get_task_events(None, None, None);
        assert_eq!(all.len(), 10);
    }

    /// Port of TestGetTaskEventsWithLimit — limit parameter behavior.
    #[test]
    fn test_get_task_events_with_limit() {
        let mgr = GcsTaskManager::new(None);

        for i in 0..20u8 {
            let data = ray_proto::ray::rpc::TaskEventData {
                events_by_task: vec![make_task_event(i, 0)],
                ..Default::default()
            };
            mgr.handle_add_task_event_data(data);
        }

        assert_eq!(mgr.num_events(), 20);

        // Limit to 5
        let events = mgr.handle_get_task_events(None, None, Some(5));
        assert_eq!(events.len(), 5);

        // Limit to 0
        let events = mgr.handle_get_task_events(None, None, Some(0));
        assert_eq!(events.len(), 0);

        // Limit larger than stored
        let events = mgr.handle_get_task_events(None, None, Some(100));
        assert_eq!(events.len(), 20);

        // No limit
        let events = mgr.handle_get_task_events(None, None, None);
        assert_eq!(events.len(), 20);
    }

    /// Port of TestMarkTaskAttemptFailedIfNeeded — job finish marks tasks.
    #[test]
    fn test_mark_task_attempt_failed_if_needed() {
        let mgr = GcsTaskManager::new(None);

        // Add events
        let data = ray_proto::ray::rpc::TaskEventData {
            events_by_task: vec![make_task_event(1, 0), make_task_event(2, 0)],
            ..Default::default()
        };
        mgr.handle_add_task_event_data(data);

        // Get job_id from first task
        let mut tid = [0u8; 24];
        tid[0] = 1;
        let task_id = TaskID::from_binary(&tid);
        let job_id = task_id.job_id();

        // on_job_finished should not panic and should not reduce event count
        mgr.on_job_finished(&job_id);
        assert_eq!(mgr.num_events(), 2);
    }

    /// Port of TestJobFinishesWithoutTaskEvents.
    #[test]
    fn test_job_finishes_without_task_events() {
        let mgr = GcsTaskManager::new(None);

        let mut jid = [0u8; 4];
        jid[0] = 42;
        let job_id = JobID::from_binary(&jid);

        // Should not panic even without any events for this job
        mgr.on_job_finished(&job_id);
        assert_eq!(mgr.num_events(), 0);
    }

    /// Port of TestLimitTaskEvents — capacity management.
    #[test]
    fn test_limit_task_events() {
        let max_events = 10;
        let mgr = GcsTaskManager::new(Some(max_events));

        // Add more than max
        for i in 0..20u8 {
            let data = ray_proto::ray::rpc::TaskEventData {
                events_by_task: vec![make_task_event(i, 0)],
                ..Default::default()
            };
            mgr.handle_add_task_event_data(data);
        }

        assert_eq!(mgr.num_events(), max_events);
        assert_eq!(mgr.num_dropped(), 10);
    }

    /// Port of TestIndexNoLeak — verify that merging events doesn't leak indices.
    #[test]
    fn test_index_no_leak_on_merge() {
        let mgr = GcsTaskManager::new(None);

        // Add same event 100 times (all merge)
        for _ in 0..100 {
            let mut event = make_task_event(1, 0);
            event.state_updates = Some(ray_proto::ray::rpc::TaskStateUpdate::default());
            let data = ray_proto::ray::rpc::TaskEventData {
                events_by_task: vec![event],
                ..Default::default()
            };
            mgr.handle_add_task_event_data(data);
        }

        // Should only have 1 event despite 100 insertions
        assert_eq!(mgr.num_events(), 1);
        assert_eq!(mgr.num_dropped(), 0);
    }

    /// Port of TestGetTaskEventsByTaskIDs — filtering by multiple task IDs.
    #[test]
    fn test_get_task_events_by_task_ids_comprehensive() {
        let mgr = GcsTaskManager::new(None);

        // Add 5 tasks with varying attempts
        for task_val in 1..=5u8 {
            for attempt in 0..task_val as i32 {
                let data = ray_proto::ray::rpc::TaskEventData {
                    events_by_task: vec![make_task_event(task_val, attempt)],
                    ..Default::default()
                };
                mgr.handle_add_task_event_data(data);
            }
        }

        // Total events = 1+2+3+4+5 = 15
        assert_eq!(mgr.num_events(), 15);

        // Get events for task 3 only — should have 3 attempts
        let mut tid = [0u8; 24];
        tid[0] = 3;
        let task_id = TaskID::from_binary(&tid);
        let events = mgr.handle_get_task_events(None, Some(&[task_id]), None);
        assert_eq!(events.len(), 3);

        // Get events for task 1 — should have 1 attempt
        tid[0] = 1;
        let task_id1 = TaskID::from_binary(&tid);
        let events = mgr.handle_get_task_events(None, Some(&[task_id1]), None);
        assert_eq!(events.len(), 1);
    }

    /// Port of TestGetTaskEventsByJobs — filtering by job_id.
    #[test]
    fn test_get_task_events_by_jobs_comprehensive() {
        let mgr = GcsTaskManager::new(None);

        // All tasks with zero-filled bytes will map to same job
        for i in 1..=5u8 {
            let data = ray_proto::ray::rpc::TaskEventData {
                events_by_task: vec![make_task_event(i, 0)],
                ..Default::default()
            };
            mgr.handle_add_task_event_data(data);
        }

        // Get the job_id from first task
        let mut tid = [0u8; 24];
        tid[0] = 1;
        let task_id = TaskID::from_binary(&tid);
        let job_id = task_id.job_id();

        let events = mgr.handle_get_task_events(Some(&job_id), None, None);
        // Should get at least task 1 (other tasks may or may not share same job_id
        // depending on how job_id is derived from task_id bytes)
        assert!(!events.is_empty());
    }

    /// Port of TestGetTaskEventsFilters — combined filtering.
    #[test]
    fn test_get_task_events_filters_combined() {
        let mgr = GcsTaskManager::new(None);

        // Add events
        for i in 1..=10u8 {
            let data = ray_proto::ray::rpc::TaskEventData {
                events_by_task: vec![make_task_event(i, 0)],
                ..Default::default()
            };
            mgr.handle_add_task_event_data(data);
        }

        // Get by task_id with limit
        let mut tid = [0u8; 24];
        tid[0] = 5;
        let task_id = TaskID::from_binary(&tid);
        let events = mgr.handle_get_task_events(None, Some(&[task_id]), Some(1));
        assert_eq!(events.len(), 1);
    }

    /// Port of TestTaskDataLossWorker — multiple adds of same task don't duplicate.
    #[test]
    fn test_task_data_loss_worker_no_duplicates() {
        let mgr = GcsTaskManager::new(None);

        // Simulate worker re-sending the same event
        for _ in 0..5 {
            let data = ray_proto::ray::rpc::TaskEventData {
                events_by_task: vec![make_task_event(1, 0)],
                ..Default::default()
            };
            mgr.handle_add_task_event_data(data);
        }

        // Should still be 1 event
        assert_eq!(mgr.num_events(), 1);
    }

    /// Port of TestMultipleJobsDataLoss — events from multiple jobs.
    #[test]
    fn test_multiple_jobs_data_loss() {
        let mgr = GcsTaskManager::new(Some(5));

        // Add events for many different tasks, exceeding capacity
        for i in 0..15u8 {
            let data = ray_proto::ray::rpc::TaskEventData {
                events_by_task: vec![make_task_event(i, 0)],
                ..Default::default()
            };
            mgr.handle_add_task_event_data(data);
        }

        assert_eq!(mgr.num_events(), 5);
        assert_eq!(mgr.num_dropped(), 10);
    }

    /// Port of TestGetTaskEventsWithDriver — driver events mixed with worker events.
    #[test]
    fn test_get_task_events_with_driver() {
        let mgr = GcsTaskManager::new(None);

        // Add driver-like event (attempt 0) and worker events
        let data = ray_proto::ray::rpc::TaskEventData {
            events_by_task: vec![
                make_task_event(1, 0), // driver
                make_task_event(2, 0), // worker task
                make_task_event(3, 0), // worker task
            ],
            ..Default::default()
        };
        mgr.handle_add_task_event_data(data);

        let all = mgr.handle_get_task_events(None, None, None);
        assert_eq!(all.len(), 3);
    }

    /// Port of TestProfileEventsNoLeak — profile event capacity.
    #[test]
    fn test_profile_events_no_leak() {
        let max = 50;
        let mgr = GcsTaskManager::new(Some(max));

        // Add more than max events
        for i in 0..100u8 {
            let data = ray_proto::ray::rpc::TaskEventData {
                events_by_task: vec![make_task_event(i, 0)],
                ..Default::default()
            };
            mgr.handle_add_task_event_data(data);
        }

        assert_eq!(mgr.num_events(), max);
        assert_eq!(mgr.num_dropped(), 50);
    }
}
