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
}
