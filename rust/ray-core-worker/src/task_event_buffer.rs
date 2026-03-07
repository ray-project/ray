// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Task event buffering for reporting task status changes to GCS.
//!
//! Ports `src/ray/core_worker/task_event_buffer.cc`.
//! Buffers task status events and periodically flushes them in batches.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;

use crate::task_manager::TaskState;

/// A task status change event.
#[derive(Debug, Clone)]
pub struct TaskStatusEvent {
    /// The task ID (binary).
    pub task_id: Vec<u8>,
    /// The new task status.
    pub status: TaskState,
    /// The attempt number for this task.
    pub attempt_number: i32,
    /// Timestamp in nanoseconds when the event was recorded.
    pub timestamp_ns: u64,
}

/// Configuration for the task event buffer.
#[derive(Debug, Clone)]
pub struct TaskEventBufferConfig {
    /// Maximum number of status events to buffer.
    pub max_status_events: usize,
    /// Maximum number of events to send per flush.
    pub send_batch_size: usize,
    /// Flush interval in milliseconds.
    pub flush_interval_ms: u64,
}

impl Default for TaskEventBufferConfig {
    fn default() -> Self {
        Self {
            max_status_events: 100_000,
            send_batch_size: 10_000,
            flush_interval_ms: 1000,
        }
    }
}

/// Tracks which task attempts had events dropped.
type TaskAttempt = (Vec<u8>, i32); // (task_id, attempt_number)

/// Buffers task status events and flushes them to GCS.
///
/// Events are stored in a bounded ring buffer. When the buffer is full,
/// the oldest event is evicted and its task attempt is marked as "dropped".
/// Dropped attempts are reported to GCS so it can mark data as incomplete.
pub struct TaskEventBuffer {
    inner: Mutex<TaskEventBufferInner>,
    enabled: AtomicBool,
    total_events_recorded: AtomicU64,
    total_events_dropped: AtomicU64,
    total_events_flushed: AtomicU64,
}

struct TaskEventBufferInner {
    /// Bounded ring buffer of status events.
    status_events: VecDeque<TaskStatusEvent>,
    /// Max capacity.
    max_status_events: usize,
    /// Task attempts that had events dropped.
    dropped_task_attempts: std::collections::HashSet<TaskAttempt>,
    /// Flush callback (set when started).
    flush_callback: Option<FlushCallback>,
}

/// Callback invoked during flush with the batch of events and dropped attempts.
pub type FlushCallback = Arc<dyn Fn(&[TaskStatusEvent], &[(Vec<u8>, i32)]) + Send + Sync>;

impl TaskEventBuffer {
    /// Create a new TaskEventBuffer with the given max capacity.
    pub fn new(max_status_events: usize) -> Self {
        Self {
            inner: Mutex::new(TaskEventBufferInner {
                status_events: VecDeque::with_capacity(max_status_events.min(10_000)),
                max_status_events,
                dropped_task_attempts: std::collections::HashSet::new(),
                flush_callback: None,
            }),
            enabled: AtomicBool::new(true),
            total_events_recorded: AtomicU64::new(0),
            total_events_dropped: AtomicU64::new(0),
            total_events_flushed: AtomicU64::new(0),
        }
    }

    /// Create from config.
    pub fn from_config(config: &TaskEventBufferConfig) -> Self {
        Self::new(config.max_status_events)
    }

    /// Record a task status event.
    ///
    /// This is the primary entry point called by TaskManager on every
    /// status transition.
    pub fn record_task_status_event(&self, task_id: &[u8], status: TaskState, attempt_number: i32) {
        if !self.enabled.load(Ordering::Relaxed) {
            return;
        }

        let event = TaskStatusEvent {
            task_id: task_id.to_vec(),
            status,
            attempt_number,
            timestamp_ns: current_time_nanos(),
        };

        let mut inner = self.inner.lock();

        // Check if this attempt is already dropped — skip if so.
        let attempt_key = (task_id.to_vec(), attempt_number);
        if inner.dropped_task_attempts.contains(&attempt_key) {
            self.total_events_dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }

        // Evict oldest event if buffer is full.
        if inner.status_events.len() >= inner.max_status_events {
            if let Some(evicted) = inner.status_events.pop_front() {
                inner
                    .dropped_task_attempts
                    .insert((evicted.task_id.clone(), evicted.attempt_number));
                self.total_events_dropped.fetch_add(1, Ordering::Relaxed);
            }
        }

        inner.status_events.push_back(event);
        self.total_events_recorded.fetch_add(1, Ordering::Relaxed);
    }

    /// Set the flush callback. This is invoked during `flush_events()`.
    pub fn set_flush_callback(&self, callback: FlushCallback) {
        self.inner.lock().flush_callback = Some(callback);
    }

    /// Flush buffered events.
    ///
    /// Drains up to `batch_size` events from the buffer and invokes
    /// the flush callback with the batch and any dropped task attempts.
    pub fn flush_events(&self, batch_size: usize) -> Vec<TaskStatusEvent> {
        let (events, dropped, callback);
        {
            let mut inner = self.inner.lock();
            let count = batch_size.min(inner.status_events.len());
            events = inner.status_events.drain(..count).collect::<Vec<_>>();
            dropped = inner.dropped_task_attempts.drain().collect::<Vec<_>>();
            callback = inner.flush_callback.clone();
        }

        if let Some(cb) = callback {
            if !events.is_empty() || !dropped.is_empty() {
                cb(&events, &dropped);
            }
        }

        self.total_events_flushed
            .fetch_add(events.len() as u64, Ordering::Relaxed);
        events
    }

    /// Enable or disable event recording.
    pub fn set_enabled(&self, enabled: bool) {
        self.enabled.store(enabled, Ordering::Relaxed);
    }

    /// Whether event recording is enabled.
    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    /// Number of events currently buffered.
    pub fn num_buffered_events(&self) -> usize {
        self.inner.lock().status_events.len()
    }

    /// Number of dropped task attempts tracked.
    pub fn num_dropped_task_attempts(&self) -> usize {
        self.inner.lock().dropped_task_attempts.len()
    }

    /// Total events recorded since creation.
    pub fn total_events_recorded(&self) -> u64 {
        self.total_events_recorded.load(Ordering::Relaxed)
    }

    /// Total events dropped since creation.
    pub fn total_events_dropped(&self) -> u64 {
        self.total_events_dropped.load(Ordering::Relaxed)
    }

    /// Total events flushed since creation.
    pub fn total_events_flushed(&self) -> u64 {
        self.total_events_flushed.load(Ordering::Relaxed)
    }

    /// Start periodic flushing with the given interval and batch size.
    ///
    /// Returns a handle that can be dropped to stop flushing.
    pub fn start_periodic_flush(
        self: &Arc<Self>,
        interval_ms: u64,
        batch_size: usize,
    ) -> PeriodicFlushHandle {
        let buffer = Arc::clone(self);
        let stop = Arc::new(AtomicBool::new(false));
        let stop_clone = stop.clone();

        let handle = tokio::spawn(async move {
            let interval = std::time::Duration::from_millis(interval_ms);
            loop {
                tokio::time::sleep(interval).await;
                if stop_clone.load(Ordering::Relaxed) {
                    break;
                }
                buffer.flush_events(batch_size);
            }
        });

        PeriodicFlushHandle {
            stop,
            _handle: handle,
        }
    }
}

/// Handle for stopping periodic flush. Flushing stops when dropped.
pub struct PeriodicFlushHandle {
    stop: Arc<AtomicBool>,
    _handle: tokio::task::JoinHandle<()>,
}

impl PeriodicFlushHandle {
    /// Stop periodic flushing.
    pub fn stop(&self) {
        self.stop.store(true, Ordering::Relaxed);
    }
}

impl Drop for PeriodicFlushHandle {
    fn drop(&mut self) {
        self.stop();
    }
}

fn current_time_nanos() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_task_id(val: u8) -> Vec<u8> {
        let mut id = vec![0u8; 28];
        id[0] = val;
        id
    }

    #[test]
    fn test_record_and_flush() {
        let buffer = TaskEventBuffer::new(100);
        buffer.record_task_status_event(&make_task_id(1), TaskState::PendingArgsAvail, 0);
        buffer.record_task_status_event(&make_task_id(2), TaskState::SubmittedToWorker, 0);

        assert_eq!(buffer.num_buffered_events(), 2);
        assert_eq!(buffer.total_events_recorded(), 2);

        let events = buffer.flush_events(10);
        assert_eq!(events.len(), 2);
        assert_eq!(buffer.num_buffered_events(), 0);
        assert_eq!(buffer.total_events_flushed(), 2);
    }

    #[test]
    fn test_bounded_buffer_eviction() {
        let buffer = TaskEventBuffer::new(3);

        for i in 0..5u8 {
            buffer.record_task_status_event(&make_task_id(i), TaskState::PendingArgsAvail, 0);
        }

        // Buffer should have 3 events (the last 3 added).
        assert_eq!(buffer.num_buffered_events(), 3);
        // 2 events should have been dropped (first 2 evicted).
        assert_eq!(buffer.total_events_dropped(), 2);
        // 2 task attempts should be tracked as dropped.
        assert_eq!(buffer.num_dropped_task_attempts(), 2);
    }

    #[test]
    fn test_dropped_attempt_skipped() {
        let buffer = TaskEventBuffer::new(2);
        let tid = make_task_id(1);

        // Fill buffer with events from different tasks.
        buffer.record_task_status_event(&tid, TaskState::PendingArgsAvail, 0);
        buffer.record_task_status_event(&make_task_id(2), TaskState::PendingArgsAvail, 0);
        // This push evicts task_id=1 attempt 0.
        buffer.record_task_status_event(&make_task_id(3), TaskState::PendingArgsAvail, 0);

        // Now try to record another event for the dropped task attempt.
        buffer.record_task_status_event(&tid, TaskState::Finished, 0);

        // Should have 2 events (not 3), because the dropped attempt was skipped.
        assert_eq!(buffer.num_buffered_events(), 2);
        // The extra event should be counted as dropped.
        assert_eq!(buffer.total_events_dropped(), 2); // 1 eviction + 1 skip
    }

    #[test]
    fn test_disabled_buffer() {
        let buffer = TaskEventBuffer::new(100);
        buffer.set_enabled(false);

        buffer.record_task_status_event(&make_task_id(1), TaskState::PendingArgsAvail, 0);
        assert_eq!(buffer.num_buffered_events(), 0);
        assert_eq!(buffer.total_events_recorded(), 0);
    }

    #[test]
    fn test_flush_callback() {
        let buffer = TaskEventBuffer::new(100);
        let flushed_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let flushed_clone = flushed_count.clone();

        buffer.set_flush_callback(Arc::new(move |events, _dropped| {
            flushed_clone.fetch_add(events.len(), Ordering::Relaxed);
        }));

        buffer.record_task_status_event(&make_task_id(1), TaskState::PendingArgsAvail, 0);
        buffer.record_task_status_event(&make_task_id(2), TaskState::Finished, 0);

        buffer.flush_events(100);
        assert_eq!(flushed_count.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn test_partial_flush() {
        let buffer = TaskEventBuffer::new(100);
        for i in 0..10u8 {
            buffer.record_task_status_event(&make_task_id(i), TaskState::PendingArgsAvail, 0);
        }

        // Flush only 3 events.
        let events = buffer.flush_events(3);
        assert_eq!(events.len(), 3);
        assert_eq!(buffer.num_buffered_events(), 7);

        // Flush the rest.
        let events = buffer.flush_events(100);
        assert_eq!(events.len(), 7);
        assert_eq!(buffer.num_buffered_events(), 0);
    }

    #[test]
    fn test_event_timestamps() {
        let buffer = TaskEventBuffer::new(100);
        let before = current_time_nanos();
        buffer.record_task_status_event(&make_task_id(1), TaskState::PendingArgsAvail, 0);
        let after = current_time_nanos();

        let events = buffer.flush_events(1);
        assert!(events[0].timestamp_ns >= before);
        assert!(events[0].timestamp_ns <= after);
    }

    #[test]
    fn test_flush_cleared_dropped_attempts() {
        let buffer = TaskEventBuffer::new(2);
        // Fill and evict to create dropped attempts.
        for i in 0..4u8 {
            buffer.record_task_status_event(&make_task_id(i), TaskState::PendingArgsAvail, 0);
        }
        assert!(buffer.num_dropped_task_attempts() > 0);

        // Flush should clear dropped attempts.
        buffer.flush_events(100);
        assert_eq!(buffer.num_dropped_task_attempts(), 0);
    }

    #[tokio::test]
    async fn test_periodic_flush() {
        let buffer = Arc::new(TaskEventBuffer::new(100));
        let flushed_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let flushed_clone = flushed_count.clone();

        buffer.set_flush_callback(Arc::new(move |events, _| {
            flushed_clone.fetch_add(events.len(), Ordering::Relaxed);
        }));

        buffer.record_task_status_event(&vec![0u8; 28], TaskState::PendingArgsAvail, 0);

        let handle = buffer.start_periodic_flush(50, 100);
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        handle.stop();

        assert!(flushed_count.load(Ordering::Relaxed) >= 1);
        assert_eq!(buffer.num_buffered_events(), 0);
    }
}
