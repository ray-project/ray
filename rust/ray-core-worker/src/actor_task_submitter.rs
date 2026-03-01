// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Actor task submission with per-actor queuing and delivery.
//!
//! Replaces `src/ray/core_worker/transport/actor_task_submitter.h/cc`.
//!
//! Manages per-actor task queues with monotonic sequence numbers.
//! Tasks are enqueued and, when a send callback is set, delivered
//! in order. Handles actor death, reconnection, and task cancellation.

use std::collections::{HashMap, VecDeque};

use parking_lot::Mutex;

use ray_common::id::ActorID;
use ray_proto::ray::rpc::{self, TaskSpec};

use crate::error::{CoreWorkerError, CoreWorkerResult};

/// Callback for sending a task to an actor worker.
/// Receives the task spec and the actor's worker address.
pub type ActorTaskSendCallback =
    Box<dyn Fn(&TaskSpec, &rpc::Address) -> CoreWorkerResult<()> + Send + Sync>;

/// State of an actor as tracked by the submitter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActorState {
    /// Actor is alive and tasks can be sent.
    Alive,
    /// Actor is restarting — tasks are queued but not sent.
    Restarting,
    /// Actor is dead — new tasks are rejected.
    Dead,
}

/// Per-actor pending task queue.
struct ActorQueue {
    pending_tasks: VecDeque<TaskSpec>,
    next_sequence_number: u64,
    state: ActorState,
    /// Worker address of the actor (if known).
    worker_address: Option<rpc::Address>,
    /// Number of tasks successfully sent.
    num_tasks_sent: u64,
}

impl ActorQueue {
    fn new() -> Self {
        Self {
            pending_tasks: VecDeque::new(),
            next_sequence_number: 0,
            state: ActorState::Alive,
            worker_address: None,
            num_tasks_sent: 0,
        }
    }
}

/// Manages actor task submission with per-actor ordering.
pub struct ActorTaskSubmitter {
    queues: Mutex<HashMap<ActorID, ActorQueue>>,
    /// Optional callback for sending tasks to actor workers.
    send_callback: Mutex<Option<ActorTaskSendCallback>>,
}

impl ActorTaskSubmitter {
    pub fn new() -> Self {
        Self {
            queues: Mutex::new(HashMap::new()),
            send_callback: Mutex::new(None),
        }
    }

    /// Set the callback for sending tasks to actor workers.
    pub fn set_send_callback(&self, callback: ActorTaskSendCallback) {
        *self.send_callback.lock() = Some(callback);
    }

    /// Register an actor so tasks can be queued to it.
    pub fn add_actor(&self, actor_id: ActorID) {
        self.queues
            .lock()
            .entry(actor_id)
            .or_insert_with(ActorQueue::new);
    }

    /// Update the worker address for an actor (called when actor is placed on a worker).
    pub fn connect_actor(&self, actor_id: &ActorID, address: rpc::Address) {
        let mut queues = self.queues.lock();
        if let Some(queue) = queues.get_mut(actor_id) {
            queue.worker_address = Some(address);
            queue.state = ActorState::Alive;
        }
    }

    /// Submit a task to an actor.
    ///
    /// The task is assigned a sequence number and enqueued. If the actor
    /// is alive with a known address and a send callback is set, the task
    /// is delivered immediately. Otherwise it waits in the queue.
    pub async fn submit_task(
        &self,
        actor_id: &ActorID,
        task_spec: TaskSpec,
    ) -> CoreWorkerResult<()> {
        let mut queues = self.queues.lock();
        let queue = queues
            .get_mut(actor_id)
            .ok_or_else(|| CoreWorkerError::ActorNotFound(actor_id.hex()))?;

        if queue.state == ActorState::Dead {
            return Err(CoreWorkerError::Internal(format!(
                "actor {} is dead, cannot submit tasks",
                actor_id.hex()
            )));
        }

        let seq = queue.next_sequence_number;
        queue.next_sequence_number += 1;
        queue.pending_tasks.push_back(task_spec);

        tracing::debug!(
            actor_id = %actor_id.hex(),
            seq,
            state = ?queue.state,
            "Actor task enqueued"
        );

        // Try to send pending tasks if actor is alive with a known address
        if queue.state == ActorState::Alive {
            if let Some(ref addr) = queue.worker_address.clone() {
                let callback = self.send_callback.lock();
                if let Some(ref cb) = *callback {
                    self.send_pending_tasks(queue, addr, cb);
                }
            }
        }

        Ok(())
    }

    /// Try to send all pending tasks for an actor queue.
    fn send_pending_tasks(
        &self,
        queue: &mut ActorQueue,
        address: &rpc::Address,
        callback: &ActorTaskSendCallback,
    ) {
        while let Some(task) = queue.pending_tasks.front() {
            match callback(task, address) {
                Ok(()) => {
                    queue.pending_tasks.pop_front();
                    queue.num_tasks_sent += 1;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "Failed to send actor task, will retry");
                    break;
                }
            }
        }
    }

    /// Flush pending tasks for an actor (called when connection is established).
    pub fn flush_actor_tasks(&self, actor_id: &ActorID) {
        let mut queues = self.queues.lock();
        let Some(queue) = queues.get_mut(actor_id) else {
            return;
        };

        if queue.state != ActorState::Alive {
            return;
        }

        let Some(ref addr) = queue.worker_address.clone() else {
            return;
        };

        let callback = self.send_callback.lock();
        if let Some(ref cb) = *callback {
            self.send_pending_tasks(queue, addr, cb);
        }
    }

    /// Cancel the oldest pending task for a specific actor.
    pub fn cancel_task(&self, actor_id: &ActorID) -> CoreWorkerResult<()> {
        let mut queues = self.queues.lock();
        let queue = queues
            .get_mut(actor_id)
            .ok_or_else(|| CoreWorkerError::ActorNotFound(actor_id.hex()))?;

        if queue.pending_tasks.pop_front().is_some() {
            tracing::debug!(actor_id = %actor_id.hex(), "Actor task cancelled");
            Ok(())
        } else {
            Err(CoreWorkerError::Internal(
                "no pending tasks to cancel".into(),
            ))
        }
    }

    /// Mark an actor as restarting — tasks remain queued but aren't sent.
    pub fn mark_actor_restarting(&self, actor_id: &ActorID) {
        let mut queues = self.queues.lock();
        if let Some(queue) = queues.get_mut(actor_id) {
            queue.state = ActorState::Restarting;
            queue.worker_address = None;
            tracing::info!(
                actor_id = %actor_id.hex(),
                pending = queue.pending_tasks.len(),
                "Actor marked as restarting"
            );
        }
    }

    /// Mark an actor as dead — fail all pending tasks.
    pub fn mark_actor_dead(&self, actor_id: &ActorID) {
        let mut queues = self.queues.lock();
        if let Some(queue) = queues.get_mut(actor_id) {
            queue.state = ActorState::Dead;
            queue.worker_address = None;
            let num_dropped = queue.pending_tasks.len();
            queue.pending_tasks.clear();
            tracing::info!(
                actor_id = %actor_id.hex(),
                num_dropped,
                "Actor marked as dead, dropped pending tasks"
            );
        }
    }

    /// Disconnect an actor, clearing its pending task queue and removing it.
    pub fn disconnect_actor(&self, actor_id: &ActorID) {
        self.queues.lock().remove(actor_id);
    }

    /// Number of pending tasks for a given actor.
    pub fn num_pending_tasks(&self, actor_id: &ActorID) -> usize {
        self.queues
            .lock()
            .get(actor_id)
            .map_or(0, |q| q.pending_tasks.len())
    }

    /// Total number of pending tasks across all actors.
    pub fn total_pending_tasks(&self) -> usize {
        self.queues
            .lock()
            .values()
            .map(|q| q.pending_tasks.len())
            .sum()
    }

    /// Get the state of an actor.
    pub fn actor_state(&self, actor_id: &ActorID) -> Option<ActorState> {
        self.queues.lock().get(actor_id).map(|q| q.state)
    }

    /// Number of tasks successfully sent for an actor.
    pub fn num_tasks_sent(&self, actor_id: &ActorID) -> u64 {
        self.queues
            .lock()
            .get(actor_id)
            .map_or(0, |q| q.num_tasks_sent)
    }
}

impl Default for ActorTaskSubmitter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    fn make_actor_id(v: u8) -> ActorID {
        let mut data = [0u8; 16];
        data[0] = v;
        ActorID::from_binary(&data)
    }

    fn make_address() -> rpc::Address {
        rpc::Address {
            node_id: vec![1; 28],
            ip_address: "10.0.0.1".to_string(),
            port: 5000,
            worker_id: vec![2; 28],
        }
    }

    #[tokio::test]
    async fn test_add_actor_and_submit() {
        let submitter = ActorTaskSubmitter::new();
        let actor_id = make_actor_id(1);
        submitter.add_actor(actor_id);

        let task_spec = TaskSpec::default();
        submitter.submit_task(&actor_id, task_spec).await.unwrap();
        assert_eq!(submitter.num_pending_tasks(&actor_id), 1);
    }

    #[tokio::test]
    async fn test_submit_to_unregistered_actor_fails() {
        let submitter = ActorTaskSubmitter::new();
        let actor_id = make_actor_id(99);
        let result = submitter
            .submit_task(&actor_id, TaskSpec::default())
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_multiple_tasks_queued_in_order() {
        let submitter = ActorTaskSubmitter::new();
        let actor_id = make_actor_id(1);
        submitter.add_actor(actor_id);

        for _ in 0..5 {
            submitter
                .submit_task(&actor_id, TaskSpec::default())
                .await
                .unwrap();
        }
        assert_eq!(submitter.num_pending_tasks(&actor_id), 5);
    }

    #[test]
    fn test_disconnect_clears_queue() {
        let submitter = ActorTaskSubmitter::new();
        let actor_id = make_actor_id(1);
        submitter.add_actor(actor_id);
        submitter.disconnect_actor(&actor_id);
        assert_eq!(submitter.num_pending_tasks(&actor_id), 0);
    }

    #[test]
    fn test_cancel_task() {
        let submitter = ActorTaskSubmitter::new();
        let actor_id = make_actor_id(1);
        submitter.add_actor(actor_id);

        // No tasks to cancel
        assert!(submitter.cancel_task(&actor_id).is_err());
    }

    #[test]
    fn test_num_pending_unregistered_actor() {
        let submitter = ActorTaskSubmitter::new();
        let actor_id = make_actor_id(99);
        assert_eq!(submitter.num_pending_tasks(&actor_id), 0);
    }

    #[tokio::test]
    async fn test_add_actor_idempotent() {
        let submitter = ActorTaskSubmitter::new();
        let actor_id = make_actor_id(1);
        submitter.add_actor(actor_id);
        submitter
            .submit_task(&actor_id, TaskSpec::default())
            .await
            .unwrap();
        // Adding again should not clear the queue
        submitter.add_actor(actor_id);
        assert_eq!(submitter.num_pending_tasks(&actor_id), 1);
    }

    #[tokio::test]
    async fn test_submit_to_dead_actor_fails() {
        let submitter = ActorTaskSubmitter::new();
        let actor_id = make_actor_id(1);
        submitter.add_actor(actor_id);
        submitter.mark_actor_dead(&actor_id);

        let result = submitter
            .submit_task(&actor_id, TaskSpec::default())
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_mark_dead_clears_pending() {
        let submitter = ActorTaskSubmitter::new();
        let actor_id = make_actor_id(1);
        submitter.add_actor(actor_id);

        for _ in 0..3 {
            submitter
                .submit_task(&actor_id, TaskSpec::default())
                .await
                .unwrap();
        }
        assert_eq!(submitter.num_pending_tasks(&actor_id), 3);

        submitter.mark_actor_dead(&actor_id);
        assert_eq!(submitter.num_pending_tasks(&actor_id), 0);
        assert_eq!(submitter.actor_state(&actor_id), Some(ActorState::Dead));
    }

    #[tokio::test]
    async fn test_restarting_queues_tasks() {
        let submitter = ActorTaskSubmitter::new();
        let actor_id = make_actor_id(1);
        submitter.add_actor(actor_id);

        submitter.mark_actor_restarting(&actor_id);
        assert_eq!(
            submitter.actor_state(&actor_id),
            Some(ActorState::Restarting)
        );

        // Tasks should be accepted but not sent
        submitter
            .submit_task(&actor_id, TaskSpec::default())
            .await
            .unwrap();
        assert_eq!(submitter.num_pending_tasks(&actor_id), 1);
    }

    #[tokio::test]
    async fn test_connect_and_flush() {
        let submitter = ActorTaskSubmitter::new();
        let actor_id = make_actor_id(1);
        submitter.add_actor(actor_id);

        // Queue 3 tasks without a connection
        for _ in 0..3 {
            submitter
                .submit_task(&actor_id, TaskSpec::default())
                .await
                .unwrap();
        }
        assert_eq!(submitter.num_pending_tasks(&actor_id), 3);

        let sent_count = Arc::new(AtomicU32::new(0));
        let count_clone = sent_count.clone();
        submitter.set_send_callback(Box::new(move |_spec, _addr| {
            count_clone.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }));

        // Connect the actor and flush
        submitter.connect_actor(&actor_id, make_address());
        submitter.flush_actor_tasks(&actor_id);

        assert_eq!(sent_count.load(Ordering::Relaxed), 3);
        assert_eq!(submitter.num_pending_tasks(&actor_id), 0);
        assert_eq!(submitter.num_tasks_sent(&actor_id), 3);
    }

    #[tokio::test]
    async fn test_submit_with_callback_sends_immediately() {
        let submitter = ActorTaskSubmitter::new();
        let actor_id = make_actor_id(1);
        submitter.add_actor(actor_id);
        submitter.connect_actor(&actor_id, make_address());

        let sent_count = Arc::new(AtomicU32::new(0));
        let count_clone = sent_count.clone();
        submitter.set_send_callback(Box::new(move |_spec, _addr| {
            count_clone.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }));

        submitter
            .submit_task(&actor_id, TaskSpec::default())
            .await
            .unwrap();

        // Task should be sent immediately since actor is connected
        assert_eq!(sent_count.load(Ordering::Relaxed), 1);
        assert_eq!(submitter.num_pending_tasks(&actor_id), 0);
    }

    #[tokio::test]
    async fn test_total_pending_tasks() {
        let submitter = ActorTaskSubmitter::new();
        let a1 = make_actor_id(1);
        let a2 = make_actor_id(2);
        submitter.add_actor(a1);
        submitter.add_actor(a2);

        submitter
            .submit_task(&a1, TaskSpec::default())
            .await
            .unwrap();
        submitter
            .submit_task(&a2, TaskSpec::default())
            .await
            .unwrap();
        submitter
            .submit_task(&a2, TaskSpec::default())
            .await
            .unwrap();

        assert_eq!(submitter.total_pending_tasks(), 3);
    }

    #[tokio::test]
    async fn test_cancel_removes_oldest_task() {
        let submitter = ActorTaskSubmitter::new();
        let actor_id = make_actor_id(1);
        submitter.add_actor(actor_id);

        submitter
            .submit_task(&actor_id, TaskSpec::default())
            .await
            .unwrap();
        submitter
            .submit_task(&actor_id, TaskSpec::default())
            .await
            .unwrap();
        assert_eq!(submitter.num_pending_tasks(&actor_id), 2);

        submitter.cancel_task(&actor_id).unwrap();
        assert_eq!(submitter.num_pending_tasks(&actor_id), 1);
    }

    #[tokio::test]
    async fn test_restart_then_reconnect() {
        let submitter = ActorTaskSubmitter::new();
        let actor_id = make_actor_id(1);
        submitter.add_actor(actor_id);
        submitter.connect_actor(&actor_id, make_address());

        // Queue task while alive
        submitter
            .submit_task(&actor_id, TaskSpec::default())
            .await
            .unwrap();

        // Actor restarts — tasks remain queued
        submitter.mark_actor_restarting(&actor_id);
        assert_eq!(submitter.num_pending_tasks(&actor_id), 1);

        // Queue more during restart
        submitter
            .submit_task(&actor_id, TaskSpec::default())
            .await
            .unwrap();
        assert_eq!(submitter.num_pending_tasks(&actor_id), 2);

        // Reconnect and flush
        let sent_count = Arc::new(AtomicU32::new(0));
        let count_clone = sent_count.clone();
        submitter.set_send_callback(Box::new(move |_spec, _addr| {
            count_clone.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }));

        submitter.connect_actor(&actor_id, make_address());
        submitter.flush_actor_tasks(&actor_id);

        assert_eq!(sent_count.load(Ordering::Relaxed), 2);
        assert_eq!(submitter.num_pending_tasks(&actor_id), 0);
    }
}
