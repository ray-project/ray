// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Direct worker-to-worker call transport for actor tasks.
//!
//! Bypasses the Raylet for actor task submission by sending gRPC calls
//! directly to the worker hosting the actor. Provides connection
//! multiplexing, ordering guarantees, and retry on transient failure.
//!
//! Replaces `src/ray/core_worker/transport/direct_actor_transport.cc`.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use parking_lot::Mutex;

use ray_common::id::ActorID;
use ray_proto::ray::rpc::{Address, TaskSpec};

/// Configuration for direct actor transport.
#[derive(Debug, Clone)]
pub struct DirectTransportConfig {
    /// Maximum number of pending tasks per actor before back-pressure.
    pub max_pending_per_actor: usize,
    /// Maximum number of retry attempts for transient failures.
    pub max_retries: u32,
    /// Delay between retry attempts.
    pub retry_delay: Duration,
    /// Timeout for a single RPC call.
    pub rpc_timeout: Duration,
}

impl Default for DirectTransportConfig {
    fn default() -> Self {
        Self {
            max_pending_per_actor: 1000,
            max_retries: 3,
            retry_delay: Duration::from_millis(100),
            rpc_timeout: Duration::from_secs(30),
        }
    }
}

/// State of a connection to a remote worker hosting an actor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    /// Not yet connected.
    Disconnected,
    /// Connection established and ready.
    Connected,
    /// Connection failed, waiting for retry.
    Reconnecting,
    /// Actor is permanently dead, no more tasks accepted.
    Dead,
}

/// Per-actor state for the direct transport.
struct ActorTransportState {
    /// Remote worker address.
    address: Option<Address>,
    /// Connection state.
    connection_state: ConnectionState,
    /// Pending tasks (ordered by submission time for FIFO delivery).
    pending_tasks: VecDeque<PendingActorTask>,
    /// Next sequence number for ordering.
    next_sequence_number: u64,
    /// Number of in-flight tasks (sent but not yet acknowledged).
    num_in_flight: u64,
}

/// A task pending delivery to an actor.
#[derive(Debug, Clone)]
pub struct PendingActorTask {
    /// The task spec.
    pub task_spec: TaskSpec,
    /// Sequence number for ordering.
    pub sequence_number: u64,
    /// Number of delivery attempts.
    pub num_attempts: u32,
}

/// Error from direct transport operations.
#[derive(Debug, thiserror::Error)]
pub enum DirectTransportError {
    #[error("actor {0} is dead")]
    ActorDead(String),
    #[error("actor {0} not connected")]
    NotConnected(String),
    #[error("actor {0} pending queue full ({1} tasks)")]
    QueueFull(String, usize),
    #[error("max retries exceeded for task to actor {0}")]
    MaxRetriesExceeded(String),
    #[error("transport error: {0}")]
    Transport(String),
}

/// Direct worker-to-worker transport for actor tasks.
///
/// Maintains per-actor connections and queues, ensuring tasks are
/// delivered in submission order with retry on transient failure.
pub struct DirectActorTransport {
    config: DirectTransportConfig,
    /// Per-actor transport state.
    actors: Mutex<HashMap<ActorID, ActorTransportState>>,
    /// Whether the transport is shutting down.
    is_shutdown: AtomicBool,
    /// Total tasks submitted.
    total_submitted: AtomicU64,
    /// Total tasks delivered successfully.
    total_delivered: AtomicU64,
    /// Total tasks failed permanently.
    total_failed: AtomicU64,
}

impl DirectActorTransport {
    pub fn new(config: DirectTransportConfig) -> Self {
        Self {
            config,
            actors: Mutex::new(HashMap::new()),
            is_shutdown: AtomicBool::new(false),
            total_submitted: AtomicU64::new(0),
            total_delivered: AtomicU64::new(0),
            total_failed: AtomicU64::new(0),
        }
    }

    /// Register an actor for direct transport.
    pub fn add_actor(&self, actor_id: ActorID) {
        let mut actors = self.actors.lock();
        actors.entry(actor_id).or_insert_with(|| ActorTransportState {
            address: None,
            connection_state: ConnectionState::Disconnected,
            pending_tasks: VecDeque::new(),
            next_sequence_number: 0,
            num_in_flight: 0,
        });
    }

    /// Connect an actor to its worker address.
    ///
    /// After connection, pending tasks can be delivered.
    pub fn connect_actor(&self, actor_id: &ActorID, address: Address) {
        let mut actors = self.actors.lock();
        if let Some(state) = actors.get_mut(actor_id) {
            state.address = Some(address);
            state.connection_state = ConnectionState::Connected;
        }
    }

    /// Mark an actor as dead. All pending tasks will be failed.
    ///
    /// Returns the number of pending tasks that were cancelled.
    pub fn disconnect_actor(&self, actor_id: &ActorID) -> usize {
        let mut actors = self.actors.lock();
        if let Some(state) = actors.get_mut(actor_id) {
            state.connection_state = ConnectionState::Dead;
            let count = state.pending_tasks.len();
            self.total_failed
                .fetch_add(count as u64, Ordering::Relaxed);
            state.pending_tasks.clear();
            state.num_in_flight = 0;
            count
        } else {
            0
        }
    }

    /// Submit an actor task for delivery.
    ///
    /// The task is queued and will be delivered when the connection is ready.
    /// Tasks are delivered in FIFO order per actor.
    pub fn submit_task(
        &self,
        actor_id: &ActorID,
        task_spec: TaskSpec,
    ) -> Result<u64, DirectTransportError> {
        if self.is_shutdown.load(Ordering::Relaxed) {
            return Err(DirectTransportError::Transport("transport shutdown".into()));
        }

        let mut actors = self.actors.lock();
        let state = actors
            .get_mut(actor_id)
            .ok_or_else(|| DirectTransportError::NotConnected(actor_id.hex()))?;

        if state.connection_state == ConnectionState::Dead {
            return Err(DirectTransportError::ActorDead(actor_id.hex()));
        }

        if state.pending_tasks.len() >= self.config.max_pending_per_actor {
            return Err(DirectTransportError::QueueFull(
                actor_id.hex(),
                state.pending_tasks.len(),
            ));
        }

        let seq = state.next_sequence_number;
        state.next_sequence_number += 1;

        state.pending_tasks.push_back(PendingActorTask {
            task_spec,
            sequence_number: seq,
            num_attempts: 0,
        });

        self.total_submitted.fetch_add(1, Ordering::Relaxed);
        Ok(seq)
    }

    /// Get the next task to deliver for an actor.
    ///
    /// Returns `None` if there are no pending tasks or the actor is not connected.
    pub fn next_task_to_deliver(&self, actor_id: &ActorID) -> Option<PendingActorTask> {
        let mut actors = self.actors.lock();
        let state = actors.get_mut(actor_id)?;

        if state.connection_state != ConnectionState::Connected {
            return None;
        }

        if let Some(mut task) = state.pending_tasks.pop_front() {
            task.num_attempts += 1;
            state.num_in_flight += 1;
            Some(task)
        } else {
            None
        }
    }

    /// Mark a task delivery as successful.
    pub fn task_delivered(&self, actor_id: &ActorID) {
        let mut actors = self.actors.lock();
        if let Some(state) = actors.get_mut(actor_id) {
            state.num_in_flight = state.num_in_flight.saturating_sub(1);
        }
        self.total_delivered.fetch_add(1, Ordering::Relaxed);
    }

    /// Mark a task delivery as failed. Re-queue if retries remain.
    pub fn task_failed(
        &self,
        actor_id: &ActorID,
        task: PendingActorTask,
    ) -> Result<(), DirectTransportError> {
        let mut actors = self.actors.lock();
        let state = actors
            .get_mut(actor_id)
            .ok_or_else(|| DirectTransportError::NotConnected(actor_id.hex()))?;

        state.num_in_flight = state.num_in_flight.saturating_sub(1);

        if task.num_attempts >= self.config.max_retries {
            self.total_failed.fetch_add(1, Ordering::Relaxed);
            return Err(DirectTransportError::MaxRetriesExceeded(actor_id.hex()));
        }

        // Re-queue at the front for retry (preserves ordering).
        state.pending_tasks.push_front(task);
        Ok(())
    }

    /// Get the connection state for an actor.
    pub fn connection_state(&self, actor_id: &ActorID) -> Option<ConnectionState> {
        self.actors
            .lock()
            .get(actor_id)
            .map(|s| s.connection_state)
    }

    /// Get the number of pending tasks for an actor.
    pub fn num_pending(&self, actor_id: &ActorID) -> usize {
        self.actors
            .lock()
            .get(actor_id)
            .map(|s| s.pending_tasks.len())
            .unwrap_or(0)
    }

    /// Get the number of in-flight tasks for an actor.
    pub fn num_in_flight(&self, actor_id: &ActorID) -> u64 {
        self.actors
            .lock()
            .get(actor_id)
            .map(|s| s.num_in_flight)
            .unwrap_or(0)
    }

    /// Get the worker address for an actor.
    pub fn get_actor_address(&self, actor_id: &ActorID) -> Option<Address> {
        self.actors
            .lock()
            .get(actor_id)
            .and_then(|s| s.address.clone())
    }

    /// Shut down the transport.
    pub fn shutdown(&self) {
        self.is_shutdown.store(true, Ordering::Relaxed);
        let mut actors = self.actors.lock();
        for (_, state) in actors.iter_mut() {
            let count = state.pending_tasks.len();
            self.total_failed
                .fetch_add(count as u64, Ordering::Relaxed);
            state.pending_tasks.clear();
            state.connection_state = ConnectionState::Dead;
        }
    }

    /// Statistics.
    pub fn total_submitted(&self) -> u64 {
        self.total_submitted.load(Ordering::Relaxed)
    }

    pub fn total_delivered(&self) -> u64 {
        self.total_delivered.load(Ordering::Relaxed)
    }

    pub fn total_failed(&self) -> u64 {
        self.total_failed.load(Ordering::Relaxed)
    }

    pub fn config(&self) -> &DirectTransportConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_aid(val: u8) -> ActorID {
        let mut data = [0u8; 16];
        data[0] = val;
        ActorID::from_binary(&data)
    }

    fn make_address(port: i32) -> Address {
        Address {
            ip_address: "127.0.0.1".to_string(),
            port,
            node_id: vec![0u8; 28],
            worker_id: vec![0u8; 28],
        }
    }

    fn make_task(name: &str) -> TaskSpec {
        TaskSpec {
            name: name.to_string(),
            ..Default::default()
        }
    }

    #[test]
    fn test_add_and_connect_actor() {
        let transport = DirectActorTransport::new(DirectTransportConfig::default());
        let aid = make_aid(1);

        transport.add_actor(aid);
        assert_eq!(
            transport.connection_state(&aid),
            Some(ConnectionState::Disconnected)
        );

        transport.connect_actor(&aid, make_address(10001));
        assert_eq!(
            transport.connection_state(&aid),
            Some(ConnectionState::Connected)
        );
    }

    #[test]
    fn test_submit_and_deliver() {
        let transport = DirectActorTransport::new(DirectTransportConfig::default());
        let aid = make_aid(1);

        transport.add_actor(aid);
        transport.connect_actor(&aid, make_address(10001));

        let seq = transport.submit_task(&aid, make_task("task1")).unwrap();
        assert_eq!(seq, 0);
        assert_eq!(transport.num_pending(&aid), 1);

        let task = transport.next_task_to_deliver(&aid).unwrap();
        assert_eq!(task.sequence_number, 0);
        assert_eq!(task.task_spec.name, "task1");
        assert_eq!(task.num_attempts, 1);
        assert_eq!(transport.num_in_flight(&aid), 1);

        transport.task_delivered(&aid);
        assert_eq!(transport.num_in_flight(&aid), 0);
        assert_eq!(transport.total_delivered(), 1);
    }

    #[test]
    fn test_fifo_ordering() {
        let transport = DirectActorTransport::new(DirectTransportConfig::default());
        let aid = make_aid(1);

        transport.add_actor(aid);
        transport.connect_actor(&aid, make_address(10001));

        transport.submit_task(&aid, make_task("first")).unwrap();
        transport.submit_task(&aid, make_task("second")).unwrap();
        transport.submit_task(&aid, make_task("third")).unwrap();

        let t1 = transport.next_task_to_deliver(&aid).unwrap();
        assert_eq!(t1.task_spec.name, "first");
        assert_eq!(t1.sequence_number, 0);

        let t2 = transport.next_task_to_deliver(&aid).unwrap();
        assert_eq!(t2.task_spec.name, "second");
        assert_eq!(t2.sequence_number, 1);

        let t3 = transport.next_task_to_deliver(&aid).unwrap();
        assert_eq!(t3.task_spec.name, "third");
        assert_eq!(t3.sequence_number, 2);
    }

    #[test]
    fn test_submit_to_dead_actor_fails() {
        let transport = DirectActorTransport::new(DirectTransportConfig::default());
        let aid = make_aid(1);

        transport.add_actor(aid);
        transport.disconnect_actor(&aid);

        let err = transport.submit_task(&aid, make_task("doomed")).unwrap_err();
        assert!(matches!(err, DirectTransportError::ActorDead(_)));
    }

    #[test]
    fn test_submit_to_unknown_actor_fails() {
        let transport = DirectActorTransport::new(DirectTransportConfig::default());
        let aid = make_aid(99);

        let err = transport.submit_task(&aid, make_task("orphan")).unwrap_err();
        assert!(matches!(err, DirectTransportError::NotConnected(_)));
    }

    #[test]
    fn test_queue_full() {
        let transport = DirectActorTransport::new(DirectTransportConfig {
            max_pending_per_actor: 2,
            ..Default::default()
        });
        let aid = make_aid(1);

        transport.add_actor(aid);
        transport.connect_actor(&aid, make_address(10001));

        transport.submit_task(&aid, make_task("1")).unwrap();
        transport.submit_task(&aid, make_task("2")).unwrap();
        let err = transport.submit_task(&aid, make_task("3")).unwrap_err();
        assert!(matches!(err, DirectTransportError::QueueFull(_, 2)));
    }

    #[test]
    fn test_retry_on_failure() {
        let transport = DirectActorTransport::new(DirectTransportConfig {
            max_retries: 3,
            ..Default::default()
        });
        let aid = make_aid(1);

        transport.add_actor(aid);
        transport.connect_actor(&aid, make_address(10001));
        transport.submit_task(&aid, make_task("retry_me")).unwrap();

        // First attempt.
        let task = transport.next_task_to_deliver(&aid).unwrap();
        assert_eq!(task.num_attempts, 1);
        transport.task_failed(&aid, task).unwrap();

        // Second attempt.
        let task = transport.next_task_to_deliver(&aid).unwrap();
        assert_eq!(task.num_attempts, 2);
        transport.task_failed(&aid, task).unwrap();

        // Third attempt.
        let task = transport.next_task_to_deliver(&aid).unwrap();
        assert_eq!(task.num_attempts, 3);

        // Max retries exceeded.
        let err = transport.task_failed(&aid, task).unwrap_err();
        assert!(matches!(err, DirectTransportError::MaxRetriesExceeded(_)));
    }

    #[test]
    fn test_disconnect_clears_pending() {
        let transport = DirectActorTransport::new(DirectTransportConfig::default());
        let aid = make_aid(1);

        transport.add_actor(aid);
        transport.connect_actor(&aid, make_address(10001));

        transport.submit_task(&aid, make_task("1")).unwrap();
        transport.submit_task(&aid, make_task("2")).unwrap();

        let cancelled = transport.disconnect_actor(&aid);
        assert_eq!(cancelled, 2);
        assert_eq!(transport.num_pending(&aid), 0);
        assert_eq!(transport.total_failed(), 2);
    }

    #[test]
    fn test_no_delivery_when_disconnected() {
        let transport = DirectActorTransport::new(DirectTransportConfig::default());
        let aid = make_aid(1);

        transport.add_actor(aid);
        transport.submit_task(&aid, make_task("waiting")).unwrap();

        // Not connected yet — should return None.
        assert!(transport.next_task_to_deliver(&aid).is_none());

        // Connect.
        transport.connect_actor(&aid, make_address(10001));

        // Now should deliver.
        assert!(transport.next_task_to_deliver(&aid).is_some());
    }

    #[test]
    fn test_shutdown() {
        let transport = DirectActorTransport::new(DirectTransportConfig::default());
        let aid = make_aid(1);

        transport.add_actor(aid);
        transport.connect_actor(&aid, make_address(10001));
        transport.submit_task(&aid, make_task("1")).unwrap();

        transport.shutdown();

        let err = transport.submit_task(&aid, make_task("2")).unwrap_err();
        assert!(matches!(err, DirectTransportError::Transport(_)));
        assert_eq!(transport.total_failed(), 1); // The pending task was failed.
    }

    #[test]
    fn test_stats() {
        let transport = DirectActorTransport::new(DirectTransportConfig::default());
        let aid = make_aid(1);

        transport.add_actor(aid);
        transport.connect_actor(&aid, make_address(10001));

        transport.submit_task(&aid, make_task("1")).unwrap();
        transport.submit_task(&aid, make_task("2")).unwrap();

        let task = transport.next_task_to_deliver(&aid).unwrap();
        transport.task_delivered(&aid);

        assert_eq!(transport.total_submitted(), 2);
        assert_eq!(transport.total_delivered(), 1);
    }
}
