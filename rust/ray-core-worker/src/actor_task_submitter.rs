// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Actor task submission with per-actor queuing.

use std::collections::{HashMap, VecDeque};

use parking_lot::Mutex;

use ray_common::id::ActorID;
use ray_proto::ray::rpc::TaskSpec;

use crate::error::{CoreWorkerError, CoreWorkerResult};

/// Per-actor pending task queue.
struct ActorQueue {
    pending_tasks: VecDeque<TaskSpec>,
    next_sequence_number: u64,
}

impl ActorQueue {
    fn new() -> Self {
        Self {
            pending_tasks: VecDeque::new(),
            next_sequence_number: 0,
        }
    }
}

/// Manages actor task submission with per-actor ordering.
pub struct ActorTaskSubmitter {
    queues: Mutex<HashMap<ActorID, ActorQueue>>,
}

impl ActorTaskSubmitter {
    pub fn new() -> Self {
        Self {
            queues: Mutex::new(HashMap::new()),
        }
    }

    /// Register an actor so tasks can be queued to it.
    pub fn add_actor(&self, actor_id: ActorID) {
        self.queues
            .lock()
            .entry(actor_id)
            .or_insert_with(ActorQueue::new);
    }

    /// Submit a task to an actor.
    ///
    /// The task is enqueued and assigned a sequence number. Actual delivery
    /// to the actor worker is deferred to a future integration phase.
    pub async fn submit_task(&self, actor_id: &ActorID, task_spec: TaskSpec) -> CoreWorkerResult<()> {
        let mut queues = self.queues.lock();
        let queue = queues.get_mut(actor_id).ok_or_else(|| {
            CoreWorkerError::ActorNotFound(actor_id.hex())
        })?;
        let _seq = queue.next_sequence_number;
        queue.next_sequence_number += 1;
        queue.pending_tasks.push_back(task_spec);
        tracing::debug!(actor_id = %actor_id.hex(), seq = _seq, "actor task enqueued (stub)");
        Ok(())
    }

    /// Cancel a task for a specific actor.
    pub fn cancel_task(&self, _actor_id: &ActorID) -> CoreWorkerResult<()> {
        Err(CoreWorkerError::Internal(
            "actor task cancellation not yet implemented".into(),
        ))
    }

    /// Disconnect an actor, clearing its pending task queue.
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
}

impl Default for ActorTaskSubmitter {
    fn default() -> Self {
        Self::new()
    }
}
