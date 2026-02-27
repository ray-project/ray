// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Normal (non-actor) task submission.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use ray_common::id::TaskID;
use ray_proto::ray::rpc::TaskSpec;

use crate::error::{CoreWorkerError, CoreWorkerResult};
use crate::reference_counter::ReferenceCounter;

/// Submitter for normal (non-actor) tasks.
///
/// Real network logic (raylet lease requests, worker assignment) is deferred
/// to a future phase that integrates the raylet RPC client.
pub struct NormalTaskSubmitter {
    reference_counter: Arc<ReferenceCounter>,
    pending_tasks: AtomicUsize,
}

impl NormalTaskSubmitter {
    pub fn new(reference_counter: Arc<ReferenceCounter>) -> Self {
        Self {
            reference_counter,
            pending_tasks: AtomicUsize::new(0),
        }
    }

    /// Submit a normal task for execution.
    ///
    /// Currently a stub: records the task as pending and updates reference counts
    /// for the task's argument objects. Actual raylet communication is deferred.
    pub async fn submit_task(&self, task_spec: &TaskSpec) -> CoreWorkerResult<()> {
        // Track references for task arguments.
        let arg_ids: Vec<_> = task_spec
            .args
            .iter()
            .filter_map(|arg| {
                arg.object_ref
                    .as_ref()
                    .map(|r| ray_common::id::ObjectID::from_binary(&r.object_id))
            })
            .collect();
        self.reference_counter
            .update_submitted_task_references(&arg_ids);

        self.pending_tasks.fetch_add(1, Ordering::Relaxed);
        tracing::debug!(
            task_id = %hex::encode(&task_spec.task_id),
            "normal task submitted (stub)"
        );
        Ok(())
    }

    /// Cancel a pending task.
    pub fn cancel_task(
        &self,
        _task_id: &TaskID,
        _force_kill: bool,
    ) -> CoreWorkerResult<()> {
        // Stub: real cancellation requires raylet interaction.
        Err(CoreWorkerError::Internal(
            "normal task cancellation not yet implemented".into(),
        ))
    }

    /// Number of pending tasks.
    pub fn num_pending_tasks(&self) -> usize {
        self.pending_tasks.load(Ordering::Relaxed)
    }

    /// Reference to the reference counter.
    pub fn reference_counter(&self) -> &Arc<ReferenceCounter> {
        &self.reference_counter
    }
}
