// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Per-worker context: current task/actor/job IDs and index counters.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use ray_common::id::{ActorID, JobID, TaskID, WorkerID};

use crate::options::WorkerType;

/// Per-worker mutable context tracking the current task, actor, and job.
pub struct WorkerContext {
    worker_type: WorkerType,
    worker_id: WorkerID,
    current_job_id: parking_lot::Mutex<JobID>,
    current_task_id: parking_lot::Mutex<TaskID>,
    current_actor_id: parking_lot::Mutex<ActorID>,
    task_index: AtomicU64,
    put_index: AtomicU64,
    job_initialized: AtomicBool,
    pub current_actor_should_exit: AtomicBool,
}

impl WorkerContext {
    /// Create a new worker context.
    pub fn new(worker_type: WorkerType, worker_id: WorkerID, job_id: JobID) -> Self {
        Self {
            worker_type,
            worker_id,
            current_job_id: parking_lot::Mutex::new(job_id),
            current_task_id: parking_lot::Mutex::new(TaskID::nil()),
            current_actor_id: parking_lot::Mutex::new(ActorID::nil()),
            task_index: AtomicU64::new(0),
            put_index: AtomicU64::new(0),
            job_initialized: AtomicBool::new(false),
            current_actor_should_exit: AtomicBool::new(false),
        }
    }

    pub fn worker_type(&self) -> WorkerType {
        self.worker_type
    }

    pub fn worker_id(&self) -> WorkerID {
        self.worker_id
    }

    pub fn current_job_id(&self) -> JobID {
        *self.current_job_id.lock()
    }

    pub fn set_current_job_id(&self, job_id: JobID) {
        *self.current_job_id.lock() = job_id;
    }

    pub fn current_task_id(&self) -> TaskID {
        *self.current_task_id.lock()
    }

    pub fn set_current_task_id(&self, task_id: TaskID) {
        *self.current_task_id.lock() = task_id;
        // Reset counters when starting a new task.
        self.task_index.store(0, Ordering::Relaxed);
        self.put_index.store(0, Ordering::Relaxed);
    }

    pub fn current_actor_id(&self) -> ActorID {
        *self.current_actor_id.lock()
    }

    pub fn set_current_actor_id(&self, actor_id: ActorID) {
        *self.current_actor_id.lock() = actor_id;
    }

    /// Get the next task index (atomically incremented).
    pub fn get_next_task_index(&self) -> u64 {
        self.task_index.fetch_add(1, Ordering::Relaxed)
    }

    /// Get the next put index (atomically incremented).
    pub fn get_next_put_index(&self) -> u64 {
        self.put_index.fetch_add(1, Ordering::Relaxed)
    }

    /// Lazily initialize job info. Returns `true` if this call performed the initialization.
    pub fn maybe_initialize_job_info(&self, job_id: JobID) -> bool {
        if self
            .job_initialized
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.set_current_job_id(job_id);
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_ctx() -> WorkerContext {
        let wid = WorkerID::from_random();
        let jid = JobID::from_int(1);
        WorkerContext::new(WorkerType::Worker, wid, jid)
    }

    #[test]
    fn test_worker_context_basic() {
        let ctx = make_ctx();
        assert_eq!(ctx.worker_type(), WorkerType::Worker);
        assert_eq!(ctx.current_job_id(), JobID::from_int(1));
        assert!(ctx.current_task_id().is_nil());
        assert!(ctx.current_actor_id().is_nil());
    }

    #[test]
    fn test_set_current_task_resets_counters() {
        let ctx = make_ctx();
        assert_eq!(ctx.get_next_task_index(), 0);
        assert_eq!(ctx.get_next_task_index(), 1);
        // Setting a new task ID resets the counters.
        ctx.set_current_task_id(TaskID::from_random());
        assert_eq!(ctx.get_next_task_index(), 0);
    }

    #[test]
    fn test_get_next_put_index() {
        let ctx = make_ctx();
        assert_eq!(ctx.get_next_put_index(), 0);
        assert_eq!(ctx.get_next_put_index(), 1);
        assert_eq!(ctx.get_next_put_index(), 2);
    }

    #[test]
    fn test_maybe_initialize_job_info() {
        let wid = WorkerID::from_random();
        let ctx = WorkerContext::new(WorkerType::Driver, wid, JobID::nil());
        let jid = JobID::from_int(42);
        assert!(ctx.maybe_initialize_job_info(jid));
        assert_eq!(ctx.current_job_id(), jid);
        // Second call is a no-op.
        assert!(!ctx.maybe_initialize_job_info(JobID::from_int(99)));
        assert_eq!(ctx.current_job_id(), jid);
    }

    #[test]
    fn test_actor_should_exit_flag() {
        let ctx = make_ctx();
        assert!(!ctx.current_actor_should_exit.load(Ordering::Relaxed));
        ctx.current_actor_should_exit.store(true, Ordering::Relaxed);
        assert!(ctx.current_actor_should_exit.load(Ordering::Relaxed));
    }

    #[test]
    fn test_set_current_actor_id() {
        let ctx = make_ctx();
        let aid = ActorID::from_random();
        ctx.set_current_actor_id(aid);
        assert_eq!(ctx.current_actor_id(), aid);
    }
}
