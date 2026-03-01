// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Domain-specific lifecycle events for Ray components.
//!
//! Provides typed event constructors for common lifecycle events:
//! actor, node, job, task, and worker events.

use crate::events::{EventSeverity, EventSourceType, RayEvent};

// ─── Actor Events ───────────────────────────────────────────────────────

/// Create an event for an actor being created.
pub fn actor_created(actor_id: &str, name: &str, job_id: &str) -> RayEvent {
    RayEvent::new(
        EventSourceType::Gcs,
        EventSeverity::Info,
        "ACTOR_CREATED",
        format!("Actor {name} created"),
    )
    .with_field("actor_id", actor_id)
    .with_field("actor_name", name)
    .with_field("job_id", job_id)
}

/// Create an event for an actor dying.
pub fn actor_died(actor_id: &str, name: &str, reason: &str) -> RayEvent {
    RayEvent::new(
        EventSourceType::Gcs,
        EventSeverity::Warning,
        "ACTOR_DIED",
        format!("Actor {name} died: {reason}"),
    )
    .with_field("actor_id", actor_id)
    .with_field("actor_name", name)
    .with_field("death_reason", reason)
}

/// Create an event for an actor being restarted.
pub fn actor_restarted(actor_id: &str, name: &str, restart_count: u32) -> RayEvent {
    RayEvent::new(
        EventSourceType::Gcs,
        EventSeverity::Info,
        "ACTOR_RESTARTED",
        format!("Actor {name} restarted (attempt {restart_count})"),
    )
    .with_field("actor_id", actor_id)
    .with_field("actor_name", name)
    .with_field("restart_count", restart_count.to_string())
}

// ─── Node Events ────────────────────────────────────────────────────────

/// Create an event for a node being registered.
pub fn node_registered(node_id: &str, ip_address: &str) -> RayEvent {
    RayEvent::new(
        EventSourceType::Gcs,
        EventSeverity::Info,
        "NODE_REGISTERED",
        format!("Node registered at {ip_address}"),
    )
    .with_field("node_id", node_id)
    .with_field("ip_address", ip_address)
}

/// Create an event for a node being removed (dead).
pub fn node_removed(node_id: &str, ip_address: &str, reason: &str) -> RayEvent {
    RayEvent::new(
        EventSourceType::Gcs,
        EventSeverity::Warning,
        "NODE_REMOVED",
        format!("Node at {ip_address} removed: {reason}"),
    )
    .with_field("node_id", node_id)
    .with_field("ip_address", ip_address)
    .with_field("removal_reason", reason)
}

/// Create an event for a node being drained (autoscaler).
pub fn node_draining(node_id: &str, deadline_ms: u64) -> RayEvent {
    RayEvent::new(
        EventSourceType::Gcs,
        EventSeverity::Info,
        "NODE_DRAINING",
        format!("Node draining with deadline {deadline_ms}ms"),
    )
    .with_field("node_id", node_id)
    .with_field("deadline_ms", deadline_ms.to_string())
}

// ─── Job Events ─────────────────────────────────────────────────────────

/// Create an event for a job being submitted.
pub fn job_started(job_id: &str, driver_pid: u32) -> RayEvent {
    RayEvent::new(
        EventSourceType::Driver,
        EventSeverity::Info,
        "JOB_STARTED",
        format!("Job {job_id} started"),
    )
    .with_field("job_id", job_id)
    .with_field("driver_pid", driver_pid.to_string())
}

/// Create an event for a job completing.
pub fn job_finished(job_id: &str, success: bool) -> RayEvent {
    let severity = if success {
        EventSeverity::Info
    } else {
        EventSeverity::Warning
    };
    RayEvent::new(
        EventSourceType::Driver,
        severity,
        "JOB_FINISHED",
        format!("Job {job_id} finished (success={success})"),
    )
    .with_field("job_id", job_id)
    .with_field("success", success.to_string())
}

// ─── Task Events ────────────────────────────────────────────────────────

/// Create an event for a task failing.
pub fn task_failed(task_id: &str, error: &str, retryable: bool) -> RayEvent {
    RayEvent::new(
        EventSourceType::Worker,
        EventSeverity::Warning,
        "TASK_FAILED",
        format!("Task failed: {error}"),
    )
    .with_field("task_id", task_id)
    .with_field("error_message", error)
    .with_field("retryable", retryable.to_string())
}

// ─── Worker Events ──────────────────────────────────────────────────────

/// Create an event for a worker process starting.
pub fn worker_started(worker_id: &str, pid: u32, language: &str) -> RayEvent {
    RayEvent::new(
        EventSourceType::Raylet,
        EventSeverity::Info,
        "WORKER_STARTED",
        format!("Worker process started (pid={pid}, language={language})"),
    )
    .with_field("worker_id", worker_id)
    .with_field("pid", pid.to_string())
    .with_field("language", language)
}

/// Create an event for a worker process dying unexpectedly.
pub fn worker_died(worker_id: &str, pid: u32, exit_code: i32) -> RayEvent {
    RayEvent::new(
        EventSourceType::Raylet,
        EventSeverity::Warning,
        "WORKER_DIED",
        format!("Worker process died (pid={pid}, exit_code={exit_code})"),
    )
    .with_field("worker_id", worker_id)
    .with_field("pid", pid.to_string())
    .with_field("exit_code", exit_code.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_actor_created_event() {
        let event = actor_created("act-1", "MyActor", "job-1");
        assert_eq!(event.label, "ACTOR_CREATED");
        assert_eq!(event.source_type, EventSourceType::Gcs);
        assert_eq!(event.severity, EventSeverity::Info);
        assert_eq!(event.custom_fields.get("actor_id").unwrap(), "act-1");
        assert_eq!(event.custom_fields.get("actor_name").unwrap(), "MyActor");
        assert_eq!(event.custom_fields.get("job_id").unwrap(), "job-1");
    }

    #[test]
    fn test_actor_died_event() {
        let event = actor_died("act-1", "MyActor", "OOM killed");
        assert_eq!(event.label, "ACTOR_DIED");
        assert_eq!(event.severity, EventSeverity::Warning);
        assert!(event.message.contains("OOM killed"));
    }

    #[test]
    fn test_actor_restarted_event() {
        let event = actor_restarted("act-1", "MyActor", 3);
        assert_eq!(event.label, "ACTOR_RESTARTED");
        assert_eq!(event.custom_fields.get("restart_count").unwrap(), "3");
    }

    #[test]
    fn test_node_registered_event() {
        let event = node_registered("node-abc", "10.0.0.1");
        assert_eq!(event.label, "NODE_REGISTERED");
        assert_eq!(event.custom_fields.get("ip_address").unwrap(), "10.0.0.1");
    }

    #[test]
    fn test_node_removed_event() {
        let event = node_removed("node-abc", "10.0.0.1", "health check failed");
        assert_eq!(event.label, "NODE_REMOVED");
        assert_eq!(event.severity, EventSeverity::Warning);
        assert_eq!(
            event.custom_fields.get("removal_reason").unwrap(),
            "health check failed"
        );
    }

    #[test]
    fn test_node_draining_event() {
        let event = node_draining("node-1", 30000);
        assert_eq!(event.label, "NODE_DRAINING");
        assert_eq!(event.custom_fields.get("deadline_ms").unwrap(), "30000");
    }

    #[test]
    fn test_job_started_event() {
        let event = job_started("job-42", 1234);
        assert_eq!(event.label, "JOB_STARTED");
        assert_eq!(event.source_type, EventSourceType::Driver);
        assert_eq!(event.custom_fields.get("driver_pid").unwrap(), "1234");
    }

    #[test]
    fn test_job_finished_success() {
        let event = job_finished("job-42", true);
        assert_eq!(event.severity, EventSeverity::Info);
        assert_eq!(event.custom_fields.get("success").unwrap(), "true");
    }

    #[test]
    fn test_job_finished_failure() {
        let event = job_finished("job-42", false);
        assert_eq!(event.severity, EventSeverity::Warning);
        assert_eq!(event.custom_fields.get("success").unwrap(), "false");
    }

    #[test]
    fn test_task_failed_event() {
        let event = task_failed("task-1", "division by zero", true);
        assert_eq!(event.label, "TASK_FAILED");
        assert_eq!(event.source_type, EventSourceType::Worker);
        assert_eq!(event.custom_fields.get("retryable").unwrap(), "true");
    }

    #[test]
    fn test_worker_started_event() {
        let event = worker_started("w-1", 5678, "PYTHON");
        assert_eq!(event.label, "WORKER_STARTED");
        assert_eq!(event.source_type, EventSourceType::Raylet);
        assert_eq!(event.custom_fields.get("language").unwrap(), "PYTHON");
    }

    #[test]
    fn test_worker_died_event() {
        let event = worker_died("w-1", 5678, -9);
        assert_eq!(event.label, "WORKER_DIED");
        assert_eq!(event.custom_fields.get("exit_code").unwrap(), "-9");
    }

    #[test]
    fn test_events_are_serializable() {
        let event = actor_created("a1", "Actor", "j1");
        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("ACTOR_CREATED"));
        assert!(json.contains("actor_id"));
        let parsed: RayEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.label, "ACTOR_CREATED");
    }
}
