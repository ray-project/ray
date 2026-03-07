// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Integration tests for the observability pipeline.

use std::sync::Arc;

use parking_lot::Mutex;

use ray_observability::domain_events;
use ray_observability::events::RayEvent;
use ray_observability::export::{EventExporter, EventSink, ExportConfig};

/// Sink that captures flushed events for inspection.
struct CaptureSink {
    events: Arc<Mutex<Vec<RayEvent>>>,
}

impl EventSink for CaptureSink {
    fn flush(&self, events: &[RayEvent]) -> usize {
        let mut captured = self.events.lock();
        captured.extend(events.iter().cloned());
        events.len()
    }
}

/// Test the full event pipeline: domain events → exporter buffer → flush → sink.
#[test]
fn test_domain_events_through_exporter() {
    let exporter = EventExporter::new(ExportConfig::default());

    let captured = Arc::new(Mutex::new(Vec::new()));
    exporter.set_sink(Box::new(CaptureSink {
        events: captured.clone(),
    }));

    // Generate domain events of various types.
    exporter.add_event(domain_events::actor_created("act-1", "MyActor", "job-1"));
    exporter.add_event(domain_events::node_registered("node-1", "10.0.0.1"));
    exporter.add_event(domain_events::job_started("job-1", 1234));
    exporter.add_event(domain_events::task_failed("task-1", "OOM", true));
    exporter.add_event(domain_events::worker_started("w-1", 5678, "PYTHON"));

    assert_eq!(exporter.buffer_len(), 5);

    // Flush and verify.
    let flushed = exporter.flush();
    assert_eq!(flushed, 5);
    assert_eq!(exporter.buffer_len(), 0);

    let events = captured.lock();
    assert_eq!(events.len(), 5);

    // Verify event labels.
    let labels: Vec<&str> = events.iter().map(|e| e.label.as_str()).collect();
    assert!(labels.contains(&"ACTOR_CREATED"));
    assert!(labels.contains(&"NODE_REGISTERED"));
    assert!(labels.contains(&"JOB_STARTED"));
    assert!(labels.contains(&"TASK_FAILED"));
    assert!(labels.contains(&"WORKER_STARTED"));

    // Verify all events have unique IDs.
    let ids: Vec<&str> = events.iter().map(|e| e.event_id.as_str()).collect();
    let unique_ids: std::collections::HashSet<&&str> = ids.iter().collect();
    assert_eq!(unique_ids.len(), 5, "all event IDs should be unique");
}

/// Test the actor lifecycle event sequence.
#[test]
fn test_actor_lifecycle_events() {
    let exporter = EventExporter::new(ExportConfig::default());

    let captured = Arc::new(Mutex::new(Vec::new()));
    exporter.set_sink(Box::new(CaptureSink {
        events: captured.clone(),
    }));

    // Simulate an actor lifecycle.
    exporter.add_event(domain_events::actor_created(
        "act-1",
        "ModelServer",
        "job-1",
    ));
    exporter.add_event(domain_events::actor_died(
        "act-1",
        "ModelServer",
        "OOM killed",
    ));
    exporter.add_event(domain_events::actor_restarted("act-1", "ModelServer", 1));
    exporter.add_event(domain_events::actor_died(
        "act-1",
        "ModelServer",
        "user request",
    ));

    exporter.flush();

    let events = captured.lock();
    assert_eq!(events.len(), 4);
    assert_eq!(events[0].label, "ACTOR_CREATED");
    assert_eq!(events[1].label, "ACTOR_DIED");
    assert_eq!(events[2].label, "ACTOR_RESTARTED");
    assert_eq!(events[3].label, "ACTOR_DIED");

    // Verify actor_id custom field is present on all events.
    for event in events.iter() {
        assert_eq!(
            event.custom_fields.get("actor_id"),
            Some(&"act-1".to_string())
        );
    }
}

/// Test event export with buffer overflow (older events dropped).
#[test]
fn test_event_buffer_overflow_preserves_newest() {
    let exporter = EventExporter::new(ExportConfig {
        max_buffer_size: 3,
        ..Default::default()
    });

    let captured = Arc::new(Mutex::new(Vec::new()));
    exporter.set_sink(Box::new(CaptureSink {
        events: captured.clone(),
    }));

    // Add 5 events to a buffer of size 3.
    for i in 0..5 {
        exporter.add_event(domain_events::job_started(&format!("job-{i}"), i as u32));
    }

    assert_eq!(exporter.buffer_len(), 3);

    exporter.flush();

    let events = captured.lock();
    assert_eq!(events.len(), 3);

    // The oldest events (job-0, job-1) should be dropped.
    let job_ids: Vec<String> = events
        .iter()
        .map(|e| e.custom_fields.get("job_id").unwrap().clone())
        .collect();
    assert_eq!(job_ids, vec!["job-2", "job-3", "job-4"]);

    // Check stats.
    let stats = exporter.stats();
    assert_eq!(stats.total_events_buffered, 5);
    assert_eq!(stats.total_events_dropped, 2);
    assert_eq!(stats.total_events_flushed, 3);
}

/// Test event serialization roundtrip through the pipeline.
#[test]
fn test_event_json_through_pipeline() {
    let exporter = EventExporter::new(ExportConfig::default());

    let captured = Arc::new(Mutex::new(Vec::new()));
    exporter.set_sink(Box::new(CaptureSink {
        events: captured.clone(),
    }));

    let event = domain_events::node_removed("node-abc", "10.0.0.5", "health check failed");
    exporter.add_event(event);
    exporter.flush();

    let events = captured.lock();
    let event = &events[0];

    // Verify JSON roundtrip.
    let json = serde_json::to_string(event).unwrap();
    let parsed: RayEvent = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed.label, "NODE_REMOVED");
    assert_eq!(
        parsed.custom_fields.get("removal_reason").unwrap(),
        "health check failed"
    );
}

/// Test event reporter writes to disk and EventExporter buffers independently.
#[test]
fn test_event_reporter_and_exporter_independent() {
    use ray_observability::events::{EventReporter, EventSeverity, EventSourceType, RayEvent};

    let tmp_dir = tempfile::tempdir().unwrap();
    let reporter = EventReporter::new(Some(tmp_dir.path().to_path_buf()));
    let exporter = EventExporter::new(ExportConfig::default());

    let event = RayEvent::new(
        EventSourceType::Gcs,
        EventSeverity::Info,
        "test_label",
        "Test message",
    );

    // Report to disk.
    reporter.report(&event);

    // Also add to exporter.
    exporter.add_event(event);

    // Disk file should exist.
    let log_path = tmp_dir.path().join("event_GCS.log");
    assert!(log_path.exists());
    let contents = std::fs::read_to_string(&log_path).unwrap();
    assert!(contents.contains("Test message"));

    // Exporter should have 1 buffered event.
    assert_eq!(exporter.buffer_len(), 1);
}
