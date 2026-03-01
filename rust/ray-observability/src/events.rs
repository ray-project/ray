// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Structured event recording for Ray lifecycle events.
//!
//! Records actor, job, node, and task lifecycle events as structured data.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Severity level for Ray events.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum EventSeverity {
    Info,
    Warning,
    Error,
    Fatal,
}

/// Source type for events.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum EventSourceType {
    Core,
    Gcs,
    Raylet,
    Worker,
    Driver,
}

/// A structured Ray event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RayEvent {
    pub event_id: String,
    pub source_type: EventSourceType,
    pub source_hostname: String,
    pub source_pid: u32,
    pub severity: EventSeverity,
    pub label: String,
    pub message: String,
    pub timestamp: u64,
    pub custom_fields: HashMap<String, String>,
}

impl RayEvent {
    pub fn new(
        source_type: EventSourceType,
        severity: EventSeverity,
        label: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            event_id: uuid::Uuid::new_v4().to_string(),
            source_type,
            source_hostname: hostname(),
            source_pid: std::process::id(),
            severity,
            label: label.into(),
            message: message.into(),
            timestamp: ray_util::time::current_time_ms(),
            custom_fields: HashMap::new(),
        }
    }

    pub fn with_field(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.custom_fields.insert(key.into(), value.into());
        self
    }
}

fn hostname() -> String {
    nix::unistd::gethostname()
        .map(|h: std::ffi::OsString| h.to_string_lossy().to_string())
        .unwrap_or_else(|_| "unknown".to_string())
}

/// Event reporter that writes events to a log file.
pub struct EventReporter {
    log_dir: Option<std::path::PathBuf>,
}

impl EventReporter {
    pub fn new(log_dir: Option<std::path::PathBuf>) -> Self {
        Self { log_dir }
    }

    pub fn report(&self, event: &RayEvent) {
        if let Some(dir) = &self.log_dir {
            let filename = format!("event_{}.log", event.source_type.as_str());
            let path = dir.join(filename);
            if let Ok(json) = serde_json::to_string(event) {
                let _ = std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(path)
                    .and_then(|mut f| {
                        use std::io::Write;
                        writeln!(f, "{json}")
                    });
            }
        }

        tracing::info!(
            source = ?event.source_type,
            severity = ?event.severity,
            label = %event.label,
            "{}",
            event.message
        );
    }
}

impl EventSourceType {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Core => "CORE",
            Self::Gcs => "GCS",
            Self::Raylet => "RAYLET",
            Self::Worker => "WORKER",
            Self::Driver => "DRIVER",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_creation() {
        let event = RayEvent::new(
            EventSourceType::Gcs,
            EventSeverity::Info,
            "node_added",
            "Node registered successfully",
        );
        assert!(!event.event_id.is_empty());
        assert_eq!(event.source_type, EventSourceType::Gcs);
        assert_eq!(event.severity, EventSeverity::Info);
        assert_eq!(event.label, "node_added");
        assert_eq!(event.message, "Node registered successfully");
        assert!(event.timestamp > 0);
        assert_eq!(event.source_pid, std::process::id());
        assert!(event.custom_fields.is_empty());
    }

    #[test]
    fn test_event_with_custom_fields() {
        let event = RayEvent::new(
            EventSourceType::Worker,
            EventSeverity::Warning,
            "task_failed",
            "Task execution failed",
        )
        .with_field("task_id", "abc123")
        .with_field("error_code", "42");
        assert_eq!(event.custom_fields.len(), 2);
        assert_eq!(event.custom_fields.get("task_id").unwrap(), "abc123");
        assert_eq!(event.custom_fields.get("error_code").unwrap(), "42");
    }

    #[test]
    fn test_event_severity_serde() {
        let json = serde_json::to_string(&EventSeverity::Error).unwrap();
        assert_eq!(json, "\"ERROR\"");
        let parsed: EventSeverity = serde_json::from_str("\"WARNING\"").unwrap();
        assert_eq!(parsed, EventSeverity::Warning);
        let parsed: EventSeverity = serde_json::from_str("\"FATAL\"").unwrap();
        assert_eq!(parsed, EventSeverity::Fatal);
    }

    #[test]
    fn test_event_source_type_serde() {
        let json = serde_json::to_string(&EventSourceType::Raylet).unwrap();
        assert_eq!(json, "\"RAYLET\"");
        let parsed: EventSourceType = serde_json::from_str("\"DRIVER\"").unwrap();
        assert_eq!(parsed, EventSourceType::Driver);
    }

    #[test]
    fn test_event_source_type_as_str() {
        assert_eq!(EventSourceType::Core.as_str(), "CORE");
        assert_eq!(EventSourceType::Gcs.as_str(), "GCS");
        assert_eq!(EventSourceType::Raylet.as_str(), "RAYLET");
        assert_eq!(EventSourceType::Worker.as_str(), "WORKER");
        assert_eq!(EventSourceType::Driver.as_str(), "DRIVER");
    }

    #[test]
    fn test_event_json_roundtrip() {
        let event = RayEvent::new(
            EventSourceType::Core,
            EventSeverity::Fatal,
            "crash",
            "System crash detected",
        )
        .with_field("core_dump", "/tmp/core.1234");
        let json = serde_json::to_string(&event).unwrap();
        let parsed: RayEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.event_id, event.event_id);
        assert_eq!(parsed.source_type, EventSourceType::Core);
        assert_eq!(parsed.severity, EventSeverity::Fatal);
        assert_eq!(parsed.label, "crash");
        assert_eq!(parsed.message, "System crash detected");
        assert_eq!(parsed.custom_fields.get("core_dump").unwrap(), "/tmp/core.1234");
    }

    #[test]
    fn test_reporter_no_log_dir() {
        let reporter = EventReporter::new(None);
        let event = RayEvent::new(
            EventSourceType::Gcs,
            EventSeverity::Info,
            "test",
            "test message",
        );
        // Should not panic even without log_dir
        reporter.report(&event);
    }

    #[test]
    fn test_reporter_with_log_dir() {
        let tmp_dir = std::env::temp_dir().join("ray_test_events");
        let _ = std::fs::create_dir_all(&tmp_dir);
        let reporter = EventReporter::new(Some(tmp_dir.clone()));
        let event = RayEvent::new(
            EventSourceType::Gcs,
            EventSeverity::Info,
            "test",
            "test log message",
        );
        reporter.report(&event);

        let log_path = tmp_dir.join("event_GCS.log");
        assert!(log_path.exists());
        let contents = std::fs::read_to_string(&log_path).unwrap();
        assert!(contents.contains("test log message"));

        // Cleanup
        let _ = std::fs::remove_dir_all(&tmp_dir);
    }

    #[test]
    fn test_unique_event_ids() {
        let e1 = RayEvent::new(EventSourceType::Core, EventSeverity::Info, "a", "b");
        let e2 = RayEvent::new(EventSourceType::Core, EventSeverity::Info, "a", "b");
        assert_ne!(e1.event_id, e2.event_id);
    }
}
