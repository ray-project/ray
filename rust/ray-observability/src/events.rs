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
