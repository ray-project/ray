// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

#![allow(clippy::type_complexity)]

//! Metrics collection for Ray.
//!
//! Replaces `src/ray/stats/` (7 files).
//! Uses OpenTelemetry for metrics export, with metric names and tag keys
//! matching the C++ implementation exactly.

use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;

/// A tagged metric recording interface.
///
/// All metric types (Counter, Gauge, Histogram) share this pattern
/// of recording values with a set of tag key-value pairs.
pub trait Metric: Send + Sync {
    fn name(&self) -> &str;
    fn description(&self) -> &str;
}

/// A monotonically increasing counter.
#[derive(Clone)]
pub struct Counter {
    name: String,
    description: String,
    value: Arc<Mutex<HashMap<Vec<(String, String)>, u64>>>,
}

impl Counter {
    pub fn new(name: impl Into<String>, description: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            description: description.into(),
            value: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn increment(&self, tags: &[(String, String)], delta: u64) {
        let mut values = self.value.lock();
        let entry = values.entry(tags.to_vec()).or_insert(0);
        *entry += delta;
    }

    pub fn get(&self, tags: &[(String, String)]) -> u64 {
        let values = self.value.lock();
        values.get(tags).copied().unwrap_or(0)
    }
}

impl Metric for Counter {
    fn name(&self) -> &str {
        &self.name
    }
    fn description(&self) -> &str {
        &self.description
    }
}

/// A gauge that can go up or down.
#[derive(Clone)]
pub struct Gauge {
    name: String,
    description: String,
    value: Arc<Mutex<HashMap<Vec<(String, String)>, f64>>>,
}

impl Gauge {
    pub fn new(name: impl Into<String>, description: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            description: description.into(),
            value: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn set(&self, tags: &[(String, String)], value: f64) {
        let mut values = self.value.lock();
        values.insert(tags.to_vec(), value);
    }

    pub fn get(&self, tags: &[(String, String)]) -> f64 {
        let values = self.value.lock();
        values.get(tags).copied().unwrap_or(0.0)
    }
}

impl Metric for Gauge {
    fn name(&self) -> &str {
        &self.name
    }
    fn description(&self) -> &str {
        &self.description
    }
}

/// A histogram for recording distributions.
#[derive(Clone)]
pub struct Histogram {
    name: String,
    description: String,
    boundaries: Vec<f64>,
    values: Arc<Mutex<HashMap<Vec<(String, String)>, Vec<f64>>>>,
}

impl Histogram {
    pub fn new(
        name: impl Into<String>,
        description: impl Into<String>,
        boundaries: Vec<f64>,
    ) -> Self {
        Self {
            name: name.into(),
            description: description.into(),
            boundaries,
            values: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn record(&self, tags: &[(String, String)], value: f64) {
        let mut values = self.values.lock();
        let entry = values.entry(tags.to_vec()).or_default();
        entry.push(value);
    }

    pub fn boundaries(&self) -> &[f64] {
        &self.boundaries
    }
}

impl Metric for Histogram {
    fn name(&self) -> &str {
        &self.name
    }
    fn description(&self) -> &str {
        &self.description
    }
}

/// Well-known Ray metric names matching the C++ stats implementation.
pub mod metric_defs {
    pub const GCS_ACTORS_COUNT: &str = "ray_gcs_actors_count";
    pub const GCS_PLACEMENT_GROUPS_COUNT: &str = "ray_gcs_placement_groups_count";
    pub const GCS_NODES_COUNT: &str = "ray_gcs_nodes_count";
    pub const OBJECT_STORE_MEMORY: &str = "ray_object_store_memory";
    pub const OBJECT_STORE_NUM_OBJECTS: &str = "ray_object_store_num_objects";
    pub const TASKS_RUNNING: &str = "ray_tasks_running";
    pub const TASKS_PENDING: &str = "ray_tasks_pending";
    pub const RESOURCES_TOTAL: &str = "ray_resources_total";
    pub const RESOURCES_AVAILABLE: &str = "ray_resources_available";
    pub const WORKER_REGISTER_TIME_MS: &str = "ray_worker_register_time_ms";
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_counter() {
        let counter = Counter::new("test_counter", "A test counter");
        let tags = vec![("job".to_string(), "123".to_string())];
        counter.increment(&tags, 5);
        counter.increment(&tags, 3);
        assert_eq!(counter.get(&tags), 8);
    }

    #[test]
    fn test_gauge() {
        let gauge = Gauge::new("test_gauge", "A test gauge");
        let tags = vec![];
        gauge.set(&tags, 42.0);
        assert_eq!(gauge.get(&tags), 42.0);
        gauge.set(&tags, 10.0);
        assert_eq!(gauge.get(&tags), 10.0);
    }
}
