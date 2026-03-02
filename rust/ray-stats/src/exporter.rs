// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Metrics exporter for Ray.
//!
//! Collects metrics from registered Counter/Gauge/Histogram instances and
//! exports them periodically. Supports in-process collection for Prometheus
//! scraping or push-based export via OpenTelemetry.

use std::sync::Arc;

use parking_lot::Mutex;

use crate::{Counter, Gauge, Histogram, Metric};

/// Configuration for the metrics exporter.
#[derive(Debug, Clone)]
pub struct ExporterConfig {
    /// Export interval in seconds.
    pub export_interval_secs: u64,
    /// Global tags applied to all metrics (e.g., node_id, job_id).
    pub global_tags: Vec<(String, String)>,
    /// Whether metrics export is enabled.
    pub enabled: bool,
}

impl Default for ExporterConfig {
    fn default() -> Self {
        Self {
            export_interval_secs: 10,
            global_tags: Vec::new(),
            enabled: true,
        }
    }
}

/// A point-in-time metric snapshot.
#[derive(Debug, Clone)]
pub enum MetricSnapshot {
    Counter {
        name: String,
        tags: Vec<(String, String)>,
        value: u64,
    },
    Gauge {
        name: String,
        tags: Vec<(String, String)>,
        value: f64,
    },
    Histogram {
        name: String,
        tags: Vec<(String, String)>,
        count: usize,
        sum: f64,
        boundaries: Vec<f64>,
        bucket_counts: Vec<u64>,
    },
}

/// Sink trait for receiving exported metrics.
///
/// Implementations can write to Prometheus, OTLP, logging, etc.
pub trait MetricsSink: Send + Sync {
    /// Export a batch of metric snapshots.
    fn export(&self, snapshots: &[MetricSnapshot]);
}

/// A logging sink that writes metrics to tracing.
pub struct LoggingSink;

impl MetricsSink for LoggingSink {
    fn export(&self, snapshots: &[MetricSnapshot]) {
        for snapshot in snapshots {
            match snapshot {
                MetricSnapshot::Counter { name, tags, value } => {
                    tracing::debug!(metric = name, ?tags, value, "counter");
                }
                MetricSnapshot::Gauge { name, tags, value } => {
                    tracing::debug!(metric = name, ?tags, value, "gauge");
                }
                MetricSnapshot::Histogram {
                    name,
                    tags,
                    count,
                    sum,
                    ..
                } => {
                    tracing::debug!(metric = name, ?tags, count, sum, "histogram");
                }
            }
        }
    }
}

/// The metrics registry and exporter.
///
/// Collects metrics from registered instruments and periodically exports
/// them via a configurable sink.
pub struct MetricsExporter {
    config: ExporterConfig,
    counters: Mutex<Vec<Counter>>,
    gauges: Mutex<Vec<Gauge>>,
    histograms: Mutex<Vec<Histogram>>,
    sinks: Mutex<Vec<Box<dyn MetricsSink>>>,
}

impl MetricsExporter {
    pub fn new(config: ExporterConfig) -> Self {
        Self {
            config,
            counters: Mutex::new(Vec::new()),
            gauges: Mutex::new(Vec::new()),
            histograms: Mutex::new(Vec::new()),
            sinks: Mutex::new(Vec::new()),
        }
    }

    /// Register a counter for export.
    pub fn register_counter(&self, counter: Counter) {
        self.counters.lock().push(counter);
    }

    /// Register a gauge for export.
    pub fn register_gauge(&self, gauge: Gauge) {
        self.gauges.lock().push(gauge);
    }

    /// Register a histogram for export.
    pub fn register_histogram(&self, histogram: Histogram) {
        self.histograms.lock().push(histogram);
    }

    /// Add a metrics sink.
    pub fn add_sink(&self, sink: Box<dyn MetricsSink>) {
        self.sinks.lock().push(sink);
    }

    /// Collect all metric snapshots.
    pub fn collect(&self) -> Vec<MetricSnapshot> {
        let mut snapshots = Vec::new();

        // Counters
        for counter in self.counters.lock().iter() {
            let values = counter.get_all_values();
            for (tags, value) in values {
                let mut all_tags = self.config.global_tags.clone();
                all_tags.extend(tags);
                snapshots.push(MetricSnapshot::Counter {
                    name: counter.name().to_string(),
                    tags: all_tags,
                    value,
                });
            }
        }

        // Gauges
        for gauge in self.gauges.lock().iter() {
            let values = gauge.get_all_values();
            for (tags, value) in values {
                let mut all_tags = self.config.global_tags.clone();
                all_tags.extend(tags);
                snapshots.push(MetricSnapshot::Gauge {
                    name: gauge.name().to_string(),
                    tags: all_tags,
                    value,
                });
            }
        }

        // Histograms
        for histogram in self.histograms.lock().iter() {
            let all_values = histogram.get_all_values();
            for (tags, values) in all_values {
                let count = values.len();
                let sum: f64 = values.iter().sum();
                let bucket_counts = compute_bucket_counts(histogram.boundaries(), &values);

                let mut all_tags = self.config.global_tags.clone();
                all_tags.extend(tags);
                snapshots.push(MetricSnapshot::Histogram {
                    name: histogram.name().to_string(),
                    tags: all_tags,
                    count,
                    sum,
                    boundaries: histogram.boundaries().to_vec(),
                    bucket_counts,
                });
            }
        }

        snapshots
    }

    /// Export all metrics to all registered sinks.
    pub fn export(&self) {
        if !self.config.enabled {
            return;
        }
        let snapshots = self.collect();
        if snapshots.is_empty() {
            return;
        }
        let sinks = self.sinks.lock();
        for sink in sinks.iter() {
            sink.export(&snapshots);
        }
    }

    /// Start periodic export in a tokio task.
    pub fn start_periodic_export(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        let interval = std::time::Duration::from_secs(self.config.export_interval_secs);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                self.export();
            }
        })
    }

    /// Format metrics as Prometheus text exposition format.
    pub fn to_prometheus_text(&self) -> String {
        let snapshots = self.collect();
        let mut output = String::new();

        for snapshot in &snapshots {
            match snapshot {
                MetricSnapshot::Counter { name, tags, value } => {
                    output.push_str(&format!("# TYPE {} counter\n", name));
                    output.push_str(&format!("{}{} {}\n", name, format_tags(tags), value));
                }
                MetricSnapshot::Gauge { name, tags, value } => {
                    output.push_str(&format!("# TYPE {} gauge\n", name));
                    output.push_str(&format!("{}{} {}\n", name, format_tags(tags), value));
                }
                MetricSnapshot::Histogram {
                    name,
                    tags,
                    count,
                    sum,
                    boundaries,
                    bucket_counts,
                } => {
                    output.push_str(&format!("# TYPE {} histogram\n", name));
                    for (i, boundary) in boundaries.iter().enumerate() {
                        let mut bucket_tags = tags.clone();
                        bucket_tags.push(("le".to_string(), format!("{}", boundary)));
                        output.push_str(&format!(
                            "{}_bucket{} {}\n",
                            name,
                            format_tags(&bucket_tags),
                            bucket_counts[i]
                        ));
                    }
                    let mut inf_tags = tags.clone();
                    inf_tags.push(("le".to_string(), "+Inf".to_string()));
                    output.push_str(&format!(
                        "{}_bucket{} {}\n",
                        name,
                        format_tags(&inf_tags),
                        count
                    ));
                    output.push_str(&format!("{}_sum{} {}\n", name, format_tags(tags), sum));
                    output.push_str(&format!("{}_count{} {}\n", name, format_tags(tags), count));
                }
            }
        }

        output
    }

    pub fn config(&self) -> &ExporterConfig {
        &self.config
    }

    pub fn num_registered_counters(&self) -> usize {
        self.counters.lock().len()
    }

    pub fn num_registered_gauges(&self) -> usize {
        self.gauges.lock().len()
    }

    pub fn num_registered_histograms(&self) -> usize {
        self.histograms.lock().len()
    }
}

/// Format tags as Prometheus label string: {key="value",key2="value2"}
fn format_tags(tags: &[(String, String)]) -> String {
    if tags.is_empty() {
        return String::new();
    }
    let pairs: Vec<String> = tags.iter().map(|(k, v)| format!("{}=\"{}\"", k, v)).collect();
    format!("{{{}}}", pairs.join(","))
}

/// Compute cumulative bucket counts for histogram boundaries.
///
/// Each value is placed in the first (smallest) bucket whose boundary >= value.
/// Counts are then made cumulative for Prometheus exposition format.
fn compute_bucket_counts(boundaries: &[f64], values: &[f64]) -> Vec<u64> {
    let mut counts = vec![0u64; boundaries.len()];
    for &value in values {
        for (i, &boundary) in boundaries.iter().enumerate() {
            if value <= boundary {
                counts[i] += 1;
                break;
            }
        }
    }
    // Make cumulative
    for i in 1..counts.len() {
        counts[i] += counts[i - 1];
    }
    counts
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exporter_config_default() {
        let config = ExporterConfig::default();
        assert_eq!(config.export_interval_secs, 10);
        assert!(config.enabled);
        assert!(config.global_tags.is_empty());
    }

    #[test]
    fn test_register_and_collect_counter() {
        let exporter = MetricsExporter::new(ExporterConfig::default());
        let counter = Counter::new("test_counter", "test");
        counter.increment(&[], 42);

        exporter.register_counter(counter);
        let snapshots = exporter.collect();

        assert_eq!(snapshots.len(), 1);
        match &snapshots[0] {
            MetricSnapshot::Counter { name, value, .. } => {
                assert_eq!(name, "test_counter");
                assert_eq!(*value, 42);
            }
            _ => panic!("expected counter snapshot"),
        }
    }

    #[test]
    fn test_register_and_collect_gauge() {
        let exporter = MetricsExporter::new(ExporterConfig::default());
        let gauge = Gauge::new("test_gauge", "test");
        gauge.set(&[], 3.14);

        exporter.register_gauge(gauge);
        let snapshots = exporter.collect();

        assert_eq!(snapshots.len(), 1);
        match &snapshots[0] {
            MetricSnapshot::Gauge { name, value, .. } => {
                assert_eq!(name, "test_gauge");
                assert_eq!(*value, 3.14);
            }
            _ => panic!("expected gauge snapshot"),
        }
    }

    #[test]
    fn test_register_and_collect_histogram() {
        let exporter = MetricsExporter::new(ExporterConfig::default());
        let hist = Histogram::new("test_hist", "test", vec![10.0, 50.0, 100.0]);
        hist.record(&[], 5.0);
        hist.record(&[], 25.0);
        hist.record(&[], 75.0);

        exporter.register_histogram(hist);
        let snapshots = exporter.collect();

        assert_eq!(snapshots.len(), 1);
        match &snapshots[0] {
            MetricSnapshot::Histogram {
                name,
                count,
                sum,
                bucket_counts,
                ..
            } => {
                assert_eq!(name, "test_hist");
                assert_eq!(*count, 3);
                assert_eq!(*sum, 105.0);
                // 5.0 <= 10.0: bucket[0] = 1
                // 25.0 <= 50.0: bucket[1] = 1
                // 75.0 <= 100.0: bucket[2] = 1
                // Cumulative: [1, 2, 3]
                assert_eq!(bucket_counts, &[1, 2, 3]);
            }
            _ => panic!("expected histogram snapshot"),
        }
    }

    #[test]
    fn test_global_tags_applied() {
        let exporter = MetricsExporter::new(ExporterConfig {
            global_tags: vec![("node_id".to_string(), "abc".to_string())],
            ..Default::default()
        });

        let counter = Counter::new("c", "test");
        counter.increment(&[("job".to_string(), "1".to_string())], 1);
        exporter.register_counter(counter);

        let snapshots = exporter.collect();
        match &snapshots[0] {
            MetricSnapshot::Counter { tags, .. } => {
                assert!(tags.contains(&("node_id".to_string(), "abc".to_string())));
                assert!(tags.contains(&("job".to_string(), "1".to_string())));
            }
            _ => panic!("expected counter"),
        }
    }

    #[test]
    fn test_prometheus_text_format() {
        let exporter = MetricsExporter::new(ExporterConfig::default());
        let counter = Counter::new("ray_tasks", "tasks");
        counter.increment(&[("state".to_string(), "running".to_string())], 5);
        exporter.register_counter(counter);

        let text = exporter.to_prometheus_text();
        assert!(text.contains("# TYPE ray_tasks counter"));
        assert!(text.contains("ray_tasks{state=\"running\"} 5"));
    }

    #[test]
    fn test_export_disabled() {
        let exporter = MetricsExporter::new(ExporterConfig {
            enabled: false,
            ..Default::default()
        });
        let counter = Counter::new("c", "test");
        counter.increment(&[], 1);
        exporter.register_counter(counter);

        // Export should be a no-op when disabled
        exporter.export();
    }

    #[test]
    fn test_format_tags_empty() {
        assert_eq!(format_tags(&[]), "");
    }

    #[test]
    fn test_format_tags_single() {
        let tags = vec![("k".to_string(), "v".to_string())];
        assert_eq!(format_tags(&tags), "{k=\"v\"}");
    }

    #[test]
    fn test_compute_bucket_counts() {
        let boundaries = vec![10.0, 50.0, 100.0];
        let values = vec![5.0, 15.0, 55.0, 200.0];
        let counts = compute_bucket_counts(&boundaries, &values);
        // 5.0 -> bucket 0, 15.0 -> bucket 1, 55.0 -> bucket 2, 200.0 -> no bucket
        // Non-cumulative: [1, 1, 1]
        // Cumulative: [1, 2, 3]
        assert_eq!(counts, vec![1, 2, 3]);
    }

    #[test]
    fn test_num_registered() {
        let exporter = MetricsExporter::new(ExporterConfig::default());
        assert_eq!(exporter.num_registered_counters(), 0);
        exporter.register_counter(Counter::new("c1", ""));
        exporter.register_counter(Counter::new("c2", ""));
        assert_eq!(exporter.num_registered_counters(), 2);
        exporter.register_gauge(Gauge::new("g1", ""));
        assert_eq!(exporter.num_registered_gauges(), 1);
    }
}
