// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Integration tests for the metrics pipeline.

use std::sync::Arc;

use parking_lot::Mutex;
use ray_stats::exporter::{ExporterConfig, MetricSnapshot, MetricsExporter, MetricsSink};
use ray_stats::{Counter, Gauge, Histogram};

/// A sink that collects snapshots for inspection.
struct CollectingSink {
    snapshots: Arc<Mutex<Vec<MetricSnapshot>>>,
}

impl MetricsSink for CollectingSink {
    fn export(&self, snapshots: &[MetricSnapshot]) {
        let mut collected = self.snapshots.lock();
        collected.extend(snapshots.iter().cloned());
    }
}

/// Test the full pipeline: register instruments → record values → collect → export → sink.
#[test]
fn test_full_metrics_pipeline() {
    let exporter = MetricsExporter::new(ExporterConfig {
        global_tags: vec![("cluster".to_string(), "test-cluster".to_string())],
        ..Default::default()
    });

    // Register various instruments.
    let counter = Counter::new("ray_tasks_total", "Total tasks submitted");
    let gauge = Gauge::new("ray_workers_active", "Active workers");
    let hist = Histogram::new(
        "ray_task_duration_ms",
        "Task execution time",
        vec![1.0, 5.0, 10.0, 50.0, 100.0],
    );

    exporter.register_counter(counter.clone());
    exporter.register_gauge(gauge.clone());
    exporter.register_histogram(hist.clone());

    // Record values with different tag sets.
    let running_tags = vec![("state".to_string(), "running".to_string())];
    let pending_tags = vec![("state".to_string(), "pending".to_string())];

    counter.increment(&running_tags, 42);
    counter.increment(&pending_tags, 10);
    gauge.set(&running_tags, 8.0);
    hist.record(&running_tags, 2.5);
    hist.record(&running_tags, 12.0);
    hist.record(&running_tags, 75.0);

    // Set up collecting sink.
    let collected = Arc::new(Mutex::new(Vec::new()));
    exporter.add_sink(Box::new(CollectingSink {
        snapshots: collected.clone(),
    }));

    // Export.
    exporter.export();

    // Verify snapshots.
    let snaps = collected.lock();
    // 2 counter entries (running + pending) + 1 gauge + 1 histogram = 4
    assert_eq!(snaps.len(), 4);

    // Verify all snapshots have the global tag.
    for snap in snaps.iter() {
        let tags = match snap {
            MetricSnapshot::Counter { tags, .. } => tags,
            MetricSnapshot::Gauge { tags, .. } => tags,
            MetricSnapshot::Histogram { tags, .. } => tags,
        };
        assert!(
            tags.iter()
                .any(|(k, v)| k == "cluster" && v == "test-cluster"),
            "global tag 'cluster=test-cluster' should be present"
        );
    }

    // Verify Prometheus text format.
    let prom_text = exporter.to_prometheus_text();
    assert!(prom_text.contains("# TYPE ray_tasks_total counter"));
    assert!(prom_text.contains("ray_tasks_total{"));
    assert!(prom_text.contains("state=\"running\""));
    assert!(prom_text.contains("# TYPE ray_workers_active gauge"));
    assert!(prom_text.contains("# TYPE ray_task_duration_ms histogram"));
    assert!(prom_text.contains("ray_task_duration_ms_bucket{"));
    assert!(prom_text.contains("le=\"+Inf\""));
    assert!(prom_text.contains("ray_task_duration_ms_sum{"));
    assert!(prom_text.contains("ray_task_duration_ms_count{"));
}

/// Test that cloned metric handles share state through the exporter.
#[test]
fn test_shared_metric_handles() {
    let exporter = MetricsExporter::new(ExporterConfig::default());

    let counter = Counter::new("shared_counter", "test");
    exporter.register_counter(counter.clone());

    // Increment from the original handle.
    counter.increment(&[], 5);

    // Increment from a clone (simulating different threads).
    let counter2 = counter.clone();
    counter2.increment(&[], 3);

    let snapshots = exporter.collect();
    assert_eq!(snapshots.len(), 1);
    match &snapshots[0] {
        MetricSnapshot::Counter { value, .. } => {
            assert_eq!(*value, 8, "both handles should contribute to same counter");
        }
        _ => panic!("expected counter"),
    }
}

/// Test multiple sinks receive the same data.
#[test]
fn test_multiple_sinks() {
    let exporter = MetricsExporter::new(ExporterConfig::default());

    let counter = Counter::new("multi_sink_counter", "test");
    counter.increment(&[], 100);
    exporter.register_counter(counter);

    let sink1_data = Arc::new(Mutex::new(Vec::new()));
    let sink2_data = Arc::new(Mutex::new(Vec::new()));

    exporter.add_sink(Box::new(CollectingSink {
        snapshots: sink1_data.clone(),
    }));
    exporter.add_sink(Box::new(CollectingSink {
        snapshots: sink2_data.clone(),
    }));

    exporter.export();

    assert_eq!(sink1_data.lock().len(), 1);
    assert_eq!(sink2_data.lock().len(), 1);
}

/// Test Prometheus text format histogram bucket correctness.
#[test]
fn test_prometheus_histogram_buckets() {
    let exporter = MetricsExporter::new(ExporterConfig::default());

    let hist = Histogram::new("latency", "test", vec![10.0, 50.0, 100.0]);
    // Record: 5 (bucket 10), 25 (bucket 50), 75 (bucket 100), 200 (no bucket)
    hist.record(&[], 5.0);
    hist.record(&[], 25.0);
    hist.record(&[], 75.0);
    hist.record(&[], 200.0);
    exporter.register_histogram(hist);

    let text = exporter.to_prometheus_text();

    // Cumulative counts: bucket[10]=1, bucket[50]=2, bucket[100]=3, +Inf=4
    assert!(text.contains("latency_bucket{le=\"10\"} 1"));
    assert!(text.contains("latency_bucket{le=\"50\"} 2"));
    assert!(text.contains("latency_bucket{le=\"100\"} 3"));
    assert!(text.contains("latency_bucket{le=\"+Inf\"} 4"));
    assert!(text.contains("latency_sum 305"));
    assert!(text.contains("latency_count 4"));
}

/// Test that disabled exporter produces no output.
#[test]
fn test_disabled_exporter_no_output() {
    let exporter = MetricsExporter::new(ExporterConfig {
        enabled: false,
        ..Default::default()
    });

    let counter = Counter::new("should_not_export", "test");
    counter.increment(&[], 999);
    exporter.register_counter(counter);

    let collected = Arc::new(Mutex::new(Vec::new()));
    exporter.add_sink(Box::new(CollectingSink {
        snapshots: collected.clone(),
    }));

    exporter.export();
    assert!(
        collected.lock().is_empty(),
        "disabled exporter should produce no output"
    );
}
