// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Event export to GCS.
//!
//! Buffers domain events (task status changes, actor lifecycle, etc.)
//! and flushes them in batches to GCS or another sink.
//! Ports the event-reporting path from C++ `event_reporter.cc`.

use std::sync::Arc;

use parking_lot::Mutex;

use crate::events::RayEvent;

/// Configuration for the event exporter.
#[derive(Debug, Clone)]
pub struct ExportConfig {
    /// Maximum events buffered before a forced flush.
    pub max_buffer_size: usize,
    /// Flush interval in milliseconds.
    pub flush_interval_ms: u64,
    /// Whether export is enabled.
    pub enabled: bool,
}

impl Default for ExportConfig {
    fn default() -> Self {
        Self {
            max_buffer_size: 1000,
            flush_interval_ms: 1000,
            enabled: true,
        }
    }
}

/// Trait for sinks that receive batches of events.
///
/// Implementations can send events to GCS, write to files, etc.
pub trait EventSink: Send + Sync {
    /// Flush a batch of events. Returns the number successfully sent.
    fn flush(&self, events: &[RayEvent]) -> usize;
}

/// A logging sink that writes events via tracing.
pub struct LoggingEventSink;

impl EventSink for LoggingEventSink {
    fn flush(&self, events: &[RayEvent]) -> usize {
        for event in events {
            tracing::info!(
                event_id = %event.event_id,
                source = ?event.source_type,
                severity = ?event.severity,
                label = %event.label,
                "{}",
                event.message,
            );
        }
        events.len()
    }
}

/// Buffered event exporter.
///
/// Accumulates events and periodically flushes them to a configured sink.
/// Thread-safe — can be shared across multiple producers.
pub struct EventExporter {
    config: ExportConfig,
    buffer: Mutex<Vec<RayEvent>>,
    sink: Mutex<Option<Box<dyn EventSink>>>,
    stats: Mutex<ExportStats>,
}

/// Statistics for the event exporter.
#[derive(Debug, Clone, Default)]
pub struct ExportStats {
    pub total_events_buffered: u64,
    pub total_events_flushed: u64,
    pub total_events_dropped: u64,
    pub total_flushes: u64,
}

impl EventExporter {
    /// Create a new event exporter.
    pub fn new(config: ExportConfig) -> Self {
        Self {
            config,
            buffer: Mutex::new(Vec::new()),
            sink: Mutex::new(None),
            stats: Mutex::new(ExportStats::default()),
        }
    }

    /// Set the event sink.
    pub fn set_sink(&self, sink: Box<dyn EventSink>) {
        *self.sink.lock() = Some(sink);
    }

    /// Add an event to the buffer.
    ///
    /// If the buffer exceeds `max_buffer_size`, the oldest event is dropped.
    pub fn add_event(&self, event: RayEvent) {
        if !self.config.enabled {
            return;
        }

        let mut buffer = self.buffer.lock();
        let mut stats = self.stats.lock();

        if buffer.len() >= self.config.max_buffer_size {
            buffer.remove(0);
            stats.total_events_dropped += 1;
        }

        buffer.push(event);
        stats.total_events_buffered += 1;
    }

    /// Flush all buffered events to the sink.
    ///
    /// Returns the number of events flushed.
    pub fn flush(&self) -> usize {
        let events: Vec<RayEvent> = {
            let mut buffer = self.buffer.lock();
            std::mem::take(&mut *buffer)
        };

        if events.is_empty() {
            return 0;
        }

        let flushed = {
            let sink = self.sink.lock();
            match &*sink {
                Some(s) => s.flush(&events),
                None => {
                    // No sink configured — events are lost.
                    0
                }
            }
        };

        let mut stats = self.stats.lock();
        stats.total_events_flushed += flushed as u64;
        stats.total_flushes += 1;

        flushed
    }

    /// Return the number of events currently in the buffer.
    pub fn buffer_len(&self) -> usize {
        self.buffer.lock().len()
    }

    /// Get export statistics.
    pub fn stats(&self) -> ExportStats {
        self.stats.lock().clone()
    }

    /// Get the configuration.
    pub fn config(&self) -> &ExportConfig {
        &self.config
    }

    /// Start periodic flush in a tokio task.
    pub fn start_periodic_flush(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        let interval = std::time::Duration::from_millis(self.config.flush_interval_ms);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                self.flush();
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::{EventSeverity, EventSourceType};
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn make_event(label: &str) -> RayEvent {
        RayEvent::new(
            EventSourceType::Gcs,
            EventSeverity::Info,
            label,
            format!("Test event: {label}"),
        )
    }

    #[test]
    fn test_export_config_default() {
        let config = ExportConfig::default();
        assert_eq!(config.max_buffer_size, 1000);
        assert_eq!(config.flush_interval_ms, 1000);
        assert!(config.enabled);
    }

    #[test]
    fn test_add_event_and_buffer_len() {
        let exporter = EventExporter::new(ExportConfig::default());
        assert_eq!(exporter.buffer_len(), 0);

        exporter.add_event(make_event("e1"));
        exporter.add_event(make_event("e2"));
        assert_eq!(exporter.buffer_len(), 2);
    }

    #[test]
    fn test_add_event_disabled() {
        let exporter = EventExporter::new(ExportConfig {
            enabled: false,
            ..Default::default()
        });
        exporter.add_event(make_event("e1"));
        assert_eq!(exporter.buffer_len(), 0);
    }

    #[test]
    fn test_flush_with_sink() {
        let flush_count = Arc::new(AtomicUsize::new(0));
        let fc = flush_count.clone();

        struct CountingSink(Arc<AtomicUsize>);
        impl EventSink for CountingSink {
            fn flush(&self, events: &[RayEvent]) -> usize {
                self.0.fetch_add(events.len(), Ordering::Relaxed);
                events.len()
            }
        }

        let exporter = EventExporter::new(ExportConfig::default());
        exporter.set_sink(Box::new(CountingSink(fc)));

        exporter.add_event(make_event("e1"));
        exporter.add_event(make_event("e2"));
        exporter.add_event(make_event("e3"));

        let flushed = exporter.flush();
        assert_eq!(flushed, 3);
        assert_eq!(flush_count.load(Ordering::Relaxed), 3);
        assert_eq!(exporter.buffer_len(), 0);

        let stats = exporter.stats();
        assert_eq!(stats.total_events_buffered, 3);
        assert_eq!(stats.total_events_flushed, 3);
        assert_eq!(stats.total_flushes, 1);
    }

    #[test]
    fn test_flush_without_sink() {
        let exporter = EventExporter::new(ExportConfig::default());
        exporter.add_event(make_event("e1"));
        let flushed = exporter.flush();
        assert_eq!(flushed, 0);
        // Buffer was drained even though events weren't delivered
        assert_eq!(exporter.buffer_len(), 0);
    }

    #[test]
    fn test_flush_empty_buffer() {
        let exporter = EventExporter::new(ExportConfig::default());
        let flushed = exporter.flush();
        assert_eq!(flushed, 0);
    }

    #[test]
    fn test_buffer_overflow_drops_oldest() {
        let exporter = EventExporter::new(ExportConfig {
            max_buffer_size: 3,
            ..Default::default()
        });

        exporter.add_event(make_event("e1"));
        exporter.add_event(make_event("e2"));
        exporter.add_event(make_event("e3"));
        assert_eq!(exporter.buffer_len(), 3);

        // Adding a 4th should drop the oldest
        exporter.add_event(make_event("e4"));
        assert_eq!(exporter.buffer_len(), 3);

        let stats = exporter.stats();
        assert_eq!(stats.total_events_buffered, 4);
        assert_eq!(stats.total_events_dropped, 1);

        // Verify the oldest was dropped
        struct CollectSink(Mutex<Vec<String>>);
        impl EventSink for CollectSink {
            fn flush(&self, events: &[RayEvent]) -> usize {
                let mut labels = self.0.lock();
                for e in events {
                    labels.push(e.label.clone());
                }
                events.len()
            }
        }

        let collected = Arc::new(CollectSink(Mutex::new(Vec::new())));
        exporter.set_sink(Box::new(CollectSink(Mutex::new(Vec::new()))));

        // Re-create to check labels
        let exporter2 = EventExporter::new(ExportConfig {
            max_buffer_size: 3,
            ..Default::default()
        });
        let sink = Arc::new(CollectSink(Mutex::new(Vec::new())));
        // We need a concrete check — add 4 events with small buffer
        exporter2.add_event(make_event("a"));
        exporter2.add_event(make_event("b"));
        exporter2.add_event(make_event("c"));
        exporter2.add_event(make_event("d")); // drops "a"

        // Use a simple sink to verify
        let labels: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let labels_clone = labels.clone();
        struct LabelSink(Arc<Mutex<Vec<String>>>);
        impl EventSink for LabelSink {
            fn flush(&self, events: &[RayEvent]) -> usize {
                let mut l = self.0.lock();
                for e in events {
                    l.push(e.label.clone());
                }
                events.len()
            }
        }
        exporter2.set_sink(Box::new(LabelSink(labels_clone)));
        exporter2.flush();

        let l = labels.lock();
        assert_eq!(*l, vec!["b", "c", "d"]);
        drop(collected);
        drop(sink);
    }

    #[test]
    fn test_logging_sink() {
        let sink = LoggingEventSink;
        let events = vec![make_event("test1"), make_event("test2")];
        let flushed = sink.flush(&events);
        assert_eq!(flushed, 2);
    }

    #[test]
    fn test_stats_initial() {
        let exporter = EventExporter::new(ExportConfig::default());
        let stats = exporter.stats();
        assert_eq!(stats.total_events_buffered, 0);
        assert_eq!(stats.total_events_flushed, 0);
        assert_eq!(stats.total_events_dropped, 0);
        assert_eq!(stats.total_flushes, 0);
    }

    #[test]
    fn test_multiple_flushes() {
        struct NoopSink;
        impl EventSink for NoopSink {
            fn flush(&self, events: &[RayEvent]) -> usize {
                events.len()
            }
        }

        let exporter = EventExporter::new(ExportConfig::default());
        exporter.set_sink(Box::new(NoopSink));

        exporter.add_event(make_event("batch1"));
        exporter.flush();

        exporter.add_event(make_event("batch2a"));
        exporter.add_event(make_event("batch2b"));
        exporter.flush();

        let stats = exporter.stats();
        assert_eq!(stats.total_events_buffered, 3);
        assert_eq!(stats.total_events_flushed, 3);
        assert_eq!(stats.total_flushes, 2);
    }

    #[test]
    fn test_config_accessor() {
        let config = ExportConfig {
            max_buffer_size: 500,
            flush_interval_ms: 2000,
            enabled: false,
        };
        let exporter = EventExporter::new(config);

        let cfg = exporter.config();
        assert_eq!(cfg.max_buffer_size, 500);
        assert_eq!(cfg.flush_interval_ms, 2000);
        assert!(!cfg.enabled);
    }

    #[test]
    fn test_disabled_exporter_ignores_events_and_flush() {
        let exporter = EventExporter::new(ExportConfig {
            enabled: false,
            ..Default::default()
        });

        // Adding multiple events when disabled should be silently ignored.
        exporter.add_event(make_event("e1"));
        exporter.add_event(make_event("e2"));
        exporter.add_event(make_event("e3"));
        assert_eq!(exporter.buffer_len(), 0);

        // Flush on an empty buffer should return 0 and not affect stats.
        let flushed = exporter.flush();
        assert_eq!(flushed, 0);

        let stats = exporter.stats();
        assert_eq!(stats.total_events_buffered, 0);
        assert_eq!(stats.total_events_flushed, 0);
        assert_eq!(stats.total_events_dropped, 0);
        assert_eq!(stats.total_flushes, 0);
    }
}
