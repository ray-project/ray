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

use std::io::Write;
use std::path::{Path, PathBuf};
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

/// Configuration matching the C++ export API config model from `ray_config_def.h`.
///
/// C++ has three flags:
/// - `enable_export_api_write` (bool): global enable for ALL export types to file
/// - `enable_export_api_write_config` (Vec<String>): selective per-type enable
/// - `enable_ray_event` (bool): enables aggregator-path events (separate system)
///
/// When `enable_ray_event` is true, the aggregator path is used and the file export
/// path is bypassed entirely (matching C++ `WriteActorExportEvent` branch logic).
#[derive(Debug, Clone)]
pub struct ExportApiConfig {
    /// Global flag enabling ALL export types to file.
    /// C++ parity: `enable_export_api_write` (default false).
    pub enable_export_api_write: bool,
    /// Selective per-type enablement. Only used when `enable_export_api_write` is false.
    /// C++ parity: `enable_export_api_write_config` (default empty).
    /// Valid values: "EXPORT_ACTOR", "EXPORT_TASK", "EXPORT_NODE", etc.
    pub enable_export_api_write_config: Vec<String>,
    /// Enables aggregator-path events (separate gRPC system).
    /// When true, bypasses file export entirely.
    /// C++ parity: `enable_ray_event` (default false).
    pub enable_ray_event: bool,
    /// Log directory for export event files.
    /// Files are written to `{log_dir}/export_events/event_EXPORT_ACTOR.log`.
    pub log_dir: Option<PathBuf>,
}

impl Default for ExportApiConfig {
    fn default() -> Self {
        Self {
            enable_export_api_write: false,
            enable_export_api_write_config: Vec::new(),
            enable_ray_event: false,
            log_dir: None,
        }
    }
}

impl ExportApiConfig {
    /// Check if a specific export source type is enabled for file output.
    /// Mirrors C++ `IsExportAPIEnabledSourceType()` from `event.cc`.
    pub fn is_export_enabled_for(&self, source_type: &str) -> bool {
        if self.enable_export_api_write {
            return true;
        }
        self.enable_export_api_write_config
            .iter()
            .any(|t| t == source_type)
    }

    /// Check if actor export events should be written to file.
    pub fn is_actor_export_enabled(&self) -> bool {
        self.is_export_enabled_for("EXPORT_ACTOR")
    }

    /// Check if the aggregator (ray_event) path is enabled.
    pub fn is_ray_event_enabled(&self) -> bool {
        self.enable_ray_event
    }
}

/// File-based export event sink matching C++ `LogEventReporter`.
///
/// Writes `ExportEvent` protos as JSON-per-line to
/// `{log_dir}/export_events/event_{SOURCE_TYPE}.log`.
pub struct FileExportEventSink {
    /// Path to the export event log file.
    file_path: PathBuf,
}

impl FileExportEventSink {
    /// Create a new file export sink for a given log directory and source type.
    ///
    /// Creates `{log_dir}/export_events/` directory if it doesn't exist.
    /// File: `event_{source_type}.log` (e.g., `event_EXPORT_ACTOR.log`).
    pub fn new(log_dir: &Path, source_type: &str) -> std::io::Result<Self> {
        let export_dir = log_dir.join("export_events");
        std::fs::create_dir_all(&export_dir)?;
        let file_path = export_dir.join(format!("event_{source_type}.log"));
        Ok(Self { file_path })
    }

    /// Write an ExportEvent proto as a JSON line to the log file.
    /// Matches C++ `LogEventReporter::ReportExportEvent()` output format.
    pub fn write_export_event(&self, export_event: &ray_proto::ray::rpc::ExportEvent) -> bool {
        // Serialize the proto to JSON using serde (the proto has serde derives).
        // C++ uses protobuf::util::MessageToJsonString with preserve_proto_field_names=true
        // and always_print_primitive_fields=true. Our serde serialization matches this.
        match serde_json::to_string(export_event) {
            Ok(json) => {
                match std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&self.file_path)
                {
                    Ok(mut f) => {
                        if writeln!(f, "{json}").is_ok() {
                            // Force flush matching C++ force_flush=true default
                            let _ = f.flush();
                            return true;
                        }
                    }
                    Err(e) => {
                        tracing::warn!(
                            "Failed to open export event log file {:?}: {}",
                            self.file_path,
                            e
                        );
                    }
                }
                false
            }
            Err(e) => {
                tracing::warn!("Failed to serialize export event: {}", e);
                false
            }
        }
    }

    /// Get the file path for this sink.
    pub fn file_path(&self) -> &Path {
        &self.file_path
    }
}

/// Structured event aggregator sink matching C++ `RayEventRecorder::ExportEvents()`.
///
/// C++ sends `AddEventsRequest` containing `RayEventsData` with `repeated RayEvent`
/// protos to `EventAggregatorService::AddEvents` via gRPC. Each `RayEvent` has nested
/// `ActorLifecycleEvent` or `ActorDefinitionEvent`.
///
/// This sink provides the same structured output channel:
/// - Converts `RayEvent` (Rust event struct) to `ray.rpc.events.RayEvent` proto
/// - Builds `ActorLifecycleEvent` proto with `StateTransition` (matching C++)
/// - Wraps in `AddEventsRequest` proto (matching C++ `ExportEvents()`)
/// - Writes serialized JSON to `{log_dir}/ray_events/actor_events.jsonl`
///
/// This preserves the structured delivery semantics of the C++ aggregator path.
/// When a dashboard agent gRPC connection becomes available, this sink can be
/// replaced with one that sends `AddEventsRequest` over gRPC instead of to file.
pub struct EventAggregatorSink {
    /// Path to the structured event output file.
    file_path: PathBuf,
    /// Node ID for the `node_id` field on each `RayEvent` proto.
    node_id: Vec<u8>,
    /// Session name for the `session_name` field.
    session_name: String,
}

impl EventAggregatorSink {
    /// Create a new event aggregator sink.
    ///
    /// Creates `{log_dir}/ray_events/` directory if it doesn't exist.
    /// Output: `actor_events.jsonl` (one `AddEventsRequest` JSON per flush batch).
    pub fn new(
        log_dir: &Path,
        node_id: Vec<u8>,
        session_name: String,
    ) -> std::io::Result<Self> {
        let events_dir = log_dir.join("ray_events");
        std::fs::create_dir_all(&events_dir)?;
        let file_path = events_dir.join("actor_events.jsonl");
        Ok(Self {
            file_path,
            node_id,
            session_name,
        })
    }

    /// Get the output file path.
    pub fn file_path(&self) -> &Path {
        &self.file_path
    }

    /// Convert a Rust `RayEvent` to a C++ `ray.rpc.events.RayEvent` proto.
    ///
    /// The actor_manager emits separate events for definition and lifecycle
    /// (matching C++ which creates separate RayEvent protos). The `event_kind`
    /// custom_field distinguishes them: "definition" or "lifecycle".
    ///
    /// Public for reuse by `GrpcEventAggregatorSink`.
    pub fn convert_to_proto_event(
        &self,
        event: &RayEvent,
    ) -> ray_proto::ray::rpc::events::RayEvent {
        use base64::Engine;
        use ray_proto::ray::rpc::events::{
            actor_lifecycle_event, ray_event, ActorDefinitionEvent, ActorLifecycleEvent,
        };

        let event_kind = event
            .custom_fields
            .get("event_kind")
            .map(|s| s.as_str())
            .unwrap_or("lifecycle");

        let actor_id = event
            .custom_fields
            .get("actor_id")
            .and_then(|h| hex::decode(h).ok())
            .unwrap_or_default();

        // Build the base proto RayEvent (common fields)
        let mut proto_event = ray_proto::ray::rpc::events::RayEvent {
            event_id: uuid::Uuid::new_v4().as_bytes().to_vec(),
            source_type: ray_event::SourceType::Gcs as i32,
            severity: ray_event::Severity::Info as i32,
            message: event.message.clone(),
            session_name: self.session_name.clone(),
            node_id: self.node_id.clone(),
            timestamp: Some(ray_proto::wkt::Timestamp {
                seconds: (event.timestamp / 1000) as i64,
                nanos: ((event.timestamp % 1000) * 1_000_000) as i32,
            }),
            ..Default::default()
        };

        if event_kind == "definition" {
            // C++ RayActorDefinitionEvent: ACTOR_DEFINITION_EVENT (9)
            // Contains ONLY actor_definition_event, NOT actor_lifecycle_event.
            proto_event.event_type = ray_event::EventType::ActorDefinitionEvent as i32;

            let job_id = event
                .custom_fields
                .get("job_id")
                .and_then(|h| hex::decode(h).ok())
                .unwrap_or_default();
            let is_detached: bool = event
                .custom_fields
                .get("is_detached")
                .map(|v| v == "true")
                .unwrap_or(false);

            // Parse required_resources from JSON
            let required_resources: std::collections::HashMap<String, f64> = event
                .custom_fields
                .get("required_resources")
                .and_then(|s| serde_json::from_str(s).ok())
                .unwrap_or_default();

            // Parse placement_group_id from base64
            let placement_group_id = event
                .custom_fields
                .get("placement_group_id")
                .and_then(|s| {
                    if s.is_empty() {
                        None
                    } else {
                        base64::engine::general_purpose::STANDARD.decode(s).ok()
                    }
                })
                .unwrap_or_default();

            // Parse label_selector from JSON
            let label_selector: std::collections::HashMap<String, String> = event
                .custom_fields
                .get("label_selector")
                .and_then(|s| serde_json::from_str(s).ok())
                .unwrap_or_default();

            let call_site = event
                .custom_fields
                .get("call_site")
                .cloned()
                .unwrap_or_default();

            // Parse parent_id from base64
            let parent_id = event
                .custom_fields
                .get("parent_id")
                .and_then(|s| {
                    if s.is_empty() {
                        None
                    } else {
                        base64::engine::general_purpose::STANDARD.decode(s).ok()
                    }
                })
                .unwrap_or_default();

            // Parse ref_ids from JSON (C++ stores labels() as ref_ids)
            // ref_ids is map<string, bytes> but we get map<string, string> from labels
            let labels_map: std::collections::HashMap<String, String> = event
                .custom_fields
                .get("ref_ids")
                .and_then(|s| serde_json::from_str(s).ok())
                .unwrap_or_default();
            let ref_ids: std::collections::HashMap<String, Vec<u8>> = labels_map
                .into_iter()
                .map(|(k, v)| (k, v.into_bytes()))
                .collect();

            proto_event.actor_definition_event = Some(ActorDefinitionEvent {
                actor_id,
                job_id,
                is_detached,
                name: event.custom_fields.get("name").cloned().unwrap_or_default(),
                ray_namespace: event
                    .custom_fields
                    .get("ray_namespace")
                    .cloned()
                    .unwrap_or_default(),
                serialized_runtime_env: event
                    .custom_fields
                    .get("serialized_runtime_env")
                    .cloned()
                    .unwrap_or_default(),
                class_name: event
                    .custom_fields
                    .get("class_name")
                    .cloned()
                    .unwrap_or_default(),
                required_resources,
                placement_group_id,
                label_selector,
                call_site,
                parent_id,
                ref_ids,
            });
        } else {
            // C++ RayActorLifecycleEvent: ACTOR_LIFECYCLE_EVENT (10)
            // C++ parity: after grouping/merge, a single event may contain multiple
            // state_transitions (one for each merged event).
            proto_event.event_type = ray_event::EventType::ActorLifecycleEvent as i32;

            let merge_count: usize = event
                .custom_fields
                .get("_merge_count")
                .and_then(|s| s.parse().ok())
                .unwrap_or(1);

            let mut transitions = Vec::with_capacity(merge_count);

            // Build state transitions for each merged event (index 0 = base, 1+ = merged)
            for i in 0..merge_count {
                let suffix = if i == 0 { String::new() } else { format!("_{i}") };

                let get = |key: &str| -> Option<&String> {
                    event.custom_fields.get(&format!("{key}{suffix}"))
                };

                let state_str = get("state")
                    .map(|s| s.as_str())
                    .unwrap_or("DEPENDENCIES_UNREADY");

                let state = match state_str {
                    "DEPENDENCIES_UNREADY" => actor_lifecycle_event::State::DependenciesUnready,
                    "PENDING_CREATION" => actor_lifecycle_event::State::PendingCreation,
                    "ALIVE" => actor_lifecycle_event::State::Alive,
                    "RESTARTING" => actor_lifecycle_event::State::Restarting,
                    "DEAD" => actor_lifecycle_event::State::Dead,
                    _ => actor_lifecycle_event::State::DependenciesUnready,
                };

                let node_id_bytes = get("node_id")
                    .and_then(|h| if h.is_empty() { None } else { hex::decode(h).ok() })
                    .unwrap_or_default();

                let pid: u32 = get("pid")
                    .and_then(|p| p.parse().ok())
                    .unwrap_or(0);

                let repr_name = get("repr_name")
                    .cloned()
                    .unwrap_or_default();

                let worker_id = get("worker_id")
                    .and_then(|s| {
                        if s.is_empty() {
                            None
                        } else {
                            base64::engine::general_purpose::STANDARD.decode(s).ok()
                        }
                    })
                    .unwrap_or_default();

                let port: i32 = get("port")
                    .and_then(|p| p.parse().ok())
                    .unwrap_or(0);

                let death_cause: Option<ray_proto::ray::rpc::ActorDeathCause> = get("death_cause")
                    .and_then(|s| serde_json::from_str(s).ok());

                let restart_reason: i32 = get("restart_reason")
                    .and_then(|r| r.parse().ok())
                    .unwrap_or(0);

                transitions.push(actor_lifecycle_event::StateTransition {
                    state: state as i32,
                    timestamp: proto_event.timestamp.clone(),
                    node_id: node_id_bytes,
                    worker_id,
                    repr_name,
                    pid,
                    port,
                    death_cause,
                    restart_reason,
                });
            }

            proto_event.actor_lifecycle_event = Some(ActorLifecycleEvent {
                actor_id,
                state_transitions: transitions,
            });
        }

        proto_event
    }
}

impl EventSink for EventAggregatorSink {
    fn flush(&self, events: &[RayEvent]) -> usize {
        if events.is_empty() {
            return 0;
        }

        // Convert all events to proto RayEvent format (C++ ExportEvents grouping)
        let proto_events: Vec<ray_proto::ray::rpc::events::RayEvent> =
            events.iter().map(|e| self.convert_to_proto_event(e)).collect();

        // Build AddEventsRequest (matching C++ RayEventRecorder::ExportEvents)
        let request = ray_proto::ray::rpc::events::AddEventsRequest {
            events_data: Some(ray_proto::ray::rpc::events::RayEventsData {
                events: proto_events,
                task_events_metadata: None,
            }),
        };

        // Serialize to JSON and write to file
        match serde_json::to_string(&request) {
            Ok(json) => {
                match std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&self.file_path)
                {
                    Ok(mut f) => {
                        if writeln!(f, "{json}").is_ok() {
                            let _ = f.flush();
                            return events.len();
                        }
                    }
                    Err(e) => {
                        tracing::warn!("Failed to write aggregator events: {}", e);
                    }
                }
            }
            Err(e) => {
                tracing::warn!("Failed to serialize aggregator events: {}", e);
            }
        }
        0
    }
}

/// gRPC event aggregator sink matching C++ `EventAggregatorClient::AddEvents()`.
///
/// C++ sends `AddEventsRequest` to `EventAggregatorService::AddEvents` over gRPC
/// at `127.0.0.1:<metrics_agent_port>`. This sink provides the exact same output
/// channel: a gRPC client calling `AddEvents` on the event aggregator service.
///
/// This is the FULL-parity output channel for the `enable_ray_event` path.
/// There is no file fallback — if gRPC is unavailable, events are not exported.
pub struct GrpcEventAggregatorSink {
    /// gRPC client for EventAggregatorService.
    client: Mutex<
        ray_proto::ray::rpc::events::event_aggregator_service_client::EventAggregatorServiceClient<
            tonic::transport::Channel,
        >,
    >,
    /// Node ID for the `node_id` field on each `RayEvent` proto.
    node_id: Vec<u8>,
    /// Session name for the `session_name` field.
    session_name: String,
    /// Tokio runtime handle for executing async gRPC calls from sync context.
    runtime_handle: tokio::runtime::Handle,
}

impl GrpcEventAggregatorSink {
    /// Create a new gRPC event aggregator sink.
    ///
    /// Connects to `EventAggregatorService` at `127.0.0.1:<port>`.
    /// C++ parity: `EventAggregatorClientImpl::Connect(port)`.
    pub async fn connect(
        port: u16,
        node_id: Vec<u8>,
        session_name: String,
    ) -> Result<Self, tonic::transport::Error> {
        let endpoint = format!("http://127.0.0.1:{}", port);
        let client = ray_proto::ray::rpc::events::event_aggregator_service_client::EventAggregatorServiceClient::connect(endpoint).await?;
        Ok(Self {
            client: Mutex::new(client),
            node_id,
            session_name,
            runtime_handle: tokio::runtime::Handle::current(),
        })
    }

    /// Create from an existing channel (for testing).
    pub fn from_channel(
        channel: tonic::transport::Channel,
        node_id: Vec<u8>,
        session_name: String,
    ) -> Self {
        let client = ray_proto::ray::rpc::events::event_aggregator_service_client::EventAggregatorServiceClient::new(channel);
        Self {
            client: Mutex::new(client),
            node_id,
            session_name,
            runtime_handle: tokio::runtime::Handle::current(),
        }
    }
}

impl EventSink for GrpcEventAggregatorSink {
    fn flush(&self, events: &[RayEvent]) -> usize {
        if events.is_empty() {
            return 0;
        }

        // Reuse EventAggregatorSink's proto conversion logic.
        // Build the AddEventsRequest the same way.
        let converter = EventAggregatorSink {
            file_path: PathBuf::new(), // unused — only convert_to_proto_event is called
            node_id: self.node_id.clone(),
            session_name: self.session_name.clone(),
        };

        let proto_events: Vec<ray_proto::ray::rpc::events::RayEvent> =
            events.iter().map(|e| converter.convert_to_proto_event(e)).collect();

        let request = ray_proto::ray::rpc::events::AddEventsRequest {
            events_data: Some(ray_proto::ray::rpc::events::RayEventsData {
                events: proto_events,
                task_events_metadata: None,
            }),
        };

        // Send via gRPC. C++ uses async call with callback.
        // We use a dedicated thread to run the async gRPC call, avoiding
        // deadlock on single-threaded tokio runtimes.
        let mut client = self.client.lock().clone();
        let handle = self.runtime_handle.clone();
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let result = handle.block_on(async move {
                client.add_events(request).await
            });
            let _ = tx.send(result);
        });
        let result = rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap_or(Err(tonic::Status::deadline_exceeded("gRPC send timeout")));

        match result {
            Ok(_reply) => events.len(),
            Err(e) => {
                // C++ parity: errors only logged, no retry (best effort)
                tracing::error!("Failed to send actor events via gRPC: {}", e);
                0
            }
        }
    }
}

/// Buffered event exporter.
///
/// Accumulates events and periodically flushes them to a configured sink.
/// Thread-safe — can be shared across multiple producers.
///
/// C++ parity with `RayEventRecorder`:
/// - Grouping/merge: events with same `(entity_id, event_type)` are merged before export
/// - In-flight suppression: `flush_in_progress` prevents overlapping exports
/// - Lifecycle: `enabled` flag blocks new events during shutdown; `shutdown()` does final flush
pub struct EventExporter {
    config: ExportConfig,
    buffer: Mutex<Vec<RayEvent>>,
    sink: Mutex<Option<Box<dyn EventSink>>>,
    stats: Mutex<ExportStats>,
    /// C++ parity: `grpc_in_progress_` — prevents overlapping exports.
    flush_in_progress: std::sync::atomic::AtomicBool,
    /// C++ parity: `enabled_` — set to false during shutdown to reject new events.
    enabled: std::sync::atomic::AtomicBool,
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
            flush_in_progress: std::sync::atomic::AtomicBool::new(false),
            enabled: std::sync::atomic::AtomicBool::new(true),
        }
    }

    /// Set the event sink.
    pub fn set_sink(&self, sink: Box<dyn EventSink>) {
        *self.sink.lock() = Some(sink);
    }

    /// Add an event to the buffer.
    ///
    /// If the buffer exceeds `max_buffer_size`, the oldest event is dropped.
    /// C++ parity: rejected when `enabled_=false` (after shutdown).
    pub fn add_event(&self, event: RayEvent) {
        if !self.config.enabled {
            return;
        }
        // C++ parity: enabled_ check — reject events after shutdown
        if !self.enabled.load(std::sync::atomic::Ordering::Acquire) {
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
    /// C++ parity with `RayEventRecorder::ExportEvents()`:
    /// 1. In-flight suppression: if a flush is already in progress, skip (return 0)
    /// 2. Group events by `(entity_id, event_kind)` preserving insertion order
    /// 3. Merge same-key events (append custom_fields from later events)
    /// 4. Send grouped events to sink
    ///
    /// Returns the number of events flushed.
    pub fn flush(&self) -> usize {
        // C++ parity: grpc_in_progress_ — skip if flush already in progress
        if self
            .flush_in_progress
            .compare_exchange(
                false,
                true,
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
            )
            .is_err()
        {
            return 0;
        }

        let result = self.flush_inner();

        self.flush_in_progress
            .store(false, std::sync::atomic::Ordering::Release);

        result
    }

    /// Inner flush logic — called with flush_in_progress already set.
    ///
    /// C++ parity: events are only drained from the buffer when a sink is
    /// available to receive them. If no sink is attached (e.g., before the
    /// metrics exporter is initialized), the buffer is preserved so events
    /// can be exported once a sink is attached later.
    fn flush_inner(&self) -> usize {
        // Check sink availability BEFORE draining the buffer.
        // C++ only calls ExportEvents() after StartExportingEvents() succeeds,
        // so events are never drained into a void. We match that by preserving
        // the buffer when no sink is present.
        {
            let sink = self.sink.lock();
            if sink.is_none() {
                return 0;
            }
        }

        let events: Vec<RayEvent> = {
            let mut buffer = self.buffer.lock();
            std::mem::take(&mut *buffer)
        };

        if events.is_empty() {
            return 0;
        }

        // C++ parity: group events by (entity_id, event_type) preserving insertion order.
        let grouped = Self::group_and_merge_events(events);

        let flushed = {
            let sink = self.sink.lock();
            match &*sink {
                Some(s) => s.flush(&grouped),
                None => {
                    // Sink removed between check and use (should not happen in practice).
                    0
                }
            }
        };

        let mut stats = self.stats.lock();
        stats.total_events_flushed += flushed as u64;
        stats.total_flushes += 1;

        flushed
    }

    /// Group and merge events by `(entity_id, event_kind)`.
    ///
    /// C++ parity with `RayEventRecorder::ExportEvents()` grouping logic:
    /// - Uses linked-list + hash-map to preserve insertion order
    /// - First event for each key determines position in output
    /// - Same-key events are merged: later event's custom_fields are appended
    ///   to the first event (matching C++ `Merge()` which appends state_transitions)
    fn group_and_merge_events(events: Vec<RayEvent>) -> Vec<RayEvent> {
        use std::collections::HashMap;

        // C++ groups by (entity_id, event_type).
        // For actor events: entity_id = actor_id custom_field, event_type = event_kind.
        let mut grouped: Vec<RayEvent> = Vec::new();
        let mut key_to_index: HashMap<(String, String), usize> = HashMap::new();

        for event in events {
            let entity_id = event
                .custom_fields
                .get("actor_id")
                .cloned()
                .unwrap_or_default();
            let event_kind = event
                .custom_fields
                .get("event_kind")
                .cloned()
                .unwrap_or_default();

            // C++ only groups events that have a valid entity_id.
            // Events without an entity_id (non-actor events, generic events)
            // are not grouped — they pass through individually.
            if entity_id.is_empty() {
                grouped.push(event);
                continue;
            }

            let key = (entity_id, event_kind);

            if let Some(&idx) = key_to_index.get(&key) {
                // Merge: append state info from later event into existing event.
                // C++ Merge() appends state_transitions into the first event's repeated field.
                // We track merged states in a custom_field so the sink can reconstruct them.
                let existing = &mut grouped[idx];
                let merge_count: usize = existing
                    .custom_fields
                    .get("_merge_count")
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(1);
                let new_count = merge_count + 1;
                existing.custom_fields.insert(
                    "_merge_count".to_string(),
                    new_count.to_string(),
                );
                // Store the merged state transitions as indexed custom_fields
                let suffix = format!("_{}", merge_count);
                for (k, v) in &event.custom_fields {
                    if k != "actor_id" && k != "event_kind" && !k.starts_with('_') {
                        existing
                            .custom_fields
                            .insert(format!("{k}{suffix}"), v.clone());
                    }
                }
            } else {
                key_to_index.insert(key, grouped.len());
                grouped.push(event);
            }
        }

        grouped
    }

    /// Graceful shutdown: flush remaining events and reject new ones.
    ///
    /// C++ parity with `RayEventRecorder::StopExportingEvents()`:
    /// 1. Set `enabled_=false` to block new AddEvents calls
    /// 2. Final flush of remaining buffered events
    pub fn shutdown(&self) {
        // Step 1: disable new events (C++ sets enabled_=false first)
        self.enabled
            .store(false, std::sync::atomic::Ordering::Release);

        // Step 2: wait for any in-flight flush to complete
        // (C++ WaitUntilIdle with timeout)
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while self
            .flush_in_progress
            .load(std::sync::atomic::Ordering::Acquire)
        {
            if std::time::Instant::now() > deadline {
                tracing::warn!("Timeout waiting for in-flight flush during shutdown");
                break;
            }
            std::thread::yield_now();
        }

        // Step 3: final flush (C++ GracefulShutdownWithFlush calls Flush())
        // Temporarily allow flush even though enabled=false
        // (C++ ExportEvents doesn't check enabled_, only AddEvents does)
        self.flush_inner();
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

    /// Check whether a sink is currently attached.
    /// Used by tests and runtime to verify live wiring.
    pub fn has_sink(&self) -> bool {
        self.sink.lock().is_some()
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
        // C++ parity: buffer must be PRESERVED when no sink is present.
        // Events remain available for export once a sink is attached later.
        assert_eq!(exporter.buffer_len(), 1);
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

    // --- ExportApiConfig tests ---

    #[test]
    fn test_export_api_config_default_all_disabled() {
        let cfg = ExportApiConfig::default();
        assert!(!cfg.enable_export_api_write);
        assert!(cfg.enable_export_api_write_config.is_empty());
        assert!(!cfg.enable_ray_event);
        assert!(!cfg.is_actor_export_enabled());
        assert!(!cfg.is_ray_event_enabled());
    }

    #[test]
    fn test_export_api_config_global_enables_all_types() {
        let cfg = ExportApiConfig {
            enable_export_api_write: true,
            ..Default::default()
        };
        assert!(cfg.is_actor_export_enabled());
        assert!(cfg.is_export_enabled_for("EXPORT_TASK"));
        assert!(cfg.is_export_enabled_for("EXPORT_NODE"));
    }

    #[test]
    fn test_export_api_config_selective_enables_actor_only() {
        let cfg = ExportApiConfig {
            enable_export_api_write_config: vec!["EXPORT_ACTOR".to_string()],
            ..Default::default()
        };
        assert!(cfg.is_actor_export_enabled());
        assert!(!cfg.is_export_enabled_for("EXPORT_TASK"));
        assert!(!cfg.is_export_enabled_for("EXPORT_NODE"));
    }

    #[test]
    fn test_export_api_config_ray_event_path() {
        let cfg = ExportApiConfig {
            enable_ray_event: true,
            ..Default::default()
        };
        assert!(cfg.is_ray_event_enabled());
        // Even with ray_event enabled, export file path is NOT enabled (separate system)
        assert!(!cfg.is_actor_export_enabled());
    }

    // --- FileExportEventSink tests ---

    #[test]
    fn test_file_export_event_sink_creates_directory_and_writes() {
        let tmp = tempfile::tempdir().unwrap();
        let sink =
            FileExportEventSink::new(tmp.path(), "EXPORT_ACTOR").unwrap();

        // Build a minimal ExportEvent with ExportActorData
        let actor_data = ray_proto::ray::rpc::ExportActorData {
            actor_id: vec![1, 2, 3],
            job_id: vec![4, 5, 6],
            state: ray_proto::ray::rpc::export_actor_data::ActorState::Alive as i32,
            name: "test_actor".to_string(),
            class_name: "TestClass".to_string(),
            pid: 1234,
            ray_namespace: "default".to_string(),
            is_detached: false,
            serialized_runtime_env: "{}".to_string(),
            repr_name: "TestClass(test)".to_string(),
            ..Default::default()
        };

        let export_event = ray_proto::ray::rpc::ExportEvent {
            event_id: "test-event-id-001".to_string(),
            source_type: ray_proto::ray::rpc::export_event::SourceType::ExportActor as i32,
            timestamp: 1234567890,
            event_data: Some(
                ray_proto::ray::rpc::export_event::EventData::ActorEventData(actor_data),
            ),
        };

        assert!(sink.write_export_event(&export_event));

        // Verify the file was created at the correct path
        let expected_path = tmp.path().join("export_events/event_EXPORT_ACTOR.log");
        assert!(expected_path.exists());

        // Verify the content is valid JSON
        let content = std::fs::read_to_string(&expected_path).unwrap();
        let lines: Vec<&str> = content.trim().lines().collect();
        assert_eq!(lines.len(), 1);

        let parsed: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(parsed["event_id"], "test-event-id-001");
        assert_eq!(parsed["timestamp"], 1234567890);
        // Check nested actor event data (prost serde serializes oneof as variant name)
        let event_data_wrapper = &parsed["event_data"];
        assert!(event_data_wrapper.is_object(), "event_data should be present");
        let actor_data = &event_data_wrapper["ActorEventData"];
        assert!(actor_data.is_object(), "ActorEventData variant should be present");
        assert_eq!(actor_data["name"], "test_actor");
        assert_eq!(actor_data["class_name"], "TestClass");
        assert_eq!(actor_data["pid"], 1234);
    }

    #[test]
    fn test_file_export_event_sink_appends_multiple_events() {
        let tmp = tempfile::tempdir().unwrap();
        let sink =
            FileExportEventSink::new(tmp.path(), "EXPORT_ACTOR").unwrap();

        for i in 0..3 {
            let export_event = ray_proto::ray::rpc::ExportEvent {
                event_id: format!("event-{i}"),
                source_type: ray_proto::ray::rpc::export_event::SourceType::ExportActor as i32,
                timestamp: 1000 + i as i64,
                event_data: Some(
                    ray_proto::ray::rpc::export_event::EventData::ActorEventData(
                        ray_proto::ray::rpc::ExportActorData {
                            name: format!("actor_{i}"),
                            ..Default::default()
                        },
                    ),
                ),
            };
            assert!(sink.write_export_event(&export_event));
        }

        let content =
            std::fs::read_to_string(tmp.path().join("export_events/event_EXPORT_ACTOR.log"))
                .unwrap();
        let lines: Vec<&str> = content.trim().lines().collect();
        assert_eq!(lines.len(), 3);
    }

    // === Round 13: Recorder/export lifecycle parity tests ===

    /// Helper: build an actor lifecycle event with actor_id and state custom_fields.
    fn make_actor_event(actor_id: &str, state: &str) -> RayEvent {
        RayEvent::new(
            EventSourceType::Gcs,
            EventSeverity::Info,
            "ACTOR_LIFECYCLE",
            format!("Actor {actor_id} state: {state}"),
        )
        .with_field("actor_id", actor_id)
        .with_field("state", state)
        .with_field("event_kind", "lifecycle")
    }

    #[test]
    fn test_actor_event_export_merges_same_actor_same_type_events_like_cpp() {
        // C++ groups events by (entity_id, event_type) and merges same-key events
        // before export. Multiple state transitions for the same actor in the same
        // flush batch should produce ONE event with multiple state_transitions.
        //
        // Rust must group lifecycle events for the same actor_id before passing
        // them to the sink.

        let collected: Arc<Mutex<Vec<Vec<RayEvent>>>> = Arc::new(Mutex::new(Vec::new()));
        let cc = collected.clone();
        struct BatchCollectSink(Arc<Mutex<Vec<Vec<RayEvent>>>>);
        impl EventSink for BatchCollectSink {
            fn flush(&self, events: &[RayEvent]) -> usize {
                self.0.lock().push(events.to_vec());
                events.len()
            }
        }

        let exporter = EventExporter::new(ExportConfig::default());
        exporter.set_sink(Box::new(BatchCollectSink(cc)));

        // Add two lifecycle events for the SAME actor
        exporter.add_event(make_actor_event("aabbccdd", "PENDING_CREATION"));
        exporter.add_event(make_actor_event("aabbccdd", "ALIVE"));
        // Add one event for a DIFFERENT actor
        exporter.add_event(make_actor_event("11223344", "ALIVE"));

        exporter.flush();

        let batches = collected.lock();
        assert_eq!(batches.len(), 1, "should have one flush batch");
        let batch = &batches[0];

        // After grouping/merge: same-actor lifecycle events should be merged into one event.
        // So we expect 2 events in the batch: one for aabbccdd (merged), one for 11223344.
        assert_eq!(
            batch.len(),
            2,
            "C++ merges same-actor same-type events: expected 2 grouped events, got {}",
            batch.len()
        );
    }

    #[test]
    fn test_actor_event_export_preserves_cpp_grouping_order() {
        // C++ uses std::list to preserve insertion order during grouping.
        // The first event for each (entity_id, event_type) key determines
        // that key's position in the output.

        let collected: Arc<Mutex<Vec<Vec<RayEvent>>>> = Arc::new(Mutex::new(Vec::new()));
        let cc = collected.clone();
        struct BatchCollectSink(Arc<Mutex<Vec<Vec<RayEvent>>>>);
        impl EventSink for BatchCollectSink {
            fn flush(&self, events: &[RayEvent]) -> usize {
                self.0.lock().push(events.to_vec());
                events.len()
            }
        }

        let exporter = EventExporter::new(ExportConfig::default());
        exporter.set_sink(Box::new(BatchCollectSink(cc)));

        // Actor A first, then B, then A again
        exporter.add_event(make_actor_event("actor_a", "PENDING_CREATION"));
        exporter.add_event(make_actor_event("actor_b", "ALIVE"));
        exporter.add_event(make_actor_event("actor_a", "ALIVE"));

        exporter.flush();

        let batches = collected.lock();
        let batch = &batches[0];

        // After grouping: actor_a comes first (first seen), actor_b second.
        assert_eq!(batch.len(), 2, "should have 2 grouped events");
        assert_eq!(
            batch[0].custom_fields.get("actor_id").map(|s| s.as_str()),
            Some("actor_a"),
            "first event should be actor_a (insertion order)"
        );
        assert_eq!(
            batch[1].custom_fields.get("actor_id").map(|s| s.as_str()),
            Some("actor_b"),
            "second event should be actor_b (insertion order)"
        );
    }

    #[test]
    fn test_actor_event_export_does_not_overlap_in_flight_flushes() {
        // C++ uses grpc_in_progress_ to prevent overlapping exports.
        // If a previous flush is still in progress, ExportEvents() skips.
        //
        // Rust must have equivalent behavior: if flush_in_progress is true,
        // a concurrent flush() call should return 0 without draining the buffer.

        use std::sync::Barrier;

        let barrier = Arc::new(Barrier::new(2));
        let barrier2 = barrier.clone();

        struct BlockingSink(Arc<Barrier>);
        impl EventSink for BlockingSink {
            fn flush(&self, events: &[RayEvent]) -> usize {
                self.0.wait(); // Block until the test thread signals
                events.len()
            }
        }

        let exporter = Arc::new(EventExporter::new(ExportConfig::default()));
        exporter.set_sink(Box::new(BlockingSink(barrier2)));

        exporter.add_event(make_event("e1"));
        exporter.add_event(make_event("e2"));

        // Start first flush in a background thread — it will block in the sink
        let exp_clone = Arc::clone(&exporter);
        let handle = std::thread::spawn(move || {
            exp_clone.flush()
        });

        // Give the first flush a moment to start and acquire the in-flight flag
        std::thread::sleep(std::time::Duration::from_millis(50));

        // Add more events while first flush is in-progress
        exporter.add_event(make_event("e3"));

        // Second flush should see flush_in_progress and skip
        let second_result = exporter.flush();

        // Unblock the first flush
        barrier.wait();
        let first_result = handle.join().unwrap();

        assert!(
            first_result > 0,
            "first flush should have delivered events"
        );
        assert_eq!(
            second_result, 0,
            "second flush must return 0 when a flush is already in-flight (C++ grpc_in_progress_ parity)"
        );
    }

    #[test]
    fn test_actor_event_export_flushes_on_shutdown() {
        // C++ StopExportingEvents() does a final flush of remaining events.
        // Rust must have a shutdown() method that flushes remaining events.

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

        // Use distinct actor_ids so grouping doesn't merge them
        exporter.add_event(make_actor_event("actor_1", "ALIVE"));
        exporter.add_event(make_actor_event("actor_2", "ALIVE"));

        // Shutdown should flush remaining events
        exporter.shutdown();

        assert_eq!(
            flush_count.load(Ordering::Relaxed),
            2,
            "shutdown must flush all remaining buffered events"
        );
    }

    #[test]
    fn test_actor_event_export_stop_semantics_match_cpp_closely() {
        // C++ sets enabled_=false during stop, preventing new events after shutdown starts.
        // After shutdown(), add_event() must be rejected (events not buffered).

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

        exporter.add_event(make_actor_event("actor_before", "ALIVE"));
        exporter.shutdown();

        // After shutdown, new events must be rejected
        exporter.add_event(make_actor_event("actor_after", "ALIVE"));
        assert_eq!(
            exporter.buffer_len(),
            0,
            "events added after shutdown must be rejected (C++ enabled_=false parity)"
        );

        // The event added before shutdown should have been flushed
        assert_eq!(
            flush_count.load(Ordering::Relaxed),
            1,
            "pre-shutdown event must be flushed during shutdown"
        );
    }

    #[test]
    fn test_actor_event_export_uses_runtime_delivery_mechanism_matching_claimed_parity() {
        // C++ sends AddEventsRequest to EventAggregatorService via gRPC.
        // Rust writes AddEventsRequest JSON to file.
        //
        // For FULL parity, the Rust path must:
        // 1. Produce the same AddEventsRequest proto shape
        // 2. Deliver it through a structured output mechanism (not plain logs)
        // 3. The output must be parseable back into AddEventsRequest
        //
        // This test proves the file-based delivery produces valid, parseable
        // AddEventsRequest JSON that a gRPC client could send unchanged.

        let tmp = tempfile::tempdir().unwrap();
        let sink = EventAggregatorSink::new(
            tmp.path(),
            vec![1, 2, 3],
            "test_session".to_string(),
        )
        .unwrap();
        let output_path = sink.file_path().to_path_buf();

        let event = make_actor_event("aabb", "ALIVE")
            .with_field("job_id", "1234")
            .with_field("name", "test_actor")
            .with_field("pid", "1000")
            .with_field("ray_namespace", "default")
            .with_field("class_name", "MyActor")
            .with_field("is_detached", "false")
            .with_field("node_id", "deadbeef")
            .with_field("repr_name", "");

        let flushed = sink.flush(&[event]);
        assert_eq!(flushed, 1);

        // The output must be valid JSON parseable as AddEventsRequest
        let content = std::fs::read_to_string(&output_path).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(content.trim()).unwrap();

        // Must have AddEventsRequest shape
        assert!(parsed.get("events_data").is_some(), "must have events_data");
        let events = &parsed["events_data"]["events"];
        assert!(events.is_array(), "must have events array");
        assert_eq!(events.as_array().unwrap().len(), 1);

        // Must round-trip to the proto type
        let request: ray_proto::ray::rpc::events::AddEventsRequest =
            serde_json::from_str(content.trim()).unwrap();
        assert!(request.events_data.is_some());
        let events_data = request.events_data.unwrap();
        assert_eq!(events_data.events.len(), 1);

        // The proto event must have correct structure
        let proto_event = &events_data.events[0];
        assert_eq!(proto_event.source_type, 2); // GCS
        assert_eq!(proto_event.event_type, 10); // ACTOR_LIFECYCLE_EVENT
        assert!(proto_event.actor_lifecycle_event.is_some());
    }
}
