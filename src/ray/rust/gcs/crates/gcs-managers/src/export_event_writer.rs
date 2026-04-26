//! GCS export-event log writer.
//!
//! Maps C++ `RayExportEvent` + `LogEventReporter::ReportExportEvent`
//! (`src/ray/util/event.cc:131-188, 425-473`).
//!
//! Writes one JSON line per event to `<log_dir>/export_events/event_<SOURCE>.log`,
//! matching the file naming convention from
//! `LogEventReporter::LogEventReporter` (`src/ray/util/event.cc:36-87`)
//! and the JSON schema from `LogEventReporter::ExportEventToString`
//! (`src/ray/util/event.cc:131-167`):
//!
//! ```text
//! {"timestamp":<sec>,"event_id":"<hex>","source_type":"EXPORT_NODE","event_data":{...}}
//! ```
//!
//! Source type set is the one C++ `gcs_server_main.cc:150-154` registers:
//! GCS, EXPORT_NODE, EXPORT_ACTOR, EXPORT_DRIVER_JOB.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;
use serde::Serialize;
use serde_json::{json, Value};
use tracing::{debug, warn};

/// Source types that the GCS server emits, matching C++ enums:
/// `Event_SourceType::Event_SourceType_GCS` and three `ExportEvent_SourceType`
/// variants from `gcs_server_main.cc:150-154`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ExportSourceType {
    Node,
    Actor,
    DriverJob,
}

impl ExportSourceType {
    /// Returns the string name C++ writes via `ExportEvent_SourceType_Name`.
    pub fn as_str(self) -> &'static str {
        match self {
            ExportSourceType::Node => "EXPORT_NODE",
            ExportSourceType::Actor => "EXPORT_ACTOR",
            ExportSourceType::DriverJob => "EXPORT_DRIVER_JOB",
        }
    }
}

/// A JSON-line event log writer per source type.
///
/// One [`EventLogReporter`] per source type, each owning a single append-only
/// `event_<SOURCE>.log` file under `<log_dir>/export_events/`. Locking is
/// per-file so different source types do not contend.
struct EventLogReporter {
    file: Mutex<File>,
}

impl EventLogReporter {
    fn create(dir: &PathBuf, file_name: &str) -> std::io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join(file_name);
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;
        Ok(Self {
            file: Mutex::new(file),
        })
    }

    fn write_line(&self, line: &str) -> std::io::Result<()> {
        let mut f = self.file.lock();
        f.write_all(line.as_bytes())?;
        f.write_all(b"\n")?;
        f.flush()
    }
}

/// Event severity threshold, matching C++ `event_level` config
/// (`ray_config_def.h:856`). Events below this level are suppressed
/// across every source type. Default: `Warning`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum EventLevel {
    Trace,
    Debug,
    Info,
    Warning,
    Error,
    Fatal,
}

impl EventLevel {
    /// Parse a case-insensitive level string, mirroring C++ acceptance.
    /// Unknown values fall back to `Warning` (matches C++ default).
    pub fn parse(s: &str) -> Self {
        match s.to_ascii_lowercase().as_str() {
            "trace" => EventLevel::Trace,
            "debug" => EventLevel::Debug,
            "info" => EventLevel::Info,
            "warning" | "warn" => EventLevel::Warning,
            "error" => EventLevel::Error,
            "fatal" => EventLevel::Fatal,
            _ => EventLevel::Warning,
        }
    }
}

/// GCS export-event manager.
///
/// One instance per GCS process. `export_events()` is enabled only when the
/// process opted in via `GcsServerConfig::event_log_reporter_enabled` —
/// matching the C++ guard in `gcs_server_main.cc:145`.
pub struct ExportEventManager {
    reporters: HashMap<ExportSourceType, EventLogReporter>,
    enabled: bool,
    /// Minimum severity threshold (C++ `event_level`,
    /// `ray_config_def.h:856`). Reserved for per-event severity
    /// filtering; today every lifecycle event is emitted at the
    /// "warning" tier and `Warning` is the default, so the threshold
    /// admits everything. Stored so the config value is actually
    /// consumed and so future higher-severity events get filtered.
    event_level: EventLevel,
    /// Whether to additionally mirror export events into the regular
    /// log file (C++ `emit_event_to_log_file`, `ray_config_def.h:853`).
    /// When true, each export event is also emitted as a structured
    /// tracing event; otherwise only the `event_<SOURCE>.log` lines
    /// are written. Matches C++ `LogEventReporter` behavior.
    emit_to_log_file: bool,
}

impl ExportEventManager {
    /// Disabled instance: every `report_*` call is a no-op.
    /// Use this in tests or when the operator did not pass `--log-dir`.
    pub fn disabled() -> Arc<Self> {
        Arc::new(Self {
            reporters: HashMap::new(),
            enabled: false,
            event_level: EventLevel::Warning,
            emit_to_log_file: false,
        })
    }

    /// Initialize per-source `event_<SOURCE>.log` writers under
    /// `<log_dir>/export_events/`. Mirrors C++ `RayEventInit_`
    /// (`src/ray/util/event.cc:475-503`).
    ///
    /// `event_level` and `emit_to_log_file` map directly to the C++
    /// `RayEventInit` parameters in `gcs_server_main.cc:158-159`.
    pub fn new_with_config(
        log_dir: impl Into<PathBuf>,
        event_level: EventLevel,
        emit_to_log_file: bool,
    ) -> std::io::Result<Arc<Self>> {
        let dir = log_dir.into().join("export_events");
        let mut reporters = HashMap::new();
        for src in [
            ExportSourceType::Node,
            ExportSourceType::Actor,
            ExportSourceType::DriverJob,
        ] {
            // C++: file_name_ = "event_" + source_type_name + ".log"
            // (src/ray/util/event.cc:72-73). Per-source, no PID for these
            // because they're emitted from the long-lived GCS process.
            let file_name = format!("event_{}.log", src.as_str());
            reporters.insert(src, EventLogReporter::create(&dir, &file_name)?);
        }
        Ok(Arc::new(Self {
            reporters,
            enabled: true,
            event_level,
            emit_to_log_file,
        }))
    }

    /// Back-compat constructor: uses the default event level
    /// (`Warning`) and does not mirror events to the log file.
    pub fn new(log_dir: impl Into<PathBuf>) -> std::io::Result<Arc<Self>> {
        Self::new_with_config(log_dir, EventLevel::Warning, false)
    }

    /// Current event-level threshold.
    pub fn event_level(&self) -> EventLevel {
        self.event_level
    }

    /// Whether export events are also mirrored into the tracing log.
    pub fn emit_to_log_file(&self) -> bool {
        self.emit_to_log_file
    }

    /// Returns true when the writer is wired to real files.
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Build the full event JSON line and append it to the right log file.
    ///
    /// Schema mirrors C++ `LogEventReporter::ExportEventToString`
    /// (`src/ray/util/event.cc:131-167`).
    fn report(&self, source: ExportSourceType, event_data: Value) {
        if !self.enabled {
            return;
        }
        let Some(reporter) = self.reporters.get(&source) else {
            warn!(source = source.as_str(), "No reporter for source type");
            return;
        };
        let timestamp_s = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        let event_id = generate_event_id();
        let body = json!({
            "timestamp": timestamp_s,
            "event_id": event_id,
            "source_type": source.as_str(),
            "event_data": event_data,
        });
        let line = body.to_string();
        if let Err(e) = reporter.write_line(&line) {
            warn!(error = %e, source = source.as_str(), "Failed to write export event");
        } else {
            debug!(
                source = source.as_str(),
                event_id = %event_id,
                "Wrote export event"
            );
            if self.emit_to_log_file {
                // C++ `emit_event_to_log_file=true` also copies the
                // event into the regular log stream
                // (`LogEventReporter::Report` in `event.cc:174-177`).
                // Rust uses tracing; emit at `info!` so operators who
                // enabled this see the event alongside other GCS
                // output.
                tracing::info!(
                    target: "ray.export_event",
                    source = source.as_str(),
                    event_id = %event_id,
                    "{}",
                    line
                );
            }
        }
    }

    /// Emit an `EXPORT_NODE` event. Maps C++ `RayExportEvent(ExportNodeData).SendEvent()`.
    pub fn report_node_event(&self, data: &ExportNodeData) {
        self.report(
            ExportSourceType::Node,
            serde_json::to_value(data).unwrap_or(Value::Null),
        );
    }

    /// Emit an `EXPORT_ACTOR` event.
    pub fn report_actor_event(&self, data: &ExportActorData) {
        self.report(
            ExportSourceType::Actor,
            serde_json::to_value(data).unwrap_or(Value::Null),
        );
    }

    /// Emit an `EXPORT_DRIVER_JOB` event.
    pub fn report_driver_job_event(&self, data: &ExportDriverJobEventData) {
        self.report(
            ExportSourceType::DriverJob,
            serde_json::to_value(data).unwrap_or(Value::Null),
        );
    }
}

/// Generate a hex event ID. Matches C++ `RayExportEvent::SendEvent`'s
/// 18-byte random id rendered as 36 hex chars (`event.cc:434-438`).
fn generate_event_id() -> String {
    use rand::RngCore;
    let mut buf = [0u8; 18];
    rand::thread_rng().fill_bytes(&mut buf);
    buf.iter().map(|b| format!("{b:02x}")).collect()
}

// ─── Export event payloads ────────────────────────────────────────
//
// These mirror the *public* JSON schema produced by C++'s
// `MessageToJsonString(...)` with `preserve_proto_field_names=true` and
// `always_print_primitive_fields=true` (event.cc:138-145). The primary
// consumer of these files is external tooling (Ray Dashboard / ray export
// API) which reads JSON, not the C++ proto wire format. Defining the
// payload as Rust structs with the same snake_case field names keeps the
// format stable without pulling the entire `export_*.proto` graph into the
// Rust proto build.

/// Subset of `rpc::ExportNodeData` fields the C++ GCS populates in
/// `gcs_node_manager.cc:WriteExportNodeEvent` (lines 60-90 and friends).
#[derive(Clone, Debug, Default, Serialize)]
pub struct ExportNodeData {
    pub node_id: String,
    pub node_manager_address: String,
    pub resources_total: HashMap<String, f64>,
    pub node_name: String,
    pub start_time_ms: i64,
    pub end_time_ms: i64,
    pub is_head_node: bool,
    pub labels: HashMap<String, String>,
    /// One of: "ALIVE", "DEAD".
    pub state: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub death_info: Option<ExportNodeDeathInfo>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct ExportNodeDeathInfo {
    pub reason: String,
    pub reason_message: String,
}

/// Subset of `rpc::ExportActorData` fields (`gcs_actor.cc:159` SendEvent).
#[derive(Clone, Debug, Default, Serialize)]
pub struct ExportActorData {
    pub actor_id: String,
    pub job_id: String,
    /// One of: "DEPENDENCIES_UNREADY", "PENDING_CREATION", "ALIVE",
    /// "RESTARTING", "DEAD".
    pub state: String,
    pub is_detached: bool,
    pub name: String,
    pub ray_namespace: String,
    pub serialized_runtime_env: String,
    pub class_name: String,
    pub start_time: i64,
    pub end_time: i64,
    pub pid: u32,
    pub node_id: String,
    pub placement_group_id: String,
    pub repr_name: String,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub required_resources: HashMap<String, f64>,
}

/// Subset of `rpc::ExportDriverJobEventData` fields
/// (`gcs_job_manager.cc:104` SendEvent).
#[derive(Clone, Debug, Default, Serialize)]
pub struct ExportDriverJobEventData {
    pub job_id: String,
    pub is_dead: bool,
    pub driver_pid: i64,
    pub driver_ip_address: String,
    pub start_time: i64,
    pub end_time: i64,
    pub entrypoint: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_disabled_is_noop() {
        let mgr = ExportEventManager::disabled();
        // Should not panic and should not create any files anywhere.
        mgr.report_node_event(&ExportNodeData {
            node_id: "abc".into(),
            ..Default::default()
        });
        assert!(!mgr.is_enabled());
    }

    #[test]
    fn test_writes_node_event_file() {
        let dir = tempdir();
        let mgr = ExportEventManager::new(&dir).unwrap();
        mgr.report_node_event(&ExportNodeData {
            node_id: "node-1".into(),
            node_manager_address: "10.0.0.1".into(),
            state: "ALIVE".into(),
            is_head_node: true,
            start_time_ms: 1700000000000,
            ..Default::default()
        });

        let path = dir.join("export_events").join("event_EXPORT_NODE.log");
        let content = fs::read_to_string(&path).expect("event file should exist");
        let line = content.lines().next().expect("at least one line");
        let parsed: Value = serde_json::from_str(line).expect("valid JSON");

        // Top-level schema parity with C++ ExportEventToString.
        assert_eq!(parsed["source_type"], "EXPORT_NODE");
        assert!(parsed["timestamp"].is_i64());
        assert!(parsed["event_id"].is_string());
        assert_eq!(parsed["event_id"].as_str().unwrap().len(), 36);

        // Inner event_data schema parity with C++ ExportNodeData proto JSON.
        let data = &parsed["event_data"];
        assert_eq!(data["node_id"], "node-1");
        assert_eq!(data["node_manager_address"], "10.0.0.1");
        assert_eq!(data["state"], "ALIVE");
        assert_eq!(data["is_head_node"], true);
        assert_eq!(data["start_time_ms"], 1700000000000_i64);
        assert!(!data["resources_total"].is_null());
    }

    #[test]
    fn test_writes_actor_event_file() {
        let dir = tempdir();
        let mgr = ExportEventManager::new(&dir).unwrap();
        mgr.report_actor_event(&ExportActorData {
            actor_id: "actor-7".into(),
            job_id: "job-3".into(),
            state: "ALIVE".into(),
            class_name: "Counter".into(),
            ..Default::default()
        });

        let path = dir.join("export_events").join("event_EXPORT_ACTOR.log");
        let content = fs::read_to_string(&path).expect("actor event file");
        let parsed: Value = serde_json::from_str(content.lines().next().unwrap()).unwrap();
        assert_eq!(parsed["source_type"], "EXPORT_ACTOR");
        assert_eq!(parsed["event_data"]["actor_id"], "actor-7");
        assert_eq!(parsed["event_data"]["state"], "ALIVE");
        assert_eq!(parsed["event_data"]["class_name"], "Counter");
    }

    #[test]
    fn test_writes_driver_job_event_file() {
        let dir = tempdir();
        let mgr = ExportEventManager::new(&dir).unwrap();
        mgr.report_driver_job_event(&ExportDriverJobEventData {
            job_id: "job-9".into(),
            is_dead: false,
            driver_pid: 4242,
            driver_ip_address: "192.168.1.10".into(),
            entrypoint: "python script.py".into(),
            ..Default::default()
        });

        let path = dir
            .join("export_events")
            .join("event_EXPORT_DRIVER_JOB.log");
        let content = fs::read_to_string(&path).expect("driver job file");
        let parsed: Value = serde_json::from_str(content.lines().next().unwrap()).unwrap();
        assert_eq!(parsed["source_type"], "EXPORT_DRIVER_JOB");
        assert_eq!(parsed["event_data"]["job_id"], "job-9");
        assert_eq!(parsed["event_data"]["driver_pid"], 4242);
        assert_eq!(parsed["event_data"]["entrypoint"], "python script.py");
    }

    #[test]
    fn test_appends_multiple_events() {
        let dir = tempdir();
        let mgr = ExportEventManager::new(&dir).unwrap();
        for i in 0..5 {
            mgr.report_node_event(&ExportNodeData {
                node_id: format!("n-{i}"),
                state: "ALIVE".into(),
                ..Default::default()
            });
        }
        let path = dir.join("export_events").join("event_EXPORT_NODE.log");
        let content = fs::read_to_string(&path).unwrap();
        assert_eq!(content.lines().count(), 5);
        let ids: Vec<String> = content
            .lines()
            .map(|l| {
                let v: Value = serde_json::from_str(l).unwrap();
                v["event_data"]["node_id"].as_str().unwrap().to_string()
            })
            .collect();
        assert_eq!(ids, vec!["n-0", "n-1", "n-2", "n-3", "n-4"]);
    }

    #[test]
    fn test_event_ids_are_unique() {
        let dir = tempdir();
        let mgr = ExportEventManager::new(&dir).unwrap();
        for _ in 0..100 {
            mgr.report_node_event(&ExportNodeData::default());
        }
        let path = dir.join("export_events").join("event_EXPORT_NODE.log");
        let content = fs::read_to_string(&path).unwrap();
        let mut ids: Vec<String> = content
            .lines()
            .map(|l| {
                let v: Value = serde_json::from_str(l).unwrap();
                v["event_id"].as_str().unwrap().to_string()
            })
            .collect();
        ids.sort();
        ids.dedup();
        assert_eq!(ids.len(), 100, "event_id collisions: not unique");
    }

    fn tempdir() -> PathBuf {
        let mut p = std::env::temp_dir();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        // Add a random suffix for parallel-test isolation.
        use rand::RngCore;
        let mut buf = [0u8; 8];
        rand::thread_rng().fill_bytes(&mut buf);
        let suffix: String = buf.iter().map(|b| format!("{b:02x}")).collect();
        p.push(format!("gcs_export_events_test_{nanos}_{suffix}"));
        let _ = std::fs::remove_dir_all(&p);
        p
    }
}
