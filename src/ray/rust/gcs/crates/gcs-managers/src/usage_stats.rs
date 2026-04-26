//! GCS usage-stats client.
//!
//! Maps C++ `ray::gcs::UsageStatsClient`
//! (`src/ray/gcs/usage_stats_client.h/cc`). The C++ class is a thin
//! shim: every `RecordExtra*` call just writes one key/value into the
//! internal KV store under the namespace `usage_stats`, with key
//! `extra_usage_tag_{lowercased_tag_name}`, overwriting any prior value.
//!
//! This Rust port preserves the exact wire shape so any tool that
//! reads the KV for telemetry (the Ray dashboard, `ray status`,
//! cluster-usage reports, etc.) sees the same entries whether the GCS
//! is C++ or Rust.

use std::fmt;
use std::sync::Arc;

use gcs_kv::InternalKVInterface;

/// KV namespace. Matches C++
/// `UsageStatsClient::kUsageStatsNamespace` and
/// `usage_constants.py`'s `USAGE_STATS_NAMESPACE`.
const USAGE_STATS_NAMESPACE: &str = "usage_stats";

/// Key prefix. Matches C++
/// `UsageStatsClient::kExtraUsageTagPrefix`.
const EXTRA_USAGE_TAG_PREFIX: &str = "extra_usage_tag_";

/// Enum of known usage tag keys. Kept in sync with
/// `src/ray/protobuf/usage.proto` — only the tags the GCS itself emits
/// need Rust representations; Python-side tags remain Python-only.
///
/// The numeric values are immaterial in Rust (we never serialize the
/// enum); what matters is the lowercased *name*, which becomes the
/// KV key suffix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TagKey {
    /// `PG_NUM_CREATED` — placement groups created. Consumer:
    /// `GcsPlacementGroupManager` metrics recorder.
    PgNumCreated,
    /// `ACTOR_NUM_CREATED` — actors created. Consumer:
    /// `GcsActorManager` metrics recorder.
    ActorNumCreated,
    /// `WORKER_CRASH_SYSTEM_ERROR` — worker deaths with
    /// `SYSTEM_ERROR` exit type. Consumer: `GcsWorkerManager`
    /// failure path.
    WorkerCrashSystemError,
    /// `WORKER_CRASH_OOM` — worker deaths with
    /// `NODE_OUT_OF_MEMORY`. Same consumer.
    WorkerCrashOom,
    /// Task-type counters, all reported by `GcsTaskManager`'s
    /// metrics recorder.
    NumActorCreationTasks,
    NumActorTasks,
    NumNormalTasks,
    NumDrivers,
}

impl TagKey {
    /// Proto-enum-style name (UPPER_SNAKE_CASE). Matches
    /// `usage::TagKey_Name(key)` in C++
    /// (`usage_stats_client.cc:27`).
    pub fn name(self) -> &'static str {
        match self {
            TagKey::PgNumCreated => "PG_NUM_CREATED",
            TagKey::ActorNumCreated => "ACTOR_NUM_CREATED",
            TagKey::WorkerCrashSystemError => "WORKER_CRASH_SYSTEM_ERROR",
            TagKey::WorkerCrashOom => "WORKER_CRASH_OOM",
            TagKey::NumActorCreationTasks => "NUM_ACTOR_CREATION_TASKS",
            TagKey::NumActorTasks => "NUM_ACTOR_TASKS",
            TagKey::NumNormalTasks => "NUM_NORMAL_TASKS",
            TagKey::NumDrivers => "NUM_DRIVERS",
        }
    }
}

impl fmt::Display for TagKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// KV-backed usage recorder. Each `record_*` call writes one
/// key/value into the internal KV under namespace `usage_stats`.
pub struct UsageStatsClient {
    kv: Arc<dyn InternalKVInterface>,
}

impl UsageStatsClient {
    pub fn new(kv: Arc<dyn InternalKVInterface>) -> Self {
        Self { kv }
    }

    /// Record a tag with a string value. Mirrors C++
    /// `RecordExtraUsageTag` (`usage_stats_client.cc:25-37`).
    ///
    /// Async because the KV put is async; callers that don't need
    /// backpressure fire-and-forget via `tokio::spawn`.
    pub async fn record_extra_usage_tag(&self, key: TagKey, value: impl Into<String>) {
        let kv_key = format!(
            "{prefix}{lower}",
            prefix = EXTRA_USAGE_TAG_PREFIX,
            lower = key.name().to_ascii_lowercase()
        );
        // C++ uses overwrite=true — most-recent-wins.
        self.kv
            .put(USAGE_STATS_NAMESPACE, &kv_key, value.into(), true)
            .await;
    }

    /// Record a monotonically increasing counter. Mirrors C++
    /// `RecordExtraUsageCounter` (`usage_stats_client.cc:39-41`):
    /// it just stringifies and calls `record_extra_usage_tag`.
    pub async fn record_extra_usage_counter(&self, key: TagKey, counter: i64) {
        self.record_extra_usage_tag(key, counter.to_string()).await;
    }

    /// Fire-and-forget variant. Convenient for hot paths that don't
    /// want to await the KV write. Spawned on the ambient tokio
    /// runtime.
    pub fn record_extra_usage_counter_spawn(
        self: &Arc<Self>,
        key: TagKey,
        counter: i64,
    ) {
        let this = self.clone();
        tokio::spawn(async move {
            this.record_extra_usage_counter(key, counter).await;
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gcs_kv::StoreClientInternalKV;
    use gcs_store::InMemoryStoreClient;

    fn kv() -> Arc<dyn InternalKVInterface> {
        Arc::new(StoreClientInternalKV::new(Arc::new(InMemoryStoreClient::new())))
    }

    #[tokio::test]
    async fn record_counter_writes_expected_key() {
        let kv = kv();
        let client = UsageStatsClient::new(kv.clone());
        client
            .record_extra_usage_counter(TagKey::PgNumCreated, 7)
            .await;

        let v = kv.get("usage_stats", "extra_usage_tag_pg_num_created").await;
        assert_eq!(v.as_deref(), Some("7"));
    }

    #[tokio::test]
    async fn record_counter_overwrites() {
        let kv = kv();
        let client = UsageStatsClient::new(kv.clone());
        client
            .record_extra_usage_counter(TagKey::ActorNumCreated, 1)
            .await;
        client
            .record_extra_usage_counter(TagKey::ActorNumCreated, 42)
            .await;

        let v = kv
            .get("usage_stats", "extra_usage_tag_actor_num_created")
            .await;
        assert_eq!(v.as_deref(), Some("42"));
    }

    #[tokio::test]
    async fn tag_keys_match_c_plus_plus_names() {
        // Parity guard: the KV-suffix lower-case form of each tag
        // must match the upper-case name in `usage.proto`. If a new
        // tag is added we only need to update the match arm — the
        // key suffix is derived.
        let cases = [
            (TagKey::PgNumCreated, "PG_NUM_CREATED"),
            (TagKey::ActorNumCreated, "ACTOR_NUM_CREATED"),
            (TagKey::WorkerCrashSystemError, "WORKER_CRASH_SYSTEM_ERROR"),
            (TagKey::WorkerCrashOom, "WORKER_CRASH_OOM"),
            (TagKey::NumActorCreationTasks, "NUM_ACTOR_CREATION_TASKS"),
            (TagKey::NumActorTasks, "NUM_ACTOR_TASKS"),
            (TagKey::NumNormalTasks, "NUM_NORMAL_TASKS"),
            (TagKey::NumDrivers, "NUM_DRIVERS"),
        ];
        for (k, expected) in cases {
            assert_eq!(k.name(), expected);
        }
    }
}
