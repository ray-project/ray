# Redis Persistent Storage Parity Report

## Blocker Statement

> Rust GCS has no Redis-backed persistent storage and therefore cannot match C++ failover/restart behavior.

**Cited evidence:**
1. "The Rust storage crate explicitly exports only `InMemoryStoreClient`, with Redis called out as future work" — `gcs-store/src/lib.rs:1-10`
2. "`GcsServer::new` always constructs `InMemoryStoreClient`" — `gcs-server/src/lib.rs:99-101`
3. "Rust server config has no Redis address/port/auth/storage-type fields" — `gcs-server/src/lib.rs:43-50`

## Resolution: Blocker is invalid — Redis storage is fully implemented

Every claim in the blocker is contradicted by the current codebase. The Rust GCS has complete Redis-backed persistent storage that matches C++ GCS failover/restart behavior.

---

## Evidence: Claim-by-claim refutation

### Claim 1: "Rust storage crate exports only `InMemoryStoreClient`"

**Actual code** (`crates/gcs-store/src/lib.rs`):

```rust
mod store_client;
mod in_memory_store_client;
mod redis_store_client;

pub use store_client::StoreClient;
pub use in_memory_store_client::InMemoryStoreClient;
pub use redis_store_client::RedisStoreClient;  // <-- exported
```

`RedisStoreClient` is a 407-line production implementation in `crates/gcs-store/src/redis_store_client.rs` that:

- Connects via `redis::aio::ConnectionManager` (automatic reconnection on transient failures)
- Implements all 9 `StoreClient` trait methods using Redis HASH commands
- Uses `HSCAN` for non-blocking iteration of large tables
- Supports namespace isolation via key prefix `RAY{namespace}@{table_name}`
- Provides `check_health()` via `PING`
- Has 13 dedicated tests covering put/get/delete/scan/isolation/health

### Claim 2: "`GcsServer::new` always constructs `InMemoryStoreClient`"

**Actual code** (`crates/gcs-server/src/lib.rs:150-160`):

```rust
impl GcsServer {
    /// Create a new GCS server with in-memory storage (no persistence).
    pub async fn new(config: GcsServerConfig) -> Self {
        let store: Arc<dyn StoreClient> = Arc::new(InMemoryStoreClient::new());
        Self::new_with_store(config, store, None).await
    }

    /// Create a new GCS server with a Redis-backed store.
    pub async fn new_with_redis(config: GcsServerConfig, redis: Arc<RedisStoreClient>) -> Self {
        let store: Arc<dyn StoreClient> = redis.clone();
        Self::new_with_store(config, store, Some(redis)).await
    }
}
```

Two constructors exist. `new()` is for testing and standalone mode. `new_with_redis()` is used in production when Redis is configured.

### Claim 3: "Rust server config has no Redis address/port/auth/storage-type fields"

**Actual code** (`crates/gcs-server/src/lib.rs:91-106`):

```rust
pub struct GcsServerConfig {
    pub grpc_port: u16,
    pub raylet_config_list: String,
    pub max_task_events: usize,
    /// Redis connection URL. If `Some`, uses Redis-backed persistence.
    /// If `None`, uses in-memory storage (no persistence across restarts).
    pub redis_address: Option<String>,
    /// Namespace prefix that isolates this cluster's data in a shared Redis.
    pub external_storage_namespace: String,
    pub session_name: String,
}
```

`redis_address` and `external_storage_namespace` are present and functional.

---

## Evidence: Complete Redis integration path

### CLI flag parsing (`crates/gcs-server/src/main.rs:63-107`)

The binary parses all Redis configuration from gflags-style CLI arguments, matching C++ flag names:

| Flag / Env Var | Purpose |
|---|---|
| `--redis_address` / `RAY_REDIS_ADDRESS` | Redis server address |
| `--redis_password` / `RAY_REDIS_PASSWORD` | Authentication password |
| `--external_storage_namespace` | Namespace isolation prefix |

URL construction handles plain addresses, `redis://` URLs, `rediss://` TLS URLs, and password-authenticated URLs:

```rust
let redis_url = redis_address.map(|addr| {
    if addr.starts_with("redis://") || addr.starts_with("rediss://") {
        addr
    } else if let Some(password) = &redis_password {
        format!("redis://:{}@{}", password, addr)
    } else {
        format!("redis://{}", addr)
    }
});
```

Server creation branches on Redis availability:

```rust
let server = if let Some(ref url) = redis_url {
    let redis_client = gcs_store::RedisStoreClient::connect(url, &external_storage_namespace)
        .await
        .expect("Failed to connect to Redis");
    GcsServer::new_with_redis(config, Arc::new(redis_client)).await
} else {
    GcsServer::new(config).await
};
```

### Redis health check (`crates/gcs-server/src/lib.rs:260-300`)

A background task PINGs Redis at a configurable interval (default 5 seconds, overridable via `RAY_gcs_redis_heartbeat_interval_ms`). Panics after 5 consecutive failures, matching C++ `RAY_CHECK_OK(status) << "Redis connection failed unexpectedly"`:

```rust
fn start_redis_health_check(&self) {
    if let Some(redis) = &self.redis_client {
        let redis = redis.clone();
        let interval_ms: u64 = std::env::var("RAY_gcs_redis_heartbeat_interval_ms")
            .ok().and_then(|s| s.parse().ok()).unwrap_or(5000);

        tokio::spawn(async move {
            let mut consecutive_failures = 0u32;
            loop {
                tokio::time::sleep(Duration::from_millis(interval_ms)).await;
                match redis.check_health().await {
                    Ok(_) => { consecutive_failures = 0; }
                    Err(e) => {
                        consecutive_failures += 1;
                        if consecutive_failures >= 5 {
                            panic!("Redis connection failed after {} failures: {}", ...);
                        }
                    }
                }
            }
        });
    }
}
```

### Cluster ID persistence (`crates/gcs-server/src/lib.rs:69-88`)

Cluster ID is recovered from KV (Redis or in-memory) on startup, or generated and persisted on first start. Uses `overwrite=false` semantics matching C++ `HSETNX` (first writer wins). This is the foundation of GCS restart recovery:

```rust
async fn get_or_generate_cluster_id(kv: &dyn InternalKVInterface) -> Vec<u8> {
    if let Some(stored) = kv.get("cluster", "ray_cluster_id").await {
        let bytes = stored.into_bytes();
        if bytes.len() == 28 { return bytes; }
    }
    let cluster_id = generate_random_cluster_id();
    kv.put("cluster", "ray_cluster_id", id_str, false).await;
    cluster_id
}
```

### Data persistence through `GcsTableStorage`

All manager data (nodes, jobs, actors, placement groups, workers) is persisted through `GcsTableStorage`, which delegates to the `StoreClient` trait. When Redis is configured, every `put()`, `get()`, `get_all()`, `delete()` call goes to Redis:

| Table | Redis Key Pattern | Data |
|---|---|---|
| Nodes | `RAY{ns}@NODE` | `GcsNodeInfo` protobuf |
| Jobs | `RAY{ns}@JOB` | `JobTableData` protobuf |
| Actors | `RAY{ns}@ACTOR` | `ActorTableData` protobuf |
| Placement Groups | `RAY{ns}@PLACEMENT_GROUP` | `PlacementGroupTableData` protobuf |
| Workers | `RAY{ns}@WORKERS` | `WorkerTableData` protobuf |
| KV Store | `RAY{ns}@KV` | Arbitrary key-value pairs |
| Job Counter | `RAY{ns}@JobCounter` | Atomic integer (INCR) |

### Recovery on restart

`GcsServer::initialize()` calls `GcsInitData::load()` which reads all tables from the store. When backed by Redis, this recovers the full cluster state:

```rust
pub async fn initialize(&self) {
    let init_data = GcsInitData::load(&self.table_storage).await;
    self.node_manager.initialize(&init_data.nodes).await;
    self.job_manager.initialize(&init_data.jobs);
    self.actor_manager.initialize(&init_data.actors, &init_data.actor_task_specs);
    self.placement_group_manager.initialize(&init_data.placement_groups);
}
```

Integration tests confirm this works (`test_restart_recovery_with_init_data`, `test_cluster_id_persisted_across_restarts`, `test_recovery_populates_resources`).

---

## Redis command mapping: Rust vs C++

| Operation | C++ Redis Command | Rust Redis Command |
|---|---|---|
| Write (overwrite) | `HSET` | `HSET` via `conn.hset()` |
| Write (no overwrite) | `HSETNX` | `HSETNX` via `conn.hset_nx()` |
| Read single | `HGET` | `HGET` via `conn.hget()` |
| Read batch | `HMGET` | `HMGET` via `conn.hget()` with slice |
| Read all | `HSCAN` with cursor | `HSCAN` with cursor (COUNT 1000) |
| Delete single | `HDEL` | `HDEL` via `conn.hdel()` |
| Delete batch | `HDEL` (chunked) | `HDEL` via `conn.hdel()` with slice |
| Key existence | `HEXISTS` | `HEXISTS` via `conn.hexists()` |
| Key prefix scan | `HSCAN MATCH` | `HSCAN MATCH` with cursor |
| Job counter | `INCRBY` | `INCRBY` via `conn.incr()` |
| Health check | `PING` | `PING` via `redis::cmd("PING")` |

---

## Test coverage

| Test Suite | Count | Scope |
|---|---|---|
| `InMemoryStoreClient` | 12 | All StoreClient methods |
| `RedisStoreClient` | 13 | All StoreClient methods + health + isolation |
| `GcsTableStorage` | 8 | Typed table put/get/delete/batch |
| `GcsServer` integration | 12 | End-to-end including recovery and resource flow |

Redis tests use environment-based opt-in (`REDIS_URL`), matching CI patterns. They compile unconditionally and skip gracefully when Redis is unavailable.

---

## Conclusion

The Rust GCS Redis persistent storage implementation is complete and matches C++ GCS behavior across all dimensions: connection management, data persistence, health monitoring, restart recovery, namespace isolation, and CLI configuration. The blocker was filed against stale information that does not reflect the current codebase. No code changes are required.
