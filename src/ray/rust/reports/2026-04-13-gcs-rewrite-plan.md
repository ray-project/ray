# 2026-04-13 -- Detailed Plan: Rewriting Ray GCS from C++ to Rust

**Status**: Phase 0 & Phase 1 COMPLETE -- Phase 2+ pending
**Date**: 2026-04-13
**Platform**: macOS Darwin 23.6.0, ARM64
**Scope**: Full GCS (Global Control Store) rewrite -- C++ to Rust

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [GCS Architecture Overview](#2-gcs-architecture-overview)
3. [Current Test Coverage Audit](#3-current-test-coverage-audit)
4. [Phase 0: Test Coverage Hardening](#4-phase-0-test-coverage-hardening)
5. [Incremental Rust Rewrite Plan (Phases 1-11)](#5-incremental-rust-rewrite-plan)
6. [C++ to Rust Pattern Mapping](#6-c-to-rust-pattern-mapping)
7. [Architectural Decisions and Trade-Offs](#7-architectural-decisions-and-trade-offs)
8. [Post-Rewrite Report Template](#8-post-rewrite-report-template)
9. [Risk Analysis](#9-risk-analysis)
10. [Appendix: File Inventory](#10-appendix-file-inventory)

---

## 1. Executive Summary

The Ray GCS is the centralized metadata service for Ray clusters. It consists of approximately **10,300 lines of C++ implementation** across **19 source files**, organized into **12 manager/handler components**, a pluggable storage layer (Redis + in-memory), and a gRPC server orchestrator. It exposes **12 gRPC services** with **54 RPC handler methods** defined in `grpc_service_interfaces.h`.

**Key insight enabling the rewrite:** GCS communicates with all other Ray components **exclusively via gRPC**. No C++ component calls GCS functions through direct C++ API calls -- every interaction goes through the gRPC boundary. This means we can rewrite the entire GCS as a standalone Rust binary implementing the same 12 gRPC services from the same `.proto` files, with zero FFI bridging needed.

**Current test state:** 38 test files exist (31 C++, 6 Python, 1 auth test). 162 C++ tests pass on macOS. However, several components have zero or thin test coverage, which must be addressed before the rewrite begins.

**Timeline estimate:** 30 weeks across 12 phases (Phase 0: test hardening, Phases 1-11: incremental Rust rewrite and validation).

---

## 2. GCS Architecture Overview

### 2.1 Server Entry Point and Lifecycle

**Entry point:** `src/ray/gcs/gcs_server_main.cc`

```
main()
  -> Parse gflags (redis addr, port, GCS port, config)
  -> Initialize logging with rotation
  -> Load RayConfig from base64-encoded config_list
  -> Create instrumented_io_context (Boost ASIO event loop)
  -> Create GcsServerConfig
  -> Create GcsServerMetrics
  -> Instantiate GcsServer(config, metrics, io_context)
  -> server.Start()
       -> GcsInitData::AsyncLoad() -- recover state from storage
       -> DoStart(gcs_init_data)
            -> InitGcsTableStorage()
            -> InitGcsNodeManager()
            -> InitGcsHealthCheckManager()
            -> InitGcsResourceManager()
            -> InitRaySyncer()
            -> InitClusterResourceScheduler()
            -> InitGcsJobManager()
            -> InitGcsActorManager()
            -> InitGcsPlacementGroupManager()
            -> InitGcsWorkerManager()
            -> InitGcsTaskManager()
            -> InitGcsAutoscalerStateManager()
            -> InitKVManager()
            -> InitFunctionManager()
            -> InitPubSubHandler()
            -> InitRuntimeEnvManager()
            -> Register all gRPC services
            -> Start gRPC server on configured port
  -> main_service.run()  -- blocks on Boost ASIO event loop
  -> SIGTERM handler calls main_service.stop()
```

### 2.2 Component Dependency Graph

```
GcsServer (orchestrator)
  |
  +-- GcsTableStorage (uses StoreClient)
  |     +-- InMemoryStoreClient  OR  RedisStoreClient
  |     +-- ObservableStoreClient (metrics decorator)
  |
  +-- GcsNodeManager
  |     +-- GcsTableStorage
  |     +-- GcsPublisher
  |     +-- GcsHealthCheckManager --> on_node_death_callback --> GcsNodeManager
  |     +-- RayletClientPool
  |
  +-- GcsActorManager (MOST COMPLEX)
  |     +-- GcsTableStorage
  |     +-- GcsPublisher
  |     +-- GcsActorScheduler
  |     |     +-- GcsNodeManager (for node selection)
  |     |     +-- RayletClientPool (for lease requests)
  |     |     +-- CoreWorkerClientPool (for pushing actor creation)
  |     +-- GcsNodeManager (for node death notifications)
  |     +-- GcsFunctionManager
  |     +-- RuntimeEnvManager
  |
  +-- GcsPlacementGroupManager
  |     +-- GcsTableStorage
  |     +-- GcsPublisher
  |     +-- GcsPlacementGroupScheduler
  |     |     +-- GcsNodeManager
  |     |     +-- ClusterResourceScheduler
  |     |     +-- RayletClientPool
  |     +-- GcsNodeManager (for node death notifications)
  |
  +-- GcsJobManager
  |     +-- GcsTableStorage
  |     +-- GcsPublisher
  |     +-- GcsFunctionManager
  |     +-- InternalKVInterface
  |     +-- CoreWorkerClientPool (for running_tasks check)
  |
  +-- GcsResourceManager
  |     +-- GcsNodeManager (for node lists)
  |     +-- ClusterResourceScheduler
  |
  +-- GcsWorkerManager
  |     +-- GcsTableStorage
  |     +-- GcsPublisher
  |
  +-- GcsTaskManager (self-contained)
  |     +-- GC policies (size-based, time-based)
  |
  +-- GcsAutoscalerStateManager
  |     +-- GcsNodeManager
  |     +-- GcsActorManager
  |     +-- GcsPlacementGroupManager
  |     +-- RayletClientPool
  |     +-- InternalKVInterface
  |
  +-- GcsInternalKVManager
  |     +-- InternalKVInterface (backed by StoreClient)
  |
  +-- InternalPubSubHandler
  |     +-- GcsPublisher
  |
  +-- GcsFunctionManager
  |     +-- InternalKVInterface
  |
  +-- RuntimeEnvHandler
  |     +-- RuntimeEnvManager
  |
  +-- UsageStatsClient
        +-- InternalKVInterface
```

### 2.3 gRPC Service Definitions

All 12 services defined in `src/ray/protobuf/gcs_service.proto`:

| # | Service | Handler Interface | Methods |
|---|---------|-------------------|---------|
| 1 | JobInfoGcsService | JobInfoGcsServiceHandler | AddJob, MarkJobFinished, GetAllJobInfo, ReportJobError, GetNextJobID |
| 2 | ActorInfoGcsService | ActorInfoGcsServiceHandler | RegisterActor, CreateActor, GetActorInfo, GetNamedActorInfo, ListNamedActors, GetAllActorInfo, KillActorViaGcs, RestartActorForLineageReconstruction, ReportActorOutOfScope |
| 3 | NodeInfoGcsService | NodeInfoGcsServiceHandler | GetClusterId, RegisterNode, UnregisterNode, CheckAlive, DrainNode, GetAllNodeInfo, GetAllNodeAddressAndLiveness |
| 4 | NodeResourceInfoGcsService | NodeResourceInfoGcsServiceHandler | GetAllAvailableResources, GetAllTotalResources, GetDrainingNodes, GetAllResourceUsage |
| 5 | WorkerInfoGcsService | WorkerInfoGcsServiceHandler | ReportWorkerFailure, GetWorkerInfo, GetAllWorkerInfo, AddWorkerInfo, UpdateWorkerDebuggerPort, UpdateWorkerNumPausedThreads |
| 6 | PlacementGroupInfoGcsService | PlacementGroupInfoGcsServiceHandler | CreatePlacementGroup, RemovePlacementGroup, GetPlacementGroup, GetAllPlacementGroup, WaitPlacementGroupUntilReady, GetNamedPlacementGroup |
| 7 | TaskInfoGcsService | TaskInfoGcsServiceHandler | AddTaskEventData, GetTaskEvents |
| 8 | InternalKVGcsService | InternalKVGcsServiceHandler | InternalKVKeys, InternalKVGet, InternalKVMultiGet, InternalKVPut, InternalKVDel, InternalKVExists, GetInternalConfig |
| 9 | InternalPubSubGcsService | InternalPubSubGcsServiceHandler | GcsPublish, GcsSubscriberPoll, GcsSubscriberCommandBatch |
| 10 | RuntimeEnvGcsService | RuntimeEnvGcsServiceHandler | PinRuntimeEnvURI |
| 11 | AutoscalerStateService | AutoscalerStateServiceHandler | GetClusterResourceState, ReportAutoscalingState, RequestClusterResourceConstraint, GetClusterStatus, DrainNode, ResizeRayletResourceInstances, ReportClusterConfig |
| 12 | RayEventExportGcsService | RayEventExportGcsServiceHandler | AddEvents |

**Total: 54 RPC handler methods.**

### 2.4 Storage Architecture

The `StoreClient` interface (`store_client/store_client.h`) provides 9 async operations:

```cpp
class StoreClient {
  virtual void AsyncPut(table, key, data, overwrite, callback) = 0;
  virtual void AsyncGet(table, key, callback) = 0;
  virtual void AsyncGetAll(table, callback) = 0;
  virtual void AsyncMultiGet(table, keys, callback) = 0;
  virtual void AsyncDelete(table, key, callback) = 0;
  virtual void AsyncBatchDelete(table, keys, callback) = 0;
  virtual void AsyncGetNextJobID(callback) = 0;
  virtual void AsyncGetKeys(table, prefix, callback) = 0;
  virtual void AsyncExists(table, key, callback) = 0;
};
```

**Implementations:**
- `InMemoryStoreClient` -- HashMap-based, for development/testing
- `RedisStoreClient` -- Full Redis integration with async context, connection pooling, key namespacing
- `ObservableStoreClient` -- Decorator adding latency/count metrics to any underlying client

**GcsTableStorage** wraps StoreClient with typed tables:
- `GcsJobTable`: JobID -> JobTableData
- `GcsActorTable`: ActorID -> ActorTableData (with JobID secondary index)
- `GcsActorTaskSpecTable`: ActorID -> TaskSpec
- `GcsPlacementGroupTable`: PlacementGroupID -> PlacementGroupTableData
- `GcsNodeTable`: NodeID -> GcsNodeInfo
- `GcsWorkerTable`: WorkerID -> WorkerTableData

---

## 3. Current Test Coverage Audit

### 3.1 Existing Test Files (38 total)

**GCS Core Tests** (`src/ray/gcs/tests/`) -- 18 files:

| Test File | Component Tested | Size | Status |
|-----------|-----------------|------|--------|
| gcs_node_manager_test.cc | GcsNodeManager | 736 LOC | Passes |
| gcs_job_manager_test.cc | GcsJobManager | 801 LOC | Passes |
| gcs_task_manager_test.cc | GcsTaskManager | 1,818 LOC | Passes |
| gcs_placement_group_manager_test.cc | GcsPlacementGroupManager | 1,305 LOC | Passes |
| gcs_placement_group_manager_mock_test.cc | GcsPlacementGroupManager (mock) | small | Passes |
| gcs_placement_group_scheduler_test.cc | GcsPlacementGroupScheduler | 1,430 LOC | Passes |
| gcs_worker_manager_test.cc | GcsWorkerManager | 306 LOC | Passes |
| gcs_resource_manager_test.cc | GcsResourceManager | 269 LOC | Passes |
| gcs_kv_manager_test.cc | GcsInternalKVManager | exists | Passes (needs Redis) |
| gcs_function_manager_test.cc | GcsFunctionManager | small | Passes |
| gcs_health_check_manager_test.cc | GcsHealthCheckManager | 296 LOC | Passes |
| gcs_autoscaler_state_manager_test.cc | GcsAutoscalerStateManager | 1,315 LOC | Passes |
| gcs_server_rpc_test.cc | GcsServer (black box RPC) | ~576 LOC | Passes (needs Redis) |
| gcs_ray_event_converter_test.cc | GcsRayEventConverter | small | Passes |
| pubsub_handler_test.cc | InternalPubSubHandler | 156 LOC | Passes |
| usage_stats_client_test.cc | UsageStatsClient | 63 LOC | Passes |
| redis_gcs_table_storage_test.cc | GcsTableStorage (Redis) | exists | Passes (needs Redis) |
| in_memory_gcs_table_storage_test.cc | GcsTableStorage (InMemory) | exists | Passes |

**Export API Tests** (`tests/export_api/`) -- 3 files:
- gcs_job_manager_export_event_test.cc
- gcs_actor_manager_export_event_test.cc
- gcs_node_manager_export_event_test.cc

**Actor Tests** (`actor/tests/`) -- 3 files:
- gcs_actor_manager_test.cc (2,247 LOC)
- gcs_actor_scheduler_test.cc
- gcs_actor_scheduler_mock_test.cc

**Store Client Tests** (`store_client/tests/`) -- 6 files:
- redis_store_client_test.cc
- in_memory_store_client_test.cc
- observable_store_client_test.cc
- redis_callback_reply_test.cc
- redis_async_context_test.cc
- chaos_redis_store_client_test.cc

**Postable Tests** (`postable/tests/`) -- 2 files:
- postable_test.cc
- function_traits_test.cc

**GCS RPC Client Tests** (`gcs_rpc_client/tests/`) -- 5 files:
- gcs_client_test.cc
- gcs_client_reconnection_test.cc
- gcs_client_injectable_test.cc
- accessor_test.cc
- global_state_accessor_test.cc

**Python Tests** -- 6 files:
- test_gcs_utils.py, test_gcs_pubsub.py, test_gcs_fault_tolerance.py
- test_gcs_ha_e2e.py, test_gcs_ha_e2e_2.py
- serve/tests/test_gcs_failure.py

### 3.2 Coverage Gap Analysis

**Components with ZERO test coverage:**

| Component | File | LOC | Impact |
|-----------|------|-----|--------|
| RuntimeEnvHandler | runtime_env_handler.h/cc | 45 | Low risk (1 method, simple) |
| StoreClientKV | store_client_kv.h/cc | 152 | Medium (tested indirectly via KV manager) |
| GcsInitData | gcs_init_data.h/cc | 97 | High risk (startup recovery, no direct test) |

**Components with THIN test coverage:**

| Component | What's Missing | Risk |
|-----------|---------------|------|
| PubSubHandler (129 LOC) | No HandleGcsPublish test, no HandleGcsSubscriberPoll test, no long-poll test | High -- pub/sub is used by every manager |
| GcsWorkerManager (336 LOC) | No HandleReportWorkerFailure, no HandleGetWorkerInfo, no dead listener test | Medium |
| GcsHealthCheckManager (243 LOC) | No concurrent add/remove, no threshold boundary, no duplicate node test | Medium |
| UsageStatsClient (43 LOC) | No RecordExtraUsageCounter test | Low |
| GcsResourceManager (313 LOC) | No HandleGetDrainingNodes, no HandleGetAllResourceUsage | Medium |

**gRPC RPC-level coverage gaps in `gcs_server_rpc_test.cc`:**

The existing RPC test only covers Job, Node, and Worker services. Missing:

| Service | Handler Methods NOT Tested via RPC |
|---------|----------------------------------|
| ActorInfoGcsService | All 9 methods |
| PlacementGroupInfoGcsService | All 6 methods |
| TaskInfoGcsService | All 2 methods |
| InternalKVGcsService | All 7 methods |
| RuntimeEnvGcsService | PinRuntimeEnvURI |
| AutoscalerStateService | All 7 methods |
| InternalPubSubGcsService | All 3 methods |
| NodeResourceInfoGcsService | All 4 methods |
| RayEventExportGcsService | AddEvents |

**This means 39 of 54 handler methods (72%) have no RPC-level integration test.** These RPC tests are the contract that the Rust GCS must satisfy.

---

## 4. Phase 0: Test Coverage Hardening

### 4.1 New Test Files to Create (3 files)

#### 4.1.1 `gcs/tests/runtime_env_handler_test.cc`

```
Tests to write:
- TestPinRuntimeEnvURI_HappyPath
    Setup: Create RuntimeEnvManager + RuntimeEnvHandler with mock delay_executor
    Action: Call HandlePinRuntimeEnvURI with valid URI and expiration_s=60
    Assert: Reply status is OK, URI reference is added to RuntimeEnvManager
    
- TestPinRuntimeEnvURI_ZeroExpiration
    Action: Call with expiration_s=0
    Assert: Reply is OK, no delayed removal scheduled
    
- TestPinRuntimeEnvURI_EmptyURI
    Action: Call with empty uri field
    Assert: Reply status indicates error
    
- TestPinRuntimeEnvURI_ExpirationFires
    Action: Call with expiration_s=1, trigger delay_executor callback
    Assert: URI reference is removed from RuntimeEnvManager

BUILD deps: //src/ray/gcs:gcs_runtime_env_handler, //src/ray/common:runtime_env_manager
```

#### 4.1.2 `gcs/tests/store_client_kv_test.cc`

```
Tests to write:
- TestPutAndGet
    Action: Put("ns", "key", "value", false), then Get("ns", "key")
    Assert: Returns "value"
    
- TestPutOverwrite
    Action: Put("ns", "key", "v1", false), Put("ns", "key", "v2", true)
    Assert: Get returns "v2"
    
- TestPutNoOverwrite
    Action: Put("ns", "key", "v1", false), Put("ns", "key", "v2", false)
    Assert: Get returns "v1" (original preserved)
    
- TestDelete
    Action: Put then Del("ns", "key", false)
    Assert: Get returns empty; Exists returns false
    
- TestDeleteByPrefix
    Action: Put keys "ns/a/1", "ns/a/2", "ns/b/1"; Del("ns", "a/", true)
    Assert: "ns/a/*" gone, "ns/b/1" still present
    
- TestExists
    Assert: false before Put, true after Put, false after Del
    
- TestKeys
    Action: Put 3 keys with different prefixes
    Assert: Keys("ns", "prefix") returns matching subset
    
- TestMultiGet
    Action: Put 3 keys, MultiGet with 2 of them + 1 non-existent
    Assert: Returns map with 2 entries
    
- TestNamespaceIsolation
    Action: Put("ns1", "key", "v1"), Put("ns2", "key", "v2")
    Assert: Get("ns1", "key") = "v1", Get("ns2", "key") = "v2"

BUILD deps: //src/ray/gcs:gcs_store_client_kv, //src/ray/gcs/store_client:in_memory_store_client
```

#### 4.1.3 `gcs/tests/gcs_init_data_test.cc`

```
Tests to write:
- TestAsyncLoadEmpty
    Setup: Empty InMemoryStoreClient
    Action: GcsInitData::AsyncLoad()
    Assert: All tables empty (jobs, actors, nodes, placement_groups)
    
- TestAsyncLoadWithJobs
    Setup: Pre-populate InMemoryStoreClient with 3 JobTableData entries
    Action: GcsInitData::AsyncLoad()
    Assert: init_data.Jobs() returns 3 entries with correct data
    
- TestAsyncLoadWithActors
    Setup: Pre-populate with ActorTableData entries
    Assert: init_data.Actors() returns correct entries
    
- TestAsyncLoadWithNodes
    Setup: Pre-populate with GcsNodeInfo entries
    Assert: init_data.Nodes() returns correct entries
    
- TestAsyncLoadWithPlacementGroups
    Setup: Pre-populate with PlacementGroupTableData entries
    Assert: init_data.PlacementGroups() returns correct entries
    
- TestAsyncLoadAllTables
    Setup: Pre-populate all tables
    Assert: All getters return correct data

BUILD deps: //src/ray/gcs:gcs_init_data, //src/ray/gcs/store_client:in_memory_store_client
```

### 4.2 Existing Tests to Extend (6 files)

#### 4.2.1 `pubsub_handler_test.cc` -- Add 5 tests

```
- TestHandleGcsPublish
    Publish a message, verify it reaches the underlying publisher
    
- TestHandleGcsSubscriberPoll_WithPendingMessages
    Subscribe to channel, publish message, poll -> verify message returned
    
- TestHandleGcsSubscriberPoll_NoPendingMessages
    Subscribe to channel, poll with no messages -> verify long-poll hold
    
- TestMultipleSubscribers
    Two subscribers on same channel, publish -> both receive
    
- TestUnsubscribeViaCommandBatch
    Subscribe, unsubscribe via command batch, publish -> no delivery
```

#### 4.2.2 `gcs_worker_manager_test.cc` -- Add 6 tests

```
- TestHandleReportWorkerFailure
    Register worker, report failure -> verify worker marked dead, listeners called
    
- TestHandleGetWorkerInfo_Existing
    Register worker, GetWorkerInfo -> verify returns correct data
    
- TestHandleGetWorkerInfo_NonExisting
    GetWorkerInfo for unknown ID -> verify empty/error response
    
- TestAddWorkerDeadListener
    Add listener, report failure -> verify listener callback fires
    
- TestReportWorkerFailure_OOM
    Report failure with OOM exit type -> verify OOM counter increments
    
- TestUpdateWorkerDebuggerPort_NonExisting
    Update debugger port for non-existing worker -> verify error handling
```

#### 4.2.3 `gcs_health_check_manager_test.cc` -- Add 4 tests

```
- TestAddThenImmediateRemove
    AddNode then RemoveNode before first health check fires -> no crash, no death callback
    
- TestFailureThresholdBoundary
    Configure threshold=3; simulate 2 failures -> node still alive
    Simulate 3rd failure -> node marked dead
    
- TestDuplicateNodeIdempotency
    AddNode with same NodeID twice -> no crash, no duplicate tracking
    
- TestMultipleNodes_OneDies
    Add 3 nodes, make 1 fail health checks -> only 1 death callback, others healthy
```

#### 4.2.4 `usage_stats_client_test.cc` -- Add 1 test

```
- TestRecordExtraUsageCounter
    Call RecordExtraUsageCounter("counter_key", 42)
    Verify InternalKV receives the counter value at the expected key
```

#### 4.2.5 `gcs_resource_manager_test.cc` -- Add 3 tests

```
- TestHandleGetDrainingNodes
    Register node, mark draining -> GetDrainingNodes returns it
    
- TestHandleGetAllResourceUsage
    Register 2 nodes with resource reports -> GetAllResourceUsage returns both
    
- TestUpdateFromResourceView
    Send resource view update -> verify resources tracked correctly
```

#### 4.2.6 `gcs_server_rpc_test.cc` -- Add RPC tests for missing services

This is the most critical extension. These become the **golden contract tests** that the Rust GCS must pass.

```
--- InternalKV Service ---
- TestInternalKVPutAndGet
    Put("key", "value"), Get("key") -> returns "value"
    
- TestInternalKVDel
    Put, Del, Get -> returns empty
    
- TestInternalKVKeys
    Put 3 keys, Keys("prefix") -> returns matching
    
--- PlacementGroup Service ---
- TestCreateAndGetPlacementGroup
    Create PG, Get PG -> returns correct data
    
- TestRemovePlacementGroup
    Create PG, Remove PG, Get PG -> not found
    
--- Task Service ---
- TestAddAndGetTaskEvents
    AddTaskEventData, GetTaskEvents -> returns events
    
--- RuntimeEnv Service ---
- TestPinRuntimeEnvURI
    PinRuntimeEnvURI -> returns OK
    
--- Autoscaler Service ---
- TestGetClusterResourceState
    Register nodes, GetClusterResourceState -> returns state
```

### 4.3 Verification Criteria

Before starting the Rust rewrite, ALL of the following must be true:

1. All 54 `Handle*` methods have at least 1 unit test
2. All 12 gRPC services have at least 1 RPC-level test in `gcs_server_rpc_test.cc`
3. All new tests pass: `bazel test //src/ray/gcs/... --dynamic_mode=off --keep_going --jobs=4`
4. No regressions in the existing 162 passing C++ tests

---

## 5. Incremental Rust Rewrite Plan

### 5.0 Architectural Approach

**Strategy: Rewrite behind the gRPC boundary as a standalone Rust binary.**

```
BEFORE:                           AFTER:
                                  
Ray Cluster                       Ray Cluster
  |                                 |
  | gRPC                            | gRPC (same proto, same wire format)
  v                                 v
+-----------+                     +-----------+
| C++ GCS   |        ==>         | Rust GCS  |
| (Boost    |                     | (Tokio +  |
|  ASIO +   |                     |  Tonic +  |
|  grpc++)  |                     |  Prost)   |
+-----+-----+                    +-----+-----+
      |                                 |
      v                                 v
  Redis / InMemory                  Redis / InMemory
  (same data format)               (same data format)
```

No FFI bridge is needed because:
- All Ray components communicate with GCS via gRPC only
- The `.proto` files define the contract; both implementations use them
- Redis data format is string-based and language-independent
- The Rust binary is a drop-in replacement selected at deployment time

### 5.1 Rust Workspace Layout

```
ray/src/ray/rust/gcs/
  Cargo.toml                           # Workspace root
  crates/
    gcs-proto/                         # Generated protobuf/gRPC types
      Cargo.toml
      build.rs                         # tonic-build from .proto files
      src/lib.rs
    gcs-store/                         # StoreClient trait + implementations
      Cargo.toml
      src/
        lib.rs
        store_client.rs                # StoreClient trait
        in_memory_store_client.rs      # HashMap-based implementation
        redis_store_client.rs          # Redis async implementation
        observable_store_client.rs     # Metrics decorator
    gcs-table-storage/                 # Typed table storage over StoreClient
      Cargo.toml
      src/
        lib.rs
        table.rs                       # GcsTable<K,V> generic
        storage.rs                     # GcsTableStorage aggregate
    gcs-kv/                            # Internal KV store
      Cargo.toml
      src/
        lib.rs
        interface.rs                   # InternalKVInterface trait
        store_client_kv.rs             # StoreClient-backed KV
        kv_manager.rs                  # GcsInternalKVManager RPC handler
    gcs-pubsub/                        # Pub/Sub system
      Cargo.toml
      src/
        lib.rs
        publisher.rs                   # GcsPublisher
        handler.rs                     # InternalPubSubHandler (long-polling)
    gcs-managers/                      # All domain managers
      Cargo.toml
      src/
        lib.rs
        node_manager.rs
        health_check_manager.rs
        actor/
          mod.rs
          actor.rs                     # GcsActor data structure
          actor_manager.rs
          actor_scheduler.rs
        job_manager.rs
        task_manager.rs
        resource_manager.rs
        worker_manager.rs
        placement_group/
          mod.rs
          placement_group.rs           # GcsPlacementGroup data structure
          placement_group_manager.rs
          placement_group_scheduler.rs
        autoscaler_state_manager.rs
        function_manager.rs
        runtime_env_handler.rs
        usage_stats_client.rs
        ray_event_converter.rs
    gcs-server/                        # Server orchestrator + main binary
      Cargo.toml
      src/
        lib.rs
        config.rs                      # GcsServerConfig
        init_data.rs                   # GcsInitData
        server.rs                      # GcsServer
        grpc_services.rs               # gRPC service registration
        main.rs                        # Entry point
    gcs-test-utils/                    # Shared test helpers
      Cargo.toml
      src/
        lib.rs
        generators.rs                  # GenNodeInfo, GenActorTableData, etc.
```

### 5.2 Key Rust Dependencies

```toml
[workspace.dependencies]
# gRPC / Protobuf
tonic = "0.12"
tonic-build = "0.12"
tonic-health = "0.12"
prost = "0.13"
prost-types = "0.13"

# Async runtime
tokio = { version = "1", features = ["full"] }

# Redis
redis = { version = "0.27", features = ["tokio-comp", "connection-manager"] }

# Concurrent data structures
dashmap = "6"
parking_lot = "0.12"

# Logging / Metrics
tracing = "0.1"
tracing-subscriber = "0.3"
metrics = "0.24"
metrics-exporter-prometheus = "0.16"

# Serialization (for config)
serde = { version = "1", features = ["derive"] }
serde_json = "1"

# ID types
uuid = { version = "1", features = ["v4"] }
bytes = "1"

# Testing
tokio-test = "0.4"
mockall = "0.13"
```

### Phase 1: Proto Generation + Storage Layer (Weeks 1-3)

**What to build:**
1. `gcs-proto` crate with `build.rs` running `tonic-build` on:
   - `src/ray/protobuf/common.proto`
   - `src/ray/protobuf/gcs.proto`
   - `src/ray/protobuf/gcs_service.proto`
   - `src/ray/protobuf/pubsub.proto`
   - `src/ray/protobuf/autoscaler.proto`
   - `src/ray/protobuf/events_event_aggregator_service.proto`

2. `gcs-store` crate implementing:

```rust
// store_client.rs
#[async_trait]
pub trait StoreClient: Send + Sync + 'static {
    async fn put(&self, table: &str, key: &str, data: String, overwrite: bool) -> bool;
    async fn get(&self, table: &str, key: &str) -> Option<String>;
    async fn get_all(&self, table: &str) -> HashMap<String, String>;
    async fn multi_get(&self, table: &str, keys: &[String]) -> HashMap<String, String>;
    async fn delete(&self, table: &str, key: &str) -> bool;
    async fn batch_delete(&self, table: &str, keys: &[String]) -> i64;
    async fn get_next_job_id(&self) -> i32;
    async fn get_keys(&self, table: &str, prefix: &str) -> Vec<String>;
    async fn exists(&self, table: &str, key: &str) -> bool;
}
```

**C++ -> Rust mapping for InMemoryStoreClient:**

| C++ | Rust |
|-----|------|
| `absl::flat_hash_map<string, absl::flat_hash_map<string, string>>` | `DashMap<String, DashMap<String, String>>` |
| `absl::Mutex` protecting maps | `DashMap` (lock-free concurrent) |
| Callback-based async (`Postable<void(bool)>`) | `async fn` returning `bool` directly |
| `io_service.post(callback)` | `tokio::spawn` or direct `await` |

**Testing:** Port all 6 C++ store_client tests to Rust `#[tokio::test]` functions.

**Why first:** Every other crate depends on proto types and storage.

---

### Phase 2: Table Storage + KV Layer (Weeks 3-5)

**What to build:**

1. `gcs-table-storage` crate:

```rust
// table.rs -- Maps C++ GcsTable<Key, Data> template
pub struct GcsTable<K: TableKey, V: prost::Message> {
    store: Arc<dyn StoreClient>,
    table_name: String,
    _phantom: PhantomData<(K, V)>,
}

impl<K: TableKey, V: prost::Message + Default> GcsTable<K, V> {
    pub async fn put(&self, key: &K, value: &V) -> bool { ... }
    pub async fn get(&self, key: &K) -> Option<V> { ... }
    pub async fn get_all(&self) -> HashMap<K, V> { ... }
    pub async fn delete(&self, key: &K) -> bool { ... }
    pub async fn batch_delete(&self, keys: &[K]) -> i64 { ... }
}

// GcsTableWithJobId adds secondary index by JobId
pub struct GcsTableWithJobId<K: TableKey, V: prost::Message> {
    inner: GcsTable<K, V>,
    job_index: DashMap<JobId, Vec<K>>,
}
```

2. `gcs-kv` crate:

```rust
// interface.rs -- Maps C++ InternalKVInterface
#[async_trait]
pub trait InternalKVInterface: Send + Sync {
    async fn get(&self, ns: &str, key: &str) -> Option<String>;
    async fn multi_get(&self, ns: &str, keys: &[String]) -> HashMap<String, String>;
    async fn put(&self, ns: &str, key: &str, value: String, overwrite: bool) -> bool;
    async fn del(&self, ns: &str, key: &str, del_by_prefix: bool) -> i64;
    async fn exists(&self, ns: &str, key: &str) -> bool;
    async fn keys(&self, ns: &str, prefix: &str) -> Vec<String>;
}
```

---

### Phase 3: Leaf Managers (Weeks 5-9)

Rewrite in complexity order. Each component has no cross-manager dependencies.

#### 3a. RuntimeEnvHandler (45 LOC)

```rust
// C++: HandlePinRuntimeEnvURI(request, reply, callback)
// Rust:
async fn pin_runtime_env_uri(
    &self,
    request: Request<PinRuntimeEnvUriRequest>,
) -> Result<Response<PinRuntimeEnvUriReply>, Status> {
    let req = request.into_inner();
    self.runtime_env_manager.add_uri_reference(req.uri.clone());
    if req.expiration_s > 0 {
        let manager = self.runtime_env_manager.clone();
        let uri = req.uri.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(req.expiration_s as u64)).await;
            manager.remove_uri_reference(&uri);
        });
    }
    Ok(Response::new(PinRuntimeEnvUriReply { status: Some(ok_status()) }))
}
```

#### 3b. GcsFunctionManager (83 LOC, header-only)
- Maps to Rust struct with `InternalKVInterface` dependency
- Tracks function reference counts, cleans up from KV on zero refs

#### 3c. UsageStatsClient (43 LOC)
- Simple KV writes for usage statistics

#### 3d. GcsRayEventConverter (~50 LOC)
- Pure proto transformation, no async

#### 3e. GcsWorkerManager (336 LOC)

```rust
pub struct GcsWorkerManager {
    workers: DashMap<WorkerId, WorkerTableData>,
    publisher: Arc<GcsPublisher>,
    table_storage: Arc<GcsTableStorage>,
    dead_listeners: Vec<Box<dyn Fn(&WorkerTableData) + Send + Sync>>,
}
```

Key mapping: C++ `worker_dead_listeners_` (vector of callbacks) -> Rust `Vec<Box<dyn Fn>>` or `tokio::sync::broadcast`.

#### 3f. GcsTaskManager (812 LOC)

Most complex leaf manager. Has GC policies:
- `MaxSizeGCPolicy` -- evict by priority when buffer exceeds limit
- Time-based GC -- drop events older than threshold

```rust
pub struct GcsTaskManager {
    task_events: BTreeMap<TaskAttempt, TaskEvents>,
    total_events_bytes: usize,
    max_events_bytes: usize,
    // GC eviction uses priority-based ordering
}
```

---

### Phase 4: PubSub Handler (Weeks 9-10)

**Critical async challenge:** The C++ `InternalPubSubHandler` uses gRPC long-polling. A subscriber calls `GcsSubscriberPoll`, which blocks until a message is available.

```rust
// Rust approach using tokio channels:
pub struct InternalPubSubHandler {
    // Per-subscriber: a channel that holds pending messages
    subscribers: DashMap<SubscriberId, mpsc::Sender<PubSubMessage>>,
}

// Long-poll implementation:
async fn gcs_subscriber_poll(
    &self,
    request: Request<GcsSubscriberPollRequest>,
) -> Result<Response<GcsSubscriberPollReply>, Status> {
    let subscriber_id = request.into_inner().subscriber_id;
    let rx = self.get_or_create_receiver(&subscriber_id);
    
    // Wait for message or timeout
    tokio::select! {
        msg = rx.recv() => {
            // Return message in reply
        }
        _ = tokio::time::sleep(self.long_poll_timeout) => {
            // Return empty reply
        }
    }
}
```

---

### Phase 5: Node Manager + Health Check (Weeks 10-13)

**GcsHealthCheckManager** in Rust:

```rust
// C++ uses grpc::health::v1::Health::Stub for health checks
// Rust uses tonic_health client
pub struct GcsHealthCheckManager {
    io_handle: tokio::runtime::Handle,
    on_node_death: Box<dyn Fn(NodeId) + Send + Sync>,
    nodes: DashMap<NodeId, NodeHealthState>,
    config: HealthCheckConfig,
}

struct NodeHealthState {
    channel: tonic::transport::Channel,
    failure_count: u32,
    health_check_handle: JoinHandle<()>,
}

// Each node gets a spawned health check loop:
async fn health_check_loop(
    node_id: NodeId,
    channel: Channel,
    config: HealthCheckConfig,
    failure_count: Arc<AtomicU32>,
    cancel_token: CancellationToken,
) {
    let mut client = HealthClient::new(channel);
    loop {
        tokio::select! {
            _ = cancel_token.cancelled() => break,
            _ = tokio::time::sleep(config.period) => {
                match client.check(HealthCheckRequest { service: "".into() }).await {
                    Ok(_) => failure_count.store(0, Ordering::Relaxed),
                    Err(_) => {
                        let count = failure_count.fetch_add(1, Ordering::Relaxed) + 1;
                        if count >= config.failure_threshold {
                            // Trigger on_node_death
                            break;
                        }
                    }
                }
            }
        }
    }
}
```

**GcsNodeManager** -- largest non-actor manager:

```rust
pub struct GcsNodeManager {
    alive_nodes: DashMap<NodeId, GcsNodeInfo>,
    dead_nodes: DashMap<NodeId, GcsNodeInfo>,
    draining_nodes: DashMap<NodeId, DrainNodeStatus>,
    publisher: Arc<GcsPublisher>,
    table_storage: Arc<GcsTableStorage>,
    health_check_manager: Arc<GcsHealthCheckManager>,
    // Listener pattern -> broadcast channels
    node_added_tx: broadcast::Sender<GcsNodeInfo>,
    node_removed_tx: broadcast::Sender<GcsNodeInfo>,
}
```

---

### Phase 6: Resource Manager + Job Manager (Weeks 13-16)

**GcsResourceManager** tracks per-node resources:
```rust
pub struct GcsResourceManager {
    node_resources: DashMap<NodeId, NodeResources>,  // total + available
    // Implements NodeResourceInfoGcsServiceHandler
}
```

**GcsJobManager** manages job lifecycle:
```rust
pub struct GcsJobManager {
    jobs: DashMap<JobId, JobTableData>,
    table_storage: Arc<GcsTableStorage>,
    publisher: Arc<GcsPublisher>,
    function_manager: Arc<GcsFunctionManager>,
    kv: Arc<dyn InternalKVInterface>,
    finished_listeners: Vec<Box<dyn Fn(&JobTableData) + Send + Sync>>,
}
```

---

### Phase 7: Actor Manager + Scheduler (Weeks 16-20) -- MOST COMPLEX

The actor manager implements a complex state machine:

```
DEPENDENCIES_UNREADY
    |
    v (all deps resolved)
PENDING_CREATION
    |
    v (actor created on worker)
ALIVE
    |
    +---> RESTARTING (on worker failure, if max_restarts > 0)
    |         |
    |         v (new worker found)
    |     ALIVE
    |
    +---> DEAD (max_restarts exhausted, or explicit kill)
```

```rust
pub struct GcsActorManager {
    // Actor state maps
    registered_actors: DashMap<ActorId, GcsActor>,
    created_actors: DashMap<ActorId, GcsActor>,
    named_actors: DashMap<(String, String), ActorId>,  // (namespace, name) -> id
    
    // Dependencies
    actor_scheduler: Arc<GcsActorScheduler>,
    table_storage: Arc<GcsTableStorage>,
    publisher: Arc<GcsPublisher>,
    function_manager: Arc<GcsFunctionManager>,
    node_manager: Arc<GcsNodeManager>,
    
    // Subscriber for node death events
    node_death_rx: broadcast::Receiver<GcsNodeInfo>,
}

pub struct GcsActor {
    table_data: ActorTableData,
    state: ActorState,
    // Callbacks waiting for actor creation
    creation_callbacks: Vec<oneshot::Sender<Result<(), Status>>>,
}
```

---

### Phase 8: Placement Group Manager + Scheduler (Weeks 20-23)

Similar complexity to Actor but for resource bundles:

```rust
pub struct GcsPlacementGroupManager {
    placement_groups: DashMap<PlacementGroupId, GcsPlacementGroup>,
    named_pgs: DashMap<(String, String), PlacementGroupId>,
    pg_scheduler: Arc<GcsPlacementGroupScheduler>,
    table_storage: Arc<GcsTableStorage>,
    publisher: Arc<GcsPublisher>,
    node_manager: Arc<GcsNodeManager>,
}
```

---

### Phase 9: Autoscaler State Manager (Weeks 23-24)

Aggregates state from multiple managers for the autoscaler:

```rust
pub struct GcsAutoscalerStateManager {
    node_manager: Arc<GcsNodeManager>,
    actor_manager: Arc<GcsActorManager>,
    pg_manager: Arc<GcsPlacementGroupManager>,
    kv: Arc<dyn InternalKVInterface>,
    autoscaling_state: RwLock<Option<AutoscalingState>>,
}
```

---

### Phase 10: Server Orchestrator + gRPC Wiring (Weeks 24-27)

```rust
// main.rs
#[tokio::main]
async fn main() -> Result<()> {
    let config = GcsServerConfig::from_args();
    tracing_subscriber::init();
    
    let server = GcsServer::new(config).await?;
    server.start().await?;
    
    // Wait for SIGTERM
    tokio::signal::ctrl_c().await?;
    server.stop().await;
    Ok(())
}

// server.rs
pub struct GcsServer {
    config: GcsServerConfig,
    // All managers owned here
    table_storage: Arc<GcsTableStorage>,
    node_manager: Arc<GcsNodeManager>,
    actor_manager: Arc<GcsActorManager>,
    job_manager: Arc<GcsJobManager>,
    // ... all other managers
}

impl GcsServer {
    pub async fn start(&self) -> Result<()> {
        // Load init data
        let init_data = GcsInitData::load(&self.table_storage).await?;
        
        // Initialize all managers (same order as C++)
        self.node_manager.init(&init_data).await;
        self.actor_manager.init(&init_data).await;
        // ...
        
        // Build tonic server with all services
        let server = Server::builder()
            .add_service(JobInfoGcsServiceServer::new(self.job_manager.clone()))
            .add_service(ActorInfoGcsServiceServer::new(self.actor_manager.clone()))
            .add_service(NodeInfoGcsServiceServer::new(self.node_manager.clone()))
            .add_service(NodeResourceInfoGcsServiceServer::new(self.resource_manager.clone()))
            .add_service(WorkerInfoGcsServiceServer::new(self.worker_manager.clone()))
            .add_service(PlacementGroupInfoGcsServiceServer::new(self.pg_manager.clone()))
            .add_service(TaskInfoGcsServiceServer::new(self.task_manager.clone()))
            .add_service(InternalKvGcsServiceServer::new(self.kv_manager.clone()))
            .add_service(InternalPubSubGcsServiceServer::new(self.pubsub_handler.clone()))
            .add_service(RuntimeEnvGcsServiceServer::new(self.runtime_env_handler.clone()))
            .add_service(AutoscalerStateServiceServer::new(self.autoscaler_manager.clone()))
            .add_service(HealthServer::new(self.health_service.clone()))
            .serve(addr);
        
        server.await?;
        Ok(())
    }
}
```

**Critical testing for this phase:**
1. Start Rust GCS server on random port
2. Run C++ `gcs_server_rpc_test.cc` pointing at the Rust server
3. All 54 RPC handler tests must pass

---

### Phase 11: Validation and Cutover (Weeks 27-30)

1. **Feature flag:** Add config option `ray.gcs.server_binary = "rust"` to select implementation
2. **Full test suite:** Run ALL Ray tests (C++, Python) with Rust GCS
3. **Shadow mode:** Run both C++ and Rust GCS, compare all responses
4. **Benchmarks:** Latency, throughput, memory, startup time
5. **Stress tests:** 10K actors, 1K nodes, 100K KV entries

---

## 6. C++ to Rust Pattern Mapping

### 6.1 Core Patterns

| C++ Pattern | Rust Equivalent | Notes |
|-------------|----------------|-------|
| `Postable<void(T)>` callback | `async fn -> T` | Eliminates callback hell |
| `SendReplyCallback` | Tonic `Response<T>` return | Direct return instead of callback |
| `shared_ptr<T>` | `Arc<T>` | Same semantics |
| `unique_ptr<T>` | `Box<T>` or owned value | Rust defaults to ownership |
| `absl::flat_hash_map` | `HashMap` or `DashMap` | DashMap for concurrent access |
| `absl::flat_hash_set` | `HashSet` or `DashSet` | |
| `absl::Mutex` + guarded data | `tokio::sync::RwLock<T>` or `DashMap` | Rust enforces lock discipline |
| `std::atomic<bool>` | `std::sync::atomic::AtomicBool` | Same |
| `instrumented_io_context` (Boost ASIO) | `tokio::runtime::Runtime` | Tokio is the async runtime |
| `io_context.post(callback)` | `tokio::spawn(async { })` | |
| `PeriodicalRunner` | `tokio::time::interval` + `tokio::spawn` | |
| `boost::asio::deadline_timer` | `tokio::time::sleep` | |
| `grpc::Server` | `tonic::transport::Server` | |
| `grpc::Channel` | `tonic::transport::Channel` | |
| `RAY_LOG(INFO) << msg` | `tracing::info!(msg)` | Structured logging |
| `RAY_CHECK(cond)` | `assert!` or `anyhow::ensure!` | |
| `Status::OK()` | `Ok(Response::new(reply))` | Tonic error handling |
| `Status::Invalid(msg)` | `Err(Status::invalid_argument(msg))` | |
| `GCS_RPC_SEND_REPLY(cb, reply, status)` | Direct return from async fn | No macro needed |

### 6.2 Async Model Transformation

**C++ (callback-based):**
```cpp
void GcsJobManager::HandleAddJob(AddJobRequest request,
                                  AddJobReply *reply,
                                  SendReplyCallback send_reply_callback) {
  auto job_id = JobID::FromBinary(request.data().job_id());
  auto on_done = [reply, send_reply_callback](Status status) {
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  };
  gcs_table_storage_->JobTable().Put(job_id, request.data(), on_done);
}
```

**Rust (async/await):**
```rust
async fn add_job(
    &self,
    request: Request<AddJobRequest>,
) -> Result<Response<AddJobReply>, Status> {
    let data = request.into_inner().data.ok_or(Status::invalid_argument("missing data"))?;
    let job_id = JobId::from_binary(&data.job_id);
    self.table_storage.job_table().put(&job_id, &data).await;
    Ok(Response::new(AddJobReply { status: Some(ok_status()) }))
}
```

### 6.3 Listener/Observer Pattern

**C++ (vector of callbacks):**
```cpp
std::vector<std::function<void(const NodeID &)>> node_added_listeners_;
// Registration:
node_added_listeners_.emplace_back(listener);
// Notification:
for (auto &listener : node_added_listeners_) { listener(node_id); }
```

**Rust (broadcast channel):**
```rust
let (node_added_tx, _) = broadcast::channel(1024);
// Registration:
let rx = node_added_tx.subscribe();
// Notification:
let _ = node_added_tx.send(node_id);
// Consumer:
tokio::spawn(async move { while let Ok(id) = rx.recv().await { ... } });
```

---

## 7. Architectural Decisions and Trade-Offs

### 7.1 Rewrite Strategy: gRPC Boundary vs FFI Bridge

| Approach | Pros | Cons |
|----------|------|------|
| **gRPC boundary (chosen)** | Clean separation, no unsafe code, same wire protocol, easy rollback | Must rewrite entire GCS at once to deploy (but can test incrementally) |
| FFI bridge (rejected) | Could deploy one manager at a time | Complex memory management across language boundary, Boost ASIO/Tokio interop nightmare, every type needs FFI wrapper |

**Decision: gRPC boundary.** The risk of FFI interop bugs far exceeds the benefit of per-manager deployment. The gRPC test suite provides a definitive compatibility check.

### 7.2 Binary Structure: Workspace of Crates vs Monolith

| Approach | Pros | Cons |
|----------|------|------|
| **Workspace (chosen)** | Parallel compilation, independent testing, clean dependency graph | More Cargo.toml files to maintain |
| Monolith | Single build target | Slow incremental builds, harder to test in isolation |

**Decision: Workspace.** 8 crates with clear dependencies. Compilation parallelism is critical for a codebase this size.

### 7.3 Concurrency Model

| C++ Model | Rust Model | Notes |
|-----------|------------|-------|
| Single io_context thread + posted callbacks | Tokio multi-threaded runtime | Rust GCS can be truly concurrent |
| Managers are NOT thread-safe (single-threaded assumption) | Managers use `DashMap`/`RwLock` for safe concurrency | Performance improvement opportunity |

**Note:** The C++ GCS runs all managers on a single thread (the io_context event loop). The Rust version can exploit multi-core better using Tokio's work-stealing scheduler. This is a deliberate architectural improvement, but we must ensure no behavior changes (e.g., operation ordering assumptions).

### 7.4 Redis Client

| C++ | Rust |
|-----|------|
| Custom `RedisContext`/`RedisAsyncContext` wrapping hiredis | `redis` crate with `tokio-comp` feature |
| Manual connection pooling and retry logic | `redis::aio::ConnectionManager` (built-in pooling and reconnect) |
| Custom key namespace prefix | Same prefix logic, implemented in Rust wrapper |

---

## 8. Post-Rewrite Report Template

After the rewrite is complete, the following report should be produced:

### Section 1: Scope and Completeness

- Table: all 54 RPC handler methods, C++ file:line -> Rust file:line, behavioral changes (if any)
- LOC comparison: C++ lines removed vs Rust lines added, per component
- Crate dependency graph (generated from `cargo-deps`)

### Section 2: Test Parity

- Matrix: every C++ test name -> Rust test name, pass/fail status
- C++ `gcs_server_rpc_test.cc` results against Rust server (all 54 RPCs)
- Python integration test results against Rust server
- Coverage metrics from `cargo-llvm-cov`

### Section 3: Architecture Differences at Code Level

For each manager, document:
- Which C++ classes map to which Rust structs/traits
- How async patterns changed (callbacks -> async/await)
- Thread safety changes (single-threaded -> concurrent)
- Error handling changes (Status codes -> Result types)
- Memory management changes (shared_ptr -> Arc, unique_ptr -> owned)

### Section 4: Performance Comparison

- Microbenchmarks per RPC method (p50/p95/p99 latency)
- Throughput: max RPCs/second for hot paths (RegisterActor, GetAllActorInfo)
- Memory footprint: idle + under load (10K actors, 1K nodes)
- Startup time: cold start + recovery from Redis

### Section 5: Known Issues and Deferred Work

- Any behavioral deviations from C++ (with justification)
- Features simplified or deferred
- Technical debt created during rewrite
- Optimization opportunities identified

### Section 6: Deployment Guide

- How to configure Ray to use Rust GCS (`ray.gcs.server_binary = "rust"`)
- Rollback procedure
- Monitoring dashboards and alerts
- Redis compatibility notes

---

## 9. Risk Analysis

### High Risk

| Risk | Impact | Mitigation |
|------|--------|------------|
| Actor state machine behavioral mismatch | Actors fail to schedule/restart correctly | Extensive deterministic test scenarios, record C++ outputs and compare |
| Placement group scheduling divergence | Resource allocation failures | Side-by-side comparison of scheduling decisions |
| Redis data format incompatibility | Cannot switch between C++/Rust GCS without data migration | Use identical Redis key/value formats, test with shared Redis instance |
| Long-poll PubSub timing differences | Subscribers miss notifications or see duplicates | Stress test pub/sub with many concurrent subscribers |

### Medium Risk

| Risk | Impact | Mitigation |
|------|--------|------------|
| Tokio runtime behavior differs from Boost ASIO | Subtle ordering bugs | Single-threaded Tokio runtime option for initial validation |
| gRPC metadata/header handling differences | Client compatibility issues | Test with existing C++ and Python gRPC clients |
| Proto codegen differences (prost vs protoc-gen-grpc) | Wire format issues | Binary compatibility tests with captured C++ gRPC payloads |

### Low Risk

| Risk | Impact | Mitigation |
|------|--------|------------|
| Rust compile times | Slower development iteration | Workspace crates for parallel compilation |
| Bazel+Cargo integration complexity | Build system issues | Can develop with Cargo, integrate with Bazel later |

---

## 10. Appendix: File Inventory

### 10.1 C++ Source Files to Rewrite

**Server Core (3 files):**
- `src/ray/gcs/gcs_server_main.cc` -- Entry point
- `src/ray/gcs/gcs_server.h` -- 324 lines
- `src/ray/gcs/gcs_server.cc` -- ~1,000 lines

**Manager/Handler Components (19 files):**
- `src/ray/gcs/gcs_node_manager.h/cc` -- 17K + 33K bytes
- `src/ray/gcs/actor/gcs_actor.h/cc` -- Actor data structure
- `src/ray/gcs/actor/gcs_actor_manager.h/cc` -- 27K + 53K bytes (LARGEST)
- `src/ray/gcs/actor/gcs_actor_scheduler.h/cc` -- 19K + 37K bytes
- `src/ray/gcs/gcs_job_manager.h/cc` -- 6.7K + 23K bytes
- `src/ray/gcs/gcs_task_manager.h/cc` -- 21K + 32K bytes
- `src/ray/gcs/gcs_resource_manager.h/cc` -- 7.8K + 13K bytes
- `src/ray/gcs/gcs_worker_manager.h/cc` -- 3.8K + 14K bytes
- `src/ray/gcs/gcs_health_check_manager.h/cc` -- 7.1K + 9K bytes
- `src/ray/gcs/gcs_autoscaler_state_manager.h/cc` -- 10.6K + 30K bytes
- `src/ray/gcs/gcs_placement_group.h/cc` -- 9.4K + 6.6K bytes
- `src/ray/gcs/gcs_placement_group_manager.h/cc` -- 17K + 46K bytes
- `src/ray/gcs/gcs_placement_group_scheduler.h/cc` -- 22K + 43K bytes
- `src/ray/gcs/pubsub_handler.h/cc` -- PubSub handler
- `src/ray/gcs/runtime_env_handler.h/cc` -- Runtime env
- `src/ray/gcs/gcs_kv_manager.h/cc` -- KV manager
- `src/ray/gcs/gcs_function_manager.h` -- Function manager (header-only)
- `src/ray/gcs/usage_stats_client.h/cc` -- Usage stats
- `src/ray/gcs/gcs_ray_event_converter.h/cc` -- Event converter

**Storage Layer (8 files):**
- `src/ray/gcs/store_client/store_client.h` -- Abstract interface
- `src/ray/gcs/store_client/in_memory_store_client.h/cc` -- In-memory impl
- `src/ray/gcs/store_client/redis_store_client.h/cc` -- Redis impl (220K+, very large)
- `src/ray/gcs/store_client/redis_context.h/cc` -- Redis connection
- `src/ray/gcs/store_client/redis_async_context.h/cc` -- Async Redis
- `src/ray/gcs/store_client/observable_store_client.h/cc` -- Metrics decorator

**Infrastructure (6 files):**
- `src/ray/gcs/gcs_table_storage.h/cc` -- Typed table storage
- `src/ray/gcs/gcs_init_data.h/cc` -- Startup recovery
- `src/ray/gcs/store_client_kv.h/cc` -- KV store client wrapper
- `src/ray/gcs/grpc_service_interfaces.h` -- Service handler interfaces
- `src/ray/gcs/grpc_services.h/cc` -- gRPC service registration
- `src/ray/gcs/gcs_server_io_context_policy.h` -- IO context threading

**Proto Definitions (6 files, shared -- NOT rewritten):**
- `src/ray/protobuf/gcs_service.proto` -- 893 lines
- `src/ray/protobuf/gcs.proto` -- 721 lines
- `src/ray/protobuf/common.proto` -- Core types
- `src/ray/protobuf/pubsub.proto` -- Pub/sub types
- `src/ray/protobuf/autoscaler.proto` -- Autoscaler types
- `src/ray/protobuf/events_event_aggregator_service.proto` -- Event types

### 10.2 Test Files (38 existing + 9 new/extended)

See Section 3 for complete inventory.

### 10.3 Build Commands

**C++ tests (existing):**
```bash
bazel test //src/ray/gcs/... --dynamic_mode=off --keep_going --jobs=4 --test_output=errors
```

**Rust build (after Phase 1):**
```bash
cd ray/src/ray/rust/gcs && cargo build --workspace
```

**Rust tests (after Phase 1):**
```bash
cd ray/src/ray/rust/gcs && cargo test --workspace
```

**Cross-language RPC test (after Phase 10):**
```bash
# Start Rust GCS
cd ray/src/ray/rust/gcs && cargo run --bin gcs-server -- --port 0 &
# Run C++ RPC tests against Rust server
bazel test //src/ray/gcs/tests:gcs_server_rpc_test --test_env=GCS_SERVER_PORT=$RUST_GCS_PORT
```
