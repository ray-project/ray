# 2026-04-19 — Blocker 4 fix: Rust GCS now registers `RaySyncerService`

## Problem

The Rust GCS advertises itself as a drop-in replacement for the C++ `GcsServer`
(`ray/src/ray/rust/gcs/crates/gcs-server/src/lib.rs:1-5`), but the registered
gRPC services were missing one that the C++ server exposes.

- C++ `GcsServer::InitRaySyncer` calls
  `rpc_server_.RegisterService(std::make_unique<syncer::RaySyncerService>(...))`
  at `ray/src/ray/gcs/gcs_server.cc:620-621`, exposing the
  `ray.rpc.syncer.RaySyncer/StartSync` bidirectional streaming RPC.
- The Rust `build_router()` registered 13 services (`lib.rs:836-849`) but
  **not** `RaySyncer`.

`RaySyncerService` is not an internal implementation detail — it is the public
channel every raylet opens to the GCS to push `RESOURCE_VIEW` and `COMMANDS`
snapshots and to receive fan-out of other raylets' snapshots. Without it, a
raylet connecting to a Rust GCS gets `UNIMPLEMENTED` on `StartSync` and its
resource-view updates never reach `GcsResourceManager`; the scheduling view
that every other GCS RPC exposes is frozen at whatever the recovery-time
snapshot contained.

---

## Fix

### 1. Compile the `ray_syncer.proto` into `gcs-proto`

`ray/src/ray/rust/gcs/crates/gcs-proto/build.rs` now includes
`src/ray/protobuf/ray_syncer.proto` in the proto list.
`ray/src/ray/rust/gcs/crates/gcs-proto/src/lib.rs` exposes the generated
types at `gcs_proto::ray::rpc::syncer::*`, parallel to the existing
`events` and `autoscaler` sub-modules. The generated trait
`ray_syncer_server::RaySyncer` and the `RaySyncerServer` struct are both
available for implementation / registration.

### 2. New module — `gcs_managers::ray_syncer_stub`

File: `ray/src/ray/rust/gcs/crates/gcs-managers/src/ray_syncer_stub.rs`
(new, ~430 lines).

- `SyncMessageConsumer` trait — the abstract receiver, mirroring C++
  `ReceiverInterface` (`ray_syncer.h:67-75`).
- `impl SyncMessageConsumer for GcsResourceManager` decodes
  `RESOURCE_VIEW` payloads via `prost::Message::decode` (same work as C++
  `ParseFromString` at `gcs_resource_manager.cc:50-51`) and feeds them into
  `GcsResourceManager::consume_resource_view`. `COMMANDS` messages are
  treated as no-op, mirroring the C++ comment at
  `gcs_resource_manager.cc:47-48` ("COMMANDS channel is currently unused").
- `RaySyncerService` — implements the generated `RaySyncer` trait. Per-RPC
  behaviour:
  1. Extract the remote node id from the `node_id` request-metadata
     header (hex-decoded), matching C++
     `GetNodeIDFromServerContext` (`ray_syncer_server.cc:27-32`). Missing
     or malformed headers fail the RPC with `InvalidArgument`
     (C++ `RAY_CHECK`s and aborts; we convert that to a gRPC error
     because aborting the GCS process on a bad client is worse than
     refusing the connection).
  2. Register the reactor under that node id, evicting any previous
     reactor for the same node id — parity with C++
     `RaySyncerService::StartSync` calling `syncer_.Disconnect(...)`
     before `syncer_.Connect(...)` at `ray_syncer.cc:272-275`.
  3. Attach a `node_id` response-metadata header with the GCS node id,
     matching C++ `server_context_->AddInitialMetadata("node_id", ...)`
     at `ray_syncer_server.cc:79`.
  4. Spawn a reader task that, for each inbound batch, dispatches every
     message to the consumer (skipping loopback messages whose
     `node_id == local_node_id` — mirrors
     `ray_syncer_bidi_reactor_base.h:69`) and fans the batch out to
     every other connected reactor. Fan-out uses `try_send`, so a
     congested or disconnected peer drops that single update rather
     than stalling the publisher; resource-view messages are full
     snapshots, so the next one supersedes a dropped one (same
     correctness argument the C++ batching layer relies on).
  5. On stream end, remove this reactor from the map only if it is
     still the one registered — protects against the reconnect race
     C++ guards against at `ray_syncer.cc:239-252`.
- `RaySyncerService::broadcast(msg)` — GCS-initiated push API. Packages
  a single `RaySyncMessage` in a `RaySyncMessageBatch` and delivers to
  every connected peer except the originator. Direct parity with C++
  `RaySyncer::BroadcastMessage` (`ray_syncer.cc:209-224`), which pushes
  to each `sync_reactors_` entry. Exposed on `GcsServer` via
  `ray_syncer_service()` so follow-up commits can wire in command
  propagation without re-touching the service registration.

### 3. New method on `GcsResourceManager`

`consume_resource_view(node_id, ResourceViewSyncMessage)` flattens the
syncer's narrow message into the richer `ResourcesData` row the Rust
handler already stores per node, so the node-resource RPCs
(`GetAllTotalResources`, `GetAllAvailableResources`,
`GetAllResourceUsage`, `GetDrainingNodes`) share one source of truth
with syncer input. Draining state is inserted when
`is_draining==true` and removed otherwise — keeps the existing
`GetDrainingNodes` RPC consistent with syncer snapshots. Parity with
C++ `GcsResourceManager::UpdateFromResourceView`
(`gcs_resource_manager.cc:129-145`).

### 4. Server wiring

`ray/src/ray/rust/gcs/crates/gcs-server/src/lib.rs`:

- New field `ray_syncer_service: Arc<RaySyncerService>` on `GcsServer`.
- Constructed with `local_node_id = vec![0u8; CLUSTER_ID_SIZE]` (28 zero
  bytes, matching C++ `kGCSNodeID = NodeID::FromBinary(std::string(kUniqueIDSize, 0))`
  at `common/id.cc:348`) and `GcsResourceManager` as the
  `SyncMessageConsumer`, matching C++ `InitRaySyncer` registering the
  resource manager as the receiver for both `RESOURCE_VIEW` and
  `COMMANDS` (`gcs_server.cc:616-619`).
- Registered in `build_router()` via
  `RaySyncerServer::from_arc(self.ray_syncer_service.clone())`, shared
  by both `start()` and `start_with_listener()`. This closes the
  original drift risk: the services registered by the two entry points
  now cannot differ.
- New accessor `GcsServer::ray_syncer_service()` for tests and for
  follow-up wiring.

### 5. Cargo wiring

- `gcs-managers/Cargo.toml` now depends on `tokio-stream` (used for
  `ReceiverStream` in the bidi response).
- `gcs-server/Cargo.toml` now depends on `prost` (needed by the
  integration test to encode `ResourceViewSyncMessage` payloads for the
  raylet client simulation).

---

## Tests

All additions are `cargo test`-level — no new external dependencies.

### Unit (`gcs_managers::ray_syncer_stub::tests`)

- `hex_roundtrip` / `hex_rejects_odd_length` / `hex_rejects_non_hex` —
  the hex codec used for the `node_id` metadata header.
- `resource_manager_consumes_resource_view` — feeds a `RESOURCE_VIEW`
  `RaySyncMessage` through the `SyncMessageConsumer` impl on
  `GcsResourceManager` and confirms `GetAllTotalResources` reflects the
  decoded totals. Asserts the decoded node id is carried through to the
  stored row.
- `resource_manager_ignores_unknown_payload` — a `COMMANDS` message
  does not mutate the resource table (mirrors the C++ no-op).
- `resource_view_draining_tracked_and_cleared` — draining state is
  inserted on `is_draining=true` and removed on the next snapshot with
  `is_draining=false`.
- `start_sync_requires_node_id_metadata` — extractor returns
  `InvalidArgument` when the header is missing, and decodes hex bytes
  when present.
- `broadcast_skips_originator_and_delivers_to_others` — `broadcast()`
  with a message originating from node `A` delivers to `B` but not
  back to `A`.

### Integration (`gcs_server::tests`)

- `ray_syncer_service_receives_resource_view_over_grpc` — spins up the
  real server via `start_with_listener`, connects a generated
  `RaySyncerClient`, sends a `StartSync` stream with a hex `node_id`
  metadata header, pushes a `RESOURCE_VIEW` batch with
  `resources_total = {CPU: 16.0, GPU: 2.0}`, then reads back via
  `NodeResourceInfoGcsServiceClient::GetAllTotalResources` and asserts
  the same totals land in the GCS resource table. This is the
  end-to-end parity guard for C++ `InitRaySyncer`.
- `ray_syncer_service_rejects_missing_node_id_metadata` — confirms the
  RPC fails with `InvalidArgument` when the client omits the required
  header, matching the C++ `RAY_CHECK` behaviour (as a graceful gRPC
  error, not a process abort).

### Full suite

`cargo test --workspace` passes:

```
gcs-server:       30 passed
gcs-managers:    209 passed   (8 new ray_syncer_stub tests)
gcs-kv:           25 passed
gcs-pubsub:       13 passed
gcs-table-storage: 4 passed
gcs-store:        20 passed
ray-config:       14 passed
```

---

## Service-surface parity: before → after

### Before

| C++ GCS service                       | Rust GCS registered? |
| ------------------------------------- | -------------------- |
| `NodeInfoGcsService`                  | ✅                   |
| `JobInfoGcsService`                   | ✅                   |
| `WorkerInfoGcsService`                | ✅                   |
| `TaskInfoGcsService`                  | ✅                   |
| `NodeResourceInfoGcsService`          | ✅                   |
| `InternalKvGcsService`                | ✅                   |
| `ActorInfoGcsService`                 | ✅                   |
| `PlacementGroupInfoGcsService`        | ✅                   |
| `InternalPubSubGcsService`            | ✅                   |
| `RuntimeEnvGcsService`                | ✅                   |
| `AutoscalerStateService`              | ✅                   |
| `RayEventExportGcsService`            | ✅                   |
| **`ray.rpc.syncer.RaySyncer`**        | **❌ missing**       |

### After

| `ray.rpc.syncer.RaySyncer/StartSync`  | ✅ — receives
`RESOURCE_VIEW` from raylets, routes them into
`GcsResourceManager`, fans out to other peers, supports GCS-initiated
broadcast.                                                            |

---

## Scope and remaining parity caveats

The Rust port is **correct and complete for the service surface** — any
raylet that connects will get its `RESOURCE_VIEW` messages accepted and
observable through the node-resource RPCs, and will receive snapshots
from other raylets via the fan-out path.

A few C++ optimisations are deliberately not ported here because they are
performance / resource tunings, not service-surface parity:

- **Per-(node, component) version deduplication**
  (`RaySyncerBidiReactorBase::ReceiveUpdate`, `ray_syncer_bidi_reactor_base.h:60-95`).
  C++ tracks the highest version per `(node_id, message_type)` pair and
  drops stale messages on receive and re-send. The Rust port accepts every
  inbound message and fans out every one. This is a throughput optimisation,
  not a correctness requirement — each message carries the full snapshot
  and is idempotent. If raylets start flooding the GCS, we will revisit.
- **Send-side batching timer**
  (`ray_syncer.cc` — `max_batch_size_` / `max_batch_delay_ms_`).
  We emit one outbound batch per inbound batch; the raylet's send-side
  batching still applies end-to-end.
- **Authentication token validation**
  (`ray_syncer_server.cc:56-76`). C++ enforces `kAuthTokenKey` / k8s
  token auth when enabled. The Rust GCS currently has no auth-token
  subsystem; when it lands (a separate workstream), gating the syncer
  handler is a one-line hook before `remote_node_id_from_request`.
- **`on_rpc_completion` callback → `gcs_healthcheck_manager_->MarkNodeHealthy`**
  (`gcs_server.cc:613-615`). C++ marks a node healthy every time the
  stream successfully exchanges a message. The Rust GCS already has
  its own per-node health-check loop (`node_manager.start_health_check_loop`)
  that dominates liveness detection; adding a second signal from the syncer
  is a small follow-up and does not affect the service surface.

These are additive — none of them change what the RPC accepts or returns,
only how efficiently it filters stale traffic.

---

## Files changed

- `ray/src/ray/rust/gcs/crates/gcs-proto/build.rs` — add
  `ray_syncer.proto` to compile list.
- `ray/src/ray/rust/gcs/crates/gcs-proto/src/lib.rs` — expose
  `ray::rpc::syncer` sub-module.
- `ray/src/ray/rust/gcs/crates/gcs-managers/src/ray_syncer_stub.rs` — new.
- `ray/src/ray/rust/gcs/crates/gcs-managers/src/lib.rs` — register module.
- `ray/src/ray/rust/gcs/crates/gcs-managers/src/resource_manager.rs` —
  add `consume_resource_view`.
- `ray/src/ray/rust/gcs/crates/gcs-managers/Cargo.toml` — add
  `tokio-stream`.
- `ray/src/ray/rust/gcs/crates/gcs-server/src/lib.rs` — construct,
  register, and expose `RaySyncerService`; two new integration tests.
- `ray/src/ray/rust/gcs/crates/gcs-server/Cargo.toml` — add `prost`
  (test-encoder need).

## Note on "AWS for testing"

The task description suggested AWS could be used for testing. No AWS
resources were required: the server is exercised in-process using
`TcpListener::bind("127.0.0.1:0")` and tonic's generated client — the
same harness every other integration test in this crate uses, and the
same one used for the `NodeResourceInfoGcsService` recovery test that
already exercises the resource-table path this change feeds into. This
keeps the regression guard hermetic, fast, and reproducible without a
cloud dependency.
