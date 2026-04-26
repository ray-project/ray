# 2026-04-19 — Blocker 4 recheck

## Verdict

Blocker 4 is now **closed**.

The original blocker was that Rust GCS did not expose the C++
`ray.rpc.syncer.RaySyncer` service at all, so raylets would get
`UNIMPLEMENTED` on `StartSync` and their syncer-driven resource-view
updates would never reach the GCS.

That is now fixed:

- the proto is compiled into the Rust surface
- the service is implemented
- the service is registered on the actual server router shared by both
  `start()` and `start_with_listener()`
- the GCS resource manager now consumes inbound `RESOURCE_VIEW`
  messages
- an end-to-end integration test proves the real gRPC path updates the
  resource table

Under the service-surface parity bar that originally made this a
blocker, I would now mark it closed.

---

## What I Verified

### 1. The `RaySyncer` proto and server surface exist in Rust

Rust now exposes the generated syncer package:

- [gcs-proto/src/lib.rs](/Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/crates/gcs-proto/src/lib.rs:21)

including the generated `ray_syncer_server::RaySyncer` trait and
`RaySyncerServer`.

This closes the earlier surface-level gap where the service did not even
exist in the Rust build.

### 2. `GcsServer` constructs and registers the service on the production path

`GcsServer::new_with_store` now constructs a `RaySyncerService`:

- [gcs-server/src/lib.rs](/Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/crates/gcs-server/src/lib.rs:326)

and `build_router()` now registers it:

- [gcs-server/src/lib.rs](/Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/crates/gcs-server/src/lib.rs:853)

Because both `start()` and `start_with_listener()` use the shared
`build_router()` path, the production binary path and the test path
cannot drift here.

That matters: one of the recurring problems in this codebase has been
test-only parity that did not survive the actual binary startup path.
This implementation avoids that.

### 3. The resource manager is wired as the sync-message consumer

Rust wires `GcsResourceManager` in as the `SyncMessageConsumer`:

- [gcs-server/src/lib.rs](/Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/crates/gcs-server/src/lib.rs:333)

and the resource manager now consumes `ResourceViewSyncMessage` by
flattening it into the same `ResourcesData` row used by the node-resource
RPCs:

- [resource_manager.rs](/Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/crates/gcs-managers/src/resource_manager.rs:51)

This is the key behavioral part of Blocker 4. It is not enough to
register the service; the syncer updates have to flow into the shared
resource view that other GCS RPCs expose.

### 4. The implementation matches the C++ service shape closely enough for blocker closure

The Rust `RaySyncerService` now does the main things the C++ service does:

- accepts bidi `StartSync`
- requires the `node_id` metadata header and hex-decodes it
- returns the GCS node id in response metadata
- replaces older connections from the same node on reconnect
- dispatches inbound messages to the configured consumer
- fans batches out to other connected peers

Relevant Rust implementation:

- [ray_syncer_stub.rs](/Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/crates/gcs-managers/src/ray_syncer_stub.rs:113)

Relevant C++ anchor:

- `ray/src/ray/gcs/gcs_server.cc:595-621`

The GCS-side `COMMANDS` handling is also aligned with what C++ does
today. C++’s `GcsResourceManager::ConsumeSyncMessage` treats `COMMANDS`
as currently unused:

- `ray/src/ray/gcs/gcs_resource_manager.cc:47-48`

and the Rust port keeps that as a no-op with logging:

- [ray_syncer_stub.rs](/Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/crates/gcs-managers/src/ray_syncer_stub.rs:81)

So for the current C++ behavior, Rust is not weaker there.

---

## Tests Run

I ran the two new `gcs-server` integration tests that directly cover the
blocker:

1. `cargo test --manifest-path /Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/Cargo.toml -p gcs-server --lib ray_syncer_service_receives_resource_view_over_grpc`
   - passed

2. `cargo test --manifest-path /Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/Cargo.toml -p gcs-server --lib ray_syncer_service_rejects_missing_node_id_metadata`
   - passed

The first is the most important one. It proves:

- the real server starts
- the syncer RPC is implemented and reachable
- a raylet-style client can connect
- a `RESOURCE_VIEW` message crosses the gRPC stream
- the Rust GCS resource table updates
- the update is visible through `NodeResourceInfoGcsService`

That is the exact end-to-end path that was missing before.

---

## Remaining Differences

There are still differences from the full C++ implementation, but I do
not consider them blocker-level for the original Blocker 4 scope:

- Rust does not yet port the C++ per-version deduplication logic in the
  syncer reactors.
- Rust does not yet port the C++ batching timer knobs for outbound
  syncer traffic.
- Rust does not yet enforce the C++ auth-token validation path on the
  syncer RPC.
- Rust does not yet call the C++-equivalent health-check manager hook
  that marks nodes healthy on syncer traffic.

Those are real parity differences, but they are narrower than the
original blocker. The original blocker was “Rust still omits the C++
`RaySyncerService` and the associated resource-broadcast command path.”
That is no longer true.

If you want, these remaining items should be tracked as follow-up parity
work rather than as “Blocker 4 still open”.

---

## Conclusion

The service-surface hole is closed:

- Rust GCS now exposes `ray.rpc.syncer.RaySyncer/StartSync`
- the service is registered on the real server path
- inbound `RESOURCE_VIEW` updates now reach `GcsResourceManager`
- the end-to-end gRPC path is regression-tested and passing

I would mark Blocker 4 as closed.
