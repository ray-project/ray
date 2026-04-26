# 2026-04-19 — Blocker 5 recheck

## Verdict

Blocker 5 is now **closed**.

The original blocker was that Rust GCS had no equivalent of the C++
periodic raylet `GetResourceLoad` pull loop in
`gcs_server.cc:415-446`, so live per-node load and usage never flowed
into either:

- `GcsResourceManager`
- `GcsAutoscalerStateManager`

That is now fixed.

The current Rust implementation has all of the blocker-level pieces:

- a real periodic load-pull loop
- the same config knob for cadence
- a real production fetcher that calls `NodeManagerService::GetResourceLoad`
- one pass updating both downstream consumers
- loop wiring through the shared initialization path used by both server
  entry points
- integration tests proving both update paths occur

Under the parity bar used for this blocker, I would now mark it closed.

---

## What I Verified

### 1. Rust now has the periodic raylet load-pull loop

`GcsServer` now starts the loop from the shared initialization path:

- [gcs-server/src/lib.rs](/Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/crates/gcs-server/src/lib.rs:858)

and the implementation is factored into:

- [gcs-managers/src/raylet_load.rs](/Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/crates/gcs-managers/src/raylet_load.rs:1)

The loop cadence is driven by:

- `gcs_pull_resource_loads_period_milliseconds`

which is the same knob C++ uses.

This directly addresses the earlier gap where Rust had an
`update_resource_load_and_usage()` method on the autoscaler but no
callers and no periodic polling path at all.

### 2. The production fetch path is the right RPC

`TonicRayletLoadFetcher` calls:

- `NodeManagerServiceClient::get_resource_load`

at:

- [raylet_load.rs](/Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/crates/gcs-managers/src/raylet_load.rs:60)

That is the correct RPC and matches the C++ logic in:

- `ray/src/ray/gcs/gcs_server.cc:424-437`

So this is not a fake or partial implementation; the production path is
pointing at the right raylet API.

### 3. One reply updates both downstream consumers, matching C++

Inside `pull_once`, every successful reply now does:

- `resource_manager.update_resource_loads(&data)`
- `autoscaler.update_resource_load_and_usage(data)`

at:

- [raylet_load.rs](/Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/crates/gcs-managers/src/raylet_load.rs:116)

That is the same two-consumer shape as C++:

- `gcs_resource_manager_->UpdateResourceLoads(...)`
- `gcs_autoscaler_state_manager_->UpdateResourceLoadAndUsage(...)`

in:

- `ray/src/ray/gcs/gcs_server.cc:435-437`

This is the core of the blocker. The loop is not just polling; it is
feeding the same two control-plane views that C++ updates.

### 4. `GcsResourceManager::update_resource_loads` matches the C++ helper shape

Rust now has:

- [resource_manager.rs](/Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/crates/gcs-managers/src/resource_manager.rs:51)

This method only copies:

- `resource_load`
- `resource_load_by_shape`

into the stored row, leaving the rest untouched.

That is the right behavior for parity with C++ `UpdateResourceLoads`.
The fuller row replacement still belongs to the autoscaler path, just as
it does in C++.

### 5. Both `start()` and `start_with_listener()` are covered

The loop starts from `install_listeners_and_initialize()`, which both
serve entry points use.

That matters because several earlier blockers in this codebase came from
production startup paths silently skipping behavior that existed only in
tests or alternate entry points. This implementation avoids that drift
shape.

---

## Tests Run

I ran the two blocker-level integration tests:

1. `cargo test --manifest-path /Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/Cargo.toml -p gcs-server --lib raylet_load_pull_loop_updates_resource_manager_and_autoscaler`
   - passed

2. `cargo test --manifest-path /Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/Cargo.toml -p gcs-server --lib raylet_load_pull_loop_period_zero_disables`
   - passed

The first one is the decisive test. It proves that:

- the server can be constructed
- the periodic loop runs
- the fetcher is actually called
- `GetAllResourceUsage` shows updated `resource_load`
- `resource_load_by_shape` becomes populated
- the autoscaler cache reflects updated `resources_available`

That is exactly the missing behavior the blocker was about.

---

## Remaining Differences

I do not see a blocker-level reason to keep Blocker 5 open.

There are still some narrower implementation differences from the full
C++ system:

- Rust currently opens a tonic channel per fetch instead of using a
  pooled raylet client object like C++.
- The warning sampling implementation uses a simple process-wide counter
  instead of the exact C++ logging macro machinery.
- There is a compiler warning for:
  - `#[cfg(any(test, feature = "test-support"))]`
  because `test-support` is not declared as a Cargo feature.

Those are real cleanup/follow-up items, but they are not reasons to say
the blocker remains open. The missing control-plane behavior is now
present.

---

## Conclusion

The original Blocker 5 condition is resolved:

- Rust now polls live raylet resource load periodically
- the result updates both the resource manager and autoscaler state
- the behavior is wired into the real server initialization path
- the end-to-end integration tests for both enabled and disabled modes
  pass

I would mark Blocker 5 as closed.
