## Blocker 7 Recheck: Health-check semantic parity with C++ GCS

Date: 2026-04-20
Reviewer: Codex
Status: Closed

### Findings

#### No blocker-level parity findings remain

The original Blocker 7 was that Rust used tonic's default cached health
reporter, which could continue returning `SERVING` even if the GCS-side
event loop was hung. That was a real semantic mismatch with C++, which
registers a custom health service specifically so a stuck event loop
causes health checks to stall / fail.

That parity gap is now closed.

#### Non-blocking issue: test-only cfg uses an undeclared feature name

File:
- `ray/src/ray/rust/gcs/crates/gcs-managers/src/health_service.rs`

During test compilation, Rust emits `unexpected cfg condition value:
test-support` warnings for several `#[cfg(any(test, feature =
"test-support"))]` sites in the new health service module.

This does **not** change runtime behavior and does **not** reopen the
blocker, but it is worth cleaning up because it adds noise to test
builds and can hide more meaningful warnings.

Recommended cleanup:

- either declare a real `test-support` feature in
  `crates/gcs-managers/Cargo.toml`
- or, if the extra feature is not intended to exist, simplify those
  guards to `#[cfg(test)]`

### What I verified

#### 1. Rust no longer uses tonic's static health reporter

The server now wires a custom `grpc.health.v1.Health` implementation
instead of `tonic_health::server::health_reporter()`.

Relevant code:

- `ray/src/ray/rust/gcs/crates/gcs-server/src/lib.rs:196-205`
- `ray/src/ray/rust/gcs/crates/gcs-server/src/lib.rs:377-384`
- `ray/src/ray/rust/gcs/crates/gcs-server/src/lib.rs:945-956`

What changed:

- `GcsServer` now owns `health_service: Arc<GcsHealthService>`
- startup spawns a `HealthProbe`
- `build_router()` registers `HealthServer::from_arc(self.health_service.clone())`

That removes the old behavior where only tonic's cached serving bit was
consulted.

#### 2. `Check` now round-trips through a GCS-side task

Relevant code:

- `ray/src/ray/rust/gcs/crates/gcs-managers/src/health_service.rs:82-156`
- `ray/src/ray/rust/gcs/crates/gcs-managers/src/health_service.rs:189-212`

What changed:

- `HealthProbe::spawn()` creates an mpsc + oneshot round-trip task
- `GcsHealthService::check()` calls `probe.ping(timeout)`
- success maps to `SERVING`
- timeout / dropped task maps to `NOT_SERVING`

This is the important parity point. The response is no longer a static
cached value; it depends on whether a GCS-side async task can actually
run and answer in time.

That is meaningfully aligned with the C++ contract described in:

- `ray/src/ray/gcs/gcs_server.cc:301-305`
- `ray/src/ray/gcs/grpc_services.h:341-357`

#### 3. `Watch` now matches C++ by returning `UNIMPLEMENTED`

Relevant code:

- `ray/src/ray/rust/gcs/crates/gcs-managers/src/health_service.rs:214-225`
- `ray/src/ray/gcs/grpc_services.cc:216-244`

The C++ health service only registers `Check`. It does not register
`Watch`.

Rust now matches that by returning `Status::unimplemented(...)` from
`watch(...)`, instead of exposing tonic's default watch behavior.

#### 4. The `service` field handling matches C++

Relevant code:

- Rust `check(...)`: `ray/src/ray/rust/gcs/crates/gcs-managers/src/health_service.rs:191-194`
- C++ note: `ray/src/ray/gcs/grpc_services.h:345-347`

The request parameter is intentionally ignored in both implementations.
That is the correct parity choice for the current C++ behavior.

### Test evidence

I reran the targeted tests for this blocker and all passed.

#### Unit tests in `gcs-managers`

Passed:

- `cargo test --manifest-path /Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/Cargo.toml -p gcs-managers --lib healthy_probe_reports_serving`
- `cargo test --manifest-path /Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/Cargo.toml -p gcs-managers --lib dropped_probe_reports_not_serving`
- `cargo test --manifest-path /Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/Cargo.toml -p gcs-managers --lib stuck_probe_reports_not_serving_within_timeout`
- `cargo test --manifest-path /Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/Cargo.toml -p gcs-managers --lib watch_returns_unimplemented`

Coverage:

- healthy probe returns `SERVING`
- aborted probe returns `NOT_SERVING`
- wedged probe times out and returns `NOT_SERVING`
- `Watch` returns `UNIMPLEMENTED`

Relevant test locations:

- `ray/src/ray/rust/gcs/crates/gcs-managers/src/health_service.rs:269-354`

#### End-to-end tests in `gcs-server`

Passed:

- `cargo test --manifest-path /Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/Cargo.toml -p gcs-server --lib health_check_service_reports_serving_and_not_serving_after_probe_aborts`
- `cargo test --manifest-path /Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/Cargo.toml -p gcs-server --lib health_watch_returns_unimplemented`

Coverage:

- real server starts
- `grpc.health.v1.Health/Check` returns `SERVING`
- after aborting the probe task, the same endpoint returns `NOT_SERVING`
- `Watch` returns `UNIMPLEMENTED` end to end

Relevant test locations:

- `ray/src/ray/rust/gcs/crates/gcs-server/src/lib.rs:2767-2827`
- `ray/src/ray/rust/gcs/crates/gcs-server/src/lib.rs:2832-2855`

### Final assessment

Blocker 7 is fixed.

The Rust GCS health-check surface now matches the C++ behavior on the
important contract points:

- health is no longer a static cached `SERVING` bit
- health depends on responsiveness of a GCS-side task
- `Watch` is not implemented
- the request `service` field is ignored

I do not see a remaining functional parity gap for this blocker.
