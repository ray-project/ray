# Blocker 4 Recheck: GCS Function Manager Parity

Date: 2026-04-21

Reviewed blocker: Blocker 4 from `2026-04-20-gcs-parity-review-recheck.md`:
Rust GCS was missing the C++ `GCSFunctionManager` behavior that tracks job-scoped references from jobs and actors and deletes exported function metadata from internal KV only after the last reference retires.

## Verdict

Blocker 4 is closed.

I did not find a remaining parity blocker in the new Rust implementation. The Rust GCS now has a shared function manager, wires it into both the job and actor managers, mirrors the C++ reference-counting semantics, and deletes the same three job-scoped KV prefix families from the same namespace.

## C++ Behavior That Must Be Matched

The C++ function manager behavior is concentrated in `ray/src/ray/gcs/gcs_function_manager.h`:

- `AddJobReference` increments `job_counter_[job_id]` (`gcs_function_manager.h:44`).
- `RemoveJobReference` is idempotent for an absent job id and returns without error, explicitly to tolerate duplicate network-retry calls (`gcs_function_manager.h:46-50`).
- `RemoveJobReference` decrements the count, erases the map entry when it reaches zero, and then calls `RemoveExportedFunctions` (`gcs_function_manager.h:52-57`).
- `RemoveExportedFunctions` prefix-deletes from namespace `fun`:
  - `RemoteFunction:<job_hex>:`
  - `ActorClass:<job_hex>:`
  - `<kWorkerSetupHookKeyName>:<job_hex>:` where `kWorkerSetupHookKeyName` is `FunctionsToRun`
  - Source: `gcs_function_manager.h:61-69`.

The C++ manager is shared through the server and both lifecycle managers:

- `GcsServer::InitFunctionManager` constructs one `GCSFunctionManager` over the internal KV manager (`gcs_server.cc:624-627`).
- `GcsJobManager::Initialize` adds one reference for every recovered job (`gcs_job_manager.cc:34-39`).
- successful job registration adds one job reference (`gcs_job_manager.cc:132-136`).
- job finish removes one job reference (`gcs_job_manager.cc:154-176`).
- actor registration adds one reference for the actor's job (`gcs_actor_manager.cc:729-731`).
- non-restartable actor destruction removes one reference (`gcs_actor_manager.cc:1110-1114`).
- actor recovery adds one reference for every recovered actor that is loaded (`gcs_actor_manager.cc:1722-1730`).

## Rust Implementation Review

The Rust implementation now has `GcsFunctionManager` in `ray/src/ray/rust/gcs/crates/gcs-managers/src/function_manager.rs`.

Matched behavior:

- The Rust manager stores `counter: Mutex<HashMap<Vec<u8>, usize>>`, equivalent to the C++ `job_counter_` keyed by `JobID` (`function_manager.rs:52-64`).
- `add_job_reference` increments the count for the exact binary job id (`function_manager.rs:66-73`).
- `remove_job_reference` returns immediately when the job id is absent, matching C++ duplicate-call idempotency (`function_manager.rs:75-90`).
- `remove_job_reference` decrements, removes the map entry on zero, and runs cleanup only then (`function_manager.rs:91-101`).
- Cleanup hex-encodes the binary job id and prefix-deletes the three required key families in namespace `fun` (`function_manager.rs:104-119`):
  - `RemoteFunction:<job_hex>:`
  - `ActorClass:<job_hex>:`
  - `FunctionsToRun:<job_hex>:`

The Rust implementation uses awaited KV deletes rather than C++'s callback-based `io_context` deletes. This is not a parity issue for the observable contract; it makes deletion completion ordered with the caller rather than weaker. The key observable behavior, reference-count threshold, namespace, prefixes, and idempotency all match.

## Rust Wiring Review

The shared instance is now wired the same way as C++:

- `GcsServer::new_with_store` constructs one `Arc<GcsFunctionManager>` from the same `kv_store` used by the rest of GCS (`gcs-server/src/lib.rs:411-421`).
- The server installs that shared instance into both the job manager and actor manager (`gcs-server/src/lib.rs:422-423`).

Job manager parity:

- recovery adds one function-manager reference for every recovered job (`job_manager.rs:138-155`), matching C++ `GcsJobManager::Initialize`.
- successful `add_job` writes storage, inserts/publishes/exports, then adds the job reference (`job_manager.rs:180-195`), matching the C++ success-callback placement after `JobTable().Put`.
- `mark_job_finished` updates storage, publishes/exports, then removes the job reference (`job_manager.rs:218-235`), matching C++ `MarkJobAsFinished`.

Actor manager parity:

- the actor manager owns an optional shared `GcsFunctionManager` reference installed by `set_function_manager` (`actor_stub.rs:151-158`, `actor_stub.rs:226-235`).
- recovery adds one reference per recovered actor's job id (`actor_stub.rs:402-426`), matching C++ actor initialization.
- actor registration adds one reference for the actor's job id after the actor is committed to tracking (`actor_stub.rs:723-734`), matching C++ `RegisterActor`.
- non-restartable actor destruction removes one reference (`actor_stub.rs:640-649`), matching C++ `DestroyActor`.
- the additional final non-restartable dead transition also removes one reference (`actor_stub.rs:1536-1545`). This is consistent with the intended invariant: every final retirement of a tracked non-restartable actor releases the actor-held function reference.

## Tests Run

All focused tests passed:

```text
cargo test --manifest-path /Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/Cargo.toml -p gcs-managers --lib add_and_remove_balance_plus_idempotent_remove
result: ok. 1 passed; 0 failed

cargo test --manifest-path /Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/Cargo.toml -p gcs-managers --lib removes_fun_namespace_entries_when_refcount_hits_zero
result: ok. 1 passed; 0 failed

cargo test --manifest-path /Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/Cargo.toml -p gcs-managers --lib partial_remove_keeps_kv_entries
result: ok. 1 passed; 0 failed

cargo test --manifest-path /Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/Cargo.toml -p gcs-server --lib function_manager_cleans_kv_on_last_reference_retirement
result: ok. 1 passed; 0 failed
```

The test runs emitted existing compiler warnings in unrelated modules, including unused imports and unexpected `cfg(feature = "test-support")` warnings. These warnings do not affect this blocker.

## Test Coverage Notes

The standalone function-manager tests cover:

- balanced add/remove behavior and idempotent duplicate removal;
- deletion of all three C++ KV prefix families when the refcount reaches zero;
- no deletion while the refcount remains above zero.

The server-level test `function_manager_cleans_kv_on_last_reference_retirement` covers the important end-to-end invariant:

- one job reference plus two actor references for the same job;
- KV entries under `RemoteFunction`, `ActorClass`, and `FunctionsToRun`;
- actor retirement drops two references without deleting metadata while the job reference remains;
- job finish drops the last reference and deletes only that job's three prefix families;
- a different job's key remains untouched.

One caveat: the server-level test seeds actor references directly through the function-manager accessor after inserting registered actor table data, rather than driving the full public actor registration RPC/task-spec path. I do not consider this a blocker because the real actor registration source path is explicitly present and source-verified at `actor_stub.rs:723-734`, and the test does exercise the matching actor destruction decrement path. A future coverage improvement would be an end-to-end actor registration test that reaches `register_actor` through the same task-spec setup used in production.

## Remaining Work

No remaining work is required to close Blocker 4.

Optional follow-up, not required for parity closure:

- Add a full public actor-registration integration test that enters through `register_actor` and then destroys the actor, so the actor add-reference path is covered dynamically rather than only source-reviewed.
- Clean up the unrelated Rust warnings so future parity test output is less noisy.

