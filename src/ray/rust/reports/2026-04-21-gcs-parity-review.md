# Rust GCS vs C++ GCS Parity Review

Date: 2026-04-21

Scope: Fresh review of the current Rust GCS implementation under `ray/src/ray/rust/gcs` against the C++ GCS implementation under `ray/src/ray/gcs`. The goal is drop-in replacement parity: public RPC semantics, manager lifecycle side effects, persisted state behavior, background cleanup, and cross-manager wiring must match C++ unless an intentional difference is documented and proven safe.

## Verdict

Rust GCS is not yet a drop-in replacement for C++ GCS.

Several previously reported gaps have been closed, including function-manager reference tracking, RaySyncer service registration, health-check semantics, autoscaler drain forwarding, usage-stats wiring, metrics exporter initialization, and task-event query behavior. However, this fresh pass found current parity blockers in runtime-env URI lifecycle management and actor lifecycle semantics.

The highest-risk issue is that Rust only tracks runtime-env URI references for `PinRuntimeEnvURI`, while C++ also pins runtime-env URIs for jobs and detached actors. This can cause Rust GCS to garbage-collect `gcs://` runtime-env packages while a job or detached actor still needs them.

## Findings

### Blocker 1: Job and detached-actor runtime-env URI references are not wired in Rust

Severity: High

Rust only implements runtime-env URI reference counting inside the `PinRuntimeEnvURI` RPC handler. The C++ GCS also adds and removes runtime-env URI references from job and detached-actor lifecycles. Those lifecycle references are required to prevent runtime-env package cleanup while the owning job or detached actor is still alive.

C++ behavior:

- `RuntimeEnvManager::AddURIReference(hex_id, RuntimeEnvInfo)` extracts both `working_dir_uri` and `py_modules_uris` and increments references for each URI (`ray/src/ray/common/runtime_env_manager.cc:28-43`).
- `RuntimeEnvManager::RemoveURIReference(hex_id)` decrements all URIs for that id, deletes the reference id, and invokes the deleter when the URI refcount reaches zero (`ray/src/ray/common/runtime_env_manager.cc:53-73`).
- Job registration pins job runtime-env URIs when `job_table_data.config().has_runtime_env_info()` is true (`ray/src/ray/gcs/gcs_job_manager.cc:132-134`).
- Job finish removes that job runtime-env reference (`ray/src/ray/gcs/gcs_job_manager.cc:170-171`).
- Detached actor registration pins the actor task runtime-env URIs under `actor_id.Hex()` (`ray/src/ray/gcs/actor/gcs_actor_manager.cc:738-745`).
- Final non-restartable detached actor destruction removes that actor runtime-env reference (`ray/src/ray/gcs/actor/gcs_actor_manager.cc:1117-1120`).

Rust behavior:

- `RuntimeEnvService` has private `add_uri_reference` and `remove_uri_reference` helpers, but the only production caller is `pin_runtime_env_uri` (`ray/src/ray/rust/gcs/crates/gcs-managers/src/runtime_env_stub.rs:86-125`, `:162-250`).
- `GcsJobManager` only stores table/publisher/export/function-manager fields; it has no runtime-env manager/service field or setter (`ray/src/ray/rust/gcs/crates/gcs-managers/src/job_manager.rs:34-43`).
- Rust `add_job` persists/publishes/exports and bumps the function manager, but does not inspect `data.config.runtime_env_info` or pin URI references (`ray/src/ray/rust/gcs/crates/gcs-managers/src/job_manager.rs:166-196`).
- Rust `mark_job_finished` persists/publishes/exports and removes the function-manager reference, but does not remove runtime-env URI references (`ray/src/ray/rust/gcs/crates/gcs-managers/src/job_manager.rs:203-236`).
- Rust `register_actor` persists the task spec and actor, inserts the actor, and bumps the function manager, but does not pin detached actor runtime-env URIs (`ray/src/ray/rust/gcs/crates/gcs-managers/src/actor_stub.rs:663-740`).
- Rust `destroy_actor` removes task spec/name/function-manager state for final non-restartable actors, but does not remove detached actor runtime-env URI references (`ray/src/ray/rust/gcs/crates/gcs-managers/src/actor_stub.rs:579-652`).
- Server construction creates `runtime_env_service` after constructing the job/actor managers and does not pass or install it into either manager (`ray/src/ray/rust/gcs/crates/gcs-server/src/lib.rs:448-450`).

Impact:

- A job with a `gcs://` working dir or py module can finish its temporary `PinRuntimeEnvURI` TTL and have its package deleted from internal KV even while the job is still alive.
- A detached actor can outlive its creating job, but Rust does not hold the actor's runtime-env URI references. C++ explicitly pins detached actor runtime envs for this case.
- Runtime-env cleanup semantics are materially different from C++; this is not safe for a drop-in replacement.

What must be done:

- Expose runtime-env reference operations that match C++ `RuntimeEnvManager::AddURIReference(id, RuntimeEnvInfo)` and `RemoveURIReference(id)`.
- Wire a shared runtime-env manager/service into `GcsJobManager` and `GcsActorManager`, analogous to the existing function-manager wiring.
- On successful job add, if `JobConfig.runtime_env_info` is present, add URI references using `job_id.Hex()` as the reference id.
- On job finish, remove URI references for `job_id.Hex()` after the same storage/publish point where C++ removes them.
- On detached actor registration, add URI references using `actor_id.Hex()` as the reference id and `task_spec.runtime_env_info`.
- On final non-restartable detached actor destruction, remove URI references for `actor_id.Hex()`.
- Add tests that seed `gcs://` runtime-env KV entries and verify that job/detached-actor references prevent deletion until their C++-matching lifecycle release points.

### Blocker 2: `ReportActorOutOfScope` stale-counter logic is reversed in Rust

Severity: High

Rust currently treats a report as stale when the request counter is greater than the actor's current lineage-reconstruction counter. C++ treats it as stale when the actor's current counter is greater than the request counter. These are opposites.

C++ behavior:

- In `HandleReportActorOutOfScope`, C++ checks `actor.current_num_restarts_due_to_lineage_reconstruction > request.num_restarts_due_to_lineage_reconstruction()` and ignores the report only in that case (`ray/src/ray/gcs/actor/gcs_actor_manager.cc:283-290`).
- This means an old out-of-scope report from before a lineage restart must not kill the restarted actor.

Rust behavior:

- Rust checks `request.num_restarts_due_to_lineage_reconstruction > entry.num_restarts_due_to_lineage_reconstruction` and ignores that case (`ray/src/ray/rust/gcs/crates/gcs-managers/src/actor_stub.rs:1114-1127`).
- The Rust unit test encodes the reversed interpretation: it calls `counter=2` stale when the actor current counter is `1` (`ray/src/ray/rust/gcs/crates/gcs-managers/src/actor_stub.rs:2093-2103`).

Impact:

- A genuinely stale out-of-scope report with request counter `0` can destroy an actor that C++ would keep alive after it has been lineage-restarted to current counter `1`.
- Conversely, a future/higher request counter is ignored by Rust even though C++ would not classify it as stale by this condition.
- This can incorrectly kill live actors or suppress actor cleanup depending on message ordering.

What must be done:

- Change the Rust stale check to `if entry.num_restarts_due_to_lineage_reconstruction > inner.num_restarts_due_to_lineage_reconstruction { ignore }`.
- Fix the test to match C++ semantics: after restarting an actor to current counter `1`, send an old report with request counter `0` and assert the actor remains alive.
- Add a complementary test where request counter equals current counter and confirm Rust proceeds to destroy/restart according to the normal out-of-scope path.

### Blocker 3: `KillActorViaGcs` returns OK for unknown actors instead of NotFound

Severity: Medium

C++ and Rust disagree on the public RPC status for killing an unknown actor.

C++ behavior:

- If `registered_actors_` does not contain the actor id, `HandleKillActorViaGcs` replies with `Status::NotFound("Could not find actor with ID ...")` (`ray/src/ray/gcs/actor/gcs_actor_manager.cc:636-658`).

Rust behavior:

- If `self.actors.get(&actor_id)` returns `None`, Rust replies with OK (`ray/src/ray/rust/gcs/crates/gcs-managers/src/actor_stub.rs:1053-1059`).
- The Rust comment says this matches C++, but the C++ source does not match that comment.

Impact:

- Callers relying on C++ NotFound behavior cannot distinguish a successful kill from a request for a nonexistent actor.
- This is a visible API compatibility difference and violates drop-in replacement semantics.

What must be done:

- Return `not_found_status(format!("Could not find actor with ID ..."))` for unknown actors.
- Keep the already-dead behavior separate. C++ only checks `registered_actors_`, not a destroyed-cache lookup, so the exact treatment of destroyed-cache actors should be verified and matched.
- Add a unit test for unknown-actor kill returning NotFound.

### Blocker 4: `RestartActorForLineageReconstruction` returns OK for permanently-dead or invalid actors where C++ returns Invalid or CHECKs

Severity: Medium

C++ and Rust disagree on error behavior for invalid lineage-restart requests.

C++ behavior:

- If the actor is not in `registered_actors_`, C++ replies `Status::Invalid("Actor is permanently dead.")` (`ray/src/ray/gcs/actor/gcs_actor_manager.cc:342-346`).
- If the request is stale (`request_counter <= current_counter`), C++ returns OK through the success callback (`ray/src/ray/gcs/actor/gcs_actor_manager.cc:365-378`).
- For a non-stale restart, C++ checks that the requested counter is exactly current + 1, that the actor state is `DEAD`, and that the actor is restartable (`ray/src/ray/gcs/actor/gcs_actor_manager.cc:380-390`).

Rust behavior:

- If the actor is not in `self.actors`, Rust replies OK (`ray/src/ray/rust/gcs/crates/gcs-managers/src/actor_stub.rs:1166-1170`).
- If the actor is present but not `DEAD`, Rust replies OK rather than treating this as a fatal invariant violation or error (`ray/src/ray/rust/gcs/crates/gcs-managers/src/actor_stub.rs:1189-1198`).
- Rust does not enforce the C++ exact `request_counter == current + 1` invariant before restart; it accepts any request counter greater than current and assigns it directly (`ray/src/ray/rust/gcs/crates/gcs-managers/src/actor_stub.rs:1212-1217`).

Impact:

- Permanently dead actors are reported as successful restart requests instead of Invalid.
- Bad counter jumps can be accepted and persisted in Rust, while C++ rejects them through `RAY_CHECK_EQ`.
- Invalid actor state transitions may be silently ignored, hiding protocol bugs that C++ would surface.

What must be done:

- Return `Invalid("Actor is permanently dead.")` when the actor is absent from the active actor map.
- Preserve OK for the exact C++ stale retry case (`request_counter <= current_counter`).
- Enforce `request_counter == current_counter + 1` for non-stale restart requests.
- Match C++ behavior for non-DEAD or non-restartable actors. If Rust should not abort like `RAY_CHECK`, it still must return a non-OK status and avoid silently reporting success.
- Add tests for absent actor, stale retry, counter jump, non-DEAD actor, non-restartable actor, and valid current+1 restart.

## Positive Parity Observations

The following areas looked materially improved relative to prior reviews:

- Function-manager parity is now present: Rust constructs a shared function manager, wires it into job and actor managers, and deletes `RemoteFunction`, `ActorClass`, and `FunctionsToRun` prefixes in namespace `fun` only after the last job/actor reference retires.
- Server router registration now includes the major GCS services, including internal KV, actor, placement group, pubsub, runtime env, autoscaler state, event export, health, and RaySyncer.
- The custom health service now checks via an event-loop task and returns unimplemented for Watch, matching C++ intent.
- Raylet-load pull, autoscaler drain forwarding, usage stats, metrics exporter initialization, and task-event filtering/GC all have visible Rust implementations and test coverage.

These positive observations do not close the blockers above.

## Verification Run

I ran the Rust manager unit test suite:

```text
cargo test --manifest-path /Users/istoica/src/cc-to-rust/ray/src/ray/rust/gcs/Cargo.toml -p gcs-managers --lib
result: ok. 247 passed; 0 failed; 0 ignored
```

The run emitted existing warnings for unused imports, unexpected `cfg(feature = "test-support")`, unused variables/fields, and naming style. More importantly, the passing suite does not catch the blockers above. In particular, `actor_stub::tests::test_stale_out_of_scope_report` currently asserts the reversed stale-counter semantics rather than C++ semantics.

## Recommended Closure Criteria

Rust GCS should not be considered a drop-in C++ GCS replacement until all of the following are true:

- Runtime-env URI references are wired through job and detached-actor lifecycle paths and are tested against C++ cleanup timing.
- `ReportActorOutOfScope` stale-counter semantics match C++ and the current inverted test is corrected.
- `KillActorViaGcs` unknown-actor status matches C++ NotFound behavior.
- `RestartActorForLineageReconstruction` absent/invalid/counter-jump behavior matches C++ status and invariants.
- New tests are written as parity tests against the C++ source behavior, not merely against the current Rust behavior.

