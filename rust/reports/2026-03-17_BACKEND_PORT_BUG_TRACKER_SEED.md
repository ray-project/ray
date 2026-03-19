# Backend Port Bug Tracker Seed List

This file is a seed bug list derived from the backend audit. It is intended to
be copied into an issue tracker. The finding IDs refer to the full audit:

- [`2026-03-17_BACKEND_PORT_REVIEW.md`](/Users/istoica/src/ray/rust/reports/2026-03-17_BACKEND_PORT_REVIEW.md)

## Suggested Fields

- `bug_id`
- `finding_id`
- `area`
- `severity`
- `title`
- `likely_user_impact`
- `suggested_owner`
- `test_to_add`

## Seed List

| bug_id | finding_id | area | severity | title | likely_user_impact | suggested_owner | test_to_add |
|---|---|---|---|---|---|---|---|
| BP-001 | GCS-1 | GCS | High | Internal KV uses in-memory backend instead of configured persistent store | KV state lost on restart; HA semantics diverge | `ray-gcs` | GCS restart persistence test |
| BP-002 | GCS-2 | GCS | High | `ReportJobError` is stubbed/reduced | missing job error propagation to Python/dashboard | `ray-gcs` | driver/job error propagation test |
| BP-003 | GCS-3 | GCS | High | Job cleanup on node death is a no-op | leaked/running job state after node failure | `ray-gcs` | node-death job cleanup test |
| BP-004 | GCS-4 | GCS | Medium-High | `GetAllJobInfo` omits C++ metadata enrichment | Python/admin tooling sees different job state | `ray-gcs` | `GetAllJobInfo` parity test |
| BP-005 | GCS-5 | GCS | Medium-High | `UnregisterNode` drops `node_death_info` semantics | weaker node-death reporting | `ray-gcs` | unregister node metadata test |
| BP-006 | GCS-6 | GCS | High | `DrainNode` does not call into raylet shutdown flow | autoscaler drain behavior diverges | `ray-gcs` + `ray-raylet` | drain-node end-to-end test |
| BP-007 | GCS-7 | GCS | Medium | `GetAllNodeAddressAndLiveness` ignores filters | incorrect filtered node views | `ray-gcs` | node filter parity test |
| BP-008 | GCS-8 | GCS | High | Pubsub long-poll/unsubscribe semantics diverge | missed/duplicate control-plane notifications | `ray-gcs` | pubsub conformance test |
| BP-009 | GCS-9 | GCS | High | Actor lineage reconstruction RPC path is stubbed | actor recovery diverges | `ray-gcs` | actor lineage reconstruction test |
| BP-010 | GCS-10 | GCS | High | `ReportActorOutOfScope` is stubbed | actor lifecycle/ref cleanup diverges | `ray-gcs` + `ray-core-worker` | actor out-of-scope test |
| BP-011 | GCS-11 | GCS | High | Worker info/update RPCs are stubbed or reduced | worker state in GCS diverges | `ray-gcs` | worker info/update parity test |
| BP-012 | GCS-12 | GCS | High | Placement group jumps directly to `Created` | missing scheduling/transition semantics | `ray-gcs` | placement-group lifecycle test |
| BP-013 | GCS-13 | GCS | High | Placement group removal deletes state instead of persisting `REMOVED` | tooling/recovery sees wrong PG state | `ray-gcs` | placement-group removal persistence test |
| BP-014 | GCS-14 | GCS | Medium-High | `WaitPlacementGroupUntilReady` reduced to sync check | wrong wait semantics/timeouts | `ray-gcs` | `wait_until_ready` parity test |
| BP-015 | GCS-15 | GCS | High | Resource manager behavior is much weaker than C++ | scheduler/autoscaler decisions diverge | `ray-gcs` | resource reporting/state test |
| BP-016 | GCS-16 | GCS | High | Autoscaler state handling drops stale-version/infeasible logic | autoscaler can accept stale/incorrect state | `ray-gcs` | autoscaler stale-version test |
| BP-017 | GCS-17 | GCS | Medium-High | `GetClusterStatus` is much less complete | admin/autoscaler tooling sees different cluster view | `ray-gcs` | cluster status parity test |
| BP-018 | GCS-18 | GCS | High | Autoscaler `DrainNode` always accepts | unsafe drain acknowledgments | `ray-gcs` + `ray-raylet` | autoscaler drain acceptance test |
| BP-019 | RAYLET-1 | Raylet | High | `ReturnWorkerLease` does not release worker/resources correctly | worker/resource leakage | `ray-raylet` | worker lease return test |
| BP-020 | RAYLET-2 | Raylet | High | `RequestWorkerLease` lifecycle diverges | scheduling/retry semantics differ | `ray-raylet` | worker lease request parity test |
| BP-021 | RAYLET-3 | Raylet | Medium | backlog reporting is not integrated | scheduler sees wrong backlog | `ray-raylet` | backlog reporting test |
| BP-022 | RAYLET-4 | Raylet | Medium-High | object pinning path is stubbed | incorrect object lifetime | `ray-raylet` | object pinning test |
| BP-023 | RAYLET-5 | Raylet | Medium | resource resizing is stubbed | runtime resource control differs | `ray-raylet` | resize-local-resource test |
| BP-024 | RAYLET-6 | Raylet | Medium | `GetNodeStats` is much weaker | tooling/debugging sees wrong stats | `ray-raylet` | node-stats parity test |
| BP-025 | RAYLET-7 | Raylet | High | `DrainRaylet` always accepts | unsafe node drain behavior | `ray-raylet` | drain-raylet rejection/acceptance test |
| BP-026 | RAYLET-8 | Raylet | High | `ShutdownRaylet` semantics diverge | shutdown ordering/cleanup differs | `ray-raylet` | shutdown-raylet test |
| BP-027 | RAYLET-9 | Raylet | High | cleanup RPCs are no-ops | leaked worker/task/actor state | `ray-raylet` | cleanup RPC parity test |
| BP-028 | RAYLET-10 | Raylet | High | dependency manager omits pull/owner/request-type semantics | fetch and lease behavior diverges | `ray-raylet` | dependency manager conformance test |
| BP-029 | RAYLET-11 | Raylet | High | `WaitManager` semantics diverge | `ray.wait` user-visible behavior differs | `ray-raylet` | `ray.wait` parity test |
| BP-030 | RAYLET-12 | Raylet | High | worker-pool lifecycle/matching is weaker | worker reuse/env behavior diverges | `ray-raylet` | worker-pool lifecycle test |
| BP-031 | RAYLET-13 | Raylet | High | local object management misses spill/owner-subscription behavior | local object lifetime diverges | `ray-raylet` + `ray-object-manager` | local-object/spill integration test |
| BP-032 | CORE-1 | Core Worker | High | Core worker remains local-memory-store-centric | distributed behavior bypassed | `ray-core-worker` | remote object/task integration test |
| BP-033 | CORE-2 | Core Worker | Critical | reference counting semantics are weaker than C++ | object leaks / premature cleanup / wrong borrowing behavior | `ray-core-worker` | borrower/owner refcount parity test |
| BP-034 | CORE-3 | Core Worker | High | normal-task submission is reduced and partially stubbed | task submission semantics diverge | `ray-core-worker` | normal-task submission conformance test |
| BP-035 | CORE-4 | Core Worker | High | actor-task submission drops restart/dependency/cancel semantics | actor task behavior diverges | `ray-core-worker` | actor task restart/cancel test |
| BP-036 | CORE-5 | Core Worker | High | task execution protocol differs from C++ task receiver | actor ordering/cancellation semantics diverge | `ray-core-worker` | task receiver ordering test |
| BP-037 | CORE-6 | Core Worker | High | task-management and RPC handlers are reduced/stubbed | CoreWorker API mismatch | `ray-core-worker` | RPC handler parity test |
| BP-038 | CORE-7 | Core Worker | High | dependency resolver is only local waiter | dependency behavior diverges | `ray-core-worker` | dependency resolver test |
| BP-039 | CORE-8 | Core Worker | High | future resolution is not full owner-RPC resolver | future/object readiness diverges | `ray-core-worker` | future resolution parity test |
| BP-040 | CORE-9 | Core Worker | High | object recovery omits lineage/pinning logic | object reconstruction differs | `ray-core-worker` | object recovery test |
| BP-041 | CORE-10 | Core Worker | Critical | ownership tracking is local instead of distributed | owner/borrower protocol broken | `ray-core-worker` | ownership directory end-to-end test |
| BP-042 | CORE-11 | Core Worker | High | direct actor transport is simplified queue | actor fault/restart/backpressure semantics diverge | `ray-core-worker` | actor transport fault-injection test |
| BP-043 | CORE-12 | Core Worker | High | actor manager is mostly registry, not lifecycle manager | named actor/state notification behavior differs | `ray-core-worker` | named actor lifecycle test |
| BP-044 | OBJECT-1 | Object Manager | High | object directory is local cache, not owner-driven directory | distributed object locations diverge | `ray-object-manager` | ownership-directory conformance test |
| BP-045 | OBJECT-2 | Object Manager | High | pull manager drops activation/pinning/failure semantics | fetch priority/failure behavior diverges | `ray-object-manager` | pull-manager parity test |
| BP-046 | OBJECT-3 | Object Manager | High | push/object-manager flow omits deferred push/resend/spill serving | fetch-serving behavior diverges | `ray-object-manager` | deferred push/resend test |
| BP-047 | OBJECT-4 | Object Manager | Critical | inbound chunk handling does not preserve write/validate/seal semantics | corrupted or phantom objects possible | `ray-object-manager` | chunk validation/readback test |
| BP-048 | OBJECT-5 | Object Manager | High | spill support is standalone, not integrated | spilled-object fetch path diverges | `ray-object-manager` | spill/restore end-to-end test |
| BP-049 | PY-1 | Python boundary | High | `_raylet` surface is far narrower than Cython contract | Python internals break | `ray-core-worker-pylib` | `_raylet` symbol parity test |
| BP-050 | PY-2 | Python boundary | High | `PyCoreWorker` constructor/lifecycle not equivalent | Python worker initialization diverges | `ray-core-worker-pylib` | `CoreWorker` init/lifecycle parity test |
| BP-051 | PY-3 | Python boundary | High | `ObjectRef` semantics are weaker | ref lifecycle/async ownership diverges | `ray-core-worker-pylib` | `ObjectRef` lifecycle/async test |
| BP-052 | PY-4 | Python boundary | Medium-High | `PyCoreWorker.get` and error behavior differ | Python user-visible get/error behavior diverges | `ray-core-worker-pylib` | `get` return/error parity test |
| BP-053 | PY-5 | Python boundary | Medium-High | `PyGcsClient` is reduced and stubbed | Python GCS client behavior diverges | `ray-core-worker-pylib` | GCS client parity test |
| BP-054 | PY-6 | Python/RDT | Medium | RDT remains hybrid Python/Rust subsystem | behavior depends on fallback/import environment | `ray-core-worker-pylib` + `ray/experimental/rdt` | two-mode RDT parity test |
