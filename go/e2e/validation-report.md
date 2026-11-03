# Ray Go E2E fleet — first real-machine validation

This report records the first time the `go/e2e` driver fleet was executed against real clusters. It covers the two-phase run matrix, every non-pass with its A/B/C/D class and its raw evidence, the eight class A defects the run exposed, and what the round deliberately did not cover. Every row in the matrix below is backed by an archived per-driver log and exit record in the results directory named with it; a row with no artifact is not listed.

## 1. Scope and environment

The fleet is `go/e2e/run_fleet.sh`, a scripted runner that rebuilds `//go/e2e/...` and `//go:ray_go_pkg_zip`, stages each selected driver (plus the `userfuncs.so` plugin and any cross-language extra) into its own job directory, submits it, and compares the result against the criterion the driver's own source declares. The runner's `--dry-run` mode and its per-row criteria are documented in `go/e2e/README.md`; this report is the record of the runs themselves.

Phase 1 is the single-language fleet (nine drivers, thirteen rows: four of the nine also declare a `--run-mode=local` row). It ran against a local single-node cluster started with `ray start --head --num-cpus=2`, whose dashboard answered at `http://127.0.0.1:8265`. The client interpreter was the tree's Python 3.12 environment with `PYTHONPATH` pointing at this repository's `python/` and the tree's `go_runtime.so` pinned through `RAY_GO_RUNTIME_PATH`, as the README's from-zero section describes.

Phase 2 is the cross-language and dependency-bound set (four rows: `driver_call_python`, `driver_call_cpp`, `runtime_env_driver` as `ray job submit` rows, plus the standalone Python lane `test_python_call_go_simple`). It ran against a two-node Kuberay cluster — one head pod and one worker pod, both from an image built from this tree — with the head dashboard reached from the client machine through a `kubectl port-forward` to `http://127.0.0.1:18265`, which `--cluster-address` was given. The client ran under Python 3.11 with the tree on `PYTHONPATH`, because the image's Python is 3.11.10 and the tree's `_raylet.so` was rebuilt as a CPython 3.11 extension for that image.

The runs were made at the branch head of their phase's fixes. The Phase 1 run was made at the Phase 1 fixes commit (recorded in the branch history as `66a2efd1fe2`, its pre-rebase SHA in the Task 3 report being `b120ec86b56`). The Phase 2 runs and the Phase 1 regression re-runs were made at the Phase 2 fixes commit `041e45def7d`, which is the branch head at the time of writing. The Phase 1 run is dated 2026-10-20; the Phase 2 and regression runs are dated 2026-10-21.

Exact commands (run from the repository root, with the environment described in `go/e2e/README.md`):

```bash
# Phase 1, local single-node cluster (ray start --head --num-cpus=2)
bash go/e2e/run_fleet.sh --phase 1 --results-dir /tmp/fleet-final

# Phase 2, multi-node Kuberay cluster (head dashboard port-forwarded)
bash go/e2e/run_fleet.sh --phase 2 --cluster-address http://127.0.0.1:18265 --results-dir /tmp/fleet-p2-final

# Phase 1 re-run against the multi-node cluster (regression check, run twice)
bash go/e2e/run_fleet.sh --phase 1 --cluster-address http://127.0.0.1:18265 --results-dir /tmp/fleet-p1-regress
bash go/e2e/run_fleet.sh --phase 1 --cluster-address http://127.0.0.1:18265 --results-dir /tmp/fleet-p1-regress2

# Scaled re-runs: Phase 1 at deadline scale 3, and the one Phase 2 polling driver at scale 3
bash go/e2e/run_fleet.sh --phase 1 --timeout-scale 3 --cluster-address http://127.0.0.1:18265 --results-dir /tmp/fleet-p2-scaled-final
bash go/e2e/run_fleet.sh --phase 2 --timeout-scale 3 --driver driver_call_cpp --cluster-address http://127.0.0.1:18265 --results-dir /tmp/fleet-p2-scaled-cpp
```

## 2. Results matrix

The criterion tier is the pass criterion the runner applied to the row, from the three tiers `compare` implements (the exact regexes are in `go/e2e/README.md`): tier 1 is count equality (the number of the drivers' `=== Test… ===` convention lines in the log against the source's `func test[A-Z]…` declarations minus its `func testLocal…` declarations), tier 2 is the presence of a success marker the source itself declares (that convention line, `✓`, `✅`, or `passed`), and tier 3 is the exit code alone, used where the source declares no marker at all. "Observed vs expected" states what the row actually produced against what the tier required.

### 2.1 Phase 1 — local single-node cluster (`/tmp/fleet-final`, branch head `66a2efd1fe2`)

| Driver | Mode | rc | Criterion tier | Observed vs expected | Verdict |
|---|---|---|---|---|---|
| `driver_app` | driver | 0 | tier 2, marker `✓` | marker present, expected present | PASS |
| `driver_app` | `--run-mode=local` | 0 | tier 2, marker `✓` | marker present, expected present | PASS |
| `actor_integration_driver` | driver | 0 | tier 1, count | 12 lines, expected 12 | PASS |
| `driver_parallel_actor` | driver | 0 | tier 1, count | 2 lines, expected 2 | PASS |
| `runtime_context_driver` | driver | 0 | tier 1, count | 3 lines, expected 3 | PASS |
| `global_state_accessor_driver` | driver | 0 | tier 1, count | 3 lines, expected 3 | PASS |
| `driver_placement_group` | driver | 0 | tier 1, count | 3 lines, expected 3 | PASS |
| `driver_placement_group` | `--run-mode=local` | 0 | tier 2, convention marker | marker present, expected present | PASS |
| `driver_gcs_client` | driver | 0 | tier 1, count | 4 lines, expected 4 | PASS |
| `driver_gcs_client` | `--run-mode=local` | 0 | tier 2, convention marker | marker present, expected present | PASS |
| `driver_metrics` | driver | 0 | tier 1, count | 1 line, expected 1 | PASS |
| `driver_metrics` | `--run-mode=local` | 0 | tier 2, convention marker | marker present, expected present | PASS |
| `zero_copy_benchmark` | driver | 0 | tier 3, exit code only | exit 0, no marker declared | PASS |

Summary: 13/13 pass. Artifacts: one `<stem>.log` and one `<stem>.log.exit` per row in `/tmp/fleet-final`, plus `/tmp/fleet-final/summary.txt`.

### 2.2 Phase 2 — multi-node Kuberay cluster (`/tmp/fleet-p2-final`, branch head `041e45def7d`)

| Driver | Mode | rc | Criterion tier | Observed vs expected | Verdict |
|---|---|---|---|---|---|
| `driver_call_python` | driver | 0 | tier 2, marker `passed` | `All Go->Python cross-language tests passed!` present | PASS |
| `driver_call_cpp` | driver | 0 | tier 2, marker `passed` | `=== All Go->C++ cross-language tests passed! ===` present | PASS |
| `runtime_env_driver` | driver | 0 | tier 1, count | 4 lines, expected 4 | PASS |
| `test_python_call_go_simple` | direct (Python lane) | 1 | exit code | exit 1 at `ray.init`; class B (section 3.1) | FAIL |

Summary: 3/4. Artifacts in `/tmp/fleet-p2-final`: `driver_call_python.log`, `driver_call_cpp.log`, `runtime_env_driver.log`, and `test_python_call_go_simple__ray_address_http___127.0.0.1_18265.log`, each with a matching `.log.exit` (the direct lane's is 1), plus `summary.txt`.

### 2.3 Phase 1 regression — multi-node Kuberay cluster (`/tmp/fleet-p1-regress` and `/tmp/fleet-p1-regress2`, branch head `041e45def7d`)

This full Phase 1 re-run was executed twice against the multi-node cluster to check that the Phase 2 serializer change (section 4, A7) regressed nothing else in the Phase 1 set. Both runs produced identical verdicts; the artifacts below are present in both directories.

| Driver | Mode | rc | Criterion tier | Observed vs expected | Verdict |
|---|---|---|---|---|---|
| `driver_app` | driver | 0 | tier 2, marker `✓` | marker present | PASS |
| `driver_app` | `--run-mode=local` | 0 | tier 2, marker `✓` | marker present | PASS |
| `actor_integration_driver` | driver | 0 | tier 1, count | 12 lines, expected 12 | PASS |
| `driver_parallel_actor` | driver | 0 | tier 1, count | 2 lines, expected 2 | PASS |
| `runtime_context_driver` | driver | 0 | tier 1, count | 3 lines, expected 3 | PASS |
| `global_state_accessor_driver` | driver | 0 | tier 1, count | 3 lines, expected 3 | PASS |
| `driver_placement_group` | driver | 0 | tier 1, count | 3 lines, expected 3 | PASS |
| `driver_placement_group` | `--run-mode=local` | 0 | tier 2, convention marker | marker present | PASS |
| `driver_gcs_client` | driver | 0 | tier 1, count | 4 lines, expected 4 | PASS |
| `driver_gcs_client` | `--run-mode=local` | 0 | tier 2, convention marker | marker present | PASS |
| `driver_metrics` | driver | 0 | tier 1, count | 1 line, expected 1 | PASS |
| `driver_metrics` | `--run-mode=local` | 0 | tier 2, convention marker | marker present | PASS |
| `zero_copy_benchmark` | driver | 1 | tier 3, exit code only | exit 1, raylet crash; class D (section 3.2) | FAIL |

Summary: 12/13, the only non-pass being the class D raylet crash. No other row regressed from the serializer change. Artifacts: one `<stem>.log` and `.log.exit` per row in both `/tmp/fleet-p1-regress` and `/tmp/fleet-p1-regress2`, plus each directory's `summary.txt`.

### 2.4 Scaled re-run — Phase 1 at deadline scale 3 (`/tmp/fleet-p2-scaled-final`, branch head `041e45def7d`)

The load-tolerance re-run of the five Phase 1 polling drivers (eight rows) against the multi-node cluster with `--timeout-scale 3`, to show the polling deadlines tolerate a loaded cluster.

| Driver | Mode | rc | Criterion tier | Observed vs expected | Verdict |
|---|---|---|---|---|---|
| `driver_app` | driver | 0 | tier 2, marker `✓` | marker present | PASS |
| `driver_app` | `--run-mode=local` | 0 | tier 2, marker `✓` | marker present | PASS |
| `actor_integration_driver` | driver | 0 | tier 1, count | 12 lines, expected 12 | PASS |
| `runtime_context_driver` | driver | 0 | tier 1, count | 3 lines, expected 3 | PASS |
| `driver_placement_group` | driver | 0 | tier 1, count | 3 lines, expected 3 | PASS |
| `driver_placement_group` | `--run-mode=local` | 0 | tier 2, convention marker | marker present | PASS |
| `driver_gcs_client` | driver | 0 | tier 1, count | 4 lines, expected 4 | PASS |
| `driver_gcs_client` | `--run-mode=local` | 0 | tier 2, convention marker | marker present | PASS |

Summary: 8/8. Artifacts in `/tmp/fleet-p2-scaled-final` with `summary.txt`.

### 2.5 Scaled re-run — the one Phase 2 polling driver at scale 3 (`/tmp/fleet-p2-scaled-cpp`, branch head `041e45def7d`)

`driver_call_cpp` is the only polling driver that lives in Phase 2, so it needs its own scaled invocation (a `--phase 1` selection would silently drop it).

| Driver | Mode | rc | Criterion tier | Observed vs expected | Verdict |
|---|---|---|---|---|---|
| `driver_call_cpp` | driver, `--timeout-scale 3` | 0 | tier 2, marker `passed` | all five cases green; `=== All Go->C++ cross-language tests passed! ===` present | PASS |

Summary: 1/1. Artifacts in `/tmp/fleet-p2-scaled-cpp` with `summary.txt`.

Across all five invocations the matrix holds 39 rows, each with a matching log and exit record; the two Phase 1 regression invocations share the 13 rows of section 2.3.

## 3. Every non-pass with its class and evidence

Two rows did not pass anywhere in the matrix. Both carry their class and their raw evidence; neither was closed by re-running.

### 3.1 `test_python_call_go_simple` (direct lane, Phase 2) — class B

The runner's `direct` row runs the Python lane with the local interpreter and passes `--ray-address=$CLUSTER_ADDRESS`. Against a remote cluster that address is the dashboard HTTP URL that `ray job submit` takes, but Ray's Python client (`ray.init(address=...)`) accepts a GCS address (`<ip>:<port>`) or a `ray://` client URL, not an HTTP dashboard URL, so the lane dies before it reaches any Go code. This is a procedure gap, not a code defect: the lane must run on a Ray node (where the local cluster needs no address) or be given a GCS address routable from where it runs, and the node that runs it needs this tree's `cross_language.py` and `_raylet.so`. `go/e2e/README.md` documents both. No runtime code was changed for this class.

Evidence: `/tmp/fleet-p2-final/test_python_call_go_simple__ray_address_http___127.0.0.1_18265.log` (exit record `/tmp/fleet-p2-final/test_python_call_go_simple__ray_address_http___127.0.0.1_18265.log.exit` = 1), whose decisive line is:

```
ValueError: Invalid Ray address: 'http://127.0.0.1:18265'. It looks like you set RAY_ADDRESS to an HTTP address ...
```

### 3.2 `zero_copy_benchmark` on the multi-node cluster — class D

On the local single-node Phase 1 cluster this row passes (section 2.1, rc 0, exit-code tier). On the multi-node Kuberay cluster it repeatedly kills the raylet with an upstream check failure:

```
core_worker_client_pool.cc:120: Check failed: RAY_PREDICT_TRUE(_left_ != _right_)
  ... ray::rpc::CoreWorkerClientPool::GetOrConnect()
  ... ray::raylet::LeaseDependencyManager::RequestLeaseDependencies()
  ... ray::raylet::NodeManager::HandleRequestWorkerLease()
```

This is an empty `worker_id` on a lease dependency's owner address, inside upstream C++ raylet code that this round neither builds nor links the Go runtime into. The crash kills the head container, so the full Phase 1 fleet cannot complete on this cluster while the row is present: 12 rows pass and the cluster restarts during the 13th. It is recorded as class D and deliberately not closed — no code was changed for it and no deadline was scaled to hide it.

Evidence, both legs of the A/B:

- The fixed-tree run: `/tmp/fleet-p1-regress/zero_copy_benchmark.log` and `/tmp/fleet-p1-regress2/zero_copy_benchmark.log` (each with `.log.exit` = 1). The client side ends in `RuntimeError: WebSocket connection closed unexpectedly with close code 1006` — the job client losing the head, not a driver error.
- The reverted-tree run: with the six Go files this round changed reverted to `66a2efd1fe2` and the reverted `go_runtime.so` synced into both pods, the run in `/tmp/fleet-zc-ab2` (`zero_copy_benchmark.log`, `.log.exit` = 1, and a `summary.txt` recording the row's failure) fails identically. The raylet-side stack from the head pod's previous-container log is archived at `/tmp/fleet-ab-evidence/head-reverted-tree-previous.log:131` (with the full frame list at lines 133-147); every frame is inside the raylet binary. A second capture of the same signature from the fixed tree is at `/tmp/fleet-ab-evidence/head-previous-105018.log:38`.
- The second leg: the Task 3 report records the identical signature before any of this round's commits, so the crash is not a regression from the round's changes.

## 4. The defects the validation found

Eight class A defects — real defects in this tree's own Go runtime, drivers, or Python binding — were found and fixed by the run. Four prior rounds of review (the per-task review, the fix-round review, the whole-branch review, and an external review) had read this code and caught none of them; none of the eight is reachable by reading the code, only by running the fleet. Each line names the defect, what it broke, and where the fix lives.

Phase 1 (Task 3):

1. `python/ray/_private/node.py` — `Node.__init__` called `_get_cached_port("metrics_agent_port", …)` before `_init_temp` had created the session dir and before the plasma-store socket name was set, so the port cache lookup dereferenced `self._session_dir` and `self._plasma_store_socket_name` before they existed. It broke `ray start --head` outright (`AttributeError: 'Node' object has no attribute '_session_dir'`), so no cluster could start and the whole round was blocked at Phase 0. Fix: move the call after the socket-name resolution block.
2. `src/ray/core_worker/lib/go/worker_context_ops.h` — `WorkerContextOperations::GetNamespace()` returned `""` (with a `TODO`), so `RuntimeContext.Namespace()` was always empty. It broke every placement-group and actor lookup by name (the GCS name index is keyed by the job's Ray namespace): `GetPlacementGroupByName(name, "")` returned nil while `GetPlacementGroup(id)` on the same live group succeeded. Fix: return `GetJobConfig().ray_namespace()`.
3. `go/internal/runtime/native/native_runtime.go` — `NativeRuntime.Start` built the object store as an empty literal (`&objectstore.NativeObjectStore{}`), so its `shutdownLock` stayed nil while `RemoveLocalReference` dereferenced it. It broke every `ObjectRef` release with a SIGSEGV in `RemoveLocalReference`, killing `driver_app`, `actor_integration_driver`, and `zero_copy_benchmark`. Fix: construct through `NewNativeObjectStore(&nr.shutdownLock, nil)` and hold the write side across the C++ teardown.
4. `go/internal/runtime/cgo/native_submitter.go` — `GetActorHandle` returned a handle for any non-nil actor ID, so the by-creation registry was write-only (`contains()` was dead code). It broke the contract that an uncreated actor ID must not resolve. Fix: consult the registry, with a red-first test in `native_submitter_test.go`.
5. actor exit never reached the core worker — `api.ExitActor` removed the Go instance and returned an `ActorExitError`, but nothing emitted the intentional-exit status and the Go worker's `check_signals` was null, so the core worker's periodic check that polls the flag never ran. It left exited actors `ALIVE` and holding the node's entire CPU budget, starving later actors and hanging `actor_integration_driver` for 12 minutes in an `api.Get`. Fix: a should-exit flag bridge in `src/ray/core_worker/lib/go/{worker_context_ops.h,native_worker_context.h,native_worker_context.cc,runtime_ops.cc}` plus a non-null `check_signals` in `go/internal/runtime/native/native_runtime.go` and `go/internal/runtime/cgo/native_context.go`.

Phase 2 (Task 4):

6. `go/pkg/runtime/api/ray.go` — `InitializeWithJobConfig` and `InitializeWithJobConfigAndNetwork` built the caller's `JobConfigBuilder` and went straight to `BuildToJobOptions()`, never calling `MergeFromEnv()`. A job submitted through the job supervisor delivers its `runtime_env` (including `working_dir`) only through `RAY_JOB_CONFIG_JSON_ENV_VAR`, so on a multi-node cluster the driver's job config reached the core worker with an empty `runtime_env_info`, the task and actor workers ran without the job's `working_dir`, and the plugin could not be resolved (`function not registered`). Fix: merge the injected config through a shared `buildJobOptionsFromBuilder` helper before `BuildToJobOptions()`, with red-first tests in `ray_job_config_env_test.go`.
7. `go/internal/runtime/serializer/msgpack_serializer.go` — the Go encoder emitted a cross-language struct as a msgpack map, while Ray's C++ runtime packs a struct declared with `MSGPACK_DEFINE` as a positional array. It broke `driver_call_cpp`'s `echo_student`, which failed in the C++ worker with `msgpack::v1::type_error: std::bad_cast`. Fix: enable `UseArrayEncodedStructs` for cross-language values (fully-exported structs) in `newEncoder`, with a red-first test.
8. Python → Go binding — `ray.cross_language` exposed `java_function`/`cpp_function` but no `go_function`, and `ray._raylet` had no `Language.GO` and no `GoFunctionDescriptor`, so the Python → Go lane died at import (`ImportError: cannot import name 'go_function'`). Fix: add `Language.GO`, a `GoFunctionDescriptor` Cython class over `FunctionDescriptorBuilder::BuildGo`, and `go_function`, across `python/ray/cross_language.py`, `python/ray/includes/{common.pxd,function_descriptor.pxd,function_descriptor.pxi}`, and `python/ray/_raylet.pyx`.

Two further non-class-A corrections were declared, not hidden: a class C assertion correction in `actor_integration_driver` (its `TestActorHandleKill` part 2 polled for a call-failure window a force-kill restart does not expose; the assertion now distinguishes the restarted instance by its creation state) and a class C assertion correction in `driver_call_cpp` (it treated the C++ descriptor's caller as a required field, contradicting the runtime's documented contract). Class B documentation fixes cover the runtime pin and token auth (Phase 1) and the direct lane's placement and the non-durable runtime sync (Phase 2).

## 5. What this round did not cover

- **The fleet is not in CI.** It is a manual, scripted lane a human invokes; this is a deliberate non-goal, not an omission. The runner's `--dry-run` mode is what keeps it verifiable without a cluster.
- **The direct lane is red for its class B reason** (section 3.1). `test_python_call_go_simple` does not pass in a remote Phase 2 run as the runner invokes it; it needs to run on a Ray node or be given a routable GCS address, and the runner now rejects the impossible combination up front rather than submitting a row certain to die at `ray.init`. The lane has never run past `ray.init`, so nothing downstream of it is verified: the runner now stages its `userfuncs.so` (which its relative `code_search_path` names) into the job directory, but whether a Go worker resolves that relative path when the job carries no `working_dir` is untested. It is documented, not worked around.
- **The archived runs predate the review-response fixes.** The logs in section 2 were produced before the serializer's cross-language predicate was unified into one decision, before `buildJobOptionsFromBuilder` was made repeatable, and before the runner staged `userfuncs.so` into the direct lane's job directory. Those fixes are covered by unit tests (each written to fail against the previous code) and by the runner's `--dry-run` plan, not by a re-run, so no row in section 2 exercised them.
- **The raylet crash blocks a clean full Phase 1 re-run on the multi-node cluster** (section 3.2). The Phase 1 set is 13/13 only on the local single-node cluster; on the multi-node cluster the `zero_copy_benchmark` row kills the head, so the full set reaches 12/13 there. The crash is upstream and reproducible with this round's changes reverted.
- **An in-pod runtime sync is not restart-durable.** The Phase 2 runtime was synced into the pods by hand; a pod restart restores the image's `go_runtime.so` and every Go driver then fails with a plugin version mismatch. A durable deployment needs the runtime baked into the image. The round re-synced after each restart.
- **The serializer wire-format change has a wide blast radius.** Cross-language structs (fully-exported) now encode as positional arrays everywhere, not only on the Go → C++ path. This converges the Go encoder with the C++ `MSGPACK_DEFINE` convention, and Go → Go round trips are unaffected (the decoder accepts both forms), but any Go → Python or Go → Java struct consumer that expects a map would now see an array. No such consumer exists in the fleet today, so the new array form has no Go → Python struct consumer exercising it here.
- **Graceful (non-force) actor kill is unimplemented for Go.** The Go worker's `check_signals` is a no-op that returns OK: it enables the core worker's periodic poll but implements no signal handling and no cancellation interruption, and the Go `KillActor` bridge always passes `force_kill=true`, so a graceful actor kill is never exercised. The gap is deferred with this evidence and documented in the code; nothing in either phase exercises it, and it should not be read as "Go has graceful kill".
- **The bazel test gate is green at the round's head: 107/107.** `bazel build //go/e2e/...` and `bazel test --nocache_test_results //go/{cmd,internal,pkg,proto,examples,e2e}/...` both pass; the result is restated here because it otherwise lives only in the round's commit bodies.

## 6. [23/N] round: the Python→Go lane's first cluster-mode run

This section records the [23/N] round's real-machine run of the rewritten `test_python_call_go` lane (section 2's direct lane, replaced by the rewrite), which reached further than any previous attempt: the lane connected to the cluster in cluster mode, a Go worker started with the plugin loaded from the relative code-search path, the cross-language function was found and executed, and the failure moved from "never got past `ray.init`" to the return path — a class A defect pair the earlier phases could not see.

Environment: the same two-node Kuberay cluster as section 1, rolled to a new image built from this branch's head (the wheel carries the `GoFunctionDescriptor.class_name` Cython binding and the void-return padding; the image build asserts the binding round-trips, the Go artifacts, and the dashboard build). Head `Ray runtime started.` 7 s after start; the worker's `wait-gcs-ready` init container reported `GCS is ready.`; the lane ran on the head node so the cluster-mode requirement holds by construction.

What worked, in order: `ray.init()` connected (cluster mode, no working_dir); the Go worker loaded `userfuncs.so` from the relative `./userfuncs.so` code-search path (the class B blocker sections 3.1 and 5 anticipated never materialized — the plugin resolved and the function was found); the Go executor ran `goAdd` with 2 args and produced a return array (its log: `Executing task {"functionDescriptor": [..., "goAdd", ""], "argsCount": 2, "numReturns": 1}` → `Returning result array {"count": 1}`).

The failure: Python's `ray.get` on that result raised `RaySystemError: Can't deserialize object ... metadata: b'{"type":"error"}'` from `python/ray/_private/serialization.py` (`int(metadata_fields[0])` on a non-numeric metadata). Two defects, one behind the other:

- **Class A (deferred to the next round): Go task-error objects are unreadable cross-language.** When a Go task fails, `convertGoResultToC` (go/internal/runtime/cgo/task_executor.go) wraps every return slot with metadata `{"type":"error"}` and a JSON error payload. Python's `_deserialize_object` parses error-object metadata as an integer error-type code and raises an opaque `RaySystemError` on anything else, so a Python caller cannot see a Go task's real error. The Go reader accepts both conventions (`ErrorObjectFromNative` in go/pkg/runtime/object/exception.go checks this metadata first, then the numeric one), so Go→Go is unaffected. Both trees share the writer, and the internal tree's Python has the same integer-only parse — an inherited defect made visible the first time a failing Go task was read from Python end to end.
- **Class A (deferred, blocked by the above): the underlying `goAdd(10, 20)` failure is not yet diagnosed.** The Go executor took its error path (the 16-byte `{"type":"error"}` metadata is written only there), so the 2495-byte JSON error payload inside the returned object holds the real cause; extracting it requires the first defect to be fixed (or a Go-side reader in the pod). Args deserialization is the suspect set, but nothing points at a specific cause yet, and this round does not guess.

Consequence: Part A's remaining calls and all of Part B (the actor path) could not run; the void-return padding fix shipped in this branch is covered by its unit tests and remains unexercised end to end. The run stopped after the first failure with the full log archived (`/tmp/lane-run-23n-first.log` on the client host) and the Go worker log on the head pod; no re-runs were attempted, because the failure is deterministic and its cause is recorded, not transient.

## 7. [24/N] round: the lane goes green end to end, the exit chain gets its real-machine leg

This section records the [24/N] round's real-machine run of `test_python_call_go` (cluster mode, on the head pod of a fresh two-node Kuberay cluster, rolled to an image built from this branch's wheel), which closed every red this report has carried: the lane passed **9/9 tests (exit 0)** — Part A's four stateless calls plus the concurrency spread, and, for the first time end to end, all of Part B: the `Counter` actor creation, the value-returning `Add`, the void-method regression (`Void()` returning `None` without crashing the worker), and the post-void state/worker-alive assertion. The void-return padding shipped in [23/N] is exercised by the real worker path, not only by unit tests.

What stood between section 6's red and this green: the wire-convention fix (task-error objects now carry decimal metadata `"3"` and a msgpack-wrapped `RayException` payload — [24/N]'s W1), a consistently-built binary set, and the runtime baked into the image instead of hand-synced. Before shipping, the three Go binaries were pinned to one package hash (`raygo`, `go_runtime.so`, `userfuncs.so` all `393b01d96cd10946`, via `go/check_pkghash.py`) — the stale-plugin failure class that section 2's D4 check guards for the repo build is now also asserted across the image boundary.

**Section 6's class-A pair, closed:** Test 1 `goAdd(10, 20)` — the round's opening red — returned **30**. The underlying failure's root cause is not recoverable: the [23/N] archived log confirms only the unreadable wrapper (`Can't deserialize object ... metadata: b'{"type":"error"}'`), and the payload's 2495 bytes died with the old cluster's object store. What can be said is bounded: the Go worker genuinely took its error path (that metadata is written only there), and this round's run differs from it precisely in the suspected dimension — consistent, hash-verified binaries replacing a hand-synced runtime against an older baked one. The unreadable-payload defect that HID the cause is itself fixed by this round: any future Go task error surfaces to Python as a readable `CrossLanguageError` carrying the message.

**Exit chain (the W2 real-machine leg):** the `userfuncs` plugin already exposed `Counter.Exit()` (`api.ExitActor()`), so a probe ran on the same cluster: baseline `Add(1)` → 11; `Exit()` raised a clean `ActorDiedError` (no hang, no worker CHECK abort, no restart loop); `Value()` after exit raised `ActorDiedError` again (actor marked dead); the driver stayed alive throughout (probe exit 0). The intentional exit surfaces to Python as a clean actor-death error — the INT-aligned `IntentionalSystemExit` path drives the teardown and neither double-signals nor swallows.

**Runtime-vs-pod-restart (section 4's "in-pod sync does not survive a restart" item):** the worker pod was deleted and its replacement pulled the same image; after re-syncing only the `userfuncs.so` plugin (the one artifact still deliberately not baked), the full lane re-ran **9/9 (exit 0)** (`/tmp/lane-run-24n.log` on the client host). The Ray runtime itself — `raygo` and `go_runtime.so` — now survives pod replacement from the image, closing the section 4 item for the runtime; the plugin remains a manual sync step by design until the lane's packaging work (section 4's CI item) takes it.

Not covered by this round's machine run: the lane's `Exit()` leg lives in the probe, not in `test_python_call_go.py` itself — folding it into the lane (and the plugin's packaging) belongs to the next round that touches the lane. `LocalModeObjectStore` and the graceful-kill path (section 5) remain untouched by design.

## 8. Reporter migration and the dashboard-agent switch path ([25/N])

This section records the first real-machine run of the migrated Go reporter module ([25/N]) and — for the first time on a cluster — the `RAY_ENABLE_GO_DASHBOARD_AGENT` switch itself. Environment: a fresh two-node Kuberay cluster; the image was built from this branch's cp311 wheel layered on the previous round's image (the wheel carries `raygo` and `go_runtime.so`); the switch was enabled on both head and worker via pod environment; the `userfuncs.so` plugin was synced manually as before. Pre-flight: the wheel's `raygo` and `go_runtime.so` and a freshly built `userfuncs.so` shared one package hash (`go/check_pkghash.py`), and the image's `raygo` was asserted to expose the round's new `--is-head` flag before deployment.

**Four defects found and fixed on this path (all inherited from the reference implementation — design gaps that upstream-Ray deployment semantics expose):**

1. **Startup deadlock in `is_head` resolution.** The raylet launches the agent and waits (CHECK, 15 s) for it to persist its gRPC port file, and only registers its own node in GCS afterwards; the agent's `resolveIsHeadRetry` polls the GCS node table for its own node (30 s budget) — a circular wait, both sides time out, `ray start` aborts. Fixed by passing `--is-head` explicitly from `services.py` (which knows the topology) and honoring it in the agent; the GCS-derived path remains as the fallback for callers that do not pass the flag.
2. **The port-persistence protocol was not implemented.** With dynamically assigned ports (the production default, `--grpc-port=0`), the raylet waits for `metrics_agent_port_<node_id>` / `metrics_export_port_<node_id>` / `dashboard_agent_listen_port_<node_id>` files. The Python agent writes them after binding (writing `-1` for services it does not run); the Go agent wrote none. Fixed: the agent persists the actual bound gRPC and HTTP ports and `-1` for metrics export (the agent does not export metrics — the Python minimal-mode precedent).
3. **GCS registration used the configured (0) ports.** With dynamic ports the agent registered `[ip, 0, 0]`; fixed to register the actual bound ports, from the same values the persistence step writes.
4. **HTTP bound to the node IP only.** The operator's readiness probe dials `localhost:<listen port>` from inside the container; a node-IP-only bind refuses it, so the pod never reached Ready. The Python agent's health endpoint answers on both. Fixed: the HTTP server binds on all interfaces.

**Results:**

| case | metric | value |
| --- | --- | --- |
| uncached union bazel (final head) | tests | 108/108 pass |
| pytest raygo + log_monitor | tests | 14/14 pass |
| package hash (wheel raygo / go_runtime.so / userfuncs.so) | check | all equal |
| head pod readiness with the switch on | time to 1/1 | ~33 s, 0 restarts (previously: permanent 0/1 or crash loop) |
| probe 1 — publish loop health | errors | agent started once; 0 `failed to publish node resource usage` lines over the window; all three port files present; agent process alive |
| probe 2 — end-to-end publish → subscribe | delivery | `RAY_REPORTER:<node id>` received by a Python `GcsAioResourceUsageSubscriber` with a parseable payload (cpu/memory/disk/agent keys — the container's real readings) |
| lane, run 1 | tests | 9/9 pass, exit 0 |
| lane, run 2 after worker-pod replacement + plugin resync | tests | 9/9 pass, exit 0 |

Probe 2's shape: subscribe to `RAY_NODE_RESOURCE_USAGE_CHANNEL` via `ray._private.gcs_pubsub.GcsAioResourceUsageSubscriber` (the same channel the Python head dashboard consumes), then long-poll. Two probe-side corrections worth recording: the subscriber must call `subscribe()` before polling (messages published before registration are not retained), and the local rehearsal was abandoned because the development host runs unrelated Ray installs that collide on the agent port and token file — the cluster run above is the authoritative evidence.

**Recorded as observations (not asserted):** the agent-side Prometheus registry remains module-private with no HTTP export point (the same shape as the reference tree; the export logic is covered by the migrated `metrics_service_test`). With the gRPC bind fixed, the raylet can push `ReportOCMetrics` into the agent, but the received gauges still land in this module-private registry with no HTTP export point, so the metrics pipeline remains write-only end to end (deliberate staged delivery; see the JAX-style gap note below); the `event` module logs transient `report_events` connection errors while the head dashboard is unavailable (expected on a cluster without the Python dashboard head, self-heals when it comes up). **Capability gap recorded per the round's ruling:** the OSS Python reporter's JAX profiling path has no Go counterpart (the reference tree has none either); with the Go agent enabled, JAX profiling is unavailable — the same trade the reference tree makes, now narrowed from "the whole reporter is missing" to "JAX profiling only".

## 9. Graceful kill and the GCS state test completion ([26/N])

This round's machine leg validates the graceful-kill work and — first among the round's findings — corrects a long-stale record: the twelve GCS state methods listed as "not implemented" in the tracking doc have in fact been implemented since round 18 (the `ErrNotImplemented` hits are the documented `cc == nil` mock fallback seam, not production stubs); this round adds the missing mock-path test coverage for the nine query methods (18 table-driven subtests) and closes the tracking item. Environment: a fresh two-node cluster from this branch's image; the pre-flight package-hash check passed for the wheel's `raygo`/`go_runtime.so` against a freshly built `userfuncs.so`.

**Graceful-kill probe, arm A (KillActor force difference, driven by the Go driver):** the extended `actor_integration_driver` kills one actor with `KillActor(force=false, noRestart=false)` and one with `force=true`; both actors become unavailable as asserted, and the raylet log shows the graceful arm's path — the raylet forwards a KillActor RPC to the worker, the worker begins its graceful exit (the RPC then reports `Socket closed`, the natural artifact of a worker that is exiting), and the disconnect is recorded as `graceful=true`. The exit-type records for both arms show `disconnect_type=3` with `graceful=true`, matching the semantics matrix documented on `KillActor` after the review corrections.

**Graceful-kill probe, arm B (direct SIGTERM, the check_signals mechanism):** a `Counter` actor was created from Python; the Go worker hosting it (identified as the process titled `ray::Counter` — see the methodology note below) was sent `kill -TERM`. The worker's core-worker log records the full chain in six milliseconds: the signal handler sets the graceful-exit flag, the periodic check returns `IntentionalSystemExit`, and the core worker logs `Executing worker exit: INTENDED_USER_EXIT - Worker exits by a signal. SIGTERM received: graceful actor kill` followed by waiting for in-flight tasks to finish; the raylet records the disconnect as `graceful=true`. This is the raylet idle-kill / placement-group-teardown / `ray stop` / pod-graceful-termination path: without this round's fix the Go process died on the default SIGTERM disposition and the exit was recorded as a system error.

**Inbound ReportOCMetrics probe (the review-suggested inbound leg):** dialing the dashboard agent's gRPC port over localhost (read from the persisted port file) and pushing metrics directly: a well-formed request is accepted; a malformed one (a descriptor with an empty label key and a series with mismatched value count) is **skipped at the data layer and the agent stays alive** — the label-alignment defense from the previous round's review response absorbs it before the recovery interceptor is even needed; a follow-up valid request confirms the agent is healthy.

**Lane regression:** 9/9, exit 0 on the same cluster after all probes.

**Methodology notes recorded for future probes:** (1) a naive `pgrep -f 'raygo setup_worker'` matches the raylet itself, whose command line embeds the worker command template — the first arm-B attempt SIGTERM'd the raylet and produced a false positive (the head pod was recreated and the probe rewritten to match real worker processes); (2) a running Go actor worker retitles its process to `ray::<ActorClass>`, so the `setup_worker` command shape only matches during startup; (3) `KillActor` is an owner-side API — a remote function executing on a worker cannot kill an actor it does not own (`NotFound: Failed to find a corresponding actor handle`), so force-matrix validation is driven from the Go driver where the handle lives; the `KillActorByID`/`killGoActor` helpers built for the abandoned probe path were removed (no remaining callers).

## 10. Switch coverage, review-park paydown, and the first CI gate ([27/N])

This quality round carries no migration: it pays down the three low findings parked by the previous round's review (the dashboard agent's `Run` error-path cleanup asymmetry, a non-positive memory-profile duration reaching the wait timer, and the `subprocesses` flag silently dropped from traceback and CPU profiling — each fixed red-first with its own commit), pulls the six Go switches up to uniform launch-level and end-to-end test coverage, and lands the repository's first CI workflow (a hosted-runner gate: fast pre-commit hooks, a mechanically derived non-cgo non-proto Go subset built/vetted/tested, a trimmed fleet dry-run, and an advisory Python switch-launch job — the full bazel union and real-cluster lanes stay with the contributor, as `go/CI.md` documents). Machine validation ran on a fresh two-node cluster from an image built by this branch's wheel; the pre-flight package-hash check passed for the wheel's `raygo`/`go_runtime.so` pair against a freshly built `userfuncs.so`.

**Switch coverage:** launch-level assertions now cover all six switches (`test_logging.py`, where the `go or raygo` selector matches 15 tests: 10 pre-existing plus 5 from this round) and the Go side reached cmd-level 7/7 with the dashboard head's flags pinned. The end-to-end matrix (`test_go_component_e2e.py`) drives each switch on a live cluster and asserts binary identity, subcommand, and a behavioral probe per component: all six green on a bare dev machine; five of six green inside a head pod, where the one failure is the matrix doing its job — see the finding below.

**Real-machine finding (product gap, recorded):** the Go dashboard agent and the Python dashboard agent share the same fixed default listen port (52365) with no occupancy probe on either side. Inside a pod whose hosting cluster already runs a Python agent, the matrix's second cluster enabled the Go agent, which hit `bind: address already in use` on 52365 and degraded exactly as designed-but-undocumented: it stayed alive, disabled its HTTP server, and persisted `-1` as its listen port (the pod's `dashboard_agent.err` captured the error verbatim). The test failed loudly on `dashboard_agent_listen_port > 0` — the correct failure mode per this round's review — and the gap is recorded in the tracking doc for the component owners: two agents cannot coexist on one node.

**Matrix hermeticity fix (class C, test-only):** the matrix's first pod run failed all six tests because `ray.init` resolved the address from the current-cluster files and connected to the hosting cluster instead of starting its own (then rejected `num_cpus` for a connect-style init). Every `ray.init` now pins `address="local"`, making the matrix hermetic on a bare machine and equally inside a pod that already hosts a cluster; the dev-machine run stayed 6/6 after the change.

**Python→Go lane ([24/N]'s class-B lane, revalidated):** the lane — merged long ago from the former `test_python_call_go_simple.py` into `test_python_call_go.py` — passed **9/9, exit 0 on the first try** on this round's cluster (job-submitted with the staged `userfuncs.so`, the one artifact deliberately not baked into the image). Part A's stateless calls, the concurrent spread, and Part B's actor chain including the void-method regression (`Void()` returns `None`, state preserved after) all held.

**Phase 1 fleet regression:** 12 of 13 driver runs passed against the same cluster through a port-forwarded dashboard. The single failure is `zero_copy_benchmark`, which crashed the head pod mid-job (Kuberay restarted it; the job's log stream died with a WebSocket 1006) — exactly the upstream raylet blocker recorded in section 4 (`core_worker_client_pool.cc:120`), unchanged by this round and pre-classed as the reason multi-node Phase 1 cannot reach 13/13.

| leg | result |
| --- | --- |
| union bazel gate (branch head) | 109/109 pass |
| pytest launch tests (`go or raygo`) | 15/15 pass |
| e2e matrix, dev machine (gate on) | 6/6 pass |
| e2e matrix, head pod (gate on) | 5/6; agent leg = the fixed-port gap above |
| Python→Go lane on the cluster | 9/9 pass, exit 0 (first try) |
| Phase 1 fleet vs cluster | 12/13; `zero_copy_benchmark` = documented upstream blocker |

**Not covered:** the token-auth path (the Go GCS clients cannot authenticate under the default token posture — a recorded product gap; the matrix pins auth off for its live run); phase 2's job rows (their surfaces are untouched by this round and carried green from the previous round); a second cluster coexisting with a Go agent on one node (blocked by the recorded port gap, not retested). The hosted-runner workflow itself has not executed yet — the trigger produced zero runs on this pull request (verified against the commit's check runs); a workflow_dispatch trigger is added so the first verified execution is a manual dispatch right after this merge, before any later pull request depends on the gate; every job's commands were validated locally, including a cold-cache reproduction of the derivation script.

## 11. CI gate completion, the port fallback, and token auth end to end ([28/N])

This round carries no migration either: it completes the CI gate section 10 landed, closes the two real-machine gaps that round recorded, and re-validates the switch matrix under the cluster's real authentication posture. The previous round's five CI review lows are all paid down: the `test_start_api_server_uses_go_dashboard_when_enabled` fake GCS client gained the kwargs-accepting `__init__` whose absence had made the production GcsClient constructor raise a TypeError that the `raise_on_failure=False` handler swallowed, so the internal-KV polling section now actually executes; the workflow's push trigger now carries the exact paths filter its pull_request trigger has; buildifier's file regexes now see the top-level `go` tree (five BUILD files mechanically fixed: a missing py_binary/py_test load in `go/BUILD.bazel`, source and attribute ordering in two files, missing trailing newlines in two more); and the Python jobs cache their pip installs while the fast-hooks job caches the pre-commit hook environments. `go.mod` is self-consistent for a plain toolchain via four pinned `go get` adds (`github.com/coder/websocket`, `github.com/sabhiram/go-gitignore`, `golang.org/x/crypto`, `golang.org/x/sync`) with the CI derivation set byte-identical (17 packages before and after), and the five dirty files the previous round's CI had to skip are clean, so end-of-file-fixer and black rejoin the fast-hook gate and the local pre-commit run is fully green again.

**The port gap is closed with a fallback, not by unseating the Python agent.** A red-first test holds the fixed default listen port 52365 and requires `Run` to succeed with a persisted listen port that is dynamic and different from the taken one; on `EADDRINUSE` the agent now warns and rebinds onto a dynamic port instead of degrading into an HTTP-less agent that registers nothing, and head-side consumers still resolve it because `Run` registers the actually bound port in GCS InternalKV — the registration is also the only channel carrying the dynamic port, since the raylet reads the persisted port files only when the configured port is 0 and otherwise keeps publishing the fixed one. The e2e matrix's agent leg now passes under port pressure. The Python agent's port-taken behavior — degrade in place, stay alive with the HTTP service disabled — is upstream's and is left as alignment; only the Go agent learned the fallback.

**Token auth: the recorded product gap dissolved on inspection into an environment-posture misread, with one real gap behind it.** Recon against the C++ sources showed the cgo GCS bridge was never broken: `RAY_AUTH_TOKEN` alone activates no client anywhere — the gate in `grpc_client.cc` is the two-state `GetAuthenticationMode() == TOKEN`, and the recorded failure had been produced with `RAY_AUTH_MODE` unset, so the loader's own precedence comment never came into play. Removing the matrix's disabled-auth pin then exposed the real gap: the dashboard head's native Go GCS client was built without a token, so under the default posture every RPC came back Unauthenticated and `ray.init` timed out waiting for the published dashboard address. The fix mirrors the C++ `AuthenticationTokenLoader` source precedence at the cmd layer (`RAY_AUTH_TOKEN`, then `RAY_AUTH_TOKEN_PATH`, then the Ray service-account token when `RAY_ENABLE_K8S_TOKEN_AUTH` is on, then the default `~/.ray/auth_token` file), and the matrix now runs under the ambient auth posture — token auth is the ray.init default — with all six legs green (verified in both postures: gate-on 6 passed, gate-off 6 skipped).

**The CI maiden run belongs to this round.** This round's pull request was the first created after the workflow landed on the default branch, and its four jobs — fast-hooks, pure-go, fleet-dry-run, python-switch-tests — all passed, including the pure-go job: its first-run failure was the one the previous round's review had predicted, and this round's first commit fixed it.

**Superseding section 10:** its token-auth product-gap item is resolved — there is no code gap in the bridge (the recorded failure was the environment posture above), the head's missing token is fixed, and the matrix pin is removed; the section's "the matrix pins auth off" statement describes the previous round's posture, superseded by this round's ambient-posture run. Its co-hosted-agents item — "a second cluster coexisting with a Go agent on one node (blocked by the recorded port gap, not retested)" — is superseded by the fallback fix and the port-pressure verification above. The rest of section 10 stands as that round's record.
