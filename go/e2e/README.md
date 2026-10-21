# Ray Go E2E Drivers

This directory contains end-to-end (E2E) integration test cases for the Ray Go
runtime. They run against a local Ray cluster to verify the core functionality
of the Go runtime and serve as reference for further development.

## Directory layout

```
e2e/
├── BUILD.bazel                 # Top-level notes: E2E driver and userfuncs build targets
├── README.md
├── driver_app/                 # Remote task submission (Free API / GetActorHandle demos)
├── actor_integration_driver/   # Actor lifecycle / isolation / multiple returns / Exit / Kill / creation options / handle.kill
├── driver_parallel_actor/      # Parallel Actor concurrency-group model: instance routing / isolation / state retention
├── driver_call_python/         # Go -> Python cross-language calls
├── driver_call_cpp/            # Go -> C++ cross-language calls
├── driver_placement_group/     # Placement Group user API: lifecycle / placement / validation + local mode
├── driver_gcs_client/          # User-side GCSClient extension: PG queries x3 / KV / Node / Actor
├── driver_metrics/             # User Metrics API: Count/Gauge/Sum/Histogram
├── runtime_context_driver/     # RuntimeContext and resource-ID API
├── global_state_accessor_driver/  # GlobalStateAccessor production implementation + actor-state filter pushdown
├── runtime_env_driver/         # User-side RuntimeEnv API + Autoscaler status-report loop
├── zero_copy_benchmark/        # Zero-copy Get performance benchmark (local / cross, 4MB / 64MB)
├── userfuncs/                  # Shared user-function package (used by the Go-task/actor drivers)
│   ├── BUILD.bazel
│   ├── funcs.go                # User function implementations
│   ├── actors.go               # Actor types (Counter, RuntimeContextProbe, etc.)
│   ├── doc.go                  # Package description and //go:ray:task marker usage
│   ├── echo_bytes.go           # High-performance byte-serialization function
│   ├── generated_registry.go / generated_wrappers.go  # Auto-generated code
│   └── plugin/                 # Compiled to userfuncs.so for worker loading
│       ├── BUILD.bazel
│       └── main.go
├── test_go_call_python.py      # Python-side file for driver_call_python
└── test_python_call_go_simple.py  # Python -> Go call example (referenced by docs)
```

## E2E cases

Every case is submitted to a local Ray cluster as a standalone driver binary. The whole fleet is driven by `go/e2e/run_fleet.sh`, which rebuilds, stages and submits each selected driver and applies the pass criterion; the single-driver manual form below is the fallback.

```bash
# Build the driver fleet and the userfuncs plugin, then run the Phase 1 set.
bazel build //go/e2e/... //go/e2e/userfuncs/plugin:userfuncs_plugin
bash go/e2e/run_fleet.sh --phase 1
```

The runner rebuilds `//go/e2e/...` and `//go:ray_go_pkg_zip` in the same invocation before staging anything, so no staged artifact can come from an earlier tree. The `raygo`/`go_runtime.so` pair the drivers and workers actually load is the copy already synced into the environment (or pinned through `RAY_GO_RUNTIME_PATH`), not the one the build just produced, so after the build the runner also compares that pair against the just-built package and fails with the remedy when they differ. See [Running the fleet from zero to finished](#running-the-fleet-from-zero-to-finished) for the full setup and [Run matrix and pass criterion](#run-matrix-and-pass-criterion) for what each phase runs.

Manual form for a single driver (steps 1 and 2 stage the plugin and the driver into one directory; the plugin step applies to the 9 of the 12 drivers below that consume the userfuncs plugin, the others skip it):

```bash
# 1. Build the userfuncs plugin and the driver.
bazel build //go/e2e/userfuncs/plugin:userfuncs_plugin
bazel build //go/e2e/driver_app          # example; replace the target name per case

# 2. Stage the plugin and the driver into one directory. The built driver lives
#    at bazel-bin/go/e2e/<driver>/<driver>_/<driver>.
mkdir -p job_dir
cp bazel-bin/go/e2e/userfuncs/plugin/userfuncs.so job_dir/
cp bazel-bin/go/e2e/driver_app/driver_app_/driver_app job_dir/

# 3. Submit as a Ray job.
cd job_dir && ray job submit --working-dir . -- ./driver_app
```

| Driver | Build target | What it verifies |
|---|---|---|
| `driver_app` | `//go/e2e/driver_app` | Remote task submission, Free API, GetActorHandle; supports `--run-mode=local` local mode |
| `actor_integration_driver` | `//go/e2e/actor_integration_driver` | Actor lifecycle, instance isolation, void/multiple returns, named tasks, concurrency groups (`_ray_system` and `ActorCreator.WithConcurrencyGroups` declared group `cg_go_add` downlink + task-level routing), Exit/Kill, creation options (lifetime/async/maxPendingCalls), handle.kill instance method |
| `driver_parallel_actor` | `//go/e2e/driver_parallel_actor` | Parallel Actor concurrency-group model (aligned with Java `io.ray.api.parallelactor`): `NewParallelActor` creates N PARALLEL_INSTANCE_&lt;i&gt; concurrency groups, `GetInstance(i).Task(...).Remote()` instance routing, inter-instance state isolation, intra-instance state retention, and the inter-instance parallel / intra-instance serial concurrency semantics |
| `driver_call_python` | `//go/e2e/driver_call_python` | Go->Python cross-language function calls and actors (needs `test_go_call_python.py` in the same directory) |
| `driver_call_cpp` | `//go/e2e/driver_call_cpp` | Go->C++ cross-language calls: RemoteCpp/RemoteCppVoid/cross-language types (including struct)/C++ Actor + CppFunctionDescriptor (the C++ Worker must register the corresponding functions/Actor; see `driver_call_cpp/README.md`) |
| `runtime_context_driver` | `//go/e2e/runtime_context_driver` | Full RuntimeContext API and GetAvailableResourceIds/GetGpuIds |
| `global_state_accessor_driver` | `//go/e2e/global_state_accessor_driver` | GlobalStateAccessor production-implementation queries + actor list filtered by state (ALIVE/DEAD) pushed down |
| `runtime_env_driver` | `//go/e2e/runtime_env_driver` | Full user-side RuntimeEnv API flow + submission via WithRuntimeEnv verified to take effect in the worker + Autoscaler status-report loop ReportAutoscalingState->GetAutoscalerStatus |
| `driver_placement_group` | `//go/e2e/driver_placement_group` | Placement Group user API: lifecycle (create/wait/get/getByName/getAll/remove, state migrates to Removed after removal), tasks/Actor bound to bundles via `WithPlacementGroup` (the call.go main downlink path), option validation; `--run-mode=local` covers local change simulation and not-found queries |
| `driver_gcs_client` | `//go/e2e/driver_gcs_client` | User-side GCSClient extension: `GetGCSClient()` + PG queries x3 (including by-name and REMOVED after removal) + InternalKV write/read-back round trip + Node/Actor queries (including ALIVE state filtering); local mode returns not-supported |
| `driver_metrics` | `//go/e2e/driver_metrics` | User Metrics API: RegisterTagKey + Count/Gauge/Sum/Histogram register/Record/Unregister + handle rejection after unregister; local mode returns not-supported |
| `zero_copy_benchmark` | `//go/e2e/zero_copy_benchmark` | Cross-process zero-copy Get performance benchmark (local/cross, 4MB/64MB) |

## userfuncs shared function package

`userfuncs/` is the shared user-function package used by the Go-task/actor
drivers (9 of the 12 drivers), compiled to the `userfuncs.so` plugin that the
Go worker loads. Those drivers point the plugin out through JobConfig's
`code_search_path`, and the worker resolves function and actor descriptors
from it.

## Lockstep and the packaging hash gate

The raygo CLI and `go_runtime.so` must be deployed in lockstep: both are built
from the same Go tree, and the packaging step verifies the hash of the
`raygo`/`go_runtime.so` pair so a mismatched combination is rejected at package
time. Note that the packaging hash gate covers only the raygo/go_runtime.so
pair — aligning a `userfuncs.so` plugin with the runtime is a manual deployment
step (keep the plugin and the runtime built from the same tree).

Against a cluster whose image was built from a different tree, the runtime
must also be synced into every node, and a synced runtime is not durable: a
container restart restores the image's copy, so a stale `go_runtime.so` comes
back and every Go driver then dies with `plugin was built with a different
version of package ...`. Re-run the sync after any pod restart, and bake the
runtime into the image for a durable deployment.

```bash
# Sync the packaged runtime into a running node (repeat per node).
bazel run //go:gen_ray_go_pkg
kubectl -n <ns> cp python/ray/go/lib/go_runtime.so <pod>:/tmp/go_runtime.so.new
kubectl -n <ns> cp python/ray/go/cmd/raygo <pod>:/tmp/raygo.new
kubectl -n <ns> exec <pod> -- sh -c 'R=/path/to/site-packages/ray; \
  cp /tmp/go_runtime.so.new $R/go/lib/go_runtime.so; \
  cp /tmp/raygo.new $R/go/cmd/raygo; \
  chmod 755 $R/go/lib/go_runtime.so $R/go/cmd/raygo'
```

## Prerequisites

1. A local Ray cluster (`ray start --head`) — required for driver mode.
2. The `raygo` binary must exist (packaged under `python/ray/go/cmd/raygo` or on
   `PATH`). When present, the Go worker command is injected automatically; no
   extra environment variable is needed.
3. The `userfuncs.so` plugin must use an absolute path (`code_search_path`),
   otherwise the worker-side `os.Stat` fails and the functions are not
   registered.
4. `runtime_env_driver` depends on newer runtime capabilities (the RuntimeEnv API and the autoscaler status report), so it must run against the `raygo`/`go_runtime.so` pair built from this tree — otherwise a plugin version mismatch or a missing method causes a runtime failure. The [from-zero setup](#running-the-fleet-from-zero-to-finished) provides this via `PYTHONPATH`; if ray is installed instead, `bazel run //go:gen_ray_go_pkg` and sync `ray/go/lib/go_runtime.so` and `ray/go/cmd/raygo` into site-packages.

Optional: `RAY_ENABLE_GO_SETUP_WORKER=1` makes the worker startup preamble for
all languages (PYTHON/CPP/JAVA/GO) use the Go implementation
(`raygo setup_worker`) instead of the default Python `setup_worker.py`. It only
affects the setup implementation, not worker command injection.

## Running the fleet from zero to finished

The steps below take an unbuilt tree to a completed run. They need bazel (remote execution is fine), the `ray` CLI, and a Python 3.12 interpreter.

1. Build the fleet and the plugin.

```bash
bazel build //go/e2e/... //go/e2e/userfuncs/plugin:userfuncs_plugin
```

2. Package the Go runtime. `bazel run //go:gen_ray_go_pkg` extracts the `raygo` CLI and `go_runtime.so` into `python/ray/go/cmd` and `python/ray/go/lib`; `bazel run //:gen_ray_pkg` refreshes the core package (`_raylet.so` and the generated protos) under `python/ray/`. Both write into the source tree, so the runtime, the CLI and the drivers all come from the tree you just built.

```bash
bazel run //go:gen_ray_go_pkg
bazel run //:gen_ray_pkg
```

3. Make `import ray` resolve to this tree. The built `_raylet.so` is a CPython 3.12 extension — 3.10 fails on `Py_Version` and 3.11 fails on `PyUnstable_Code_NewWithPosOnly` — so a Python 3.12 interpreter is required. No install is needed: put this repository's `python/` on `PYTHONPATH` and make sure the 3.12 interpreter's `bin/` is first on `PATH`, so the `ray` CLI and the `python3` the runner invokes both come from that interpreter.

```bash
export PYTHONPATH="$PWD/python"     # run from the repository root
```

Then pin the Go runtime the fleet loads. `raygo` and the driver binaries resolve `go_runtime.so` through a multi-tier search that consults the Python installation first, so a `ray` left behind by an earlier `pip install --user` under `~/.local/lib/pythonX.Y/site-packages` shadows the runtime just built here and every Go driver fails with `plugin was built with a different version of package ...`. Pin the tree's runtime:

```bash
export RAY_GO_RUNTIME_PATH="$PWD/python/ray/go/lib/go_runtime.so"
```

The pin has to cover the cluster as well as the client. `ray start` passes its environment to the raylet, and the raylet passes it to the task workers it spawns, so exporting the variable before `ray start` gives the driver and its workers the same build. Exporting it only for `ray job submit` reaches the driver but not the workers, which then load the stale runtime; the two builds disagree on the wire and the driver dies with a SIGSEGV in the object-store release path.

4. Bring up a cluster or point at one. Phase 1 runs against a local head node; Phase 2 runs against an existing cluster selected with `RAY_ADDRESS` (or `--cluster-address`).

```bash
ray start --head                    # Phase 1
# or, for Phase 2 against an existing cluster:
export RAY_ADDRESS=<ray-address>    # the address printed by `ray start` / the cluster's dashboard
```

If a token already exists at `~/.ray/auth_token`, `ray start --head` enables token authentication for the cluster by default, and `ray status` or `ray job submit` run from a fresh shell is then rejected with `InvalidAuthToken`, because a client with `RAY_AUTH_MODE` unset defaults to auth mode `disabled` and sends no token. Either export `RAY_AUTH_MODE=token` for the session (the existing token file already matches the cluster), or opt out of token auth for a local run with `RAY_AUTH_MODE=disabled` before `ray start`.

5. Run the fleet.

```bash
bash go/e2e/run_fleet.sh --phase 1  # or --phase 2
```

6. Read the results. The runner writes one log and one `<log>.exit` record per driver into the results directory (default `./fleet-results-<UTC timestamp>`), plus `summary.txt`; the process exits 0 only when every selected driver passed.

```bash
cat fleet-results-*/summary.txt
```

Three things must come from the same tree for a run to be meaningful (see [Lockstep and the packaging hash gate](#lockstep-and-the-packaging-hash-gate)): `raygo` and `go_runtime.so`, whose pair hash the packaging step verifies so a mismatch is rejected at package time; the `userfuncs.so` plugin, which the packaging hash gate does not cover and so must be rebuilt with the same tree by hand; and the plugin path, which must be absolute — the drivers satisfy this by joining `os.Getwd()` with `userfuncs.so`, so the plugin is found in the job's working directory.

## Run matrix and pass criterion

The runner has two phases, selected with `--phase 1|2`. Phase 1 is the single-language fleet and runs against a local head node. Phase 2 is the cross-language set and runs against a remote cluster; its rows depend on artifacts and runtime capabilities the Phase 1 set does not. The run finishes with a load-tolerance re-run of the polling drivers at a raised deadline scale. Both phases stage the `userfuncs.so` plugin next to every row, including the standalone Python lane, whose `code_search_path` is the relative `./userfuncs.so` and which therefore needs the copy in its job directory as much as a Go driver does; only the Go driver binary is lane-specific and skipped there.

### Phase 1 — single-language fleet

Nine drivers, thirteen rows: four of the nine declare a `--run-mode=local` row in addition to their driver-mode row.

| Driver | Rows | What it verifies |
|---|---|---|
| `driver_app` | driver mode; `--run-mode=local` | remote task submission, Free API, GetActorHandle |
| `actor_integration_driver` | driver mode | actor lifecycle, isolation, Exit/Kill, creation options |
| `driver_parallel_actor` | driver mode | parallel actor instance routing and isolation |
| `runtime_context_driver` | driver mode | RuntimeContext and resource-ID API |
| `global_state_accessor_driver` | driver mode | GlobalStateAccessor queries + actor-state filter |
| `driver_placement_group` | driver mode; `--run-mode=local` | placement-group lifecycle, placement, validation |
| `driver_gcs_client` | driver mode; `--run-mode=local` | GCSClient extension: PG/KV/Node/Actor queries |
| `driver_metrics` | driver mode; `--run-mode=local` | user Metrics API register/Record/Unregister |
| `zero_copy_benchmark` | driver mode | zero-copy Get benchmark |

### Phase 2 — cross-language and dependency-bound

Four rows: three dependency-bound drivers submitted as Ray jobs, plus the standalone Python lane run directly on the head node with its interpreter.

| Driver | Row | Dependency |
|---|---|---|
| `driver_call_python` | driver mode | `test_go_call_python.py`, staged into the job directory by the runner |
| `driver_call_cpp` | driver mode | the C++ worker `bazel-bin/go/e2e/driver_call_cpp/cpp_xlang_worker.so`, staged in, and a C++ worker running to register the matching functions |
| `runtime_env_driver` | driver mode | the newer runtime (RuntimeEnv API + autoscaler status report) in the packaged `ray/go` |
| `test_python_call_go_simple` | direct | Python -> Go lane, run on the head node (`--ray-address`), not through `ray job submit` |

### The direct lane must run on a Ray node

The `direct` row runs its Python script with the local interpreter. That is correct only when the fleet runs on the same machine as the cluster head (the Phase 1 layout), because the script calls `ray.init(address=...)` and Ray's Python client accepts a GCS address (`<ip>:<port>`) or a Ray Client URL (`ray://`), not the dashboard HTTP URL that `ray job submit` takes. Against a remote cluster reached through a dashboard URL, pass the lane a GCS address that is routable from where it runs, or run it on a Ray node:

```bash
# On the head node, the local cluster needs no address at all:
kubectl -n <ns> cp go/e2e/test_python_call_go_simple.py <head-pod>:/tmp/lane/
kubectl -n <ns> cp bazel-bin/go/e2e/userfuncs/plugin/userfuncs.so <head-pod>:/tmp/lane/userfuncs.so
kubectl -n <ns> exec <head-pod> -- sh -c 'cd /tmp/lane && python3 test_python_call_go_simple.py'
```

The node that runs the lane also needs the tree's Python binding: `ray.cross_language.go_function` and the `Language.GO` / `GoFunctionDescriptor` support it uses live in `python/ray/cross_language.py` and `python/ray/_raylet.so`, so a ray installed from an older tree fails at import. Rebuild `bazel build //:python/ray/_raylet.so` and sync the resulting `_raylet.so` (plus `cross_language.py`) into that ray.

Phase 2 is followed by a load-tolerance re-run with the deadline scale raised, to show the polling drivers tolerate a loaded cluster. The six drivers that honour `RAY_GO_E2E_TIMEOUT_SCALE` are `actor_integration_driver`, `driver_app`, `driver_call_cpp`, `driver_gcs_client`, `driver_placement_group` and `runtime_context_driver`; five of them are Phase 1 rows and `driver_call_cpp` is a Phase 2 row, so the scaled re-run covers both phases. A row that passes only when scaled is the evidence the scale knob works; a row that fails even scaled is a real defect or an over-strict assertion, not a flake.

```bash
# Re-run every Phase 1 row with the deadlines scaled up.
bash go/e2e/run_fleet.sh --phase 1 --timeout-scale 3
# driver_call_cpp is the one polling driver that lives in Phase 2; scale that phase too.
bash go/e2e/run_fleet.sh --phase 2 --timeout-scale 3 --driver driver_call_cpp
```

See [Manual run form](#manual-run-form) for the semantics of the two deadline knobs.

### Pass criterion

Each row is compared against the criterion its own source defines, and the runner exits 0 only when every selected row passed.

1. The row must produce both an exit record (`<log>.exit`) and a log; a missing one is a hard failure naming the path, never a silent skip.
2. The driver's exit code must be 0.
3. For a no-argument row whose source carries the `=== Test... PASSED ===` convention, the number of `=== Test... PASSED ===` lines in the log must equal the driver-mode test count in the source — the `func test[A-Z]...` declarations minus the `func testLocal...` declarations, which only the `--run-mode=local` path calls.
4. Otherwise, the log must contain at least one success marker the source itself declares: the `=== Test... PASSED ===` convention, `✓`, `✅`, or `passed`.
5. Where the source declares no marker at all (the zero-copy benchmark reports a latency table), the exit code alone passes.

## Manual run form

These drivers are manual targets: build them with `bazel build //go/e2e/<target>`
and run them against a real cluster via `ray job submit`. They are not wired
into CI. The two pytest scripts (`test_go_call_python.py` and
`test_python_call_go_simple.py`) are manual real-machine lanes for
cross-language verification.

On a shared or loaded cluster the poll-loop deadlines can flake even though the behavior under test is correct. Export `RAY_GO_E2E_TIMEOUT_SCALE` to scale every poll deadline in the six polling drivers (listed in [Run matrix and pass criterion](#run-matrix-and-pass-criterion)) — e.g. `RAY_GO_E2E_TIMEOUT_SCALE=3 ray job submit ...`, or `--timeout-scale 3` through the runner — instead of editing the per-driver constants; values below 1.0 are ignored so an assertion deadline is never shortened. `global_state_accessor_driver` keeps its own validated override `GSA_ACTOR_STATE_TIMEOUT` (seconds) for the actor-state probe.

## Troubleshooting

**1. Build fails: dependency not found**
```
Make sure you have run: bazel run //go:gazelle
```

**2. userfuncs.so not found at runtime**
```
First build the plugin: bazel build //go/e2e/userfuncs/plugin:userfuncs_plugin
and copy the .so file into the driver's directory (uploaded via --working-dir)
```

**3. Worker reports "function not registered"**
```
code_search_path must be an absolute path; or use RAY_GO_USERFUNC_SO to point
at the absolute .so path
```

**4. Cannot connect to the Ray cluster**
```
- Confirm Ray is running: ray start --head --port=6379
- Check the environment variable: echo $RAY_ADDRESS
- Or test with driver_app's local mode: ./driver_app --run-mode=local
```

### Overriding the plugin search path

`raygo` resolves `go_runtime.so` from an explicit `RAY_GO_RUNTIME_PATH`
environment variable first, then falls back to a multi-tier search that can
consult the Python installation. A stale Go build under `~/.local` can shadow
the intended runtime and break every subcommand with a plugin-loadability
error; when in doubt, set `RAY_GO_RUNTIME_PATH` to the exact `go_runtime.so`
so the deployment is pinned (the packaging hash gate covers only the
raygo/go_runtime.so pair).
