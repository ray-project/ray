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

Every case is submitted to a local Ray cluster as a standalone driver binary.
The common flow (steps 1 and 3 apply to the drivers that consume the
userfuncs plugin — 9 of the 12 drivers below; the others skip them):

```bash
# 1. Build the userfuncs plugin
bazel build //go/e2e/userfuncs/plugin:userfuncs_plugin

# 2. Build the target driver
bazel build //go/e2e/driver_app          # example; replace the target name per case

# 3. Prepare the run directory (plugin and driver in the same directory)
mkdir -p job_dir
cp bazel-bin/go/e2e/userfuncs/plugin/userfuncs.so job_dir/
cp bazel-bin/go/e2e/driver_app_/driver_app job_dir/

# 4. Submit as a Ray job
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

## Prerequisites

1. A local Ray cluster (`ray start --head`) — required for driver mode.
2. The `raygo` binary must exist (packaged under `python/ray/go/cmd/raygo` or on
   `PATH`). When present, the Go worker command is injected automatically; no
   extra environment variable is needed.
3. The `userfuncs.so` plugin must use an absolute path (`code_search_path`),
   otherwise the worker-side `os.Stat` fails and the functions are not
   registered.
4. `runtime_env_driver` depends on newer runtime capabilities (the RuntimeEnv
   API and the autoscaler status report); site-packages must have the latest
   `ray/go/lib/go_runtime.so` and `ray/go/cmd/raygo` synced (`bazel run
   //go:gen_ray_go_pkg` then sync), otherwise the plugin version mismatch or a
   missing method causes a runtime failure.

Optional: `RAY_ENABLE_GO_SETUP_WORKER=1` makes the worker startup preamble for
all languages (PYTHON/CPP/JAVA/GO) use the Go implementation
(`raygo setup_worker`) instead of the default Python `setup_worker.py`. It only
affects the setup implementation, not worker command injection.

## Manual run form

These drivers are manual targets: build them with `bazel build //go/e2e/<target>`
and run them against a real cluster via `ray job submit`. They are not wired
into CI. The two pytest scripts (`test_go_call_python.py` and
`test_python_call_go_simple.py`) are manual real-machine lanes for
cross-language verification.

On a shared or loaded cluster the poll-loop deadlines can flake even though the
behavior under test is correct. Export `RAY_GO_E2E_TIMEOUT_SCALE` to scale every
poll deadline in `actor_integration_driver`, `driver_app`, `driver_call_cpp`,
`driver_gcs_client`, `driver_placement_group`, and `runtime_context_driver`
(e.g. `RAY_GO_E2E_TIMEOUT_SCALE=3 ray job submit ...`) instead of editing the
per-driver constants; values below 1.0 are ignored so an assertion deadline is
never shortened. `global_state_accessor_driver` keeps its own validated override
`GSA_ACTOR_STATE_TIMEOUT` (seconds) for the actor-state probe.

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
