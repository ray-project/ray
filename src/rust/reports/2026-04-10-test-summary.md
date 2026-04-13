# Ray Build & Test Summary

## Step 1: Clone & Branch
- Cloned `ray-project/ray` to `/Users/istoica/src/cc-to-rust/ray` (shallow clone)
- Created branch `cc-to-rust` and pushed to `origin`
- Branch URL: https://github.com/ray-project/ray/tree/cc-to-rust

## Step 2: Build
- Built Ray 3.0.0.dev0 from source (editable install via Bazel + pip)
- Dashboard built via `npm ci && npm run build`
- Verified: `ray --version` works, `ray.init()` starts local cluster with 12 CPUs / 45GB RAM
- Platform: macOS Darwin 23.6.0, ARM64 (Apple M-series), Python 3.11.3, Bazel 7.5.0

## Step 3: Test Results

| Test Suite | Passed | Failed | Errors | Skipped | Notes |
|---|---|---|---|---|---|
| **C++ (Bazel)** | 50 | 4 | 111 build failures | 8 (Linux-only) | 111 targets need Redis or Linux |
| **Core Python** | ~128+ | ~17 | ~24 collection | ~2 | 274 test files; many need cloud infra |
| **Ray Data** | ~905 | ~107 | ~527 | ~181 | Errors mostly from BigQuery/Snowflake/etc. deps |
| **Ray Tune** | ~562 | ~235 | ~102 | ~237 | XGBoost version mismatches |
| **Ray Train** | ~31 | ~20 | ~1 | ~3 | |
| **Ray Serve** | 15+ | 0 | 41 | 0 | 5,788 tests total; resource-intensive |
| **Remaining (DAG, Dashboard, Air, Autoscaler)** | 2+ | 1 | 17 | 1 | |
| **RLlib** | - | - | all | - | ~~Blocked by opentelemetry dep conflict~~ **Import fixed** (2026-04-11) |

## Key Issues

- **Dependency conflicts**: Ray's test suite has ~200+ Python deps with version conflicts. The compiled requirements file (`requirements_compiled_py3.11.txt`) has packages unavailable on macOS ARM.
- **Resource-intensive**: Each test starts/stops Ray clusters. Running suites in parallel causes OOM.
- **Cloud/GPU deps**: Many tests require GCP/AWS/Azure SDKs, Redis, GPUs (CUDA), or Kubernetes -- these are skipped/errored locally.
- ~~**OpenTelemetry conflict**: Mixed `opentelemetry-*` versions prevent Ray startup for some suites (notably RLlib).~~ **FIXED** (2026-04-11): Upgraded opentelemetry-proto 1.27.0->1.41.0, protobuf 4.25.9->6.33.6, downgraded wrapt 2.1.2->1.17.3. All modules now import correctly including RLlib.
- **C++ build failures**: 111 out of 165 C++ test targets failed to build, primarily due to missing Redis server and Linux-only platform constraints (cgroup, memory monitor, mutable object tests).

## C++ Test Details

### Passed (50 targets)
Targets in `src/ray/util/tests/`, `src/ray/common/scheduling/tests/`, `src/ray/common/tests/`, `src/ray/observability/tests/`, `src/ray/stats/tests/`, `bazel/tests/cpp/` all passed.

### Failed at Runtime (4 targets -> 3 after fix)
- `//cpp:cluster_mode_test` -- FAILED in 35.6s (needs full Ray cluster in sandbox)
- `//cpp:metric_example` -- FAILED in 40.2s (needs full Ray cluster in sandbox)
- `//cpp:simple_kv_store` -- FAILED in 35.6s (needs full Ray cluster in sandbox)
- ~~`//src/ray/util/tests:signal_test` -- FAILED in 0.7s~~ **FIXED** (2026-04-11): Added `setpgid(0,0)` + `waitpid()` to isolate forked child process groups on macOS. Now PASSED in 1.4s.

### Skipped (Linux-only, 8 targets)
- `memory_monitor_utils_test`, `pressure_memory_monitor_test`, `threshold_memory_monitor_test`, `event_memory_monitor_test`
- `sysfs_cgroup_driver_test`, `sysfs_cgroup_driver_integration_test`
- `mutable_object_test`, `chaos_redis_store_client_test`

## Log File Locations
- C++ tests: `/tmp/ray_cpp_tests.log`
- Core Python: `/tmp/ray_core_final.log`
- Ray Data: `/tmp/ray_data_tests.log`
- Ray Tune: `/tmp/ray_tune_tests.log`
- Ray Train: `/tmp/ray_train_tests.log`
- Ray Serve: `/tmp/ray_serve_final2.log`
- Remaining: `/tmp/ray_remaining_final.log`
- RLlib: `/tmp/ray_rllib_final.log`
