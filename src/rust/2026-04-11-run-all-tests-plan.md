# 2026-04-11 — Plan to Run All Ray Tests

## Table of Contents
1. [Environment Setup](#1-environment-setup)
2. [C++ Tests (Bazel)](#2-c-tests-bazel)
3. [Core Python Tests](#3-core-python-tests)
4. [Ray Data Tests](#4-ray-data-tests)
5. [Ray Tune Tests](#5-ray-tune-tests)
6. [Ray Train Tests](#6-ray-train-tests)
7. [Ray Serve Tests](#7-ray-serve-tests)
8. [RLlib Tests](#8-rllib-tests)
9. [Ray DAG Tests](#9-ray-dag-tests)
10. [Ray Dashboard Tests](#10-ray-dashboard-tests)
11. [Ray Air Tests](#11-ray-air-tests)
12. [Ray Autoscaler Tests](#12-ray-autoscaler-tests)
13. [Ray Common Tests](#13-ray-common-tests)
14. [Ray LLM Tests](#14-ray-llm-tests)
15. [Release / Integration Tests](#15-release--integration-tests)
16. [Result Aggregation](#16-result-aggregation)
17. [Known Limitations on macOS](#17-known-limitations-on-macos)

---

## 1. Environment Setup

### 1.1 Prerequisites Already in Place
- **Bazel**: 7.5.0 (matches `.bazelversion`)
- **Python**: 3.11.3 via Homebrew
- **Virtual env**: `.venv/` with Ray 3.0.0.dev0 editable install
- **Platform**: macOS Darwin 23.6.0, ARM64 (Apple M-series)

### 1.2 Install Missing Test Dependencies

The main test requirements file is `python/requirements/test-requirements.txt` (146 packages).
Current `.venv` has most but not all of them. Install the full set:

```bash
cd /Users/istoica/src/cc-to-rust/ray

# Install the full test requirements
.venv/bin/pip install -r python/requirements/test-requirements.txt 2>&1 | tee /tmp/ray_test_deps_install.log

# Install component-specific extras
.venv/bin/pip install -e "python/[all]" 2>&1 | tee -a /tmp/ray_test_deps_install.log

# Install ML/DL CPU requirements (no GPU on macOS)
.venv/bin/pip install -r python/requirements/ml/dl-cpu-requirements.txt 2>&1 | tee -a /tmp/ray_test_deps_install.log

# Install serve requirements
.venv/bin/pip install -r python/requirements/serve/serve-requirements.txt 2>&1 | tee -a /tmp/ray_test_deps_install.log

# Verify no import failures for key modules
.venv/bin/python -c "
for m in ['ray','ray.data','ray.tune','ray.train','ray.serve','ray.rllib',
          'ray.dag','ray.dashboard','ray.air','ray._common']:
    try: __import__(m); print(f'OK: {m}')
    except Exception as e: print(f'FAIL: {m} -> {e}')
"
```

### 1.3 Fix Known Dependency Conflicts Before Running

These were already applied on 2026-04-11 but verify they're still in place:

```bash
# Fix opentelemetry/wrapt conflict
.venv/bin/pip install "wrapt>=1.14,<2.0"

# Fix protobuf conflict (grpcio-status vs opentelemetry-proto)
.venv/bin/pip install "opentelemetry-proto>=1.34" "protobuf>=6.31.1,<7"

# Verify
.venv/bin/pip check 2>&1 | head -20
```

### 1.4 Create Results Directory

```bash
mkdir -p /tmp/ray_test_results/2026-04-11
export RAY_TEST_RESULTS="/tmp/ray_test_results/2026-04-11"
```

### 1.5 Environment Variables

Set these before running any tests:

```bash
export RAY_ENABLE_WINDOWS_OR_OSX_CLUSTER=1
export RAY_USAGE_STATS_REPORT_URL="http://127.0.0.1:8000"
export RAY_USE_RANDOM_PORTS=1
export LC_ALL=en_US.UTF-8
export LANG=en_US.UTF-8
export CI=true
```

### 1.6 Clean Up Between Test Suites

Between each major suite, kill stale Ray processes and clean up:

```bash
ray stop --force 2>/dev/null || true
pkill -9 -f raylet 2>/dev/null || true
pkill -9 -f gcs_server 2>/dev/null || true
pkill -9 -f "python -m pytest" 2>/dev/null || true
sleep 2
```

---

## 2. C++ Tests (Bazel)

**Total targets**: ~204 across 33 BUILD.bazel files
**Expected pass rate**: ~50 targets pass, ~111 fail to build (Redis/Linux deps), 4 runtime failures (3 cluster-mode + 1 fixed signal_test)

### 2.1 Run All C++ Unit Tests (non-integration)

These are hermetic tests that don't need a running Ray cluster.
Run module-by-module to isolate failures:

```bash
# ---- Utility tests (30 targets) ----
bazel test //src/ray/util/tests/... \
  --test_output=errors \
  --test_tag_filters="-no_windows" \
  --jobs=4 \
  2>&1 | tee $RAY_TEST_RESULTS/cpp_util_tests.log

# ---- Common tests (15 targets) ----
bazel test //src/ray/common/tests/... \
  --test_output=errors \
  2>&1 | tee $RAY_TEST_RESULTS/cpp_common_tests.log

# ---- Common scheduling tests (7 targets) ----
bazel test //src/ray/common/scheduling/tests/... \
  --test_output=errors \
  2>&1 | tee $RAY_TEST_RESULTS/cpp_scheduling_tests.log

# ---- GCS tests (22 targets, some need Redis) ----
bazel test //src/ray/gcs/tests/... \
  --test_output=errors \
  2>&1 | tee $RAY_TEST_RESULTS/cpp_gcs_tests.log

# ---- GCS actor tests (4 targets) ----
bazel test //src/ray/gcs/actor/tests/... \
  --test_output=errors \
  2>&1 | tee $RAY_TEST_RESULTS/cpp_gcs_actor_tests.log

# ---- GCS store client tests (7 targets, some need Redis) ----
bazel test //src/ray/gcs/store_client/tests/... \
  --test_output=errors \
  2>&1 | tee $RAY_TEST_RESULTS/cpp_gcs_store_tests.log

# ---- GCS postable tests (3 targets) ----
bazel test //src/ray/gcs/postable/tests/... \
  --test_output=errors \
  2>&1 | tee $RAY_TEST_RESULTS/cpp_gcs_postable_tests.log

# ---- GCS RPC client tests (6 targets, some need Redis) ----
bazel test //src/ray/gcs_rpc_client/tests/... \
  --test_output=errors \
  2>&1 | tee $RAY_TEST_RESULTS/cpp_gcs_rpc_tests.log

# ---- Core worker tests (13 targets) ----
bazel test //src/ray/core_worker/tests/... \
  --test_output=errors \
  2>&1 | tee $RAY_TEST_RESULTS/cpp_core_worker_tests.log

# ---- Core worker task submission tests (6 targets) ----
bazel test //src/ray/core_worker/task_submission/tests/... \
  --test_output=errors \
  2>&1 | tee $RAY_TEST_RESULTS/cpp_task_submission_tests.log

# ---- Core worker task execution tests (7 targets) ----
bazel test //src/ray/core_worker/task_execution/tests/... \
  --test_output=errors \
  2>&1 | tee $RAY_TEST_RESULTS/cpp_task_execution_tests.log

# ---- Core worker actor management tests (3 targets) ----
bazel test //src/ray/core_worker/actor_management/tests/... \
  --test_output=errors \
  2>&1 | tee $RAY_TEST_RESULTS/cpp_actor_mgmt_tests.log

# ---- Raylet tests (11 targets) ----
bazel test //src/ray/raylet/tests/... \
  --test_output=errors \
  2>&1 | tee $RAY_TEST_RESULTS/cpp_raylet_tests.log

# ---- Raylet scheduling tests (7 targets) ----
bazel test //src/ray/raylet/scheduling/tests/... \
  --test_output=errors \
  2>&1 | tee $RAY_TEST_RESULTS/cpp_raylet_scheduling_tests.log

# ---- Raylet scheduling policy tests (2 targets) ----
bazel test //src/ray/raylet/scheduling/policy/tests/... \
  --test_output=errors \
  2>&1 | tee $RAY_TEST_RESULTS/cpp_scheduling_policy_tests.log

# ---- Object manager tests (9 targets) ----
bazel test //src/ray/object_manager/tests/... \
  --test_output=errors \
  2>&1 | tee $RAY_TEST_RESULTS/cpp_objmgr_tests.log

# ---- Plasma tests (6 targets) ----
bazel test //src/ray/object_manager/plasma/tests/... \
  --test_output=errors \
  2>&1 | tee $RAY_TEST_RESULTS/cpp_plasma_tests.log

# ---- Stats tests (4 targets) ----
bazel test //src/ray/stats/tests/... \
  --test_output=errors \
  2>&1 | tee $RAY_TEST_RESULTS/cpp_stats_tests.log

# ---- Observability tests (7 targets) ----
bazel test //src/ray/observability/tests/... \
  --test_output=errors \
  2>&1 | tee $RAY_TEST_RESULTS/cpp_observability_tests.log

# ---- PubSub tests (5 targets) ----
bazel test //src/ray/pubsub/tests/... \
  --test_output=errors \
  2>&1 | tee $RAY_TEST_RESULTS/cpp_pubsub_tests.log

# ---- RPC tests (4 targets) ----
bazel test //src/ray/rpc/tests/... \
  --test_output=errors \
  2>&1 | tee $RAY_TEST_RESULTS/cpp_rpc_tests.log

# ---- RPC auth tests (4 targets) ----
bazel test //src/ray/rpc/authentication/tests/... \
  --test_output=errors \
  2>&1 | tee $RAY_TEST_RESULTS/cpp_rpc_auth_tests.log

# ---- Ray syncer tests (2 targets) ----
bazel test //src/ray/ray_syncer/tests/... \
  --test_output=errors \
  2>&1 | tee $RAY_TEST_RESULTS/cpp_syncer_tests.log

# ---- Raylet IPC client tests (2 targets) ----
bazel test //src/ray/raylet_ipc_client/tests/... \
  --test_output=errors \
  2>&1 | tee $RAY_TEST_RESULTS/cpp_ipc_client_tests.log

# ---- Core worker RPC client tests (2 targets) ----
bazel test //src/ray/core_worker_rpc_client/tests/... \
  --test_output=errors \
  2>&1 | tee $RAY_TEST_RESULTS/cpp_cw_rpc_tests.log

# ---- Raylet RPC client tests (1 target) ----
bazel test //src/ray/raylet_rpc_client/tests/... \
  --test_output=errors \
  2>&1 | tee $RAY_TEST_RESULTS/cpp_raylet_rpc_tests.log

# ---- Cgroup tests (3 targets, Linux-only — expect all to skip/fail) ----
bazel test //src/ray/common/cgroup2/tests/... \
  --test_output=errors \
  2>&1 | tee $RAY_TEST_RESULTS/cpp_cgroup_tests.log

# ---- Cgroup integration tests (2 targets, Linux-only) ----
bazel test //src/ray/common/cgroup2/integration_tests/... \
  --test_output=errors \
  2>&1 | tee $RAY_TEST_RESULTS/cpp_cgroup_integration_tests.log

# ---- Internal util tests (2 targets) ----
bazel test //src/ray/util/internal/tests/... \
  --test_output=errors \
  2>&1 | tee $RAY_TEST_RESULTS/cpp_util_internal_tests.log

# ---- Bazel infra test (1 target) ----
bazel test //bazel/tests/cpp/... \
  --test_output=errors \
  2>&1 | tee $RAY_TEST_RESULTS/cpp_bazel_infra_tests.log
```

### 2.2 Run C++ API / Integration Tests

These require a running Ray cluster. They will likely fail in the Bazel sandbox:

```bash
# C++ API tests (5 targets) — filter out macOS-incompatible
bazel test //cpp:all \
  --test_output=errors \
  --test_strategy=exclusive \
  --test_tag_filters="-no_macos" \
  2>&1 | tee $RAY_TEST_RESULTS/cpp_api_tests.log
```

### 2.3 Shotgun: Run Everything at Once

As an alternative to the per-module approach, run all C++ in one go:

```bash
bazel test //src/ray/... //cpp:all //bazel/tests/cpp/... \
  --test_output=errors \
  --keep_going \
  --jobs=4 \
  --test_tag_filters="-no_macos" \
  2>&1 | tee $RAY_TEST_RESULTS/cpp_all_tests.log
```

**Estimated time**: 15-30 minutes (build + test)

---

## 3. Core Python Tests

**Location**: `python/ray/tests/` — **~330 test files**
**Notes**: These are the largest suite. Tests start/stop Ray clusters, so they're resource-heavy. Run sequentially, not in parallel.

### 3.1 Via Bazel (how CI does it)

The CI groups tests by size tags. We replicate that approach:

```bash
# --- Small tests ---
bazel query 'attr(tags, "small_size_python_tests", tests(//python/ray/tests/...))' 2>/dev/null | \
  xargs bazel test --config=ci \
    --test_output=errors \
    --jobs=1 \
    --test_timeout=300 \
    2>&1 | tee $RAY_TEST_RESULTS/py_core_small.log

# --- Medium tests (shard 0) ---
bazel query 'attr(tags, "medium_size_python_tests_shard_0", tests(//python/ray/tests/...))' 2>/dev/null | \
  xargs bazel test --config=ci \
    --test_output=errors \
    --jobs=1 \
    --test_timeout=600 \
    2>&1 | tee $RAY_TEST_RESULTS/py_core_medium_0.log

# --- Medium tests (shard 1) ---
bazel query 'attr(tags, "medium_size_python_tests_shard_1", tests(//python/ray/tests/...))' 2>/dev/null | \
  xargs bazel test --config=ci \
    --test_output=errors \
    --jobs=1 \
    --test_timeout=600 \
    2>&1 | tee $RAY_TEST_RESULTS/py_core_medium_1.log

# --- Large tests (shard 0) ---
bazel query 'attr(tags, "large_size_python_tests_shard_0", tests(//python/ray/tests/...))' 2>/dev/null | \
  xargs bazel test --config=ci \
    --test_output=errors \
    --jobs=1 \
    --test_timeout=900 \
    2>&1 | tee $RAY_TEST_RESULTS/py_core_large_0.log

# --- Large tests (shard 1) ---
bazel query 'attr(tags, "large_size_python_tests_shard_1", tests(//python/ray/tests/...))' 2>/dev/null | \
  xargs bazel test --config=ci \
    --test_output=errors \
    --jobs=1 \
    --test_timeout=900 \
    2>&1 | tee $RAY_TEST_RESULTS/py_core_large_1.log

# --- Large tests (shard 2) ---
bazel query 'attr(tags, "large_size_python_tests_shard_2", tests(//python/ray/tests/...))' 2>/dev/null | \
  xargs bazel test --config=ci \
    --test_output=errors \
    --jobs=1 \
    --test_timeout=900 \
    2>&1 | tee $RAY_TEST_RESULTS/py_core_large_2.log

# --- Ray Client tests ---
bazel query 'attr(tags, "ray_client", tests(//python/ray/tests/...))' 2>/dev/null | \
  xargs bazel test --config=ci \
    --test_output=errors \
    --jobs=1 \
    --test_timeout=600 \
    2>&1 | tee $RAY_TEST_RESULTS/py_core_ray_client.log
```

### 3.2 Via pytest (direct, faster for local dev)

```bash
# Run all core tests via pytest (slower but easier to filter)
.venv/bin/python -m pytest python/ray/tests/ \
  -v --tb=short \
  --timeout=300 \
  -x \
  --ignore=python/ray/tests/kuberay \
  --ignore=python/ray/tests/spark \
  --ignore=python/ray/tests/runtime_env_container \
  -q 2>&1 | tee $RAY_TEST_RESULTS/py_core_pytest.log
```

**Estimated time**: 2-6 hours (330 test files, each starting/stopping Ray)

---

## 4. Ray Data Tests

**Location**: `python/ray/data/tests/` — **~207 test files**
**Dependencies**: pyarrow, pandas, polars, numpy

```bash
# Via Bazel
bazel test //python/ray/data/tests/... \
  --config=ci \
  --test_output=errors \
  --jobs=1 \
  --test_timeout=600 \
  --test_tag_filters="-data_integration,-doctest,-data_non_parallel,-needs_credentials,-tensorflow_datasets,-cudf" \
  --keep_going \
  2>&1 | tee $RAY_TEST_RESULTS/py_data_tests.log

# Via pytest (alternative)
.venv/bin/python -m pytest python/ray/data/tests/ \
  -v --tb=short \
  --timeout=300 \
  -q \
  --ignore=python/ray/data/tests/test_bigquery.py \
  --ignore=python/ray/data/tests/test_snowflake.py \
  --ignore=python/ray/data/tests/test_databricks_unity_catalog_datasource.py \
  -k "not bigquery and not snowflake and not gcs_filesystem and not s3_filesystem" \
  2>&1 | tee $RAY_TEST_RESULTS/py_data_pytest.log
```

**Estimated time**: 1-3 hours

---

## 5. Ray Tune Tests

**Location**: `python/ray/tune/tests/` — **~55 test files**
**Dependencies**: tensorboardX, optuna, hyperopt, scikit-learn

```bash
# Via Bazel
bazel test //python/ray/tune/tests/... \
  --config=ci \
  --test_output=errors \
  --jobs=1 \
  --test_timeout=600 \
  --keep_going \
  2>&1 | tee $RAY_TEST_RESULTS/py_tune_tests.log

# Via pytest
.venv/bin/python -m pytest python/ray/tune/tests/ \
  -v --tb=short \
  --timeout=300 \
  -q \
  2>&1 | tee $RAY_TEST_RESULTS/py_tune_pytest.log
```

**Estimated time**: 1-2 hours

---

## 6. Ray Train Tests

**Location**: `python/ray/train/tests/` — **~49 test files**
**Dependencies**: torch, tensorflow, xgboost, lightgbm

```bash
# Via Bazel
bazel test //python/ray/train/tests/... \
  --config=ci \
  --test_output=errors \
  --jobs=1 \
  --test_timeout=600 \
  --test_tag_filters="-gpu,-multi_gpu" \
  --keep_going \
  2>&1 | tee $RAY_TEST_RESULTS/py_train_tests.log

# Via pytest
.venv/bin/python -m pytest python/ray/train/tests/ \
  -v --tb=short \
  --timeout=300 \
  -q \
  -k "not gpu and not multi_gpu" \
  2>&1 | tee $RAY_TEST_RESULTS/py_train_pytest.log
```

**Estimated time**: 30-60 minutes

---

## 7. Ray Serve Tests

**Location**: `python/ray/serve/tests/` — **~145 test files, ~5,788 individual tests**
**Dependencies**: fastapi, uvicorn, starlette, httpx, aiohttp

```bash
# Via Bazel
bazel test //python/ray/serve/tests/... \
  --config=ci \
  --test_output=errors \
  --jobs=1 \
  --test_timeout=600 \
  --test_tag_filters="-post_wheel_build,-gpu" \
  --keep_going \
  2>&1 | tee $RAY_TEST_RESULTS/py_serve_tests.log

# Via pytest (unit tests first, they're fast)
.venv/bin/python -m pytest python/ray/serve/tests/unit/ \
  -v --tb=short \
  --timeout=180 \
  -q \
  2>&1 | tee $RAY_TEST_RESULTS/py_serve_unit_pytest.log

# Serve integration tests
.venv/bin/python -m pytest python/ray/serve/tests/ \
  -v --tb=short \
  --timeout=300 \
  -q \
  --ignore=python/ray/serve/tests/unit \
  2>&1 | tee $RAY_TEST_RESULTS/py_serve_integration_pytest.log
```

**Estimated time**: 2-4 hours

---

## 8. RLlib Tests

**Location**: `rllib/**/tests/` — **~131 test files** (distributed across subdirectories)
**Dependencies**: gymnasium, dm_tree, scipy, lz4, torch
**Note**: RLlib import was previously blocked by opentelemetry conflict — now fixed.

```bash
# Via pytest — run by subdirectory to isolate failures

# Algorithm tests
.venv/bin/python -m pytest rllib/algorithms/tests/ \
  -v --tb=short --timeout=300 -q \
  -k "not atari and not ale_py" \
  2>&1 | tee $RAY_TEST_RESULTS/py_rllib_algorithm_tests.log

# Algorithm-specific tests (PPO, DQN, SAC, etc.)
for algo in ppo dqn sac appo impala bc cql marwil dreamerv3 tqc; do
  .venv/bin/python -m pytest rllib/algorithms/$algo/tests/ \
    -v --tb=short --timeout=600 -q \
    2>&1 | tee $RAY_TEST_RESULTS/py_rllib_${algo}_tests.log
done

# Core model tests
.venv/bin/python -m pytest rllib/core/models/tests/ \
  -v --tb=short --timeout=300 -q \
  2>&1 | tee $RAY_TEST_RESULTS/py_rllib_models_tests.log

# Core learner tests
.venv/bin/python -m pytest rllib/core/learner/tests/ \
  -v --tb=short --timeout=300 -q \
  2>&1 | tee $RAY_TEST_RESULTS/py_rllib_learner_tests.log

# RL module tests
.venv/bin/python -m pytest rllib/core/rl_module/tests/ \
  -v --tb=short --timeout=300 -q \
  2>&1 | tee $RAY_TEST_RESULTS/py_rllib_rl_module_tests.log

# Env runner tests
.venv/bin/python -m pytest rllib/env/tests/ \
  -v --tb=short --timeout=300 -q \
  2>&1 | tee $RAY_TEST_RESULTS/py_rllib_env_tests.log

# Policy tests
.venv/bin/python -m pytest rllib/policy/tests/ \
  -v --tb=short --timeout=300 -q \
  2>&1 | tee $RAY_TEST_RESULTS/py_rllib_policy_tests.log

# Evaluation tests (old API)
.venv/bin/python -m pytest rllib/evaluation/tests/ \
  -v --tb=short --timeout=300 -q \
  2>&1 | tee $RAY_TEST_RESULTS/py_rllib_evaluation_tests.log

# Offline RL tests
.venv/bin/python -m pytest rllib/offline/tests/ \
  -v --tb=short --timeout=300 -q \
  2>&1 | tee $RAY_TEST_RESULTS/py_rllib_offline_tests.log

# Callback tests
.venv/bin/python -m pytest rllib/callbacks/tests/ \
  -v --tb=short --timeout=300 -q \
  2>&1 | tee $RAY_TEST_RESULTS/py_rllib_callbacks_tests.log

# Utils tests
.venv/bin/python -m pytest rllib/utils/ -k "test_" \
  -v --tb=short --timeout=300 -q \
  2>&1 | tee $RAY_TEST_RESULTS/py_rllib_utils_tests.log
```

**Estimated time**: 2-4 hours

---

## 9. Ray DAG Tests

**Location**: `python/ray/dag/tests/` — **~18 test files**

```bash
# Via Bazel
bazel test //python/ray/dag/tests/... \
  --config=ci \
  --test_output=errors \
  --jobs=1 \
  --test_timeout=600 \
  --keep_going \
  2>&1 | tee $RAY_TEST_RESULTS/py_dag_tests.log

# Via pytest
.venv/bin/python -m pytest python/ray/dag/tests/ \
  -v --tb=short --timeout=300 -q \
  2>&1 | tee $RAY_TEST_RESULTS/py_dag_pytest.log
```

**Estimated time**: 15-30 minutes

---

## 10. Ray Dashboard Tests

**Location**: `python/ray/dashboard/tests/` — **~5 test files**

```bash
# Via Bazel (excludes serve/rllib dependencies)
bazel test //python/ray/dashboard/... \
  --config=ci \
  --test_output=errors \
  --dynamic_mode=off \
  --jobs=1 \
  --test_timeout=600 \
  --test_tag_filters="-post_wheel_build" \
  --keep_going \
  -- -python/ray/serve/... -rllib/... \
  2>&1 | tee $RAY_TEST_RESULTS/py_dashboard_tests.log

# Via pytest
.venv/bin/python -m pytest python/ray/dashboard/tests/ \
  -v --tb=short --timeout=300 -q \
  2>&1 | tee $RAY_TEST_RESULTS/py_dashboard_pytest.log
```

**Estimated time**: 10-20 minutes

---

## 11. Ray Air Tests

**Location**: `python/ray/air/tests/` — **~20 test files**

```bash
# Via Bazel
bazel test //python/ray/air/tests/... \
  --config=ci \
  --test_output=errors \
  --jobs=1 \
  --test_timeout=600 \
  --keep_going \
  2>&1 | tee $RAY_TEST_RESULTS/py_air_tests.log

# Via pytest
.venv/bin/python -m pytest python/ray/air/tests/ \
  -v --tb=short --timeout=300 -q \
  2>&1 | tee $RAY_TEST_RESULTS/py_air_pytest.log
```

**Estimated time**: 15-30 minutes

---

## 12. Ray Autoscaler Tests

**Location**: `python/ray/autoscaler/` (tests spread across subdirectories)

```bash
# Via Bazel
bazel test //python/ray/autoscaler/v2/tests/... \
  --config=ci \
  --test_output=errors \
  --jobs=1 \
  --test_timeout=600 \
  --keep_going \
  2>&1 | tee $RAY_TEST_RESULTS/py_autoscaler_tests.log

# Via pytest
.venv/bin/python -m pytest python/ray/autoscaler/ -k "test_" \
  -v --tb=short --timeout=300 -q \
  2>&1 | tee $RAY_TEST_RESULTS/py_autoscaler_pytest.log
```

**Estimated time**: 10-20 minutes

---

## 13. Ray Common Tests

**Location**: `python/ray/_common/tests/` — **~13 test files**

```bash
# Via Bazel
bazel test //python/ray/_common/tests/... \
  --config=ci \
  --test_output=errors \
  --jobs=1 \
  --test_timeout=300 \
  --keep_going \
  2>&1 | tee $RAY_TEST_RESULTS/py_common_tests.log

# Via pytest
.venv/bin/python -m pytest python/ray/_common/tests/ \
  -v --tb=short --timeout=180 -q \
  2>&1 | tee $RAY_TEST_RESULTS/py_common_pytest.log
```

**Estimated time**: 10-20 minutes

---

## 14. Ray LLM Tests

**Location**: `python/ray/llm/tests/` (if present)
**Dependencies**: vLLM, transformers — likely not available on macOS ARM (requires CUDA)

```bash
# Check if tests exist
ls python/ray/llm/tests/test_*.py 2>/dev/null

# If they exist, try to collect them
.venv/bin/python -m pytest python/ray/llm/tests/ --collect-only -q \
  2>&1 | tee $RAY_TEST_RESULTS/py_llm_collect.log

# Run whatever is collectable (expect most to skip without GPU)
.venv/bin/python -m pytest python/ray/llm/tests/ \
  -v --tb=short --timeout=300 -q \
  2>&1 | tee $RAY_TEST_RESULTS/py_llm_pytest.log
```

**Estimated time**: 5-15 minutes (most will skip/error on macOS)

---

## 15. Release / Integration Tests

**Location**: `release/` — **~84 test files**
**Note**: These are heavy integration/performance tests. Most require cloud infrastructure (AWS/GCP), multi-node clusters, or GPUs. Expect the majority to fail locally.

```bash
# Collect available tests
find release/ -name "test_*.py" -o -name "*_test.py" | head -20

# Run what's available (will be heavily filtered by missing deps)
.venv/bin/python -m pytest release/ \
  -v --tb=short --timeout=600 -q \
  --ignore=release/nightly_tests \
  --ignore=release/long_running_tests \
  -k "not aws and not gcp and not kubernetes" \
  2>&1 | tee $RAY_TEST_RESULTS/py_release_pytest.log
```

**Estimated time**: Variable (most will fail/skip)

---

## 16. Result Aggregation

After all suites complete, aggregate results:

```bash
echo "====== RAY TEST RESULTS SUMMARY ======" > $RAY_TEST_RESULTS/SUMMARY.md
echo "Date: $(date)" >> $RAY_TEST_RESULTS/SUMMARY.md
echo "" >> $RAY_TEST_RESULTS/SUMMARY.md

# C++ results
echo "## C++ Tests (Bazel)" >> $RAY_TEST_RESULTS/SUMMARY.md
for f in $RAY_TEST_RESULTS/cpp_*.log; do
  suite=$(basename $f .log)
  passed=$(grep -c "PASSED" $f 2>/dev/null || echo 0)
  failed=$(grep -c "FAILED" $f 2>/dev/null || echo 0)
  build_fail=$(grep -c "FAILED TO BUILD" $f 2>/dev/null || echo 0)
  echo "- **$suite**: $passed passed, $failed failed, $build_fail build failures" >> $RAY_TEST_RESULTS/SUMMARY.md
done

echo "" >> $RAY_TEST_RESULTS/SUMMARY.md

# Python results
echo "## Python Tests" >> $RAY_TEST_RESULTS/SUMMARY.md
for f in $RAY_TEST_RESULTS/py_*.log; do
  suite=$(basename $f .log)
  # Extract pytest summary line
  summary=$(grep -E "^(FAILED|PASSED|ERROR|=)" $f 2>/dev/null | tail -1)
  echo "- **$suite**: $summary" >> $RAY_TEST_RESULTS/SUMMARY.md
done

cat $RAY_TEST_RESULTS/SUMMARY.md
```

### Copy Final Results to Project

```bash
cp $RAY_TEST_RESULTS/SUMMARY.md /Users/istoica/src/cc-to-rust/ray/src/rust/2026-04-11-test-results-summary.md
```

---

## 17. Known Limitations on macOS

### Will Not Run (Infrastructure)
| Category | Reason | Tests Affected |
|----------|--------|----------------|
| **GPU/CUDA tests** | macOS has no NVIDIA GPU support | ~50+ tests across Train, RLlib, Data |
| **Cgroup tests** | Linux-only kernel feature | 5 C++ targets |
| **Mutable object tests** | Linux-only shared memory | 2 C++ targets |
| **Redis-dependent C++ tests** | Need Redis binary in Bazel sandbox | ~10 C++ targets |
| **C++ cluster_mode tests** | Need full Ray runtime in Bazel sandbox | 3 C++ targets |
| **Cloud integration tests** | Need AWS/GCP/Azure credentials | ~50+ Python tests |
| **Kubernetes tests** | Need K8s cluster | ~20+ Python tests |
| **Docker-in-Docker tests** | Need Docker daemon | ~10+ Python tests |
| **LLM/vLLM tests** | Need GPU + vLLM | All LLM tests |
| **Multi-node tests** | Need actual cluster | ~30+ Python tests |

### Will Run but May Fail (Dependency Issues)
| Issue | Affected Suite | Workaround |
|-------|----------------|------------|
| `ale_py` not installed | RLlib Atari tests | `pip install ale-py` or skip with `-k "not atari"` |
| `memray` unavailable on macOS/ARM | Memory profiling tests | Automatically skipped |
| `tensordict` unavailable on macOS | Distributed gloo tests | Automatically skipped |
| XGBoost version mismatch | Tune XGBoost integration | Pin `xgboost==2.0.3` |
| `gradio` version conflict | HF integration tests | Skip with `-k "not gradio"` |

### Resource Constraints
- **Memory**: Each test file starts/stops Ray clusters; don't run more than 1-2 test files in parallel
- **Ports**: Use `RAY_USE_RANDOM_PORTS=1` to avoid port conflicts
- **Disk**: Bazel cache grows fast; periodically run `bazel clean --expunge` if running low
- **Time**: Full suite takes **8-16 hours** running sequentially on a single machine

### Recommended Execution Order (Priority)
1. **C++ unit tests** (fast, hermetic, most will pass) — 15-30 min
2. **Python common tests** (fast, few deps) — 10-20 min
3. **Core Python small tests** (good coverage baseline) — 30-60 min
4. **Ray Data tests** (important for data pipeline work) — 1-3 hours
5. **Ray Serve tests** (important for serving) — 2-4 hours
6. **Ray Tune/Train tests** (ML training) — 1-2 hours
7. **RLlib tests** (RL-specific) — 2-4 hours
8. **Everything else** (DAG, Dashboard, Air, Autoscaler, LLM, Release)

### Total Estimated Runtime
- **Optimistic** (only what passes cleanly): ~4-6 hours
- **Full run** (all suites, including failures): ~8-16 hours
- **Recommended**: Run priority order above, stop after item 5 for an 80% coverage baseline in ~5-8 hours
