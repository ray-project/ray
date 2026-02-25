# Ray Configuration Reference for Eugo HPC Deployments

> **Eugo — The Future of Supercomputing**
>
> This document is the **comprehensive catalog** of every tunable Ray configuration variable — C++ (`ray_config_def.h`, overridden via `RAY_<name>=value`), Python (environment variables), CLI flags (`ray start`, `ray stop`, `ray submit`, `ray job submit`), and library-specific knobs (Serve, Data, Train, Tune, DAG) — with HPC-specific recommendations for GPU-cluster / InfiniBand deployments.
>
> **Last updated:** 2025-07-17 — covers Ray 3.0.0.dev0 (nightly master).

---

## Table of Contents

1. [How Ray Configuration Works](#1-how-ray-configuration-works)
2. [Quick-Start HPC Export Block](#2-quick-start-hpc-export-block)
3. [CLI Flags Reference](#3-cli-flags-reference)
4. [Tier 1 — Critical Performance Tuning (C++)](#4-tier-1--critical-performance-tuning-c)
5. [Tier 2 — Stability & Resilience (C++)](#5-tier-2--stability--resilience-c)
6. [Tier 3 — Operational Tuning (C++)](#6-tier-3--operational-tuning-c)
7. [Tier 4 — Security / TLS (C++)](#7-tier-4--security--tls-c)
8. [GPU & Direct Transport (NIXL/RDMA)](#8-gpu--direct-transport-nixlrdma)
9. [Compiled Graphs (ADAG) — GPU Pipeline Performance](#9-compiled-graphs-adag--gpu-pipeline-performance)
10. [Object Store & Plasma](#10-object-store--plasma)
11. [Memory Profiling & jemalloc](#11-memory-profiling--jemalloc)
12. [Logging & Diagnostics](#12-logging--diagnostics)
13. [Metrics & Telemetry](#13-metrics--telemetry)
14. [OMP / Thread Control](#14-omp--thread-control)
15. [Ray Serve — Complete Configuration (90 vars)](#15-ray-serve--complete-configuration-90-vars)
16. [Ray Serve LLM (23 vars)](#16-ray-serve-llm-23-vars)
17. [Ray Data — Complete Configuration (71 vars)](#17-ray-data--complete-configuration-71-vars)
18. [Ray Train v2 — Distributed Training](#18-ray-train-v2--distributed-training)
19. [Ray Tune — Hyperparameter Tuning](#19-ray-tune--hyperparameter-tuning)
20. [Collective Communication (NCCL / Gloo)](#20-collective-communication-nccl--gloo)
21. [Accelerator Visibility & Device Management](#21-accelerator-visibility--device-management)
22. [Runtime Environment](#22-runtime-environment)
23. [Networking, Cluster & Redis](#23-networking-cluster--redis)
24. [Dashboard](#24-dashboard)
25. [Worker & Process Management](#25-worker--process-management)
26. [Autoscaler & Cloud](#26-autoscaler--cloud)
27. [Usage Stats & Telemetry Reporting](#27-usage-stats--telemetry-reporting)
28. [Ray AIR Integrations (W&B, MLflow, Comet)](#28-ray-air-integrations-wb-mlflow-comet)
29. [Spark & Databricks Integration](#29-spark--databricks-integration)
30. [Ray Client Configuration](#30-ray-client-configuration)
31. [Miscellaneous Environment Variables](#31-miscellaneous-environment-variables)
32. [Third-Party / System Environment Variables](#32-third-party--system-environment-variables)
33. [C++ Direct std::getenv Calls](#33-c-direct-stdgetenv-calls)
34. [Raylet & GCS Server Internal Process Flags](#34-raylet--gcs-server-internal-process-flags)
35. [All C++ RAY_CONFIG Entries (Complete Table)](#35-all-c-ray_config-entries-complete-table)
36. [Leave at Defaults](#36-leave-at-defaults)
37. [Internal / Do Not Touch](#37-internal--do-not-touch)
38. [Build-Time Only (Not Runtime)](#38-build-time-only-not-runtime)

---

## 1. How Ray Configuration Works

### C++ Configuration (`ray_config_def.h`)

All C++ runtime configuration is defined in `src/ray/common/ray_config_def.h` using the `RAY_CONFIG(type, name, default_value)` macro (~160 entries as of this writing). The `RayConfig` class (in `ray_config.h`) generates a private field and public getter for each config. At construction time, `ray_config.cc` performs a **two-pass initialization**:

1. **Pass 1 — Environment variables**: For each config named `xyz`, reads `RAY_xyz` from the environment via `ReadEnv<T>("RAY_xyz")`. If set, overrides the compiled default.
2. **Pass 2 — JSON config_list**: If `--system-config` JSON is passed (via `ray start`), each key in the JSON overrides the value from Pass 1.

**Override pattern:** `export RAY_<name>=<value>` before starting any Ray process (raylet, GCS server, workers).

### Naming Convention Summary

| Source | Prefix | Casing | Examples |
|--------|--------|--------|----------|
| **C++ `ray_config_def.h`** | `RAY_` | **lowercase_with_underscores** (exact config name) | `RAY_memory_usage_threshold`, `RAY_worker_niceness`, `RAY_scheduler_spread_threshold` |
| **C++ `ray_config_def.h` (auth/TLS)** | `RAY_` | **UPPER_CASE** (exception) | `RAY_USE_TLS`, `RAY_TLS_SERVER_CERT`, `RAY_AUTH_MODE`, `RAY_ENABLE_K8S_TOKEN_AUTH` |
| **Python core (`ray_constants.py`)** | `RAY_` | **UPPER_SNAKE_CASE** | `RAY_ENABLE_ZERO_COPY_TORCH_TENSORS`, `RAY_DEFAULT_OBJECT_STORE_MAX_MEMORY_BYTES` |
| **Python core (bridged C++ names)** | `RAY_` | **lowercase** (matching C++ exactly) | `RAY_gcs_server_request_timeout_seconds`, `RAY_grpc_enable_http_proxy`, `RAY_worker_niceness` |
| **Ray Serve** | `RAY_SERVE_` | **UPPER_SNAKE_CASE** | `RAY_SERVE_USE_PACK_SCHEDULING_STRATEGY`, `RAY_SERVE_THROUGHPUT_OPTIMIZED` |
| **Ray Data** | `RAY_DATA_` | **UPPER_SNAKE_CASE** | `RAY_DATA_DEFAULT_SHUFFLE_STRATEGY`, `RAY_DATA_OP_RESERVATION_RATIO` |
| **Ray Train** | `RAY_TRAIN_` | **UPPER_SNAKE_CASE** | `RAY_TRAIN_HEALTH_CHECK_INTERVAL_S`, `RAY_TRAIN_V2_ENABLED` |
| **Compiled Graphs (ADAG)** | `RAY_CGRAPH_` | **lowercase** (C++ style) | `RAY_CGRAPH_buffer_size_bytes`, `RAY_CGRAPH_overlap_gpu_communication` |
| **Compiled Graphs (constants.py)** | `RAY_CGRAPH_` | **UPPER_SNAKE_CASE** | `RAY_CGRAPH_ENABLE_PROFILING`, `RAY_CGRAPH_ENABLE_DETECT_DEADLOCK` |
| **Ray LLM Serve** | `RAYLLM_` or `RAY_SERVE_LLM_` | **UPPER_SNAKE_CASE** | `RAYLLM_ENGINE_START_TIMEOUT_S`, `RAY_SERVE_LLM_DEFAULT_MAX_REPLICAS` |
| **Tune** | `TUNE_` | **UPPER_SNAKE_CASE** | `TUNE_RESULT_BUFFER_LENGTH`, `TUNE_GLOBAL_CHECKPOINT_S` |
| **Dashboard** | `RAY_DASHBOARD_` or mixed | **UPPER_SNAKE_CASE** | `RAY_DASHBOARD_STARTUP_TIMEOUT_S`, `RAY_STATE_SERVER_MAX_HTTP_REQUEST` |
| **System / third-party** | (no `RAY_` prefix) | **UPPER_CASE** | `OMP_NUM_THREADS`, `CUDA_VISIBLE_DEVICES`, `NCCL_SOCKET_IFNAME` |

> **Key insight**: C++ configs use `RAY_<exact_lowercase_name>` — **not** uppercased. Setting `RAY_MEMORY_USAGE_THRESHOLD` would be **silently ignored**. The correct form is `RAY_memory_usage_threshold`. Python-side vars are uppercase. Some Python vars bridge to C++ names and preserve the lowercase (e.g., `RAY_worker_niceness`, `RAY_gcs_server_request_timeout_seconds`).

### Python Configuration (Environment Variables)

Python-side configuration uses `os.getenv()`, `env_bool()`, `env_integer()`, `env_float()` from `ray_constants.py`. These are read at import time or process startup.

### Notation

| Column | Meaning |
|--------|---------|
| **Variable** | Env var name to `export` |
| **Default** | Default value if not set |
| **Type** | `int`, `float`, `bool`, `str`, `bytes` |
| **HPC Rec** | Suggested value for Eugo GPU-cluster deployments |
| **Source** | File where the variable is defined |

---

## 2. Quick-Start HPC Export Block

A minimal set of env vars to export before `ray start` on a dedicated GPU HPC node:

```bash
# ============================================================
# Eugo HPC — Recommended Ray Environment Variables
# Export before `ray start` on every node.
# ============================================================

# === Performance (Tier 1 — C++ configs, lowercase after RAY_) ===
export RAY_worker_niceness=0
export RAY_worker_oom_score_adjustment=0
export RAY_object_manager_default_chunk_size=268435456      # 256 MB
export RAY_object_manager_max_bytes_in_flight=17179869184    # 16 GB
export RAY_object_manager_rpc_threads_num=16
export RAY_object_manager_client_connection_num=8
export RAY_max_direct_call_object_size=10485760              # 10 MB
export RAY_task_rpc_inlined_bytes_limit=104857600            # 100 MB
export RAY_scheduler_spread_threshold=0.1
export RAY_scheduler_top_k_fraction=0.5

# === Performance (Python configs, UPPERCASE) ===
export RAY_ENABLE_ZERO_COPY_TORCH_TENSORS=1

# === Stability (Tier 2 — C++ configs) ===
export RAY_memory_usage_threshold=0.90
export RAY_min_memory_free_bytes=8589934592                  # 8 GB
export RAY_health_check_initial_delay_ms=15000
export RAY_health_check_period_ms=5000
export RAY_health_check_timeout_ms=30000
export RAY_health_check_failure_threshold=10
export RAY_gcs_rpc_server_reconnect_timeout_s=300
export RAY_kill_worker_timeout_milliseconds=30000
export RAY_actor_graceful_shutdown_timeout_ms=120000

# === Operations (Tier 3 — C++ configs) ===
export RAY_preallocate_plasma_memory=1
export RAY_idle_worker_killing_time_threshold_ms=30000
export RAY_worker_maximum_startup_concurrency=16
export RAY_scheduler_avoid_gpu_nodes=false
export RAY_max_io_workers=16

# === Object Store (Python, UPPERCASE) ===
export RAY_DEFAULT_OBJECT_STORE_MEMORY_PROPORTION=0.6
export RAY_DEFAULT_SYSTEM_RESERVED_MEMORY_PROPORTION=0.1

# === Thread Control ===
# Ray auto-sets OMP_NUM_THREADS per worker; override globally only if needed:
# export OMP_NUM_THREADS=<your_value>

# === Logging (mixed casing) ===
export RAY_BACKEND_LOG_JSON=1
export RAY_ROTATION_BACKUP_COUNT=10
export RAY_LOGGING_LEVEL=warning

# === GPU / RDMA (Python, UPPERCASE) ===
export RAY_rdt_fetch_fail_timeout_milliseconds=120000
export RAY_NIXL_REMOTE_AGENT_CACHE_MAXSIZE=5000

# === Compiled Graphs (if using ADAG) ===
export RAY_CGRAPH_buffer_size_bytes=67108864                 # 64 MB
export RAY_CGRAPH_max_inflight_executions=2
export RAY_CGRAPH_overlap_gpu_communication=1

# === Train (if using distributed training) ===
export RAY_TRAIN_WORKER_HEALTH_CHECK_TIMEOUT_S=1200
export RAY_TRAIN_WORKER_GROUP_START_TIMEOUT_S=300

# === Telemetry (disable on air-gapped clusters) ===
export RAY_USAGE_STATS_ENABLED=0
export RAY_USAGE_STATS_PROMPT_ENABLED=0
```

---

## 3. CLI Flags Reference

### 3.1. `ray start`

Source: `python/ray/scripts/scripts.py`

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--head` | flag | `False` | Provide this flag for the head node |
| `--address` | str | None | Address to use for Ray (head node's `<ip>:<port>`) |
| `--port` | int | `6379` | Port of the head Ray process. `0` = auto-allocate |
| `--node-ip-address` | str | auto | IP address of this node |
| `--node-name` | str | auto | User-provided node identifier |
| `--num-cpus` | int | auto | Number of CPUs on this node |
| `--num-gpus` | int | auto | Number of GPUs on this node |
| `--resources` | JSON str | `"{}"` | Custom resources: `'{"TPU": 4, "custom_res": 8}'` |
| `--labels` | str | `""` | Node labels: `"key1=val1,key2=val2"` |
| `--labels-file` | str | `""` | Path to YAML file with label key-value mapping |
| `--object-store-memory` | int (bytes) | ~30% RAM | Amount of memory for the object store |
| `--memory` | int (bytes) | None | Memory available to workers |
| `--plasma-directory` | str | None | Object store directory for memory-mapped files |
| `--object-spilling-directory` | str | None | Path for object spilling when store is full |
| `--dashboard-host` | str | `"127.0.0.1"` | Dashboard bind host |
| `--dashboard-port` | int | `8265` | Dashboard bind port |
| `--dashboard-agent-listen-port` | int | `52365` | Dashboard agent HTTP port |
| `--dashboard-agent-grpc-port` | int | None | Dashboard agent gRPC port |
| `--runtime-env-agent-port` | int | None | Runtime environment agent HTTP port |
| `--include-dashboard` | bool | None | Start the Ray dashboard GUI |
| `--include-log-monitor` | bool | None | Start log monitor to push logs to GCS |
| `--metrics-export-port` | int | None | Prometheus metrics export port |
| `--min-worker-port` | int | `10002` | Lowest worker bind port |
| `--max-worker-port` | int | `19999` | Highest worker bind port |
| `--worker-port-list` | str | None | Comma-separated explicit worker port list |
| `--ray-client-server-port` | int | `10001` | Ray client server port |
| `--object-manager-port` | int | None | Object manager port |
| `--node-manager-port` | int | `0` | Node manager port |
| `--system-config` | JSON str | None | Override C++ system config defaults (JSON dict) |
| `--temp-dir` | str | None | Root temporary directory (head only) |
| `--autoscaling-config` | str | None | Autoscaling config YAML file |
| `--block` | flag | `False` | Block forever after starting |
| `--no-redirect-output` | flag | `False` | Don't redirect stdout/stderr to files |
| `--no-monitor` | flag | `False` | Don't start autoscaler monitor |
| `--redis-username` | str | `""` | Redis auth username |
| `--redis-password` | str | `""` | Redis auth password |
| `--tracing-startup-hook` | str | None | Tracing provider setup function |
| `--ray-debugger-external` | flag | `False` | Make debugger externally accessible |
| `--disable-usage-stats` | flag | `False` | Disable usage stats collection |
| `--enable-resource-isolation` | flag | `False` | Enable cgroupv2 resource isolation |
| `--system-reserved-cpu` | float | None | CPU cores reserved for Ray system (requires `--enable-resource-isolation`) |
| `--system-reserved-memory` | int (bytes) | None | Memory reserved for Ray system (requires `--enable-resource-isolation`) |
| `--cgroup-path` | str | `/sys/fs/cgroup` | Cgroup path for resource isolation |
| `--enable-object-reconstruction` | flag | `False` | Enable object reconstruction on failure |

### 3.2. `ray stop`

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `-f` / `--force` | flag | `False` | Send SIGKILL instead of SIGTERM |
| `-g` / `--grace-period` | int | `16` | Seconds to wait before force termination |

### 3.3. `ray submit`

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `CLUSTER_CONFIG_FILE` | str (arg) | required | Cluster config YAML file |
| `SCRIPT` | str (arg) | required | Script to upload and run |
| `SCRIPT_ARGS` | variadic | — | Arguments to pass to the script |
| `--stop` | flag | `False` | Stop cluster after command finishes |
| `--start` | flag | `False` | Start cluster if needed |
| `--screen` | flag | `False` | Run command in screen |
| `--tmux` | flag | `False` | Run command in tmux |
| `--cluster-name` / `-n` | str | None | Override cluster name |
| `--no-config-cache` | flag | `False` | Disable local cluster config cache |
| `--port-forward` / `-p` | int (multiple) | None | Port(s) to forward |
| `--disable-usage-stats` | flag | `False` | Disable usage stats |
| `--extra-screen-args` | str | None | Extra screen arguments |

### 3.4. `ray job submit`

Source: `python/ray/dashboard/modules/job/cli.py`

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `ENTRYPOINT` | variadic (arg) | required | Entrypoint command to run |
| `--address` | str | `RAY_API_SERVER_ADDRESS` or `RAY_ADDRESS` | Address of the Ray cluster |
| `--submission-id` | str | auto | Submission ID for the job |
| `--runtime-env` | str | None | Path to local YAML runtime_env |
| `--runtime-env-json` | str | None | JSON-serialized runtime_env |
| `--working-dir` | str | None | Directory with job files (local or remote URI) |
| `--metadata-json` | str | None | JSON metadata dict to attach to job |
| `--entrypoint-num-cpus` | float | None | CPUs reserved for entrypoint |
| `--entrypoint-num-gpus` | float | None | GPUs reserved for entrypoint |
| `--entrypoint-memory` | int | None | Memory reserved for entrypoint |
| `--entrypoint-resources` | JSON str | None | Custom resources for entrypoint |
| `--entrypoint-label-selector` | JSON str | None | Label selector for entrypoint placement |
| `--no-wait` | flag | `False` | Don't stream logs / wait for job exit |
| `--verify` | bool/str | `True` | Verify TLS certificate, or path to trusted certs |
| `--headers` | JSON str | None | HTTP headers to pass to Ray cluster |

---

## 4. Tier 1 — Critical Performance Tuning (C++)

These directly impact throughput, latency, and resource utilization on GPU clusters. **Tune these first.**

All variables below are C++ configs from `ray_config_def.h`. Override via `export RAY_<exact_lowercase_name>=<value>`.

| Variable | Default | Type | HPC Recommendation | Rationale |
|----------|---------|------|---------------------|-----------|
| `RAY_worker_niceness` | `15` | int | **`0`** | Default de-prioritizes workers. On dedicated HPC nodes, workers ARE the workload. |
| `RAY_worker_oom_score_adjustment` | `1000` | int | **`0`** | Default makes workers first to be OOM-killed. On HPC nodes the job IS the workers. |
| `RAY_object_manager_default_chunk_size` | `5 MB` | uint64 | **`256 MB` – `1 GB`** | Larger chunks reduce per-chunk overhead on InfiniBand. |
| `RAY_object_manager_max_bytes_in_flight` | `2 GB` | uint64 | **`8–16 GB`** | With InfiniBand (100+ Gbps), the default bottlenecks transfers. |
| `RAY_object_manager_rpc_threads_num` | `0` (auto: min(max(2, ncpus/4), 8)) | int | **`16–32`** | Actual thread count is 3× this value. With many GPUs, 2 threads is a bottleneck. |
| `RAY_object_manager_client_connection_num` | `4` | int | **`8–16`** | Parallel gRPC connections per peer node. More = more InfiniBand QPs. |
| `RAY_max_grpc_message_size` | `512 MB` | size_t | **`1 GB`** | Large model checkpoints may exceed 512 MB. |
| `RAY_max_direct_call_object_size` | `100 KB` | int64 | **`1–10 MB`** | Objects below this inlined in gRPC (no plasma). Raising reduces plasma round-trips for medium tensors. |
| `RAY_task_rpc_inlined_bytes_limit` | `10 MB` | int64 | **`50–100 MB`** | Cap on total inlined bytes per task response. Raise with `max_direct_call_object_size`. |
| `RAY_scheduler_spread_threshold` | `0.5` | float | **`0.0–0.2`** | Lower = earlier spreading = better GPU utilization across nodes. `0.0` = always spread. |
| `RAY_scheduler_top_k_fraction` | `0.2` | float | **`0.5–1.0`** | Fraction of nodes considered for placement. Higher = better spreading. |
| `RAY_scheduler_top_k_absolute` | `1` | int32 | **`4–8`** | Minimum number of nodes in the top-k set. |
| `RAY_num_server_call_thread` | `max(1, ncpus/4)` | int64 | **`8–16`** | gRPC server reply thread pool size for raylet/GCS. |
| `RAY_worker_num_grpc_internal_threads` | `0` (grpc default) | int64 | **`4–8`** | gRPC completion queue threads per worker process. |
| `RAY_grpc_stream_buffer_size` | `512 KB` | int64 | **`2–4 MB`** | gRPC stream buffer size. Larger for high-throughput streaming. |

### Python Performance Configs

| Variable | Default | Type | HPC Recommendation | Source |
|----------|---------|------|---------------------|--------|
| `RAY_ENABLE_ZERO_COPY_TORCH_TENSORS` | `0` | bool | **`1`** | `ray_constants.py` — critical for GPU; avoids memcpy for torch tensor deserialization |
| `RAY_DEFAULT_OBJECT_STORE_MAX_MEMORY_BYTES` | auto (~30% RAM, max 200 GB) | int | **Explicit large value** | `ray_constants.py` |
| `RAY_DEFAULT_OBJECT_STORE_MEMORY_PROPORTION` | `0.3` | float | **`0.5–0.7`** | `ray_constants.py` — on dedicated HPC nodes, give plasma more RAM |

---

## 5. Tier 2 — Stability & Resilience (C++)

Prevent false-positive failures in large clusters where transient delays are normal.

| Variable | Default | Type | HPC Recommendation | Rationale |
|----------|---------|------|---------------------|-----------|
| `RAY_memory_usage_threshold` | `0.95` | float | **`0.90`** | Start interventions earlier on large-memory nodes. |
| `RAY_min_memory_free_bytes` | `-1` (disabled) | int64 | **`8–16 GB`** | Absolute floor. On 512 GB+ nodes, tight margins are dangerous. |
| `RAY_memory_monitor_refresh_ms` | `250` | uint64 | Keep or **`500`** | Reduce CPU overhead on busy nodes. |
| `RAY_health_check_initial_delay_ms` | `5000` | int64 | **`10000–30000`** | Large cluster startup takes longer. |
| `RAY_health_check_period_ms` | `3000` | int64 | **`5000–10000`** | Less frequent checks reduce load. |
| `RAY_health_check_timeout_ms` | `10000` | int64 | **`30000`** | Workers under heavy GPU load may be slow to respond. |
| `RAY_health_check_failure_threshold` | `5` | int64 | **`10`** | More tolerance before declaring worker dead. |
| `RAY_gcs_rpc_server_reconnect_timeout_s` | `60` | uint32 | **`120–300`** | GCS hiccups shouldn't kill long-running training jobs. |
| `RAY_gcs_grpc_max_request_queued_max_bytes` | `5 GB` | uint64 | **`10–20 GB`** | Large clusters generate heavy GCS traffic. |
| `RAY_gcs_rpc_server_connect_timeout_s` | `5` | int32 | **`10`** | More time for initial GCS connection. |
| `RAY_gcs_grpc_initial_reconnect_backoff_ms` | `100` | int32 | Keep or **`200`** | Avoid hammering GCS during recovery. |
| `RAY_gcs_grpc_min_reconnect_backoff_ms` | `1000` | int32 | Keep default. | |
| `RAY_gcs_grpc_max_reconnect_backoff_ms` | `2000` | int32 | Keep or **`4000`** | Cap backoff higher on congested clusters. |
| `RAY_kill_worker_timeout_milliseconds` | `5000` | int64 | **`30000`** | GPU workers take time to flush CUDA contexts. |
| `RAY_actor_graceful_shutdown_timeout_ms` | `30000` | int64 | **`60000–120000`** | Actors with large GPU state need graceful cleanup. |
| `RAY_grpc_keepalive_time_ms` | `10000` | int64 | Keep default | Aggressive keepalive detects broken InfiniBand connections. |
| `RAY_grpc_keepalive_timeout_ms` | `20000` | int64 | Keep default | |
| `RAY_grpc_client_keepalive_time_ms` | `300000` | int64 | **`60000`** | More aggressive client-side keepalive for long-lived connections. |
| `RAY_grpc_client_keepalive_timeout_ms` | `120000` | int64 | Keep default | |
| `RAY_grpc_client_idle_timeout_ms` | `1800000` (30 min) | int64 | Keep default | |
| `RAY_raylet_client_num_connect_attempts` | `10` | int64 | **`20`** | More retries to raylet during startup. |
| `RAY_raylet_client_connect_timeout_milliseconds` | `1000` | int64 | **`2000`** | |
| `RAY_worker_register_timeout_seconds` | `60` | int64 | **`120`** | GPU workers take longer to initialize (CUDA context). |
| `RAY_task_oom_retries` | `-1` (unlimited) | uint64 | Keep `-1` | Let Ray auto-retry OOM tasks on nodes with free memory. |
| `RAY_oom_grace_period_s` | `2` | int64 | **`5`** | More time before declaring OOM. |

### Python Stability Configs

| Variable | Default | Type | HPC Recommendation | Source |
|----------|---------|------|---------------------|--------|
| `RAY_DEFAULT_SYSTEM_RESERVED_MEMORY_BYTES` | auto | int | **Explicit (e.g., 4 GB)** | `ray_constants.py` |
| `RAY_DEFAULT_SYSTEM_RESERVED_MEMORY_PROPORTION` | `0.10` | float | Keep or lower to **`0.05`** on large nodes | `ray_constants.py` |
| `RAY_DEFAULT_MIN_SYSTEM_RESERVED_MEMORY_BYTES` | `500 MB` | int | **`2 GB`** | `ray_constants.py` |
| `RAY_DEFAULT_MAX_SYSTEM_RESERVED_MEMORY_BYTES` | `10 GB` | int | Keep default | `ray_constants.py` |

---

## 6. Tier 3 — Operational Tuning (C++)

Important for day-to-day cluster operations but not as performance-critical.

| Variable | Default | Type | HPC Recommendation | Rationale |
|----------|---------|------|---------------------|-----------|
| `RAY_preallocate_plasma_memory` | `false` | bool | **`true`** | Pre-fault object store memory; avoids page-fault latency during execution. |
| `RAY_raylet_report_resources_period_milliseconds` | `100` | uint64 | **`500–1000`** | Reduces GCS load on large clusters. |
| `RAY_raylet_check_gc_period_milliseconds` | `100` | uint64 | Keep default | GC pressure check frequency. |
| `RAY_idle_worker_killing_time_threshold_ms` | `1000` | int64 | **`10000–30000`** | Avoid churning workers; GPU workers have high startup cost (CUDA context). |
| `RAY_kill_idle_workers_interval_ms` | `200` | uint64 | **`1000`** | Less frequent idle worker culling. |
| `RAY_worker_maximum_startup_concurrency` | `0` (auto) | int64 | **`8–16`** | Allow more concurrent worker startups on high-core nodes. |
| `RAY_scheduler_avoid_gpu_nodes` | `true` | bool | **`false`** | On all-GPU clusters, everything runs on GPU nodes. |
| `RAY_num_workers_soft_limit` | `-1` (no limit) | int64 | **Set explicitly per node** | Prevent unbounded worker spawning. |
| `RAY_max_io_workers` | `4` | int | **`8–16`** | More I/O workers for parallel checkpoint loads / data staging. |
| `RAY_object_spilling_threshold` | `0.8` | float | **`0.9`** | Delay spilling on large-memory nodes. |
| `RAY_min_spilling_size` | `100 MB` | int64 | **`500 MB`** | Avoid spilling small objects. |
| `RAY_automatic_object_spilling_enabled` | `true` | bool | Keep `true` | |
| `RAY_free_objects_period_milliseconds` | `1000` | int64 | Keep or **`500`** | More aggressive GC if plasma fills fast. |
| `RAY_free_objects_batch_size` | `100` | size_t | Keep default | |
| `RAY_max_pending_lease_requests_per_scheduling_category` | `-1` (unlimited) | int64 | Keep default | |
| `RAY_metrics_report_interval_ms` | `10000` | uint64 | Keep default | |
| `RAY_metrics_report_batch_size` | `10000` | int64 | Keep default | |
| `RAY_debug_dump_period_milliseconds` | `10000` | uint64 | Keep or **`30000`** | Less frequent debug dumps on busy nodes. |
| `RAY_gcs_pull_resource_loads_period_milliseconds` | `1000` | uint64 | Keep default | |
| `RAY_maximum_gcs_destroyed_actor_cached_count` | `100000` | uint32 | Keep or reduce for small clusters | GCS memory footprint. |
| `RAY_maximum_gcs_dead_node_cached_count` | `1000` | uint32 | Keep default | |
| `RAY_maximum_gcs_deletion_batch_size` | `1000` | uint32 | Keep default | |
| `RAY_record_ref_creation_sites` | `false` | bool | Keep `false` | Debug feature; overhead in production. |
| `RAY_record_task_actor_creation_sites` | `false` | bool | Keep `false` | |
| `RAY_max_task_args_memory_fraction` | `0.7` | float | Keep default | |
| `RAY_enable_worker_prestart` | `false` | bool | **`true`** for latency-sensitive workloads | Pre-starts worker pool. |
| `RAY_prestart_worker_first_driver` | `true` | bool | Keep `true` | |
| `RAY_worker_cap_enabled` | `true` | bool | Keep `true` | |
| `RAY_lineage_pinning_enabled` | `true` | bool | Keep `true` | |
| `RAY_max_lineage_bytes` | `1 GB` | int64 | Keep default | |
| `RAY_locality_aware_leasing_enabled` | `true` | bool | Keep `true` | Important for data locality on multi-node clusters. |

---

## 7. Tier 4 — Security / TLS (C++)

Set at deployment time based on cluster security requirements.

> **Note:** These C++ config names are UPPERCASE (an exception to the general lowercase rule).

| Variable | Default | Type | Notes |
|----------|---------|------|-------|
| `RAY_USE_TLS` | `false` | bool | Enable TLS for all Ray gRPC communication |
| `RAY_TLS_SERVER_CERT` | `""` | str | Path to server certificate |
| `RAY_TLS_SERVER_KEY` | `""` | str | Path to server private key |
| `RAY_TLS_CA_CERT` | `""` | str | Path to CA certificate |
| `RAY_REDIS_CA_CERT` | `""` | str | Path to Redis CA certificate |
| `RAY_REDIS_CA_PATH` | `""` | str | Path to Redis CA directory |
| `RAY_REDIS_CLIENT_CERT` | `""` | str | Path to Redis client certificate |
| `RAY_REDIS_CLIENT_KEY` | `""` | str | Path to Redis client key |
| `RAY_REDIS_SERVER_NAME` | `""` | str | Redis server name for SNI |
| `RAY_enable_cluster_auth` | `true` | bool | Enable cluster authentication |
| `RAY_AUTH_MODE` | `"disabled"` | str | Authentication mode (`"disabled"`, `"token"`) |
| `RAY_ENABLE_K8S_TOKEN_AUTH` | `false` | bool | Kubernetes token-based authentication |

---

## 8. GPU & Direct Transport (NIXL/RDMA)

Critical for GPU-to-GPU data transfer performance.

| Variable | Default | Type | HPC Recommendation | Source |
|----------|---------|------|---------------------|--------|
| `RAY_ENABLE_ZERO_COPY_TORCH_TENSORS` | `0` | bool | **`1`** | `ray_constants.py` — enables zero-copy deserialization of torch tensors from plasma |
| `RAY_rdt_fetch_fail_timeout_milliseconds` | `60000` | int | **`120000`** for large models | `ray_constants.py` — timeout for remote direct transport fetch |
| `RAY_NIXL_REMOTE_AGENT_CACHE_MAXSIZE` | `1000` | int | **`5000`** for large clusters | `ray_constants.py` — LRU cache for remote NIXL agent handles |
| `CUDA_VISIBLE_DEVICES` | system | str | Managed by Ray automatically | Set per-worker by Ray; **do not** set globally |
| `FI_EFA_USE_DEVICE_RDMA` | None | bool | **`1`** on AWS EFA | Libfabric EFA — enables device-level RDMA for NCCL |
| `NCCL_USE_MULTISTREAM` | `True` | bool | Keep `True` | `ray.util.collective.const` — multi-stream NCCL |
| `NCCL_SOCKET_IFNAME` | None | str | Set to your InfiniBand interface (e.g., `ib0`) | System NCCL config — restrict to IB fabric |
| `NCCL_IB_DISABLE` | `0` | str | Keep `0` (IB enabled) | Ensure InfiniBand is used for NCCL |
| `NCCL_NET_GDR_LEVEL` | None | str | **`5` (PIX)** or auto | GPU Direct RDMA level for optimal NVLink/IB topology |

---

## 9. Compiled Graphs (ADAG) — GPU Pipeline Performance

Compiled graphs are critical for low-latency GPU pipeline DAGs (actor DAGs compiled to shared-memory channels).

### From `dag/context.py` (lowercase after prefix)

| Variable | Default | Type | HPC Recommendation | Description |
|----------|---------|------|---------------------|-------------|
| `RAY_CGRAPH_buffer_size_bytes` | `1000000` (1 MB) | int | **`16–64 MB`** for large model tensors | Shared-memory buffer per channel |
| `RAY_CGRAPH_max_inflight_executions` | `10` | int | **`2–4`** for pipelining | Concurrent DAG executions |
| `RAY_CGRAPH_overlap_gpu_communication` | `0` | bool | **`1`** | Overlap GPU comm with compute — critical for pipeline throughput |
| `RAY_CGRAPH_submit_timeout` | `10` | int | **`60–120`** for large models | DAG submission timeout (seconds) |
| `RAY_CGRAPH_get_timeout` | `10` | int | **`30`** | DAG result retrieval timeout |
| `RAY_CGRAPH_teardown_timeout` | `30` | int | **`60`** | Compiled graph teardown timeout |
| `RAY_CGRAPH_read_iteration_timeout_s` | `0.1` | float | Keep default | Read iteration timeout |
| `RAY_CGRAPH_max_buffered_results` | `1000` | int | Keep default | Max results buffered at driver |

### From `dag/constants.py` (UPPERCASE after prefix)

| Variable | Default | Type | HPC Recommendation | Description |
|----------|---------|------|---------------------|-------------|
| `RAY_CGRAPH_ENABLE_DETECT_DEADLOCK` | `"1"` (True) | bool | Keep `1` | Deadlock detection |
| `RAY_CGRAPH_ENABLE_PROFILING` | `"0"` (False) | bool | `1` during development, `0` in production | Profiling overhead |
| `RAY_CGRAPH_ENABLE_NVTX_PROFILING` | `"0"` (False) | bool | `1` for NVIDIA Nsight profiling | NVTX markers |
| `RAY_CGRAPH_ENABLE_TORCH_PROFILING` | `"0"` (False) | bool | `1` for torch profiler integration | |
| `RAY_CGRAPH_VISUALIZE_SCHEDULE` | `"0"` (False) | bool | `1` for debugging | Visualize execution schedule |

---

## 10. Object Store & Plasma

| Variable | Default | Type | HPC Recommendation | Source |
|----------|---------|------|---------------------|--------|
| `RAY_DEFAULT_OBJECT_STORE_MAX_MEMORY_BYTES` | `200 GB` (cap) | int | **Explicit (e.g., `200000000000`)** | `ray_constants.py` |
| `RAY_DEFAULT_OBJECT_STORE_MEMORY_PROPORTION` | `0.3` | float | **`0.5–0.7`** | `ray_constants.py` |
| `RAY_DEFAULT_SYSTEM_RESERVED_MEMORY_BYTES` | auto | int | **Explicit** | `ray_constants.py` |
| `RAY_DEFAULT_SYSTEM_RESERVED_MEMORY_PROPORTION` | `0.10` | float | **`0.05`** on large nodes | `ray_constants.py` |
| `RAY_DEFAULT_SYSTEM_RESERVED_CPU_PROPORTION` | `0.05` | float | Keep default | `ray_constants.py` |
| `RAY_DEFAULT_MIN_SYSTEM_RESERVED_CPU_CORES` | `1.0` | float | Keep default | `ray_constants.py` |
| `RAY_DEFAULT_MAX_SYSTEM_RESERVED_CPU_CORES` | `3.0` | float | Keep default | `ray_constants.py` |
| `RAY_preallocate_plasma_memory` | `false` | bool (C++) | **`true`** | Pre-faults plasma memory |
| `RAY_object_spilling_config` | `""` (auto) | str (C++) | Set to NVMe spill paths | JSON config for spill backend |
| `RAY_object_spilling_directory` | `""` | str (C++) | **Fast NVMe path** | Directory for spilled objects |
| `RAY_object_spilling_threshold` | `0.8` | float (C++) | **`0.9`** | Delay spilling |
| `RAY_plasma_store_usage_trigger_gc_threshold` | `0.7` | double (C++) | Keep default | Plasma usage threshold to trigger GC |
| `RAY_object_store_full_delay_ms` | `10` | uint32 (C++) | Keep default | |
| `RAY_yield_plasma_lock_workaround` | `true` | bool (C++) | Keep default | |
| `RAY_worker_core_dump_exclude_plasma_store` | `true` | bool (C++) | Keep `true` | Exclude plasma pages from core dumps |
| `RAY_raylet_core_dump_exclude_plasma_store` | `true` | bool (C++) | Keep `true` | |

---

## 11. Memory Profiling & jemalloc

| Variable | Default | Type | HPC Recommendation | Source |
|----------|---------|------|---------------------|--------|
| `RAY_JEMALLOC_LIB_PATH` | None | str | Path to jemalloc `.so` | `ray_constants.py` |
| `RAY_JEMALLOC_CONF` | None | str | `"prof:true,lg_prof_interval:30"` for profiling | `ray_constants.py` |
| `RAY_JEMALLOC_PROFILE` | None | str | `"1"` to enable profiling | `ray_constants.py` |
| `RAY_LD_PRELOAD` | None | str | Path to libs to LD_PRELOAD on C++ processes | `ray_constants.py` |
| `RAY_LD_PRELOAD_ON_WORKERS` | `"0"` | str | `"0"` to NOT inherit LD_PRELOAD on workers | `raylet/main.cc` |
| `RAY_INTERNAL_MEM_PROFILE_COMPONENTS` | None | str | `"raylet,gcs"` | `services.py` — memray components |
| `RAY_INTERNAL_MEM_PROFILE_OPTIONS` | None | str | memray CLI options | `services.py` |

---

## 12. Logging & Diagnostics

### C++ Logging

| Variable | Default | Type | HPC Recommendation | Source |
|----------|---------|------|---------------------|--------|
| `RAY_BACKEND_LOG_LEVEL` | `"info"` | str | `"warning"` in production | `util/logging.cc` |
| `RAY_BACKEND_LOG_JSON` | `"0"` | str | **`"1"`** for structured logging | `util/logging.cc` |
| `RAY_log_rotation_max_bytes` | `100 MB` | int64 (C++) | Keep default | `ray_config_def.h` |
| `RAY_log_rotation_backup_count` | `5` | int64 (C++) | **`10`** for longer history | `ray_config_def.h` |
| `RAY_event_level` | `"warning"` | str (C++) | Keep default | Event severity filter |
| `RAY_event_log_reporter_enabled` | `true` | bool (C++) | Keep default | |
| `RAY_emit_event_to_log_file` | `false` | bool (C++) | Keep default | |
| `RAY_pipe_logger_read_buf_size` | `1024` | uint64 (C++) | Keep default | |

### Python/Process Logging

| Variable | Default | Type | HPC Recommendation | Source |
|----------|---------|------|---------------------|--------|
| `RAY_LOG_TO_STDERR` | unset | bool | `0` (use files) | `ray_constants.py` |
| `RAY_LOGGER_LEVEL` | `"info"` | str | `"warning"` in production | `ray_constants.py` |
| `RAY_LOGGING_CONFIG_ENCODING` | None | str | None or `"JSON"` | `ray_constants.py` |
| `RAY_LOG_TO_DRIVER` | `True` | bool | Keep `True` | `ray_constants.py` |
| `RAY_LOG_TO_DRIVER_EVENT_LEVEL` | `"INFO"` | str | Keep default | `ray_constants.py` |
| `RAY_SCHEDULER_EVENTS` | `1` | int | Keep default | `ray_constants.py` |
| `RAY_COLOR_PREFIX` | None | str | `"0"` in headless/batch | `worker.py` |
| `RAY_DEDUP_LOGS` | `True` | bool | Keep `True` | `ray_constants.py` |
| `RAY_DEDUP_LOGS_AGG_WINDOW_S` | `5` | int | Keep default | `ray_constants.py` |
| `RAY_DEDUP_LOGS_ALLOW_REGEX` | test pattern | str | Customize if needed | `ray_constants.py` |
| `RAY_DEDUP_LOGS_SKIP_REGEX` | None | str | Regex for logs to always suppress | `ray_constants.py` |
| `RAY_LOG_MONITOR_MAX_OPEN_FILES` | `200` | int | **`1000`** on large clusters | `ray_constants.py` |
| `RAY_LOG_MONITOR_NUM_LINES_TO_READ` | `1000` | int | Keep default | `ray_constants.py` |
| `RAY_LOG_MONITOR_MANY_FILES_THRESHOLD` | `1000` | int | **`5000`** on large clusters | `log_monitor.py` |
| `RAY_RUNTIME_ENV_LOG_TO_DRIVER_ENABLED` | `0` | int | `1` for debugging | `log_monitor.py` |
| `RAY_STREAM_RUNTIME_ENV_LOG_TO_JOB_DRIVER_LOG` | unset | str | Keep default | `dashboard/consts.py` |
| `RAY_DISABLE_DASHBOARD_LOG_INFO` | `0` | int | Keep default | `ray_constants.py` |
| `RAY_MAX_APPLICATION_ERROR_LENGTH` | `500` | int | Keep default | `ray_constants.py` |

---

## 13. Metrics & Telemetry

| Variable | Default | Type | HPC Recommendation | Source |
|----------|---------|------|---------------------|--------|
| `RAY_enable_metrics_collection` | `true` | bool (C++) | Keep `true` | `ray_config_def.h` |
| `RAY_metric_cardinality_level` | `"legacy"` | str (C++) | `"recommended"` or `"low"` | `ray_config_def.h` |
| `RAY_enable_open_telemetry` | `true` | bool (C++) | Keep `true` if using OTel | `ray_config_def.h` |
| `RAY_disable_open_telemetry_sdk_log` | `true` | bool (C++) | Keep default | |
| `RAY_enable_grpc_metrics_collection_for` | `""` | str (C++) | `"raylet,gcs"` for debugging | `ray_config_def.h` |
| `RAY_metrics_report_interval_ms` | `10000` | uint64 (C++) | Keep default | |
| `RAY_metrics_report_batch_size` | `10000` | int64 (C++) | Keep default | |
| `RAY_event_stats` | `true` | bool (C++) | Keep default | |
| `RAY_emit_main_service_metrics` | `true` | bool (C++) | Keep default | |
| `RAY_event_stats_print_interval_ms` | `60000` | int64 (C++) | Keep default | |
| `RAY_io_context_event_loop_lag_collection_interval_ms` | `10000` | int64 (C++) | Keep default | |
| `RAY_enable_timeline` | `true` | bool (C++) | `false` in production for perf | `ray_config_def.h` |
| `RAY_enable_export_api_write` | `false` | bool (C++) | `true` if using export events | `ray_config_def.h` |
| `RAY_enable_export_api_write_config` | `""` | str (C++) | `"EXPORT_ACTOR,EXPORT_TASK"` | Selective types |
| `RAY_EXPORT_EVENT_MAX_FILE_SIZE_BYTES` | `100 MB` | int | Keep default | `ray_constants.py` |
| `RAY_EXPORT_EVENT_MAX_BACKUP_COUNT` | `20` | int | Keep default | `ray_constants.py` |

---

## 14. OMP / Thread Control

Ray automatically manages `OMP_NUM_THREADS` for worker processes to prevent deep-learning frameworks from oversubscribing CPU cores.

**How it works** (from `python/ray/_private/utils.py:set_omp_num_threads_if_unset()`):
- If a worker is assigned `num_cpus`, Ray sets `OMP_NUM_THREADS` = `num_cpus` (floored to 1).
- If `num_cpus` is not specified, Ray forces `OMP_NUM_THREADS=1` to prevent contention.
- Setting `OMP_NUM_THREADS` globally before `ray start` overrides this behavior.

| Variable | Default | Type | HPC Recommendation | Source |
|----------|---------|------|---------------------|--------|
| `OMP_NUM_THREADS` | auto (per-worker) | int | Let Ray manage, or set explicitly if needed | `utils.py` |
| `MKL_NUM_THREADS` | Not set by Ray | int | Set if using Intel MKL directly | System env |
| `RAY_USE_MULTIPROCESSING_CPU_COUNT` | unset | str | Set if Docker CPU detection is wrong | `utils.py` |
| `RAY_DISABLE_DOCKER_CPU_WARNING` | unset | bool | Set to suppress Docker CPU count warning | `utils.py` |

> **Serve doc note** (`doc/source/serve/resource-allocation.md`): Ray Serve sets `OMP_NUM_THREADS` per replica based on `num_cpus` to avoid performance degradation from CPU oversubscription. Set explicitly in deployment config if the default doesn't match your workload.

---

## 15. Ray Serve — Complete Configuration (90 vars)

Source: `python/ray/serve/_private/constants.py`

### 15.1. Control Loop & Retry

| Variable | Default | Type | HPC Rec | Description |
|----------|---------|------|---------|-------------|
| `RAY_SERVE_CONTROL_LOOP_INTERVAL_S` | `0.1` | float | Keep default | Controller poll interval |
| `RAY_SERVE_MAX_PER_REPLICA_RETRY_COUNT` | `3` | int | Keep default | Per-replica retry count |

### 15.2. Scheduling Strategy

| Variable | Default | Type | HPC Rec | Description |
|----------|---------|------|---------|-------------|
| `RAY_SERVE_USE_PACK_SCHEDULING_STRATEGY` | `"0"` (False) | bool | **`"1"`** on GPU clusters | Pack replicas onto fewer nodes (better GPU utilization). Replaces deprecated `RAY_SERVE_USE_COMPACT_SCHEDULING_STRATEGY`. |
| `RAY_SERVE_USE_COMPACT_SCHEDULING_STRATEGY` | `"0"` | bool | Deprecated — use above | |
| `RAY_SERVE_HIGH_PRIORITY_CUSTOM_RESOURCES` | `""` | str (list) | Set custom resource names | Comma-separated custom resources for priority scheduling |

### 15.3. Routing & Locality

| Variable | Default | Type | HPC Rec | Description |
|----------|---------|------|---------|-------------|
| `RAY_SERVE_PROXY_PREFER_LOCAL_NODE_ROUTING` | `"1"` (True) | bool | Keep `"1"` | Route to replicas on the same node — critical for GPU locality |
| `RAY_SERVE_PROXY_PREFER_LOCAL_AZ_ROUTING` | `"1"` (True) | bool | Keep `"1"` | Route to same availability zone |
| `RAY_SERVE_MULTIPLEXED_MODEL_ID_MATCHING_TIMEOUT_S` | `1.0` | float | Keep default | Timeout for multiplexed model ID matching |

### 15.4. Router Queue & Backoff

| Variable | Default | Type | HPC Rec | Description |
|----------|---------|------|---------|-------------|
| `RAY_SERVE_ROUTER_RETRY_INITIAL_BACKOFF_S` | `0.025` | float | Keep default | Initial retry backoff |
| `RAY_SERVE_ROUTER_RETRY_BACKOFF_MULTIPLIER` | `2` | int | Keep default | Exponential backoff multiplier |
| `RAY_SERVE_ROUTER_RETRY_MAX_BACKOFF_S` | `0.5` | float | Keep default | Max retry backoff |
| `RAY_SERVE_QUEUE_LENGTH_RESPONSE_DEADLINE_S` | `0.1` | float | Keep default | Queue length response deadline |
| `RAY_SERVE_MAX_QUEUE_LENGTH_RESPONSE_DEADLINE_S` | `1.0` | float | Keep default | Max queue length deadline |
| `RAY_SERVE_QUEUE_LENGTH_CACHE_TIMEOUT_S` | `10.0` | float | Keep default | Queue length cache TTL |
| `RAY_SERVE_ROUTER_QUEUE_LEN_GAUGE_THROTTLE_S` | `0.1` | float | Keep default | Gauge reporting throttle |

### 15.5. Proxy Health & Draining

| Variable | Default | Type | HPC Rec | Description |
|----------|---------|------|---------|-------------|
| `RAY_SERVE_PROXY_HEALTH_CHECK_TIMEOUT_S` | `10.0` | float | **`30.0`** for GPU replicas | Health check timeout |
| `RAY_SERVE_PROXY_HEALTH_CHECK_PERIOD_S` | `10.0` | float | Keep default | Health check interval |
| `RAY_SERVE_PROXY_READY_CHECK_TIMEOUT_S` | `5.0` | float | Keep default | Ready-check timeout |
| `RAY_SERVE_PROXY_MIN_DRAINING_PERIOD_S` | `30.0` | float | **`60.0`** | Min drain time before shutdown |

### 15.6. Handle & Controller Limits

| Variable | Default | Type | HPC Rec | Description |
|----------|---------|------|---------|-------------|
| `RAY_SERVE_MAX_CACHED_HANDLES` | `100` | int | Keep default | Max cached ServeHandle objects |
| `RAY_SERVE_CONTROLLER_MAX_CONCURRENCY` | `15000` | int | Keep default | Max concurrent controller calls |

### 15.7. gRPC Configuration

| Variable | Default | Type | HPC Rec | Description |
|----------|---------|------|---------|-------------|
| `RAY_SERVE_GRPC_MAX_MESSAGE_SIZE` | `2147483647` (2 GiB−1) | int | Keep default | gRPC max message size |
| `RAY_SERVE_REPLICA_GRPC_MAX_MESSAGE_LENGTH` | `4194304` (4 MB) | int | **`20971520` (20 MB)** for large payloads | Per-replica gRPC message limit |
| `RAY_SERVE_USE_GRPC_BY_DEFAULT` | `"0"` (False) | bool | `"1"` if using gRPC | Use gRPC instead of HTTP |
| `RAY_SERVE_PROXY_USE_GRPC` | None (complex) | bool | Set explicitly if needed | Override proxy protocol |

### 15.8. HTTP Configuration

| Variable | Default | Type | HPC Rec | Description |
|----------|---------|------|---------|-------------|
| `RAY_SERVE_HTTP_KEEP_ALIVE_TIMEOUT_S` | `0` (disabled) | int | Keep default | HTTP keep-alive timeout |

### 15.9. Logging

| Variable | Default | Type | HPC Rec | Description |
|----------|---------|------|---------|-------------|
| `RAY_SERVE_LOG_TO_STDERR` | `"1"` (True) | bool | `"0"` in production (use files) | Log to stderr |
| `RAY_SERVE_REQUEST_PATH_LOG_BUFFER_SIZE` | `1` | int | Keep default | Request path log buffer |

### 15.10. Throughput Optimization

| Variable | Default | Type | HPC Rec | Description |
|----------|---------|------|---------|-------------|
| `RAY_SERVE_THROUGHPUT_OPTIMIZED` | `"0"` (False) | bool | **`"1"`** for max throughput | Master toggle — overrides several other vars with throughput-optimized defaults |
| `RAY_SERVE_RUN_SYNC_IN_THREADPOOL` | `"0"` (False) | bool | `"1"` if sync methods in async deployments | Offload sync methods to threadpool |
| `RAY_SERVE_RUN_USER_CODE_IN_SEPARATE_THREAD` | `"1"` (True) | bool | Keep (overridden to `"0"` by THROUGHPUT_OPTIMIZED) | Run user code in separate thread |
| `RAY_SERVE_RUN_ROUTER_IN_SEPARATE_LOOP` | `"1"` (True) | bool | Keep (overridden to `"0"` by THROUGHPUT_OPTIMIZED) | Router in separate event loop |
| `RAY_SERVE_ENABLE_PROXY_GC_OPTIMIZATIONS` | `"1"` (True) | bool | Keep default | GC optimizations in proxy |
| `RAY_SERVE_PROXY_GC_THRESHOLD` | `700` | int | Keep default | GC threshold |

> When `RAY_SERVE_THROUGHPUT_OPTIMIZED=1`, the following defaults change: `RUN_USER_CODE_IN_SEPARATE_THREAD` → `"0"`, `REQUEST_PATH_LOG_BUFFER_SIZE` → `1000`, `RUN_ROUTER_IN_SEPARATE_LOOP` → `"0"`, `LOG_TO_STDERR` → `"0"`, `USE_GRPC_BY_DEFAULT` → `"1"`, `ENABLE_DIRECT_INGRESS` → `"1"`.

### 15.11. Autoscaling Metrics

| Variable | Default | Type | HPC Rec | Description |
|----------|---------|------|---------|-------------|
| `RAY_SERVE_RECORD_AUTOSCALING_STATS_TIMEOUT_S` | `10.0` | float | Keep default | Autoscaling stats timeout |
| `RAY_SERVE_REPLICA_AUTOSCALING_METRIC_RECORD_INTERVAL_S` | `0.5` | float | Keep default | Replica metric record interval |
| `RAY_SERVE_REPLICA_AUTOSCALING_METRIC_PUSH_INTERVAL_S` | `10.0` | float | Keep default | Push interval |
| `RAY_SERVE_HANDLE_AUTOSCALING_METRIC_RECORD_INTERVAL_S` | `0.5` | float | Keep default | Handle metric record interval |
| `RAY_SERVE_HANDLE_AUTOSCALING_METRIC_PUSH_INTERVAL_S` | `10.0` | float | Keep default | Push interval |
| `RAY_SERVE_ASYNC_INFERENCE_TASK_QUEUE_METRIC_PUSH_INTERVAL_S` | `10.0` | float | Keep default | Async queue metrics |
| `RAY_SERVE_COLLECT_AUTOSCALING_METRICS_ON_HANDLE` | `"1"` (True) | bool | Keep default | Collect on handle |
| `RAY_SERVE_MIN_HANDLE_METRICS_TIMEOUT_S` | `10.0` | float | Keep default | Min handle metrics timeout |
| `RAY_SERVE_AGGREGATE_METRICS_AT_CONTROLLER` | `"0"` (False) | bool | Keep default | Aggregate at controller |

### 15.12. Replica Utilization Metrics

| Variable | Default | Type | Description |
|----------|---------|------|-------------|
| `RAY_SERVE_REPLICA_UTILIZATION_WINDOW_S` | `600` | float | Utilization observation window |
| `RAY_SERVE_REPLICA_UTILIZATION_REPORT_INTERVAL_S` | `10` | float | Report interval |
| `RAY_SERVE_REPLICA_UTILIZATION_NUM_BUCKETS` | `60` | int | Histogram buckets |

### 15.13. Replica Health

| Variable | Default | Type | Description |
|----------|---------|------|-------------|
| `RAY_SERVE_FORCE_STOP_UNHEALTHY_REPLICAS` | `"0"` (False) | bool | Force SIGKILL unhealthy replicas |
| `RAY_SERVE_REPLICA_HEALTH_GAUGE_REPORT_INTERVAL_S` | `10.0` | float | Health gauge report interval |

### 15.14. Histogram Buckets (Latency/Utilization/Batch)

These allow customizing Prometheus histogram buckets. Leave empty for defaults.

| Variable | Default | Type | Description |
|----------|---------|------|-------------|
| `RAY_SERVE_REQUEST_LATENCY_BUCKETS_MS` | `""` | str | Request latency histogram buckets |
| `RAY_SERVE_MODEL_LOAD_LATENCY_BUCKETS_MS` | `""` | str | Model load latency buckets |
| `RAY_SERVE_REPLICA_STARTUP_SHUTDOWN_LATENCY_BUCKETS_MS` | `""` | str | Startup/shutdown latency buckets |
| `RAY_SERVE_BATCH_UTILIZATION_BUCKETS_PERCENT` | `""` | str | Batch utilization buckets |
| `RAY_SERVE_BATCH_SIZE_BUCKETS` | `""` | str | Batch size buckets |

### 15.15. Profiling & Debugging

| Variable | Default | Type | Description |
|----------|---------|------|-------------|
| `RAY_SERVE_ENABLE_MEMORY_PROFILING` | `"0"` (False) | bool | Enable memory profiling in Serve |
| `RAY_SERVE_ENABLE_TASK_EVENTS` | `"0"` (False) | bool | Enable task events |
| `RAY_SERVE_FORCE_LOCAL_TESTING_MODE` | `"0"` (False) | bool | Force local testing mode |
| `RAY_SERVE_FAIL_ON_RANK_ERROR` | `"0"` (False) | bool | Fail on rank error |

### 15.16. Direct Ingress

| Variable | Default | Type | Description |
|----------|---------|------|-------------|
| `RAY_SERVE_ENABLE_DIRECT_INGRESS` | `"0"` (False) | bool | Enable direct ingress to replicas |
| `RAY_SERVE_DIRECT_INGRESS_MIN_HTTP_PORT` | `30000` | int | Min HTTP port range |
| `RAY_SERVE_DIRECT_INGRESS_MAX_HTTP_PORT` | `31000` | int | Max HTTP port range |
| `RAY_SERVE_DIRECT_INGRESS_MIN_GRPC_PORT` | `40000` | int | Min gRPC port range |
| `RAY_SERVE_DIRECT_INGRESS_MAX_GRPC_PORT` | `41000` | int | Max gRPC port range |
| `RAY_SERVE_DIRECT_INGRESS_PORT_RETRY_COUNT` | `100` | int | Port bind retry count |
| `RAY_SERVE_DIRECT_INGRESS_MIN_DRAINING_PERIOD_S` | `30.0` | float | Min draining period |

### 15.17. HAProxy Configuration

These configure an HAProxy sidecar for high-performance load balancing.

| Variable | Default | Type | Description |
|----------|---------|------|-------------|
| `RAY_SERVE_ENABLE_HA_PROXY` | `"0"` (False) | bool | Enable HAProxy sidecar |
| `RAY_SERVE_HAPROXY_MAXCONN` | `20000` | int | Max connections |
| `RAY_SERVE_HAPROXY_NBTHREAD` | `4` | int | HAProxy thread count |
| `RAY_SERVE_HAPROXY_CONFIG_FILE_LOC` | `/tmp/haproxy-serve/haproxy.cfg` | str | Config file location |
| `RAY_SERVE_HAPROXY_SOCKET_PATH` | `/tmp/haproxy-serve/admin.sock` | str | Admin socket path |
| `RAY_SERVE_ENABLE_HAPROXY_OPTIMIZED_CONFIG` | `"1"` (True) | bool | Use optimized config |
| `RAY_SERVE_HAPROXY_SERVER_STATE_BASE` | `/tmp/haproxy-serve` | str | Server state base dir |
| `RAY_SERVE_HAPROXY_SERVER_STATE_FILE` | `/tmp/haproxy-serve/server-state` | str | Server state file |
| `RAY_SERVE_HAPROXY_HARD_STOP_AFTER_S` | `120` | int | Hard stop timeout |
| `RAY_SERVE_HAPROXY_METRICS_PORT` | `9101` | int | Prometheus metrics port |
| `RAY_SERVE_HAPROXY_SYSLOG_PORT` | `514` | int | Syslog port |
| `RAY_SERVE_HAPROXY_TIMEOUT_SERVER_S` | None | int | Server timeout |
| `RAY_SERVE_HAPROXY_TIMEOUT_CONNECT_S` | None | int | Connect timeout |
| `RAY_SERVE_HAPROXY_TIMEOUT_CLIENT_S` | `3600` | int | Client timeout |
| `RAY_SERVE_HAPROXY_HEALTH_CHECK_FALL` | `2` | int | Failures before down |
| `RAY_SERVE_HAPROXY_HEALTH_CHECK_RISE` | `2` | int | Successes before up |
| `RAY_SERVE_HAPROXY_HEALTH_CHECK_INTER` | `"5s"` | str | Check interval |
| `RAY_SERVE_HAPROXY_HEALTH_CHECK_FASTINTER` | `"250ms"` | str | Fast interval (transitioning) |
| `RAY_SERVE_HAPROXY_HEALTH_CHECK_DOWNINTER` | `"250ms"` | str | Down interval |

### 15.18. KV / GCS & Callbacks

| Variable | Default | Type | Description |
|----------|---------|------|-------------|
| `RAY_SERVE_KV_TIMEOUT_S` | None | float | KV (GCS) operation timeout |
| `RAY_SERVE_HTTP_PROXY_CALLBACK_IMPORT_PATH` | None | str | Custom HTTP proxy callback |
| `RAY_SERVE_CONTROLLER_CALLBACK_IMPORT_PATH` | None | str | Custom controller callback |

### 15.19. Metrics Export & Event Loop

| Variable | Default | Type | Description |
|----------|---------|------|-------------|
| `RAY_SERVE_METRICS_EXPORT_INTERVAL_MS` | `100` | int | Metrics export interval |
| `RAY_SERVE_EVENT_LOOP_MONITORING_INTERVAL_S` | `5.0` | float | Event loop monitoring interval |

---

## 16. Ray Serve LLM (23 vars)

Source: `python/ray/llm/_internal/serve/constants.py`

| Variable | Default | Type | HPC Rec | Description |
|----------|---------|------|---------|-------------|
| `RAY_SERVE_LLM_DEFAULT_HEALTH_CHECK_PERIOD_S` | `10` | int | Keep default | LLM replica health check period |
| `RAY_SERVE_LLM_DEFAULT_HEALTH_CHECK_TIMEOUT_S` | `10` | int | **`30`** for large models | Health check timeout |
| `RAY_SERVE_LLM_DEFAULT_MAX_ONGOING_REQUESTS` | `10^9` | int | Keep default | Max concurrent requests |
| `RAY_SERVE_LLM_DEFAULT_MAX_REPLICAS` | `10` | int | Match GPU count | Max autoscale replicas |
| `RAY_SERVE_LLM_DEFAULT_MAX_TARGET_ONGOING_REQUESTS` | `10^9` | int | Keep default | |
| `RAY_SERVE_LLM_ROUTER_HTTP_TIMEOUT` | `600` | float | **`1200`** for large models | HTTP timeout for LLM router |
| `RAY_SERVE_LLM_ROUTER_TO_MODEL_REPLICA_RATIO` | `2.0` | float | Keep default | Router-to-replica ratio |
| `RAY_SERVE_LLM_ROUTER_MIN_REPLICAS` | `2` | int | Keep default | Min router replicas |
| `RAY_SERVE_LLM_ROUTER_INITIAL_REPLICAS` | `2` | int | Keep default | Initial router replicas |
| `RAY_SERVE_LLM_ROUTER_MAX_REPLICAS` | `1000` | int | Keep default | Max router replicas |
| `RAY_SERVE_LLM_ROUTER_TARGET_ONGOING_REQUESTS` | `10^9` | int | Keep default | |
| `RAYLLM_ENGINE_START_TIMEOUT_S` | `3600` | int | Keep or increase for very large models | Engine startup timeout |
| `RAYLLM_MODEL_RESPONSE_BATCH_TIMEOUT_MS` | `50` | float | Keep default | Response batching timeout |
| `RAYLLM_ALLOW_NEW_PLACEMENT_GROUPS_IN_DEPLOYMENT` | `1` | int | Keep default | |
| `RAYLLM_ENABLE_WORKER_PROCESS_SETUP_HOOK` | `"1"` (True) | bool | Keep default | |
| `RAYLLM_ENABLE_REQUEST_PROMPT_LOGS` | `"1"` (True) | bool | `"0"` in production | Prompt logging |
| `RAYLLM_GUIDED_DECODING_BACKEND` | `"xgrammar"` | str | Keep default | |
| `RAYLLM_MAX_NUM_STOPPING_SEQUENCES` | `8` | int | Keep default | |
| `RAYLLM_ENABLE_VERBOSE_TELEMETRY` | `"0"` (False) | bool | `"1"` for debugging | |
| `RAYLLM_VLLM_ENGINE_CLS` | (stores env var name) | str | For custom vLLM engine class | |
| `RAYLLM_HOME_DIR` | `~/.ray/llm` | str | Keep default | |
| `DEFAULT_MULTIPLEX_DOWNLOAD_TIMEOUT_S` | `30.0` | float | Keep default | Multiplex model download timeout |
| `DEFAULT_MULTIPLEX_DOWNLOAD_RETRIES` | `3` | int | Keep default | |

---

## 17. Ray Data — Complete Configuration (71 vars)

Source: `python/ray/data/context.py` and various files under `python/ray/data/_internal/`

### 17.1. Shuffle & Parallelism

| Variable | Default | Type | HPC Rec | Description |
|----------|---------|------|---------|-------------|
| `RAY_DATA_DEFAULT_SHUFFLE_STRATEGY` | `"hash_shuffle"` | str | Keep default | Shuffle algorithm |
| `RAY_DATA_MAX_HASH_SHUFFLE_AGGREGATORS` | `128` | int | **`256`** on large clusters | Max aggregating actors for hash-shuffle |
| `RAY_DATA_DEFAULT_HASH_SHUFFLE_AGGREGATOR_MAX_CONCURRENCY` | `8` | int | Keep default | Per-aggregator concurrency |
| `RAY_DATA_DEFAULT_HASH_SHUFFLE_AGGREGATOR_MEMORY_ALLOCATION` | `1 GiB` | int | Keep default | Memory per aggregator |
| `RAY_DATA_DEFAULT_MIN_PARALLELISM` | `200` | int | **`500`** for large datasets | Min read output blocks |
| `RAY_DATA_PUSH_BASED_SHUFFLE` | None (disabled) | bool | Deprecated — use strategy | |
| `RAY_DATA_MIN_HASH_SHUFFLE_AGGREGATOR_WAIT_TIME_IN_S` | `300` | int | Keep default | |
| `RAY_DATA_HASH_SHUFFLE_AGGREGATOR_HEALTH_WARNING_INTERVAL_S` | `30` | int | Keep default | |

### 17.2. Resource Management

| Variable | Default | Type | HPC Rec | Description |
|----------|---------|------|---------|-------------|
| `RAY_DATA_ENABLE_OP_RESOURCE_RESERVATION` | `True` | bool | Keep `True` | Resource reservation per operator |
| `RAY_DATA_OP_RESERVATION_RATIO` | `0.5` | float | Keep default | Resource reservation ratio |
| `RAY_DATA_OBJECT_STORE_MEMORY_LIMIT_FRACTION` | `0.5` | float | **`0.7`** with large RAM | Object store memory limit fraction |
| `RAY_DATA_USE_OP_RESOURCE_ALLOCATOR_VERSION` | `"V1"` | str | Keep default | Resource allocator version |
| `RAY_DATA_DEFAULT_WAIT_FOR_MIN_ACTORS_S` | `-1` (disabled) | int | Keep default | Min actors wait timeout |
| `RAY_DATA_ACTOR_DEFAULT_MAX_TASKS_IN_FLIGHT_TO_MAX_CONCURRENCY_FACTOR` | `2` | int | Keep default | Tasks-in-flight factor |

### 17.3. Backpressure & Scaling

| Variable | Default | Type | HPC Rec | Description |
|----------|---------|------|---------|-------------|
| `RAY_DATA_ENABLE_DYNAMIC_OUTPUT_QUEUE_SIZE_BACKPRESSURE` | `False` | bool | Keep default | Dynamic output queue backpressure |
| `RAY_DATA_DOWNSTREAM_CAPACITY_BACKPRESSURE_RATIO` | `10.0` | float | Keep default | |
| `RAY_DATA_CONCURRENCY_CAP_EWMA_ALPHA` | `0.1` | float | Keep default | EWMA smoothing factor |
| `RAY_DATA_CONCURRENCY_CAP_K_DEV` | `1.0` | float | Keep default | Deadband width |
| `RAY_DATA_CONCURRENCY_CAP_BACKOFF_FACTOR` | `1` | float | Keep default | |
| `RAY_DATA_CONCURRENCY_CAP_RAMPUP_FACTOR` | `1` | float | Keep default | |
| `RAY_DATA_CONCURRENCY_CAP_AVAILABLE_OBJECT_STORE_BUDGET_THRESHOLD` | `0.1` | float | Keep default | |
| `RAY_DATA_DOWNSTREAM_CAPACITY_OBJECT_STORE_BUDGET_UTIL_THRESHOLD` | `0.9` | float | Keep default | |
| `RAY_DATA_DEFAULT_ACTOR_POOL_UTIL_UPSCALING_THRESHOLD` | `1.75` | float | Keep default | |
| `RAY_DATA_DEFAULT_ACTOR_POOL_UTIL_DOWNSCALING_THRESHOLD` | `0.5` | float | Keep default | |
| `RAY_DATA_DEFAULT_ACTOR_POOL_MAX_UPSCALING_DELTA` | `1` | int | Keep default | |
| `RAY_DATA_DEFAULT_OUTPUT_SPLITTER_MAX_BUFFERING_FACTOR` | `2` | float | Keep default | |

### 17.4. Arrow & Block Configuration

| Variable | Default | Type | Description |
|----------|---------|------|-------------|
| `RAY_DATA_PANDAS_BLOCK_IGNORE_METADATA` | `False` | bool | Ignore pandas metadata in Arrow conversions |
| `RAY_DATA_ENABLE_TENSOR_EXTENSION_CASTING` | `True` | bool | Auto-cast NumPy columns to tensor extension |
| `RAY_DATA_USE_ARROW_TENSOR_V2` | `True` | bool | V2 ArrowTensorArray for >2 GB tensors |
| `RAY_DATA_ARROW_MAX_CHUNK_SIZE_BYTES` | `4 MiB` | int | Arrow-to-batch chunk size |
| `RAY_DATA_MAX_UNCOMPACTED_SIZE_BYTES` | `128 MiB` | int | Buffer before compacting |
| `RAY_DATA_MIN_NUM_CHUNKS_TO_TRIGGER_COMBINE_CHUNKS` | `10` | int | Chunk combine trigger |
| `RAY_DATA_ARROW_EXTENSION_SERIALIZATION_LEGACY_JSON_FORMAT` | `0` | int | Use legacy JSON serialization |
| `RAY_EXTENSION_SERIALIZATION_CACHE_MAXSIZE` | `100000` | int | Serialization cache size |
| `RAY_DATA_ENFORCE_SCHEMAS` | `False` | bool | Enforce schema consistency |
| `RAY_DATA_AUTOLOAD_PYEXTENSIONTYPE` | `False` | bool | Auto-load PyArrow extension types |

### 17.5. Progress Bars & Logging

| Variable | Default | Type | Description |
|----------|---------|------|-------------|
| `RAY_TQDM` | `"1"` (True) | bool | Enable distributed tqdm |
| `RAY_DATA_DISABLE_PROGRESS_BARS` | `0` (enabled) | int | `1` to globally disable |
| `RAY_DATA_ENABLE_PROGRESS_BAR_NAME_TRUNCATION` | `True` | bool | Truncate long progress bar names |
| `RAY_DATA_ENABLE_RICH_PROGRESS_BARS` | `0` (False) | int | Enable rich progress bars |
| `RAY_DATA_VERBOSE_PROGRESS` | `"1"` (True) | bool | Verbose execution progress |
| `RAY_DATA_NON_TTY_PROGRESS_LOG_INTERVAL` | `10` | int | Progress log interval for non-TTY |
| `RAY_DATA_LOGGING_CONFIG` | None | str | Custom YAML logging config path |
| `RAY_DATA_LOG_ENCODING` | None | str | Log encoding format (e.g., `"JSON"`) |
| `RAY_DATA_LOG_INTERNAL_STACK_TRACE_TO_STDOUT` | `False` | bool | Include internal stack frames |

### 17.6. Memory & Debugging

| Variable | Default | Type | Description |
|----------|---------|------|-------------|
| `RAY_DATA_EAGER_FREE` | `"0"` (False) | bool | Eager memory free |
| `RAY_DATA_TRACE_ALLOCATIONS` | `"0"` (False) | bool | Trace allocations (significant overhead) |
| `RAY_DATA_DEBUG_RESOURCE_MANAGER` | None | bool | Debug resource manager telemetry |
| `RAY_DATA_RAISE_ORIGINAL_MAP_EXCEPTION` | `False` | bool | Raise original UDF exceptions |
| `RAY_DATA_PER_NODE_METRICS` | `"0"` (False) | bool | Per-node metrics reporting |

### 17.7. Iceberg Integration

| Variable | Default | Type | Description |
|----------|---------|------|-------------|
| `RAY_DATA_ICEBERG_WRITE_FILE_MAX_ATTEMPTS` | `10` | int | Write retry attempts |
| `RAY_DATA_ICEBERG_WRITE_FILE_RETRY_MAX_BACKOFF_S` | `32` | int | Write retry max backoff |
| `RAY_DATA_ICEBERG_CATALOG_MAX_ATTEMPTS` | `5` | int | Catalog retry attempts |
| `RAY_DATA_ICEBERG_CATALOG_RETRY_MAX_BACKOFF_S` | `16` | int | Catalog retry max backoff |

### 17.8. Autoscaler (Ray Data)

| Variable | Default | Type | Description |
|----------|---------|------|-------------|
| `RAY_DATA_CLUSTER_AUTOSCALER` | `"V2"` | str | Autoscaler version |
| `RAY_DATA_CLUSTER_SCALING_UP_UTIL_THRESHOLD` | `0.75` | float | Utilization threshold to scale up |
| `RAY_DATA_CLUSTER_UTIL_AVG_WINDOW_S` | `10` | int | Averaging window |
| `RAY_DATA_CLUSTER_SCALING_UP_DELTA` | `1` | int | Nodes to add per scaling decision |
| `RAY_DATA_MIN_GAP_BETWEEN_AUTOSCALING_REQUESTS` | `10` | int | Min gap between requests |
| `RAY_DATA_AUTOSCALING_REQUEST_EXPIRE_TIME_S` | `180` | int | Request expiry |
| `RAY_DATA_DISABLE_AUTOSCALER_LOGGING` | `False` | bool | Disable autoscaler logs |
| `RAY_DATA_AUTOSCALING_COORDINATOR_REQUEST_GET_TIMEOUT_S` | `5` | int | Coordinator timeout |
| `RAY_DATA_AUTOSCALING_COORDINATOR_MAX_CONSECUTIVE_FAILURES` | `10` | int | Max consecutive failures |

### 17.9. UDF & Threadpool

| Variable | Default | Type | Description |
|----------|---------|------|-------------|
| `RAY_DATA_DEFAULT_ASYNC_ROW_UDF_MAX_CONCURRENCY` | `16` | int | Async row UDF concurrency |
| `RAY_DATA_DEFAULT_ASYNC_BATCH_UDF_MAX_CONCURRENCY` | `4` | int | Async batch UDF concurrency |
| `RAY_DATA_MAX_FORMAT_THREADPOOL_NUM_WORKERS` | `4` | int | Batch formatting threadpool |
| `RAY_DATA_DEFAULT_COLLATE_FN_THREADPOOL_MAX_WORKERS` | `4` | int | Collate function threadpool |
| `RAY_DATA_EXECUTION_CALLBACKS` | `""` | str | Execution callback classes |

### 17.10. Miscellaneous

| Variable | Default | Type | Description |
|----------|---------|------|-------------|
| `RAY_DATA_CHECKPOINT_PATH_BUCKET` | None | str | Cloud/FS bucket for checkpoints |
| `RAY_DISABLE_PYARROW_VERSION_CHECK` | `"0"` | bool | Skip PyArrow version check |
| `RAY_DOC_BUILD` | `"0"` | bool | Doc build mode (skips version check) |

---

## 18. Ray Train v2 — Distributed Training

Source: `python/ray/train/v2/_internal/constants.py`

| Variable | Default | Type | HPC Recommendation | Description |
|----------|---------|------|---------------------|-------------|
| `RAY_TRAIN_V2_ENABLED` | `True` | bool | Keep `True` | Enable Train v2 |
| `RAY_TRAIN_HEALTH_CHECK_INTERVAL_S` | `2.0` | float | **`5.0`** on large GPU clusters | Controller poll interval |
| `RAY_TRAIN_WORKER_HEALTH_CHECK_TIMEOUT_S` | `600` (10 min) | float | **`1200` (20 min)** | Workers under heavy NCCL allreduce may be unresponsive for minutes |
| `RAY_TRAIN_WORKER_GROUP_START_TIMEOUT_S` | `60` | float | **`300`** for large clusters | GPU actor placement takes time |
| `RAY_TRAIN_COLLECTIVE_TIMEOUT_S` | `-1` (no timeout) | float | Keep `-1` or `3600` | Collective operation timeout |
| `RAY_TRAIN_COLLECTIVE_WARN_INTERVAL_S` | `60` | float | Keep default | Warning interval for hanging collectives |
| `RAY_TRAIN_STATE_ACTOR_RECONCILIATION_INTERVAL_S` | `30` | float | Keep default | State actor polling |
| `RAY_TRAIN_ENABLE_STATE_ACTOR_RECONCILIATION` | `True` | bool | Keep `True` | |
| `RAY_TRAIN_ENABLE_PRINT_PATCH` | `True` | bool | Keep `True` | Patch print() for distributed logging |
| `RAY_TRAIN_ENABLE_CONTROLLER_STRUCTURED_LOGGING` | `True` | bool | Keep `True` | |
| `RAY_TRAIN_ENABLE_WORKER_STRUCTURED_LOGGING` | `True` | bool | Keep `True` | |
| `RAY_TRAIN_METRICS_ENABLED` | unset | bool | `1` for monitoring | Train metrics |
| `RAY_TRAIN_CALLBACKS` | unset | str | Custom callbacks | |

### 18.2. Train Internal Backend Configuration

Source: `python/ray/train/_internal/backend_executor.py`

| Variable | Default | Type | HPC Recommendation | Description |
|----------|---------|------|---------------------|-------------|
| `RAY_TRAIN_ENABLE_STATE_TRACKING` | unset | str | `"1"` for monitoring | Enable state tracking in backend executor |
| `TRAIN_ENABLE_SHARE_CUDA_VISIBLE_DEVICES` | unset | str | Keep default | Share `CUDA_VISIBLE_DEVICES` across workers on same node |
| `TRAIN_ENABLE_SHARE_HIP_VISIBLE_DEVICES` | unset | str | Keep default | Share `HIP_VISIBLE_DEVICES` across workers |
| `TRAIN_ENABLE_SHARE_NEURON_RT_VISIBLE_CORES` | unset | str | Keep default | Share `NEURON_RT_VISIBLE_CORES` across workers |
| `TRAIN_ENABLE_SHARE_ASCEND_RT_VISIBLE_DEVICES` | unset | str | Keep default | Share `ASCEND_RT_VISIBLE_DEVICES` across workers |
| `TRAIN_ENABLE_WORKER_SPREAD` | unset | str | Keep default | Spread workers across nodes |
| `TRAIN_PLACEMENT_GROUP_TIMEOUT_S` | `100` | int | **`300`** for large GPU clusters | Placement group creation timeout |

### 18.3. Torch Distributed Configuration

Source: `python/ray/train/torch/config.py`, `python/ray/train/v2/execution/local_mode/torch.py`

These are set **by Ray Train** on worker processes when initializing PyTorch distributed:

| Variable | Default | Type | Description |
|----------|---------|------|-------------|
| `NCCL_ASYNC_ERROR_HANDLING` | `"1"` | str | Enable async error handling for NCCL (or `TORCH_NCCL_ASYNC_ERROR_HANDLING`) |
| `NCCL_BLOCKING_WAIT` | `"1"` | str | Enable blocking wait for NCCL (or `TORCH_NCCL_BLOCKING_WAIT`) |
| `TORCH_PROCESS_GROUP_SHUTDOWN_TIMEOUT_S` | unset | str | Torch process group shutdown timeout |
| `RANK` | set by Ray | int | Global worker rank |
| `LOCAL_RANK` | set by Ray | int | Node-local worker rank |
| `WORLD_SIZE` | set by Ray | int | Total number of workers |
| `LOCAL_WORLD_SIZE` | set by Ray | int | Workers on this node |
| `MASTER_ADDR` | set by Ray | str | Master node address for distributed init |
| `MASTER_PORT` | set by Ray | str | Master node port for distributed init |

### 18.4. Torch XLA / Neuron Configuration

Source: `python/ray/train/torch/xla/config.py`

| Variable | Default | Type | Description |
|----------|---------|------|-------------|
| `LOCAL_RANK` | derived | str | Local rank for XLA workers |
| `NEURON_CC_FLAGS` | derived | str | Neuron compiler flags (set by Ray for Neuron XLA) |

### 18.5. JAX Distributed Configuration

Source: `python/ray/train/v2/jax/config.py`

| Variable | Default | Type | Description |
|----------|---------|------|-------------|
| `JAX_PLATFORMS` | derived | str | JAX platform selection (e.g., `"tpu"`, `"gpu"`) |
| `JAX_DISTRIBUTED_SHUTDOWN_TIMEOUT_S` | unset | str | JAX distributed shutdown timeout |

### 18.6. Accelerator Sharing in Callbacks

Source: `python/ray/train/v2/callbacks/accelerators.py`

| Variable | Default | Type | Description |
|----------|---------|------|-------------|
| `TRAIN_ENABLE_SHARE_CUDA_VISIBLE_DEVICES` | unset | str | Also read in v2 callback for GPU sharing |

---

## 19. Ray Tune — Hyperparameter Tuning

Source: `python/ray/tune/execution/tune_controller.py` and `python/ray/tune/tune.py`

| Variable | Default | Type | HPC Recommendation | Description |
|----------|---------|------|---------------------|-------------|
| `TUNE_RESULT_BUFFER_LENGTH` | `1` | int | **`10–50`** for high-throughput | Buffer more results before processing |
| `TUNE_RESULT_BUFFER_MIN_TIME_S` | `0.0` | float | **`1.0`** | Minimum buffering time |
| `TUNE_RESULT_BUFFER_MAX_TIME_S` | `100.0` | float | Keep default | Maximum buffering time |
| `TUNE_GLOBAL_CHECKPOINT_S` | `"auto"` | str | Keep default or explicit interval | Global checkpoint frequency |
| `TUNE_MAX_PENDING_TRIALS_PG` | `"auto"` | str | Keep default | Max pending trial placement groups |
| `TUNE_DISABLE_SIGINT_HANDLER` | `0` | int | **`1`** in Slurm/batch jobs | Disable SIGINT handler (Slurm signal forwarding) |

---

## 20. Collective Communication (NCCL / Gloo)

Source: `python/ray/util/collective/const.py` and `python/ray/util/collective/collective.py`

| Variable | Default | Type | HPC Recommendation | Description |
|----------|---------|------|---------------------|-------------|
| `NCCL_USE_MULTISTREAM` | `True` | bool | Keep `True` | Multi-stream NCCL for compute-comm overlap |
| `NCCL_SOCKET_IFNAME` | None | str | Set to IB interface (e.g., `ib0`) | Restrict NCCL to InfiniBand |
| `NCCL_IB_DISABLE` | `0` | str | Keep `0` | Ensure IB is used |
| `NCCL_NET_GDR_LEVEL` | auto | str | `5` (PIX) for optimal topology | GPU Direct RDMA level |
| `NCCL_P2P_DISABLE` | `0` | str | Keep `0` | P2P transfers |
| `NCCL_CROSS_NIC` | `0` | str | `1` for multi-rail | Cross-NIC communication |
| `NCCL_IB_HCA` | auto | str | Set to specific HCA if needed | InfiniBand HCA selection |

---

## 21. Accelerator Visibility & Device Management

Ray automatically manages per-worker accelerator device visibility environment variables. Each accelerator type has:
1. A **visibility env var** that Ray sets per-worker (e.g., `CUDA_VISIBLE_DEVICES`)
2. A **`RAY_EXPERIMENTAL_NOSET_*`** override to prevent Ray from setting the visibility var

Source: `python/ray/_private/accelerators/`

### 21.1. NVIDIA GPU

| Variable | Default | Type | Description |
|----------|---------|------|-------------|
| `CUDA_VISIBLE_DEVICES` | system | str | **Managed automatically by Ray per-worker.** Do NOT set globally. |
| `RAY_EXPERIMENTAL_NOSET_CUDA_VISIBLE_DEVICES` | unset | str | `"1"` to prevent Ray from setting `CUDA_VISIBLE_DEVICES` |

### 21.2. AMD GPU (ROCm)

| Variable | Default | Type | Description |
|----------|---------|------|-------------|
| `HIP_VISIBLE_DEVICES` | system | str | AMD ROCm GPU visibility (managed by Ray) |
| `ROCR_VISIBLE_DEVICES` | system | str | AMD ROCr GPU visibility (managed by Ray) |
| `RAY_EXPERIMENTAL_NOSET_HIP_VISIBLE_DEVICES` | unset | str | `"1"` to prevent Ray from setting HIP/ROCr vars |

### 21.3. Intel GPU (oneAPI)

| Variable | Default | Type | Description |
|----------|---------|------|-------------|
| `ONEAPI_DEVICE_SELECTOR` | system | str | Intel GPU visibility via oneAPI (managed by Ray) |
| `RAY_EXPERIMENTAL_NOSET_ONEAPI_DEVICE_SELECTOR` | unset | str | `"1"` to prevent Ray from setting it |

### 21.4. Habana Gaudi (HPU)

| Variable | Default | Type | Description |
|----------|---------|------|-------------|
| `HABANA_VISIBLE_MODULES` | system | str | Habana Gaudi HPU visibility (managed by Ray) |
| `RAY_EXPERIMENTAL_NOSET_HABANA_VISIBLE_MODULES` | unset | str | `"1"` to prevent Ray from setting it |

### 21.5. AWS Neuron

| Variable | Default | Type | Description |
|----------|---------|------|-------------|
| `NEURON_RT_VISIBLE_CORES` | system | str | Neuron accelerator core visibility (managed by Ray) |
| `RAY_EXPERIMENTAL_NOSET_NEURON_RT_VISIBLE_CORES` | unset | str | `"1"` to prevent Ray from setting it |

### 21.6. Huawei Ascend NPU

| Variable | Default | Type | Description |
|----------|---------|------|-------------|
| `ASCEND_RT_VISIBLE_DEVICES` | system | str | Ascend NPU visibility (managed by Ray) |
| `RAY_EXPERIMENTAL_NOSET_ASCEND_RT_VISIBLE_DEVICES` | unset | str | `"1"` to prevent Ray from setting it |

### 21.7. MetaX GPU

MetaX GPUs use CUDA-compatible device selection:

| Variable | Default | Type | Description |
|----------|---------|------|-------------|
| `CUDA_VISIBLE_DEVICES` | system | str | MetaX GPU visibility (same env var as NVIDIA) |
| `RAY_EXPERIMENTAL_NOSET_CUDA_VISIBLE_DEVICES` | unset | str | `"1"` to prevent Ray from setting it |

### 21.8. Rebellions (RBLN)

| Variable | Default | Type | Description |
|----------|---------|------|-------------|
| `RBLN_DEVICES` | system | str | RBLN device visibility (managed by Ray) |
| `RAY_EXPERIMENTAL_NOSET_RBLN_RT_VISIBLE_DEVICES` | unset | str | `"1"` to prevent Ray from setting it |

### 21.9. Google TPU

| Variable | Default | Type | Description |
|----------|---------|------|-------------|
| `TPU_VISIBLE_CHIPS` | system | str | TPU chip visibility (managed by Ray) |
| `RAY_EXPERIMENTAL_NOSET_TPU_VISIBLE_CHIPS` | unset | str | `"1"` to prevent Ray from setting it |
| `TPU_CHIPS_PER_HOST_BOUNDS` | system | str | TPU topology: chips per host bounds |
| `TPU_HOST_BOUNDS` | system | str | TPU topology: host bounds |
| `TPU_ACCELERATOR_TYPE` | system | str | TPU accelerator type (e.g., `"v4-8"`) |
| `TPU_TOPOLOGY` | system | str | TPU topology string |
| `TPU_WORKER_ID` | system | str | TPU worker ID in multi-host setup |
| `TPU_NAME` | system | str | TPU name identifier |

### 21.10. General Accelerator Settings

| Variable | Default | Type | Description |
|----------|---------|------|-------------|
| `RAY_ACCEL_ENV_VAR_OVERRIDE_ON_ZERO` | unset | str | Override accelerator env vars when set to 0 |
| `RAY_TPU_MAX_CONCURRENT_ACTIVE_CONNECTIONS` | unset | str | Max concurrent TPU connections |
| `RAY_predefined_unit_instance_resources` | `"GPU"` | str (C++) | Predefined unit instance resources |
| `RAY_custom_unit_instance_resources` | `"neuron_cores,TPU,NPU,HPU,RBLN"` | str (C++) | Custom unit resources |

---

## 22. Runtime Environment

### 22.1. Core Settings

| Variable | Default | Type | HPC Recommendation | Source |
|----------|---------|------|---------------------|--------|
| `RAY_RUNTIME_ENV` | None | str | Set if all workers need specific env | `ray_constants.py` |
| `RAY_RUNTIME_ENV_HOOK` | None | str | Custom hook module path | `ray_constants.py` |
| `RAY_START_HOOK` | None | str | Hook on `ray start` | `ray_constants.py` |
| `RAY_JOB_SUBMIT_HOOK` | None | str | Hook on `ray job submit` | `ray_constants.py` |
| `RAY_RUNTIME_ENV_CREATE_WORKING_DIR` | None | str | Working dir for runtime env | `ray_constants.py` |
| `RAY_RUNTIME_ENV_TEMPORARY_REFERENCE_EXPIRATION_S` | `600` | int | **`1200`** for large envs | `ray_constants.py` |
| `RAY_RUNTIME_ENV_IGNORE_GITIGNORE` | unset | str | `"1"` to ignore .gitignore | `ray_constants.py` |
| `RAY_OVERRIDE_RUNTIME_ENV_DEFAULT_EXCLUDES` | `.git,.venv,venv,__pycache__` | str | Customize if needed | `ray_constants.py` |
| `RAY_OVERRIDE_JOB_RUNTIME_ENV` | `0` | str | Keep `0` | `worker.py` |
| `RAY_ENABLE_UV_RUN_RUNTIME_ENV` | `True` | bool | `True` if using `uv` | `ray_constants.py` |
| `RAY_runtime_env_skip_local_gc` | `false` | bool (C++) | Keep default | Skip local GC for runtime envs |

### 22.2. Plugins & Caching

| Variable | Default | Type | Description | Source |
|----------|---------|------|-------------|--------|
| `RAY_RUNTIME_ENV_PLUGINS` | None | str | JSON list of plugin class import paths | `plugin.py` |
| `RAY_RUNTIME_ENV_{PLUGIN_NAME}_CACHE_SIZE_GB` | None | float | Per-plugin cache size limit in GB | `plugin.py` |
| `RAY_RUNTIME_ENV_LOCAL_DEV_MODE` | unset | str | `"1"` to skip conda/pip install (local dev) | `conda.py` |

### 22.3. Remote Package Upload / Protocol

| Variable | Default | Type | Description | Source |
|----------|---------|------|-------------|--------|
| `RAY_RUNTIME_ENV_HTTP_USER_AGENT` | None | str | Custom User-Agent for HTTP downloads | `protocol.py` |
| `RAY_RUNTIME_ENV_BEARER_TOKEN` | None | str | Bearer token for authenticated downloads | `protocol.py` |
| `AZURE_STORAGE_ACCOUNT` | None | str | Azure storage account for runtime env packages | `protocol.py` |
| `RAY_RUNTIME_ENV_FAIL_UPLOAD_FOR_TESTING` | unset | str | Test only — force upload failure | `packaging.py` |
| `RAY_RUNTIME_ENV_FAIL_DOWNLOAD_FOR_TESTING` | unset | str | Test only — force download failure | `packaging.py` |

### 22.4. Agent & Setup Hooks

| Variable | Default | Type | Description | Source |
|----------|---------|------|-------------|--------|
| `RAY_WORKER_PROCESS_SETUP_HOOK_LOAD_TIMEOUT` | unset | int | Timeout for loading worker setup hooks | `setup_hook.py` |
| `RAY_RUNTIME_ENV_SLEEP_FOR_TESTING_S` | unset | float | Test only — inject sleep in runtime env agent | `runtime_env_agent.py` |
| `DYLD_LIBRARY_PATH` | system | str | macOS dynamic library path (set in runtime env context) | `context.py` |

---

## 23. Networking, Cluster & Redis

| Variable | Default | Type | HPC Recommendation | Source |
|----------|---------|------|---------------------|--------|
| `RAY_ADDRESS` | None | str | Cluster address for `ray.init()` | `ray_constants.py` |
| `RAY_API_SERVER_ADDRESS` | None | str | API server address | `ray_constants.py` |
| `RAY_NAMESPACE` | None | str | Ray namespace | `ray_constants.py` |
| `RAY_GCS_SERVER_PORT` | None | str | Set via `ray start --port` | `ray_constants.py` |
| `RAY_gcs_server_request_timeout_seconds` | `60` | int (C++) | **`120`** | `ray_config_def.h` |
| `RAY_gcs_storage` | `"memory"` | str (C++) | Keep default | GCS storage backend |
| `RAY_grpc_enable_http_proxy` | `"0"` | str | Keep `"0"` | `ray_constants.py` |
| `RAY_grpc_server_retry_timeout_milliseconds` | `1000` | int64 (C++) | Keep default | |
| `RAY_grpc_client_check_connection_status_interval_milliseconds` | `1000` | int32 (C++) | Keep default | |
| `RAY_redis_db_connect_retries` | `120` | int64 (C++) | Keep default | |
| `RAY_redis_db_connect_wait_milliseconds` | `500` | int64 (C++) | Keep default | |
| `RAY_num_redis_request_retries` | `5` | size_t (C++) | **`10`** | |
| `RAY_redis_retry_base_ms` | `100` | int64 (C++) | Keep default | |
| `RAY_redis_retry_max_ms` | `1000` | int64 (C++) | Keep default | |
| `RAY_HEALTHCHECK_EXPIRATION_S` | `10` | int | Keep default | `ray_constants.py` |
| `RAY_NUM_REDIS_GET_RETRIES` | `20` | int | Keep default | `ray_constants.py` |
| `RAY_CLOUD_INSTANCE_ID` | None | str | Auto-detected | `services.py` |
| `RAY_JOB_HEADERS` | None | JSON str | Headers for Job CLI | `ray_constants.py` |
| `RAY_DISABLE_FAILURE_SIGNAL_HANDLER` | `False` | bool | Keep `False` | `ray_constants.py` |

---

## 24. Dashboard

### 24.1. Core Dashboard Settings

Source: `python/ray/dashboard/consts.py`

| Variable | Default | Type | Description |
|----------|---------|------|-------------|
| `RAY_DASHBOARD_STARTUP_TIMEOUT_S` | `60` | int | Dashboard startup timeout |
| `RAY_DASHBOARD_AGENT_CHECK_PARENT_INTERVAL_S` | `0.4` | float | Agent parent check interval |
| `RAY_STATE_SERVER_MAX_HTTP_REQUEST` | `100` | int | Max in-progress state API requests |
| `RAY_DASHBOARD_STATS_PURGING_INTERVAL` | `600` | int | Stats purging interval |
| `RAY_DASHBOARD_STATS_UPDATING_INTERVAL` | `15` | int | Stats update interval |
| `RAY_DASHBOARD_GCS_RPC_TIMEOUT_SECONDS` | `60` | int | GCS RPC timeout |
| `GCS_CHECK_ALIVE_INTERVAL_SECONDS` | `5` | int | GCS liveness check interval |
| `RAY_DASHBOARD_NO_CACHE` | unset | str | Disable aiohttp cache |
| `RAY_CLUSTER_ACTIVITY_HOOK` | None | str | Cluster activity endpoint hook |
| `CANDIDATE_AGENT_NUMBER` | `1` | int | Candidate agent count |
| `RAY_JOB_ALLOW_DRIVER_ON_WORKER_NODES` | unset | str | Allow driver on worker nodes |
| `RAY_JOB_START_TIMEOUT_SECONDS` | `900` (15 min) | int | JobSupervisor start timeout |
| `DASHBOARD_METRIC_PORT` | `44227` | int | Dashboard Prometheus metrics port |
| `METRICS_RECORD_INTERVAL_S` | `5` | int | Metrics record interval |
| `RAY_enable_pipe_based_agent_to_parent_health_check` | `False` | bool | Pipe-based agent health check |
| `RAY_DASHBOARD_SUBPROCESS_MODULE_WAIT_READY_TIMEOUT` | `30.0` | float | Module ready timeout |
| `RAY_DASHBOARD_SUBPROCESS_MODULE_GRACEFUL_SHUTDOWN_TIMEOUT` | `5.0` | float | Graceful shutdown timeout |
| `RAY_DASHBOARD_SUBPROCESS_MODULE_JOIN_TIMEOUT` | `2.0` | float | Join timeout |
| `RAY_OVERRIDE_DASHBOARD_URL` | None | str | Override dashboard display URL |
| `RAY_DASHBOARD_BUILD_FOLLOW_SYMLINKS` | unset | str | Follow symlinks in dashboard build | 
| `RAY_DASHBOARD_DEV` | unset | str | Enable dashboard development mode |
| `RAY_NODE_ID` | internal | str | Current node ID (set by agent) |

### 24.2. Grafana & Prometheus Metrics Integration

Source: `python/ray/dashboard/modules/metrics/metrics_head.py` and `grafana_dashboard_factory.py`

| Variable | Default | Type | Description |
|----------|---------|------|-------------|
| `RAY_GRAFANA_HOST` | `"http://localhost:3000"` | str | Grafana server URL |
| `RAY_GRAFANA_IFRAME_HOST` | None | str | Grafana iframe URL (if different from API host) |
| `RAY_GRAFANA_ORG_ID` | `1` | int | Grafana organization ID |
| `RAY_GRAFANA_CLUSTER_FILTER` | None | str | Grafana cluster filter value |
| `RAY_PROMETHEUS_HOST` | `"http://localhost:9090"` | str | Prometheus server URL |
| `RAY_PROMETHEUS_NAME` | `"Prometheus"` | str | Prometheus datasource name in Grafana |
| `RAY_PROMETHEUS_HEADERS` | None | JSON str | HTTP headers for Prometheus API calls |
| `RAY_METRICS_OUTPUT_ROOT` | `/tmp/ray/session_latest/metrics` | str | Root directory for exported metrics configs |
| `RAY_METRICS_GRAFANA_DASHBOARD_OUTPUT_DIR` | derived from above | str | Grafana dashboard JSON output dir |
| `RAY_PROMETHEUS_DOWNLOAD_TEST_MODE` | unset | str | Test mode for Prometheus download |

### 24.3. Grafana Dashboard Customization (Per-Dashboard)

Source: `python/ray/dashboard/modules/metrics/grafana_dashboard_factory.py`

These are template-based: replace `{name}` with the dashboard name (`default`, `serve`, `serve_deployment`, `data`, `llm`).

| Variable Pattern | Type | Description |
|-----------------|------|-------------|
| `RAY_GRAFANA_{NAME}_DASHBOARD_UID` | str | Override the UID of a specific Grafana dashboard |
| `RAY_GRAFANA_{NAME}_DASHBOARD_GLOBAL_FILTERS` | str | Override global filter expression per dashboard |
| `RAY_GRAFANA_{NAME}_LOG_LINK_URL` | str | Override log link URL per dashboard |

### 24.4. Reporter Agent

Source: `python/ray/dashboard/modules/reporter/reporter_agent.py`

| Variable | Default | Type | Description |
|----------|---------|------|-------------|
| `KUBERNETES_SERVICE_HOST` | system | str | Presence-checked for K8s detection |
| `RAY_DASHBOARD_ENABLE_K8S_DISK_USAGE` | `"0"` | str | Enable K8s disk usage reporting |
| `RAY_DASHBOARD_REPORTER_AGENT_TPE_MAX_WORKERS` | `2` | int | Reporter agent thread pool max workers |
| `TPU_DEVICE_PLUGIN_ADDR` | None | str | TPU device plugin gRPC address |

### 24.5. Event Module

Source: `python/ray/dashboard/modules/event/event_head.py`

| Variable | Default | Type | Description |
|----------|---------|------|-------------|
| `RAY_DASHBOARD_MAX_EVENTS_TO_CACHE` | `10000` | int | Max events retained in dashboard cache |
| `RAY_DASHBOARD_EVENT_HEAD_TPE_MAX_WORKERS` | `2` | int | Event head thread pool max workers |

### 24.6. Job Manager

Source: `python/ray/dashboard/modules/job/job_manager.py`

| Variable | Default | Type | Description |
|----------|---------|------|-------------|
| `RAY_JOB_START_TIMEOUT_SECONDS` | `900` | int | JobSupervisor start timeout |
| `RAY_STREAM_RUNTIME_ENV_LOG_TO_JOB_DRIVER_LOG` | unset | str | Stream runtime env logs to job driver |
| `RAY_JOB_ALLOW_DRIVER_ON_WORKER_NODES` | unset | str | Allow job driver on non-head nodes |

### 24.7. Aggregator Agent

Source: `python/ray/dashboard/modules/aggregator/aggregator_agent.py`

| Variable | Default | Type | Description |
|----------|---------|------|-------------|
| `RAY_DASHBOARD_AGGREGATOR_AGENT_MAX_BUFFER_SIZE` | varies | int | Max event buffer size |
| `RAY_DASHBOARD_AGGREGATOR_AGENT_BUFFER_FLUSH_INTERVAL_S` | varies | float | Buffer flush interval |
| `RAY_DASHBOARD_AGGREGATOR_AGENT_BATCH_SIZE` | varies | int | Batch size per flush |
| `RAY_DASHBOARD_AGGREGATOR_AGENT_MAX_CONCURRENT_RPCS` | varies | int | Max concurrent RPC calls |
| `RAY_DASHBOARD_AGGREGATOR_AGENT_RPC_TIMEOUT_S` | varies | float | RPC timeout |
| `RAY_DASHBOARD_AGGREGATOR_AGENT_MAX_RETRY` | varies | int | Max retry count |
| `RAY_DASHBOARD_AGGREGATOR_AGENT_RETRY_INTERVAL_S` | varies | float | Retry interval |
| `RAY_DASHBOARD_AGGREGATOR_AGENT_ENABLED` | varies | bool | Enable aggregator agent |

### 24.8. Aggregator Publisher

Source: `python/ray/dashboard/modules/aggregator/publisher/configs.py`

| Variable | Default | Type | Description |
|----------|---------|------|-------------|
| `RAY_DASHBOARD_AGGREGATOR_AGENT_PUBLISHER_BATCH_SIZE` | varies | int | Publisher batch size |
| `RAY_DASHBOARD_AGGREGATOR_AGENT_PUBLISHER_FLUSH_INTERVAL_S` | varies | float | Flush interval |
| `RAY_DASHBOARD_AGGREGATOR_AGENT_PUBLISHER_MAX_BUFFER_SIZE` | varies | int | Max buffer |
| `RAY_DASHBOARD_AGGREGATOR_AGENT_PUBLISHER_MAX_CONCURRENT_RPCS` | varies | int | Max concurrent RPCs |
| `RAY_DASHBOARD_AGGREGATOR_AGENT_PUBLISHER_RPC_TIMEOUT_S` | varies | float | RPC timeout |
| `RAY_DASHBOARD_AGGREGATOR_AGENT_PUBLISHER_MAX_RETRY` | varies | int | Max retries |
| `RAY_DASHBOARD_AGGREGATOR_AGENT_PUBLISHER_RETRY_INTERVAL_S` | varies | float | Retry interval |

### 24.9. Dashboard Client & SDK

Source: `python/ray/dashboard/modules/dashboard_sdk.py`, `python/ray/dashboard/utils.py`

| Variable | Default | Type | Description |
|----------|---------|------|-------------|
| `RAY_API_SERVER_ADDRESS` | None | str | API server address for dashboard SDK |
| `RAY_ADDRESS` | None | str | Ray cluster address (used by SDK as fallback) |
| `RAY_RUNTIME_ENV_IGNORE_GITIGNORE` | unset | str | Ignore .gitignore in working_dir uploads |

---

## 25. Worker & Process Management

### C++ Configs

| Variable | Default | Type | HPC Rec | Description |
|----------|---------|------|---------|-------------|
| `RAY_worker_niceness` | `15` | int | **`0`** | Process niceness |
| `RAY_worker_oom_score_adjustment` | `1000` | int | **`0`** | OOM score adjustment (Linux) |
| `RAY_kill_child_processes_on_worker_exit` | `true` | bool | Keep `true` | Kill child processes on exit |
| `RAY_kill_child_processes_on_worker_exit_with_raylet_subreaper` | `false` | bool | **`true`** | Raylet as subreaper for cleanup (Linux ≥3.4) |
| `RAY_process_group_cleanup_enabled` | `false` | bool | **`true`** | Per-worker process group cleanup |
| `RAY_start_python_gc_manager_thread` | `true` | bool | Keep default | Python GC manager thread |
| `RAY_preload_python_modules` | `""` | str (C++) | `"torch,numpy"` | Preload Python imports in workers |
| `RAY_support_fork` | `false` | bool (C++) | Keep `false` | Fork support |

### Python Configs

| Variable | Default | Type | Source | Description |
|----------|---------|------|--------|-------------|
| `__RAY_WORKER_PROCESS_SETUP_HOOK_ENV_VAR` | None | str | `ray_constants.py` | Worker setup hook (internal) |
| `RAY_WORKER_PROCESS_SETUP_HOOK_LOAD_TIMEOUT` | unset | str | `ray_constants.py` | Hook load timeout |
| `RAY_ENABLE_RECORD_ACTOR_TASK_LOGGING` | `False` | bool | `ray_constants.py` | Record actor task log offsets |
| `FUNCTION_SIZE_ERROR_THRESHOLD` | `10^8` | int | `ray_constants.py` | Serialized function size error threshold |
| `RAY_ENABLE_WINDOWS_OR_OSX_CLUSTER` | platform-dependent | bool | `ray_constants.py` | Enable clusters on Windows/macOS |
| `RAY_RESOURCES` | None | JSON str | `resource_and_label_spec.py` | Custom resources |
| `RAY_LABELS` | None | JSON str | `resource_and_label_spec.py` | Node labels |
| `RAY_OVERRIDE_RESOURCES` | None | JSON str | `ray_constants.py` | Override node resources |
| `RAY_OVERRIDE_LABELS` | None | JSON str | `ray_constants.py` | Override node labels |

---

## 26. Autoscaler & Cloud

Relevant only if using Ray's autoscaler (unlikely on static HPC clusters).

### 26.1. C++ Autoscaler Configs

| Variable | Default | Type | Description | Source |
|----------|---------|------|-------------|--------|
| `RAY_enable_autoscaler_v2` | `false` | bool (C++) | Autoscaler v2 | `ray_config_def.h` |
| `RAY_enable_infeasible_task_early_exit` | `false` | bool (C++) | Cancel infeasible tasks | `ray_config_def.h` |

### 26.2. Autoscaler Constants

Source: `python/ray/autoscaler/_private/constants.py`

| Variable | Default | Type | Description |
|----------|---------|------|-------------|
| `RAY_ENABLE_CLUSTER_STATUS_LOG` | `True` | bool | Enable cluster status log |
| `AUTOSCALER_CONSERVE_GPU_NODES` | `1` | int | Conserve GPU nodes (avoid spinning up for non-GPU tasks) |
| `AUTOSCALER_NODE_START_WAIT_S` | `900` (15 min) | int | Wait time for node to start |
| `AUTOSCALER_MAX_NUM_FAILURES` | `5` | int | Max consecutive failures before abort |
| `AUTOSCALER_MAX_LAUNCH_BATCH` | `5` | int | Max nodes per launch batch |
| `AUTOSCALER_MAX_CONCURRENT_LAUNCHES` | `10` | int | Max concurrent node launches |
| `AUTOSCALER_UPDATE_INTERVAL_S` | `5` | int | Autoscaler update interval |
| `AUTOSCALER_HEARTBEAT_TIMEOUT_S` | `30` | int | Heartbeat timeout before marking node dead |
| `BOTO_MAX_RETRIES` | `12` | int | Max AWS Boto API retries |
| `BOTO_CREATE_MAX_RETRIES` | `5` | int | Max Boto retries for create operations |
| `MAX_PARALLEL_SHUTDOWN_WORKERS` | `50` | int | Max parallel worker shutdown |

### 26.3. Autoscaler Monitor

Source: `python/ray/autoscaler/_private/monitor.py`

| Variable | Default | Type | Description |
|----------|---------|------|-------------|
| `RAY_AUTOSCALER_FATESHARE_WORKERS` | `True` | bool | Kill workers when autoscaler exits |
| `AUTOSCALER_LOG_RESOURCE_BATCH_DATA` | `0` | str | Log raw resource batch data |

### 26.4. Fake Multi-Node Cluster (Testing)

Source: `python/ray/autoscaler/_private/fake_multi_node/node_provider.py`

| Variable | Default | Type | Description |
|----------|---------|------|-------------|
| `RAY_TEMPDIR` | None | str | Temp directory for fake cluster |
| `RAY_HOSTDIR` | None | str | Host directory mapping for fake cluster |
| `RAY_HAS_SSH` | None | str | Whether SSH is available |
| `FAKE_CLUSTER_DEV` | None | str | Enable fake cluster dev mode |
| `FAKE_CLUSTER_DEV_MODULES` | None | str | Modules to install in fake cluster |
| `RAY_FAKE_CLUSTER` | None | str | Enable fake cluster mode |

### 26.5. GCP Provider

Source: `python/ray/autoscaler/_private/gcp/config.py`

| Variable | Default | Type | Description |
|----------|---------|------|-------------|
| `RAY_TRIM_AUTOSCALER_SSH_KEYS` | None | str | Trim SSH keys in GCP metadata |

### 26.6. vSphere Provider

Source: `python/ray/autoscaler/_private/vsphere/cluster_operator_client.py`

| Variable | Default | Type | Description |
|----------|---------|------|-------------|
| `VMRAY_CRD_VER` | `"v1alpha1"` | str | VMRay CRD version |
| `VMSERVICE_CRD_VER` | `"v1alpha2"` | str | VMService CRD version |
| `SVC_ACCOUNT_TOKEN` | system | str | Service account token path |

### 26.7. KubeRay / Kubernetes

| Variable | Default | Type | Description |
|----------|---------|------|-------------|
| `KUBERAY_CRD_VER` | `v1alpha1` | str | KubeRay CRD version |
| `KUBERAY_REQUEST_TIMEOUT_S` | `60` | int | KubeRay request timeout |
| `RAY_HEAD_POD_NAME` | None | str | Head pod name |
| `KUBERNETES_SERVICE_HOST` | system | str | K8s service host (presence-checked) |

---

## 27. Usage Stats & Telemetry Reporting

On air-gapped HPC clusters, **disable these**.

| Variable | Default | Type | HPC Recommendation | Source |
|----------|---------|------|---------------------|--------|
| `RAY_USAGE_STATS_ENABLED` | `1` | str | **`0`** | `usage_lib.py` |
| `RAY_USAGE_STATS_REPORT_URL` | `https://usage-stats.ray.io/` | str | N/A if disabled | `usage_constants.py` |
| `RAY_USAGE_STATS_REPORT_INTERVAL_S` | `3600` | int | N/A if disabled | `usage_constants.py` |
| `RAY_USAGE_STATS_CONFIG_PATH` | `~/.ray/config.json` | str | N/A if disabled | `usage_constants.py` |
| `RAY_USAGE_STATS_PROMPT_ENABLED` | `1` | int | **`0`** in headless mode | `usage_constants.py` |
| `RAY_USAGE_STATS_EXTRA_TAGS` | None | str | Custom tracking tags (`;`-separated `key=value`) | `usage_lib.py` |
| `RAY_USAGE_STATS_SOURCE` | `"OSS"` | str | Keep default | `usage_constants.py` |
| `RAY_USAGE_STATS_KUBERAY_IN_USE` | unset | str | K8s indicator | `usage_lib.py` |

---

## 28. Ray AIR Integrations (W&B, MLflow, Comet)

These are checked at import time from `ray.air.integrations.*`:

| Variable | Default | Type | Description |
|----------|---------|------|-------------|
| `WANDB_API_KEY` | None | str | Weights & Biases API key |
| `WANDB_MODE` | `"online"` | str | W&B mode (`"offline"` for air-gapped) |
| `MLFLOW_TRACKING_URI` | None | str | MLflow tracking server URI |
| `COMET_API_KEY` | None | str | Comet ML API key |
| `RAY_AIR_FULL_TRACEBACKS` | unset | str | `"1"` for full stack traces in AIR errors |
| `RAY_STORAGE` | None | str | Default storage URI for AIR (deprecated in favor of `storage_path`) |

---

## 29. Spark & Databricks Integration

Source: `python/ray/util/spark/`

### 29.1. Spark Cluster Init

Source: `python/ray/util/spark/cluster_init.py`

| Variable | Default | Type | Description |
|----------|---------|------|-------------|
| `RAY_ADDRESS` | None | str | Existing Ray cluster address (skip auto-launch) |
| `RAY_ON_SPARK_BACKGROUND_JOB_STARTUP_WAIT` | `30` | int | Wait time for background job startup |
| `RAY_ON_SPARK_RAY_WORKER_NODE_STARTUP_INTERVAL` | `10` | float | Interval between worker node startups |
| `RAY_TMPDIR` | None | str | Override temp directory for Ray on Spark |
| `RAY_ON_SPARK_START_HOOK` | None | str | Custom start hook class import path |
| `DATABRICKS_RUNTIME_VERSION` | None | str | Databricks runtime version (auto-detected) |

### 29.2. Spark Worker Configuration

Source: `python/ray/util/spark/utils.py`

| Variable | Default | Type | Description |
|----------|---------|------|-------------|
| `RAY_ON_SPARK_WORKER_PHYSICAL_MEMORY_BYTES` | auto | int | Physical memory per Ray worker on Spark |
| `RAY_ON_SPARK_WORKER_SHARED_MEMORY_BYTES` | auto | int | Shared memory per Ray worker on Spark |
| `RAY_ON_SPARK_DRIVER_PHYSICAL_MEMORY_BYTES` | auto | int | Physical memory for Ray head on Spark driver |
| `RAY_ON_SPARK_DRIVER_SHARED_MEMORY_BYTES` | auto | int | Shared memory for Ray head on Spark driver |
| `RAY_ON_SPARK_WORKER_CPU_CORES` | auto | int | CPU cores per Ray worker on Spark |
| `RAY_ON_SPARK_WORKER_GPU_NUM` | auto | int | GPUs per Ray worker on Spark |
| `RAY_OBJECT_STORE_ALLOW_SLOW_STORAGE` | `"1"` | str | Allow slow (non-tmpfs) object store on Spark |

### 29.3. Spark Log Collection

Source: `python/ray/util/spark/start_ray_node.py`

| Variable | Default | Type | Description |
|----------|---------|------|-------------|
| `RAY_ON_SPARK_COLLECT_LOG_TO_PATH` | None | str | Path to collect Ray logs |
| `RAY_ON_SPARK_START_RAY_PARENT_PID` | system | str | Parent PID for Spark Ray process monitoring |

### 29.4. Databricks-Specific

Source: `python/ray/util/spark/databricks_hook.py`

| Variable | Default | Type | Description |
|----------|---------|------|-------------|
| `DATABRICKS_HOST` | None | str | Databricks workspace URL |
| `DATABRICKS_TOKEN` | None | str | Databricks API token |
| `DATABRICKS_CLIENT_ID` | None | str | Databricks OAuth client ID |
| `DATABRICKS_CLIENT_SECRET` | None | str | Databricks OAuth client secret |
| `DATABRICKS_RAY_ON_SPARK_AUTOSHUTDOWN_MINUTES` | `30` | int | Idle auto-shutdown timeout on Databricks |

---

## 30. Ray Client Configuration

Source: `python/ray/util/client/worker.py`

| Variable | Default | Type | HPC Recommendation | Description |
|----------|---------|------|---------------------|-------------|
| `RAY_CLIENT_INITIAL_CONNECTION_TIMEOUT_S` | `300` (5 min) | int | Keep default | Initial connection timeout to Ray client server |
| `RAY_CLIENT_MAX_CONNECTION_TIMEOUT_S` | `300` (5 min) | int | Keep default | Max connection timeout on reconnect |
| `RAY_CLIENT_MAX_BLOCKING_OPERATION_TIME_S` | `None` (no limit) | int | Keep None | Max blocking operation timeout |
| `RAY_CLIENT_RECONNECT_GRACE_PERIOD` | `30` | int | **`60`** for HPC networks | Grace period to reconnect before raising error |

---

## 31. Miscellaneous Environment Variables

Env vars that don't fit neatly into other categories:

### 31.1. Serialization

| Variable | Default | Type | Description | Source |
|----------|---------|------|-------------|--------|
| `RAY_PICKLE_VERBOSE_DEBUG` | unset | str | `"1"` for verbose custom pickle debug logging | `cloudpickle/__init__.py` |
| `RAY_DISABLE_CUSTOM_ARROW_DATA_SERIALIZATION` | unset | str | Disable custom Arrow data serialization | `arrow_serialization.py` |
| `RAY_DISABLE_CUSTOM_ARROW_JSON_OPTIONS_SERIALIZATION` | unset | str | Disable custom Arrow JSON options serialization | `arrow_serialization.py` |

### 31.2. Task & Actor Configuration

| Variable | Default | Type | Description | Source |
|----------|---------|------|-------------|--------|
| `RAY_TASK_MAX_RETRIES` | `3` | int | Default max retries for tasks | `remote_function.py` |
| `RAY_ENABLE_AUTO_CONNECT` | `"1"` | str | Auto-connect to running Ray cluster | `auto_init_hook.py` |
| `RAY_CLIENT_MODE` | unset | str | Force Ray client mode | `client_mode_hook.py` |
| `RAY_NAMESPACE` | None | str | Default namespace for `ray.init()` | `client_builder.py` |

### 31.3. Port & Network Configuration

| Variable | Default | Type | Description | Source |
|----------|---------|------|-------------|--------|
| `RAY_USE_RANDOM_PORTS` | unset | str | Use random ports instead of defaults | `parameter.py` |
| `VIRTUAL_ENV` | system | str | Active virtualenv path (detected by Ray) | `parameter.py` |

### 31.4. Ray Serve Agent/Dashboard Addresses

| Variable | Default | Type | Description | Source |
|----------|---------|------|-------------|--------|
| `RAY_AGENT_ADDRESS` | None | str | Agent address for Serve | `serve/_private/` |
| `RAY_DASHBOARD_ADDRESS` | None | str | Dashboard address for Serve | `serve/_private/` |

### 31.5. RLlib Testing Env Vars

| Variable | Default | Type | Description | Source |
|----------|---------|------|-------------|--------|
| `RLLIB_TEST_NO_JAX_IMPORT` | unset | str | Skip JAX import in RLlib tests | `rllib/utils/framework.py` |
| `RLLIB_TEST_NO_TF_IMPORT` | unset | str | Skip TensorFlow import in RLlib tests | `rllib/utils/framework.py` |
| `RLLIB_TEST_NO_TORCH_IMPORT` | unset | str | Skip PyTorch import in RLlib tests | `rllib/utils/framework.py` |
| `TF_CPP_MIN_LOG_LEVEL` | unset | str | TensorFlow C++ log level (set by RLlib) | `rllib/utils/framework.py` |

### 31.6. LLM / Model Serving Extras

| Variable | Default | Type | Description | Source |
|----------|---------|------|-------------|--------|
| `RAYLLM_LORA_STORAGE_URI` | None | str | LoRA adapter storage URI | `ray/llm/` config generator |
| `GIT_COMMIT` | None | str | Git commit hash for LLM metrics | `ray/llm/` fast_api_metrics |

### 31.7. Redis / Node Logging

Source: `python/ray/_private/node.py`

| Variable | Default | Type | Description |
|----------|---------|------|-------------|
| `RAY_REDIS_ADDRESS` | None | str | Redis address override |
| `RAY_ROTATION_MAX_BYTES` | `100 MB` | int | Log rotation max file size (also in C++) |
| `RAY_ROTATION_BACKUP_COUNT` | `5` | int | Log rotation backup count (also in C++) |

### 31.8. Databricks Credentials

Source: `python/ray/_private/databricks_credentials.py`

| Variable | Default | Type | Description |
|----------|---------|------|-------------|
| `DATABRICKS_TOKEN` | None | str | Databricks API token |
| `DATABRICKS_HOST` | None | str | Databricks workspace URL |

---

## 32. Third-Party / System Environment Variables

These are NOT set by Ray but are read by Ray or its dependencies:

| Variable | Description | Notes |
|----------|-------------|-------|
| `OMP_NUM_THREADS` | OpenMP thread count | Auto-set by Ray per worker (see §14) |
| `MKL_NUM_THREADS` | Intel MKL thread count | Not managed by Ray |
| `CUDA_VISIBLE_DEVICES` | GPU visibility | Managed by Ray per worker |
| `NCCL_SOCKET_IFNAME` | NCCL network interface | Set to InfiniBand interface |
| `NCCL_IB_DISABLE` | Disable InfiniBand for NCCL | Keep `0` |
| `NCCL_NET_GDR_LEVEL` | GPU Direct RDMA level | Set for optimal NVLink/IB topology |
| `FI_EFA_USE_DEVICE_RDMA` | AWS EFA device RDMA | Set `1` on EFA |
| `CONDA_EXE` | Conda executable path | Auto-detected |
| `CONDA_PREFIX` | Active conda env prefix | Auto-detected |
| `CONDA_DEFAULT_ENV` | Active conda env name | Auto-detected |
| `KUBERNETES_SERVICE_HOST` | K8s service host | Presence-checked for K8s detection |
| `TURBOPUFFER_API_KEY` | TurboPuffer API key | For vector store integrations |
| `VIRTUAL_ENV` | Python virtualenv path | Detected by Ray for environment info |
| `TMPDIR` | System temp directory | Fallback for `RAY_TMPDIR` |
| `USER` | Current user name | Used by Torch XLA and other integrations |
| `HOME` | User home directory | Fallback for various config paths |
| `DYLD_LIBRARY_PATH` | macOS dynamic library path | Set in runtime env context |

---

## 33. C++ Direct std::getenv Calls

Some C++ files read env vars directly via `std::getenv()` independent of the `RAY_CONFIG` system:

| Variable | Source | Description |
|----------|--------|-------------|
| `RAY_BACKEND_LOG_LEVEL` | `util/logging.cc` | C++ log level |
| `RAY_BACKEND_LOG_JSON` | `util/logging.cc` | Enable C++ JSON logging |
| `RAY_ROTATION_MAX_BYTES` | `util/logging.cc` | Log rotation max file size |
| `RAY_ROTATION_BACKUP_COUNT` | `util/logging.cc` | Log rotation backup count |
| `RAY_LD_PRELOAD_ON_WORKERS` | `raylet/main.cc` | Control LD_PRELOAD inheritance |
| `RAY_AUTH_TOKEN` | `rpc/authentication/authentication_token_loader.cc` | Auth token |
| `RAY_AUTH_TOKEN_PATH` | `rpc/authentication/authentication_token_loader.cc` | Auth token file path |
| `RAY_num_grpc_internal_threads` | `thirdparty/patches/grpc-configurable-thread-count.patch` | **Eugo-patched** gRPC completion queue thread count. Default: `sysconf(_SC_NPROCESSORS_CONF)`. |

---

## 34. Raylet & GCS Server Internal Process Flags

These are passed as command-line flags to `raylet` and `gcs_server` executables, NOT as env vars. They are set by `ray start` internals.

| Flag | Process | Description |
|------|---------|-------------|
| `--raylet_socket_name` | raylet | Unix socket path |
| `--store_socket_name` | raylet | Plasma store socket path |
| `--object_store_memory` | raylet | Object store bytes |
| `--node_ip_address` | raylet/gcs | Node IP |
| `--node_manager_port` | raylet | Node manager port |
| `--agent_command` | raylet | Dashboard agent command |
| `--gcs_address` | raylet | GCS server address |
| `--gcs_server_port` | gcs_server | GCS bind port |
| `--metrics-export-port` | raylet | Prometheus export port |
| `--temp-dir` | raylet/gcs | Temp directory |
| `--session-dir` | raylet/gcs | Session directory |
| `--log-dir` | raylet/gcs | Log directory |

---

## 35. All C++ RAY_CONFIG Entries (Complete Table)

Every `RAY_CONFIG(type, name, default)` in `src/ray/common/ray_config_def.h`. Override via `export RAY_<name>=<value>`.

| Env Var | Type | Default | Brief Description |
|---------|------|---------|-------------------|
| `RAY_debug_dump_period_milliseconds` | uint64 | `10000` | Debug info dump interval |
| `RAY_event_stats` | bool | `true` | Event stats collection |
| `RAY_emit_main_service_metrics` | bool | `true` | Main service metrics |
| `RAY_enable_cluster_auth` | bool | `true` | Cluster authentication |
| `RAY_AUTH_MODE` | string | `"disabled"` | Authentication mode |
| `RAY_ENABLE_K8S_TOKEN_AUTH` | bool | `false` | K8s token auth |
| `RAY_event_stats_print_interval_ms` | int64 | `60000` | Event stats print interval |
| `RAY_ray_cookie` | int64 | `0x5241590000000000` | Ray cookie for mismatch detection |
| `RAY_handler_warning_timeout_ms` | int64 | `1000` | Event loop handler warning threshold |
| `RAY_gcs_pull_resource_loads_period_milliseconds` | uint64 | `1000` | GCS resource load pull period |
| `RAY_raylet_report_resources_period_milliseconds` | uint64 | `100` | Raylet resource report period |
| `RAY_raylet_check_gc_period_milliseconds` | uint64 | `100` | GC check period |
| `RAY_memory_usage_threshold` | float | `0.95` | Memory usage threshold |
| `RAY_memory_monitor_refresh_ms` | uint64 | `250` | Memory monitor interval |
| `RAY_min_memory_free_bytes` | int64 | `-1` | Min free memory bytes |
| `RAY_task_failure_entry_ttl_ms` | uint64 | `900000` | Task failure entry TTL |
| `RAY_task_oom_retries` | uint64 | `-1` (unlimited) | OOM task retries |
| `RAY_report_actor_placement_resources` | bool | `true` | Report actor placement |
| `RAY_record_ref_creation_sites` | bool | `false` | Record ref creation sites |
| `RAY_record_task_actor_creation_sites` | bool | `false` | Record task/actor creation sites |
| `RAY_free_objects_period_milliseconds` | int64 | `1000` | Object GC period |
| `RAY_free_objects_batch_size` | size_t | `100` | Object GC batch |
| `RAY_lineage_pinning_enabled` | bool | `true` | Lineage pinning |
| `RAY_max_lineage_bytes` | int64 | `1 GB` | Max lineage bytes |
| `RAY_preallocate_plasma_memory` | bool | `false` | Pre-fault plasma memory |
| `RAY_worker_cap_enabled` | bool | `true` | Worker cap |
| `RAY_worker_cap_initial_backoff_delay_ms` | int64 | `1000` | Worker cap backoff |
| `RAY_worker_cap_max_backoff_delay_ms` | int64 | `10000` | Worker cap max backoff |
| `RAY_scheduler_spread_threshold` | float | `0.5` | Spread scheduling threshold |
| `RAY_scheduler_top_k_fraction` | float | `0.2` | Top-k node fraction |
| `RAY_scheduler_top_k_absolute` | int32 | `1` | Top-k absolute minimum |
| `RAY_scheduler_report_pinned_bytes_only` | bool | `true` | Report pinned bytes only |
| `RAY_max_direct_call_object_size` | int64 | `100 KB` | Inline object threshold |
| `RAY_max_grpc_message_size` | size_t | `512 MB` | gRPC max message |
| `RAY_agent_max_grpc_message_size` | int64 | `20 MB` | Agent gRPC max |
| `RAY_grpc_server_retry_timeout_milliseconds` | int64 | `1000` | gRPC server retry timeout |
| `RAY_grpc_enable_http_proxy` | bool | `false` | gRPC HTTP proxy |
| `RAY_actor_excess_queueing_warn_threshold` | uint64 | `5000` | Actor queue warning |
| `RAY_object_timeout_milliseconds` | int64 | `100` | Object fetch timeout |
| `RAY_worker_lease_timeout_milliseconds` | int64 | `500` | Worker lease timeout |
| `RAY_raylet_death_check_interval_milliseconds` | uint64 | `1000` | Raylet death check |
| `RAY_get_check_signal_interval_milliseconds` | int64 | `1000` | Signal check interval |
| `RAY_worker_fetch_request_size` | int64 | `10000` | Worker fetch batch |
| `RAY_fetch_warn_timeout_milliseconds` | int64 | `60000` | Fetch warn timeout |
| `RAY_fetch_fail_timeout_milliseconds` | int64 | `600000` | Fetch fail timeout |
| `RAY_yield_plasma_lock_workaround` | bool | `true` | Plasma lock workaround |
| `RAY_raylet_client_num_connect_attempts` | int64 | `10` | Raylet connect retries |
| `RAY_raylet_client_connect_timeout_milliseconds` | int64 | `1000` | Raylet connect timeout |
| `RAY_kill_worker_timeout_milliseconds` | int64 | `5000` | Kill worker timeout |
| `RAY_actor_graceful_shutdown_timeout_ms` | int64 | `30000` | Actor shutdown timeout |
| `RAY_worker_register_timeout_seconds` | int64 | `60` | Worker registration timeout |
| `RAY_worker_maximum_startup_concurrency` | int64 | `0` (auto) | Max concurrent worker startups |
| `RAY_worker_max_resource_analysis_iteration` | uint32 | `128` | Resource analysis iterations |
| `RAY_max_num_generator_returns` | uint32 | `100M` | Max generator returns |
| `RAY_worker_oom_score_adjustment` | int | `1000` | OOM score adjustment |
| `RAY_worker_niceness` | int | `15` | Worker niceness |
| `RAY_redis_db_connect_retries` | int64 | `120` | Redis connect retries |
| `RAY_redis_db_connect_wait_milliseconds` | int64 | `500` | Redis connect wait |
| `RAY_num_redis_request_retries` | size_t | `5` | Redis request retries |
| `RAY_redis_retry_base_ms` | int64 | `100` | Redis retry base |
| `RAY_redis_retry_multiplier` | int64 | `2` | Redis retry multiplier |
| `RAY_redis_retry_max_ms` | int64 | `1000` | Redis retry max |
| `RAY_object_manager_timer_freq_ms` | int | `100` | Object manager timer |
| `RAY_object_manager_pull_timeout_ms` | int | `10000` | Pull timeout |
| `RAY_object_manager_push_timeout_ms` | int | `10000` | Push timeout |
| `RAY_object_manager_default_chunk_size` | uint64 | `5 MB` | Chunk size |
| `RAY_object_manager_max_bytes_in_flight` | uint64 | `2 GB` | Bytes in flight |
| `RAY_maximum_gcs_deletion_batch_size` | uint32 | `1000` | GCS deletion batch |
| `RAY_maximum_gcs_storage_operation_batch_size` | uint32 | `1000` | GCS storage batch |
| `RAY_object_store_get_max_ids_to_print_in_warning` | uint32 | `20` | Warning ID limit |
| `RAY_gcs_redis_heartbeat_interval_milliseconds` | uint64 | `100` | GCS Redis heartbeat |
| `RAY_gcs_lease_worker_retry_interval_ms` | uint32 | `200` | Lease worker retry |
| `RAY_gcs_create_actor_retry_interval_ms` | uint32 | `200` | Create actor retry |
| `RAY_gcs_create_placement_group_retry_min_interval_ms` | uint64 | `100` | PG retry min |
| `RAY_gcs_create_placement_group_retry_max_interval_ms` | uint64 | `1000` | PG retry max |
| `RAY_gcs_create_placement_group_retry_multiplier` | double | `1.5` | PG retry multiplier |
| `RAY_maximum_gcs_destroyed_actor_cached_count` | uint32 | `100000` | Cached destroyed actors |
| `RAY_maximum_gcs_dead_node_cached_count` | uint32 | `1000` | Cached dead nodes |
| `RAY_gcs_storage` | string | `"memory"` | GCS storage backend |
| `RAY_object_store_full_delay_ms` | uint32 | `10` | Store full delay |
| `RAY_plasma_store_usage_trigger_gc_threshold` | double | `0.7` | Plasma GC trigger |
| `RAY_local_gc_interval_s` | uint64 | `5400` | Local GC interval |
| `RAY_local_gc_min_interval_s` | uint64 | `10` | Local GC min interval |
| `RAY_global_gc_min_interval_s` | uint64 | `30` | Global GC min interval |
| `RAY_task_retry_delay_ms` | uint32 | `0` | Task retry delay |
| `RAY_task_oom_retry_delay_base_ms` | uint32 | `1000` | OOM retry delay base |
| `RAY_cancellation_retry_ms` | uint32 | `2000` | Cancellation retry |
| `RAY_support_fork` | bool | `false` | Fork support |
| `RAY_gcs_rpc_server_reconnect_timeout_s` | uint32 | `60` | GCS reconnect timeout |
| `RAY_gcs_rpc_server_connect_timeout_s` | int32 | `5` | GCS connect timeout |
| `RAY_gcs_grpc_max_reconnect_backoff_ms` | int32 | `2000` | Max reconnect backoff |
| `RAY_gcs_grpc_min_reconnect_backoff_ms` | int32 | `1000` | Min reconnect backoff |
| `RAY_gcs_grpc_initial_reconnect_backoff_ms` | int32 | `100` | Initial reconnect backoff |
| `RAY_gcs_grpc_max_request_queued_max_bytes` | uint64 | `5 GB` | GCS queue max bytes |
| `RAY_grpc_client_check_connection_status_interval_milliseconds` | int32 | `1000` | Connection status check |
| `RAY_ray_syncer_message_refresh_interval_ms` | int64 | `3000` | Ray syncer refresh |
| `RAY_metrics_report_batch_size` | int64 | `10000` | Metrics report batch |
| `RAY_task_events_skip_driver_for_test` | bool | `false` | Test only |
| `RAY_task_events_report_interval_ms` | int64 | `1000` | Task events report interval |
| `RAY_ray_events_report_interval_ms` | int64 | `1000` | Ray events report interval |
| `RAY_task_events_max_num_task_in_gcs` | int64 | `100000` | Max tasks in GCS |
| `RAY_task_events_max_num_status_events_buffer_on_worker` | uint64 | `100K` | Worker status event buffer |
| `RAY_task_events_send_batch_size` | uint64 | `10K` | Task events send batch |
| `RAY_export_task_events_write_batch_size` | uint64 | `10K` | Export events batch |
| `RAY_task_events_max_num_profile_events_per_task` | int64 | `1000` | Profile events per task |
| `RAY_task_events_max_num_profile_events_buffer_on_worker` | uint64 | `10K` | Profile event buffer |
| `RAY_task_events_dropped_task_attempt_batch_size` | int64 | `10K` | Dropped task batch |
| `RAY_task_events_shutdown_flush_timeout_ms` | int64 | `5000` | Shutdown flush timeout |
| `RAY_gcs_mark_task_failed_on_job_done_delay_ms` | uint64 | `15000` | Mark task failed on job done |
| `RAY_gcs_mark_task_failed_on_worker_dead_delay_ms` | uint64 | `1000` | Mark task failed on worker dead |
| `RAY_enable_metrics_collection` | bool | `true` | Enable metrics |
| `RAY_metric_cardinality_level` | string | `"legacy"` | Metric cardinality |
| `RAY_enable_open_telemetry` | bool | `true` | OpenTelemetry |
| `RAY_disable_open_telemetry_sdk_log` | bool | `true` | Suppress OTel SDK logs |
| `RAY_enable_ray_event` | bool | `false` | Ray event system |
| `RAY_ray_event_recorder_max_queued_events` | uint64 | `10000` | Event queue size |
| `RAY_enable_grpc_metrics_collection_for` | string | `""` | gRPC metrics components |
| `RAY_io_context_event_loop_lag_collection_interval_ms` | int64 | `10000` | Event loop lag |
| `RAY_task_rpc_inlined_bytes_limit` | int64 | `10 MB` | Inline bytes limit |
| `RAY_max_pending_lease_requests_per_scheduling_category` | int64 | `-1` | Pending lease limit |
| `RAY_agent_register_timeout_ms` | uint32 | `30000`/`100000` | Agent register timeout (platform-dependent) |
| `RAY_enable_pipe_based_agent_to_parent_health_check` | bool | `true` | Agent health check |
| `RAY_agent_manager_retry_interval_ms` | uint32 | `1000` | Agent retry interval |
| `RAY_max_resource_shapes_per_load_report` | int64 | `100` | Resource shapes limit |
| `RAY_gcs_server_request_timeout_seconds` | int64 | `60` | GCS request timeout |
| `RAY_enable_worker_prestart` | bool | `false` | Pre-start workers |
| `RAY_prestart_worker_first_driver` | bool | `true` | Pre-start for first driver |
| `RAY_kill_idle_workers_interval_ms` | uint64 | `200` | Idle worker kill interval |
| `RAY_idle_worker_killing_time_threshold_ms` | int64 | `1000` | Idle killing threshold |
| `RAY_num_workers_soft_limit` | int64 | `-1` | Worker soft limit |
| `RAY_metrics_report_interval_ms` | uint64 | `10000` | Metrics report interval |
| `RAY_enable_timeline` | bool | `true` | Timeline tracing |
| `RAY_max_placement_group_load_report_size` | int64 | `1000` | PG load report size |
| `RAY_object_spilling_config` | string | `""` | Spill config JSON |
| `RAY_object_spilling_directory` | string | `""` | Spill directory |
| `RAY_verbose_spill_logs` | int64 | `2 GB` | Spill log threshold |
| `RAY_automatic_object_spilling_enabled` | bool | `true` | Auto spilling |
| `RAY_max_io_workers` | int | `4` | I/O workers |
| `RAY_min_spilling_size` | int64 | `100 MB` | Min spill size |
| `RAY_max_spilling_file_size_bytes` | int64 | `-1` | Max spill file |
| `RAY_object_spilling_threshold` | float | `0.8` | Spill threshold |
| `RAY_max_fused_object_count` | int64 | `2000` | Max fused objects |
| `RAY_oom_grace_period_s` | int64 | `2` | OOM grace period |
| `RAY_is_external_storage_type_fs` | bool | `true` | FS storage type |
| `RAY_local_fs_capacity_threshold` | float | `0.95` | Local FS capacity |
| `RAY_local_fs_monitor_interval_ms` | uint64 | `100` | FS monitor interval |
| `RAY_locality_aware_leasing_enabled` | bool | `true` | Locality-aware leasing |
| `RAY_log_rotation_max_bytes` | int64 | `100 MB` | Log rotation max bytes |
| `RAY_log_rotation_backup_count` | int64 | `5` | Log rotation backups |
| `RAY_timeout_ms_task_wait_for_death_info` | int64 | `1000` | Death info wait timeout |
| `RAY_core_worker_internal_heartbeat_ms` | int64 | `1000` | Core worker heartbeat |
| `RAY_core_worker_rpc_server_reconnect_timeout_base_s` | uint32 | `1` | Core worker reconnect base |
| `RAY_core_worker_rpc_server_reconnect_timeout_max_s` | uint32 | `60` | Core worker reconnect max |
| `RAY_max_task_args_memory_fraction` | float | `0.7` | Task args memory fraction |
| `RAY_publish_batch_size` | int | `5000` | Publish batch size |
| `RAY_publisher_entity_buffer_max_bytes` | int | `1 GB` | Publisher buffer |
| `RAY_max_command_batch_size` | int64 | `2000` | Command batch size |
| `RAY_max_object_report_batch_size` | int64 | `2000` | Object report batch |
| `RAY_subscriber_timeout_ms` | uint64 | `300000` | Subscriber timeout |
| `RAY_gcs_actor_table_min_duration_ms` | uint64 | `300000` | Actor table min duration |
| `RAY_max_error_msg_size_bytes` | uint32 | `512 KB` | Max error message size |
| `RAY_predefined_unit_instance_resources` | string | `"GPU"` | Predefined unit resources |
| `RAY_custom_unit_instance_resources` | string | `"neuron_cores,TPU,NPU,HPU,RBLN"` | Custom unit resources |
| `RAY_system_concurrency_group_name` | string | `"_ray_system"` | System concurrency group |
| `RAY_grpc_keepalive_time_ms` | int64 | `10000` | gRPC keepalive time |
| `RAY_grpc_keepalive_timeout_ms` | int64 | `20000` | gRPC keepalive timeout |
| `RAY_grpc_client_keepalive_time_ms` | int64 | `300000` | Client keepalive time |
| `RAY_grpc_client_keepalive_timeout_ms` | int64 | `120000` | Client keepalive timeout |
| `RAY_grpc_client_idle_timeout_ms` | int64 | `1800000` | Client idle timeout |
| `RAY_grpc_stream_buffer_size` | int64 | `512 KB` | Stream buffer |
| `RAY_event_log_reporter_enabled` | bool | `true` | Event log reporter |
| `RAY_emit_event_to_log_file` | bool | `false` | Emit events to log file |
| `RAY_event_level` | string | `"warning"` | Event level |
| `RAY_scheduler_avoid_gpu_nodes` | bool | `true` | Avoid GPU nodes for non-GPU tasks |
| `RAY_runtime_env_skip_local_gc` | bool | `false` | Skip local GC for runtime envs |
| `RAY_external_storage_namespace` | string | `"default"` | External storage namespace |
| `RAY_USE_TLS` | bool | `false` | TLS on/off |
| `RAY_TLS_SERVER_CERT` | string | `""` | TLS server cert |
| `RAY_TLS_SERVER_KEY` | string | `""` | TLS server key |
| `RAY_TLS_CA_CERT` | string | `""` | TLS CA cert |
| `RAY_REDIS_CA_CERT` | string | `""` | Redis CA cert |
| `RAY_REDIS_CA_PATH` | string | `""` | Redis CA path |
| `RAY_REDIS_CLIENT_CERT` | string | `""` | Redis client cert |
| `RAY_REDIS_CLIENT_KEY` | string | `""` | Redis client key |
| `RAY_REDIS_SERVER_NAME` | string | `""` | Redis server name |
| `RAY_testing_asio_delay_us` | string | `""` | Test only |
| `RAY_testing_rpc_failure` | string | `""` | Test only |
| `RAY_testing_rpc_failure_avoid_intra_node_failures` | bool | `false` | Test only |
| `RAY_health_check_initial_delay_ms` | int64 | `5000` | Health check initial delay |
| `RAY_health_check_period_ms` | int64 | `3000` | Health check period |
| `RAY_health_check_timeout_ms` | int64 | `10000` | Health check timeout |
| `RAY_health_check_failure_threshold` | int64 | `5` | Health check failure threshold |
| `RAY_num_server_call_thread` | int64 | `max(1, ncpus/4)` | Server call thread pool |
| `RAY_core_worker_num_server_call_thread` | int64 | `ncpus>=8 ? 2 : 1` | Core worker threads |
| `RAY_worker_core_dump_exclude_plasma_store` | bool | `true` | Exclude plasma from core dumps |
| `RAY_raylet_core_dump_exclude_plasma_store` | bool | `true` | Exclude plasma from raylet dumps |
| `RAY_preload_python_modules` | vector\<string\> | `{}` | Preload Python modules |
| `RAY_raylet_liveness_self_check_interval_ms` | int64 | `60000` | Raylet liveness check |
| `RAY_kill_child_processes_on_worker_exit` | bool | `true` | Kill children on exit |
| `RAY_kill_child_processes_on_worker_exit_with_raylet_subreaper` | bool | `false` | Subreaper cleanup |
| `RAY_process_group_cleanup_enabled` | bool | `false` | Process group cleanup |
| `RAY_enable_autoscaler_v2` | bool | `false` | Autoscaler v2 |
| `RAY_nums_py_gcs_reconnect_retry` | int64 | `5` | Python GCS reconnect retries |
| `RAY_py_gcs_connect_timeout_s` | int64 | `30` | Python GCS connect timeout |
| `RAY_object_manager_client_connection_num` | int | `4` | Object manager connections |
| `RAY_object_manager_rpc_threads_num` | int | `0` (auto) | Object manager RPC threads |
| `RAY_enable_export_api_write` | bool | `false` | Export API write |
| `RAY_enable_export_api_write_config` | vector\<string\> | `{}` | Selective export types |
| `RAY_enable_core_worker_task_event_to_gcs` | bool | `true` | Task events to GCS |
| `RAY_enable_core_worker_ray_event_to_aggregator` | bool | `false` | Events to aggregator |
| `RAY_pipe_logger_read_buf_size` | uint64 | `1024` | Pipe logger buffer |
| `RAY_enable_infeasible_task_early_exit` | bool | `false` | Infeasible task exit |
| `RAY_raylet_check_for_unexpected_worker_disconnect_interval_ms` | int64 | `1000` | Worker disconnect check |
| `RAY_actor_scheduling_queue_max_reorder_wait_seconds` | int64 | `30` | Actor task reorder timeout |
| `RAY_raylet_rpc_server_reconnect_timeout_base_s` | uint32 | `1` | Raylet RPC reconnect base |
| `RAY_raylet_rpc_server_reconnect_timeout_max_s` | uint32 | `60` | Raylet RPC reconnect max |
| `RAY_worker_num_grpc_internal_threads` | int64 | `0` | Worker gRPC threads |
| `RAY_start_python_gc_manager_thread` | bool | `true` | Python GC thread |
| `RAY_enable_output_error_log_if_still_retry` | bool | `true` | Error log if still retrying |
| `RAY_gcs_resource_broadcast_max_batch_size` | size_t | `1` | Resource broadcast batch |
| `RAY_gcs_resource_broadcast_max_batch_delay_ms` | uint64 | `0` | Resource broadcast delay |

---

## 36. Leave at Defaults

The following C++ configs are safe to leave at defaults for most HPC deployments:

- `RAY_event_stats` — debug feature
- `RAY_enable_timeline` — tracing overhead; disable in production
- `RAY_ray_cookie` — protocol magic number
- `RAY_handler_warning_timeout_ms` — event loop warning
- `RAY_object_timeout_milliseconds` — fetch timeout
- `RAY_get_check_signal_interval_milliseconds` — signal check
- `RAY_worker_fetch_request_size` — fetch batch
- `RAY_gcs_storage` — storage backend
- `RAY_system_concurrency_group_name` — internal
- `RAY_publish_batch_size` — pubsub batch
- `RAY_publisher_entity_buffer_max_bytes` — pubsub buffer
- `RAY_subscriber_timeout_ms` — subscriber timeout
- `RAY_max_error_msg_size_bytes` — error size cap
- All `RAY_testing_*` — test-only variables

---

## 37. Internal / Do Not Touch

Set automatically by Ray processes. **Do not set manually.**

| Variable | Set By | Source |
|----------|--------|--------|
| `RAY_JOB_ID` | Raylet → worker | `ray_internal_flag_def.h` |
| `RAY_RAYLET_PID` | Raylet → worker | `ray_internal_flag_def.h` |
| `RAY_OVERRIDE_NODE_ID_FOR_TESTING` | Test only | `ray_internal_flag_def.h` |
| `RAY_NODE_IP_ADDRESS` | `ray start` | `ray_constants.py` |
| `RAY_SESSION_DIR` | `ray start` | `ray_constants.py` |
| `RAY_CLUSTER_ID` | GCS | `ray_constants.py` |
| `RAY_JOB_CONFIG_JSON_ENV_VAR` | Internal | `config_internal.cc` |

---

## 38. Build-Time Only (Not Runtime)

Used by the upstream Bazel build system (`setup.py`). **Irrelevant to the Eugo Meson build.**

| Variable | Purpose | Source |
|----------|---------|--------|
| `RAY_BUILD_CORE` | Whether to build C++ core | `setup.py` |
| `RAY_INSTALL_JAVA` | Whether to build Java | `setup.py` |
| `RAY_DISABLE_EXTRA_CPP` | Disable C++ extras | `setup.py` |
| `RAY_BUILD_REDIS` | Whether to build Redis | `setup.py` |
| `SKIP_BAZEL_BUILD` | Skip Bazel entirely | `setup.py` |
| `BAZEL_ARGS` | Extra Bazel flags | `setup.py` |
| `BAZEL_LIMIT_CPUS` | Bazel CPU limit | `setup.py` |
| `BAZEL_PATH` | Custom Bazel binary | `setup.py` |
| `RAY_DEBUG_BUILD` | Debug/release build type | `setup.py` |
| `RAY_INSTALL_CPP` | Install C++ SDK | `setup.py` |
| `SKIP_THIRDPARTY_INSTALL_CONDA_FORGE` | Skip vendored packages | `setup.py` |
