# Shared Profiling Module

Shared profiling and monitoring infrastructure for Ray Data benchmarks.
Used by benchmarks under `release/nightly_tests/dataset/` (e.g.
`image_embedding_from_jsonl`).

## How to instrument a benchmark

### Step 1: Build an image with profiling tools

`build_incremental_ray.sh` must be invoked from a Ray or Rayturbo source
root (it shells out to `bazel`, looks for a `python/ray` subdirectory,
and pushes the resulting image to ECR).

```bash
cd ~/work/anyscale/rayturbo2   # or wherever your source tree is
release/nightly_tests/dataset/profiling/build/build_incremental_ray.sh \
    --tag <your-tag> --extra
```

`--extra` adds nsys, perf, gdb, and the cloud SDKs to the base image.
See `profiling/build/README.md` for `--ml`, `--python-only`, and other
build modes.

### Step 2: Add profiling to your benchmark's `main.py`

```python
from profiling.coordinator import Profiling
from profiling import nvtx as profiling_nvtx

# Create a Profiling instance with your output directory and GPU node count.
# All configuration is read from environment variables (see table below).
profiling = Profiling(
    outdir="/mnt/shared_storage/my_benchmark/<job_id>",
    num_gpu_nodes=40,
)

# Start all enabled profilers and monitors.
profiling.start()

# Get nsys runtime_env for GPU workers (returns {} if PROFILER_MODE != "nsys").
infer_kwargs["runtime_env"] = profiling.nsys_runtime_env()

# Add NVTX annotations to your GPU actor's __call__ method:
class MyActor:
    def __call__(self, batch):
        with profiling_nvtx.profiling_range("my_operation"):
            ...

# After the benchmark completes, stop profilers and upload telemetry.
profiling.stop(s3_prefix="my-benchmark/<job_id>")
```

That's it. The `Profiling` class handles starting/stopping py-spy, perf,
nvidia-smi, and network monitors based on which env vars are set.

### Step 3: Configure env vars in `job.yaml`

```yaml
image_uri: 830883877497.dkr.ecr.us-west-2.amazonaws.com/anyscale/ray:<your-tag>

working_dir: .

env_vars:
    PROFILER_MODE: "nsys"          # "nsys", "torch", or "none"
    PYSPY_ENABLED: "1"             # Enable py-spy CPU profiling
    PERF_PROFILING_ENABLED: "1"    # Enable perf record on raylet/gcs
```

### Step 4: Submit the job from the `dataset/` directory

```bash
cd release/nightly_tests/dataset
anyscale jobs submit -f my_benchmark/job.yaml
```

Submitting from `dataset/` ensures `benchmark.py` and `profiling/` are
in scope as the working directory.

### Step 5: Download results and analyze

```bash
./profiling/analysis/download_job_output.sh prodjob_abc123 my-benchmark/prodjob_abc123
cd prodjob_abc123
../profiling/analysis/analyze_pyspy_profile.py pyspy_driver.speedscope.json --list-threads
# Worker-node UDF profiles (one per sampled ray:: worker):
../profiling/analysis/analyze_pyspy_profile.py pyspy_worker_<nodeip>_Infer.speedscope.json --list-threads
../profiling/analysis/analyze_perf_profiles.sh
```

See [`analysis/README.md`](analysis/README.md) for more analysis examples.

## Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `PROFILER_MODE` | `"none"` | GPU profiler: `"nsys"`, `"torch"`, or `"none"` |
| `PROFILE_SKIP_BATCHES` | `0` | Batches to skip before nsys capture starts |
| `PROFILE_ACTIVE_BATCHES` | `10000` | Upper bound on captured batches per actor; left high so atexit closes the capture range |
| `PYSPY_ENABLED`          | `"0"` | Set to `"1"` to enable py-spy CPU profiling |
| `PYSPY_NUM_CPU_WORKERS`  | `5`   | Number of CPU worker nodes to py-spy (0 to disable worker py-spy) |
| `PYSPY_NUM_GPU_WORKERS`  | `5`   | Number of GPU worker nodes to py-spy (0 to disable worker py-spy) |
| `PERF_PROFILING_ENABLED` | `"0"` | Set to `"1"` to enable perf record on raylet/gcs |
| `PERF_NUM_CPU_WORKERS`   | `5`   | Number of CPU worker nodes to perf profile |
| `PERF_NUM_GPU_WORKERS`   | `5`   | Number of GPU worker nodes to perf profile |
| `GPU_MONITOR_ENABLED` | `"0"` | Set to `"1"` to enable nvidia-smi monitoring |
| `NET_MONITOR_ENABLED` | `"0"` | Set to `"1"` to enable network I/O monitoring |
| `OBJECT_STORE_MONITOR_ENABLED` | `"0"` | Set to `"1"` to enable per-(node, operator) object-store sampling |
| `OBJECT_STORE_MONITOR_INTERVAL_S` | `5` | Seconds between samples after the fast window |
| `OBJECT_STORE_MONITOR_FAST_WINDOW_S` | `60` | Seconds at the start of the run to sample at the fast interval (set `0` to disable) |
| `OBJECT_STORE_MONITOR_FAST_INTERVAL_S` | `1` | Tick interval during the fast window |
| `RAY_MAX_LIMIT_FROM_API_SERVER` | `10000` | Object-store sampling caps `list_objects()` at 10k by default; set to e.g. `200000` on the head node to fully sample large runs |
| `PROFILING_S3_BUCKET` | `anyscale-staging-data-cld-kvedzwag2qa8i5bjxuevf5i7` | S3 bucket for telemetry upload |

## Modules

| Module | What it does |
|--------|-------------|
| `coordinator.py` | Orchestrates all profilers — the main entry point for benchmarks |
| `pyspy.py` | Attaches py-spy to the driver process and to Ray UDF workers on sampled worker nodes |
| `perf.py` | Runs `perf record` on GCS/raylet C++ processes (head + worker nodes) |
| `gpu_monitor.py` | Launches `nvidia-smi dmon` on GPU nodes as they join the cluster |
| `net_monitor.py` | Samples `psutil.net_io_counters` on every node, writes CSV |
| `nsys.py` | Builds `runtime_env` config to wrap Ray workers with `nsys profile` |
| `nvtx.py` | NVTX range annotations and CUDA profiler start/stop for nsys capture control |
| `object_store.py` | Samples Ray's state API for per-(node, operator) primary-byte time series; writes `object_store_state.csv`, `actor_placement.csv`, and `plasma_stats.csv` |
| `telemetry.py` | Uploads profiling artifacts from shared storage to S3 |

## Subdirectories

- [`analysis/`](analysis/README.md) -- post-run analysis scripts for profile data
- [`build/`](build/README.md) -- image build tooling

## Example

See `image_embedding_from_jsonl/main.py` for a complete working example.
