# Experiment Report: Full Rust Backend vs Full C++ Backend (RDT Tests)

**Date**: March 14-15, 2026
**Branch**: `cc-to-rust-experimental`
**Commit**: `5e1aaf83e9`

## Objective

Verify that the **full Rust backend** (GCS + Raylet + Core Worker + Object Store all in Rust, via `_raylet.so` built from `ray-core-worker-pylib`) produces identical test results to the **full C++ backend** (standard Ray nightly) for the NIXL and NCCL RDT tests.

This extends the previous experiment (March 14) which only tested Rust GCS with C++ Core Worker/Raylet.

## Architecture Comparison

| Component | Full Rust Backend | Full C++ Backend |
|-----------|-------------------|------------------|
| GCS | Rust (in-process via `_raylet.so`) | C++ (`gcs_server` binary) |
| Raylet | Rust (in-process via `_raylet.so`) | C++ (`raylet` binary) |
| Core Worker | Rust (`_raylet.so` via PyO3) | C++ (`_raylet.so` via Cython) |
| Object Store | Rust (`ray-object-manager`) | C++ (Plasma) |
| RDT Store | Python (standard) | Python (standard) |
| RDT Transport | Python (NIXL + NCCL) | Python (NIXL + NCCL) |
| `ray` module | `rust/ray/__init__.py` (1800+ lines) | `pip install ray` nightly wheel |

## Setup

### Infrastructure
- **Instance type**: `g4dn.12xlarge` (4x T4 GPUs)
- **AMI**: `ami-0fb0010639c839abe` (Deep Learning OSS Nvidia Driver AMI GPU PyTorch 2.7, Ubuntu 22.04)
- **Region**: us-west-2
- **GPUs**: 4x NVIDIA T4, CUDA 12.8
- **Python**: 3.10.12

### C++ Backend Instance (`i-0b0077286d50aad1c`, IP: 35.90.202.162)
```bash
pip3 install -U 'ray[default] @ https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-3.0.0.dev0-cp310-cp310-manylinux2014_x86_64.whl'
pip3 install cupy-cuda12x nixl pytest pytest-timeout
```

### Rust Backend Instance (`i-0922a63733219292f`, IP: 35.163.235.223)
```bash
# Rust toolchain + protoc 28.3
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
# protoc 28.3 from GitHub releases

# Python deps (no ray nightly wheel)
pip3 install cupy-cuda12x nixl pytest pytest-timeout maturin torch cloudpickle

# Clone and build
git clone --depth=1 --branch cc-to-rust-experimental https://github.com/ray-project/ray.git
cd ~/ray/rust
cargo build --release -p ray-core-worker-pylib --features python  # 1m 13s
cp target/release/lib_raylet.so target/release/_raylet.so

# PYTHONPATH makes `import ray` load rust/ray/ and `import _raylet` load the Rust .so
export PYTHONPATH="$HOME/ray/rust:$HOME/ray/rust/target/release"
```

### Test Configuration
- Test files: `test_rdt_nixl.py` (21 tests), `test_rdt_nccl.py` (1 test)
- conftest.py: Simple `ray_start_regular` fixture (init/shutdown)
- Tests run from isolated `~/rdt-tests/` directory (not under `python/ray/tests/`)
- Timeout: 300s per test

## Results

### C++ Backend (Baseline)

```
NIXL: 20 passed, 1 skipped, 5 warnings in 186.93s
NCCL: 1 passed, 1 warning in 12.05s
```

### Rust Backend

**Initial run** (missing `cloudpickle` dependency):
- All 18 GPU-actor tests failed with `ModuleNotFoundError: No module named 'cloudpickle'`
- 2 CPU-only tests passed, 1 skipped

**After `pip3 install cloudpickle`**:
```
NIXL: 20 passed, 1 skipped in 94.92s
NCCL: 1 passed in 5.47s
```

### Comparison

| Test Suite | C++ Backend | Rust Backend | Match? |
|-----------|-------------|--------------|--------|
| NIXL passed | 20 | 20 | YES |
| NIXL skipped | 1 | 1 | YES |
| NIXL failed | 0 | 0 | YES |
| NCCL passed | 1 | 1 | YES |
| **Total** | **21 passed, 0 failed, 1 skipped** | **21 passed, 0 failed, 1 skipped** | **IDENTICAL** |

## Transient Failures — Root Cause and Fixes (March 15, 2026)

During initial runs, two tests showed transient failures on the Rust backend. Both were root-caused and fixed:

### 1. `test_out_of_order_actors` — NIXL_ERR_NOT_FOUND

**Root cause**: With `max_concurrency=10`, multiple `__ray_recv_and_call__` handlers run concurrently on the receiver. On first invocation, the UCX backend may not have fully established connections when the first NIXL transfer is attempted, causing `createXferReq: no specified or potential backend had the required registrations`.

**Fix** (`rust/ray/rdt.py`): Added retry with exponential backoff (up to 2 retries, 100ms/200ms delays) in the `__ray_recv_and_call__` handler when `recv_multiple_tensors` fails with `NOT_FOUND`. This gives UCX time to establish connections on first use.

**Result**: Passes 5/5 runs consistently after fix (including first cold-start run).

### 2. `test_nixl_abort_sender_dies_before_sending` — Wrong error type

**Root cause**: In `_remote_rdt_nixl_deferred` (`rust/ray/__init__.py`), a `_killed_actors` check short-circuited with `ActorDiedError` before the NIXL transfer started. The test expects `RayTaskError(RayDirectTransportError("nixlBackendError..."))`, which requires the NIXL transfer to actually attempt and fail. Additionally, the catch-all `except` converted all exceptions to `ActorDiedError`, losing the original error.

**Fix** (`rust/ray/__init__.py`):
1. Removed the `_killed_actors` short-circuit in `_deferred` — let the NIXL transfer proceed and fail naturally with the proper NIXL disconnect error
2. Changed catch-all from `result_ref._rdt_error = exceptions.ActorDiedError()` to `result_ref._rdt_error = e` to preserve original errors

**Result**: Passes 5/5 runs consistently after fix, with correct error type.

## Warnings

### C++ Backend (5 warnings)
- 1x `FutureWarning: Ray will no longer override accelerator visible devices env var`
- 4x `PytestUnhandledThreadExceptionWarning: Exception in thread _monitor_failures` (benign RDT cleanup thread race — `ValueError: unknown object owner`)

### Rust Backend (0 pytest warnings)
- The Rust backend's `_monitor_failures` thread is not present (different RDT manager implementation), so the benign cleanup warnings don't appear.
- NIXL INFO logs appear in stderr (agent initialization) — these are normal.

## Fixes Applied

### Fix 1: Missing `cloudpickle` dependency (setup-only)
`cloudpickle` was not listed as a dependency for the Rust backend's GPU worker spawning code. Added `pip3 install cloudpickle` to setup steps.

### Fix 2: NIXL retry for UCX warmup (`rust/ray/rdt.py`)
Added retry with backoff in `__ray_recv_and_call__` handler when `recv_multiple_tensors` fails with `NOT_FOUND`. Retries up to 2 times with 100ms/200ms delays.

### Fix 3: Deferred dispatch error handling (`rust/ray/__init__.py`)
- Removed `_killed_actors` short-circuit in `_remote_rdt_nixl_deferred` that prevented proper NIXL error propagation
- Changed catch-all to preserve original exceptions instead of converting to `ActorDiedError`

## Performance Notes

| Metric | C++ Backend | Rust Backend |
|--------|-------------|--------------|
| NIXL suite time | 186.93s | 94.92s |
| NCCL suite time | 12.05s | 5.47s |
| Build time | N/A (pre-built wheel) | 73s (cargo build --release) |
| `_raylet.so` size | ~180MB (Cython, many deps) | ~14.5MB (Rust, static) |

The Rust backend is ~2x faster on the NIXL suite. This is likely because the Rust backend's in-process architecture avoids IPC overhead (no separate GCS/Raylet processes), and the Rust object store is more lightweight than Plasma.

## Stability Verification (Post-Fix)

After applying fixes, ran stability checks on instance `i-02ee7b855ced7e3bb`:

- **Full NIXL suite**: 20 passed, 1 skipped (first run, cold start) — **no failures**
- **Full NCCL suite**: 1 passed
- **`test_out_of_order_actors`**: 5/5 passes
- **`test_nixl_abort_sender_dies_before_sending`**: 5/5 passes

## Conclusion

The **full Rust backend** produces **identical test results** to the **full C++ backend** for all RDT tests (NIXL and NCCL). Two transient failures were root-caused and fixed with targeted changes to the Python adapter layer (`rust/ray/__init__.py` and `rust/ray/rdt.py`). No Rust code or C++ code changes were needed.

### Instances (all terminated)
- Rust (initial): `i-0922a63733219292f`
- C++: `i-0b0077286d50aad1c`
- Rust (fix verification): `i-02ee7b855ced7e3bb`
