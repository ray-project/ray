# NIXL Tensor Transport Test — Rust Ray Backend

## Repository Version

**Branch:** `cc-to-rust-experimental`
**Date:** March 10, 2026

To reproduce, check out the branch at the time of this experiment:

```bash
git clone --branch cc-to-rust-experimental https://github.com/istoica/ray.git
cd ray/rust

# Build Rust backend
cargo build --release -p ray-core-worker-pylib --features python
cp target/release/lib_raylet.so target/release/_raylet.so
export PYTHONPATH=$PWD:$PWD/target/release:$PYTHONPATH

# Copy test files and run
mkdir -p /tmp/nixl-test
cp ../python/ray/tests/rdt/test_rdt_nixl.py /tmp/nixl-test/
cp examples/python/rdt-test/conftest.py /tmp/nixl-test/
cd /tmp/nixl-test && python3 -m pytest test_rdt_nixl.py -sv
```

## Summary

| Metric | Value |
|--------|-------|
| **Result** | **17 passed, 1 skipped, 0 failed** |
| **Duration** | 77.01 seconds |
| **Date** | March 10, 2026 |
| **Test file** | `python/ray/tests/rdt/test_rdt_nixl.py` (GitHub master, unmodified) |
| **Backend** | All-Rust Ray (`_raylet.so` via PyO3), no C++ Ray code |
| **Instance** | AWS g4dn.12xlarge (4x NVIDIA T4, 48 vCPUs, 192 GiB RAM) |
| **Region** | us-east-1 |
| **Instance ID** | i-0aac6680b1e208dee (terminated after test) |

## Test Results

| # | Test | GPUs | Result |
|---|------|------|--------|
| 1 | `test_ray_get_rdt_ref_created_by_actor_task` | 1 | PASSED |
| 2 | `test_p2p` | 2 | PASSED |
| 3 | `test_intra_rdt_tensor_transfer` | 1 | PASSED |
| 4 | `test_put_and_get_object_with_nixl` | 2 | PASSED |
| 5 | `test_put_and_get_object_with_object_store` | 2 | PASSED |
| 6 | `test_put_gc` | 1 | PASSED |
| 7 | `test_send_duplicate_tensor` | 2 | PASSED |
| 8 | `test_nixl_abort_sender_dies_before_sending` | 2 | PASSED |
| 9 | `test_nixl_del_before_creating` | 2 | PASSED |
| 10 | `test_nixl_borrow_after_abort` | 2 | SKIPPED (expected) |
| 11 | `test_shared_tensor_deduplication` | 1 | PASSED |
| 12 | `test_nixl_agent_reuse` | 2 | PASSED |
| 13 | `test_nixl_agent_reuse_with_partial_tensors` | 2 | PASSED |
| 14 | `test_storage_level_overlapping_views_reference_count` | 1 | PASSED |
| 15 | `test_storage_level_overlapping_views` | 2 | PASSED |
| 16 | `test_wait_tensor_freed_views` | 1 | PASSED |
| 17 | `test_nixl_get_into_tensor_buffers` | 2 | PASSED |
| 18 | `test_register_nixl_memory` | 1 | PASSED |

## What Was Tested

The test suite exercises the full NIXL (one-sided RDMA) tensor transport path through
the Rust Ray backend:

- **Basic transfers**: GPU-to-GPU and CPU-to-CPU peer-to-peer tensor movement via NIXL
- **Intra-actor**: Tensor passing within a single GPU actor
- **ray.put / ray.get**: Tensor round-trips through both NIXL and object store paths
- **Garbage collection**: Reference counting, tensor lifecycle, `wait_tensor_freed`
- **Duplicate tensors**: Proper deduplication when the same tensor appears in multiple calls
- **Error handling**: Sender death mid-transfer (abort path), early ref deletion
- **Agent reuse**: NIXL agent caching across multiple transfers and after GC
- **Overlapping views**: Tensors sharing underlying storage (view-level registration)
- **Pre-allocated buffers**: `set_target_for_ref` for zero-copy receives into existing buffers
- **Persistent registration**: `register_nixl_memory` for long-lived tensor buffers

## Architecture

```
Driver Process
  |
  +-- ray.init() -> Rust GCS + CoreWorker (via _raylet.so / PyO3)
  |
  +-- GPUTestActor.remote() -> spawns GPU subprocess per actor
        |
        +-- CUDA_VISIBLE_DEVICES=N
        +-- PyCoreWorker (Rust) + gRPC server
        +-- RDTManager + NixlTensorTransport (UCX backend)
        +-- ThreadPoolExecutor for task dispatch
```

Each GPU actor runs in an isolated subprocess with its own:
- `PyCoreWorker` (Rust, connected to GCS via gRPC)
- NIXL agent (UCX backend for one-sided RDMA reads)
- `RDTManager` for tensor lifecycle and metadata tracking
- `ThreadPoolExecutor` to avoid tokio `block_on` deadlocks

Tensor transfers use NIXL one-sided semantics: the **receiver** initiates an RDMA READ
from the sender's registered memory. No collective groups are needed.

## How to Reproduce

### Prerequisites

- AWS g4dn.12xlarge (or any instance with 2+ NVIDIA GPUs)
- Ubuntu 22.04 with NVIDIA drivers
- Python 3.10+, PyTorch with CUDA, NIXL library
- Rust toolchain + the built `_raylet.so`

### Steps

```bash
# 1. Build the Rust backend
cd ray/rust
cargo build --release -p ray-python
cp target/release/lib_raylet.so target/release/_raylet.so

# 2. Set PYTHONPATH
export PYTHONPATH=$PWD:$PWD/target/release:$PYTHONPATH

# 3. Copy test file and conftest.py into a test directory
mkdir -p /tmp/nixl-test
cp <path-to>/python/ray/tests/rdt/test_rdt_nixl.py /tmp/nixl-test/
cp rust/examples/python/rdt-test/conftest.py /tmp/nixl-test/

# 4. Run
cd /tmp/nixl-test
python3 -m pytest test_rdt_nixl.py -sv
```

## Key Files (Rust Backend)

| File | Role |
|------|------|
| `rust/ray/__init__.py` | Ray Python API (init, remote, put, get, wait, kill) |
| `rust/ray/rdt.py` | GPU worker entry point, task callback, NIXL/NCCL dispatch |
| `rust/ray/exceptions.py` | RayTaskError, ActorDiedError |
| `rust/ray/_private/worker.py` | Global worker state (rdt_manager, core_worker) |
| `rust/ray/_common/test_utils.py` | SignalActor, wait_for_condition |
| `rust/ray/experimental/__init__.py` | set_target_for_ref, wait_tensor_freed |
| `rust/ray/experimental/rdt/nixl_tensor_transport.py` | NixlTensorTransport (UCX RDMA) |
| `rust/ray/experimental/rdt/rdt_manager.py` | RDTManager, RDTStore |
| `rust/ray/experimental/rdt/util.py` | Transport registry, tensor utilities |
| `rust/ray-core-worker-pylib/src/core_worker.rs` | PyCoreWorker (Rust/PyO3 binding) |

## Raw Output

```
============================= test session starts ==============================
platform linux -- Python 3.10.12, pytest-9.0.2, pluggy-1.6.0 -- /usr/bin/python3
cachedir: .pytest_cache
rootdir: /home/ubuntu/nixl-test
plugins: timeout-2.4.0
collecting ... collected 18 items

test_rdt_nixl.py::test_ray_get_rdt_ref_created_by_actor_task[ray_start_regular0] PASSED
test_rdt_nixl.py::test_p2p[ray_start_regular0] PASSED
test_rdt_nixl.py::test_intra_rdt_tensor_transfer[ray_start_regular0] PASSED
test_rdt_nixl.py::test_put_and_get_object_with_nixl[ray_start_regular0] PASSED
test_rdt_nixl.py::test_put_and_get_object_with_object_store[ray_start_regular0] PASSED
test_rdt_nixl.py::test_put_gc[ray_start_regular0] PASSED
test_rdt_nixl.py::test_send_duplicate_tensor[ray_start_regular0] PASSED
test_rdt_nixl.py::test_nixl_abort_sender_dies_before_sending[ray_start_regular0] PASSED
test_rdt_nixl.py::test_nixl_del_before_creating[ray_start_regular0] PASSED
test_rdt_nixl.py::test_nixl_borrow_after_abort[ray_start_regular0] SKIPPED
test_rdt_nixl.py::test_shared_tensor_deduplication[ray_start_regular0] PASSED
test_rdt_nixl.py::test_nixl_agent_reuse[ray_start_regular0] PASSED
test_rdt_nixl.py::test_nixl_agent_reuse_with_partial_tensors[ray_start_regular0] PASSED
test_rdt_nixl.py::test_storage_level_overlapping_views_reference_count[ray_start_regular0] PASSED
test_rdt_nixl.py::test_storage_level_overlapping_views[ray_start_regular0] PASSED
test_rdt_nixl.py::test_wait_tensor_freed_views[ray_start_regular0] PASSED
test_rdt_nixl.py::test_nixl_get_into_tensor_buffers[ray_start_regular0] PASSED
test_rdt_nixl.py::test_register_nixl_memory[ray_start_regular0] PASSED

=================== 17 passed, 1 skipped in 77.01s (0:01:17) ===================
```
