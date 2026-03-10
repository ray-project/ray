# Combined NIXL + NCCL Tensor Transport Test — Rust Ray Backend

## Summary

| Metric | Value |
|--------|-------|
| **Result** | **21 passed, 1 skipped, 0 failed** |
| **Duration** | 97.50 seconds |
| **Date** | March 10, 2026 |
| **Test files** | `test_rdt_nixl.py` (21 tests) + `test_rdt_nccl.py` (1 test) from GitHub master |
| **Backend** | All-Rust Ray (`_raylet.so` via PyO3), no C++ Ray code |
| **Instance** | AWS g4dn.12xlarge (4x NVIDIA T4, 48 vCPUs, 192 GiB RAM) |
| **Region** | us-east-1 |
| **Instance ID** | i-0bed6bbc12f86e059 |

## Test Results — NIXL (`test_rdt_nixl.py`)

| # | Test | GPUs | Result |
|---|------|------|--------|
| 1 | `test_ray_get_rdt_ref_created_by_actor_task` | 1 | PASSED |
| 2 | `test_p2p` | 2 | PASSED |
| 3 | `test_intra_rdt_tensor_transfer` | 1 | PASSED |
| 4 | `test_put_and_get_object_with_nixl` | 2 | PASSED |
| 5 | `test_put_and_get_object_with_object_store` | 2 | PASSED |
| 6 | `test_put_gc` | 1 | PASSED |
| 7 | `test_send_duplicate_tensor` | 2 | PASSED |
| 8 | `test_nixl_abort_sender_dies_before_creating` | 2 | PASSED |
| 9 | `test_nixl_abort_sender_dies_before_sending` | 2 | PASSED |
| 10 | `test_nixl_del_before_creating` | 1 | PASSED |
| 11 | `test_nixl_owner_gets_from_launched_task` | 1 | PASSED |
| 12 | `test_out_of_order_actors` | 2 | PASSED |
| 13 | `test_nixl_borrow_after_abort` | 2 | SKIPPED (expected) |
| 14 | `test_shared_tensor_deduplication` | 1 | PASSED |
| 15 | `test_nixl_agent_reuse` | 2 | PASSED |
| 16 | `test_nixl_agent_reuse_with_partial_tensors` | 2 | PASSED |
| 17 | `test_storage_level_overlapping_views_reference_count` | 1 | PASSED |
| 18 | `test_storage_level_overlapping_views` | 2 | PASSED |
| 19 | `test_wait_tensor_freed_views` | 1 | PASSED |
| 20 | `test_nixl_get_into_tensor_buffers` | 2 | PASSED |
| 21 | `test_register_nixl_memory` | 1 | PASSED |

## Test Results — NCCL (`test_rdt_nccl.py`)

| # | Test | GPUs | Result |
|---|------|------|--------|
| 1 | `test_p2p` | 2 | PASSED |

## What Was Tested

### NIXL Tests
The NIXL test suite exercises the full one-sided RDMA tensor transport path:
- **Basic transfers**: GPU-to-GPU and CPU-to-CPU peer-to-peer via NIXL (UCX backend)
- **Intra-actor**: Tensor passing within a single GPU actor
- **ray.put / ray.get**: Tensor round-trips through both NIXL and object store paths
- **Garbage collection**: Reference counting, tensor lifecycle, `wait_tensor_freed`
- **Duplicate tensors**: Deduplication when the same tensor appears in multiple calls
- **Error handling**: Sender death mid-transfer (abort path), early ref deletion
- **Agent reuse**: NIXL agent caching across multiple transfers and after GC
- **Overlapping views**: Tensors sharing underlying storage (view-level registration)
- **Pre-allocated buffers**: `set_target_for_ref` for zero-copy receives
- **Persistent registration**: `register_nixl_memory` for long-lived tensor buffers
- **Out-of-order actors**: 100 sequential tensor transfers between 2 actors
- **Owner-initiated gets**: Driver `ray.get()` on NIXL tensor refs

### NCCL Test
- **P2P transfer via NCCL**: Creates a collective group with 2 GPU actors, sends a tensor from rank 0 to rank 1 using `create_collective_group` + NCCL transport

## Key Fix: Thread Pool Shutdown

The combined test run required fixing a thread pool lifecycle issue:

**Problem**: `_dispatch_pool.shutdown(wait=True)` blocked during `ray.shutdown()` fixture teardown, because background threads were stuck in long-running `driver.get()` calls (120s timeout) for dead actors (killed by abort tests). This ate the pytest timeout budget.

**Solution**: Create a fresh pool first, then shutdown the old pool non-blocking:
```python
old_pool = _dispatch_pool
_dispatch_pool = _ThreadPoolExecutor(max_workers=256, ...)
old_pool.shutdown(wait=False, cancel_futures=True)
```

This ensures:
1. New test sessions immediately get a clean pool
2. Stuck threads in the old pool don't block teardown
3. Queued-but-not-started work is cancelled
4. Idle threads exit promptly via the shutdown sentinel

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
  |
  +-- NCCL: create_collective_group() -> torch.distributed NCCL backend
        +-- GPU actors joined into a ProcessGroup
        +-- P2P send/recv via NCCL allreduce
```

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
cargo build --release -p ray-core-worker-pylib --features python
cp target/release/lib_raylet.so target/release/_raylet.so

# 2. Set PYTHONPATH
export PYTHONPATH=$PWD:$PWD/target/release:$PYTHONPATH

# 3. Copy test files and conftest.py into a test directory
mkdir -p /tmp/nixl-test
cp <path-to>/python/ray/tests/rdt/test_rdt_nixl.py /tmp/nixl-test/
cp <path-to>/python/ray/tests/rdt/test_rdt_nccl.py /tmp/nixl-test/
cp rust/examples/python/rdt-test/conftest.py /tmp/nixl-test/

# 4. Run
cd /tmp/nixl-test
python3 -m pytest test_rdt_nixl.py test_rdt_nccl.py -sv --timeout=120
```

## Raw Output

```
============================= test session starts ==============================
platform linux -- Python 3.10.12, pytest-9.0.2, pluggy-1.6.0 -- /usr/bin/python3
cachedir: .pytest_cache
rootdir: /home/ubuntu/nixl-test
plugins: timeout-2.4.0
timeout: 120.0s
timeout method: signal
timeout func_only: False
collecting ... collected 22 items

test_rdt_nixl.py::test_ray_get_rdt_ref_created_by_actor_task[ray_start_regular0] PASSED
test_rdt_nixl.py::test_p2p[ray_start_regular0] PASSED
test_rdt_nixl.py::test_intra_rdt_tensor_transfer[ray_start_regular0] PASSED
test_rdt_nixl.py::test_put_and_get_object_with_nixl[ray_start_regular0] PASSED
test_rdt_nixl.py::test_put_and_get_object_with_object_store[ray_start_regular0] PASSED
test_rdt_nixl.py::test_put_gc[ray_start_regular0] PASSED
test_rdt_nixl.py::test_send_duplicate_tensor[ray_start_regular0] PASSED
test_rdt_nixl.py::test_nixl_abort_sender_dies_before_creating[ray_start_regular0] PASSED
test_rdt_nixl.py::test_nixl_abort_sender_dies_before_sending[ray_start_regular0] PASSED
test_rdt_nixl.py::test_nixl_del_before_creating[ray_start_regular0] PASSED
test_rdt_nixl.py::test_nixl_owner_gets_from_launched_task[ray_start_regular0] PASSED
test_rdt_nixl.py::test_out_of_order_actors[ray_start_regular0] PASSED
test_rdt_nixl.py::test_nixl_borrow_after_abort[ray_start_regular0] SKIPPED
test_rdt_nixl.py::test_shared_tensor_deduplication[ray_start_regular0] PASSED
test_rdt_nixl.py::test_nixl_agent_reuse[ray_start_regular0] PASSED
test_rdt_nixl.py::test_nixl_agent_reuse_with_partial_tensors[ray_start_regular0] PASSED
test_rdt_nixl.py::test_storage_level_overlapping_views_reference_count[ray_start_regular0] PASSED
test_rdt_nixl.py::test_storage_level_overlapping_views[ray_start_regular0] PASSED
test_rdt_nixl.py::test_wait_tensor_freed_views[ray_start_regular0] PASSED
test_rdt_nixl.py::test_nixl_get_into_tensor_buffers[ray_start_regular0] PASSED
test_rdt_nixl.py::test_register_nixl_memory[ray_start_regular0] PASSED
test_rdt_nccl.py::test_p2p[ray_start_regular0] PASSED

=================== 21 passed, 1 skipped in 97.50s (0:01:37) ===================
```
