# Ray Rust Backend - Multi-Node Distributed Experiment Report

## Overview

This experiment demonstrates the Ray distributed computing framework running on a **100% Rust backend** across a 3-node AWS cluster. All core components — GCS server, Raylet, and CoreWorker — are implemented in Rust, with Python task functions executed via PyO3 bindings. No C++ files are used.

This report documents two independent runs of the experiment on separate clusters.

---

## Run 2 — March 9, 2026 (us-east-1)

### Infrastructure
- **Cloud**: AWS EC2, us-east-1 region
- **Instance type**: t3.large (2 vCPUs, 8 GB RAM)
- **OS**: Ubuntu 22.04 LTS (x86_64)
- **Nodes**: 3

| Role | Instance ID | Public IP | Private IP |
|------|-------------|-----------|------------|
| Head | i-0fe5b2d5dede2bbcd | 54.162.152.104 | 172.31.39.163 |
| Worker 1 | i-0270fb5f2a74a6797 | 54.226.168.64 | 172.31.39.189 |
| Worker 2 | i-0b4cfffdf1d3950b3 | 23.20.46.8 | 172.31.47.126 |

### Software Stack
- **Rust**: 1.94.0 (stable-x86_64-unknown-linux-gnu)
- **Python**: 3.10
- **Build tool**: maturin 1.12.6 (for PyO3 wheel)
- **Protobuf compiler**: protoc 25.1 (installed from GitHub releases)
- **Rust crates**: 17 crates in the `ray/rust/` workspace

### Rust Components Running
1. **Rust GCS Server** (`gcs_server`, 11 MB release binary) — on head node, port 6379
2. **Rust Raylet** (`raylet`, 7.4 MB release binary) — on all 3 nodes, random ports
3. **Rust CoreWorker** (`_raylet.so` via PyO3, 4.3 MB wheel) — driver on head, workers on all 3 nodes

### Build & Deployment

```bash
# On head node — build binaries (~4 min) and wheel (~4 min):
cargo build --release -p ray-raylet -p ray-gcs
cd ray-core-worker-pylib && maturin build --release --features python

# Deploy wheel to all nodes via scp + pip install:
pip3 install ray_rust_raylet-3.0.0.dev0-cp310-cp310-manylinux_2_34_x86_64.whl
```

### Startup Sequence

**1. GCS Server (head node)**
```bash
./gcs_server --gcs-server-port 6379 --node-ip-address 172.31.39.163 --session-name rust-cluster-test
```

**2. Raylets (all 3 nodes)**
```bash
./raylet --node-ip-address <NODE_IP> --port 0 --gcs-address 172.31.39.163:6379 \
  --resources CPU:2 --session-name rust-cluster-test
```

**3. Workers (all 3 nodes)**
```bash
python3 worker.py --gcs-address 172.31.39.163:6379
```

**4. Driver (head node)**
```bash
python3 driver.py --gcs-address 172.31.39.163:6379 \
  --worker 172.31.39.163:38543:fd9b852d2538982c2dc8e9d0e6ad679654881a5ab09a1477c3993fc2 \
  --worker 172.31.39.189:41721:31892b1e64061d9f21f65d4d20307c10d5eadec0f8ba896f96fb4ff1 \
  --worker 172.31.47.126:32833:ea9d1cf54960074c5eaeb793929f780fc5d75941f9ca103333106112
```

### Worker Outputs

Each worker reported its own node IP correctly upon startup:

**Head node worker (172.31.39.163):**
```
[worker] Node IP: 172.31.39.163
[worker] GCS address: 172.31.39.163:6379
[worker] Worker ID: fd9b852d2538982c2dc8e9d0e6ad679654881a5ab09a1477c3993fc2
[worker] gRPC server listening on 0.0.0.0:38543
[worker] Ready to accept tasks. Registered functions: ['get_node_ip', 'compute_square', 'sum_range', 'process_text', 'coordinator']
[worker] Worker info: ip=172.31.39.163 port=38543 id=fd9b852d2538982c2dc8e9d0e6ad679654881a5ab09a1477c3993fc2
[worker] CONNECT_STRING=172.31.39.163:38543:fd9b852d2538982c2dc8e9d0e6ad679654881a5ab09a1477c3993fc2
```

**Worker 1 node (172.31.39.189):**
```
[worker] Node IP: 172.31.39.189
[worker] GCS address: 172.31.39.163:6379
[worker] Worker ID: 31892b1e64061d9f21f65d4d20307c10d5eadec0f8ba896f96fb4ff1
[worker] gRPC server listening on 0.0.0.0:41721
[worker] Ready to accept tasks. Registered functions: ['get_node_ip', 'compute_square', 'sum_range', 'process_text', 'coordinator']
[worker] Worker info: ip=172.31.39.189 port=41721 id=31892b1e64061d9f21f65d4d20307c10d5eadec0f8ba896f96fb4ff1
[worker] CONNECT_STRING=172.31.39.189:41721:31892b1e64061d9f21f65d4d20307c10d5eadec0f8ba896f96fb4ff1
```

**Worker 2 node (172.31.47.126):**
```
[worker] Node IP: 172.31.47.126
[worker] GCS address: 172.31.39.163:6379
[worker] Worker ID: ea9d1cf54960074c5eaeb793929f780fc5d75941f9ca103333106112
[worker] gRPC server listening on 0.0.0.0:32833
[worker] Ready to accept tasks. Registered functions: ['get_node_ip', 'compute_square', 'sum_range', 'process_text', 'coordinator']
[worker] Worker info: ip=172.31.47.126 port=32833 id=ea9d1cf54960074c5eaeb793929f780fc5d75941f9ca103333106112
[worker] CONNECT_STRING=172.31.47.126:32833:ea9d1cf54960074c5eaeb793929f780fc5d75941f9ca103333106112
```

### Results

#### Test 1: get_node_ip() — 30 tasks
```
Task distribution across nodes:
  172.31.39.163: 10 tasks
  172.31.39.189: 10 tasks
  172.31.47.126: 10 tasks
Total tasks: 30, Unique nodes: 3
```

#### Test 2: compute_square(x) — 20 tasks
```
     0 = square(0)  on 172.31.39.163
     1 = square(1)  on 172.31.39.189
     4 = square(2)  on 172.31.47.126
     9 = square(3)  on 172.31.39.163
    16 = square(4)  on 172.31.39.189
    25 = square(5)  on 172.31.47.126
    36 = square(6)  on 172.31.39.163
    49 = square(7)  on 172.31.39.189
    64 = square(8)  on 172.31.47.126
    81 = square(9)  on 172.31.39.163
   100 = square(10) on 172.31.39.189
   121 = square(11) on 172.31.47.126
   144 = square(12) on 172.31.39.163
   169 = square(13) on 172.31.39.189
   196 = square(14) on 172.31.47.126
   225 = square(15) on 172.31.39.163
   256 = square(16) on 172.31.39.189
   289 = square(17) on 172.31.47.126
   324 = square(18) on 172.31.39.163
   361 = square(19) on 172.31.39.189

Distribution: 172.31.39.163: 7, 172.31.39.189: 7, 172.31.47.126: 6
```

#### Test 3: sum_range(n) — 9 CPU-intensive tasks
```
  sum(0.. 100000) =   4,999,950,000 on 172.31.47.126
  sum(0.. 200000) =  19,999,900,000 on 172.31.39.163
  sum(0.. 300000) =  44,999,850,000 on 172.31.39.189
  sum(0.. 400000) =  79,999,800,000 on 172.31.47.126
  sum(0.. 500000) = 124,999,750,000 on 172.31.39.163
  sum(0.. 600000) = 179,999,700,000 on 172.31.39.189
  sum(0.. 700000) = 244,999,650,000 on 172.31.47.126
  sum(0.. 800000) = 319,999,600,000 on 172.31.39.163
  sum(0.. 900000) = 404,999,550,000 on 172.31.39.189
```

#### Test 4: process_text() — 6 string processing tasks
```
  'hello world' -> 'HELLO WORLD' (len=11) on 172.31.47.126
  'ray rust backend' -> 'RAY RUST BACKEND' (len=16) on 172.31.39.163
  'distributed computing' -> 'DISTRIBUTED COMPUTING' (len=21) on 172.31.39.189
  'multi node cluster' -> 'MULTI NODE CLUSTER' (len=18) on 172.31.47.126
  'python tasks' -> 'PYTHON TASKS' (len=12) on 172.31.39.163
  'three aws nodes' -> 'THREE AWS NODES' (len=15) on 172.31.39.189
```

#### Summary
```
Backend: 100% Rust (GCS + Raylet + CoreWorker)
Nodes: 3 (from 3 workers)
Total tasks executed: 65
Task distribution: {'172.31.39.163': 10, '172.31.39.189': 10, '172.31.47.126': 10}
```

### IP Address Verification

Every task's reported IP was checked against the expected node for its round-robin slot.
The dispatch order cycles: Head (172.31.39.163) -> Worker1 (172.31.39.189) -> Worker2 (172.31.47.126).

| Task Index (mod 3) | Expected Node | Reported IP | Match |
|---|---|---|---|
| 0 | Head (172.31.39.163) | 172.31.39.163 | YES |
| 1 | Worker1 (172.31.39.189) | 172.31.39.189 | YES |
| 2 | Worker2 (172.31.47.126) | 172.31.47.126 | YES |

All 65 tasks reported the correct node IP matching their assigned worker. **Zero mismatches.**

---

## Run 1 — Original Experiment (us-west-2)

### Infrastructure
- **Cloud**: AWS EC2, us-west-2 region
- **Instance type**: t3.large (2 vCPUs, 8 GB RAM)
- **OS**: Ubuntu 22.04 LTS (x86_64)
- **Nodes**: 3

| Role | Public IP | Private IP |
|------|-----------|------------|
| Head | 35.91.74.143 | 172.31.16.144 |
| Worker 1 | 54.185.27.181 | 172.31.21.211 |
| Worker 2 | 18.237.101.218 | 172.31.27.203 |

### Software Stack
- **Rust**: 1.86.0 (compiled from source)
- **Python**: 3.10
- **Build tool**: maturin (for PyO3 wheel)
- **Rust crates**: 17 crates in the `ray/rust/` workspace

### Rust Components Running
1. **Rust GCS Server** (`gcs_server`) — on head node, port 6379
2. **Rust Raylet** (`raylet`) — on all 3 nodes, random ports
3. **Rust CoreWorker** (`_raylet.so` via PyO3) — driver on head, workers on all 3 nodes

### Build & Deployment

```bash
# On head node:
cargo build --release -p ray-raylet -p ray-gcs    # ~3.5 min
maturin build --release --features python          # ~4 min (builds _raylet.so wheel)
pip install target/wheels/*.whl                    # Install on all nodes
```

### Startup Sequence

**1. GCS Server (head node)**
```bash
./gcs_server --gcs-server-port 6379 --node-ip-address 172.31.16.144 --session-name rust-experiment
```

**2. Raylets (all 3 nodes)**
```bash
./raylet --node-ip-address <NODE_IP> --port 0 --gcs-address 172.31.16.144:6379 \
  --resources CPU:2 --session-name rust-experiment --node-id <NODE_ID>
```

**3. Workers (all 3 nodes)**
```bash
python3 worker.py --gcs-address 172.31.16.144:6379
```

**4. Driver (head node)**
```bash
python3 driver.py --gcs-address 172.31.16.144:6379 \
  --worker 172.31.16.144:38111:<worker_id_hex> \
  --worker 172.31.21.211:42819:<worker_id_hex> \
  --worker 172.31.27.203:42099:<worker_id_hex>
```

### Results

#### Test 1: get_node_ip() — 30 tasks
```
Task distribution across nodes:
  172.31.16.144: 10 tasks
  172.31.21.211: 10 tasks
  172.31.27.203: 10 tasks
Total tasks: 30, Unique nodes: 3
```

#### Test 2: compute_square(x) — 20 tasks
```
   0 = square(0) on 172.31.16.144
   1 = square(1) on 172.31.21.211
   4 = square(2) on 172.31.27.203
   9 = square(3) on 172.31.16.144
  16 = square(4) on 172.31.21.211
  25 = square(5) on 172.31.27.203
  ...
 361 = square(19) on 172.31.21.211

Distribution: 172.31.16.144: 7, 172.31.21.211: 7, 172.31.27.203: 6
```

#### Test 3: sum_range(n) — 9 CPU-intensive tasks
```
  sum(0.. 100000) =   4,999,950,000 on 172.31.27.203
  sum(0.. 200000) =  19,999,900,000 on 172.31.16.144
  sum(0.. 300000) =  44,999,850,000 on 172.31.21.211
  sum(0.. 400000) =  79,999,800,000 on 172.31.27.203
  sum(0.. 500000) = 124,999,750,000 on 172.31.16.144
  sum(0.. 600000) = 179,999,700,000 on 172.31.21.211
  sum(0.. 700000) = 244,999,650,000 on 172.31.27.203
  sum(0.. 800000) = 319,999,600,000 on 172.31.16.144
  sum(0.. 900000) = 404,999,550,000 on 172.31.21.211
```

#### Test 4: process_text() — 6 string processing tasks
```
  'hello world' -> 'HELLO WORLD' (len=11) on 172.31.27.203
  'ray rust backend' -> 'RAY RUST BACKEND' (len=16) on 172.31.16.144
  'distributed computing' -> 'DISTRIBUTED COMPUTING' (len=21) on 172.31.21.211
  'multi node cluster' -> 'MULTI NODE CLUSTER' (len=18) on 172.31.27.203
  'python tasks' -> 'PYTHON TASKS' (len=12) on 172.31.16.144
  'three aws nodes' -> 'THREE AWS NODES' (len=15) on 172.31.21.211
```

#### Summary
```
Backend: 100% Rust (GCS + Raylet + CoreWorker)
Nodes: 3
Total tasks executed: 65
Task distribution: {172.31.16.144: 10, 172.31.21.211: 10, 172.31.27.203: 10}
```

---

## Architecture

```
                   Head Node
                   ┌───────────────────────┐
                   │  Rust GCS Server      │
                   │  (port 6379)          │
                   │                       │
                   │  Rust Raylet          │
                   │  (random port)        │
                   │                       │
                   │  Rust CoreWorker      │
                   │  (driver + worker)    │
                   └───────────┬───────────┘
                               │ gRPC PushTask
              ┌────────────────┼────────────────┐
              │                │                │
  Worker 1 Node                │    Worker 2 Node
  ┌──────────────────┐        │    ┌──────────────────┐
  │  Rust Raylet     │        │    │  Rust Raylet     │
  │  Rust CoreWorker │        │    │  Rust CoreWorker │
  │  (worker)        │        │    │  (worker)        │
  └──────────────────┘        │    └──────────────────┘
                               │
                        Round-Robin
                        Dispatch
```

## Distributed Program

The driver submits 65 tasks across 4 test categories:

### Task Functions
1. **`get_node_ip()`** — Returns the executing node's private IP
2. **`compute_square(x)`** — Computes x^2, returns result + node IP
3. **`sum_range(n)`** — Computes sum(0..n), returns sum + node IP
4. **`process_text(text)`** — Transforms text to uppercase, returns metadata + node IP

### Dispatch Mechanism
Tasks are dispatched from the driver to workers via **round-robin** using the Rust `RoundRobinDispatchClient` (a `RayletClient` implementation). Each task is sent as a `PushTask` gRPC call to the assigned worker's `CoreWorkerService`.

### Communication Flow
1. Driver creates `PyCoreWorker` (Rust CoreWorker via PyO3)
2. `setup_multi_worker_dispatch()` configures round-robin across 3 workers
3. `submit_task()` calls `NormalTaskSubmitter` -> `RoundRobinDispatchClient.request_worker_lease()`
4. Dispatch callback sends `PushTask` gRPC to the selected worker
5. Worker's `CoreWorkerService` receives task, calls Python callback via PyO3
6. Python function executes, result serialized with `pickle`
7. Return objects sent back in `PushTaskReply`, stored in driver's `MemoryStore`
8. Driver calls `get()` to retrieve results

## Code Changes Made

### New: `RoundRobinDispatchClient`
**File**: `ray-core-worker-pylib/src/core_worker.rs`

A `RayletClient` implementation that distributes lease grants across multiple workers using atomic round-robin indexing. This enables multi-node task dispatch without requiring full raylet-based scheduling.

### New: `setup_multi_worker_dispatch()` PyO3 method
**File**: `ray-core-worker-pylib/src/core_worker.rs`

Python-callable method that accepts a list of `(ip, port, worker_id_hex)` tuples and configures the task submitter for multi-worker dispatch.

### Modified: `start_grpc_server()` — accept bind address
**File**: `ray-core-worker-pylib/src/core_worker.rs`

Changed from hardcoded `127.0.0.1:0` to accept `bind_ip` and `bind_port` parameters, allowing workers on different nodes to accept cross-network gRPC connections.

## Issues Encountered

1. **C++ CoreWorker incompatibility**: The C++ `_raylet.so` requires Flatbuffers-over-Unix-socket IPC to communicate with the raylet. The Rust raylet only provides gRPC. Resolved by using the Rust CoreWorker instead.

2. **`GetSystemConfig` auth_mode crash**: C++ CoreWorker's `RayConfig::initialize()` crashes on unrecognized config keys. Fixed by returning the original raw config JSON instead of a Rust-serialized version.

3. **gRPC bind address**: Workers defaulted to `127.0.0.1`, making them unreachable from the driver on other nodes. Fixed by parameterizing the bind address to `0.0.0.0`.

4. **Python output buffering**: Worker logs appeared empty due to Python's output buffering with `nohup`. Fixed with `PYTHONUNBUFFERED=1`.

5. **protoc version (Run 2)**: Ubuntu 22.04 ships protoc 3.12 which does not support proto3 optional fields. Resolved by installing protoc 25.1 from GitHub releases.

## Conclusion

This experiment has been successfully reproduced twice on independent AWS clusters (us-west-2 and us-east-1) with identical results. The Ray Rust backend is capable of running distributed Python tasks across multiple AWS nodes with **zero C++ code** in the critical path. All 65 tasks were successfully executed and returned correct results in both runs, with even distribution across all 3 nodes. Every task's reported IP address matches the expected node for its round-robin dispatch slot — **zero mismatches** across 130 total tasks (65 per run). The Rust implementation provides:

- Full gRPC-based communication (no Flatbuffers/Unix sockets)
- PyO3 Python interop for task execution
- Cross-node task dispatch via `PushTask` RPC
- In-memory object storage for return values
