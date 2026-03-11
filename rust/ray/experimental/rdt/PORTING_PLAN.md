# Rust Porting Plan: Python RDT → Rust

## Goal

Port the Python RDT (Ray Direct Transport) module from `python/ray/experimental/rdt/` to Rust for improved performance and reliability. The Rust implementation will be exposed to Python via PyO3 bindings, replacing the pure-Python RDT layer.

## Source Files to Port

| Python File | Lines | Complexity | Priority |
|-------------|-------|------------|----------|
| `tensor_transport_manager.py` | 186 | Low | Phase 1 |
| `rdt_store.py` | 359 | Medium | Phase 1 |
| `util.py` | 280 | Medium | Phase 1 |
| `rdt_manager.py` | 812 | High | Phase 2 |
| `nixl_tensor_transport.py` | 405 | High | Phase 3 |
| `collective_tensor_transport.py` | 204 | Medium | Phase 4 |
| `cuda_ipc_transport.py` | 215 | Medium | Phase 4 |
| `__init__.py` | 26 | Low | Phase 1 |

---

## Architecture Decision: Rust + PyO3 Hybrid

### Approach
Port the core data structures and hot-path logic to Rust, exposed via PyO3 `#[pyclass]` / `#[pymethods]`. Transport backends (NIXL, NCCL, CUDA IPC) remain as Python trait implementations initially, since they call into Python-only libraries (nixl, torch.distributed, torch.cuda).

```
┌──────────────────────────────────────┐
│         Python Layer (thin)          │
│  - Transport backend implementations │
│  - Actor task callbacks              │
│  - torch/nixl/nccl library calls     │
├──────────────────────────────────────┤
│         Rust Layer (via PyO3)        │
│  - RDTStore (lock-free or Mutex)     │
│  - RDTManager (transfer orchestration│
│  - Transport registry (singleton)    │
│  - Metadata types (zero-copy serde)  │
└──────────────────────────────────────┘
```

### Rationale
1. **RDTStore**: Hot path — every `ray.get()` on RDT objects goes through it. Thread-safe operations with condition variables are more efficient in Rust (no GIL contention).
2. **RDTManager**: Medium path — orchestrates transfers. Complex state machine that benefits from Rust's ownership model and fearless concurrency.
3. **Transport backends**: Cold path — called once per transfer. They interact with Python C extensions (NIXL, NCCL) that have no Rust bindings. Keep in Python initially.

---

## Phase 1: Core Data Structures (Week 1-2)

### 1.1 Metadata Types (`tensor_transport_manager.py`)

**Rust module**: `ray-rdt/src/metadata.rs`

```rust
/// Base metadata for tensor transport.
#[pyclass]
#[derive(Clone, Debug)]
pub struct TensorTransportMetadata {
    /// List of (shape, dtype_str) tuples
    #[pyo3(get, set)]
    pub tensor_meta: Vec<(Vec<i64>, String)>,
    /// Device type string (e.g., "cuda", "cpu")
    #[pyo3(get, set)]
    pub tensor_device: Option<String>,
}

/// Base communicator metadata (empty, subclassed in Python).
#[pyclass(subclass)]
#[derive(Clone, Debug)]
pub struct CommunicatorMetadata {}
```

**Type mapping**:
| Python | Rust |
|--------|------|
| `List[Tuple[torch.Size, torch.dtype]]` | `Vec<(Vec<i64>, String)>` — store dtype as string, convert at boundary |
| `Optional[str]` | `Option<String>` |
| `dataclass` | `#[pyclass]` struct with `#[pyo3(get, set)]` |

**Key decisions**:
- Store `torch.dtype` as string in Rust, convert to/from Python at the boundary. This avoids depending on PyTorch in Rust.
- Use `#[pyclass(subclass)]` on `CommunicatorMetadata` so Python subclasses (`NixlCommunicatorMetadata`, `CollectiveCommunicatorMetadata`) can extend it.

### 1.2 RDT Store (`rdt_store.py`)

**Rust module**: `ray-rdt/src/store.rs`

This is the highest-priority port because it's the hot path and benefits most from Rust's concurrency.

```rust
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Condvar, Mutex, Arc};
use pyo3::prelude::*;
use pyo3::types::PyList;

#[derive(Clone)]
struct RDTObject {
    data: Py<PyList>,      // List of tensors (Python objects)
    is_primary: bool,
    error: Option<Py<PyAny>>, // Python exception
}

#[pyclass]
pub struct RDTStore {
    inner: Arc<Mutex<RDTStoreInner>>,
    object_present: Arc<Condvar>,
    object_freed: Arc<Condvar>,
}

struct RDTStoreInner {
    store: HashMap<String, VecDeque<RDTObject>>,
    tensor_to_object_ids: HashMap<usize, HashSet<String>>,
}
```

**Critical implementation notes**:
1. **GIL release**: All blocking waits (`wait_and_get_object`, `wait_tensor_freed`) must release the GIL using `py.allow_threads()`. This prevents deadlock when Python threads need to add objects to the store.
2. **Condvar instead of Python threading.Condition**: Use Rust `std::sync::Condvar` for wake-up efficiency.
3. **Python object ownership**: Tensors are `Py<PyAny>` (reference-counted Python objects). The Rust store holds strong references.
4. **tensor_to_object_ids key**: Use tensor's `id()` (Python `id()` returns memory address as `usize`).

**Methods to implement**:
| Method | Notes |
|--------|-------|
| `has_object(obj_id)` | Lock, check HashMap |
| `has_tensor(tensor)` | Lock, check tensor_to_object_ids by Python id() |
| `get_object(obj_id)` | Lock, peek front of deque, raise stored error |
| `add_object(obj_id, rdt_object, is_primary)` | Lock, append to deque, track tensors, notify condvar |
| `add_object_primary(obj_id, tensors, transport)` | Calls add_object + Python transport.extract_metadata |
| `pop_object(obj_id)` | Lock, pop front, remove tensor tracking, notify freed condvar |
| `wait_and_get_object(obj_id, timeout)` | Condvar wait with GIL release |
| `wait_and_pop_object(obj_id, timeout)` | Condvar wait with GIL release |
| `wait_tensor_freed(tensor, timeout)` | Condvar wait on tensor removal |
| `get_num_objects()` | Lock, sum deque lengths |
| `is_primary_copy(obj_id)` | Lock, check front of deque |

### 1.3 Transport Registry (`util.py`)

**Rust module**: `ray-rdt/src/registry.rs`

```rust
use std::collections::HashMap;
use std::sync::{Mutex, Once};
use pyo3::prelude::*;

#[pyclass]
#[derive(Clone)]
pub struct TransportManagerInfo {
    pub transport_manager_class: Py<PyAny>,
    pub devices: Vec<String>,
    pub data_type: Py<PyAny>,
}

static INIT: Once = Once::new();

#[pyclass]
pub struct TransportRegistry {
    info: Mutex<HashMap<String, TransportManagerInfo>>,
    managers: Mutex<HashMap<String, Py<PyAny>>>,
    has_custom: bool,
}
```

**Functions to implement as `#[pyfunction]`**:
- `register_tensor_transport(name, devices, class, data_type)`
- `get_tensor_transport_manager(name)` — lazy singleton
- `get_transport_data_type(name)`
- `device_match_transport(device, transport)`
- `normalize_and_validate_tensor_transport(transport)`
- `validate_one_sided(transport, func_name)`
- `register_nixl_memory(tensor)` — delegates to NIXL transport
- `create_empty_tensors_from_metadata(meta)` — creates `torch.empty()` via PyO3

### 1.4 Helper Functions (module-level in `rdt_store.py`)

**Rust module**: `ray-rdt/src/store_helpers.rs`

These are actor task callbacks. Keep as Python functions initially, move hot paths to Rust later:
- `__ray_send__` — calls Python transport.send_multiple_tensors
- `__ray_recv__` — calls Python transport.recv_multiple_tensors
- `__ray_abort_transport__` — calls Python transport.abort_transport
- `__ray_free__` — calls Python transport.garbage_collect
- `__ray_fetch_rdt_object__` — returns tensors from store
- `validate_tensor_buffers` — pure validation, easy to port

---

## Phase 2: Transfer Orchestration (Week 3-4)

### 2.1 RDT Manager (`rdt_manager.py`)

**Rust module**: `ray-rdt/src/manager.rs`

This is the most complex module. The RDTManager runs on the owner process and orchestrates transfers.

```rust
#[pyclass]
pub struct RDTManager {
    inner: Arc<Mutex<RDTManagerInner>>,
    meta_cv: Arc<Condvar>,
    store: Arc<RDTStore>,  // Lazy, created on first access
    unmonitored_transfers: Arc<Mutex<VecDeque<TransferMetadata>>>,
    monitor_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    shutdown_event: Arc<AtomicBool>,
}

struct RDTManagerInner {
    managed_metadata: HashMap<String, RDTMeta>,
    queued_transfers: HashMap<String, Vec<Py<PyAny>>>,  // ActorHandles
    queued_frees: HashSet<String>,
    actor_transports_registered: HashMap<String, TransportRegistrationState>,
}

#[derive(Clone)]
struct RDTMeta {
    src_actor: Py<PyAny>,
    tensor_transport_backend: String,
    tensor_transport_meta: Option<Py<PyAny>>,  // TensorTransportMetadata
    sent_dest_actors: HashSet<String>,
    warned: bool,
    target_buffers: Option<Vec<Py<PyAny>>>,  // weak refs
}
```

**Key methods to port**:

| Method | Complexity | Notes |
|--------|------------|-------|
| `add_rdt_ref` | Low | Insert into HashMap |
| `set_rdt_metadata` / `get_rdt_metadata` | Low | Lock + HashMap ops |
| `set_tensor_transport_metadata_and_trigger_queued_operations` | Medium | Lock, update, drain queued transfers/frees, notify condvar |
| `set_target_buffers_for_ref` | Low | Lock, update metadata |
| `queue_or_trigger_out_of_band_tensor_transfer` | Medium | Iterate task args, check ObjectRefs, queue or trigger |
| `trigger_out_of_band_tensor_transfer` | High | Core transfer logic — needs GIL for actor.__ray_call__ |
| `_monitor_failures` | High | Background thread calling ray.wait/ray.get via PyO3 |
| `_abort_transport` | Medium | Calls ray.kill, destroy_collective_group via PyO3 |
| `_fetch_object` | Medium | Two paths: object store and direct |
| `get_rdt_object` | Medium | Entry point for ray.get on RDT objects |
| `queue_or_free_object_primary_copy` | Medium | Lock, queue or free |
| `free_object_primary_copy` | Low | Pop metadata, submit __ray_free__ |
| `put_object` | Low | Add to store, register metadata |

**Threading considerations**:
- The monitor thread must acquire the GIL periodically to call `ray.wait()` and `ray.get()`.
- Use `py.allow_threads()` for blocking operations.
- The background thread should use `Python::with_gil()` when it needs Python access.

### 2.2 Public API Functions

Port `wait_tensor_freed()` and `set_target_for_ref()` as `#[pyfunction]`:

```rust
#[pyfunction]
fn wait_tensor_freed(py: Python, tensor: Py<PyAny>, timeout: Option<f64>) -> PyResult<()> {
    let rdt_manager = get_global_rdt_manager(py)?;
    let store = rdt_manager.rdt_store(py)?;
    py.allow_threads(|| store.wait_tensor_freed(tensor_id, timeout))
}
```

---

## Phase 3: NIXL Transport (Week 5-6)

### 3.1 NIXL Tensor Transport (`nixl_tensor_transport.py`)

**Rust module**: `ray-rdt/src/nixl_transport.rs`

The NIXL transport is the most performance-critical transport. Key data structures:

```rust
#[pyclass]
pub struct NixlTensorTransport {
    nixl_agent: Mutex<Option<Py<PyAny>>>,  // Lazy nixl_agent
    aborted_ids: Mutex<HashSet<String>>,
    tensor_desc_cache: Mutex<HashMap<usize, TensorDesc>>,  // data_ptr → desc
    managed_meta: Mutex<HashMap<String, Py<PyAny>>>,  // obj_id → NixlTransportMetadata
    remote_agents: Mutex<OrderedMap<String, i64>>,  // LRU cache
    meta_version: AtomicI64,
}

struct TensorDesc {
    reg_desc: Py<PyAny>,  // nixlRegDList
    metadata_count: usize,
}
```

**Critical methods**:

| Method | Port strategy |
|--------|---------------|
| `get_nixl_agent()` | Keep calling Python nixl API via PyO3 |
| `_add_tensor_descs(tensors)` | Port refcounting to Rust, call nixl.register_memory via PyO3 |
| `extract_tensor_transport_metadata` | Mostly Python interop (torch.cuda.synchronize, nixl calls) |
| `recv_multiple_tensors` | **Hot path** — port poll loop to Rust with GIL release between polls |
| `garbage_collect` | Port refcount decrement and deregister to Rust |
| `abort_transport` | Trivial — add to HashSet |

**Performance opportunity**: The `recv_multiple_tensors` poll loop currently sleeps 1ms between state checks. In Rust, we can use a tighter loop with `std::thread::yield_now()` or a custom poll mechanism.

### 3.2 NIXL Metadata Types

```rust
#[pyclass(extends=TensorTransportMetadata)]
pub struct NixlTransportMetadata {
    pub nixl_serialized_descs: Option<Py<PyBytes>>,
    pub nixl_agent_meta: Option<Py<PyBytes>>,
    pub nixl_agent_name: Option<String>,
    pub nixl_agent_meta_version: i64,
}

#[pyclass(extends=CommunicatorMetadata)]
pub struct NixlCommunicatorMetadata {}
```

---

## Phase 4: Collective & IPC Transports (Week 7-8)

### 4.1 Collective Transport (`collective_tensor_transport.py`)

Lower priority — these are thin wrappers around `torch.distributed` and `ray.util.collective`. Keep in Python initially, port to Rust only if performance matters.

### 4.2 CUDA IPC Transport (`cuda_ipc_transport.py`)

Lower priority — uses PyTorch's `reduce_tensor` and CUDA IPC handles. The logic is simple and heavily Python-interop. Keep in Python initially.

---

## Phase 5: Testing & Integration (Week 9-10)

### 5.1 Rust Unit Tests
- Test `RDTStore` operations (add, get, pop, wait, deque ordering)
- Test `RDTManager` state machine (metadata lifecycle, queued transfers, queued frees)
- Test transport registry (register, singleton, validation)
- Test concurrent access (multi-threaded add/get/pop)

### 5.2 Python Integration Tests
- Run existing `test_rdt_nixl.py` (21 tests) against Rust-backed implementation
- Run existing `test_rdt_nccl.py` (1 test) against Rust-backed implementation
- Run `test_rdt_unit.py` (105 tests) with mocked transports

### 5.3 Performance Benchmarks
- Compare `RDTStore.add_object/get_object/pop_object` latency (Python vs Rust)
- Compare `RDTManager.trigger_out_of_band_tensor_transfer` overhead
- Measure GIL contention reduction in multi-actor scenarios

---

## Crate Structure

```
rust/
├── ray-rdt/
│   ├── Cargo.toml
│   ├── src/
│   │   ├── lib.rs              # PyO3 module registration
│   │   ├── metadata.rs         # TensorTransportMetadata, CommunicatorMetadata
│   │   ├── store.rs            # RDTStore
│   │   ├── store_helpers.rs    # validate_tensor_buffers, helper functions
│   │   ├── manager.rs          # RDTManager
│   │   ├── registry.rs         # Transport registry (util.py)
│   │   ├── nixl_transport.rs   # NixlTensorTransport
│   │   └── error.rs            # Error types
│   └── tests/
│       ├── test_store.rs
│       ├── test_manager.rs
│       └── test_registry.rs
```

**Cargo.toml dependencies**:
```toml
[dependencies]
pyo3 = { version = "0.23", features = ["extension-module"] }
parking_lot = "0.12"  # Faster Mutex/Condvar
dashmap = "6"          # Concurrent HashMap (optional)
```

---

## Risk Assessment

| Risk | Impact | Mitigation |
|------|--------|------------|
| GIL contention in Rust↔Python boundary | Performance regression | Batch Python calls, release GIL during blocking ops |
| PyO3 subclassing limitations | Metadata types can't be extended in Python | Use `#[pyclass(subclass)]`, or keep metadata in Python |
| NIXL/torch API changes | Rust bindings break | Keep NIXL/torch calls in Python, only port orchestration |
| Thread safety bugs | Deadlocks or data races | Rust's ownership model prevents data races; use `parking_lot` for deadlock detection in debug |
| Backward compatibility | Existing Python code breaks | Maintain identical Python API via `#[pymethods]` |

---

## Migration Strategy

1. **Phase 1**: Implement Rust `RDTStore` and registry, export via PyO3. Modify `rdt_store.py` and `util.py` to delegate to Rust.
2. **Phase 2**: Implement Rust `RDTManager`, modify `rdt_manager.py` to delegate.
3. **Phase 3**: Port NIXL transport hot paths (recv poll loop, descriptor cache).
4. **Phase 4**: Port remaining transports if beneficial.
5. **Each phase**: Run full test suite, benchmark, compare.

The Python files remain as thin wrappers until the Rust implementation is proven. This allows instant rollback by removing the Rust delegation.

---

## Success Criteria

1. All 22 existing tests pass (21 NIXL + 1 NCCL)
2. All 105 unit tests pass
3. `RDTStore` operations are ≥2x faster (measured by microbenchmark)
4. No increase in P99 latency for end-to-end tensor transfers
5. Zero GIL-related deadlocks under concurrent access
6. Identical Python API (no breaking changes)
