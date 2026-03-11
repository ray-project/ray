# RDT Rust Port Report

## Overview

The Ray Direct Transport (RDT) subsystem has been partially ported from Python to Rust using PyO3. The core data structure (`RDTStore`) now runs in Rust for improved concurrency, while the transport registry and metadata types remain in Python for compatibility reasons.

## File Mapping

### Rust Implementation Files

Located in `rust/ray-core-worker-pylib/src/rdt/`:

| Rust File | Original Python File | What It Implements |
|-----------|---------------------|-------------------|
| `mod.rs` | — | Module declaration |
| `store.rs` | `python/ray/experimental/rdt/rdt_store.py` | `PyRDTStore` — thread-safe per-actor tensor storage with `Mutex` + 2 `Condvar`s |
| `metadata.rs` | `python/ray/experimental/rdt/tensor_transport_manager.py` | `PyTensorTransportMetadata` — tensor shape/dtype metadata with pickle support |
| `registry.rs` | `python/ray/experimental/rdt/util.py` | Transport registry singleton — registration, lookup, validation of transport backends |

The Rust module is registered in `rust/ray-core-worker-pylib/src/lib.rs` and compiled into `_raylet.so` via maturin.

### Python Wrapper Files

Located in `rust/ray/experimental/rdt/` (replaced symlinks to `python/ray/experimental/rdt/`):

| Python Wrapper | What It Does |
|---------------|-------------|
| `rdt_store.py` | Wraps Rust `PyRDTStore` via composition; keeps `add_object_primary` and all helper functions (`__ray_send__`, `__ray_recv__`, `__ray_free__`, etc.) in Python. Includes full Python fallback. |
| `tensor_transport_manager.py` | Keeps `TensorTransportMetadata` as a Python `@dataclass` (required because `CudaIpcTransportMetadata` subclasses it). Keeps `TensorTransportManager` ABC and `CommunicatorMetadata` in Python. |
| `util.py` | Keeps the transport registry in Python (tests inspect module-level globals like `transport_manager_info` directly). All functions identical to original. |
| `__init__.py` | Same as original — re-exports from submodules. |

### What's Currently Active

Only `PyRDTStore` (from `store.rs`) is actively used at runtime. The `RDTStore` wrapper in `rdt_store.py` delegates all core operations to the Rust implementation. The metadata and registry Rust implementations are compiled into `_raylet.so` and available for future use but are not wired in by default due to Python subclassing and test compatibility constraints.

## Advantages of the Rust RDT Implementation

### 1. GIL-Free Blocking Waits (Primary Benefit)

The Python `RDTStore` holds the GIL during `threading.Condition.wait_for()` calls. In the Rust implementation, `wait_and_get_object`, `wait_and_pop_object`, and `wait_tensor_freed` use `py.allow_threads()` to release the GIL during condvar waits. This eliminates a class of deadlocks where:

- Thread A holds the GIL and waits on a tensor to appear in the store
- Thread B needs the GIL to run Python code that would add that tensor to the store
- Both threads are stuck forever

With the Rust implementation, Thread A releases the GIL while waiting, allowing Thread B to proceed.

### 2. Lower Lock Contention

Python's `threading.RLock` and `threading.Condition` acquire the GIL on every operation. The Rust `Mutex` and `Condvar` operate entirely outside the GIL for internal state management, only acquiring the GIL when touching Python objects. This reduces contention when multiple threads (main thread, `_ray_system` thread, GC thread) access the store concurrently.

### 3. More Efficient Condition Variable Semantics

Python's `Condition.wait_for(predicate, timeout)` re-evaluates the predicate and re-acquires the lock on every spurious wakeup, each time going through Python's method dispatch. The Rust implementation uses a tight loop over `Condvar::wait_timeout` with direct `HashMap` lookups — no Python overhead per wakeup.

### 4. Deadline-Based Timeouts

Python's `Condition.wait_for` with a timeout can overshoot if spurious wakeups occur, because each `wait()` restarts with the full timeout. The Rust implementation computes a deadline once (`Instant::now() + Duration`) and uses `saturating_duration_since` to compute the remaining time, giving precise timeout behavior regardless of spurious wakeups.

### 5. No Python Object Overhead for Internal State

The store's internal bookkeeping (`HashMap<String, VecDeque<RDTObject>>`, `HashMap<isize, HashSet<String>>`) uses Rust's standard library collections with no per-operation Python object allocation. Only the tensor data itself remains as `Py<PyAny>` references.
