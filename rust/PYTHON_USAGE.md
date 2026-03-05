# Using and Running Ray Python Programs on the Rust Backend

## 1. Writing a Ray Python Program (the API)

The Rust backend provides the **standard Ray Python API**. Programs use `import ray` and `@ray.remote` exactly like upstream Ray:

```python
import ray

ray.init(num_task_workers=4)

# Remote functions
@ray.remote
def square(x):
    return x * x

print(ray.get(square.remote(5)))           # 25
print(ray.get([square.remote(i) for i in range(4)]))  # [0, 1, 4, 9]

# Remote classes (actors)
@ray.remote
class Counter:
    def __init__(self):
        self.n = 0
    def incr(self, delta=1):
        self.n += delta
        return self.n
    def get(self):
        return self.n

c = Counter.remote()
ray.get(c.incr.remote(10))    # 10
ray.get(c.incr.remote(5))     # 15
print(ray.get(c.get.remote())) # 15

# Object store
ref = ray.put({"key": "value"})
print(ray.get(ref))            # {'key': 'value'}

# Wait for results
refs = [square.remote(i) for i in range(10)]
ready, pending = ray.wait(refs, num_returns=3)
print(ray.get(ready))          # first 3 results

ray.shutdown()
```

### Supported API

| API | Description |
|-----|-------------|
| `ray.init(num_task_workers=N)` | Start in-process cluster with N task workers |
| `ray.shutdown()` | Shut down the cluster |
| `ray.is_initialized()` | Check if Ray is running |
| `@ray.remote` | Decorate functions or classes for remote execution |
| `ray.put(obj)` | Store object, returns `ObjectRef` |
| `ray.get(ref_or_list)` | Retrieve object(s), blocks until ready |
| `ray.wait(refs, num_returns=N)` | Wait for N refs to become ready |
| `ray.get_actor(name, namespace)` | Look up a named actor via GCS |
| `.remote(*args)` | Submit task or create actor |
| `.options(...)` | Override per-call options (see below) |

### Per-call options

```python
# Multi-return: returns a list of ObjectRefs instead of one
@ray.remote(num_returns=3)
def split(data):
    return [data[:3], data[3:6], data[6:]]

ref_a, ref_b, ref_c = split.remote([1,2,3,4,5,6,7,8,9])

# Retries on failure
result = ray.get(flaky_fn.options(max_retries=3).remote())

# Named actors
counter = Counter.options(name="my_counter", namespace="prod").remote()
# ... later, from another task or driver:
handle = ray.get_actor("my_counter", "prod")
ray.get(handle.get.remote())
```

## 2. Building and Running (Setup)

### Prerequisites

- Python 3.9+
- Rust toolchain (rustc, cargo)
- [maturin](https://www.maturin.rs/) (`pip install maturin`)
- protobuf compiler (`protoc`)

### Step 1 -- Create a virtual environment

```bash
cd /Users/istoica/src/ray/rust
python3.11 -m venv .venv
source .venv/bin/activate
pip install maturin
```

### Step 2 -- Build and install the `_raylet` extension module

```bash
cd ray-core-worker-pylib
maturin develop --features python
```

This compiles all 17 Rust crates (including protobuf codegen from 35 `.proto`
files) and installs the resulting `_raylet.cpython-*.so` into the venv's
`site-packages/`.

### Step 3 -- Run your program

```bash
cd /Users/istoica/src/ray/rust
source .venv/bin/activate
python my_program.py
```

The working directory must be `/Users/istoica/src/ray/rust/` so that
`import ray` finds the `ray/` package there.

### Example -- run the existing demos

```bash
source .venv/bin/activate
python complex_app.py            # 7-stage financial pipeline, 18 verifications
python run_all_doc_examples.py   # 59 doc examples, full test suite
```

### Rebuilding after Rust changes

```bash
cd ray-core-worker-pylib
maturin develop --features python   # recompile + reinstall
```

### Running Rust tests

```bash
cd /Users/istoica/src/ray/rust
cargo test --all                    # 1,111 unit + integration tests
```

## Architecture

```
  my_program.py
      |  import ray
      v
  ray/__init__.py          <-- Pure Python: @ray.remote, ray.put/get/wait
      |  from _raylet import start_cluster, PyCoreWorker, ...
      v
  _raylet.cpython-*.so     <-- PyO3 extension (47 MB): bridges Python <-> Rust
      |
      v
  17 Rust crates (Tokio)   <-- GCS server, Raylet, CoreWorker, object store,
                               gRPC services, scheduling, pub/sub, etc.
```

`ray.init()` calls `start_cluster()` in Rust, which spawns an in-process GCS
server + Raylet on Tokio, then creates a driver `PyCoreWorker`. All task
submission, object storage, and actor management flow through gRPC to these
Rust services.

### Key source files

| Path | Purpose |
|------|---------|
| `ray/__init__.py` | Pure Python API layer (~500 lines) |
| `ray-core-worker-pylib/src/lib.rs` | PyO3 module definition |
| `ray-core-worker-pylib/src/core_worker.rs` | `PyCoreWorker` -- task/object store bridge |
| `ray-core-worker-pylib/src/gcs_client.rs` | `PyGcsClient` -- GCS operations |
| `ray-core-worker-pylib/src/cluster.rs` | `start_cluster()` -- in-process cluster |
| `ray-core-worker-pylib/pyproject.toml` | Maturin build configuration |
| `Cargo.toml` | Workspace root -- all 17 crates |
| `ray-proto/build.rs` | Protobuf codegen (35 `.proto` files) |
