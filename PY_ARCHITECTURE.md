# Ray Core Python Architecture

This document provides detailed architecture information about Ray's Python components. For a quick reference, see [CLAUDE.md](CLAUDE.md).

## Table of Contents
- [Core Components](#core-components)
- [Key Design Patterns](#key-design-patterns)
- [Important Files Reference](#important-files-reference)

---

## Core Components

### Main API (`python/ray/__init__.py`)
The entry point for all Ray operations. Exports:
- `ray.init()` - Initialize Ray runtime
- `ray.remote` - Decorator for remote functions/actors
- `ray.get()` - Retrieve object values
- `ray.put()` - Store objects in object store
- `ray.wait()` - Wait for objects to be ready
- ID types: `ObjectRef`, `ActorID`, `TaskID`, `JobID`, `NodeID`

### Remote Functions (`python/ray/remote_function.py`)

```python
@ray.remote
def my_func(x):
    return x * 2
```

**`RemoteFunction` class** wraps decorated functions:
- `_function` - Original Python function
- `_function_descriptor` - Unique identifier (computed from pickled function)
- `_num_cpus`, `_num_gpus`, `_resources` - Resource requirements
- `_max_retries`, `_retry_exceptions` - Retry configuration
- `_runtime_env` - Runtime environment settings
- `_scheduling_strategy` - Scheduling hints

**Key methods:**
- `remote(*args, **kwargs)` - Submit task, returns `ObjectRef`
- `options(**kwargs)` - Create new RemoteFunction with modified options
- `bind(*args, **kwargs)` - For DAG API (Ray Compiled Graphs)

### Actor System (`python/ray/actor.py`)

```python
@ray.remote
class MyActor:
    def method(self, x):
        return x
```

**`ActorClass`** - Wrapper for `@ray.remote` decorated classes:
- `_modified_class` - Original class with tracing injected
- `_actor_method_cpus` - Per-method CPU requirements
- `_default_options` - Default actor options

**`ActorHandle`** - Reference to a remote actor:
- `_actor_id` - Unique actor identifier
- `_ray_actor_language` - Python, Java, or C++
- `_ray_actor_method_cpus` - Method resource requirements

**Actor States:** `DEPENDENCIES_UNREADY` → `PENDING_CREATION` → `ALIVE` → `DEAD`

### Object References

**`ObjectRef`** (defined in `_raylet.pyx`, exposed via `__init__.py`):
- Immutable reference to a remote object
- Backed by C++ `ObjectID`
- Can be passed to remote functions/actors
- Retrieved via `ray.get(ref)`

**`ObjectRefGenerator`** - For streaming returns:
- Yields `ObjectRef`s as they become available
- Used with `num_returns="streaming"`

### Worker Process (`python/ray/_private/worker.py`)

The main worker process implementation:
- `Worker` class manages worker lifecycle
- `global_worker` - Singleton worker instance
- Handles:
  - Task execution loop
  - Object serialization/deserialization
  - Communication with Raylet via CoreWorker

**Key functions:**
- `init()` - Initialize Ray (starts/connects to cluster)
- `shutdown()` - Clean shutdown
- `get()`, `put()`, `wait()` - Object store operations

### Cython Bridge (`python/ray/_raylet.pyx`)

Bridges Python to C++ CoreWorker:
- Compiles to `_raylet.so` (or `.pyd` on Windows)
- Wraps C++ types: `ObjectID`, `TaskID`, `ActorID`, etc.
- Provides `CoreWorker` Python bindings
- Handles serialization between Python objects and Ray's internal format

### Runtime Context (`python/ray/runtime_context.py`)

Access runtime information:
```python
ctx = ray.get_runtime_context()
ctx.get_job_id()
ctx.get_node_id()
ctx.get_task_id()
ctx.get_actor_id()
ctx.was_current_actor_reconstructed
```

### Exceptions (`python/ray/exceptions.py`)

Ray-specific exception hierarchy:
- `RayError` - Base class
- `RayTaskError` - Task execution failed
- `RayActorError` - Actor died or unreachable
- `ObjectLostError` - Object no longer available
- `ObjectFetchTimedOutError` - Timeout fetching object
- `GetTimeoutError` - `ray.get()` timeout
- `TaskCancelledError` - Task was cancelled

---

## Key Design Patterns

### 1. Task Submission Flow

```
User Code                          Python Runtime                    C++ CoreWorker
    │                                    │                                 │
    │ func.remote(args)                  │                                 │
    │───────────────────────────────────►│                                 │
    │                                    │                                 │
    │                                    │ Serialize args                  │
    │                                    │ Create TaskSpec                 │
    │                                    │                                 │
    │                                    │ submit_task()                   │
    │                                    │────────────────────────────────►│
    │                                    │                                 │
    │                                    │ ObjectRef                       │
    │◄───────────────────────────────────│◄────────────────────────────────│
    │                                    │                                 │
    │ ray.get(ref)                       │                                 │
    │───────────────────────────────────►│                                 │
    │                                    │ get_objects()                   │
    │                                    │────────────────────────────────►│
    │                                    │                                 │
    │ result                             │ Deserialize                     │
    │◄───────────────────────────────────│◄────────────────────────────────│
```

### 2. Actor Method Invocation

```
User Code                   ActorHandle                    Actor Worker
    │                           │                               │
    │ actor.method.remote(x)    │                               │
    │──────────────────────────►│                               │
    │                           │                               │
    │                           │ Direct RPC (bypasses Raylet)  │
    │                           │──────────────────────────────►│
    │                           │                               │
    │ ObjectRef                 │                               │ Execute method
    │◄──────────────────────────│                               │
    │                           │                               │
    │                           │ Result                        │
    │                           │◄──────────────────────────────│
```

### 3. Actor Creation

```python
@ray.remote
class Counter:
    def __init__(self, start=0):
        self.value = start

    def increment(self):
        self.value += 1
        return self.value

# Creates actor
counter = Counter.remote(start=10)

# Calls method
result = ray.get(counter.increment.remote())  # Returns 11
```

**Flow:**
1. `Counter.remote()` calls `ActorClass._remote()`
2. CoreWorker sends `RegisterActor` to GCS
3. GCS schedules actor on a node
4. Raylet starts worker process for actor
5. Actor runs `__init__()` with provided args
6. `ActorHandle` returned to caller

### 4. Serialization

Ray uses CloudPickle for Python object serialization:
- Functions are pickled (including closure)
- Large numpy arrays use zero-copy via Arrow
- Custom serializers can be registered

```python
# Register custom serializer
ray.util.register_serializer(
    MyClass,
    serializer=my_serialize,
    deserializer=my_deserialize
)
```

---

## Important Files Reference

### Core API
| File | Purpose |
|------|---------|
| `__init__.py` | Main API exports, `ray.init()`, `ray.get()`, `ray.put()` |
| `remote_function.py` | `@ray.remote` for functions, `RemoteFunction` class |
| `actor.py` | `@ray.remote` for classes, `ActorClass`, `ActorHandle` |
| `runtime_context.py` | `ray.get_runtime_context()` |
| `exceptions.py` | `RayError`, `RayTaskError`, `RayActorError`, etc. |

### Private Implementation
| File | Purpose |
|------|---------|
| `_private/worker.py` | Worker process, `global_worker`, task execution |
| `_private/ray_constants.py` | Constants (timeouts, limits, defaults) |
| `_private/client_mode_hook.py` | Ray Client hooks for remote clusters |
| `_private/serialization.py` | Object serialization utilities |
| `_private/utils.py` | General utilities |

### Cython/C++ Bridge
| File | Purpose |
|------|---------|
| `_raylet.pyx` | Cython bindings to C++ CoreWorker |
| `includes/*.pxd` | Cython declarations for C++ headers |

### Configuration
| File | Purpose |
|------|---------|
| `job_config.py` | `JobConfig` for job-level settings |
| `runtime_env/` | Runtime environment implementation |

### Client Mode
| File | Purpose |
|------|---------|
| `client_builder.py` | `ray.client()` for connecting to remote clusters |
| `_private/client_mode_hook.py` | Hooks to redirect calls to Ray Client |

---

## Environment Variables

| Variable | Purpose |
|----------|---------|
| `RAY_ADDRESS` | Default address for `ray.init()` |
| `RAY_NAMESPACE` | Default namespace for jobs |
| `RAY_RUNTIME_ENV` | JSON runtime env config |
| `RAY_JOB_CONFIG_JSON_ENV_VAR` | Job config override |

---

## Common Patterns

### Resource Specification
```python
@ray.remote(num_cpus=2, num_gpus=1, memory=1024*1024*1024)
def gpu_task():
    pass

@ray.remote(num_cpus=4)
class Worker:
    pass
```

### Options Override
```python
# Override options for specific call
func.options(num_cpus=4).remote(args)

# Override actor options
Actor.options(num_gpus=2).remote()
```

### Async Actors
```python
@ray.remote
class AsyncActor:
    async def async_method(self):
        await asyncio.sleep(1)
        return "done"
```

### Streaming Returns
```python
@ray.remote(num_returns="streaming")
def generate():
    for i in range(10):
        yield i

gen = generate.remote()
for ref in gen:
    print(ray.get(ref))
```
