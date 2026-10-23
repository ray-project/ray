# Ray Client

`ray_client` connects external Python processes to a Ray cluster over the Ray Dashboard HTTP API (`:8265`), providing a single unified **Workspace + Live Actors & Stateless Tasks** REST engine (`/api/workspaces/...`):

1. **Stateful Live Actors (`client.actor`, `ActorProxy`)**: Create, reattach to, and invoke stateful Ray actors directly over HTTP (`actor.call`, `actor.<method>`, `actor.stream`) without spawning a driver subprocess per call.
2. **Stateless Workspace Tasks (`client.run`, `client.stream`)**: Execute plain Python functions or generator functions as `@ray.remote` tasks inside the workspace, with full native support for spawning inner tasks/actors, Ray Data, Train, Tune, and Serve.
3. **In-Cluster Object Reference DAGs (`RemoteRef` / `{"$ray_ref": "..."}`)**: Keep intermediate objects (including non-JSON types such as tuples, NumPy arrays, or DataFrames) in the Ray object store via `return_ref=True` or non-blocking `wait=False`, and pass `RemoteRef` tokens directly into subsequent `client.run()` or `actor.call()` invocations without round-tripping through the client.
4. **Isolated Multi-Tenant Workspaces (`workspace=...`, `ttl_seconds=...`)**: Named workspaces persist across notebook/agent reconnects in the `workspace:<name>` Ray namespace; unnamed clients automatically allocate an isolated ephemeral workspace (`ws_<hex>`) that cleans up on exit or after its idle TTL expires.

Only files, JSON data, and lightweight `$ray_ref` reference tokens cross the client-cluster boundary. You can run tasks and interact with live actors from any Python 3.9+ environment with only `requests` installed, without installing `ray` or `cloudpickle` or matching the cluster's Python version.

## Get started

### Prerequisites

- Client environment: Python 3.9+ and `requests>=2.28.0`.
- Cluster environment: A running Ray cluster with the Ray Dashboard HTTP server reachable on port `8265` by default.

Install the client package in editable mode from this directory:

```bash
pip install -e .
```

### Quickstart

Run a stateless function or create a stateful actor in a cluster workspace:

```python
from ray_client import RayClient

SCALE = 10


def main(items: list[int]) -> dict:
    import ray

    @ray.remote
    def scale(x: int) -> int:
        return x * SCALE

    values = ray.get([scale.remote(i) for i in items])
    return {"values": values, "sum": sum(values)}


class Counter:
    def __init__(self, start: int = 0):
        self.val = start

    def add(self, delta: int) -> int:
        self.val += delta
        return self.val


with RayClient("http://127.0.0.1:8265") as client:
    print(client.run(main, [1, 2, 3, 4]))
    # {"values": [10, 20, 30, 40], "sum": 100}

    counter = client.actor(Counter, 10, name="my_counter")
    print(counter.add(5))  # 15
```

## Function and actor class requirements

Functions passed to `client.run` / `client.stream` and classes passed to `client.actor` follow these rules:

- Pass either a named, undecorated `def` / `async def` function (or plain class / `@ray.remote`-decorated class) defined in a `.py` file or interactive notebook cell, **or** a `"module:qualname"` string (such as `"my_pkg.workers:ShardActor"`) referencing a module in `working_dir` or installed on the cluster.
- Use round-trip JSON types (`None`, `bool`, `int`, finite `float`, `str`, `list`, and `dict` with `str` keys) or `RemoteRef` (`{"$ray_ref": "..."}`) tokens for values that cross the client-cluster HTTP boundary. Intermediate results kept in-cluster with `return_ref=True` (`.ref(...)`) or `wait=False` (`.remote(...)`) can be arbitrary Python objects.
- Top-level `import` and `from ... import ...` statements referenced by the function or class are automatically emitted in the shipped module header. Place imports for cluster-only packages that are not installed on the client inside the function/method body (or invoke the callable by `"module:qualname"` string).
- All tasks (`client.run`, `client.stream`) and actors (`client.actor`) in a workspace execute inside the `workspace:<name>` Ray namespace, so `ray.get_actor("my_actor")` inside a task automatically resolves actors created via `client.actor(..., name="my_actor")`.

## API overview

### `RayClient`, `run`, and `stream`

```python
from ray_client import RayClient, run, stream

# One-off stateless task or generator stream (auto-creates & closes an ephemeral workspace)
result = run(fn, *args, address="http://127.0.0.1:8265", kwargs={"lr": 1e-3})
for event in stream(gen_fn, *args, address="http://127.0.0.1:8265"):
    print(event)

# Persistent named workspace across reconnects
client = RayClient(
    address="http://127.0.0.1:8265",
    workspace="notebook_alice",
    ttl_seconds=1800.0,
    default_runtime_env={"working_dir": "./my_pkg"},
)
```

Key `RayClient` methods:

| Method | Description |
| :--- | :--- |
| `client.run(fn_or_path, *args, kwargs=None, wait=True, return_ref=False, ...)` | Runs `fn_or_path` (callable or `"module:fn"`) as a stateless `@ray.remote` task in the workspace. Returns the decoded JSON value by default, or a `RemoteRef` when `wait=False` (non-blocking) or `return_ref=True` (blocking, pinned in object store). |
| `client.stream(fn_or_path, *args, kwargs=None, ...)` | Runs generator `fn_or_path` in the workspace and streams yielded JSON items over chunked NDJSON in real time. |
| `client.actor(cls_or_path, *args, kwargs=None, name=None, lifetime="workspace", get_if_exists=True, max_concurrency=10, ...)` | Creates or reattaches to a named Ray actor in the workspace and returns an `ActorProxy`. |
| `client.get_actor(name)` / `client.list_actors()` / `client.kill_actor(name)` | Reattaches to, lists, or terminates live actors in the workspace. |
| `client.workspace_info()` / `client.list_workspaces()` / `client.heartbeat()` | Inspects or refreshes workspace TTL leases. |
| `client.close()` / `client.close_workspace(kill_detached=False)` | `close()` destroys auto-generated ephemeral workspaces while leaving named workspaces active; `close_workspace()` explicitly destroys the workspace and its workspace-scoped actors/refs. |

### `ActorProxy` and `RemoteRef` (`$ray_ref`)

```python
actor = client.actor(ShardProcessor, "alpha", name="processor")

# Synchronous JSON method call
out = actor.process([1, 2, 3])

# Keep intermediate object in Ray's object store (supports non-JSON types)
ref = actor.build_shard.ref(1000)

# Non-blocking async submission (`wait=False`) & in-cluster DAG pipelining
in_flight = actor.transform_shard.remote(ref)
final = client.run(summarize_shard, in_flight)

# Real-time NDJSON streaming from a generator method with concurrent control calls
for event in actor.stream_batches.stream(batches):
    if event.get("step") == 0:
        actor.scale(3)  # Runs concurrently while stream_batches is paused at yield!
```

`RemoteRef` methods:
- `ref.get(timeout=None)`: Resolves and returns the JSON value from the cluster object store.
- `ref.ready(timeout=0.0)`: Returns `True` if the in-flight task or actor call has finished.
- `ref.cancel(force=False)`: Cancels an in-flight task or actor method via `ray.cancel(...)`.
- `ref.delete()`: Unpins the `ObjectRef` from the workspace table.

## Run tests

Run the offline unit and mock workspace server tests without `ray` installed:

```bash
PYTHONPATH=src pytest tests/test_unit.py -v
```

Run the live cluster integration suite against a running Ray cluster:

```bash
RAY_LIVE_CLUSTER_ADDRESS=http://127.0.0.1:8265 PYTHONPATH=src pytest tests/test_integration_live_cluster.py -v
```
