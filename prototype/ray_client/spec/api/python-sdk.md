# `ray_client` Python SDK

`ray_client` connects external Python programs on laptops, notebooks, and coding agents to a
remote Ray cluster over the Ray Dashboard HTTP API (`:8265`). Only working directory files and
JSON data cross the client-cluster boundary.

Everything in this doc is in the `ray_client` module unless stated otherwise.

## Design principles

- **Zero `ray` or `cloudpickle` dependency on the client:** The client requires only Python 3.9+
  and `requests>=2.28.0`. Python version or architecture differences between the client and the
  cluster cannot break execution.
- **No global client state:** Each `RayCluster` instance holds its own HTTP session and package
  cache, so a single Python process can talk to multiple Ray clusters concurrently.
- **Strict JSON boundary:** Arguments, return values, yielded stream items, and captured
  module-level constants are validated for lossless round-trip JSON serialization before any
  network request is made.
- **Native in-cluster execution:** Functions (`cluster.run`) and stateful classes
  (`cluster.actor`) run inside root actors on the cluster, where `ray` is already initialized and
  all tasks, actors, and libraries use internal cluster networking and shared memory.

## Overview

```python
from ray_client import RayCluster

SCALE = 10


def compute_sum(n: int) -> dict:
    import ray

    @ray.remote
    class Worker:
        def scale(self, x: int) -> int:
            return x * SCALE

    workers = [Worker.remote() for _ in range(n)]
    values = ray.get([w.scale.remote(i) for i, w in enumerate(workers)])
    return {"sum": sum(values), "values": values}


class Trainer:
    def __init__(self, lr: float = 1e-3):
        self.lr = lr
        self.step = 0

    def train_step(self) -> dict:
        self.step += 1
        return {"step": self.step, "loss": 1.0 / self.step, "lr": self.lr}

    def stream_steps(self, count: int):
        for _ in range(count):
            yield self.train_step()


with RayCluster("http://127.0.0.1:8265") as cluster:
    # 1. Run a self-contained function in an ephemeral root actor.
    out = cluster.run(compute_sum, 4)

    # 2. Deploy a stateful actor and stream generator items over SSE.
    with cluster.actor(
        Trainer, kwargs={"lr": 1e-4}, name="trainer", get_if_exists="recreate"
    ) as trainer:
        metrics = trainer.train_step()
        for item in trainer.stream_steps.stream(3):
            print(item)

    # 3. Submit a shell entrypoint job.
    job = cluster.submit_job(
        "python -c 'import ray; ray.init(); print(ray.cluster_resources())'",
        wait=True,
    )
    print(job.logs())
```

## Public exports

`ray_client` exports:

- `RayCluster`, `run`
- `ActorHandle`
- `JobResult`
- `RayClientError`, `RayClientSerializationError`, `RayJobExecutionError`, `RayJobTimeoutError`

## Connecting to a cluster

### `RayCluster`

```python
class RayCluster:
    def __init__(
        self,
        address: str | None = None,
        headers: dict[str, str] | None = None,
        verify: bool | str = True,
        timeout: float = 30.0,
        default_runtime_env: dict[str, Any] | None = None,
    ) -> None: ...
```

- `address`: HTTP or HTTPS URL of the Ray Dashboard (for example `"http://127.0.0.1:8265"`). When
  `None`, resolves from `RAY_API_SERVER_ADDRESS`, then `RAY_ADDRESS` (if it starts with
  `http://` or `https://`), defaulting to `"http://127.0.0.1:8265"`. Passing a legacy `ray://...`
  URI raises `ValueError`.
- `headers`: Optional HTTP headers attached to every request. When `"Authorization"` is not
  supplied, the client checks `RAY_AUTH_TOKEN`, `RAY_AUTH_TOKEN_PATH`, and `~/.ray/auth_token`,
  attaching `Authorization: Bearer <token>` if present.
- `verify`: TLS certificate verification flag (`bool`) or path to a CA bundle (`str`).
- `timeout`: Default HTTP connect/read timeout in seconds (must be `> 0`).
- `default_runtime_env`: Default `runtime_env` merged into every `run()`, `actor()`, and
  `submit_job()` call on this client. Per-call `runtime_env` keys override `default_runtime_env`,
  and `env_vars` dictionaries are merged key-by-key.

`RayCluster` supports context manager usage (`with RayCluster(...) as cluster:`) and
`cluster.close()`, which closes the underlying `requests.Session`.

## Running driver functions

### `RayCluster.run` and `ray_client.run`

```python
def run(
    self,
    fn: Callable[..., Any],
    *args: Any,
    kwargs: dict[str, Any] | None = None,
    runtime_env: dict[str, Any] | None = None,
    node_id: str | None = None,
    num_cpus: float | None = None,
    num_gpus: float | None = None,
    resources: dict[str, float] | None = None,
    label_selector: dict[str, str] | None = None,
    timeout: float | None = None,
) -> Any:
    """Runs fn in an ephemeral non-detached root actor on the cluster."""
```

The module-level `ray_client.run(fn, *args, address=None, headers=None, verify=True, ...)`
convenience function accepts the same options, creates a temporary `RayCluster`, and closes it
when the call (or returned generator stream) finishes.

Parameters:

- `fn`: A Python function or generator function (`def`). Lambdas, callable instances, `async def`
  functions, and decorated top-level functions are rejected locally with
  `RayClientSerializationError`.
- `*args`, `kwargs`: Positional arguments and an explicit dictionary of keyword arguments for
  `fn`, validated for round-trip JSON compatibility. Passing `kwargs` as a dictionary prevents
  collisions between user keyword arguments and `run()` scheduling options.
- `runtime_env`: Optional runtime environment dictionary. If `runtime_env["working_dir"]` points
  to a local directory, it is zipped and uploaded to `/api/packages/gcs/` before actor creation.
- `node_id`: Pin execution to a specific Ray node ID (hex string). Cannot be combined with
  `label_selector`.
- `resources`, `num_cpus`, `num_gpus`: Resource requirements held by the root actor. Defaults to
  `resources={}` (`num_cpus=0`) on the head node so lightweight driver orchestration does not
  consume worker CPUs. `resources` accepts unified Ray 3.0 keys (`"CPU"`, `"GPU"`, `"memory"`,
  plus custom resources such as `"TPU"`). Setting both `num_cpus` and `resources["CPU"]` (or
  `num_gpus` and `resources["GPU"]`) to different values raises `ValueError`.
- `label_selector`: Node label constraints (`dict[str, str]`) for actor placement.
- `timeout`: Overall wall-clock budget in seconds across root actor creation and execution of
  `fn`.

Behavior:

- **Regular functions:** Returns the decoded JSON return value of `fn(*args, **(kwargs or {}))`.
  The ephemeral root actor (`detached=False`) is destroyed before `run()` returns, or immediately
  if the client disconnects or times out.
- **Generator functions (`yield`):** Returns an `Iterator[Any]` streaming each yielded JSON value
  over Server-Sent Events (`text/event-stream`). Consuming the iterator to completion or calling
  `.close()` destroys the ephemeral root actor in its `finally` block.

## Stateful actors

### `RayCluster.actor`

```python
def actor(
    self,
    cls_or_path: type | str,
    *args: Any,
    kwargs: dict[str, Any] | None = None,
    name: str | None = None,
    runtime_env: dict[str, Any] | None = None,
    node_id: str | None = None,
    num_cpus: float | None = None,
    num_gpus: float | None = None,
    resources: dict[str, float] | None = None,
    label_selector: dict[str, str] | None = None,
    max_concurrency: int | None = None,
    ttl_seconds: float | None = None,
    get_if_exists: bool | str = True,
    detached: bool = True,
    timeout: float | None = None,
) -> ActorHandle:
    """Deploys or reattaches to a stateful root actor on the cluster."""
```

Parameters:

- `cls_or_path`: A Python class (plain or `@ray.remote`-decorated) or an import path string
  (`"package.module:ClassName"` or `"package.module.ClassName"`). When a `@ray.remote`-decorated
  class is passed, its decorator options (`num_cpus`, `num_gpus`, `resources`, `max_concurrency`,
  `label_selector`) are inherited unless overridden by explicit `cluster.actor(...)` arguments.
- `*args`, `kwargs`: Positional and keyword arguments passed to `__init__`.
- `name`: Actor name on `/api/actors`. Auto-generated as `actor_<10-hex>` when omitted.
- `runtime_env`, `node_id`, `num_cpus`, `num_gpus`, `resources`, `label_selector`: Placement and
  resource options. Unlike `run()`, `actor()` defaults to `resources={"CPU": 1}`.
- `max_concurrency`: Maximum concurrent method executions on the actor (default `1`, matching
  native Ray actor ordering). Set `max_concurrency=2` (or higher) when issuing concurrent
  steering calls while a generator stream is active.
- `ttl_seconds`: Idle time-to-live in seconds. Newly created actors default to `1800.0` seconds
  (30 minutes) on the server; passing `0` or a negative number disables idle reaping. When
  reattaching to an existing actor with `ttl_seconds=None`, its existing TTL is preserved.
- `get_if_exists`: Reattachment policy when an actor with `name` is already alive:
  - `True` (default): Reattaches to the existing actor (`handle.created == False`). If the
    caller's `spec_hash` differs from the running actor's `spec_hash`, emits a `UserWarning`. If
    the existing actor's health check times out under load, raises `RayJobTimeoutError` without
    killing or replacing the actor.
  - `"recreate"`: Reattaches when `spec_hash` matches (`handle.created == False`), or kills and
    recreates the actor when `spec_hash` differs (`handle.created == True`).
  - `False`: Raises `RayClientError` (`400`) if a live actor with `name` already exists.
- `detached`: Defaults to `True` so the actor survives across multiple client scripts. Set
  `detached=False` to bind the actor's ownership to the Dashboard agent process.
- `timeout`: Timeout in seconds for actor creation and `__init__` completion.

### `RayCluster.get_actor`, `list_actors`, and `kill_actor`

```python
def get_actor(self, name: str) -> ActorHandle: ...
def list_actors(self) -> list[dict[str, Any]]: ...
def kill_actor(self, name: str) -> bool: ...
```

- `get_actor(name)` fetches `GET /api/actors/{name}`, raises `RayClientError` if the actor does
  not exist or is `"DEAD"`, and returns an `ActorHandle` with `created=False`.
- `list_actors()` returns metadata dictionaries for all active client-managed root actors.
- `kill_actor(name)` sends `DELETE /api/actors/{name}` and returns `True` if the actor was killed.

### `ActorHandle`

```python
class ActorHandle:
    name: str
    created: bool
    package_uploaded: bool

    def call(
        self,
        method: str,
        *args: Any,
        kwargs: dict[str, Any] | None = None,
        timeout: float | None = None,
    ) -> Any: ...

    def stream(
        self,
        method: str,
        *args: Any,
        kwargs: dict[str, Any] | None = None,
        timeout: float | None = None,
    ) -> Iterator[Any]: ...

    def info(self) -> dict[str, Any]: ...
    def logs(self) -> str: ...
    def kill(self) -> bool: ...
```

- `actor.<method>(*args, **kwargs)`: Invokes `<method>` on the remote actor. Regular methods
  return the decoded JSON result; generator methods automatically return an SSE `Iterator[Any]`.
- `actor.<method>.stream(*args, **kwargs)` and `actor.stream(method, *args, kwargs=None, timeout=None)`:
  Explicitly invoke `<method>` in SSE streaming mode (`stream=True`), returning an
  `Iterator[Any]`. Raises `RayJobExecutionError` (`TypeError`) if `<method>` is not a generator.
- `actor.call(method, *args, kwargs=None, timeout=None)`: Explicitly invokes `<method>` with a
  per-call `timeout` or when `<method>` shares a name with an `ActorHandle` attribute (`info`,
  `logs`, `kill`, `call`, `stream`, `name`, `created`, `package_uploaded`).
- `actor.info()`: Returns the latest actor metadata dictionary from `GET /api/actors/{name}`.
- `actor.logs()`: Returns captured `stdout` and `stderr` from the root actor process.
- `actor.kill()`: Permanently terminates the actor (`DELETE /api/actors/{name}`).
- Context manager (`with cluster.actor(...) as actor:`): Calls `actor.kill()` when exiting the
  `with` block.

## Submitting shell jobs

### `RayCluster.submit_job` and `get_job`

```python
def submit_job(
    self,
    entrypoint: str,
    *,
    runtime_env: dict[str, Any] | None = None,
    metadata: dict[str, str] | None = None,
    submission_id: str | None = None,
    entrypoint_num_cpus: float | None = None,
    entrypoint_num_gpus: float | None = None,
    entrypoint_memory: int | None = None,
    entrypoint_resources: dict[str, float] | None = None,
    entrypoint_label_selector: dict[str, str] | None = None,
    wait: bool = False,
    timeout: float | None = None,
    poll_interval: float = 0.2,
    on_log_line: Callable[[str], None] | None = None,
) -> JobResult: ...

def get_job(self, job_id: str) -> JobResult: ...
```

Submits a shell command to `/api/jobs/` (packaging and uploading any local `working_dir` first)
and returns a `JobResult` handle. When `wait=True`, blocks until the job reaches a terminal state
(`SUCCEEDED`, `FAILED`, or `STOPPED`) and raises `RayJobExecutionError` if it failed or stopped.

### `JobResult`

```python
class JobResult:
    job_id: str
    submission_id: str
    package_uploaded: bool

    def status(self) -> str: ...
    def info(self) -> dict[str, Any]: ...
    def logs(self) -> str: ...
    def wait(
        self,
        timeout: float | None = None,
        poll_interval: float = 0.2,
        on_log_line: Callable[[str], None] | None = None,
        raise_on_error: bool = True,
    ) -> dict[str, Any]: ...
    def stop(self) -> bool: ...
    def delete(self) -> bool: ...
```

## Cluster node management

`RayCluster` wraps the `/api/cluster/*` REST endpoints:

```python
def list_node_groups(self) -> list[dict[str, Any]]: ...
def get_node_group(self, group_name: str) -> dict[str, Any]: ...
def add_nodes(
    self,
    group_name: str,
    count: int = 1,
    *,
    wait_ready: bool = False,
    timeout: float | None = None,
) -> list[dict[str, Any]]: ...
def list_nodes(
    self,
    *,
    group_name: str | None = None,
    states: str | Sequence[str] | None = None,
    include_dead: bool = False,
) -> list[dict[str, Any]]: ...
def get_node(self, node_id: str) -> dict[str, Any]: ...
def destroy_node(
    self,
    node_id: str,
    reason: str,
    *,
    grace_period_s: float = 0.0,
) -> dict[str, Any]: ...
```

- `add_nodes`: Requests `count` (`>= 1`) detached nodes in `group_name`. When `wait_ready=True`,
  blocks until every newly added node leaves `PENDING` or `timeout` expires.
- `list_nodes`: Filters by `group_name`, `states` (a state string or sequence of state strings
  from `"PENDING"`, `"ALIVE"`, `"TERMINATING_SOON"`, `"DEAD"`), and `include_dead`.
- `destroy_node`: Requires a non-empty `reason` string and non-negative `grace_period_s`.

## Validation, AST extraction, and `working_dir` packaging

Before sending any network request, `ray_client` validates and packages the workload locally:

1. **Source extraction and constant capture:**
   - Top-level functions and classes ship their defining module's source (stripped of any
     `if __name__ == "__main__":` block) when the module is self-contained (`<= 512 KiB` and no
     relative imports); otherwise, the dedented function or class AST is extracted alongside
     referenced module-level constants.
   - Indented or notebook-defined functions and classes are dedented (`textwrap.dedent`),
     verified not to reference enclosing `nonlocal` variables in their bodies, default arguments,
     or type annotations, and shipped with their referenced module-level JSON constants in
     `captured_globals`.
2. **Round-trip JSON validation:**
   - Permits `None`, `bool`, `int`, `float` (finite only; `NaN` and `Infinity` are rejected),
     `str`, `list`, and `dict` with `str` keys.
   - Rejects `tuple`, `set`, `bytes`, `Enum`, non-string dictionary keys, `ObjectRef`, and actor
     handles with `RayClientSerializationError` pointing to the exact argument path (for example
     `args[0].items[2]`).
3. **Deterministic `working_dir` zip packaging:**
   - Local `working_dir` directories are zipped deterministically (sorted entries, fixed
     `1980-01-01` timestamp, normalized permissions) into `_ray_pkg_<sha1>.zip`, respecting
     `.gitignore`, `.rayignore`, and default excludes (`.git`, `__pycache__`, `.venv`, `venv`,
     `node_modules`, `.pytest_cache`, `.mypy_cache`, `.ruff_cache`, `*.pyc`).
   - Archives exceeding `100 MiB` fail locally before upload and list the 5 largest files.
   - Remote URIs (`s3://`, `gs://`, `gcs://`, `http://`, `https://`) pass through untouched.

## Configuration

| Environment variable | Default | Meaning |
|---|---|---|
| `RAY_API_SERVER_ADDRESS` | `None` | Primary Ray Dashboard HTTP URL when `address` is omitted. |
| `RAY_ADDRESS` | `"http://127.0.0.1:8265"` | Fallback Dashboard HTTP URL when `RAY_API_SERVER_ADDRESS` is unset and the value starts with `http://` or `https://`. |
| `RAY_AUTH_TOKEN` | `None` | Bearer token attached as `Authorization: Bearer <token>`. |
| `RAY_AUTH_TOKEN_PATH` | `~/.ray/auth_token` | File path read for the Bearer token when `RAY_AUTH_TOKEN` is unset. |
| `RAY_RUNTIME_ENV_IGNORE_GITIGNORE` | `"0"` | Set to `"1"`, `"true"`, or `"yes"` to ignore `.gitignore` files when packaging a local `working_dir`. |
| `RAY_OVERRIDE_RUNTIME_ENV_DEFAULT_EXCLUDES` | `None` | Comma-separated gitignore patterns replacing the default `working_dir` excludes (`""` disables all default excludes). |

## Errors

| Error | Raised when |
|---|---|
| `RayClientError` | Base exception for all `ray_client` errors; also raised directly on HTTP status errors (`400`, `401`, `403`, `404`, `409`, `501`), connection failures, or premature stream disconnects. |
| `RayClientSerializationError` | A function, class, argument, captured constant, or return value fails AST validation or round-trip JSON serialization. Subclasses `RayClientError` and `TypeError`. |
| `RayJobExecutionError` | Remote driver function, actor constructor, actor method, or submitted job failed. Carries `.job_id`, `.status`, `.logs`, `.error_info`, `.exc_type`, and `.remote_traceback`. Subclasses `RayClientError`. |
| `RayJobTimeoutError` | Actor creation, `run()`, method invocation, `add_nodes(wait_ready=True)`, or `job.wait()` exceeded its `timeout`. Subclasses `RayClientError` and `TimeoutError`. |
