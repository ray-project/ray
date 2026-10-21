# Ray Client

`ray_client` runs a Python function as an in-cluster driver job over the Ray Jobs HTTP API. Instead of proxying individual `@ray.remote` and `ray.get` calls over gRPC, the client extracts the source code of a plain Python function, packages any local working directory files, and submits the function to run as a driver script inside the Ray cluster.

Only files and JSON data cross the client-cluster boundary. You can submit workloads from any Python 3.9+ environment with only `requests` installed, without installing `ray` or `cloudpickle` or matching the cluster's Python version.

## Get started

Install the client and submit a function to a running Ray cluster.

### Prerequisites

- Client environment: Python 3.9+ and `requests>=2.28.0`.
- Cluster environment: A running Ray cluster with the Ray Jobs HTTP server reachable on port `8265` by default.

Install the client package in editable mode from this directory:

```bash
pip install -e .
```

### Quickstart

Define a plain Python function and pass it to `run_function_as_driver`:

```python
import ray
from ray_client import run_function_as_driver

SCALE = 10


def main(items: list[int]) -> dict:
    @ray.remote
    def scale(x: int) -> int:
        return x * SCALE

    values = ray.get([scale.remote(i) for i in items])
    return {"values": values, "sum": sum(values)}


job = run_function_as_driver(main, [1, 2, 3, 4], address="http://127.0.0.1:8265")
print(job.status())  # SUCCEEDED
print(job.logs())
print(job.result())  # {"values": [10, 20, 30, 40], "sum": 100}
```

If `ray` isn't installed in your local client environment, import `ray` inside the driver function instead of at the top of the file.

## Driver function requirements

Functions passed to `run_function_as_driver` must follow these rules:

- Define a named, undecorated `def` or `async def` function in a `.py` file or interactive notebook cell so the client can inspect its source. Lambdas, callable class instances, and decorated functions raise `RayClientSerializationError`.
- Use round-trip JSON types for all arguments, referenced module-level constants, closure variables, and return values: `None`, `bool`, `int`, finite `float`, `str`, `list`, and `dict` with `str` keys. Tuples, enums, namedtuples, non-string dictionary keys, `NaN`, `Infinity`, `ObjectRef` instances, and actor handles aren't supported across the client-cluster boundary. For large datasets or model artifacts, read and write through object storage and pass URIs in JSON.
- Place imports for cluster-only or third-party libraries inside the function body, or put helper modules in a local `working_dir`. Referencing imported modules, helper functions, or classes defined outside the driver function raises `RayClientSerializationError`. Uncaptured names referenced only in type annotations receive a placeholder automatically.
- The client binds default argument values locally and serializes them with the call arguments. A default expression that names a local variable of an enclosing function or class raises `RayClientSerializationError`, even if a module global shares the same name.
- The runner initializes Ray in the job's namespace when the driver calls a Ray API, and skips initialization for pure-Python functions. Calling `ray.init` inside the driver function forwards your options on first initialization and acts as a no-op if Ray is already initialized.

## API overview

The package provides a top-level `run_function_as_driver` function, a `RayClient` class, a `JobResult` handle, and four exception classes.

### `run_function_as_driver` and `RayClient`

Use `run_function_as_driver` for one-off submissions, or instantiate `RayClient` to reuse connection settings across calls:

```python
from ray_client import RayClient, run_function_as_driver

# Submit a single function.
job = run_function_as_driver(
    fn,
    *args,
    address="http://127.0.0.1:8265",
    kwargs={"lr": 1e-3},
    runtime_env={"working_dir": "./my_pkg", "pip": ["torch"]},
    wait=True,
    timeout=300.0,
    stream_logs=True,
)

# Reuse connection settings across submissions.
client = RayClient(
    address="http://127.0.0.1:8265",
    headers=None,
    verify=True,
    namespace=None,
    default_runtime_env=None,
    python_executable="python",
    timeout=30.0,
)
job = client.run_function_as_driver(fn, *args)
existing_job = client.get_job(job.submission_id)
all_jobs = client.list_jobs()
```

`run_function_as_driver` accepts the following options:

| Option | Default | Description |
| :--- | :--- | :--- |
| `address` | `None` | Cluster HTTP address. Checks `address`, `RAY_API_SERVER_ADDRESS`, `RAY_ADDRESS`, and then `http://127.0.0.1:8265`. Rejects `ray://` and bare `host:6379` GCS addresses. |
| `kwargs` | `None` | Dictionary of keyword arguments passed to `fn`. |
| `runtime_env` | `None` | Ray runtime environment dictionary, such as `working_dir`, `excludes`, `pip`, and `env_vars`. Local `working_dir` paths are packaged as described in [Working directory packaging](#working-directory-packaging). Any `py_modules` entries must be remote URIs. |
| `namespace` | `None` | Ray namespace for the job. `None` uses Ray's default per-job anonymous namespace. Pass a shared name to look up named detached actors across jobs. |
| `wait` | `True` | Blocks until the job finishes and checks its result. If `False`, returns the `JobResult` handle immediately after submission. |
| `timeout` | `None` | Maximum seconds to wait when `wait=True`. If exceeded during a blocking call, stops the remote job and raises `RayJobTimeoutError`. |
| `stream_logs` | `False` | Prints driver log lines as they arrive when `wait=True`. |
| `submission_id` | `None` | Custom Ray job submission ID. Defaults to `rayclient_<fn>_<hex>`. |
| `metadata` | `None` | Job metadata dictionary merged with `{"ray_client_fn": fn.__qualname__}`. |
| `entrypoint_*` | `None` | Resource options for the driver process: `entrypoint_num_cpus`, `entrypoint_num_gpus`, `entrypoint_memory`, `entrypoint_resources`, and `entrypoint_label_selector`. |

The top-level `run_function_as_driver` function also accepts `headers`, `verify`, `default_runtime_env`, `python_executable`, and `http_timeout` and forwards them to `RayClient`. Unless `RAY_AUTH_MODE` is set to `disabled` or `headers` already includes an `Authorization` header, `RayClient` reads a token from `RAY_AUTH_TOKEN`, `RAY_AUTH_TOKEN_PATH`, or `~/.ray/auth_token` and sends it as a Bearer token.

### Working directory packaging

When `runtime_env["working_dir"]` is a local directory, the client zips it together with the driver function and uploads the archive once per unique SHA-1 hash. Per-call JSON payloads up to 32 KiB are passed through `env_vars` and scrubbed from the driver environment before user code runs. Payloads over 32 KiB are uploaded as a separate content-addressed archive in `runtime_env["py_modules"]` so the `working_dir` archive stays cached across calls.

The client packages `working_dir` files using these rules:

- Files are stored relative to `working_dir` with deterministic timestamps and preserved executable permissions. Symlinked directories are followed and empty directories are preserved. A symlink that points to one of its own ancestors raises `RayClientError`; add the symlink path to `excludes`.
- `.gitignore` and `.rayignore` files inside `working_dir` follow [git's pattern rules](https://git-scm.com/docs/gitignore). A file in a subdirectory applies to that subtree, the last matching pattern wins, deeper files override shallower ones, and `.rayignore` overrides `.gitignore` in the same directory. A nested `!pattern` can re-include a path excluded by a parent ignore file if the parent directory itself wasn't excluded. Set `RAY_RUNTIME_ENV_IGNORE_GITIGNORE=1` to skip `.gitignore` files. Ignore files outside `working_dir`, such as parent `.gitignore` files, `~/.config/git/ignore`, and `.git/info/exclude`, aren't read, and patterns are matched case-sensitively.
- `runtime_env["excludes"]` accepts a list of gitignore-style patterns relative to `working_dir` and applies in addition to ignore files. `.git`, `.venv`, `venv`, `__pycache__`, `*.pyc`, `.pytest_cache`, and `.mypy_cache` are excluded by default. Set `RAY_OVERRIDE_RUNTIME_ENV_DEFAULT_EXCLUDES` to override the default exclude list.
- The Ray Jobs HTTP server caps request bodies at 100 MiB. Packaging stops if the compressed archive reaches that size and raises `RayClientError` naming the largest entries. Keep datasets and model weights out of `working_dir` and pass object storage URIs instead.

### `JobResult`

`run_function_as_driver` and `client.get_job` return a `JobResult` handle with the following members:

| Member | Description |
| :--- | :--- |
| `submission_id` | Ray job ID string. |
| `package_uploaded` | `True` if this call uploaded the `working_dir` archive; `False` if the cluster already had it cached. |
| `result(timeout=None)` | Waits for completion and returns the decoded JSON return value on `SUCCEEDED`, or raises `RayJobExecutionError`. |
| `wait(timeout=None, on_log_line=None)` | Polls job status until terminal and streams completed log lines to `on_log_line` if provided. A blocking `run_function_as_driver` call with `wait=True` stops its remote job on timeout, Ctrl-C, or poll error. Calling `wait()` on a detached (`wait=False`) or `get_job()` handle leaves the remote job running. |
| `logs()` | Returns driver stdout and stderr with the internal result marker stripped. |
| `status()` | Returns the job status string: `PENDING`, `RUNNING`, `SUCCEEDED`, `FAILED`, or `STOPPED`. |
| `done()` | Returns `True` if the job is in a terminal status. |
| `info()` | Returns the job status dictionary from the Ray Jobs server. |
| `stop()` | Requests cancellation of the running job and returns `True` if stopped. |
| `delete()` | Deletes a terminal job and returns `True` if deleted. |

### Exceptions

The package defines four exception classes:

- `RayClientError(Exception)`: Base exception for client errors. Exposes `job` and `submission_id` when raised after a job is created.
- `RayClientSerializationError(RayClientError, TypeError)`: Raised on the client before network I/O when the function, its arguments, or its referenced globals can't be packaged or serialized to round-trip JSON.
- `RayJobTimeoutError(RayClientError, TimeoutError)`: Raised when `wait` exceeds `timeout`. Attaches the `JobResult` handle on `exc.job`.
- `RayJobExecutionError(RayClientError)`: Raised by `result()` when the driver raises an unhandled exception, returns a non-JSON value on the cluster, fails, or is stopped. Exposes `submission_id`, `status`, `message`, `exc_type`, `exc_message`, `remote_traceback`, and `logs`.

## Common workflows

You can use `RayClient` to fan out jobs across multiple clusters or pass state across sequential driver runs.

### Multi-cluster fan-out and non-blocking jobs

`RayClient` holds no process-global state. Connect to multiple clusters from one process and submit non-blocking jobs with `wait=False`:

```python
from ray_client import RayClient

clients = [
    RayClient("http://cluster-a:8265"),
    RayClient("http://cluster-b:8265"),
]

jobs = [
    client.run_function_as_driver(main, shard, wait=False)
    for client, shard in zip(clients, [[1, 2], [3, 4]])
]
results = [job.result(timeout=120) for job in jobs]
```

### Multi-step workflows and detached actors

To pass state across sequential driver calls, either pass the JSON return value of the first call as an argument to the second, or create a named detached actor in an explicit shared `namespace`:

```python
from ray_client import RayClient


def create_counter() -> str:
    import ray

    @ray.remote
    class Counter:
        def __init__(self):
            self.value = 0

        def increment(self, delta: int) -> int:
            self.value += delta
            return self.value

    Counter.options(name="counter", lifetime="detached").remote()
    return "counter"


def increment_counter(name: str, delta: int) -> int:
    import ray

    actor = ray.get_actor(name)
    return ray.get(actor.increment.remote(delta))


client = RayClient("http://127.0.0.1:8265", namespace="counter_app")
name = client.run_function_as_driver(create_counter).result()
print(client.run_function_as_driver(increment_counter, name, 5).result())
```

See `examples/` for runnable scripts covering sequential pipelines, detached actor wrappers, and multi-cluster sharding.

## Customize and extend

The package is split into six modules under `src/ray_client/` so you can fork and adapt it:

- Override `RayClient.request` in `client.py` to customize HTTP authentication, such as OAuth token refresh, mTLS certificates, or custom proxy headers.
- Edit `_driver_runner.py` to change how the cluster-side entrypoint initializes Ray, loads inputs, or writes large outputs to S3, GCS, or Redis.
- Call `build_driver_package` and `parse_logs_and_result` in `packaging.py` directly to submit packaged functions to custom orchestrators such as KubeRay RayJob resources without using `requests`.
- Replace `_ignore.py` if your fork wants simpler exclusion rules. It implements the gitignore pattern matcher behind `excludes` and `.gitignore` and `.rayignore` files, and `packaging.py` calls three functions from it.

## Run tests

Run the offline unit and mock job server tests without `ray` installed:

```bash
PYTHONPATH=src python3 -m unittest tests/test_unit.py -v
```

Run the live cluster integration suite against a running Ray cluster:

```bash
RAY_LIVE_CLUSTER_ADDRESS=http://127.0.0.1:8265 PYTHONPATH=src python3 -m unittest tests/test_integration_live_cluster.py -v
```
