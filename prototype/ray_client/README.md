# `ray_client` — Lightweight HTTP Ray Client (`run_function_as_driver`)

`ray_client` runs a Python function as an in-cluster driver job over the Ray Job Submission HTTP API (`/api/packages/` and `/api/jobs/`).

## Design Principles

`ray_client` separates client-side job submission from in-cluster Ray execution:

- Requires only `requests` and the Python standard library, so the client runs from any Python >= 3.9 environment without installing `ray` or `cloudpickle` or matching the cluster's Python version.
- Extracts the AST source of the target `def` function and executes it as an in-cluster driver script, so Ray libraries (`ray.data`, `ray.train`, and `@ray.remote` tasks and actors) run natively inside the cluster without importing the caller's surrounding module.
- Restricts the client-cluster boundary to files (`runtime_env["working_dir"]` zip archives) and JSON data (arguments, referenced module-level constants, and return values).
- Fits in five small source files under `src/ray_client/`, making it straightforward to extract into a standalone repository (`git subtree split --prefix=prototype/ray_client`) or fork for custom workflows.

## Rules for Driver Functions

Driver functions passed to `run_function_as_driver` must follow four rules:

- Define a plain, undecorated `def` or `async def` function in a `.py` file or notebook cell. The client extracts the function source and runs it on the cluster.
- Place `import` statements for packages other than `ray` inside the function body or in a module under `runtime_env["working_dir"]`. The client environment does not need those packages installed.
- Pass JSON-serializable arguments, referenced module-level constants, and return values. Return summaries or storage URIs rather than large arrays.
- Assume Ray is already initialized in a shared namespace (`"ray_client"` by default) when the function starts; calling `ray.init()` inside the function is a no-op. Named detached actors created in one call remain accessible to subsequent calls via `ray.get_actor(name)`.

## Quickstart

```python
import ray_client


def main_1(items: list) -> dict:
    import ray

    @ray.remote
    def square(x: int) -> int:
        return x * x

    values = ray.get([square.remote(i) for i in items])
    return {"values": values, "sum": sum(values)}


def main_2(summary: dict) -> int:
    return summary["sum"] * 2


job_result_1 = ray_client.run_function_as_driver(
    main_1, [1, 2, 3, 4], address="http://127.0.0.1:8265"
)
print(job_result_1.status())  # "SUCCEEDED"
print(job_result_1.logs())
print(job_result_1.result())  # {"values": [1, 4, 9, 16], "sum": 30}

job_result_2 = ray_client.run_function_as_driver(
    main_2, job_result_1.result(), address="http://127.0.0.1:8265"
)
print(job_result_2.result())  # 60
```

## Package Layout

The public API exports one function (`run_function_as_driver`), two classes (`RayClient` and `JobResult`), and four exceptions. Internally, the package separates transport-agnostic archive construction from the HTTP client so forks can swap the transport layer (for example, submitting the generated package as a KubeRay `RayJob` resource):

- `src/ray_client/client.py`: Implements `RayClient` (`run_function_as_driver`, `get_job`, `list_jobs`, and `request` as the single HTTP override point), `JobResult` (`result`, `logs`, `status`, `done`, `wait`, `stop`, and `delete`), and the module-level `run_function_as_driver` helper.
- `src/ray_client/packaging.py`: Implements the transport-agnostic packaging layer. `build_driver_package` converts `(fn, args, kwargs, working_dir)` into a deterministic zip archive and base64 JSON payload; `parse_logs_and_result` splits raw driver logs into user logs, return values, and structured error metadata.
- `src/ray_client/_driver_runner.py`: Implements the cluster-side entrypoint packaged with every job. Edit this file in a fork to customize how the driver initializes Ray, loads inputs, or writes results.
- `src/ray_client/exceptions.py`: Defines `RayClientError`, `RayClientSerializationError`, `RayJobExecutionError`, and `RayJobTimeoutError`.
- `examples/`: Contains runnable examples for sequential driver workflows (`01_sequential_interactive_workflow.py`), stateful detached actor wrappers (`02_vibecoded_stateful_actor_client.py`), and multi-cluster job sharding (`03_vibecoded_multicluster_sharding_client.py`).
- `tests/`: Contains offline unit and mock HTTP server tests (`test_unit.py`) and live cluster end-to-end tests (`test_integration_live_cluster.py`).
