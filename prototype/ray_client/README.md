# `ray_client` — Lightweight REST-Only Ray Client (`run_function_as_driver`)

A minimal, forkable reference implementation of the Ray 3.0 Client built on top of the existing Ray Job Submission REST API (`/api/packages/` and `/api/jobs/`).

## Design Principles

1. **Zero `ray` or `cloudpickle` Dependency**: Requires only `requests`. Runs on any Python version or environment (laptop notebooks, CI runners, web services) without matching the cluster's Python or Ray version.
2. **Run a Function, Not Imported Files**: Extracts the AST source of the target Python `def` function and executes it as a real in-cluster driver process, so Ray libraries (`ray.data`, `ray.train`, `@ray.remote` tasks/actors) work natively without importing the caller's surrounding module on the cluster.
3. **Strict JSON & File Sync Boundary**: Only files (`working_dir` zip packages) and JSON data cross the client-cluster boundary.
4. **Forkable & Standalone Repo Ready**: Self-contained in 5 files under `prototype/ray_client/src/ray_client/` so it can be split into its own repository (`git subtree split --prefix=prototype/ray_client`) or forked by users to vibe-code custom clients.

## Rules for Driver Functions

- Write a plain, undecorated `def` (sync or `async`) in a `.py` file or notebook cell. Its exact source is extracted and executed on the cluster.
- Put `import` statements for anything other than `ray` inside the function (or in a module under `working_dir`); the client machine does not need those packages.
- Arguments, referenced module-level constants, and the return value must be JSON. Return summaries or storage URIs rather than large arrays.
- Ray is already initialized in a shared namespace (`"ray_client"` by default) when the function runs; calling `ray.init()` again is a no-op. Named detached actors created by one call are visible to the next via `ray.get_actor(name)`.

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

## Layout

The public API is one function (`run_function_as_driver`), two classes (`RayClient`, `JobResult`), and four exceptions. Internally the package is split into a transport-agnostic packaging layer and the HTTP transport, so a fork can keep the former and swap the latter (for example, submitting the same package as a KubeRay `RayJob` instead of through the Jobs REST API).

- `src/ray_client/client.py`: `RayClient` (`run_function_as_driver`, `get_job`, `list_jobs`, and `request` as the single HTTP seam to override), `JobResult` (`result`, `logs`, `status`, `done`, `wait`, `stop`, `delete`), and top-level `run_function_as_driver`.
- `src/ray_client/packaging.py`: Transport-agnostic layer. `build_driver_package` turns `(fn, args, kwargs, working_dir)` into a deterministic zip plus a base64 JSON payload; `parse_logs_and_result` turns raw driver logs back into `(user_logs, output, error)`.
- `src/ray_client/_driver_runner.py`: The cluster-side entrypoint shipped with every job. Fork this file to change how the driver initializes Ray, loads inputs, or reports results.
- `src/ray_client/exceptions.py`: `RayClientError`, `RayClientSerializationError`, `RayJobExecutionError`, and `RayJobTimeoutError`.
- `examples/`:
  - `01_sequential_interactive_workflow.py`
  - `02_vibecoded_stateful_actor_client.py`
  - `03_vibecoded_multicluster_sharding_client.py`
- `tests/`:
  - `test_unit.py`: Offline unit & mock HTTP server tests (zero `ray` dependency).
  - `test_integration_live_cluster.py`: Live Ray cluster end-to-end tests.
