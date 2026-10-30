# Ray Client

`ray_client` connects external Python programs on laptops, notebooks, and coding agents to a remote Ray cluster over the Ray Dashboard HTTP API (`:8265`). Only working directory files and JSON data cross the client-cluster boundary:

| Method | What you pass | How it runs | Use-case |
| :--- | :--- | :--- | :--- |
| `cluster.submit_job("python job.py")` | Shell command | Submits an entrypoint script to `/api/jobs/` as a Ray Job and returns a `JobResult` handle. | Existing CLI scripts (replaces `JobSubmissionClient`). |
| `cluster.run(fn, ...)` | Python function | Runs `fn` in a non-detached root actor (`resources={}` by default) on `/api/actors`, returning JSON or an SSE stream. | Self-contained functions or generators that run once to completion. |
| `cluster.actor(Cls, ...)` | Python class | Deploys `Cls` as a detached root actor (`resources={"CPU": 1}`, `max_concurrency=1` by default) on `/api/actors` across calls. | Interactive notebooks or agents needing persistent state, SSE streaming, and mid-run steering. |

## Prerequisites

The client and cluster require the following setup:

- **Client environment**: Python 3.9+ and `requests>=2.28.0`, with no `ray` or `cloudpickle` dependency on the client.
- **Cluster environment**: A running Ray cluster with the Ray Dashboard HTTP server reachable on port `8265`.

Install in editable mode from this directory:

```bash
pip install -e .
```

## Quickstart

```python
from ray_client import RayCluster

SCALE = 10


def main(n: int) -> dict:
    import ray

    @ray.remote
    class Worker:
        def compute(self, x: int) -> int:
            return x * SCALE

    workers = [Worker.remote() for _ in range(n)]
    values = ray.get([w.compute.remote(i) for i, w in enumerate(workers)])
    return {"sum": sum(values), "values": values}


class Trainer:
    def __init__(self, lr: float = 1e-3):
        self.lr = lr
        self.step = 0
        self.loss = 1.0

    def train_step(self) -> dict:
        self.step += 1
        self.loss = 1.0 / self.step
        return self.get_metrics()

    def get_metrics(self) -> dict:
        return {"step": self.step, "loss": self.loss, "lr": self.lr}

    def update_lr(self, lr: float) -> dict:
        self.lr = lr
        return {"lr": self.lr}


cluster = RayCluster("http://127.0.0.1:8265")

# 1. One-shot driver function (runs in a non-detached root actor on the cluster)
result = cluster.run(main, 4)
print(result)  # {"sum": 60, "values": [0, 10, 20, 30]}

# 2. Stateful actor session with step-by-step monitoring and mid-run steering
with cluster.actor(
    Trainer, kwargs={"lr": 1e-3}, name="exp-1", get_if_exists="recreate"
) as trainer:
    for _ in range(100):
        metrics = trainer.train_step()
        if metrics["step"] == 20:
            trainer.update_lr(1e-4)
        if metrics["loss"] < 0.02:
            break
```

## Cluster execution

The Ray Dashboard runs `run()` and `actor()` workloads in root actors on `/api/actors`:

- **Resource and placement options (`resources`, `label_selector`, `node_id`)**: `cluster.run(fn, ...)` defaults to `resources={}` (`num_cpus=0`) and runs on the head node unless `RAY_JOB_ALLOW_DRIVER_ON_WORKER_NODES=1` or explicit placement options (`label_selector`, `node_id`) are set. `cluster.actor(Cls, ...)` defaults to `resources={"CPU": 1}` and `max_concurrency=1` (pass `max_concurrency=2` when issuing concurrent mid-run steering calls while a synchronous generator stream is active). Both accept unified `resources={"CPU": 2, "GPU": 1, "memory": 1073741824, "TPU": 4}`, node label constraints (`label_selector={"ray.io/accelerator-type": "TPU-V6E"}`), or target node pinning (`node_id=...`).
- **Explicit and automatic SSE streaming (`actor.stream()`)**: Generator methods on actors and generator functions passed to `cluster.run()` stream yielded JSON values over Server-Sent Events (`text/event-stream`). You can also invoke `actor.stream("method", *args)` or `actor.method.stream(*args)` to require an SSE `Iterator[Any]`.
- **Disconnect and timeout cancellation**: One-shot `run()` root actors are non-detached (`detached=False`, owner-bound to the Dashboard agent so warm child `@ray.remote` workers are reused across consecutive `run()` calls) and are killed when a call finishes or the client disconnects. On persistent `actor()` sessions, a disconnect or timeout calls `ray.cancel(..., recursive=True)` to cancel queued calls, active `async` coroutines, generators, and child tasks.
- **Stale actor detection (`get_if_exists`)**: Named actors compute a SHA-256 `spec_hash` over their source, arguments, `runtime_env`, and resources. Reattaching with `get_if_exists=True` emits a `UserWarning` on a mismatch, whereas `get_if_exists="recreate"` replaces the actor when `spec_hash` changes.
- **Graceful teardown (`close()`, `ttl_seconds`)**: Using `with cluster.actor(...) as actor:` kills the actor on block exit. Before an actor is killed or reaped by idle `ttl_seconds`, it runs any user-defined `close()` or `__exit__()` hook and removes owned placement groups and child actors.
- **Child namespace isolation**: Non-detached child actors created in the root actor process use namespace `ray_client_<name>`, while detached child actors use `"ray_client"`. `ray.get_actor(name)` in the root process checks `ray_client_<name>` before falling back to `"ray_client"`; nested tasks and child actors must pass `namespace=` explicitly.
- **Root actor log scope**: `actor.logs()` (`GET /api/actors/{name}/logs`) returns `stdout` and `stderr` from the root actor process. `job.logs()` (`submit_job`) aggregates logs across all job workers.
- **Cluster activity reporting**: Active `/api/actors` sessions report activity to `/api/component_activities` so auto-terminating clusters stay alive while `run()` or `actor()` workloads are running.
- **Cluster node management**: `RayCluster` exposes `list_node_groups()`, `get_node_group(group_name)`, `add_nodes(group_name, count=1, *, wait_ready=False, timeout=None)`, `list_nodes(*, group_name=None, states=None, include_dead=False)`, `get_node(node_id)`, and `destroy_node(node_id, reason, *, grace_period_s=0.0)` over `/api/cluster/*`.

## Environment variables

| Variable | Description |
| :--- | :--- |
| `RAY_API_SERVER_ADDRESS` / `RAY_ADDRESS` | Default Ray Dashboard HTTP URL when `address` is omitted (falls back to `http://127.0.0.1:8265`). |
| `RAY_AUTH_TOKEN` | Bearer token sent in `Authorization: Bearer <token>` when `headers["Authorization"]` is omitted. |
| `RAY_AUTH_TOKEN_PATH` | Path to a file containing the Bearer token (defaults to `~/.ray/auth_token` if present). |
| `RAY_RUNTIME_ENV_IGNORE_GITIGNORE` | Set to `1` to skip `.gitignore` rules when packaging local `working_dir` directories. |
| `RAY_OVERRIDE_RUNTIME_ENV_DEFAULT_EXCLUDES` | Comma-separated list of gitignore patterns replacing the default `working_dir` excludes. |

## Examples

The `examples/` directory contains runnable end-to-end scripts:

- `examples/01_sequential_interactive_workflow.py`: Two-step driver function pipeline (`main_1` scoring -> `main_2` filtering) passing JSON between `cluster.run()` calls.
- `examples/02_vibecoded_stateful_actor_client.py`: Deploying stateful actors, calling methods, reconnecting by name, and inspecting logs.
- `examples/03_vibecoded_multicluster_sharding_client.py`: Dispatching sharded workloads across multiple `RayCluster` instances concurrently.
- `examples/04_interactive_streaming_and_workspaces.py`: Real-time SSE generator streaming, mid-run actor steering (`max_concurrency=2`), and idle TTL expiration.

## Running tests

Run the offline unit and mock REST server tests (no `ray` required):

```bash
PYTHONPATH=src pytest tests/test_unit.py -v
```

Run the live cluster integration tests (auto-starts an isolated local Ray cluster when `RAY_LIVE_CLUSTER_ADDRESS` is unset):

```bash
PYTHONPATH=../../python:src pytest tests/test_integration_live_cluster.py -v
```
