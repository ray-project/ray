# Ray Client

`ray_client` connects external Python programs on laptops, notebooks, and coding agents to a remote Ray cluster over the Ray Dashboard HTTP API (`:8265`). Only working directory files and JSON data cross the client-cluster boundary:

| Method | What you pass | How it runs | Use-case |
| :--- | :--- | :--- | :--- |
| `cluster.submit_job("python job.py")` | Shell command | Submits an entrypoint script to `/api/jobs/` as a Ray Job and returns a `JobResult` handle. | Existing CLI scripts (replaces `JobSubmissionClient`). |
| `cluster.run(fn, ...)` | Python function | Runs `fn` in a non-detached root actor (`num_cpus=0` by default) on `/api/actors`, returning JSON or an SSE stream. | Self-contained functions or generators that run once to completion. |
| `cluster.actor(Cls, ...)` | Python class | Deploys `Cls` as a detached root actor (`num_cpus=1` by default) on `/api/actors` across calls. | Interactive notebooks or agents needing persistent state, SSE streaming, and concurrent mid-run steering. |

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

- **Head-node driver scheduling (`num_cpus=0`)**: `cluster.run(fn, ...)` defaults to `num_cpus=0`. Root actors with `num_cpus=0` and no placement options run on the head node unless `RAY_JOB_ALLOW_DRIVER_ON_WORKER_NODES=1` is set. This matches `submit_job` and reserves worker CPUs for placement groups.
- **Disconnect and timeout cancellation**: One-shot `run()` root actors are non-detached (`detached=False`, owner-bound to the Dashboard agent so warm child `@ray.remote` workers are reused across consecutive `run()` calls) and are killed when a call finishes or the client disconnects. On persistent `actor()` sessions, a disconnect or timeout calls `ray.cancel(..., recursive=True)` to cancel queued calls, active `async` coroutines, generators, and child tasks.
- **Stale actor detection (`get_if_exists`)**: Named actors compute a SHA-256 `spec_hash` over their source, arguments, `runtime_env`, and resources. Reattaching with `get_if_exists=True` emits a `UserWarning` on a mismatch, whereas `get_if_exists="recreate"` replaces the actor when `spec_hash` changes.
- **Graceful teardown (`close()`, `ttl_seconds`)**: Using `with cluster.actor(...) as actor:` kills the actor on block exit. Before an actor is killed or reaped by idle `ttl_seconds`, it runs any user-defined `close()` or `__exit__()` hook and removes owned placement groups and child actors.
- **Child namespace isolation**: Non-detached child actors created in the root actor process use namespace `ray_client_<name>`, while detached child actors use `"ray_client"`. `ray.get_actor(name)` in the root process checks `ray_client_<name>` before falling back to `"ray_client"`; nested tasks and child actors must pass `namespace=` explicitly.
- **Root actor log scope**: `actor.logs()` (`GET /api/actors/{name}/logs`) returns `stdout` and `stderr` from the root actor process. `job.logs()` (`submit_job`) aggregates logs across all job workers.
- **Cluster activity reporting**: Active `/api/actors` sessions report activity to `/api/component_activities` so auto-terminating clusters stay alive while `run()` or `actor()` workloads are running.

## Running tests

Run the offline unit and mock REST server tests (no `ray` required):

```bash
PYTHONPATH=src pytest tests/test_unit.py -v
```

Run the live cluster integration tests against a running Ray cluster:

```bash
RAY_LIVE_CLUSTER_ADDRESS=http://127.0.0.1:8265 PYTHONPATH=src pytest tests/test_integration_live_cluster.py -v
```
