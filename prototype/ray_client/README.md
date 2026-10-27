# Ray Client

`ray_client` connects external Python programs (laptops, notebooks, coding agents) to a remote Ray cluster over the Ray Dashboard HTTP API (`:8265`). Only working directory files and JSON data cross the client-cluster boundary:

| Method | What you pass | How it runs | Use-case |
| :--- | :--- | :--- | :--- |
| `cluster.submit_job("python job.py")` | Shell command | Submits entrypoint script to `/api/jobs/` as a Ray Job, returning a `JobResult` handle (`status()`, `logs()`, `wait()`, `stop()`) | Existing CLI scripts, replaces `JobSubmissionClient` |
| `cluster.run(fn, ...)` | Python function | Packages function source and runs it in an ephemeral top-level Ray Actor (`num_cpus=0` by default) on `/api/actors`, returning JSON (or streaming yielded JSON items over SSE for generators) | Self-contained functions or generators that run once to completion |
| `cluster.actor(Cls, ...)` | Python class | Instantiates class as a persistent top-level Ray Actor (`num_cpus=1` by default) on `/api/actors` across calls | Interactive notebooks or agents needing persistent state, SSE generator streaming, and concurrent mid-run steering |

## Prerequisites

- Client environment: Python 3.9+ and `requests>=2.28.0` (no `ray` or `cloudpickle` required on the client).
- Cluster environment: A running Ray cluster with the Ray Dashboard HTTP server reachable on port `8265` by default.

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

    def update_lr(self, lr: float) -> dict:
        self.lr = lr
        return {"lr": self.lr}

    def train_steps(self, steps: int):
        for step in range(steps):
            yield {"step": step, "loss": 1.0 / (step + 1), "lr": self.lr}


cluster = RayCluster("http://127.0.0.1:8265")

# 1. One-shot driver function (runs in an ephemeral actor on the cluster)
result = cluster.run(main, 4)
print(result)  # {"sum": 60, "values": [0, 10, 20, 30]}

# 2. Stateful actor session with SSE streaming and mid-run steering
trainer = cluster.actor(Trainer, kwargs={"lr": 1e-3}, name="exp-1")
for metrics in trainer.train_steps(100):
    if metrics["step"] == 20:
        trainer.update_lr(1e-4)
    if metrics["loss"] < 0.02:
        break
trainer.kill()
```

## Cluster execution

- **Head-node driver scheduling (`num_cpus=0`)**: `cluster.run(fn, ...)` defaults to `num_cpus=0`. Any root actor with `num_cpus=0` and no GPU, memory, custom resource, or label-selector options is scheduled on the head node (unless `RAY_JOB_ALLOW_DRIVER_ON_WORKER_NODES=1`, matching `submit_job`), so driver functions start immediately on `num-cpus=0` head nodes and never steal CPUs from worker placement groups. When a call completes or the client disconnects mid-call, the server kills ephemeral `run()` actors outright; on persistent `actor()` sessions (`num_cpus=1` by default), a client disconnect cancels the call if it has not started yet and stops a generator at its next `yield` (a synchronous method already executing runs to completion).
- **Child namespace isolation**: Non-detached child actors created in the root actor process default to a per-actor Ray namespace (`ray_client_<name>`), while `lifetime="detached"` child actors default to `"ray_client"` and `ray.get_actor(name)` checks `ray_client_<name>` before falling back to `"ray_client"`. Isolation and fallback apply to actors created and looked up in the root actor process; nested tasks or child actors must pass `namespace=` explicitly (for example, forwarding `ray.get_runtime_context().namespace` from the root).
- **Root actor log scope**: `GET /api/actors/{name}/logs` (`actor.logs()`) captures `stdout` and `stderr` of the root actor process, whereas `submit_job` (`job.logs()`) aggregates logs across all job workers.
- **Cluster activity reporting**: Active `/api/actors` sessions report cluster activity to `/api/component_activities` so auto-terminating clusters do not shut down while `run()` or `actor()` workloads are active.

## Running tests

Offline unit and mock REST server tests (no `ray` required):

```bash
PYTHONPATH=src pytest tests/test_unit.py -v
```

Live cluster integration tests:

```bash
RAY_LIVE_CLUSTER_ADDRESS=http://127.0.0.1:8265 PYTHONPATH=src pytest tests/test_integration_live_cluster.py -v
```
