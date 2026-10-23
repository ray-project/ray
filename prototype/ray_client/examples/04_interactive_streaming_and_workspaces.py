"""Example 4: Interactive generator streaming, mid-run scaling, `$ray_ref` DAGs, and workspaces.

Demonstrates all Ray 3.0 Client interactive workspace capabilities:
  1. Real-time generator streaming (`actor.stream`) paired with mid-run concurrent
     actor control calls (`actor.scale(3)`) to dynamically scale in-cluster workers.
  2. Live actor creation (`client.actor`), in-cluster `$ray_ref` chaining across
     notebook cells (`return_ref=True`), async task pipelining (`wait=False`), and
     passing a `RemoteRef` into a stateless task (`client.run`).
  3. Multi-tenant named workspaces (`workspace=...`), selective reconnection, and
     automatic disconnect TTL cleanup (`ttl_seconds=...`).
"""

import sys
import time
from typing import TYPE_CHECKING, Iterator, Optional

from ray_client import RayClient

if TYPE_CHECKING:
    import ray


# --- 1. Interactive streaming & dynamic resource scaling coordinator actor ---
class AdaptivePoolCoordinator:
    def __init__(self, initial_workers: int = 1):
        @ray.remote(num_cpus=0)
        class BatchWorker:
            def __init__(self, worker_id: int):
                self.worker_id = worker_id

            def process(self, items: list) -> int:
                return sum(x * (self.worker_id + 1) for x in items)

        self._worker_cls = BatchWorker
        self._workers = [BatchWorker.remote(i) for i in range(initial_workers)]

    def scale(self, target: int) -> dict:
        target = int(target)
        while len(self._workers) < target:
            self._workers.append(self._worker_cls.remote(len(self._workers)))
        while len(self._workers) > target:
            ray.kill(self._workers.pop())
        return {"event": "scaled", "num_workers": len(self._workers)}

    def stream_batches(self, batches: list) -> Iterator[dict]:
        yield {"event": "ready", "num_workers": len(self._workers)}
        for step_idx, batch in enumerate(batches):
            partials = ray.get([w.process.remote(batch) for w in self._workers])
            yield {
                "event": "computed",
                "step": step_idx,
                "num_workers": len(self._workers),
                "batch_total": sum(partials),
            }
            time.sleep(0.05)

    def shutdown_pool(self) -> int:
        count = len(self._workers)
        for w in self._workers:
            ray.kill(w)
        self._workers = []
        return count


# --- 2. Live Actor & `$ray_ref` notebook workflow ---
class ShardProcessor:
    def __init__(self, prefix: str = "shard"):
        self.prefix = prefix

    def build_shard(self, count: int, scale: int = 2) -> list:
        return [{"idx": i, "val": i * scale} for i in range(count)]

    def filter_and_sum(self, shard: list, min_val: int = 0) -> dict:
        kept = [row["val"] for row in shard if row["val"] >= min_val]
        return {"prefix": self.prefix, "kept_count": len(kept), "sum": sum(kept)}


def inspect_ref_in_task(shard: list) -> dict:
    """Stateless workspace task that receives an in-cluster `$ray_ref` automatically."""
    return {"first": shard[0]["val"], "last": shard[-1]["val"], "len": len(shard)}


def run_demo(address: Optional[str] = None) -> dict:
    # 1. Real-time generator streaming + mid-run dynamic worker scaling
    stream_client = RayClient(address=address, workspace="interactive-scaling")
    coordinator = stream_client.actor(
        AdaptivePoolCoordinator, 1, name="pool_coordinator"
    )
    emitted_events = []
    totals = []
    for event in coordinator.stream("stream_batches", [[1, 2, 3], [1, 2, 3]]):
        emitted_events.append(event)
        if event.get("event") == "computed":
            totals.append(event["batch_total"])
            if event["step"] == 0:
                # Dynamically scale the worker pool from 1 -> 3 before step 1 runs!
                scale_ack = coordinator.scale(3)
                emitted_events.append(scale_ack)

    coordinator.shutdown_pool()
    stream_client.close_workspace()
    driver_summary = {"steps": len(totals), "totals": totals}

    # 2. Live actor + in-cluster `$ray_ref` chaining + multi-tenancy
    tenant_a = RayClient(address=address, workspace="tenant-a", ttl_seconds=60.0)
    tenant_b = RayClient(address=address, workspace="tenant-b", ttl_seconds=60.0)

    proc_a = tenant_a.actor(ShardProcessor, "alpha", name="processor")
    proc_b = tenant_b.actor(ShardProcessor, "beta", name="processor")

    # Cell 1 -> Cell 2: keep intermediate shard in-cluster via RemoteRef (`$ray_ref`)
    shard_ref_a = proc_a.build_shard.ref(5, scale=10)
    summary_a = proc_a.filter_and_sum(shard_ref_a, min_val=20)
    task_check = tenant_a.run(inspect_ref_in_task, shard_ref_a)

    # Reconnect a fresh client handle to `tenant-a` and reattach to its actor
    reconnected_a = RayClient(address=address, workspace="tenant-a")
    reattached_proc_a = reconnected_a.get_actor("processor")
    reattached_summary = reattached_proc_a.filter_and_sum(shard_ref_a, min_val=30)

    # Tenant B has its own isolated actor with the same name ("processor")
    shard_ref_b = proc_b.build_shard.ref(3, scale=5)
    summary_b = proc_b.filter_and_sum(shard_ref_b)

    tenant_a.close_workspace()
    tenant_b.close_workspace()

    # 3. Disconnect TTL cleanup verification
    ephemeral = RayClient(address=address, workspace="ephemeral-ttl", ttl_seconds=0.8)
    ephemeral.actor(ShardProcessor, "temp", name="temp_actor")
    assert len(ephemeral.list_actors()) == 1
    time.sleep(1.3)
    # After idle TTL expires, workspace-owned actor is automatically reaped.
    with RayClient(address=address) as probe:
        active_ws_names = [w["workspace"] for w in probe.list_workspaces()]
        ttl_cleaned = "ephemeral-ttl" not in active_ws_names

    return {
        "emitted_events": emitted_events,
        "driver_summary": driver_summary,
        "summary_a": summary_a,
        "driver_check": task_check,
        "reattached_summary": reattached_summary,
        "summary_b": summary_b,
        "ttl_cleaned": ttl_cleaned,
    }


if __name__ == "__main__":
    cluster_addr = sys.argv[1] if len(sys.argv) > 1 else None
    print("Interactive & workspace demo output:", run_demo(cluster_addr))
