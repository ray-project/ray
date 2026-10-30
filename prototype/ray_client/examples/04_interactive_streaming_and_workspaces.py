"""Example 4: SSE output streaming, mid-run steering, reconnection, and idle TTL.

Demonstrates:
  1. Streaming yielded dicts over SSE (`coordinator.stream_batches(...)`) while sending
     concurrent mid-run steering calls (`coordinator.scale(3)`).
  2. Stateful actors (`cluster.actor`), reconnecting (`cluster.get_actor`), and
     passing JSON state to a driver function (`cluster.run`).
  3. Idle TTL expiration (`ttl_seconds`) reaping an inactive actor once all
     calls and streams have finished.
"""

import sys
import threading
import time
from typing import TYPE_CHECKING, Iterator, Optional

from ray_client import RayCluster

if TYPE_CHECKING:
    import ray


class AdaptivePoolCoordinator:
    def __init__(self, initial_workers: int = 1):
        @ray.remote(num_cpus=0)
        class BatchWorker:
            def __init__(self, worker_id: int):
                self.worker_id = worker_id

            def process(self, items: list) -> int:
                return sum(x * (self.worker_id + 1) for x in items)

        self._lock = threading.Lock()
        self._worker_cls = BatchWorker
        self._workers = [BatchWorker.remote(i) for i in range(initial_workers)]

    def scale(self, target: int) -> dict:
        target = int(target)
        to_terminate = []
        with self._lock:
            while len(self._workers) < target:
                self._workers.append(self._worker_cls.remote(len(self._workers)))
            while len(self._workers) > target:
                to_terminate.append(self._workers.pop())
            num_workers = len(self._workers)
        for worker in to_terminate:
            worker.__ray_terminate__.remote()
        return {"event": "scaled", "num_workers": num_workers}

    def stream_batches(self, batches: list) -> Iterator[dict]:
        with self._lock:
            initial_count = len(self._workers)
        yield {"event": "ready", "num_workers": initial_count}
        for step_idx, batch in enumerate(batches):
            with self._lock:
                workers = list(self._workers)
            partials = ray.get([w.process.remote(batch) for w in workers])
            yield {
                "event": "computed",
                "step": step_idx,
                "num_workers": len(workers),
                "batch_total": sum(partials),
            }
            time.sleep(0.05)


class ShardProcessor:
    def __init__(self, prefix: str = "shard"):
        self.prefix = prefix

    def build_shard(self, count: int, scale: int = 2) -> list:
        return [{"idx": i, "val": i * scale} for i in range(count)]

    def filter_and_sum(self, shard: list, min_val: int = 0) -> dict:
        kept = [row["val"] for row in shard if row["val"] >= min_val]
        return {"prefix": self.prefix, "kept_count": len(kept), "sum": sum(kept)}


def inspect_shard_in_driver(shard: list) -> dict:
    return {"first": shard[0]["val"], "last": shard[-1]["val"], "len": len(shard)}


def run_demo(address: Optional[str] = None) -> dict:
    cluster = RayCluster(address=address)

    # 1. Real-time SSE generator streaming + concurrent mid-run worker scaling
    coordinator = cluster.actor(
        AdaptivePoolCoordinator,
        1,
        name="pool_coordinator",
        max_concurrency=2,
        get_if_exists=False,
    )
    emitted_events = []
    totals = []
    for event in coordinator.stream_batches([[1, 2, 3], [1, 2, 3]]):
        emitted_events.append(event)
        if event.get("event") == "computed":
            totals.append(event["batch_total"])
            if event["step"] == 0:
                scale_ack = coordinator.scale(3)
                emitted_events.append(scale_ack)

    coordinator.kill()
    driver_summary = {"steps": len(totals), "totals": totals}

    # 2. Multi-step stateful actors + reconnection
    proc_a = cluster.actor(
        ShardProcessor,
        "alpha",
        name="processor_a",
        ttl_seconds=60.0,
        get_if_exists=False,
    )
    proc_b = cluster.actor(
        ShardProcessor,
        "beta",
        name="processor_b",
        ttl_seconds=60.0,
        get_if_exists=False,
    )

    shard_a = proc_a.build_shard(5, scale=10)
    summary_a = proc_a.filter_and_sum(shard_a, min_val=20)
    driver_check = cluster.run(inspect_shard_in_driver, shard_a)

    reconnected = RayCluster(address=address)
    reattached_proc_a = reconnected.get_actor("processor_a")
    reattached_summary = reattached_proc_a.filter_and_sum(shard_a, min_val=30)

    shard_b = proc_b.build_shard(3, scale=5)
    summary_b = proc_b.filter_and_sum(shard_b)

    proc_a.kill()
    proc_b.kill()
    reconnected.close()

    # 3. Idle TTL cleanup verification
    cluster.actor(
        ShardProcessor,
        "temp",
        name="ephemeral_ttl_actor",
        ttl_seconds=0.6,
        get_if_exists=False,
    )
    active_before = [a["name"] for a in cluster.list_actors()]
    assert "ephemeral_ttl_actor" in active_before
    deadline = time.monotonic() + 5.0
    ttl_cleaned = False
    while time.monotonic() < deadline:
        active_after = [a["name"] for a in cluster.list_actors()]
        if "ephemeral_ttl_actor" not in active_after:
            ttl_cleaned = True
            break
        time.sleep(0.05)
    cluster.close()

    return {
        "emitted_events": emitted_events,
        "driver_summary": driver_summary,
        "summary_a": summary_a,
        "driver_check": driver_check,
        "reattached_summary": reattached_summary,
        "summary_b": summary_b,
        "ttl_cleaned": ttl_cleaned,
    }


if __name__ == "__main__":
    cluster_addr = sys.argv[1] if len(sys.argv) > 1 else None
    print("Interactive streaming & actor demo output:", run_demo(cluster_addr))
