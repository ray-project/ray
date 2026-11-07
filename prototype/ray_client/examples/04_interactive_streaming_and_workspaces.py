"""Example 4: SSE streaming, mid-run steering, working_dir workspaces, and idle TTL.

Demonstrates:
  1. Streaming yielded dicts over SSE (`coordinator.stream_batches.stream(...)`) while
     sending concurrent mid-run steering calls (`coordinator.scale(3)` with `max_concurrency=2`).
  2. Packaging a local `working_dir` workspace once and reusing it across `cluster.run()`
     and `cluster.submit_job()`, plus inspecting cluster node groups and nodes.
  3. Idle TTL expiration (`ttl_seconds`) reaping an inactive actor once all
     calls and streams have finished.
"""

import sys
import tempfile
import threading
import time
from pathlib import Path
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


def run_workspace_task(values: list) -> dict:
    from workspace_helpers import summarize_values

    return summarize_values(values)


def run_demo(address: Optional[str] = None) -> dict:
    with RayCluster(address=address) as cluster:
        # 1. Real-time SSE generator streaming + concurrent mid-run worker scaling
        with cluster.actor(
            AdaptivePoolCoordinator,
            1,
            name="pool_coordinator",
            resources={"CPU": 1},
            max_concurrency=2,
            get_if_exists="recreate",
        ) as coordinator:
            emitted_events = []
            totals = []
            for event in coordinator.stream_batches.stream([[1, 2, 3], [1, 2, 3]]):
                emitted_events.append(event)
                if event.get("event") == "computed":
                    totals.append(event["batch_total"])
                    if event["step"] == 0:
                        scale_ack = coordinator.scale(3)
                        emitted_events.append(scale_ack)

        driver_summary = {"steps": len(totals), "totals": totals}

        # 2. Local working_dir packaging shared across cluster.run() and cluster.submit_job()
        with tempfile.TemporaryDirectory() as tmpdir:
            helper_path = Path(tmpdir) / "workspace_helpers.py"
            helper_path.write_text(
                "def summarize_values(vals):\n"
                "    return {'count': len(vals), 'sum': sum(vals)}\n",
                encoding="utf-8",
            )
            workspace_run = cluster.run(
                run_workspace_task, [10, 20, 30], runtime_env={"working_dir": tmpdir}
            )
            job = cluster.submit_job(
                'python3 -c "from workspace_helpers import summarize_values; '
                'print(summarize_values([1, 2, 3]))"',
                runtime_env={"working_dir": tmpdir},
                wait=True,
            )
            assert "'sum': 6" in job.logs()

        # 3. Cluster node group & node inspection
        node_groups = cluster.list_node_groups()
        nodes = cluster.list_nodes()
        assert len(node_groups) >= 1
        assert len(nodes) >= 1
        first_node = cluster.get_node(nodes[0]["id"])
        assert first_node["id"] == nodes[0]["id"]

        # 4. Idle TTL cleanup verification
        cluster.actor(
            AdaptivePoolCoordinator,
            1,
            name="ephemeral_ttl_actor",
            ttl_seconds=0.6,
            get_if_exists="recreate",
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

    return {
        "emitted_events": emitted_events,
        "driver_summary": driver_summary,
        "workspace_run": workspace_run,
        "node_count": len(nodes),
        "ttl_cleaned": ttl_cleaned,
    }


if __name__ == "__main__":
    cluster_addr = sys.argv[1] if len(sys.argv) > 1 else None
    print("Interactive streaming & workspace demo output:", run_demo(cluster_addr))
