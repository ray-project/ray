"""Live Ray cluster integration tests for ray_client.

Run with:
    RAY_LIVE_CLUSTER_ADDRESS=http://127.0.0.1:8265 pytest prototype/ray_client/tests/
"""

import os
import sys
import tempfile
import time
import unittest
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, List

from ray_client import RayClient, RayExecutionError, RemoteRef

LIVE_ADDRESS = os.environ.get("RAY_LIVE_CLUSTER_ADDRESS")

GLOBAL_FACTOR = 10


@unittest.skipUnless(
    LIVE_ADDRESS,
    "Set RAY_LIVE_CLUSTER_ADDRESS=http://127.0.0.1:8265 to run live cluster tests",
)
class TestLiveRayClusterIntegration(unittest.TestCase):
    def setUp(self):
        assert LIVE_ADDRESS is not None
        self.client = RayClient(address=LIVE_ADDRESS)

    def tearDown(self):
        self.client.close()

    def test_tasks_actors_defaults_and_nested_classes(self):
        def compute_batch(items: List[int], scale: int = GLOBAL_FACTOR) -> dict:
            import ray

            total_cpus = ray.cluster_resources().get("CPU", 0.0)
            # Calling ray.init() inside a workspace task is a safe no-op.
            ray.init()

            # Top-level `dataclass` and `sys` are automatically imported in the shipped module!
            @dataclass
            class Item:
                x: int

            @ray.remote
            def mul(item: Item, factor: int) -> Item:
                return Item(item.x * factor)

            # `List` exists only on the client; the shipped module stubs hint-only globals.
            @ray.remote
            def total(xs: List[int]) -> int:
                return sum(xs)

            items_out = ray.get([mul.remote(Item(i), scale) for i in items])
            out = [it.x for it in items_out]
            return {
                "values": out,
                "sum": ray.get(total.remote(out)),
                "cluster_py": list(sys.version_info[:2]),
                "namespace": ray.get_runtime_context().namespace,
                "total_cpus": total_cpus,
            }

        out1 = self.client.run(compute_batch, [1, 2, 3])
        self.assertEqual(out1["values"], [10, 20, 30])
        self.assertEqual(out1["sum"], 60)
        self.assertGreater(out1["total_cpus"], 0)
        self.assertEqual(out1["namespace"], self.client.namespace)
        self.assertEqual(
            self.client.workspace_info()["namespace"], self.client.namespace
        )

        out2 = self.client.run(compute_batch, [4, 5], kwargs={"scale": 3})
        self.assertEqual(out2["values"], [12, 15])

    def test_async_task_pipelining_remote_ref_ready_and_cancel(self):
        def produce_numbers(n: int, base: int = 10) -> list:
            return [base * (i + 1) for i in range(n)]

        def sum_numbers(xs: list, bonus: int = 0) -> dict:
            return {"count": len(xs), "total": sum(xs) + bonus}

        def slow_task(seconds: float) -> int:
            time.sleep(seconds)
            return 1

        # Submit non-blocking task (`wait=False`) -> pass in-flight RemoteRef directly
        # into downstream task without pulling intermediate list to the client!
        in_flight_ref = self.client.run(produce_numbers, 4, wait=False)
        self.assertIsInstance(in_flight_ref, RemoteRef)
        pipelined = self.client.run(sum_numbers, in_flight_ref, kwargs={"bonus": 5})
        self.assertEqual(pipelined, {"count": 4, "total": 105})
        self.assertTrue(in_flight_ref.ready())
        self.assertEqual(in_flight_ref.get(), [10, 20, 30, 40])
        self.assertTrue(in_flight_ref.delete())

        # Cancellation of an in-flight task via RemoteRef.cancel()
        slow_ref = self.client.run(slow_task, 10.0, wait=False)
        self.assertFalse(slow_ref.ready(timeout=0.05))
        self.assertTrue(slow_ref.cancel(force=True))

    def test_working_dir_ray_data_task_streaming_and_error_propagation(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "work"
            shared = Path(tmpdir) / "shared"
            (root / "outputs").mkdir(parents=True)
            shared.mkdir()
            (root / "my_helper.py").write_text(
                "def transform(s: str) -> str:\n    return f'[{s.upper()}]'\n"
                "class HelperCounter:\n"
                "    def __init__(self, start: int = 0):\n"
                "        self.val = start\n"
                "    def inc(self, delta: int = 1) -> int:\n"
                "        self.val += delta\n"
                "        return self.val\n",
                encoding="utf-8",
            )
            (shared / "shared_mod.py").write_text("FACTOR = 10\n", encoding="utf-8")
            os.symlink(shared, root / "linked")
            (root / ".gitignore").write_text("*.secret\n", encoding="utf-8")
            (root / "creds.secret").write_text("never uploaded\n", encoding="utf-8")

            def use_helper_and_ray_data(word: str) -> dict:
                from linked.shared_mod import FACTOR  # type: ignore[import-not-found]
                from my_helper import transform  # type: ignore[import-not-found]

                import ray

                ds = ray.data.from_items([{"x": 1}, {"x": 2}, {"x": 3}])
                rows = ds.map(lambda r: {"x": r["x"] * FACTOR}).take_all()
                return {
                    "transformed": transform(word),
                    "total": sum(r["x"] for r in rows),
                    "has_outputs_dir": os.path.isdir("outputs"),
                    "has_secret": os.path.exists("creds.secret"),
                }

            assert LIVE_ADDRESS is not None
            with RayClient(
                address=LIVE_ADDRESS,
                default_runtime_env={"working_dir": root},
            ) as wd_client:
                res = wd_client.run(use_helper_and_ray_data, "ray")
                self.assertEqual(
                    res,
                    {
                        "transformed": "[RAY]",
                        "total": 60,
                        "has_outputs_dir": True,
                        "has_secret": False,
                    },
                )
                # Also verify module-path string entrypoints (`module:name`) on live cluster:
                self.assertEqual(wd_client.run("my_helper:transform", "ray"), "[RAY]")
                hc = wd_client.actor("my_helper:HelperCounter", 10, name="hc")
                self.assertEqual(hc.inc(5), 15)

        def stream_progress(steps: int) -> Iterator[dict]:
            for i in range(steps):
                yield {"step": i, "pct": int((i + 1) * 100 / steps)}

        events = list(self.client.stream(stream_progress, 4))
        self.assertEqual(
            events,
            [
                {"step": 0, "pct": 25},
                {"step": 1, "pct": 50},
                {"step": 2, "pct": 75},
                {"step": 3, "pct": 100},
            ],
        )

        def crash_task():
            raise RuntimeError("expected cluster task failure")

        with self.assertRaises(RayExecutionError) as ctx:
            self.client.run(crash_task)
        self.assertEqual(ctx.exception.exc_type, "RuntimeError")
        self.assertIn("expected cluster task failure", str(ctx.exception))

    def test_examples_run_end_to_end(self):
        examples_dir = Path(__file__).resolve().parents[1] / "examples"
        sys.path.insert(0, str(examples_dir))
        try:
            ex1 = __import__("01_sequential_interactive_workflow")
            ex2 = __import__("02_vibecoded_stateful_actor_client")
            ex3 = __import__("03_vibecoded_multicluster_sharding_client")
            ex4 = __import__("04_interactive_streaming_and_workspaces")

            assert LIVE_ADDRESS is not None
            out1 = ex1.run_demo(LIVE_ADDRESS)
            self.assertEqual(out1["count"], 4)

            out2 = ex2.run_demo(LIVE_ADDRESS)
            self.assertEqual(out2, {"v1": 15, "v2": 42})

            out3 = ex3.run_demo([LIVE_ADDRESS, LIVE_ADDRESS])
            self.assertEqual(out3["total_sum_of_squares"], 285)

            out4 = ex4.run_demo(LIVE_ADDRESS)
            self.assertEqual(out4["driver_summary"], {"steps": 2, "totals": [6, 36]})
            self.assertEqual(
                out4["summary_a"],
                {"prefix": "alpha", "kept_count": 3, "sum": 90},
            )
            self.assertEqual(
                out4["driver_check"],
                {"first": 0, "last": 40, "len": 5},
            )
            self.assertEqual(
                out4["reattached_summary"],
                {"prefix": "alpha", "kept_count": 2, "sum": 70},
            )
            self.assertEqual(
                out4["summary_b"],
                {"prefix": "beta", "kept_count": 3, "sum": 15},
            )
            self.assertTrue(out4["ttl_cleaned"])
        finally:
            sys.path.pop(0)

    def test_live_actor_remote_ref_non_json_intermediate_and_streaming(self):
        class StatefulWorker:
            def __init__(self, base: int = 10):
                self.base = base

            def set_base(self, new_base: int) -> int:
                self.base = new_base
                return self.base

            def make_in_cluster_tuple(self, n: int) -> tuple:
                # Tuples are rejected at the JSON boundary, but can stay in-cluster
                # as a RemoteRef (`$ray_ref`) and be consumed by subsequent actor/task calls!
                return tuple(self.base + i for i in range(n))

            def sum_tuple(self, data: tuple, bonus: int = 0) -> dict:
                return {"count": len(data), "total": sum(data) + bonus}

            def make_json_list(self, n: int) -> list:
                return [self.base * (i + 1) for i in range(n)]

            def stream_chunks(self, count: int):
                for i in range(count):
                    yield {"chunk": i, "val": self.base + i}
                    time.sleep(0.02)

        def call_workspace_actor_from_task(actor_name: str, bonus: int) -> dict:
            import ray

            # `ray.get_actor(actor_name)` automatically resolves in the workspace namespace!
            handle = ray.get_actor(actor_name)
            vals = ray.get(handle.make_json_list.remote(2))
            return {
                "namespace": ray.get_runtime_context().namespace,
                "sum": sum(vals) + bonus,
            }

        assert LIVE_ADDRESS is not None
        ws_client = RayClient(
            address=LIVE_ADDRESS, workspace="test_live_actor_ws", ttl_seconds=60.0
        )
        try:
            worker = ws_client.actor(StatefulWorker, 100, name="worker_1")
            self.assertEqual(worker.name, "worker_1")
            self.assertEqual(worker.workspace, "test_live_actor_ws")

            # Keep non-JSON tuple in-cluster via `.ref()` (`return_ref=True`) and pass to next call
            tuple_ref = worker.make_in_cluster_tuple.ref(4)
            self.assertTrue(tuple_ref.ref_id.startswith("ref_"))
            summary = worker.sum_tuple(tuple_ref, bonus=5)
            self.assertEqual(summary, {"count": 4, "total": 411})

            # Non-blocking actor call via `.remote()` (`wait=False`)
            async_ref = worker.make_json_list.remote(3)
            self.assertEqual(async_ref.get(), [100, 200, 300])
            self.assertTrue(async_ref.delete())

            # Verify a workspace task (`client.run`) can look up `worker_1` via `ray.get_actor("worker_1")`
            # without explicitly passing `namespace=...`!
            cross_check = ws_client.run(call_workspace_actor_from_task, "worker_1", 7)
            self.assertEqual(
                cross_check,
                {"namespace": "workspace:test_live_actor_ws", "sum": 307},
            )

            # Real-time streaming generator method with mid-stream state update
            chunks = []
            for chunk in worker.stream_chunks.stream(3):
                chunks.append(chunk)
                if chunk["chunk"] == 0:
                    # Mutate actor state while generator is paused between yields!
                    worker.set_base(200)
            self.assertEqual(
                chunks,
                [
                    {"chunk": 0, "val": 100},
                    {"chunk": 1, "val": 201},
                    {"chunk": 2, "val": 202},
                ],
            )
        finally:
            ws_client.close_workspace()


if __name__ == "__main__":
    unittest.main()
