"""Live Ray cluster integration tests for ray_client.

Run with:
    RAY_LIVE_CLUSTER_ADDRESS=http://127.0.0.1:8265 pytest prototype/ray_client/tests/
"""

import os
import sys
import tempfile
import unittest
from pathlib import Path
from typing import List

from ray_client import RayClient, RayJobExecutionError

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

    def test_tasks_actors_defaults_and_package_cache_reuse(self):
        def compute_batch(items: List[int], scale: int = GLOBAL_FACTOR) -> dict:
            import os as _os
            import sys as _sys

            import ray

            ray.init()  # Must be a no-op: the runner already initialized Ray.

            @ray.remote
            def mul(x: int, factor: int) -> int:
                assert "RAY_CLIENT_PAYLOAD_B64" not in _os.environ
                return x * factor

            out = ray.get([mul.remote(i, scale) for i in items])
            print(f"Processed {len(out)} items on Python {_sys.version_info[:2]}")
            return {
                "values": out,
                "sum": sum(out),
                "cluster_py": list(_sys.version_info[:2]),
                "namespace": ray.get_runtime_context().namespace,
            }

        res1 = self.client.run_function_as_driver(compute_batch, [1, 2, 3])
        self.assertEqual(res1.status(), "SUCCEEDED")
        out1 = res1.result()
        self.assertEqual(out1["values"], [10, 20, 30])
        self.assertEqual(out1["sum"], 60)
        self.assertEqual(out1["namespace"], self.client.namespace)
        self.assertIn("Processed 3 items", res1.logs())

        res2 = self.client.run_function_as_driver(
            compute_batch, [4, 5], kwargs={"scale": 3}
        )
        self.assertFalse(res2.package_uploaded)
        self.assertEqual(res2.result()["values"], [12, 15])

    def test_detached_actor_across_driver_calls_and_reattach(self):
        actor_name = "test_detached_kv"

        def create_actor(name: str, initial: dict) -> str:
            import ray

            @ray.remote
            class KVStore:
                default_tag = GLOBAL_FACTOR

                def __init__(self, data: dict):
                    self.data = dict(data)
                    self.data["tag"] = self.default_tag

                def put(self, k: str, v: int) -> dict:
                    self.data[k] = v
                    return dict(self.data)

            try:
                ray.kill(ray.get_actor(name))
            except ValueError:
                pass
            store = KVStore.options(name=name, lifetime="detached").remote(initial)
            ray.get(store.put.remote("boot", 1))
            return name

        def update_and_cleanup(name: str, key: str, val: int) -> dict:
            import ray

            store = ray.get_actor(name)
            snap = ray.get(store.put.remote(key, val))
            ray.kill(store)
            return snap

        job1 = self.client.run_function_as_driver(
            create_actor, actor_name, {"a": 10}, wait=False
        )
        reattached = self.client.get_job(job1.submission_id)
        self.assertEqual(reattached.result(), actor_name)

        final = self.client.run_function_as_driver(
            update_and_cleanup, actor_name, "b", 20
        ).result()
        self.assertEqual(final, {"a": 10, "tag": 10, "boot": 1, "b": 20})

    def test_working_dir_ray_data_streaming_logs_and_error_propagation(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            (Path(tmpdir) / "my_helper.py").write_text(
                "def transform(s: str) -> str:\n    return f'[{s.upper()}]'\n",
                encoding="utf-8",
            )

            def use_helper_and_ray_data(word: str) -> dict:
                from my_helper import transform  # type: ignore[import-not-found]

                import ray

                print(f"transforming {word}")
                ds = ray.data.from_items([{"x": 1}, {"x": 2}, {"x": 3}])
                rows = ds.map(lambda r: {"x": r["x"] * 10}).take_all()
                return {
                    "transformed": transform(word),
                    "total": sum(r["x"] for r in rows),
                }

            lines: List[str] = []
            job = self.client.run_function_as_driver(
                use_helper_and_ray_data,
                "ray",
                runtime_env={"working_dir": tmpdir},
                wait=False,
            )
            self.assertEqual(job.wait(on_log_line=lines.append), "SUCCEEDED")
            self.assertEqual(job.result(), {"transformed": "[RAY]", "total": 60})
            self.assertTrue(any("transforming ray" in ln for ln in lines))

        def crash_driver():
            raise RuntimeError("expected cluster driver failure")

        with self.assertRaises(RayJobExecutionError) as ctx:
            self.client.run_function_as_driver(crash_driver)
        self.assertEqual(ctx.exception.exc_type, "RuntimeError")

    def test_examples_run_end_to_end(self):
        examples_dir = Path(__file__).resolve().parents[1] / "examples"
        sys.path.insert(0, str(examples_dir))
        try:
            ex1 = __import__("01_sequential_interactive_workflow")
            ex2 = __import__("02_vibecoded_stateful_actor_client")
            ex3 = __import__("03_vibecoded_multicluster_sharding_client")

            assert LIVE_ADDRESS is not None
            out1 = ex1.run_demo(LIVE_ADDRESS)
            self.assertEqual(out1["count"], 4)

            out2 = ex2.run_demo(LIVE_ADDRESS)
            self.assertEqual(out2, {"v1": 15, "v2": 42})

            out3 = ex3.run_demo([LIVE_ADDRESS, LIVE_ADDRESS])
            self.assertEqual(out3["total_sum_of_squares"], 285)
        finally:
            sys.path.pop(0)


if __name__ == "__main__":
    unittest.main()
