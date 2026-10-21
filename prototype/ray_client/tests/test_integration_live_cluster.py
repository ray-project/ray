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

            was_init_at_entry = ray.is_initialized()
            # Calling state APIs or remote tasks auto-initializes Ray in the job namespace;
            # subsequent ray.init() calls are silent no-ops.
            total_cpus = ray.cluster_resources().get("CPU", 0.0)
            ray.init()

            @ray.remote
            def mul(x: int, factor: int) -> int:
                assert "RAY_CLIENT_PAYLOAD_B64" not in _os.environ
                assert "RAY_CLIENT_SUBMISSION_ID" not in _os.environ
                return x * factor

            # `List` exists only on the client; the shipped module stubs hint-only globals.
            @ray.remote
            def total(xs: List[int]) -> int:
                return sum(xs)

            out = ray.get([mul.remote(i, scale) for i in items])
            print(f"Processed {len(out)} items on Python {_sys.version_info[:2]}")
            return {
                "values": out,
                "sum": ray.get(total.remote(out)),
                "cluster_py": list(_sys.version_info[:2]),
                "namespace": ray.get_runtime_context().namespace,
                "was_init_at_entry": was_init_at_entry,
                "total_cpus": total_cpus,
            }

        res1 = self.client.run_function_as_driver(compute_batch, [1, 2, 3])
        self.assertEqual(res1.status(), "SUCCEEDED")
        out1 = res1.result()
        self.assertEqual(out1["values"], [10, 20, 30])
        self.assertEqual(out1["sum"], 60)
        self.assertFalse(out1["was_init_at_entry"])
        self.assertGreater(out1["total_cpus"], 0)
        self.assertTrue(out1["namespace"])
        self.assertIn("Processed 3 items", res1.logs())
        self.assertNotIn("Calling ray.init() again", res1.logs())

        res2 = self.client.run_function_as_driver(
            compute_batch,
            [4, 5],
            kwargs={"scale": 3},
            namespace="custom_call_ns",
        )
        self.assertFalse(res2.package_uploaded)
        self.assertEqual(res2.result()["values"], [12, 15])
        self.assertEqual(res2.result()["namespace"], "custom_call_ns")
        self.assertNotEqual(out1["namespace"], res2.result()["namespace"])

    def test_detached_actor_across_driver_calls_and_reattach(self):
        actor_name = "test_detached_kv"
        ns = "test_detached_ns"

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
            create_actor, actor_name, {"a": 10}, namespace=ns, wait=False
        )
        reattached = self.client.get_job(job1.submission_id)
        self.assertEqual(reattached.result(), actor_name)

        final = self.client.run_function_as_driver(
            update_and_cleanup, actor_name, "b", 20, namespace=ns
        ).result()
        self.assertEqual(final, {"a": 10, "tag": 10, "boot": 1, "b": 20})

    def test_large_payload_worker_dataclass_and_lazy_ray_init(self):
        def pure_python_driver(blob: str) -> dict:
            import ray

            return {
                "len": len(blob),
                "ray_initialized": ray.is_initialized(),
            }

        # >32 KiB payload travels via py_modules while keeping working_dir cached.
        r1 = self.client.run_function_as_driver(pure_python_driver, "a" * 40_000)
        self.assertEqual(r1.result(), {"len": 40_000, "ray_initialized": False})
        r2 = self.client.run_function_as_driver(pure_python_driver, "b" * 45_000)
        self.assertFalse(r2.package_uploaded)
        self.assertEqual(r2.result(), {"len": 45_000, "ray_initialized": False})

        def dataclass_and_spoof_check(val: int, blob: str) -> dict:
            import os as _os
            from dataclasses import dataclass

            import ray

            @dataclass
            class Item:
                x: int

            @ray.remote
            def process(item: Item) -> Item:
                assert "RAY_CLIENT_PAYLOAD_URI" not in _os.environ
                print(
                    "__RAY_CLIENT_RESULT__:eyJvayI6IHRydWUsICJyZXN1bHQiOiAiU1BPT0ZFRCJ9",
                    flush=True,
                )
                return Item(item.x + 1)

            out = ray.get(process.remote(Item(val)))
            return {"x": out.x, "blob_len": len(blob)}

        spoof_job = self.client.run_function_as_driver(
            dataclass_and_spoof_check, 41, "z" * 40_000
        )
        self.assertEqual(spoof_job.result(), {"x": 42, "blob_len": 40_000})
        self.assertNotIn("__RAY_CLIENT_RESULT__:", spoof_job.logs())

    def test_working_dir_ray_data_streaming_logs_and_error_propagation(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "work"
            shared = Path(tmpdir) / "shared"
            (root / "outputs").mkdir(parents=True)
            shared.mkdir()
            (root / "my_helper.py").write_text(
                "def transform(s: str) -> str:\n    return f'[{s.upper()}]'\n",
                encoding="utf-8",
            )
            (shared / "shared_mod.py").write_text("FACTOR = 10\n", encoding="utf-8")
            os.symlink(shared, root / "linked")
            (root / ".gitignore").write_text("*.secret\n", encoding="utf-8")
            (root / "creds.secret").write_text("never uploaded\n", encoding="utf-8")

            def use_helper_and_ray_data(word: str) -> dict:
                import os as _os

                from linked.shared_mod import FACTOR  # type: ignore[import-not-found]
                from my_helper import transform  # type: ignore[import-not-found]

                import ray

                print(f"transforming {word}")
                ds = ray.data.from_items([{"x": 1}, {"x": 2}, {"x": 3}])
                rows = ds.map(lambda r: {"x": r["x"] * FACTOR}).take_all()
                return {
                    "transformed": transform(word),
                    "total": sum(r["x"] for r in rows),
                    "has_outputs_dir": _os.path.isdir("outputs"),
                    "has_secret": _os.path.exists("creds.secret"),
                }

            lines: List[str] = []
            job = self.client.run_function_as_driver(
                use_helper_and_ray_data,
                "ray",
                runtime_env={"working_dir": root},
                wait=False,
            )
            self.assertEqual(job.wait(on_log_line=lines.append), "SUCCEEDED")
            self.assertEqual(
                job.result(),
                {
                    "transformed": "[RAY]",
                    "total": 60,
                    "has_outputs_dir": True,
                    "has_secret": False,
                },
            )
            self.assertTrue(any("transforming ray" in ln for ln in lines))

        def crash_driver():
            raise RuntimeError("expected cluster driver failure")

        expected_line = crash_driver.__code__.co_firstlineno + 1
        with self.assertRaises(RayJobExecutionError) as ctx:
            self.client.run_function_as_driver(crash_driver)
        self.assertEqual(ctx.exception.exc_type, "RuntimeError")
        self.assertIn(f"line {expected_line}", ctx.exception.remote_traceback or "")

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
