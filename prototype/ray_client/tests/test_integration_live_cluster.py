"""Live Ray cluster integration tests for ray_client.

Run with:
    RAY_LIVE_CLUSTER_ADDRESS=http://127.0.0.1:8265 pytest prototype/ray_client/tests/
"""

import math
import os
import sys
import tempfile
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, List

from ray_client import ActorHandle, JobResult, RayCluster, RayJobExecutionError

import ray

LIVE_ADDRESS = os.environ.get("RAY_LIVE_CLUSTER_ADDRESS")

GLOBAL_FACTOR = 10


@unittest.skipUnless(
    LIVE_ADDRESS,
    "Set RAY_LIVE_CLUSTER_ADDRESS=http://127.0.0.1:8265 to run live cluster tests",
)
class TestLiveRayClusterIntegration(unittest.TestCase):
    def setUp(self):
        assert LIVE_ADDRESS is not None
        self.cluster = RayCluster(address=LIVE_ADDRESS)

    def tearDown(self):
        self.cluster.close()

    def test_run_driver_function_tasks_actors_and_fate_sharing(self):
        def compute_batch(items: List[int], scale: int = GLOBAL_FACTOR) -> dict:
            import ray

            total_cpus = ray.cluster_resources().get("CPU", 0.0)
            ray.init()

            @dataclass
            class Item:
                x: int

            @ray.remote
            def mul(item: Item, factor: int) -> Item:
                return Item(item.x * factor)

            @ray.remote
            class ChildWorker:
                def ping(self) -> str:
                    return "alive"

            # Spawn a named child actor owned by this driver actor
            child = ChildWorker.options(name="ephemeral_child_worker").remote()
            assert ray.get(child.ping.remote()) == "alive"

            @ray.remote
            def total(xs: List[int]) -> int:
                return sum(xs)

            items_out = ray.get([mul.remote(Item(i), scale) for i in items])
            out = [it.x for it in items_out]
            return {
                "values": out,
                "sum": ray.get(total.remote(out)),
                "cluster_py": list(sys.version_info[:2]),
                "total_cpus": total_cpus,
            }

        def check_child_exists(name: str) -> bool:
            import ray

            return any(
                a["name"] == name
                for a in ray.util.list_named_actors(all_namespaces=True)
            )

        out1 = self.cluster.run(compute_batch, [1, 2, 3])
        self.assertEqual(out1["values"], [10, 20, 30])
        self.assertEqual(out1["sum"], 60)
        self.assertGreater(out1["total_cpus"], 0)
        # Child actor created inside compute_batch is fate-shared and terminated on exit
        self.assertFalse(self.cluster.run(check_child_exists, "ephemeral_child_worker"))

        out2 = self.cluster.run(compute_batch, [4, 5], kwargs={"scale": 3})
        self.assertEqual(out2["values"], [12, 15])

        # Verify top-level imports (math) and unpassed non-JSON defaults (cb=print)
        # work on the cluster without re-importing inside the function
        def use_top_level_import_and_default(x: int, cb=print) -> dict:
            cb(f"computing isqrt({x})")
            return {"isqrt": math.isqrt(x), "callable": callable(cb)}

        out3 = self.cluster.run(use_top_level_import_and_default, 81)
        self.assertEqual(out3, {"isqrt": 9, "callable": True})

        # Verify @ray.remote class works directly with cluster.actor(...)
        @ray.remote(num_cpus=0.5)
        class DecoratedWorker:
            def __init__(self, base: int = 10):
                self.base = base

            def compute(self, x: int, extra: int = 5) -> int:
                return self.base + x + extra

        worker = self.cluster.actor(DecoratedWorker, 20, name="shared_top_actor")
        try:
            self.assertEqual(worker.compute(7), 32)
            # Verify /api/component_activities reports driver is_active == "ACTIVE"
            # while a client actor is alive on /api/actors
            activities = self.cluster._json("GET", "/api/component_activities")
            self.assertEqual(activities["driver"]["is_active"], "ACTIVE")

            # Verify two concurrent cluster.run() calls can both create a named child
            # actor with the same name ("same_child_name"), look it up via
            # ray.get_actor("same_child_name") without colliding, AND look up the
            # persistent top-level actor via ray.get_actor("shared_top_actor").
            def concurrent_child_and_shared_lookup(tag: str) -> dict:
                import ray

                @ray.remote(num_cpus=0)
                class Child:
                    def __init__(self, label: str):
                        self.label = label

                    def get_label(self) -> str:
                        return self.label

                _ = Child.options(name="same_child_name").remote(tag)
                time.sleep(0.25)
                found_child = ray.get_actor("same_child_name")
                found_shared = ray.get_actor("shared_top_actor")
                shared_res = ray.get(found_shared.invoke.remote("compute", [7], {}))
                return {
                    "child_label": ray.get(found_child.get_label.remote()),
                    "shared_compute": shared_res["result"],
                    "ns": ray.get_runtime_context().namespace,
                }

            with ThreadPoolExecutor(max_workers=2) as pool:
                fut_a = pool.submit(
                    self.cluster.run, concurrent_child_and_shared_lookup, "alpha"
                )
                fut_b = pool.submit(
                    self.cluster.run, concurrent_child_and_shared_lookup, "beta"
                )
                res_a, res_b = fut_a.result(), fut_b.result()

            self.assertEqual(res_a["child_label"], "alpha")
            self.assertEqual(res_b["child_label"], "beta")
            self.assertEqual(res_a["shared_compute"], 32)
            self.assertEqual(res_b["shared_compute"], 32)
            self.assertNotEqual(res_a["ns"], res_b["ns"])
            self.assertFalse(self.cluster.run(check_child_exists, "same_child_name"))

            # Verify a detached child actor created in run() A is placed in "ray_client"
            # and found by run() B via ray.get_actor(name) without specifying namespace.
            def create_detached_child(name: str, val: int) -> str:
                import ray

                @ray.remote(num_cpus=0)
                class DetachedSvc:
                    def __init__(self, v: int):
                        self.v = v

                    def read(self) -> int:
                        return self.v

                svc = DetachedSvc.options(name=name, lifetime="detached").remote(val)
                assert ray.get(svc.read.remote()) == val
                return name

            def read_and_kill_detached_child(name: str) -> int:
                import ray

                svc = ray.get_actor(name)
                try:
                    return ray.get(svc.read.remote())
                finally:
                    ray.kill(svc, no_restart=True)

            self.assertEqual(
                self.cluster.run(create_detached_child, "shared_detached_svc", 77),
                "shared_detached_svc",
            )
            self.assertEqual(
                self.cluster.run(read_and_kill_detached_child, "shared_detached_svc"),
                77,
            )
        finally:
            worker.kill()

    def test_submit_job_cli_script_and_logs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "cli_job.py").write_text(
                "import sys\n"
                "import ray\n"
                "ray.init()\n"
                "@ray.remote\n"
                "def work(x):\n"
                "    return x * 3\n"
                "n = int(sys.argv[1])\n"
                "print(f'CLI_JOB_RESULT={ray.get(work.remote(n))}', flush=True)\n",
                encoding="utf-8",
            )
            py_bin_dir = str(Path(sys.executable).parent)
            job = self.cluster.submit_job(
                "python cli_job.py 14",
                runtime_env={
                    "working_dir": root,
                    "env_vars": {"PATH": f"{py_bin_dir}:{os.environ.get('PATH', '')}"},
                },
                wait=True,
            )
            self.assertIsInstance(job, JobResult)
            self.assertEqual(job.status(), "SUCCEEDED")
            self.assertTrue(job.done())
            self.assertIn("CLI_JOB_RESULT=42", job.logs())

            reattached = self.cluster.get_job(job.submission_id)
            self.assertEqual(reattached.status(), "SUCCEEDED")
            self.assertTrue(
                any(
                    j.get("submission_id") == job.submission_id
                    for j in self.cluster.list_jobs()
                )
            )

    def test_working_dir_ray_data_streaming_and_error_propagation(self):
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
            with RayCluster(
                address=LIVE_ADDRESS,
                default_runtime_env={"working_dir": root},
            ) as wd_cluster:
                res = wd_cluster.run(use_helper_and_ray_data, "ray")
                self.assertEqual(
                    res,
                    {
                        "transformed": "[RAY]",
                        "total": 60,
                        "has_outputs_dir": True,
                        "has_secret": False,
                    },
                )
                self.assertEqual(wd_cluster.run("my_helper:transform", "ray"), "[RAY]")
                hc = wd_cluster.actor("my_helper:HelperCounter", 10, name="hc")
                self.assertEqual(hc.inc(5), 15)
                hc.kill()

        def stream_progress(steps: int) -> Iterator[dict]:
            for i in range(steps):
                yield {"step": i, "pct": int((i + 1) * 100 / steps)}

        events = list(self.cluster.run(stream_progress, 4))
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
            print("about to crash inside driver actor", flush=True)
            raise RuntimeError("expected cluster task failure")

        with self.assertRaises(RayJobExecutionError) as ctx:
            self.cluster.run(crash_task)
        self.assertEqual(ctx.exception.exc_type, "RuntimeError")
        self.assertIn("expected cluster task failure", str(ctx.exception))
        self.assertIn("about to crash inside driver actor", ctx.exception.logs)

    def test_stateful_actor_steering_early_break_cancel_logs_and_idle_ttl(self):
        class Trainer:
            def __init__(self, lr: float = 1e-3):
                self.lr = lr
                self.steps_executed = 0

            def update_lr(self, lr: float) -> dict:
                self.lr = lr
                print(f"updated lr to {self.lr}", flush=True)
                return {"lr": self.lr}

            def get_state(self) -> dict:
                return {"lr": self.lr, "steps_executed": self.steps_executed}

            def slow_call(self, duration: float) -> str:
                time.sleep(duration)
                return "done"

            def train_steps(self, steps: int):
                for step in range(steps):
                    self.steps_executed += 1
                    yield {
                        "step": step,
                        "loss": round(1.0 / (step + 1), 4),
                        "lr": self.lr,
                    }
                    time.sleep(0.12)

        trainer = self.cluster.actor(
            Trainer,
            kwargs={"lr": 1e-3},
            name="live_trainer",
            ttl_seconds=60.0,
            get_if_exists=False,
        )
        self.assertIsInstance(trainer, ActorHandle)
        try:
            # 1. Concurrent mid-run steering + early-break SSE cancellation
            seen = []
            for metrics in trainer.train_steps(50):
                seen.append(metrics)
                if metrics["step"] == 0:
                    trainer.update_lr(1e-4)
                if metrics["step"] >= 2 and any(m["lr"] == 1e-4 for m in seen):
                    break

            self.assertGreaterEqual(len(seen), 3)
            self.assertEqual(seen[0]["lr"], 1e-3)
            self.assertTrue(any(m["lr"] == 1e-4 for m in seen[1:]))

            # Verify generator was cancelled on the cluster rather than running all 50 steps
            time.sleep(0.25)
            state_after = trainer.get_state()
            self.assertLess(state_after["steps_executed"], 15)
            time.sleep(0.2)
            self.assertEqual(
                trainer.get_state()["steps_executed"], state_after["steps_executed"]
            )
            self.assertIn("updated lr to 0.0001", trainer.logs())
        finally:
            trainer.kill()

        # 2. Verify ttl_seconds does not kill actor while a call is active (active_calls > 0)
        busy_actor = self.cluster.actor(
            Trainer,
            name="busy_ttl_actor",
            ttl_seconds=0.6,
            get_if_exists=False,
        )
        try:
            # Run a 1.0s call (> 0.6s TTL): must succeed without being reaped mid-call
            self.assertEqual(busy_actor.slow_call(1.0), "done")
            # Once idle for > 0.6s, actor is automatically reaped
            time.sleep(0.9)
            active_names = [a["name"] for a in self.cluster.list_actors()]
            self.assertNotIn("busy_ttl_actor", active_names)
        finally:
            busy_actor.kill()

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


if __name__ == "__main__":
    unittest.main()
