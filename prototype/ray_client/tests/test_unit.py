"""Offline unit and mock-REST-server tests for ray_client."""

import base64
import copy
import functools
import io
import json
import os
import subprocess
import sys
import tempfile
import threading
import unittest
import zipfile
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict
from unittest.mock import patch

from ray_client import (
    JobResult,
    RayClient,
    RayClientError,
    RayClientSerializationError,
    RayJobExecutionError,
    RayJobTimeoutError,
    run_function_as_driver,
)
from ray_client.packaging import (
    RESULT_LINE_PREFIX,
    RUNNER_SCRIPT_CONTENT,
    build_driver_package,
    extract_function_source_and_globals,
    parse_logs_and_result,
)

GLOBAL_SCALE = 42


def top_level_fn(x: int, y: int = GLOBAL_SCALE) -> dict:
    return {"result": (x + y) * GLOBAL_SCALE}


class _MockClusterState:
    def __init__(self, tmp_root: str):
        self.tmp_root = Path(tmp_root)
        self.packages: Dict[str, bytes] = {}
        self.jobs: Dict[str, Dict[str, Any]] = {}
        self.counter = 0
        self.fail_next_status_polls = 0
        self.stopped_jobs: list = []
        self.lock = threading.Lock()


class _MockJobServerHandler(BaseHTTPRequestHandler):
    state: _MockClusterState

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
        return

    def _send_json(self, code: int, obj: Any) -> None:
        data = json.dumps(obj).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/api/jobs/":
            with self.state.lock:
                items = [
                    {
                        "submission_id": sid,
                        "status": info["status"],
                        "message": info.get("message", ""),
                    }
                    for sid, info in self.state.jobs.items()
                ]
            self._send_json(200, items)
            return
        if self.path.startswith("/api/packages/gcs/"):
            pkg_name = self.path.split("/api/packages/gcs/", 1)[1]
            with self.state.lock:
                exists = pkg_name in self.state.packages
            self._send_json(200 if exists else 404, {"exists": exists})
            return
        if self.path.startswith("/api/jobs/") and self.path.endswith("/logs"):
            sub_id = self.path[len("/api/jobs/") : -len("/logs")]
            with self.state.lock:
                job = self.state.jobs.get(sub_id)
            if not job:
                self._send_json(404, {"error": "not found"})
                return
            self._send_json(200, {"logs": job["logs"]})
            return
        if self.path.startswith("/api/jobs/"):
            sub_id = self.path[len("/api/jobs/") :]
            with self.state.lock:
                if self.state.fail_next_status_polls > 0:
                    self.state.fail_next_status_polls -= 1
                    self._send_json(503, {"error": "temporary gateway error"})
                    return
                job = self.state.jobs.get(sub_id)
            if not job:
                self._send_json(404, {"error": "not found"})
                return
            self._send_json(
                200,
                {
                    "submission_id": sub_id,
                    "status": job["status"],
                    "message": job.get("message", ""),
                },
            )
            return
        self._send_json(404, {"error": "unknown endpoint"})

    def do_PUT(self) -> None:  # noqa: N802
        if self.path.startswith("/api/packages/gcs/"):
            pkg_name = self.path.split("/api/packages/gcs/", 1)[1]
            length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(length)
            with self.state.lock:
                self.state.packages[pkg_name] = body
            # Real Ray returns 200 with an empty body here.
            self.send_response(200)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        self._send_json(404, {"error": "unknown endpoint"})

    def do_POST(self) -> None:  # noqa: N802
        if self.path.startswith("/api/jobs/") and self.path.endswith("/stop"):
            sub_id = self.path[len("/api/jobs/") : -len("/stop")]
            with self.state.lock:
                self.state.stopped_jobs.append(sub_id)
                if sub_id in self.state.jobs:
                    self.state.jobs[sub_id]["status"] = "STOPPED"
            self._send_json(200, {"stopped": True})
            return
        if self.path == "/api/jobs/":
            length = int(self.headers.get("Content-Length", "0"))
            req = json.loads(self.rfile.read(length).decode("utf-8"))
            with self.state.lock:
                self.state.counter += 1
                sub_id = req.get("submission_id") or f"raysubmit_{self.state.counter}"
                pkg_uri = req["runtime_env"]["working_dir"]
                pkg_name = pkg_uri.split("gcs://", 1)[1]
                zip_bytes = self.state.packages[pkg_name]
                env_vars = req["runtime_env"].get("env_vars", {})

            work_dir = self.state.tmp_root / sub_id
            work_dir.mkdir(parents=True, exist_ok=True)
            (work_dir / "ray.py").write_text(
                "def init(*a, **kw): return None\n"
                "class cloudpickle:\n"
                "    @staticmethod\n"
                "    def register_pickle_by_value(module): return None\n",
                encoding="utf-8",
            )
            with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
                zf.extractall(work_dir)

            env = os.environ.copy()
            env.update(env_vars)
            proc = subprocess.run(
                [sys.executable, "_ray_client_driver_runner.py"],
                cwd=str(work_dir),
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                check=False,
            )
            status = "SUCCEEDED" if proc.returncode == 0 else "FAILED"
            with self.state.lock:
                self.state.jobs[sub_id] = {
                    "status": status,
                    "logs": proc.stdout,
                    "message": ""
                    if status == "SUCCEEDED"
                    else "entrypoint exited non-zero",
                }
            self._send_json(200, {"submission_id": sub_id})
            return
        self._send_json(404, {"error": "unknown endpoint"})


class TestPackagingAndExtraction(unittest.TestCase):
    def test_extracts_top_level_defaults_and_nested_classes(self):
        fn_name, source, captured = extract_function_source_and_globals(top_level_fn)
        self.assertEqual(fn_name, "top_level_fn")
        self.assertIn("def top_level_fn(", source)
        self.assertEqual(captured, {"GLOBAL_SCALE": 42})

        def fn_only_default(x: int = GLOBAL_SCALE) -> int:
            return x

        _, _, default_captured = extract_function_source_and_globals(fn_only_default)
        self.assertEqual(default_captured, {"GLOBAL_SCALE": 42})

        multiplier = 3

        def nested_with_class(items: list) -> list:
            local_offset = 1

            class LocalHelper:
                scale = GLOBAL_SCALE
                offset = local_offset

                def apply(self, v: int = multiplier) -> int:
                    return (v + self.offset) * self.scale

            h = LocalHelper()
            return [h.apply(x) for x in items]

        _, _, nested_captured = extract_function_source_and_globals(nested_with_class)
        self.assertEqual(nested_captured, {"GLOBAL_SCALE": 42, "multiplier": 3})

        def implicit_ray_driver(n: int) -> int:
            return ray.get(n)  # noqa: F821

        _, _, ray_captured = extract_function_source_and_globals(implicit_ray_driver)
        self.assertEqual(ray_captured, {})

    def test_rejects_decorated_functions_and_generated_module_compiles(self):
        """Decorated functions are rejected (the wrapper's bytecode/closure no longer
        match the shipped source); undecorated output and the runner must compile."""

        def my_deco(f):
            return f

        @my_deco
        def decorated_driver(x: int) -> dict:
            return {"x": x}

        with self.assertRaisesRegex(RayClientSerializationError, "is decorated"):
            extract_function_source_and_globals(decorated_driver)

        def wraps_deco(f):
            @functools.wraps(f)
            def wrapper(*a, **kw):
                return f(*a, **kw)

            return wrapper

        @wraps_deco
        def wrapped_driver(x: int) -> dict:
            return {"x": x}

        with self.assertRaisesRegex(RayClientSerializationError, "is decorated"):
            extract_function_source_and_globals(wrapped_driver)

        _, source, _ = extract_function_source_and_globals(top_level_fn)
        self.assertIn("import ray", source)
        compile(source, "<test>", "exec")
        compile(RUNNER_SCRIPT_CONTENT, "_ray_client_driver_runner.py", "exec")

    def test_actionable_errors_for_module_function_and_non_json_captures(self):
        with self.assertRaises(RayClientSerializationError):
            extract_function_source_and_globals(lambda x: x)

        def uses_top_level_module() -> str:
            return json.dumps({"ok": True})

        with self.assertRaisesRegex(
            RayClientSerializationError, "move 'import json' inside"
        ):
            extract_function_source_and_globals(uses_top_level_module)

        def uses_helper_fn() -> dict:
            return top_level_fn(1)

        with self.assertRaisesRegex(
            RayClientSerializationError, "is a function/class; define 'top_level_fn'"
        ):
            extract_function_source_and_globals(uses_helper_fn)

        lock = threading.Lock()

        def bad_closure():
            return str(lock)

        with self.assertRaisesRegex(RayClientSerializationError, "lock"):
            extract_function_source_and_globals(bad_closure)

        with self.assertRaises(RayClientSerializationError):
            build_driver_package(top_level_fn, (object(),), {})

        with self.assertRaises(RayClientSerializationError):
            build_driver_package(top_level_fn, (float("nan"),), {})

        with self.assertRaises(TypeError):
            build_driver_package(top_level_fn, (), {})

        with self.assertRaises(TypeError):
            build_driver_package(top_level_fn, (1,), {"unknown_kw": 2})

    def test_deterministic_zip_hash_and_large_payload_fallback(self):
        zip_1, uri_1, env_1 = build_driver_package(top_level_fn, (1,), {})
        zip_2, uri_2, env_2 = build_driver_package(top_level_fn, (99,), {})
        self.assertEqual(uri_1, uri_2)
        self.assertEqual(zip_1, zip_2)
        self.assertIsNotNone(env_1)
        self.assertNotEqual(env_1, env_2)

        zip_big, _, env_big = build_driver_package(top_level_fn, ("x" * 50_000,), {})
        self.assertIsNone(env_big)
        with zipfile.ZipFile(io.BytesIO(zip_big)) as zf:
            self.assertIn("_ray_client_payload.json", zf.namelist())

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "keep.py").write_text("X = 1\n", encoding="utf-8")
            (root / "debug.log").write_text("log\n", encoding="utf-8")
            (root / "data").mkdir()
            (root / "data" / "big.bin").write_bytes(b"123")
            zbytes, _, _ = build_driver_package(
                top_level_fn,
                (1,),
                {},
                working_dir=root,
                excludes=["*.log", "data/big.bin"],
            )
            with zipfile.ZipFile(io.BytesIO(zbytes)) as zf:
                names = set(zf.namelist())
                self.assertIn("keep.py", names)
                self.assertNotIn("debug.log", names)
                self.assertNotIn("data/big.bin", names)

    def test_parse_logs_and_result_strips_marker_line(self):
        payload = {"ok": True, "result": {"items": [1, 2, 3]}}
        b64 = base64.b64encode(json.dumps(payload).encode("utf-8")).decode("ascii")
        raw = f"line 1\n{RESULT_LINE_PREFIX}{b64}\nline 2\n"
        clean, has_out, out, err = parse_logs_and_result(raw, final=True)
        self.assertEqual(clean, "line 1\nline 2\n")
        self.assertTrue(has_out)
        self.assertEqual(out, {"items": [1, 2, 3]})
        self.assertIsNone(err)

        # Also handles user stdout written without a trailing newline before the marker.
        raw_partial = f"partial{RESULT_LINE_PREFIX}{b64}\n"
        clean_p, has_out_p, out_p, _ = parse_logs_and_result(raw_partial, final=True)
        self.assertEqual(clean_p, "partial")
        self.assertTrue(has_out_p)
        self.assertEqual(out_p, {"items": [1, 2, 3]})

    def test_auth_headers_and_address_resolution(self):
        with patch.dict(
            os.environ,
            {"RAY_AUTH_TOKEN": "env_secret_token", "RAY_ADDRESS": "10.0.0.5:8265"},
            clear=False,
        ):
            c_default = RayClient()
            self.assertEqual(c_default.address, "http://10.0.0.5:8265")
            self.assertEqual(
                c_default._headers["Authorization"], "Bearer env_secret_token"
            )

            c_auto = RayClient(address="auto")
            self.assertEqual(c_auto.address, "http://10.0.0.5:8265")

            c_custom = RayClient(
                address="127.0.0.1:8265/",
                headers={"authorization": "Bearer custom_lower"},
            )
            self.assertEqual(c_custom.address, "http://127.0.0.1:8265")
            self.assertEqual(c_custom._headers["authorization"], "Bearer custom_lower")
            self.assertNotIn("Authorization", c_custom._headers)


class TestMockJobServerEndToEnd(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.state = _MockClusterState(self._tmp.name)

        class BoundHandler(_MockJobServerHandler):
            pass

        BoundHandler.state = self.state
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), BoundHandler)
        self.port = self.server.server_address[1]
        self.address = f"http://127.0.0.1:{self.port}"
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self._tmp.cleanup()

    def test_sync_async_caching_and_gc_reupload(self):
        client = RayClient(address=self.address)

        def step(x: int) -> dict:
            print(f"running step({x})")
            return {"doubled": x * 2}

        r1 = client.run_function_as_driver(step, 5)
        self.assertTrue(r1.package_uploaded)
        self.assertEqual(r1.status(), "SUCCEEDED")
        self.assertEqual(r1.result(), {"doubled": 10})
        self.assertEqual(r1.logs(), "running step(5)\n")

        r2 = client.run_function_as_driver(step, 21)
        self.assertFalse(r2.package_uploaded)
        self.assertEqual(r2.result(), {"doubled": 42})

        # Simulate cluster GCS package expiration (#46625): client must re-upload cleanly.
        with self.state.lock:
            self.state.packages.clear()
        r3 = client.run_function_as_driver(step, 9)
        self.assertTrue(r3.package_uploaded)
        self.assertEqual(r3.result(), {"doubled": 18})

        async def async_step(msg: str) -> str:
            return msg.upper()

        r4 = run_function_as_driver(async_step, "hello", address=self.address)
        self.assertEqual(r4.result(), "HELLO")

    def test_runtime_env_working_dir_immutability_and_reattach(self):
        client = RayClient(address=self.address)
        with tempfile.TemporaryDirectory() as tmpdir:
            (Path(tmpdir) / "helper_mod.py").write_text("VAL = 123\n", encoding="utf-8")
            (Path(tmpdir) / "big.bin").write_bytes(b"\0" * 1024)
            user_runtime_env = {
                "working_dir": tmpdir,
                "excludes": ["*.bin"],
                "env_vars": {"K": "V"},
            }
            snapshot = copy.deepcopy(user_runtime_env)

            def read_helper() -> int:
                import os as _os

                import helper_mod  # type: ignore[import-not-found]

                assert _os.environ["K"] == "V"
                assert "RAY_CLIENT_PAYLOAD_B64" not in _os.environ
                assert not _os.path.exists("big.bin")
                return helper_mod.VAL

            detached = client.run_function_as_driver(
                read_helper, runtime_env=user_runtime_env, wait=False
            )
            self.assertEqual(user_runtime_env, snapshot)

            reattached = client.get_job(detached.submission_id)
            self.assertEqual(reattached.result(), 123)
            self.assertTrue(
                any(
                    j["submission_id"] == detached.submission_id
                    for j in client.list_jobs()
                )
            )

    def test_keyboard_interrupt_and_sync_timeout_stop_remote_job(self):
        """Ctrl-C during wait() and a timeout on a synchronous call both stop the
        remote job; a timeout on a detached handle leaves it running."""
        client = RayClient(address=self.address)

        def nop() -> int:
            return 1

        job = client.run_function_as_driver(nop, wait=False)
        with patch.object(job, "status", side_effect=KeyboardInterrupt):
            with self.assertRaises(KeyboardInterrupt):
                job.wait()
        self.assertIn(job.submission_id, self.state.stopped_jobs)

        detached = client.run_function_as_driver(nop, wait=False)
        with patch.object(detached, "status", return_value="RUNNING"):
            with self.assertRaises(RayJobTimeoutError) as ctx:
                detached.wait(timeout=0)
        self.assertIs(ctx.exception.job, detached)
        self.assertNotIn(detached.submission_id, self.state.stopped_jobs)

        with patch.object(JobResult, "status", return_value="RUNNING"):
            with self.assertRaises(RayJobTimeoutError) as sync_ctx:
                client.run_function_as_driver(nop, timeout=0)
        self.assertIn(sync_ctx.exception.job.submission_id, self.state.stopped_jobs)

    def test_thread_safe_concurrent_submissions(self):
        client = RayClient(address=self.address)

        def square(n: int) -> int:
            return n * n

        with ThreadPoolExecutor(max_workers=4) as pool:
            futures = [
                pool.submit(
                    lambda v=i: client.run_function_as_driver(square, v).result()
                )
                for i in range(8)
            ]
            results = [f.result() for f in futures]
        self.assertEqual(results, [i * i for i in range(8)])

    def test_remote_exception_and_sys_exit_propagate_cleanly(self):
        client = RayClient(address=self.address)

        def failing_driver():
            raise ValueError("invalid shard configuration")

        with self.assertRaises(RayJobExecutionError) as ctx:
            client.run_function_as_driver(failing_driver)
        self.assertEqual(ctx.exception.exc_type, "ValueError")
        self.assertIn("invalid shard configuration", str(ctx.exception))

        def exiting_driver():
            import sys as _sys

            _sys.exit(2)

        with self.assertRaises(RayJobExecutionError) as exit_ctx:
            client.run_function_as_driver(exiting_driver)
        self.assertEqual(exit_ctx.exception.exc_type, "SystemExit")

    def test_retries_transient_503_and_rejects_legacy_ray_scheme(self):
        with self.assertRaises(RayClientError):
            RayClient(address="ray://127.0.0.1:10001")

        self.state.fail_next_status_polls = 2
        client = RayClient(address=self.address)

        def ok_fn() -> int:
            return 7

        self.assertEqual(client.run_function_as_driver(ok_fn).result(), 7)


if __name__ == "__main__":
    unittest.main()
