"""Offline unit and mock REST server tests for ray_client."""

import copy
import functools
import inspect
import io
import json
import os
import pickle
import sys
import tempfile
import threading
import time
import traceback
import types
import unittest
import urllib.parse
import uuid
import zipfile
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple
from unittest.mock import patch

from ray_client import (
    ActorHandle,
    JobResult,
    RayClient,
    RayClientError,
    RayClientSerializationError,
    RayCluster,
    RayJobExecutionError,
    RayJobTimeoutError,
    packaging,
    run,
)
from ray_client._ignore import _compile_ignore_rules, _is_ignored
from ray_client.packaging import (
    IGNORE_GITIGNORE_ENV_VAR,
    build_driver_package,
    build_working_dir_package,
    extract_class_source_and_globals,
    extract_function_source_and_globals,
)

GLOBAL_SCALE = 42


def top_level_fn(x: int, y: int = GLOBAL_SCALE) -> dict:
    return {"result": (x + y) * GLOBAL_SCALE}


class _MockClusterState:
    def __init__(self, tmp_root: str):
        self.tmp_root = Path(tmp_root)
        self.packages: Dict[str, bytes] = {}
        self.actors: Dict[str, Dict[str, Any]] = {}
        self.jobs: Dict[str, Dict[str, Any]] = {}
        self.last_created_actor_spec: Optional[Dict[str, Any]] = None
        self.fail_next_requests = 0
        self.drop_next_job_post_response = False
        self.job_info_gets = 0
        self.job_logs_gets = 0
        self.lock = threading.Lock()

    def check_ttls_locked(self) -> None:
        now = time.monotonic()
        for name, act in list(self.actors.items()):
            ttl = act.get("ttl_seconds")
            if (
                ttl is not None
                and ttl > 0
                and act.get("active_calls", 0) == 0
                and (now - act["last_active"]) >= ttl
            ):
                self.actors.pop(name, None)


class _MockJobServerHandler(BaseHTTPRequestHandler):
    state: _MockClusterState

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
        return

    def _send_json(self, code: int, obj: Any, *, keepalive_prefix: bytes = b"") -> None:
        data = keepalive_prefix + json.dumps(obj).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _send_sse_stream(self, items: Any, logs_buf: List[str]) -> None:
        frames = [": ping\n\n"]
        try:
            for item in items:
                frames.append(
                    "data: "
                    + json.dumps({"ok": True, "item": item}, allow_nan=False)
                    + "\n\n"
                )
        except Exception as exc:
            frames.append(
                "data: "
                + json.dumps(
                    {
                        "ok": False,
                        "error": {
                            "exc_type": type(exc).__name__,
                            "exc_message": str(exc),
                            "traceback": traceback.format_exc(),
                        },
                        "logs": "".join(logs_buf),
                    }
                )
                + "\n\n"
            )
        payload = "".join(frames).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _read_json(self) -> Any:
        length = int(self.headers.get("Content-Length", "0"))
        if length <= 0:
            return {}
        return json.loads(self.rfile.read(length).decode("utf-8"))

    def _unpack_actor_runtime_env_locked(
        self, actor_name: str, runtime_env: Optional[Dict[str, Any]]
    ) -> Tuple[Optional[Path], Dict[str, str]]:
        if not runtime_env:
            return None, {}
        act_dir = self.state.tmp_root / f"actor_{actor_name}"
        act_dir.mkdir(parents=True, exist_ok=True)
        wd_uri = runtime_env.get("working_dir")
        if wd_uri and str(wd_uri).startswith("gcs://"):
            pkg_name = str(wd_uri).split("gcs://", 1)[1]
            if pkg_name not in self.state.packages:
                raise KeyError(f"Package '{pkg_name}' not uploaded")
            with zipfile.ZipFile(io.BytesIO(self.state.packages[pkg_name])) as zf:
                zf.extractall(act_dir)
        for mod_uri in runtime_env.get("py_modules") or ():
            if str(mod_uri).startswith("gcs://"):
                pkg_name = str(mod_uri).split("gcs://", 1)[1]
                if pkg_name in self.state.packages:
                    with zipfile.ZipFile(
                        io.BytesIO(self.state.packages[pkg_name])
                    ) as zf:
                        zf.extractall(act_dir)
        env_vars = {
            str(k): str(v) for k, v in (runtime_env.get("env_vars") or {}).items()
        }
        return act_dir, env_vars

    def _exec_in_actor_env(
        self, act: Dict[str, Any], fn: Any, args: list, kwargs: dict
    ) -> Any:
        work_dir: Optional[Path] = act.get("work_dir")
        env_vars: Dict[str, str] = act.get("env_vars") or {}
        prev_cwd = os.getcwd()
        prev_path = list(sys.path)
        prev_env = {k: os.environ.get(k) for k in env_vars}
        try:
            if work_dir is not None:
                os.chdir(work_dir)
                sys.path.insert(0, str(work_dir))
            for k, v in env_vars.items():
                os.environ[k] = str(v)
            out = fn(*args, **kwargs)
            if inspect.iscoroutine(out):
                import asyncio

                out = asyncio.run(out)
            return out
        finally:
            os.chdir(prev_cwd)
            sys.path[:] = prev_path
            for k, old_v in prev_env.items():
                if old_v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = old_v

    def do_GET(self) -> None:  # noqa: N802
        parsed = urllib.parse.urlsplit(self.path)
        path = parsed.path

        with self.state.lock:
            if self.state.fail_next_requests > 0:
                self.state.fail_next_requests -= 1
                self._send_json(503, {"error": "temporary gateway error"})
                return

        if path.startswith("/api/packages/gcs/"):
            pkg_name = path.split("/api/packages/gcs/", 1)[1]
            with self.state.lock:
                exists = pkg_name in self.state.packages
            self._send_json(200 if exists else 404, {"exists": exists})
            return

        if path == "/api/jobs/":
            with self.state.lock:
                items = list(self.state.jobs.values())
            self._send_json(200, items)
            return

        if path.startswith("/api/jobs/"):
            parts = path[len("/api/jobs/") :].split("/")
            job_id = parts[0]
            with self.state.lock:
                job = self.state.jobs.get(job_id)
                if job is None:
                    self._send_json(404, {"error": f"Job {job_id} does not exist"})
                    return
                if len(parts) == 1:
                    self.state.job_info_gets += 1
                    if job.get("transitions"):
                        job["status"] = job["transitions"].pop(0)
                    self._send_json(
                        200,
                        {
                            "submission_id": job["submission_id"],
                            "status": job["status"],
                            "entrypoint": job["entrypoint"],
                            "message": job.get("message", ""),
                            "metadata": job.get("metadata") or {},
                        },
                    )
                    return
                if len(parts) == 2 and parts[1] == "logs":
                    self.state.job_logs_gets += 1
                    if job.get("log_chunks"):
                        job["logs"] += job["log_chunks"].pop(0)
                    self._send_json(200, {"logs": job.get("logs", "")})
                    return

        if path == "/api/actors":
            with self.state.lock:
                self.state.check_ttls_locked()
                items = [
                    {
                        "name": name,
                        "class_name": act["class_name"],
                        "status": "ALIVE",
                        "active_calls": act.get("active_calls", 0),
                        "ttl_seconds": act["ttl_seconds"],
                    }
                    for name, act in self.state.actors.items()
                ]
            self._send_json(200, items)
            return

        if path.startswith("/api/actors/"):
            parts = path[len("/api/actors/") :].split("/")
            actor_name = parts[0]
            with self.state.lock:
                self.state.check_ttls_locked()
                act = self.state.actors.get(actor_name)
                if act is None:
                    self._send_json(404, {"ok": False, "error": "actor not found"})
                    return
                act["last_active"] = time.monotonic()
                if len(parts) == 1:
                    self._send_json(
                        200,
                        {
                            "ok": True,
                            "name": actor_name,
                            "class_name": act["class_name"],
                            "status": "ALIVE",
                            "active_calls": act.get("active_calls", 0),
                            "ttl_seconds": act["ttl_seconds"],
                        },
                    )
                    return
                if len(parts) == 2 and parts[1] == "logs":
                    self._send_json(
                        200,
                        {
                            "ok": True,
                            "name": actor_name,
                            "logs": "".join(act["logs"]),
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
            self.send_response(200)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        self._send_json(404, {"error": "unknown endpoint"})

    def do_DELETE(self) -> None:  # noqa: N802
        parsed = urllib.parse.urlsplit(self.path)
        path = parsed.path
        if path.startswith("/api/jobs/"):
            job_id = path[len("/api/jobs/") :]
            with self.state.lock:
                existed = self.state.jobs.pop(job_id, None) is not None
            self._send_json(200 if existed else 404, {"deleted": existed})
            return
        if path.startswith("/api/actors/"):
            actor_name = path[len("/api/actors/") :]
            with self.state.lock:
                self.state.check_ttls_locked()
                existed = self.state.actors.pop(actor_name, None) is not None
            self._send_json(200 if existed else 404, {"ok": existed, "killed": existed})
            return
        self._send_json(404, {"error": "unknown endpoint"})

    def do_POST(self) -> None:  # noqa: N802
        parsed = urllib.parse.urlsplit(self.path)
        path = parsed.path
        query = dict(urllib.parse.parse_qsl(parsed.query))
        body = self._read_json()

        if path == "/api/jobs/":
            sub_id = body.get("submission_id") or f"raysubmit_{uuid.uuid4().hex[:12]}"
            with self.state.lock:
                self.state.jobs[sub_id] = {
                    "submission_id": sub_id,
                    "entrypoint": body["entrypoint"],
                    "status": "SUCCEEDED",
                    "logs": f"ran: {body['entrypoint']}\n",
                    "metadata": body.get("metadata") or {},
                    "runtime_env": body.get("runtime_env") or {},
                }
                if self.state.drop_next_job_post_response:
                    self.state.drop_next_job_post_response = False
                    self._send_json(503, {"error": "simulated dropped response"})
                    return
            self._send_json(200, {"job_id": sub_id, "submission_id": sub_id})
            return

        if path.startswith("/api/jobs/") and path.endswith("/stop"):
            job_id = path[len("/api/jobs/") : -len("/stop")]
            with self.state.lock:
                job = self.state.jobs.get(job_id)
                if job is None:
                    self._send_json(404, {"stopped": False})
                    return
                job["status"] = "STOPPED"
            self._send_json(200, {"stopped": True})
            return

        if path == "/api/actors":
            kind = body.get("kind") or ("function" if "fn_name" in body else "class")
            target_name = (
                body.get("fn_name")
                if kind == "function"
                else (body.get("class_name") or body.get("name"))
            )
            name = body.get("name") or f"{target_name}_{uuid.uuid4().hex[:8]}"
            with self.state.lock:
                self.state.last_created_actor_spec = dict(body)
                self.state.check_ttls_locked()
                if body.get("get_if_exists", True) and name in self.state.actors:
                    act = self.state.actors[name]
                    act["last_active"] = time.monotonic()
                    self._send_json(
                        200,
                        {
                            "ok": True,
                            "name": name,
                            "class_name": act["class_name"],
                            "created": False,
                        },
                    )
                    return

                try:
                    work_dir, env_vars = self._unpack_actor_runtime_env_locked(
                        name, body.get("runtime_env")
                    )
                    ephemeral = bool(body.get("ephemeral", kind == "function"))
                    act_entry: Dict[str, Any] = {
                        "name": name,
                        "kind": kind,
                        "ephemeral": ephemeral,
                        "class_name": target_name,
                        "ttl_seconds": body.get("ttl_seconds", 1800.0),
                        "last_active": time.monotonic(),
                        "active_calls": 0,
                        "work_dir": work_dir,
                        "env_vars": env_vars,
                        "logs": [],
                    }
                    captured_globals = body.get("captured_globals") or {}
                    init_args = body.get("args") or []
                    init_kwargs = body.get("kwargs") or {}

                    if body.get("module_source") is not None:
                        mod_name = f"_mock_actor_{name}_{uuid.uuid4().hex[:6]}"
                        mod = types.ModuleType(mod_name)
                        sys.modules[mod_name] = mod
                        mod.__dict__.update(captured_globals)
                        src = body["module_source"].replace("import ray", "pass")
                        exec(compile(src, "<actor>", "exec"), mod.__dict__)
                        target_obj = getattr(mod, target_name)
                    else:
                        import importlib

                        def _load_from_mod() -> Any:
                            importlib.invalidate_caches()
                            m = importlib.import_module(body["module"])
                            return getattr(m, target_name)

                        target_obj = self._exec_in_actor_env(
                            act_entry, _load_from_mod, [], {}
                        )

                    if kind == "function":
                        act_entry["fn"] = target_obj
                        act_entry["instance"] = None
                    else:
                        act_entry["fn"] = None
                        act_entry["instance"] = self._exec_in_actor_env(
                            act_entry, target_obj, init_args, init_kwargs
                        )
                    self.state.actors[name] = act_entry
                    self._send_json(
                        200,
                        {
                            "ok": True,
                            "name": name,
                            "class_name": target_name,
                            "created": True,
                        },
                    )
                except Exception as exc:
                    self._send_json(
                        400,
                        {
                            "ok": False,
                            "error": {
                                "exc_type": type(exc).__name__,
                                "exc_message": str(exc),
                                "traceback": traceback.format_exc(),
                            },
                        },
                    )
                return

        if path.startswith("/api/actors/"):
            parts = path[len("/api/actors/") :].split("/")
            if len(parts) == 3 and parts[1] == "methods":
                actor_name, method_name = parts[0], parts[2]
                is_stream = query.get("stream") in ("1", "true", "True") or bool(
                    body.get("stream", False)
                )
                with self.state.lock:
                    self.state.check_ttls_locked()
                    act = self.state.actors.get(actor_name)
                    if act is None:
                        self._send_json(
                            404,
                            {
                                "ok": False,
                                "error": {
                                    "exc_type": "KeyError",
                                    "exc_message": f"Actor '{actor_name}' not found",
                                },
                            },
                        )
                        return
                    act["active_calls"] = act.get("active_calls", 0) + 1
                    act["last_active"] = time.monotonic()

                try:
                    args = body.get("args") or []
                    kwargs = body.get("kwargs") or {}

                    if act["kind"] == "function":
                        target_callable = act["fn"]
                    else:
                        target_callable = getattr(act["instance"], method_name)

                    out = self._exec_in_actor_env(act, target_callable, args, kwargs)
                    if is_stream or inspect.isgenerator(out):
                        self._send_sse_stream(out, act["logs"])
                        return
                    self._send_json(
                        200,
                        {"ok": True, "result": out},
                        keepalive_prefix=b"   ",
                    )
                except SystemExit as exc:
                    code = exc.code
                    if code in (None, 0):
                        self._send_json(
                            200,
                            {"ok": True, "result": None},
                            keepalive_prefix=b"   ",
                        )
                    else:
                        self._send_json(
                            200,
                            {
                                "ok": False,
                                "error": {
                                    "exc_type": "SystemExit",
                                    "exc_message": f"Process called sys.exit({code!r})",
                                    "traceback": f"SystemExit: {code}\n",
                                },
                                "logs": "".join(act["logs"]),
                            },
                            keepalive_prefix=b"   ",
                        )
                except Exception as exc:
                    self._send_json(
                        200,
                        {
                            "ok": False,
                            "error": {
                                "exc_type": type(exc).__name__,
                                "exc_message": str(exc),
                                "traceback": traceback.format_exc(),
                            },
                            "logs": "".join(act["logs"]),
                        },
                        keepalive_prefix=b"   ",
                    )
                finally:
                    with self.state.lock:
                        act["active_calls"] = max(0, act.get("active_calls", 1) - 1)
                        act["last_active"] = time.monotonic()
                        if act.get("ephemeral") or body.get("ephemeral"):
                            self.state.actors.pop(actor_name, None)
                return

        self._send_json(404, {"error": "unknown endpoint"})


class _HtmlLoginPageHandler(BaseHTTPRequestHandler):
    """Answers every GET with an HTML page, like an ingress login redirect."""

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
        return

    def do_GET(self) -> None:  # noqa: N802
        body = b"<html><body>Sign in</body></html>"
        self.send_response(200)
        self.send_header("Content-Type", "text/html")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


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

        def implicit_ray_task(n: int) -> int:
            return ray.get(n)  # noqa: F821

        _, _, ray_captured = extract_function_source_and_globals(implicit_ray_task)
        self.assertEqual(ray_captured, {})

    def test_rejects_decorated_functions_and_generated_module_compiles(self):
        def my_deco(f):
            return f

        @my_deco
        def decorated_task(x: int) -> dict:
            return {"x": x}

        with self.assertRaisesRegex(RayClientSerializationError, "is decorated"):
            extract_function_source_and_globals(decorated_task)

        def wraps_deco(f):
            @functools.wraps(f)
            def wrapper(*a, **kw):
                return f(*a, **kw)

            return wrapper

        @wraps_deco
        def wrapped_task(x: int) -> dict:
            return {"x": x}

        with self.assertRaisesRegex(RayClientSerializationError, "is decorated"):
            extract_function_source_and_globals(wrapped_task)

        _, source, _ = extract_function_source_and_globals(top_level_fn)
        self.assertIn("import ray", source)
        compile(source, "<test>", "exec")

    def test_actionable_errors_for_module_function_and_non_json_captures(self):
        with self.assertRaises(RayClientSerializationError):
            extract_function_source_and_globals(lambda x: x)

        def uses_top_level_imports() -> str:
            return json.dumps({"ok": True, "cwd": Path(".").name})

        _, mod_src, mod_captured = extract_function_source_and_globals(
            uses_top_level_imports
        )
        self.assertIn("import json", mod_src)
        self.assertIn("from pathlib import Path", mod_src)
        self.assertEqual(mod_captured, {})

        main_mod = sys.modules["__main__"]

        def uses_unimportable_main_module() -> str:
            return str(main_mod)

        with self.assertRaisesRegex(
            RayClientSerializationError, "is a module that cannot be imported"
        ):
            extract_function_source_and_globals(uses_unimportable_main_module)

        def uses_helper_fn() -> dict:
            return top_level_fn(1)

        with self.assertRaisesRegex(
            RayClientSerializationError, "other callable; define 'top_level_fn'"
        ):
            extract_function_source_and_globals(uses_helper_fn)

        scaled = functools.partial(top_level_fn, 1)

        def uses_partial() -> dict:
            return scaled()

        with self.assertRaisesRegex(RayClientSerializationError, "other callable"):
            extract_function_source_and_globals(uses_partial)

        lock = threading.Lock()

        def bad_closure():
            return str(lock)

        with self.assertRaisesRegex(RayClientSerializationError, "lock"):
            extract_function_source_and_globals(bad_closure)

        def uses_unbound_closure() -> int:
            return assigned_later

        with self.assertRaisesRegex(RayClientSerializationError, "not assigned yet"):
            extract_function_source_and_globals(uses_unbound_closure)
        assigned_later = 1

        with self.assertRaisesRegex(RayClientError, "is a local path"):
            RayCluster(address="http://127.0.0.1:1")._prepare_runtime_env(
                {"py_modules": ["./local_pkg"]}
            )

        merged, _, excludes = RayCluster(
            address="http://127.0.0.1:1", default_runtime_env={"excludes": ("*.log",)}
        )._prepare_runtime_env({"excludes": ["*.bin"]})
        self.assertEqual(excludes, ["*.log", "*.bin"])
        self.assertNotIn("excludes", merged)

        with self.assertRaisesRegex(TypeError, "excludes.* must be a list"):
            RayCluster(address="http://127.0.0.1:1")._prepare_runtime_env(
                {"excludes": "*.log"}
            )

    def test_default_argument_from_enclosing_scope_fails_locally(self):
        def make_task():
            scale = 3

            def task(x: int, factor: int = scale) -> int:
                return x * factor

            return task

        class Config:
            FACTOR = 2

            def run(self, x: int, factor: int = FACTOR) -> int:
                return x * factor

        for fn in (make_task(), Config.run):
            with self.assertRaisesRegex(
                RayClientSerializationError, "Default argument .* enclosing"
            ):
                extract_function_source_and_globals(fn)

        def builtin_default(x: int, cb=print) -> bool:
            return cb is print

        self.assertEqual(extract_function_source_and_globals(builtin_default)[2], {})

    def test_nested_annotations_recursion_and_class_bodies(self):
        def nested_hints(items: List[int]) -> int:
            def total(xs: List[int]) -> Optional[int]:
                return sum(xs)

            class Acc:
                def add(self, xs: Dict[str, int]) -> int:
                    return len(xs)

            return total(items) + Acc().add({})

        self.assertEqual(extract_function_source_and_globals(nested_hints)[2], {})

        def make_recursive():
            def fact(n: int) -> int:
                return 1 if n <= 1 else n * fact(n - 1)

            return fact

        self.assertEqual(extract_function_source_and_globals(make_recursive())[2], {})

        def rebinding_class_body() -> int:
            class Config:
                GLOBAL_SCALE = GLOBAL_SCALE + 1

            return Config.GLOBAL_SCALE

        _, _, captured = extract_function_source_and_globals(rebinding_class_body)
        self.assertEqual(captured, {"GLOBAL_SCALE": 42})

    def test_indented_source_ships_verbatim(self):
        class Task:
            def query(self) -> str:
                sql = """
                SELECT *
                  FROM t
                """
                flush = """left
col0
"""
                return sql + flush

        expected = Task().query()
        fn_name, source, _ = extract_function_source_and_globals(Task.query)
        self.assertTrue(source.endswith(f"if True:\n{inspect.getsource(Task.query)}"))
        namespace: Dict[str, Any] = {}
        exec(compile(source.replace("import ray", "pass"), "<src>", "exec"), namespace)
        self.assertEqual(namespace[fn_name](None), expected)

    def test_deterministic_working_dir_zip_and_build_driver_package(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "keep.py").write_text("X = 1\n", encoding="utf-8")
            (root / "run.sh").write_text("#!/bin/sh\necho ok\n", encoding="utf-8")
            (root / "run.sh").chmod(0o755)
            (root / "debug.log").write_text("log\n", encoding="utf-8")
            (root / "data").mkdir()
            (root / "data" / "big.bin").write_bytes(b"123")
            zbytes_1, uri_1 = build_working_dir_package(
                root, excludes=["*.log", "data/big.bin"]
            )
            zbytes_2, uri_2 = build_working_dir_package(
                root, excludes=["*.log", "data/big.bin"]
            )
            self.assertEqual(uri_1, uri_2)
            self.assertEqual(zbytes_1, zbytes_2)
            with zipfile.ZipFile(io.BytesIO(zbytes_1)) as zf:
                names = set(zf.namelist())
                self.assertIn("keep.py", names)
                self.assertIn("run.sh", names)
                self.assertNotIn("debug.log", names)
                self.assertNotIn("data/big.bin", names)
                self.assertEqual(
                    (zf.getinfo("run.sh").external_attr >> 16) & 0o777, 0o755
                )
                self.assertEqual(
                    (zf.getinfo("keep.py").external_attr >> 16) & 0o777, 0o644
                )
                self.assertEqual(zf.getinfo("run.sh").create_system, 3)

            drv_bytes, drv_uri, target_name, _, captured = build_driver_package(
                top_level_fn, working_dir=root, excludes=["*.log", "data/big.bin"]
            )
            self.assertTrue(drv_uri.startswith("gcs://_ray_pkg_"))
            self.assertEqual(target_name, "top_level_fn")
            self.assertEqual(captured, {"GLOBAL_SCALE": 42})
            with zipfile.ZipFile(io.BytesIO(drv_bytes)) as zf:
                self.assertIn("_ray_client_user_fn.py", zf.namelist())
                self.assertIn("keep.py", zf.namelist())

    def test_generator_allowance_shadowed_defaults_and_conditional_globals(self):
        def gen_fn():
            yield 1

        with self.assertRaisesRegex(RayClientSerializationError, "generator function"):
            extract_function_source_and_globals(gen_fn, allow_generator=False)

        fn_name, src, _ = extract_function_source_and_globals(
            gen_fn, allow_generator=True
        )
        self.assertEqual(fn_name, "gen_fn")
        self.assertIn("yield 1", src)

        class ShadowedConfig:
            GLOBAL_SCALE = 999

            def run(self, x: int, factor: int = GLOBAL_SCALE) -> int:
                return x * factor

        with self.assertRaisesRegex(
            RayClientSerializationError, "Default argument .* enclosing"
        ):
            extract_function_source_and_globals(ShadowedConfig.run)

        def conditional_global(override: dict) -> int:
            global GLOBAL_SCALE
            if "scale" in override:
                GLOBAL_SCALE = override["scale"]
            return GLOBAL_SCALE

        _, _, cond_captured = extract_function_source_and_globals(conditional_global)
        self.assertEqual(cond_captured, {"GLOBAL_SCALE": 42})

    def test_gitignore_pattern_semantics(self):
        cases = [
            (
                ["foo/**/bar", "abc/**", "**/node_modules/"],
                [
                    ("foo/bar", False, True),
                    ("foo/x/y/bar", False, True),
                    ("bar", False, False),
                    ("abc", True, False),
                    ("abc/e/f", False, True),
                    ("a/node_modules", True, True),
                    ("node_modules", False, False),
                ],
            ),
            (
                ["/top", "sub/x", "q/a?", "cls/[!a-c]*", "[Bb]uild/"],
                [
                    ("top", True, True),
                    ("a/top", True, False),
                    ("sub/x", False, True),
                    ("a/sub/x", False, False),
                    ("q/ab", False, True),
                    ("q/abc", False, False),
                    ("cls/d1", False, True),
                    ("cls/a1", False, False),
                    ("Build", True, True),
                    ("build", False, False),
                ],
            ),
            (
                ["*.log", "!keep.log", "\\#lit", "trail  ", "[x", "a\\*b"],
                [
                    ("x/app.log", False, True),
                    ("x/keep.log", False, False),
                    ("#lit", False, True),
                    ("trail", False, True),
                    ("trail  ", False, False),
                    ("[x", False, False),
                    ("a*b", False, True),
                    ("ab", False, False),
                ],
            ),
        ]
        for patterns, expectations in cases:
            rules = _compile_ignore_rules(patterns, "")
            for arcname, is_dir, expected in expectations:
                with self.subTest(patterns=patterns, arcname=arcname, is_dir=is_dir):
                    self.assertEqual(_is_ignored(arcname, is_dir, rules), expected)

    def test_working_dir_ignore_files_symlinks_and_empty_dirs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "work"
            outside = Path(tmpdir) / "outside"
            files = {
                ".gitignore": "*.log\nbuild/\n/top_only\n!keep.log\ndata/**/*.bin\n"
                "# comment\n\\#literal\ntrail  \n",
                ".rayignore": "!important.log\nsecrets/\n",
                "sub/.gitignore": "\ufeff*.txt\n!notes.txt\n",
                "keep.log": "",
                "app.log": "",
                "important.log": "",
                "build/out.txt": "",
                "sub/build": "",
                "top_only/x.py": "",
                "sub/top_only/y.py": "",
                "#literal": "",
                "trail": "",
                "data/raw/f.bin": "",
                "data/f.bin": "",
                "data/f.txt": "",
                "secrets/key": "",
                "sub/a.txt": "",
                "sub/notes.txt": "",
                "c.txt": "",
                "README.md": "",
                "docs/README.md": "",
                ".venv/lib/x.py": "",
                "__pycache__/m.pyc": "",
            }
            for name, content in files.items():
                (root / name).parent.mkdir(parents=True, exist_ok=True)
                (root / name).write_text(content, encoding="utf-8")
            (root / "empty_dir").mkdir()
            outside.mkdir()
            (outside / "mod.py").write_text("VAL = 1\n", encoding="utf-8")
            os.symlink(outside, root / "linked")

            def names_for(**env: str):
                with patch.dict(os.environ, env, clear=False):
                    zbytes, _ = build_working_dir_package(
                        root, excludes=["*.md", "!/README.md"]
                    )
                with zipfile.ZipFile(io.BytesIO(zbytes)) as zf:
                    self.assertTrue(zf.getinfo("empty_dir/").is_dir())
                    return zbytes, set(zf.namelist())

            zbytes, names = names_for()
            self.assertEqual(
                names,
                {
                    ".gitignore",
                    ".rayignore",
                    "README.md",
                    "c.txt",
                    "data/f.txt",
                    "empty_dir/",
                    "important.log",
                    "keep.log",
                    "linked/mod.py",
                    "sub/.gitignore",
                    "sub/build",
                    "sub/notes.txt",
                    "sub/top_only/y.py",
                },
            )
            self.assertEqual(zbytes, names_for()[0])

            _, no_gitignore = names_for(**{IGNORE_GITIGNORE_ENV_VAR: "1"})
            self.assertTrue({"app.log", "build/out.txt", "sub/a.txt"} <= no_gitignore)
            self.assertFalse(
                {"secrets/key", "docs/README.md", ".venv/lib/x.py"} & no_gitignore
            )

            os.symlink(root, root / "loop")
            with self.assertRaisesRegex(
                RayClientError, "'loop' points to its own ancestor"
            ):
                build_working_dir_package(root)
            zbytes, _ = build_working_dir_package(root, excludes=["loop"])
            with zipfile.ZipFile(io.BytesIO(zbytes)) as zf:
                self.assertIn("linked/mod.py", zf.namelist())

    def test_package_size_limit_fails_before_upload_and_names_largest_entries(self):
        with tempfile.TemporaryDirectory() as tmpdir, patch.object(
            packaging, "MAX_PACKAGE_BYTES", 256 * 1024
        ):
            root = Path(tmpdir)
            (root / "small.py").write_text("X = 1\n", encoding="utf-8")
            (root / "data").mkdir()
            (root / "data" / "big.bin").write_bytes(os.urandom(512 * 1024))
            with self.assertRaisesRegex(RayClientError, "caps uploads at") as ctx:
                build_working_dir_package(root)
            self.assertIn("data/big.bin", str(ctx.exception))
            self.assertIn("runtime_env['excludes']", str(ctx.exception))

            zbytes, _ = build_working_dir_package(root, excludes=["data/"])
            with zipfile.ZipFile(io.BytesIO(zbytes)) as zf:
                self.assertEqual(set(zf.namelist()), {"small.py"})

    def test_auth_headers_and_address_resolution(self):
        self.assertIs(RayClient, RayCluster)
        env_overrides = {
            "RAY_AUTH_TOKEN": "env_secret_token",
            "RAY_ADDRESS": "10.0.0.5:8265",
            "RAY_API_SERVER_ADDRESS": "",
            "RAY_AUTH_MODE": "",
            "RAY_JOB_CONFIG_JSON_ENV_VAR": "",
        }
        with patch.dict(os.environ, env_overrides, clear=False):
            os.environ.pop("RAY_API_SERVER_ADDRESS", None)
            os.environ.pop("RAY_AUTH_MODE", None)
            os.environ.pop("RAY_JOB_CONFIG_JSON_ENV_VAR", None)
            c_default = RayCluster()
            self.assertEqual(c_default.address, "http://10.0.0.5:8265")
            self.assertEqual(
                c_default._headers["Authorization"], "Bearer env_secret_token"
            )

            c_auto = RayCluster(address="auto")
            self.assertEqual(c_auto.address, "http://10.0.0.5:8265")

            c_custom = RayCluster(
                address="HTTPS://127.0.0.1:8265/",
                headers={"authorization": "Bearer custom_lower"},
            )
            self.assertEqual(c_custom.address, "https://127.0.0.1:8265")
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

    def test_run_driver_function_and_auto_stream(self):
        with RayCluster(address=self.address) as cluster:

            def step(x: int) -> dict:
                return {"doubled": x * 2}

            self.assertEqual(cluster.run(step, 5), {"doubled": 10})
            self.assertIsNotNone(self.state.last_created_actor_spec)
            self.assertEqual(self.state.last_created_actor_spec["num_cpus"], 0)
            self.assertIs(self.state.last_created_actor_spec["ephemeral"], True)
            self.assertEqual(
                cluster.run(step, 21, timeout=5.0),
                {"doubled": 42},
            )
            # Ephemeral driver actors are deleted in `finally`
            self.assertEqual(cluster.list_actors(), [])

            async def async_step(msg: str) -> str:
                return msg.upper()

            self.assertEqual(run(async_step, "hello", address=self.address), "HELLO")

            def stream_numbers(count: int) -> Iterator[dict]:
                for i in range(count):
                    yield {"i": i, "sq": i * i}

            self.assertEqual(
                list(run(stream_numbers, 3, address=self.address)),
                [{"i": 0, "sq": 0}, {"i": 1, "sq": 1}, {"i": 2, "sq": 4}],
            )
            self.assertEqual(cluster.list_actors(), [])

            # Early break on stream still deletes ephemeral driver actor
            gen = cluster.run(stream_numbers, 10)
            self.assertEqual(next(gen), {"i": 0, "sq": 0})
            gen.close()
            self.assertEqual(cluster.list_actors(), [])

            # Closing or exiting a context manager on an un-advanced stream iterator
            # still deletes the ephemeral driver actor and closes the HTTP response
            unstarted = cluster.run(stream_numbers, 10)
            unstarted.close()
            self.assertEqual(cluster.list_actors(), [])

            with run(stream_numbers, 10, address=self.address) as ctx_stream:
                self.assertEqual(next(ctx_stream), {"i": 0, "sq": 0})
            self.assertEqual(cluster.list_actors(), [])

    def test_submit_job_lifecycle_caching_and_recovery(self):
        with RayCluster(address=self.address) as cluster:
            # 1. Blocking CLI job (`wait=True`)
            job = cluster.submit_job(
                "python train.py --epochs 5",
                metadata={"team": "ml"},
                wait=True,
            )
            self.assertIsInstance(job, JobResult)
            self.assertEqual(job.status(), "SUCCEEDED")
            self.assertTrue(job.done())
            self.assertIn("ran: python train.py --epochs 5", job.logs())

            # Terminal info and logs are cached
            info_gets_before = self.state.job_info_gets
            logs_gets_before = self.state.job_logs_gets
            _ = job.status()
            _ = job.logs()
            self.assertEqual(self.state.job_info_gets, info_gets_before)
            self.assertEqual(self.state.job_logs_gets, logs_gets_before)

            # 2. Detached CLI job (`wait=False`) with partial log line buffering
            with self.state.lock:
                self.state.jobs["job_streaming_logs"] = {
                    "submission_id": "job_streaming_logs",
                    "entrypoint": "python eval.py",
                    "status": "PENDING",
                    "transitions": ["RUNNING", "RUNNING", "SUCCEEDED"],
                    "logs": "",
                    "log_chunks": ["step 1: ", "loss=0.5\nstep 2: loss=0.2\ntrailing"],
                }
            reattached = cluster.get_job("job_streaming_logs")
            lines: List[str] = []
            final_st = reattached.wait(on_log_line=lines.append)
            self.assertEqual(final_st, "SUCCEEDED")
            self.assertEqual(
                lines, ["step 1: loss=0.5", "step 2: loss=0.2", "trailing"]
            )

            # 3. Stop and delete a running job
            with self.state.lock:
                self.state.jobs["job_to_stop"] = {
                    "submission_id": "job_to_stop",
                    "entrypoint": "python long.py",
                    "status": "RUNNING",
                    "logs": "",
                }
            running_job = cluster.get_job("job_to_stop")
            with self.assertRaises(RayJobTimeoutError) as t_ctx:
                running_job.wait(timeout=0.15)
            self.assertIs(t_ctx.exception.job, running_job)
            self.assertTrue(running_job.stop())
            self.assertEqual(running_job.status(), "STOPPED")
            self.assertTrue(running_job.delete())

            # 4. Dropped POST /api/jobs/ response recovers via GET /api/jobs/{submission_id}
            self.state.drop_next_job_post_response = True
            recovered = cluster.submit_job(
                "python idempotent.py", submission_id="sub_recover_1"
            )
            self.assertEqual(recovered.submission_id, "sub_recover_1")
            self.assertGreaterEqual(len(cluster.list_jobs()), 1)

    def test_working_dir_upload_caching_and_large_payload(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            (Path(tmpdir) / "helper_mod.py").write_text(
                "VAL = 123\n"
                "def get_val(mult: int = 1) -> int:\n"
                "    return VAL * mult\n"
                "class HelperActor:\n"
                "    def __init__(self, base: int = 0):\n"
                "        self.base = base\n"
                "    def compute(self, x: int) -> int:\n"
                "        return self.base + VAL + x\n",
                encoding="utf-8",
            )
            (Path(tmpdir) / "big.bin").write_bytes(b"\0" * 1024)
            user_runtime_env = {
                "working_dir": tmpdir,
                "excludes": ["*.bin"],
                "env_vars": {"K": "V"},
            }
            snapshot = copy.deepcopy(user_runtime_env)

            def read_helper() -> int:
                import helper_mod  # type: ignore[import-not-found]

                assert os.environ["K"] == "V"
                assert not os.path.exists("big.bin")
                return helper_mod.VAL

            with RayCluster(
                address=self.address, default_runtime_env=user_runtime_env
            ) as cluster:
                self.assertEqual(cluster.run(read_helper), 123)
                self.assertEqual(cluster.run("helper_mod:get_val", 2), 246)
                h_actor = cluster.actor("helper_mod:HelperActor", 10, name="h_act")
                self.assertEqual(h_actor.compute(5), 138)
                # working_dir was already uploaded by cluster.run(read_helper), so h_actor hits the cache:
                self.assertFalse(h_actor.package_uploaded)
                h_actor.kill()
                self.assertEqual(user_runtime_env, snapshot)
                self.assertEqual(len(self.state.packages), 1)

                # Large (>32 KiB) argument payload is sent inline in the HTTP JSON body
                # without uploading an extra package to self.state.packages
                big_text = "x" * (40 * 1024)

                def echo_len(s: str) -> int:
                    return len(s)

                self.assertEqual(cluster.run(echo_len, big_text), 40 * 1024)
                self.assertEqual(len(self.state.packages), 1)

                # Simulate cluster GCS package expiration: next call re-uploads cleanly
                with self.state.lock:
                    self.state.packages.clear()

            with RayCluster(
                address=self.address, default_runtime_env=user_runtime_env
            ) as cluster2:
                self.assertEqual(cluster2.run(read_helper), 123)
                self.assertEqual(len(self.state.packages), 1)

    def test_thread_safe_concurrent_submissions(self):
        with RayCluster(address=self.address) as cluster:

            def square(n: int) -> int:
                return n * n

            with ThreadPoolExecutor(max_workers=4) as pool:
                futures = [
                    pool.submit(lambda v=i: cluster.run(square, v)) for i in range(8)
                ]
                results = [f.result() for f in futures]
            self.assertEqual(results, [i * i for i in range(8)])

    def test_remote_exception_and_sys_exit_propagate_cleanly(self):
        with RayCluster(address=self.address) as cluster:

            def failing_task():
                raise ValueError("invalid shard configuration")

            with self.assertRaises(RayJobExecutionError) as ctx:
                cluster.run(failing_task)
            self.assertEqual(ctx.exception.exc_type, "ValueError")
            self.assertIn("invalid shard configuration", str(ctx.exception))
            restored = pickle.loads(pickle.dumps(ctx.exception))
            self.assertEqual(restored.exc_type, "ValueError")

            def exiting_task():
                import sys as _sys

                _sys.exit(2)

            with self.assertRaises(RayJobExecutionError) as exit_ctx:
                cluster.run(exiting_task)
            self.assertEqual(exit_ctx.exception.exc_type, "SystemExit")

            def clean_exit_task():
                import sys as _sys

                _sys.exit(0)

            self.assertIsNone(cluster.run(clean_exit_task))

    def test_retries_transient_503_rejects_ray_scheme_and_non_json_bodies(self):
        with self.assertRaises(RayClientError):
            RayCluster(address="ray://127.0.0.1:10001")

        self.state.fail_next_requests = 2
        with RayCluster(address=self.address) as cluster:
            self.assertEqual(cluster.list_actors(), [])

            def ok_fn() -> int:
                return 7

            self.assertEqual(cluster.run(ok_fn), 7)

        html_server = ThreadingHTTPServer(("127.0.0.1", 0), _HtmlLoginPageHandler)
        threading.Thread(target=html_server.serve_forever, daemon=True).start()
        try:
            html_address = f"http://127.0.0.1:{html_server.server_address[1]}"
            with self.assertRaisesRegex(RayClientError, "non-JSON response"):
                RayCluster(address=html_address).list_actors()
        finally:
            html_server.shutdown()
            html_server.server_close()

    def test_stateful_actors_streaming_logs_and_idle_ttl(self):
        offset = 5

        class Accumulator:
            scale = GLOBAL_SCALE

            def __init__(self, init_val: int = 0):
                self.val = init_val + offset

            def make_batch(self, count: int) -> list:
                return [self.val + i for i in range(count)]

            def summarize(self, batch: list, tag: str = "default") -> dict:
                return {"tag": tag, "sum": sum(batch), "scale": self.scale}

            def stream_items(self, count: int):
                for i in range(count):
                    yield {"idx": i, "val": self.val + i}

        cls_name, mod_src, captured = extract_class_source_and_globals(Accumulator)
        self.assertEqual(cls_name, "Accumulator")
        self.assertIn("class Accumulator", mod_src)
        self.assertEqual(captured, {"GLOBAL_SCALE": 42, "offset": 5})

        cluster = RayCluster(address=self.address)
        a1 = cluster.actor(Accumulator, 10, name="acc_a", ttl_seconds=60.0)
        self.assertIsNotNone(self.state.last_created_actor_spec)
        self.assertEqual(self.state.last_created_actor_spec["num_cpus"], 1)
        self.assertIsNone(self.state.last_created_actor_spec.get("ephemeral"))
        a2 = cluster.actor(Accumulator, 100, name="acc_b", ttl_seconds=60.0)
        self.assertIsInstance(a1, ActorHandle)
        self.assertTrue(a1.created)

        # Reattaching to existing actor
        a1_re = cluster.get_actor("acc_a")
        self.assertFalse(a1_re.created)
        self.assertEqual(len(cluster.list_actors()), 2)

        batch_a = a1.make_batch(3)
        self.assertEqual(batch_a, [15, 16, 17])
        summary_a = a1_re.summarize(batch_a, tag="a")
        self.assertEqual(summary_a, {"tag": "a", "sum": 48, "scale": 42})

        batch_b = a2.make_batch(2)
        summary_b = a2.summarize(batch_b, tag="b")
        self.assertEqual(summary_b, {"tag": "b", "sum": 211, "scale": 42})

        # Generator method on actor automatically streams over SSE
        chunks = list(a1.stream_items(2))
        self.assertEqual(chunks, [{"idx": 0, "val": 15}, {"idx": 1, "val": 16}])
        self.assertEqual(a1.logs(), "")

        self.assertTrue(a1.kill())
        self.assertTrue(a2.kill())

        # Idle TTL cleanup test (only reaps when active_calls == 0)
        short_actor = cluster.actor(
            Accumulator, 1, name="short_lived", ttl_seconds=0.15
        )
        self.assertEqual(short_actor.info()["name"], "short_lived")
        with self.state.lock:
            self.state.actors["short_lived"]["active_calls"] = 1
        time.sleep(0.22)
        # Paused while active_calls > 0
        self.assertIn("short_lived", [a["name"] for a in cluster.list_actors()])
        with self.state.lock:
            self.state.actors["short_lived"]["active_calls"] = 0
            self.state.actors["short_lived"]["last_active"] = time.monotonic() - 1.0
        self.assertNotIn("short_lived", [a["name"] for a in cluster.list_actors()])
        cluster.close()

    def test_annotations_notebook_classes_defaults_and_actor_options(self):
        import datetime
        import linecache
        import typing
        from os import path as osp

        from ray.dashboard.modules.job.actor_manager import (
            _ClientRootActor,
            _load_user_target,
            _LogTeeStream,
            _validate_ttl_seconds,
        )

        # 1. P6: Dotted annotations (datetime.date), callable annotations, get_type_hints, and os.path
        class _LocalPkg:
            class Sub:
                def __class_getitem__(cls, item: Any) -> Any:
                    return cls

        def _local_validator(**kw: Any) -> Any:
            return kw

        def annotated_fn(
            d: datetime.date,
            items: List[int],
            meta: Optional[Dict[str, int]] = None,
            custom: _LocalPkg.Sub[int] | None = None,
            validated: typing.Annotated[int, _local_validator(gt=0)] = 1,
        ) -> str:
            hints = typing.get_type_hints(annotated_fn)
            assert hints["d"] is datetime.date
            return osp.join("a", "b")

        fn_name, mod_src, captured = packaging.extract_function(annotated_fn)
        self.assertEqual(captured, {})
        self.assertIn("from os import path as osp", mod_src)
        self.assertIn("import datetime", mod_src)
        loaded_fn = _load_user_target("test_act", fn_name, mod_src, None, captured)
        self.assertEqual(loaded_fn(datetime.date(2026, 1, 1), [1, 2]), "a/b")

        # 2. P7 & D9: Simulated IPython notebook cell class extraction (no __main__.__file__)
        cell_filename = "<ipython-input-99-notebookcell>"
        cell_source = (
            "# line 1\n"
            "# line 2\n"
            "class NotebookTrainer:\n"
            "    def __init__(self, base: int = 5):\n"
            "        self.base = base + GLOBAL_SCALE\n"
            "    def step(self, x: int) -> int:\n"
            "        return inspect.currentframe().f_lineno\n"
        )
        linecache.cache[cell_filename] = (
            len(cell_source),
            None,
            cell_source.splitlines(keepends=True),
            cell_filename,
        )
        main_mod = sys.modules["__main__"]
        had_main_file = hasattr(main_mod, "__file__")
        old_main_file = getattr(main_mod, "__file__", None)
        try:
            if had_main_file:
                delattr(main_mod, "__file__")
            cell_ns: Dict[str, Any] = {
                "__name__": "__main__",
                "GLOBAL_SCALE": GLOBAL_SCALE,
                "inspect": inspect,
            }
            exec(compile(cell_source, cell_filename, "exec"), cell_ns)
            nb_cls = cell_ns["NotebookTrainer"]
            cls_name, cls_src, cls_captured = packaging.extract_class(nb_cls)
            self.assertEqual(cls_name, "NotebookTrainer")
            self.assertEqual(cls_captured, {"GLOBAL_SCALE": 42})
            loaded_cls = _load_user_target(
                "test_nb", cls_name, cls_src, None, cls_captured
            )
            inst = loaded_cls()
            self.assertEqual(inst.base, 47)
            # Line number of `return inspect.currentframe().f_lineno` in cell_source is line 7
            self.assertEqual(inst.step(1), 7)
        finally:
            linecache.cache.pop(cell_filename, None)
            if had_main_file:
                main_mod.__file__ = old_main_file

        # 3. P8: Non-JSON and dynamic call default arguments are NOT forced through client JSON
        def fn_with_non_json_defaults(
            x: int,
            shape: Tuple[int, int] = (224, 224),
            cb: Any = print,
            nan_v: float = float("nan"),
            ts: float = time.time(),
        ) -> dict:
            return {
                "x": x,
                "shape": list(shape),
                "is_print": cb is print,
                "is_nan": nan_v != nan_v,
                "has_ts": ts > 0,
            }

        def nested_dynamic_default_maker():
            def fn_dyn(x: int, ts: float = time.time()) -> bool:
                return x > 0 and ts > 0

            return fn_dyn

        dyn_fn = nested_dynamic_default_maker()
        # Extracting source must NOT fail on dynamic call defaults like `time.time()`
        _, dyn_src, _ = extract_function_source_and_globals(dyn_fn)
        self.assertIn("import time", dyn_src)

        with RayCluster(address=self.address) as cluster:
            res = cluster.run(fn_with_non_json_defaults, 7)
            self.assertEqual(
                res,
                {
                    "x": 7,
                    "shape": [224, 224],
                    "is_print": True,
                    "is_nan": True,
                    "has_ts": True,
                },
            )

            # 4. P14: Class body referencing enclosing function scope fails locally
            def make_bad_class():
                enclosing_secret = 99

                class BadActor:
                    val = enclosing_secret

                    def get(self) -> int:
                        return self.val

                return BadActor

            with self.assertRaisesRegex(
                RayClientSerializationError, "enclosing function or class scope"
            ):
                extract_class_source_and_globals(make_bad_class())

            # Class descriptors (@property, @classmethod, @staticmethod) capture closures properly
            closure_bonus = 11

            class DescriptorActor:
                def __init__(self, v: int, tup: Tuple[int, int] = (1, 2)):
                    self._v = v + sum(tup)

                @property
                def prop_val(self) -> int:
                    return self._v + closure_bonus

                @staticmethod
                def static_val() -> int:
                    return closure_bonus * 2

                def read_all(self) -> dict:
                    return {"prop": self.prop_val, "static": self.static_val()}

            d_act = cluster.actor(DescriptorActor, 10, name="desc_act")
            self.assertEqual(d_act.read_all(), {"prop": 24, "static": 22})
            d_act.kill()

            # 5. P9 & P15: @ray.remote options inheritance and ttl_seconds=None preserved
            import ray

            @ray.remote(num_cpus=2, num_gpus=1, max_concurrency=4)
            class DecoratedActor:
                def ping(self) -> str:
                    return "pong"

            dec_handle = cluster.actor(DecoratedActor, name="dec_act", ttl_seconds=None)
            self.assertEqual(dec_handle.ping(), "pong")
            self.assertIsNone(dec_handle.info()["ttl_seconds"])
            dec_handle.kill()

            # 6. P11 & P19: Eager argument validation, _ActorMethodProxy.call(timeout=...) on generator, cyclic JSON
            def gen_with_defaults(x: int):
                yield {"x": x}

            with self.assertRaises(RayClientSerializationError):
                # Must fail eagerly on cluster.run() call without calling next()
                cluster.run(gen_with_defaults, (1, 2))

            cyc_list: List[Any] = []
            cyc_list.append(cyc_list)
            with self.assertRaisesRegex(
                RayClientSerializationError, "circular reference"
            ):
                cluster.run(fn_with_non_json_defaults, cyc_list)

            class Streamer:
                def gen(self, count: int, step: int = 1):
                    for i in range(count):
                        yield {"v": i * step}

            st_act = cluster.actor(Streamer, name="st_act")
            # `timeout=` on proxy `.call()` must not leak into remote `kwargs`
            items = list(st_act.gen.call(3, step=2, timeout=5.0))
            self.assertEqual(items, [{"v": 0}, {"v": 2}, {"v": 4}])
            st_act.kill()

            # 7. P16: User-raised TimeoutError raises RayJobExecutionError (not RayJobTimeoutError)
            def raises_user_timeout():
                raise TimeoutError("downstream socket timed out")

            with self.assertRaises(RayJobExecutionError) as ut_ctx:
                cluster.run(raises_user_timeout)
            self.assertEqual(ut_ctx.exception.exc_type, "TimeoutError")
            self.assertIn("downstream socket timed out", str(ut_ctx.exception))

            # 8. KubeRay resource management methods route to /api/cluster/worker_groups
            with self.assertRaises(RayClientError):
                cluster.scale_worker_group("gpu-group", replicas=2)
            with self.assertRaises(RayClientError):
                cluster.add_worker_group(
                    "tpu-workers", replicas=2, resources={"TPU": 4}
                )

        # 9. P10, P12, P16: Direct unit tests for _LogTeeStream, _validate_ttl_seconds, _ClientRootActor
        for bad_ttl in (-1.0, float("nan"), float("inf"), True):
            with self.assertRaises(ValueError):
                _validate_ttl_seconds(bad_ttl)
        self.assertIsNone(_validate_ttl_seconds(None))
        self.assertEqual(_validate_ttl_seconds(0), 0.0)

        from collections import deque

        buf: deque = deque()
        tee = _LogTeeStream(sys.__stdout__, buf, threading.Lock())
        self.assertTrue(tee.writable())
        self.assertIsInstance(tee.encoding, str)
        tee.write("0123456789abcdef")
        self.assertEqual("".join(buf), "0123456789abcdef")

        # Verify _ClientRootActor catches SystemExit in async methods and generators without crashing,
        # does not evaluate @property getters when scanning for generator methods during __init__,
        # and _install_ray_hooks isolates child actors in f"ray_client_{name}" while falling back
        # to "ray_client" on ray.get_actor(name).
        import ray._private.worker
        from ray.actor import ActorClass
        from ray.runtime_context import RuntimeContext

        orig_stdout, orig_stderr = sys.stdout, sys.stderr
        orig_ray_ns_env = os.environ.get("RAY_NAMESPACE")
        orig_ray_init = ray.init
        orig_ray_shutdown = ray.shutdown
        orig_get_actor = ray.get_actor
        orig_worker_get_actor = ray._private.worker.get_actor
        orig_actor_remote = ActorClass._remote
        orig_pg = ray.util.placement_group
        orig_rt_ns = RuntimeContext.__dict__.get("namespace")

        recorded_remote_opts: List[Dict[str, Any]] = []
        recorded_get_actor_calls: List[Tuple[str, Optional[str]]] = []

        def fake_actor_remote(actor_cls, args=None, kwargs=None, **actor_options):
            recorded_remote_opts.append(dict(actor_options))
            return {"actor_cls": actor_cls, "options": dict(actor_options)}

        def fake_get_actor(name: str, namespace: Optional[str] = None):
            recorded_get_actor_calls.append((name, namespace))
            if name == "child_only" and namespace == "ray_client_test_root_exit":
                return "handle_child_only"
            if name == "shared_top" and namespace == "ray_client":
                return "handle_shared_top"
            if name == "explicit_actor" and namespace == "custom_ns":
                return "handle_explicit"
            raise ValueError(f"Actor {name!r} not found in namespace {namespace!r}")

        async_src = (
            "class AsyncExitActor:\n"
            "    @property\n"
            "    def boom_prop(self):\n"
            "        raise RuntimeError('property getter should not run during __init__')\n"
            "    async def exit_zero(self):\n"
            "        raise SystemExit(0)\n"
            "    async def exit_nonzero(self):\n"
            "        raise SystemExit(3)\n"
            "    def gen_exit(self):\n"
            "        yield {'step': 1}\n"
            "        raise SystemExit(4)\n"
        )
        root_act = None
        try:
            ray.get_actor = fake_get_actor
            ActorClass._remote = fake_actor_remote
            root_act = _ClientRootActor(
                {
                    "name": "test_root_exit",
                    "kind": "class",
                    "class_name": "AsyncExitActor",
                    "module_source": async_src,
                    "ttl_seconds": None,
                }
            )
            self.assertEqual(
                os.environ.get("RAY_NAMESPACE"), "ray_client_test_root_exit"
            )
            self.assertEqual(
                RuntimeContext.namespace.fget(None), "ray_client_test_root_exit"
            )

            # Child actor creation defaults to f"ray_client_{name}", detached defaults to
            # "ray_client", and explicit namespace is preserved
            ActorClass._remote("ChildCls", args=[], kwargs={})
            ActorClass._remote("ChildCls", args=[], kwargs={}, lifetime="detached")
            ActorClass._remote("ChildCls", args=[], kwargs={}, namespace="custom_ns")
            self.assertEqual(
                recorded_remote_opts[0]["namespace"], "ray_client_test_root_exit"
            )
            self.assertEqual(recorded_remote_opts[1]["namespace"], "ray_client")
            self.assertEqual(recorded_remote_opts[2]["namespace"], "custom_ns")

            # ray.get_actor checks f"ray_client_{name}" first, then falls back to "ray_client"
            self.assertEqual(ray.get_actor("child_only"), "handle_child_only")
            self.assertEqual(
                recorded_get_actor_calls,
                [("child_only", "ray_client_test_root_exit")],
            )
            recorded_get_actor_calls.clear()

            self.assertEqual(ray.get_actor("shared_top"), "handle_shared_top")
            self.assertEqual(
                recorded_get_actor_calls,
                [
                    ("shared_top", "ray_client_test_root_exit"),
                    ("shared_top", "ray_client"),
                ],
            )
            recorded_get_actor_calls.clear()

            self.assertEqual(
                ray.get_actor("explicit_actor", namespace="custom_ns"),
                "handle_explicit",
            )
            self.assertEqual(
                recorded_get_actor_calls,
                [("explicit_actor", "custom_ns")],
            )

            self.assertIn("gen_exit", root_act._generator_methods)
            self.assertNotIn("boom_prop", root_act._generator_methods)
            res_zero = root_act.invoke("exit_zero", [], {})
            self.assertEqual(res_zero, {"ok": True, "result": None})
            res_nonzero = root_act.invoke("exit_nonzero", [], {})
            self.assertFalse(res_nonzero["ok"])
            self.assertEqual(res_nonzero["error"]["exc_type"], "SystemExit")

            gen_frames = list(root_act.invoke_stream("gen_exit", [], {}, "s1"))
            self.assertEqual(gen_frames[0], {"ok": True, "item": {"step": 1}})
            self.assertFalse(gen_frames[1]["ok"])
            self.assertEqual(gen_frames[1]["error"]["exc_type"], "SystemExit")
        finally:
            if root_act is not None:
                root_act.shutdown()
            sys.stdout, sys.stderr = orig_stdout, orig_stderr
            if orig_ray_ns_env is None:
                os.environ.pop("RAY_NAMESPACE", None)
            else:
                os.environ["RAY_NAMESPACE"] = orig_ray_ns_env
            ray.init = orig_ray_init
            ray.shutdown = orig_ray_shutdown
            ray.get_actor = orig_get_actor
            ray._private.worker.get_actor = orig_worker_get_actor
            ActorClass._remote = orig_actor_remote
            ray.util.placement_group = orig_pg
            if orig_rt_ns is not None:
                RuntimeContext.namespace = orig_rt_ns


if __name__ == "__main__":
    unittest.main()
