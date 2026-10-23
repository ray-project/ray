"""Offline unit and mock-REST-server tests for ray_client."""

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
from typing import Any, Dict, Iterator, List, Optional
from unittest.mock import patch

from ray_client import (
    ActorProxy,
    RayClient,
    RayClientError,
    RayClientSerializationError,
    RayExecutionError,
    RemoteRef,
    packaging,
    run,
    stream,
)
from ray_client._ignore import _compile_ignore_rules, _is_ignored
from ray_client.packaging import (
    IGNORE_GITIGNORE_ENV_VAR,
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
        self.workspaces: Dict[str, Dict[str, Any]] = {}
        self.fail_next_requests = 0
        self.lock = threading.Lock()

    def check_ttls_locked(self) -> None:
        now = time.monotonic()
        for ws_name, ws in list(self.workspaces.items()):
            ttl = ws.get("ttl_seconds")
            if ttl is not None and ttl > 0 and (now - ws["last_active"]) >= ttl:
                self.workspaces.pop(ws_name, None)


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

    def _send_ndjson_stream(self, items: Any) -> None:
        lines = []
        try:
            for item in items:
                lines.append(
                    json.dumps({"ok": True, "item": item}, allow_nan=False) + "\n"
                )
        except Exception as exc:
            lines.append(
                json.dumps(
                    {
                        "ok": False,
                        "error": {
                            "exc_type": type(exc).__name__,
                            "exc_message": str(exc),
                            "traceback": traceback.format_exc(),
                        },
                    }
                )
                + "\n"
            )
        payload = "".join(lines).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/x-ndjson")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _read_json(self) -> Any:
        length = int(self.headers.get("Content-Length", "0"))
        if length <= 0:
            return {}
        return json.loads(self.rfile.read(length).decode("utf-8"))

    def _unpack_workspace_runtime_env_locked(
        self, ws_name: str, ws: Dict[str, Any], runtime_env: Optional[Dict[str, Any]]
    ) -> Optional[Path]:
        if not runtime_env:
            return ws.get("work_dir")
        ws_dir = self.state.tmp_root / ws_name
        ws_dir.mkdir(parents=True, exist_ok=True)
        wd_uri = runtime_env.get("working_dir")
        if wd_uri and str(wd_uri).startswith("gcs://"):
            pkg_name = str(wd_uri).split("gcs://", 1)[1]
            if pkg_name not in self.state.packages:
                raise KeyError(f"Package '{pkg_name}' not uploaded")
            with zipfile.ZipFile(io.BytesIO(self.state.packages[pkg_name])) as zf:
                zf.extractall(ws_dir)
        for mod_uri in runtime_env.get("py_modules") or ():
            if str(mod_uri).startswith("gcs://"):
                pkg_name = str(mod_uri).split("gcs://", 1)[1]
                if pkg_name in self.state.packages:
                    with zipfile.ZipFile(
                        io.BytesIO(self.state.packages[pkg_name])
                    ) as zf:
                        zf.extractall(ws_dir)
        env_vars = dict(runtime_env.get("env_vars") or {})
        ws["env_vars"] = env_vars
        ws["work_dir"] = ws_dir
        return ws_dir

    def do_GET(self) -> None:  # noqa: N802
        parsed = urllib.parse.urlsplit(self.path)
        path = parsed.path
        query = dict(urllib.parse.parse_qsl(parsed.query))

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
        if path == "/api/workspaces":
            with self.state.lock:
                self.state.check_ttls_locked()
                items = [
                    {
                        "workspace": ws_name,
                        "namespace": f"workspace:{ws_name}",
                        "ttl_seconds": ws["ttl_seconds"],
                        "actors": list(ws["actors"].keys()),
                        "ref_count": len(ws["refs"]),
                    }
                    for ws_name, ws in self.state.workspaces.items()
                ]
            self._send_json(200, items)
            return
        if path.startswith("/api/workspaces/"):
            parts = path[len("/api/workspaces/") :].split("/")
            ws_name = parts[0]
            with self.state.lock:
                self.state.check_ttls_locked()
                ws = self.state.workspaces.get(ws_name)
                if ws is None:
                    self._send_json(404, {"error": "workspace not found"})
                    return
                ws["last_active"] = time.monotonic()
                if len(parts) == 1:
                    self._send_json(
                        200,
                        {
                            "workspace": ws_name,
                            "namespace": f"workspace:{ws_name}",
                            "ttl_seconds": ws["ttl_seconds"],
                            "actors": [
                                {"name": k, "lifetime": v["lifetime"]}
                                for k, v in ws["actors"].items()
                            ],
                            "ref_count": len(ws["refs"]),
                        },
                    )
                    return
                if len(parts) == 2 and parts[1] == "actors":
                    self._send_json(
                        200,
                        [
                            {
                                "name": k,
                                "workspace": ws_name,
                                "lifetime": v["lifetime"],
                            }
                            for k, v in ws["actors"].items()
                        ],
                    )
                    return
                if len(parts) == 3 and parts[1] == "actors":
                    actor_name = parts[2]
                    act = ws["actors"].get(actor_name)
                    if act is None:
                        self._send_json(404, {"error": "actor not found"})
                        return
                    self._send_json(
                        200,
                        {
                            "name": actor_name,
                            "workspace": ws_name,
                            "lifetime": act["lifetime"],
                        },
                    )
                    return
                if len(parts) == 3 and parts[1] == "refs":
                    ref_id = parts[2]
                    if ref_id not in ws["refs"]:
                        self._send_json(
                            404,
                            {
                                "ok": False,
                                "not_found": True,
                                "error": {
                                    "exc_type": "KeyError",
                                    "exc_message": f"Ref '{ref_id}' not found",
                                },
                            },
                        )
                        return
                    entry = ws["refs"][ref_id]
                    if query.get("ready_only") in ("1", "true", "True"):
                        self._send_json(
                            200,
                            {
                                "ok": True,
                                "ready": not entry.get("canceled", False),
                            },
                        )
                        return
                    if entry.get("canceled"):
                        self._send_json(
                            200,
                            {
                                "ok": False,
                                "error": {
                                    "exc_type": "TaskCancelledError",
                                    "exc_message": f"Ref '{ref_id}' was cancelled",
                                },
                            },
                        )
                        return
                    if entry.get("error"):
                        self._send_json(200, {"ok": False, "error": entry["error"]})
                        return
                    self._send_json(200, {"ok": True, "result": entry["value"]})
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
        if path.startswith("/api/workspaces/"):
            parts = path[len("/api/workspaces/") :].split("/")
            ws_name = parts[0]
            with self.state.lock:
                self.state.check_ttls_locked()
                if len(parts) == 1:
                    existed = self.state.workspaces.pop(ws_name, None) is not None
                    self._send_json(200 if existed else 404, {"closed": existed})
                    return
                ws = self.state.workspaces.get(ws_name)
                if ws is None:
                    self._send_json(404, {"error": "workspace not found"})
                    return
                ws["last_active"] = time.monotonic()
                if len(parts) == 3 and parts[1] == "actors":
                    existed = ws["actors"].pop(parts[2], None) is not None
                    self._send_json(200 if existed else 404, {"killed": existed})
                    return
                if len(parts) == 3 and parts[1] == "refs":
                    existed = ws["refs"].pop(parts[2], None) is not None
                    self._send_json(200 if existed else 404, {"deleted": existed})
                    return
        self._send_json(404, {"error": "unknown endpoint"})

    @staticmethod
    def _resolve_mock_refs(val: Any, refs: Dict[str, Any]) -> Any:
        if isinstance(val, dict) and set(val.keys()) == {"$ray_ref"}:
            ref_id = val["$ray_ref"]
            if ref_id not in refs:
                raise KeyError(f"RemoteRef '{ref_id}' not found")
            entry = refs[ref_id]
            if entry.get("error"):
                raise RuntimeError(entry["error"]["exc_message"])
            return entry["value"]
        if isinstance(val, list):
            return [_MockJobServerHandler._resolve_mock_refs(v, refs) for v in val]
        if isinstance(val, dict):
            return {
                k: _MockJobServerHandler._resolve_mock_refs(v, refs)
                for k, v in val.items()
            }
        return val

    def _exec_in_workspace_env(
        self, ws: Dict[str, Any], fn: Any, args: list, kwargs: dict
    ) -> Any:
        work_dir: Optional[Path] = ws.get("work_dir")
        env_vars: Dict[str, str] = ws.get("env_vars") or {}
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

    def do_POST(self) -> None:  # noqa: N802
        parsed = urllib.parse.urlsplit(self.path)
        path = parsed.path

        if path == "/api/workspaces":
            body = self._read_json()
            ws_name = body["workspace"]
            with self.state.lock:
                self.state.check_ttls_locked()
                ws = self.state.workspaces.get(ws_name)
                created = ws is None
                if ws is None:
                    ws = self.state.workspaces[ws_name] = {
                        "ttl_seconds": body.get("ttl_seconds", 1800.0),
                        "last_active": time.monotonic(),
                        "actors": {},
                        "refs": {},
                        "work_dir": None,
                        "env_vars": {},
                    }
                else:
                    ws["last_active"] = time.monotonic()
                    if "ttl_seconds" in body and body["ttl_seconds"] is not None:
                        ws["ttl_seconds"] = body["ttl_seconds"]
                if body.get("runtime_env"):
                    self._unpack_workspace_runtime_env_locked(
                        ws_name, ws, body["runtime_env"]
                    )
                self._send_json(
                    200,
                    {
                        "created": created,
                        "workspace": ws_name,
                        "namespace": f"workspace:{ws_name}",
                        "ttl_seconds": ws["ttl_seconds"],
                        "actors": list(ws["actors"].keys()),
                        "ref_count": len(ws["refs"]),
                    },
                )
            return

        if path.startswith("/api/workspaces/"):
            parts = path[len("/api/workspaces/") :].split("/")
            ws_name = parts[0]
            body = self._read_json()
            with self.state.lock:
                self.state.check_ttls_locked()
                ws = self.state.workspaces.get(ws_name)
                if ws is None:
                    ws = self.state.workspaces[ws_name] = {
                        "ttl_seconds": 1800.0,
                        "last_active": time.monotonic(),
                        "actors": {},
                        "refs": {},
                        "work_dir": None,
                        "env_vars": {},
                    }
                ws["last_active"] = time.monotonic()
                if body.get("runtime_env"):
                    self._unpack_workspace_runtime_env_locked(
                        ws_name, ws, body["runtime_env"]
                    )

                if len(parts) == 2 and parts[1] == "heartbeat":
                    self._send_json(200, {"workspace": ws_name, "ok": True})
                    return

                if len(parts) == 4 and parts[1] == "refs" and parts[3] == "cancel":
                    ref_id = parts[2]
                    if ref_id not in ws["refs"]:
                        self._send_json(404, {"canceled": False})
                        return
                    ws["refs"][ref_id]["canceled"] = True
                    self._send_json(200, {"canceled": True})
                    return

                if len(parts) == 2 and parts[1] == "tasks":
                    try:
                        args = self._resolve_mock_refs(
                            body.get("args") or [], ws["refs"]
                        )
                        kwargs = self._resolve_mock_refs(
                            body.get("kwargs") or {}, ws["refs"]
                        )
                        if body.get("module_source") is not None:
                            mod_name = f"_mock_task_{uuid.uuid4().hex[:8]}"
                            mod = types.ModuleType(mod_name)
                            sys.modules[mod_name] = mod
                            mod.__dict__.update(body.get("captured_globals") or {})
                            src = body["module_source"].replace("import ray", "pass")
                            exec(compile(src, "<task>", "exec"), mod.__dict__)
                            fn = getattr(mod, body["fn_name"])
                            out = self._exec_in_workspace_env(ws, fn, args, kwargs)
                        else:
                            import importlib

                            def _run_imported(*a: Any, **kw: Any) -> Any:
                                importlib.invalidate_caches()
                                m = importlib.import_module(body["module"])
                                return getattr(m, body["fn_name"])(*a, **kw)

                            out = self._exec_in_workspace_env(
                                ws, _run_imported, args, kwargs
                            )
                        if body.get("stream"):
                            self._send_ndjson_stream(out)
                            return
                        if not body.get("wait", True) or body.get("return_ref"):
                            ref_id = f"ref_{uuid.uuid4().hex[:12]}"
                            ws["refs"][ref_id] = {"value": out, "canceled": False}
                            self._send_json(
                                200, {"ok": True, "result": {"$ray_ref": ref_id}}
                            )
                            return
                        self._send_json(200, {"ok": True, "result": out})
                    except SystemExit as exc:
                        code = exc.code
                        if code in (None, 0):
                            self._send_json(200, {"ok": True, "result": None})
                        else:
                            self._send_json(
                                200,
                                {
                                    "ok": False,
                                    "error": {
                                        "exc_type": "SystemExit",
                                        "exc_message": f"Task called sys.exit({code!r})",
                                        "traceback": traceback.format_exc(),
                                    },
                                },
                            )
                    except Exception as exc:
                        if not body.get("wait", True):
                            ref_id = f"ref_{uuid.uuid4().hex[:12]}"
                            ws["refs"][ref_id] = {
                                "value": None,
                                "canceled": False,
                                "error": {
                                    "exc_type": type(exc).__name__,
                                    "exc_message": str(exc),
                                    "traceback": traceback.format_exc(),
                                },
                            }
                            self._send_json(
                                200, {"ok": True, "result": {"$ray_ref": ref_id}}
                            )
                        else:
                            self._send_json(
                                200,
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

                if len(parts) == 2 and parts[1] == "actors":
                    name = body.get("name") or body["class_name"]
                    if body.get("get_if_exists", True) and name in ws["actors"]:
                        self._send_json(
                            200,
                            {
                                "ok": True,
                                "name": name,
                                "workspace": ws_name,
                                "created": False,
                            },
                        )
                        return
                    try:
                        args = self._resolve_mock_refs(
                            body.get("args") or [], ws["refs"]
                        )
                        kwargs = self._resolve_mock_refs(
                            body.get("kwargs") or {}, ws["refs"]
                        )
                        if body.get("module_source") is not None:
                            mod = types.ModuleType(f"_mock_actor_{name}")
                            mod.__dict__.update(body.get("captured_globals") or {})
                            src = body["module_source"].replace("import ray", "pass")
                            exec(compile(src, "<actor>", "exec"), mod.__dict__)
                            cls = getattr(mod, body["class_name"])
                            instance = self._exec_in_workspace_env(
                                ws, cls, args, kwargs
                            )
                        else:
                            import importlib

                            def _instantiate_imported(*a: Any, **kw: Any) -> Any:
                                importlib.invalidate_caches()
                                m = importlib.import_module(body["module"])
                                return getattr(m, body["class_name"])(*a, **kw)

                            instance = self._exec_in_workspace_env(
                                ws, _instantiate_imported, args, kwargs
                            )
                        ws["actors"][name] = {
                            "instance": instance,
                            "lifetime": body.get("lifetime", "workspace"),
                        }
                        self._send_json(
                            200,
                            {
                                "ok": True,
                                "name": name,
                                "workspace": ws_name,
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

                if len(parts) == 5 and parts[1] == "actors" and parts[3] == "methods":
                    actor_name, method_name = parts[2], parts[4]
                    act = ws["actors"].get(actor_name)
                    if act is None:
                        self._send_json(
                            404,
                            {
                                "ok": False,
                                "not_found": True,
                                "error": {
                                    "exc_type": "ValueError",
                                    "exc_message": f"Actor '{actor_name}' not found",
                                },
                            },
                        )
                        return
                    try:
                        args = self._resolve_mock_refs(
                            body.get("args") or [], ws["refs"]
                        )
                        kwargs = self._resolve_mock_refs(
                            body.get("kwargs") or {}, ws["refs"]
                        )
                        fn = getattr(act["instance"], method_name)
                        out = self._exec_in_workspace_env(ws, fn, args, kwargs)
                        if body.get("stream"):
                            self._send_ndjson_stream(out)
                            return
                        if not body.get("wait", True) or body.get("return_ref"):
                            ref_id = f"ref_{uuid.uuid4().hex[:12]}"
                            ws["refs"][ref_id] = {"value": out, "canceled": False}
                            self._send_json(
                                200, {"ok": True, "result": {"$ray_ref": ref_id}}
                            )
                            return
                        self._send_json(200, {"ok": True, "result": out})
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
                            },
                        )
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
            RayClient(address="http://127.0.0.1:1")._prepare_runtime_env(
                {"py_modules": ["./local_pkg"]}
            )

        merged, _, excludes = RayClient(
            address="http://127.0.0.1:1", default_runtime_env={"excludes": ("*.log",)}
        )._prepare_runtime_env({"excludes": ["*.bin"]})
        self.assertEqual(excludes, ["*.log", "*.bin"])
        self.assertNotIn("excludes", merged)

        with self.assertRaisesRegex(TypeError, "excludes.* must be a list"):
            RayClient(address="http://127.0.0.1:1")._prepare_runtime_env(
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

    def test_deterministic_working_dir_zip_and_permissions(self):
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
            c_default = RayClient()
            self.assertEqual(c_default.address, "http://10.0.0.5:8265")
            self.assertTrue(c_default.workspace.startswith("ws_"))
            self.assertEqual(c_default.namespace, f"workspace:{c_default.workspace}")
            self.assertEqual(
                c_default._headers["Authorization"], "Bearer env_secret_token"
            )

            c_auto = RayClient(address="auto")
            self.assertEqual(c_auto.address, "http://10.0.0.5:8265")

            c_custom = RayClient(
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

    def test_stateless_tasks_async_refs_and_task_streaming(self):
        with RayClient(address=self.address) as client:

            def step(x: int) -> dict:
                return {"doubled": x * 2}

            self.assertEqual(client.run(step, 5), {"doubled": 10})
            self.assertEqual(client.run(step, 21), {"doubled": 42})

            # Non-blocking task (`wait=False`) returns RemoteRef
            ref = client.run(step, 9, wait=False)
            self.assertIsInstance(ref, RemoteRef)
            self.assertTrue(ref.ready())
            self.assertEqual(ref.get(), {"doubled": 18})
            self.assertTrue(ref.delete())

            # Return ref (`return_ref=True`) pins result in workspace
            pinned = client.run(step, 7, return_ref=True)
            self.assertEqual(pinned.get(), {"doubled": 14})

            async def async_step(msg: str) -> str:
                return msg.upper()

            self.assertEqual(run(async_step, "hello", address=self.address), "HELLO")

            def stream_numbers(count: int) -> Iterator[dict]:
                for i in range(count):
                    yield {"i": i, "sq": i * i}

            self.assertEqual(
                list(stream(stream_numbers, 3, address=self.address)),
                [{"i": 0, "sq": 0}, {"i": 1, "sq": 1}, {"i": 2, "sq": 4}],
            )

    def test_working_dir_upload_caching_and_gc_reupload(self):
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

            with RayClient(
                address=self.address, default_runtime_env=user_runtime_env
            ) as client:
                self.assertEqual(client.run(read_helper), 123)
                # Also verify module-path string entrypoints for tasks and actors:
                self.assertEqual(client.run("helper_mod:get_val", 2), 246)
                h_actor = client.actor("helper_mod:HelperActor", 10, name="h_act")
                self.assertEqual(h_actor.compute(5), 138)
                self.assertEqual(user_runtime_env, snapshot)
                self.assertEqual(len(self.state.packages), 1)

                # Simulate cluster GCS package expiration: new workspace re-uploads cleanly
                with self.state.lock:
                    self.state.packages.clear()

            with RayClient(
                address=self.address, default_runtime_env=user_runtime_env
            ) as client2:
                self.assertEqual(client2.run(read_helper), 123)
                self.assertEqual(len(self.state.packages), 1)

    def test_thread_safe_concurrent_submissions(self):
        with RayClient(address=self.address) as client:

            def square(n: int) -> int:
                return n * n

            with ThreadPoolExecutor(max_workers=4) as pool:
                futures = [
                    pool.submit(lambda v=i: client.run(square, v)) for i in range(8)
                ]
                results = [f.result() for f in futures]
            self.assertEqual(results, [i * i for i in range(8)])

    def test_remote_exception_and_sys_exit_propagate_cleanly(self):
        with RayClient(address=self.address) as client:

            def failing_task():
                raise ValueError("invalid shard configuration")

            with self.assertRaises(RayExecutionError) as ctx:
                client.run(failing_task)
            self.assertEqual(ctx.exception.exc_type, "ValueError")
            self.assertIn("invalid shard configuration", str(ctx.exception))
            restored = pickle.loads(pickle.dumps(ctx.exception))
            self.assertEqual(restored.exc_type, "ValueError")

            def exiting_task():
                import sys as _sys

                _sys.exit(2)

            with self.assertRaises(RayExecutionError) as exit_ctx:
                client.run(exiting_task)
            self.assertEqual(exit_ctx.exception.exc_type, "SystemExit")

            def clean_exit_task():
                import sys as _sys

                _sys.exit(0)

            self.assertIsNone(client.run(clean_exit_task))

    def test_retries_transient_503_rejects_ray_scheme_and_non_json_bodies(self):
        with self.assertRaises(RayClientError):
            RayClient(address="ray://127.0.0.1:10001")

        self.state.fail_next_requests = 2
        with RayClient(address=self.address) as client:
            self.assertEqual(client.list_workspaces(), [])

            def ok_fn() -> int:
                return 7

            self.assertEqual(client.run(ok_fn), 7)

        html_server = ThreadingHTTPServer(("127.0.0.1", 0), _HtmlLoginPageHandler)
        threading.Thread(target=html_server.serve_forever, daemon=True).start()
        try:
            html_address = f"http://127.0.0.1:{html_server.server_address[1]}"
            with self.assertRaisesRegex(RayClientError, "non-JSON response"):
                RayClient(address=html_address).list_workspaces()
        finally:
            html_server.shutdown()
            html_server.server_close()

    def test_live_actors_remote_refs_and_workspace_ttl(self):
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

        c1 = RayClient(address=self.address, workspace="ws_a", ttl_seconds=60.0)
        c2 = RayClient(address=self.address, workspace="ws_b", ttl_seconds=60.0)
        self.assertEqual(c1.workspace, "ws_a")
        self.assertEqual(c1.namespace, "workspace:ws_a")

        a1 = c1.actor(Accumulator, 10, name="acc")
        a2 = c2.actor(Accumulator, 100, name="acc")
        self.assertIsInstance(a1, ActorProxy)
        self.assertTrue(a1.created)

        # Reattaching to existing actor in ws_a
        a1_re = c1.get_actor("acc")
        self.assertFalse(a1_re.created)
        self.assertEqual(len(c1.list_actors()), 1)

        # In-cluster $ray_ref chaining between cell 1 and cell 2
        batch_ref = a1.make_batch.ref(3)
        self.assertIsInstance(batch_ref, RemoteRef)
        self.assertEqual(batch_ref.get(), [15, 16, 17])

        summary_a = a1_re.summarize(batch_ref, tag="a")
        self.assertEqual(summary_a, {"tag": "a", "sum": 48, "scale": 42})

        # Pass actor-created $ray_ref into a stateless workspace task (`c1.run`)
        def count_items(items: list) -> int:
            return len(items)

        self.assertEqual(c1.run(count_items, batch_ref), 3)
        self.assertTrue(batch_ref.delete())

        # Tenant B is isolated from Tenant A
        batch_ref_b = a2.make_batch.remote(2)
        summary_b = a2.summarize(batch_ref_b, tag="b")
        self.assertEqual(summary_b, {"tag": "b", "sum": 211, "scale": 42})

        # Stream method on actor
        chunks = list(a1.stream_items.stream(2))
        self.assertEqual(chunks, [{"idx": 0, "val": 15}, {"idx": 1, "val": 16}])

        self.assertTrue(a1.kill())
        self.assertTrue(c1.close_workspace())
        self.assertTrue(c2.close_workspace())

        # Disconnect TTL cleanup test
        c_ttl = RayClient(address=self.address, workspace="ws_ttl", ttl_seconds=0.15)
        c_ttl.actor(Accumulator, 1, name="short_lived")
        self.assertEqual(c_ttl.workspace_info()["workspace"], "ws_ttl")
        time.sleep(0.25)
        with RayClient(address=self.address) as probe:
            active = [w["workspace"] for w in probe.list_workspaces()]
        self.assertNotIn("ws_ttl", active)


if __name__ == "__main__":
    unittest.main()
