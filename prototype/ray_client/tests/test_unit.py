"""Offline unit and mock REST server tests for ray_client."""

import contextlib
import copy
import functools
import hashlib
import importlib
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
import warnings
import zipfile
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple
from unittest.mock import patch

import ray_client
from ray_client import (
    ActorHandle,
    JobResult,
    RayClientError,
    RayClientSerializationError,
    RayCluster,
    RayJobExecutionError,
    RayJobTimeoutError,
    packaging,
    run,
)
from ray_client._ignore import _compile_ignore_rules, _is_ignored
from ray_client.client import (
    _normalize_resources,
    _validate_get_if_exists,
    _validate_max_concurrency,
    _validate_node_id_and_label_selector,
    _validate_ttl_seconds,
)
from ray_client.packaging import (
    IGNORE_GITIGNORE_ENV_VAR,
    build_working_dir_package,
    extract_class_source_and_globals,
    extract_function_source_and_globals,
)

try:
    from ray.dashboard.modules.job.actor_manager import (
        ActorManager,
        MaxNodesExceededError,
        _ClientRootActor,
        _compute_spec_hash,
        _is_client_root_actor_handle,
        _load_user_target,
        _LogTeeStream,
        _normalize_module_source,
    )
except (ImportError, OSError):
    import ast
    import math
    from typing import Union

    ActorManager = None  # type: ignore[assignment]
    MaxNodesExceededError = ValueError  # type: ignore[assignment,misc]
    _ClientRootActor = None  # type: ignore[assignment]
    _LogTeeStream = None  # type: ignore[assignment]
    _is_client_root_actor_handle = None  # type: ignore[assignment]
    _am_path = (
        Path(__file__).resolve().parents[3]
        / "python"
        / "ray"
        / "dashboard"
        / "modules"
        / "job"
        / "actor_manager.py"
    )
    _am_tree = ast.parse(_am_path.read_text(encoding="utf-8"))
    _am_keep = {
        "_normalize_module_source",
        "_validate_resource_num",
        "_normalize_spec_resources",
        "_compute_spec_hash",
        "_load_user_target",
    }
    _am_mod = ast.Module(
        body=[
            n
            for n in _am_tree.body
            if isinstance(n, ast.FunctionDef) and n.name in _am_keep
        ],
        type_ignores=[],
    )
    _am_ns: Dict[str, Any] = {
        "Any": Any,
        "Dict": Dict,
        "Optional": Optional,
        "Tuple": Tuple,
        "Union": Union,
        "contextlib": contextlib,
        "hashlib": hashlib,
        "importlib": importlib,
        "json": json,
        "math": math,
        "os": os,
        "ray": types.SimpleNamespace(),
        "sys": sys,
        "types": types,
        "uuid": uuid,
    }
    exec(compile(_am_mod, str(_am_path), "exec"), _am_ns)
    _normalize_module_source = _am_ns["_normalize_module_source"]
    _compute_spec_hash = _am_ns["_compute_spec_hash"]
    _raw_load_user_target = _am_ns["_load_user_target"]

    def _load_user_target(*args: Any, **kwargs: Any) -> Any:
        had_ray = "ray" in sys.modules
        orig_ray = sys.modules.get("ray")
        if orig_ray is None:
            sys.modules["ray"] = types.ModuleType("ray")
        try:
            return _raw_load_user_target(*args, **kwargs)
        finally:
            if not had_ray:
                sys.modules.pop("ray", None)
            elif orig_ray is None:
                sys.modules["ray"] = None  # type: ignore[assignment]


GLOBAL_SCALE = 42


def top_level_fn(x: int, y: int = GLOBAL_SCALE) -> dict:
    return {"result": (x + y) * GLOBAL_SCALE}


class TopLevelClassWithAttrDefault:
    DEFAULT_STEP = 3
    SCALED_STEP = GLOBAL_SCALE

    def __init__(self, base: int = DEFAULT_STEP):
        self.val = base

    def step(self, delta: int = DEFAULT_STEP, mult: int = SCALED_STEP) -> int:
        self.val += delta * mult
        return self.val


class _MockClusterState:
    def __init__(self, tmp_root: str):
        self.tmp_root = Path(tmp_root)
        self.packages: Dict[str, bytes] = {}
        self.actors: Dict[str, Dict[str, Any]] = {}
        self.jobs: Dict[str, Dict[str, Any]] = {}
        self.nodes: Dict[str, Dict[str, Any]] = {
            "node_head": {
                "id": "node_head",
                "group_name": "head",
                "state": "ALIVE",
                "ip_address": "127.0.0.1",
                "resources": {"CPU": 4.0},
                "labels": {"ray.io/node-group": "head"},
                "detached": True,
            },
            "node_worker_1": {
                "id": "node_worker_1",
                "group_name": "gpu-workers",
                "state": "ALIVE",
                "ip_address": "10.0.0.2",
                "resources": {"CPU": 8.0, "GPU": 1.0},
                "labels": {"ray.io/node-group": "gpu-workers"},
                "detached": True,
            },
        }
        self.last_created_actor_spec: Optional[Dict[str, Any]] = None
        self.delete_actor_calls = 0
        self.fail_next_requests = 0
        self.drop_next_job_post_response = False
        self.job_info_gets = 0
        self.job_logs_gets = 0
        self.lock = threading.Lock()

    def _cleanup_actor_locked(self, act: Optional[Dict[str, Any]]) -> None:
        if not act or act.get("cleaned_up"):
            return
        act["cleaned_up"] = True
        inst = act.get("instance")
        if inst is None:
            return
        try:
            close_fn = getattr(inst, "close", None)
            if callable(close_fn):
                res = close_fn()
                if inspect.iscoroutine(res):
                    import asyncio

                    asyncio.run(res)
            else:
                exit_fn = getattr(inst, "__exit__", None)
                if callable(exit_fn):
                    exit_fn(None, None, None)
        except Exception:
            pass

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
                popped = self.actors.pop(name, None)
                self._cleanup_actor_locked(popped)


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
            frames.append("data: " + json.dumps({"ok": True, "done": True}) + "\n\n")
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
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
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
                        "detached": act.get("detached", True),
                        "active_calls": act.get("active_calls", 0),
                        "ttl_seconds": act["ttl_seconds"],
                        "spec_hash": act.get("spec_hash", ""),
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
                            "detached": act.get("detached", True),
                            "active_calls": act.get("active_calls", 0),
                            "ttl_seconds": act["ttl_seconds"],
                            "spec_hash": act.get("spec_hash", ""),
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

        if path == "/api/cluster/node_groups":
            with self.state.lock:
                groups: Dict[str, Dict[str, Any]] = {}
                for node in self.state.nodes.values():
                    g = node["group_name"]
                    groups.setdefault(
                        g,
                        {
                            "name": g,
                            "max_nodes": 1 if g == "head" else 10,
                            "resources": dict(node.get("resources") or {}),
                            "labels": dict(node.get("labels") or {}),
                        },
                    )
                items = list(groups.values())
            self._send_json(200, items)
            return

        if path.startswith("/api/cluster/node_groups/"):
            group = path[len("/api/cluster/node_groups/") :]
            with self.state.lock:
                matching = [
                    n for n in self.state.nodes.values() if n["group_name"] == group
                ]
            if not matching:
                self._send_json(
                    404, {"ok": False, "error": f"Node group '{group}' not found"}
                )
                return
            first = matching[0]
            self._send_json(
                200,
                {
                    "name": group,
                    "max_nodes": 1 if group == "head" else 10,
                    "resources": dict(first.get("resources") or {}),
                    "labels": dict(first.get("labels") or {}),
                },
            )
            return

        if path == "/api/cluster/nodes":
            query = dict(urllib.parse.parse_qsl(parsed.query))
            group_filter = query.get("node_group")
            state_filter = query.get("state")
            include_dead = query.get("include_dead", "false").lower() in ("1", "true")
            allowed_states = (
                {s.strip().upper() for s in state_filter.split(",") if s.strip()}
                if state_filter is not None
                else None
            )
            with self.state.lock:
                items = []
                for n in self.state.nodes.values():
                    if group_filter is not None and n["group_name"] != group_filter:
                        continue
                    n_state = str(n.get("state", "ALIVE")).upper()
                    if allowed_states is not None:
                        if n_state not in allowed_states:
                            continue
                    elif not include_dead and n_state == "DEAD":
                        continue
                    items.append(dict(n))
            self._send_json(200, items)
            return

        if path.startswith("/api/cluster/nodes/"):
            node_id = path[len("/api/cluster/nodes/") :]
            with self.state.lock:
                node = self.state.nodes.get(node_id)
            if node is None:
                self._send_json(
                    404, {"ok": False, "error": f"Node '{node_id}' not found"}
                )
                return
            self._send_json(200, dict(node))
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
                self.state.delete_actor_calls += 1
                self.state.check_ttls_locked()
                popped = self.state.actors.pop(actor_name, None)
                existed = popped is not None
                self.state._cleanup_actor_locked(popped)
            self._send_json(200 if existed else 404, {"ok": existed, "killed": existed})
            return
        if path.startswith("/api/cluster/nodes/"):
            node_id = path[len("/api/cluster/nodes/") :]
            body = self._read_json()
            reason = body.get("reason")
            if not isinstance(reason, str) or not reason.strip():
                self._send_json(
                    400,
                    {
                        "ok": False,
                        "error": {
                            "exc_type": "ValueError",
                            "exc_message": "reason must be a non-empty string.",
                        },
                    },
                )
                return
            grace_period_s = body.get("grace_period_s", 0.0)
            with self.state.lock:
                node = self.state.nodes.get(node_id)
                if node is None:
                    self._send_json(
                        404,
                        {
                            "ok": False,
                            "error": {
                                "exc_type": "KeyError",
                                "exc_message": f"Node '{node_id}' does not exist.",
                            },
                        },
                    )
                    return
                if node.get("group_name") == "head":
                    self._send_json(
                        400,
                        {
                            "ok": False,
                            "error": {
                                "exc_type": "ValueError",
                                "exc_message": "Cannot destroy the head node.",
                            },
                        },
                    )
                    return
                new_state = (
                    "TERMINATING_SOON"
                    if (
                        node.get("state") == "ALIVE"
                        and isinstance(grace_period_s, (int, float))
                        and grace_period_s > 0
                    )
                    else "DEAD"
                )
                node["state"] = new_state
                res_payload = {
                    **node,
                    "ok": True,
                    "destroyed": True,
                    "state": new_state,
                    "reason": reason,
                }
            self._send_json(200, res_payload)
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

        if path.startswith("/api/cluster/node_groups/") and path.endswith("/nodes"):
            group = path[len("/api/cluster/node_groups/") : -len("/nodes")]
            count = int(body.get("count", 1))
            with self.state.lock:
                added = []
                for _ in range(count):
                    nid = f"node_{group}_{uuid.uuid4().hex[:6]}"
                    info = {
                        "id": nid,
                        "group_name": group,
                        "state": "ALIVE",
                        "ip_address": "10.0.0.9",
                        "resources": {"CPU": 8.0},
                        "labels": {"ray.io/node-group": group},
                        "detached": True,
                    }
                    self.state.nodes[nid] = info
                    added.append(info)
            self._send_json(200, added)
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

                try:
                    if (
                        not target_name
                        or not isinstance(target_name, str)
                        or not target_name.strip()
                    ):
                        raise ValueError(
                            "Actor spec must specify 'class_name' or 'fn_name'."
                        )
                    has_explicit_ttl = "ttl_seconds" in body
                    ttl_seconds = _validate_ttl_seconds(
                        body["ttl_seconds"] if has_explicit_ttl else 1800.0
                    )
                    get_if_exists = _validate_get_if_exists(
                        body.get("get_if_exists", True)
                    )
                    _validate_node_id_and_label_selector(
                        body.get("node_id"), body.get("label_selector")
                    )
                    num_cpus, _, _, _ = _normalize_resources(
                        body.get("num_cpus"),
                        body.get("num_gpus"),
                        body.get("memory"),
                        body.get("resources"),
                        default_cpus=0 if kind == "function" else 1,
                    )
                    max_concurrency = (
                        _validate_max_concurrency(body["max_concurrency"])
                        if body.get("max_concurrency") is not None
                        else 1
                    )
                    spec_hash = _compute_spec_hash(
                        body,
                        kind,
                        target_name,
                        num_cpus,
                        max_concurrency,
                    )
                except (ValueError, TypeError) as exc:
                    self._send_json(
                        400,
                        {
                            "ok": False,
                            "error": {
                                "exc_type": type(exc).__name__,
                                "exc_message": str(exc),
                            },
                        },
                    )
                    return

                try:
                    if name in self.state.actors:
                        if get_if_exists is False:
                            raise ValueError(
                                f"Actor '{name}' already exists. Pass get_if_exists=True "
                                "or use cluster.get_actor() to reattach."
                            )
                        act = self.state.actors[name]
                        spec_mismatch = bool(
                            act.get("spec_hash") and act.get("spec_hash") != spec_hash
                        )
                        class_mismatch = bool(
                            act.get("class_name")
                            and act.get("class_name") != target_name
                        )
                        if get_if_exists == "recreate" and (
                            spec_mismatch or class_mismatch
                        ):
                            popped = self.state.actors.pop(name, None)
                            self.state._cleanup_actor_locked(popped)
                        else:
                            if class_mismatch:
                                raise ValueError(
                                    f"Actor '{name}' already exists with class "
                                    f"'{act['class_name']}', cannot reattach as "
                                    f"'{target_name}'."
                                )
                            if has_explicit_ttl:
                                act["ttl_seconds"] = ttl_seconds
                            act["last_active"] = time.monotonic()
                            self._send_json(
                                200,
                                {
                                    "ok": True,
                                    "name": name,
                                    "class_name": act["class_name"],
                                    "detached": act.get("detached", True),
                                    "created": False,
                                    "ttl_seconds": act.get("ttl_seconds"),
                                    "spec_hash": act.get("spec_hash", ""),
                                    "spec_mismatch": spec_mismatch,
                                },
                                keepalive_prefix=b"   ",
                            )
                            return

                    work_dir, env_vars = self._unpack_actor_runtime_env_locked(
                        name, body.get("runtime_env")
                    )
                    detached = bool(body.get("detached", kind != "function"))
                    act_entry: Dict[str, Any] = {
                        "name": name,
                        "kind": kind,
                        "detached": detached,
                        "class_name": target_name,
                        "spec_hash": spec_hash,
                        "ttl_seconds": ttl_seconds,
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
                            "detached": detached,
                            "created": True,
                            "ttl_seconds": ttl_seconds,
                            "spec_hash": spec_hash,
                            "spec_mismatch": False,
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

                def _finish_non_stream_call() -> None:
                    with self.state.lock:
                        act["active_calls"] = max(0, act.get("active_calls", 1) - 1)
                        act["last_active"] = time.monotonic()
                        if act.get("kind") == "function":
                            popped = self.state.actors.pop(actor_name, None)
                            self.state._cleanup_actor_locked(popped)

                stream_sent = False
                try:
                    args = body.get("args") or []
                    kwargs = body.get("kwargs") or {}

                    if act["kind"] == "function":
                        target_callable = act["fn"]
                    else:
                        if method_name.startswith("_") and method_name != "__call__":
                            raise AttributeError(
                                f"Cannot call private method '{method_name}' on actor '{actor_name}'."
                            )
                        target_callable = getattr(act["instance"], method_name)

                    out = self._exec_in_actor_env(act, target_callable, args, kwargs)
                    if inspect.isgenerator(out):
                        stream_sent = True
                        self._send_sse_stream(out, act["logs"])
                        return
                    if is_stream:
                        raise TypeError(
                            f"'{act['class_name']}.{method_name}' is not a generator function."
                        )
                    _finish_non_stream_call()
                    self._send_json(
                        200,
                        {"ok": True, "result": out},
                        keepalive_prefix=b"   ",
                    )
                except SystemExit as exc:
                    _finish_non_stream_call()
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
                    _finish_non_stream_call()
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
                    if stream_sent:
                        _finish_non_stream_call()
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

    def test_deterministic_working_dir_zip(self):
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
        for removed_alias in (
            "RayClient",
            "ActorProxy",
            "RayTimeoutError",
            "RayExecutionError",
        ):
            self.assertFalse(hasattr(ray_client, removed_alias))
        for bad_addr in ("   ", "http://", "https://", "http:///api"):
            with self.assertRaises(RayClientError):
                RayCluster(address=bad_addr)
        for bad_timeout in (None, 0, -1.0, float("nan"), float("inf"), True):
            with self.assertRaises((ValueError, TypeError)):
                RayCluster(timeout=bad_timeout)  # type: ignore[arg-type]

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
            self.assertIs(self.state.last_created_actor_spec["detached"], False)
            self.assertEqual(
                cluster.run(step, 21, timeout=5.0),
                {"doubled": 42},
            )
            # One-shot driver actors are reaped on completion
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

            def _secret(self) -> int:
                return 999

            def __call__(self, bonus: int = 0) -> int:
                return self.val + bonus

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
        # Validate client-side rejection of ttl_seconds <= 0 and invalid get_if_exists
        for bad_ttl in (0, 0.0, -5.0, float("nan"), float("inf"), True):
            with self.assertRaises(ValueError):
                cluster.actor(Accumulator, 1, ttl_seconds=bad_ttl)
        for bad_gie in ("force", 1, None):
            with self.assertRaises(ValueError):
                cluster.actor(Accumulator, 1, get_if_exists=bad_gie)  # type: ignore[arg-type]

        a1 = cluster.actor(Accumulator, 10, name="acc_a", ttl_seconds=60.0)
        self.assertIsNotNone(self.state.last_created_actor_spec)
        self.assertEqual(self.state.last_created_actor_spec["num_cpus"], 1)
        self.assertEqual(self.state.last_created_actor_spec["max_concurrency"], 1)
        self.assertIsNone(self.state.last_created_actor_spec.get("ephemeral"))
        a2 = cluster.actor(Accumulator, 100, name="acc_b", ttl_seconds=60.0)
        self.assertIsInstance(a1, ActorHandle)
        self.assertTrue(a1.created)

        # Reattaching with identical spec emits no warning
        with warnings.catch_warnings(record=True) as caught_same:
            warnings.simplefilter("always")
            a1_same = cluster.actor(Accumulator, 10, name="acc_a", ttl_seconds=60.0)
        self.assertFalse(a1_same.created)
        self.assertEqual(len(caught_same), 0)

        # Reattaching with different spec (init_val=50 vs 10) emits UserWarning
        with warnings.catch_warnings(record=True) as caught_diff:
            warnings.simplefilter("always")
            a1_mismatch = cluster.actor(
                Accumulator, 50, name="acc_a", ttl_seconds=60.0, get_if_exists=True
            )
        self.assertFalse(a1_mismatch.created)
        self.assertEqual(len(caught_diff), 1)
        self.assertIs(caught_diff[0].category, UserWarning)
        self.assertIn("differ from the requested spec", str(caught_diff[0].message))
        self.assertIn("get_if_exists='recreate'", str(caught_diff[0].message))
        # State on the cluster was preserved (10 + 5 = 15, not 50 + 5 = 55)
        self.assertEqual(a1_mismatch.make_batch(1), [15])

        # get_if_exists="recreate" replaces the actor when spec differs, and reattaches when identical
        a1_recreated = cluster.actor(
            Accumulator, 50, name="acc_a", ttl_seconds=60.0, get_if_exists="recreate"
        )
        self.assertTrue(a1_recreated.created)
        self.assertEqual(a1_recreated.make_batch(1), [55])

        a1_reattach_after = cluster.actor(
            Accumulator, 50, name="acc_a", ttl_seconds=60.0, get_if_exists="recreate"
        )
        self.assertFalse(a1_reattach_after.created)
        # Switch back to init_val=10 for remaining checks
        a1 = cluster.actor(
            Accumulator, 10, name="acc_a", ttl_seconds=60.0, get_if_exists="recreate"
        )
        self.assertTrue(a1.created)

        # Reattaching to existing actor via get_actor
        a1_re = cluster.get_actor("acc_a")
        self.assertFalse(a1_re.created)
        self.assertEqual(len(cluster.list_actors()), 2)

        # __call__ is allowed, while private _secret is rejected with AttributeError
        self.assertEqual(a1.call("__call__", 5), 20)
        with self.assertRaises(RayJobExecutionError) as priv_ctx:
            a1.call("_secret")
        self.assertEqual(priv_ctx.exception.exc_type, "AttributeError")
        self.assertIn("Cannot call private method '_secret'", str(priv_ctx.exception))

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

        # ActorHandle context manager (`with cluster.actor(...) as ctx_act:`) kills actor on exit
        with cluster.actor(Accumulator, 7, name="scoped_ctx_actor") as ctx_act:
            self.assertEqual(ctx_act.make_batch(1), [12])
            self.assertIn(
                "scoped_ctx_actor", [a["name"] for a in cluster.list_actors()]
            )
        self.assertNotIn("scoped_ctx_actor", [a["name"] for a in cluster.list_actors()])

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

        fn_name, mod_src, captured = extract_function_source_and_globals(annotated_fn)
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
            cls_name, cls_src, cls_captured = extract_class_source_and_globals(nb_cls)
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

            # 5. P9, P15 & Finding 1.3: @ray.remote options inheritance and ttl_seconds preserved on reattach
            try:
                import ray  # type: ignore[import-not-found]
            except (ImportError, OSError):

                def _fake_ray_remote(**opts: Any) -> Any:
                    def _wrap(cls: type) -> Any:
                        return types.SimpleNamespace(
                            _default_options=dict(opts),
                            __ray_metadata=None,
                            __ray_metadata__=types.SimpleNamespace(
                                modified_class=types.SimpleNamespace(
                                    __ray_actor_class__=cls
                                )
                            ),
                        )

                    return _wrap

                ray = types.SimpleNamespace(remote=_fake_ray_remote)  # type: ignore[assignment]

            @ray.remote(
                num_cpus=2,
                num_gpus=1,
                max_concurrency=4,
                runtime_env={
                    "env_vars": {"FAKE_HEADER": "class DecoratedActor:\n@ray.remote"}
                },
            )
            class DecoratedActor:
                def ping(self) -> str:
                    return "pong"

            dec_handle = cluster.actor(DecoratedActor, name="dec_act", ttl_seconds=None)
            self.assertEqual(self.state.last_created_actor_spec["num_cpus"], 2)
            self.assertEqual(self.state.last_created_actor_spec["num_gpus"], 1)
            self.assertEqual(self.state.last_created_actor_spec["max_concurrency"], 4)
            self.assertIn("ttl_seconds", self.state.last_created_actor_spec)
            self.assertIsNone(self.state.last_created_actor_spec["ttl_seconds"])
            self.assertEqual(dec_handle.ping(), "pong")
            self.assertIsNone(dec_handle.info()["ttl_seconds"])

            # Reattaching without passing ttl_seconds omits "ttl_seconds" from spec and preserves None
            dec_reattached = cluster.actor(DecoratedActor, name="dec_act")
            self.assertFalse(dec_reattached.created)
            self.assertNotIn("ttl_seconds", self.state.last_created_actor_spec)
            self.assertIsNone(dec_reattached.info()["ttl_seconds"])

            # Reattaching with explicit ttl_seconds=120.0 updates TTL; subsequent reattach without ttl_seconds preserves 120.0
            dec_updated = cluster.actor(
                DecoratedActor, name="dec_act", ttl_seconds=120.0
            )
            self.assertFalse(dec_updated.created)
            self.assertEqual(self.state.last_created_actor_spec["ttl_seconds"], 120.0)
            self.assertEqual(dec_updated.info()["ttl_seconds"], 120.0)

            dec_preserved_120 = cluster.actor(DecoratedActor, name="dec_act")
            self.assertFalse(dec_preserved_120.created)
            self.assertNotIn("ttl_seconds", self.state.last_created_actor_spec)
            self.assertEqual(dec_preserved_120.info()["ttl_seconds"], 120.0)
            dec_handle.kill()

            # Finding 1.1: Top-level class and nested class using class attributes in method defaults
            top_cls_handle = cluster.actor(
                TopLevelClassWithAttrDefault, name="top_cls_def"
            )
            self.assertEqual(top_cls_handle.step(), 3 + 3 * 42)
            self.assertEqual(top_cls_handle.step(2, 10), 3 + 3 * 42 + 20)
            top_cls_handle.kill()

            class NestedWithValidClassAttrDefault:
                STEP = 4
                SCALED = GLOBAL_SCALE

                def __init__(self, base: int = STEP):
                    self.v = base

                def add(self, delta: int = STEP, mult: int = SCALED) -> int:
                    return self.v + delta * mult

            nested_cls_handle = cluster.actor(
                NestedWithValidClassAttrDefault, name="nested_cls_def"
            )
            self.assertEqual(nested_cls_handle.add(), 4 + 4 * 42)
            nested_cls_handle.kill()

            def make_nested_shadowed_class_attr_default():
                GLOBAL_SCALE = 999  # shadows module-level GLOBAL_SCALE

                class ShadowedAttrDefault:
                    STEP = GLOBAL_SCALE

                    def run(self, x: int = STEP) -> int:
                        return x

                return ShadowedAttrDefault

            with self.assertRaisesRegex(
                RayClientSerializationError, "enclosing function or class scope"
            ):
                extract_class_source_and_globals(
                    make_nested_shadowed_class_attr_default()
                )

            class NestedComplexClassBody:
                STEP = 1
                STEP: int
                STEP = GLOBAL_SCALE
                STEP += 1
                PAIR_A, PAIR_B = 5, GLOBAL_SCALE
                DYNAMIC_TAG = str(GLOBAL_SCALE)
                DERIVED_TAG = DYNAMIC_TAG + "_ok"
                TEMP = 99
                del TEMP
                if GLOBAL_SCALE > 0:
                    MODE = "pos"
                else:
                    MODE = "neg"

                def overwritten(self) -> int:
                    return 1

                OVERWRITTEN_TUP = (overwritten, 10)

                def overwritten(self) -> int:  # noqa: F811
                    return 2

                def helper(self, x: int) -> int:
                    return x + self.STEP

                ALIAS = helper

                def compute(
                    self,
                    val: int = PAIR_A,
                    mult: int = PAIR_B,
                    fn: Any = helper,
                    tag: str = DERIVED_TAG,
                    mode: str = MODE,
                ) -> Dict[str, Any]:
                    return {
                        "total": fn(self, val * mult) + self.overwritten(),
                        "tag": tag,
                        "mode": mode,
                    }

            complex_handle = cluster.actor(
                NestedComplexClassBody, name="nested_complex_cls"
            )
            self.assertEqual(
                complex_handle.compute(),
                {"total": (5 * 42) + 43 + 2, "tag": "42_ok", "mode": "pos"},
            )
            complex_handle.kill()

            ext_mod_obj = types.ModuleType("_ray_test_ext_base_mod")
            ext_base_src = (
                "GLOBAL_SCALE = -999\n"
                "class ExtBase:\n"
                "    def base_val(self) -> int:\n"
                "        return GLOBAL_SCALE\n"
            )
            linecache.cache["<_ray_test_ext_base_mod>"] = (
                len(ext_base_src),
                None,
                ext_base_src.splitlines(keepends=True),
                "<_ray_test_ext_base_mod>",
            )
            exec(
                compile(ext_base_src, "<_ray_test_ext_base_mod>", "exec"),
                ext_mod_obj.__dict__,
            )
            sys.modules["_ray_test_ext_base_mod"] = ext_mod_obj
            globals()["ext_base_mod"] = ext_mod_obj
            try:

                class SubOfExternalBase(ext_base_mod.ExtBase):  # noqa: F821
                    def get_scale(self) -> int:
                        return GLOBAL_SCALE

                _, _, sub_captured = extract_class_source_and_globals(SubOfExternalBase)
                self.assertEqual(sub_captured.get("GLOBAL_SCALE"), GLOBAL_SCALE)
            finally:
                globals().pop("ext_base_mod", None)
                sys.modules.pop("_ray_test_ext_base_mod", None)
                linecache.cache.pop("<_ray_test_ext_base_mod>", None)

            def make_nested_shadowed_tuple_unpack():
                GLOBAL_SCALE = 777

                class ShadowedTupleActor:
                    A, B = 1, GLOBAL_SCALE

                    def get(self) -> int:
                        return self.A + self.B

                return ShadowedTupleActor

            with self.assertRaisesRegex(
                RayClientSerializationError, "enclosing function or class scope"
            ):
                extract_class_source_and_globals(make_nested_shadowed_tuple_unpack())

            def make_nested_shadowed_property_setter():
                GLOBAL_SCALE = 888

                class ShadowedPropSetterActor:
                    @property
                    def val(self) -> int:
                        return 1

                    @val.setter
                    def val(self, v: int = GLOBAL_SCALE) -> None:
                        self._v = v

                return ShadowedPropSetterActor

            with self.assertRaisesRegex(
                RayClientSerializationError, "enclosing function or class scope"
            ):
                extract_class_source_and_globals(make_nested_shadowed_property_setter())

            # Shadowed enclosing variables inside function-call expressions are caught
            def make_nested_shadowed_call_default():
                GLOBAL_SCALE = -500

                def fn_call_def(x: int = abs(GLOBAL_SCALE)) -> int:
                    return x

                class ShadowedCallAttrActor:
                    TAG = str(GLOBAL_SCALE)

                    def get(self) -> str:
                        return self.TAG

                return fn_call_def, ShadowedCallAttrActor

            bad_call_fn, bad_call_cls = make_nested_shadowed_call_default()
            with self.assertRaisesRegex(
                RayClientSerializationError, "enclosing function or class scope"
            ):
                extract_function_source_and_globals(bad_call_fn)
            with self.assertRaisesRegex(
                RayClientSerializationError, "enclosing function or class scope"
            ):
                extract_class_source_and_globals(bad_call_cls)

            # Mutating a class attribute on a nested class before extraction does not falsely fail
            class MutatedAttrActor:
                COUNT = 0

                def next_count(self) -> int:
                    return self.COUNT + GLOBAL_SCALE

            MutatedAttrActor.COUNT += 5
            _, _, mut_captured = extract_class_source_and_globals(MutatedAttrActor)
            self.assertEqual(mut_captured, {"GLOBAL_SCALE": 42})

            # Class-body comprehension captures module global even when class defines same-named attr
            class CompBodyActor:
                GLOBAL_SCALE = 10
                ITEMS = [i + GLOBAL_SCALE for i in [1, 2]]

                def get_items(self) -> List[int]:
                    return self.ITEMS

            _, _, comp_captured = extract_class_source_and_globals(CompBodyActor)
            self.assertEqual(comp_captured, {"GLOBAL_SCALE": 42})

            # Enclosing comprehension loop variable does not falsely shadow module global in default arg
            def make_fn_after_comp():
                _ = [GLOBAL_SCALE + 1 for GLOBAL_SCALE in [1, 2]]

                def fn_after_comp(x: int = GLOBAL_SCALE) -> int:
                    return x

                return fn_after_comp

            _, _, after_comp_captured = extract_function_source_and_globals(
                make_fn_after_comp()
            )
            self.assertEqual(after_comp_captured, {"GLOBAL_SCALE": 42})

            # Self-referential unquoted return annotation on actor method (e.g., re-run notebook cell) emits _RayHint
            SelfAnnotatedActor: Any = object()

            class SelfAnnotatedActor:
                def clone(self) -> SelfAnnotatedActor:
                    return self

            _, self_ann_src, _ = extract_class_source_and_globals(SelfAnnotatedActor)
            self.assertIn("SelfAnnotatedActor = _RayHint", self_ann_src)

            # Methodless actor class in a module with `from __future__ import annotations` preserves future import
            future_cell_file = "<ipython-input-future-methodless>"
            future_cell_src = (
                "from __future__ import annotations\n"
                "class MethodlessFutureActor:\n"
                "    pending: UndefinedFutureType\n"
            )
            linecache.cache[future_cell_file] = (
                len(future_cell_src),
                None,
                future_cell_src.splitlines(keepends=True),
                future_cell_file,
            )
            future_mod = types.ModuleType("_test_future_methodless_mod")
            future_mod.__file__ = future_cell_file
            sys.modules[future_mod.__name__] = future_mod
            try:
                exec(
                    compile(future_cell_src, future_cell_file, "exec"),
                    future_mod.__dict__,
                )
                _, mless_src, _ = extract_class_source_and_globals(
                    future_mod.MethodlessFutureActor
                )
                self.assertIn("from __future__ import annotations", mless_src)
            finally:
                sys.modules.pop(future_mod.__name__, None)
                linecache.cache.pop(future_cell_file, None)

            # Conditionally defined / redefined class name extracts the active class definition
            if False:

                class RedefinedActor:
                    def v1(self) -> int:
                        return 1

            else:

                class RedefinedActor:
                    def v2(self) -> int:
                        return 2

            _, redef_src, _ = extract_class_source_and_globals(RedefinedActor)
            self.assertIn("def v2", redef_src)
            self.assertNotIn("def v1", redef_src)

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

            # 8. Legacy add_worker_group and scale_worker_group are removed in favor of
            #    Ray 3.0 node management (add_nodes / destroy_node), and runtime_env
            #    image_uri/container error message is accurate.
            self.assertFalse(hasattr(cluster, "add_worker_group"))
            self.assertFalse(hasattr(cluster, "scale_worker_group"))
            for bad_group in ("", "   ", 123):
                with self.assertRaises(ValueError):
                    cluster.add_nodes(bad_group, count=1)  # type: ignore[arg-type]
            for bad_count in (0, -1, True, 1.5, "2"):
                with self.assertRaises(ValueError):
                    cluster.add_nodes("workers", count=bad_count)  # type: ignore[arg-type]
            for bad_env_key in ("image_uri", "container"):
                with self.assertRaisesRegex(
                    RayClientError,
                    f"runtime_env\\[{bad_env_key!r}\\] is not supported; configure the container image on the cluster\\.",
                ):
                    cluster._prepare_runtime_env({bad_env_key: "rayproject/ray:latest"})

        # 9. Validation helpers (_validate_ttl_seconds, _validate_get_if_exists)
        for bad_ttl in (0, 0.0, -1.0, float("nan"), float("inf"), True):
            with self.assertRaises(ValueError):
                _validate_ttl_seconds(bad_ttl)
        self.assertIsNone(_validate_ttl_seconds(None))
        self.assertEqual(_validate_ttl_seconds(1.5), 1.5)

        for good_gie in (True, False, "recreate"):
            self.assertEqual(_validate_get_if_exists(good_gie), good_gie)
        for bad_gie in ("force", 1, 0, None):
            with self.assertRaises(ValueError):
                _validate_get_if_exists(bad_gie)

    def test_server_actor_manager_internals(self) -> None:
        if _ClientRootActor is None or _LogTeeStream is None:
            self.skipTest("ray not installed in client-only environment")

        from collections import deque

        buf: deque = deque()
        tee = _LogTeeStream(sys.__stdout__, buf, threading.Lock())
        self.assertTrue(tee.writable())
        self.assertIsInstance(tee.encoding, str)
        tee.write("0123456789abcdef")
        self.assertEqual("".join(buf), "0123456789abcdef")

        # Verify _ClientRootActor catches SystemExit in async methods and generators without crashing,
        # does not evaluate @property getters when scanning for generator methods during __init__,
        # cancels async coroutines / async generators when _is_current_task_canceled() or cancel_stream() triggers,
        # rejects private _method calls while allowing __call__, invokes user close() / __exit__() once on shutdown,
        # and _install_ray_hooks isolates child actors in f"ray_client_{name}" while falling back
        # to "ray_client" on ray.get_actor(name).
        import ray
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
        orig_remove_pg = ray.util.remove_placement_group
        orig_rt_ns = RuntimeContext.__dict__.get("namespace")

        recorded_remote_opts: List[Dict[str, Any]] = []
        recorded_get_actor_calls: List[Tuple[str, Optional[str]]] = []
        removed_pgs: List[Any] = []

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
            "import asyncio\n"
            "EVENTS = []\n"
            "class AsyncExitActor:\n"
            "    @property\n"
            "    def boom_prop(self):\n"
            "        raise RuntimeError('property getter should not run during __init__')\n"
            "    def _private_helper(self):\n"
            "        return 'secret'\n"
            "    def __call__(self, x: int) -> int:\n"
            "        return x + 1\n"
            "    def read_events(self):\n"
            "        return list(EVENTS)\n"
            "    async def exit_zero(self):\n"
            "        raise SystemExit(0)\n"
            "    async def exit_nonzero(self):\n"
            "        raise SystemExit(3)\n"
            "    async def cancellable_sleep(self):\n"
            "        try:\n"
            "            await asyncio.sleep(10.0)\n"
            "        except asyncio.CancelledError:\n"
            "            EVENTS.append('coro_cancelled_swallowed')\n"
            "            return 'swallowed_value'\n"
            "    async def cancellable_async_gen(self):\n"
            "        try:\n"
            "            yield {'step': 0}\n"
            "            await asyncio.sleep(10.0)\n"
            "        except asyncio.CancelledError:\n"
            "            EVENTS.append('agen_cancelled')\n"
            "            raise\n"
            "    def gen_exit(self):\n"
            "        yield {'step': 1}\n"
            "        raise SystemExit(4)\n"
            "    def gen_exit_on_close(self):\n"
            "        try:\n"
            "            yield {'step': 0}\n"
            "            yield {'step': 1}\n"
            "        finally:\n"
            "            raise SystemExit(5)\n"
            "    async def close(self):\n"
            "        EVENTS.append('async_closed')\n"
        )
        exit_ctx_src = (
            "import time\n"
            "CTX_EVENTS = []\n"
            "class ContextExitActor:\n"
            "    def __exit__(self, exc_type, exc_val, exc_tb):\n"
            "        time.sleep(0.12)\n"
            "        CTX_EVENTS.append(('exit', exc_type, exc_val, exc_tb))\n"
        )
        failing_init_src = (
            "import ray\n"
            "class FailingInitActor:\n"
            "    def __init__(self):\n"
            "        print('init_log_before_crash', flush=True)\n"
            "        ray.util.placement_group([{'CPU': 1}])\n"
            "        raise RuntimeError('boom in __init__')\n"
        )
        root_act = None
        ctx_act = None
        fail_init_act = None
        try:
            ray.get_actor = fake_get_actor
            ActorClass._remote = fake_actor_remote
            ray.util.placement_group = lambda *a, **kw: ("pg_created", a, kw)
            ray.util.remove_placement_group = lambda pg: removed_pgs.append(pg)

            # Finding 1.5: _ClientRootActor.__init__ catches user exceptions, records logs in _init_error,
            # and removes any non-detached placement groups created before __init__ failed.
            fail_init_act = _ClientRootActor(
                {
                    "name": "test_fail_init",
                    "kind": "class",
                    "class_name": "FailingInitActor",
                    "module_source": failing_init_src,
                    "ttl_seconds": None,
                }
            )
            fail_info = fail_init_act.get_info()
            self.assertFalse(fail_info["ok"])
            self.assertEqual(fail_info["error"]["exc_type"], "RuntimeError")
            self.assertIn("boom in __init__", fail_info["error"]["exc_message"])
            self.assertIn("init_log_before_crash", fail_info.get("logs", ""))
            self.assertEqual(len(removed_pgs), 1)

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

            # Private method rejection vs __call__ allowance
            res_call = root_act.invoke("__call__", [41], {})
            self.assertEqual(res_call, {"ok": True, "result": 42})
            res_priv = root_act.invoke("_private_helper", [], {})
            self.assertFalse(res_priv["ok"])
            self.assertEqual(res_priv["error"]["exc_type"], "AttributeError")
            self.assertIn(
                "Cannot call private method '_private_helper'",
                res_priv["error"]["exc_message"],
            )

            # Async coroutine cancellation when _is_current_task_canceled() becomes True
            # (even when the coroutine catches CancelledError and returns a fallback value)
            cancel_flag = [False]
            with patch.object(
                root_act,
                "_is_current_task_canceled",
                side_effect=lambda: cancel_flag[0],
            ):
                t_cancel = threading.Timer(
                    0.06, lambda: cancel_flag.__setitem__(0, True)
                )
                t_cancel.start()
                t0 = time.monotonic()
                res_canc = root_act.invoke("cancellable_sleep", [], {})
                elapsed = time.monotonic() - t0
                t_cancel.cancel()
            self.assertLess(elapsed, 2.0)
            self.assertFalse(res_canc["ok"])
            self.assertIn(
                "coro_cancelled_swallowed",
                root_act.invoke("read_events", [], {})["result"],
            )

            # Async generator cancellation when cancel_stream() is called while awaiting
            agen_iter = root_act.invoke_stream(
                "cancellable_async_gen", [], {}, "stream_cancel_1"
            )
            first_frame = next(agen_iter)
            self.assertEqual(first_frame, {"ok": True, "item": {"step": 0}})
            t_stream_cancel = threading.Timer(
                0.06, lambda: root_act.cancel_stream("stream_cancel_1")
            )
            t_stream_cancel.start()
            t0 = time.monotonic()
            remaining_frames = list(agen_iter)
            elapsed_stream = time.monotonic() - t0
            t_stream_cancel.cancel()
            self.assertLess(elapsed_stream, 2.0)
            self.assertEqual(remaining_frames, [])
            self.assertIn(
                "agen_cancelled",
                root_act.invoke("read_events", [], {})["result"],
            )

            gen_frames = list(root_act.invoke_stream("gen_exit", [], {}, "s1"))
            self.assertEqual(gen_frames[0], {"ok": True, "item": {"step": 1}})
            self.assertFalse(gen_frames[1]["ok"])
            self.assertEqual(gen_frames[1]["error"]["exc_type"], "SystemExit")

            # Finding 1.6: Generator raising SystemExit inside finally on close() still decrements _active_calls
            close_exit_iter = root_act.invoke_stream(
                "gen_exit_on_close", [], {}, "s_close_exit"
            )
            self.assertEqual(next(close_exit_iter), {"ok": True, "item": {"step": 0}})
            close_exit_iter.close()
            self.assertEqual(root_act._active_calls, 0)

            # Verify _ClientRootActor.__repr__ returns class name by default and custom __repr__ when defined
            self.assertEqual(repr(root_act), "AsyncExitActor")

            # Verify `from ray.util.placement_group import placement_group` is hooked
            from ray.util.placement_group import placement_group as imported_pg

            self.assertIs(imported_pg, ray.util.placement_group)

            # Verify shutdown(only_if_idle=True) checks _active_calls and elapsed >= ttl atomically
            root_act._ttl_seconds = 10.0
            root_act._last_active_mono = time.monotonic()
            self.assertEqual(
                root_act.shutdown(only_if_idle=True), {"ok": True, "reaped": False}
            )
            self.assertFalse(root_act._shutting_down)

            root_act._last_active_mono = time.monotonic() - 20.0
            root_act._active_calls = 1
            self.assertEqual(
                root_act.shutdown(only_if_idle=True), {"ok": True, "reaped": False}
            )
            self.assertFalse(root_act._shutting_down)
            root_act._active_calls = 0

            # Verify async close() runs exactly once across multiple _cleanup_resources() / shutdown() calls
            mod_events = sys.modules[root_act._instance.__class__.__module__].EVENTS
            self.assertEqual(
                root_act.shutdown(only_if_idle=True), {"ok": True, "reaped": True}
            )
            root_act.shutdown()
            self.assertEqual(mod_events.count("async_closed"), 1)

            # Once _shutting_down is True, get_info(), set_ttl(), and _begin_call() / invoke() reject calls
            with self.assertRaisesRegex(RuntimeError, "is shutting down"):
                root_act.get_info()
            with self.assertRaisesRegex(RuntimeError, "is shutting down"):
                root_act.set_ttl(60.0)
            with self.assertRaisesRegex(RuntimeError, "is shutting down"):
                root_act.invoke("__call__", [1], {})

            # Verify __exit__(None, None, None) fallback when close() is not defined,
            # and verify a concurrent shutdown() call waits until _cleanup_resources() completes
            ctx_act = _ClientRootActor(
                {
                    "name": "test_ctx_exit",
                    "kind": "class",
                    "class_name": "ContextExitActor",
                    "module_source": exit_ctx_src,
                    "ttl_seconds": None,
                }
            )
            ctx_mod_events = sys.modules[
                ctx_act._instance.__class__.__module__
            ].CTX_EVENTS
            t_bg_cleanup = threading.Thread(target=ctx_act._cleanup_resources)
            t_bg_cleanup.start()
            time.sleep(0.02)
            # Calling shutdown() while _cleanup_resources() is sleeping inside __exit__
            # must block until __exit__ has appended to CTX_EVENTS
            ctx_act.shutdown()
            self.assertEqual(ctx_mod_events, [("exit", None, None, None)])
            t_bg_cleanup.join(timeout=1.0)
        finally:
            if ctx_act is not None:
                ctx_act.shutdown()
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
            ray.util.remove_placement_group = orig_remove_pg
            pg_mod = sys.modules.get("ray.util.placement_group")
            if pg_mod is not None:
                pg_mod.placement_group = orig_pg
                pg_mod.remove_placement_group = orig_remove_pg
            if orig_rt_ns is not None:
                RuntimeContext.namespace = orig_rt_ns

    def test_lifetimes_observability_and_edge_case_fixes(self) -> None:
        # 1. Bug 3: _compute_spec_hash normalizes leading blank lines and pad_lines after header,
        #    and includes memory + label_selector
        header = "import ray  # noqa: F401\nimport math\n"
        body = "class MyWorker:\n    def run(self):\n        return math.ceil(1.2)\n"
        spec_pad_1 = {"module_source": header + "\n" + body}
        spec_pad_20 = {"module_source": "\n\n" + header + ("\n" * 20) + body}
        h1 = _compute_spec_hash(spec_pad_1, "class", "MyWorker", 1, 10)
        h2 = _compute_spec_hash(spec_pad_20, "class", "MyWorker", 1, 10)
        self.assertEqual(h1, h2)

        h_mem = _compute_spec_hash(
            {**spec_pad_1, "memory": 1024 * 1024}, "class", "MyWorker", 1, 10
        )
        h_lbl = _compute_spec_hash(
            {**spec_pad_1, "label_selector": {"accel": "tpu"}},
            "class",
            "MyWorker",
            1,
            10,
        )
        self.assertNotEqual(h1, h_mem)
        self.assertNotEqual(h1, h_lbl)
        self.assertNotEqual(h_mem, h_lbl)

        # 2. Item 1: cluster.run() uses detached=False and makes 0 redundant DELETE calls
        #    on normal non-streaming completion, while early SSE close DOES send DELETE.
        with RayCluster(address=self.address) as cluster:
            with self.state.lock:
                self.state.delete_actor_calls = 0
            out = cluster.run(top_level_fn, 3, kwargs={"y": 4})
            self.assertEqual(out, {"result": 294})
            self.assertIs(self.state.last_created_actor_spec["detached"], False)
            self.assertNotIn("ephemeral", self.state.last_created_actor_spec)
            self.assertEqual(self.state.delete_actor_calls, 0)
            self.assertEqual(cluster.list_actors(), [])

            # When POST /api/actors returns a JSON error (already reaped by server),
            # run() does not send a redundant DELETE /api/actors/{name}.
            with self.state.lock:
                self.state.delete_actor_calls = 0
            with self.assertRaises(RayJobExecutionError):
                cluster.run("nonexistent_module_xyz:missing_fn")
            self.assertEqual(self.state.delete_actor_calls, 0)

            def stream_three():
                yield {"i": 0}
                yield {"i": 1}
                yield {"i": 2}

            with self.state.lock:
                self.state.delete_actor_calls = 0
            self.assertEqual(
                list(cluster.run(stream_three)),
                [{"i": 0}, {"i": 1}, {"i": 2}],
            )
            self.assertEqual(self.state.delete_actor_calls, 0)

            gen = cluster.run(stream_three)
            self.assertEqual(next(gen), {"i": 0})
            gen.close()
            self.assertGreaterEqual(self.state.delete_actor_calls, 1)

            # cluster.actor() defaults to detached=True and supports detached=False
            class SimpleSvc:
                def __repr__(self) -> str:
                    return "SimpleSvc(custom=1)"

                def ping(self) -> str:
                    return "ok"

            h_det = cluster.actor(SimpleSvc, name="svc_det")
            self.assertIs(self.state.last_created_actor_spec["detached"], True)
            self.assertIs(h_det.info()["detached"], True)
            h_det.kill()

            h_owned = cluster.actor(SimpleSvc, name="svc_owned", detached=False)
            self.assertIs(self.state.last_created_actor_spec["detached"], False)
            self.assertIs(h_owned.info()["detached"], False)
            h_owned.kill()

            with self.assertRaises(ValueError):
                cluster.actor(SimpleSvc, detached="yes")  # type: ignore[arg-type]

            # Short timeout is NOT floored at self._timeout (30.0s) and is sent in POST /api/actors
            recorded_timeouts: List[Any] = []
            orig_req = cluster.request

            def spy_request(method: str, endpoint: str, **kw: Any) -> Any:
                recorded_timeouts.append((method, endpoint, kw.get("timeout")))
                return orig_req(method, endpoint, **kw)

            with patch.object(cluster, "request", side_effect=spy_request):
                cluster.run(top_level_fn, 1, timeout=0.5)
            post_actor_call = [
                t for m, e, t in recorded_timeouts if m == "POST" and e == "/api/actors"
            ]
            self.assertEqual(post_actor_call, [(5.5, 5.5)])
            self.assertEqual(self.state.last_created_actor_spec.get("timeout"), 0.5)

            # Finding 1.2: _deploy_actor_spec enforces monotonic deadline even when server trickles keepalive spaces
            class _SlowTrickleCreateHandler(BaseHTTPRequestHandler):
                def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
                    return

                def do_POST(self) -> None:  # noqa: N802
                    length = int(self.headers.get("Content-Length", "0"))
                    if length > 0:
                        _ = self.rfile.read(length)
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    for _ in range(100):
                        try:
                            self.wfile.write(b" ")
                            self.wfile.flush()
                            time.sleep(0.05)
                        except Exception:
                            return

            trickle_srv = ThreadingHTTPServer(
                ("127.0.0.1", 0), _SlowTrickleCreateHandler
            )
            threading.Thread(target=trickle_srv.serve_forever, daemon=True).start()
            try:
                trickle_addr = f"http://127.0.0.1:{trickle_srv.server_address[1]}"
                with RayCluster(address=trickle_addr) as trickle_cluster, patch(
                    "ray_client.client.time.monotonic",
                    side_effect=[0.0, 0.0, 10.0, 10.0],
                ):
                    with self.assertRaises(RayJobTimeoutError):
                        trickle_cluster.actor(SimpleSvc, name="slow_act", timeout=0.1)
            finally:
                trickle_srv.shutdown()
                trickle_srv.server_close()

        if ActorManager is None or _ClientRootActor is None:
            return

        # 3. Item 2 & Bugs 1, 2, 5 & Findings 1.5, 1.7, 1.8: Test ActorManager create_actor timeout
        #    (including name_lock contention), CancelledError cleanup, kill_actor non-blocking during
        #    stuck creation, _lookup_actor non-_ClientRootActor and _creating_handles check,
        #    _init_error cleanup, _name_locks pruning, and RAY_BACKEND=rust fallbacks.
        import asyncio

        async def _run_async_checks() -> None:
            mgr = ActorManager()
            killed_handles: List[Tuple[Any, bool]] = []
            destroyed_handles: List[Tuple[Any, str]] = []
            recorded_invoke_names: List[str] = []
            recorded_stream_names: List[str] = []
            raise_options_not_impl = [False]
            raise_kill_not_impl = [False]

            class _FakeMethod:
                def __init__(self, fn, name_sink: Optional[List[str]] = None):
                    self._fn = fn
                    self._name_sink = name_sink

                def options(self, **opts: Any) -> "_FakeMethod":
                    if raise_options_not_impl[0]:
                        raise NotImplementedError(
                            "options not supported on Rust handle"
                        )
                    if self._name_sink is not None and "name" in opts:
                        self._name_sink.append(opts["name"])
                    return self

                def remote(self, *args: Any, **kwargs: Any) -> Any:
                    return self._fn(*args, **kwargs)

            class _FakeRootHandle:
                def __init__(
                    self, mode: str = "ready", cls_desc: str = "_ClientRootActor"
                ):
                    self._ray_actor_creation_function_descriptor = (
                        types.SimpleNamespace(class_name=cls_desc)
                    )
                    self._mode = mode
                    self.reap_allowed = True
                    self.unblock_event = asyncio.Event()
                    self.get_info = _FakeMethod(self._get_info)
                    self.shutdown = _FakeMethod(self._shutdown)
                    self.invoke = _FakeMethod(self._invoke, recorded_invoke_names)
                    self.invoke_stream = _FakeMethod(
                        self._invoke_stream, recorded_stream_names
                    )
                    self.cancel_stream = _FakeMethod(lambda *_a, **_kw: None)

                def __ray_destroy__(self, reason: str = "") -> None:
                    destroyed_handles.append((self, reason))
                    self.unblock_event.set()

                async def _get_info(self) -> Dict[str, Any]:
                    if self._mode == "hang":
                        await self.unblock_event.wait()
                    if self._mode == "init_error":
                        return {
                            "ok": False,
                            "error": {
                                "exc_type": "ValueError",
                                "exc_message": "bad actor init arg",
                            },
                            "logs": "init log line\n",
                        }
                    return {
                        "ok": True,
                        "name": "test_act",
                        "class_name": "MyService",
                        "kind": "class",
                        "namespace": "ray_client",
                        "status": "ALIVE",
                        "detached": True,
                        "generator_methods": ["stream_vals"],
                    }

                async def _shutdown(self, only_if_idle: bool = False) -> Dict[str, Any]:
                    if self._mode == "hang_shutdown":
                        await self.unblock_event.wait()
                    if only_if_idle and not self.reap_allowed:
                        return {"ok": True, "reaped": False}
                    return {"ok": True, "reaped": True}

                async def _invoke(
                    self, method_name: str, args: list, kwargs: dict
                ) -> Dict[str, Any]:
                    return {"ok": True, "result": {"method": method_name, "args": args}}

                def _invoke_stream(
                    self, method_name: str, args: list, kwargs: dict, stream_id: str
                ):
                    async def _agen():
                        fut: asyncio.Future = asyncio.Future()
                        fut.set_result({"ok": True, "item": 1})
                        yield fut

                    return _agen()

            created_handles: List[_FakeRootHandle] = []
            current_mode = ["hang"]
            recorded_actor_options: List[Dict[str, Any]] = []

            class _FakeRemoteActorClass:
                def options(self, **opts: Any) -> Any:
                    recorded_actor_options.append(dict(opts))

                    def _remote(_spec: Dict[str, Any]) -> _FakeRootHandle:
                        h = _FakeRootHandle(mode=current_mode[0])
                        created_handles.append(h)
                        return h

                    return types.SimpleNamespace(remote=_remote)

            def _fake_ray_kill(handle: Any, no_restart: bool = True) -> None:
                if raise_kill_not_impl[0]:
                    raise NotImplementedError("ray.kill not implemented on Rust handle")
                killed_handles.append((handle, no_restart))
                if isinstance(handle, _FakeRootHandle):
                    handle.unblock_event.set()

            user_child_handle = _FakeRootHandle(
                mode="ready", cls_desc="user_mod.UserChildActor"
            )
            user_child_alive = [True]
            creating_ready_handle = _FakeRootHandle(mode="ready")

            def _fake_ray_get_actor(name: str, namespace: Optional[str] = None) -> Any:
                if name == "user_detached_child" and user_child_alive[0]:
                    return user_child_handle
                if name == "in_progress_actor":
                    return creating_ready_handle
                raise ValueError(f"Actor {name} not found")

            with patch(
                "ray.dashboard.modules.job.actor_manager._RemoteClientRootActor",
                new=_FakeRemoteActorClass(),
            ), patch(
                "ray.dashboard.modules.job.actor_manager.ray.kill",
                side_effect=_fake_ray_kill,
            ), patch(
                "ray.dashboard.modules.job.actor_manager.ray.get_actor",
                side_effect=_fake_ray_get_actor,
            ), patch(
                "ray.dashboard.modules.job.actor_manager.ray.get_runtime_context"
            ) as mock_rt:
                mock_rt.return_value.get_node_id.return_value = "node_head"

                # Bug 5: _lookup_actor, kill_actor, and create_actor(get_if_exists=True/"recreate")
                # ignore non-_ClientRootActor handles in ray_client namespace while alive,
                # and allow recreate once the non-root actor is killed externally or replaced.
                looked_up = await mgr._lookup_actor("user_detached_child")
                self.assertIsNone(looked_up)
                self.assertFalse(
                    await mgr._terminate_actor_for_recreate(
                        "user_detached_child", "ray_client"
                    )
                )
                self.assertIsNone(await mgr.kill_actor("user_detached_child"))
                for gie_mode in (True, "recreate"):
                    collision_res = await mgr.create_actor(
                        {
                            "name": "user_detached_child",
                            "kind": "class",
                            "class_name": "MyService",
                            "module_source": "class MyService:\n    pass\n",
                            "get_if_exists": gie_mode,
                        }
                    )
                    self.assertFalse(collision_res["ok"])
                    self.assertEqual(collision_res["error"]["exc_type"], "ValueError")
                    self.assertIn(
                        "non-ray_client actor", collision_res["error"]["exc_message"]
                    )
                self.assertEqual(len(killed_handles), 0)

                user_child_alive[0] = False
                current_mode[0] = "ready"
                recreate_after_external_kill = await mgr.create_actor(
                    {
                        "name": "user_detached_child",
                        "kind": "class",
                        "class_name": "MyService",
                        "module_source": "class MyService:\n    pass\n",
                        "get_if_exists": "recreate",
                    }
                )
                self.assertTrue(recreate_after_external_kill["ok"])
                self.assertTrue(recreate_after_external_kill["created"])
                self.assertNotIn("user_detached_child", mgr._non_root_names)
                await mgr.kill_actor("user_detached_child")
                killed_handles.clear()

                # If a name in _non_root_names is now occupied by a broken _ClientRootActor,
                # _lookup_actor and _terminate_actor_for_recreate discard it from _non_root_names.
                mgr._non_root_names.add("user_detached_child")
                broken_root_handle = _FakeRootHandle(mode="init_error")
                with patch(
                    "ray.dashboard.modules.job.actor_manager.ray.get_actor",
                    side_effect=lambda n, namespace=None: (
                        broken_root_handle
                        if n == "user_detached_child" and not killed_handles
                        else (_ for _ in ()).throw(ValueError(n))
                    ),
                ):
                    recreate_broken_root = await mgr.create_actor(
                        {
                            "name": "user_detached_child",
                            "kind": "class",
                            "class_name": "MyService",
                            "module_source": "class MyService:\n    pass\n",
                            "get_if_exists": "recreate",
                        }
                    )
                self.assertTrue(recreate_broken_root["ok"])
                self.assertNotIn("user_detached_child", mgr._non_root_names)
                await mgr.kill_actor("user_detached_child")
                killed_handles.clear()

                mgr._non_root_names.add("dead_non_root")
                with patch(
                    "ray.dashboard.modules.job.actor_manager.ray.util.list_named_actors",
                    return_value=[],
                ):
                    await mgr.list_actors()
                self.assertNotIn("dead_non_root", mgr._non_root_names)
                for bad_spec in (
                    {"class_name": "A", "kind": "bogus"},
                    {"class_name": "A", "args": "not_a_list"},
                    {"class_name": "A", "kwargs": [1]},
                    {"class_name": "A", "runtime_env": "not_a_dict"},
                    {"class_name": "A", "captured_globals": [1]},
                ):
                    with self.assertRaises(ValueError):
                        mgr.validate_actor_spec(bad_spec)
                current_mode[0] = "hang"

                # Finding 1.7: _lookup_actor returns None while actor is in _creating_handles
                mgr._creating_handles["in_progress_actor"] = creating_ready_handle
                self.assertIsNone(await mgr._lookup_actor("in_progress_actor"))
                mgr._creating_handles.pop("in_progress_actor", None)

                # Finding 1.5: create_actor cleans up handle and returns _init_error when __init__ fails
                current_mode[0] = "init_error"
                res_init_err = await mgr.create_actor(
                    {
                        "name": "bad_init_act",
                        "kind": "class",
                        "class_name": "MyService",
                        "module_source": "class MyService:\n    pass\n",
                    }
                )
                self.assertFalse(res_init_err["ok"])
                self.assertEqual(res_init_err["error"]["exc_type"], "ValueError")
                self.assertEqual(res_init_err.get("logs"), "init log line\n")
                self.assertEqual(len(killed_handles), 1)
                self.assertNotIn("bad_init_act", mgr._actors)
                self.assertNotIn("bad_init_act", mgr._name_locks)
                killed_handles.clear()
                current_mode[0] = "hang"

                # Bug 1a: create_actor with timeout kills unschedulable actor and returns TimeoutError
                res_timeout = await mgr.create_actor(
                    {
                        "name": "unsched_timeout",
                        "kind": "class",
                        "class_name": "MyService",
                        "module_source": "class MyService:\n    pass\n",
                        "timeout": 0.05,
                    }
                )
                self.assertFalse(res_timeout["ok"])
                self.assertEqual(res_timeout["error"]["exc_type"], "TimeoutError")
                self.assertTrue(res_timeout["error"]["timed_out"])
                self.assertEqual(len(killed_handles), 1)
                self.assertIs(killed_handles[0][0], created_handles[-1])
                self.assertNotIn("unsched_timeout", mgr._name_locks)
                killed_handles.clear()

                # Bug 1b: cancelling create_actor (e.g. client disconnect) kills the handle
                create_task = asyncio.create_task(
                    mgr.create_actor(
                        {
                            "name": "unsched_cancel",
                            "kind": "class",
                            "class_name": "MyService",
                            "module_source": "class MyService:\n    pass\n",
                        }
                    )
                )
                await asyncio.sleep(0.02)
                create_task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await create_task
                self.assertEqual(len(killed_handles), 1)
                self.assertIs(killed_handles[0][0], created_handles[-1])
                self.assertNotIn("unsched_cancel", mgr._name_locks)
                killed_handles.clear()

                # Cancelling _terminate_actor_for_recreate while awaiting shutdown.remote()
                # still calls _destroy_actor_handle in finally so the actor does not leak.
                hang_shutdown_handle = _FakeRootHandle(mode="hang_shutdown")
                mgr._actors["recreate_cancel_act"] = {
                    "name": "recreate_cancel_act",
                    "handle": hang_shutdown_handle,
                    "class_name": "MyService",
                    "kind": "class",
                    "namespace": "ray_client",
                    "detached": True,
                    "spec_hash": "h",
                    "ttl_seconds": None,
                    "created_at": 1.0,
                    "last_active": 1.0,
                    "last_active_mono": 1.0,
                    "active_calls": 0,
                    "generator_methods": set(),
                }
                term_task = asyncio.create_task(
                    mgr._terminate_actor_for_recreate(
                        "recreate_cancel_act", "ray_client", timeout=2.0
                    )
                )
                await asyncio.sleep(0.02)
                term_task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await term_task
                self.assertEqual(len(killed_handles), 1)
                self.assertIs(killed_handles[0][0], hang_shutdown_handle)
                self.assertNotIn("recreate_cancel_act", mgr._actors)
                killed_handles.clear()

                # Bug 1c: kill_actor(name), concurrent create_actor(name, timeout=...), and
                # cancelled waiter while create_actor is awaiting get_info do NOT deadlock or leak name_lock
                stuck_task = asyncio.create_task(
                    mgr.create_actor(
                        {
                            "name": "stuck_actor",
                            "kind": "class",
                            "class_name": "MyService",
                            "module_source": "class MyService:\n    pass\n",
                        }
                    )
                )
                await asyncio.sleep(0.02)
                cancelled_waiter = asyncio.create_task(
                    mgr.create_actor(
                        {
                            "name": "stuck_actor",
                            "kind": "class",
                            "class_name": "MyService",
                            "module_source": "class MyService:\n    pass\n",
                        }
                    )
                )
                await asyncio.sleep(0.01)
                cancelled_waiter.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await cancelled_waiter
                contended_res = await asyncio.wait_for(
                    mgr.create_actor(
                        {
                            "name": "stuck_actor",
                            "kind": "class",
                            "class_name": "MyService",
                            "module_source": "class MyService:\n    pass\n",
                            "timeout": 0.05,
                        }
                    ),
                    timeout=0.5,
                )
                self.assertFalse(contended_res["ok"])
                self.assertTrue(contended_res["error"]["timed_out"])
                kill_res = await asyncio.wait_for(
                    mgr.kill_actor("stuck_actor"), timeout=0.5
                )
                self.assertEqual(
                    kill_res, {"ok": True, "name": "stuck_actor", "killed": True}
                )
                stuck_res = await asyncio.wait_for(stuck_task, timeout=0.5)
                self.assertFalse(stuck_res["ok"])
                self.assertNotIn("stuck_actor", mgr._actors)
                self.assertNotIn("stuck_actor", mgr._name_locks)
                self.assertNotIn("stuck_actor", mgr._name_lock_waiters)

                # Item 1 & Item 2: verify detached vs non-detached options and task name options
                current_mode[0] = "ready"
                recorded_actor_options.clear()
                res_fn = await mgr.create_actor(
                    {
                        "name": "driver_my_fn_1",
                        "kind": "function",
                        "fn_name": "my_fn",
                        "module_source": "def my_fn():\n    return 1\n",
                    }
                )
                self.assertTrue(res_fn["ok"])
                self.assertNotIn("lifetime", recorded_actor_options[-1])

                res_cls = await mgr.create_actor(
                    {
                        "name": "svc_1",
                        "kind": "class",
                        "class_name": "MyService",
                        "module_source": "class MyService:\n    pass\n",
                    }
                )
                self.assertTrue(res_cls["ok"])
                self.assertEqual(recorded_actor_options[-1].get("lifetime"), "detached")
                self.assertEqual(recorded_actor_options[-1].get("max_concurrency"), 1)

                await mgr.call_actor_method("svc_1", "compute", {"args": [1]})
                await mgr.call_actor_method("svc_1", "__call__", {"args": [1]})
                await mgr.call_actor_method("driver_my_fn_1", "__call__", {"args": []})
                self.assertEqual(
                    recorded_invoke_names,
                    ["MyService.compute", "MyService", "my_fn"],
                )

                items = [
                    it
                    async for it in mgr.stream_actor_method(
                        "svc_1", "stream_vals", {"args": []}
                    )
                ]
                self.assertTrue(any(it == {"ok": True, "item": 1} for it in items))
                self.assertEqual(recorded_stream_names, ["MyService.stream_vals"])

                # Finding 1.8 & Part 4.2: RAY_BACKEND=rust fallbacks when .options() or ray.kill raises NotImplementedError
                raise_options_not_impl[0] = True
                raise_kill_not_impl[0] = True
                try:
                    res_rust_call = await mgr.call_actor_method(
                        "svc_1", "compute", {"args": [9]}
                    )
                    self.assertEqual(
                        res_rust_call,
                        {"ok": True, "result": {"method": "compute", "args": [9]}},
                    )
                    rust_stream_items = [
                        it
                        async for it in mgr.stream_actor_method(
                            "svc_1", "stream_vals", {"args": []}
                        )
                    ]
                    self.assertTrue(
                        any(it == {"ok": True, "item": 1} for it in rust_stream_items)
                    )
                    kill_rust = await mgr.kill_actor("svc_1")
                    self.assertEqual(
                        kill_rust, {"ok": True, "name": "svc_1", "killed": True}
                    )
                    self.assertEqual(len(destroyed_handles), 1)
                    self.assertNotIn("svc_1", mgr._name_locks)
                finally:
                    raise_options_not_impl[0] = False
                    raise_kill_not_impl[0] = False

                # Item 3: verify unified Ray 3.0 resources (CPU, GPU, memory) normalization in ActorManager
                recorded_actor_options.clear()
                res_unified = await mgr.create_actor(
                    {
                        "name": "svc_unified_res",
                        "kind": "class",
                        "class_name": "MyService",
                        "module_source": "class MyService:\n    pass\n",
                        "resources": {
                            "CPU": 2,
                            "GPU": 1,
                            "memory": 1073741824,
                            "TPU": 4,
                        },
                        "label_selector": {"ray.io/accelerator-type": "TPU-V6E"},
                    }
                )
                self.assertTrue(res_unified["ok"])
                opts_unified = recorded_actor_options[-1]
                self.assertEqual(opts_unified["num_cpus"], 2)
                self.assertEqual(opts_unified["num_gpus"], 1)
                self.assertEqual(opts_unified["memory"], 1073741824)
                self.assertEqual(opts_unified["resources"], {"TPU": 4.0})
                self.assertEqual(
                    opts_unified["label_selector"],
                    {"ray.io/accelerator-type": "TPU-V6E"},
                )

                # Reattaching with Ray 2.x explicit num_cpus/num_gpus/memory produces identical spec_hash
                hash_explicit = _compute_spec_hash(
                    {
                        "module_source": "class MyService:\n    pass\n",
                        "num_gpus": 1,
                        "memory": 1073741824,
                        "resources": {"TPU": 4},
                        "label_selector": {"ray.io/accelerator-type": "TPU-V6E"},
                    },
                    "class",
                    "MyService",
                    2,
                    1,
                )
                self.assertEqual(res_unified["spec_hash"], hash_explicit)

                # Conflicting explicit num_cpus vs resources['CPU'] returns ValueError
                res_conflict = await mgr.create_actor(
                    {
                        "name": "svc_conflict_res",
                        "kind": "class",
                        "class_name": "MyService",
                        "module_source": "class MyService:\n    pass\n",
                        "num_cpus": 2,
                        "resources": {"CPU": 4},
                    }
                )
                self.assertFalse(res_conflict["ok"])
                self.assertEqual(res_conflict["error"]["exc_type"], "ValueError")

                # Item 4: node_id placement via label_selector
                recorded_actor_options.clear()
                res_node_v2 = await mgr.create_actor(
                    {
                        "name": "svc_node_v2",
                        "kind": "class",
                        "class_name": "MyService",
                        "module_source": "class MyService:\n    pass\n",
                        "node_id": "node_abc123",
                    }
                )
                self.assertTrue(res_node_v2["ok"])
                opts_node_v2 = recorded_actor_options[-1]
                self.assertEqual(
                    opts_node_v2.get("label_selector"),
                    {"ray.io/node-id": "node_abc123"},
                )

                # Conflicting node_id and label_selector returns ValueError
                res_node_conflict = await mgr.create_actor(
                    {
                        "name": "svc_node_conflict",
                        "kind": "class",
                        "class_name": "MyService",
                        "module_source": "class MyService:\n    pass\n",
                        "node_id": "node_abc123",
                        "label_selector": {"tier": "spot"},
                    }
                )
                self.assertFalse(res_node_conflict["ok"])
                self.assertEqual(res_node_conflict["error"]["exc_type"], "ValueError")

                # Item 5: ActorManager cluster node management fallback (ray.nodes) and ray.cluster delegation
                fake_ray_nodes = [
                    {
                        "NodeID": "n_head",
                        "Alive": True,
                        "NodeManagerAddress": "127.0.0.1",
                        "Resources": {"CPU": 4.0, "node:__internal_head__": 1.0},
                        "Labels": {"ray.io/node-id": "n_head"},
                    },
                    {
                        "NodeID": "n_worker",
                        "Alive": True,
                        "NodeManagerAddress": "10.0.0.5",
                        "Resources": {"CPU": 8.0, "GPU": 1.0},
                        "Labels": {
                            "ray.io/node-id": "n_worker",
                            "ray.io/node-group": "gpu-pool",
                        },
                    },
                ]
                with patch(
                    "ray.dashboard.modules.job.actor_manager.ray.nodes",
                    return_value=fake_ray_nodes,
                ):
                    ng_res = mgr.list_node_groups()
                    self.assertEqual(
                        [g["name"] for g in ng_res],
                        ["head", "gpu-pool", "default"],
                    )
                    g_info = mgr.get_node_group("gpu-pool")
                    self.assertEqual(g_info["name"], "gpu-pool")
                    with self.assertRaises(KeyError):
                        mgr.get_node_group("missing-pool")

                    nodes_all = mgr.list_nodes()
                    self.assertEqual(len(nodes_all), 2)
                    self.assertNotIn("node_id", nodes_all[0])
                    self.assertNotIn("node_group", nodes_all[0])
                    self.assertNotIn("status", nodes_all[0])
                    nodes_gpu = mgr.list_nodes(node_group="gpu-pool")
                    self.assertEqual([n["id"] for n in nodes_gpu], ["n_worker"])
                    self.assertEqual(mgr.get_node("n_worker")["ip_address"], "10.0.0.5")
                    with self.assertRaises(KeyError):
                        mgr.get_node("missing_node")
                    from ray.dashboard.modules.job.job_agent import _cluster_response

                    with self.assertRaises(MaxNodesExceededError):
                        mgr.add_nodes("head", 1)
                    with self.assertRaises(NotImplementedError):
                        mgr.add_nodes("gpu-pool", 1)
                    self.assertEqual(
                        _cluster_response(mgr.add_nodes, "gpu-pool", 1).status, 501
                    )
                    with self.assertRaises(ValueError):
                        mgr.destroy_node("n_head", "try head")
                    with self.assertRaises(ValueError):
                        mgr.destroy_node("n_worker", "")
                    with self.assertRaises(NotImplementedError):
                        mgr.destroy_node("n_worker", "scale down")
                    self.assertEqual(
                        _cluster_response(
                            mgr.destroy_node, "n_worker", "scale down"
                        ).status,
                        501,
                    )
                    with self.assertRaises(ValueError):
                        mgr.list_nodes(state="INVALID_STATE")

                recorded_destroys: List[Tuple[str, str, Optional[float]]] = []
                ready_waits: List[Optional[float]] = []

                class _FakeNodeObj:
                    def __init__(self, nid: str, group_name: str, state: str = "ALIVE"):
                        self.id = nid
                        self.group_name = group_name
                        self.resources = {"CPU": 8.0}
                        self.labels = {"ray.io/node-group": group_name}
                        self.detached = True
                        self._state = state

                    def status(self):
                        return types.SimpleNamespace(
                            state=types.SimpleNamespace(value=self._state),
                            death_reason=None,
                            termination_deadline=None,
                        )

                    def ready(self):
                        def _result(timeout=None):
                            ready_waits.append(timeout)
                            if timeout is not None and timeout > 50.0:
                                raise TimeoutError("wait_ready timed out")
                            return True

                        return types.SimpleNamespace(result=_result)

                    def destroy(
                        self,
                        *,
                        reason: str,
                        grace_period_s: Optional[float] = None,
                    ):
                        recorded_destroys.append((self.id, reason, grace_period_s))
                        self._state = (
                            "TERMINATING_SOON"
                            if (
                                self._state != "PENDING"
                                and grace_period_s is not None
                                and grace_period_s > 0
                            )
                            else "DEAD"
                        )

                fake_node_head = _FakeNodeObj("n_head", "head", state="ALIVE")
                fake_node_n1 = _FakeNodeObj("n1", "workers", state="ALIVE")
                fake_node_pending = _FakeNodeObj(
                    "n_pending", "workers", state="PENDING"
                )

                class _FakeNodeGroupObj:
                    def __init__(self, name: str, max_nodes: int = 10):
                        self.name = name
                        self.resources = {"CPU": 8.0}
                        self.labels = {}
                        self.max_nodes = max_nodes

                    def add_nodes(self, count: int, *, detached: bool = False):
                        assert detached is True
                        return [
                            _FakeNodeObj(f"n_added_{i}", self.name)
                            for i in range(count)
                        ]

                fake_groups = {
                    "head": _FakeNodeGroupObj("head", max_nodes=1),
                    "workers": _FakeNodeGroupObj("workers", max_nodes=10),
                }

                def _get_ng(g_name: str):
                    if g_name not in fake_groups:
                        raise KeyError(g_name)
                    return fake_groups[g_name]

                def _get_node(nid: str):
                    if nid == "n_head":
                        return fake_node_head
                    if nid == "n1":
                        return fake_node_n1
                    if nid == "n_pending":
                        return fake_node_pending
                    raise KeyError(nid)

                fake_cluster_mod = types.SimpleNamespace(
                    list_node_groups=lambda: list(fake_groups.values()),
                    get_node_group=_get_ng,
                    list_nodes=lambda: [
                        fake_node_head,
                        fake_node_n1,
                        fake_node_pending,
                    ],
                    get_node=_get_node,
                )
                with patch(
                    "ray.dashboard.modules.job.actor_manager.ray.cluster",
                    new=fake_cluster_mod,
                    create=True,
                ):
                    self.assertEqual(
                        [g["name"] for g in mgr.list_node_groups()],
                        ["head", "workers"],
                    )
                    self.assertEqual(
                        mgr.get_node_group("head")["name"],
                        "head",
                    )
                    from ray.dashboard.modules.job.actor_manager import (
                        MaxNodesExceededError as _AMMaxNodesExceededError,
                    )
                    from ray.dashboard.modules.job.job_agent import _cluster_response

                    with self.assertRaises(_AMMaxNodesExceededError):
                        mgr.add_nodes("head", 1)
                    with self.assertRaises(_AMMaxNodesExceededError):
                        mgr.add_nodes("workers", 9)

                    # Verify JobAgent _cluster_response HTTP status mappings (400, 404, 408, 409, 501)
                    self.assertEqual(
                        _cluster_response(mgr.add_nodes, "workers", 0).status, 400
                    )
                    self.assertEqual(
                        _cluster_response(
                            mgr.add_nodes, "workers", 1, wait_ready="false"
                        ).status,
                        400,
                    )
                    self.assertEqual(
                        _cluster_response(mgr.get_node_group, "missing").status, 404
                    )
                    self.assertEqual(
                        _cluster_response(
                            mgr.add_nodes, "workers", 1, wait_ready=True, timeout_s=99.0
                        ).status,
                        408,
                    )
                    self.assertEqual(
                        _cluster_response(mgr.add_nodes, "head", 1).status, 409
                    )
                    recorded_destroys.clear()
                    ready_waits.clear()

                    added_res = mgr.add_nodes(
                        "workers", 2, wait_ready=True, timeout_s=3.0
                    )
                    self.assertEqual(len(added_res), 2)
                    self.assertEqual(len(ready_waits), 2)
                    destroyed_n1 = mgr.destroy_node(
                        "n1", "drain test", grace_period_s=15.0
                    )
                    self.assertTrue(destroyed_n1["destroyed"])
                    self.assertEqual(destroyed_n1["state"], "TERMINATING_SOON")
                    self.assertEqual(recorded_destroys, [("n1", "drain test", 15.0)])
                    destroyed_pending = mgr.destroy_node(
                        "n_pending", "cancel pending", grace_period_s=15.0
                    )
                    self.assertTrue(destroyed_pending["destroyed"])
                    self.assertEqual(destroyed_pending["state"], "DEAD")
                    with self.assertRaises(TimeoutError):
                        mgr.add_nodes("workers", 2, wait_ready=True, timeout_s=99.0)
                    # Both newly created nodes must be cleaned up on wait_ready failure
                    self.assertEqual(
                        [d[0] for d in recorded_destroys[2:]],
                        ["n_added_0", "n_added_1"],
                    )
                    with self.assertRaises(ValueError):
                        mgr.add_nodes("workers", 0)
                    with self.assertRaises(KeyError):
                        mgr.destroy_node("missing", "cleanup")

            if mgr._reaper_task is not None:
                mgr._reaper_task.cancel()

        asyncio.run(_run_async_checks())

    def test_unified_resources_label_selector_and_remote_working_dir_uri(self):
        with RayCluster(address=self.address) as cluster:
            # 1. Remote working_dir URIs pass through without local zip packaging
            merged, local_wd, _ = cluster._prepare_runtime_env(
                {"working_dir": "https://example.com/archive.zip"}
            )
            self.assertEqual(merged["working_dir"], "https://example.com/archive.zip")
            self.assertIsNone(local_wd)

            def check_fn(x: int) -> int:
                return x + 1

            out = cluster.run(
                check_fn,
                41,
                runtime_env={"working_dir": "s3://my-bucket/pkg.zip"},
                resources={"CPU": 2, "GPU": 0.5, "memory": 256 * 1024**2, "TPU": 4},
                label_selector={"tier": "spot"},
            )
            self.assertEqual(out, 42)
            self.assertEqual(len(self.state.packages), 0)
            last_spec = self.state.last_created_actor_spec
            assert last_spec is not None
            self.assertEqual(
                last_spec["runtime_env"], {"working_dir": "s3://my-bucket/pkg.zip"}
            )
            self.assertEqual(last_spec["num_cpus"], 2)
            self.assertEqual(last_spec["num_gpus"], 0.5)
            self.assertEqual(last_spec["memory"], 256 * 1024**2)
            self.assertEqual(last_spec["resources"], {"TPU": 4.0})
            self.assertEqual(last_spec["label_selector"], {"tier": "spot"})

            # 2. Actor unified resources and Ray 2.x num_cpus/num_gpus equivalence (no spec_mismatch warning)
            class PlacedWorker:
                def ping(self) -> str:
                    return "pong"

            h1 = cluster.actor(
                PlacedWorker,
                name="placed_worker_1",
                resources={"CPU": 2, "GPU": 1, "memory": 512 * 1024**2, "TPU": 2},
                label_selector={"zone": "us-central1-a"},
            )
            self.assertTrue(h1.created)
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                h2 = cluster.actor(
                    PlacedWorker,
                    name="placed_worker_1",
                    num_cpus=2,
                    num_gpus=1,
                    resources={"memory": 512 * 1024**2, "TPU": 2},
                    label_selector={"zone": "us-central1-a"},
                )
            self.assertFalse(h2.created)
            self.assertEqual(len(caught), 0)
            h1.kill()

            # 3. @ray.remote _default_options inheritance and override via unified resources
            class FakeRemoteClass:
                _default_options = {
                    "num_cpus": 2,
                    "memory": 128 * 1024**2,
                    "resources": {"custom_accel": 1},
                    "label_selector": {"pool": "default"},
                    "max_concurrency": 4,
                }
                __ray_metadata__ = types.SimpleNamespace(
                    modified_class=types.SimpleNamespace(
                        __ray_actor_class__=PlacedWorker
                    )
                )

            # 3a. Overriding only CPU via unified resources preserves default custom resources
            h3_partial = cluster.actor(
                FakeRemoteClass,
                name="placed_worker_partial_override",
                resources={"CPU": 4},
            )
            spec3_partial = self.state.last_created_actor_spec
            assert spec3_partial is not None
            self.assertEqual(spec3_partial["num_cpus"], 4)
            self.assertEqual(spec3_partial["memory"], 128 * 1024**2)
            self.assertEqual(spec3_partial["resources"], {"custom_accel": 1.0})
            self.assertEqual(spec3_partial["label_selector"], {"pool": "default"})
            self.assertEqual(spec3_partial["max_concurrency"], 4)
            h3_partial.kill()

            # 3b. Overriding custom resources or clearing with resources={}
            h3 = cluster.actor(
                FakeRemoteClass,
                name="placed_worker_inherited",
                resources={"CPU": 4, "custom_accel": 2},
            )
            spec3 = self.state.last_created_actor_spec
            assert spec3 is not None
            self.assertEqual(spec3["num_cpus"], 4)
            self.assertEqual(spec3["memory"], 128 * 1024**2)
            self.assertEqual(spec3["resources"], {"custom_accel": 2.0})
            h3.kill()

            h3_clear = cluster.actor(
                FakeRemoteClass,
                name="placed_worker_cleared",
                resources={},
            )
            spec3_clear = self.state.last_created_actor_spec
            assert spec3_clear is not None
            self.assertEqual(spec3_clear["num_cpus"], 2)
            self.assertNotIn("resources", spec3_clear)
            h3_clear.kill()

            # 4. Direct _compute_spec_hash equivalence between Ray 2.x and Ray 3.0 specs
            hash_v2 = _compute_spec_hash(
                {"num_cpus": 2, "num_gpus": 1, "memory": 4096, "resources": {"TPU": 4}},
                "class",
                "PlacedWorker",
                2,
                10,
            )
            hash_v3 = _compute_spec_hash(
                {"resources": {"CPU": 2, "GPU": 1, "memory": 4096, "TPU": 4}},
                "class",
                "PlacedWorker",
                None,
                10,
            )
            self.assertEqual(hash_v2, hash_v3)

            # 5. Conflict detection and invalid resource value validation
            with self.assertRaisesRegex(ValueError, "Conflicting CPU"):
                cluster.run(check_fn, 1, num_cpus=1, resources={"CPU": 2})
            with self.assertRaisesRegex(ValueError, "Conflicting GPU"):
                cluster.actor(PlacedWorker, num_gpus=1, resources={"GPU": 2})
            with self.assertRaisesRegex(ValueError, "Conflicting memory"):
                _normalize_resources(None, None, 1024, {"memory": 2048})
            with self.assertRaisesRegex(ValueError, "Invalid num_cpus"):
                cluster.run(check_fn, 1, num_cpus=-1)
            with self.assertRaisesRegex(ValueError, "Invalid num_gpus"):
                cluster.actor(PlacedWorker, num_gpus=True)
            with self.assertRaisesRegex(ValueError, "resources\\['memory'\\]"):
                cluster.actor(PlacedWorker, resources={"memory": 1024.5})
            with self.assertRaisesRegex(ValueError, "resources\\['memory'\\]"):
                cluster.run(check_fn, 1, resources={"memory": 1024.5})

    def test_node_id_actor_stream_and_cluster_node_management(self):
        with RayCluster(address=self.address) as cluster:
            # 1. node_id placement on run(), top-level run(), actor(), and @ray.remote inheritance
            def get_val(x: int) -> int:
                return x * 2

            self.assertEqual(cluster.run(get_val, 21, node_id="node_head"), 42)
            assert self.state.last_created_actor_spec is not None
            self.assertEqual(self.state.last_created_actor_spec["node_id"], "node_head")

            self.assertEqual(
                run(get_val, 10, address=self.address, node_id="node_worker_1"), 20
            )
            self.assertEqual(
                self.state.last_created_actor_spec["node_id"], "node_worker_1"
            )

            class StreamService:
                def gen_items(self, count: int) -> Iterator[int]:
                    for i in range(count):
                        yield i * 10

                def gen_expr(self, count: int):
                    return (i + 1 for i in range(count))

                def non_stream(self, x: int) -> int:
                    return x + 5

            class FakeRemoteNodePinned:
                _default_options = {"node_id": "node_worker_1"}
                __ray_metadata__ = types.SimpleNamespace(
                    modified_class=types.SimpleNamespace(
                        __ray_actor_class__=StreamService
                    )
                )

            with cluster.actor(
                FakeRemoteNodePinned, name="node_pinned_actor"
            ) as pinned_act:
                assert self.state.last_created_actor_spec is not None
                self.assertEqual(
                    self.state.last_created_actor_spec["node_id"], "node_worker_1"
                )
                # Explicit label_selector overrides inherited _default_options["node_id"]
                with cluster.actor(
                    FakeRemoteNodePinned,
                    name="node_override_actor",
                    label_selector={"tier": "on-demand"},
                ):
                    self.assertIsNone(self.state.last_created_actor_spec.get("node_id"))
                    self.assertEqual(
                        self.state.last_created_actor_spec["label_selector"],
                        {"tier": "on-demand"},
                    )

                # 2. ActorHandle.stream() and _ActorMethodProxy.stream()
                self.assertEqual(list(pinned_act.stream("gen_items", 3)), [0, 10, 20])
                self.assertEqual(list(pinned_act.gen_items.stream(3)), [0, 10, 20])
                self.assertEqual(list(pinned_act.gen_expr.stream(3)), [1, 2, 3])

                # Calling stream() on a non-generator method raises RayJobExecutionError (a RayClientError)
                with self.assertRaises(RayClientError):
                    list(pinned_act.stream("non_stream", 7))
                with self.assertRaises(RayClientError):
                    list(pinned_act.non_stream.stream(7))

                # If server returns non-SSE 200 JSON when require_stream=True, raise RayClientError
                fake_resp = types.SimpleNamespace(
                    status_code=200,
                    headers={"Content-Type": "application/json"},
                    text='{"ok": true, "result": 12}',
                    json=lambda: {"ok": True, "result": 12},
                    close=lambda: None,
                )
                with patch.object(cluster, "request", return_value=fake_resp):
                    with self.assertRaisesRegex(RayClientError, "Expected SSE stream"):
                        pinned_act.stream("non_stream", 7)

                # If SSE stream closes prematurely without {"ok": true, "done": true}, raise RayClientError
                truncated_sse_resp = types.SimpleNamespace(
                    status_code=200,
                    headers={"Content-Type": "text/event-stream; charset=utf-8"},
                    iter_lines=lambda: iter([b'data: {"ok": true, "item": 100}']),
                    close=lambda: None,
                )
                with patch.object(cluster, "request", return_value=truncated_sse_resp):
                    s_iter = pinned_act.stream("gen_items", 2)
                    self.assertEqual(next(s_iter), 100)
                    with self.assertRaisesRegex(
                        RayClientError,
                        "closed prematurely before receiving completion frame",
                    ):
                        next(s_iter)

            # Validation errors for invalid or conflicting node_id, max_concurrency, timeout, label_selector
            with self.assertRaisesRegex(ValueError, "Cannot specify both 'node_id'"):
                cluster.run(get_val, 1, node_id="node_head", label_selector={"a": "b"})
            with self.assertRaisesRegex(ValueError, "Cannot specify both 'node_id'"):
                cluster.actor(
                    StreamService, node_id="node_head", label_selector={"a": "b"}
                )
            with self.assertRaisesRegex(ValueError, "node_id must be a non-empty"):
                cluster.run(get_val, 1, node_id="")
            for bad_mc in (0, -1, True, 1.5):
                with self.assertRaisesRegex(ValueError, "max_concurrency"):
                    cluster.actor(StreamService, max_concurrency=bad_mc)  # type: ignore[arg-type]
            for bad_timeout in (0, -1.0, float("nan"), float("inf"), True):
                with self.assertRaisesRegex(ValueError, "timeout"):
                    cluster.run(get_val, 1, timeout=bad_timeout)  # type: ignore[arg-type]
            with self.assertRaisesRegex(ValueError, "entrypoint_label_selector"):
                cluster.submit_job("python -V", entrypoint_label_selector={"": "v"})

            # 3. Cluster node management APIs on RayCluster
            groups = cluster.list_node_groups()
            self.assertEqual({g["name"] for g in groups}, {"head", "gpu-workers"})

            gpu_group = cluster.get_node_group("gpu-workers")
            self.assertEqual(gpu_group["name"], "gpu-workers")
            self.assertEqual(gpu_group["max_nodes"], 10)

            all_nodes = cluster.list_nodes()
            self.assertEqual(len(all_nodes), 2)
            gpu_nodes = cluster.list_nodes(group_name="gpu-workers")
            self.assertEqual([n["id"] for n in gpu_nodes], ["node_worker_1"])

            n_info = cluster.get_node("node_worker_1")
            self.assertEqual(n_info["ip_address"], "10.0.0.2")

            added = cluster.add_nodes("gpu-workers", 2, wait_ready=True, timeout=5.0)
            self.assertEqual(len(added), 2)
            self.assertEqual(len(cluster.list_nodes(group_name="gpu-workers")), 3)

            with self.assertRaises(ValueError):
                cluster.add_nodes("gpu-workers", -1)
            with self.assertRaises(ValueError):
                cluster.add_nodes("gpu-workers", 1, wait_ready="false")  # type: ignore[arg-type]
            with self.assertRaises(ValueError):
                cluster.add_nodes("", 1)
            with self.assertRaises(ValueError):
                cluster.get_node("")
            with self.assertRaises(ValueError):
                cluster.list_nodes(states="INVALID_STATE")
            with self.assertRaises(ValueError):
                cluster.list_nodes(include_dead="yes")  # type: ignore[arg-type]
            with self.assertRaises(ValueError):
                cluster.destroy_node(added[0]["id"], "")

            destroyed_res = cluster.destroy_node(added[0]["id"], "scale down")
            self.assertTrue(destroyed_res["destroyed"])
            self.assertEqual(len(cluster.list_nodes(group_name="gpu-workers")), 2)
            self.assertEqual(
                len(cluster.list_nodes(group_name="gpu-workers", include_dead=True)), 3
            )
            self.assertEqual(
                len(cluster.list_nodes(group_name="gpu-workers", states="DEAD")), 1
            )
            with self.assertRaises(RayClientError):
                cluster.destroy_node("nonexistent_node_id", "cleanup")


if __name__ == "__main__":
    unittest.main()
