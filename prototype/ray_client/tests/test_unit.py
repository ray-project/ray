"""Offline unit and mock-REST-server tests for ray_client."""

import base64
import copy
import functools
import inspect
import io
import json
import os
import pickle
import subprocess
import sys
import tempfile
import threading
import unittest
import zipfile
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List, Optional
from unittest.mock import patch

from ray_client import (
    JobResult,
    RayClient,
    RayClientError,
    RayClientSerializationError,
    RayJobExecutionError,
    RayJobTimeoutError,
    packaging,
    run_function_as_driver,
)
from ray_client._ignore import _compile_ignore_rules, _is_ignored
from ray_client.packaging import (
    IGNORE_GITIGNORE_ENV_VAR,
    RESULT_LINE_PREFIX,
    RUNNER_FILENAME,
    RUNNER_SCRIPT_CONTENT,
    USER_FN_FILENAME,
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

    def do_DELETE(self) -> None:  # noqa: N802
        if self.path.startswith("/api/jobs/"):
            sub_id = self.path[len("/api/jobs/") :]
            with self.state.lock:
                existed = self.state.jobs.pop(sub_id, None) is not None
            self._send_json(200 if existed else 404, {"deleted": existed})
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
                py_mod_bytes = [
                    self.state.packages[u.split("gcs://", 1)[1]]
                    for u in (req["runtime_env"].get("py_modules") or ())
                    if u.startswith("gcs://")
                    and u.split("gcs://", 1)[1] in self.state.packages
                ]
                env_vars = dict(req["runtime_env"].get("env_vars", {}))

            work_dir = self.state.tmp_root / sub_id
            work_dir.mkdir(parents=True, exist_ok=True)
            (work_dir / "ray.py").write_text(
                "_INIT_CALLS = []\n"
                "def init(*a, **kw):\n"
                "    _INIT_CALLS.append((a, kw))\n"
                "    return None\n"
                "def is_initialized(): return bool(_INIT_CALLS)\n"
                "def shutdown(): _INIT_CALLS.clear()\n"
                "class cloudpickle:\n"
                "    @staticmethod\n"
                "    def register_pickle_by_value(module): return None\n",
                encoding="utf-8",
            )
            with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
                zf.extractall(work_dir)
            for mod_zip in py_mod_bytes:
                with zipfile.ZipFile(io.BytesIO(mod_zip)) as zf:
                    zf.extractall(work_dir)

            env = os.environ.copy()
            env.update(env_vars)
            env["RAY_JOB_CONFIG_JSON_ENV_VAR"] = json.dumps(
                {
                    "runtime_env": req["runtime_env"],
                    "metadata": req.get("metadata", {}),
                }
            )
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
                    else f"entrypoint exited non-zero\n{proc.stdout[-200:]}",
                }
            self._send_json(200, {"submission_id": sub_id})
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

        with self.assertRaises(RayClientSerializationError):
            build_driver_package(top_level_fn, (object(),), {})

        with self.assertRaises(RayClientSerializationError):
            build_driver_package(top_level_fn, (float("nan"),), {})

        with self.assertRaises(TypeError):
            build_driver_package(top_level_fn, (), {})

        with self.assertRaises(TypeError):
            build_driver_package(top_level_fn, (1,), {"unknown_kw": 2})

        with self.assertRaisesRegex(RayClientError, "is a local path"):
            RayClient(address="http://127.0.0.1:1")._prepare_runtime_env(
                {"py_modules": ["./local_pkg"]}
            )

        merged, _, excludes = RayClient(
            address="http://127.0.0.1:1", default_runtime_env={"excludes": ("*.log",)}
        )._prepare_runtime_env({"excludes": ["*.bin"]})
        self.assertEqual(excludes, ["*.log", "*.bin"])
        self.assertNotIn("excludes", merged)

        # A bare string would unpack into one-character patterns, and `*` among
        # them excludes everything under gitignore semantics.
        with self.assertRaisesRegex(TypeError, "excludes.* must be a list"):
            RayClient(address="http://127.0.0.1:1")._prepare_runtime_env(
                {"excludes": "*.log"}
            )

    def test_default_argument_from_enclosing_scope_fails_locally(self):
        """A default-argument expression is re-evaluated when the shipped module is
        imported, so a name that only an enclosing function or class scope could
        resolve must be rejected on the client rather than NameError on the cluster;
        builtin defaults need no capture."""

        def make_driver():
            scale = 3

            def driver(x: int, factor: int = scale) -> int:
                return x * factor

            return driver

        class Config:
            FACTOR = 2

            def run(self, x: int, factor: int = FACTOR) -> int:
                return x * factor

        for fn in (make_driver(), Config.run):
            with self.assertRaisesRegex(
                RayClientSerializationError, "Default argument .* enclosing"
            ):
                extract_function_source_and_globals(fn)

        def builtin_default(x: int, cb=print) -> bool:
            return cb is print

        self.assertEqual(extract_function_source_and_globals(builtin_default)[2], {})

    def test_nested_annotations_recursion_and_class_bodies(self):
        """Typing names used only in nested signatures are not captured (the
        shipped module defers annotations); nested recursion and class bodies that
        rebind a global both resolve correctly."""

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
        """Method and nested-function source is shipped byte-for-byte: dedenting
        would silently rewrite the contents of indented multi-line strings."""

        class Driver:
            def query(self) -> str:
                sql = """
                SELECT *
                  FROM t
                """
                flush = """left
col0
"""
                return sql + flush

        expected = Driver().query()
        fn_name, source, _ = extract_function_source_and_globals(Driver.query)
        self.assertTrue(source.endswith(f"if True:\n{inspect.getsource(Driver.query)}"))
        namespace: Dict[str, Any] = {}
        exec(compile(source.replace("import ray", "pass"), "<src>", "exec"), namespace)
        self.assertEqual(namespace[fn_name](None), expected)

    def test_deterministic_zip_hash_and_large_payload_fallback(self):
        zip_1, uri_1, env_1 = build_driver_package(top_level_fn, (1,), {})
        zip_2, uri_2, env_2 = build_driver_package(top_level_fn, (99,), {})
        self.assertEqual(uri_1, uri_2)
        self.assertEqual(zip_1, zip_2)
        self.assertIsNotNone(env_1)
        self.assertNotEqual(env_1, env_2)

        # Payloads > 32 KiB keep the working_dir/driver zip 100% call-invariant
        # and ship the payload in a separate content-addressed py_modules archive.
        (
            zip_big_a,
            uri_big_a,
            env_big_a,
            payload_pkg_a,
        ) = packaging.build_driver_and_payload_packages(
            top_level_fn, ("x" * 50_000,), {}
        )
        (
            zip_big_b,
            uri_big_b,
            env_big_b,
            payload_pkg_b,
        ) = packaging.build_driver_and_payload_packages(
            top_level_fn, ("y" * 50_000,), {}
        )
        self.assertIsNone(env_big_a)
        self.assertIsNone(env_big_b)
        self.assertEqual(uri_big_a, uri_1)
        self.assertEqual(uri_big_a, uri_big_b)
        self.assertEqual(zip_big_a, zip_big_b)
        self.assertIsNotNone(payload_pkg_a)
        self.assertIsNotNone(payload_pkg_b)
        assert payload_pkg_a is not None and payload_pkg_b is not None
        self.assertNotEqual(payload_pkg_a[1], payload_pkg_b[1])
        with zipfile.ZipFile(io.BytesIO(payload_pkg_a[0])) as zf:
            self.assertEqual(zf.namelist(), ["_ray_client_payload.json"])

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "keep.py").write_text("X = 1\n", encoding="utf-8")
            (root / "run.sh").write_text("#!/bin/sh\necho ok\n", encoding="utf-8")
            (root / "run.sh").chmod(0o755)
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

    def test_strict_json_shadowed_defaults_and_conditional_globals(self):
        # Rejects non-str dict keys, tuples, and generators locally; preserves dict order.
        with self.assertRaisesRegex(RayClientSerializationError, "non-string dict key"):
            build_driver_package(top_level_fn, ({1: "one"},), {})
        with self.assertRaisesRegex(RayClientSerializationError, "is a tuple"):
            build_driver_package(top_level_fn, ((1, 2),), {})

        def gen_fn():
            yield 1

        with self.assertRaisesRegex(RayClientSerializationError, "generator function"):
            extract_function_source_and_globals(gen_fn)

        # Enclosing class attribute shadowing a module-level global in a default arg is rejected.
        class ShadowedConfig:
            GLOBAL_SCALE = 999

            def run(self, x: int, factor: int = GLOBAL_SCALE) -> int:
                return x * factor

        with self.assertRaisesRegex(
            RayClientSerializationError, "Default argument .* enclosing"
        ):
            extract_function_source_and_globals(ShadowedConfig.run)

        def make_comp_default():
            local_items = [1, 2]

            def fn(xs=[i + 1 for i in local_items]):  # noqa: B006
                return xs

            return fn

        with self.assertRaisesRegex(
            RayClientSerializationError, "Default argument .* enclosing"
        ):
            extract_function_source_and_globals(make_comp_default())

        # Conditional `global` assignment inside `fn` does not hide reading the module global.
        def conditional_global(override: dict) -> int:
            global GLOBAL_SCALE
            if "scale" in override:
                GLOBAL_SCALE = override["scale"]
            return GLOBAL_SCALE

        _, _, cond_captured = extract_function_source_and_globals(conditional_global)
        self.assertEqual(cond_captured, {"GLOBAL_SCALE": 42})

    def test_gitignore_pattern_semantics(self):
        """Pattern forms follow https://git-scm.com/docs/gitignore (expectations
        cross-checked against `git check-ignore`): an inner or leading slash
        anchors, `**` spans directories while `*`/`?` do not, a trailing `/`
        matches only directories, the last matching pattern wins, brackets take
        POSIX classes and never match `/`, a reversed range keeps its start
        character, and an unterminated `[` or a doubled slash matches nothing."""
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
            (
                ["[z-a]", "[[:digit:]]*", "[a[:space:]]b", "[[:nope:]]", "a[/]b"],
                [
                    ("z", False, True),
                    ("a", False, False),
                    ("0abc", False, True),
                    ("ab", False, True),
                    (" b", False, True),
                    ("cb", False, False),
                    ("a/b", False, False),
                ],
            ),
            (
                ["foo//", "//bar", "[a-c-e]", "[]x]"],
                [
                    ("foo", True, False),
                    ("bar", False, False),
                    ("-", False, True),
                    ("d", False, False),
                    ("]", False, True),
                ],
            ),
        ]
        for patterns, expectations in cases:
            rules = _compile_ignore_rules(patterns, "")
            for arcname, is_dir, expected in expectations:
                with self.subTest(patterns=patterns, arcname=arcname, is_dir=is_dir):
                    self.assertEqual(_is_ignored(arcname, is_dir, rules), expected)

    def test_working_dir_ignore_files_symlinks_and_empty_dirs(self):
        """working_dir packaging matches Ray's native behavior: nested .gitignore
        and .rayignore files apply with git's precedence (.rayignore last), a
        UTF-8 BOM on an ignore file is skipped, the
        RAY_RUNTIME_ENV_IGNORE_GITIGNORE=1 switch drops only .gitignore files,
        `excludes` use gitignore syntax, symlinked directories are followed,
        a symlink cycle is reported instead of recursed into, and empty
        directories survive the round trip."""
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
                    zbytes, _, _ = build_driver_package(
                        top_level_fn,
                        (1,),
                        {},
                        working_dir=root,
                        excludes=["*.md", "!/README.md"],
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
                    USER_FN_FILENAME,
                    RUNNER_FILENAME,
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
                build_driver_package(top_level_fn, (1,), {}, working_dir=root)
            zbytes, _, _ = build_driver_package(
                top_level_fn, (1,), {}, working_dir=root, excludes=["loop"]
            )
            with zipfile.ZipFile(io.BytesIO(zbytes)) as zf:
                self.assertIn("linked/mod.py", zf.namelist())

    def test_package_size_limit_fails_before_upload_and_names_largest_entries(self):
        """A working_dir whose compressed archive reaches the Jobs HTTP server's
        body limit is rejected while packaging, before any network I/O, with the
        offending entries named so the user knows what to exclude."""
        with tempfile.TemporaryDirectory() as tmpdir, patch.object(
            packaging, "MAX_PACKAGE_BYTES", 256 * 1024
        ):
            root = Path(tmpdir)
            (root / "small.py").write_text("X = 1\n", encoding="utf-8")
            (root / "data").mkdir()
            (root / "data" / "big.bin").write_bytes(os.urandom(512 * 1024))
            with self.assertRaisesRegex(RayClientError, "caps uploads at") as ctx:
                build_driver_package(top_level_fn, (1,), {}, working_dir=root)
            self.assertIn("data/big.bin", str(ctx.exception))
            self.assertIn("runtime_env['excludes']", str(ctx.exception))

            zbytes, _, _ = build_driver_package(
                top_level_fn, (1,), {}, working_dir=root, excludes=["data/"]
            )
            with zipfile.ZipFile(io.BytesIO(zbytes)) as zf:
                self.assertEqual(
                    set(zf.namelist()), {"small.py", USER_FN_FILENAME, RUNNER_FILENAME}
                )

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

        # Submission-ID-bound markers ignore foreign markers from nested jobs and
        # late spoofed worker lines after the driver's own marker.
        outer_payload = {"submission_id": "outer_1", "ok": True, "result": "OUTER-OK"}
        outer_b64 = base64.b64encode(json.dumps(outer_payload).encode("utf-8")).decode(
            "ascii"
        )
        spoof_payload = {"ok": True, "result": "SPOOFED"}
        spoof_b64 = base64.b64encode(json.dumps(spoof_payload).encode("utf-8")).decode(
            "ascii"
        )
        nested_raw = (
            f"{{'message': 'err\\n{RESULT_LINE_PREFIX}inner_1:bad_b64'}}\n"
            f"{RESULT_LINE_PREFIX}inner_1:{spoof_b64}\n"
            f"{RESULT_LINE_PREFIX}outer_1:{outer_b64}\n"
            f"{RESULT_LINE_PREFIX}{spoof_b64}\n"
        )
        clean_n, has_out_n, out_n, err_n = parse_logs_and_result(
            nested_raw, final=True, submission_id="outer_1"
        )
        self.assertTrue(has_out_n)
        self.assertEqual(out_n, "OUTER-OK")
        self.assertIsNone(err_n)
        self.assertIn("inner_1:bad_b64", clean_n)

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
            self.assertIsNone(c_default.namespace)
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

            with patch.dict(os.environ, {"RAY_AUTH_MODE": "disabled"}, clear=False):
                c_disabled = RayClient(address="http://127.0.0.1:8265")
                self.assertNotIn("Authorization", c_disabled._headers)

            with patch.dict(
                os.environ,
                {
                    "RAY_AUTH_TOKEN": "",
                    "RAY_AUTH_TOKEN_PATH": "/nonexistent/token/file",
                },
                clear=False,
            ):
                os.environ.pop("RAY_AUTH_TOKEN", None)
                with self.assertRaisesRegex(RayClientError, "RAY_AUTH_TOKEN_PATH"):
                    RayClient(address="http://127.0.0.1:8265")

            with patch.dict(
                os.environ,
                {"RAY_ADDRESS": "10.0.0.5:6379", "RAY_JOB_CONFIG_JSON_ENV_VAR": "{}"},
                clear=False,
            ):
                with self.assertRaisesRegex(RayClientError, "GCS gRPC port"):
                    RayClient()


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
        self.assertTrue(r1.done())
        self.assertEqual(r1.status(), "SUCCEEDED")
        self.assertEqual(r1.result(), {"doubled": 10})
        self.assertEqual(r1.logs(), "running step(5)\n")

        # Mutating info() copy must not corrupt cached status.
        info_copy = r1.info()
        info_copy.pop("status", None)
        self.assertEqual(r1.status(), "SUCCEEDED")

        r2 = client.run_function_as_driver(step, 21)
        self.assertFalse(r2.package_uploaded)
        self.assertEqual(r2.result(), {"doubled": 42})
        self.assertTrue(r2.delete())

        # Simulate cluster GCS package expiration (#46625): client must re-upload cleanly.
        with self.state.lock:
            self.state.packages.clear()
        r3 = client.run_function_as_driver(step, 9)
        self.assertTrue(r3.package_uploaded)
        self.assertEqual(r3.result(), {"doubled": 18})

        async def async_step(msg: str) -> str:
            return msg.upper()

        r4 = run_function_as_driver(
            async_step,
            "hello",
            address=self.address,
            headers={"X-Custom": "1"},
            namespace="custom_ns",
        )
        self.assertEqual(r4.result(), "HELLO")

    def test_large_payload_dataclass_annotations_and_dict_order(self):
        client = RayClient(address=self.address)

        def big_and_ordered(blob: str, **kw: Any) -> dict:
            from dataclasses import dataclass, fields
            from typing import ClassVar, get_type_hints

            class InnerPoint:
                x: int

            @dataclass
            class LocalCfg:
                pt: InnerPoint
                registry: ClassVar[dict] = {}

            hints = get_type_hints(LocalCfg, localns=locals())
            return {
                "len": len(blob),
                "kw_keys": list(kw.keys()),
                "fields": [f.name for f in fields(LocalCfg)],
                "has_pt_hint": "pt" in hints,
            }

        res_a = client.run_function_as_driver(
            big_and_ordered,
            "a" * 40_000,
            kwargs={"zeta": 1, "alpha": 2, "mid": 3},
        )
        self.assertTrue(res_a.package_uploaded)
        self.assertEqual(
            res_a.result(),
            {
                "len": 40_000,
                "kw_keys": ["zeta", "alpha", "mid"],
                "fields": ["pt"],
                "has_pt_hint": True,
            },
        )
        # Second >32 KiB call with different args reuses the driver/working_dir package!
        res_b = client.run_function_as_driver(
            big_and_ordered,
            "b" * 40_000,
            kwargs={"first": 1, "second": 2},
        )
        self.assertFalse(res_b.package_uploaded)
        self.assertEqual(res_b.result()["kw_keys"], ["first", "second"])

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
                import json as _json
                import os as _os

                import helper_mod  # type: ignore[import-not-found]

                assert _os.environ["K"] == "V"
                assert "RAY_CLIENT_PAYLOAD_B64" not in _os.environ
                assert "RAY_CLIENT_SUBMISSION_ID" not in _os.environ
                job_cfg = _json.loads(_os.environ["RAY_JOB_CONFIG_JSON_ENV_VAR"])
                assert "RAY_CLIENT_PAYLOAD_B64" not in job_cfg["runtime_env"].get(
                    "env_vars", {}
                )
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
        """Owned wait=True calls stop the remote job on Ctrl-C, timeout, or poll
        failure and attach .job; detached (wait=False) and get_job() observer
        handles never auto-stop on Ctrl-C or timeout."""
        client = RayClient(address=self.address)

        def nop() -> int:
            return 1

        # Observer / detached handles do NOT stop the job on Ctrl-C or timeout.
        detached = client.run_function_as_driver(nop, wait=False)
        with patch.object(detached, "_fetch_info", side_effect=KeyboardInterrupt):
            with self.assertRaises(KeyboardInterrupt):
                detached.wait()
        self.assertNotIn(detached.submission_id, self.state.stopped_jobs)

        with patch.object(detached, "_fetch_info", return_value={"status": "RUNNING"}):
            with self.assertRaises(RayJobTimeoutError) as ctx:
                detached.wait(timeout=0)
        self.assertIs(ctx.exception.job, detached)
        # RayJobTimeoutError and its JobResult/RayClient are picklable across processes.
        unpickled_timeout = pickle.loads(pickle.dumps(ctx.exception))
        self.assertEqual(unpickled_timeout.job.submission_id, detached.submission_id)
        self.assertNotIn(detached.submission_id, self.state.stopped_jobs)

        # Owned wait=True calls stop the remote job on timeout, Ctrl-C, and poll errors.
        with patch.object(JobResult, "_fetch_info", return_value={"status": "RUNNING"}):
            with self.assertRaises(RayJobTimeoutError) as sync_ctx:
                client.run_function_as_driver(nop, timeout=0)
        self.assertIn(sync_ctx.exception.job.submission_id, self.state.stopped_jobs)

        with patch.object(JobResult, "_fetch_info", side_effect=KeyboardInterrupt):
            with self.assertRaises(KeyboardInterrupt):
                client.run_function_as_driver(nop)
        self.assertEqual(len(self.state.stopped_jobs), 2)

        with patch.object(
            JobResult, "_fetch_info", side_effect=RayClientError("503 gateway down")
        ):
            with self.assertRaises(RayClientError) as err_ctx:
                client.run_function_as_driver(nop)
        self.assertIsNotNone(err_ctx.exception.job)
        self.assertIn(err_ctx.exception.submission_id, self.state.stopped_jobs)

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
        # Must survive crossing a process boundary (e.g. ProcessPoolExecutor).
        restored = pickle.loads(pickle.dumps(ctx.exception))
        self.assertEqual(restored.exc_type, "ValueError")
        self.assertEqual(str(restored), str(ctx.exception))

        def exiting_driver():
            import sys as _sys

            _sys.exit(2)

        with self.assertRaises(RayJobExecutionError) as exit_ctx:
            client.run_function_as_driver(exiting_driver)
        self.assertEqual(exit_ctx.exception.exc_type, "SystemExit")

        def clean_exit_driver():
            import sys as _sys

            _sys.exit(0)

        self.assertIsNone(client.run_function_as_driver(clean_exit_driver).result())

    def test_method_defined_driver_with_hints_runs_verbatim(self):
        """A driver defined as a method, using typing hints in nested defs and an
        indented multi-line string, runs on the (mock) cluster and returns the
        string with its original indentation."""

        class Pipeline:
            def render(self, rows: List[int]) -> Dict[str, Any]:
                def total(xs: List[int]) -> Optional[int]:
                    return sum(xs)

                template = """
                SELECT {n}
                  FROM t
                """
                return {"sql": template.format(n=total(rows)), "scale": GLOBAL_SCALE}

        expected = Pipeline().render([1, 2])
        client = RayClient(address=self.address)
        self.assertEqual(
            client.run_function_as_driver(Pipeline.render, None, [1, 2]).result(),
            expected,
        )

    def test_retries_transient_503_rejects_ray_scheme_and_non_json_bodies(self):
        with self.assertRaises(RayClientError):
            RayClient(address="ray://127.0.0.1:10001")

        self.state.fail_next_status_polls = 2
        client = RayClient(address=self.address)

        def ok_fn() -> int:
            return 7

        self.assertEqual(client.run_function_as_driver(ok_fn).result(), 7)

        html_server = ThreadingHTTPServer(("127.0.0.1", 0), _HtmlLoginPageHandler)
        threading.Thread(target=html_server.serve_forever, daemon=True).start()
        try:
            html_address = f"http://127.0.0.1:{html_server.server_address[1]}"
            with self.assertRaisesRegex(RayClientError, "non-JSON response"):
                RayClient(address=html_address).list_jobs()
        finally:
            html_server.shutdown()
            html_server.server_close()


if __name__ == "__main__":
    unittest.main()
