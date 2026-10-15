"""Function source extraction, JSON validation, and deterministic zip packaging.

Design constraints:
  * Zero `ray` or `cloudpickle` dependency on the client.
  * Only two things cross the client-cluster boundary: files (via `runtime_env`
    `working_dir`) and JSON data.
"""

import ast
import base64
import builtins
import dis
import fnmatch
import hashlib
import inspect
import io
import json
import textwrap
import types
import zipfile
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

from ray_client.exceptions import RayClientError, RayClientSerializationError

# Linux caps a single environment variable at 128 KiB (MAX_ARG_STRLEN); above this
# threshold the payload ships as a file inside the package instead.
MAX_ENV_PAYLOAD_BYTES = 32 * 1024
RAY_PKG_PREFIX = "_ray_pkg_"
RESULT_LINE_PREFIX = "__RAY_CLIENT_RESULT__:"
USER_FN_FILENAME = "_ray_client_user_fn.py"
RUNNER_FILENAME = "_ray_client_driver_runner.py"
PAYLOAD_FILENAME = "_ray_client_payload.json"
RESERVED_ARCHIVE_NAMES = frozenset(
    {USER_FN_FILENAME, RUNNER_FILENAME, PAYLOAD_FILENAME}
)
DEFAULT_EXCLUDES = frozenset(
    {".git", "__pycache__", "*.pyc", ".pytest_cache", ".mypy_cache", ".venv"}
)
# Names that exist in the generated cluster-side module without being captured
# (`builtins`, the module-level `import ray`, and module dunders).
_PROVIDED_NAMES = frozenset(dir(builtins)) | {
    "ray",
    "__name__",
    "__doc__",
    "__qualname__",
    "__package__",
    "__annotations__",
    "__file__",
}

RUNNER_SCRIPT_CONTENT = (
    Path(__file__).with_name("_driver_runner.py").read_text(encoding="utf-8")
)


def to_json(value: Any, context_name: str) -> str:
    """Serializes `value` to strict, key-sorted JSON (no NaN/Infinity)."""
    try:
        return json.dumps(value, allow_nan=False, sort_keys=True)
    except (TypeError, OverflowError, ValueError) as exc:
        raise RayClientSerializationError(
            f"{context_name} must be JSON-serializable (no NaN/Infinity): {exc}"
        ) from exc


def parse_logs_and_result(
    raw_logs: str, final: bool = True
) -> Tuple[str, bool, Any, Optional[Dict[str, str]]]:
    """Splits raw job logs into `(user_logs, has_output, output, error_dict)`.

    With `final=False` (live streaming), an incomplete marker line is skipped
    instead of treated as malformed.
    """
    user_lines: List[str] = []
    has_output = False
    output: Any = None
    error: Optional[Dict[str, str]] = None

    for line in raw_logs.splitlines(keepends=True):
        prefix, marker, rest = line.partition(RESULT_LINE_PREFIX)
        if not marker:
            user_lines.append(line)
            continue
        if prefix:
            user_lines.append(prefix)
        if not final and not line.endswith("\n"):
            continue
        try:
            payload = json.loads(base64.b64decode(rest.strip()))
        except Exception as exc:
            if final:
                raise RayClientError(
                    f"Malformed Ray Client result line: {exc}"
                ) from exc
            continue
        if payload.get("ok"):
            has_output, output = True, payload.get("result")
        else:
            error = payload.get("error")

    return "".join(user_lines), has_output, output, error


def _referenced_global_names(
    code: types.CodeType, func_node: ast.FunctionDef
) -> Set[str]:
    """Names `fn` reads from module scope: bytecode globals plus default-arg expressions.

    Bytecode gives exact `LOAD_GLOBAL`s (including nested functions/classes).
    Default argument values are evaluated at definition time, so they never
    appear in the function's bytecode and are taken from the AST instead.
    """
    names: Set[str] = set()
    stack = [code]
    while stack:
        current = stack.pop()
        insns = list(dis.get_instructions(current))
        stored = {i.argval for i in insns if i.opname == "STORE_NAME"}
        for instr in insns:
            if instr.opname == "LOAD_GLOBAL" or (
                instr.opname == "LOAD_NAME" and instr.argval not in stored
            ):
                names.add(instr.argval)
        stack.extend(c for c in current.co_consts if isinstance(c, types.CodeType))
    for default in [*func_node.args.defaults, *func_node.args.kw_defaults]:
        if default is not None:
            names.update(
                n.id
                for n in ast.walk(default)
                if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Load)
            )
    return names - _PROVIDED_NAMES


def _validate_captured(name: str, val: Any, fn_name: str, kind: str) -> None:
    if isinstance(val, types.ModuleType):
        raise RayClientSerializationError(
            f"{kind} '{name}' referenced by '{fn_name}' is a module; move "
            f"'import {name}' inside '{fn_name}' so it is imported on the cluster."
        )
    if inspect.isfunction(val) or isinstance(val, type):
        raise RayClientSerializationError(
            f"{kind} '{name}' referenced by '{fn_name}' is a function/class; "
            f"define '{name}' inside '{fn_name}' or place it in a module under working_dir."
        )
    to_json(val, f"{kind} '{name}' referenced by '{fn_name}'")


def _parse_function_ast(fn: Callable[..., Any]) -> Tuple[ast.FunctionDef, str]:
    try:
        raw_source = inspect.getsource(fn)
    except (OSError, TypeError) as exc:
        raise RayClientSerializationError(
            f"Could not extract source code for function '{fn.__name__}': {exc}. "
            "Define it in a .py file or a notebook cell."
        ) from exc

    # Methods and nested functions are indented; `dedent` handles the common case,
    # and the `if True:` wrapper handles bodies that `dedent` cannot normalize
    # (e.g., multi-line strings flush with column 0).
    dedented = textwrap.dedent(raw_source)
    try:
        tree = ast.parse(dedented)
        source = dedented
    except (IndentationError, SyntaxError):
        wrapped = ast.parse("if True:\n" + textwrap.indent(raw_source, "    "))
        tree = ast.Module(body=wrapped.body[0].body, type_ignores=[])  # type: ignore[attr-defined]
        source = ast.unparse(tree) + "\n"

    func_node = tree.body[0] if tree.body else None
    if not isinstance(func_node, (ast.FunctionDef, ast.AsyncFunctionDef)):
        raise RayClientSerializationError(
            f"Expected a named `def` function for '{fn.__name__}'."
        )
    if func_node.decorator_list or hasattr(fn, "__wrapped__"):
        # A decorated `fn` is usually the decorator's wrapper: its bytecode,
        # closure, and __name__ no longer describe the source we would ship.
        raise RayClientSerializationError(
            f"'{fn.__name__}' is decorated; run_function_as_driver requires a plain "
            "`def` so the exact source can be executed on the cluster."
        )
    return func_node, source if source.endswith("\n") else source + "\n"  # type: ignore[return-value]


def extract_function_source_and_globals(
    fn: Callable[..., Any],
) -> Tuple[str, str, Dict[str, Any]]:
    """Returns `(fn_name, module_source, captured_globals)` for executing `fn` on the cluster.

    `module_source` is a standalone module containing only `fn`; `captured_globals`
    are the JSON constants it references from the client's module/closure scope.
    """
    if not inspect.isfunction(fn) or fn.__name__ == "<lambda>":
        raise RayClientSerializationError(
            "run_function_as_driver requires a named Python `def` function."
        )

    func_node, body_source = _parse_function_ast(fn)

    captured: Dict[str, Any] = {}
    for name, cell in zip(fn.__code__.co_freevars, fn.__closure__ or ()):
        _validate_captured(name, cell.cell_contents, fn.__name__, "Closure variable")
        captured[name] = cell.cell_contents

    for name in sorted(_referenced_global_names(fn.__code__, func_node)):
        if name in captured or name == fn.__name__ or name not in fn.__globals__:
            continue
        _validate_captured(name, fn.__globals__[name], fn.__name__, "Global")
        captured[name] = fn.__globals__[name]

    # `annotations` keeps type hints lazy so `List[int]`-style hints from the
    # client file do not need their imports on the cluster.
    module_source = (
        "from __future__ import annotations\n\nimport ray  # noqa: F401\n\n"
        + body_source
    )
    return fn.__name__, module_source, captured


def _write_deterministic_entry(zf: zipfile.ZipFile, arcname: str, data: bytes) -> None:
    info = zipfile.ZipInfo(filename=arcname, date_time=(1980, 1, 1, 0, 0, 0))
    info.compress_type = zipfile.ZIP_DEFLATED
    info.external_attr = 0o644 << 16
    zf.writestr(info, data)


def _is_excluded(arcname: str, patterns: Set[str]) -> bool:
    """Patterns match a single path segment (`.git`, `*.pyc`) or a path relative to working_dir (`data/big.bin`, `logs/`)."""
    segments = arcname.split("/")
    for pat in patterns:
        rel = pat.strip("/")
        if fnmatch.fnmatch(arcname, rel) or arcname.startswith(rel + "/"):
            return True
        if any(fnmatch.fnmatch(seg, pat) for seg in segments):
            return True
    return False


def build_driver_package(
    fn: Callable[..., Any],
    args: Tuple[Any, ...],
    kwargs: Dict[str, Any],
    working_dir: Optional[Union[str, Path]] = None,
    excludes: Optional[List[str]] = None,
) -> Tuple[bytes, str, Optional[str]]:
    """Builds the job package and returns `(zip_bytes, gcs_uri, env_payload_b64)`.

    The zip holds only call-invariant content (user function source, the driver
    runner, and `working_dir` files) with fixed timestamps, so repeated calls with
    different arguments hash to the same `gcs_uri` and hit Ray's package cache.
    Per-call arguments travel in `env_payload_b64` unless they exceed
    MAX_ENV_PAYLOAD_BYTES, in which case they are written into the zip instead.
    """
    inspect.signature(fn).bind(*args, **kwargs)
    fn_name, user_fn_source, captured_globals = extract_function_source_and_globals(fn)
    payload_bytes = to_json(
        {
            "fn_name": fn_name,
            "args": list(args),
            "kwargs": kwargs,
            "captured_globals": captured_globals,
        },
        "Function arguments",
    ).encode("utf-8")
    payload_b64 = base64.b64encode(payload_bytes).decode("ascii")
    use_env_payload = len(payload_b64) <= MAX_ENV_PAYLOAD_BYTES

    patterns = DEFAULT_EXCLUDES | set(excludes or ())
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        if working_dir is not None:
            root = Path(working_dir).resolve()
            if not root.is_dir():
                raise ValueError(f"working_dir '{working_dir}' is not a directory.")
            for p in sorted(root.rglob("*")):
                arcname = p.relative_to(root).as_posix()
                if (
                    p.is_file()
                    and arcname not in RESERVED_ARCHIVE_NAMES
                    and not _is_excluded(arcname, patterns)
                ):
                    _write_deterministic_entry(zf, arcname, p.read_bytes())
        _write_deterministic_entry(zf, USER_FN_FILENAME, user_fn_source.encode("utf-8"))
        _write_deterministic_entry(
            zf, RUNNER_FILENAME, RUNNER_SCRIPT_CONTENT.encode("utf-8")
        )
        if not use_env_payload:
            _write_deterministic_entry(zf, PAYLOAD_FILENAME, payload_bytes)

    zip_bytes = buf.getvalue()
    gcs_uri = f"gcs://{RAY_PKG_PREFIX}{hashlib.sha1(zip_bytes).hexdigest()}.zip"
    return zip_bytes, gcs_uri, (payload_b64 if use_env_payload else None)
