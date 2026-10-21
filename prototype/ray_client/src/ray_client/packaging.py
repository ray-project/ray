"""Function source extraction, JSON validation, and deterministic zip packaging.

Design constraints:
  * Zero `ray` or `cloudpickle` dependency on the client.
  * Only two things cross the client-cluster boundary: files (via `runtime_env`
    `working_dir`) and JSON data.

The gitignore-syntax matcher behind `excludes` and .gitignore/.rayignore files
lives in `_ignore.py`.
"""

import __future__

import ast
import base64
import builtins
import dis
import hashlib
import importlib.resources
import inspect
import io
import json
import math
import os
import types
import zipfile
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    FrozenSet,
    Iterator,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
    Union,
)

from ray_client._ignore import (
    _compile_ignore_rules,
    _ignore_rules_in,
    _IgnoreRule,
    _is_ignored,
)
from ray_client.exceptions import RayClientError, RayClientSerializationError

# Linux caps a single environment variable at 128 KiB (MAX_ARG_STRLEN); above this
# threshold the payload ships as a separate content-addressed package in py_modules.
MAX_ENV_PAYLOAD_BYTES = 32 * 1024
# The Ray dashboard's HTTP server (aiohttp `client_max_size`, see
# ray_constants.DASHBOARD_CLIENT_MAX_SIZE) caps request bodies at 100 MiB, so a
# larger package can never be uploaded through the Jobs API.
MAX_PACKAGE_BYTES = 100 * 1024 * 1024
RAY_PKG_PREFIX = "_ray_pkg_"
RESULT_LINE_PREFIX = "__RAY_CLIENT_RESULT__:"
USER_FN_FILENAME = "_ray_client_user_fn.py"
RUNNER_FILENAME = "_ray_client_driver_runner.py"
PAYLOAD_FILENAME = "_ray_client_payload.json"
RESERVED_ARCHIVE_NAMES = frozenset(
    {USER_FN_FILENAME, RUNNER_FILENAME, PAYLOAD_FILENAME}
)
# gitignore-syntax patterns applied to every working_dir, like Ray's
# RAY_RUNTIME_ENV_DEFAULT_EXCLUDES plus the usual Python caches.
DEFAULT_EXCLUDES = (
    ".git",
    ".venv",
    "venv",
    "__pycache__",
    "*.pyc",
    ".pytest_cache",
    ".mypy_cache",
)
# Same switches `ray job submit` honors: "1" skips .gitignore files (.rayignore
# files are always applied), and RAY_OVERRIDE_RUNTIME_ENV_DEFAULT_EXCLUDES
# replaces DEFAULT_EXCLUDES with a comma-separated list.
IGNORE_GITIGNORE_ENV_VAR = "RAY_RUNTIME_ENV_IGNORE_GITIGNORE"
OVERRIDE_DEFAULT_EXCLUDES_ENV_VAR = "RAY_OVERRIDE_RUNTIME_ENV_DEFAULT_EXCLUDES"
_READ_CHUNK_BYTES = 1024 * 1024
_HINT_HELPER_LINE = (
    "_RayHint = type('_RayHint', (type,), {"
    "'__getitem__': lambda c, _: c, "
    "'__or__': lambda c, _: c, "
    "'__ror__': lambda c, _: c"
    "})('_RayHint', (), {})"
)
# Module attributes the import system sets on the cluster-side module; they must
# not be overwritten with the client module's values.
_MODULE_ATTRS = frozenset(
    {
        "__name__",
        "__qualname__",
        "__module__",
        "__doc__",
        "__package__",
        "__spec__",
        "__loader__",
        "__file__",
        "__cached__",
        "__builtins__",
        "__annotations__",
    }
)
# LOAD_FROM_DICT_OR_GLOBALS is emitted for class bodies on Python 3.12+.
_LOAD_OPS = frozenset({"LOAD_GLOBAL", "LOAD_NAME", "LOAD_FROM_DICT_OR_GLOBALS"})
# Only class/module-scope STORE_NAME defines a name before a subsequent load in
# the same scope; STORE_GLOBAL inside a function (`global X; if cond: X = 1`)
# may be conditional and must not suppress capturing the module global `X`.
_STORE_OPS = frozenset({"STORE_NAME"})


def _load_runner_script() -> str:
    try:
        return (
            importlib.resources.files("ray_client")
            .joinpath("_driver_runner.py")
            .read_text(encoding="utf-8")
        )
    except Exception:
        return Path(__file__).with_name("_driver_runner.py").read_text(encoding="utf-8")


RUNNER_SCRIPT_CONTENT = _load_runner_script()


def _validate_strict_json(value: Any, path: str) -> None:
    """Validates that `value` round-trips losslessly through JSON with exact types."""
    if value is None or value is True or value is False:
        return
    val_type = type(value)
    if val_type is int or val_type is str:
        return
    if val_type is float:
        if not math.isfinite(value):
            raise RayClientSerializationError(
                f"{path} must be JSON-serializable (no NaN/Infinity): got {value!r}."
            )
        return
    if val_type is list:
        for idx, item in enumerate(value):
            _validate_strict_json(item, f"{path}[{idx}]")
        return
    if val_type is dict:
        for k, v in value.items():
            if type(k) is not str:
                raise RayClientSerializationError(
                    f"{path} has non-string dict key {k!r} of type "
                    f"'{type(k).__name__}'; JSON object keys must be str."
                )
            _validate_strict_json(v, f"{path}[{k!r}]")
        return
    if isinstance(value, tuple):
        raise RayClientSerializationError(
            f"{path} is a tuple, which JSON would convert to a list; "
            "convert tuples to lists explicitly."
        )
    raise RayClientSerializationError(
        f"{path} must be JSON-serializable (dict, list, str, int, float, bool, "
        f"or None), got '{val_type.__name__}'."
    )


def to_json(value: Any, context_name: str, *, sort_keys: bool = False) -> str:
    """Validates and serializes `value` to strict JSON (preserving dict key order by default)."""
    _validate_strict_json(value, context_name)
    try:
        return json.dumps(value, allow_nan=False, sort_keys=sort_keys)
    except (TypeError, OverflowError, ValueError) as exc:
        raise RayClientSerializationError(
            f"{context_name} must be JSON-serializable (no NaN/Infinity): {exc}"
        ) from exc


def parse_logs_and_result(
    raw_logs: str,
    final: bool = True,
    submission_id: Optional[str] = None,
) -> Tuple[str, bool, Any, Optional[Dict[str, str]]]:
    """Splits raw job logs into `(user_logs, has_output, output, error_dict)`.

    With `final=False` (live streaming), an incomplete marker line is skipped
    instead of treated as malformed. When `submission_id` is provided, markers
    from other jobs (e.g., nested `ray_client` calls whose `JobInfo.message` is
    logged by an outer driver) or spoofed worker lines after the first valid
    driver marker are ignored.
    """
    user_lines: List[str] = []
    has_output = False
    output: Any = None
    error: Optional[Dict[str, str]] = None
    has_tagged_match = False
    malformed_exc: Optional[Exception] = None

    for line in raw_logs.splitlines(keepends=True):
        prefix, marker, rest = line.partition(RESULT_LINE_PREFIX)
        if not marker:
            user_lines.append(line)
            continue
        if not final and not line.endswith("\n"):
            if prefix:
                user_lines.append(prefix)
            continue

        body = rest.strip()
        line_sub_id: Optional[str] = None
        if ":" in body:
            candidate_id, _, candidate_b64 = body.partition(":")
            if candidate_id and all(c.isalnum() or c in "_-" for c in candidate_id):
                line_sub_id = candidate_id
                body = candidate_b64.strip()

        if (
            submission_id is not None
            and line_sub_id is not None
            and line_sub_id != submission_id
        ):
            # Foreign marker from a nested job's status/logs printed by this job.
            user_lines.append(line)
            continue

        try:
            payload = json.loads(base64.b64decode(body, validate=True))
            if not isinstance(payload, dict) or "ok" not in payload:
                raise ValueError("Result marker JSON is missing 'ok' field")
        except Exception as exc:
            if prefix:
                # Marker substring appeared mid-line (e.g., inside a printed dict or JSON log).
                user_lines.append(line)
                continue
            malformed_exc = exc
            continue

        effective_sub_id = line_sub_id or payload.get("submission_id")
        if (
            submission_id is not None
            and effective_sub_id is not None
            and effective_sub_id != submission_id
        ):
            user_lines.append(line)
            continue

        if prefix:
            user_lines.append(prefix)
        is_tagged = submission_id is not None and effective_sub_id == submission_id
        if is_tagged:
            # Authenticated driver marker (tagged with RAY_CLIENT_SUBMISSION_ID,
            # which is scrubbed before user/worker code runs) overrides any
            # earlier untagged worker spoof and locks in the first tagged match.
            if not has_tagged_match:
                has_tagged_match = True
                if payload.get("ok"):
                    has_output, output, error = True, payload.get("result"), None
                else:
                    has_output, output, error = False, None, payload.get("error")
        elif not has_tagged_match and not has_output and error is None:
            if payload.get("ok"):
                has_output, output = True, payload.get("result")
            else:
                error = payload.get("error")

    if final and not has_output and error is None and malformed_exc is not None:
        raise RayClientError(
            f"Malformed Ray Client result line: {malformed_exc}"
        ) from malformed_exc

    return "".join(user_lines), has_output, output, error


def _referenced_global_names(module_code: types.CodeType) -> Set[str]:
    """Names the shipped module reads from module scope without defining them first.

    Walks every code object in the module (nested functions, classes, and
    comprehensions included) in instruction order, so a name bound before it is
    read (`import json` inside the function, a class attribute) is not reported.
    Module-level loads cover default-argument expressions, which are evaluated
    when the `def` statement runs rather than inside the function's bytecode.
    """
    names: Set[str] = set()
    stack = [module_code]
    while stack:
        current = stack.pop()
        stored: Set[str] = set()
        for instr in dis.get_instructions(current):
            if instr.opname in _STORE_OPS:
                stored.add(instr.argval)
            elif instr.opname in _LOAD_OPS and instr.argval not in stored:
                names.add(instr.argval)
        stack.extend(c for c in current.co_consts if isinstance(c, types.CodeType))
    defined_at_module_level = {
        i.argval for i in dis.get_instructions(module_code) if i.opname == "STORE_NAME"
    }
    return names - defined_at_module_level - _MODULE_ATTRS


def _definition_time_names(module_code: types.CodeType, fn_name: str) -> Set[str]:
    """Names loaded when the module body (and any comprehension in `def` defaults) runs."""
    names: Set[str] = set()
    stack = [module_code]
    while stack:
        current = stack.pop()
        for instr in dis.get_instructions(current):
            if instr.opname in _LOAD_OPS:
                names.add(instr.argval)
        stack.extend(
            c
            for c in current.co_consts
            if isinstance(c, types.CodeType) and c.co_name != fn_name
        )
    return names


def _validate_captured(name: str, val: Any, fn_name: str, kind: str) -> None:
    if isinstance(val, types.ModuleType):
        raise RayClientSerializationError(
            f"{kind} '{name}' referenced by '{fn_name}' is a module; move "
            f"'import {name}' inside '{fn_name}' so it is imported on the cluster."
        )
    if callable(val):
        raise RayClientSerializationError(
            f"{kind} '{name}' referenced by '{fn_name}' is a function, class, or "
            f"other callable; define '{name}' inside '{fn_name}' or place it in a "
            "module under working_dir."
        )
    to_json(val, f"{kind} '{name}' referenced by '{fn_name}'")


def _check_shadowed_defaults(
    func_node: Union[ast.FunctionDef, ast.AsyncFunctionDef],
    fn: Callable[..., Any],
    cells: Dict[str, Any],
) -> None:
    """Verifies that default argument expressions did not resolve to an enclosing
    class or function scope variable that happens to share a name with a module global."""
    cell_env: Dict[str, Any] = {}
    for k, cell in cells.items():
        try:
            cell_env[k] = cell.cell_contents
        except ValueError:
            pass
    eval_globals = {"__builtins__": builtins.__dict__, **fn.__globals__, **cell_env}

    pairs: List[Tuple[ast.expr, Any]] = []
    if fn.__defaults__ and func_node.args.defaults:
        pairs.extend(zip(func_node.args.defaults, fn.__defaults__))
    if fn.__kwdefaults__ and func_node.args.kw_defaults:
        for kw_arg, kw_def in zip(
            func_node.args.kwonlyargs, func_node.args.kw_defaults
        ):
            if kw_def is not None and kw_arg.arg in fn.__kwdefaults__:
                pairs.append((kw_def, fn.__kwdefaults__[kw_arg.arg]))

    for expr_node, actual_default in pairs:
        ref_names = [
            n.id
            for n in ast.walk(expr_node)
            if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Load)
        ]
        if not ref_names:
            continue
        try:
            expr = ast.Expression(body=expr_node)
            ast.fix_missing_locations(expr)
            re_eval = eval(compile(expr, "<default>", "eval"), eval_globals)
        except Exception:
            for name in ref_names:
                if name not in eval_globals:
                    raise RayClientSerializationError(
                        f"Default argument of '{fn.__name__}' references '{name}' from an "
                        "enclosing function or class scope; pass it as an argument or "
                        "define it at module level."
                    ) from None
            continue
        if type(re_eval) is not type(actual_default) or re_eval != actual_default:
            shadowed = next(
                (n for n in ref_names if n in fn.__globals__ and n not in cell_env),
                ref_names[0],
            )
            raise RayClientSerializationError(
                f"Default argument of '{fn.__name__}' references '{shadowed}' from an "
                "enclosing function or class scope; pass it as an argument or define "
                "it at module level."
            )


def _extract_func_node_and_body(
    fn: Callable[..., Any],
) -> Tuple[Union[ast.FunctionDef, ast.AsyncFunctionDef], str, bool]:
    try:
        raw_source = inspect.getsource(fn)
    except (OSError, TypeError) as exc:
        raise RayClientSerializationError(
            f"Could not extract source code for function '{fn.__name__}': {exc}. "
            "Define it in a .py file or a notebook cell."
        ) from exc

    indented = raw_source[0].isspace()
    # Methods and nested functions are indented. Wrapping them in `if True:`
    # still defines `fn` at module scope but ships the source byte-for-byte;
    # dedenting would also strip the inside of multi-line string literals.
    body = f"if True:\n{raw_source}" if indented else raw_source
    if not body.endswith("\n"):
        body += "\n"
    try:
        tree = ast.parse(body)
    except SyntaxError as exc:
        raise RayClientSerializationError(
            f"Could not parse the source of '{fn.__name__}': {exc}"
        ) from exc

    func_node = tree.body[0]
    if isinstance(func_node, ast.If):
        func_node = func_node.body[0]
    if (
        not isinstance(func_node, (ast.FunctionDef, ast.AsyncFunctionDef))
        or func_node.name != fn.__name__
    ):
        raise RayClientSerializationError(
            f"Expected the source of '{fn.__name__}' to start with "
            f"`def {fn.__name__}`; it starts with `{ast.unparse(func_node)[:60]}`."
        )
    if func_node.decorator_list or hasattr(fn, "__wrapped__"):
        # A decorated `fn` is usually the decorator's wrapper: its bytecode,
        # closure, and __name__ no longer describe the source we would ship.
        raise RayClientSerializationError(
            f"'{fn.__name__}' is decorated; run_function_as_driver requires a plain "
            "`def` so the exact source can be executed on the cluster."
        )
    return func_node, body, indented


def extract_function_source_and_globals(
    fn: Callable[..., Any],
) -> Tuple[str, str, Dict[str, Any]]:
    """Returns `(fn_name, module_source, captured_globals)` for executing `fn` on the cluster.

    `module_source` is a standalone module containing `fn` (with line numbers aligned
    to the source file); `captured_globals` are the strict-JSON constants it
    references from the client's closure/module scope.
    """
    if not inspect.isfunction(fn) or fn.__name__ == "<lambda>":
        raise RayClientSerializationError(
            "run_function_as_driver requires a named Python `def` function."
        )
    if inspect.isgeneratorfunction(fn) or inspect.isasyncgenfunction(fn):
        raise RayClientSerializationError(
            f"'{fn.__name__}' is a generator function; run_function_as_driver "
            "requires a regular or async function returning strict JSON."
        )

    func_node, body, indented = _extract_func_node_and_body(fn)
    try:
        deferred_code = compile(
            body,
            USER_FN_FILENAME,
            "exec",
            flags=__future__.CO_FUTURE_ANNOTATIONS,
            dont_inherit=True,
        )
        eager_code = compile(body, USER_FN_FILENAME, "exec", dont_inherit=True)
    except SyntaxError as exc:
        raise RayClientSerializationError(
            f"'{fn.__name__}' cannot run as a standalone module: {exc}"
        ) from exc

    runtime_names = _referenced_global_names(deferred_code) - {"ray"}
    all_names = _referenced_global_names(eager_code) - {"ray"}
    # Names referenced solely inside type annotations (e.g. top-level `from typing
    # import List, Optional`) receive a lightweight subscriptable placeholder in the
    # module header so runtime annotation consumers (FastAPI, dataclasses,
    # get_type_hints) inside `fn` evaluate real local types without injecting
    # `from __future__ import annotations`.
    hint_names = sorted((all_names - runtime_names) - set(dir(builtins)))

    header_lines = ["import ray  # noqa: F401"]
    if hint_names:
        header_lines.append(f"{_HINT_HELPER_LINE}; {' = '.join(hint_names)} = _RayHint")
    client_future_annotations = bool(
        fn.__code__.co_flags & __future__.CO_FUTURE_ANNOTATIONS
    )
    if client_future_annotations:
        header_lines.insert(0, "from __future__ import annotations")

    header = "\n".join(header_lines) + "\n"
    target_lineno = getattr(fn.__code__, "co_firstlineno", 1)
    wrapper_lines = 1 if indented else 0
    pad_lines = max(1, target_lineno - len(header_lines) - wrapper_lines)
    module_source = header + ("\n" * (pad_lines - 1)) + body

    cells = dict(zip(fn.__code__.co_freevars, fn.__closure__ or ()))
    _check_shadowed_defaults(func_node, fn, cells)
    definition_time = _definition_time_names(deferred_code, fn.__name__)

    captured: Dict[str, Any] = {}
    for name in sorted(runtime_names):
        if name in cells:
            try:
                value = cells[name].cell_contents
            except ValueError:
                raise RayClientSerializationError(
                    f"Closure variable '{name}' referenced by '{fn.__name__}' is "
                    "not assigned yet; assign it before calling run_function_as_driver."
                ) from None
            kind = "Closure variable"
        elif name in fn.__globals__:
            value, kind = fn.__globals__[name], "Global"
        elif name in definition_time and not hasattr(builtins, name):
            raise RayClientSerializationError(
                f"Default argument of '{fn.__name__}' references '{name}' from an "
                "enclosing function or class scope; pass it as an argument or define "
                "it at module level."
            )
        else:
            # A builtin, or a NameError at call time on the client and cluster alike.
            continue
        _validate_captured(name, value, fn.__name__, kind)
        captured[name] = value
    return fn.__name__, module_source, captured


def _write_deterministic_entry(
    zf: zipfile.ZipFile,
    arcname: str,
    data: bytes,
    entry_sizes: Optional[List[Tuple[int, str]]] = None,
) -> None:
    info = zipfile.ZipInfo(filename=arcname, date_time=(1980, 1, 1, 0, 0, 0))
    info.compress_type = zipfile.ZIP_DEFLATED
    info.create_system = 3
    info.external_attr = (0o100000 | 0o644) << 16
    zf.writestr(info, data)
    if entry_sizes is not None:
        entry_sizes.append((zf.getinfo(arcname).compress_size, arcname))


def _default_excludes() -> Tuple[str, ...]:
    override = os.environ.get(OVERRIDE_DEFAULT_EXCLUDES_ENV_VAR)
    if override is not None:
        if not override.strip():
            return ()
        return tuple(p.strip() for p in override.split(",") if p.strip())
    return DEFAULT_EXCLUDES


def _iter_working_dir(
    root: Path, exclude_rules: Sequence[_IgnoreRule], include_gitignore: bool
) -> Iterator[Tuple[str, Optional[Path]]]:
    """Yields `(arcname, path)` for each file to package and `(arcname + "/", None)`
    for each empty directory, in a deterministic order (per directory: its files
    in name order, then each subdirectory in name order).

    Mirrors Ray's native working_dir packaging: symlinked directories are followed
    and an excluded directory is never entered, so a negated pattern cannot
    re-include anything below it. Nested .gitignore/.rayignore files apply to
    their own subtree and take precedence over those of parent directories.
    """
    # Pending directories as (path, arcname prefix, inherited rules, real paths
    # of the ancestors); an explicit stack keeps deep trees off the Python
    # recursion limit.
    stack: List[Tuple[Path, str, List[_IgnoreRule], FrozenSet[str]]] = [
        (root, "", [], frozenset({os.path.realpath(root)}))
    ]
    while stack:
        directory, base, rules, ancestors = stack.pop()
        rules = rules + _ignore_rules_in(directory, base, include_gitignore)
        with os.scandir(directory) as it:
            entries = sorted(it, key=lambda e: e.name)
        if not entries and base:
            yield base, None
        subdirs: List[Tuple[Path, str, List[_IgnoreRule], FrozenSet[str]]] = []
        for entry in entries:
            arcname = base + entry.name
            is_dir = entry.is_dir()
            if (
                arcname in RESERVED_ARCHIVE_NAMES
                or _is_ignored(arcname, is_dir, exclude_rules)
                or _is_ignored(arcname, is_dir, rules)
            ):
                continue
            if is_dir:
                real = os.path.realpath(entry.path)
                if real in ancestors:
                    raise RayClientError(
                        f"working_dir symlink '{arcname}' points to its own ancestor "
                        f"'{real}' and cannot be packaged; add it to "
                        "runtime_env['excludes']."
                    )
                subdirs.append(
                    (Path(entry.path), arcname + "/", rules, ancestors | {real})
                )
            elif entry.is_file():  # skips broken symlinks, sockets, and FIFOs
                yield arcname, Path(entry.path)
        stack.extend(reversed(subdirs))


def _mib(num_bytes: int) -> str:
    return f"{num_bytes / 1024**2:.1f} MiB"


def _package_too_large(size: int, entries: Sequence[Tuple[int, str]]) -> RayClientError:
    largest = ", ".join(
        f"{name} ({_mib(n)})" for n, name in sorted(entries, reverse=True)[:5]
    )
    return RayClientError(
        f"The job package is {_mib(size)}, but the Ray Jobs HTTP server caps "
        f"uploads at {_mib(MAX_PACKAGE_BYTES)}. Exclude large files with "
        "runtime_env['excludes'] or a .rayignore file, or keep data in object storage "
        f"and pass URIs. Largest compressed entries so far: {largest or 'none'}."
    )


def _write_file_entry(
    zf: zipfile.ZipFile,
    buf: io.BytesIO,
    arcname: str,
    path: Path,
    entry_sizes: List[Tuple[int, str]],
) -> None:
    """Streams `path` into the zip, failing as soon as the archive hits the size limit."""
    st = path.stat()
    info = zipfile.ZipInfo(filename=arcname, date_time=(1980, 1, 1, 0, 0, 0))
    info.compress_type = zipfile.ZIP_DEFLATED
    info.create_system = 3
    mode = 0o755 if (st.st_mode & 0o111) else 0o644
    info.external_attr = (0o100000 | mode) << 16
    info.file_size = st.st_size  # lets zipfile pick zip64 headers when needed
    with path.open("rb") as src, zf.open(info, "w") as dst:
        while chunk := src.read(_READ_CHUNK_BYTES):
            dst.write(chunk)
            if buf.tell() >= MAX_PACKAGE_BYTES:
                partial = (buf.tell() - info.header_offset, arcname)
                raise _package_too_large(buf.tell(), [*entry_sizes, partial])
    entry_sizes.append((info.compress_size, arcname))


def _write_dir_entry(zf: zipfile.ZipFile, arcname: str) -> None:
    info = zipfile.ZipInfo(filename=arcname, date_time=(1980, 1, 1, 0, 0, 0))
    info.compress_type = zipfile.ZIP_STORED
    info.create_system = 3
    info.external_attr = (0o40755 << 16) | 0x10
    zf.writestr(info, b"")


def build_payload_archive(payload_bytes: bytes) -> Tuple[bytes, str]:
    """Packages `_ray_client_payload.json` into its own small content-addressed zip."""
    buf = io.BytesIO()
    entry_sizes: List[Tuple[int, str]] = []
    with zipfile.ZipFile(buf, mode="w") as zf:
        _write_deterministic_entry(zf, PAYLOAD_FILENAME, payload_bytes, entry_sizes)
    zip_bytes = buf.getvalue()
    if len(zip_bytes) >= MAX_PACKAGE_BYTES:
        raise _package_too_large(len(zip_bytes), entry_sizes)
    gcs_uri = f"gcs://{RAY_PKG_PREFIX}{hashlib.sha1(zip_bytes).hexdigest()}.zip"
    return zip_bytes, gcs_uri


def _encode_call_payload(
    fn: Callable[..., Any],
    fn_name: str,
    args: Tuple[Any, ...],
    kwargs: Dict[str, Any],
    captured_globals: Dict[str, Any],
) -> Tuple[bytes, Optional[str]]:
    bound = inspect.signature(fn).bind(*args, **kwargs)
    bound.apply_defaults()
    to_json(list(bound.args), "Function positional arguments")
    to_json(dict(bound.kwargs), "Function keyword arguments")
    payload_bytes = to_json(
        {
            "fn_name": fn_name,
            "args": list(bound.args),
            "kwargs": dict(bound.kwargs),
            "captured_globals": captured_globals,
        },
        "Function arguments",
    ).encode("utf-8")
    payload_b64 = base64.b64encode(payload_bytes).decode("ascii")
    if len(payload_b64) <= MAX_ENV_PAYLOAD_BYTES:
        return payload_bytes, payload_b64
    return payload_bytes, None


def _build_call_invariant_driver_zip(
    user_fn_source: str,
    working_dir: Optional[Union[str, Path]] = None,
    excludes: Optional[List[str]] = None,
) -> Tuple[bytes, str]:
    buf = io.BytesIO()
    entry_sizes: List[Tuple[int, str]] = []
    with zipfile.ZipFile(buf, mode="w") as zf:
        if working_dir is not None:
            root = Path(working_dir).resolve()
            if not root.is_dir():
                raise ValueError(f"working_dir '{working_dir}' is not a directory.")
            exclude_rules = _compile_ignore_rules(
                [*_default_excludes(), *(excludes or ())], ""
            )
            include_gitignore = os.environ.get(IGNORE_GITIGNORE_ENV_VAR, "0") != "1"
            for arcname, path in _iter_working_dir(
                root, exclude_rules, include_gitignore
            ):
                if path is None:
                    _write_dir_entry(zf, arcname)
                else:
                    _write_file_entry(zf, buf, arcname, path, entry_sizes)
        _write_deterministic_entry(
            zf, USER_FN_FILENAME, user_fn_source.encode("utf-8"), entry_sizes
        )
        _write_deterministic_entry(
            zf, RUNNER_FILENAME, RUNNER_SCRIPT_CONTENT.encode("utf-8"), entry_sizes
        )

    zip_bytes = buf.getvalue()
    if len(zip_bytes) >= MAX_PACKAGE_BYTES:
        raise _package_too_large(len(zip_bytes), entry_sizes)
    gcs_uri = f"gcs://{RAY_PKG_PREFIX}{hashlib.sha1(zip_bytes).hexdigest()}.zip"
    return zip_bytes, gcs_uri


def build_driver_and_payload_packages(
    fn: Callable[..., Any],
    args: Tuple[Any, ...],
    kwargs: Dict[str, Any],
    working_dir: Optional[Union[str, Path]] = None,
    excludes: Optional[List[str]] = None,
) -> Tuple[bytes, str, Optional[str], Optional[Tuple[bytes, str]]]:
    """Builds the call-invariant driver/`working_dir` package plus either an inline
    `env_payload_b64` (`<= 32 KiB`) or a separate `(payload_zip_bytes, payload_uri)`
    package (`> 32 KiB`) for `runtime_env["py_modules"]`."""
    inspect.signature(fn).bind(*args, **kwargs)
    fn_name, user_fn_source, captured_globals = extract_function_source_and_globals(fn)
    payload_bytes, payload_b64 = _encode_call_payload(
        fn, fn_name, args, kwargs, captured_globals
    )
    zip_bytes, gcs_uri = _build_call_invariant_driver_zip(
        user_fn_source, working_dir, excludes
    )
    payload_pkg = (
        None if payload_b64 is not None else build_payload_archive(payload_bytes)
    )
    return zip_bytes, gcs_uri, payload_b64, payload_pkg


def build_driver_package(
    fn: Callable[..., Any],
    args: Tuple[Any, ...],
    kwargs: Dict[str, Any],
    working_dir: Optional[Union[str, Path]] = None,
    excludes: Optional[List[str]] = None,
) -> Tuple[bytes, str, Optional[str]]:
    """Builds the call-invariant job package and returns `(zip_bytes, gcs_uri, env_payload_b64)`.

    The zip holds only call-invariant content (user function source, the driver
    runner, and `working_dir` files) with fixed timestamps and permissions, so
    repeated calls with different arguments always hash to the same `gcs_uri` and
    hit Ray's package cache. Per-call arguments up to MAX_ENV_PAYLOAD_BYTES travel
    in `env_payload_b64`; larger payloads return `env_payload_b64=None` and are
    shipped by `RayClient` as a separate `py_modules` package via
    `build_driver_and_payload_packages`.
    """
    zip_bytes, gcs_uri, payload_b64, _ = build_driver_and_payload_packages(
        fn, args, kwargs, working_dir, excludes
    )
    return zip_bytes, gcs_uri, payload_b64
