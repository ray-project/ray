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
import builtins
import dis
import hashlib
import inspect
import io
import json
import math
import os
import sys
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

# The Ray dashboard's HTTP server (aiohttp `client_max_size`, see
# ray_constants.DASHBOARD_CLIENT_MAX_SIZE) caps request bodies at 100 MiB, so a
# larger package can never be uploaded through the Dashboard API.
MAX_PACKAGE_BYTES = 100 * 1024 * 1024
RAY_PKG_PREFIX = "_ray_pkg_"
USER_FN_FILENAME = "_ray_client_user_fn.py"
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


def normalize_json_refs(value: Any) -> Any:
    """Converts `RemoteRef` instances inside `value` into `{"$ray_ref": "<ref_id>"}` dicts."""
    if getattr(value, "__ray_client_ref__", False) is True:
        return {"$ray_ref": value.ref_id}
    val_type = type(value)
    if val_type is list:
        return [normalize_json_refs(item) for item in value]
    if val_type is dict:
        return {k: normalize_json_refs(v) for k, v in value.items()}
    return value


def _validate_strict_json(value: Any, path: str) -> None:
    """Validates that `value` round-trips losslessly through JSON with exact types."""
    if getattr(value, "__ray_client_ref__", False) is True:
        return
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
    normalized = normalize_json_refs(value)
    try:
        return json.dumps(normalized, allow_nan=False, sort_keys=sort_keys)
    except (TypeError, OverflowError, ValueError) as exc:
        raise RayClientSerializationError(
            f"{context_name} must be JSON-serializable (no NaN/Infinity): {exc}"
        ) from exc


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


def _import_statement_for(
    name: str, val: Any, owner_module: Optional[str]
) -> Optional[str]:
    """Returns a module header import statement if `val` is an imported module or symbol."""
    if isinstance(val, types.ModuleType):
        mod_name = getattr(val, "__name__", None)
        if not isinstance(mod_name, str) or not mod_name or mod_name == "__main__":
            return None
        return (
            f"import {mod_name}" if mod_name == name else f"import {mod_name} as {name}"
        )

    mod_name = getattr(val, "__module__", None)
    obj_name = getattr(val, "__name__", None)
    qual_name = getattr(val, "__qualname__", obj_name)
    if (
        isinstance(mod_name, str)
        and mod_name
        and mod_name not in ("__main__", owner_module)
        and isinstance(obj_name, str)
        and obj_name
        and qual_name == obj_name
    ):
        candidates = [mod_name]
        if "._" in mod_name:
            public_mod = mod_name.split("._", 1)[0]
            if public_mod and public_mod not in ("__main__", owner_module):
                candidates = [public_mod, mod_name]
        for candidate_mod in candidates:
            mod_obj = sys.modules.get(candidate_mod)
            if mod_obj is not None and getattr(mod_obj, obj_name, None) is val:
                return (
                    f"from {candidate_mod} import {obj_name}"
                    if obj_name == name
                    else f"from {candidate_mod} import {obj_name} as {name}"
                )
    return None


def _validate_captured(name: str, val: Any, fn_name: str, kind: str) -> None:
    if isinstance(val, types.ModuleType):
        raise RayClientSerializationError(
            f"{kind} '{name}' referenced by '{fn_name}' is a module that cannot "
            f"be imported by name on the cluster; import it inside '{fn_name}'."
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
            f"'{fn.__name__}' is decorated; client.run requires a plain "
            "`def` so the exact source can be executed on the cluster."
        )
    return func_node, body, indented


def extract_function_source_and_globals(
    fn: Callable[..., Any],
    *,
    allow_generator: bool = False,
) -> Tuple[str, str, Dict[str, Any]]:
    """Returns `(fn_name, module_source, captured_globals)` for executing `fn` on the cluster.

    `module_source` is a standalone module containing `fn` (with line numbers aligned
    to the source file); `captured_globals` are the strict-JSON constants it
    references from the client's closure/module scope.
    """
    if not inspect.isfunction(fn) or fn.__name__ == "<lambda>":
        raise RayClientSerializationError(
            "RayClient requires a named Python `def` function."
        )
    if not allow_generator and (
        inspect.isgeneratorfunction(fn) or inspect.isasyncgenfunction(fn)
    ):
        raise RayClientSerializationError(
            f"'{fn.__name__}' is a generator function; use `client.stream(...)` "
            "or provide a regular/async function returning strict JSON."
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

    cells = dict(zip(fn.__code__.co_freevars, fn.__closure__ or ()))
    _check_shadowed_defaults(func_node, fn, cells)
    definition_time = _definition_time_names(deferred_code, fn.__name__)

    captured: Dict[str, Any] = {}
    import_stmts: List[str] = []
    for name in sorted(runtime_names):
        if name in cells:
            try:
                value = cells[name].cell_contents
            except ValueError:
                raise RayClientSerializationError(
                    f"Closure variable '{name}' referenced by '{fn.__name__}' is "
                    "not assigned yet; assign it before calling client.run."
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
        imp_stmt = _import_statement_for(name, value, getattr(fn, "__module__", None))
        if imp_stmt is not None:
            import_stmts.append(imp_stmt)
            continue
        _validate_captured(name, value, fn.__name__, kind)
        captured[name] = value

    header_lines = ["import ray  # noqa: F401"]
    if import_stmts:
        header_lines.append("; ".join(import_stmts))
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
    return fn.__name__, module_source, captured


def unwrap_actor_class(cls: Any) -> type:
    """Unwraps `@ray.remote` `ActorClass` if needed and returns the underlying Python class."""
    meta = getattr(cls, "__ray_metadata__", None)
    if meta is not None:
        mod_cls = getattr(meta, "modified_class", None)
        raw = getattr(mod_cls, "__ray_actor_class__", None)
        if inspect.isclass(raw):
            return raw
    if inspect.isclass(cls):
        return cls
    raise RayClientSerializationError(
        f"client.actor() requires a Python class or @ray.remote class, got {cls!r}."
    )


def _is_ray_remote_decorator(dec: ast.expr) -> bool:
    target = dec.func if isinstance(dec, ast.Call) else dec
    if isinstance(target, ast.Attribute) and target.attr == "remote":
        if isinstance(target.value, ast.Name) and target.value.id == "ray":
            return True
    return False


def extract_class_source_and_globals(
    cls: Any,
) -> Tuple[str, str, Dict[str, Any]]:
    """Returns `(class_name, module_source, captured_globals)` for a live actor class."""
    raw_cls = unwrap_actor_class(cls)
    try:
        raw_source = inspect.getsource(raw_cls)
    except (OSError, TypeError) as exc:
        raise RayClientSerializationError(
            f"Could not extract source code for class '{raw_cls.__name__}': {exc}. "
            "Define it in a .py file or a notebook cell."
        ) from exc

    indented = raw_source[0].isspace()
    body = f"if True:\n{raw_source}" if indented else raw_source
    if not body.endswith("\n"):
        body += "\n"
    try:
        tree = ast.parse(body)
    except SyntaxError as exc:
        raise RayClientSerializationError(
            f"Could not parse the source of '{raw_cls.__name__}': {exc}"
        ) from exc

    cls_node = tree.body[0]
    if isinstance(cls_node, ast.If):
        cls_node = cls_node.body[0]
    if not isinstance(cls_node, ast.ClassDef) or cls_node.name != raw_cls.__name__:
        raise RayClientSerializationError(
            f"Expected the source of '{raw_cls.__name__}' to start with "
            f"`class {raw_cls.__name__}`."
        )

    non_ray_decos = [
        d for d in cls_node.decorator_list if not _is_ray_remote_decorator(d)
    ]
    if non_ray_decos:
        raise RayClientSerializationError(
            f"Class '{raw_cls.__name__}' has unsupported decorator(s); only "
            "`@ray.remote` or undecorated classes are supported by client.actor()."
        )
    if cls_node.decorator_list:
        # Strip `@ray.remote` decorator lines from the source while preserving the class body.
        lines = raw_source.splitlines(keepends=True)
        class_line_idx = next(
            (
                idx
                for idx, ln in enumerate(lines)
                if ln.lstrip().startswith(f"class {raw_cls.__name__}")
            ),
            0,
        )
        raw_source = "".join(lines[class_line_idx:])
        indented = raw_source[0].isspace()
        body = f"if True:\n{raw_source}" if indented else raw_source
        if not body.endswith("\n"):
            body += "\n"

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
            f"'{raw_cls.__name__}' cannot run as a standalone module: {exc}"
        ) from exc

    runtime_names = _referenced_global_names(deferred_code) - {"ray"}
    all_names = _referenced_global_names(eager_code) - {"ray"}
    hint_names = sorted((all_names - runtime_names) - set(dir(builtins)))

    cls_globals: Dict[str, Any] = {}
    cls_cells: Dict[str, Any] = {}
    sys_mod = sys.modules.get(raw_cls.__module__)
    if sys_mod is not None:
        cls_globals.update(sys_mod.__dict__)
    for _, member in inspect.getmembers(raw_cls, inspect.isfunction):
        unwrapped = inspect.unwrap(member)
        if not cls_globals:
            cls_globals.update(unwrapped.__globals__)
        for k, cell in zip(unwrapped.__code__.co_freevars, unwrapped.__closure__ or ()):
            cls_cells[k] = cell

    captured: Dict[str, Any] = {}
    import_stmts: List[str] = []
    for name in sorted(runtime_names):
        if name in cls_cells:
            try:
                value = cls_cells[name].cell_contents
            except ValueError:
                raise RayClientSerializationError(
                    f"Closure variable '{name}' referenced by '{raw_cls.__name__}' is "
                    "not assigned yet."
                ) from None
            kind = "Closure variable"
        elif name in cls_globals:
            value, kind = cls_globals[name], "Global"
        else:
            continue
        imp_stmt = _import_statement_for(name, value, raw_cls.__module__)
        if imp_stmt is not None:
            import_stmts.append(imp_stmt)
            continue
        _validate_captured(name, value, raw_cls.__name__, kind)
        captured[name] = value

    header_lines = ["import ray  # noqa: F401"]
    if import_stmts:
        header_lines.append("; ".join(import_stmts))
    if hint_names:
        header_lines.append(f"{_HINT_HELPER_LINE}; {' = '.join(hint_names)} = _RayHint")
    module_source = "\n".join(header_lines) + "\n\n" + body
    return raw_cls.__name__, module_source, captured


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
            if _is_ignored(arcname, is_dir, exclude_rules) or _is_ignored(
                arcname, is_dir, rules
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


def build_working_dir_package(
    working_dir: Union[str, Path],
    excludes: Optional[List[str]] = None,
) -> Tuple[bytes, str]:
    """Packages a local `working_dir` into a deterministic zip and returns `(zip_bytes, gcs_uri)`."""
    root = Path(working_dir).resolve()
    if not root.is_dir():
        raise ValueError(f"working_dir '{working_dir}' is not a directory.")
    buf = io.BytesIO()
    entry_sizes: List[Tuple[int, str]] = []
    exclude_rules = _compile_ignore_rules([*_default_excludes(), *(excludes or ())], "")
    include_gitignore = os.environ.get(IGNORE_GITIGNORE_ENV_VAR, "0") != "1"
    with zipfile.ZipFile(buf, mode="w") as zf:
        for arcname, path in _iter_working_dir(root, exclude_rules, include_gitignore):
            if path is None:
                _write_dir_entry(zf, arcname)
            else:
                _write_file_entry(zf, buf, arcname, path, entry_sizes)
    zip_bytes = buf.getvalue()
    if len(zip_bytes) >= MAX_PACKAGE_BYTES:
        raise _package_too_large(len(zip_bytes), entry_sizes)
    gcs_uri = f"gcs://{RAY_PKG_PREFIX}{hashlib.sha1(zip_bytes).hexdigest()}.zip"
    return zip_bytes, gcs_uri
