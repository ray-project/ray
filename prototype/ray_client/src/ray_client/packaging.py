"""Function and class source extraction, JSON validation, and deterministic zip packaging.

Design constraints:
  * Zero `ray` or `cloudpickle` dependency on the client.
  * Only two things cross the client-cluster boundary: files (via `runtime_env`
    `working_dir` / `py_modules`) and JSON data.

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
import linecache
import math
import os
import sys
import threading
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
    "'__getitem__': lambda c, *_: c, "
    "'__getattr__': lambda c, _: c, "
    "'__call__': lambda c, *_, **__: c, "
    "'__or__': lambda c, _: c, "
    "'__ror__': lambda c, _: c"
    "})('_RayHint', (), {})"
)
_STDLIB_MODULES: FrozenSet[str] = getattr(sys, "stdlib_module_names", frozenset())
_WORKING_DIR_CACHE_LOCK = threading.Lock()
_WORKING_DIR_CACHE: Dict[Any, Tuple[bytes, str]] = {}
_WORKING_DIR_CACHE_MAX = 16
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


def _validate_strict_json(
    value: Any, path: str, _seen: Optional[Set[int]] = None
) -> None:
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
        if _seen is None:
            _seen = set()
        obj_id = id(value)
        if obj_id in _seen:
            raise RayClientSerializationError(
                f"{path} contains a circular reference and cannot be JSON-serialized."
            )
        _seen.add(obj_id)
        try:
            for idx, item in enumerate(value):
                _validate_strict_json(item, f"{path}[{idx}]", _seen)
        finally:
            _seen.discard(obj_id)
        return
    if val_type is dict:
        if _seen is None:
            _seen = set()
        obj_id = id(value)
        if obj_id in _seen:
            raise RayClientSerializationError(
                f"{path} contains a circular reference and cannot be JSON-serialized."
            )
        _seen.add(obj_id)
        try:
            for k, v in value.items():
                if type(k) is not str:
                    raise RayClientSerializationError(
                        f"{path} has non-string dict key {k!r} of type "
                        f"'{type(k).__name__}'; JSON object keys must be str."
                    )
                _validate_strict_json(v, f"{path}[{k!r}]", _seen)
        finally:
            _seen.discard(obj_id)
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


_FAST_OR_DEREF_LOADS = frozenset(
    {
        "LOAD_FAST",
        "LOAD_FAST_CHECK",
        "LOAD_FAST_BORROW",
        "LOAD_DEREF",
        "LOAD_CLASSDEREF",
        "LOAD_CLOSURE",
    }
)


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
            elif instr.opname == "DELETE_NAME":
                stored.discard(instr.argval)
            elif instr.opname in _LOAD_OPS and (
                (current is not module_code and instr.opname == "LOAD_GLOBAL")
                or instr.argval not in stored
            ):
                names.add(instr.argval)
        stack.extend(c for c in current.co_consts if isinstance(c, types.CodeType))
    defined_at_module_level = {
        i.argval for i in dis.get_instructions(module_code) if i.opname == "STORE_NAME"
    }
    return names - defined_at_module_level - _MODULE_ATTRS


def _definition_time_names(module_code: types.CodeType) -> Tuple[Set[str], Set[str]]:
    """Returns `(header_names, body_names)` loaded externally when the module body,
    class bodies, and `def` default-argument expressions execute."""
    header_names: Set[str] = set()
    body_names: Set[str] = set()
    stack: List[Tuple[types.CodeType, bool]] = [(module_code, True)]
    while stack:
        current, is_header = stack.pop()
        stored: Set[str] = set()
        target_set = header_names if is_header else body_names
        for instr in dis.get_instructions(current):
            if instr.opname in _STORE_OPS:
                stored.add(instr.argval)
            elif instr.opname == "DELETE_NAME":
                stored.discard(instr.argval)
            elif instr.opname in _LOAD_OPS and (
                (current is not module_code and instr.opname == "LOAD_GLOBAL")
                or instr.argval not in stored
            ):
                target_set.add(instr.argval)
        for c in current.co_consts:
            if not isinstance(c, types.CodeType):
                continue
            if not (c.co_flags & inspect.CO_OPTIMIZED):
                stack.append((c, False))
            elif c.co_name.startswith("<"):
                stack.append((c, is_header))
    return header_names - _MODULE_ATTRS, body_names - _MODULE_ATTRS


def _find_enclosing_code_path(
    root: types.CodeType,
    target_name: str,
    target_linenos: Set[int],
    qualname: str,
) -> Tuple[Optional[List[types.CodeType]], Optional[types.CodeType]]:
    stack: List[Tuple[types.CodeType, List[types.CodeType]]] = [(root, [])]
    best_path: Optional[List[types.CodeType]] = None
    best_target: Optional[types.CodeType] = None
    while stack:
        curr, path = stack.pop()
        for c in curr.co_consts:
            if not isinstance(c, types.CodeType):
                continue
            next_path = path + [curr]
            if c.co_name == target_name and c.co_firstlineno in target_linenos:
                if getattr(c, "co_qualname", qualname) == qualname:
                    return next_path, c
                best_path, best_target = next_path, c
            stack.append((c, next_path))
    return best_path, best_target


def _unclosed_enclosing_names(
    filename: Optional[str],
    target_name: str,
    target_linenos: Set[int],
    end_lineno: int,
    qualname: str,
    scope_globals: Dict[str, Any],
    cells: Dict[str, Any],
) -> Tuple[Set[str], Set[str]]:
    """Returns `(header_unclosed, body_unclosed)` for a nested function or class
    using Python's compiled `CodeType` scope tree."""
    if "." not in qualname or not filename:
        return set(), set()
    all_lines = linecache.getlines(filename, scope_globals)
    if not all_lines:
        return set(), set()
    try:
        file_code = compile("".join(all_lines), filename, "exec", dont_inherit=True)
    except SyntaxError:
        return set(), set()
    ancestors, target_code = _find_enclosing_code_path(
        file_code, target_name, target_linenos, qualname
    )
    if not ancestors or target_code is None:
        return set(), set()

    min_lineno = min(target_linenos)
    body_unclosed = set(target_code.co_freevars) - set(cells)
    parent_code = ancestors[-1]
    header_unclosed: Set[str] = set()

    if not (parent_code.co_flags & inspect.CO_OPTIMIZED):
        parent_cls_vars: Set[str] = set()
        for instr in dis.get_instructions(parent_code):
            lineno = (
                instr.positions.lineno
                if hasattr(instr, "positions") and instr.positions is not None
                else instr.starts_line
            )
            if lineno is not None and lineno >= min_lineno:
                break
            if instr.opname in _STORE_OPS:
                parent_cls_vars.add(instr.argval)
            elif instr.opname == "DELETE_NAME":
                parent_cls_vars.discard(instr.argval)
        header_unclosed = parent_cls_vars | (set(parent_code.co_freevars) - set(cells))
    else:
        comp_stack: List[Dict[str, Any]] = []
        for instr in dis.get_instructions(parent_code):
            lineno = (
                instr.positions.lineno
                if hasattr(instr, "positions") and instr.positions is not None
                else instr.starts_line
            )
            if lineno is None or not (min_lineno <= lineno <= end_lineno):
                continue
            if instr.opname == "LOAD_FAST_AND_CLEAR":
                if not comp_stack or comp_stack[-1]["for_depth"] > 0:
                    comp_stack.append({"vars": set(), "for_depth": 0})
                comp_stack[-1]["vars"].add(instr.argval)
            elif instr.opname == "FOR_ITER" and comp_stack:
                comp_stack[-1]["for_depth"] += 1
            elif instr.opname in ("END_FOR", "CLEANUP_THROW") and comp_stack:
                comp_stack[-1]["for_depth"] -= 1
                if comp_stack[-1]["for_depth"] <= 0:
                    comp_stack.pop()
            else:
                active_comp = (
                    set().union(*(f["vars"] for f in comp_stack))
                    if comp_stack
                    else set()
                )
                if instr.opname in _FAST_OR_DEREF_LOADS:
                    if instr.argval not in cells and instr.argval not in active_comp:
                        header_unclosed.add(instr.argval)
                elif (
                    instr.opname == "STORE_FAST_LOAD_FAST"
                    and isinstance(instr.argval, tuple)
                    and len(instr.argval) == 2
                ):
                    loaded = instr.argval[1]
                    if loaded not in cells and loaded not in active_comp:
                        header_unclosed.add(loaded)

    return header_unclosed, body_unclosed


def _import_statement_for(
    name: str, val: Any, owner_module: Optional[str]
) -> Optional[str]:
    """Returns a module header import statement if `val` is an imported module or symbol."""
    if val is os.path:
        if name == "os":
            return "import os"
        return (
            "from os import path"
            if name == "path"
            else f"from os import path as {name}"
        )

    if isinstance(val, types.ModuleType):
        mod_name = getattr(val, "__name__", None)
        if not isinstance(mod_name, str) or not mod_name or mod_name == "__main__":
            return None
        return (
            f"import {mod_name}" if mod_name == name else f"import {mod_name} as {name}"
        )

    mod_name = getattr(val, "__module__", None)
    obj_name = getattr(val, "__name__", None) or getattr(val, "_name", None)
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


def _is_stdlib_import(imp_stmt: str) -> bool:
    if imp_stmt.startswith("import "):
        mod_part = imp_stmt[len("import ") :].split(" as ", 1)[0].strip()
    elif imp_stmt.startswith("from "):
        mod_part = imp_stmt[len("from ") :].split(" import ", 1)[0].strip()
    else:
        return False
    root_mod = mod_part.split(".", 1)[0]
    return bool(_STDLIB_MODULES and root_mod in _STDLIB_MODULES)


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
            f"'{fn.__name__}' is decorated; cluster.run requires a plain "
            "`def` so the exact source can be executed on the cluster."
        )
    return func_node, body, indented


def _compile_and_assemble_module(
    target_name: str,
    body: str,
    indented: bool,
    target_lineno: int,
    owner_module: Optional[str],
    scope_globals: Dict[str, Any],
    cells: Dict[str, Any],
    *,
    is_class: bool,
    client_future_annotations: bool,
    filename: Optional[str] = None,
    qualname: str = "",
    target_linenos: Optional[Set[int]] = None,
) -> Tuple[str, str, Dict[str, Any]]:
    compile_filename = filename or f"<{target_name}>"
    try:
        deferred_code = compile(
            body,
            compile_filename,
            "exec",
            flags=__future__.CO_FUTURE_ANNOTATIONS,
            dont_inherit=True,
        )
        eager_code = compile(body, compile_filename, "exec", dont_inherit=True)
    except SyntaxError as exc:
        raise RayClientSerializationError(
            f"'{target_name}' cannot run as a standalone module: {exc}"
        ) from exc

    header_def_names, body_def_names = _definition_time_names(deferred_code)
    definition_time = header_def_names | body_def_names
    eager_header_def, eager_body_def = _definition_time_names(eager_code)
    eager_def_time = eager_header_def | eager_body_def
    runtime_names = (_referenced_global_names(deferred_code) | definition_time) - {
        "ray"
    }
    all_names = (_referenced_global_names(eager_code) | eager_def_time) - {"ray"}
    raw_hint_names = sorted((all_names - runtime_names) - set(dir(builtins)))
    end_lineno = target_lineno + len(body.splitlines()) - (2 if indented else 1)
    header_unclosed, body_unclosed = _unclosed_enclosing_names(
        filename,
        target_name,
        target_linenos or {target_lineno},
        end_lineno,
        qualname or target_name,
        scope_globals,
        cells,
    )

    captured: Dict[str, Any] = {}
    import_stmts: List[str] = []
    for name in sorted(runtime_names):
        if (name in header_def_names and name in header_unclosed) or (
            name in body_def_names and name in body_unclosed
        ):
            if is_class:
                raise RayClientSerializationError(
                    f"Class '{target_name}' references '{name}' from an enclosing "
                    "function or class scope; pass it to __init__ or define it at module level."
                )
            raise RayClientSerializationError(
                f"Default argument of '{target_name}' references '{name}' from an "
                "enclosing function or class scope; pass it as an argument or define "
                "it at module level."
            )
        if name in cells:
            try:
                value = cells[name].cell_contents
            except ValueError:
                raise RayClientSerializationError(
                    f"Closure variable '{name}' referenced by '{target_name}' is "
                    "not assigned yet; assign it before calling cluster.run."
                ) from None
            kind = "Closure variable"
        elif name in scope_globals:
            value, kind = scope_globals[name], "Global"
        elif name in definition_time and not hasattr(builtins, name):
            if is_class:
                raise RayClientSerializationError(
                    f"Class '{target_name}' references '{name}' from an enclosing "
                    "function or class scope; pass it to __init__ or define it at module level."
                )
            raise RayClientSerializationError(
                f"Default argument of '{target_name}' references '{name}' from an "
                "enclosing function or class scope; pass it as an argument or define "
                "it at module level."
            )
        else:
            continue
        imp_stmt = _import_statement_for(name, value, owner_module)
        if imp_stmt is not None:
            import_stmts.append(imp_stmt)
            continue
        _validate_captured(name, value, target_name, kind)
        captured[name] = value

    hint_names: List[str] = []
    for name in raw_hint_names:
        hint_val: Any = None
        if name in cells:
            try:
                hint_val = cells[name].cell_contents
            except ValueError:
                hint_val = None
        elif name in scope_globals:
            hint_val = scope_globals[name]
        imp_stmt = (
            _import_statement_for(name, hint_val, owner_module)
            if hint_val is not None
            else None
        )
        if imp_stmt is not None and _is_stdlib_import(imp_stmt):
            import_stmts.append(imp_stmt)
        else:
            hint_names.append(name)

    header_lines = ["import ray  # noqa: F401"]
    if import_stmts:
        header_lines.append("; ".join(import_stmts))
    if hint_names:
        header_lines.append(f"{_HINT_HELPER_LINE}; {' = '.join(hint_names)} = _RayHint")
    if client_future_annotations:
        header_lines.insert(0, "from __future__ import annotations")

    header = "\n".join(header_lines) + "\n"
    wrapper_lines = 1 if indented else 0
    pad_lines = max(1, target_lineno - len(header_lines) - wrapper_lines)
    module_source = header + ("\n" * (pad_lines - 1)) + body
    return target_name, module_source, captured


def extract_function_source_and_globals(
    fn: Callable[..., Any],
    *,
    allow_generator: bool = True,
) -> Tuple[str, str, Dict[str, Any]]:
    """Returns `(fn_name, module_source, captured_globals)` for executing `fn` on the cluster.

    `module_source` is a standalone module containing `fn` (with line numbers aligned
    to the source file); `captured_globals` are the strict-JSON constants it
    references from the client's closure/module scope.
    """
    if not inspect.isfunction(fn) or fn.__name__ == "<lambda>":
        raise RayClientSerializationError(
            "RayCluster requires a named Python `def` function."
        )
    if not allow_generator and (
        inspect.isgeneratorfunction(fn) or inspect.isasyncgenfunction(fn)
    ):
        raise RayClientSerializationError(
            f"'{fn.__name__}' is a generator function; provide a regular/async "
            "function returning strict JSON."
        )

    _, body, indented = _extract_func_node_and_body(fn)
    cells = dict(zip(fn.__code__.co_freevars, fn.__closure__ or ()))
    fn_lineno = getattr(fn.__code__, "co_firstlineno", 1)
    return _compile_and_assemble_module(
        fn.__name__,
        body,
        indented,
        fn_lineno,
        getattr(fn, "__module__", None),
        fn.__globals__,
        cells,
        is_class=False,
        client_future_annotations=bool(
            fn.__code__.co_flags & __future__.CO_FUTURE_ANNOTATIONS
        ),
        filename=getattr(fn.__code__, "co_filename", None),
        qualname=getattr(fn, "__qualname__", "") or fn.__name__,
        target_linenos={fn_lineno},
    )


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
        f"cluster.actor() requires a Python class or @ray.remote class, got {cls!r}."
    )


def _is_ray_remote_decorator(dec: ast.expr) -> bool:
    target = dec.func if isinstance(dec, ast.Call) else dec
    if isinstance(target, ast.Attribute) and target.attr == "remote":
        if isinstance(target.value, ast.Name) and target.value.id == "ray":
            return True
    return False


def _iter_class_functions(raw_cls: type) -> Iterator[types.FunctionType]:
    """Yields all plain functions defined on `raw_cls` (unwrapping descriptors)."""
    seen: Set[int] = set()
    for val in raw_cls.__dict__.values():
        candidates: List[Any] = []
        if isinstance(val, (staticmethod, classmethod)):
            candidates.append(val.__func__)
        elif isinstance(val, property):
            candidates.extend([val.fget, val.fset, val.fdel])
        else:
            candidates.append(val)
        for cand in candidates:
            if cand is None:
                continue
            unwrapped = inspect.unwrap(cand)
            if inspect.isfunction(unwrapped) and id(unwrapped) not in seen:
                seen.add(id(unwrapped))
                yield unwrapped


def _get_class_source_and_lineno(raw_cls: type) -> Tuple[str, int, Optional[str]]:
    """Extracts `(raw_source, start_lineno, filename)` for `raw_cls`, falling back to
    method code objects + `linecache` for classes defined in interactive notebook cells."""
    first_exc: Optional[BaseException] = None
    try:
        src_lines, start_lineno = inspect.getsourcelines(raw_cls)
        try:
            filename: Optional[str] = inspect.getsourcefile(raw_cls) or inspect.getfile(
                raw_cls
            )
        except TypeError:
            filename = None
        end_lineno = start_lineno + len(src_lines) - 1
        methods = list(_iter_class_functions(raw_cls))
        if not methods or all(
            start_lineno
            <= getattr(m.__code__, "co_firstlineno", start_lineno)
            <= end_lineno
            for m in methods
        ):
            return "".join(src_lines), max(1, int(start_lineno)), filename
    except (OSError, TypeError) as exc:
        first_exc = exc

    for fn_member in _iter_class_functions(raw_cls):
        code = getattr(fn_member, "__code__", None)
        if code is None:
            continue
        filename = getattr(code, "co_filename", None)
        method_lineno = getattr(code, "co_firstlineno", 0)
        if not filename or method_lineno <= 0:
            continue
        mod_globals = getattr(fn_member, "__globals__", None)
        all_lines = linecache.getlines(filename, mod_globals)
        if not all_lines:
            continue
        try:
            tree = ast.parse("".join(all_lines))
        except SyntaxError:
            continue
        candidates: List[ast.ClassDef] = []
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef) and node.name == raw_cls.__name__:
                end_lineno = getattr(node, "end_lineno", len(all_lines))
                if node.lineno <= method_lineno <= end_lineno:
                    candidates.append(node)
        if candidates:
            best = max(candidates, key=lambda n: n.lineno)
            start = (
                best.decorator_list[0].lineno if best.decorator_list else best.lineno
            )
            end = getattr(best, "end_lineno", len(all_lines))
            return "".join(all_lines[start - 1 : end]), max(1, int(start)), filename
    raise RayClientSerializationError(
        f"Could not extract source code for class '{raw_cls.__name__}': {first_exc}. "
        "Define it in a .py file or a notebook cell."
    ) from first_exc


def extract_class_source_and_globals(
    cls: Any,
) -> Tuple[str, str, Dict[str, Any]]:
    """Returns `(class_name, module_source, captured_globals)` for an actor class."""
    raw_cls = unwrap_actor_class(cls)
    raw_source, orig_lineno, filename = _get_class_source_and_lineno(raw_cls)
    cls_lineno = orig_lineno

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
            "`@ray.remote` or undecorated classes are supported by cluster.actor()."
        )
    if cls_node.decorator_list:
        lines = raw_source.splitlines(keepends=True)
        class_line_idx = max(0, cls_node.lineno - (2 if indented else 1))
        cls_lineno += class_line_idx
        raw_source = "".join(lines[class_line_idx:])
        indented = raw_source[0].isspace()
        body = f"if True:\n{raw_source}" if indented else raw_source
        if not body.endswith("\n"):
            body += "\n"

    cls_globals: Dict[str, Any] = {}
    cls_cells: Dict[str, Any] = {}
    sys_mod = sys.modules.get(raw_cls.__module__)
    if sys_mod is not None:
        cls_globals.update(sys_mod.__dict__)
    client_future_annotations = cls_globals.get("annotations") is __future__.annotations
    for fn_member in _iter_class_functions(raw_cls):
        if filename is None:
            filename = getattr(fn_member.__code__, "co_filename", None)
        cls_globals.update(fn_member.__globals__)
        if fn_member.__code__.co_flags & __future__.CO_FUTURE_ANNOTATIONS:
            client_future_annotations = True
        for k, cell in zip(fn_member.__code__.co_freevars, fn_member.__closure__ or ()):
            cls_cells[k] = cell

    return _compile_and_assemble_module(
        raw_cls.__name__,
        body,
        indented,
        cls_lineno,
        raw_cls.__module__,
        cls_globals,
        cls_cells,
        is_class=True,
        client_future_annotations=client_future_annotations,
        filename=filename,
        qualname=getattr(raw_cls, "__qualname__", "") or raw_cls.__name__,
        target_linenos={orig_lineno, cls_lineno},
    )


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
            elif entry.is_file():
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
    info.file_size = st.st_size
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
    exclude_rules = _compile_ignore_rules([*_default_excludes(), *(excludes or ())], "")
    include_gitignore = os.environ.get(IGNORE_GITIGNORE_ENV_VAR, "0") != "1"
    entries = list(_iter_working_dir(root, exclude_rules, include_gitignore))

    cache_key: Optional[Any] = None
    try:
        stat_sig = tuple(
            (
                arcname,
                None
                if path is None
                else (
                    (st := path.stat()).st_mtime_ns,
                    st.st_size,
                    st.st_mode,
                ),
            )
            for arcname, path in entries
        )
        cache_key = (
            str(root),
            tuple(excludes or ()),
            _default_excludes(),
            include_gitignore,
            MAX_PACKAGE_BYTES,
            stat_sig,
        )
        with _WORKING_DIR_CACHE_LOCK:
            cached = _WORKING_DIR_CACHE.get(cache_key)
            if cached is not None:
                return cached
    except OSError:
        cache_key = None

    buf = io.BytesIO()
    entry_sizes: List[Tuple[int, str]] = []
    with zipfile.ZipFile(buf, mode="w") as zf:
        for arcname, path in entries:
            if path is None:
                _write_dir_entry(zf, arcname)
            else:
                _write_file_entry(zf, buf, arcname, path, entry_sizes)
    zip_bytes = buf.getvalue()
    if len(zip_bytes) >= MAX_PACKAGE_BYTES:
        raise _package_too_large(len(zip_bytes), entry_sizes)
    gcs_uri = f"gcs://{RAY_PKG_PREFIX}{hashlib.sha1(zip_bytes).hexdigest()}.zip"
    if cache_key is not None:
        with _WORKING_DIR_CACHE_LOCK:
            if len(_WORKING_DIR_CACHE) >= _WORKING_DIR_CACHE_MAX:
                _WORKING_DIR_CACHE.pop(next(iter(_WORKING_DIR_CACHE)))
            _WORKING_DIR_CACHE[cache_key] = (zip_bytes, gcs_uri)
    return zip_bytes, gcs_uri
