import asyncio
import collections
import concurrent.futures
import contextlib
import hashlib
import importlib
import inspect
import io
import json
import logging
import math
import os
import sys
import threading
import time
import traceback
import types
import uuid
from typing import Any, Deque, Dict, List, Optional, Set, Tuple, Union

import ray
import ray._private.worker
from ray._common.utils import run_background_task
from ray.actor import ActorClass
from ray.dashboard.consts import RAY_JOB_ALLOW_DRIVER_ON_WORKER_NODES_ENV_VAR
from ray.exceptions import RayActorError, RayTaskError
from ray.runtime_context import RuntimeContext

logger = logging.getLogger(__name__)

CLIENT_ACTOR_NAMESPACE = "ray_client"
MAX_LOG_CHARS = 1024 * 1024
_CONTROL_TIMEOUT_S = 5.0


def _validate_strict_json(
    value: Any, path: str = "Value", _seen: Optional[Set[int]] = None
) -> None:
    if value is None or value is True or value is False:
        return
    val_type = type(value)
    if val_type is int or val_type is str:
        return
    if val_type is float:
        if not math.isfinite(value):
            raise TypeError(
                f"{path} contains non-finite float {value!r}; NaN and Infinity "
                "are not valid JSON."
            )
        return
    if val_type is list:
        if _seen is None:
            _seen = set()
        obj_id = id(value)
        if obj_id in _seen:
            raise TypeError(f"{path} contains a circular list reference.")
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
            raise TypeError(f"{path} contains a circular dict reference.")
        _seen.add(obj_id)
        try:
            for k, v in value.items():
                if type(k) is not str:
                    raise TypeError(
                        f"{path} has non-string dict key {k!r} of type "
                        f"{type(k).__name__}; JSON object keys must be exact str."
                    )
                _validate_strict_json(v, f"{path}[{k!r}]", _seen)
        finally:
            _seen.discard(obj_id)
        return
    raise TypeError(
        f"{path} contains value of type '{val_type.__name__}', which is not "
        "round-trip JSON serializable."
    )


def _validate_ttl_seconds(ttl_seconds: Any) -> Optional[float]:
    if ttl_seconds is None:
        return None
    if isinstance(ttl_seconds, bool) or not isinstance(ttl_seconds, (int, float)):
        raise ValueError(f"ttl_seconds must be > 0 or None, got {ttl_seconds!r}.")
    val = float(ttl_seconds)
    if not math.isfinite(val) or val <= 0:
        raise ValueError(f"ttl_seconds must be > 0 or None, got {ttl_seconds!r}.")
    return val


def _validate_get_if_exists(get_if_exists: Any) -> Union[bool, str]:
    if isinstance(get_if_exists, bool) or get_if_exists == "recreate":
        return get_if_exists
    raise ValueError(
        f"get_if_exists must be True, False, or 'recreate', got {get_if_exists!r}."
    )


class MaxNodesExceededError(ValueError):
    """Raised when adding nodes would exceed a node group's max_nodes limit."""


def _validate_timeout(timeout: Any, name: str = "timeout") -> Optional[float]:
    if timeout is None:
        return None
    if (
        isinstance(timeout, bool)
        or not isinstance(timeout, (int, float))
        or not math.isfinite(timeout)
        or timeout <= 0
    ):
        raise ValueError(
            f"{name} must be a positive finite number or None, got {timeout!r}."
        )
    return float(timeout)


def _validate_max_concurrency(max_concurrency: Any) -> int:
    if (
        isinstance(max_concurrency, bool)
        or not isinstance(max_concurrency, int)
        or max_concurrency < 1
    ):
        raise ValueError(
            f"max_concurrency must be a positive integer, got {max_concurrency!r}."
        )
    return max_concurrency


def _validate_label_selector(label_selector: Any) -> Optional[Dict[str, str]]:
    if label_selector is None:
        return None
    if not isinstance(label_selector, dict):
        raise ValueError(
            f"label_selector must be a dict[str, str], got {label_selector!r}."
        )
    out: Dict[str, str] = {}
    for k, v in label_selector.items():
        if not isinstance(k, str) or not k.strip() or not isinstance(v, str):
            raise ValueError(
                f"label_selector keys must be non-empty strings and values must be strings, got {k!r}: {v!r}."
            )
        out[k] = v
    return out


def _normalize_module_source(module_source: Optional[str]) -> Optional[str]:
    if module_source is None:
        return None
    src = module_source.lstrip("\n")
    if src.startswith(
        ("import ray  # noqa: F401\n", "from __future__ import annotations\n")
    ):
        lines = src.splitlines(keepends=True)
        idx = 0
        while idx < len(lines) and lines[idx].startswith(
            (
                "from __future__ import annotations",
                "import ",
                "from ",
                "_RayHint =",
            )
        ):
            idx += 1
        return "".join(lines[:idx]) + "".join(lines[idx:]).lstrip("\n")
    return src


def _validate_resource_num(
    val: Any, name: str, *, integer: bool = False
) -> Union[int, float]:
    if (
        isinstance(val, bool)
        or not isinstance(val, (int, float))
        or not math.isfinite(val)
        or val < 0
    ):
        raise ValueError(
            f"Invalid {name} value {val!r}; must be a non-negative finite number."
        )
    if integer:
        if isinstance(val, float) and not val.is_integer():
            raise ValueError(
                f"Invalid {name} value {val!r}; must be an integer number of bytes."
            )
        return int(val)
    return val


def _normalize_spec_resources(
    spec: Dict[str, Any],
    kind: str,
    default_cpus: Optional[Union[int, float]] = None,
) -> Tuple[
    Union[int, float],
    Optional[Union[int, float]],
    Optional[int],
    Dict[str, float],
]:
    raw_cpus = spec.get("num_cpus")
    raw_gpus = spec.get("num_gpus")
    raw_memory = spec.get("memory")
    num_cpus = (
        _validate_resource_num(raw_cpus, "num_cpus") if raw_cpus is not None else None
    )
    num_gpus = (
        _validate_resource_num(raw_gpus, "num_gpus") if raw_gpus is not None else None
    )
    memory = (
        int(_validate_resource_num(raw_memory, "memory", integer=True))
        if raw_memory is not None
        else None
    )
    raw_resources = spec.get("resources")
    custom_resources: Dict[str, float] = {}
    if raw_resources is not None:
        if not isinstance(raw_resources, dict):
            raise ValueError(
                f"resources must be a dict[str, float], got {raw_resources!r}."
            )
        for k, v in raw_resources.items():
            if not isinstance(k, str):
                raise ValueError(
                    f"Invalid resource key {k!r}; resource keys must be strings."
                )
            v_num = _validate_resource_num(
                v, f"resources[{k!r}]", integer=(k == "memory")
            )
            if k == "CPU":
                if num_cpus is not None and float(num_cpus) != float(v_num):
                    raise ValueError(
                        f"Conflicting CPU requirements: num_cpus={num_cpus!r} vs "
                        f"resources['CPU']={v!r}."
                    )
                num_cpus = v_num
            elif k == "GPU":
                if num_gpus is not None and float(num_gpus) != float(v_num):
                    raise ValueError(
                        f"Conflicting GPU requirements: num_gpus={num_gpus!r} vs "
                        f"resources['GPU']={v!r}."
                    )
                num_gpus = v_num
            elif k == "memory":
                if memory is not None and int(memory) != int(v_num):
                    raise ValueError(
                        f"Conflicting memory requirements: memory={memory!r} vs "
                        f"resources['memory']={v!r}."
                    )
                memory = int(v_num)
            elif v_num > 0:
                custom_resources[k] = float(v_num)
    if num_cpus is None:
        num_cpus = (
            default_cpus
            if default_cpus is not None
            else (0 if kind == "function" else 1)
        )
    return num_cpus, num_gpus, memory, custom_resources


def _compute_spec_hash(
    spec: Dict[str, Any],
    kind: str,
    target_name: str,
    num_cpus: Any,
    max_concurrency: Any,
) -> str:
    norm_cpus, norm_gpus, norm_memory, norm_resources = _normalize_spec_resources(
        spec,
        kind,
        default_cpus=num_cpus if isinstance(num_cpus, (int, float)) else None,
    )
    canonical = {
        "kind": kind,
        "target_name": target_name,
        "module_source": _normalize_module_source(spec.get("module_source")),
        "module": spec.get("module"),
        "captured_globals": spec.get("captured_globals") or {},
        "args": spec.get("args") or [],
        "kwargs": spec.get("kwargs") or {},
        "runtime_env": spec.get("runtime_env") or {},
        "num_cpus": (
            float(norm_cpus) if isinstance(norm_cpus, (int, float)) else norm_cpus
        ),
        "num_gpus": (
            float(norm_gpus) if isinstance(norm_gpus, (int, float)) else norm_gpus
        ),
        "memory": norm_memory,
        "resources": norm_resources,
        "label_selector": spec.get("label_selector") or {},
        "node_id": spec.get("node_id"),
        "max_concurrency": (
            int(max_concurrency)
            if isinstance(max_concurrency, int)
            else max_concurrency
        ),
    }
    payload = json.dumps(
        canonical, sort_keys=True, separators=(",", ":"), default=repr
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _format_exception_dict(exc: BaseException) -> Dict[str, Any]:
    cause = getattr(exc, "cause", None) or exc
    msg = str(cause)
    if isinstance(cause, RuntimeError) and msg.startswith("__RAY_SYSTEM_EXIT__:"):
        code = msg.split(":", 1)[1]
        return {
            "exc_type": "SystemExit",
            "exc_message": code,
            "traceback": f"SystemExit: {code}\n",
        }
    if isinstance(exc, RayTaskError):
        tb_str = str(exc)
        exc_type_name = type(cause).__name__ if cause is not exc else "RayTaskError"
        exc_msg = str(cause) if cause is not exc else str(exc)
        return {
            "exc_type": exc_type_name,
            "exc_message": exc_msg,
            "traceback": tb_str,
        }
    return {
        "exc_type": type(cause).__name__,
        "exc_message": msg,
        "traceback": "".join(
            traceback.format_exception(type(exc), exc, exc.__traceback__)
        ),
    }


class _LogTeeStream(io.TextIOBase):
    """Forwards writes to the underlying stream while appending to an in-memory log buffer.

    Installed as `sys.stdout` and `sys.stderr` of the root actor process, so
    `GET /api/actors/{name}/logs` returns what the root's Python code printed, capped
    at `MAX_LOG_CHARS`. Writes that bypass `sys.stdout` (for example, a subprocess or
    C extension writing to fd 1) and the output of child tasks and actors only reach
    the per-worker log files. The GCS log channel can't attribute child output to a
    root either: it keys log batches by job ID, and the dashboard agent's
    `_ray_internal_dashboard` driver creates every root, so the children of all
    concurrent roots share one job ID.
    """

    def __init__(
        self,
        stream: Any,
        buffer: Deque[str],
        lock: threading.Lock,
    ) -> None:
        self._stream = stream
        self._buffer = buffer
        self._lock = lock
        self._chars = 0

    @property
    def encoding(self) -> str:
        return getattr(self._stream, "encoding", None) or "utf-8"

    def writable(self) -> bool:
        return True

    def readable(self) -> bool:
        return False

    def fileno(self) -> int:
        return self._stream.fileno()

    def isatty(self) -> bool:
        return bool(getattr(self._stream, "isatty", lambda: False)())

    def write(self, s: str) -> int:
        try:
            self._stream.write(s)
        except Exception:
            pass
        if s and not s.startswith((":actor_name:", ":task_name:", ":job_id:")):
            with self._lock:
                self._buffer.append(s)
                self._chars += len(s)
                while self._chars > MAX_LOG_CHARS and len(self._buffer) > 1:
                    removed = self._buffer.popleft()
                    self._chars -= len(removed)
        return len(s)

    def flush(self) -> None:
        try:
            self._stream.flush()
        except Exception:
            pass

    def __getattr__(self, attr: str) -> Any:
        return getattr(self._stream, attr)


def _load_user_target(
    actor_name: str,
    target_name: str,
    module_source: Optional[str],
    module_path: Optional[str],
    captured_globals: Dict[str, Any],
) -> Any:
    cwd = os.getcwd()
    if not sys.path or sys.path[0] != cwd:
        with contextlib.suppress(ValueError):
            sys.path.remove(cwd)
        sys.path.insert(0, cwd)

    if module_source is not None:
        mod_name = (
            f"_ray_client_actor_{actor_name}_{target_name}_{uuid.uuid4().hex[:6]}"
        )
        module = types.ModuleType(mod_name)
        module.__file__ = f"<{mod_name}>"
        sys.modules[mod_name] = module
        module.__dict__.update(captured_globals)
        exec(compile(module_source, module.__file__, "exec"), module.__dict__)
        with contextlib.suppress(Exception):
            ray.cloudpickle.register_pickle_by_value(module)
        return getattr(module, target_name)
    if module_path is not None:
        importlib.invalidate_caches()
        module = importlib.import_module(module_path)
        return getattr(module, target_name)
    raise ValueError(
        f"Spec for '{target_name}' must provide either 'module_source' or 'module'."
    )


async def _await_coro_catching_system_exit(
    coro: Any, task_holder: Optional[List[Any]] = None
) -> Any:
    if task_holder is not None:
        task_holder.append(asyncio.current_task())
    try:
        return False, await coro
    except SystemExit as exc:
        return True, exc.code


async def _anext_catching_system_exit(
    agen: Any, task_holder: Optional[List[Any]] = None
) -> Any:
    if task_holder is not None:
        task_holder.append(asyncio.current_task())
    try:
        return "item", await agen.__anext__()
    except StopAsyncIteration:
        return "stop", None
    except SystemExit as exc:
        return "exit", exc.code


class _ClientRootActor:
    """Root Ray Actor hosting a user class or driver function.

    All user-facing methods (`invoke`, `invoke_stream`) run on the default
    `max_concurrency` thread pool, while control methods (`get_info`, `get_logs`,
    `cancel_stream`, `set_ttl`, `shutdown`) run on a dedicated `_control`
    concurrency group so status checks and stream cancellations never block
    behind saturated user threads.
    """

    def __init__(self, spec: Dict[str, Any]) -> None:
        self._name: str = spec["name"]
        self._namespace: str = spec.get("namespace") or CLIENT_ACTOR_NAMESPACE
        self._kind: str = spec.get("kind") or (
            "function" if "fn_name" in spec else "class"
        )
        self._target_name: str = (
            spec.get("fn_name")
            if self._kind == "function"
            else spec.get("class_name") or self._name
        )
        self._ttl_seconds: Optional[float] = _validate_ttl_seconds(
            spec.get("ttl_seconds", 1800.0)
        )
        self._detached: bool = bool(spec.get("detached", self._kind != "function"))
        self._spec_hash: Optional[str] = spec.get("spec_hash")
        self._created_at: float = time.time()
        self._last_active: float = time.time()
        self._last_active_mono: float = time.monotonic()
        self._active_calls: int = 0
        self._state_lock = threading.Lock()
        self._cancelled_streams: "collections.OrderedDict[str, None]" = (
            collections.OrderedDict()
        )
        self._placement_groups: List[Any] = []
        self._shutting_down = False
        self._shutdown_event = threading.Event()
        self._cleaned_up = False
        self._cleanup_lock = threading.RLock()

        with contextlib.suppress(Exception):
            sys.__stdout__.write(f":actor_name:{self._target_name}\n")
            sys.__stdout__.flush()
            sys.__stderr__.write(f":actor_name:{self._target_name}\n")
            sys.__stderr__.flush()
        self._log_buffer: Deque[str] = collections.deque()
        self._log_lock = threading.Lock()
        sys.stdout = _LogTeeStream(sys.__stdout__, self._log_buffer, self._log_lock)
        sys.stderr = _LogTeeStream(sys.__stderr__, self._log_buffer, self._log_lock)

        self._async_loop: Optional[asyncio.AbstractEventLoop] = None
        self._async_thread: Optional[threading.Thread] = None
        self._async_lock = threading.Lock()

        self._install_ray_hooks()

        self._fn = None
        self._instance = None
        self._generator_methods: Set[str] = set()
        self._init_error: Optional[Dict[str, Any]] = None
        try:
            target = _load_user_target(
                self._name,
                self._target_name,
                spec.get("module_source"),
                spec.get("module"),
                spec.get("captured_globals") or {},
            )
            if self._kind == "function":
                self._fn = target
                unwrapped_fn = inspect.unwrap(target)
                if inspect.isgeneratorfunction(
                    unwrapped_fn
                ) or inspect.isasyncgenfunction(unwrapped_fn):
                    self._generator_methods = {"__call__", self._target_name}
            else:
                raw_cls = getattr(target, "__ray_actor_class__", None)
                if raw_cls is None and hasattr(target, "__ray_metadata__"):
                    mod_cls = getattr(target.__ray_metadata__, "modified_class", None)
                    raw_cls = getattr(mod_cls, "__ray_actor_class__", target)
                cls_to_instantiate = raw_cls if inspect.isclass(raw_cls) else target
                self._instance = cls_to_instantiate(
                    *(spec.get("args") or []), **(spec.get("kwargs") or {})
                )
                for attr_name in dir(self._instance):
                    if (
                        attr_name != "__call__"
                        and attr_name.startswith("__")
                        and attr_name.endswith("__")
                    ):
                        continue
                    with contextlib.suppress(Exception):
                        static_attr = inspect.getattr_static(self._instance, attr_name)
                        if isinstance(
                            static_attr, property
                        ) or inspect.isdatadescriptor(static_attr):
                            continue
                        member = getattr(self._instance, attr_name)
                        unwrapped = inspect.unwrap(member)
                        if inspect.isgeneratorfunction(
                            unwrapped
                        ) or inspect.isasyncgenfunction(unwrapped):
                            self._generator_methods.add(attr_name)
        except (Exception, SystemExit) as exc:
            self._init_error = {
                "ok": False,
                "error": _format_exception_dict(exc),
                "logs": self._get_logs_text(),
            }
            self._cleanup_resources()
            return

        if self._kind != "function":
            self._reaper_thread = threading.Thread(
                target=self._self_reap_loop,
                name=f"ray-client-ttl-{self._name}",
                daemon=True,
            )
            self._reaper_thread.start()

    def __repr__(self) -> str:
        if (
            getattr(self, "_instance", None) is not None
            and type(self._instance).__repr__ is not object.__repr__
        ):
            with contextlib.suppress(Exception):
                return repr(self._instance)
        return getattr(self, "_target_name", "_ClientRootActor")

    def _install_ray_hooks(self) -> None:
        ray.init = lambda *a, **kw: None
        ray.shutdown = lambda *a, **kw: None
        if os.environ.get("RAY_BACKEND") == "rust":
            return
        ns = self._namespace
        # Fate-shared actors created in this process default to a namespace of their
        # own, so concurrent roots that create identically named children don't
        # collide. Detached actors outlive the root, so they default to `ns`, where
        # other roots can find them by name. Lookups without a namespace fall back
        # to `ns`, which also holds the root client actors. The hooks are
        # process-local: nested tasks and actors run in other workers and use the
        # dashboard agent job's namespace.
        child_ns = f"{ns}_{self._name}"
        pg_list = self._placement_groups
        state_lock = self._state_lock
        os.environ["RAY_NAMESPACE"] = child_ns

        _orig_get_actor = ray.get_actor
        _orig_actor_remote = ActorClass._remote
        _orig_pg = getattr(ray.util, "placement_group", None)

        def _client_get_actor(name: str, namespace: Optional[str] = None):
            if namespace is not None:
                return _orig_get_actor(name, namespace=namespace)
            try:
                return _orig_get_actor(name, namespace=child_ns)
            except ValueError:
                return _orig_get_actor(name, namespace=ns)

        def _client_actor_remote(actor_cls, args=None, kwargs=None, **actor_options):
            if actor_options.get("namespace") is None:
                detached = actor_options.get("lifetime") == "detached" or bool(
                    actor_options.get("detached")
                )
                actor_options["namespace"] = ns if detached else child_ns
            return _orig_actor_remote(
                actor_cls, args=args, kwargs=kwargs, **actor_options
            )

        def _client_placement_group(*args, **kwargs):
            pg = _orig_pg(*args, **kwargs)
            if kwargs.get("lifetime") != "detached" and not kwargs.get("detached"):
                with state_lock:
                    pg_list.append(pg)
            return pg

        ray.get_actor = _client_get_actor
        ray._private.worker.get_actor = _client_get_actor
        ActorClass._remote = _client_actor_remote
        if callable(_orig_pg):
            ray.util.placement_group = _client_placement_group
            pg_mod = sys.modules.get("ray.util.placement_group")
            if pg_mod is not None:
                pg_mod.placement_group = _client_placement_group
        if hasattr(RuntimeContext, "namespace"):
            RuntimeContext.namespace = property(lambda _self: child_ns)

    def _self_reap_loop(self) -> None:
        while not self._shutdown_event.wait(timeout=0.5):
            should_reap = False
            with self._state_lock:
                if self._shutting_down:
                    return
                ttl = self._ttl_seconds
                active = self._active_calls
                elapsed = time.monotonic() - self._last_active_mono
                if ttl is not None and active == 0 and elapsed >= ttl:
                    self._shutting_down = True
                    should_reap = True
            if should_reap:
                self._cleanup_resources()
                try:
                    ray.actor.exit_actor()
                except Exception:
                    with contextlib.suppress(Exception):
                        ray.get_runtime_context().current_actor.__ray_destroy__(
                            reason="idle TTL expired"
                        )
                return

    def _cleanup_resources(self) -> None:
        self._shutdown_event.set()
        with self._cleanup_lock:
            with self._state_lock:
                if self._cleaned_up:
                    return
                self._cleaned_up = True
                self._shutting_down = True
                pgs = list(self._placement_groups)
                self._placement_groups.clear()
            if self._instance is not None:
                try:
                    close_fn = getattr(self._instance, "close", None)
                    exit_fn = getattr(self._instance, "__exit__", None)
                    if callable(close_fn):
                        res = close_fn()
                    elif callable(exit_fn):
                        res = exit_fn(None, None, None)
                    else:
                        res = None
                    if inspect.iscoroutine(res):
                        asyncio.run_coroutine_threadsafe(
                            res, self._ensure_async_loop()
                        ).result(timeout=_CONTROL_TIMEOUT_S)
                except BaseException:
                    logger.debug(
                        "Error in user cleanup hook for actor '%s'",
                        self._name,
                        exc_info=True,
                    )
            for pg in pgs:
                with contextlib.suppress(Exception):
                    ray.util.remove_placement_group(pg)
            if self._async_loop is not None:
                with contextlib.suppress(Exception):
                    self._async_loop.call_soon_threadsafe(self._async_loop.stop)

    def _ensure_async_loop(self) -> asyncio.AbstractEventLoop:
        with self._async_lock:
            if (
                self._async_loop is None
                or self._async_loop.is_closed()
                or (
                    self._async_thread is not None and not self._async_thread.is_alive()
                )
            ):
                loop = asyncio.new_event_loop()

                def _run_loop() -> None:
                    try:
                        loop.run_forever()
                    finally:
                        with contextlib.suppress(Exception):
                            loop.close()

                thread = threading.Thread(
                    target=_run_loop,
                    name=f"ray-client-async-{self._name}",
                    daemon=True,
                )
                thread.start()
                self._async_loop = loop
                self._async_thread = thread
            return self._async_loop

    def _is_current_task_canceled(self) -> bool:
        if self._shutting_down:
            return True
        with contextlib.suppress(Exception):
            return bool(ray.get_runtime_context().is_canceled())
        return False

    def _is_stream_canceled(self, stream_id: str) -> bool:
        with self._state_lock:
            if stream_id in self._cancelled_streams:
                return True
        return self._is_current_task_canceled()

    def _wait_coroutine_future(
        self,
        fut: concurrent.futures.Future,
        loop: asyncio.AbstractEventLoop,
        task_holder: List[Any],
        stream_id: Optional[str] = None,
    ) -> Any:
        while True:
            try:
                return fut.result(timeout=0.05)
            except concurrent.futures.TimeoutError:
                cancelled = (
                    self._is_stream_canceled(stream_id)
                    if stream_id is not None
                    else self._is_current_task_canceled()
                )
                if cancelled:
                    if task_holder and task_holder[0] is not None:
                        loop.call_soon_threadsafe(task_holder[0].cancel)
                    else:
                        fut.cancel()
                    with contextlib.suppress(BaseException):
                        fut.result(timeout=1.0)
                    fut.cancel()
                    raise asyncio.CancelledError("Task was cancelled")

    def _begin_call(self) -> None:
        with self._state_lock:
            if self._shutting_down:
                raise RuntimeError(f"Actor '{self._name}' is shutting down.")
            self._active_calls += 1
            self._last_active = time.time()
            self._last_active_mono = time.monotonic()

    def _end_call(self) -> None:
        with self._state_lock:
            self._active_calls = max(0, self._active_calls - 1)
            self._last_active = time.time()
            self._last_active_mono = time.monotonic()

    def _get_logs_text(self) -> str:
        with self._log_lock:
            return "".join(self._log_buffer)

    def _resolve_callable(self, method_name: str) -> Any:
        if self._kind == "function":
            if method_name in ("__call__", self._target_name):
                return self._fn
            raise AttributeError(
                f"Driver function '{self._target_name}' does not have method '{method_name}'."
            )
        if method_name.startswith("_") and method_name != "__call__":
            raise AttributeError(
                f"Cannot call private method '{method_name}' on actor '{self._name}'."
            )
        if not hasattr(self._instance, method_name):
            raise AttributeError(
                f"Actor '{self._name}' ({self._target_name}) has no method '{method_name}'."
            )
        return getattr(self._instance, method_name)

    @ray.method(concurrency_group="_control")
    def get_info(self) -> Dict[str, Any]:
        if self._init_error is not None:
            return self._init_error
        shutting_down = False
        with self._state_lock:
            if self._shutting_down:
                shutting_down = True
            else:
                active_calls = self._active_calls
                last_active = self._last_active
                elapsed = time.monotonic() - self._last_active_mono
                ttl = self._ttl_seconds
        if shutting_down:
            raise RuntimeError(f"Actor '{self._name}' is shutting down.")
        remaining_ttl: Optional[float] = None
        if ttl is not None:
            if active_calls > 0:
                remaining_ttl = float(ttl)
            else:
                remaining_ttl = max(0.0, float(ttl) - elapsed)
        return {
            "ok": True,
            "name": self._name,
            "class_name": self._target_name,
            "kind": self._kind,
            "namespace": self._namespace,
            "status": "ALIVE",
            "detached": self._detached,
            "spec_hash": self._spec_hash,
            "active_calls": active_calls,
            "ttl_seconds": ttl,
            "remaining_ttl_seconds": remaining_ttl,
            "created_at": self._created_at,
            "last_active": last_active,
            "generator_methods": sorted(self._generator_methods),
        }

    @ray.method(concurrency_group="_control")
    def set_ttl(self, ttl_seconds: Optional[float]) -> Dict[str, Any]:
        validated = _validate_ttl_seconds(ttl_seconds)
        with self._state_lock:
            if self._shutting_down:
                raise RuntimeError(f"Actor '{self._name}' is shutting down.")
            self._ttl_seconds = validated
            self._last_active = time.time()
            self._last_active_mono = time.monotonic()
        return self.get_info()

    @ray.method(concurrency_group="_control")
    def get_logs(self) -> Dict[str, Any]:
        return {"ok": True, "name": self._name, "logs": self._get_logs_text()}

    @ray.method(concurrency_group="_control")
    def cancel_stream(self, stream_id: str) -> Dict[str, Any]:
        with self._state_lock:
            self._cancelled_streams[stream_id] = None
            while len(self._cancelled_streams) > 256:
                self._cancelled_streams.popitem(last=False)
            self._last_active = time.time()
            self._last_active_mono = time.monotonic()
        return {"ok": True, "cancelled": True}

    def invoke(
        self,
        method_name: str,
        args: List[Any],
        kwargs: Dict[str, Any],
    ) -> Dict[str, Any]:
        self._begin_call()
        try:
            target = self._resolve_callable(method_name)
            try:
                res = target(*args, **kwargs)
                if inspect.iscoroutine(res):
                    loop = self._ensure_async_loop()
                    task_holder: List[Any] = []
                    fut = asyncio.run_coroutine_threadsafe(
                        _await_coro_catching_system_exit(res, task_holder),
                        loop,
                    )
                    is_exit, val = self._wait_coroutine_future(fut, loop, task_holder)
                    if is_exit:
                        raise SystemExit(val)
                    res = val
            except SystemExit as exc:
                if exc.code in (0, None):
                    res = None
                else:
                    raise RuntimeError(f"__RAY_SYSTEM_EXIT__:{exc.code}") from None

            label = (
                f"Function '{self._target_name}' return value"
                if self._kind == "function"
                else f"Return value of '{self._name}.{method_name}'"
            )
            _validate_strict_json(res, label)
            return {"ok": True, "result": res}
        except (Exception, asyncio.CancelledError) as exc:
            return {
                "ok": False,
                "error": _format_exception_dict(exc),
                "logs": self._get_logs_text(),
            }
        finally:
            self._end_call()
            if self._kind == "function":
                self._cleanup_resources()

    def invoke_stream(
        self,
        method_name: str,
        args: List[Any],
        kwargs: Dict[str, Any],
        stream_id: str,
    ):
        self._begin_call()
        gen = None
        try:
            target = self._resolve_callable(method_name)
            gen = target(*args, **kwargs)
            label = (
                f"Streamed item from '{self._target_name}'"
                if self._kind == "function"
                else f"Streamed item from '{self._name}.{method_name}'"
            )
            if inspect.isasyncgen(gen):
                loop = self._ensure_async_loop()
                while not self._is_stream_canceled(stream_id):
                    task_holder: List[Any] = []
                    fut = asyncio.run_coroutine_threadsafe(
                        _anext_catching_system_exit(gen, task_holder), loop
                    )
                    tag, item = self._wait_coroutine_future(
                        fut, loop, task_holder, stream_id=stream_id
                    )
                    if tag == "stop":
                        break
                    if tag == "exit":
                        if item in (0, None):
                            break
                        raise RuntimeError(f"__RAY_SYSTEM_EXIT__:{item}")
                    with self._state_lock:
                        self._last_active = time.time()
                        self._last_active_mono = time.monotonic()
                    if self._is_stream_canceled(stream_id):
                        break
                    _validate_strict_json(item, label)
                    yield {"ok": True, "item": item}
            elif inspect.isgenerator(gen):
                while not self._is_stream_canceled(stream_id):
                    try:
                        item = next(gen)
                    except StopIteration:
                        break
                    except SystemExit as exc:
                        if exc.code in (0, None):
                            break
                        raise RuntimeError(f"__RAY_SYSTEM_EXIT__:{exc.code}") from None
                    with self._state_lock:
                        self._last_active = time.time()
                        self._last_active_mono = time.monotonic()
                    if self._is_stream_canceled(stream_id):
                        break
                    _validate_strict_json(item, label)
                    yield {"ok": True, "item": item}
            else:
                raise TypeError(
                    f"'{self._target_name}.{method_name}' is not a generator function."
                )
        except (GeneratorExit, asyncio.CancelledError):
            pass
        except Exception as exc:
            if not self._is_stream_canceled(stream_id):
                yield {
                    "ok": False,
                    "error": _format_exception_dict(exc),
                    "logs": self._get_logs_text(),
                }
        finally:
            with self._state_lock:
                self._cancelled_streams.pop(stream_id, None)
            if gen is not None:
                with contextlib.suppress(BaseException):
                    if inspect.isasyncgen(gen) and self._async_loop is not None:
                        asyncio.run_coroutine_threadsafe(
                            gen.aclose(), self._async_loop
                        ).result(timeout=2.0)
                    elif hasattr(gen, "close"):
                        gen.close()
            self._end_call()
            if self._kind == "function":
                self._cleanup_resources()

    @ray.method(concurrency_group="_control")
    def shutdown(self, only_if_idle: bool = False) -> Dict[str, Any]:
        if only_if_idle:
            with self._state_lock:
                if not self._shutting_down:
                    ttl = self._ttl_seconds
                    elapsed = time.monotonic() - self._last_active_mono
                    if ttl is None or self._active_calls > 0 or elapsed < ttl:
                        return {"ok": True, "reaped": False}
                    self._shutting_down = True
        self._cleanup_resources()
        return {"ok": True, "reaped": True}


_RemoteClientRootActor = ray.remote(concurrency_groups={"_control": 4})(
    _ClientRootActor
)


def _is_client_root_actor_handle(handle: Any) -> bool:
    desc = getattr(handle, "_ray_actor_creation_function_descriptor", None)
    cls_name = getattr(desc, "class_name", None)
    if isinstance(cls_name, str):
        return cls_name.endswith("_ClientRootActor")
    alt_cls_name = getattr(handle, "_class_name", None) or getattr(
        getattr(handle, "_actor_cls", None), "__name__", None
    )
    if isinstance(alt_cls_name, str):
        return alt_cls_name.endswith("_ClientRootActor")
    return False


def _destroy_actor_handle(
    handle: Any, reason: str = "terminated by ray_client"
) -> None:
    try:
        ray.kill(handle, no_restart=True)
    except (NotImplementedError, AttributeError):
        destroy_fn = getattr(handle, "__ray_destroy__", None)
        if callable(destroy_fn):
            destroy_fn(reason=reason)
        else:
            raise


async def _wait_actor_deregistered(
    name: str, namespace: str, timeout: float = 2.0
) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            ray.get_actor(name, namespace=namespace)
        except Exception:
            return
        await asyncio.sleep(0.02)


class ActorManager:
    """Dashboard agent manager for `/api/actors` root Ray Actors."""

    def __init__(self) -> None:
        self._actors: Dict[str, Dict[str, Any]] = {}
        self._creating_handles: Dict[str, Any] = {}
        self._non_root_names: Set[str] = set()
        self._lock = asyncio.Lock()
        self._name_locks: Dict[str, asyncio.Lock] = {}
        self._name_lock_waiters: Dict[str, int] = {}
        self._reaper_task: Optional[asyncio.Task] = None

    async def _get_name_lock(self, name: str) -> asyncio.Lock:
        async with self._lock:
            lock = self._name_locks.get(name)
            if lock is None:
                lock = self._name_locks[name] = asyncio.Lock()
            self._name_lock_waiters[name] = self._name_lock_waiters.get(name, 0) + 1
            return lock

    def _release_name_lock_ref_locked(self, name: str) -> None:
        rem = self._name_lock_waiters.get(name, 1) - 1
        if rem <= 0:
            self._name_lock_waiters.pop(name, None)
        else:
            self._name_lock_waiters[name] = rem
        self._prune_name_lock_locked(name)

    def _prune_name_lock_locked(self, name: str) -> None:
        if (
            name in self._actors
            or name in self._creating_handles
            or self._name_lock_waiters.get(name, 0) > 0
        ):
            return
        lock = self._name_locks.get(name)
        if lock is not None and not lock.locked():
            self._name_locks.pop(name, None)

    def _ensure_reaper(self) -> None:
        if self._reaper_task is None or self._reaper_task.done():
            with contextlib.suppress(RuntimeError):
                loop = asyncio.get_running_loop()
                self._reaper_task = loop.create_task(self._ttl_reaper_loop())

    async def _ttl_reaper_loop(self) -> None:
        while True:
            await asyncio.sleep(0.5)
            now_mono = time.monotonic()
            candidates: List[Tuple[str, Dict[str, Any]]] = []
            async with self._lock:
                for name, meta in list(self._actors.items()):
                    ttl = meta.get("ttl_seconds")
                    if (
                        ttl is not None
                        and meta.get("active_calls", 0) == 0
                        and (now_mono - meta.get("last_active_mono", now_mono))
                        >= (ttl + 1.5)
                    ):
                        candidates.append((name, meta))
            for name, meta in candidates:
                if meta.get("active_calls", 0) > 0:
                    continue
                handle = meta["handle"]
                try:
                    res = await asyncio.wait_for(
                        handle.shutdown.remote(True), timeout=_CONTROL_TIMEOUT_S
                    )
                    if isinstance(res, dict) and res.get("reaped"):
                        logger.info("Actor '%s' idle TTL expired; terminating.", name)
                        async with self._lock:
                            if self._actors.get(name) is meta:
                                self._actors.pop(name, None)
                                self._prune_name_lock_locked(name)
                        with contextlib.suppress(Exception):
                            _destroy_actor_handle(
                                handle, reason="idle TTL expired in ray_client"
                            )
                except (RayActorError, RayTaskError):
                    async with self._lock:
                        if self._actors.get(name) is meta:
                            self._actors.pop(name, None)
                            self._prune_name_lock_locked(name)
                except Exception:
                    logger.debug(
                        "Reaper check failed for actor '%s'", name, exc_info=True
                    )

    @contextlib.asynccontextmanager
    async def _track_call(self, meta: Dict[str, Any]):
        meta["active_calls"] = meta.get("active_calls", 0) + 1
        meta["last_active"] = time.time()
        meta["last_active_mono"] = time.monotonic()
        try:
            yield
        finally:
            meta["active_calls"] = max(0, meta.get("active_calls", 1) - 1)
            meta["last_active"] = time.time()
            meta["last_active_mono"] = time.monotonic()

    async def _reap_one_shot(self, name: str, clean: bool = False) -> None:
        async def _do_reap() -> None:
            async with self._lock:
                meta = self._actors.pop(name, None)
                self._prune_name_lock_locked(name)
            if meta is None:
                return
            handle = meta["handle"]
            if not clean:
                with contextlib.suppress(Exception):
                    await asyncio.wait_for(handle.shutdown.remote(), timeout=1.0)
            with contextlib.suppress(Exception):
                _destroy_actor_handle(handle, reason="one-shot driver finished")

        # Run the kill as its own task so that a client disconnect, which cancels
        # the request awaiting it, can't interrupt the cleanup halfway.
        with contextlib.suppress(Exception):
            await asyncio.shield(run_background_task(_do_reap()))

    async def _lookup_actor_with_status(
        self,
        name: str,
        *,
        namespace: str = CLIENT_ACTOR_NAMESPACE,
        timeout: float = _CONTROL_TIMEOUT_S,
        fetch_info: bool = False,
    ) -> Tuple[str, Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
        self._ensure_reaper()
        async with self._lock:
            if name in self._creating_handles:
                return "CREATING", None, None
            meta = self._actors.get(name)
        if meta is not None:
            if not fetch_info:
                return "FOUND", meta, None
            try:
                info = await asyncio.wait_for(
                    meta["handle"].get_info.remote(), timeout=timeout
                )
            except (RayActorError, RayTaskError):
                async with self._lock:
                    if self._actors.get(name) is meta:
                        self._actors.pop(name, None)
                        self._prune_name_lock_locked(name)
                return "BROKEN", None, None
            except Exception:
                return "UNRESPONSIVE", meta, None
            if not isinstance(info, dict) or not info.get("ok", True):
                return "BROKEN", meta, None
            info = dict(info)
            info["active_calls"] = max(
                info.get("active_calls", 0), meta.get("active_calls", 0)
            )
            return "FOUND", meta, info

        try:
            handle = ray.get_actor(name, namespace=namespace)
        except Exception:
            async with self._lock:
                self._non_root_names.discard(name)
            return "MISSING", None, None
        if not _is_client_root_actor_handle(handle):
            async with self._lock:
                self._non_root_names.add(name)
            return "NON_ROOT", None, None
        async with self._lock:
            self._non_root_names.discard(name)
        try:
            info = await asyncio.wait_for(handle.get_info.remote(), timeout=timeout)
        except (RayActorError, RayTaskError):
            return "BROKEN", None, None
        except Exception:
            return "UNRESPONSIVE", None, None
        if not isinstance(info, dict) or not info.get("ok", True):
            return "BROKEN", None, None
        now_mono = time.monotonic()
        rem = info.get("remaining_ttl_seconds")
        ttl = info.get("ttl_seconds")
        elapsed = (
            max(0.0, float(ttl) - float(rem))
            if (ttl is not None and rem is not None)
            else 0.0
        )
        meta = {
            "name": name,
            "handle": handle,
            "class_name": info.get("class_name", name),
            "kind": info.get("kind", "class"),
            "namespace": info.get("namespace", namespace),
            "detached": bool(info.get("detached", True)),
            "spec_hash": info.get("spec_hash"),
            "ttl_seconds": ttl,
            "created_at": info.get("created_at", time.time()),
            "last_active": info.get("last_active", time.time()),
            "last_active_mono": now_mono - elapsed,
            "active_calls": 0,
            "generator_methods": set(info.get("generator_methods") or []),
        }
        async with self._lock:
            if name in self._creating_handles:
                return "CREATING", None, None
            existing = self._actors.get(name)
            if existing is not None:
                meta = existing
            else:
                self._actors[name] = meta
        info = dict(info)
        info["active_calls"] = max(
            info.get("active_calls", 0), meta.get("active_calls", 0)
        )
        return "FOUND", meta, info

    async def lookup_actor(
        self, name: str, timeout: float = _CONTROL_TIMEOUT_S
    ) -> Optional[Dict[str, Any]]:
        status, meta, _ = await self._lookup_actor_with_status(
            name, timeout=timeout, fetch_info=False
        )
        return meta if status == "FOUND" else None

    async def _terminate_actor_for_recreate(
        self,
        name: str,
        namespace: str,
        already_killed: bool = False,
        timeout: float = 2.0,
    ) -> bool:
        wait_timeout = min(2.0, max(0.01, float(timeout)))
        deadline = time.monotonic() + wait_timeout
        async with self._lock:
            meta = self._actors.pop(name, None)
            creating_handle = self._creating_handles.pop(name, None)
            if meta is not None or creating_handle is not None or already_killed:
                self._non_root_names.discard(name)
            self._prune_name_lock_locked(name)
        handle = meta["handle"] if meta is not None else creating_handle
        if handle is None and not already_killed:
            try:
                handle = ray.get_actor(name, namespace=namespace)
            except Exception:
                async with self._lock:
                    self._non_root_names.discard(name)
                return False
            if not _is_client_root_actor_handle(handle):
                async with self._lock:
                    self._non_root_names.add(name)
                return False
            async with self._lock:
                self._non_root_names.discard(name)
        if handle is not None:
            try:
                if (
                    meta is not None or creating_handle is None
                ) and _is_client_root_actor_handle(handle):
                    rem = max(0.01, deadline - time.monotonic())
                    with contextlib.suppress(Exception):
                        await asyncio.wait_for(handle.shutdown.remote(), timeout=rem)
            finally:
                try:
                    _destroy_actor_handle(handle, reason="recreated by ray_client")
                except ValueError:
                    if meta is None and creating_handle is None and not already_killed:
                        return False
                except Exception:
                    pass
        elif not already_killed:
            return False
        rem = max(0.01, deadline - time.monotonic())
        await _wait_actor_deregistered(name, namespace, timeout=rem)
        return True

    async def _cleanup_failed_creation(
        self, name: str, namespace: str, handle: Any
    ) -> None:
        async def _do_cleanup() -> None:
            async with self._lock:
                if self._creating_handles.get(name) is handle:
                    self._creating_handles.pop(name, None)
                    self._prune_name_lock_locked(name)
            if handle is not None:
                with contextlib.suppress(Exception):
                    _destroy_actor_handle(
                        handle, reason="failed actor creation cleanup"
                    )
                with contextlib.suppress(Exception):
                    await _wait_actor_deregistered(name, namespace, timeout=2.0)

        with contextlib.suppress(BaseException):
            await asyncio.shield(run_background_task(_do_cleanup()))

    def validate_actor_spec(self, spec: Dict[str, Any]) -> Dict[str, Any]:
        raw_kind = spec.get("kind")
        if raw_kind is not None and raw_kind not in ("class", "function"):
            raise ValueError(f"kind must be 'class' or 'function', got {raw_kind!r}.")
        kind = raw_kind or ("function" if "fn_name" in spec else "class")
        target_name = (
            spec.get("fn_name")
            if kind == "function"
            else (spec.get("class_name") or spec.get("name"))
        )
        if (
            not target_name
            or not isinstance(target_name, str)
            or not target_name.strip()
        ):
            raise ValueError("Actor spec must specify 'class_name' or 'fn_name'.")
        if "name" in spec and spec["name"] is not None:
            if not isinstance(spec["name"], str) or not spec["name"].strip():
                raise ValueError("Actor name must be a non-empty string.")
        if (
            "detached" in spec
            and spec["detached"] is not None
            and not isinstance(spec["detached"], bool)
        ):
            raise ValueError(f"detached must be a bool, got {spec['detached']!r}.")
        if spec.get("args") is not None and not isinstance(spec["args"], list):
            raise ValueError(f"args must be a list, got {spec['args']!r}.")
        if spec.get("kwargs") is not None and not isinstance(spec["kwargs"], dict):
            raise ValueError(f"kwargs must be a dict, got {spec['kwargs']!r}.")
        if spec.get("runtime_env") is not None and not isinstance(
            spec["runtime_env"], dict
        ):
            raise ValueError(
                f"runtime_env must be a dict, got {spec['runtime_env']!r}."
            )
        if spec.get("captured_globals") is not None and not isinstance(
            spec["captured_globals"], dict
        ):
            raise ValueError(
                f"captured_globals must be a dict, got {spec['captured_globals']!r}."
            )
        has_explicit_ttl = "ttl_seconds" in spec
        ttl_seconds = _validate_ttl_seconds(
            spec["ttl_seconds"] if has_explicit_ttl else 1800.0
        )
        get_if_exists = _validate_get_if_exists(spec.get("get_if_exists", True))
        node_id = spec.get("node_id")
        if node_id is not None:
            if not isinstance(node_id, str) or not node_id.strip():
                raise ValueError(
                    f"node_id must be a non-empty string, got {node_id!r}."
                )
            node_id = node_id.strip()
        label_selector = _validate_label_selector(spec.get("label_selector"))
        if node_id is not None and label_selector:
            raise ValueError("Cannot specify both 'node_id' and 'label_selector'.")
        max_concurrency = (
            _validate_max_concurrency(spec["max_concurrency"])
            if spec.get("max_concurrency") is not None
            else 1
        )
        create_timeout = _validate_timeout(spec.get("timeout"), "timeout")
        (
            num_cpus,
            num_gpus,
            memory,
            custom_resources,
        ) = _normalize_spec_resources(spec, kind)
        return {
            "kind": kind,
            "target_name": target_name,
            "has_explicit_ttl": has_explicit_ttl,
            "ttl_seconds": ttl_seconds,
            "get_if_exists": get_if_exists,
            "node_id": node_id,
            "label_selector": label_selector,
            "max_concurrency": max_concurrency,
            "create_timeout": create_timeout,
            "num_cpus": num_cpus,
            "num_gpus": num_gpus,
            "memory": memory,
            "custom_resources": custom_resources,
        }

    async def create_actor(
        self, spec: Dict[str, Any], *, _validated: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        self._ensure_reaper()
        if _validated is not None:
            validated = _validated
        else:
            try:
                validated = self.validate_actor_spec(spec)
            except ValueError as exc:
                return {"ok": False, "error": _format_exception_dict(exc)}

        kind = validated["kind"]
        target_name = validated["target_name"]
        has_explicit_ttl = validated["has_explicit_ttl"]
        ttl_seconds = validated["ttl_seconds"]
        get_if_exists = validated["get_if_exists"]
        node_id = validated["node_id"]
        label_selector = validated["label_selector"]
        max_concurrency = validated["max_concurrency"]
        create_timeout = validated["create_timeout"]
        num_cpus = validated["num_cpus"]
        num_gpus = validated["num_gpus"]
        memory = validated["memory"]
        custom_resources = validated["custom_resources"]

        name = spec.get("name") or f"{target_name}_{uuid.uuid4().hex[:8]}"
        # One-shot driver functions (`cluster.run`) default to owner-bound
        # (`detached=False`) so they fate-share with the agent and allow warm
        # worker reuse across consecutive runs; persistent actors default to
        # `detached=True`.
        detached = bool(spec.get("detached", kind != "function"))
        spec = dict(spec, node_id=node_id, label_selector=label_selector)
        spec_hash = _compute_spec_hash(
            spec, kind, target_name, num_cpus, max_concurrency
        )
        spec = dict(
            spec,
            name=name,
            kind=kind,
            ttl_seconds=ttl_seconds,
            detached=detached,
            num_cpus=num_cpus,
            num_gpus=num_gpus,
            memory=memory,
            resources=custom_resources or None,
            spec_hash=spec_hash,
        )
        namespace = spec.get("namespace") or CLIENT_ACTOR_NAMESPACE
        deadline = (
            time.monotonic() + float(create_timeout)
            if create_timeout is not None
            else None
        )

        def _remaining_timeout(default: Optional[float] = None) -> Optional[float]:
            if deadline is None:
                return default
            rem = deadline - time.monotonic()
            if rem <= 0:
                raise asyncio.TimeoutError()
            return min(rem, default) if default is not None else rem

        def _timeout_error() -> Dict[str, Any]:
            return {
                "ok": False,
                "error": {
                    "exc_type": "TimeoutError",
                    "exc_message": (
                        f"Creation of actor '{name}' timed out after {create_timeout}s."
                    ),
                    "timed_out": True,
                },
            }

        name_lock = await self._get_name_lock(name)
        try:
            if deadline is not None:
                await asyncio.wait_for(
                    name_lock.acquire(), timeout=_remaining_timeout()
                )
            else:
                await name_lock.acquire()
        except BaseException as exc:
            async with self._lock:
                self._release_name_lock_ref_locked(name)
            if isinstance(exc, asyncio.TimeoutError):
                return _timeout_error()
            raise

        try:
            if get_if_exists in (True, "recreate"):
                try:
                    lookup_timeout = _remaining_timeout(_CONTROL_TIMEOUT_S)
                except asyncio.TimeoutError:
                    return _timeout_error()
                eff_lookup_timeout = (
                    lookup_timeout if lookup_timeout is not None else _CONTROL_TIMEOUT_S
                )
                status, existing, info = await self._lookup_actor_with_status(
                    name,
                    namespace=namespace,
                    timeout=eff_lookup_timeout,
                    fetch_info=True,
                )
                should_terminate_existing = False
                if status == "NON_ROOT":
                    action = "reattached" if get_if_exists is True else "replaced"
                    return {
                        "ok": False,
                        "error": {
                            "exc_type": "ValueError",
                            "exc_message": (
                                f"Actor '{name}' already exists in namespace '{namespace}' "
                                f"as a non-ray_client actor and cannot be {action}."
                            ),
                        },
                    }
                if status == "UNRESPONSIVE":
                    if deadline is not None and time.monotonic() >= deadline:
                        return _timeout_error()
                    if get_if_exists == "recreate":
                        should_terminate_existing = True
                    else:
                        return {
                            "ok": False,
                            "error": {
                                "exc_type": "TimeoutError",
                                "exc_message": (
                                    f"Actor '{name}' exists but did not respond within "
                                    f"{_CONTROL_TIMEOUT_S}s; pass get_if_exists='recreate' to replace it."
                                ),
                                "timed_out": True,
                            },
                        }
                elif status == "BROKEN" or (
                    status == "CREATING" and get_if_exists == "recreate"
                ):
                    should_terminate_existing = True
                elif status == "FOUND" and existing is not None and info is not None:
                    existing_cls = info.get("class_name") or existing.get("class_name")
                    existing_hash = info.get("spec_hash") or existing.get("spec_hash")
                    class_mismatch = bool(
                        spec.get("class_name")
                        and existing_cls
                        and existing_cls != target_name
                    )
                    spec_mismatch = bool(class_mismatch or existing_hash != spec_hash)
                    if get_if_exists == "recreate" and spec_mismatch:
                        should_terminate_existing = True
                    elif class_mismatch:
                        return {
                            "ok": False,
                            "error": {
                                "exc_type": "ValueError",
                                "exc_message": (
                                    f"Actor '{name}' already exists with class "
                                    f"'{existing_cls}', cannot reattach as '{target_name}'."
                                ),
                            },
                        }
                    else:
                        try:
                            if has_explicit_ttl:
                                info = await asyncio.wait_for(
                                    existing["handle"].set_ttl.remote(ttl_seconds),
                                    timeout=_remaining_timeout(_CONTROL_TIMEOUT_S),
                                )
                                existing["ttl_seconds"] = ttl_seconds
                        except asyncio.TimeoutError:
                            if deadline is not None and time.monotonic() >= deadline:
                                return _timeout_error()
                            if get_if_exists == "recreate":
                                should_terminate_existing = True
                            else:
                                return {
                                    "ok": False,
                                    "error": {
                                        "exc_type": "TimeoutError",
                                        "exc_message": (
                                            f"Actor '{name}' exists but did not respond within "
                                            f"{_CONTROL_TIMEOUT_S}s; pass get_if_exists='recreate' to replace it."
                                        ),
                                        "timed_out": True,
                                    },
                                }
                        except Exception:
                            should_terminate_existing = True
                        else:
                            existing["generator_methods"] = set(
                                info.get("generator_methods") or []
                            )
                            existing["last_active"] = time.time()
                            existing["last_active_mono"] = time.monotonic()
                            return {
                                **info,
                                "ok": True,
                                "name": name,
                                "created": False,
                                "spec_hash": existing_hash or spec_hash,
                                "spec_mismatch": spec_mismatch,
                            }
                if should_terminate_existing:
                    try:
                        term_timeout = _remaining_timeout(2.0)
                    except asyncio.TimeoutError:
                        return _timeout_error()
                    await self._terminate_actor_for_recreate(
                        name,
                        namespace,
                        timeout=term_timeout if term_timeout is not None else 2.0,
                    )
                if name in self._non_root_names:
                    action = "reattached" if get_if_exists is True else "replaced"
                    return {
                        "ok": False,
                        "error": {
                            "exc_type": "ValueError",
                            "exc_message": (
                                f"Actor '{name}' already exists in namespace '{namespace}' "
                                f"as a non-ray_client actor and cannot be {action}."
                            ),
                        },
                    }

            handle = None
            try:
                options: Dict[str, Any] = {
                    "name": name,
                    "namespace": namespace,
                    "num_cpus": num_cpus,
                    "max_concurrency": max_concurrency,
                }
                if detached:
                    options["lifetime"] = "detached"
                if spec.get("runtime_env"):
                    options["runtime_env"] = spec["runtime_env"]
                placement_keys = (
                    "num_gpus",
                    "memory",
                    "resources",
                    "label_selector",
                )
                for opt_key in placement_keys:
                    if spec.get(opt_key) is not None:
                        options[opt_key] = spec[opt_key]
                if spec.get("node_id") is not None:
                    options["label_selector"] = {
                        ray._raylet.RAY_NODE_ID_KEY: spec["node_id"]
                    }
                elif (
                    num_cpus == 0
                    and not any(spec.get(k) for k in placement_keys)
                    and os.environ.get(RAY_JOB_ALLOW_DRIVER_ON_WORKER_NODES_ENV_VAR)
                    != "1"
                ):
                    # Like a Ray Jobs driver, a root that requests no resources runs
                    # on the head node, where this agent runs. It then starts even on
                    # a `num-cpus=0` head and never takes a CPU from worker nodes.
                    head_node_id = ray.get_runtime_context().get_node_id()
                    options["label_selector"] = {
                        ray._raylet.RAY_NODE_ID_KEY: head_node_id
                    }

                handle = _RemoteClientRootActor.options(**options).remote(spec)
                async with self._lock:
                    self._non_root_names.discard(name)
                    self._creating_handles[name] = handle
                info_ref = handle.get_info.remote()
                if deadline is not None:
                    info = await asyncio.wait_for(
                        info_ref, timeout=_remaining_timeout()
                    )
                else:
                    info = await info_ref
                if not isinstance(info, dict) or not info.get("ok", True):
                    await self._cleanup_failed_creation(name, namespace, handle)
                    return (
                        info
                        if isinstance(info, dict)
                        else {
                            "ok": False,
                            "error": {
                                "exc_type": "RuntimeError",
                                "exc_message": f"Actor '{name}' failed to initialize.",
                            },
                        }
                    )
                now = time.time()
                now_mono = time.monotonic()
                async with self._lock:
                    if self._creating_handles.get(name) is not handle:
                        raise RuntimeError(
                            f"Actor '{name}' was killed during creation."
                        )
                    self._creating_handles.pop(name, None)
                    self._actors[name] = {
                        "name": name,
                        "handle": handle,
                        "class_name": target_name,
                        "kind": kind,
                        "namespace": namespace,
                        "detached": detached,
                        "spec_hash": spec_hash,
                        "ttl_seconds": ttl_seconds,
                        "created_at": now,
                        "last_active": now,
                        "last_active_mono": now_mono,
                        "active_calls": 0,
                        "generator_methods": set(info.get("generator_methods") or []),
                    }
                return {
                    **info,
                    "ok": True,
                    "name": name,
                    "created": True,
                    "spec_hash": spec_hash,
                    "spec_mismatch": False,
                }
            except asyncio.TimeoutError:
                await self._cleanup_failed_creation(name, namespace, handle)
                return _timeout_error()
            except BaseException as exc:
                await self._cleanup_failed_creation(name, namespace, handle)
                if not isinstance(exc, Exception):
                    raise
                return {"ok": False, "error": _format_exception_dict(exc)}
        finally:
            name_lock.release()
            async with self._lock:
                self._release_name_lock_ref_locked(name)

    async def list_actors(self) -> List[Dict[str, Any]]:
        self._ensure_reaper()
        discovered_info: Dict[str, Dict[str, Any]] = {}
        with contextlib.suppress(Exception):
            named = ray.util.list_named_actors(all_namespaces=True)
            active_ns_names = [
                entry["name"]
                for entry in named
                if isinstance(entry, dict)
                and entry.get("namespace") == CLIENT_ACTOR_NAMESPACE
                and isinstance(entry.get("name"), str)
            ]
            async with self._lock:
                self._non_root_names.intersection_update(active_ns_names)
                known_names = (
                    set(self._actors)
                    | set(self._creating_handles)
                    | set(self._non_root_names)
                )
            uncached_names = [n for n in active_ns_names if n not in known_names]
            if uncached_names:
                discovered = await asyncio.gather(
                    *(
                        self._lookup_actor_with_status(n, fetch_info=True)
                        for n in uncached_names
                    )
                )
                for n, (st, _, info) in zip(uncached_names, discovered):
                    if st == "FOUND" and info is not None:
                        discovered_info[n] = info

        async with self._lock:
            snapshot = list(self._actors.items())

        async def _query_one(
            name: str, meta: Dict[str, Any]
        ) -> Optional[Dict[str, Any]]:
            if name in discovered_info:
                return discovered_info[name]
            try:
                info = await asyncio.wait_for(
                    meta["handle"].get_info.remote(), timeout=_CONTROL_TIMEOUT_S
                )
                if not isinstance(info, dict) or not info.get("ok", True):
                    return None
                info["active_calls"] = max(
                    info.get("active_calls", 0), meta.get("active_calls", 0)
                )
                return info
            except (RayActorError, RayTaskError):
                async with self._lock:
                    if self._actors.get(name) is meta:
                        self._actors.pop(name, None)
                        self._prune_name_lock_locked(name)
                return None
            except Exception:
                return {
                    "ok": True,
                    "name": name,
                    "class_name": meta.get("class_name", name),
                    "kind": meta.get("kind", "class"),
                    "namespace": meta.get("namespace", CLIENT_ACTOR_NAMESPACE),
                    "status": "ALIVE",
                    "detached": meta.get("detached", True),
                    "spec_hash": meta.get("spec_hash"),
                    "active_calls": meta.get("active_calls", 0),
                    "ttl_seconds": meta.get("ttl_seconds"),
                    "created_at": meta.get("created_at"),
                    "last_active": meta.get("last_active"),
                }

        results = await asyncio.gather(*(_query_one(n, m) for n, m in snapshot))
        return [r for r in results if r is not None]

    async def get_actor_info(self, name: str) -> Optional[Dict[str, Any]]:
        status, _, info = await self._lookup_actor_with_status(name, fetch_info=True)
        return info if status == "FOUND" else None

    async def get_actor_logs(self, name: str) -> Optional[Dict[str, Any]]:
        meta = await self.lookup_actor(name)
        if meta is None:
            return None
        try:
            return await asyncio.wait_for(
                meta["handle"].get_logs.remote(), timeout=_CONTROL_TIMEOUT_S
            )
        except RayActorError:
            async with self._lock:
                if self._actors.get(name) is meta:
                    self._actors.pop(name, None)
                    self._prune_name_lock_locked(name)
            return None
        except Exception:
            return None

    async def call_actor_method(
        self,
        name: str,
        method_name: str,
        payload: Dict[str, Any],
        *,
        meta: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        """Calls `method_name` on actor `name` and returns the result payload.

        Returns None if the actor doesn't exist. Every other failure is returned as
        an `{"ok": False, "error": ...}` payload, so only cancellation propagates.
        """
        if meta is None:
            meta = await self.lookup_actor(name)
        if meta is None:
            return None
        one_shot = meta.get("kind") == "function"
        try:
            raw_args = payload.get("args")
            if raw_args is not None and not isinstance(raw_args, list):
                raise ValueError(f"args must be a list, got {raw_args!r}.")
            raw_kwargs = payload.get("kwargs")
            if raw_kwargs is not None and not isinstance(raw_kwargs, dict):
                raise ValueError(f"kwargs must be a dict, got {raw_kwargs!r}.")
            args = raw_args or []
            kwargs = raw_kwargs or {}
            timeout = _validate_timeout(payload.get("timeout"), "timeout")
        except ValueError as exc:
            if one_shot:
                await self._reap_one_shot(name, clean=False)
            return {"ok": False, "error": _format_exception_dict(exc)}

        target_name = meta.get("class_name") or name
        task_name = (
            target_name
            if (method_name == "__call__" or one_shot)
            else f"{target_name}.{method_name}"
        )

        async with self._track_call(meta):
            completed_in_worker = False
            try:
                try:
                    invoke_method = meta["handle"].invoke.options(name=task_name)
                except (AttributeError, TypeError, NotImplementedError):
                    invoke_method = meta["handle"].invoke
                obj_ref = invoke_method.remote(method_name, args, kwargs)
                if timeout is not None:
                    res = await asyncio.wait_for(obj_ref, timeout=float(timeout))
                else:
                    res = await obj_ref
                completed_in_worker = True
                return res
            except asyncio.TimeoutError:
                with contextlib.suppress(Exception):
                    ray.cancel(obj_ref, force=False, recursive=True)
                return {
                    "ok": False,
                    "error": {
                        "exc_type": "TimeoutError",
                        "exc_message": (
                            f"Execution of '{name}.{method_name}' timed out after {timeout}s."
                        ),
                        "timed_out": True,
                    },
                }
            except asyncio.CancelledError:
                # The caller went away, e.g. the HTTP client disconnected.
                with contextlib.suppress(Exception):
                    ray.cancel(obj_ref, force=False, recursive=True)
                raise
            except Exception as exc:
                return {"ok": False, "error": _format_exception_dict(exc)}
            finally:
                if one_shot:
                    await self._reap_one_shot(name, clean=completed_in_worker)

    async def stream_actor_method(
        self,
        name: str,
        method_name: str,
        payload: Dict[str, Any],
        *,
        meta: Optional[Dict[str, Any]] = None,
    ):
        if meta is None:
            meta = await self.lookup_actor(name)
        if meta is None:
            yield {
                "ok": False,
                "not_found": True,
                "error": {
                    "exc_type": "KeyError",
                    "exc_message": f"Actor '{name}' does not exist.",
                },
            }
            return

        one_shot = meta.get("kind") == "function"
        try:
            raw_args = payload.get("args")
            if raw_args is not None and not isinstance(raw_args, list):
                raise ValueError(f"args must be a list, got {raw_args!r}.")
            raw_kwargs = payload.get("kwargs")
            if raw_kwargs is not None and not isinstance(raw_kwargs, dict):
                raise ValueError(f"kwargs must be a dict, got {raw_kwargs!r}.")
            args = raw_args or []
            kwargs = raw_kwargs or {}
            timeout = _validate_timeout(payload.get("timeout"), "timeout")
        except ValueError as exc:
            if one_shot:
                await self._reap_one_shot(name, clean=False)
            yield {"ok": False, "error": _format_exception_dict(exc)}
            return

        deadline = (time.monotonic() + float(timeout)) if timeout is not None else None
        target_name = meta.get("class_name") or name
        task_name = (
            target_name
            if (method_name == "__call__" or one_shot)
            else f"{target_name}.{method_name}"
        )
        stream_id = f"stream_{uuid.uuid4().hex[:12]}"
        handle = meta["handle"]

        async with self._track_call(meta):
            try:
                stream_method = handle.invoke_stream.options(
                    name=task_name,
                    _generator_backpressure_num_objects=16,
                )
            except (AttributeError, TypeError, NotImplementedError):
                stream_method = handle.invoke_stream
            gen = stream_method.remote(method_name, args, kwargs, stream_id)
            ait = gen.__aiter__()
            next_task: Optional[asyncio.Task] = None
            completed_normally = False
            try:
                while True:
                    if next_task is None:
                        next_task = asyncio.ensure_future(ait.__anext__())
                    wait_slice = 0.25
                    if deadline is not None:
                        rem = deadline - time.monotonic()
                        if rem <= 0:
                            raise asyncio.TimeoutError()
                        wait_slice = min(wait_slice, rem)
                    done, _ = await asyncio.wait({next_task}, timeout=wait_slice)
                    if not done:
                        if deadline is not None and time.monotonic() >= deadline:
                            raise asyncio.TimeoutError()
                        meta["last_active"] = time.time()
                        meta["last_active_mono"] = time.monotonic()
                        yield None  # SSE heartbeat comment to detect client disconnect
                        continue
                    try:
                        item_ref = next_task.result()
                    except StopAsyncIteration:
                        completed_normally = True
                        break
                    finally:
                        next_task = None
                    meta["last_active"] = time.time()
                    meta["last_active_mono"] = time.monotonic()
                    item = await item_ref
                    yield item
                if completed_normally:
                    yield {"ok": True, "done": True}
            except asyncio.TimeoutError:
                yield {
                    "ok": False,
                    "error": {
                        "exc_type": "TimeoutError",
                        "exc_message": (
                            f"Stream '{name}.{method_name}' timed out after {timeout}s."
                        ),
                        "timed_out": True,
                    },
                }
            except (asyncio.CancelledError, GeneratorExit):
                raise
            except Exception as exc:
                yield {"ok": False, "error": _format_exception_dict(exc)}
            finally:
                if next_task is not None and not next_task.done():
                    next_task.cancel()
                    with contextlib.suppress(BaseException):
                        await next_task
                if not completed_normally:
                    with contextlib.suppress(Exception):
                        handle.cancel_stream.remote(stream_id)
                    with contextlib.suppress(Exception):
                        ray.cancel(gen, force=False, recursive=True)
                if one_shot:
                    await self._reap_one_shot(name, clean=completed_normally)

    async def kill_actor(self, name: str) -> Optional[Dict[str, Any]]:
        async with self._lock:
            creating_handle = self._creating_handles.pop(name, None)
        killed_creating = False
        if creating_handle is not None:
            try:
                _destroy_actor_handle(creating_handle, reason="killed by ray_client")
                killed_creating = True
            except Exception:
                pass
        name_lock = await self._get_name_lock(name)
        try:
            async with name_lock:
                killed = await self._terminate_actor_for_recreate(
                    name,
                    CLIENT_ACTOR_NAMESPACE,
                    already_killed=killed_creating,
                )
        finally:
            async with self._lock:
                self._release_name_lock_ref_locked(name)
        if not killed and not killed_creating:
            return None
        return {"ok": True, "name": name, "killed": True}

    @staticmethod
    def _node_group_to_dict(g: Any) -> Dict[str, Any]:
        return {
            "name": getattr(g, "name", ""),
            "resources": dict(getattr(g, "resources", None) or {}),
            "labels": dict(getattr(g, "labels", None) or {}),
            "max_nodes": getattr(g, "max_nodes", None),
        }

    @staticmethod
    def _node_to_dict(n: Any) -> Dict[str, Any]:
        status_attr = getattr(n, "status", None)
        status_obj = status_attr() if callable(status_attr) else status_attr
        state = (
            getattr(status_obj, "state", status_obj)
            if status_obj is not None
            else getattr(n, "state", "ALIVE")
        )
        if hasattr(state, "value"):
            state = state.value
        death_reason = getattr(status_obj, "death_reason", None)
        if hasattr(death_reason, "value"):
            death_reason = death_reason.value
        return {
            "id": getattr(n, "id", ""),
            "group_name": getattr(n, "group_name", "default"),
            "resources": dict(getattr(n, "resources", None) or {}),
            "labels": dict(getattr(n, "labels", None) or {}),
            "detached": bool(getattr(n, "detached", True)),
            "state": str(state),
            "death_reason": death_reason,
            "termination_deadline": getattr(status_obj, "termination_deadline", None),
        }

    def _all_nodes(self, *, include_dead: bool = False) -> List[Dict[str, Any]]:
        cluster_mod = getattr(ray, "cluster", None)
        if cluster_mod is not None:
            node_state_cls = getattr(cluster_mod, "NodeState", None)
            if include_dead and node_state_cls is not None:
                raw_nodes = cluster_mod.list_nodes(states=list(node_state_cls))
            else:
                raw_nodes = cluster_mod.list_nodes()
            return [self._node_to_dict(n) for n in raw_nodes]

        nodes: List[Dict[str, Any]] = []
        for entry in ray.nodes():
            raw_res = entry.get("Resources") or {}
            labels = dict(entry.get("Labels") or {})
            is_head = "node:__internal_head__" in raw_res
            group = labels.get("ray.io/node-group") or (
                "head" if is_head else "default"
            )
            state = "ALIVE" if entry.get("Alive") else "DEAD"
            resources = {
                k: float(v) for k, v in raw_res.items() if not k.startswith("node:")
            }
            nid = entry.get("NodeID", "")
            nodes.append(
                {
                    "id": nid,
                    "group_name": group,
                    "state": state,
                    "ip_address": entry.get("NodeManagerAddress", ""),
                    "resources": resources,
                    "labels": labels,
                    "detached": True,
                }
            )
        return nodes

    def list_node_groups(self) -> List[Dict[str, Any]]:
        cluster_mod = getattr(ray, "cluster", None)
        if cluster_mod is not None:
            return [self._node_group_to_dict(g) for g in cluster_mod.list_node_groups()]
        groups: Dict[str, Dict[str, Any]] = {}
        for n in self._all_nodes():
            g_name = n["group_name"]
            if g_name not in groups or n["state"] == "ALIVE":
                groups[g_name] = {
                    "name": g_name,
                    "max_nodes": 1 if g_name == "head" else None,
                    "resources": dict(n["resources"]),
                    "labels": {
                        k: v
                        for k, v in n["labels"].items()
                        if k != ray._raylet.RAY_NODE_ID_KEY
                    },
                }
        if "default" not in groups:
            groups["default"] = {
                "name": "default",
                "max_nodes": None,
                "resources": {},
                "labels": {},
            }
        return list(groups.values())

    def get_node_group(self, name: str) -> Dict[str, Any]:
        cluster_mod = getattr(ray, "cluster", None)
        if cluster_mod is not None:
            return self._node_group_to_dict(cluster_mod.get_node_group(name))
        for g in self.list_node_groups():
            if g.get("name") == name:
                return g
        raise KeyError(f"Node group '{name}' does not exist.")

    _VALID_NODE_STATES = frozenset({"PENDING", "ALIVE", "TERMINATING_SOON", "DEAD"})

    def list_nodes(
        self,
        *,
        state: Optional[str] = None,
        node_group: Optional[str] = None,
        include_dead: bool = False,
    ) -> List[Dict[str, Any]]:
        if not isinstance(include_dead, bool):
            raise ValueError(f"include_dead must be a bool, got {include_dead!r}.")
        if node_group is not None:
            self.get_node_group(node_group)
        allowed: Optional[Set[str]] = None
        if state is not None:
            if not isinstance(state, str):
                raise ValueError(f"state must be a string, got {state!r}.")
            allowed = {s.strip().upper() for s in state.split(",") if s.strip()}
            if not allowed or not allowed.issubset(self._VALID_NODE_STATES):
                raise ValueError(
                    f"Invalid node state filter {state!r}; expected one or more of "
                    f"{sorted(self._VALID_NODE_STATES)}."
                )
        need_dead = include_dead or (allowed is not None and "DEAD" in allowed)
        nodes = self._all_nodes(include_dead=need_dead)
        if node_group is not None:
            nodes = [n for n in nodes if n["group_name"] == node_group]
        if allowed is not None:
            nodes = [n for n in nodes if n["state"].upper() in allowed]
        elif not include_dead:
            nodes = [n for n in nodes if n["state"] != "DEAD"]
        return nodes

    def get_node(self, node_id: str) -> Dict[str, Any]:
        cluster_mod = getattr(ray, "cluster", None)
        if cluster_mod is not None:
            return self._node_to_dict(cluster_mod.get_node(node_id))
        for n in self._all_nodes(include_dead=True):
            if n.get("id") == node_id:
                return n
        raise KeyError(f"Node '{node_id}' does not exist.")

    def add_nodes(
        self,
        group: str,
        count: int = 1,
        *,
        wait_ready: bool = False,
        timeout_s: Optional[float] = None,
    ) -> List[Dict[str, Any]]:
        if not isinstance(group, str) or not group.strip():
            raise ValueError("group must be a non-empty string.")
        if isinstance(count, bool) or not isinstance(count, int) or count < 1:
            raise ValueError(f"count must be a positive integer, got {count!r}.")
        if not isinstance(wait_ready, bool):
            raise ValueError(f"wait_ready must be a bool, got {wait_ready!r}.")
        _validate_timeout(timeout_s, "timeout_s")
        grp = self.get_node_group(group)
        if group == "head":
            raise MaxNodesExceededError(
                "Cannot add nodes to the 'head' node group (max_nodes=1)."
            )
        max_nodes = grp.get("max_nodes")
        if max_nodes is not None:
            current_count = len(self.list_nodes(node_group=group))
            if current_count + count > int(max_nodes):
                raise MaxNodesExceededError(
                    f"Adding {count} node(s) to group '{group}' ({current_count} active) "
                    f"would exceed max_nodes={max_nodes}."
                )
        cluster_mod = getattr(ray, "cluster", None)
        if cluster_mod is None:
            raise NotImplementedError(
                "Cluster node provisioning requires Ray 3.0 (ray.cluster)."
            )
        ng_obj = cluster_mod.get_node_group(group)
        added_nodes = ng_obj.add_nodes(count, detached=True)
        if wait_ready:
            deadline = (
                (time.monotonic() + float(timeout_s)) if timeout_s is not None else None
            )
            try:
                for n in added_nodes:
                    rem = (
                        max(0.0, deadline - time.monotonic())
                        if deadline is not None
                        else None
                    )
                    n.ready().result(timeout=rem)
            except BaseException:
                for n in added_nodes:
                    with contextlib.suppress(Exception):
                        n.destroy(reason="add_nodes wait_ready failed or timed out")
                raise
        return [self._node_to_dict(n) for n in added_nodes]

    def destroy_node(
        self,
        node_id: str,
        reason: str,
        *,
        grace_period_s: Optional[float] = None,
    ) -> Dict[str, Any]:
        if not isinstance(node_id, str) or not node_id.strip():
            raise ValueError("node_id must be a non-empty string.")
        if not isinstance(reason, str) or not reason.strip():
            raise ValueError("reason must be a non-empty string.")
        if grace_period_s is not None and (
            isinstance(grace_period_s, bool)
            or not isinstance(grace_period_s, (int, float))
            or not math.isfinite(grace_period_s)
            or grace_period_s < 0
        ):
            raise ValueError(
                f"grace_period_s must be a non-negative finite number, got {grace_period_s!r}."
            )
        node = self.get_node(node_id)
        if node.get("group_name") == "head":
            raise ValueError("Cannot destroy the head node.")
        cluster_mod = getattr(ray, "cluster", None)
        if cluster_mod is None:
            raise NotImplementedError(
                "Cluster node destruction requires Ray 3.0 (ray.cluster)."
            )
        node_obj = cluster_mod.get_node(node_id)
        destroy_kwargs: Dict[str, Any] = {"reason": reason}
        if grace_period_s is not None:
            destroy_kwargs["grace_period_s"] = float(grace_period_s)
        node_obj.destroy(**destroy_kwargs)
        return {
            **self._node_to_dict(node_obj),
            "ok": True,
            "destroyed": True,
        }


with contextlib.suppress(Exception):
    ray.cloudpickle.register_pickle_by_value(sys.modules[__name__])
