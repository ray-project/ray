import asyncio
import collections
import contextlib
import importlib
import inspect
import io
import logging
import math
import os
import sys
import threading
import time
import traceback
import types
import uuid
from typing import Any, Deque, Dict, List, Optional, Set

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
        raise ValueError(
            f"ttl_seconds must be a finite number or None, got {ttl_seconds!r}."
        )
    val = float(ttl_seconds)
    if not math.isfinite(val) or val < 0:
        raise ValueError(
            f"ttl_seconds must be a non-negative finite number or None, got {ttl_seconds!r}."
        )
    return val


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
        if not isinstance(s, str):
            s = str(s)
        try:
            self._stream.write(s)
        except Exception:
            pass
        if s:
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


async def _await_coro_catching_system_exit(coro: Any) -> Any:
    try:
        return False, await coro
    except SystemExit as exc:
        return True, exc.code


async def _anext_catching_system_exit(agen: Any) -> Any:
    try:
        return "item", await agen.__anext__()
    except StopAsyncIteration:
        return "stop", None
    except SystemExit as exc:
        return "exit", exc.code


class _ClientRootActor:
    """Top-level Ray Actor hosting a user class or driver function.

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
        self._ephemeral: bool = bool(spec.get("ephemeral", self._kind == "function"))
        self._created_at: float = time.time()
        self._last_active: float = time.time()
        self._last_active_mono: float = time.monotonic()
        self._active_calls: int = 0
        self._state_lock = threading.Lock()
        self._cancelled_streams: Set[str] = set()
        self._placement_groups: List[Any] = []
        self._shutting_down = False

        self._log_buffer: Deque[str] = collections.deque()
        self._log_lock = threading.Lock()
        sys.stdout = _LogTeeStream(sys.__stdout__, self._log_buffer, self._log_lock)
        sys.stderr = _LogTeeStream(sys.__stderr__, self._log_buffer, self._log_lock)

        self._async_loop: Optional[asyncio.AbstractEventLoop] = None
        self._async_thread: Optional[threading.Thread] = None
        self._async_lock = threading.Lock()

        self._install_ray_hooks()

        target = _load_user_target(
            self._name,
            self._target_name,
            spec.get("module_source"),
            spec.get("module"),
            spec.get("captured_globals") or {},
        )
        self._generator_methods: Set[str] = set()
        if self._kind == "function":
            self._fn = target
            self._instance = None
            unwrapped_fn = inspect.unwrap(target)
            if inspect.isgeneratorfunction(unwrapped_fn) or inspect.isasyncgenfunction(
                unwrapped_fn
            ):
                self._generator_methods = {"__call__", self._target_name}
        else:
            raw_cls = getattr(target, "__ray_actor_class__", None)
            if raw_cls is None and hasattr(target, "__ray_metadata__"):
                mod_cls = getattr(target.__ray_metadata__, "modified_class", None)
                raw_cls = getattr(mod_cls, "__ray_actor_class__", target)
            cls_to_instantiate = raw_cls if inspect.isclass(raw_cls) else target
            self._fn = None
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
                    if isinstance(static_attr, property) or inspect.isdatadescriptor(
                        static_attr
                    ):
                        continue
                    member = getattr(self._instance, attr_name)
                    unwrapped = inspect.unwrap(member)
                    if inspect.isgeneratorfunction(
                        unwrapped
                    ) or inspect.isasyncgenfunction(unwrapped):
                        self._generator_methods.add(attr_name)

        self._reaper_thread = threading.Thread(
            target=self._self_reap_loop,
            name=f"ray-client-ttl-{self._name}",
            daemon=True,
        )
        self._reaper_thread.start()

    def _install_ray_hooks(self) -> None:
        ns = self._namespace
        # Fate-shared actors created in this process default to a namespace of their
        # own, so concurrent roots that create identically named children don't
        # collide. Detached actors outlive the root, so they default to `ns`, where
        # other roots can find them by name. Lookups without a namespace fall back
        # to `ns`, which also holds the top-level client actors. The hooks are
        # process-local: nested tasks and actors run in other workers and use the
        # dashboard agent job's namespace.
        child_ns = f"{ns}_{self._name}"
        pg_list = self._placement_groups
        state_lock = self._state_lock
        os.environ["RAY_NAMESPACE"] = child_ns

        _orig_get_actor = ray.get_actor
        _orig_actor_remote = ActorClass._remote
        _orig_pg = ray.util.placement_group

        def _client_get_actor(name: str, namespace: Optional[str] = None):
            if namespace is not None:
                return _orig_get_actor(name, namespace=namespace)
            try:
                return _orig_get_actor(name, namespace=child_ns)
            except ValueError:
                return _orig_get_actor(name, namespace=ns)

        def _client_actor_remote(actor_cls, args=None, kwargs=None, **actor_options):
            if actor_options.get("namespace") is None:
                detached = actor_options.get("lifetime") == "detached"
                actor_options["namespace"] = ns if detached else child_ns
            return _orig_actor_remote(
                actor_cls, args=args, kwargs=kwargs, **actor_options
            )

        def _client_placement_group(*args, **kwargs):
            pg = _orig_pg(*args, **kwargs)
            if kwargs.get("lifetime") != "detached":
                with state_lock:
                    pg_list.append(pg)
            return pg

        ray.init = lambda *a, **kw: None
        ray.shutdown = lambda *a, **kw: None
        ray.get_actor = _client_get_actor
        ray._private.worker.get_actor = _client_get_actor
        ActorClass._remote = _client_actor_remote
        ray.util.placement_group = _client_placement_group
        RuntimeContext.namespace = property(lambda _self: child_ns)

    def _self_reap_loop(self) -> None:
        while not self._shutting_down:
            time.sleep(0.2)
            with self._state_lock:
                if self._shutting_down:
                    return
                ttl = self._ttl_seconds
                active = self._active_calls
                elapsed = time.monotonic() - self._last_active_mono
            if ttl is not None and ttl > 0 and active == 0 and elapsed >= ttl:
                self._cleanup_resources()
                with contextlib.suppress(Exception):
                    ray.actor.exit_actor()
                return

    def _cleanup_resources(self) -> None:
        with self._state_lock:
            self._shutting_down = True
            pgs = list(self._placement_groups)
            self._placement_groups.clear()
        for pg in pgs:
            with contextlib.suppress(Exception):
                ray.util.remove_placement_group(pg)
        if self._async_loop is not None:
            with contextlib.suppress(Exception):
                self._async_loop.call_soon_threadsafe(self._async_loop.stop)

    def _ensure_async_loop(self) -> asyncio.AbstractEventLoop:
        with self._async_lock:
            if self._async_loop is None or not self._async_loop.is_running():
                loop = asyncio.new_event_loop()
                thread = threading.Thread(
                    target=loop.run_forever,
                    name=f"ray-client-async-{self._name}",
                    daemon=True,
                )
                thread.start()
                self._async_loop = loop
                self._async_thread = thread
            return self._async_loop

    def _begin_call(self) -> None:
        with self._state_lock:
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
        if not hasattr(self._instance, method_name):
            raise AttributeError(
                f"Actor '{self._name}' ({self._target_name}) has no method '{method_name}'."
            )
        return getattr(self._instance, method_name)

    @ray.method(concurrency_group="_control")
    def get_info(self) -> Dict[str, Any]:
        with self._state_lock:
            active_calls = self._active_calls
            last_active = self._last_active
            elapsed = time.monotonic() - self._last_active_mono
            ttl = self._ttl_seconds
        remaining_ttl: Optional[float] = None
        if ttl is not None and ttl > 0:
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
            "ephemeral": self._ephemeral,
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
            self._cancelled_streams.add(stream_id)
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
                if inspect.iscoroutinefunction(target):
                    fut = asyncio.run_coroutine_threadsafe(
                        _await_coro_catching_system_exit(target(*args, **kwargs)),
                        self._ensure_async_loop(),
                    )
                    is_exit, val = fut.result()
                    if is_exit:
                        raise SystemExit(val)
                    res = val
                else:
                    res = target(*args, **kwargs)
                    if inspect.iscoroutine(res):
                        fut = asyncio.run_coroutine_threadsafe(
                            _await_coro_catching_system_exit(res),
                            self._ensure_async_loop(),
                        )
                        is_exit, val = fut.result()
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
        except Exception as exc:
            return {
                "ok": False,
                "error": _format_exception_dict(exc),
                "logs": self._get_logs_text(),
            }
        finally:
            self._end_call()

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
                while True:
                    with self._state_lock:
                        if stream_id in self._cancelled_streams:
                            break
                    fut = asyncio.run_coroutine_threadsafe(
                        _anext_catching_system_exit(gen), loop
                    )
                    tag, item = fut.result()
                    if tag == "stop":
                        break
                    if tag == "exit":
                        if item in (0, None):
                            break
                        raise RuntimeError(f"__RAY_SYSTEM_EXIT__:{item}")
                    with self._state_lock:
                        self._last_active = time.time()
                        self._last_active_mono = time.monotonic()
                        if stream_id in self._cancelled_streams:
                            break
                    _validate_strict_json(item, label)
                    yield {"ok": True, "item": item}
            elif inspect.isgenerator(gen):
                while True:
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
                        if stream_id in self._cancelled_streams:
                            break
                    _validate_strict_json(item, label)
                    yield {"ok": True, "item": item}
                    with self._state_lock:
                        if stream_id in self._cancelled_streams:
                            break
            else:
                raise TypeError(
                    f"'{self._target_name}.{method_name}' is not a generator function."
                )
        except GeneratorExit:
            pass
        except Exception as exc:
            with self._state_lock:
                cancelled = stream_id in self._cancelled_streams
            if not cancelled:
                yield {
                    "ok": False,
                    "error": _format_exception_dict(exc),
                    "logs": self._get_logs_text(),
                }
        finally:
            with self._state_lock:
                self._cancelled_streams.discard(stream_id)
            if gen is not None:
                with contextlib.suppress(Exception):
                    if inspect.isasyncgen(gen) and self._async_loop is not None:
                        asyncio.run_coroutine_threadsafe(
                            gen.aclose(), self._async_loop
                        ).result(timeout=2.0)
                    elif hasattr(gen, "close"):
                        gen.close()
            self._end_call()

    @ray.method(concurrency_group="_control")
    def shutdown(self) -> Dict[str, Any]:
        self._cleanup_resources()
        return {"ok": True}


_RemoteClientRootActor = ray.remote(concurrency_groups={"_control": 4})(
    _ClientRootActor
)


class ActorManager:
    """Dashboard agent manager for `/api/actors` top-level Ray Actors."""

    def __init__(self) -> None:
        self._actors: Dict[str, Dict[str, Any]] = {}
        self._lock = asyncio.Lock()
        self._name_locks: Dict[str, asyncio.Lock] = {}
        self._reaper_task: Optional[asyncio.Task] = None

    async def _get_name_lock(self, name: str) -> asyncio.Lock:
        async with self._lock:
            lock = self._name_locks.get(name)
            if lock is None:
                lock = self._name_locks[name] = asyncio.Lock()
            return lock

    def _ensure_reaper(self) -> None:
        if self._reaper_task is None or self._reaper_task.done():
            with contextlib.suppress(RuntimeError):
                loop = asyncio.get_running_loop()
                self._reaper_task = loop.create_task(self._ttl_reaper_loop())

    async def _ttl_reaper_loop(self) -> None:
        while True:
            await asyncio.sleep(0.5)
            now_mono = time.monotonic()
            to_reap: List[str] = []
            async with self._lock:
                for name, meta in list(self._actors.items()):
                    ttl = meta.get("ttl_seconds")
                    if (
                        isinstance(ttl, (int, float))
                        and ttl > 0
                        and meta.get("active_calls", 0) == 0
                        and (now_mono - meta.get("last_active_mono", now_mono)) >= ttl
                    ):
                        to_reap.append(name)
            for name in to_reap:
                try:
                    info = await self.get_actor_info(name)
                    if info is None:
                        continue
                    rem = info.get("remaining_ttl_seconds")
                    if (
                        info.get("active_calls", 0) == 0
                        and rem is not None
                        and rem <= 0
                    ):
                        logger.info("Actor '%s' idle TTL expired; terminating.", name)
                        await self.kill_actor(name, no_restart=True)
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

    async def _reap_ephemeral(self, name: str) -> None:
        # Run the kill as its own task so that a client disconnect, which cancels
        # the request awaiting it, can't interrupt the cleanup halfway.
        with contextlib.suppress(Exception):
            await asyncio.shield(
                run_background_task(self.kill_actor(name, no_restart=True))
            )

    async def _lookup_actor(self, name: str) -> Optional[Dict[str, Any]]:
        self._ensure_reaper()
        async with self._lock:
            meta = self._actors.get(name)
        if meta is not None:
            return meta
        try:
            handle = ray.get_actor(name, namespace=CLIENT_ACTOR_NAMESPACE)
            info = await asyncio.wait_for(
                handle.get_info.remote(), timeout=_CONTROL_TIMEOUT_S
            )
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
                "namespace": info.get("namespace", CLIENT_ACTOR_NAMESPACE),
                "ephemeral": bool(info.get("ephemeral", False)),
                "ttl_seconds": ttl,
                "created_at": info.get("created_at", time.time()),
                "last_active": info.get("last_active", time.time()),
                "last_active_mono": now_mono - elapsed,
                "active_calls": 0,
                "generator_methods": set(info.get("generator_methods") or []),
            }
            async with self._lock:
                self._actors[name] = meta
            return meta
        except Exception:
            return None

    async def create_actor(self, spec: Dict[str, Any]) -> Dict[str, Any]:
        self._ensure_reaper()
        kind = spec.get("kind") or ("function" if "fn_name" in spec else "class")
        target_name = (
            spec.get("fn_name")
            if kind == "function"
            else (spec.get("class_name") or spec.get("name"))
        )
        if not target_name:
            return {
                "ok": False,
                "error": {
                    "exc_type": "ValueError",
                    "exc_message": "Actor spec must specify 'class_name' or 'fn_name'.",
                },
            }
        try:
            ttl_seconds = _validate_ttl_seconds(
                spec["ttl_seconds"] if "ttl_seconds" in spec else 1800.0
            )
        except ValueError as exc:
            return {"ok": False, "error": _format_exception_dict(exc)}

        name = spec.get("name") or f"{target_name}_{uuid.uuid4().hex[:8]}"
        # Driver functions are single-use: the call that runs them also kills them.
        ephemeral = bool(spec.get("ephemeral", kind == "function"))
        spec = dict(
            spec, name=name, kind=kind, ttl_seconds=ttl_seconds, ephemeral=ephemeral
        )
        namespace = spec.get("namespace") or CLIENT_ACTOR_NAMESPACE
        get_if_exists = bool(spec.get("get_if_exists", True))

        name_lock = await self._get_name_lock(name)
        async with name_lock:
            if get_if_exists:
                existing = await self._lookup_actor(name)
                if existing is not None:
                    try:
                        info = await asyncio.wait_for(
                            existing["handle"].get_info.remote(),
                            timeout=_CONTROL_TIMEOUT_S,
                        )
                        existing_cls = info.get("class_name") or existing.get(
                            "class_name"
                        )
                        if (
                            spec.get("class_name")
                            and existing_cls
                            and existing_cls != target_name
                        ):
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
                        if "ttl_seconds" in spec:
                            info = await asyncio.wait_for(
                                existing["handle"].set_ttl.remote(ttl_seconds),
                                timeout=_CONTROL_TIMEOUT_S,
                            )
                            existing["ttl_seconds"] = ttl_seconds
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
                        }
                    except Exception:
                        async with self._lock:
                            self._actors.pop(name, None)

            handle = None
            try:
                num_cpus = spec.get("num_cpus")
                if num_cpus is None:
                    num_cpus = 0 if kind == "function" else 1
                max_concurrency = (
                    spec["max_concurrency"]
                    if spec.get("max_concurrency") is not None
                    else 10
                )
                options: Dict[str, Any] = {
                    "name": name,
                    "namespace": namespace,
                    "lifetime": "detached",
                    "num_cpus": num_cpus,
                    "max_concurrency": max_concurrency,
                }
                if spec.get("runtime_env"):
                    options["runtime_env"] = spec["runtime_env"]
                placement_keys = ("num_gpus", "memory", "resources", "label_selector")
                for opt_key in placement_keys:
                    if spec.get(opt_key) is not None:
                        options[opt_key] = spec[opt_key]
                if (
                    num_cpus == 0
                    and not any(spec.get(k) for k in placement_keys)
                    and os.environ.get(RAY_JOB_ALLOW_DRIVER_ON_WORKER_NODES_ENV_VAR)
                    != "1"
                ):
                    # Like a Ray Jobs driver, a root that requests no resources runs
                    # on the head node, where this agent runs. It then starts even on
                    # a `num-cpus=0` head and never takes a CPU from worker nodes.
                    options["label_selector"] = {
                        ray._raylet.RAY_NODE_ID_KEY: ray.get_runtime_context().get_node_id()
                    }

                handle = _RemoteClientRootActor.options(**options).remote(spec)
                info = await handle.get_info.remote()
                now = time.time()
                now_mono = time.monotonic()
                async with self._lock:
                    self._actors[name] = {
                        "name": name,
                        "handle": handle,
                        "class_name": target_name,
                        "kind": kind,
                        "namespace": namespace,
                        "ephemeral": ephemeral,
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
                }
            except Exception as exc:
                if handle is not None:
                    with contextlib.suppress(Exception):
                        ray.kill(handle, no_restart=True)
                return {"ok": False, "error": _format_exception_dict(exc)}

    async def list_actors(self) -> List[Dict[str, Any]]:
        self._ensure_reaper()
        with contextlib.suppress(Exception):
            named = ray.util.list_named_actors(all_namespaces=True)
            for entry in named:
                if (
                    isinstance(entry, dict)
                    and entry.get("namespace") == CLIENT_ACTOR_NAMESPACE
                    and entry.get("name") not in self._actors
                ):
                    await self._lookup_actor(entry["name"])

        async with self._lock:
            snapshot = list(self._actors.items())

        async def _query_one(
            name: str, meta: Dict[str, Any]
        ) -> Optional[Dict[str, Any]]:
            try:
                info = await asyncio.wait_for(
                    meta["handle"].get_info.remote(), timeout=_CONTROL_TIMEOUT_S
                )
                info["active_calls"] = max(
                    info.get("active_calls", 0), meta.get("active_calls", 0)
                )
                return info
            except RayActorError:
                async with self._lock:
                    self._actors.pop(name, None)
                return None
            except Exception:
                return {
                    "ok": True,
                    "name": name,
                    "class_name": meta.get("class_name", name),
                    "kind": meta.get("kind", "class"),
                    "namespace": meta.get("namespace", CLIENT_ACTOR_NAMESPACE),
                    "status": "ALIVE",
                    "active_calls": meta.get("active_calls", 0),
                    "ttl_seconds": meta.get("ttl_seconds"),
                    "created_at": meta.get("created_at"),
                    "last_active": meta.get("last_active"),
                }

        results = await asyncio.gather(*(_query_one(n, m) for n, m in snapshot))
        return [r for r in results if r is not None]

    async def get_actor_info(self, name: str) -> Optional[Dict[str, Any]]:
        meta = await self._lookup_actor(name)
        if meta is None:
            return None
        try:
            info = await asyncio.wait_for(
                meta["handle"].get_info.remote(), timeout=_CONTROL_TIMEOUT_S
            )
            info["active_calls"] = max(
                info.get("active_calls", 0), meta.get("active_calls", 0)
            )
            return info
        except RayActorError:
            async with self._lock:
                self._actors.pop(name, None)
            return None
        except Exception:
            return None

    async def get_actor_logs(self, name: str) -> Optional[Dict[str, Any]]:
        meta = await self._lookup_actor(name)
        if meta is None:
            return None
        try:
            return await asyncio.wait_for(
                meta["handle"].get_logs.remote(), timeout=_CONTROL_TIMEOUT_S
            )
        except RayActorError:
            async with self._lock:
                self._actors.pop(name, None)
            return None
        except Exception:
            return None

    async def call_actor_method(
        self,
        name: str,
        method_name: str,
        payload: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        """Calls `method_name` on actor `name` and returns the result payload.

        Returns None if the actor doesn't exist. Every other failure is returned as
        an `{"ok": False, "error": ...}` payload, so only cancellation propagates.
        """
        meta = await self._lookup_actor(name)
        if meta is None:
            return None
        args = payload.get("args") or []
        kwargs = payload.get("kwargs") or {}
        timeout = payload.get("timeout")
        ephemeral = bool(meta.get("ephemeral") or payload.get("ephemeral"))

        async with self._track_call(meta):
            try:
                obj_ref = meta["handle"].invoke.remote(method_name, args, kwargs)
                if timeout is not None:
                    return await asyncio.wait_for(obj_ref, timeout=float(timeout))
                return await obj_ref
            except asyncio.TimeoutError:
                with contextlib.suppress(Exception):
                    ray.cancel(obj_ref, recursive=False)
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
                    ray.cancel(obj_ref, recursive=False)
                raise
            except Exception as exc:
                return {"ok": False, "error": _format_exception_dict(exc)}
            finally:
                if ephemeral:
                    await self._reap_ephemeral(name)

    async def stream_actor_method(
        self,
        name: str,
        method_name: str,
        payload: Dict[str, Any],
    ):
        meta = await self._lookup_actor(name)
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

        args = payload.get("args") or []
        kwargs = payload.get("kwargs") or {}
        timeout = payload.get("timeout")
        deadline = (time.monotonic() + float(timeout)) if timeout is not None else None
        ephemeral = bool(meta.get("ephemeral") or payload.get("ephemeral"))
        stream_id = f"stream_{uuid.uuid4().hex[:12]}"
        handle = meta["handle"]

        async with self._track_call(meta):
            gen = handle.invoke_stream.options(
                _generator_backpressure_num_objects=16
            ).remote(method_name, args, kwargs, stream_id)
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
                if not completed_normally:
                    with contextlib.suppress(Exception):
                        handle.cancel_stream.remote(stream_id)
                    with contextlib.suppress(Exception):
                        ray.cancel(gen, recursive=False)
                if ephemeral:
                    await self._reap_ephemeral(name)

    async def kill_actor(
        self, name: str, no_restart: bool = True
    ) -> Optional[Dict[str, Any]]:
        async with self._lock:
            meta = self._actors.pop(name, None)
            self._name_locks.pop(name, None)
        handle = meta["handle"] if meta is not None else None
        if handle is None:
            try:
                handle = ray.get_actor(name, namespace=CLIENT_ACTOR_NAMESPACE)
            except ValueError:
                return None
        with contextlib.suppress(Exception):
            await asyncio.wait_for(handle.shutdown.remote(), timeout=2.0)
        try:
            ray.kill(handle, no_restart=no_restart)
        except ValueError:
            return None
        except Exception:
            pass
        return {"ok": True, "name": name, "killed": True}


with contextlib.suppress(Exception):
    ray.cloudpickle.register_pickle_by_value(sys.modules[__name__])
