import asyncio
import contextlib
import importlib
import inspect
import logging
import math
import os
import sys
import time
import traceback
import types
import uuid
from typing import Any, Dict, List, Optional

import ray
import ray._private.worker
from ray.actor import ActorClass, ActorHandle
from ray.exceptions import RayTaskError
from ray.runtime_context import RuntimeContext

logger = logging.getLogger(__name__)

WORKSPACE_NAMESPACE_PREFIX = "workspace:"
WORKSPACE_SUPERVISOR_PREFIX = "_ray_client_workspace_supervisor_"


def workspace_namespace(workspace: str) -> str:
    return f"{WORKSPACE_NAMESPACE_PREFIX}{workspace}"


def workspace_supervisor_name(workspace: str) -> str:
    return f"{WORKSPACE_SUPERVISOR_PREFIX}{workspace}"


def _validate_strict_json(value: Any, path: str = "Value") -> None:
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
        for idx, item in enumerate(value):
            _validate_strict_json(item, f"{path}[{idx}]")
        return
    if val_type is dict:
        for k, v in value.items():
            if type(k) is not str:
                raise TypeError(
                    f"{path} has non-string dict key {k!r} of type "
                    f"{type(k).__name__}; JSON object keys must be exact str."
                )
            _validate_strict_json(v, f"{path}[{k!r}]")
        return
    raise TypeError(
        f"{path} contains value of type '{val_type.__name__}', which is not "
        "round-trip JSON serializable. Use return_ref=True to keep non-JSON "
        "objects in the cluster object store as a RemoteRef."
    )


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


@contextlib.contextmanager
def _workspace_execution_context(ws_namespace: str):
    """Configures ray.init, ray.get_actor, ActorClass.options, and RuntimeContext.namespace for a workspace."""
    _orig_init = ray.init
    _orig_get_actor = ray.get_actor
    _orig_worker_get_actor = ray._private.worker.get_actor
    _orig_actor_remote = ActorClass._remote
    _orig_ns_prop = RuntimeContext.namespace

    def _ws_get_actor(name: str, namespace: Optional[str] = None):
        return _orig_get_actor(name, namespace=namespace or ws_namespace)

    def _ws_actor_remote(self, args=None, kwargs=None, **actor_options):
        if actor_options.get("namespace") is None:
            actor_options["namespace"] = ws_namespace
        return _orig_actor_remote(self, args=args, kwargs=kwargs, **actor_options)

    ray.init = lambda *a, **kw: None
    ray.get_actor = _ws_get_actor
    ray._private.worker.get_actor = _ws_get_actor
    ActorClass._remote = _ws_actor_remote
    RuntimeContext.namespace = property(lambda _self: ws_namespace)
    try:
        yield
    finally:
        ray.init = _orig_init
        ray.get_actor = _orig_get_actor
        ray._private.worker.get_actor = _orig_worker_get_actor
        ActorClass._remote = _orig_actor_remote
        RuntimeContext.namespace = _orig_ns_prop


def _load_user_callable(
    prefix: str,
    ws_name: str,
    target_name: str,
    module_source: Optional[str],
    module_path: Optional[str],
    captured_globals: Dict[str, Any],
) -> Any:
    if module_source is not None:
        mod_name = f"_ray_ws_{prefix}_{ws_name}_{target_name}_{uuid.uuid4().hex[:6]}"
        module = types.ModuleType(mod_name)
        module.__file__ = f"<{mod_name}>"
        sys.modules[mod_name] = module
        module.__dict__.update(captured_globals)
        exec(compile(module_source, module.__file__, "exec"), module.__dict__)
        try:
            ray.cloudpickle.register_pickle_by_value(module)
        except Exception:
            pass
        return getattr(module, target_name)
    if module_path is not None:
        cwd = os.getcwd()
        if cwd not in sys.path:
            sys.path.insert(0, cwd)
        importlib.invalidate_caches()
        module = importlib.import_module(module_path)
        return getattr(module, target_name)
    raise ValueError(
        f"Spec for '{target_name}' must provide either 'module_source' or 'module'."
    )


@ray.remote(num_cpus=0)
class WorkspaceSupervisor:
    """In-cluster supervisor actor managing tasks, live actors, ObjectRefs, and TTL for a workspace."""

    def __init__(
        self,
        workspace: str,
        ttl_seconds: Optional[float] = 1800.0,
        runtime_env: Optional[Dict[str, Any]] = None,
    ):
        self._workspace = workspace
        self._namespace = workspace_namespace(workspace)
        self._ttl_seconds = ttl_seconds
        self._runtime_env = runtime_env or {}
        self._created_at = time.time()
        self._last_active = time.time()
        self._actors: Dict[str, Dict[str, Any]] = {}
        self._refs: Dict[str, ray.ObjectRef] = {}
        self._closed = False
        self._ttl_task = asyncio.create_task(self._ttl_monitor_loop())

    def _touch(self) -> float:
        self._last_active = time.time()
        return self._last_active

    async def _ttl_monitor_loop(self) -> None:
        while not self._closed:
            interval = 1.0
            if self._ttl_seconds is not None and self._ttl_seconds > 0:
                interval = max(0.05, min(1.0, self._ttl_seconds / 4.0))
            await asyncio.sleep(interval)
            if self._ttl_seconds is not None and self._ttl_seconds > 0:
                idle = time.time() - self._last_active
                if idle >= self._ttl_seconds:
                    logger.info(
                        "Workspace '%s' idle for %.1fs (TTL=%.1fs); cleaning up.",
                        self._workspace,
                        idle,
                        self._ttl_seconds,
                    )
                    await self.cleanup(kill_detached=False)
                    ray.actor.exit_actor()
                    return

    async def ping(
        self,
        ttl_seconds: Optional[float] = None,
        update_ttl: bool = False,
    ) -> Dict[str, Any]:
        self._touch()
        if update_ttl:
            self._ttl_seconds = ttl_seconds
        return await self.get_info()

    async def get_info(self) -> Dict[str, Any]:
        self._touch()
        actors_info = []
        for name, meta in list(self._actors.items()):
            actors_info.append(
                {
                    "name": name,
                    "class_name": meta["class_name"],
                    "lifetime": meta["lifetime"],
                    "created_at": meta["created_at"],
                }
            )
        return {
            "workspace": self._workspace,
            "namespace": self._namespace,
            "ttl_seconds": self._ttl_seconds,
            "created_at": self._created_at,
            "last_active": self._last_active,
            "actors": actors_info,
            "num_refs": len(self._refs),
            "runtime_env": self._runtime_env,
        }

    async def _resolve_arg(self, value: Any, top_level: bool = False) -> Any:
        if isinstance(value, dict) and set(value.keys()) == {"$ray_ref"}:
            ref_id = value["$ray_ref"]
            if ref_id not in self._refs:
                raise KeyError(
                    f"ObjectRef '{ref_id}' not found in workspace '{self._workspace}'."
                )
            obj_ref = self._refs[ref_id]
            return obj_ref if top_level else await obj_ref
        if isinstance(value, list):
            return [await self._resolve_arg(v, top_level=False) for v in value]
        if isinstance(value, dict):
            return {
                k: await self._resolve_arg(v, top_level=False) for k, v in value.items()
            }
        return value

    def _merge_runtime_env(
        self, override_env: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        merged = dict(self._runtime_env)
        if override_env:
            for k, v in override_env.items():
                if k == "env_vars" and isinstance(v, dict):
                    env_vars = dict(merged.get("env_vars") or {})
                    env_vars.update(v)
                    merged["env_vars"] = env_vars
                else:
                    merged[k] = v
        return merged

    async def _await_obj_ref(
        self, obj_ref: ray.ObjectRef, timeout: Optional[float] = None
    ) -> Any:
        if timeout is not None:
            try:
                return await asyncio.wait_for(
                    asyncio.shield(obj_ref), timeout=float(timeout)
                )
            except asyncio.TimeoutError:
                raise TimeoutError(
                    f"Operation timed out after {timeout}s in workspace '{self._workspace}'."
                ) from None
        return await obj_ref

    def _build_remote_task(self, spec: Dict[str, Any]):
        fn_name = spec["fn_name"]
        module_source = spec.get("module_source")
        module_path = spec.get("module")
        if module_source is None and module_path is None:
            raise ValueError(
                f"Task spec for '{fn_name}' must provide either 'module_source' or 'module'."
            )
        captured_globals = spec.get("captured_globals") or {}
        ws_name = self._workspace
        ws_namespace = self._namespace

        def _task_entry(*_args: Any, **_kwargs: Any) -> Any:
            with _workspace_execution_context(ws_namespace):
                try:
                    user_fn = _load_user_callable(
                        "task",
                        ws_name,
                        fn_name,
                        module_source,
                        module_path,
                        captured_globals,
                    )
                    res = user_fn(*_args, **_kwargs)
                    if inspect.iscoroutine(res):
                        res = asyncio.run(res)
                    return res
                except SystemExit as exc:
                    if exc.code in (0, None):
                        return None
                    raise RuntimeError(f"__RAY_SYSTEM_EXIT__:{exc.code}") from None

        _task_entry.__name__ = fn_name
        _task_entry.__qualname__ = fn_name

        options: Dict[str, Any] = {
            "num_cpus": (spec["num_cpus"] if spec.get("num_cpus") is not None else 0),
        }
        task_env = self._merge_runtime_env(spec.get("runtime_env"))
        if task_env:
            options["runtime_env"] = task_env
        for opt_key in ("num_gpus", "memory", "resources", "label_selector"):
            if spec.get(opt_key) is not None:
                options[opt_key] = spec[opt_key]
        return fn_name, ray.remote(_task_entry).options(**options)

    def _build_remote_generator_task(self, spec: Dict[str, Any]):
        fn_name = spec["fn_name"]
        module_source = spec.get("module_source")
        module_path = spec.get("module")
        if module_source is None and module_path is None:
            raise ValueError(
                f"Task spec for '{fn_name}' must provide either 'module_source' or 'module'."
            )
        captured_globals = spec.get("captured_globals") or {}
        ws_name = self._workspace
        ws_namespace = self._namespace

        def _gen_entry(*_args: Any, **_kwargs: Any):
            with _workspace_execution_context(ws_namespace):
                user_fn = _load_user_callable(
                    "stream",
                    ws_name,
                    fn_name,
                    module_source,
                    module_path,
                    captured_globals,
                )
                gen = user_fn(*_args, **_kwargs)
                if inspect.isasyncgen(gen):
                    loop = asyncio.new_event_loop()
                    try:
                        while True:
                            try:
                                yield loop.run_until_complete(gen.__anext__())
                            except StopAsyncIteration:
                                break
                    finally:
                        loop.close()
                else:
                    yield from gen

        _gen_entry.__name__ = fn_name
        _gen_entry.__qualname__ = fn_name

        options: Dict[str, Any] = {
            "num_cpus": (spec["num_cpus"] if spec.get("num_cpus") is not None else 0),
        }
        task_env = self._merge_runtime_env(spec.get("runtime_env"))
        if task_env:
            options["runtime_env"] = task_env
        for opt_key in ("num_gpus", "memory", "resources", "label_selector"):
            if spec.get(opt_key) is not None:
                options[opt_key] = spec[opt_key]
        return fn_name, ray.remote(_gen_entry).options(**options)

    async def run_task(self, spec: Dict[str, Any]) -> Dict[str, Any]:
        self._touch()
        raw_args = spec.get("args") or []
        raw_kwargs = spec.get("kwargs") or {}
        wait = bool(spec.get("wait", True))
        return_ref = bool(spec.get("return_ref", False))
        timeout = spec.get("timeout")

        try:
            fn_name, remote_task = self._build_remote_task(spec)
            resolved_args = [
                await self._resolve_arg(a, top_level=True) for a in raw_args
            ]
            resolved_kwargs = {
                k: await self._resolve_arg(v, top_level=True)
                for k, v in raw_kwargs.items()
            }
            obj_ref = remote_task.remote(*resolved_args, **resolved_kwargs)
            if not wait:
                ref_id = f"ref_{uuid.uuid4().hex[:12]}"
                self._refs[ref_id] = obj_ref
                return {
                    "ok": True,
                    "ref": {"$ray_ref": ref_id},
                    "workspace": self._workspace,
                }
            val = await self._await_obj_ref(obj_ref, timeout=timeout)
            if return_ref:
                ref_id = f"ref_{uuid.uuid4().hex[:12]}"
                self._refs[ref_id] = obj_ref
                return {
                    "ok": True,
                    "ref": {"$ray_ref": ref_id},
                    "workspace": self._workspace,
                }
            _validate_strict_json(val, f"Function '{fn_name}' return value")
            return {"ok": True, "result": val}
        except Exception as exc:
            return {"ok": False, "error": _format_exception_dict(exc)}

    async def stream_task(self, spec: Dict[str, Any]):
        self._touch()
        raw_args = spec.get("args") or []
        raw_kwargs = spec.get("kwargs") or {}
        try:
            fn_name, remote_gen = self._build_remote_generator_task(spec)
            resolved_args = [
                await self._resolve_arg(a, top_level=True) for a in raw_args
            ]
            resolved_kwargs = {
                k: await self._resolve_arg(v, top_level=True)
                for k, v in raw_kwargs.items()
            }
            gen = remote_gen.remote(*resolved_args, **resolved_kwargs)
            async for item_ref in gen:
                self._touch()
                item = await item_ref
                _validate_strict_json(item, f"Streamed item from '{fn_name}'")
                yield {"ok": True, "item": item}
        except Exception as exc:
            yield {"ok": False, "error": _format_exception_dict(exc)}

    async def create_actor(self, spec: Dict[str, Any]) -> Dict[str, Any]:
        self._touch()
        name = spec.get("name") or f"actor_{uuid.uuid4().hex[:8]}"
        class_name = spec["class_name"]
        module_source = spec.get("module_source")
        module_path = spec.get("module")
        captured_globals = spec.get("captured_globals") or {}
        raw_args = spec.get("args") or []
        raw_kwargs = spec.get("kwargs") or {}
        lifetime = spec.get("lifetime") or "workspace"
        get_if_exists = bool(spec.get("get_if_exists", True))

        if get_if_exists:
            if name in self._actors:
                meta = self._actors[name]
                return {
                    "ok": True,
                    "name": name,
                    "workspace": self._workspace,
                    "namespace": self._namespace,
                    "class_name": meta["class_name"],
                    "lifetime": meta["lifetime"],
                    "created": False,
                }
            try:
                ext_handle = ray.get_actor(name, namespace=self._namespace)
                self._actors[name] = {
                    "handle": ext_handle,
                    "class_name": class_name,
                    "lifetime": lifetime,
                    "created_at": time.time(),
                }
                return {
                    "ok": True,
                    "name": name,
                    "workspace": self._workspace,
                    "namespace": self._namespace,
                    "class_name": class_name,
                    "lifetime": lifetime,
                    "created": False,
                }
            except ValueError:
                pass

        try:
            user_cls = _load_user_callable(
                "actor",
                self._workspace,
                class_name,
                module_source,
                module_path,
                captured_globals,
            )
            remote_cls = (
                user_cls
                if hasattr(user_cls, "__ray_metadata__")
                else ray.remote(user_cls)
            )

            resolved_args = [
                await self._resolve_arg(a, top_level=True) for a in raw_args
            ]
            resolved_kwargs = {
                k: await self._resolve_arg(v, top_level=True)
                for k, v in raw_kwargs.items()
            }

            actor_runtime_env = self._merge_runtime_env(spec.get("runtime_env"))
            options: Dict[str, Any] = {
                "name": name,
                "namespace": self._namespace,
                "num_cpus": (
                    spec["num_cpus"] if spec.get("num_cpus") is not None else 0
                ),
                "max_concurrency": (
                    spec["max_concurrency"]
                    if spec.get("max_concurrency") is not None
                    else 10
                ),
            }
            if lifetime == "detached":
                options["lifetime"] = "detached"
            if actor_runtime_env:
                options["runtime_env"] = actor_runtime_env
            for opt_key in (
                "num_gpus",
                "memory",
                "resources",
                "label_selector",
            ):
                if spec.get(opt_key) is not None:
                    options[opt_key] = spec[opt_key]

            handle = remote_cls.options(**options).remote(
                *resolved_args, **resolved_kwargs
            )
            await handle.__ray_ready__.remote()
            self._actors[name] = {
                "handle": handle,
                "class_name": class_name,
                "lifetime": lifetime,
                "created_at": time.time(),
            }
            return {
                "ok": True,
                "name": name,
                "workspace": self._workspace,
                "namespace": self._namespace,
                "class_name": class_name,
                "lifetime": lifetime,
                "created": True,
            }
        except Exception as exc:
            return {"ok": False, "error": _format_exception_dict(exc)}

    async def get_actor_info(self, name: str) -> Dict[str, Any]:
        self._touch()
        if name in self._actors:
            meta = self._actors[name]
            return {
                "ok": True,
                "name": name,
                "workspace": self._workspace,
                "namespace": self._namespace,
                "class_name": meta["class_name"],
                "lifetime": meta["lifetime"],
                "created_at": meta["created_at"],
            }
        try:
            ext_handle = ray.get_actor(name, namespace=self._namespace)
            self._actors[name] = {
                "handle": ext_handle,
                "class_name": "Unknown",
                "lifetime": "detached",
                "created_at": time.time(),
            }
            return {
                "ok": True,
                "name": name,
                "workspace": self._workspace,
                "namespace": self._namespace,
                "class_name": "Unknown",
                "lifetime": "detached",
            }
        except ValueError:
            return {
                "ok": False,
                "error": {
                    "exc_type": "KeyError",
                    "exc_message": (
                        f"Actor '{name}' does not exist in workspace '{self._workspace}'."
                    ),
                },
            }

    async def list_actors(self) -> Dict[str, Any]:
        info = await self.get_info()
        return {"ok": True, "actors": info["actors"]}

    async def _get_actor_handle(self, name: str) -> ActorHandle:
        if name in self._actors:
            return self._actors[name]["handle"]
        try:
            handle = ray.get_actor(name, namespace=self._namespace)
            self._actors[name] = {
                "handle": handle,
                "class_name": "Unknown",
                "lifetime": "detached",
                "created_at": time.time(),
            }
            return handle
        except ValueError as exc:
            raise KeyError(
                f"Actor '{name}' does not exist in workspace '{self._workspace}'."
            ) from exc

    async def call_actor_method(
        self,
        name: str,
        method_name: str,
        payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        self._touch()
        raw_args = payload.get("args") or []
        raw_kwargs = payload.get("kwargs") or {}
        wait = bool(payload.get("wait", True))
        return_ref = bool(payload.get("return_ref", False))
        timeout = payload.get("timeout")

        try:
            handle = await self._get_actor_handle(name)
            method = getattr(handle, method_name)
            resolved_args = [
                await self._resolve_arg(a, top_level=True) for a in raw_args
            ]
            resolved_kwargs = {
                k: await self._resolve_arg(v, top_level=True)
                for k, v in raw_kwargs.items()
            }
            obj_ref = method.remote(*resolved_args, **resolved_kwargs)
            if not wait:
                ref_id = f"ref_{uuid.uuid4().hex[:12]}"
                self._refs[ref_id] = obj_ref
                return {
                    "ok": True,
                    "ref": {"$ray_ref": ref_id},
                    "workspace": self._workspace,
                }
            val = await self._await_obj_ref(obj_ref, timeout=timeout)
            if return_ref:
                ref_id = f"ref_{uuid.uuid4().hex[:12]}"
                self._refs[ref_id] = obj_ref
                return {
                    "ok": True,
                    "ref": {"$ray_ref": ref_id},
                    "workspace": self._workspace,
                }
            _validate_strict_json(val, f"Return value of '{name}.{method_name}'")
            return {"ok": True, "result": val}
        except Exception as exc:
            return {"ok": False, "error": _format_exception_dict(exc)}

    async def stream_actor_method(
        self,
        name: str,
        method_name: str,
        payload: Dict[str, Any],
    ):
        self._touch()
        raw_args = payload.get("args") or []
        raw_kwargs = payload.get("kwargs") or {}
        try:
            handle = await self._get_actor_handle(name)
            method = getattr(handle, method_name)
            resolved_args = [
                await self._resolve_arg(a, top_level=True) for a in raw_args
            ]
            resolved_kwargs = {
                k: await self._resolve_arg(v, top_level=True)
                for k, v in raw_kwargs.items()
            }
            gen = method.remote(*resolved_args, **resolved_kwargs)
            async for item_ref in gen:
                self._touch()
                item = await item_ref
                _validate_strict_json(
                    item, f"Streamed item from '{name}.{method_name}'"
                )
                yield {"ok": True, "item": item}
        except Exception as exc:
            yield {"ok": False, "error": _format_exception_dict(exc)}

    async def kill_actor(self, name: str, no_restart: bool = True) -> Dict[str, Any]:
        self._touch()
        handle = None
        if name in self._actors:
            handle = self._actors.pop(name)["handle"]
        else:
            try:
                handle = ray.get_actor(name, namespace=self._namespace)
            except ValueError:
                return {"ok": False, "killed": False}
        try:
            ray.kill(handle, no_restart=no_restart)
        except Exception:
            pass
        return {"ok": True, "killed": True}

    async def get_ref(
        self,
        ref_id: str,
        ready_only: bool = False,
        timeout: Optional[float] = None,
    ) -> Dict[str, Any]:
        self._touch()
        if ref_id not in self._refs:
            return {
                "ok": False,
                "error": {
                    "exc_type": "KeyError",
                    "exc_message": (
                        f"ObjectRef '{ref_id}' not found in workspace '{self._workspace}'."
                    ),
                },
            }
        obj_ref = self._refs[ref_id]
        if ready_only:
            ready, _ = ray.wait([obj_ref], timeout=0, fetch_local=False)
            return {"ok": True, "ready": bool(ready)}
        try:
            val = await self._await_obj_ref(obj_ref, timeout=timeout)
            _validate_strict_json(val, f"ObjectRef '{ref_id}'")
            return {"ok": True, "result": val}
        except Exception as exc:
            return {"ok": False, "error": _format_exception_dict(exc)}

    async def cancel_ref(self, ref_id: str, force: bool = False) -> Dict[str, Any]:
        self._touch()
        if ref_id not in self._refs:
            return {"ok": False, "cancelled": False}
        obj_ref = self._refs[ref_id]
        try:
            ray.cancel(obj_ref, force=force)
        except Exception:
            pass
        return {"ok": True, "cancelled": True}

    async def delete_ref(self, ref_id: str) -> Dict[str, Any]:
        self._touch()
        existed = self._refs.pop(ref_id, None) is not None
        return {"ok": True, "deleted": existed}

    async def cleanup(self, kill_detached: bool = False) -> Dict[str, Any]:
        self._closed = True
        killed_actors = []
        for name, meta in list(self._actors.items()):
            if meta["lifetime"] != "detached" or kill_detached:
                try:
                    ray.kill(meta["handle"], no_restart=True)
                    killed_actors.append(name)
                except Exception:
                    pass
        self._actors.clear()
        num_refs = len(self._refs)
        self._refs.clear()
        return {
            "ok": True,
            "workspace": self._workspace,
            "killed_actors": killed_actors,
            "cleared_refs": num_refs,
        }


class WorkspaceManager:
    """Dashboard agent manager for client workspaces, tasks, live actors, and ObjectRefs."""

    def __init__(self):
        self._supervisors: Dict[str, ActorHandle] = {}
        self._lock = asyncio.Lock()

    async def get_or_create_workspace(
        self,
        workspace: str,
        ttl_seconds: Optional[float] = 1800.0,
        runtime_env: Optional[Dict[str, Any]] = None,
        update_ttl: bool = False,
    ) -> Dict[str, Any]:
        async with self._lock:
            sup = await self._lookup_supervisor(workspace)
            if sup is not None:
                try:
                    return await sup.ping.remote(
                        ttl_seconds=ttl_seconds, update_ttl=update_ttl
                    )
                except Exception:
                    self._supervisors.pop(workspace, None)

            ns = workspace_namespace(workspace)
            sup_name = workspace_supervisor_name(workspace)
            try:
                ray.cloudpickle.register_pickle_by_value(sys.modules[__name__])
            except Exception:
                pass
            sup_options: Dict[str, Any] = {
                "name": sup_name,
                "namespace": ns,
                "lifetime": "detached",
                "get_if_exists": True,
            }
            if runtime_env:
                sup_options["runtime_env"] = runtime_env
            sup = WorkspaceSupervisor.options(**sup_options).remote(
                workspace,
                ttl_seconds=ttl_seconds,
                runtime_env=runtime_env,
            )
            self._supervisors[workspace] = sup
            return await sup.get_info.remote()

    async def list_workspaces(self) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        for ws_name, sup in list(self._supervisors.items()):
            try:
                info = await sup.get_info.remote()
                items.append(info)
            except Exception:
                self._supervisors.pop(ws_name, None)
        return items

    async def _lookup_supervisor(self, workspace: str) -> Optional[ActorHandle]:
        if workspace in self._supervisors:
            return self._supervisors[workspace]
        try:
            sup = ray.get_actor(
                workspace_supervisor_name(workspace),
                namespace=workspace_namespace(workspace),
            )
            self._supervisors[workspace] = sup
            return sup
        except ValueError:
            return None

    async def get_workspace(self, workspace: str) -> Optional[Dict[str, Any]]:
        sup = await self._lookup_supervisor(workspace)
        if sup is None:
            return None
        try:
            return await sup.get_info.remote()
        except Exception:
            self._supervisors.pop(workspace, None)
            return None

    async def heartbeat_workspace(
        self,
        workspace: str,
        ttl_seconds: Optional[float] = None,
        update_ttl: bool = False,
    ) -> Optional[Dict[str, Any]]:
        sup = await self._lookup_supervisor(workspace)
        if sup is None:
            return None
        try:
            return await sup.ping.remote(ttl_seconds=ttl_seconds, update_ttl=update_ttl)
        except Exception:
            self._supervisors.pop(workspace, None)
            return None

    async def delete_workspace(
        self, workspace: str, kill_detached: bool = False
    ) -> Optional[Dict[str, Any]]:
        async with self._lock:
            sup = await self._lookup_supervisor(workspace)
            self._supervisors.pop(workspace, None)
            if sup is None:
                return None
            try:
                res = await sup.cleanup.remote(kill_detached=kill_detached)
                try:
                    ray.kill(sup, no_restart=True)
                except Exception:
                    pass
                return res
            except Exception:
                return {"ok": True, "workspace": workspace}

    async def run_task(self, workspace: str, spec: Dict[str, Any]) -> Dict[str, Any]:
        sup = await self._lookup_supervisor(workspace)
        if sup is None:
            await self.get_or_create_workspace(
                workspace, runtime_env=spec.get("runtime_env")
            )
            sup = await self._lookup_supervisor(workspace)
        assert sup is not None
        return await sup.run_task.remote(spec)

    async def stream_task(self, workspace: str, spec: Dict[str, Any]):
        sup = await self._lookup_supervisor(workspace)
        if sup is None:
            await self.get_or_create_workspace(
                workspace, runtime_env=spec.get("runtime_env")
            )
            sup = await self._lookup_supervisor(workspace)
        assert sup is not None
        async for item_ref in sup.stream_task.remote(spec):
            yield await item_ref

    async def create_actor(
        self, workspace: str, spec: Dict[str, Any]
    ) -> Dict[str, Any]:
        sup = await self._lookup_supervisor(workspace)
        if sup is None:
            await self.get_or_create_workspace(
                workspace, runtime_env=spec.get("runtime_env")
            )
            sup = await self._lookup_supervisor(workspace)
        assert sup is not None
        return await sup.create_actor.remote(spec)

    async def list_actors(self, workspace: str) -> Optional[Dict[str, Any]]:
        sup = await self._lookup_supervisor(workspace)
        if sup is None:
            return None
        return await sup.list_actors.remote()

    async def get_actor_info(
        self, workspace: str, name: str
    ) -> Optional[Dict[str, Any]]:
        sup = await self._lookup_supervisor(workspace)
        if sup is None:
            return None
        return await sup.get_actor_info.remote(name)

    async def call_actor_method(
        self,
        workspace: str,
        name: str,
        method_name: str,
        payload: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        sup = await self._lookup_supervisor(workspace)
        if sup is None:
            return None
        return await sup.call_actor_method.remote(name, method_name, payload)

    async def stream_actor_method(
        self,
        workspace: str,
        name: str,
        method_name: str,
        payload: Dict[str, Any],
    ):
        sup = await self._lookup_supervisor(workspace)
        if sup is None:
            yield {
                "ok": False,
                "error": {
                    "exc_type": "KeyError",
                    "exc_message": f"Workspace '{workspace}' does not exist.",
                },
            }
            return
        async for item_ref in sup.stream_actor_method.remote(
            name, method_name, payload
        ):
            yield await item_ref

    async def kill_actor(
        self, workspace: str, name: str, no_restart: bool = True
    ) -> Optional[Dict[str, Any]]:
        sup = await self._lookup_supervisor(workspace)
        if sup is None:
            return None
        return await sup.kill_actor.remote(name, no_restart=no_restart)

    async def get_ref(
        self,
        workspace: str,
        ref_id: str,
        ready_only: bool = False,
        timeout: Optional[float] = None,
    ) -> Optional[Dict[str, Any]]:
        sup = await self._lookup_supervisor(workspace)
        if sup is None:
            return None
        return await sup.get_ref.remote(ref_id, ready_only=ready_only, timeout=timeout)

    async def cancel_ref(
        self, workspace: str, ref_id: str, force: bool = False
    ) -> Optional[Dict[str, Any]]:
        sup = await self._lookup_supervisor(workspace)
        if sup is None:
            return None
        return await sup.cancel_ref.remote(ref_id, force=force)

    async def delete_ref(self, workspace: str, ref_id: str) -> Optional[Dict[str, Any]]:
        sup = await self._lookup_supervisor(workspace)
        if sup is None:
            return None
        return await sup.delete_ref.remote(ref_id)
