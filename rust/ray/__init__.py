# Copyright 2024 The Ray Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0

"""Ray Python API for the Rust backend.

Provides the standard ``import ray`` interface backed by the Rust
``_raylet`` extension module.  Supports:

    ray.init()
    @ray.remote       -- functions and classes
    ray.put / get / wait
    ray.get_actor
    ray.shutdown

Example::

    import ray
    ray.init(num_task_workers=4)

    @ray.remote
    def square(x):
        return x * x

    @ray.remote
    class Counter:
        def __init__(self):
            self.n = 0
        def incr(self, delta=1):
            self.n += delta
            return self.n

    print(ray.get(square.remote(5)))         # 25
    c = Counter.remote()
    print(ray.get(c.incr.remote(10)))        # 10
    ray.shutdown()
"""

from __future__ import annotations

import pickle as _pickle
from typing import Any, List, Optional, Sequence, Union

from _raylet import (
    start_cluster as _start_cluster,
    PyCoreWorker as _PyCoreWorker,
    PyGcsClient as _PyGcsClient,
    PyWorkerID as _PyWorkerID,
)

# Re-export _raylet as ray._raylet for convenience
from _raylet import (
    start_cluster,
    PyCoreWorker,
    PyGcsClient,
    PyWorkerID,
    PyObjectID,
    PyActorID,
    PyTaskID,
    PyNodeID,
    PyJobID,
    PyObjectRef,
    PyPlacementGroupID,
)

__version__ = "3.0.0.dev0"

__all__ = [
    "__version__",
    "init",
    "shutdown",
    "is_initialized",
    "remote",
    "put",
    "get",
    "wait",
    "get_actor",
    "ObjectRef",
    "ActorHandle",
]

# =====================================================================
# ObjectRef
# =====================================================================


class ObjectRef:
    """Reference to an object in the distributed object store."""

    __slots__ = ("_binary", "_owner")

    def __init__(self, binary: bytes, owner: _PyCoreWorker):
        self._binary = binary
        self._owner = owner

    def __repr__(self) -> str:
        return f"ObjectRef({self._binary[:8].hex()}...)"

    def __eq__(self, other: object) -> bool:
        return isinstance(other, ObjectRef) and self._binary == other._binary

    def __hash__(self) -> int:
        return hash(self._binary)

    def __getstate__(self):
        return self._binary

    def __setstate__(self, state):
        self._binary = state
        self._owner = None


# =====================================================================
# RemoteFunction
# =====================================================================


class RemoteFunction:
    """Callable wrapper returned by ``@ray.remote`` on a function."""

    def __init__(self, func, options: dict | None = None):
        self._func = func
        self._options = options or {}
        self.__name__ = func.__name__
        self.__doc__ = func.__doc__
        self.__module__ = func.__module__

    def remote(self, *args: Any) -> ObjectRef:
        """Submit the function as a remote task."""
        rt = _runtime
        serialized = [_pickle.dumps(a) for a in args]
        _, _, _, task_driver = rt._pick_task_worker()
        num_returns = self._options.get("num_returns", 1)
        max_retries = self._options.get("max_retries", 0)
        refs = task_driver.submit_task(
            self._func.__name__, serialized,
            num_returns=num_returns, max_retries=max_retries,
        )
        if num_returns == 1:
            return ObjectRef(refs[0].binary(), task_driver)
        return [ObjectRef(r.binary(), task_driver) for r in refs]

    def options(self, **kwargs) -> RemoteFunction:
        """Return a copy with overridden options."""
        merged = {**self._options, **kwargs}
        return RemoteFunction(self._func, merged)


# =====================================================================
# Actor classes
# =====================================================================


class _ActorMethodHandle:
    """Proxy for ``actor.method`` that provides ``.remote()``."""

    __slots__ = ("_handle", "_method")

    def __init__(self, handle: ActorHandle, method: str):
        self._handle = handle
        self._method = method

    def remote(self, *args: Any) -> ObjectRef:
        serialized = [_pickle.dumps(a) for a in args]
        driver = _runtime.driver
        oid = driver.submit_actor_method(
            self._handle._actor_id, self._method, serialized
        )
        return ObjectRef(oid.binary(), driver)


class ActorHandle:
    """Handle to a live remote actor.  Methods accessed as attributes."""

    __slots__ = ("_actor_id", "_name", "_namespace")

    def __init__(self, actor_id, name: str, namespace: str = "default"):
        self._actor_id = actor_id
        self._name = name
        self._namespace = namespace

    def __getattr__(self, name: str):
        if name.startswith("_"):
            raise AttributeError(name)
        return _ActorMethodHandle(self, name)

    def __repr__(self) -> str:
        return f"Actor({self._name})"


class RemoteClass:
    """Factory returned by ``@ray.remote`` on a class."""

    _counter = 0

    def __init__(self, cls, options: dict | None = None):
        self._cls = cls
        self._options = options or {}

    def remote(self, *args: Any, **kwargs: Any) -> ActorHandle:
        """Instantiate the actor on a new worker."""
        rt = _runtime
        instance = self._cls(*args, **kwargs)

        wid = _PyWorkerID.py_from_random()
        cluster = rt.cluster
        worker = _PyCoreWorker(
            0, "127.0.0.1", cluster.gcs_address(), 1,
            worker_id=wid, node_id=cluster.node_id(),
        )

        def callback(method, raw_args, num_returns=1):
            deserialized = [_pickle.loads(a) for a in raw_args]
            result = getattr(instance, method)(*deserialized)
            return _pickle.dumps(result)

        worker.set_task_callback(callback)
        port = worker.start_grpc_server()

        RemoteClass._counter += 1
        namespace = self._options.get("namespace", "default")
        name = self._options.get(
            "name", f"{self._cls.__name__}_{RemoteClass._counter}"
        )
        actor_id = rt.gcs.register_actor(name, namespace)

        rt.driver.setup_actor(
            actor_id, name, namespace, "127.0.0.1", port,
            cluster.node_id(), wid,
        )

        rt._actor_workers.append(worker)
        return ActorHandle(actor_id, name, namespace)

    def options(self, **kwargs) -> RemoteClass:
        """Return a copy with overridden options (name, namespace, ...)."""
        merged = {**self._options, **kwargs}
        return RemoteClass(self._cls, merged)


# =====================================================================
# Runtime singleton
# =====================================================================


class _RayRuntime:
    """Internal singleton managing cluster state."""

    def __init__(self):
        self.cluster = None
        self.gcs = None
        self.driver = None
        self._task_pool: list = []
        self._func_registry: dict = {}
        self._next_worker = 0
        self._actor_workers: list = []
        self._task_call_counts: list = []
        self._initialized = False

    # -- lifecycle ---------------------------------------------------

    def init(self, *, num_task_workers: int = 2, **_kwargs):
        if self._initialized:
            return
        self.cluster = _start_cluster()
        self.gcs = _PyGcsClient(self.cluster.gcs_address())
        self.driver = _PyCoreWorker(
            1, "127.0.0.1", self.cluster.gcs_address(), 1,
            node_id=self.cluster.node_id(),
        )
        self._task_call_counts = [0] * num_task_workers
        for _ in range(num_task_workers):
            self._add_task_worker()
        self._initialized = True

    def shutdown(self):
        if not self._initialized:
            return
        if self.cluster is not None:
            self.cluster.shutdown()
        self.cluster = None
        self.gcs = None
        self.driver = None
        self._task_pool.clear()
        self._func_registry.clear()
        self._actor_workers.clear()
        self._task_call_counts.clear()
        self._next_worker = 0
        self._initialized = False
        RemoteClass._counter = 0

    def is_initialized(self) -> bool:
        return self._initialized

    # -- task worker pool --------------------------------------------

    def _add_task_worker(self):
        runtime = self
        worker_idx = len(self._task_pool)
        wid = _PyWorkerID.py_from_random()
        worker = _PyCoreWorker(
            0, "127.0.0.1", self.cluster.gcs_address(), 1,
            worker_id=wid, node_id=self.cluster.node_id(),
        )

        def callback(method, raw_args, num_returns=1):
            runtime._task_call_counts[worker_idx] += 1
            func = runtime._func_registry[method]
            deserialized = [_pickle.loads(a) for a in raw_args]
            result = func(*deserialized)
            if num_returns > 1 and isinstance(result, (list, tuple)):
                return [_pickle.dumps(r) for r in result]
            return _pickle.dumps(result)

        worker.set_task_callback(callback)
        port = worker.start_grpc_server()

        task_driver = _PyCoreWorker(
            1, "127.0.0.1", self.cluster.gcs_address(), 1,
            node_id=self.cluster.node_id(),
        )
        task_driver.setup_task_dispatch("127.0.0.1", port, wid)
        self._task_pool.append((worker, wid, port, task_driver))

    def _pick_task_worker(self):
        idx = self._next_worker % len(self._task_pool)
        self._next_worker += 1
        return self._task_pool[idx]

    # -- decorator ---------------------------------------------------

    def remote(self, func_or_class=None, **options):
        def decorator(target):
            if isinstance(target, type):
                return RemoteClass(target, options)
            self._func_registry[target.__name__] = target
            return RemoteFunction(target, options)
        if func_or_class is not None:
            return decorator(func_or_class)
        return decorator

    # -- object store ------------------------------------------------

    def put(self, obj: Any) -> ObjectRef:
        data = _pickle.dumps(obj)
        oid = self.driver.put(data, b"pickle")
        return ObjectRef(oid.binary(), self.driver)

    def get(
        self,
        refs: Union[ObjectRef, Sequence[ObjectRef]],
        *,
        timeout: float = 10.0,
    ):
        single = isinstance(refs, ObjectRef)
        if single:
            refs = [refs]
        timeout_ms = int(timeout * 1000)
        by_owner: dict = {}
        for i, ref in enumerate(refs):
            key = id(ref._owner)
            by_owner.setdefault(key, (ref._owner, []))[1].append((i, ref))
        results = [None] * len(refs)
        for owner, items in by_owner.values():
            binaries = [ref._binary for _, ref in items]
            raw = owner.get(binaries, timeout_ms)
            for (idx, _), r in zip(items, raw):
                if r is not None:
                    results[idx] = _pickle.loads(r[0])
        return results[0] if single else results

    def wait(
        self,
        refs: Sequence[ObjectRef],
        *,
        num_returns: int = 1,
        timeout: Optional[float] = None,
    ):
        timeout_ms = int(timeout * 1000) if timeout is not None else 30000
        by_owner: dict = {}
        for ref in refs:
            key = id(ref._owner)
            by_owner.setdefault(key, (ref._owner, []))[1].append(ref)
        ready, remaining = [], []
        for owner, items in by_owner.values():
            binaries = [ref._binary for ref in items]
            flags = owner.wait(binaries, len(binaries), timeout_ms)
            for ref, flag in zip(items, flags):
                (ready if flag else remaining).append(ref)
        if len(ready) > num_returns:
            remaining.extend(ready[num_returns:])
            ready = ready[:num_returns]
        return ready, remaining

    # -- actor lookup ------------------------------------------------

    def get_actor(self, name: str, namespace: str = "default") -> ActorHandle:
        actor_id = self.gcs.get_named_actor(name, namespace)
        if actor_id is None:
            raise ValueError(
                f"Actor '{name}' not found in namespace '{namespace}'"
            )
        return ActorHandle(actor_id, name, namespace)


# Module-level singleton
_runtime = _RayRuntime()


# =====================================================================
# Module-level API (matches standard Ray)
# =====================================================================


def init(*, num_task_workers: int = 2, **kwargs):
    """Initialize Ray (start in-process cluster)."""
    _runtime.init(num_task_workers=num_task_workers, **kwargs)


def shutdown():
    """Shut down Ray."""
    _runtime.shutdown()


def is_initialized() -> bool:
    """Return True if Ray has been initialized."""
    return _runtime.is_initialized()


def remote(func_or_class=None, **options):
    """Decorator to mark a function or class as remote.

    Usage::

        @ray.remote
        def f(x): ...

        @ray.remote(num_returns=2)
        def g(x): ...

        @ray.remote
        class MyActor: ...
    """
    return _runtime.remote(func_or_class, **options)


def put(obj: Any) -> ObjectRef:
    """Store an object in the object store."""
    return _runtime.put(obj)


def get(
    refs: Union[ObjectRef, List[ObjectRef]],
    *,
    timeout: float = 10.0,
) -> Any:
    """Retrieve object(s) from the object store."""
    return _runtime.get(refs, timeout=timeout)


def wait(
    refs: List[ObjectRef],
    *,
    num_returns: int = 1,
    timeout: Optional[float] = None,
):
    """Wait for a list of ObjectRefs to become ready.

    Returns (ready, remaining) lists.
    """
    return _runtime.wait(refs, num_returns=num_returns, timeout=timeout)


def get_actor(name: str, namespace: str = "default") -> ActorHandle:
    """Look up a named actor via GCS."""
    return _runtime.get_actor(name, namespace)
