"""Creating and resolving detached ``SandboxHost`` and ``MultiSandboxHost`` actors.

Shared by the REST app (``app.py``) and the gRPC facade, and free of FastAPI
so the facade runs without the Serve extra.
"""

import asyncio
import concurrent.futures
import hashlib
import json
import logging
import random
import secrets
import threading
import uuid
import zlib
from collections import OrderedDict
from typing import Any, Dict, List, Optional

from ray.experimental.sandbox.http.host import MultiSandboxHost, SandboxHost
from ray.experimental.sandbox.http.schemas import SandboxAPISettings
from ray.util.annotations import PublicAPI

logger = logging.getLogger(__name__)

SANDBOX_ID_PREFIX = "sb-"
SANDBOX_POOL_ACTOR_PREFIX = "sb-pool-"
SANDBOX_POOL_COORDINATOR_PREFIX = "sb-pool-coord-"

# 22 hex chars (the gRPC facade's id shape too): long enough that random ids
# never collide in practice, so a fresh create skips get_if_exists and a
# collision could only fail loudly, never attach a second client to someone
# else's sandbox.
_SANDBOX_ID_HEX_CHARS = 22
_SHARD_HEX_CHARS = 8


def _new_random_sandbox_id() -> str:
    return f"{SANDBOX_ID_PREFIX}{uuid.uuid4().hex[:_SANDBOX_ID_HEX_CHARS]}"


def _sandbox_name_for_token(client_token: str) -> str:
    digest = hashlib.sha256(client_token.encode("utf-8")).hexdigest()
    return f"{SANDBOX_ID_PREFIX}{digest[:_SANDBOX_ID_HEX_CHARS]}"


def _shard_for_sandbox_id(sandbox_id: str, num_actors: int) -> int:
    """Deterministically map any ``sandbox_id`` onto ``[0, num_actors)``.

    Standard V1 sandbox IDs (``sb-`` plus 22 hex characters) route by their
    leading 32-bit hex prefix, which both random sharded IDs and SHA-256 named
    IDs populate uniformly. Arbitrary test IDs fall back to CRC32.
    """
    if num_actors <= 1:
        return 0
    prefix_end = len(SANDBOX_ID_PREFIX) + _SHARD_HEX_CHARS
    if sandbox_id.startswith(SANDBOX_ID_PREFIX) and len(sandbox_id) >= prefix_end:
        hex_part = sandbox_id[len(SANDBOX_ID_PREFIX) : prefix_end]
        try:
            return int(hex_part, 16) % num_actors
        except ValueError:
            pass
    return zlib.crc32(sandbox_id.encode("utf-8")) % num_actors


def _new_sharded_sandbox_id(shard_idx: int, num_actors: int) -> str:
    """Mint a V1-shaped ``sb-<22 hex>`` ID that hashes to ``shard_idx``."""
    shard_idx %= max(1, num_actors)
    two32 = 1 << 32
    if 1 < num_actors < two32:
        limit = two32 - (two32 % num_actors)
        r = secrets.randbelow(limit)
        prefix32 = (r - (r % num_actors)) + shard_idx
    elif num_actors >= two32:
        prefix32 = shard_idx % two32
    else:
        prefix32 = secrets.randbits(32)
    return f"{SANDBOX_ID_PREFIX}{prefix32:08x}{secrets.token_hex(7)}"


def _is_unschedulable(exc: BaseException) -> bool:
    """True when Ray reports the actor can never fit the cluster's resources.

    Matched by class name for the same reason as ``_is_actor_gone``.
    """
    names = {type(exc).__name__, *(base.__name__ for base in type(exc).__mro__)}
    return any("ActorUnschedulableError" in name for name in names)


def _is_actor_unavailable(exc: BaseException) -> bool:
    """True when a remote call failed because the actor is *transiently*
    unreachable (a restart or network blip), not because it is gone.

    Ray makes ``ActorUnavailableError`` a subclass of ``RayActorError``, so
    this must be checked before ``_is_actor_gone`` — otherwise a recoverable
    blip is misread as a permanent death. Matched by class name for the same
    importability reason as ``_is_actor_gone``.
    """
    names = {type(exc).__name__, *(base.__name__ for base in type(exc).__mro__)}
    return any("ActorUnavailableError" in name for name in names)


def _is_actor_gone(exc: BaseException) -> bool:
    """True when a remote call failed because the actor no longer exists.

    Matched by class name because Ray re-raises remote failures as
    dynamically-built subclasses, and so this module stays importable (and
    unit-testable) without a live Ray context. Transient unreachability
    (``ActorUnavailableError``) is handled separately by
    ``_is_actor_unavailable`` and must be ruled out first.
    """
    names = {type(exc).__name__, *(base.__name__ for base in type(exc).__mro__)}
    return any("RayActorError" in name or "ActorDiedError" in name for name in names)


def _pool_actor_options(settings: SandboxAPISettings) -> Dict[str, Any]:
    num_cpus = (
        settings.sandbox_actor_num_cpus
        if settings.sandbox_actor_num_cpus is not None
        else settings.default_actor_num_cpus
    )
    options: Dict[str, Any] = {
        "num_cpus": num_cpus,
        "max_concurrency": settings.sandbox_actor_max_concurrency,
        "scheduling_strategy": "SPREAD",
    }
    if settings.sandbox_actor_memory_mb is not None:
        options["memory"] = int(settings.sandbox_actor_memory_mb * 1024 * 1024)
    return options


def _pool_host_settings(settings: SandboxAPISettings) -> Dict[str, Any]:
    return {
        "max_output_bytes": settings.max_output_bytes,
        "max_exec_history": settings.max_exec_history,
        "max_file_bytes": settings.max_file_bytes,
    }


def _pool_tag(settings: SandboxAPISettings) -> str:
    payload = {
        "n": settings.num_sandbox_actors,
        "cpus": (
            settings.sandbox_actor_num_cpus
            if settings.sandbox_actor_num_cpus is not None
            else settings.default_actor_num_cpus
        ),
        "mem_mb": settings.sandbox_actor_memory_mb,
        "conc": settings.sandbox_actor_max_concurrency,
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()[:8]


def is_multiplexed_handle(handle: Any) -> bool:
    """True only for pooled ``_MultiplexedSandboxHandle`` wrappers.

    Uses ``isinstance`` rather than ``getattr`` so proxy handles that implement
    ``__getattr__`` (such as Ray's ``ActorHandle`` or test doubles) are never
    probed for a method named ``is_multiplexed``.
    """
    return isinstance(handle, _MultiplexedSandboxHandle)


class _NoOpRemoteMethod:
    def remote(self, *args: Any, **kwargs: Any) -> Any:
        fut = asyncio.get_running_loop().create_future()
        fut.set_result({"ok": True})
        return fut


class _MultiplexedRemoteMethod:
    __slots__ = ("_actor_method", "_sandbox_id")

    def __init__(self, actor_method: Any, sandbox_id: str) -> None:
        self._actor_method = actor_method
        self._sandbox_id = sandbox_id

    def remote(self, *args: Any, **kwargs: Any) -> Any:
        return self._actor_method.remote(self._sandbox_id, *args, **kwargs)


class _MultiplexedSandboxHandle:
    """Binds a shared ``MultiSandboxHost`` actor handle to one ``sandbox_id``."""

    __slots__ = ("actor_handle", "sandbox_id", "shard_idx", "_boot_started")
    is_multiplexed = True

    def __init__(
        self,
        actor_handle: Any,
        sandbox_id: str,
        shard_idx: int,
        *,
        boot_started: bool = False,
    ) -> None:
        self.actor_handle = actor_handle
        self.sandbox_id = sandbox_id
        self.shard_idx = shard_idx
        self._boot_started = boot_started

    def __getattr__(self, name: str) -> Any:
        if name == "boot" and self._boot_started:
            # create_sandbox() already scheduled boot() in the same RPC; skip a
            # redundant fire-and-forget round trip on initial creation.
            self._boot_started = False
            return _NoOpRemoteMethod()
        return _MultiplexedRemoteMethod(
            getattr(self.actor_handle, name), self.sandbox_id
        )


class _SandboxPoolCoordinator:
    """Creates and warms the ``MultiSandboxHost`` pool once per cluster config.

    All Serve facade replicas fetch the full handle table from this single
    coordinator instead of each replica issuing ``N`` GCS lookups at startup.
    """

    def __init__(
        self,
        namespace: str,
        actor_prefix: str,
        num_actors: int,
        actor_options: Dict[str, Any],
        host_settings: Dict[str, Any],
    ) -> None:
        self._namespace = namespace
        self._actor_prefix = actor_prefix
        self._num_actors = num_actors
        self._actor_options = actor_options
        self._host_settings = host_settings
        self._lock = asyncio.Lock()
        self._handles: Optional[List[Any]] = None

    def _create_all_sync(self) -> List[Any]:
        import ray
        from ray.util import list_named_actors

        multi_cls = ray.remote(MultiSandboxHost)
        existing_names = {
            entry["name"]
            for entry in list_named_actors(all_namespaces=True)
            if entry.get("namespace") == self._namespace
            and entry.get("name", "").startswith(self._actor_prefix)
        }

        def ensure_one(idx: int) -> Any:
            name = f"{self._actor_prefix}{idx}"
            if name in existing_names:
                try:
                    return ray.get_actor(name, namespace=self._namespace)
                except ValueError:
                    pass
            handle = multi_cls.options(
                name=name,
                namespace=self._namespace,
                lifetime="detached",
                get_if_exists=True,
                **self._actor_options,
            ).remote(settings=self._host_settings)
            handle.warmup.remote()
            return handle

        workers = min(32, max(1, self._num_actors))
        if workers == 1:
            return [ensure_one(0)]
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=workers, thread_name_prefix="sandbox-pool-init"
        ) as pool:
            return list(pool.map(ensure_one, range(self._num_actors)))

    async def ensure_pool(self) -> List[Any]:
        async with self._lock:
            if self._handles is None:
                self._handles = await asyncio.to_thread(self._create_all_sync)
            return self._handles

    async def replace_shard(self, shard_idx: int, dead_handle: Any) -> Any:
        async with self._lock:
            import ray

            def _replace_sync() -> Any:
                try:
                    ray.kill(dead_handle)
                except Exception:
                    pass
                multi_cls = ray.remote(MultiSandboxHost)
                name = f"{self._actor_prefix}{shard_idx}"
                handle = multi_cls.options(
                    name=name,
                    namespace=self._namespace,
                    lifetime="detached",
                    get_if_exists=True,
                    **self._actor_options,
                ).remote(settings=self._host_settings)
                handle.warmup.remote()
                return handle

            new_handle = await asyncio.to_thread(_replace_sync)
            if self._handles is not None and 0 <= shard_idx < len(self._handles):
                self._handles[shard_idx] = new_handle
            return new_handle


@PublicAPI(stability="alpha")
class RayActorHandleResolver:
    """Creates and resolves detached ``SandboxHost`` or pooled ``MultiSandboxHost`` actors.

    When ``settings.num_sandbox_actors`` is None (the default), each sandbox is
    backed by its own named detached ``SandboxHost`` actor. When
    ``settings.num_sandbox_actors`` is set, sandboxes are multiplexed onto a
    fixed pool of ``num_sandbox_actors`` detached ``MultiSandboxHost`` actors,
    sized with ``sandbox_actor_num_cpus`` and ``sandbox_actor_memory_mb``, and
    routed deterministically by ``sandbox_id`` with zero per-sandbox GCS calls.

    Args:
        settings: The sandbox API settings.
    """

    # Bounds the handle cache, far above the sandboxes one process serves.
    _MAX_CACHED_HANDLES = 65536

    def __init__(self, settings: SandboxAPISettings) -> None:
        self._settings = settings
        self._lock = threading.Lock()
        self._host_class: Any = None
        self._multi_host_class: Any = None
        self._coord_class: Any = None
        self._handles: "OrderedDict[str, Any]" = OrderedDict()

        # Multiplexed pool state (used only when settings.num_sandbox_actors is set).
        self._num_pool_actors = settings.num_sandbox_actors
        self._tag = _pool_tag(settings) if self._num_pool_actors is not None else ""
        self._pool_actor_prefix = (
            f"{SANDBOX_POOL_ACTOR_PREFIX}{self._tag}-"
            if self._num_pool_actors is not None
            else ""
        )
        self._pool_coord_name = (
            f"{SANDBOX_POOL_COORDINATOR_PREFIX}{self._tag}"
            if self._num_pool_actors is not None
            else ""
        )
        self._pool_handles: Optional[List[Any]] = None
        self._pool_async_locks: Dict[int, asyncio.Lock] = {}
        if self._num_pool_actors is not None:
            self._shard_order = list(range(self._num_pool_actors))
            random.SystemRandom().shuffle(self._shard_order)
        else:
            self._shard_order = []
        self._shard_cursor = 0

    @property
    def is_multiplexed(self) -> bool:
        """True when sandboxes multiplex across a fixed pool of actors."""
        return self._num_pool_actors is not None

    def next_sandbox_id(self) -> str:
        """Mint a random sandbox ID balanced across the pool (when multiplexed)."""
        if self._num_pool_actors is None:
            return _new_random_sandbox_id()
        with self._lock:
            if self._shard_cursor >= self._num_pool_actors:
                random.SystemRandom().shuffle(self._shard_order)
                self._shard_cursor = 0
            shard_idx = self._shard_order[self._shard_cursor]
            self._shard_cursor += 1
        return _new_sharded_sandbox_id(shard_idx, self._num_pool_actors)

    def _actor_class(self) -> Any:
        # One ActorClass for the resolver's lifetime: a fresh
        # ray.remote(SandboxHost) per create re-exports the class each time
        # and made creates about 12x slower.
        with self._lock:
            if self._host_class is None:
                import ray

                self._host_class = ray.remote(SandboxHost)
            return self._host_class

    def _multi_actor_class(self) -> Any:
        with self._lock:
            if self._multi_host_class is None:
                import ray

                self._multi_host_class = ray.remote(MultiSandboxHost)
            return self._multi_host_class

    def _coordinator_actor(self) -> Any:
        import ray

        with self._lock:
            if self._coord_class is None:
                self._coord_class = ray.remote(_SandboxPoolCoordinator)
            coord_cls = self._coord_class
        try:
            return ray.get_actor(
                self._pool_coord_name, namespace=self._settings.namespace
            )
        except ValueError:
            return coord_cls.options(
                name=self._pool_coord_name,
                namespace=self._settings.namespace,
                lifetime="detached",
                get_if_exists=True,
                num_cpus=0,
                max_concurrency=256,
            ).remote(
                namespace=self._settings.namespace,
                actor_prefix=self._pool_actor_prefix,
                num_actors=self._num_pool_actors,
                actor_options=_pool_actor_options(self._settings),
                host_settings=_pool_host_settings(self._settings),
            )

    def _ensure_pool_sync(self) -> List[Any]:
        handles = self._pool_handles
        if handles is not None:
            return handles
        import ray

        coord = self._coordinator_actor()
        fetched = list(ray.get(coord.ensure_pool.remote()))
        with self._lock:
            if self._pool_handles is None:
                self._pool_handles = fetched
            return self._pool_handles

    async def ensure_pool_async(self) -> List[Any]:
        """Ensure the multiplexed actor pool exists and cache all shard handles."""
        handles = self._pool_handles
        if handles is not None:
            return handles
        loop = asyncio.get_running_loop()
        loop_id = id(loop)
        with self._lock:
            lock = self._pool_async_locks.get(loop_id)
            if lock is None:
                lock = asyncio.Lock()
                self._pool_async_locks[loop_id] = lock
        async with lock:
            if self._pool_handles is not None:
                return self._pool_handles
            coord = await asyncio.to_thread(self._coordinator_actor)
            fetched = list(await coord.ensure_pool.remote())
            with self._lock:
                if self._pool_handles is None:
                    self._pool_handles = fetched
                return self._pool_handles

    async def _replace_dead_shard_async(self, shard_idx: int, dead_handle: Any) -> Any:
        coord = await asyncio.to_thread(self._coordinator_actor)
        new_handle = await coord.replace_shard.remote(shard_idx, dead_handle)
        with self._lock:
            if self._pool_handles is not None and 0 <= shard_idx < len(
                self._pool_handles
            ):
                self._pool_handles[shard_idx] = new_handle
        return new_handle

    def _replace_dead_shard_sync(self, shard_idx: int, dead_handle: Any) -> Any:
        import ray

        coord = self._coordinator_actor()
        new_handle = ray.get(coord.replace_shard.remote(shard_idx, dead_handle))
        with self._lock:
            if self._pool_handles is not None and 0 <= shard_idx < len(
                self._pool_handles
            ):
                self._pool_handles[shard_idx] = new_handle
        return new_handle

    async def create_async(
        self,
        name: str,
        actor_options: Dict[str, Any],
        ctor_kwargs: Dict[str, Any],
        get_if_exists: bool = True,
    ) -> Any:
        """Async sandbox host creation; avoids thread hops in multiplexed mode."""
        if self._num_pool_actors is None:
            return await asyncio.to_thread(
                self.create, name, actor_options, ctor_kwargs, get_if_exists
            )
        handles = await self.ensure_pool_async()
        shard_idx = _shard_for_sandbox_id(name, self._num_pool_actors)
        actor_handle = handles[shard_idx]
        spec = ctor_kwargs["spec"]
        settings = ctor_kwargs.get("settings")
        try:
            await actor_handle.create_sandbox.remote(
                name, spec, settings, get_if_exists=get_if_exists
            )
        except Exception as exc:
            if not _is_actor_gone(exc) or _is_actor_unavailable(exc):
                raise
            actor_handle = await self._replace_dead_shard_async(shard_idx, actor_handle)
            await actor_handle.create_sandbox.remote(
                name, spec, settings, get_if_exists=get_if_exists
            )
        return _MultiplexedSandboxHandle(
            actor_handle, name, shard_idx, boot_started=True
        )

    def create(
        self,
        name: str,
        actor_options: Dict[str, Any],
        ctor_kwargs: Dict[str, Any],
        get_if_exists: bool = True,
    ) -> Any:
        """Create the named detached host actor (or register on a pool shard).

        Args:
            name: The sandbox id, used as the actor name in per-sandbox mode.
            actor_options: Extra ``.options()`` (resources) in per-sandbox mode.
            ctor_kwargs: ``SandboxHost`` constructor arguments.
            get_if_exists: Atomic get-or-create, for idempotent names: two
                replicas racing on one client token converge on one sandbox
                (``boot()`` is idempotent). A fresh random name passes False.

        Returns:
            The actor (or bound shard) handle.
        """
        if self._num_pool_actors is not None:
            import ray

            handles = self._ensure_pool_sync()
            shard_idx = _shard_for_sandbox_id(name, self._num_pool_actors)
            actor_handle = handles[shard_idx]
            spec = ctor_kwargs["spec"]
            settings = ctor_kwargs.get("settings")
            try:
                ray.get(
                    actor_handle.create_sandbox.remote(
                        name, spec, settings, get_if_exists=get_if_exists
                    )
                )
            except Exception as exc:
                if not _is_actor_gone(exc) or _is_actor_unavailable(exc):
                    raise
                actor_handle = self._replace_dead_shard_sync(shard_idx, actor_handle)
                ray.get(
                    actor_handle.create_sandbox.remote(
                        name, spec, settings, get_if_exists=get_if_exists
                    )
                )
            return _MultiplexedSandboxHandle(
                actor_handle, name, shard_idx, boot_started=True
            )

        handle = (
            self._actor_class()
            .options(
                name=name,
                namespace=self._settings.namespace,
                lifetime="detached",
                get_if_exists=get_if_exists,
                **actor_options,
            )
            .remote(**ctor_kwargs)
        )
        if not get_if_exists:
            with self._lock:
                self._handles[name] = handle
                while len(self._handles) > self._MAX_CACHED_HANDLES:
                    self._handles.popitem(last=False)
        return handle

    def cached(self, name: str) -> Optional[Any]:
        """``get`` from memory alone, or None: never waits on the GCS.

        For event-loop callers, which then fall back to ``get`` in a thread.

        Args:
            name: The sandbox id.

        Returns:
            The cached handle, or None.
        """
        if self._num_pool_actors is not None:
            handles = self._pool_handles
            if handles is None:
                return None
            shard_idx = _shard_for_sandbox_id(name, self._num_pool_actors)
            actor_handle = handles[shard_idx]
            if actor_handle is None:
                return None
            return _MultiplexedSandboxHandle(actor_handle, name, shard_idx)

        with self._lock:
            handle = self._handles.get(name)
            if handle is not None:
                self._handles.move_to_end(name)
            return handle

    def get(self, name: str) -> Optional[Any]:
        """Look up a sandbox actor by name.

        Args:
            name: The actor name / sandbox id.

        Returns:
            The actor handle, from the cache or from ``ray.get_actor`` in the
            configured namespace, or None if no actor has that name.
        """
        handle = self.cached(name)
        if handle is not None:
            return handle
        if self._num_pool_actors is not None:
            handles = self._ensure_pool_sync()
            shard_idx = _shard_for_sandbox_id(name, self._num_pool_actors)
            actor_handle = handles[shard_idx]
            if actor_handle is None:
                import ray

                actor_name = f"{self._pool_actor_prefix}{shard_idx}"
                actor_handle = (
                    self._multi_actor_class()
                    .options(
                        name=actor_name,
                        namespace=self._settings.namespace,
                        lifetime="detached",
                        get_if_exists=True,
                        **_pool_actor_options(self._settings),
                    )
                    .remote(settings=_pool_host_settings(self._settings))
                )
                with self._lock:
                    handles[shard_idx] = actor_handle
            return _MultiplexedSandboxHandle(actor_handle, name, shard_idx)

        import ray

        try:
            return ray.get_actor(name, namespace=self._settings.namespace)
        except ValueError:
            return None

    def forget(self, name: str) -> None:
        """Drop the cached handle of a per-sandbox actor that is gone.

        Args:
            name: The actor name / sandbox id.
        """
        if self._num_pool_actors is not None:
            return
        with self._lock:
            self._handles.pop(name, None)

    def forget_dead_shard(self, handle: Any) -> None:
        """Drop a cached multiplexed shard handle whose underlying actor died."""
        if self._num_pool_actors is None or not is_multiplexed_handle(handle):
            return
        shard_idx = getattr(handle, "shard_idx", -1)
        with self._lock:
            if (
                self._pool_handles is not None
                and 0 <= shard_idx < len(self._pool_handles)
                and self._pool_handles[shard_idx]
                is getattr(handle, "actor_handle", None)
            ):
                self._pool_handles[shard_idx] = None

    async def describe_all_async(self) -> Optional[List[Dict[str, Any]]]:
        """Fetch sandbox info dicts from every pool shard when multiplexed."""
        if self._num_pool_actors is None:
            return None
        handles = await self.ensure_pool_async()
        results = await asyncio.gather(
            *(h.describe_all.remote() for h in handles), return_exceptions=True
        )
        sandboxes: List[Dict[str, Any]] = []
        for shard_idx, item in enumerate(results):
            if isinstance(item, BaseException):
                logger.warning(
                    "Listing: failed to describe pool shard %s: %r", shard_idx, item
                )
                continue
            sandboxes.extend(item)
        return sandboxes

    def list_names(self) -> List[str]:
        """List the names of the sandbox actors in the configured namespace.

        Returns:
            The names of the sandboxes in the namespace whose names start with
            the sandbox ID prefix.
        """
        if self._num_pool_actors is not None:
            import ray

            handles = self._ensure_pool_sync()
            batches = ray.get([h.list_sandbox_ids.remote() for h in handles])
            names: List[str] = []
            for batch in batches:
                names.extend(batch)
            return names

        from ray.util import list_named_actors

        names = []
        for entry in list_named_actors(all_namespaces=True):
            name = entry.get("name", "")
            if (
                entry.get("namespace") == self._settings.namespace
                and name.startswith(SANDBOX_ID_PREFIX)
                and not name.startswith(SANDBOX_POOL_ACTOR_PREFIX)
                and not name.startswith(SANDBOX_POOL_COORDINATOR_PREFIX)
            ):
                names.append(name)
        return names

    def kill(self, handle: Any) -> None:
        """Kill a per-sandbox actor with ``ray.kill`` (no-op for pooled handles).

        A failure to kill the actor is logged at debug level and not raised.

        Args:
            handle: The actor handle to kill.
        """
        if is_multiplexed_handle(handle):
            return
        import ray

        try:
            ray.kill(handle)
        except Exception as exc:
            logger.debug("Failed to kill sandbox actor: %s", exc)
