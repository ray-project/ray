"""Creating and resolving the named detached ``SandboxHost`` actors.

Shared by the REST app (``app.py``) and the gRPC facade, and free of FastAPI
so the facade runs without the Serve extra.
"""

import asyncio
import hashlib
import itertools
import logging
import re
import threading
import time
import uuid
from collections import OrderedDict
from typing import Any, Dict, List, Optional, Tuple

from ray.experimental.sandbox.http.host import SandboxHost
from ray.experimental.sandbox.http.host_channel import HostChannel, HostChannelError
from ray.experimental.sandbox.http.node_host import CHANNEL_METHODS, SandboxNodeHost
from ray.experimental.sandbox.http.schemas import SandboxAPISettings
from ray.util.annotations import PublicAPI

logger = logging.getLogger(__name__)

SANDBOX_ID_PREFIX = "sb-"

# 22 hex chars (the gRPC facade's id shape too): long enough that random ids
# never collide in practice, so a fresh create skips get_if_exists and a
# collision could only fail loudly, never attach a second client to someone
# else's sandbox.
_SANDBOX_ID_HEX_CHARS = 22


# Name prefix of the per-node hosts (host_mode="node"); not SANDBOX_ID_PREFIX,
# so listings never mistake a host for a sandbox.
NODE_HOST_PREFIX = "sandbox-node-host-"
# Long polls (describe, get_exec) from every sandbox on a node share one host.
_NODE_HOST_MAX_CONCURRENCY = 100_000
# How long the list of nodes that can run sandboxes is reused.
_NODES_TTL_SECONDS = 5.0
# How long a listing waits for the node hosts to report their sandboxes.
_LIST_HOSTS_SECONDS = 10.0
# After a host channel fails to connect or breaks, calls to that host go
# through Ray for this long before the next attempt.
_CHANNEL_RETRY_SECONDS = 30.0
_CHANNEL_CONNECT_SECONDS = 10.0
# A hosted sandbox's id names its node, so any resolver finds the sandbox
# without a registry: "sb-", then the first _NODE_PREFIX_CHARS hex characters
# of the node id, a marker that hex never contains, and random hex, 22
# characters after "sb-" like every other sandbox id.
_NODE_PREFIX_CHARS = 10
_HOSTED_MARKER = "z"
_HOSTED_ID = re.compile(
    rf"{SANDBOX_ID_PREFIX}([0-9a-f]{{{_NODE_PREFIX_CHARS}}}){_HOSTED_MARKER}"
    r"[0-9a-f]{11}"
)


def _hosted_sandbox_id(node_id: str) -> str:
    return (
        f"{SANDBOX_ID_PREFIX}{node_id[:_NODE_PREFIX_CHARS]}{_HOSTED_MARKER}"
        f"{uuid.uuid4().hex[:11]}"
    )


class _ChannelSlot:
    """One event loop's channel to one node host (see host_channel)."""

    __slots__ = ("channel", "connecting", "retry_at", "task")

    def __init__(self) -> None:
        self.channel: Optional[HostChannel] = None
        self.connecting = False
        self.retry_at = 0.0
        # The connect task (the loop keeps only weak references to tasks).
        self.task: Optional[asyncio.Task] = None


class _HostedCall:
    """A call on a sandbox's host that follows the host across a restart.

    The resolver caches host handles, and a host started again under the same
    name (after its actor died) is a new actor: a call that fails because the
    cached one is gone looks the host up by name once and runs there. The new
    host has the sandboxes created on it and reports any other as
    terminated. Awaitable; a call nobody awaits still runs (on the cached
    host).
    """

    def __init__(
        self, handle: "HostedSandboxHandle", method: str, args: Any, kwargs: Any
    ) -> None:
        self._handle = handle
        self._method = method
        self._args = args
        self._kwargs = kwargs
        self._ref: Any = None
        self._sent: Any = None
        resolver = handle._resolver
        channel = (
            resolver.host_channel(handle.host)
            if resolver is not None and method in CHANNEL_METHODS
            else None
        )
        if channel is not None:
            try:
                self._sent = channel.call(method, (handle.sandbox_id, *args), kwargs)
            except HostChannelError:
                self._sent = None
        if self._sent is None:
            self._ref = self._submit()

    def _submit(self) -> Any:
        return getattr(self._handle.host, self._method).remote(
            self._handle.sandbox_id, *self._args, **self._kwargs
        )

    def __await__(self):
        return self._run().__await__()

    async def _run(self) -> Any:
        if self._sent is not None:
            try:
                return await self._sent
            except HostChannelError:
                # The connection broke; every method the channel serves is
                # safe to repeat, so run it as a Ray task instead.
                self._ref = self._submit()
        try:
            return await self._ref
        except Exception as exc:
            resolver = self._handle._resolver
            if (
                resolver is None
                or _is_actor_unavailable(exc)
                or not _is_actor_gone(exc)
            ):
                raise
            host = await asyncio.to_thread(resolver._refresh_host, self._handle)
            if host is None:
                raise
            return await getattr(host, self._method).remote(
                self._handle.sandbox_id, *self._args, **self._kwargs
            )


class _HostedMethod:
    __slots__ = ("_handle", "_method")

    def __init__(self, handle: "HostedSandboxHandle", method: str) -> None:
        self._handle = handle
        self._method = method

    def remote(self, *args: Any, **kwargs: Any) -> _HostedCall:
        return _HostedCall(self._handle, self._method, args, kwargs)


class HostedSandboxHandle:
    """A sandbox on a ``SandboxNodeHost``, with a ``SandboxHost`` handle's surface.

    ``handle.describe.remote(...)`` calls ``host.describe.remote(sandbox_id,
    ...)``, so the API layers drive both kinds of sandboxes the same way.
    """

    def __init__(
        self,
        host: Any,
        host_name: str,
        sandbox_id: str,
        resolver: Optional["RayActorHandleResolver"] = None,
    ) -> None:
        self.host = host
        self.host_name = host_name
        self.sandbox_id = sandbox_id
        self._resolver = resolver

    def __getattr__(self, name: str) -> _HostedMethod:
        if name.startswith("_"):
            raise AttributeError(name)
        return _HostedMethod(self, name)


def _reservation_bundle(actor_options: Dict[str, Any]) -> Dict[str, float]:
    """The placement-group bundle that reserves what the host actor would have."""
    bundle: Dict[str, float] = {}
    if actor_options.get("num_cpus"):
        bundle["CPU"] = float(actor_options["num_cpus"])
    if actor_options.get("memory"):
        bundle["memory"] = float(actor_options["memory"])
    for key, value in (actor_options.get("resources") or {}).items():
        if value:
            bundle[key] = float(value)
    return bundle


def _new_random_sandbox_id() -> str:
    return f"{SANDBOX_ID_PREFIX}{uuid.uuid4().hex[:_SANDBOX_ID_HEX_CHARS]}"


def _sandbox_name_for_token(client_token: str) -> str:
    digest = hashlib.sha256(client_token.encode("utf-8")).hexdigest()
    return f"{SANDBOX_ID_PREFIX}{digest[:_SANDBOX_ID_HEX_CHARS]}"


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


@PublicAPI(stability="alpha")
class RayActorHandleResolver:
    """Creates and resolves the named detached SandboxHost actors.

    The default (and only production) resolver; tests inject a fake with the
    same methods so the HTTP layer runs without a Ray cluster.

    Every method may wait on a GCS round trip, so the async layers call them
    from worker threads and the class is thread-safe. Handles of actors this
    resolver created under fresh random names are cached: such a name is
    never reused, so a cached handle can only go stale by its actor dying,
    which surfaces as "not found" on the next call. Deterministic names (a
    client token, a named sandbox) always resolve through ``ray.get_actor``,
    whose own cache follows actor deaths and re-creations.

    Args:
        settings: The API settings; ``host_mode`` chooses per-sandbox or
            per-node hosting.
    """

    # Bounds the handle cache, far above the sandboxes one process serves.
    _MAX_CACHED_HANDLES = 65536
    # The host classes this resolver starts.
    _sandbox_host_class: type = SandboxHost
    _node_host_class: type = SandboxNodeHost

    def __init__(self, settings: SandboxAPISettings) -> None:
        self._settings = settings
        self._lock = threading.Lock()
        self._host_class: Any = None
        self._node_host_actor_class: Any = None
        self._handles: "OrderedDict[str, Any]" = OrderedDict()
        # host_mode="node": node id -> (actor name, handle), and the nodes
        # that sandboxes without a reservation spread over.
        self._node_hosts: Dict[str, Tuple[str, Any]] = {}
        # host_mode="node": sandbox specs each node host keeps booted ahead
        # of creates, as [{"spec", "size"}] (set by the gRPC facade).
        self.warm_templates: List[Dict[str, Any]] = []
        self._nodes: List[str] = []
        self._nodes_at = 0.0
        self._refreshing_nodes = False
        self._spread = itertools.count()
        # host_channel: (host actor id, event loop id) -> its _ChannelSlot.
        self._channels: Dict[Tuple[str, int], "_ChannelSlot"] = {}

    def _actor_class(self) -> Any:
        # One ActorClass for the resolver's lifetime: a fresh
        # ray.remote(SandboxHost) per create re-exports the class each time
        # and made creates about 12x slower.
        with self._lock:
            if self._host_class is None:
                import ray

                self._host_class = ray.remote(self._sandbox_host_class)
            return self._host_class

    def _cache(self, name: str, handle: Any) -> None:
        with self._lock:
            self._handles[name] = handle
            while len(self._handles) > self._MAX_CACHED_HANDLES:
                self._handles.popitem(last=False)

    # ------------------------------------------------------------------
    # host_mode="node": sandboxes hosted by one actor per node
    # ------------------------------------------------------------------

    def _node_host(self, node_id: str) -> Tuple[str, Any]:
        """The host on ``node_id``, created on first use."""
        with self._lock:
            entry = self._node_hosts.get(node_id)
            if entry is not None:
                return entry
            if self._node_host_actor_class is None:
                import ray

                self._node_host_actor_class = ray.remote(self._node_host_class)
            actor_class = self._node_host_actor_class
        from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

        name = f"{NODE_HOST_PREFIX}{node_id}"
        config = (
            {"CPU": self._settings.reservation_slab_cpus}
            if self._settings.reservation_slab_cpus
            else None,
            self.warm_templates,
            {
                "max_output_bytes": self._settings.max_output_bytes,
                "max_exec_history": self._settings.max_exec_history,
                "max_file_bytes": self._settings.max_file_bytes,
            },
        )
        handle = actor_class.options(
            name=name,
            namespace=self._settings.namespace,
            lifetime="detached",
            get_if_exists=True,
            num_cpus=0,
            max_concurrency=_NODE_HOST_MAX_CONCURRENCY,
            scheduling_strategy=NodeAffinitySchedulingStrategy(node_id, soft=False),
        ).remote(self._sandbox_host_class, *config)
        # The name may find a host an earlier deployment started, with other
        # settings: this resolver's apply from now on.
        handle.configure.remote(*config)
        with self._lock:
            entry = self._node_hosts.setdefault(node_id, (name, handle))
        return entry

    def _cached_node_host(self, node_id: str) -> Optional[Tuple[str, Any]]:
        with self._lock:
            return self._node_hosts.get(node_id)

    async def _admit(
        self,
        demand: Dict[str, float],
        ctor_kwargs: Dict[str, Any],
    ) -> Optional[HostedSandboxHandle]:
        """Place a sandbox in some node host's slab, round robin; None if all are full."""
        nodes = await asyncio.to_thread(self._cpu_nodes)
        if not nodes:
            return None
        start = next(self._spread)
        for i in range(len(nodes)):
            node_id = nodes[(start + i) % len(nodes)]
            name = _hosted_sandbox_id(node_id)
            for attempt in (0, 1):
                entry = self._cached_node_host(node_id) or await asyncio.to_thread(
                    self._node_host, node_id
                )
                host_name, host = entry
                try:
                    args = (name, ctor_kwargs["spec"], ctor_kwargs["settings"], demand)
                    result = None
                    channel = self.host_channel(host)
                    if channel is not None:
                        try:
                            result = await channel.call("admit", args, {})
                        except HostChannelError:
                            result = None
                    if result is None:
                        result = await host.admit.remote(*args)
                    break
                except Exception as exc:
                    if attempt or not _is_actor_gone(exc):
                        raise
                    self._drop_node_host(node_id, host)
            if result.get("ok"):
                handle = HostedSandboxHandle(host, host_name, name, self)
                self._cache(name, handle)
                return handle
        return None

    def prestart(self) -> int:
        """Start the host on every node that can run sandboxes (host_mode="node").

        Hosts otherwise start with the first sandbox placed on their node,
        which then waits for the host's worker process.

        Returns:
            How many hosts were started or found.
        """
        import ray

        started = 0
        for node_id in self._cpu_nodes(refresh=True):
            for attempt in (0, 1):
                handle = self._node_host(node_id)[1]
                try:
                    ray.get(handle.sandbox_ids.remote())
                    started += 1
                    break
                except Exception as exc:
                    if attempt or not _is_actor_gone(exc):
                        raise
                    self._drop_node_host(node_id, handle)
        return started

    def _refresh_host(self, handle: HostedSandboxHandle) -> Optional[Any]:
        """The live host under ``handle``'s host name, if it isn't the one
        ``handle`` has; updates ``handle`` and the cache."""
        import ray

        node_id = handle.host_name[len(NODE_HOST_PREFIX) :]
        with self._lock:
            entry = self._node_hosts.get(node_id)
        if entry is not None and entry[1]._actor_id != handle.host._actor_id:
            # Another call already found the new host.
            handle.host = entry[1]
            return entry[1]
        try:
            host = ray.get_actor(handle.host_name, namespace=self._settings.namespace)
        except ValueError:
            return None
        if host._actor_id == handle.host._actor_id:
            return None
        with self._lock:
            self._node_hosts[node_id] = (handle.host_name, host)
        handle.host = host
        return host

    def host_channel(self, host: Any) -> Optional[HostChannel]:
        """A connected channel to ``host`` for calls on this event loop, or None.

        With ``host_channel`` set, the first call to a host from a loop starts
        connecting in the background; calls go through Ray until it is up,
        and for _CHANNEL_RETRY_SECONDS after it fails or breaks.

        Args:
            host: A ``SandboxNodeHost`` actor handle.

        Returns:
            The channel, or None while there is no usable one.
        """
        if not self._settings.host_channel:
            return None
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return None
        key = (host._actor_id.hex(), id(loop))
        slot = self._channels.get(key)
        if slot is None:
            slot = self._channels[key] = _ChannelSlot()
        channel = slot.channel
        if channel is not None and channel.ready:
            return channel
        now = time.monotonic()
        if not slot.connecting and now >= slot.retry_at:
            slot.connecting = True
            slot.retry_at = now + _CHANNEL_RETRY_SECONDS
            slot.task = loop.create_task(self._open_channel(slot, host))
        return None

    async def _open_channel(self, slot: "_ChannelSlot", host: Any) -> None:
        try:
            endpoint = await asyncio.wait_for(
                host.channel_endpoint.remote(), _CHANNEL_CONNECT_SECONDS
            )
            channel = HostChannel(endpoint)
            await asyncio.wait_for(channel.connect(), _CHANNEL_CONNECT_SECONDS)
            if slot.channel is not None:
                slot.channel.close()
            slot.channel = channel
        except Exception as exc:  # noqa: BLE001 - Ray carries the calls meanwhile
            logger.info("No channel to sandbox node host (using Ray calls): %s", exc)
        finally:
            slot.connecting = False

    def _drop_node_host(self, node_id: str, handle: Any) -> None:
        """Forget a cached host that has died, so the next use starts another."""
        with self._lock:
            if self._node_hosts.get(node_id, (None, None))[1] is handle:
                del self._node_hosts[node_id]

    def _cpu_nodes(self, refresh: bool = False) -> List[str]:
        """Alive nodes with CPUs, reused for a few seconds.

        One caller refreshes a stale list while the others keep using it, so
        a burst of lookups doesn't send the GCS a node listing each.
        """
        import ray

        now = time.monotonic()
        with self._lock:
            stale = now - self._nodes_at > _NODES_TTL_SECONDS or not self._nodes
            if not (stale or refresh) or (self._nodes and self._refreshing_nodes):
                return list(self._nodes)
            self._refreshing_nodes = True
        try:
            nodes = sorted(
                n["NodeID"]
                for n in ray.nodes()
                if n.get("Alive") and n.get("Resources", {}).get("CPU", 0) > 0
            )
            with self._lock:
                self._nodes, self._nodes_at = nodes, time.monotonic()
        finally:
            with self._lock:
                self._refreshing_nodes = False
        with self._lock:
            return list(self._nodes)

    def _spread_node(self) -> str:
        """A node for a sandbox that reserves nothing: round robin over CPU nodes."""
        nodes = self._cpu_nodes()
        if not nodes:
            raise RuntimeError("no alive node with CPUs to host a sandbox")
        return nodes[next(self._spread) % len(nodes)]

    def _node_for_prefix(self, prefix: str) -> Optional[str]:
        """The alive node whose id starts with ``prefix``, if exactly one does."""
        for refresh in (False, True):
            matches = [n for n in self._cpu_nodes(refresh) if n.startswith(prefix)]
            if len(matches) == 1:
                return matches[0]
            if len(matches) > 1:
                return None
        return None

    async def acreate(
        self,
        name: str,
        actor_options: Dict[str, Any],
        ctor_kwargs: Dict[str, Any],
        get_if_exists: bool = True,
    ) -> Any:
        """``create`` for event-loop callers.

        With ``host_mode="node"``, a fresh sandbox waits for its reservation
        on the loop (up to ``scheduling_grace_seconds``) instead of in a
        worker thread, so a burst of creates holds no threads.

        Args:
            name: The sandbox id, used as the actor name.
            actor_options: Extra ``.options()`` (resources).
            ctor_kwargs: ``SandboxHost`` constructor arguments.
            get_if_exists: Atomic get-or-create, as in ``create``.

        Returns:
            The actor handle, or for a hosted sandbox a handle with the same
            methods.
        """
        if self._settings.host_mode != "node" or get_if_exists:
            return await asyncio.to_thread(
                self.create, name, actor_options, ctor_kwargs, get_if_exists
            )
        from ray.util.placement_group import (
            placement_group,
            placement_group_table,
            remove_placement_group,
        )

        bundle = _reservation_bundle(actor_options)
        if bundle and self._settings.reservation_slab_cpus and set(bundle) == {"CPU"}:
            handle = await self._admit(bundle, ctor_kwargs)
            if handle is not None:
                return handle
            # Every host is full: reserve this sandbox on its own, which also
            # shows the autoscaler the demand.
        reservation = None
        if bundle:
            reservation = await asyncio.to_thread(
                placement_group, [bundle], strategy="STRICT_PACK", lifetime="detached"
            )
            try:
                await asyncio.wait_for(
                    reservation.ready(), timeout=self._settings.scheduling_grace_seconds
                )
                table = await asyncio.to_thread(placement_group_table, reservation)
            except BaseException:
                await asyncio.to_thread(remove_placement_group, reservation)
                raise
            node_id = next(iter(table["bundles_to_node_id"].values()))
        else:
            node_id = await asyncio.to_thread(self._spread_node)
        # The id names the node the reservation landed on (see
        # _hosted_sandbox_id), so it is chosen only now.
        name = _hosted_sandbox_id(node_id)
        ctor_kwargs = dict(ctor_kwargs, sandbox_id=name)
        try:
            for attempt in (0, 1):
                host_name, host = await asyncio.to_thread(self._node_host, node_id)
                try:
                    await host.add.remote(
                        name,
                        ctor_kwargs["spec"],
                        ctor_kwargs["settings"],
                        reservation.id.hex() if reservation is not None else None,
                    )
                    break
                except Exception as exc:
                    if attempt or not _is_actor_gone(exc):
                        raise
                    # A host this resolver cached has died; start another.
                    self._drop_node_host(node_id, host)
        except BaseException:
            if reservation is not None:
                await asyncio.to_thread(remove_placement_group, reservation)
            raise
        handle = HostedSandboxHandle(host, host_name, name, self)
        self._cache(name, handle)
        return handle

    def _hosted(self, name: str) -> Optional[HostedSandboxHandle]:
        """The handle for a hosted sandbox id, or None if ``name`` isn't one.

        The id names the node; the node's host is the one live actor with
        that host name. A sandbox whose host has restarted since reads as
        terminated there.
        """
        import ray

        match = _HOSTED_ID.fullmatch(name)
        if match is None:
            return None
        node_id = self._node_for_prefix(match.group(1))
        if node_id is None:
            return None
        with self._lock:
            entry = self._node_hosts.get(node_id)
        if entry is None:
            host_name = f"{NODE_HOST_PREFIX}{node_id}"
            try:
                host = ray.get_actor(host_name, namespace=self._settings.namespace)
            except ValueError:
                return None
            with self._lock:
                entry = self._node_hosts.setdefault(node_id, (host_name, host))
        return HostedSandboxHandle(entry[1], entry[0], name, self)

    def create(
        self,
        name: str,
        actor_options: Dict[str, Any],
        ctor_kwargs: Dict[str, Any],
        get_if_exists: bool = True,
    ) -> Any:
        """Create the named detached host actor.

        Args:
            name: The sandbox id, used as the actor name.
            actor_options: Extra ``.options()`` (resources).
            ctor_kwargs: ``SandboxHost`` constructor arguments.
            get_if_exists: Atomic get-or-create, for idempotent names: two
                replicas racing on one client token converge on one actor
                (``boot()`` is idempotent). A fresh random name passes False,
                which skips a GCS lookup and makes its handle cacheable.

        Returns:
            The actor handle.
        """
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
            self._cache(name, handle)
        return handle

    def cached(self, name: str) -> Optional[Any]:
        """``get`` from memory alone, or None: never waits on the GCS.

        For event-loop callers, which then fall back to ``get`` in a thread.
        A hosted sandbox resolves here once its node's host is cached.

        Args:
            name: The sandbox id.

        Returns:
            The cached handle, or None.
        """
        with self._lock:
            handle = self._handles.get(name)
            if handle is not None:
                self._handles.move_to_end(name)
                return handle
            fresh = time.monotonic() - self._nodes_at <= _NODES_TTL_SECONDS
            nodes = self._nodes if fresh else None
        match = _HOSTED_ID.fullmatch(name)
        if match is None or not nodes:
            return None
        matches = [n for n in nodes if n.startswith(match.group(1))]
        if len(matches) != 1:
            return None
        with self._lock:
            entry = self._node_hosts.get(matches[0])
        if entry is None:
            return None
        handle = HostedSandboxHandle(entry[1], entry[0], name, self)
        self._cache(name, handle)
        return handle

    def get(self, name: str) -> Optional[Any]:
        with self._lock:
            handle = self._handles.get(name)
            if handle is not None:
                self._handles.move_to_end(name)
                return handle
        import ray

        if _HOSTED_ID.fullmatch(name):
            hosted = self._hosted(name)
            if hosted is not None:
                self._cache(name, hosted)
            return hosted
        try:
            return ray.get_actor(name, namespace=self._settings.namespace)
        except ValueError:
            return None

    def forget(self, name: str) -> None:
        """Drop the cached handle of a sandbox whose actor is gone."""
        with self._lock:
            self._handles.pop(name, None)

    def list_names(self) -> List[str]:
        import ray
        from ray.util import list_named_actors

        names: List[str] = []
        hosts = []
        for entry in list_named_actors(all_namespaces=True):
            if entry.get("namespace") != self._settings.namespace:
                continue
            name = entry.get("name", "")
            if name.startswith(SANDBOX_ID_PREFIX):
                names.append(name)
            elif name.startswith(NODE_HOST_PREFIX):
                try:
                    hosts.append(
                        ray.get_actor(name, namespace=self._settings.namespace)
                    )
                except ValueError:
                    pass
        # A dead or stuck host lists nothing rather than failing the listing.
        refs = [h.sandbox_ids.remote() for h in hosts]
        ready, missing = (
            ray.wait(refs, num_returns=len(refs), timeout=_LIST_HOSTS_SECONDS)
            if refs
            else ([], [])
        )
        for ref in ready:
            try:
                names.extend(ray.get(ref))
            except Exception as exc:  # noqa: BLE001
                logger.warning("A sandbox node host failed to list sandboxes: %s", exc)
        if missing:
            logger.warning(
                "%d sandbox node hosts did not answer the listing", len(missing)
            )
        return names

    def kill(self, handle: Any) -> None:
        import ray

        if isinstance(handle, HostedSandboxHandle):
            # Deletes the sandbox and frees its reservation; the host lives on.
            handle.host.remove.remote(handle.sandbox_id)
            return
        try:
            ray.kill(handle)
        except Exception as exc:
            logger.debug("Failed to kill sandbox actor: %s", exc)
