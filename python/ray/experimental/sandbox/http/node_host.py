"""One actor per node that hosts many API sandboxes.

A ``SandboxHost`` actor per sandbox costs a Ray worker process per sandbox:
starting one takes about a CPU-second, so a burst of creates is bound by how
fast each node can start Python workers (measured: ~24 no-op actors/s per
30-CPU node, and 5 s for 100 sandboxes per node, against 1.2 s for the same
sandboxes without per-sandbox actors). A ``SandboxNodeHost`` runs the same
``SandboxHost`` objects in one long-lived actor per node instead, addressed
by sandbox id. Each sandbox's resources are reserved by a placement group,
its own one-bundle group or a share of a slab its host holds (see
``SandboxNodeHost.admit``), which Ray schedules (and the autoscaler sees)
like the actor's own request, without starting a process; the host releases
it when the sandbox goes.

The resolver (``RayActorHandleResolver`` with ``host_mode="node"``) creates
the reservation, picks the host on the node Ray chose, and hands the API
layers a proxy with the ``SandboxHost`` method surface, so they don't change.
"""

import asyncio
import collections
import logging
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Coroutine, Dict, List, Optional, Set

from ray.experimental.sandbox.http.host import HostSettings, SandboxHost, SandboxSpec
from ray.experimental.sandbox.http.host_channel import HostChannelServer
from ray.util.annotations import DeveloperAPI

logger = logging.getLogger(__name__)

# Threads for the blocking parts of every hosted sandbox (image pulls,
# `runsc run` until running, `runsc exec`), shared by the whole node.
_EXECUTOR_THREADS = 512
# A slab with no sandboxes left is released after this long, so a burst that
# follows a burst of terminates finds it still reserved.
_SLAB_IDLE_SECONDS = 60.0
# How long a host waits for a new slab's placement before calling itself
# full. Room on its own node places it at once; a slab still pending after
# this would only wait for a node to free up or join (and show the
# autoscaler demand nobody has), so it is released instead.
_SLAB_PLACEMENT_SECONDS = 0.2
# After a failed growth the host answers "full" at once for this long (or
# until a sandbox leaves), so a burst on a full cluster doesn't wait out a
# placement per admit.
_FULL_SECONDS = 1.0
_EPSILON = 1e-9
# Concurrent boots a host spends refilling its warm pool, so a refill never
# crowds out the creates that drained it.
_WARM_FILL_CONCURRENCY = 4
# After a warm boot fails (say, its image can't be pulled), its template
# boots no replacements for this long, rather than on every create, as in
# SandboxRuntime's warm pool.
_WARM_RETRY_SECONDS = 30.0
# While the sandboxes on a host start more execs a second than this, its
# refill boots one sandbox at a time: right after a burst drained the pool,
# full-speed refills cost the new sandboxes' execs ~30 % of their throughput.
_BUSY_EXECS_PER_SECOND = 50
# What API replicas may call over a host channel (see host_channel.py): the
# per-sandbox calls and admit, all safe to repeat through Ray when a
# connection breaks.
CHANNEL_METHODS = frozenset(
    {
        "admit",
        "describe",
        "start_exec",
        "get_exec",
        "get_exec_by_key",
        "fs_read",
        "fs_write_open",
        "stdin_write",
        "stdin_status",
        "write_file",
        "read_file",
        "terminate",
    }
)


def warm_key(spec: SandboxSpec) -> Optional[tuple]:
    """What a pre-booted sandbox must share with a create to stand in for it.

    None for a create no pre-booted sandbox can serve: one with a workdir
    (mounted at boot) or resource limits (set at boot).
    """
    if (
        spec.get("workdir")
        or spec.get("cpu_limit") is not None
        or spec.get("memory_limit_mb") is not None
    ):
        return None
    capabilities = spec.get("capabilities")
    return (
        spec["image"],
        spec.get("network", "none"),
        bool(spec.get("rootless", True)),
        bool(spec.get("readonly", True)),
        tuple(capabilities) if capabilities is not None else None,
        spec.get("shell"),
        tuple(spec.get("dns") or ()),
    )


class _Slab:
    """One placement group this host reserved on its node, shared by sandboxes."""

    def __init__(self, reservation: Any, capacity: Dict[str, float]) -> None:
        self.reservation = reservation
        self.capacity = dict(capacity)
        self.used: Dict[str, float] = {k: 0.0 for k in capacity}
        self.sandboxes: Dict[str, Dict[str, float]] = {}
        self.idle_timer: Optional[asyncio.TimerHandle] = None

    def fits(self, demand: Dict[str, float]) -> bool:
        return all(
            self.used.get(k, 0.0) + v <= self.capacity.get(k, 0.0) + _EPSILON
            for k, v in demand.items()
        )

    def take(self, sandbox_id: str, demand: Dict[str, float]) -> None:
        if sandbox_id in self.sandboxes:
            return
        if self.idle_timer is not None:
            self.idle_timer.cancel()
            self.idle_timer = None
        for k, v in demand.items():
            self.used[k] = self.used.get(k, 0.0) + v
        self.sandboxes[sandbox_id] = dict(demand)

    def give_back(self, sandbox_id: str) -> None:
        for k, v in self.sandboxes.pop(sandbox_id, {}).items():
            self.used[k] = max(0.0, self.used.get(k, 0.0) - v)


def _gone(sandbox_id: str) -> Dict[str, Any]:
    # Never placed here, or already released: the facade answers NOT_FOUND,
    # as for a sandbox actor that no longer exists.
    return {
        "error_code": "sandbox_not_found",
        "message": f"sandbox {sandbox_id!r} not found",
    }


@DeveloperAPI
class SandboxNodeHost:
    """Hosts the API sandboxes placed on one node.

    Every method takes the sandbox id first and otherwise matches the
    ``SandboxHost`` method of the same name. A sandbox this host doesn't
    have (never placed here, or already gone) reads as terminated, with the
    error code ``sandbox_not_found``.

    Args:
        host_class: The per-sandbox host class (``SandboxHost`` or a
            subclass), instantiated in this process.
        slab: The smallest reservation the host makes at a time and shares
            among its sandboxes, such as ``{"CPU": 4}`` (see ``admit``).
            None reserves each sandbox on its own.
        warm_pool: Sandboxes to keep booted, as ``{"spec", "size"}`` dicts
            with an optional ``"reserve"``: resources to hold in slabs for
            the full pool (ignored without ``slab``).
        warm_settings: The ``HostSettings`` that pre-booted sandboxes start
            with; a create that adopts one applies its own.
    """

    def __init__(
        self,
        host_class: type = SandboxHost,
        slab: Optional[Dict[str, float]] = None,
        warm_pool: Optional[List[Dict[str, Any]]] = None,
        warm_settings: Optional[HostSettings] = None,
    ) -> None:
        import ray

        self._host_class = host_class
        self._sandboxes: Dict[str, SandboxHost] = {}
        # Sandbox id -> its placement group's id (hex), for sandboxes the
        # resolver reserved one by one.
        self._reservations: Dict[str, str] = {}
        self._executor_set = False
        self._node_id = ray.get_runtime_context().get_node_id()
        # Slab reservations (see admit): the smallest one this host reserves.
        self._slab_shape = dict(slab or {})
        self._slabs: list = []
        self._slab_of: Dict[str, _Slab] = {}
        self._grow_lock = asyncio.Lock()
        self._waiting: list = []
        self._full_until = 0.0
        # Boots and refills run as tasks nobody awaits; the loop keeps only
        # weak references to tasks.
        self._background: Set[asyncio.Task] = set()
        # Warm pool: per template, sandboxes booted ahead of the creates
        # that will adopt them (see add), and the reservations to hold for
        # full pools (see _set_warm_pool).
        self._warm_templates: Dict[tuple, Any] = {}
        self._warm_reserve: Dict[tuple, Dict[str, float]] = {}
        self._reserve_floor: Dict[str, float] = {}
        self._warm: Dict[tuple, collections.deque] = {}
        self._warm_booting: Dict[tuple, int] = {}
        # When each template's last warm boot failed (see _WARM_RETRY_SECONDS).
        self._warm_failed_at: Dict[tuple, float] = {}
        self._set_warm_pool(warm_pool)
        self._warm_settings: HostSettings = dict(warm_settings or {})
        self._fill_slots = asyncio.Semaphore(_WARM_FILL_CONCURRENCY)
        self._busy_slot = asyncio.Semaphore(1)
        self._exec_starts: collections.deque = collections.deque()
        self._channel: Optional[HostChannelServer] = None

    def _set_warm_pool(self, warm_pool: Optional[List[Dict[str, Any]]]) -> None:
        """Set the warm pools; a pool no longer listed shuts its sandboxes down."""
        templates: Dict[tuple, Any] = {}
        # Reservations to hold for full pools: {"CPU": size x cpu} per
        # template that asks for it (warm_pool "cpu"), reserved while
        # filling, and the slab capacity they keep: never released for
        # idleness.
        reserve: Dict[tuple, Dict[str, float]] = {}
        floor: Dict[str, float] = {}
        for profile in warm_pool or []:
            key = warm_key(profile["spec"])
            if key is None or int(profile.get("size", 0)) <= 0:
                continue
            templates[key] = (dict(profile["spec"]), int(profile["size"]))
            # Only creates placed in slabs use the reserve, so a host
            # without slabs would hold it beside each create's own group.
            if profile.get("reserve") and self._slab_shape:
                reserve[key] = {k: float(v) for k, v in profile["reserve"].items()}
                for k, v in reserve[key].items():
                    floor[k] = floor.get(k, 0.0) + v
        for key in set(self._warm) - set(templates):
            for host in self._warm.pop(key):
                self._spawn(host._shutdown())
        for key in templates:
            self._warm.setdefault(key, collections.deque())
            self._warm_booting.setdefault(key, 0)
        self._warm_templates = templates
        self._warm_reserve = reserve
        self._reserve_floor = floor

    async def configure(
        self,
        slab: Optional[Dict[str, float]],
        warm_pool: Optional[List[Dict[str, Any]]],
        warm_settings: Optional[HostSettings],
    ) -> None:
        """Take the settings of the facade that calls this host.

        A host is detached and found by name, so it outlives the deployment
        that started it; each resolver sends its own settings on first use
        (see ``RayActorHandleResolver``). Slabs and booted sandboxes stay;
        later reservations and boots follow these settings.

        Args:
            slab: As in the constructor.
            warm_pool: As in the constructor.
            warm_settings: As in the constructor.
        """
        self._use_executor()
        self._slab_shape = dict(slab or {})
        self._warm_settings = dict(warm_settings or {})
        self._set_warm_pool(warm_pool)
        self._refill()

    def _spawn(self, coro: Coroutine[Any, Any, Any]) -> None:
        task = asyncio.ensure_future(coro)
        self._background.add(task)
        task.add_done_callback(self._background.discard)

    def _use_executor(self) -> None:
        # asyncio.to_thread uses the loop's default executor, sized for a
        # handful of blocking calls; this process makes them for every
        # sandbox on the node.
        if not self._executor_set:
            asyncio.get_running_loop().set_default_executor(
                ThreadPoolExecutor(max_workers=_EXECUTOR_THREADS)
            )
            self._executor_set = True

    async def add(
        self,
        sandbox_id: str,
        spec: SandboxSpec,
        settings: HostSettings,
        reservation: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Register a sandbox and start booting it. Idempotent.

        A create that a warm template matches takes a pre-booted sandbox
        instead, which is running already.
        """
        self._use_executor()
        if sandbox_id not in self._sandboxes:
            warm = self._take_warm(spec)
            if warm is not None:
                warm.adopt(
                    sandbox_id,
                    spec,
                    settings,
                    on_exit=lambda: self._release(sandbox_id),
                )
                self._sandboxes[sandbox_id] = warm
            else:
                self._sandboxes[sandbox_id] = self._host_class(
                    sandbox_id,
                    spec,
                    settings,
                    on_exit=lambda: self._release(sandbox_id),
                )
                self._spawn(self._sandboxes[sandbox_id].boot())
            if reservation:
                self._reservations[sandbox_id] = reservation
            # Every create tops the pools up, which also fills them on a host
            # that prestart never reached (its node joined later) and after
            # a fill whose boots failed.
            self._refill()
        return {"ok": True}

    def _take_warm(self, spec: SandboxSpec) -> Optional[SandboxHost]:
        key = warm_key(spec)
        pool = self._warm.get(key) if key is not None else None
        while pool:
            host = pool.popleft()
            if host._status == "running" and not host._terminating:
                if host.container_running():
                    return host
                # Died while pooled: free what it holds, try the next one.
                self._spawn(host._shutdown())
        return None

    def _refill(self) -> None:
        """Boot sandboxes until every warm template's pool is full again."""
        if self._warm_reserve:
            reserve, self._warm_reserve = self._warm_reserve, {}
            self._spawn(self._pregrow(reserve))
        now = time.monotonic()
        for key, (template, size) in self._warm_templates.items():
            failed_at = self._warm_failed_at.get(key)
            if failed_at is not None and now - failed_at < _WARM_RETRY_SECONDS:
                continue
            missing = size - len(self._warm[key]) - self._warm_booting[key]
            for _ in range(max(0, missing)):
                self._warm_booting[key] += 1
                self._spawn(self._boot_warm(key, template))

    async def _pregrow(self, reserve: Dict[tuple, Dict[str, float]]) -> None:
        """Reserve slabs for full warm pools ahead of the creates that use them.

        Each entry is one sandbox's demand times its pool's size. Slabs that
        never held a sandbox are not released for idleness.
        """
        for key, total in reserve.items():
            size = self._warm_templates.get(key, (None, 1))[1]
            one = {k: v / max(1, size) for k, v in total.items()}
            # While it waits, the pool's total rides along with any admit that
            # grows slabs; then only what is still short of the floor grows.
            waiting = dict(total)
            self._waiting.append(waiting)
            try:
                async with self._grow_lock:
                    for k in waiting:
                        floor = self._reserve_floor.get(k, 0.0)
                        waiting[k] = max(0.0, floor - self._held(k))
                    if any(v > _EPSILON for v in waiting.values()):
                        await self._grow(one)
            except Exception as exc:
                logger.warning("Failed to reserve slabs for the warm pool: %s", exc)
            finally:
                self._waiting.remove(waiting)

    def _recent_exec_starts(self, now: float) -> int:
        """Execs this host's sandboxes started in the last second."""
        starts = self._exec_starts
        while starts and starts[0] < now - 1.0:
            starts.popleft()
        return len(starts)

    def _busy(self) -> bool:
        return self._recent_exec_starts(time.monotonic()) > _BUSY_EXECS_PER_SECOND

    async def _boot_warm(self, key: tuple, template: SandboxSpec) -> None:
        booted = False
        try:
            async with self._fill_slots:
                host = self._host_class(
                    f"warm-{uuid.uuid4().hex[:12]}", template, self._warm_settings
                )
                if self._busy():
                    async with self._busy_slot:
                        await host.boot()
                else:
                    await host.boot()
                booted = host._status == "running"
                if not booted:
                    logger.warning("A warm sandbox failed to boot: %s", host._error)
                elif key in self._warm_templates:
                    self._warm[key].append(host)
                else:
                    await host._shutdown()  # its pool was removed meanwhile
        finally:
            if not booted:
                self._warm_failed_at[key] = time.monotonic()
            self._warm_booting[key] -= 1

    async def admit(
        self,
        sandbox_id: str,
        spec: SandboxSpec,
        settings: HostSettings,
        demand: Dict[str, float],
    ) -> Dict[str, Any]:
        """Reserve ``demand`` from this host's slabs and add the sandbox.

        A slab is one placement group on this node that many sandboxes share,
        so a burst of creates costs Ray a reservation per slab instead of per
        sandbox. When no slab has room, the host reserves another, at least
        ``slab`` and at least ``demand``, aimed at its own node; one Ray puts
        elsewhere (the node is full) is released again and the host reports
        ``{"error_code": "full"}``, so the caller tries another host.
        """
        self._use_executor()
        if sandbox_id in self._sandboxes:
            return {"ok": True}
        slab = self._fitting_slab(demand)
        loop = asyncio.get_running_loop()
        if slab is None and loop.time() < self._full_until:
            return {"error_code": "full", "message": f"node {self._node_id} is full"}
        if slab is None:
            # Admits that arrive while slabs grow wait here; the grower
            # reserves for all of them at once.
            self._waiting.append(demand)
            try:
                async with self._grow_lock:
                    slab = self._fitting_slab(demand) or await self._grow(demand)
            finally:
                self._waiting.remove(demand)
        if slab is None:
            self._full_until = loop.time() + _FULL_SECONDS
            return {"error_code": "full", "message": f"node {self._node_id} is full"}
        # A repeat of this admit (its reply was lost) may have finished
        # while this one waited; nothing yields from here to add().
        if sandbox_id in self._sandboxes:
            return {"ok": True}
        slab.take(sandbox_id, demand)
        self._slab_of[sandbox_id] = slab
        return await self.add(sandbox_id, spec, settings)

    def _fitting_slab(self, demand: Dict[str, float]) -> Optional[_Slab]:
        for slab in self._slabs:
            if slab.fits(demand):
                return slab
        return None

    async def _grow(self, demand: Dict[str, float]) -> Optional[_Slab]:
        """Reserve slabs for every waiting admit; the one that fits ``demand``.

        The slabs for all the admits waiting right now are reserved at once,
        so a burst on a node waits for one round of reservations rather
        than one per slab. None when the node has no room.
        """
        need = {k: 0.0 for k in demand}
        for waiting in self._waiting:
            for k in need:
                need[k] += waiting.get(k, 0.0)
        # Never reserve past what the node has free: slabs that don't fit go
        # to other nodes (or pend), and capacity reserved but unused here is
        # capacity other hosts can't use.
        free = await asyncio.to_thread(self._node_free)
        shapes = []
        while True:
            shape = {}
            for k, v in demand.items():
                want = max(v, self._slab_shape.get(k, 0.0))
                if free is not None and k in free:
                    want = min(want, free[k])
                shape[k] = want
            if any(shape[k] + _EPSILON < demand[k] for k in demand):
                break  # no room left for even this sandbox
            shapes.append(shape)
            for k in demand:
                need[k] -= shape[k]
                if free is not None and k in free:
                    free[k] -= shape[k]
            if all(need[k] <= _EPSILON for k in demand) or len(shapes) >= 64:
                break
        if not shapes:
            return None
        slabs = await asyncio.gather(*(self._grow_one(shape) for shape in shapes))
        grown = [slab for slab in slabs if slab is not None]
        return self._fitting_slab(demand) if grown else None

    def _node_free(self) -> Optional[Dict[str, float]]:
        """This node's unreserved resources, or None if Ray can't tell."""
        try:
            import ray

            return dict(
                ray._private.state.available_resources_per_node()[self._node_id]
            )
        except Exception:
            return None

    async def _grow_one(self, shape: Dict[str, float]) -> Optional[_Slab]:
        """Reserve one slab of ``shape`` on this node, or None if it lands elsewhere."""
        from ray.util.placement_group import (
            placement_group,
            placement_group_table,
            remove_placement_group,
        )

        # Not detached: Ray removes a slab once the actor that made it (this
        # host) exits, however it exits, and the sandboxes in it go with it.
        reservation = await asyncio.to_thread(
            placement_group,
            [shape],
            strategy="STRICT_PACK",
            _soft_target_node_id=self._node_id,
        )
        try:
            await asyncio.wait_for(reservation.ready(), _SLAB_PLACEMENT_SECONDS)
            table = await asyncio.to_thread(placement_group_table, reservation)
            placed = next(iter(table["bundles_to_node_id"].values()))
        except Exception:
            placed = None
        if placed == self._node_id:
            return self._add_slab(reservation, shape)
        await asyncio.to_thread(remove_placement_group, reservation)
        return None

    def _add_slab(self, reservation: Any, shape: Dict[str, float]) -> _Slab:
        slab = _Slab(reservation, shape)
        self._slabs.append(slab)
        # Idle from the start, not only once a sandbox leaves: a slab no
        # admit ends up taking (a repeated admit whose sandbox was already
        # added, say) would stay reserved for good. A take cancels this, and
        # the warm pools' floor keeps the slabs it needs (see _drop_slab).
        slab.idle_timer = asyncio.get_running_loop().call_later(
            _SLAB_IDLE_SECONDS, self._drop_slab, slab
        )
        return slab

    def _release_slab_share(self, sandbox_id: str) -> None:
        slab = self._slab_of.pop(sandbox_id, None)
        if slab is None:
            return
        slab.give_back(sandbox_id)
        self._full_until = 0.0  # room again
        if (
            not slab.sandboxes
            and slab.idle_timer is None
            and not self._within_floor(slab)
        ):
            slab.idle_timer = asyncio.get_running_loop().call_later(
                _SLAB_IDLE_SECONDS, self._drop_slab, slab
            )

    def _held(self, k: str) -> float:
        """How much of resource ``k`` this host's slabs hold."""
        return sum(s.capacity.get(k, 0.0) for s in self._slabs)

    def _within_floor(self, slab: _Slab) -> bool:
        """True if releasing ``slab`` would drop below the warm pools' reservation."""
        for k, floor in self._reserve_floor.items():
            if self._held(k) - slab.capacity.get(k, 0.0) < floor - _EPSILON:
                return True
        return False

    def _drop_slab(self, slab: _Slab) -> None:
        slab.idle_timer = None
        # The floor again: other idle slabs may have gone since this timer
        # started.
        if slab.sandboxes or slab not in self._slabs or self._within_floor(slab):
            return
        self._slabs.remove(slab)
        self._spawn(self._remove_group(slab.reservation, "a sandbox slab"))

    async def _remove_group(self, reservation: Any, what: str) -> None:
        """Remove a placement group (or the one a hex id names) from a worker
        thread: it's a GCS call."""
        from ray._raylet import PlacementGroupID
        from ray.util.placement_group import PlacementGroup, remove_placement_group

        try:
            if isinstance(reservation, str):
                reservation = PlacementGroup(PlacementGroupID.from_hex(reservation))
            await asyncio.to_thread(remove_placement_group, reservation)
        except Exception as exc:
            logger.warning("Failed to release %s: %s", what, exc)

    def _release(self, sandbox_id: str) -> None:
        """Forget a sandbox whose container is gone and free its reservation."""
        self._sandboxes.pop(sandbox_id, None)
        self._release_slab_share(sandbox_id)
        reservation = self._reservations.pop(sandbox_id, None)
        if reservation:
            self._spawn(
                self._remove_group(
                    reservation, f"the reservation of sandbox {sandbox_id}"
                )
            )

    async def boot(self, sandbox_id: str) -> None:
        host = self._sandboxes.get(sandbox_id)
        if host is not None:
            await host.boot()

    async def describe(
        self, sandbox_id: str, wait_seconds: float = 0.0
    ) -> Dict[str, Any]:
        host = self._sandboxes.get(sandbox_id)
        if host is None:
            return {
                **_gone(sandbox_id),
                "sandbox_id": sandbox_id,
                "status": "terminated",
                "error": None,
            }
        return await host.describe(wait_seconds)

    async def start_exec(
        self, sandbox_id: str, *args: Any, **kwargs: Any
    ) -> Dict[str, Any]:
        host = self._sandboxes.get(sandbox_id)
        now = time.monotonic()
        self._recent_exec_starts(now)
        self._exec_starts.append(now)
        if host is None:
            return _gone(sandbox_id)
        return await host.start_exec(*args, **kwargs)

    async def get_exec(
        self, sandbox_id: str, exec_id: str, wait_seconds: float = 0.0
    ) -> Dict[str, Any]:
        host = self._sandboxes.get(sandbox_id)
        if host is None:
            return {
                "error_code": "exec_not_found",
                "message": f"unknown exec id {exec_id!r}",
            }
        return await host.get_exec(exec_id, wait_seconds)

    async def get_exec_by_key(
        self, sandbox_id: str, exec_key: str, wait_seconds: float = 0.0
    ) -> Dict[str, Any]:
        host = self._sandboxes.get(sandbox_id)
        if host is None:
            return {
                "error_code": "exec_not_found",
                "message": f"unknown exec {exec_key!r}",
            }
        return await host.get_exec_by_key(exec_key, wait_seconds)

    async def fs_read(
        self, sandbox_id: str, exec_key: str, path: str
    ) -> Dict[str, Any]:
        host = self._sandboxes.get(sandbox_id)
        if host is None:
            return _gone(sandbox_id)
        return await host.fs_read(exec_key, path)

    async def fs_write_open(
        self, sandbox_id: str, exec_key: str, path: str
    ) -> Dict[str, Any]:
        host = self._sandboxes.get(sandbox_id)
        if host is None:
            return _gone(sandbox_id)
        return await host.fs_write_open(exec_key, path)

    async def stdin_write(
        self,
        sandbox_id: str,
        exec_key: str,
        data: bytes,
        offset: int,
        eof: bool = False,
    ) -> Dict[str, Any]:
        host = self._sandboxes.get(sandbox_id)
        if host is None:
            return {
                "error_code": "exec_not_found",
                "message": f"unknown exec {exec_key!r}",
            }
        return await host.stdin_write(exec_key, data, offset, eof)

    async def stdin_status(self, sandbox_id: str, exec_key: str) -> Dict[str, Any]:
        host = self._sandboxes.get(sandbox_id)
        if host is None:
            return {
                "error_code": "exec_not_found",
                "message": f"unknown exec {exec_key!r}",
            }
        return await host.stdin_status(exec_key)

    async def write_file(
        self, sandbox_id: str, path: str, content: bytes, append: bool = False
    ) -> Dict[str, Any]:
        host = self._sandboxes.get(sandbox_id)
        if host is None:
            return _gone(sandbox_id)
        return await host.write_file(path, content, append)

    async def read_file(self, sandbox_id: str, path: str) -> Dict[str, Any]:
        host = self._sandboxes.get(sandbox_id)
        if host is None:
            return _gone(sandbox_id)
        return await host.read_file(path)

    async def terminate(self, sandbox_id: str) -> Dict[str, Any]:
        """Delete the sandbox; it leaves this host shortly after (see SandboxHost)."""
        host = self._sandboxes.get(sandbox_id)
        if host is None:
            return {"ok": True}
        return await host.terminate()

    async def remove(self, sandbox_id: str) -> None:
        """Delete the sandbox and leave at once (the per-actor ``ray.kill``)."""
        host = self._sandboxes.get(sandbox_id)
        if host is not None:
            await host._shutdown()
        self._release(sandbox_id)

    async def channel_endpoint(self) -> Dict[str, Any]:
        """Where and how to connect a host channel; serves one from the first call."""
        import ray

        if self._channel is None:
            self._channel = HostChannelServer(self, CHANNEL_METHODS)
        return await self._channel.start(ray.util.get_node_ip_address())

    async def sandbox_ids(self) -> list:
        """The sandboxes hosted here; also starts filling the warm pool."""
        self._use_executor()
        self._refill()
        return list(self._sandboxes)

    def warm_status(self) -> Dict[str, Any]:
        return {
            "ready": {str(k[:2]): len(v) for k, v in self._warm.items()},
            "booting": {str(k[:2]): n for k, n in self._warm_booting.items()},
        }
