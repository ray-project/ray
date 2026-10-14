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
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Coroutine, Dict, Optional, Set

from ray.experimental.sandbox.http.host import HostSettings, SandboxHost, SandboxSpec
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
    """

    def __init__(
        self,
        host_class: type = SandboxHost,
        slab: Optional[Dict[str, float]] = None,
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
        # Boots run as tasks nobody awaits; the loop keeps only weak
        # references to tasks.
        self._background: Set[asyncio.Task] = set()

    async def configure(self, slab: Optional[Dict[str, float]]) -> None:
        """Take the settings of the facade that calls this host.

        A host is detached and found by name, so it outlives the deployment
        that started it; each resolver sends its own settings on first use
        (see ``RayActorHandleResolver``). Slabs stay; later reservations
        follow these settings.

        Args:
            slab: As in the constructor.
        """
        self._slab_shape = dict(slab or {})

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
        """Register a sandbox and start booting it. Idempotent."""
        self._use_executor()
        if sandbox_id not in self._sandboxes:
            self._sandboxes[sandbox_id] = self._host_class(
                sandbox_id,
                spec,
                settings,
                on_exit=lambda: self._release(sandbox_id),
            )
            self._spawn(self._sandboxes[sandbox_id].boot())
            if reservation:
                self._reservations[sandbox_id] = reservation
        return {"ok": True}

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
        # added, say) would stay reserved for good. A take cancels this.
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
        if not slab.sandboxes and slab.idle_timer is None:
            slab.idle_timer = asyncio.get_running_loop().call_later(
                _SLAB_IDLE_SECONDS, self._drop_slab, slab
            )

    def _drop_slab(self, slab: _Slab) -> None:
        slab.idle_timer = None
        if slab.sandboxes or slab not in self._slabs:
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

    async def sandbox_ids(self) -> list:
        """The sandboxes hosted here."""
        self._use_executor()
        return list(self._sandboxes)
