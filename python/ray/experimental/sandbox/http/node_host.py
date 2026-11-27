"""One actor per node that hosts many API sandboxes.

A ``SandboxHost`` actor per sandbox costs a Ray worker process per sandbox:
starting one takes about a CPU-second, so a burst of creates is bound by how
fast each node can start Python workers (measured: ~24 no-op actors/s per
30-CPU node, and 5 s for 100 sandboxes per node, against 1.2 s for the same
sandboxes without per-sandbox actors). A ``SandboxNodeHost`` runs the same
``SandboxHost`` objects in one long-lived actor per node instead, addressed
by sandbox id. Each sandbox's resources are reserved by its own one-bundle
placement group, which Ray schedules (and the autoscaler sees) like the
actor's own request, without starting a process; the host releases it when
the sandbox goes.

The resolver (``RayActorHandleResolver`` with ``host_mode="node"``) creates
the reservation, picks the host on the node Ray chose, and hands the API
layers a proxy with the ``SandboxHost`` method surface, so they don't change.
"""

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Coroutine, Dict, Optional, Set

from ray.experimental.sandbox.http.host import HostSettings, SandboxHost, SandboxSpec
from ray.util.annotations import DeveloperAPI

logger = logging.getLogger(__name__)

# Threads for the blocking parts of every hosted sandbox (image pulls,
# `runsc run` until running, `runsc exec`), shared by the whole node.
_EXECUTOR_THREADS = 512


def _gone(sandbox_id: str, *_: Any) -> Dict[str, Any]:
    # Never placed here, or already released: the facade answers NOT_FOUND,
    # as for a sandbox actor that no longer exists.
    return {
        "error_code": "sandbox_not_found",
        "message": f"sandbox {sandbox_id!r} not found",
    }


def _terminated(sandbox_id: str, *_: Any) -> Dict[str, Any]:
    return {
        **_gone(sandbox_id),
        "sandbox_id": sandbox_id,
        "status": "terminated",
        "error": None,
    }


def _no_exec(sandbox_id: str, exec_key: str, *_: Any) -> Dict[str, Any]:
    return {"error_code": "exec_not_found", "message": f"unknown exec {exec_key!r}"}


def _no_exec_id(sandbox_id: str, exec_id: str, *_: Any) -> Dict[str, Any]:
    return {"error_code": "exec_not_found", "message": f"unknown exec id {exec_id!r}"}


def _hosted(
    name: str, missing: Callable[..., Any] = _gone
) -> Callable[..., Coroutine[Any, Any, Any]]:
    """The node host's method ``name``: the ``SandboxHost`` method of that
    name, called on the sandbox whose id comes first.

    Args:
        name: The ``SandboxHost`` method.
        missing: What answers for a sandbox this host doesn't have, given
            the call's positional arguments.

    Returns:
        The method.
    """

    async def call(
        self: "SandboxNodeHost", sandbox_id: str, *args: Any, **kwargs: Any
    ) -> Any:
        host = self._sandboxes.get(sandbox_id)
        if host is None:
            return missing(sandbox_id, *args)
        return await getattr(host, name)(*args, **kwargs)

    call.__name__ = name
    call.__qualname__ = f"SandboxNodeHost.{name}"
    return call


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
    """

    def __init__(self, host_class: type = SandboxHost) -> None:
        self._host_class = host_class
        self._sandboxes: Dict[str, SandboxHost] = {}
        # Sandbox id -> its placement group's id (hex).
        self._reservations: Dict[str, str] = {}
        self._executor_set = False
        # Boots run as tasks nobody awaits; the loop keeps only weak
        # references to tasks.
        self._background: Set[asyncio.Task] = set()

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
        reservation = self._reservations.pop(sandbox_id, None)
        if reservation:
            self._spawn(
                self._remove_group(
                    reservation, f"the reservation of sandbox {sandbox_id}"
                )
            )

    # What the API layers call on a sandbox: its id, then the arguments of
    # the SandboxHost method of the same name. A hosted handle calls these by
    # name (see HostedSandboxHandle), so each method the API calls needs one.
    boot = _hosted("boot", lambda *_: None)
    describe = _hosted("describe", _terminated)
    set_network_access = _hosted("set_network_access")
    start_exec = _hosted("start_exec")
    get_exec = _hosted("get_exec", _no_exec_id)
    get_exec_by_key = _hosted("get_exec_by_key", _no_exec)
    fs_read = _hosted("fs_read")
    fs_write_open = _hosted("fs_write_open")
    stdin_write = _hosted("stdin_write", _no_exec)
    stdin_status = _hosted("stdin_status", _no_exec)
    write_file = _hosted("write_file")
    read_file = _hosted("read_file")
    # The sandbox leaves this host shortly after (see SandboxHost.terminate).
    terminate = _hosted("terminate", lambda *_: {"ok": True})

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
