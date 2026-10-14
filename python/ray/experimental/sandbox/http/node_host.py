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
from typing import Any, Coroutine, Dict, Optional, Set

from ray.experimental.sandbox.http.host import HostSettings, SandboxHost, SandboxSpec
from ray.util.annotations import DeveloperAPI

logger = logging.getLogger(__name__)

# Threads for the blocking parts of every hosted sandbox (image pulls,
# `runsc run` until running, `runsc exec`), shared by the whole node.
_EXECUTOR_THREADS = 512


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
