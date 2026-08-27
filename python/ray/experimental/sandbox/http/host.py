"""Per-sandbox actor backing the Ray Sandbox HTTP API.

Each API sandbox is one ``SandboxHost``, created by the HTTP layer as a
*named, detached* Ray actor (name = sandbox id). The detached actors are the
API's registry: any Serve replica resolves a sandbox with ``ray.get_actor``
and the service keeps no state of its own, so replicas can restart or scale
without losing sandboxes.

``SandboxHost`` composes :class:`~ray.experimental.sandbox.runtime.SandboxRuntime`
rather than the ``ray.experimental.sandbox.Sandbox`` actor because the API
needs behavior the upstream actor does not provide:

* **Boot in the background** — the upstream actor pulls the image and boots
  the container inside ``__init__``, so creation errors only surface on the
  first method call. Here ``__init__`` is trivial and ``boot()`` runs as a
  background task, making progress (``pending -> pulling -> starting ->
  running``) and failures pollable over HTTP.
* **Exec as jobs** — commands can outrun any HTTP request (and the load
  balancers in front of an Anyscale service), so ``start_exec`` returns an id
  immediately and results are polled.
* **A TTL that reclaims everything** — the upstream TTL timer deletes the
  sandbox but leaks the hosting actor and its resource reservation; this one
  deletes the sandbox and then kills its own actor.

Capability grants and host-network behavior (netns, resolv.conf) are plain
``SandboxConfig`` fields handled by the core runtime; nothing is patched here.

Cross-actor control flow uses plain dicts (``{"error_code": ...}``) instead
of exceptions: Ray re-raises remote exceptions as dynamically-built
``RayTaskError`` subclasses, which makes matching them in the HTTP layer
fragile. Unexpected exceptions still propagate and map to HTTP 500.
"""

import asyncio
import logging
import os
import platform
import shutil
import tempfile
import urllib.request
import uuid
from collections import OrderedDict
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from ray.experimental.sandbox.exceptions import SandboxError, SandboxTimeoutError
from ray.experimental.sandbox.http.schemas import DOCKER_DEFAULT_CAPABILITIES
from ray.util.annotations import DeveloperAPI

logger = logging.getLogger(__name__)

_TERMINAL_EXEC_STATUSES = ("completed", "timeout", "error")
_BOOTING_STATUSES = ("pending", "pulling", "starting")


def _truncate_output(text: str, max_bytes: int) -> Tuple[str, bool]:
    """Cap *text* at *max_bytes* of UTF-8, with a loud trailing marker."""
    data = text.encode("utf-8", errors="replace")
    if len(data) <= max_bytes:
        return text, False
    clipped = data[:max_bytes].decode("utf-8", errors="replace")
    return (
        clipped + f"\n[truncated by ray-sandbox: output exceeded {max_bytes} bytes]",
        True,
    )


def _ensure_runsc_installed() -> None:
    """Download runsc from the official gVisor release bucket onto this node.

    Opt-in via the ``auto_install_runsc`` server setting, for running the
    service on stock node images. The download lands in a temp directory
    prepended to this process's PATH, which the runsc subprocesses inherit.
    """
    if shutil.which("runsc"):
        return
    temp_bin = tempfile.mkdtemp(prefix="ray-sandbox-runsc-")
    os.chmod(temp_bin, 0o755)
    runsc_path = os.path.join(temp_bin, "runsc")
    arch = "aarch64" if platform.machine().lower() in ("aarch64", "arm64") else "x86_64"
    url = (
        "https://storage.googleapis.com/gvisor/releases/release/latest/" f"{arch}/runsc"
    )
    logger.info("runsc not found on this node; downloading from %s", url)
    urllib.request.urlretrieve(url, runsc_path)
    os.chmod(runsc_path, 0o755)
    os.environ["PATH"] = f"{temp_bin}:{os.environ.get('PATH', '')}"


class _ExecJob:
    """One submitted command and its (eventual) result."""

    def __init__(self, exec_id: str) -> None:
        self.exec_id = exec_id
        self.status = "running"
        self.exit_code: Optional[int] = None
        self.stdout: Optional[str] = None
        self.stderr: Optional[str] = None
        self.stdout_truncated = False
        self.stderr_truncated = False
        self.duration_seconds: Optional[float] = None
        self.error: Optional[str] = None
        self.done = asyncio.Event()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "exec_id": self.exec_id,
            "status": self.status,
            "exit_code": self.exit_code,
            "stdout": self.stdout,
            "stderr": self.stderr,
            "stdout_truncated": self.stdout_truncated,
            "stderr_truncated": self.stderr_truncated,
            "duration_seconds": self.duration_seconds,
            "error": self.error,
        }


@DeveloperAPI
class SandboxHost:
    """Hosts one gVisor sandbox for the HTTP API.

    Instantiated by the HTTP layer via ``ray.remote(SandboxHost)`` as a named
    detached async actor; unit tests instantiate it directly with a fake
    runtime factory, so nothing here may assume a Ray context except the
    self-destruct path (which degrades to a no-op outside an actor).

    Args:
        sandbox_id: The API-level sandbox id; also the detached actor's name.
        spec: Sandbox creation spec (validated request data, plain dict).
        settings: Server limits (``max_output_bytes``, ``max_exec_history``).
        runtime_factory: Test seam; defaults to ``SandboxRuntime``.
    """

    def __init__(
        self,
        sandbox_id: str,
        spec: Dict[str, Any],
        settings: Dict[str, Any],
        runtime_factory: Optional[Callable[[], Any]] = None,
    ) -> None:
        self._sandbox_id = sandbox_id
        self._spec = spec
        self._max_output_bytes = int(settings.get("max_output_bytes", 10 * 1024**2))
        self._max_exec_history = int(settings.get("max_exec_history", 256))
        self._auto_install_runsc = bool(settings.get("auto_install_runsc", False))
        self._runtime_factory = runtime_factory
        self._runtime: Optional[Any] = None
        self._instance_id: Optional[str] = None
        self._status = "pending"
        self._error: Optional[str] = None
        self._created_at = datetime.now(timezone.utc)
        self._status_changed = asyncio.Event()
        self._execs: "OrderedDict[str, _ExecJob]" = OrderedDict()
        self._exec_tasks: Dict[str, asyncio.Task] = {}
        self._ttl_task: Optional[asyncio.Task] = None
        self._boot_started = False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def boot(self) -> None:
        """Pull the image and start the sandbox, recording progress.

        Fired by the HTTP layer right after actor creation and never awaited
        for its result; every outcome (including failure) lands in the status
        this actor reports. Idempotent so a lost-then-retried create (via
        ``get_if_exists``) cannot boot twice.
        """
        if self._boot_started:
            return
        self._boot_started = True

        ttl_seconds = self._spec.get("ttl_seconds")
        if ttl_seconds is not None:
            # Started before the boot work so even a sandbox stuck in a
            # failing boot is eventually reclaimed.
            self._ttl_task = asyncio.create_task(self._ttl_watchdog(ttl_seconds))

        try:
            if self._auto_install_runsc:
                await asyncio.to_thread(_ensure_runsc_installed)
            factory = self._runtime_factory
            if factory is None:
                from ray.experimental.sandbox.runtime import SandboxRuntime

                factory = SandboxRuntime
            self._runtime = factory()

            self._set_status("pulling")
            await asyncio.to_thread(
                self._runtime.pull_image,
                self._spec["image"],
                timeout_seconds=float(
                    self._spec.get("image_pull_timeout_seconds", 600.0)
                ),
            )

            self._set_status("starting")
            capabilities = self._spec.get("capabilities")
            if capabilities is None:
                capabilities = list(DOCKER_DEFAULT_CAPABILITIES)

            memory_limit_mb = self._spec.get("memory_limit_mb")
            cpu_limit = self._spec.get("cpu_limit")
            create_kwargs: Dict[str, Any] = {}
            if self._spec.get("shell") is not None:
                # Omitted rather than passed as None: SandboxConfig.shell is
                # a plain str with a /bin/bash default.
                create_kwargs["shell"] = self._spec["shell"]
            # Requests and limits are deliberately decoupled: cpu_request /
            # memory_request_mb size the hosting actor (cluster scheduling)
            # and only cpu_limit / memory_limit_mb become cgroup caps —
            # unlike the upstream Sandbox actor, which infers a cpu quota
            # from its assigned resources.
            self._instance_id = await asyncio.to_thread(
                self._runtime.create,
                self._spec["image"],
                # 0 means "no cgroup limit" to Ray Sandbox. Note that it still
                # derives a cpu quota from the hosting actor's assigned CPUs
                # when no explicit limit is set.
                cpu=float(cpu_limit) if cpu_limit is not None else 0.0,
                memory=f"{memory_limit_mb}Mi" if memory_limit_mb is not None else 0,
                env=dict(self._spec.get("env") or {}),
                workdir=self._spec.get("workdir"),
                # Writability is the runtime's explicit contract: a scratch
                # dir exists only for an explicitly passed workdir on a
                # readonly rootfs; readonly=False sandboxes are fully
                # writable with image WORKDIR content visible.
                # The API owns the TTL (see _ttl_watchdog); the upstream
                # runtime stores but never enforces this, and the upstream
                # actor's timer would reclaim only the sandbox, not the actor.
                ttl_seconds=None,
                timeout_seconds=float(self._spec.get("start_timeout_seconds", 60.0)),
                rootless=bool(self._spec.get("rootless", True)),
                network=self._spec.get("network", "none"),
                dns=self._spec.get("dns"),
                capabilities=capabilities,
                readonly=bool(self._spec.get("readonly", True)),
                **create_kwargs,
            )

            self._set_status("running")
            logger.info(
                "Sandbox %s running (instance %s, image %s)",
                self._sandbox_id,
                self._instance_id,
                self._spec["image"],
            )
        except Exception as exc:
            # The actor stays alive holding the error so clients can read it;
            # DELETE or the TTL reclaims it.
            logger.warning("Sandbox %s failed to boot: %s", self._sandbox_id, exc)
            self._error = str(exc)
            self._set_status("error")
            await self._delete_sandbox_instance()

    async def _ttl_watchdog(self, ttl_seconds: float) -> None:
        await asyncio.sleep(ttl_seconds)
        logger.info(
            "Sandbox %s reached its TTL (%ss); terminating",
            self._sandbox_id,
            ttl_seconds,
        )
        await self._shutdown()
        self._self_destruct()

    def _self_destruct(self) -> None:
        """Kill this actor so the TTL reclaims its name and reservation.

        ``ray.kill`` on the self-handle (rather than ``exit_actor``) because
        the watchdog runs as a self-spawned asyncio task, outside any Ray
        method invocation, where ``exit_actor``'s control-flow exception has
        nothing to catch it. No-op outside an actor (unit tests).
        """
        try:
            import ray

            handle = ray.get_runtime_context().current_actor
            ray.kill(handle)
        except Exception:
            logger.debug(
                "Sandbox %s host is not a Ray actor; skipping self-destruct",
                self._sandbox_id,
            )

    async def _shutdown(self) -> None:
        """Cancel work and delete the sandbox instance. Idempotent."""
        if self._ttl_task is not None and self._ttl_task is not asyncio.current_task():
            self._ttl_task.cancel()
            self._ttl_task = None
        for task in self._exec_tasks.values():
            task.cancel()
        self._exec_tasks.clear()
        for job in self._execs.values():
            if job.status == "running":
                job.status = "error"
                job.error = "sandbox terminated while the command was running"
                job.done.set()
        await self._delete_sandbox_instance()
        self._set_status("terminated")

    async def _delete_sandbox_instance(self) -> None:
        if self._runtime is None or self._instance_id is None:
            return
        instance_id, self._instance_id = self._instance_id, None
        try:
            await asyncio.to_thread(self._runtime.delete, instance_id)
        except Exception as exc:
            logger.warning("Failed to delete sandbox instance %s: %s", instance_id, exc)

    async def terminate(self) -> Dict[str, Any]:
        """Delete the sandbox and mark this host terminated.

        The HTTP layer kills the actor afterwards; splitting the two keeps
        this method's reply deliverable.
        """
        await self._shutdown()
        return {"ok": True}

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    def _set_status(self, status: str) -> None:
        self._status = status
        # Replace-then-set so current waiters wake once and later waiters
        # block on a fresh event.
        event, self._status_changed = self._status_changed, asyncio.Event()
        event.set()

    def _info(self) -> Dict[str, Any]:
        ttl_seconds = self._spec.get("ttl_seconds")
        expires_at = (
            self._created_at + timedelta(seconds=ttl_seconds)
            if ttl_seconds is not None
            else None
        )
        return {
            "sandbox_id": self._sandbox_id,
            "status": self._status,
            "image": self._spec["image"],
            "created_at": self._created_at.isoformat(),
            "ttl_seconds": ttl_seconds,
            "expires_at": expires_at.isoformat() if expires_at else None,
            "network": self._spec.get("network", "none"),
            "labels": dict(self._spec.get("labels") or {}),
            "error": self._error,
        }

    async def describe(self, wait_seconds: float = 0.0) -> Dict[str, Any]:
        """Report sandbox state, optionally long-polling while it boots."""
        if wait_seconds > 0 and self._status in _BOOTING_STATUSES:
            event = self._status_changed
            try:
                await asyncio.wait_for(event.wait(), timeout=wait_seconds)
            except asyncio.TimeoutError:
                pass
        return self._info()

    # ------------------------------------------------------------------
    # Exec jobs
    # ------------------------------------------------------------------

    async def start_exec(
        self,
        command: Union[str, List[str]],
        cwd: Optional[str] = None,
        env: Optional[Dict[str, str]] = None,
        timeout_seconds: Optional[float] = None,
        shell: Optional[str] = None,
    ) -> Dict[str, Any]:
        if self._status != "running":
            return {
                "error_code": "conflict",
                "message": (
                    f"sandbox {self._sandbox_id} is {self._status}, not running"
                ),
            }
        exec_id = f"ex-{uuid.uuid4().hex[:12]}"
        job = _ExecJob(exec_id)
        self._execs[exec_id] = job
        self._prune_exec_history()
        task = asyncio.create_task(
            self._run_exec(job, command, cwd, env, timeout_seconds, shell)
        )
        self._exec_tasks[exec_id] = task
        task.add_done_callback(lambda _: self._exec_tasks.pop(exec_id, None))
        return {"exec_id": exec_id, "status": job.status}

    def _prune_exec_history(self) -> None:
        # Evict oldest *finished* jobs beyond the cap; running jobs must stay
        # addressable, so with a pathological number in flight the dict may
        # exceed the cap rather than lose one.
        finished = [
            exec_id
            for exec_id, job in self._execs.items()
            if job.status in _TERMINAL_EXEC_STATUSES
        ]
        excess = len(self._execs) - self._max_exec_history
        for exec_id in finished[: max(0, excess)]:
            del self._execs[exec_id]

    async def _run_exec(
        self,
        job: _ExecJob,
        command: Union[str, List[str]],
        cwd: Optional[str],
        env: Optional[Dict[str, str]],
        timeout_seconds: Optional[float],
        shell: Optional[str],
    ) -> None:
        try:
            result = await self._runtime.exec_async(
                self._instance_id,
                command,
                timeout=timeout_seconds,
                cwd=cwd,
                env=env or None,
                shell=shell,
            )
        except asyncio.CancelledError:
            # _shutdown already marked the job; just stop.
            raise
        except SandboxTimeoutError:
            job.status = "timeout"
            job.error = f"command timed out after {timeout_seconds} seconds"
        except SandboxError as exc:
            job.status = "error"
            job.error = str(exc)
        except Exception as exc:
            logger.warning(
                "Exec %s in sandbox %s failed unexpectedly: %s",
                job.exec_id,
                self._sandbox_id,
                exc,
            )
            job.status = "error"
            job.error = str(exc)
        else:
            job.status = "completed"
            job.exit_code = result.exit_code
            job.stdout, job.stdout_truncated = _truncate_output(
                result.stdout, self._max_output_bytes
            )
            job.stderr, job.stderr_truncated = _truncate_output(
                result.stderr, self._max_output_bytes
            )
            job.duration_seconds = result.duration_seconds
        finally:
            if not job.done.is_set():
                job.done.set()

    async def get_exec(self, exec_id: str, wait_seconds: float = 0.0) -> Dict[str, Any]:
        job = self._execs.get(exec_id)
        if job is None:
            return {
                "error_code": "exec_not_found",
                "message": f"unknown exec id {exec_id!r}",
            }
        if wait_seconds > 0 and job.status == "running":
            try:
                await asyncio.wait_for(job.done.wait(), timeout=wait_seconds)
            except asyncio.TimeoutError:
                pass
        return job.to_dict()

    # ------------------------------------------------------------------
    # Files
    # ------------------------------------------------------------------

    async def write_file(
        self, path: str, content: bytes, append: bool = False
    ) -> Dict[str, Any]:
        if self._status != "running":
            return {
                "error_code": "conflict",
                "message": (
                    f"sandbox {self._sandbox_id} is {self._status}, not running"
                ),
            }
        await asyncio.to_thread(
            self._runtime.write_file,
            self._instance_id,
            path,
            content,
            append,
        )
        return {"ok": True}

    async def read_file(self, path: str) -> Dict[str, Any]:
        if self._status != "running":
            return {
                "error_code": "conflict",
                "message": (
                    f"sandbox {self._sandbox_id} is {self._status}, not running"
                ),
            }
        try:
            content = await asyncio.to_thread(
                self._runtime.read_file, self._instance_id, path
            )
        except SandboxError as exc:
            # Upstream read_file shells out to `cat`; a missing file is the
            # overwhelmingly common failure, so report it as such.
            return {"error_code": "file_not_found", "message": str(exc)}
        return {"ok": True, "content": content}
