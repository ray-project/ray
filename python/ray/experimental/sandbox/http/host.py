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
import uuid
from collections import OrderedDict
from datetime import datetime, timedelta, timezone
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
    TypedDict,
    Union,
)

from ray.experimental.sandbox.backend.base import SandboxStatus
from ray.experimental.sandbox.exceptions import SandboxError, SandboxTimeoutError
from ray.experimental.sandbox.http.schemas import DOCKER_DEFAULT_CAPABILITIES
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    from ray.experimental.sandbox.runtime import SandboxRuntime

logger = logging.getLogger(__name__)

_TERMINAL_EXEC_STATUSES = ("completed", "timeout", "error")
_BOOTING_STATUSES = ("pending", "pulling", "starting")


class _SandboxSpecBase(TypedDict):
    image: str


@DeveloperAPI
class SandboxSpec(_SandboxSpecBase, total=False):
    """What a ``SandboxHost`` boots: validated creation-request data.

    Built by the REST layer and the gRPC facade from their own request
    types. Every key but ``image`` is optional and falls back to the
    ``SandboxConfig`` default.
    """

    env: Dict[str, str]
    workdir: Optional[str]
    ttl_seconds: Optional[int]
    network: str
    dns: Optional[List[str]]
    shell: Optional[str]
    rootless: bool
    readonly: bool
    capabilities: Optional[List[str]]
    cpu_limit: Optional[float]
    memory_limit_mb: Optional[int]
    image_pull_timeout_seconds: float
    start_timeout_seconds: float
    labels: Dict[str, str]


@DeveloperAPI
class HostSettings(TypedDict, total=False):
    """Server limits a ``SandboxHost`` enforces, taken from ``SandboxAPISettings``."""

    max_output_bytes: int
    max_exec_history: int
    max_file_bytes: int


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


# Grace between a completed terminate() and the host killing itself, so the
# reply to the HTTP layer is delivered before the actor disappears.
_TERMINATE_EXIT_DELAY_SECONDS = 5.0


# File writes buffered from a client's stdin go to the sandbox in slices.
_WRITE_SLICE_BYTES = 4 * 1024 * 1024


class _ExecJob:
    """One submitted command, or one file operation, and its (eventual) result.

    ``kind`` is "command" (a process in the sandbox), "fs_read" (stdout holds
    the file's bytes), or "fs_write" (stdin is buffered here, then written to
    ``fs_path`` when it closes).
    """

    def __init__(self, exec_id: str, kind: str = "command") -> None:
        self.exec_id = exec_id
        self.kind = kind
        self.status = "running"
        self.exit_code: Optional[int] = None
        self.stdout: Optional[str] = None
        self.stderr: Optional[str] = None
        self.stdout_truncated = False
        self.stderr_truncated = False
        self.duration_seconds: Optional[float] = None
        self.error: Optional[str] = None
        self.error_code: Optional[str] = None
        self.done = asyncio.Event()
        # fs_read: the file's content.
        self.content: Optional[bytes] = None
        # fs_write: the target and the stdin buffered so far.
        self.fs_path: Optional[str] = None
        self.stdin_chunks: List[bytes] = []
        self.stdin_bytes = 0
        self.stdin_closed = False

    def finish(
        self,
        status: str,
        exit_code: Optional[int] = None,
        error: Optional[str] = None,
        error_code: Optional[str] = None,
    ) -> None:
        if self.done.is_set():
            # Already settled, say by a terminate while a file op ran: a
            # late result must not turn that into a success.
            return
        self.status = status
        self.exit_code = exit_code
        self.error = error
        self.error_code = error_code
        self.done.set()

    def to_dict(self) -> Dict[str, Any]:
        if self.kind != "command":
            return {
                "exec_id": self.exec_id,
                "kind": self.kind,
                "status": self.status,
                "exit_code": self.exit_code,
                "content": self.content,
                "error": self.error,
                # Not "error_code", which marks a failed call, not a failed job.
                "failure_code": self.error_code,
            }
        return {
            "exec_id": self.exec_id,
            "kind": self.kind,
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
        spec: Sandbox creation spec (validated request data).
        settings: Server limits (``max_output_bytes``, ``max_exec_history``).
        runtime_factory: Test seam; defaults to ``SandboxRuntime``.
        on_exit: Called instead of killing this actor once the sandbox is
            gone (terminate or TTL), for a host that shares its actor with
            other sandboxes (``SandboxNodeHost``).
    """

    def __init__(
        self,
        sandbox_id: str,
        spec: SandboxSpec,
        settings: HostSettings,
        runtime_factory: Optional[Callable[[], "SandboxRuntime"]] = None,
        on_exit: Optional[Callable[[], None]] = None,
    ) -> None:
        self._sandbox_id = sandbox_id
        self._on_exit = on_exit
        self._spec = spec
        # Set when a pre-booted sandbox is adopted (see adopt): the create's
        # env, which the container booted without, goes on every exec.
        self._exec_env: Dict[str, str] = {}
        self._max_output_bytes = int(settings.get("max_output_bytes", 10 * 1024**2))
        self._max_exec_history = int(settings.get("max_exec_history", 256))
        self._max_file_bytes = int(settings.get("max_file_bytes", 256 * 1024**2))
        self._runtime_factory = runtime_factory
        self._runtime: Optional["SandboxRuntime"] = None
        self._instance_id: Optional[str] = None
        self._status = "pending"
        self._error: Optional[str] = None
        self._created_at = datetime.now(timezone.utc)
        self._status_changed = asyncio.Event()
        self._execs: "OrderedDict[str, _ExecJob]" = OrderedDict()
        # A client's own exec id -> the exec id here: a start retried with the
        # same key (by any API replica) joins the first one.
        self._exec_keys: Dict[str, str] = {}
        self._exec_tasks: Dict[str, asyncio.Task] = {}
        self._ttl_task: Optional[asyncio.Task] = None
        self._boot_started = False
        # Set by _shutdown. boot() checks it after every step, so a
        # terminate() or TTL that races the boot never leaves a container
        # behind.
        self._terminating = False
        # True while runtime.create runs; _shutdown waits for it to settle
        # (the new instance lands in _instance_id) before deleting.
        self._creating = False
        self._create_settled = asyncio.Event()

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
        if self._terminating:
            # terminate() ran before this call was scheduled: there is
            # nothing to start and the host is on its way out.
            return

        ttl_seconds = self._spec.get("ttl_seconds")
        if ttl_seconds is not None:
            # Started before the boot work so even a sandbox stuck in a
            # failing boot is eventually reclaimed.
            self._ttl_task = asyncio.create_task(self._ttl_watchdog(ttl_seconds))

        try:
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
            if self._terminating:
                # terminate() arrived during the pull. No container exists
                # yet, so there is nothing to clean up.
                return

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
            self._creating = True
            try:
                self._instance_id = await asyncio.to_thread(
                    self._runtime.create,
                    self._spec["image"],
                    # 0 means "no cgroup limit": without cpu_limit the sandbox
                    # may use idle CPU on its node (a request only sizes the
                    # hosting actor's reservation).
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
                    timeout_seconds=float(
                        self._spec.get("start_timeout_seconds", 60.0)
                    ),
                    rootless=bool(self._spec.get("rootless", True)),
                    network=self._spec.get("network", "none"),
                    dns=self._spec.get("dns"),
                    capabilities=capabilities,
                    readonly=bool(self._spec.get("readonly", True)),
                    **create_kwargs,
                )
            finally:
                self._creating = False
                self._create_settled.set()
            if self._terminating:
                # terminate() arrived while the container was starting; it
                # waited on _create_settled and deletes the new instance.
                return

            self._set_status("running")
            logger.info(
                "Sandbox %s running (instance %s, image %s)",
                self._sandbox_id,
                self._instance_id,
                self._spec["image"],
            )
        except Exception as exc:
            if self._terminating:
                # A boot failure after terminate() must not flip the host
                # from terminated back to error.
                return
            # The actor stays alive holding the error so clients can read it;
            # DELETE or the TTL reclaims it.
            logger.warning("Sandbox %s failed to boot: %s", self._sandbox_id, exc)
            self._error = str(exc)
            self._set_status("error")
            await self._delete_sandbox_instance()

    def adopt(
        self,
        sandbox_id: str,
        spec: SandboxSpec,
        settings: HostSettings,
        on_exit: Optional[Callable[[], None]] = None,
    ) -> None:
        """Take over this running, pre-booted sandbox for a new create.

        The container booted from a template spec (same image and isolation
        settings, see ``SandboxNodeHost``); the create's id, TTL, labels,
        and limits apply from now on, and its env goes on every exec.
        """
        self._sandbox_id = sandbox_id
        self._spec = spec
        self._exec_env = dict(spec.get("env") or {})
        self._max_output_bytes = int(
            settings.get("max_output_bytes", self._max_output_bytes)
        )
        self._max_exec_history = int(
            settings.get("max_exec_history", self._max_exec_history)
        )
        self._max_file_bytes = int(settings.get("max_file_bytes", self._max_file_bytes))
        self._on_exit = on_exit
        self._created_at = datetime.now(timezone.utc)
        ttl_seconds = spec.get("ttl_seconds")
        if ttl_seconds is not None and self._ttl_task is None:
            self._ttl_task = asyncio.create_task(self._ttl_watchdog(ttl_seconds))

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

        A hosted sandbox (``on_exit`` set) leaves its shared host instead.

        ``ray.kill`` on the self-handle (rather than ``exit_actor``) because
        the watchdog runs as a self-spawned asyncio task, outside any Ray
        method invocation, where ``exit_actor``'s control-flow exception has
        nothing to catch it. No-op outside an actor (unit tests).
        """
        if self._on_exit is not None:
            self._on_exit()
            return
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
        """Cancel work and delete the sandbox instance. Idempotent.

        Safe against a boot still in flight: ``_terminating`` stops boot()
        from starting a container, and one that is already starting is
        waited for and deleted here, so the host never exits (and orphans
        it) before the container is gone.
        """
        self._terminating = True
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
        if self._creating:
            await self._create_settled.wait()
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

        The HTTP layer kills the actor as soon as this returns; splitting the
        two keeps this method's reply deliverable. The host also schedules
        its own exit so that a delete whose HTTP call timed out (a slow
        ``runsc delete``, or an actor that was still being scheduled) never
        leaves an idle actor behind: the container is gone by then and the
        delay only lets the reply reach a caller that is still waiting.
        """
        await self._shutdown()
        asyncio.get_running_loop().call_later(
            _TERMINATE_EXIT_DELAY_SECONDS, self._self_destruct
        )
        return {"ok": True}

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    def container_running(self) -> bool:
        """Whether the runtime still has this sandbox's container running."""
        if self._runtime is None or self._instance_id is None:
            return False
        return self._runtime.get_status(self._instance_id) == SandboxStatus.RUNNING

    def _not_running_error(self) -> Optional[Dict[str, Any]]:
        """The conflict error for work on a sandbox that isn't running, or None.

        Checks ``_terminating`` as well as the status: ``_shutdown`` reports
        ``terminated`` only after the container is deleted, and work accepted
        in that window would run against a sandbox being torn down.
        """
        if self._terminating:
            return {
                "error_code": "conflict",
                "message": f"sandbox {self._sandbox_id} is terminating",
            }
        if self._status != "running":
            return {
                "error_code": "conflict",
                "message": (
                    f"sandbox {self._sandbox_id} is {self._status}, not running"
                ),
            }
        return None

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

    def _keyed_job(self, exec_key: Optional[str]) -> Optional[_ExecJob]:
        if exec_key is None:
            return None
        exec_id = self._exec_keys.get(exec_key)
        return self._execs.get(exec_id) if exec_id is not None else None

    def _new_job(self, exec_key: Optional[str], kind: str = "command") -> _ExecJob:
        exec_id = f"ex-{uuid.uuid4().hex[:12]}"
        job = _ExecJob(exec_id, kind)
        self._execs[exec_id] = job
        if exec_key is not None:
            self._exec_keys[exec_key] = exec_id
        self._prune_exec_history()
        return job

    async def start_exec(
        self,
        command: Union[str, List[str]],
        cwd: Optional[str] = None,
        env: Optional[Dict[str, str]] = None,
        timeout_seconds: Optional[float] = None,
        shell: Optional[str] = None,
        user: Optional[str] = None,
        exec_key: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Start a command; ``exec_key`` makes a retried start join the first."""
        existing = self._keyed_job(exec_key)
        if existing is not None:
            return {"exec_id": existing.exec_id, "status": existing.status}
        error = self._not_running_error()
        if error is not None:
            return error
        job = self._new_job(exec_key)
        exec_id = job.exec_id
        task = asyncio.create_task(
            self._run_exec(job, command, cwd, env, timeout_seconds, shell, user)
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
        evicted = set(finished[: max(0, excess)])
        for exec_id in evicted:
            del self._execs[exec_id]
        if evicted and self._exec_keys:
            self._exec_keys = {
                key: exec_id
                for key, exec_id in self._exec_keys.items()
                if exec_id not in evicted
            }

    async def _run_exec(
        self,
        job: _ExecJob,
        command: Union[str, List[str]],
        cwd: Optional[str],
        env: Optional[Dict[str, str]],
        timeout_seconds: Optional[float],
        shell: Optional[str],
        user: Optional[str],
    ) -> None:
        if self._exec_env:
            env = {**self._exec_env, **(env or {})}
        try:
            result = await self._runtime.exec_async(
                self._instance_id,
                command,
                timeout=timeout_seconds,
                cwd=cwd,
                env=env or None,
                shell=shell,
                user=user,
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

    async def get_exec_by_key(
        self, exec_key: str, wait_seconds: float = 0.0
    ) -> Dict[str, Any]:
        """``get_exec`` for a job started with ``exec_key``."""
        job = self._keyed_job(exec_key)
        if job is None:
            return {
                "error_code": "exec_not_found",
                "message": f"unknown exec {exec_key!r}",
            }
        return await self.get_exec(job.exec_id, wait_seconds)

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
    # File operations as exec jobs (for clients that run file operations
    # as commands, such as the gRPC facade's): their results and buffered
    # stdin live here, so any API replica can serve any of the calls.
    # ------------------------------------------------------------------

    async def fs_read(self, exec_key: str, path: str) -> Dict[str, Any]:
        """Read ``path`` into a finished fs_read job. Idempotent per key."""
        job = self._keyed_job(exec_key)
        if job is not None:
            await job.done.wait()
            return {"exec_id": job.exec_id, "status": job.status}
        error = self._not_running_error()
        if error is not None:
            return error
        job = self._new_job(exec_key, "fs_read")
        try:
            result = await self.read_file(path)
        except Exception as exc:  # the job must finish, or its readers wait forever
            logger.warning(
                "Reading %s in sandbox %s failed: %s", path, self._sandbox_id, exc
            )
            result = {"error_code": "read_failed", "message": f"read failed: {exc}"}
        if result.get("error_code"):
            job.finish("error", 1, result.get("message"), result["error_code"])
        elif not job.done.is_set():
            job.content = result["content"]
            job.finish("completed", 0)
        return {"exec_id": job.exec_id, "status": job.status}

    async def fs_write_open(self, exec_key: str, path: str) -> Dict[str, Any]:
        """Start an fs_write job: stdin goes to ``path`` when it closes."""
        job = self._keyed_job(exec_key)
        if job is not None:
            return {"exec_id": job.exec_id, "status": job.status}
        error = self._not_running_error()
        if error is not None:
            return error
        job = self._new_job(exec_key, "fs_write")
        job.fs_path = path
        return {"exec_id": job.exec_id, "status": job.status}

    async def stdin_write(
        self, exec_key: str, data: bytes, offset: int, eof: bool = False
    ) -> Dict[str, Any]:
        """Buffer stdin for an fs_write job; at ``eof``, write the file.

        Returns the job's stdin state, or an ``error_code``: exec_not_found,
        stdin_unsupported (commands get no stdin), offset_mismatch, or
        too_large (over ``max_file_bytes``; the job fails and takes no more
        input). A repeat of the last accepted chunk, as when its reply was
        lost, is acknowledged without appending it again.
        """
        job = self._keyed_job(exec_key)
        if job is None:
            return {
                "error_code": "exec_not_found",
                "message": f"unknown exec {exec_key!r}",
            }
        if job.kind != "fs_write":
            return {
                "error_code": "stdin_unsupported",
                "message": "commands take no stdin",
            }
        if job.error_code == "too_large":
            return {"error_code": "too_large", "message": job.error}
        if data and not job.stdin_closed:
            if offset != job.stdin_bytes:
                if (
                    job.stdin_chunks
                    and offset + len(data) == job.stdin_bytes
                    and job.stdin_chunks[-1] == data
                ):
                    return {
                        "num_bytes_written": job.stdin_bytes,
                        "closed": job.stdin_closed,
                    }
                return {
                    "error_code": "offset_mismatch",
                    "message": f"stdin offset {offset} != {job.stdin_bytes}",
                }
            if job.stdin_bytes + len(data) > self._max_file_bytes:
                job.stdin_chunks.clear()
                job.stdin_closed = True
                job.finish(
                    "error",
                    1,
                    f"file exceeds the {self._max_file_bytes}-byte max_file_bytes",
                    "too_large",
                )
                return {"error_code": "too_large", "message": job.error}
            job.stdin_chunks.append(data)
            job.stdin_bytes += len(data)
        if eof and not job.stdin_closed:
            job.stdin_closed = True
            await self._finish_write(job)
        return {"num_bytes_written": job.stdin_bytes, "closed": job.stdin_closed}

    async def _finish_write(self, job: _ExecJob) -> None:
        content = b"".join(job.stdin_chunks)
        job.stdin_chunks.clear()
        try:
            for start in range(0, max(len(content), 1), _WRITE_SLICE_BYTES):
                result = await self.write_file(
                    job.fs_path,
                    content[start : start + _WRITE_SLICE_BYTES],
                    append=start > 0,
                )
                if result.get("error_code"):
                    job.finish(
                        "error",
                        1,
                        result.get("message", "write failed"),
                        "write_failed",
                    )
                    return
        except Exception as exc:  # stdin is closed: only finishing ends the job
            logger.warning(
                "Writing %s in sandbox %s failed: %s",
                job.fs_path,
                self._sandbox_id,
                exc,
            )
            job.finish("error", 1, f"write failed: {exc}", "write_failed")
            return
        job.finish("completed", 0)

    async def stdin_status(self, exec_key: str) -> Dict[str, Any]:
        job = self._keyed_job(exec_key)
        if job is None:
            return {
                "error_code": "exec_not_found",
                "message": f"unknown exec {exec_key!r}",
            }
        return {"num_bytes_written": job.stdin_bytes, "closed": job.stdin_closed}

    # ------------------------------------------------------------------
    # Files
    # ------------------------------------------------------------------

    async def write_file(
        self, path: str, content: bytes, append: bool = False
    ) -> Dict[str, Any]:
        error = self._not_running_error()
        if error is not None:
            return error
        try:
            await asyncio.to_thread(
                self._runtime.write_file,
                self._instance_id,
                path,
                content,
                append,
            )
        except SandboxError as exc:
            # A path the sandbox cannot write (a directory, a read-only
            # rootfs outside the workdir) is the caller's error, not a
            # server fault.
            return {"error_code": "write_failed", "message": str(exc)}
        return {"ok": True}

    async def read_file(self, path: str) -> Dict[str, Any]:
        error = self._not_running_error()
        if error is not None:
            return error
        try:
            content = await asyncio.to_thread(
                self._runtime.read_file, self._instance_id, path
            )
        except SandboxError as exc:
            # Upstream read_file shells out to `cat`; a missing file is the
            # overwhelmingly common failure, so report it as such.
            return {"error_code": "file_not_found", "message": str(exc)}
        return {"ok": True, "content": content}
