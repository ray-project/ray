import asyncio
import collections
import logging
import os
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Union

from ray.experimental.sandbox.backend.base import (
    ExecResult,
    SandboxStatus,
)
from ray.experimental.sandbox.backend.gvisor import GVisorSandboxBackend
from ray.experimental.sandbox.config import SandboxConfig, parse_memory_bytes
from ray.experimental.sandbox.image_manager import ImageManager
from ray.util.annotations import PublicAPI

logger = logging.getLogger(__name__)

# Warm-pool boots run a few at a time, so filling a pool leaves CPU for the
# creates that boot cold meanwhile.
_WARM_BOOT_CONCURRENCY = 4
# After a warm-pool boot fails (say, its image can't be pulled), its profile
# boots no replacements for this long, rather than on every create.
_WARM_RETRY_SECONDS = 30.0


def _boot_key(cfg: SandboxConfig) -> Optional[tuple]:
    """What a booted sandbox must share with a create to stand in for it.

    Everything that is fixed when the sandbox boots; ``env``, ``ttl_seconds``
    and ``timeout_seconds`` are not. None for a create no booted sandbox can
    serve (a custom OCI spec transform).
    """
    if cfg._oci_spec_transform_fn is not None:
        return None
    return (
        cfg.image,
        float(cfg.cpu or 0.0),
        parse_memory_bytes(cfg.memory) or 0,
        cfg.workdir,
        bool(cfg.rootless),
        cfg.network,
        tuple(cfg.dns) if cfg.dns is not None else None,
        tuple(cfg.capabilities) if cfg.capabilities is not None else None,
        cfg.shell,
        bool(cfg.readonly),
        bool(cfg._ignore_cgroups),
    )


class _WarmPool:
    """Booted sandboxes per profile, handed out by ``SandboxRuntime.create``."""

    def __init__(self, runtime: "SandboxRuntime", profiles: List[Dict[str, Any]]):
        self._runtime = runtime
        self._lock = threading.Lock()
        self._slots = threading.Semaphore(_WARM_BOOT_CONCURRENCY)
        # Boot key -> (SandboxConfig to boot with, pool size).
        self._profiles: Dict[tuple, Any] = {}
        self._ready: Dict[tuple, collections.deque] = {}
        self._booting: Dict[tuple, int] = {}
        self._failed_at: Dict[tuple, float] = {}
        self._closed = False
        for profile in profiles:
            kwargs = dict(profile)
            size = int(kwargs.pop("size", 0))
            kwargs.pop("ttl_seconds", None)
            cfg = SandboxConfig(**kwargs)
            key = _boot_key(cfg)
            if key is None or size <= 0:
                raise ValueError(
                    f"Invalid warm_pool profile {profile!r}: it needs a positive "
                    "'size' and no _oci_spec_transform_fn."
                )
            self._profiles[key] = (cfg, size)
            self._ready[key] = collections.deque()
            self._booting[key] = 0
        for key in self._profiles:
            self.refill(key)

    def take(self, cfg: SandboxConfig) -> Optional[str]:
        """A booted sandbox for ``cfg`` (and a replacement booting), or None."""
        key = _boot_key(cfg)
        if key not in self._profiles:
            return None
        instance_id = None
        dead = []
        with self._lock:
            pool = self._ready[key]
            while pool and instance_id is None:
                candidate = pool.popleft()
                if self._runtime.get_status(candidate) == SandboxStatus.RUNNING:
                    instance_id = candidate
                else:
                    dead.append(candidate)
        for candidate in dead:  # died while pooled: free what it holds
            threading.Thread(
                target=self._discard, args=(candidate,), daemon=True
            ).start()
        self.refill(key)
        return instance_id

    def _discard(self, instance_id: str) -> None:
        try:
            self._runtime._backend.delete_sandbox(instance_id)
        except Exception as exc:
            logger.warning(
                "Failed to delete dead warm-pool sandbox %s: %s", instance_id, exc
            )

    def refill(self, key: tuple) -> None:
        with self._lock:
            failed_at = self._failed_at.get(key)
            if self._closed or (
                failed_at is not None
                and time.monotonic() - failed_at < _WARM_RETRY_SECONDS
            ):
                return
            size = self._profiles[key][1]
            missing = max(0, size - len(self._ready[key]) - self._booting[key])
            self._booting[key] += missing
        for _ in range(missing):
            threading.Thread(target=self._boot, args=(key,), daemon=True).start()

    def _boot(self, key: tuple) -> None:
        instance_id = None
        try:
            with self._slots:
                if not self._closed:
                    instance_id = self._runtime._backend.create_sandbox(
                        self._profiles[key][0]
                    )
        except Exception as exc:  # a create after _WARM_RETRY_SECONDS retries
            logger.warning("A warm-pool sandbox failed to boot: %s", exc)
            with self._lock:
                self._failed_at[key] = time.monotonic()
        with self._lock:
            self._booting[key] -= 1
            if instance_id is not None and not self._closed:
                self._ready[key].append(instance_id)
                instance_id = None
        if instance_id is not None:  # closed while it booted
            self._runtime._backend.delete_sandbox(instance_id)

    def full(self) -> bool:
        with self._lock:
            return all(
                len(self._ready[key]) >= size
                for key, (_, size) in self._profiles.items()
            )

    def close(self) -> List[str]:
        """Stop refilling; the booted sandboxes, for the caller to delete."""
        with self._lock:
            self._closed = True
            ready = [i for pool in self._ready.values() for i in pool]
            for pool in self._ready.values():
                pool.clear()
        return ready


@PublicAPI(stability="alpha")
class SandboxRuntime:
    """Low-level interface for managing local sandbox runtime environments.

    Args:
        warm_pool: Sandboxes to keep booted ahead of creates: a list of
            profiles, each the ``create()`` keyword arguments of one boot
            configuration plus its pool ``"size"``. A create whose arguments
            match a profile's (``env``, ``ttl_seconds`` and
            ``timeout_seconds`` aside) takes a booted sandbox at once, and
            the runtime boots a replacement in the background. Such a
            sandbox runs every command with the create's ``env`` (on top of
            the profile's). None or empty keeps no pool.
    """

    def __init__(self, warm_pool: Optional[List[Dict[str, Any]]] = None):
        self._image_manager = ImageManager()
        self._backend = GVisorSandboxBackend(image_manager=self._image_manager)
        self._ttl_timers: Dict[str, threading.Timer] = {}
        # Instance id -> the env its create asked for, for sandboxes taken
        # from the warm pool (booted before the create, with the profile's).
        self._exec_env: Dict[str, Dict[str, str]] = {}
        self._warm = _WarmPool(self, warm_pool) if warm_pool else None

    @property
    def image_manager(self) -> ImageManager:
        """The ImageManager instance used by this runtime."""
        return self._image_manager

    @property
    def backend(self) -> GVisorSandboxBackend:
        """The backend instance used by this runtime."""
        return self._backend

    def pull_image(self, image: str, timeout_seconds: float = 120.0) -> str:
        """Download and extract container image into local cache.

        Args:
            image: Container image name or tar path.
            timeout_seconds: Request timeout.

        Returns:
            Extracted image directory path.
        """
        return self._image_manager.pull_image(image, timeout_seconds=timeout_seconds)

    def create(
        self,
        image: str,
        cpu: float = 0.0,
        memory: Union[str, int, float] = 0,
        env: Optional[Dict[str, str]] = None,
        workdir: Optional[str] = None,
        ttl_seconds: Optional[int] = None,
        timeout_seconds: float = 30.0,
        rootless: bool = True,
        network: str = "none",
        dns: Optional[List[str]] = None,
        capabilities: Optional[List[str]] = None,
        readonly: bool = True,
        _oci_spec_transform_fn: Optional[Callable[[Dict], Optional[Dict]]] = None,
        _ignore_cgroups: bool = False,
        **kwargs,
    ) -> str:
        """Provision the sandbox instance and return unique instance ID.

        Args:
            image: Container image for the sandbox environment.
            cpu: Number of CPU cores allocated to the sandbox.
            memory: Amount of memory allocated to the sandbox (e.g. "1Gi", "512Mi").
            env: Environment variables to inject into the sandbox.
            workdir: Working directory for commands; None uses the image's
                WORKDIR. On a readonly rootfs an explicit workdir is also the
                sandbox's only writable path; see
                :class:`~ray.experimental.sandbox.config.SandboxConfig`.
            ttl_seconds: Optional time-to-live in seconds, wall-clock from
                creation (not idle time), enforced by this runtime with a
                daemon timer. None (default) or <= 0 disables it.
            timeout_seconds: Timeout in seconds for sandbox creation.
            rootless: If True, run gVisor in rootless mode.
            network: Network mode ("none", "public", "host", "sandbox");
                see :class:`~ray.experimental.sandbox.config.SandboxConfig`.
                "public" is the recommended internet-access mode.
            dns: Optional nameserver IPs for the generated /etc/resolv.conf
                (public resolvers by default for "public").
            capabilities: Linux capabilities, written exactly (None keeps
                the runtime default; ``[]`` means none). Use
                ``DOCKER_DEFAULT_CAPABILITIES`` for Docker parity.
            readonly: If True (default), mount container image rootfs in read-only mode
                such that only ``workdir`` is writable. If False, the entire root filesystem
                is writable. Writes are isolated within a per-sandbox copy-on-write overlay
                filesystem, ensuring multiple sandboxes running the same container image do
                not interfere with each other or modify the base image.
            _oci_spec_transform_fn: PRIVATE — development/testing only. Called with the fully-built OCI
                spec dict before it is written; may mutate in place or return a new dict. Must be
                cloudpickle-serializable. No stability guarantees. Accepts a transform function.
            _ignore_cgroups: PRIVATE — testing only. If True, passes --ignore-cgroups to runsc.
            **kwargs: Additional parameters.

        Returns:
            A unique string identifier for the created sandbox.
        """
        cfg = SandboxConfig(
            image=image,
            cpu=cpu,
            memory=memory,
            env=env or {},
            workdir=workdir,
            ttl_seconds=ttl_seconds,
            timeout_seconds=timeout_seconds,
            rootless=rootless,
            network=network,
            dns=dns,
            capabilities=capabilities,
            readonly=readonly,
            _oci_spec_transform_fn=_oci_spec_transform_fn,
            _ignore_cgroups=_ignore_cgroups,
            **kwargs,
        )
        instance_id = self._warm.take(cfg) if self._warm is not None else None
        if instance_id is not None:
            if cfg.env:
                self._exec_env[instance_id] = dict(cfg.env)
        else:
            instance_id = self._backend.create_sandbox(cfg)
        if cfg.ttl_seconds is not None and cfg.ttl_seconds > 0:
            timer = threading.Timer(cfg.ttl_seconds, self._expire, args=(instance_id,))
            timer.daemon = True
            self._ttl_timers[instance_id] = timer
            timer.start()
        return instance_id

    def _expire(self, instance_id: str) -> None:
        """TTL callback: best-effort delete (the sandbox may already be gone)."""
        try:
            self.delete(instance_id)
        except Exception:
            pass

    def exec(
        self,
        instance_id: str,
        command: Union[str, List[str]],
        timeout: Optional[float] = None,
        cwd: Optional[str] = None,
        env: Optional[Dict[str, str]] = None,
        shell: Optional[str] = None,
        user: Optional[str] = None,
    ) -> ExecResult:
        """Execute a command inside the specified sandbox.

        Args:
            instance_id: Unique identifier of the sandbox instance.
            command: Command to execute, either as a string or a list of arguments.
            timeout: Maximum execution time in seconds.
            cwd: Working directory inside the sandbox for command execution.
            env: Environment variables to set for the command.
            shell: Optional shell for string commands, overriding the
                sandbox's configured shell (default /bin/bash).
            user: Optional user to run as: a numeric uid, "uid:gid", or a
                user (optionally ":group") name, resolved against the
                /etc/passwd and /etc/group inside the running sandbox
                (default: the image user).

        Returns:
            ExecResult containing exit code, stdout, and stderr.
        """
        create_env = self._exec_env.get(instance_id)
        if create_env:
            env = {**create_env, **(env or {})}
        return self._backend.exec_command(
            instance_id,
            command,
            timeout=timeout,
            cwd=cwd,
            env=env,
            shell=shell,
            user=user,
        )

    async def exec_async(
        self,
        instance_id: str,
        command: Union[str, List[str]],
        timeout: Optional[float] = None,
        cwd: Optional[str] = None,
        env: Optional[Dict[str, str]] = None,
        shell: Optional[str] = None,
        user: Optional[str] = None,
    ) -> ExecResult:
        """Execute a command inside the specified sandbox asynchronously.

        Args:
            instance_id: Unique identifier of the sandbox instance.
            command: Command to execute, either as a string or a list of arguments.
            timeout: Maximum execution time in seconds.
            cwd: Working directory inside the sandbox for command execution.
            env: Environment variables to set for the command.
            shell: Optional shell for string commands, overriding the
                sandbox's configured shell (default /bin/bash).
            user: Optional user to run as: a numeric uid, "uid:gid", or a
                user (optionally ":group") name, resolved against the
                /etc/passwd and /etc/group inside the running sandbox
                (default: the image user).

        Returns:
            ExecResult containing exit code, stdout, and stderr.
        """
        return await asyncio.to_thread(
            self.exec,
            instance_id,
            command,
            timeout=timeout,
            cwd=cwd,
            env=env,
            shell=shell,
            user=user,
        )

    def upload_file(self, instance_id: str, local_path: str, remote_path: str) -> None:
        """Copy local file into the sandbox.

        Args:
            instance_id: Unique identifier of the sandbox instance.
            local_path: Path to the source file on the local filesystem.
            remote_path: Destination path inside the sandbox.
        """
        with open(local_path, "rb") as f:
            content = f.read()
        self._backend.write_file(instance_id, remote_path, content)

    def download_file(
        self, instance_id: str, remote_path: str, local_path: str
    ) -> None:
        """Copy file from the sandbox to local.

        Args:
            instance_id: Unique identifier of the sandbox instance.
            remote_path: Path to the source file inside the sandbox.
            local_path: Destination path on the local filesystem.
        """
        content = self._backend.read_file(instance_id, remote_path)
        local_dir = os.path.dirname(os.path.abspath(local_path))
        if local_dir:
            os.makedirs(local_dir, exist_ok=True)
        with open(local_path, "wb") as f:
            f.write(content)

    def write_file(
        self,
        instance_id: str,
        path: str,
        content: Union[str, bytes],
        append: bool = False,
    ) -> None:
        """Write string or binary content directly to a file inside the sandbox.

        Args:
            instance_id: Unique identifier of the sandbox instance.
            path: Destination file path inside the sandbox.
            content: String or binary content to write into the file.
            append: Append to the file instead of truncating it.
        """
        self._backend.write_file(instance_id, path, content, append=append)

    def read_file(self, instance_id: str, path: str) -> bytes:
        """Read binary content from a file inside the sandbox.

        Args:
            instance_id: Unique identifier of the sandbox instance.
            path: Path to the file inside the sandbox to read.

        Returns:
            File content as bytes.
        """
        return self._backend.read_file(instance_id, path)

    def get_status(self, instance_id: str) -> SandboxStatus:
        """Query operational status of the sandbox.

        Args:
            instance_id: Unique identifier of the sandbox instance.

        Returns:
            SandboxStatus of the sandbox instance.
        """
        return self._backend.get_status(instance_id)

    def delete(self, instance_id: str) -> None:
        """Clean up and terminate the sandbox instance.

        Args:
            instance_id: Unique identifier of the sandbox instance.
        """
        timer = self._ttl_timers.pop(instance_id, None)
        if timer is not None:
            timer.cancel()
        self._exec_env.pop(instance_id, None)
        self._backend.delete_sandbox(instance_id)

    def wait_for_warm_pool(self, timeout_seconds: float = 300.0) -> bool:
        """Wait until every warm-pool profile has its full pool booted.

        Args:
            timeout_seconds: How long to wait.

        Returns:
            True once the pools are full; False on timeout, or with no pool.
        """
        if self._warm is None:
            return False
        deadline = time.monotonic() + timeout_seconds
        while not self._warm.full():
            if time.monotonic() >= deadline:
                return False
            time.sleep(0.05)
        return True

    def close(self) -> None:
        """Delete the sandboxes the warm pool keeps booted, and stop refilling it.

        Sandboxes that ``create`` returned stay; delete them with ``delete``.
        """
        if self._warm is not None:
            for instance_id in self._warm.close():
                try:
                    self._backend.delete_sandbox(instance_id)
                except Exception as exc:
                    logger.warning(
                        "Failed to delete warm-pool sandbox %s: %s", instance_id, exc
                    )

    def terminate(self, instance_id: str) -> None:
        """Clean up and terminate the sandbox instance.

        Args:
            instance_id: Unique identifier of the sandbox instance.
        """
        self.delete(instance_id)

    async def delete_async(self, instance_id: str) -> None:
        """Clean up and terminate the sandbox instance asynchronously.

        Args:
            instance_id: Unique identifier of the sandbox instance.
        """
        await asyncio.to_thread(self.delete, instance_id)
