import asyncio
import json
import os
import tempfile
import threading
from typing import Callable, Dict, List, Optional, Union

from ray.experimental.sandbox._internal import image_push
from ray.experimental.sandbox.backend.base import (
    BaseSandboxBackend,
    ExecResult,
    SandboxStatus,
)
from ray.experimental.sandbox.backend.base import check_local_resources
from ray.experimental.sandbox.backend.registry import (
    needs_image_manager,
    registered_backends,
    resolve_backend_factory,
)
from ray.experimental.sandbox.config import (
    SandboxConfig,
    pop_removed_readonly,
)
from ray.experimental.sandbox.exceptions import SandboxCreationError, SandboxError
from ray.experimental.sandbox.image_manager import ImageManager
from ray.util.annotations import PublicAPI


def _checkpoint_format(checkpoint_path: str) -> Optional[str]:
    """The backend that took a checkpoint, if this process can tell.

    A ``<name>://`` handle names its backend when ``<name>`` is a registered
    backend; other schemes are left to the backend.
    """
    if "://" in checkpoint_path:
        scheme = checkpoint_path.split("://", 1)[0].lower()
        return scheme if scheme in registered_backends() else None
    try:
        with open(
            os.path.join(checkpoint_path, "manifest.json"), encoding="utf-8"
        ) as f:
            return json.load(f).get("backend", "gvisor")
    except (OSError, ValueError, AttributeError):
        return None


@PublicAPI(stability="alpha")
class SandboxRuntime:
    """Low-level interface for managing local sandbox runtime environments."""

    def __init__(self, backend: Union[str, BaseSandboxBackend, None] = None):
        """Create a runtime on a sandbox backend.

        Args:
            backend: A backend instance, or the name of one (see
                ``ray.experimental.sandbox.backend.registry``). None reads
                the ``RAY_SANDBOX_BACKEND`` environment variable and
                defaults to gVisor. A backend that doesn't use a local image
                cache (``needs_image_manager = False``) gets no
                ``ImageManager``.
        """
        if backend is None or isinstance(backend, str):
            factory = resolve_backend_factory(backend)
            manager = ImageManager() if needs_image_manager(factory) else None
            self._backend = factory(image_manager=manager)
        else:
            self._backend = backend
        self._image_manager = getattr(self._backend, "image_manager", None)
        self._ttl_timers: Dict[str, threading.Timer] = {}

    @property
    def image_manager(self) -> ImageManager:
        """The ImageManager instance used by this runtime."""
        return self._image_manager

    @property
    def backend(self) -> BaseSandboxBackend:
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

    def check_resources(
        self,
        cpu: float = 0.0,
        memory: Union[str, int, float] = 0,
    ) -> None:
        """Reject unsupported resource limits before downloading an image.

        The backend decides: by default (gVisor) the node's cgroups must be
        able to enforce them; a backend that runs sandboxes elsewhere may
        check nothing here.

        Args:
            cpu: CPU limit to enforce. Zero means no limit.
            memory: Memory limit to enforce, such as "512Mi". Zero means no limit.
        """
        check = getattr(self._backend, "check_resources", None)
        try:
            if check is not None:
                check(cpu=cpu, memory=memory)
            else:  # a backend that doesn't subclass BaseSandboxBackend
                check_local_resources(cpu, memory)
        except SandboxCreationError:
            raise
        except Exception as err:
            raise SandboxCreationError(
                f"Cannot enforce sandbox resources: {err}"
            ) from err

    def get_termination_reason(self, instance_id: str) -> Optional[str]:
        """Report an unexpected runtime exit, including a cgroup OOM kill.

        Args:
            instance_id: Sandbox instance ID.

        Returns:
            Why the sandbox stopped unexpectedly, or None if it's running or
            this runtime no longer tracks it.
        """
        return self._backend.get_termination_reason(instance_id)

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
        cidr_allowlist: Optional[List[str]] = None,
        capabilities: Optional[List[str]] = None,
        gpu_ids: Optional[List[Union[int, str]]] = None,
        _oci_spec_transform_fn: Optional[Callable[[Dict], Optional[Dict]]] = None,
        _ignore_cgroups: bool = False,
        disk: Union[str, int, float] = 0,
        **kwargs,
    ) -> str:
        """Provision the sandbox instance and return unique instance ID.

        The sandbox writes to a private copy-on-write overlay over its image.
        Sandboxes that share an image don't see each other's writes, the
        cached image is never modified, and the overlay is discarded with
        the sandbox.

        Args:
            image: Container image for the sandbox environment.
            cpu: Number of CPU cores allocated to the sandbox.
            memory: Amount of memory allocated to the sandbox (e.g. "1Gi", "512Mi").
            env: Environment variables to inject into the sandbox.
            workdir: Working directory for commands; None uses the image's
                WORKDIR.
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
            cidr_allowlist: Optional egress allowlist for network="public":
                only the listed destinations are reachable (plus DNS to
                the ``dns`` resolvers). None means the public internet,
                with internal addresses left out as a best effort, and a
                denylist keeps instance metadata and the Kubernetes API
                service closed whatever the list says; see
                :class:`~ray.experimental.sandbox.config.SandboxConfig`.
                Change it later with :meth:`set_egress_allowlist`.
            capabilities: Linux capabilities, written exactly (None keeps
                the runtime default; ``[]`` means none). Use
                ``DOCKER_DEFAULT_CAPABILITIES`` for Docker parity.
            gpu_ids: GPU device ids/UUIDs to expose via CDI; see
                :class:`~ray.experimental.sandbox.config.SandboxConfig`.
            _oci_spec_transform_fn: PRIVATE — development/testing only. Called with the fully-built OCI
                spec dict before it is written; may mutate in place or return a new dict. Must be
                cloudpickle-serializable. No stability guarantees. Accepts a transform function.
            _ignore_cgroups: PRIVATE — testing only. If True, passes --ignore-cgroups to runsc.
            disk: Size cap of the root filesystem overlay (e.g. "10Gi");
                writes past it fail with ENOSPC. Zero means no cap; see
                :class:`~ray.experimental.sandbox.config.SandboxConfig`.
            **kwargs: Additional parameters. The removed ``readonly`` option
                is still recognized: ``readonly=False`` is ignored with a
                deprecation warning, and ``readonly=True`` raises a
                ``ValueError``.

        Returns:
            A unique string identifier for the created sandbox.
        """
        # The option is gone, but callers written for it still pass it.
        pop_removed_readonly(kwargs)
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
            cidr_allowlist=cidr_allowlist,
            capabilities=capabilities,
            gpu_ids=gpu_ids,
            _oci_spec_transform_fn=_oci_spec_transform_fn,
            _ignore_cgroups=_ignore_cgroups,
            disk=disk,
            **kwargs,
        )
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
        combine_output: bool = False,
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
            combine_output: Return stdout and stderr together, as raw bytes in
                the order written, in ``ExecResult.output``.

        Returns:
            ExecResult containing exit code, stdout, and stderr.
        """
        return self._backend.exec_command(
            instance_id,
            command,
            timeout=timeout,
            cwd=cwd,
            env=env,
            shell=shell,
            user=user,
            combine_output=combine_output,
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
        combine_output: bool = False,
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
            combine_output: Return stdout and stderr together, as raw bytes in
                the order written, in ``ExecResult.output``.

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
            combine_output=combine_output,
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

    def set_egress_allowlist(self, instance_id: str, cidrs: List[str]) -> None:
        """Replace the egress allowlist of a network="public" sandbox.

        The new list takes effect for the next packet of every flow; flows
        it no longer permits stall rather than being reset. No list opens
        the denylist; ``ALLOW_ALL_CIDRS`` opens everything else, private
        networks and any Ray node on them included.

        Args:
            instance_id: Unique identifier of the sandbox instance.
            cidrs: The new allowlist, in the forms ``cidr_allowlist`` takes.
        """
        self._backend.set_egress_allowlist(instance_id, cidrs)

    def commit(
        self,
        instance_id: str,
        image: str,
        env: Optional[Dict[str, str]] = None,
        timeout_seconds: float = 1800.0,
    ) -> str:
        """Push the sandbox's base image and root filesystem changes to a registry.

        Mounts aren't included. Commit leaves the sandbox in the state it was
        in: a running sandbox is paused only while its changes are exported,
        and a paused sandbox stays paused. Registry credentials come from
        ``$DOCKER_CONFIG`` and must allow push.

        Args:
            instance_id: Unique identifier of the sandbox instance.
            image: Target reference with a tag, such as ``registry/repo:tag``.
            env: Variables to replace or extend the base image config's Env.
            timeout_seconds: Timeout for export and each registry request.

        Returns:
            The immutable ``registry/repo@sha256:...`` reference.
        """
        for method in (
            "get_config",
            "sandbox_dir",
            "export_rootfs_changes",
            "mount_points",
        ):
            if not hasattr(self._backend, method):
                raise NotImplementedError(
                    f"{type(self._backend).__name__} does not support commit()"
                )
        cfg = self._backend.get_config(instance_id)
        # Fail before the export, which pauses a running sandbox.
        image_push.validate_commit(cfg.image, image, self._image_manager.images_dir)
        with tempfile.TemporaryDirectory(
            prefix="commit-", dir=self._backend.sandbox_dir(instance_id)
        ) as directory:
            rootfs_tar = os.path.join(directory, "rootfs.tar")
            self._backend.export_rootfs_changes(
                instance_id, rootfs_tar, timeout=timeout_seconds
            )
            layers = image_push.build_layers(
                rootfs_tar, directory, self._backend.mount_points(instance_id)
            )
            return image_push.push_image(
                cfg.image,
                image,
                layers,
                env,
                self._image_manager.images_dir,
                timeout_seconds,
            )

    def checkpoint(
        self,
        instance_id: str,
        checkpoint_path: Optional[str] = None,
        leave_running: bool = True,
        timeout_seconds: float = 30.0,
        remote: bool = False,
        **kwargs,
    ) -> str:
        """Create a checkpoint of the running sandbox.

        Args:
            instance_id: Unique identifier of the sandbox instance.
            checkpoint_path: Path where the checkpoint directory will be saved.
                If None, a default directory inside the sandbox storage root is used.
            leave_running: If True, keep the sandbox running after checkpointing.
            timeout_seconds: Timeout for the checkpoint operation.
            remote: If True, mark checkpoint as remote/shared across VM nodes.
                Requires a ``checkpoint_path`` on storage that every node mounts.
            **kwargs: Backend-specific arguments.

        Returns:
            Absolute path to the checkpoint directory.
        """
        res = self._backend.checkpoint_sandbox(
            sandbox_id=instance_id,
            checkpoint_path=checkpoint_path,
            leave_running=leave_running,
            remote=remote,
            timeout_seconds=timeout_seconds,
            **kwargs,
        )
        if not leave_running:
            timer = self._ttl_timers.pop(instance_id, None)
            if timer is not None:
                timer.cancel()

        if isinstance(res, dict) and "checkpoint_path" in res:
            return res["checkpoint_path"]
        return checkpoint_path or ""

    def restore(
        self,
        checkpoint_path: str,
        cpu: Optional[float] = None,
        memory: Optional[Union[str, int, float]] = None,
        ttl_seconds: Optional[int] = None,
        timeout_seconds: float = 30.0,
        remote: Optional[bool] = None,
        **kwargs,
    ) -> str:
        """Restore a sandbox instance from a previously saved checkpoint.

        The restored sandbox preserves the full execution state, network mode,
        mounts, Linux capabilities, and process environment of the checkpointed
        sandbox. Cgroup resource allocations (cpu, memory) and lifecycle limits
        (ttl_seconds, timeout_seconds) can optionally be overridden.

        Args:
            checkpoint_path: Path to the checkpoint directory.
            cpu: CPU allocation override.
            memory: Memory allocation override.
            ttl_seconds: Optional time-to-live for the restored sandbox.
            timeout_seconds: Timeout for the restore operation.
            remote: If True, allow restore on a different VM node than where the
                checkpoint was taken. If None, inherits from checkpoint manifest.
            **kwargs: Backend-specific arguments. The removed ``readonly``
                option is handled as in ``create``.

        Returns:
            A unique string identifier for the restored sandbox.
        """
        pop_removed_readonly(kwargs)
        self._check_checkpoint_format(checkpoint_path)
        instance_id = self._backend.restore_sandbox(
            checkpoint_path=checkpoint_path,
            cpu=cpu,
            memory=memory,
            remote=remote,
            timeout_seconds=timeout_seconds,
            **kwargs,
        )
        if ttl_seconds is not None and ttl_seconds > 0:
            # A backend can hand back an id this runtime already tracks (one
            # that resumes the checkpointed sandbox in place): its old timer
            # must not delete the restored sandbox.
            old = self._ttl_timers.pop(instance_id, None)
            if old is not None:
                old.cancel()
            timer = threading.Timer(ttl_seconds, self._expire, args=(instance_id,))
            timer.daemon = True
            self._ttl_timers[instance_id] = timer
            timer.start()
        return instance_id

    def _check_checkpoint_format(self, checkpoint_path: str) -> None:
        expected = getattr(self._backend, "checkpoint_format", None)
        found = _checkpoint_format(str(checkpoint_path))
        if expected is not None and found is not None and found != expected:
            raise SandboxError(
                f"The checkpoint at '{checkpoint_path}' was taken by the {found} "
                f"sandbox backend, but this runtime uses {expected}: select "
                f"{found} (RAY_SANDBOX_BACKEND={found}) to restore it"
            )

    def delete_checkpoint(self, checkpoint_path: str) -> None:
        """Delete a checkpoint that's no longer needed.

        On gVisor that removes the checkpoint directory. A backend whose
        checkpoints hold resources elsewhere releases them.

        Args:
            checkpoint_path: The checkpoint, as ``checkpoint()`` returned it.
        """
        self._check_checkpoint_format(checkpoint_path)
        self._backend.delete_checkpoint(checkpoint_path)

    def pause(self, instance_id: str, timeout_seconds: float = 10.0) -> None:
        """Pause a running sandbox instance.

        Args:
            instance_id: Unique identifier of the sandbox instance.
            timeout_seconds: Timeout for pause operation.
        """
        self._backend.pause_sandbox(instance_id, timeout_seconds=timeout_seconds)

    def resume(self, instance_id: str, timeout_seconds: float = 10.0) -> None:
        """Resume a paused sandbox instance.

        Args:
            instance_id: Unique identifier of the sandbox instance.
            timeout_seconds: Timeout for resume operation.
        """
        self._backend.resume_sandbox(instance_id, timeout_seconds=timeout_seconds)

    def delete(self, instance_id: str) -> None:
        """Clean up and terminate the sandbox instance.

        Args:
            instance_id: Unique identifier of the sandbox instance.
        """
        timer = self._ttl_timers.pop(instance_id, None)
        if timer is not None:
            timer.cancel()
        self._backend.delete_sandbox(instance_id)

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
