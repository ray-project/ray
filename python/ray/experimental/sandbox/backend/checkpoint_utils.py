"""Utility helpers for sandbox checkpoint and restore operations.

This module encapsulates:
- Filesystem tree copying with permission/sudo fallback for root-owned guest files.
- Atomic bundle staging and rollback via the StagedCheckpoint context manager.
- Manifest creation and schema validation for checkpoint bundles.
- Permission tightening for sensitive guest memory dumps and rootfs state.
"""

import dataclasses
import json
import logging
import os
import shutil
import subprocess
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple

from ray.experimental.sandbox.config import SandboxConfig
from ray.experimental.sandbox.exceptions import (
    SandboxError,
)

logger = logging.getLogger(__name__)


def copy_fs_tree(
    src: str,
    dst: str,
    ignore_patterns: Optional[List[str]] = None,
) -> None:
    """Copy a filesystem directory tree, with sudo fallback for root-owned guest files."""
    ignore_fn = shutil.ignore_patterns(*ignore_patterns) if ignore_patterns else None
    try:
        shutil.copytree(src, dst, dirs_exist_ok=True, ignore=ignore_fn, symlinks=True)
    except (PermissionError, shutil.Error) as err:
        if os.geteuid() != 0 and shutil.which("sudo"):
            os.makedirs(dst, exist_ok=True)
            cmd = ["sudo", "cp", "-a", os.path.join(src, "."), dst]
            res = subprocess.run(cmd, capture_output=True, text=True)
            if res.returncode != 0:
                raise OSError(
                    f"Failed to copy '{src}' to '{dst}' with sudo: {res.stderr}"
                ) from err
            if ignore_patterns:
                for pat in ignore_patterns:
                    subprocess.run(
                        ["sudo", "find", dst, "-name", pat, "-delete"],
                        capture_output=True,
                    )
        else:
            raise


def get_current_node_id() -> Optional[str]:
    """Get current Ray node ID, or hostname fallback if Ray is not initialized."""
    try:
        import ray

        if ray.is_initialized():
            node_id = ray.get_runtime_context().get_node_id()
            if node_id:
                return str(node_id)
    except Exception:
        pass
    import socket

    return socket.gethostname()


class CheckpointPath(str):
    """String path to a sandbox checkpoint bundle carrying scheduling metadata.

    Subclasses ``str`` for 100% backward compatibility with filesystem path
    operations (``os.path``, ``open``, equality with ``str``), while carrying
    ``node_id``, ``remote``, ``cpu``, and ``memory`` so ``sandbox.restore()``
    can schedule the restored actor onto the right node with the right resources
    even when the driver is on a different node than a node-local checkpoint.
    """

    node_id: Optional[str]
    remote: bool
    cpu: Optional[float]
    memory: Optional[Any]

    def __new__(
        cls,
        path: str,
        *,
        node_id: Optional[str] = None,
        remote: bool = False,
        cpu: Optional[float] = None,
        memory: Optional[Any] = None,
    ):
        obj = super().__new__(cls, path)
        obj.node_id = node_id
        obj.remote = bool(remote)
        obj.cpu = cpu
        obj.memory = memory
        return obj

    def __reduce__(self):
        return (
            self.__class__,
            (str(self),),
            {
                "node_id": self.node_id,
                "remote": self.remote,
                "cpu": self.cpu,
                "memory": self.memory,
            },
        )


def create_manifest(
    sandbox_id: str,
    config: SandboxConfig,
    cwd: str,
    workdir: Optional[str],
    leave_running: bool,
    remote: bool = False,
    node_id: Optional[str] = None,
    compression: str = "none",
) -> Dict[str, Any]:
    """Generate the manifest metadata dictionary for a checkpoint bundle."""
    return {
        "version": "1.0",
        "created_at": time.time(),
        "sandbox_id": sandbox_id,
        "node_id": node_id or get_current_node_id(),
        "remote": bool(remote),
        # How the memory image is compressed: restore can load memory in
        # the background only from an uncompressed one.
        "compression": compression,
        "image": config.image,
        "cwd": cwd,
        "workdir": workdir,
        "config": {
            "image": config.image,
            "cpu": config.cpu,
            "memory": config.memory,
            "network": config.network,
            "readonly": config.readonly,
            "shell": config.shell,
            "rootless": config.rootless,
            "env": config.env,
            "workdir": config.workdir,
            "capabilities": config.capabilities,
            "dns": config.dns,
            "cidr_allowlist": config.cidr_allowlist,
            "disk": config.disk,
            "_ignore_cgroups": getattr(config, "_ignore_cgroups", False),
        },
        "state_dir": "state",
        "fs_dir": "fs",
        "leave_running": leave_running,
    }


def load_checkpoint_manifest(checkpoint_path: str) -> Tuple[Dict[str, Any], str]:
    """Validate and load the manifest from a checkpoint bundle directory.

    Args:
        checkpoint_path: Path to the checkpoint bundle directory.

    Returns:
        A tuple of (manifest_dict, state_dir_path).
    """
    manifest_file = os.path.join(checkpoint_path, "manifest.json")
    state_dir = os.path.join(checkpoint_path, "state")

    if not os.path.isfile(manifest_file):
        raise SandboxError(
            f"Missing manifest.json in checkpoint directory '{checkpoint_path}'."
        )
    if not os.path.isdir(state_dir):
        raise SandboxError(
            f"Missing state directory in checkpoint directory '{checkpoint_path}'."
        )

    with open(manifest_file, "r", encoding="utf-8") as f:
        manifest = json.load(f)

    return manifest, state_dir


# Configuration that the checkpointed memory state depends on: the image
# and the mounts, root filesystem mode and network stack that the guest
# kernel was saved with. A restore can't change them. ``workdir`` is one of
# them only on a readonly rootfs, where it is a host-backed mount.
_RESTORE_IMMUTABLE_FIELDS = ("image", "network", "readonly", "workdir", "rootless")


def build_restored_config(
    manifest: Dict[str, Any],
    cpu: Optional[float] = None,
    memory: Optional[Any] = None,
    **kwargs,
) -> SandboxConfig:
    """Build the SandboxConfig for a restored sandbox from manifest metadata.

    The restored sandbox keeps the checkpointed configuration. Overrides in
    ``cpu``, ``memory`` and ``kwargs`` (such as ``env``, ``dns``,
    ``capabilities``, ``disk``, ``cidr_allowlist``, ``timeout_seconds``,
    ``_oci_spec_transform_fn`` and ``_ignore_cgroups``) replace the recorded
    values; None means no override.

    Args:
        manifest: The checkpoint's manifest.
        cpu: CPU override, or None.
        memory: Memory override, or None.
        **kwargs: Other SandboxConfig overrides; unknown keys are ignored.

    Returns:
        The restored sandbox's configuration.

    Raises:
        SandboxError: If an override changes a field in
            ``_RESTORE_IMMUTABLE_FIELDS``, which the restored memory state
            depends on.
    """
    cfg_dict = dict(manifest.get("config", {}))
    if cpu is not None:
        cfg_dict["cpu"] = cpu
    if memory is not None:
        cfg_dict["memory"] = memory

    valid_fields = {f.name for f in dataclasses.fields(SandboxConfig)}
    for k, v in kwargs.items():
        if k not in valid_fields or v is None:
            continue
        immutable = k in _RESTORE_IMMUTABLE_FIELDS and not (
            k == "workdir" and not cfg_dict.get("readonly", True)
        )
        if immutable and k in cfg_dict and v != cfg_dict[k]:
            raise SandboxError(
                f"Cannot restore with {k}={v!r}: the checkpoint was taken with "
                f"{k}={cfg_dict[k]!r}, and its saved memory state depends on it. "
                f"Restore without overriding {k!r}."
            )
        cfg_dict[k] = v

    if cfg_dict.get("network") not in ("public", "host") and kwargs.get("dns") is None:
        cfg_dict["dns"] = None
    if cfg_dict.get("disk") is None:
        cfg_dict.pop("disk", None)

    return SandboxConfig(**cfg_dict)


class StagedCheckpoint:
    """Context manager managing atomic checkpoint bundle staging and swap.

    Writes all checkpoint state to a temporary sibling directory. Upon successful
    exit, it atomically swaps the staged directory into `target_path`. If an existing
    bundle is present at `target_path`, it is backed up and safely restored if
    the swap fails.
    """

    def __init__(self, target_path: str):
        self.target_path = os.path.abspath(target_path)
        self.staging_id = uuid.uuid4().hex[:8]
        self.staging_path = f"{self.target_path}.tmp.{self.staging_id}"
        self.backup_path = f"{self.target_path}.old.{self.staging_id}"
        self.state_dir = os.path.join(self.staging_path, "state")
        self.fs_dir = os.path.join(self.staging_path, "fs")
        self._has_backup = False
        self._swap_succeeded = False

    def __enter__(self) -> "StagedCheckpoint":
        parent_dir = os.path.dirname(self.target_path)
        os.makedirs(parent_dir, mode=0o700, exist_ok=True)
        os.makedirs(self.staging_path, mode=0o700, exist_ok=True)
        os.makedirs(self.state_dir, mode=0o700, exist_ok=True)
        os.makedirs(self.fs_dir, mode=0o700, exist_ok=True)
        return self

    def copy_bundle_filesystems(
        self,
        root_dir: str,
        workdir: Optional[str],
        sandbox_id: str,
    ) -> None:
        """Snapshot the container overlay filesystem and writable workdir into staging."""
        saved_rootfs = os.path.join(root_dir, "rootfs")
        if os.path.isdir(saved_rootfs):
            dest_rootfs = os.path.join(self.fs_dir, "rootfs")
            try:
                copy_fs_tree(
                    saved_rootfs,
                    dest_rootfs,
                    ignore_patterns=[".gvisor.filestore*"],
                )
            except Exception as e:
                raise SandboxError(
                    f"Failed to copy rootfs overlay for sandbox '{sandbox_id}': {e}"
                ) from e

        if workdir and os.path.isdir(workdir):
            try:
                copy_fs_tree(
                    workdir,
                    os.path.join(self.fs_dir, "workdir"),
                )
            except Exception as e:
                raise SandboxError(
                    f"Failed to copy workdir for sandbox '{sandbox_id}': {e}"
                ) from e

        bundle_config_path = os.path.join(root_dir, "config.json")
        if os.path.isfile(bundle_config_path):
            shutil.copy2(
                bundle_config_path, os.path.join(self.staging_path, "config.json")
            )

        for net_file in ("resolv.conf", "hosts"):
            net_file_path = os.path.join(root_dir, net_file)
            if os.path.isfile(net_file_path):
                shutil.copy2(net_file_path, os.path.join(self.staging_path, net_file))

    def write_manifest(self, manifest: Dict[str, Any]) -> None:
        """Write the bundle manifest to manifest.json inside staging."""
        manifest_path = os.path.join(self.staging_path, "manifest.json")
        with open(manifest_path, "w", encoding="utf-8") as f:
            json.dump(manifest, f, indent=2)

    def harden_permissions(self, rootless: bool = False, remote: bool = False) -> None:
        """Ensure bundle outer directory and memory dumps are properly restricted,
        while preserving guest file mode permissions inside fs/ intact.
        Permissions are restricted to owner only (0700/0600) to protect memory dumps
        and environment secrets even on shared cross-node filesystems.
        """
        paths_to_harden = [self.staging_path, self.state_dir]
        for top_file in ("manifest.json", "config.json", "resolv.conf", "hosts"):
            p = os.path.join(self.staging_path, top_file)
            if os.path.isfile(p):
                paths_to_harden.append(p)

        dir_mode = "0700"
        file_mode = "0600"
        state_mode = "u+rwX,go-rwx"
        dir_octal = 0o700
        file_octal = 0o600

        if not rootless and os.geteuid() != 0 and shutil.which("sudo"):
            uid = os.getuid()
            gid = os.getgid()
            subprocess.run(
                ["sudo", "chown", "-R", f"{uid}:{gid}", self.staging_path],
                capture_output=True,
            )
            for path in paths_to_harden:
                if os.path.isdir(path):
                    subprocess.run(
                        ["sudo", "chmod", dir_mode, path],
                        capture_output=True,
                    )
                elif os.path.isfile(path):
                    subprocess.run(
                        ["sudo", "chmod", file_mode, path],
                        capture_output=True,
                    )
            subprocess.run(
                ["sudo", "chmod", "-R", state_mode, self.state_dir],
                capture_output=True,
            )
        else:
            try:
                for path in paths_to_harden:
                    if os.path.isdir(path):
                        os.chmod(path, dir_octal)
                    elif os.path.isfile(path):
                        os.chmod(path, file_octal)
                subprocess.run(
                    ["chmod", "-R", state_mode, self.state_dir],
                    capture_output=True,
                )
            except OSError:
                pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            # Operation failed: clean up staging directory.
            # Any previously valid checkpoint at target_path is untouched.
            if os.path.exists(self.staging_path):
                shutil.rmtree(self.staging_path, ignore_errors=True)
            return False

        # Attempt atomic swap
        if os.path.exists(self.target_path):
            os.replace(self.target_path, self.backup_path)
            self._has_backup = True

        try:
            os.replace(self.staging_path, self.target_path)
            self._swap_succeeded = True
        except Exception:
            if self._has_backup and os.path.exists(self.backup_path):
                try:
                    os.replace(self.backup_path, self.target_path)
                except Exception as rollback_err:
                    logger.warning(
                        f"Failed to restore checkpoint backup from '{self.backup_path}' "
                        f"to '{self.target_path}': {rollback_err}. "
                        f"Backup remains at '{self.backup_path}'."
                    )
            raise
        finally:
            if (
                self._swap_succeeded
                and self._has_backup
                and os.path.exists(self.backup_path)
            ):
                shutil.rmtree(self.backup_path, ignore_errors=True)
        return False
