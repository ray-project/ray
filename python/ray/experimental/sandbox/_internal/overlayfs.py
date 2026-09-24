"""The kernel overlay an overlayfs sandbox boots from.

An overlayfs sandbox (see ``config.ROOTFS_OVERLAYFS``) boots from a kernel
overlayfs over its image's unpacked tree (see
``image_utils.get_unpacked_rootfs``), mounted on the spec's root.path in a
private mount namespace that ``runsc run`` then runs in. The unpacked tree is
the read-only lower layer, and every host-side write to the rootfs lands in the
sandbox's upper layer instead, including OCI hooks' writes and the mount points
runsc creates for the sandbox's mounts.

With mount privilege, the kernel overlay is mounted directly and keeps the
image's file owners. Without it, the kernel overlay is mounted inside a user
namespace of the sandbox's own, where every file belongs to the worker. That
takes Linux 5.11+ and a host that allows unprivileged user namespaces.

The upper and work layers live on a tmpfs mounted in that same namespace, so
they go away with it. A directory under /tmp won't do, since /tmp is often an
overlayfs inside a container, and overlayfs can't use another overlayfs as its
upper layer. The tmpfs only holds host-side writes, since gVisor keeps the
sandbox's own writes in a layer of its own. Inside a user namespace, the
kernel overlay keeps its metadata in user xattrs, which tmpfs supports from
Linux 6.6. Before that, such an overlay can't rename or replace a lower-layer
directory from the host.

The tmpfs is mounted on the bundle's ``overlayfs-tmpfs/`` directory, and holds
``upper/`` and ``work/``.
"""

import enum
import functools
import os
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from typing import List

from ray.experimental.sandbox._internal import fs_utils
from ray.experimental.sandbox.exceptions import SandboxCreationError

# The bundle directory the kernel overlay's tmpfs is mounted on. It holds the
# upper and work layers.
_TMPFS_DIRNAME = "overlayfs-tmpfs"

# Caps the host-side writes to an overlayfs sandbox's rootfs (OCI hooks' files
# and runsc's mount points).
TMPFS_SIZE = "64m"


class MountMode(enum.Enum):
    """How this process can mount an overlayfs sandbox's kernel overlay."""

    # Directly, in a mount namespace of the sandbox's own.
    PRIVILEGED = "privileged"
    # Inside a user namespace of the sandbox's own.
    USERNS = "userns"


@dataclass(frozen=True)
class RootfsOverlay:
    """An overlayfs sandbox's kernel overlay, ready to mount.

    ``wrap`` turns a command into one that mounts the overlay in fresh
    namespaces and then runs it there.
    """

    # The unpacked tree the overlay sits on.
    lower: str
    # The directory the overlay is mounted on (the spec's root.path).
    mountpoint: str
    # The directory the overlay's tmpfs is mounted on.
    tmpfs_dir: str
    # Whether the overlay is mounted inside a user namespace.
    in_userns: bool

    def _mount_script(self) -> str:
        """A shell snippet that mounts the kernel overlay.

        Its positional args are ``_mount_script_args``, so no path is quoted
        into the script. overlayfs gives the overlay's root the upper layer's
        attributes, so the snippet copies the unpacked tree's mode and
        timestamps onto the upper layer, and its owner outside a user
        namespace.
        """
        # Inside a user namespace, overlayfs keeps its metadata in user
        # xattrs, since it can't write trusted ones there.
        options = "lowerdir=$lower,upperdir=$tmpfs/upper,workdir=$tmpfs/work"
        if self.in_userns:
            options += ",userxattr"

        # Give the overlay's root the unpacked tree's attributes. Inside a
        # user namespace every file belongs to the worker, so the owner
        # already matches.
        copy_root_attrs = [
            'chmod --reference="$lower" "$tmpfs/upper"',
            'touch -r "$lower" "$tmpfs/upper"',
        ]
        if not self.in_userns:
            copy_root_attrs.append('chown --reference="$lower" "$tmpfs/upper"')

        steps = [
            'lower="$1" tmpfs="$2" mnt="$3"',
            f'mount -t tmpfs -o size={TMPFS_SIZE},mode=0755 tmpfs "$tmpfs"',
            'mkdir "$tmpfs/upper" "$tmpfs/work"',
            *copy_root_attrs,
            f'mount -t overlay overlay -o "{options}" "$mnt"',
        ]
        return (
            "{ "
            + " && ".join(steps)
            + '; } || { echo "rootfs overlay mount failed" >&2; exit 1; }'
        )

    def _mount_script_args(self) -> List[str]:
        """The mount snippet's positional args: the unpacked tree, the
        directory to mount the tmpfs on, and the directory to mount the
        overlay on."""
        return [self.lower, self.tmpfs_dir, self.mountpoint]

    def wrap(self, argv: List[str]) -> List[str]:
        """``argv`` run in a private mount namespace, and a user namespace if
        needed, after mounting the kernel overlay there."""
        script_args = self._mount_script_args()
        userns = ["--user", "--map-root-user"] if self.in_userns else []

        # The script mounts the overlay, drops its own args, and execs argv
        # in the namespaces it mounted into.
        return [
            "unshare",
            *userns,
            "--mount",
            "--",
            "bash",
            "-c",
            f'{self._mount_script()} && shift {len(script_args)} && exec "$@"',
            "_",
            *script_args,
            *argv,
        ]


def _is_initial_userns(uid_map: str) -> bool:
    """Whether a /proc/<pid>/uid_map is the initial user namespace's."""
    return uid_map.split() == ["0", "0", "4294967295"]


@functools.lru_cache(maxsize=None)
def has_mount_privilege() -> bool:
    """Whether this process can mount directly, keeping the image's owners.

    Root in a nested user namespace (e.g. a rootless container) can also
    unshare a mount namespace, but can't mount an overlay without
    ``userxattr`` or unpack an image with its real owners, so this also
    checks for the initial user namespace. Probed once per process.
    """
    try:
        with open("/proc/self/uid_map", encoding="utf-8") as f:
            if not _is_initial_userns(f.read()):
                return False
    except OSError:
        return False

    # Root in the initial user namespace can still lack CAP_SYS_ADMIN (e.g.
    # an unprivileged container), so try unsharing a mount namespace.
    # A hang says nothing about whether this worker can mount, so it raises
    # rather than returning (and caching) an answer.
    try:
        res = subprocess.run(
            ["unshare", "--mount", "true"], capture_output=True, timeout=30
        )
    except subprocess.TimeoutExpired as err:
        raise SandboxCreationError(
            "Timed out checking whether this worker has mount privileges."
        ) from err
    return res.returncode == 0


@functools.lru_cache(maxsize=None)
def can_mount_in_userns() -> bool:
    """Whether this process can mount a kernel overlay inside a user
    namespace of its own. Probed once per process, by mounting a throwaway
    one."""
    scratch = tempfile.mkdtemp(prefix="ray-overlay-probe-")
    try:
        lower, tmpfs, mountpoint = (
            os.path.join(scratch, d) for d in ("lower", "tmpfs", "mnt")
        )
        for d in (lower, tmpfs, mountpoint):
            os.mkdir(d)

        # Mount an empty overlay exactly the way a sandbox would, and run
        # nothing in it.
        overlay = RootfsOverlay(
            lower=lower,
            mountpoint=mountpoint,
            tmpfs_dir=tmpfs,
            in_userns=True,
        )
        # A hang says nothing about whether this worker can mount, so it
        # raises rather than returning (and caching) an answer.
        try:
            res = subprocess.run(
                overlay.wrap(["true"]), capture_output=True, timeout=30
            )
        except subprocess.TimeoutExpired as err:
            raise SandboxCreationError(
                "Timed out checking whether this worker can mount a kernel "
                "overlay in a user namespace."
            ) from err
        return res.returncode == 0
    finally:
        fs_utils.rmtree(scratch, ignore_errors=True)


def mount_mode() -> MountMode:
    """How this process can mount an overlayfs sandbox's kernel overlay.

    Returns:
        ``MountMode.PRIVILEGED`` with mount privileges, else
        ``MountMode.USERNS``.

    Raises:
        SandboxCreationError: If it can't mount one, naming what's missing.
    """
    # Both ways of mounting need util-linux.
    missing = [b for b in ("unshare", "mount") if not shutil.which(b)]
    if missing:
        raise SandboxCreationError(
            "An overlayfs sandbox mounts its kernel overlay in a private mount "
            f"namespace, but PATH has no {', '.join(repr(b) for b in missing)}. "
            "Install util-linux on the node image."
        )

    # Prefer mounting directly, which keeps the image's file owners.
    if has_mount_privilege():
        return MountMode.PRIVILEGED
    if can_mount_in_userns():
        return MountMode.USERNS

    raise SandboxCreationError(
        "Without mount privilege, an overlayfs sandbox mounts its kernel "
        "overlay inside an unprivileged user namespace, and a test mount of "
        "one failed on this node. That takes Linux 5.11 or later and "
        "unprivileged user namespaces enabled (check the "
        "kernel.apparmor_restrict_unprivileged_userns, "
        "user.max_user_namespaces and kernel.unprivileged_userns_clone "
        "sysctls)."
    )


def prepare(
    *,
    bundle_dir: str,
    lower: str,
    mountpoint: str,
    mount_mode: MountMode,
) -> RootfsOverlay:
    """Check ``lower``, create an overlayfs sandbox's overlay directories in
    its bundle, and describe how to mount its kernel overlay.

    Args:
        bundle_dir: The sandbox's bundle directory.
        lower: The unpacked tree the kernel overlay sits on.
        mountpoint: The directory to mount the kernel overlay on.
        mount_mode: How the kernel overlay gets mounted (see ``mount_mode``).

    Returns:
        The sandbox's kernel overlay, ready to wrap its runsc command with.

    Raises:
        SandboxCreationError: If ``lower`` can't appear in overlayfs mount
            options.
    """
    if "," in lower or ":" in lower:
        raise SandboxCreationError(
            f"Can't use {lower!r} as an overlayfs lower layer, since overlayfs "
            "mount options treat ',' and ':' as separators."
        )

    # The directories the tmpfs and the overlay get mounted on.
    tmpfs_dir = os.path.join(bundle_dir, _TMPFS_DIRNAME)
    os.makedirs(tmpfs_dir, exist_ok=True)
    os.makedirs(mountpoint, exist_ok=True)

    return RootfsOverlay(
        lower=lower,
        mountpoint=mountpoint,
        tmpfs_dir=tmpfs_dir,
        in_userns=mount_mode is MountMode.USERNS,
    )
