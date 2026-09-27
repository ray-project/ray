"""The kernel overlay an overlayfs sandbox boots from.

An overlayfs sandbox (see ``needs_rootfs_overlay``) boots from a kernel
overlayfs mounted at its ``root.path``. Its read-only lower layer is a
per-sandbox mount of a cached EROFS image. Every host-side write to the rootfs
lands in the overlay's upper layer, including files written by any OCI hooks
and the mount points runsc creates for the sandbox's mounts.

Where the rootfs gets mounted, and how its EROFS image does, depends on
whether the worker has mount privileges or not (see ``UserNamespaceType.detect``
and ``ImageMountMode.detect``). A worker with them mounts it in a per-sandbox
mount namespace. There, the kernel's erofs driver mounts the image on a loop
device, or ``erofsfuse`` mounts it through FUSE if that fails, and every file
keeps its owner from the image either way. A worker without mount privileges
uses ``erofsfuse`` to mount the image inside a per-sandbox user namespace.
A worker running as root maps every id it has to itself in the new user
namespace, so every file keeps its owner from the image. A worker running
as any other user maps the subordinate ids getsubids or /etc/subuid and
/etc/subgid list for it, using newuidmap and newgidmap, so every file keeps
its owner there too. If it has none, it maps only root, so files owned by
any other user show up as owned by nobody.

The overlay's upper and work layers live on a small per-sandbox tmpfs mounted
on an ``overlayfs-tmpfs/`` directory next to the sandbox's ``config.json``. It
holds ``upper/``, ``work/``, and ``lower/``, where the EROFS image is mounted.
Only host-side writes land there, since gVisor keeps writes made inside the
sandbox in a separate layer. Inside a user namespace, the overlay keeps its
metadata in user xattrs on the tmpfs, which tmpfs supports from Linux 6.6. On
older kernels, host-side writes can't rename or replace a directory from the
lower layer.
"""

import enum
import functools
import logging
import os
import pwd
import shlex
import shutil
import signal
import subprocess
import tempfile
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from ray.experimental.sandbox._internal import image_utils
from ray.experimental.sandbox.exceptions import SandboxCreationError

logger = logging.getLogger(__name__)

# The bundle directory the kernel overlay's tmpfs is mounted on. It holds the
# upper and work layers.
_TMPFS_DIRNAME = "overlayfs-tmpfs"

# Caps the host-side writes to an overlayfs sandbox's rootfs (OCI hooks' files
# and runsc's mount points).
_TMPFS_SIZE = "64m"

# The kernel gives the host's user namespace a fixed inode number,
# PROC_USER_INIT_INO, which no other user namespace has. It has stayed the
# same since Linux 3.8, but no userspace header exports it.
_HOST_USERNS_INO = 0xEFFFFFFD


class _ProbeTimeoutError(SandboxCreationError):
    """A detection probe or lookup timed out, which says nothing about what
    this worker can do, so its result must not get cached."""


class UserNamespaceType(enum.Enum):
    """The type of user namespace an overlayfs sandbox's rootfs gets mounted in.

    Either way, the rootfs gets mounted in a new mount namespace for the
    sandbox alone.
    """

    # A sandbox runs in one of two kinds of user namespace, HOST or PRIVATE.
    # HOST is the host's user namespace, which requires mount privileges.
    # PRIVATE is a new user namespace created for the sandbox alone. A worker
    # running as root maps every id it has to itself in the new user
    # namespace. A worker running as any other user attempts to map root to
    # its user, and every other uid and gid to one of its subordinate ids.
    # Every file in the image then keeps its owner inside the sandbox. If such
    # a worker has no subordinate ids, it maps only root, so only root's files
    # keep their owner, and files owned by any other user show up as owned by
    # nobody. The detect method determines which one applies to the sandboxes
    # this worker creates.
    HOST = "host"
    PRIVATE = "private"

    @classmethod
    @functools.lru_cache(maxsize=None)
    def detect(cls) -> "UserNamespaceType":
        """Which user namespace this process mounts an overlayfs sandbox's
        rootfs in. Decided once per process.

        Returns:
            ``UserNamespaceType.HOST`` if it runs in the host's user namespace
            with mount privileges, else ``UserNamespaceType.PRIVATE``.

        Raises:
            SandboxCreationError: If checking for mount privileges times out.
        """
        # If this process runs in the host's user namespace and has mount
        # privileges, the rootfs is mounted in the host's user namespace. Even
        # root in the host's user namespace can lack mount privileges, such as
        # in an unprivileged container without CAP_SYS_ADMIN.
        if _is_host_userns() and _has_mount_privileges():
            return cls.HOST

        # Otherwise, the rootfs is mounted in a new user namespace created for
        # the sandbox alone. A worker running as root maps every id it has to
        # itself in the new user namespace, so every file keeps its owner.
        if os.getuid() == 0:
            return cls.PRIVATE

        # A worker running as any other user maps its subordinate ids so files
        # inside the sandbox keep their owners. Without any, only root maps and
        # every other file shows up as owned by nobody, so warn about it. With
        # them, files owned by ids beyond the mapped ranges still show up as
        # owned by nobody, and the image can have any ids, so say which ones
        # map.
        id_maps = IdMaps.detect()
        if id_maps is None:
            logger.warning(
                "Overlayfs sandboxes on this worker run in a new user namespace. "
                "It has no subordinate ids to map, so only uid 0 and gid 0 inside "
                "it map to uid %d on the host. Files in an image owned by any "
                "other uid or gid show up as owned by nobody inside the sandbox.",
                os.getuid(),
            )
        else:
            logger.debug(
                "Overlayfs sandboxes on this worker run in a new user namespace. "
                "Inside it, uid 0 and gid 0 map to uid %d on the host, and uids 1 "
                "through %d and gids 1 through %d map to its subordinate ids. "
                "Files in an image owned by any uid or gid outside those ranges "
                "show up as owned by nobody inside the sandbox.",
                os.getuid(),
                sum(count for inside, _, count in id_maps.uid_map if inside),
                sum(count for inside, _, count in id_maps.gid_map if inside),
            )
        return cls.PRIVATE


@dataclass(frozen=True)
class IdMaps:
    """The uid and gid maps for an overlayfs sandbox's user namespace, built
    from the worker's subordinate ids, or from its own ids when it runs as
    root."""

    # The uid_map and gid_map variables hold the maps' entries as
    # (inside_id, outside_id, count) triples. For example,
    # ((0, 1000, 1), (1, 100000, 65536)) maps root to uid 1000 on the host,
    # and ids 1 through 65536 onto ids 100000 through 165535.
    uid_map: Tuple[Tuple[int, int, int], ...]
    gid_map: Tuple[Tuple[int, int, int], ...]

    @classmethod
    @functools.lru_cache(maxsize=None)
    def detect(cls) -> Optional["IdMaps"]:
        """This worker's id maps for a sandbox's new user namespace, or None
        if it runs as a user other than root and has no subordinate ids.
        Decided once per process."""
        # A worker running as root takes every id range it has, including 0,
        # from its own maps, and maps each of them to itself. Otherwise, root
        # would lose its access to files other users own, such as a runsc
        # installed in another user's home directory.
        if os.getuid() == 0:
            uid_map = cls._build_idmap_from_proc_self("uid_map")
            gid_map = cls._build_idmap_from_proc_self("gid_map")
            return cls(uid_map=uid_map, gid_map=gid_map)

        # Otherwise, look up the worker's user name, which its subordinate ids
        # are listed under. newuidmap and newgidmap refuse to run for a uid
        # without one.
        try:
            uid = os.getuid()
            gid = os.getgid()
            username = pwd.getpwuid(uid).pw_name
        except KeyError:
            return None

        # Try to get subid ranges using the getsubids tool. This is more robust,
        # as it finds the subid ranges wherever newuidmap and newgidmap would,
        # including through SSSD or LDAP. Fall back to reading them directly
        # from /etc/subuid and /etc/subgid when getsubids isn't available.
        if shutil.which("getsubids"):
            subuids = cls._getsubids(username, "uid")
            subgids = cls._getsubids(username, "gid")
        else:
            subuids = cls._read_ranges("/etc/subuid", username, uid)
            subgids = cls._read_ranges("/etc/subgid", username, uid)
        if not subuids or not subgids:
            return None

        # Map root to the worker, and ids 1 and up onto its subordinate ids in turn.
        uid_map = cls._build_idmap(uid, subuids)
        gid_map = cls._build_idmap(gid, subgids)
        return cls(uid_map=uid_map, gid_map=gid_map)

    @staticmethod
    def _build_idmap_from_proc_self(name: str) -> Tuple[Tuple[int, int, int], ...]:
        """Reads the ranges of ids this process has from its ``name`` map in
        /proc/self, ``uid_map`` or ``gid_map``, where each line reads
        "<inside_id> <outside_id> <count>". Returns a map that maps each
        range of inside ids to itself."""
        idmap = []
        with open(os.path.join("/proc/self", name), encoding="utf-8") as f:
            for line in f:
                start, _, count = map(int, line.split())
                idmap.append((start, start, count))
        return tuple(idmap)

    @staticmethod
    def _build_idmap(
        worker_id: int, ranges: Tuple[Tuple[int, int], ...]
    ) -> Tuple[Tuple[int, int, int], ...]:
        """A map that maps id 0 to ``worker_id`` on the host, and ids from 1 up
        to each of ``ranges``, as (start, count) pairs, in turn."""
        idmap = [(0, worker_id, 1)]
        inside_id = 1
        for outside_id, count in ranges:
            idmap.append((inside_id, outside_id, count))
            inside_id += count
        return tuple(idmap)

    @staticmethod
    def _getsubids(username: str, kind: str) -> Tuple[Tuple[int, int], ...]:
        """Shells out to getsubids for ``username``'s subordinate ``kind``
        ranges, "uid" or "gid". Returns each range as a (start, count) pair."""
        flags = ["-g"] if kind == "gid" else []
        try:
            res = subprocess.run(
                ["getsubids", *flags, username],
                capture_output=True,
                text=True,
                timeout=30,
            )
        except subprocess.TimeoutExpired as err:
            # A hang, such as SSSD or LDAP stalling, says nothing about which
            # subordinate ids the worker has, so it raises rather than
            # returning an answer that gets cached.
            raise _ProbeTimeoutError(
                "Timed out looking up this worker's subordinate ids with getsubids."
            ) from err
        except OSError:
            return ()
        if res.returncode != 0:
            return ()

        # Each line reads "<index>: <username> <start> <count>".
        ranges = []
        for line in res.stdout.splitlines():
            _, _, start, count = line.split()
            ranges.append((int(start), int(count)))
        return tuple(ranges)

    @staticmethod
    def _read_ranges(path: str, username: str, uid: int) -> Tuple[Tuple[int, int], ...]:
        """Reads the subordinate ranges of the user named ``username`` with
        ``uid`` from ``path``, /etc/subuid or /etc/subgid, which list a user by
        name or by uid. Returns each range as a (start, count) pair."""
        # Each line reads "<owner>:<start>:<count>".
        owners = (username, str(uid))
        ranges = []
        try:
            with open(path, encoding="utf-8") as f:
                for line in f:
                    owner, _, rest = line.strip().partition(":")
                    if owner in owners:
                        start, count = rest.split(":")
                        ranges.append((int(start), int(count)))
        except OSError:
            pass
        return tuple(ranges)


class ImageMountMode(enum.Enum):
    """How an overlayfs sandbox's EROFS image gets mounted."""

    # A sandbox's EROFS image is mounted in one of two ways, KERNEL or FUSE.
    # KERNEL mounts it with the kernel's erofs driver on a loop device, which
    # requires mount privileges. FUSE mounts it with erofsfuse, which runs as a
    # regular process and works in either user namespace. The detect method
    # determines which one applies to the sandboxes this worker creates.
    KERNEL = "kernel"
    FUSE = "fuse"

    @classmethod
    @functools.lru_cache(maxsize=None)
    def detect(cls, userns: UserNamespaceType) -> "ImageMountMode":
        """How this process mounts an overlayfs sandbox's EROFS image, given
        the user namespace ``UserNamespaceType.detect`` chose for the rootfs.
        Decided once per process.

        Args:
            userns: Which user namespace the rootfs gets mounted in.

        Returns:
            ``ImageMountMode.KERNEL`` if it has mount privileges and the
            kernel's erofs driver can mount the image on a loop device, else
            ``ImageMountMode.FUSE``.

        Raises:
            SandboxCreationError: If it can't mount the image either way.
        """
        # In the host's user namespace, the kernel's erofs driver should mount
        # the EROFS image if it has sufficient privileges. If that fails,
        # erofsfuse mounts it instead.
        if userns is UserNamespaceType.HOST:
            mounted, kernel_errstr = _probe_mount(UserNamespaceType.HOST, cls.KERNEL)
            if mounted:
                return cls.KERNEL
            mounted, fuse_errstr = _probe_mount(UserNamespaceType.HOST, cls.FUSE)
            if mounted:
                return cls.FUSE
            raise SandboxCreationError(
                "Can't mount an overlayfs sandbox's rootfs on this node. "
                f"Mounting with the kernel's erofs driver failed with: {kernel_errstr}."
                f" Mounting with erofsfuse failed with: {fuse_errstr}"
            )

        # In a private user namespace, only erofsfuse can mount the image,
        # since the kernel doesn't allow erofs mounts inside it.
        mounted, fuse_errstr = _probe_mount(UserNamespaceType.PRIVATE, cls.FUSE)
        if mounted:
            return cls.FUSE
        raise SandboxCreationError(
            "Can't mount an overlayfs sandbox's rootfs on this node. "
            f"Mounting with erofsfuse failed with: {fuse_errstr}"
        )


@dataclass(frozen=True)
class RootfsOverlay:
    """An overlayfs sandbox's kernel overlay, ready to mount.

    ``wrap`` turns a command into one that mounts the overlay in fresh
    namespaces and then runs it there.
    """

    # The EROFS image the overlay sits on.
    image: str
    # How the EROFS image is mounted.
    image_mount_mode: ImageMountMode
    # Which user namespace the overlay is mounted in.
    userns: UserNamespaceType
    # The uid and gid maps written for that user namespace, so files keep
    # their owners from the image.
    id_maps: Optional[IdMaps]
    # The directory the overlay is mounted on. It's root.path in the bundle's
    # config.json.
    mountpoint: str
    # The directory the overlay's tmpfs is mounted on.
    tmpfs_dir: str

    def wrap(self, command: List[str]) -> List[str]:
        """Wrap a command so it runs with the kernel overlay mounted at
        ``mountpoint``. The overlay is mounted in a new mount namespace, and if
        ``userns`` is PRIVATE, in a new user namespace as well.

        Args:
            command: The command to run with the overlay mounted.

        Returns:
            The wrapped command.
        """
        command = self._wrap_in_overlay_mount(command)
        match self.userns:
            case UserNamespaceType.HOST:
                return self._wrap_in_host_userns(command)
            case UserNamespaceType.PRIVATE:
                return self._wrap_in_private_userns(command)
            case _:
                raise ValueError(f"Unknown user namespace type {self.userns!r}")

    def _wrap_in_overlay_mount(self, command: List[str]) -> List[str]:
        """Wrap a command so it runs through a bash script that first mounts
        the kernel overlay and then executes it.

        Builds up each step of the script as a shell command, in the order the
        script runs them, then stitches them together into the script at the
        end.

        Args:
            command: The command to run once the overlay is mounted.

        Returns:
            A command that runs the script with bash.
        """
        # The arguments the script works with.
        set_args = [
            f"image={shlex.quote(self.image)}",
            f"tmpfs={shlex.quote(self.tmpfs_dir)}",
            f"mnt={shlex.quote(self.mountpoint)}",
        ]

        # Mount the tmpfs, and create the directories for the image mount and
        # the overlay's writable layers on it.
        setup_tmpfs = [
            f'mount -t tmpfs -o size={_TMPFS_SIZE},mode=0755 tmpfs "$tmpfs"',
            'mkdir "$tmpfs/lower" "$tmpfs/upper" "$tmpfs/work"',
        ]

        match self.image_mount_mode:
            case ImageMountMode.KERNEL:
                # TODO(klueska): Attempt to mount the image straight from its
                # file first, before falling back to a loop device. Supported on
                # Linux 6.12+ kernels built with file-backed EROFS support.
                # Blocked until util-linux's mount can request a file-backed
                # mount. Otherwise, it has to call libc's mount() through
                # Python's ctypes.

                # mount sets up one loop device per image and reuses it for
                # every sandbox on that image, but sandboxes that set it up at
                # the same time race, and all but one fail. flock on the image
                # makes them take turns.
                mount_image = [
                    'flock "$image" mount -t erofs -o ro,loop "$image" "$tmpfs/lower"'
                ]
            case ImageMountMode.FUSE:
                # erofsfuse runs in the foreground with -f, so it stays in the
                # sandbox's process group and gets killed when the sandbox is
                # torn down. Passing --dbglevel=0 makes it only print errors to
                # the sandbox's stderr log. setpriv --pdeathsig stops erofsfuse
                # once the process that started it exits, whether a later step
                # fails, the command it execs exits, or it gets killed.
                start_erofsfuse = (
                    "{ setpriv --pdeathsig KILL "
                    "erofsfuse -f --dbglevel=0 -o allow_other "
                    '"$image" "$tmpfs/lower" >/dev/null & fuse=$!; }'
                )
                # Wait up to 5s for its mount.
                wait_for_mount = (
                    "for _ in $(seq 100); do "
                    'mountpoint -q "$tmpfs/lower" && break; '
                    'kill -0 "$fuse" 2>/dev/null || break; '
                    "sleep 0.05; "
                    "done"
                )
                # Fail if the mount never came up.
                check_mount = 'mountpoint -q "$tmpfs/lower"'
                mount_image = [start_erofsfuse, wait_for_mount, check_mount]
            case _:
                raise ValueError(f"Unknown image mount mode {self.image_mount_mode!r}")

        # overlayfs gives the overlay's root the upper layer's attributes, so
        # copy the image root's owner, mode and timestamps onto the upper layer.
        # A user namespace that doesn't map the image root's owner can't chown
        # to it, which leaves the upper layer's root owned by root.
        copy_root_attrs = [
            '{ chown --reference="$tmpfs/lower" "$tmpfs/upper" 2>/dev/null || true; }',
            'chmod --reference="$tmpfs/lower" "$tmpfs/upper"',
            'touch -r "$tmpfs/lower" "$tmpfs/upper"',
        ]

        # The overlay's read-only lower layer is the mounted image, and its
        # writable upper and work layers sit on the tmpfs.
        options = "lowerdir=$tmpfs/lower,upperdir=$tmpfs/upper,workdir=$tmpfs/work"

        # Inside a user namespace, overlayfs keeps its metadata in user
        # xattrs, since it can't write trusted ones there.
        if self.userns is UserNamespaceType.PRIVATE:
            options += ",userxattr"

        # Mount the overlay with these options at the mountpoint.
        mount_overlay = f'mount -t overlay overlay -o "{options}" "$mnt"'

        steps = [
            *set_args,
            *setup_tmpfs,
            *mount_image,
            *copy_root_attrs,
            mount_overlay,
        ]

        script = (
            f"{{ {' && '.join(steps)}; }} "
            '|| { echo "rootfs overlay mount failed" >&2; exit 1; } '
            f"&& exec {shlex.join(command)}"
        )

        return ["bash", "-c", script]

    def _wrap_in_host_userns(self, command: List[str]) -> List[str]:
        """Wrap a command in a new mount namespace, in the host's user namespace."""
        return ["unshare", "--mount", "--", *command]

    def _wrap_in_private_userns(self, command: List[str]) -> List[str]:
        """Wrap a command so it runs in a new mount namespace and a new user
        namespace.

        For a worker running as root, the new user namespace maps every id the
        worker has to itself, so files in the image keep their owners, and
        root can reach every file it can reach outside it. For a worker running
        as any other user, it maps root to the worker's user. If that worker
        has subordinate ids, it also maps the ids from 1 up inside it onto
        them, one by one, so files in the image owned by those ids keep their
        owners.

        Only a worker running as root, or newuidmap and newgidmap for
        subordinate ids, can write these maps, and only into a namespace
        another process created, so the command waits on a FIFO in the bundle
        until they have.

        Args:
            command: The command to run in the new namespaces.

        Returns:
            The wrapped command.
        """
        # Without subordinate ids, unshare can create the new user and mount
        # namespaces itself, mapping only root to the worker. Files owned by
        # any other id show up as owned by nobody.
        if self.id_maps is None:
            return [
                "unshare",
                "--user",
                "--map-root-user",
                "--mount",
                "--",
                *command,
            ]

        # With subordinate ids, lay out the uid and gid maps' entries, as
        # newuidmap and newgidmap take them.
        uidmap = " ".join(" ".join(map(str, entry)) for entry in self.id_maps.uid_map)
        gidmap = " ".join(" ".join(map(str, entry)) for entry in self.id_maps.gid_map)

        # When running as root, the worker writes the maps itself, one entry
        # per line, since newuidmap and newgidmap only map ids /etc/subuid and
        # /etc/subgid allow. cat writes each map in a single write, as the
        # kernel requires. When running as any other user, newuidmap and
        # newgidmap write them.
        if os.getuid() == 0:
            map_uids_command = (
                f'm=$(printf "%s %s %s\\n" {uidmap}) && '
                'cat <<< "$m" > /proc/$ns/uid_map'
            )
            map_gids_command = (
                f'm=$(printf "%s %s %s\\n" {gidmap}) && '
                'cat <<< "$m" > /proc/$ns/gid_map'
            )
        else:
            map_uids_command = f'newuidmap "$ns" {uidmap}'
            map_gids_command = f'newgidmap "$ns" {gidmap}'

        # Now, build out the script needed to run the command in a new mount
        # namespace and a new user namespace, with all uids and gids mapped as
        # defined above. Each step of the script is built up as a shell
        # command, in the order the script runs them, then stitched together
        # into the script at the end.

        # First define ready_file, a FIFO that gates the starting of the command
        # after it has been launched in its new namespaces, until its ids are
        # fully mapped. Note that run_when_ready starts before its ids are
        # mapped, so it caches its uid as nobody. Execing the command directly
        # would check the command's permissions as nobody. Execing it through
        # env defers its execution until after the exec of env has picked up
        # the mapped ids.
        ready_file = shlex.quote(f"{self.tmpfs_dir}.ready")
        create_fifo = f"mkfifo {ready_file} || exit 1"
        run_when_ready = f"read -r _ <&3; exec env -- {shlex.join(command)} 3<&-"

        # Launch run_when_ready in new user and mount namespaces, with
        # ready_file open as its fd 3. It's opened before entering them, since
        # until its ids are mapped, run_when_ready may not be able to reach
        # ready_file's path. Opening a FIFO for reading and writing doesn't
        # wait for a writer.
        launch_command = (
            f"unshare --user --mount -- bash -c {shlex.quote(run_when_ready)} "
            f"3<> {ready_file} & ns=$!"
        )

        # Wait for the user namespace to come up. The id maps can only be
        # written once run_when_ready is in its new user namespace. Until then,
        # /proc/$ns/uid_map is the worker's own, already written map. If
        # run_when_ready dies first, readlink finds nothing, which ends the wait
        # too, and mapping its ids then fails and reports it.
        wait_for_userns = (
            'until [ "$(readlink /proc/$ns/ns/user)" != '
            '"$(readlink /proc/$$/ns/user)" ]; do sleep 0.01; done'
        )

        # Map the ids and release run_when_ready, or stop it if mapping fails.
        map_ids = (
            f"if {map_uids_command} && {map_gids_command}; "
            f"then echo > {ready_file}; "
            'else echo "mapping ids failed" >&2; kill "$ns"; '
            "fi"
        )

        # Exit with run_when_ready's status.
        wait_command = f'wait "$ns"; status=$?; rm -f {ready_file}; exit "$status"'

        steps = [
            create_fifo,
            launch_command,
            wait_for_userns,
            map_ids,
            wait_command,
        ]

        return ["bash", "-c", "; ".join(steps)]


# The OCI hooks that run on the host before a container starts, and so can
# write to its rootfs.
_PRESTART_HOOKS = ("prestart", "createRuntime", "createContainer")


def needs_rootfs_overlay(spec: Dict[str, Any]) -> bool:
    """Whether a sandbox with this final OCI spec boots from a kernel overlay
    over its cached EROFS image, rather than from the image itself.

    It does when OCI hooks run on the host before it starts, since those can
    write to its rootfs, and those writes must never reach the cached image.
    It also does when its rootfs is read-only and some of its mounts need
    mount points the image doesn't have. runsc applies no overlay of its own
    to a read-only root, and can't create them in the image, but can in the
    kernel overlay's upper layer.
    """
    if has_prestart_hooks(spec):
        return True
    if not spec.get("root", {}).get("readonly"):
        return False
    return len(missing_mount_points(spec)) > 0


def has_prestart_hooks(spec: Dict[str, Any]) -> bool:
    """Whether this OCI spec has hooks that run on the host before the
    container starts."""
    hooks = spec.get("hooks") or {}
    return any(hooks.get(kind) for kind in _PRESTART_HOOKS)


def missing_mount_points(spec: Dict[str, Any]) -> List[str]:
    """The destinations of this OCI spec's mounts that need a mount point in
    the image, which it may not have.

    Every image has the mount points image_utils seeds into it at build time,
    so mounts there never need one. Neither does a mount inside another
    mount, such as a device node under /dev, since its mount point gets
    created in that mount rather than in the image. This doesn't check
    whether the image has any others, so a mount onto a path it does have
    counts too.
    """
    # Build a list of available mount points, which every image has. This
    # includes "/" as well as the directories and files image_utils seeds into
    # every image at build time.
    available = {"/"}
    for path in image_utils._MOUNTPOINT_DIRS + image_utils._MOUNTPOINT_FILES:
        available.add(os.path.join("/", path))

    # Build the list of desired mounts from the spec.
    mounts = []
    for m in spec.get("mounts") or []:
        if m.get("destination"):
            mounts.append(os.path.normpath(m["destination"]))

    # Define a helper function to check if a given mount is inside another
    # mount or not. If it is, its mount point comes from that other mount
    # rather than the image, so it doesn't need to be in "available".
    def inside_another_mount(path: str) -> bool:
        parent = os.path.dirname(path)
        if parent == path:
            return False
        return parent in mounts or inside_another_mount(parent)

    # Build the list of missing mount points and return it. Skip any mounts
    # that are verified as already available or inside another mount.
    missing = []
    for m in mounts:
        if m in available or inside_another_mount(m):
            continue
        missing.append(m)
    return missing


def can_mount_rootfs_overlay() -> bool:
    """Whether this worker can mount an overlayfs sandbox's rootfs, as
    ``UserNamespaceType.detect`` and ``ImageMountMode.detect`` decide.

    Returns:
        True if it can, else False.

    Raises:
        SandboxCreationError: If a probe times out, since that says nothing
            about whether it can.
    """
    try:
        ImageMountMode.detect(UserNamespaceType.detect())
    except _ProbeTimeoutError:
        raise
    except SandboxCreationError:
        return False
    return True


def prepare(
    *,
    image: str,
    mountpoint: str,
    bundle_dir: str,
    userns: Optional[UserNamespaceType] = None,
    image_mount_mode: Optional[ImageMountMode] = None,
) -> RootfsOverlay:
    """Create an overlayfs sandbox's overlay directories in its bundle, and
    return the RootfsOverlay that mounts its kernel overlay the way this
    worker can.

    Args:
        image: The EROFS image the kernel overlay sits on.
        mountpoint: The directory to mount the kernel overlay on.
        bundle_dir: The sandbox's bundle directory.
        userns: The type of user namespace to mount it in. Detected if not given.
        image_mount_mode: How to mount the EROFS image. Detected if not given.

    Returns:
        The sandbox's kernel overlay, ready to wrap its runsc command with.

    Raises:
        SandboxCreationError: If the bundle's path can't appear in overlayfs
            mount options, or this worker can't mount the kernel overlay.
    """
    # The overlay's layers live in the tmpfs mounted here.
    tmpfs_dir = os.path.join(bundle_dir, _TMPFS_DIRNAME)
    if "," in tmpfs_dir or ":" in tmpfs_dir:
        raise SandboxCreationError(
            f"Can't mount an overlay's layers under {tmpfs_dir!r}, since "
            "overlayfs mount options treat ',' and ':' as separators."
        )

    # Where and how this worker mounts the overlay, or an error saying why it
    # can't.
    if userns is None:
        userns = UserNamespaceType.detect()
    if image_mount_mode is None:
        image_mount_mode = ImageMountMode.detect(userns)

    # Only a private user namespace maps the worker's subordinate ids.
    id_maps = None
    if userns is UserNamespaceType.PRIVATE:
        id_maps = IdMaps.detect()

    # The directories the tmpfs and the overlay get mounted on.
    os.makedirs(tmpfs_dir, exist_ok=True)
    os.makedirs(mountpoint, exist_ok=True)

    return RootfsOverlay(
        image=image,
        image_mount_mode=image_mount_mode,
        userns=userns,
        id_maps=id_maps,
        mountpoint=mountpoint,
        tmpfs_dir=tmpfs_dir,
    )


def _is_host_userns() -> bool:
    """Whether this process runs in the host's user namespace."""
    try:
        return os.stat("/proc/self/ns/user").st_ino == _HOST_USERNS_INO
    except OSError:
        return False


@functools.lru_cache(maxsize=None)
def _has_mount_privileges() -> bool:
    """Whether this process has mount privileges, found by unsharing a mount
    namespace. Probed once per process."""
    # Run unshare in a subprocess. A timeout says nothing about whether this
    # worker has mount privileges, so it raises rather than caching an answer.
    try:
        res = subprocess.run(
            ["unshare", "--mount", "true"],
            capture_output=True,
            timeout=30,
        )
    except subprocess.TimeoutExpired as err:
        raise _ProbeTimeoutError(
            "Timed out checking whether this worker has mount privileges."
        ) from err
    except OSError:
        return False
    return res.returncode == 0


@functools.lru_cache(maxsize=None)
def _probe_mount(
    userns: UserNamespaceType, image_mount_mode: ImageMountMode
) -> Tuple[bool, str]:
    """Checks whether this worker can mount a sandbox's rootfs, by mounting a
    throwaway one over an empty EROFS image. Probed once per process for each
    pair of arguments.

    Args:
        userns: The type of user namespace to mount it in.
        image_mount_mode: How to mount the EROFS image.

    Returns:
        Whether the mount worked, and if not, the error from its stderr.
    """
    with tempfile.TemporaryDirectory(
        prefix="ray-overlay-probe-", ignore_cleanup_errors=True
    ) as scratch:
        empty = os.path.join(scratch, "empty")
        os.mkdir(empty)
        image = os.path.join(scratch, "probe.erofs")
        image_utils.build_erofs_image(empty, {}, image)

        # Mount it exactly the way a sandbox would, and run nothing in it.
        overlay = prepare(
            image=image,
            mountpoint=os.path.join(scratch, "mnt"),
            bundle_dir=scratch,
            userns=userns,
            image_mount_mode=image_mount_mode,
        )

        # Run it the way a sandbox runs, in its own session with stderr going
        # to a file, so that a helper such as erofsfuse that outlives it can't
        # keep the probe waiting on a pipe, and gets killed with its process
        # group once the probe is done.
        with open(os.path.join(scratch, "probe.stderr"), "w+") as stderr:
            try:
                proc = subprocess.Popen(
                    overlay.wrap(["true"]),
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL,
                    stderr=stderr,
                    start_new_session=True,
                )
            except OSError as err:
                return False, str(err)
            try:
                returncode = proc.wait(timeout=30)
            except subprocess.TimeoutExpired as err:
                # A hang says nothing about whether this worker can mount, so
                # it raises rather than returning an answer that gets cached.
                raise _ProbeTimeoutError(
                    "Timed out checking whether this worker can mount an "
                    "overlayfs sandbox's kernel overlay."
                ) from err
            finally:
                try:
                    os.killpg(proc.pid, signal.SIGKILL)
                except (ProcessLookupError, PermissionError):
                    if proc.poll() is None:
                        proc.kill()
                proc.wait()
            if returncode != 0:
                stderr.seek(0)
                errstr = stderr.read().strip()[-500:]
                return False, errstr or f"exit status {returncode}"
            return True, ""
