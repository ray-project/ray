"""Mounting public cloud buckets on the node, for gVisor sandboxes to bind-mount.

A sandbox never runs the mount itself. Each bucket is mounted in the node's
namespace by its own ``mount-s3`` (AWS Mountpoint) process and handed to the
container as an ordinary bind mount, so the sandbox sees files and never sees
how they are fetched. Bucket traffic leaves through the node's network, so a
sandbox with ``network="none"`` can still read its buckets, as on Modal.

Four constraints shape everything here.

A bucket is mounted on the node only until its sandbox is running. runsc's
gofer binds each mount into the container in a mount namespace of its own when
the container starts, and the sandbox then reaches the bucket through that
bind and the file descriptors gVisor holds on it -- never through the node's
mountpoint again. So the node's mountpoint is detached right after the start
(:meth:`BucketMountSet.detach`), and from then on the filesystem lives only as
long as the container: when the container ends, the kernel drops the last
reference, and mount-s3, its connection closed, exits by itself. The container
ends with the sandbox however that ends -- deleted, or, when the worker owning
it dies, ended by the sandbox's supervisor (see ``teardown``) -- and nothing on
the node is ever left attached to a bucket.

The mountpoints live **outside** the sandbox's own directory tree. The gVisor
backend ends a sandbox with ``shutil.rmtree(root_dir)``, and that walk would
delete a writable bucket's objects if a mount were still attached beneath it --
as one is while the sandbox is being created. Mountpoints therefore live under
:data:`MOUNT_ROOT`, and this module removes only files and empty directories it
created itself, never recursively -- save each mount's cache directory, which
is never a mountpoint and is removed only when nothing is mounted at or under
it.

A mount process never sees the node's cloud credentials. Only public buckets
are supported, so every mount is anonymous (``--no-sign-request``) and runs
with a freshly built environment: no ``AWS_*`` from the worker, a private
``HOME``, and instance metadata disabled.

The flag set is fixed. A bucket must behave the same in every sandbox
configuration -- network mode, rootless or not, whatever user a command runs
as -- so nothing below branches on the sandbox's settings. The owner and modes
are mountpoint's defaults, which is what Modal shows: everything belongs to
the uid that mounted it, and gVisor presents that uid as the sandbox's root.
Without ``--allow-other``, only that uid can enter the mount on the host. The
caching is Modal's, as measured on Modal: ``--cache`` keeps object contents on
the node's disk, in each mount's own directory and capped at
:data:`_CACHE_MAX_MIB`, and keeps metadata -- that a name does not exist
included -- for 60 seconds. Listings are always current; a file changed or
deleted elsewhere can look unchanged for up to a minute, and one created
elsewhere after its name was looked up can look missing for as long. Nothing
refreshes it sooner: Mountpoint cannot drop a cache short of a remount, and a
running container cannot remount -- so ``reload_volumes()`` cannot, just as
Modal's reloads only Volumes.
"""

import functools
import json
import logging
import os
import re
import shutil
import signal
import subprocess
import tempfile
import threading
import time
from typing import Dict, List, Optional, Sequence, Tuple
from urllib.parse import urlparse

from ray.experimental.sandbox._internal import teardown as _host
from ray.experimental.sandbox.config import BucketMount
from ray.experimental.sandbox.exceptions import SandboxCreationError

logger = logging.getLogger(__name__)

# The mount table and process helpers live in teardown, which a sandbox's
# supervisor runs without ray; these names stay for existing callers.
is_mounted = _host.is_mounted
_proc_start_time = _host._proc_start_time
_pid_namespace = _host._pid_namespace

# Where bucket mountpoints live: a sibling of the sandbox tree
# (/tmp/ray/sandbox/<id>), never inside it. See the module docstring.
MOUNT_ROOT = "/tmp/ray/sandbox-mounts"

# The version these flags were tested against. --memory-target is the newest
# of them (1.24.0); requiring one version keeps a bucket's behavior the same on
# every node.
MIN_MOUNT_S3_VERSION = (1, 24, 0)

_MOUNT_S3 = _host.MOUNT_S3
_FUSE_DEVICE = "/dev/fuse"
# mount-s3 links libfuse 2, which mounts as a non-root user by running this
# setuid helper, by name. fuse3's fusermount3 does not stand in for it.
_MOUNT_HELPER = "fusermount"

# Mountpoint's own defaults, pinned so a future default cannot change what a
# sandbox sees. They match what Modal's bucket mounts show.
_DIR_MODE = "0755"
_FILE_MODE = "0644"

# Mountpoint defaults to 95% of the node's memory, per process, and a node may
# run many sandboxes. The target caps read-ahead and how many files can be open
# for writing at once (opening one more fails with ENOMEM), not correctness.
# mount-s3's minimum, chosen to keep each mount's memory -- and Ray's
# reservation for it -- small. Measured against a local S3 server, it peaked
# near 430 MiB, read one large file about half as fast as 1024 or 2048 did, and
# allowed 47 files open for writing at once.
_MEMORY_TARGET_MIB = 512

# Each mount's cache of object contents on the node's disk, which mount-s3
# otherwise lets grow until the disk is 95% full -- where Ray itself starts
# refusing work. Least recently used data goes first; reads past it stream
# from the bucket. mount-s3 empties the cache when it exits cleanly, as it does
# when its container ends; what a killed one leaves is removed with its mount.
_CACHE_MAX_MIB = 1024


def memory_reservation_bytes(mounts: Sequence[BucketMount]) -> int:
    """The memory to reserve in Ray for a sandbox's bucket mounts.

    Each mount runs its own mount-s3 on the node, outside the sandbox's memory
    limit, and it may use up to its target. Reserving that keeps Ray from
    placing more bucket mounts on a node than its memory can hold.

    Args:
        mounts: The sandbox's bucket mounts.

    Returns:
        Bytes of Ray ``memory`` to reserve for them.
    """
    return len(mounts) * _MEMORY_TARGET_MIB * 1024 * 1024


# How long a mount may take to appear, then answer a stat. Fixed rather than
# derived from the sandbox's creation timeout, so it is the same everywhere.
_MOUNT_READY_TIMEOUT_S = 60.0
_HEALTH_CHECK_TIMEOUT_S = 20.0
_POLL_INTERVAL_S = 0.05

# After an unmount, mount-s3 finishes in-flight uploads and exits on its own;
# only then is it signalled.
_EXIT_GRACE_S = 10.0

_R2_HOST_SUFFIX = "r2.cloudflarestorage.com"
_AWS_HOST_SUFFIX = "amazonaws.com"
_LOG_TAIL_BYTES = 4096

# Environment variables a mount process may inherit. Built from an allowlist,
# never copied wholesale: see the module docstring. LD_LIBRARY_PATH is how a
# node with libfuse outside the default paths lets mount-s3 load it.
_INHERITED_ENV = (
    "PATH",
    "LD_LIBRARY_PATH",
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "NO_PROXY",
    "http_proxy",
    "https_proxy",
    "no_proxy",
)


def check_prerequisites() -> None:
    """Check that this node can mount a bucket at all.

    Raises:
        SandboxCreationError: The node lacks mount-s3 (or has one older than
            :data:`MIN_MOUNT_S3_VERSION`), a usable ``/dev/fuse``, or an
            unmount helper.
    """
    if shutil.which(_MOUNT_S3) is None:
        raise SandboxCreationError(
            "Mounting a cloud bucket requires the 'mount-s3' binary (AWS "
            "Mountpoint) on the node running the sandbox, and it was not found "
            "on PATH. Install it from https://github.com/awslabs/mountpoint-s3."
        )
    version = _mount_s3_version()
    if version is None or version < MIN_MOUNT_S3_VERSION:
        found = ".".join(map(str, version)) if version else "an unknown version"
        raise SandboxCreationError(
            "Mounting a cloud bucket requires mount-s3 "
            f"{'.'.join(map(str, MIN_MOUNT_S3_VERSION))} or newer on the node; "
            f"found {found}."
        )
    if not os.access(_FUSE_DEVICE, os.R_OK | os.W_OK):
        raise SandboxCreationError(
            f"Mounting a cloud bucket requires read/write access to {_FUSE_DEVICE} "
            "on the node running the sandbox. Install FUSE; in a Kubernetes pod, "
            "expose the device to the container."
        )
    # Root mounts directly; anyone else goes through libfuse 2's helper.
    if os.geteuid() != 0 and shutil.which(_MOUNT_HELPER) is None:
        raise SandboxCreationError(
            f"Mounting a cloud bucket as a non-root user requires '{_MOUNT_HELPER}' "
            "on the node: mount-s3 uses libfuse 2, which mounts through it, and "
            "'fusermount3' does not stand in for it. Install the package that "
            "provides it ('fuse' on Debian and Ubuntu)."
        )
    if os.geteuid() != 0 and _no_new_privs():
        # Measured: mount-s3 then fails with "Failed to create FUSE session:
        # Operation not permitted", after the image pull, with no hint why.
        raise SandboxCreationError(
            "Mounting a cloud bucket as a non-root user goes through the setuid "
            f"'{_MOUNT_HELPER}' helper, but this process runs with no_new_privs "
            "set, which stops that helper from gaining the privilege to mount. "
            "In Kubernetes this is allowPrivilegeEscalation: false; allow "
            "privilege escalation for the Ray worker's container on nodes that "
            "mount buckets."
        )
    if _host._unmount_binary() is None:
        raise SandboxCreationError(
            "Mounting a cloud bucket requires 'fusermount' or 'fusermount3' on "
            "the node, so the mount can be released when the sandbox ends."
        )


def _no_new_privs() -> bool:
    """Whether this process, and so any mount-s3 it starts, may not gain
    privileges through a setuid binary."""
    try:
        with open("/proc/self/status", encoding="utf-8") as f:
            for line in f:
                if line.startswith("NoNewPrivs:"):
                    return line.split()[1:2] == ["1"]
    except OSError:
        pass
    return False


def _mount_s3_version() -> Optional[Tuple[int, ...]]:
    """The node's mount-s3 version, or None if it cannot be read.

    Asked once per binary: every sandbox creation checks it, and an upgrade
    in place changes the file, so it is asked again.
    """
    path = shutil.which(_MOUNT_S3)
    try:
        stat = os.stat(path) if path else None
    except OSError:
        stat = None
    if stat is None:
        return _read_mount_s3_version(_MOUNT_S3)
    return _cached_mount_s3_version(path, stat.st_mtime_ns, stat.st_size)


@functools.lru_cache(maxsize=4)
def _cached_mount_s3_version(
    path: str, mtime_ns: int, size: int
) -> Optional[Tuple[int, ...]]:
    return _read_mount_s3_version(path)


def _read_mount_s3_version(binary: str) -> Optional[Tuple[int, ...]]:
    try:
        result = subprocess.run(
            [binary, "--version"], capture_output=True, text=True, timeout=10
        )
    except (OSError, subprocess.SubprocessError):
        return None
    match = re.search(r"(\d+)\.(\d+)\.(\d+)", result.stdout or result.stderr or "")
    if match is None:
        return None
    return tuple(int(part) for part in match.groups())


def _endpoint_host(mount: BucketMount) -> str:
    return (urlparse(mount.endpoint_url).hostname or "").lower()


def build_mount_command(
    mount: BucketMount, mountpoint: str, home_dir: str, cache_dir: str
) -> Tuple[List[str], Dict[str, str]]:
    """Build the argv and environment of one mount process.

    Pure, so the properties that matter -- an anonymous mount, the fixed flag
    set, and an environment carrying none of the node's credentials -- are
    testable without FUSE.

    Args:
        mount: The bucket to mount.
        mountpoint: Existing empty directory to mount onto.
        home_dir: A private HOME for the process, so it cannot read the
            invoking user's AWS config or credentials files.
        cache_dir: Existing private directory for the mount's cache.

    Returns:
        ``(argv, env)`` for :class:`subprocess.Popen`.
    """
    argv = [
        _MOUNT_S3,
        # A child we can wait on, rather than a daemon that forked away.
        "--foreground",
        # Public buckets only: this is also what stops mount-s3 falling back
        # to any ambient credential provider.
        "--no-sign-request",
        f"--dir-mode={_DIR_MODE}",
        f"--file-mode={_FILE_MODE}",
        f"--memory-target={_MEMORY_TARGET_MIB}",
        # Its logs would otherwise go to the node's syslog -- naming the
        # objects a sandbox touches -- and, in the foreground, to stdout
        # without bound. A failed start and a panic still explain themselves
        # on stderr, which is what failure_reason reads.
        "--no-log",
        # Modal's caching (measured on Modal): object contents kept locally,
        # metadata and missing names for 60 seconds, which --cache implies.
        f"--cache={cache_dir}",
        f"--max-cache-size={_CACHE_MAX_MIB}",
    ]
    if mount.key_prefix:
        argv.append(f"--prefix={mount.key_prefix}")
    if mount.endpoint_url:
        argv.append(f"--endpoint-url={mount.endpoint_url}")
    region = mount.region
    if region is None and _endpoint_host(mount).endswith(_R2_HOST_SUFFIX):
        # R2 has no regions, but the client still signs for one.
        region = "auto"
    if region:
        argv.append(f"--region={region}")
    if mount.force_path_style:
        argv.append("--force-path-style")
    if mount.endpoint_url and not _endpoint_host(mount).endswith(_AWS_HOST_SUFFIX):
        # S3-compatible services other than S3 itself (GCS among them) reject
        # the additional checksums mount-s3 sends on upload by default.
        argv.append("--upload-checksums=off")
    if mount.read_only:
        argv.append("--read-only")
    else:
        # Mountpoint refuses both by default; a Modal bucket mount allows them.
        argv += ["--allow-delete", "--allow-overwrite"]
    argv += ["--", mount.bucket_name, mountpoint]

    env = {key: os.environ[key] for key in _INHERITED_ENV if key in os.environ}
    env.setdefault("PATH", "/usr/local/bin:/usr/bin:/bin")
    env["HOME"] = home_dir
    # Otherwise mount-s3 asks the instance metadata service for a region,
    # which on a cloud node is also the node's identity.
    env["AWS_EC2_METADATA_DISABLED"] = "true"
    return argv, env


class _Mount:
    """One bucket at one path, served by its own mount-s3 process."""

    def __init__(self, spec: BucketMount, parent_dir: str, index: int):
        self.spec = spec
        self.host_dir = os.path.join(parent_dir, str(index))
        self.home_dir = os.path.join(parent_dir, f"{index}-home")
        self.cache_dir = os.path.join(parent_dir, f"{index}-cache")
        self.pid_file = os.path.join(parent_dir, f"{index}.pid")
        self.log_file = os.path.join(parent_dir, f"{index}.log")
        self.process: Optional[subprocess.Popen] = None

    def __repr__(self) -> str:
        return (
            f"_Mount(bucket={self.spec.bucket_name!r}, path={self.spec.mount_path!r})"
        )

    def failure_reason(self) -> str:
        """What mount-s3 said about why it failed: its error, or the tail of
        its output (a panic, say)."""
        try:
            with open(self.log_file, "rb") as f:
                f.seek(0, os.SEEK_END)
                f.seek(max(0, f.tell() - _LOG_TAIL_BYTES))
                text = f.read().decode("utf-8", errors="replace")
        except OSError:
            return ""
        error_at = text.find("Error:")
        text = text[error_at:] if error_at >= 0 else text
        return " ".join(line.strip() for line in text.splitlines() if line.strip())

    def spawn(self) -> None:
        argv, env = build_mount_command(
            self.spec, self.host_dir, self.home_dir, self.cache_dir
        )
        with open(self.log_file, "wb") as log:
            self.process = subprocess.Popen(
                argv,
                env=env,
                stdin=subprocess.DEVNULL,
                stdout=log,
                stderr=subprocess.STDOUT,
                close_fds=True,
                # Its own group, so the whole process can be signalled. No
                # preexec_fn or parent-death signal: both are tied to the
                # spawning thread, and Ray workers are multithreaded. Its
                # lifetime is the container's once detach() has run.
                start_new_session=True,
            )
        with open(self.pid_file, "w", encoding="utf-8") as f:
            f.write(str(self.process.pid))

    def describe(self) -> str:
        return f"bucket {self.spec.bucket_name!r} at {self.spec.mount_path}"


class BucketMountSet:
    """The bucket mounts of one sandbox: started before it, stopped after it."""

    def __init__(self, parent_dir: str, mounts: List[_Mount]):
        self._parent_dir = parent_dir
        self._mounts = mounts
        self._lock = threading.Lock()
        self._stopped = False
        self._detached = False

    @classmethod
    def allocate(
        cls,
        sandbox_id: str,
        mounts: Sequence[BucketMount],
        supervisor: Optional[Tuple[int, Optional[int]]] = None,
    ) -> "BucketMountSet":
        """Create the private directories one sandbox's mounts need.

        The per-sandbox directory has an unpredictable name and mode 0700, so
        nothing else on the node can pre-create or swap a mountpoint. Its name
        starts with this process's pid, and an ``owner`` file records enough to
        tell a dead owner from a reused pid, and the sandbox's supervisor, if
        it has one, from a dead one; see :func:`sweep_leaked_mounts`.

        Args:
            sandbox_id: The sandbox the mounts are for.
            mounts: The buckets to mount.
            supervisor: ``(pid, start_time)`` of the sandbox's supervisor.

        Returns:
            The mounts, not yet started.

        Raises:
            SandboxCreationError: A directory could not be created.
        """
        os.makedirs(MOUNT_ROOT, exist_ok=True)
        try:
            # Shared by every user on the node, like /tmp.
            os.chmod(MOUNT_ROOT, 0o1777)
        except PermissionError:
            pass
        try:
            parent = tempfile.mkdtemp(
                prefix=f"{os.getpid()}-{sandbox_id}-", dir=MOUNT_ROOT
            )
        except OSError as err:
            raise SandboxCreationError(
                f"Failed to create a bucket mount directory in {MOUNT_ROOT}: {err}"
            ) from err
        try:
            with open(os.path.join(parent, "owner"), "w", encoding="utf-8") as f:
                json.dump(
                    {
                        "pid": os.getpid(),
                        "start_time": _host._proc_start_time(os.getpid()),
                        "pid_ns": _host._pid_namespace(),
                        "supervisor_pid": supervisor[0] if supervisor else None,
                        "supervisor_start_time": (
                            supervisor[1] if supervisor else None
                        ),
                    },
                    f,
                )
            items = []
            for index, spec in enumerate(mounts):
                mount = _Mount(spec, parent, index)
                os.mkdir(mount.host_dir, 0o700)
                os.mkdir(mount.home_dir, 0o700)
                os.mkdir(mount.cache_dir, 0o700)
                items.append(mount)
        except BaseException as err:
            # Nobody else will: the caller gets no set to stop, and the sweep
            # leaves a live owner's directory alone.
            _host._remove_mount_files(parent, len(mounts))
            if isinstance(err, OSError):
                raise SandboxCreationError(
                    f"Failed to prepare bucket mount directories in {parent}: {err}"
                ) from err
            raise
        return cls(parent, items)

    def bind_mounts(self) -> List[Tuple[str, str, bool]]:
        """``(destination, host_dir, read_only)`` for each mount."""
        return [(m.spec.mount_path, m.host_dir, m.spec.read_only) for m in self._mounts]

    def start(self) -> None:
        """Mount every bucket and wait until each one answers.

        Raises:
            SandboxCreationError: A mount failed or did not come up in time.
                Every mount is stopped before this propagates.
        """
        try:
            for mount in self._mounts:
                mount.spawn()
            self._wait_until_mounted()
            for mount in self._mounts:
                self._health_check(mount)
        except BaseException:
            self.stop()
            raise
        for mount in self._mounts:
            threading.Thread(
                target=self._watch,
                args=(mount,),
                name=f"bucket-mount-watch-{mount.spec.bucket_name}",
                daemon=True,
            ).start()

    def _wait_until_mounted(self) -> None:
        deadline = time.monotonic() + _MOUNT_READY_TIMEOUT_S
        pending = list(self._mounts)
        while True:
            points = _host._mount_points()
            still = []
            for mount in pending:
                if mount.host_dir in points:
                    continue
                code = mount.process.poll()
                if code is not None:
                    raise SandboxCreationError(
                        f"Failed to mount {mount.describe()}: mount-s3 exited "
                        f"with code {code}. {mount.failure_reason()}".rstrip()
                    )
                still.append(mount)
            if not still:
                return
            if time.monotonic() >= deadline:
                raise SandboxCreationError(
                    f"Timed out after {_MOUNT_READY_TIMEOUT_S:g}s waiting for "
                    f"{still[0].describe()} to mount. {still[0].failure_reason()}".rstrip()
                )
            pending = still
            time.sleep(_POLL_INTERVAL_S)

    @staticmethod
    def _health_check(mount: _Mount) -> None:
        """Confirm the filesystem answers, not merely that it is attached.

        The stat runs in a daemon thread: a stat of a wedged FUSE mount does not
        return, and that must not hang sandbox creation with it.
        """
        outcome: Dict[str, BaseException] = {}

        def probe():
            try:
                os.stat(mount.host_dir)
            except BaseException as err:  # noqa: BLE001
                outcome["error"] = err

        thread = threading.Thread(target=probe, daemon=True)
        thread.start()
        thread.join(_HEALTH_CHECK_TIMEOUT_S)
        if thread.is_alive():
            raise SandboxCreationError(
                f"{mount.describe().capitalize()} mounted but did not respond "
                f"within {_HEALTH_CHECK_TIMEOUT_S:g}s. {mount.failure_reason()}".rstrip()
            )
        if "error" in outcome:
            raise SandboxCreationError(
                f"{mount.describe().capitalize()} mounted but is not usable: "
                f"{outcome['error']}. {mount.failure_reason()}".rstrip()
            )

    def detach(self) -> bool:
        """Detach every mount from the node, once the sandbox is running.

        The sandbox does not need the node's mountpoint any more: runsc's gofer
        bound the filesystem into the container when it started, and gVisor
        holds descriptors on it, so the sandbox's files, their behavior and
        their speed are unchanged. What changes is who holds the filesystem.
        Only the container does, so it ends when the container ends, and
        mount-s3, its connection closed, exits by itself. However the sandbox
        ends -- deleted, or its worker killed, when the sandbox's supervisor
        ends the container -- nothing on the node stays attached to the
        bucket, where a walk of ``/tmp/ray`` could reach into it.

        Never raises. A mount that cannot be detached stays attached, and
        :meth:`stop` or the supervisor releases it.

        Returns:
            Whether every mount was detached.
        """
        detached = True
        for mount in self._mounts:
            try:
                _host._unmount(mount.host_dir)
                detached = detached and not _host.is_mounted(mount.host_dir)
            except Exception:  # noqa: BLE001
                logger.warning("Failed to detach %r", mount, exc_info=True)
                detached = False
        self._detached = True
        return detached

    def _watch(self, mount: _Mount) -> None:
        """Report a mount process that dies while its sandbox is running.

        Nothing is remounted: the container holds the filesystem that was there
        when it started, and a fresh mount underneath would not reach it. Inside
        the sandbox every access to the path now fails with ENOTCONN.

        Once detached, mount-s3 also exits -- cleanly, with 0 -- when the
        container ends before :meth:`stop` runs, as it does whenever a sandbox
        is deleted. That is its lifetime working, not a failure.
        """
        code = mount.process.wait()
        if self._stopped or (self._detached and code == 0):
            return
        logger.warning(
            "mount-s3 for %s exited with code %s while the sandbox was running; "
            "the path reports 'Transport endpoint is not connected' until the "
            "sandbox is recreated. %s",
            mount.describe(),
            code,
            mount.failure_reason(),
        )

    def stop(self) -> None:
        """Unmount, stop every mount process, and remove the directories.

        Idempotent and never raises. Unmounting comes first (a no-op after
        :meth:`detach`): mount-s3 then finishes any in-flight upload and exits
        on its own once the container is gone, and is signalled only if it
        does not. Directories are removed with ``rmdir`` alone and
        skipped while still mounted, so nothing can recurse into a bucket.
        """
        with self._lock:
            if self._stopped:
                return
            self._stopped = True
        self._unmount_all()
        for mount in self._mounts:
            try:
                _stop_process(mount.process)
            except Exception:  # noqa: BLE001
                logger.warning("Failed to stop mount-s3 for %r", mount, exc_info=True)
        # Again: a mount that came up only during the grace period above -- one
        # that timed out starting -- is attached now, and its server is gone.
        self._unmount_all()
        _host._remove_mount_files(self._parent_dir, len(self._mounts))

    def _unmount_all(self) -> None:
        for mount in self._mounts:
            try:
                _host._unmount(mount.host_dir)
            except Exception:  # noqa: BLE001
                logger.warning("Failed to unmount %r", mount, exc_info=True)


def _stop_process(process: Optional[subprocess.Popen]) -> None:
    if process is None or process.poll() is not None:
        return
    try:
        process.wait(_EXIT_GRACE_S)
        return
    except subprocess.TimeoutExpired:
        pass
    for sig in (signal.SIGTERM, signal.SIGKILL):
        _host._signal_group(process.pid, sig)
        try:
            process.wait(_host._SIGNAL_WAIT_S)
            return
        except subprocess.TimeoutExpired:
            continue
    logger.warning("mount-s3 (pid %d) did not exit after SIGKILL.", process.pid)


def sweep_leaked_mounts() -> None:
    """Reclaim the mounts of sandboxes whose creating process is gone.

    A worker that is killed outright runs no teardown; its sandboxes'
    supervisors do, and reclaim these directories with the rest. This is the
    backstop for mounts with no supervisor left to: a dead one's, or one made
    without a sandbox around it. A mount-s3 still serving is stopped, and a
    mount still attached -- of a sandbox that was being created -- is
    unmounted. Only directories owned by this pid namespace whose owner is
    dead, or whose pid now belongs to another process, and whose supervisor
    is not running, are touched; anything unreadable, such as another user's
    0700 directory, is skipped.
    """
    try:
        names = os.listdir(MOUNT_ROOT)
    except OSError:
        return
    pid_ns = _host._pid_namespace()
    for name in names:
        parent = os.path.join(MOUNT_ROOT, name)
        try:
            with open(os.path.join(parent, "owner"), encoding="utf-8") as f:
                owner = json.load(f)
        except (OSError, ValueError):
            continue
        if not isinstance(owner, dict):
            continue
        if owner.get("pid_ns") != pid_ns or owner.get("pid") == os.getpid():
            continue
        start_time = _host._proc_start_time(owner.get("pid", -1))
        if start_time is not None and start_time == owner.get("start_time"):
            continue
        if _host._process_alive(
            owner.get("supervisor_pid"), owner.get("supervisor_start_time")
        ):
            continue
        try:
            _host._reclaim(parent)
        except Exception:  # noqa: BLE001
            logger.warning(
                "Failed to reclaim leaked bucket mounts in %s", parent, exc_info=True
            )
