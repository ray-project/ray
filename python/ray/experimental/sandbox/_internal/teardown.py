"""Tearing a gVisor sandbox down from what is on disk, and the process that does
it when the Ray worker owning the sandbox dies without doing so itself.

A worker that ends without running Python -- SIGKILLed by the memory monitor,
``ray.kill``, a crash, ``ray stop --force`` -- leaves its sandboxes behind: the
container, the mount-s3 serving a bucket to it, the network="public" helpers,
the sandbox directory and its image pin. So every sandbox gets a supervisor,
started before anything else of it exists, that shares the worker's fate
through a pipe:

- The worker holds the pipe's only write end, which no child it execs inherits
  (PEP 446); the supervisor's stdin is the read end. However the worker exits,
  the kernel closes the write end and the supervisor's read returns EOF: it
  runs :func:`teardown_sandbox`. A delete that finished writes ``D`` first,
  and the supervisor exits without acting. Unlike a parent-death signal, none
  of this depends on which of the worker's threads started the supervisor.
- The supervisor is a grandchild. Ray SIGKILLs the direct children of a worker
  it kills, so a keeper -- the parent half of the one ``fork()`` in this file,
  in a fresh interpreter -- takes that kill while the supervisor survives it.
  On a normal delete the supervisor exits, the keeper reaps it and exits, and
  the worker reaps the keeper: nothing is ever left for PID 1 to reap.

Teardown is rebuilt from the sandbox id, the runsc flags, and files written
before the resource each describes exists: ``owner.json``, ``run.pgid``, and a
bucket mount directory's pid files. Nothing read from a file is ever run; a
file can only select which of our own processes to signal, after a check of
what each one is. If the worker and its supervisor both die,
:func:`sweep_orphaned_sandboxes` reclaims what they left.

This file also runs as a script (``python -I teardown.py supervise ...``), in a
process that must never import ray, so it uses the standard library alone.
"""

import fcntl
import glob
import json
import logging
import os
import re
import select
import shutil
import signal
import subprocess
import sys
import threading
import time
from typing import Dict, List, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)

MOUNT_S3 = "mount-s3"
# Unmount helpers, newest first: either can unmount any FUSE mount the caller
# owns.
_UNMOUNT_BINARIES = ("fusermount3", "fusermount")
_SIGNAL_WAIT_S = 5.0
# How long a reclaimed mount-s3 gets to finish its uploads and exit by itself.
_EXIT_GRACE_S = 10.0
_POLL_INTERVAL_S = 0.05

_SANDBOX_ID = re.compile(r"^ray-sandbox-[0-9a-f]{12}$")
_OWNER_FILE = "owner.json"
_RUN_GROUP_FILE = "run.pgid"
# image_utils._USERS_SUBDIR, which this module cannot import.
_IMAGE_USERS_SUBDIR = ".users"
# What the process group of a sandbox's `runsc run` may hold besides runsc: the
# network="public" script, its namespace holder, and slirp4netns.
_RUN_GROUP_COMMANDS = ("slirp4netns", "unshare", "sleep", "bash", "nsenter")

_READY_TIMEOUT_S = 5.0
_DISARM = b"D"


def _unmount_binary() -> Optional[str]:
    for binary in _UNMOUNT_BINARIES:
        if shutil.which(binary):
            return binary
    return None


def _unescape_mountinfo(field: str) -> str:
    """Undo mountinfo's octal escapes (``\\040`` is a space)."""
    return re.sub(r"\\([0-7]{3})", lambda m: chr(int(m.group(1), 8)), field)


def _mount_points(mountinfo: str = "/proc/self/mountinfo") -> set:
    points = set()
    try:
        with open(mountinfo, encoding="utf-8", errors="replace") as f:
            for line in f:
                fields = line.split()
                if len(fields) > 4:
                    points.add(_unescape_mountinfo(fields[4]))
    except OSError:
        pass
    return points


def is_mounted(path: str, mountinfo: str = "/proc/self/mountinfo") -> bool:
    """Whether a filesystem is attached at ``path``, alive or dead.

    Reads the mount table rather than stat-ing the path. ``os.path.ismount``
    stats it, which hangs on a wedged FUSE server and reports a dead one
    (ENOTCONN) as "not a mount point" -- so teardown would skip unmounting the
    very filesystem that most needs it.
    """
    return os.path.abspath(path) in _mount_points(mountinfo)


def _unmount(path: str) -> None:
    """Detach the filesystem at ``path``, lazily if a plain unmount fails.

    A plain unmount succeeds even while a container still uses the mount: the
    gofer holds its own reference, which keeps the files reachable inside the
    sandbox until it is gone.
    """
    if not is_mounted(path):
        return
    binary = _unmount_binary()
    if binary is not None:
        for args in (["-u"], ["-u", "-z"]):
            try:
                subprocess.run(
                    [binary, *args, path], capture_output=True, timeout=_SIGNAL_WAIT_S
                )
            except (OSError, subprocess.SubprocessError):
                continue
            if not is_mounted(path):
                return
    logger.warning(
        "Could not unmount bucket mount %s; leaving it in place rather than "
        "removing a live mountpoint.",
        path,
    )


def _signal_group(pid: int, sig: int) -> None:
    try:
        os.killpg(pid, sig)
    except (ProcessLookupError, PermissionError):
        pass


def _proc_start_time(pid: int) -> Optional[int]:
    """The process's start time in clock ticks, which a reused pid won't share."""
    try:
        with open(f"/proc/{pid}/stat", encoding="utf-8") as f:
            stat = f.read()
    except OSError:
        return None
    # The command name is in parentheses and may contain spaces; field 22 is
    # the 20th after it.
    fields = stat[stat.rfind(")") + 2 :].split()
    try:
        return int(fields[19])
    except (IndexError, ValueError):
        return None


def _pid_namespace() -> Optional[int]:
    try:
        return os.stat("/proc/self/ns/pid").st_ino
    except OSError:
        return None


def _process_alive(pid: object, start_time: object = None) -> bool:
    """Whether ``pid`` is a live process: the one that started at
    ``start_time``, when that is given.

    A zombie counts as dead -- it has exited; only its parent's wait is
    missing -- and so does anything that is not a positive int, as a pid read
    from a file may be.
    """
    if type(pid) is not int or pid <= 0:
        return False
    try:
        with open(f"/proc/{pid}/stat", encoding="utf-8") as f:
            stat = f.read()
    except OSError:
        return False
    fields = stat[stat.rfind(")") + 2 :].split()
    if not fields or fields[0] in ("Z", "X"):
        return False
    if start_time is None:
        return True
    try:
        return int(fields[19]) == start_time
    except (IndexError, ValueError):
        return False


def _unlink(path: str) -> None:
    try:
        os.unlink(path)
    except FileNotFoundError:
        pass
    except OSError as err:
        logger.warning("Could not remove %s: %s", path, err)


def _rmdir(path: str) -> None:
    try:
        os.rmdir(path)
    except FileNotFoundError:
        pass
    except OSError as err:
        logger.warning("Could not remove %s: %s", path, err)


def _remove_mount_files(parent_dir: str, count: int) -> None:
    """Remove what :meth:`BucketMountSet.allocate` created, non-recursively.

    A mountpoint that is still mounted -- and its parent with it -- is left in
    place: removing it is exactly the mistake this module exists to avoid.
    """
    for index in range(count):
        for name in (f"{index}.pid", f"{index}.log"):
            _unlink(os.path.join(parent_dir, name))
        for name in (f"{index}-home", str(index)):
            path = os.path.join(parent_dir, name)
            if is_mounted(path):
                logger.warning("Leaving %s in place: it is still mounted.", path)
                continue
            _rmdir(path)
    _unlink(os.path.join(parent_dir, "owner"))
    _rmdir(parent_dir)


_MOUNT_ENTRY = re.compile(r"^(\d+)(?:-home|\.pid|\.log)?$")


def _reclaim(parent: str, grace_s: float = 0) -> None:
    """Unmount and remove a bucket mount directory, ending its mount-s3s.

    A mount-s3 gets ``grace_s`` to exit by itself -- unmounted, and with no
    container holding its filesystem, it does once its uploads are done --
    and is SIGKILLed after that.
    """
    # Every index anything was created for, including a mount whose process
    # never started (a crash between mkdir and spawn leaves no pid file).
    indexes = sorted(
        {int(m.group(1)) for m in map(_MOUNT_ENTRY.match, os.listdir(parent)) if m}
    )
    servers = []
    for index in indexes:
        host_dir = os.path.join(parent, str(index))
        _unmount(host_dir)
        try:
            with open(os.path.join(parent, f"{index}.pid"), encoding="utf-8") as f:
                pid = int(f.read().strip())
            with open(f"/proc/{pid}/cmdline", "rb") as f:
                cmdline = f.read().split(b"\0")
        except (OSError, ValueError):
            continue
        # Only a mount-s3 serving this very directory: the pid may have been
        # reused by something else entirely.
        if (
            cmdline
            and os.path.basename(cmdline[0]) == MOUNT_S3.encode()
            and (host_dir.encode() in cmdline)
        ):
            servers.append(pid)
    deadline = time.monotonic() + grace_s
    while servers and time.monotonic() < deadline:
        servers = [pid for pid in servers if _process_alive(pid)]
        if servers:
            time.sleep(_POLL_INTERVAL_S)
    for pid in servers:
        _signal_group(pid, signal.SIGKILL)
    count = max(indexes) + 1 if indexes else 0
    _remove_mount_files(parent, count)


# -- What a sandbox leaves on disk for its teardown -----------------------------


def _write_json(path: str, data: Dict) -> None:
    """Replace ``path`` atomically, so a reader never sees half of it."""
    temp = f"{path}.{os.getpid()}.tmp"
    with open(temp, "w", encoding="utf-8") as f:
        json.dump(data, f)
    os.replace(temp, path)


def _read_json(path: str) -> Optional[Dict]:
    """A JSON object this user wrote at ``path``, or None."""
    try:
        with open(path, encoding="utf-8") as f:
            if os.fstat(f.fileno()).st_uid != os.geteuid():
                return None
            data = json.load(f)
    except (OSError, ValueError):
        return None
    return data if isinstance(data, dict) else None


def write_owner(
    root_dir: str,
    *,
    rootless: bool,
    ignore_cgroups: bool,
    supervisor: Optional[Tuple[int, Optional[int]]] = None,
) -> None:
    """Record who owns the sandbox in ``root_dir``, for the backstop sweep.

    Args:
        root_dir: The sandbox's directory.
        rootless: Whether runsc runs the sandbox with ``--rootless``.
        ignore_cgroups: Whether runsc runs it with ``--ignore-cgroups``.
        supervisor: ``(pid, start_time)`` of its supervisor, once known.
    """
    pid = os.getpid()
    _write_json(
        os.path.join(root_dir, _OWNER_FILE),
        {
            "pid": pid,
            "start_time": _proc_start_time(pid),
            "pid_ns": _pid_namespace(),
            "supervisor_pid": supervisor[0] if supervisor else None,
            "supervisor_start_time": supervisor[1] if supervisor else None,
            # Flags, never argv: the sweep rebuilds runsc's arguments from
            # these, so the file cannot choose what runs.
            "rootless": bool(rootless),
            "ignore_cgroups": bool(ignore_cgroups),
        },
    )


def read_owner(root_dir: str) -> Optional[Dict]:
    """What :func:`write_owner` recorded in ``root_dir``, or None."""
    return _read_json(os.path.join(root_dir, _OWNER_FILE))


def write_run_group(root_dir: str, pid: int) -> None:
    """Record the process group ``runsc run`` leads, before anything joins it.

    For network="public" the group also holds the namespace holder and
    slirp4netns, which outlive ``runsc run`` and are tied to it only by the
    group. The start time keeps a later group that reuses the id out.
    """
    _write_json(
        os.path.join(root_dir, _RUN_GROUP_FILE),
        {"pgid": pid, "start_time": _proc_start_time(pid)},
    )


def runsc_base_args(rootless: bool, ignore_cgroups: bool, root: str) -> List[str]:
    """runsc's global flags, shared by every command for one sandbox."""
    args = ["runsc"]
    if rootless:
        args.append("--rootless")
    if ignore_cgroups:
        args.append("--ignore-cgroups")
    args.extend(["--root", root])
    return args


# -- Teardown ---------------------------------------------------------------------


def _attempt(what: str, sandbox_id: str, step) -> None:
    try:
        step()
    except Exception:  # noqa: BLE001
        logger.warning(
            "Failed to %s while tearing down sandbox %s",
            what,
            sandbox_id,
            exc_info=True,
        )


def _lock_owner(root_dir: str) -> Tuple[bool, Optional[int]]:
    """Take the sandbox's teardown lock: ``(proceed, fd to close)``.

    Without an ``owner.json`` there is nothing to serialize on, and nothing
    needs it: every step is idempotent.
    """
    try:
        fd = os.open(
            os.path.join(root_dir, _OWNER_FILE),
            os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC,
        )
    except OSError:
        return True, None
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        os.close(fd)
        return False, None
    return True, fd


def _run_runsc(runsc_args: Sequence[str], args: List[str], timeout: float) -> None:
    subprocess.run(
        [*runsc_args, *args],
        stdin=subprocess.DEVNULL,
        capture_output=True,
        timeout=timeout,
    )


def _kill_run_group(root_dir: str) -> None:
    """SIGKILL what is left of the ``runsc run`` process group.

    The id and start time come from a file, so each member is checked before it
    is signalled: in the group, started no earlier than its leader, and one of
    the commands a sandbox's group runs.
    """
    record = _read_json(os.path.join(root_dir, _RUN_GROUP_FILE)) or {}
    pgid, started = record.get("pgid"), record.get("start_time")
    if type(pgid) is not int or pgid <= 1 or type(started) is not int:
        return
    for name in os.listdir("/proc"):
        if not name.isdigit():
            continue
        try:
            with open(f"/proc/{name}/stat", encoding="utf-8") as f:
                stat = f.read()
        except OSError:
            continue
        command = stat[stat.find("(") + 1 : stat.rfind(")")]
        fields = stat[stat.rfind(")") + 2 :].split()
        try:
            group, start_time = int(fields[2]), int(fields[19])
        except (IndexError, ValueError):
            continue
        if group != pgid or start_time < started:
            continue
        if command not in _RUN_GROUP_COMMANDS and not command.startswith("runsc"):
            continue
        try:
            os.kill(int(name), signal.SIGKILL)
        except OSError:
            pass


def _remove_sandbox_dir(root_dir: str) -> None:
    root = os.path.abspath(root_dir)
    # Bucket mounts never live here, but an rmtree must not reach into one
    # however it got here.
    if any(p == root or p.startswith(root + os.sep) for p in _mount_points()):
        logger.warning("Leaving %s in place: something is mounted under it.", root)
        return
    shutil.rmtree(root, ignore_errors=True)


def teardown_sandbox(
    sandbox_id: str,
    runsc_args: Sequence[str],
    sandbox_dir: str,
    mount_root: str,
    images_dir: str,
    grace_s: float = _EXIT_GRACE_S,
) -> bool:
    """Remove everything of a sandbox, from its id and what is on disk.

    Idempotent, and each step is guarded and bounded, so one that fails does
    not skip the rest: the container (whose end also ends its mount-s3s), the
    rest of its ``runsc run`` process group, its bucket mount directories, its
    directory, and its image pins.

    Args:
        sandbox_id: The sandbox, ``ray-sandbox-<12 hex digits>``.
        runsc_args: runsc's global flags for it (:func:`runsc_base_args`).
        sandbox_dir: The directory holding sandbox directories.
        mount_root: The directory holding bucket mount directories.
        images_dir: The image cache, whose pins name their sandboxes.
        grace_s: How long a mount-s3 may take to exit by itself.

    Returns:
        False if another reaper is tearing the sandbox down already.

    Raises:
        ValueError: ``sandbox_id`` is not a sandbox id.
    """
    if not _SANDBOX_ID.match(sandbox_id):
        raise ValueError(f"Not a sandbox id: {sandbox_id!r}")
    root_dir = os.path.join(sandbox_dir, sandbox_id)
    proceed, lock = _lock_owner(root_dir)
    if not proceed:
        return False
    try:
        _attempt(
            "kill its container",
            sandbox_id,
            lambda: _run_runsc(runsc_args, ["kill", sandbox_id, "SIGKILL"], 5),
        )
        _attempt(
            "delete its container",
            sandbox_id,
            lambda: _run_runsc(runsc_args, ["delete", "-force", sandbox_id], 10),
        )
        _attempt(
            "stop its process group", sandbox_id, lambda: _kill_run_group(root_dir)
        )
        pattern = os.path.join(glob.escape(mount_root), f"*-{sandbox_id}-*")
        for parent in sorted(glob.glob(pattern)):
            _attempt(
                f"reclaim its bucket mounts in {parent}",
                sandbox_id,
                lambda parent=parent: _reclaim(parent, grace_s),
            )
        _attempt(
            "remove its directory", sandbox_id, lambda: _remove_sandbox_dir(root_dir)
        )
        pattern = os.path.join(
            glob.escape(images_dir), "*", _IMAGE_USERS_SUBDIR, sandbox_id
        )
        for pin in glob.glob(pattern):
            _attempt("release its image", sandbox_id, lambda pin=pin: os.unlink(pin))
    finally:
        if lock is not None:
            os.close(lock)
    return True


def sweep_orphaned_sandboxes(
    sandbox_dir: str, mount_root: str, images_dir: str, runsc_root: str
) -> List[str]:
    """Tear down the sandboxes whose worker and supervisor are both gone.

    The backstop for the rare case that both die. Only directories of this
    user, with an ``owner.json`` from this pid namespace, are considered;
    one without the file is left alone. Liveness is judged by pid and start
    time, so a reused pid does not keep a sandbox alive.

    Args:
        sandbox_dir: The directory holding sandbox directories.
        mount_root: The directory holding bucket mount directories.
        images_dir: The image cache, whose pins name their sandboxes.
        runsc_root: runsc's state directory (``--root``).

    Returns:
        The ids of the sandboxes torn down.
    """
    try:
        names = os.listdir(sandbox_dir)
    except OSError:
        return []
    pid_ns = _pid_namespace()
    reclaimed = []
    for name in names:
        if not _SANDBOX_ID.match(name):
            continue
        root_dir = os.path.join(sandbox_dir, name)
        try:
            if os.lstat(root_dir).st_uid != os.geteuid():
                continue
        except OSError:
            continue
        owner = read_owner(root_dir)
        if owner is None or owner.get("pid_ns") != pid_ns:
            continue
        if _process_alive(owner.get("pid"), owner.get("start_time")):
            continue
        if _process_alive(
            owner.get("supervisor_pid"), owner.get("supervisor_start_time")
        ):
            continue
        runsc_args = runsc_base_args(
            owner.get("rootless") is True,
            owner.get("ignore_cgroups") is True,
            runsc_root,
        )
        logger.info("Tearing down sandbox %s: its worker is gone.", name)
        if teardown_sandbox(name, runsc_args, sandbox_dir, mount_root, images_dir):
            reclaimed.append(name)
    return reclaimed


# -- The supervisor ---------------------------------------------------------------


class SandboxSupervisor:
    """The worker's handle on one sandbox's supervisor."""

    def __init__(self, keeper: subprocess.Popen, lifeline: int, ready: int):
        self._keeper = keeper
        self._lifeline: Optional[int] = lifeline
        self._ready: Optional[int] = ready
        self._lock = threading.Lock()
        self.pid: Optional[int] = None
        self.start_time: Optional[int] = None

    @classmethod
    def start(
        cls,
        sandbox_id: str,
        runsc_args: Sequence[str],
        sandbox_dir: str,
        mount_root: str,
        images_dir: str,
    ) -> "SandboxSupervisor":
        """Start the keeper, which forks the supervisor; see :meth:`wait_ready`.

        Args:
            sandbox_id: The sandbox to supervise.
            runsc_args: runsc's global flags for it (:func:`runsc_base_args`).
            sandbox_dir: The directory holding sandbox directories.
            mount_root: The directory holding bucket mount directories.
            images_dir: The image cache, whose pins name their sandboxes.

        Returns:
            The handle, whose lifeline this process alone holds.

        Raises:
            OSError: The keeper could not be started.
        """
        lifeline_r, lifeline_w = os.pipe()
        ready_r, ready_w = os.pipe()
        try:
            keeper = subprocess.Popen(
                [
                    sys.executable,
                    # Isolated: no PYTHONPATH, no user site -- this file alone.
                    "-I",
                    os.path.abspath(__file__),
                    "supervise",
                    sandbox_id,
                    sandbox_dir,
                    mount_root,
                    images_dir,
                    *runsc_args,
                ],
                stdin=lifeline_r,
                stdout=ready_w,
                close_fds=True,
                # Out of the worker's process group and terminal, so only the
                # lifeline decides when it acts.
                start_new_session=True,
                cwd="/",
            )
        except BaseException:
            os.close(lifeline_w)
            os.close(ready_r)
            raise
        finally:
            os.close(lifeline_r)
            os.close(ready_w)
        return cls(keeper, lifeline_w, ready_r)

    def wait_ready(self, timeout: float = _READY_TIMEOUT_S) -> Tuple[int, int]:
        """Wait for the supervisor to report that it runs.

        Args:
            timeout: How long to wait, in seconds.

        Returns:
            The supervisor's ``(pid, start_time)``, also kept on the handle.

        Raises:
            RuntimeError: It was not reported within ``timeout`` seconds.
        """
        if self._ready is None:
            raise RuntimeError("The sandbox's supervisor was already waited for.")
        data = b""
        deadline = time.monotonic() + timeout
        try:
            # poll() rather than select(): a worker's fds can exceed FD_SETSIZE.
            poller = select.poll()
            poller.register(self._ready, select.POLLIN)
            while not data.endswith(b"\n"):
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise RuntimeError(
                        f"The sandbox's supervisor did not start within {timeout:g}s."
                    )
                if not poller.poll(remaining * 1000):
                    continue
                chunk = os.read(self._ready, 64)
                if not chunk:
                    raise RuntimeError(
                        "The sandbox's supervisor exited before it started "
                        f"(exit code {self._keeper.poll()})."
                    )
                data += chunk
        finally:
            self._close_ready()
        try:
            pid, start_time = (int(part) for part in data.split())
        except ValueError:
            raise RuntimeError(
                f"The sandbox's supervisor reported {data!r} when it started."
            ) from None
        self.pid, self.start_time = pid, start_time
        return pid, start_time

    def disarm(self) -> None:
        """Dismiss the supervisor: the sandbox is gone. Idempotent."""
        if not self._close_lifeline(_DISARM):
            return
        try:
            self._keeper.wait(_SIGNAL_WAIT_S)
        except subprocess.TimeoutExpired:
            self._keeper.kill()
            self._keeper.wait()

    def hand_over(self) -> None:
        """Leave the teardown to the supervisor, as if this process had died.

        For a teardown that did not finish: the supervisor's is complete and
        retries whatever failed. Idempotent.
        """
        if not self._close_lifeline(None):
            return
        threading.Thread(
            target=self._keeper.wait, name="sandbox-supervisor-reap", daemon=True
        ).start()

    def alive(self) -> bool:
        """Whether the supervisor is still running."""
        if self.pid is None:
            return self._keeper.poll() is None
        return _process_alive(self.pid, self.start_time)

    def _close_lifeline(self, message: Optional[bytes]) -> bool:
        with self._lock:
            if self._lifeline is None:
                return False
            lifeline, self._lifeline = self._lifeline, None
        if message:
            try:
                os.write(lifeline, message)
            except OSError:
                pass  # The supervisor is gone already.
        os.close(lifeline)
        self._close_ready()
        return True

    def _close_ready(self) -> None:
        with self._lock:
            ready, self._ready = self._ready, None
        if ready is not None:
            os.close(ready)


def _await_lifeline() -> bool:
    """Block until the worker disarms (False) or is gone (True)."""
    while True:
        data = os.read(0, 1)
        if not data:
            return True
        if data == _DISARM:
            return False


def _supervise(
    sandbox_id: str,
    runsc_args: List[str],
    sandbox_dir: str,
    mount_root: str,
    images_dir: str,
) -> int:
    child = os.fork()
    if child == 0:
        # The supervisor. Its copy of the ready pipe goes at once, so the worker
        # sees EOF if the keeper dies before reporting.
        devnull = os.open(os.devnull, os.O_WRONLY)
        os.dup2(devnull, 1)
        os.close(devnull)
        code = 0
        try:
            if _await_lifeline():
                logger.info(
                    "The Ray worker that owned sandbox %s is gone without "
                    "deleting it; tearing it down.",
                    sandbox_id,
                )
                teardown_sandbox(
                    sandbox_id, runsc_args, sandbox_dir, mount_root, images_dir
                )
        except BaseException:  # noqa: BLE001
            logger.exception("Failed to tear down sandbox %s", sandbox_id)
            code = 1
        finally:
            os._exit(code)
    # The keeper: report the supervisor, then wait for it.
    os.close(0)
    try:
        os.write(1, f"{child} {_proc_start_time(child)}\n".encode())
    except OSError:
        pass  # The worker is gone; the supervisor sees that by itself.
    os.close(1)
    _, status = os.waitpid(child, 0)
    return os.waitstatus_to_exitcode(status)


def main(argv: Sequence[str]) -> int:
    if len(argv) < 6 or argv[0] != "supervise":
        print(
            "usage: teardown.py supervise SANDBOX_ID SANDBOX_DIR MOUNT_ROOT "
            "IMAGES_DIR RUNSC [RUNSC_FLAGS...]",
            file=sys.stderr,
        )
        return 2
    sandbox_id, sandbox_dir, mount_root, images_dir = argv[1:5]
    if not _SANDBOX_ID.match(sandbox_id):
        print(f"teardown.py: not a sandbox id: {sandbox_id!r}", file=sys.stderr)
        return 2
    # Into the worker's log, which stderr is.
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s sandbox supervisor %(process)d -- %(message)s",
    )
    return _supervise(sandbox_id, list(argv[5:]), sandbox_dir, mount_root, images_dir)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
