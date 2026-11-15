"""Reap the orphaned runsc helpers that a sandbox worker adopts.

``runsc`` starts helpers that can exit after the runsc process that started
them, and so are orphaned: the ``runsc-fd-parking`` helper, which holds file
descriptors until the sandbox exits (after a ``runsc restore``, about half of
the restored sandboxes left one with runsc release-20261109.0), and, under
concurrent load, the gofer, which runsc starts through ``/proc/self/exe`` so
its command name is ``exe``. Linux hands an orphan to the nearest child
subreaper, or else to PID 1, and a Ray worker container's PID 1
(``ray start --block``) never waits for it, so the zombie stays for the life
of the pod. Nodes that restored sandboxes at thousands per second collected
tens of thousands of them, and only a raised ``kernel.pid_max`` kept them
from running out of process IDs.

``start`` makes the worker process that runs the gVisor backend a child
subreaper, so these helpers become its children, and starts a thread that
reaps them. The thread only reaps zombies whose command name is in
``_REAPED_COMMS`` and that no live ``subprocess.Popen`` in this process
waits for: reaping such a child would take the exit status that
``subprocess`` waits for (``Popen.wait`` reports 0 for a child it can't
find). Other orphans in the worker's subtree still become its children and
are not reaped here; they are reparented again when the worker exits.

Set ``RAY_SANDBOX_REAP_ORPHANS=0`` to turn this off, for example when the
pod's init reaps orphans (``shareProcessNamespace: true`` or ``tini``).
"""

import ctypes
import gc
import logging
import os
import subprocess
import sys
import threading
import time
from typing import Optional

logger = logging.getLogger(__name__)

REAP_ORPHANS_ENV = "RAY_SANDBOX_REAP_ORPHANS"
REAP_INTERVAL_ENV = "RAY_SANDBOX_ORPHAN_REAP_INTERVAL_S"
_DEFAULT_INTERVAL_SECONDS = 5.0

# From <linux/prctl.h>.
_PR_SET_CHILD_SUBREAPER = 36

# Command names (/proc/<pid>/comm, cut to 15 bytes) of the helpers to reap.
# runsc starts its gofer by executing /proc/self/exe, so the gofer's comm is
# "exe"; it outlives runsc the same way runsc-fd-parking does. The backend
# runs runsc itself as "runsc", which is not in the set.
_REAPED_COMMS = frozenset({"runsc-fd-parking"[:15], "exe"})

_lock = threading.Lock()
_thread: Optional[threading.Thread] = None


def _reset_after_fork() -> None:
    # A forked child is not a subreaper and doesn't run the thread.
    global _lock, _thread
    _lock = threading.Lock()
    _thread = None


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_reset_after_fork)


def _enabled() -> bool:
    return os.environ.get(REAP_ORPHANS_ENV, "1").strip().lower() not in (
        "0",
        "false",
        "no",
        "off",
    )


def _interval_seconds() -> float:
    raw = os.environ.get(REAP_INTERVAL_ENV)
    if not raw:
        return _DEFAULT_INTERVAL_SECONDS
    try:
        value = float(raw)
    except ValueError:
        value = 0.0
    if value <= 0:
        logger.warning(
            "Ignoring %s=%r, which isn't a positive number of seconds.",
            REAP_INTERVAL_ENV,
            raw,
        )
        return _DEFAULT_INTERVAL_SECONDS
    return value


def _enable_subreaper() -> bool:
    """Make this process a child subreaper.

    Returns:
        Whether the call succeeded.
    """
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(_PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) != 0:
        err = ctypes.get_errno()
        logger.warning(
            "prctl(PR_SET_CHILD_SUBREAPER) failed (%s), so orphaned runsc "
            "helpers stay zombies unless the pod's init reaps them.",
            os.strerror(err),
        )
        return False
    return True


def _parse_stat(stat: str):
    """Return (comm, state, ppid) from the text of /proc/<pid>/stat."""
    # comm is in parentheses and may itself contain spaces or parentheses.
    open_paren = stat.index("(")
    close_paren = stat.rindex(")")
    comm = stat[open_paren + 1 : close_paren]
    state, ppid = stat[close_paren + 2 :].split()[:2]
    return comm, state, int(ppid)


def _popen_pids() -> set:
    """The pids of this process's subprocess.Popen children not yet waited for."""
    return {
        o.pid
        for o in gc.get_objects()
        if isinstance(o, subprocess.Popen) and o.returncode is None
    }


def reap_orphans() -> int:
    """Reap this process's zombie children that are orphaned runsc helpers.

    Returns:
        The number of zombies reaped.
    """
    me = os.getpid()
    reaped = 0
    waited_for = None
    try:
        entries = os.listdir("/proc")
    except OSError:
        return 0
    for entry in entries:
        if not entry.isdigit():
            continue
        try:
            with open(f"/proc/{entry}/stat", encoding="utf-8", errors="replace") as f:
                comm, state, ppid = _parse_stat(f.read())
        except (OSError, ValueError):
            continue
        if state != "Z" or ppid != me or comm not in _REAPED_COMMS:
            continue
        # "exe" is a common name for programs that re-execute themselves, so
        # never take a child that a live Popen in this process waits for.
        if waited_for is None:
            waited_for = _popen_pids()
        if int(entry) in waited_for:
            continue
        # A zombie's pid can't be reused until it's reaped, so this can only
        # reap the process that was just read.
        try:
            pid, _ = os.waitpid(int(entry), os.WNOHANG)
        except ChildProcessError:
            continue
        if pid:
            reaped += 1
    return reaped


def _run(interval: float) -> None:
    while True:
        try:
            reap_orphans()
        except Exception:
            logger.exception("Reaping orphaned runsc helpers failed.")
        time.sleep(interval)


def start() -> bool:
    """Adopt and reap orphaned runsc helpers in this process.

    Safe to call more than once: the first call that succeeds starts the
    reaper and later calls do nothing.

    Returns:
        Whether the reaper is running.
    """
    global _thread
    if not sys.platform.startswith("linux") or not _enabled():
        return False
    with _lock:
        if _thread is not None:
            return True
        if not _enable_subreaper():
            return False
        thread = threading.Thread(
            target=_run,
            args=(_interval_seconds(),),
            name="ray-sandbox-orphan-reaper",
            daemon=True,
        )
        thread.start()
        _thread = thread
        return True
