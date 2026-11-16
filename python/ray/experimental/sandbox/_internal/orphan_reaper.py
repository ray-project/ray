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
``_REAPED_COMMS`` and that run in a session of their own, as runsc's
helpers do. It lists the worker's children from
``/proc/self/task/*/children`` where the kernel has those files, and reads
all of ``/proc`` otherwise. When a caller deletes a sandbox, ``reap_now``
reaps them at once, so a worker that exits right afterwards leaves none
behind; after a checkpoint stops a sandbox, ``reap_soon`` only wakes the
thread, so checkpoints don't wait for the scan. Children this process starts with ``subprocess``, such as runsc
itself, share its session, so their exit statuses stay for ``Popen.wait``
(which reports 0 for a child it can't find). Other orphans in the worker's subtree still become its children and
are not reaped here; they are reparented again when the worker exits.

Set ``RAY_SANDBOX_REAP_ORPHANS=0`` to turn this off, for example when the
pod's init reaps orphans (``shareProcessNamespace: true`` or ``tini``).
"""

import ctypes
import logging
import math
import os
import sys
import threading
from typing import List, Optional

logger = logging.getLogger(__name__)

REAP_ORPHANS_ENV = "RAY_SANDBOX_REAP_ORPHANS"
REAP_INTERVAL_ENV = "RAY_SANDBOX_ORPHAN_REAP_INTERVAL_S"
# runsc's teardown (kill, delete, checkpoint) waits for the sandbox's helpers
# to be gone, and a zombie still counts, so reap them soon after they exit.
_DEFAULT_INTERVAL_SECONDS = 0.25

# From <linux/prctl.h>.
_PR_SET_CHILD_SUBREAPER = 36

# Command names (/proc/<pid>/comm, cut to 15 bytes) of the helpers to reap.
# runsc starts its gofer by executing /proc/self/exe, so the gofer's comm is
# "exe"; it outlives runsc the same way runsc-fd-parking does. The backend
# runs runsc itself as "runsc", which is not in the set.
_REAPED_COMMS = frozenset({"runsc-fd-parking"[:15], "exe"})

_lock = threading.Lock()
_thread: Optional[threading.Thread] = None
# Set to make the thread's next pass start at once.
_wake = threading.Event()


def _reset_after_fork() -> None:
    # A forked child is not a subreaper and doesn't run the thread.
    global _lock, _thread, _wake
    _lock = threading.Lock()
    _thread = None
    _wake = threading.Event()


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
    if not math.isfinite(value) or value <= 0:
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
    try:
        libc = ctypes.CDLL(None, use_errno=True)
        failed = libc.prctl(_PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) != 0
        reason = os.strerror(ctypes.get_errno()) if failed else None
    except (AttributeError, OSError) as err:
        # No libc to load, or one without prctl.
        failed, reason = True, str(err)
    if failed:
        logger.warning(
            "prctl(PR_SET_CHILD_SUBREAPER) failed (%s), so orphaned runsc "
            "helpers stay zombies unless the pod's init reaps them.",
            reason,
        )
        return False
    return True


def _parse_stat(stat: str):
    """Return (comm, state, ppid, session) from the text of /proc/<pid>/stat."""
    # comm is in parentheses and may itself contain spaces or parentheses.
    open_paren = stat.index("(")
    close_paren = stat.rindex(")")
    comm = stat[open_paren + 1 : close_paren]
    state, ppid, _pgrp, session = stat[close_paren + 2 :].split(maxsplit=4)[:4]
    return comm, state, int(ppid), int(session)


def _child_pids() -> Optional[List[str]]:
    """List this process's children from /proc/self/task/<tid>/children.

    Reading one small file per thread costs far less than reading the stat of
    every process in /proc. The kernel calls these files reliable only while
    the children are stopped, since a child that starts or exits during the
    read can be missed; a zombie no longer changes, so it's listed.

    Returns:
        The children's pids, or None when the kernel has no children files
        (CONFIG_PROC_CHILDREN is off).
    """
    try:
        tids = os.listdir("/proc/self/task")
    except OSError:
        return None
    pids: List[str] = []
    found = False
    for tid in tids:
        try:
            with open(f"/proc/self/task/{tid}/children", encoding="ascii") as f:
                pids.extend(f.read().split())
        except FileNotFoundError:
            # A thread that just exited, or no children files at all.
            continue
        except OSError:
            continue
        found = True
    return pids if found else None


def reap_orphans() -> int:
    """Reap this process's zombie children that are orphaned runsc helpers.

    Returns:
        The number of zombies reaped.
    """
    me = os.getpid()
    my_session = os.getsid(0)
    reaped = 0
    entries = _child_pids()
    if entries is None:
        try:
            entries = os.listdir("/proc")
        except OSError:
            return 0
    for entry in entries:
        if not entry.isdigit():
            continue
        try:
            with open(f"/proc/{entry}/stat", encoding="utf-8", errors="replace") as f:
                comm, state, ppid, session = _parse_stat(f.read())
        except (OSError, ValueError):
            continue
        if state != "Z" or ppid != me or comm not in _REAPED_COMMS:
            continue
        # runsc starts its helpers with setsid. "exe" is a common name for
        # programs that re-execute themselves, so never take a child in this
        # process's own session: subprocess children, e.g. runsc, stay in it.
        if session == my_session:
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


def reap_soon() -> None:
    """Start the thread's next pass now instead of at the next interval.

    Doesn't wait for it, so it costs the caller nothing; passes that several
    callers ask for at once run once.
    """
    if _thread is not None:
        _wake.set()


def reap_now() -> int:
    """Reap orphaned runsc helpers now, if the reaper runs in this process.

    The backend calls this when a caller deletes a sandbox. The worker can
    exit right afterwards, before the thread's next pass, and an exiting
    subreaper's children go to PID 1, which doesn't reap them.

    Returns:
        The number of zombies reaped; 0 when the reaper isn't running.
    """
    if _thread is None:
        return 0
    try:
        return reap_orphans()
    except Exception:
        logger.exception("Reaping orphaned runsc helpers failed.")
        return 0


def _run(interval: float) -> None:
    while True:
        # Clear before the pass, so a reap_soon() during it starts another.
        _wake.clear()
        try:
            reap_orphans()
        except Exception:
            logger.exception("Reaping orphaned runsc helpers failed.")
        _wake.wait(interval)


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
