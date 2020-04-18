#!/usr/bin/env python

"""
This command must be run in a git repository.

It watches the remote branch for changes, killing the given PID when it detects
that the remote branch no longer points to the local commit.

If the commit message contains a line saying "CI_KEEP_ALIVE", then killing
will not occur until the branch is deleted from the remote.

If no PID is given, then the entire process group of this process is killed.
"""

# Prefer to keep this file Python 2-compatible so that it can easily run early
# in the CI process on any system.

import errno
import logging
import os
import re
import signal
import subprocess
import sys
import time

from timeit import default_timer  # monotonic timer

if sys.platform == "win32":
    import ctypes
    from ctypes.wintypes import HANDLE, DWORD, BOOL
    CloseHandle = ctypes.WINFUNCTYPE(BOOL, HANDLE)(
        ("CloseHandle", ctypes.windll.kernel32))
    GetProcessId = ctypes.WINFUNCTYPE(DWORD, HANDLE)(
        ("GetProcessId", ctypes.windll.kernel32))
    OpenProcess = ctypes.WINFUNCTYPE(HANDLE, DWORD, BOOL, DWORD)(
        ("OpenProcess", ctypes.windll.kernel32))
    WaitForSingleObject = ctypes.WINFUNCTYPE(DWORD, HANDLE, DWORD)(
        ("WaitForSingleObject", ctypes.windll.kernel32))

logger = logging.getLogger(__name__)


def open_process(pid):
    if sys.platform == "win32":
        # access masks defined in <winnt.h>
        SYNCHRONIZE = 0x00100000
        PROCESS_TERMINATE = 0x0001
        handle = OpenProcess(SYNCHRONIZE | PROCESS_TERMINATE, False, pid)
        if not handle:
            raise OSError(errno.EBADF, "invalid PID: {}".format(pid))
    else:
        handle = pid
    return handle


def get_process_id(handle):
    if sys.platform == "win32":
        pid = GetProcessId(handle)
    else:
        pid = handle
    return pid


def wait_process(handle, timeout_ms=None):
    no_such_process = errno.EINVAL if sys.platform == "win32" else errno.ESRCH
    dead = False
    try:
        if sys.platform == "win32":
            WAIT_OBJECT_0 = 0
            if timeout_ms is None:
                timeout_ms = 0xFFFFFFFF
            dead = WaitForSingleObject(handle, timeout_ms) == WAIT_OBJECT_0
        else:
            os.kill(handle, 0)  # Quick test
            time.sleep(0)  # If alive, do minimal sleep; maybe it'll die now
            tstart = default_timer()
            tsleep = 1.0 / (1 << 20)  # Start with small granularity
            while True:  # Exponentially increase interval
                os.kill(handle, 0)
                tremaining = tstart + (timeout_ms / 1000.0) - default_timer()
                if tremaining <= 0:
                    break
                tsleep = min(tremaining, tsleep * 2)
                time.sleep(tremaining)
            # If we reach here, the process is still alive (kill() never threw)
    except OSError as ex:
        if ex.errno != no_such_process:
            raise
        dead = True
    return dead


def close_process(handle):
    if sys.platform == "win32":
        CloseHandle(handle)


def git(*args, **kwargs):
    capture = kwargs.pop("capture", True)
    assert len(kwargs) == 0, "Unexpected kwargs: {}".format(kwargs)
    cmdline = ["git"] + list(args)
    if capture:
        result = subprocess.check_output(cmdline).decode("utf-8").rstrip()
    else:
        result = subprocess.check_call(cmdline)
    return result


def git_remotes():
    return git("remote", "show", "-n").splitlines()


def get_current_ci():
    result = None
    result = result or os.getenv("TRAVIS")
    result = result or os.getenv("GITHUB_WORKFLOW")
    return result


def git_branch_info():
    ref = None
    try:
        head = git("symbolic-ref", "-q", "HEAD")
        ref = git("for-each-ref", "--format=%(upstream:short)", head)
    except subprocess.CalledProcessError:
        pass
    remote = None
    expected_sha = None
    if ref:
        (remote, ref) = ref.split("/", 1)
        ref = "refs/heads/" + ref
    else:
        remote = git_remotes()[0]
        ref = os.getenv("TRAVIS_PULL_REQUEST_BRANCH")
        if ref:
            TRAVIS_PULL_REQUEST = os.environ["TRAVIS_PULL_REQUEST"]
            if os.getenv("TRAVIS_EVENT_TYPE") == "pull_request":
                ref = "refs/pull/{}/merge".format(TRAVIS_PULL_REQUEST)
            else:
                ref = "refs/heads/{}".format(TRAVIS_PULL_REQUEST)
            expected_sha = os.getenv("TRAVIS_COMMIT")
        else:
            ref = os.getenv("CI_REF")
    if not remote:
        raise ValueError("Invalid remote: {!r}".format(remote))
    if not ref:
        raise ValueError("Invalid ref: {!r}".format(ref))
    expected_sha = git("rev-parse", "--verify", "HEAD")
    return (ref, remote, expected_sha)


def terminate_process(handle, sleeps):
    result = 0
    pid = get_process_id(handle) if handle else 0
    is_posix = True
    nonsignals = []
    if sys.platform == "win32":
        is_posix = False
        nonsignals.append(signal.CTRL_C_EVENT)
        nonsignals.append(signal.CTRL_BREAK_EVENT)
    sig = None
    try:
        attempts = [
            signal.SIGINT if is_posix else signal.CTRL_C_EVENT,
            signal.SIGTERM if is_posix else signal.CTRL_BREAK_EVENT,
            signal.SIGKILL if is_posix else signal.SIGTERM,
        ]
        for i, sig in enumerate(attempts):
            tsleep = sleeps[i] if i < len(sleeps) else None
            if pid == 0 or sig not in nonsignals:
                logger.info("Attempting os.kill(%s, %s)...", pid, sig)
                old_handler = None
                if pid == 0:  # Ignore our own signal
                    try:
                        old_handler = signal.signal(sig, signal.SIG_IGN)
                    except (OSError, RuntimeError, ValueError):
                        pass  # Some signals can't be ignored; just continue
                try:
                    os.kill(pid, sig)
                except OSError as ex:
                    if sys.platform == "win32" and ex.winerror == 87:
                        pass  # This can sometimes happen; just ignore it
                    else:
                        raise
                finally:
                    if pid == 0 and old_handler is not None:  # Restore handler
                        signal.signal(sig, old_handler)
                if wait_process(handle, tsleep):
                    break
    except OSError as ex:
        if ex.errno not in (errno.EBADF, errno.ESRCH):
            raise
        logger.error("os.kill(%r, %r) error %s: %s", pid, sig, ex.errno,
                     ex.strerror)
        result = ex.errno
    return result


def monitor(process_handle=None, server_poll_interval=None, sleeps=None):
    if sleeps is None:
        sleeps = [5, 15]
    if not server_poll_interval:
        server_poll_interval = 30
    ci = get_current_ci() or ""
    (ref, remote, expected_sha) = git_branch_info()
    expected_line = "{}\t{}".format(expected_sha, ref)
    commit_msg = git("show", "-s", "--format=%B", "HEAD^-")
    keep_alive = False
    for match in re.findall("(?m)^([# ]*CI_KEEP_ALIVE(:(.*))?)$", commit_msg):
        if not match[2].strip():
            keep_alive = True
        else:
            for ci_name in match[2].split(","):
                if ci_name.strip().lower() == ci.lower():
                    keep_alive = True
    logger.info("Monitoring %s (%s) for changes in %s every %s seconds: %s",
                remote, git("ls-remote", "--get-url", remote),
                ref, server_poll_interval, expected_line)
    prev_ignored_line = None
    tstart = default_timer()
    tprev = None
    terminate = False
    while True:
        to_wait = 0
        if tprev is not None:
            to_wait = max(tprev + server_poll_interval - default_timer(), 0)

        # Try to increase waiting time gradually, for better responsiveness
        to_wait = min(to_wait, max(default_timer() - tstart, 5.0))

        if not process_handle:
            time.sleep(to_wait)
        elif wait_process(process_handle, to_wait):
            # The process has exited, so there's no need to keep polling
            break
        status = 0
        line = None
        try:
            line = git("ls-remote", "--exit-code", remote, ref)
        except subprocess.CalledProcessError as ex:
            status = ex.returncode
        if status == 2:
            terminate = True
            logger.info("Terminating job as %s has been deleted on %s: %s",
                        ref, remote, expected_line)
            break
        elif status != 0:
            logger.error("Error %d: unable to check %s on %s: %s",
                         status, ref, remote, expected_line)
        elif line == expected_line:
            pass  # everything good
        elif keep_alive:
            if prev_ignored_line != line:
                logger.info("Not terminating job even though %s has changed "
                            "on %s due to keep-alive on: %s",
                            ref, remote, expected_line)
                prev_ignored_line = line
        else:
            terminate = True
            logger.info("\n".join(
                [
                    "Terminating job as %s has changed on %s",
                    "    from:\t%s",
                    "    to:  \t%s",
                ]), ref, remote, expected_line, line)
            break
        tprev = default_timer()
    if terminate:
        result = terminate_process(process_handle, sleeps)
    return result


def main(program, pid="", server_poll_interval="", sleeps=""):
    if pid:
        pid = int(pid)
    else:
        logger.info("No PID provided; monitoring entire process group...")
        pid = 0
    if server_poll_interval:
        server_poll_interval = float(server_poll_interval)
    else:
        server_poll_interval = None
    if sleeps.strip():
        sleeps = [float(s) for s in sleeps.strip().split(",") if s.strip()]
    else:
        sleeps = None
    result = 0
    process_handle = open_process(pid) if pid else 0
    try:
        result = monitor(process_handle, server_poll_interval, sleeps)
    finally:
        if process_handle:
            close_process(process_handle)
    return result


if __name__ == "__main__":
    logging.basicConfig(format="%(levelname)s: %(message)s",
                        stream=sys.stderr, level=logging.DEBUG)
    try:
        raise SystemExit(main(*sys.argv) or 0)
    except KeyboardInterrupt:
        pass
