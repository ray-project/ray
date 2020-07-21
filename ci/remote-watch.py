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

logger = logging.getLogger(__name__)


def git(*args, **kwargs):
    capture = kwargs.pop("capture", True)
    assert len(kwargs) == 0, "Unexpected kwargs: {}".format(kwargs)
    cmdline = ["git"] + list(args)
    if capture:
        result = subprocess.check_output(cmdline).decode("utf-8").rstrip()
    else:
        result = subprocess.check_call(cmdline)
    return result


def get_current_ci():
    result = None
    if "GITHUB_WORKFLOW" in os.environ:
        result = "GitHub"
    elif "TRAVIS" in os.environ:
        result = "Travis"
    return result


def get_event_name():
    result = None
    ci = get_current_ci()
    if ci.startswith("GitHub"):
        result = os.environ["GITHUB_EVENT_NAME"]
    elif ci.startswith("Travis"):
        result = os.environ["TRAVIS_EVENT_TYPE"]
    return result


def get_repo_slug():
    ci = get_current_ci()
    result = None
    if ci.startswith("GitHub"):
        result = os.environ["GITHUB_REPOSITORY"]
    elif ci.startswith("Travis"):
        result = os.environ["TRAVIS_REPO_SLUG"]
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


def terminate_parent_or_process_group(sleeps):
    result = 0
    pid = 0 if sys.platform == "win32" else os.getppid()
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
                time.sleep(tsleep)
    except OSError as ex:
        if ex.errno not in (errno.EBADF, errno.ESRCH):
            raise
        logger.error("os.kill(%r, %r) error %s: %s", pid, sig, ex.errno,
                     ex.strerror)
        result = ex.errno
    return result


def monitor(server_poll_interval=None, sleeps=None):
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

        time.sleep(to_wait)
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
        result = terminate_parent_or_process_group(sleeps)
    return result


def main(program, skipped_repos="", server_poll_interval="", sleeps=""):
    if server_poll_interval:
        server_poll_interval = float(server_poll_interval)
    else:
        server_poll_interval = None
    if sleeps.strip():
        sleeps = [float(s) for s in sleeps.strip().split(",") if s.strip()]
    else:
        sleeps = None
    repo_slug = get_repo_slug()
    skipped_repo_list = []
    if skipped_repos:
        skipped_repo_list.extend(skipped_repos.split(","))
    event_name = get_event_name()
    if repo_slug not in skipped_repo_list or event_name == "pull_request":
        result = monitor(server_poll_interval, sleeps)
    else:
        logger.info("Skipping monitoring %s %s build", repo_slug, event_name)
    return result


if __name__ == "__main__":
    logging.basicConfig(format="%(levelname)s: %(message)s",
                        stream=sys.stderr, level=logging.DEBUG)
    try:
        raise SystemExit(main(*sys.argv) or 0)
    except KeyboardInterrupt:
        pass
