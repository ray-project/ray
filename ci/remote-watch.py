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

import argparse
import errno
import logging
import os
import re
import signal
import subprocess
import sys
import time

logger = logging.getLogger(__name__)

GITHUB = "GitHub"
TRAVIS = "Travis"


def git(*args):
    cmdline = ["git"] + list(args)
    return subprocess.check_output(cmdline).decode("utf-8").rstrip()


def get_current_ci():
    if "GITHUB_WORKFLOW" in os.environ:
        return GITHUB
    elif "TRAVIS" in os.environ:
        return TRAVIS
    return None


def get_ci_event_name():
    ci = get_current_ci()
    if ci == GITHUB:
        return os.environ["GITHUB_EVENT_NAME"]
    elif ci == TRAVIS:
        return os.environ["TRAVIS_EVENT_TYPE"]
    return None


def get_repo_slug():
    ci = get_current_ci()
    if ci == GITHUB:
        return os.environ["GITHUB_REPOSITORY"]
    elif ci == TRAVIS:
        return os.environ["TRAVIS_REPO_SLUG"]
    return None


def git_remote_branch_info():
    # Obtains the remote branch name, remote name, and commit hash that
    # correspond to the local HEAD.
    # Example: ("refs/heads/mybranch", "origin", "1A2B3C4...")
    ref = None
    try:
        # Try to get the local branch ref. (e.g. refs/heads/mybranch)
        head = git("symbolic-ref", "-q", "HEAD")
        # Try to get the remotely tracked ref, if any. (e.g. origin/mybranch)
        ref = git("for-each-ref", "--format=%(upstream:short)", head)
    except subprocess.CalledProcessError:
        pass
    remote = None
    expected_sha = None
    if ref:
        (remote, ref) = ref.split("/", 1)
        ref = "refs/heads/" + ref
    else:
        remote = git("remote", "show", "-n").splitlines()[0]
        ref = os.getenv("TRAVIS_PULL_REQUEST_BRANCH")
        if ref:
            TRAVIS_PULL_REQUEST = os.environ["TRAVIS_PULL_REQUEST"]
            if os.getenv("TRAVIS_EVENT_TYPE") == "pull_request":
                ref = "refs/pull/{}/merge".format(TRAVIS_PULL_REQUEST)
            else:
                ref = "refs/heads/{}".format(TRAVIS_PULL_REQUEST)
            expected_sha = os.getenv("TRAVIS_COMMIT")
        else:
            ref = os.getenv("GITHUB_REF")
    if not remote:
        raise ValueError("Invalid remote: {!r}".format(remote))
    if not ref:
        raise ValueError("Invalid ref: {!r}".format(ref))
    expected_sha = git("rev-parse", "--verify", "HEAD")
    return (ref, remote, expected_sha)


def terminate_my_process_group():
    result = 0
    timeout = 15
    try:
        logger.warning("Attempting kill...")
        if sys.platform == "win32":
            os.kill(0, signal.CTRL_BREAK_EVENT)  # this might get ignored
            time.sleep(timeout)
            os.kill(os.getppid(), signal.SIGTERM)
        else:
            os.kill(os.getppid(), signal.SIGTERM)  # apparently this is needed
            time.sleep(timeout)
            os.kill(0, signal.SIGKILL)
    except OSError as ex:
        if ex.errno not in (errno.EBADF, errno.ESRCH):
            raise
        logger.error("Kill error %s: %s", ex.errno, ex.strerror)
        result = ex.errno
    return result


def yield_poll_schedule():
    schedule = [0, 5, 5, 10, 20, 40, 40] + [60] * 5 + [120] * 10 + [300, 600]
    for item in schedule:
        yield item
    while True:
        yield schedule[-1]


def monitor():
    ci = get_current_ci() or ""
    (ref, remote, expected_sha) = git_remote_branch_info()
    expected_line = "{}\t{}".format(expected_sha, ref)
    keep_alive = False
    commit_msg = git("show", "-s", "--format=%B", "HEAD^-")
    for line in commit_msg.splitlines():
        parts = line.strip("# ").split(':', 1)
        (key, val) = parts if len(parts) > 1 else (parts[0], "")
        if key == "CI_KEEP_ALIVE":
            ci_names = val.replace(",", " ").lower().split() if val else []
            if len(ci_names) == 0 or ci.lower() in ci_names:
                keep_alive = True
    if keep_alive:
        logger.info("Not monitoring %s on %s due to keep-alive on: %s",
                    ref, remote, expected_line)
        return
    # Show which branch on which remote we are monitoring.
    logger.info("Monitoring %s (%s) for changes in %s: %s",
                remote, git("ls-remote", "--get-url", remote),
                ref, expected_line)
    terminate = False
    for to_wait in yield_poll_schedule():
        time.sleep(to_wait)
        status = 0
        line = None
        try:
            # Query the commit on the remote ref (without fetching the commit).
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
        else:
            terminate = True
            logger.info("\n".join(
                [
                    "Terminating job as %s has changed on %s",
                    "    from:\t%s",
                    "    to:  \t%s",
                ]), ref, remote, expected_line, line)
            break
    if terminate:
        result = terminate_my_process_group()
    return result


def main(program, *args):
    p = argparse.ArgumentParser()
    p.add_argument("--skip_repo", action="append", help="Repo to exclude.")
    parsed_args = p.parse_args(args)
    skipped_repos = parsed_args.skip_repo or []
    repo_slug = get_repo_slug()
    event_name = get_ci_event_name()
    if repo_slug not in skipped_repos or event_name == "pull_request":
        result = monitor()
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
