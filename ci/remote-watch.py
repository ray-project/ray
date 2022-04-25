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


def get_remote_url(remote):
    return git("ls-remote", "--get-url", remote)


def replace_suffix(base, old_suffix, new_suffix=""):
    if base.endswith(old_suffix):
        base = base[: len(base) - len(old_suffix)] + new_suffix
    return base


def git_branch_info_to_track():
    """Obtains the remote branch name, remote name, and commit hash that
    should be tracked for changes.

    Returns:
        ("refs/heads/mybranch", "origin", "1A2B3C4...")
    """
    expected_sha = None
    ref = None
    remote = git("remote", "show", "-n").splitlines()[0]

    ci = get_current_ci()
    if ci == GITHUB:
        expected_sha = os.getenv("GITHUB_HEAD_SHA") or os.environ["GITHUB_SHA"]
        ref = replace_suffix(os.environ["GITHUB_REF"], "/merge", "/head")
    elif ci == TRAVIS:
        pr = os.getenv("TRAVIS_PULL_REQUEST", "false")
        if pr != "false":
            expected_sha = os.environ["TRAVIS_PULL_REQUEST_SHA"]
            ref = "refs/pull/{}/head".format(pr)
        else:
            expected_sha = os.environ["TRAVIS_COMMIT"]
            ref = "refs/heads/{}".format(os.environ["TRAVIS_BRANCH"])

    result = (ref, remote, expected_sha)

    if not all(result):
        msg = "Invalid remote {!r}, ref {!r}, or hash {!r} for CI {!r}"
        raise ValueError(msg.format(remote, ref, expected_sha, ci))

    return result


def get_commit_metadata(hash):
    """Get the commit info (content hash, parents, message, etc.) as a list of
    key-value pairs.
    """
    info = git("cat-file", "-p", hash)
    parts = info.split("\n\n", 1)  # Split off the commit message
    records = parts[0]
    message = parts[1] if len(parts) > 1 else None
    result = []
    records = records.replace("\n ", "\0 ")  # Join multiple lines into one
    for record in records.splitlines(True):
        (key, value) = record.split(" ", 1)
        value = value.replace("\0 ", "\n ")  # Re-split lines
        result.append((key, value))
    result.append(("message", message))
    return result


def terminate_my_process_group():
    result = 0
    timeout = 15
    try:
        logger.warning("Attempting kill...")
        if sys.platform == "win32":
            os.kill(0, signal.CTRL_BREAK_EVENT)  # This might get ignored.
            time.sleep(timeout)
            os.kill(os.getppid(), signal.SIGTERM)
        else:
            # This SIGTERM seems to be needed to prevent jobs from lingering.
            os.kill(os.getppid(), signal.SIGTERM)
            time.sleep(timeout)
            os.kill(0, signal.SIGKILL)
    except OSError as ex:
        if ex.errno not in (errno.EBADF, errno.ESRCH):
            raise
        logger.error("Kill error %s: %s", ex.errno, ex.strerror)
        result = ex.errno
    return result


def yield_poll_schedule():
    schedule = [0, 5, 5, 10, 20, 40, 40] + [60] * 5 + [120] * 10 + [300]
    for item in schedule:
        yield item
    while True:
        yield schedule[-1]


def detect_spurious_commit(actual, expected, remote):
    """GitHub sometimes spuriously generates commits multiple times with
    different dates but identical contents. See here:
    https://github.com/travis-ci/travis-ci/issues/7459#issuecomment-601346831
    We need to detect whether this might be the case, and we do so by
    comparing the commits' contents ("tree" objects) and their parents.

    Args:
        actual: The commit line on the remote from git ls-remote, e.g.:
            da39a3ee5e6b4b0d3255bfef95601890afd80709    refs/heads/master
        expected: The commit line initially expected.

    Returns:
        The new (actual) commit line, if it is suspected to be spurious.
        Otherwise, the previously expected commit line.
    """
    actual_hash = actual.split(None, 1)[0]
    expected_hash = expected.split(None, 1)[0]
    relevant = ["tree", "parent"]  # relevant parts of a commit for comparison
    if actual != expected:
        git("fetch", "-q", remote, actual_hash)
        actual_info = get_commit_metadata(actual_hash)
        expected_info = get_commit_metadata(expected_hash)
        a = [pair for pair in actual_info if pair[0] in relevant]
        b = [pair for pair in expected_info if pair[0] in relevant]
        if a == b:
            expected = actual
    return expected


def should_keep_alive(commit_msg):
    result = False
    ci = get_current_ci() or ""
    for line in commit_msg.splitlines():
        parts = line.strip("# ").split(":", 1)
        (key, val) = parts if len(parts) > 1 else (parts[0], "")
        if key == "CI_KEEP_ALIVE":
            ci_names = val.replace(",", " ").lower().split() if val else []
            if len(ci_names) == 0 or ci.lower() in ci_names:
                result = True
    return result


def monitor():
    (ref, remote, expected_sha) = git_branch_info_to_track()
    expected_line = "{}\t{}".format(expected_sha, ref)

    if should_keep_alive(git("show", "-s", "--format=%B", "HEAD^-")):
        logger.info(
            "Not monitoring %s on %s due to keep-alive on: %s",
            ref,
            remote,
            expected_line,
        )
        return

    logger.info(
        "Monitoring %s (%s) for changes in %s: %s",
        remote,
        get_remote_url(remote),
        ref,
        expected_line,
    )

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
            logger.info(
                "Terminating job as %s has been deleted on %s: %s",
                ref,
                remote,
                expected_line,
            )
            break
        elif status != 0:
            logger.error(
                "Error %d: unable to check %s on %s: %s",
                status,
                ref,
                remote,
                expected_line,
            )
        else:
            prev = expected_line
            expected_line = detect_spurious_commit(line, expected_line, remote)
            if expected_line != line:
                logger.info(
                    "Terminating job as %s has been updated on %s\n"
                    "    from:\t%s\n"
                    "    to:  \t%s",
                    ref,
                    remote,
                    expected_line,
                    line,
                )
                time.sleep(1)  # wait for CI to flush output
                break
            if expected_line != prev:
                logger.info(
                    "%s appeared to spuriously change on %s\n"
                    "    from:\t%s\n"
                    "    to:  \t%s",
                    ref,
                    remote,
                    prev,
                    expected_line,
                )

    return terminate_my_process_group()


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
        result = 0
    return result


if __name__ == "__main__":
    logging.basicConfig(
        format="%(levelname)s: %(message)s", stream=sys.stderr, level=logging.DEBUG
    )
    try:
        raise SystemExit(main(*sys.argv) or 0)
    except KeyboardInterrupt:
        pass
