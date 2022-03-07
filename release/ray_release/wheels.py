import os
import re
import subprocess
import sys
import tempfile
import time
import urllib.request
from typing import Optional, List

from ray_release.config import set_test_env_var
from ray_release.exception import (
    RayWheelsUnspecifiedError,
    RayWheelsNotFoundError,
    RayWheelsTimeoutError,
    ReleaseTestSetupError,
)
from ray_release.logger import logger
from ray_release.util import url_exists

DEFAULT_BRANCH = "master"
DEFAULT_GIT_OWNER = "ray-project"
DEFAULT_GIT_PACKAGE = "ray"

REPO_URL_TPL = "https://github.com/{owner}/{package}.git"
INIT_URL_TPL = (
    "https://raw.githubusercontent.com/{fork}/{commit}/python/ray/__init__.py"
)

DEFAULT_REPO = REPO_URL_TPL.format(owner=DEFAULT_GIT_OWNER, package=DEFAULT_GIT_PACKAGE)


def get_ray_version(repo_url: str, commit: str) -> str:
    assert "https://github.com/" in repo_url
    _, fork = repo_url.split("https://github.com/", maxsplit=2)

    if fork.endswith(".git"):
        fork = fork[:-4]

    init_url = INIT_URL_TPL.format(fork=fork, commit=commit)

    try:
        for line in urllib.request.urlopen(init_url):
            line = line.decode("utf-8")
            if line.startswith("__version__"):
                version = line.split(" = ")[1].strip('"\r\n ')
                return version
    except Exception as e:
        raise ReleaseTestSetupError(
            f"Couldn't load version info from branch URL: {init_url}"
        ) from e

    raise RayWheelsNotFoundError(
        f"Unable to parse Ray version information for repo {repo_url} "
        f"and commit {commit} (please check this URL: {init_url})"
    )


def get_latest_commits(
    repo_url: str, branch: str = "master", ref: Optional[str] = None
) -> List[str]:
    cur = os.getcwd()
    with tempfile.TemporaryDirectory() as tmpdir:
        os.chdir(tmpdir)

        clone_cmd = [
            "git",
            "clone",
            "--filter=tree:0",
            "--no-checkout",
            # "--single-branch",
            # "--depth=10",
            f"--branch={branch}",
            repo_url,
            tmpdir,
        ]
        log_cmd = [
            "git",
            "log",
            "-n",
            "10",
            "--pretty=format:%H",
        ]

        subprocess.check_output(clone_cmd)
        if ref:
            subprocess.check_output(["git", "checkout", ref])
        commits = (
            subprocess.check_output(log_cmd).decode(sys.stdout.encoding).split("\n")
        )
    os.chdir(cur)
    return commits


def get_ray_wheels_url(
    repo_url: str, branch: str, commit: str, ray_version: str
) -> str:
    if not repo_url.startswith("https://github.com/ray-project/ray"):
        raise RayWheelsNotFoundError(
            f"Automatically retrieving Ray wheels URLs is currently not "
            f"implemented for PRs or foreign repositories. Got repository "
            f"{repo_url}, branch {branch}, commit {commit}, "
            f"version {ray_version}"
        )

    return (
        f"https://s3-us-west-2.amazonaws.com/ray-wheels/"
        f"{branch}/{commit}/"
        f"ray-{ray_version}-cp37-cp37m-manylinux2014_x86_64.whl"
    )


def wait_for_url(
    url, timeout: float = 300.0, check_time: float = 30.0, status_time: float = 60.0
) -> str:
    start_time = time.monotonic()
    timeout_at = start_time + timeout
    next_status = start_time + status_time
    logger.info(f"Waiting up to {timeout} seconds until URL is available " f"({url})")
    while not url_exists(url):
        now = time.monotonic()
        if now >= timeout_at:
            raise RayWheelsTimeoutError(
                f"Time out when waiting for URL to be available: {url}"
            )

        if now >= next_status:
            logger.info(
                f"... still waiting for URL {url} "
                f"({int(now - start_time)} seconds) ..."
            )
            next_status += status_time

        # Sleep `check_time` sec before next check.
        time.sleep(check_time)
    logger.info(f"URL is now available: {url}")
    return url


def find_and_wait_for_ray_wheels_url(
    ray_wheels: Optional[str] = None, timeout: float = 3600.0
) -> str:
    ray_wheels_url = find_ray_wheels_url(ray_wheels)
    logger.info(f"Using Ray wheels URL: {ray_wheels_url}")
    return wait_for_url(ray_wheels_url, timeout=timeout)


def find_ray_wheels_url(ray_wheels: Optional[str] = None) -> str:
    if not ray_wheels:
        # If no wheels are specified, default to BUILDKITE_COMMIT
        commit = os.environ.get("BUILDKITE_COMMIT", None)
        if not commit:
            raise RayWheelsUnspecifiedError(
                "No Ray wheels specified. Pass `--ray-wheels` or set "
                "`BUILDKITE_COMMIT` environment variable. "
                "Hint: You can use `-ray-wheels master` to fetch "
                "the latest available master wheels."
            )

        branch = os.environ.get("BUILDKITE_BRANCH", DEFAULT_BRANCH)
        repo_url = os.environ.get("BUILDKITE_REPO", DEFAULT_REPO)

        if not re.match(r"\b([a-f0-9]{40})\b", commit):
            # commit is symbolic, like HEAD
            latest_commits = get_latest_commits(repo_url, branch, ref=commit)
            commit = latest_commits[0]

        ray_version = get_ray_version(repo_url, commit)

        set_test_env_var("RAY_COMMIT", commit)
        set_test_env_var("RAY_BRANCH", branch)
        set_test_env_var("RAY_VERSION", ray_version)

        return get_ray_wheels_url(repo_url, branch, commit, ray_version)

    # If this is a URL, return
    if ray_wheels.startswith("https://") or ray_wheels.startswith("http://"):
        return ray_wheels

    # Else, this is either a commit hash, a branch name, or a combination
    # with a repo, e.g. ray-project:master or ray-project:<commit>
    if ":" in ray_wheels:
        # Repo is specified
        owner_or_url, commit_or_branch = ray_wheels.split(":")
    else:
        # Repo is not specified, use ray-project instead
        owner_or_url = DEFAULT_GIT_OWNER
        commit_or_branch = ray_wheels

    # Construct repo URL for cloning
    if "https://" in owner_or_url:
        # Already is a repo URL
        repo_url = owner_or_url
    else:
        repo_url = REPO_URL_TPL.format(owner=owner_or_url, package=DEFAULT_GIT_PACKAGE)

    # Todo: This is not ideal as branches that mimic a SHA1 hash
    # will also match this.
    if not re.match(r"\b([a-f0-9]{40})\b", commit_or_branch):
        # This is a branch
        branch = commit_or_branch
        latest_commits = get_latest_commits(repo_url, branch)

        # Let's assume the ray version is constant over these commits
        # (otherwise just move it into the for loop)
        ray_version = get_ray_version(repo_url, latest_commits[0])

        for commit in latest_commits:
            wheels_url = get_ray_wheels_url(repo_url, branch, commit, ray_version)
            if url_exists(wheels_url):
                set_test_env_var("RAY_COMMIT", commit)

                return wheels_url

        raise RayWheelsNotFoundError(
            f"Couldn't find latest available wheels for repo "
            f"{repo_url}, branch {branch} (version {ray_version}). "
            f"Try again later or check Buildkite logs if wheel builds "
            f"failed."
        )

    # Else, this is a commit
    commit = commit_or_branch
    ray_version = get_ray_version(repo_url, commit)
    branch = os.environ.get("BUILDKITE_BRANCH", DEFAULT_BRANCH)
    wheels_url = get_ray_wheels_url(repo_url, branch, commit, ray_version)

    set_test_env_var("RAY_COMMIT", commit)
    set_test_env_var("RAY_BRANCH", branch)
    set_test_env_var("RAY_VERSION", ray_version)

    return wheels_url
