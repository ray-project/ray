import importlib
import os
import re
import shlex
import subprocess
import sys
import tempfile
import time
import urllib.request
from typing import Optional, List, Tuple

from ray_release.config import DEFAULT_PYTHON_VERSION, parse_python_version
from ray_release.template import set_test_env_var
from ray_release.exception import (
    RayWheelsUnspecifiedError,
    RayWheelsNotFoundError,
    RayWheelsTimeoutError,
    ReleaseTestSetupError,
)
from ray_release.logger import logger
from ray_release.util import url_exists, python_version_str, resolve_url

DEFAULT_BRANCH = "master"
DEFAULT_GIT_OWNER = "ray-project"
DEFAULT_GIT_PACKAGE = "ray"

REPO_URL_TPL = "https://github.com/{owner}/{package}.git"
INIT_URL_TPL = (
    "https://raw.githubusercontent.com/{fork}/{commit}/python/ray/__init__.py"
)

DEFAULT_REPO = REPO_URL_TPL.format(owner=DEFAULT_GIT_OWNER, package=DEFAULT_GIT_PACKAGE)

# Modules to be reloaded after installing a new local ray version
RELOAD_MODULES = ["ray", "ray.job_submission"]


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


def get_wheels_filename(
    ray_version: str, python_version: Tuple[int, int] = DEFAULT_PYTHON_VERSION
) -> str:
    version_str = python_version_str(python_version)
    suffix = "m" if python_version[1] <= 7 else ""
    return (
        f"ray-{ray_version}-cp{version_str}-cp{version_str}{suffix}-"
        f"manylinux2014_x86_64.whl"
    )


def parse_wheels_filename(
    filename: str,
) -> Tuple[Optional[str], Optional[Tuple[int, int]]]:
    """Parse filename and return Ray version + python version"""
    matched = re.search(
        r"ray-([0-9a-z\.]+)-cp([0-9]{2,3})-cp([0-9]{2,3})m?-manylinux2014_x86_64\.whl$",
        filename,
    )
    if not matched:
        return None, None

    ray_version = matched.group(1)
    py_version_str = matched.group(2)

    try:
        python_version = parse_python_version(py_version_str)
    except Exception:
        return ray_version, None

    return ray_version, python_version


def get_ray_wheels_url(
    repo_url: str,
    branch: str,
    commit: str,
    ray_version: str,
    python_version: Tuple[int, int] = DEFAULT_PYTHON_VERSION,
) -> str:
    if not repo_url.startswith("https://github.com/ray-project/ray"):
        return (
            f"https://ray-ci-artifact-pr-public.s3.amazonaws.com/"
            f"{commit}/tmp/artifacts/.whl/"
            f"{get_wheels_filename(ray_version, python_version)}"
        )

    # Else, ray repo
    return (
        f"https://s3-us-west-2.amazonaws.com/ray-wheels/"
        f"{branch}/{commit}/{get_wheels_filename(ray_version, python_version)}"
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
    ray_wheels: Optional[str] = None,
    python_version: Tuple[int, int] = DEFAULT_PYTHON_VERSION,
    timeout: float = 3600.0,
) -> str:
    ray_wheels_url = find_ray_wheels_url(ray_wheels, python_version=python_version)
    logger.info(f"Using Ray wheels URL: {ray_wheels_url}")
    return wait_for_url(ray_wheels_url, timeout=timeout)


def get_buildkite_repo_branch() -> Tuple[str, str]:
    if "BUILDKITE_BRANCH" not in os.environ:
        return DEFAULT_REPO, DEFAULT_BRANCH

    branch_str = os.environ["BUILDKITE_BRANCH"]

    # BUILDKITE_PULL_REQUEST_REPO can be empty string, use `or` to catch this
    repo_url = os.environ.get("BUILDKITE_PULL_REQUEST_REPO", None) or os.environ.get(
        "BUILDKITE_REPO", DEFAULT_REPO
    )

    if ":" in branch_str:
        # If the branch is user:branch, we split into user, branch
        owner, branch = branch_str.split(":", maxsplit=1)

        # If this is a PR, the repo_url is already set via env variable.
        # We only construct our own repo url if this is a branch build.
        if not os.environ.get("BUILDKITE_PULL_REQUEST_REPO"):
            repo_url = f"https://github.com/{owner}/ray.git"
    else:
        branch = branch_str

    repo_url = repo_url.replace("git://", "https://")
    return repo_url, branch


def find_ray_wheels_url(
    ray_wheels: Optional[str] = None,
    python_version: Tuple[int, int] = DEFAULT_PYTHON_VERSION,
) -> str:
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

        repo_url, branch = get_buildkite_repo_branch()

        if not re.match(r"\b([a-f0-9]{40})\b", commit):
            # commit is symbolic, like HEAD
            latest_commits = get_latest_commits(repo_url, branch, ref=commit)
            commit = latest_commits[0]

        ray_version = get_ray_version(repo_url, commit)

        set_test_env_var("RAY_COMMIT", commit)
        set_test_env_var("RAY_BRANCH", branch)
        set_test_env_var("RAY_VERSION", ray_version)

        return get_ray_wheels_url(repo_url, branch, commit, ray_version, python_version)

    # If this is a URL, return
    if ray_wheels.startswith("https://") or ray_wheels.startswith("http://"):
        ray_wheels_url = maybe_rewrite_wheels_url(
            ray_wheels, python_version=python_version
        )
        return ray_wheels_url

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
            try:
                wheels_url = get_ray_wheels_url(
                    repo_url, branch, commit, ray_version, python_version
                )
            except Exception as e:
                logger.info(f"Commit not found for PR: {e}")
                continue
            if url_exists(wheels_url):
                set_test_env_var("RAY_COMMIT", commit)

                return wheels_url
            else:
                logger.info(
                    f"Wheels URL for commit {commit} does not exist: " f"{wheels_url}"
                )

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
    wheels_url = get_ray_wheels_url(
        repo_url, branch, commit, ray_version, python_version
    )

    set_test_env_var("RAY_COMMIT", commit)
    set_test_env_var("RAY_BRANCH", branch)
    set_test_env_var("RAY_VERSION", ray_version)

    return wheels_url


def maybe_rewrite_wheels_url(
    ray_wheels_url: str, python_version: Tuple[int, int]
) -> str:
    full_url = resolve_url(ray_wheels_url)

    # If the version is matching, just return the full url
    if is_wheels_url_matching_ray_verison(
        ray_wheels_url=full_url, python_version=python_version
    ):
        return full_url

    # Try to parse the version from the filename / URL
    parsed_ray_version, parsed_python_version = parse_wheels_filename(full_url)
    if not parsed_ray_version or not python_version:
        # If we can't parse, we don't know the version, so we raise a warning
        logger.warning(
            f"The passed Ray wheels URL may not work with the python version "
            f"used in this test! Got python version {python_version} and "
            f"wheels URL: {ray_wheels_url}."
        )
        return full_url

    # If we parsed this and the python version is different from the actual version,
    # try to rewrite the URL
    current_filename = get_wheels_filename(parsed_ray_version, parsed_python_version)
    rewritten_filename = get_wheels_filename(parsed_ray_version, python_version)

    new_url = full_url.replace(current_filename, rewritten_filename)
    if new_url != full_url:
        logger.warning(
            f"The passed Ray wheels URL were for a different python version than "
            f"used in this test! Found python version {parsed_python_version} "
            f"but expected {python_version}. The wheels URL was re-written to "
            f"{new_url}."
        )

    return new_url


def is_wheels_url_matching_ray_verison(
    ray_wheels_url: str, python_version: Tuple[int, int]
) -> bool:
    """Return True if the wheels URL wheel matches the supplied python version."""
    expected_filename = get_wheels_filename(
        ray_version="xxx", python_version=python_version
    )
    expected_filename = expected_filename[7:]  # Cut ray-xxx

    return ray_wheels_url.endswith(expected_filename)


def install_matching_ray_locally(ray_wheels: Optional[str]):
    if not ray_wheels:
        logger.warning(
            "No Ray wheels found - can't install matching Ray wheels locally!"
        )
        return
    assert "manylinux2014_x86_64" in ray_wheels, ray_wheels
    if sys.platform == "darwin":
        platform = "macosx_10_15_intel"
    elif sys.platform == "win32":
        platform = "win_amd64"
    else:
        platform = "manylinux2014_x86_64"
    ray_wheels = ray_wheels.replace("manylinux2014_x86_64", platform)
    logger.info(f"Installing matching Ray wheels locally: {ray_wheels}")
    subprocess.check_output(
        "pip uninstall -y ray", shell=True, env=os.environ, text=True
    )
    subprocess.check_output(
        f"pip install -U {shlex.quote(ray_wheels)}",
        shell=True,
        env=os.environ,
        text=True,
    )
    for module_name in RELOAD_MODULES:
        if module_name in sys.modules:
            importlib.reload(sys.modules[module_name])
