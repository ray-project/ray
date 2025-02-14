import os
from typing import Tuple

DEFAULT_BRANCH = "master"
DEFAULT_GIT_OWNER = "ray-project"
DEFAULT_GIT_PACKAGE = "ray"

REPO_URL_TPL = "https://github.com/{owner}/{package}.git"

DEFAULT_REPO = REPO_URL_TPL.format(owner=DEFAULT_GIT_OWNER, package=DEFAULT_GIT_PACKAGE)


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
