import re
import subprocess
from typing import List, Optional

RELEASE_BRANCH_PREFIX = "releases/"

# Release versions are plain X.Y.Z. Release candidates and dev versions are not
# cut as release branches, so they are rejected rather than silently accepted.
VERSION_PATTERN = re.compile(r"^\d+\.\d+\.\d+$")


def validate_version(version: str) -> None:
    """
    Raise if the version is not a X.Y.Z release version.
    """
    if not VERSION_PATTERN.match(version):
        raise ValueError(
            f"Invalid Ray version: {version}. Expected a release version of the "
            "form X.Y.Z, for example 2.59.0."
        )


def get_release_branch_name(version: str) -> str:
    """
    Return the release branch name for a Ray version.
    """
    validate_version(version)
    return f"{RELEASE_BRANCH_PREFIX}{version}"


def get_update_version_command(version: str) -> List[str]:
    """
    Return the command that updates the Ray version in the tree.
    """
    # TODO(elliot-barn): Remove the Bazel indirection and call
    # update_version_lib.update_file_version() in process. update_version is
    # invoked as a Bazel target for now so that this script updates the version
    # exactly the way the release process already does, rather than
    # reimplementing which files carry a version.
    return [
        "bazel",
        "run",
        "//ci/ray_ci/automation:update_version",
        "--",
        f"--new_version={version}",
    ]


def get_commit_message(version: str, commit: str) -> str:
    """
    Return the message for the version bump commit on the release branch.
    """
    return f"[release] Cut {get_release_branch_name(version)} from {commit}"


def run_command(
    command: List[str],
    cwd: str,
    dry_run: bool = False,
    capture: bool = True,
) -> str:
    """
    Run a command in cwd and return its stdout. Skipped when dry_run is set,
    unless the command only reads state (capture and read-only callers pass
    dry_run=False explicitly).
    """
    if dry_run:
        print(f"[dry-run] {' '.join(command)}")
        return ""
    if capture:
        return subprocess.check_output(command, cwd=cwd, text=True).strip()
    subprocess.check_call(command, cwd=cwd)
    return ""


def git(args: List[str], cwd: str, dry_run: bool = False) -> str:
    return run_command(["git"] + args, cwd=cwd, dry_run=dry_run)


def assert_clean_worktree(repo_dir: str) -> None:
    """
    Refuse to run against a dirty tree, so the version bump commit cannot pick
    up unrelated local changes.
    """
    status = git(["status", "--porcelain"], cwd=repo_dir)
    if status:
        raise RuntimeError(
            "The working tree has uncommitted changes; commit or stash them "
            f"before cutting a release branch:\n{status}"
        )


def assert_commit_exists(repo_dir: str, commit: str) -> str:
    """
    Return the full sha of commit, raising if it is not a commit in this repo.
    """
    try:
        return git(["rev-parse", "--verify", f"{commit}^{{commit}}"], cwd=repo_dir)
    except subprocess.CalledProcessError:
        raise RuntimeError(f"Commit {commit} does not exist in {repo_dir}.")


def assert_branch_absent(repo_dir: str, branch: str, remote: Optional[str]) -> None:
    """
    Raise if the release branch already exists locally or on the remote, so an
    existing release branch is never moved.
    """
    local = subprocess.run(
        ["git", "rev-parse", "--verify", "--quiet", f"refs/heads/{branch}"],
        cwd=repo_dir,
        capture_output=True,
        text=True,
    )
    if local.returncode == 0:
        raise RuntimeError(f"Branch {branch} already exists locally.")

    if remote:
        remote_refs = git(["ls-remote", "--heads", remote, branch], cwd=repo_dir)
        if remote_refs:
            raise RuntimeError(f"Branch {branch} already exists on {remote}.")


def create_branch(repo_dir: str, branch: str, commit: str, dry_run: bool) -> None:
    git(["checkout", "-b", branch, commit], cwd=repo_dir, dry_run=dry_run)


def update_version(repo_dir: str, version: str, dry_run: bool) -> None:
    run_command(
        get_update_version_command(version),
        cwd=repo_dir,
        dry_run=dry_run,
        capture=False,
    )


def commit_version_bump(
    repo_dir: str, version: str, commit: str, dry_run: bool
) -> None:
    """
    Commit whatever update_version changed. Raises if it changed nothing, which
    means the tree was already at this version.
    """
    if not dry_run:
        changed = git(["status", "--porcelain"], cwd=repo_dir)
        if not changed:
            raise RuntimeError(
                f"update_version made no changes; is the tree already at {version}?"
            )
    git(["add", "-A"], cwd=repo_dir, dry_run=dry_run)
    git(
        ["commit", "-s", "-m", get_commit_message(version, commit)],
        cwd=repo_dir,
        dry_run=dry_run,
    )


def push_branch(repo_dir: str, branch: str, remote: str, dry_run: bool) -> None:
    git(["push", "--set-upstream", remote, branch], cwd=repo_dir, dry_run=dry_run)
