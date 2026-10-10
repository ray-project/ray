import os
from typing import Optional

import click

from ci.ray_ci.automation.cut_release_branch_lib import (
    assert_branch_absent,
    assert_clean_worktree,
    assert_commit_exists,
    commit_version_bump,
    create_branch,
    get_release_branch_name,
    push_branch,
    update_version,
    validate_version,
)


@click.command()
@click.option("--ray_version", required=True, type=str, help="Release version, X.Y.Z.")
@click.option(
    "--commit", required=True, type=str, help="Commit to cut the release branch from."
)
@click.option(
    "--root_dir",
    required=False,
    type=str,
    help="Ray repository to cut the branch in. Defaults to the Bazel workspace.",
)
@click.option("--remote", default="origin", type=str, help="Remote to check and push.")
@click.option(
    "--push/--no-push",
    default=False,
    help="Push the branch to the remote. Off by default.",
)
@click.option(
    "--dry_run", is_flag=True, default=False, help="Print the steps without running."
)
def main(
    ray_version: str,
    commit: str,
    root_dir: Optional[str],
    remote: str,
    push: bool,
    dry_run: bool,
):
    """
    Cut a Ray release branch from a commit and set the version on it.

    Creates releases/<ray_version> at <commit>, runs update_version so the tree
    carries <ray_version>, and commits that. The branch is only pushed with
    --push.
    """
    if not root_dir:
        root_dir = os.environ.get("BUILD_WORKSPACE_DIRECTORY")
        if not root_dir:
            raise Exception("Please specify --root_dir when not running with Bazel.")

    validate_version(ray_version)
    branch = get_release_branch_name(ray_version)

    assert_clean_worktree(root_dir)
    full_commit = assert_commit_exists(root_dir, commit)
    assert_branch_absent(root_dir, branch, remote)

    click.echo(f"Cutting {branch} from {full_commit} in {root_dir}")
    create_branch(root_dir, branch, full_commit, dry_run)
    update_version(root_dir, ray_version, dry_run)
    commit_version_bump(root_dir, ray_version, full_commit, dry_run)

    if push:
        push_branch(root_dir, branch, remote, dry_run)
        click.echo(f"Pushed {branch} to {remote}.")
    else:
        click.echo(
            f"Created {branch} locally. Push it with: git push {remote} {branch}"
        )


if __name__ == "__main__":
    main()
