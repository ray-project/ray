import os
import subprocess
import sys
from ray_logger import get_logger
from typing import List


def main() -> int:
    """
    This script determines and runs the right tests for the PR changes. To be used in
    CI and buildkite environment only.
    """
    logger = get_logger()
    changed_files = _get_changed_files()
    logger.info(f"Changed files: {changed_files}")
    return 0


def _get_changed_files() -> List[str]:
    """
    Get the list of changed files in the current PR.
    """
    base_branch = os.environ["BUILDKITE_PULL_REQUEST_BASE_BRANCH"]
    return (
        subprocess.check_output(
            ["git", "diff", "--name-only", f"origin/{base_branch}..HEAD"]
        )
        .decode("utf-8")
        .splitlines()
    )


if __name__ == "__main__":
    sys.exit(main())
