import os
import subprocess
import sys
from ray_logger import logger
from typing import List


def main() -> int:
    """
    This script determines and runs the right tests for the PR changes. To be used in
    CI and buildkite environment only.
    """
    changed_files = _get_changed_files()
=======
    changed_files = get_changed_files()
>>>>>>> 0ccebbe38a (Add a script to run tests using coverage information):ci/test/ray_release_test.py
    logger.info(f"Changed files: {changed_files}")
    return 0


<<<<<<< HEAD:ci/pipeline/ray_release_test.py
def _get_changed_files() -> List[str]:
=======
def get_changed_files() -> List[str]:
>>>>>>> 0ccebbe38a (Add a script to run tests using coverage information):ci/test/ray_release_test.py
    """
    Get the list of changed files in the current PR.
    """
    base_branch = os.environ.get("BUILDKITE_PULL_REQUEST_BASE_BRANCH")
    if not base_branch:
        return []
    return (
        subprocess.check_output(
            ["git", "diff", "--name-only", f"origin/{base_branch}..HEAD"]
        )
        .decode("utf-8")
        .splitlines()
    )


if __name__ == "__main__":
    sys.exit(main())
