import os

import click
from ray_release.test import (
    LINUX_TEST_PREFIX,
    MACOS_TEST_PREFIX,
    WINDOWS_TEST_PREFIX,
    Test,
)

from ci.ray_ci.utils import ci_init

BAZEL_WORKSPACE_DIR = os.environ.get("BUILD_WORKSPACE_DIRECTORY", "")


@click.command()
def main() -> None:
    """
    This script determines the rayci step ids to run microcheck tests.
    """
    ci_init()
    steps = (
        Test.gen_microcheck_step_ids(LINUX_TEST_PREFIX, BAZEL_WORKSPACE_DIR)
        .union(Test.gen_microcheck_step_ids(WINDOWS_TEST_PREFIX, BAZEL_WORKSPACE_DIR))
        .union(Test.gen_microcheck_step_ids(MACOS_TEST_PREFIX, BAZEL_WORKSPACE_DIR))
    )

    print(",".join(steps))


if __name__ == "__main__":
    main()
