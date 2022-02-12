import json
import os
import shutil
import subprocess
import sys
import tempfile
from typing import Optional

import click

from ray_release.buildkite.filter import filter_tests
from ray_release.buildkite.settings import get_pipeline_settings
from ray_release.buildkite.step import get_step
from ray_release.config import read_and_validate_release_test_collection
from ray_release.exception import ReleaseTestCLIError


@click.command()
@click.option(
    "--test-collection-file",
    default=None,
    type=str,
    help="File containing test configurations",
)
def main(test_collection_file: Optional[str] = None):
    settings = get_pipeline_settings()

    repo = settings["ray_test_repo"]
    branch = settings["ray_test_branch"]
    tmpdir = None

    env = {}
    if repo:
        # If the Ray test repo is set, we clone that repo to fetch
        # the test configuration file. Otherwise we might be missing newly
        # added test.
        repo = settings["ray_test_repo"]
        tmpdir = tempfile.mktemp()

        clone_cmd = f"git clone --depth 1 --branch {branch} {repo} {tmpdir}"
        try:
            subprocess.check_output(clone_cmd, shell=True)
        except Exception as e:
            raise ReleaseTestCLIError(
                f"Could not clone test repository " f"{repo} (branch {branch}): {e}"
            ) from e
        test_collection_file = os.path.join(tmpdir, "release", "release_tests.yaml")
        env = {
            "RAY_TEST_REPO": repo,
            "RAY_TEST_BRANCH": branch,
        }
    else:
        test_collection_file = test_collection_file or os.path.join(
            os.path.dirname(__file__), "..", "..", "release_tests.yaml"
        )
    test_collection = read_and_validate_release_test_collection(test_collection_file)

    if tmpdir:
        shutil.rmtree(tmpdir, ignore_errors=True)

    frequency = settings["frequency"]
    test_name_filter = settings["test_name_filter"]
    ray_wheels = settings["ray_wheels"]

    filtered_tests = filter_tests(
        test_collection, frequency=frequency, test_name_filter=test_name_filter
    )

    steps = []
    for test, smoke_test in filtered_tests:
        step = get_step(test, smoke_test=smoke_test, ray_wheels=ray_wheels, env=env)
        steps.append(step)

    json.dump(steps, sys.stdout)
    sys.stdout.flush()


if __name__ == "__main__":
    sys.exit(main())
