import boto3
import json
import os
import subprocess
import sys

import boto3
import click
from ray_logger import get_logger
from ray_coverage import (
    COVERAGE_FILE_NAME,
    S3_BUCKET_DIR,
    S3_BUCKET_NAME,
    S3_BUCKET_FILE_PREFIX,
)
from typing import List, Set


@click.command()
@click.option(
    "--artifact-dir",
    default="/artifact-mount",
    type=str,
    help=(
        "This directory is used to store artifacts, such as coverage information. "
        "In buildkite CI, this is usually artifact-mount."
    ),
)
def main(artifact_dir: str) -> int:
    """
    This script determines and runs the right tests for the PR changes. To be used in
    CI and buildkite environment only.
    """
    logger = get_logger()
    changed_files = _get_changed_files()
    logger.info(f"Changed files: {changed_files}")
    test_targets = _get_test_targets_for_changed_files(changed_files, artifact_dir)
    logger.info(f"Found the following test targets to run: {test_targets}")
    if not test_targets:
        logger.info("No test targets found, skipping tests.")
        return 0
    _run_tests(test_targets)
    return 0


def _run_tests(test_targets: Set[str]) -> None:
    """
    Run the tests.
    """
    subprocess.check_call(
        ["bazel", "test", "--test_output=streamed"] + list(test_targets)
    )


def _get_test_targets_for_changed_files(
    changed_files: List[str], 
    artifact_dir: str,
) -> Set[str]:
    """
    Get the test target for the changed files.
    """
    coverage_file = _get_coverage_file(artifact_dir)
    logger = get_logger()
    try:
        subprocess.check_output(
            [
                "coverage",
                "json",
                f"--data-file={coverage_file}",
                "--show-contexts",
                f"--include={','.join(changed_files)}",
            ]
        )
    except subprocess.CalledProcessError as e:
        logger.info(f"Failed to generate coverage data: {e.output}")
        return set()
    # coverage data is generated into a json file named coverage.json, with the
    # following format:
    # {
    #   "files": {
    #      "file_name": {
    #        "contexts": {
    #          "line_number": ["test_name"]
    #           ...
    #        }
    #      }
    #      ...
    #   }
    # }
    #
    coverage_data = json.load(open("coverage.json"))
    test_targets = set()
    for data in coverage_data["files"].values():
        context = data["contexts"]
        for tests in context.values():
            for test in tests:
                # test is in the format of file/path/test_name.py::run,
                # convert it to test target in the format of //release::test_name
                test_targets.add(
                    f"//release:{test.split('::')[0].split('/')[-1][:-3]}",
                )
    return test_targets


def _get_coverage_file(artifact_dir: str) -> str:
    """
    Get the location of the test coverage file.
    """

    def _get_last_modified(file):
        return int(file["LastModified"].strftime("%s"))

    s3 = boto3.client("s3")
    files = s3.list_objects_v2(
        Bucket=S3_BUCKET_NAME,
        Prefix=f"{S3_BUCKET_DIR}/{S3_BUCKET_FILE_PREFIX}",
    )["Contents"]
    latest_coverage = sorted(files, key=_get_last_modified)[-1]
    coverage_file_name = os.path.join(artifact_dir, COVERAGE_FILE_NAME)
    s3.download_file(
        Bucket=S3_BUCKET_NAME,
        Key=latest_coverage["Key"],
        Filename=coverage_file_name,
    )
    return coverage_file_name


def _get_changed_files() -> List[str]:
    """
    Get the list of changed files in the current PR.
    """
#    base_branch = os.environ.get("BUILDKITE_PULL_REQUEST_BASE_BRANCH")
    base_branch = 'can-coverage'
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
