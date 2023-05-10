import os
import subprocess
import sys
from datetime import date
from typing import Optional
from ray_logger import logger

import click
import boto3


COVERAGE_FILE_NAME = "ray_python_small.cov"
S3_BUCKET_NAME = "ray-test-coverage"
S3_BUCKET_DIR = "ci"
S3_BUCKET_FILE_PREFIX = "ray-release-"


@click.command()
@click.argument("test_target", required=True, type=str)
@click.argument("source_dir", required=True, type=str)
@click.option(
    "--artifact-dir",
    default="/artifact-mount",
    type=str,
    help=(
        "This directory is used to store artifacts, such as coverage information. "
        "In buildkite CI, this is usually artifact-mount."
    ),
)
@click.option(
    "--upload",
    is_flag=True,
    show_default=True,
    default=False,
    help=("Upload the computed coverage data to S3."),
)
@click.option(
    "--test-tag-filters",
    type=str,
    help=("Bazel test_tag_filters to use when running tests."),
)
def main(
    test_target: str,
    source_dir: str,
    artifact_dir: str = "/artifact-mount",
    upload: bool = False,
    test_tag_filters: Optional[str] = None,
) -> None:
    """
    This script collects dynamic coverage data for the test target, and upload the
    results to database (S3).
    """
    logger.info(f"Collecting coverage for test target: {test_target}")
    coverage_file = os.path.join(artifact_dir, COVERAGE_FILE_NAME)
    _run_test(test_target, coverage_file, source_dir, test_tag_filters)
    coverage_info = _collect_coverage(coverage_file)
    logger.info(coverage_info)
    if upload:
        _upload_coverage_info(coverage_file)
    return 0


def _upload_coverage_info(coverage_file: str) -> None:
    s3_file_name = (
        f"{S3_BUCKET_DIR}/{S3_BUCKET_FILE_PREFIX}"
        f"{date.today().strftime('%Y-%m-%d')}.cov"
    )
    boto3.client("s3").upload_file(
        coverage_file,
        S3_BUCKET_NAME,
        s3_file_name,
    )
    s3_file_name = _upload_coverage_info(coverage_file)
    logger.info(f"Successfully uploaded coverage data to s3 as {s3_file_name}")


def _run_test(
        test_target: str, 
        coverage_file: str, 
        source_dir: str, 
        test_tag_filters: Optional[str],
) -> None:
    """
    Run test target serially using bazel and compute coverage data using pycov.
    We need to run tests serially to avoid data races when pytest creates the initial
    coverage DB per test run. Also use 'test' dynamic context so we can store
    file coverage information.
    """
    source_dir = os.path.join(os.getcwd(), source_dir)
    additional_bazel_flags = []
    if test_tag_filters:
        additional_bazel_flags.append(f"--test_tag_filters={test_tag_filters}")
    subprocess.check_call(
        [
            "bazel",
            "test",
            "--config=ci $(./ci/run/bazel_export_options)",
            test_target,
            "--jobs",
            "1",
            "--test_env",
            f"PYTEST_ADDOPTS=--cov-context=test --cov={source_dir} --cov-append",
            "--test_env",
            f"COVERAGE_FILE={coverage_file}",
            "--cache_test_results=no",
        ] + additional_bazel_flags
    )


def _collect_coverage(coverage_file: str) -> str:
    return subprocess.check_output(
        ["coverage", "report", f"--data-file={coverage_file}"]
    ).decode("utf-8")


if __name__ == "__main__":
    sys.exit(main())
