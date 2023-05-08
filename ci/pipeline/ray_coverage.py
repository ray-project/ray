import logging
import os
import subprocess
import sys

import click


def _get_logger():
    logging.basicConfig(
        stream=sys.stderr,
        level=logging.INFO,
        format="%(asctime)s:%(levelname)s:%(name)s:%(message)s",
    )
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    return logger


_logger = _get_logger()
_COVERAGE_FILE_NAME = "ray_release.cov"


@click.command()
@click.argument("test_target", required=True, type=str)
@click.option(
    "--artifact-dir",
    default="/artifact-mount",
    type=str,
    help=(
        "This directory is used to store artifacts, such as coverage information. "
        "In buildkite CI, this is usually artifact-mount."
    ),
)
def main(test_target: str, artifact_dir: str = "/artifact-mount") -> None:
    """
    This script collects dynamic coverage data for the test target, and upload the
    results to database (S3).
    """
    _logger.info(f"Collecting coverage for test target: {test_target}")
    coverage_file = os.path.join(artifact_dir, _COVERAGE_FILE_NAME)
    _run_test(test_target, coverage_file)
    coverage_info = _collect_coverage(coverage_file)
    _logger.info(coverage_info)
    return 0


def _run_test(test_target: str, coverage_file: str) -> None:
    """
    Run test target serially using bazel and compute coverage data using pycov.
    We need to run tests serially to avoid data races when pytest creates the initial
    coverage DB per test run. Also use 'test' dynamic context so we can store
    file coverage information.
    """
    source_dir = os.path.join(os.getcwd(), "release")
    subprocess.check_call(
        [
            "bazel",
            "test",
            test_target,
            "--test_tag_filters",
            "release_unit",
            "--jobs",
            "1",
            "--test_env",
            f"PYTEST_ADDOPTS=--cov-context=test --cov={source_dir} --cov-append",
            "--test_env",
            f"COVERAGE_FILE={coverage_file}",
            "--cache_test_results=no",
        ]
    )


def _collect_coverage(coverage_file: str) -> str:
    return subprocess.check_output(
        ["coverage", "report", f"--data-file={coverage_file}"]
    ).decode("utf-8")


if __name__ == "__main__":
    sys.exit(main())
