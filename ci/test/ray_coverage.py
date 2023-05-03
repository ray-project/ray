import click
import logging
import os
import subprocess
import sys
import tempfile

COVERAGE_FILE_NAME = "ray_release.coverage"


@click.command()
@click.argument("test_target", required=True, type=str)
def main(test_target: str) -> None:
    logger = _get_logger()
    logger.info(f"Collecting coverage for test target: {test_target}")
    coverage_file = os.path.join(tempfile.gettempdir(), COVERAGE_FILE_NAME)
    _run_test(test_target, coverage_file)
    coverage_info = _collect_coverage(coverage_file)
    logger.info(coverage_info)
    return 0


def _run_test(test_target: str, coverage_file: str) -> None:
    subprocess.check_output(
        [
            "bazel",
            "test",
            test_target,
            "--test_env=PYTEST_ADDOPTS=--cov-context=test --cov=ray_release",
            f"--test_env=COVERAGE_FILE={coverage_file}",
        ]
    )


def _collect_coverage(coverage_file: str) -> str:
    return subprocess.check_output(
        ["coverage", "report", f"--data-file={coverage_file}"]
    ).decode("utf-8")


def _get_logger():
    logging.basicConfig(
        stream=sys.stderr,
        level=logging.INFO,
        format="%(asctime)s:%(levelname)s:%(name)s:%(message)s",
    )
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    return logger


if __name__ == "__main__":
    sys.exit(main())
