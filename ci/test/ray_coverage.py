import click
import logging
import os
import subprocess
import sys


@click.command()
@click.argument("test_name", required=True, type=str)
def main(test_name: str) -> None:
    logger = _get_logger()
    logger.info(f"Collecting coverage for test: {test_name}")
    subprocess.check_output(
        [
            "bazel",
            "test",
            test_name,
            "--test_env=PYTEST_ADDOPTS='--cov=ray_release'",
            f"--test_env=COVERAGE_FILE='{os.getcwd()}/.coverage'",
        ]
    )
    report = subprocess.check_output(
        [
            "python",
            "-m",
            "coverage",
            "report",
        ]
    ).decode("utf-8")
    logger.info(report)

    return 0


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
