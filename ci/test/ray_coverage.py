import click
import logging
import os
import subprocess
import sys
from contextlib import contextmanager


@click.command()
@click.argument("test_name", required=True, type=str)
def main(test_name: str) -> None:
    logger = _get_logger()
    logger.info(f"Collecting coverage for test: {test_name}")
    with _pushd("release"):
        subprocess.check_output(
            ["python", "-m", "coverage", "run", "-m", "pytest", test_name]
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


@contextmanager
def _pushd(new_dir):
    previous_dir = os.getcwd()
    os.chdir(new_dir)
    try:
        yield
    finally:
        os.chdir(previous_dir)


if __name__ == "__main__":
    sys.exit(main())
