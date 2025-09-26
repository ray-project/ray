"""
This script ensures that some dependencies are _not_ installed in the
current python environment.

This is to ensure that tests with minimal dependencies are not tainted
by too many installed packages.

It also ensures the correct Python version.
"""

import argparse
import sys
from typing import List

# These are taken from `setup.py` for ray[default]
DEFAULT_BLACKLIST = [
    "aiohttp",
    "aiohttp_cors",
    "colorful",
    "py-spy",
    "opencensus",
    "prometheus_client",
    "smart_open",
    "virtualenv",
    "torch",
    "tensorflow",
    "jax",
]


def assert_packages_not_installed(blacklist: List[str]):
    try:
        from pip._internal.operations import freeze
    except ImportError:  # pip < 10.0
        from pip.operations import freeze

    installed_packages = [p.split("==")[0].split(" @ ")[0] for p in freeze.freeze()]

    assert not any(p in installed_packages for p in blacklist), (
        f"Found blacklisted packages in installed python packages: "
        f"{[p for p in blacklist if p in installed_packages]}. "
        f"Minimal dependency tests could be tainted by this. "
        f"Check the install logs and primary dependencies if any of these "
        f"packages were installed as part of another install step."
    )

    print(
        f"Confirmed that blacklisted packages are not installed in "
        f"current Python environment: {blacklist}"
    )


def assert_python_version(expected_python_version: str) -> None:
    actual_major, actual_minor = sys.version_info[:2]
    actual_version = f"{actual_major}.{actual_minor}"
    expected_version = expected_python_version.strip()
    assert expected_version == actual_version, (
        f"Expected Python version expected_version={expected_version}, "
        f"actual_version={actual_version}"
    )


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--expected-python-version",
        type=str,
        help="Expected Python version in MAJOR.MINOR format, e.g. 3.11",
        default=None,
    )
    args = parser.parse_args()

    assert_packages_not_installed(DEFAULT_BLACKLIST)

    if args.expected_python_version is not None:
        assert_python_version(args.expected_python_version)
