"""
This script ensures that some dependencies are _not_ installed in the
current python environment.

This is to ensure that tests with minimal dependencies are not tainted
by too many installed packages.
"""

from typing import List

# These are taken from `setup.py` for ray[default]
DEFAULT_BLACKLIST = [
    "aiohttp",
    "aiohttp_cors",
    "colorful",
    "py-spy",
    "gpustat",
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


if __name__ == "__main__":
    assert_packages_not_installed(DEFAULT_BLACKLIST)
