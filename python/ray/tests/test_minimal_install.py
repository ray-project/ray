# coding: utf-8
"""
Tests that are specific to minimal installations.
"""

import pytest
import os
import sys


@pytest.mark.skipif(
    os.environ.get("RAY_MINIMAL", "0") != "1",
    reason="Skip unless running in a minimal install.",
)
def test_correct_python_version():
    """
    Validate that Bazel uses the correct Python version in our minimal tests.
    """
    expected_python_version = os.environ.get("EXPECTED_PYTHON_VERSION", "").strip()
    assert (
        expected_python_version
    ), f"EXPECTED_PYTHON_VERSION is {expected_python_version}"

    actual_major, actual_minor = sys.version_info[:2]
    actual_version = f"{actual_major}.{actual_minor}"
    assert actual_version == expected_python_version, (
        f"expected_python_version={expected_python_version}"
        f"actual_version={actual_version}"
    )


@pytest.mark.skipif(
    os.environ.get("RAY_MINIMAL", "0") != "1",
    reason="Skip unless running in a minimal install.",
)
def test_module_import_with_various_non_minimal_deps():
    import unittest.mock as mock
    import itertools

    optional_modules = [
        "opencensus",
        "prometheus_client",
        "aiohttp",
        "aiohttp_cors",
        "pydantic",
        "grpc",
    ]
    for i in range(len(optional_modules)):
        for install_modules in itertools.combinations(optional_modules, i):
            print(install_modules)
            with mock.patch.dict(
                "sys.modules", {mod: mock.Mock() for mod in install_modules}
            ):
                from ray.dashboard.utils import get_all_modules
                from ray.dashboard.utils import DashboardHeadModule

                get_all_modules(DashboardHeadModule)


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
