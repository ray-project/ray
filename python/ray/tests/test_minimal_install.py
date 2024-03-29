# coding: utf-8
"""
Tests that are specific to minimal installations.
"""

import unittest.mock as mock
import itertools
import packaging
import os
import sys
from typing import Dict

import pytest


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


class MockBaseModel:
    def __init__(self, *args, **kwargs):
        pass

    def __init_subclass__(self, *args, **kwargs):
        pass


def _make_mock_pydantic_modules(pydantic_version: str) -> Dict:
    """Make a mock for the `pydantic` module.

    This module requires special handling to:
        - Make `BaseModel` a class object so type hints work.
        - Set the `__version__` attribute appropriately.
        - Also mock `pydantic.v1` for `pydantic >= 2.0`.
        - Also mock `pydantic.dataclasses`.

    Returns a dict of mocked modules.
    """
    mock_modules = {
        "pydantic": mock.MagicMock(),
        "pydantic.dataclasses": mock.MagicMock(),
    }
    mock_modules["pydantic"].BaseModel = MockBaseModel
    if packaging.version.parse(pydantic_version) >= packaging.version.parse("1.9.0"):
        mock_modules["pydantic"].__version__ = pydantic_version

    if packaging.version.parse(pydantic_version) >= packaging.version.parse("2.0.0"):
        mock_modules["pydantic.v1"] = mock_modules["pydantic"]

    return mock_modules


@pytest.mark.parametrize("pydantic_version", ["1.8.0", "1.9.0", "2.0.0"])
@pytest.mark.skipif(
    os.environ.get("RAY_MINIMAL", "0") != "1",
    reason="Skip unless running in a minimal install.",
)
def test_module_import_with_various_non_minimal_deps(pydantic_version: str):
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
            mock_modules = {}
            for mod in install_modules:
                if mod == "pydantic":
                    mock_modules.update(**_make_mock_pydantic_modules(pydantic_version))
                else:
                    mock_modules[mod] = mock.MagicMock()

            with mock.patch.dict("sys.modules", mock_modules):
                from ray.dashboard.utils import get_all_modules
                from ray.dashboard.utils import DashboardHeadModule

                get_all_modules(DashboardHeadModule)


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
