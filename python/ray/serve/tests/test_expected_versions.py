import os
import sys

import pydantic
import pytest


@pytest.mark.skipif(
    "EXPECTED_PYTHON_VERSION" not in os.environ,
    reason="EXPECTED_PYTHON_VERSION environment variable not set.",
)
def test_expected_python_version():
    """
    Sanity check that tests are running using the expected Python version.
    """
    expected_python_version = os.environ["EXPECTED_PYTHON_VERSION"]
    actual_major, actual_minor = sys.version_info[:2]
    actual_version = f"{actual_major}.{actual_minor}"
    assert expected_python_version == actual_version, (
        f"expected_python_version={expected_python_version}, "
        f"actual_version={actual_version}"
    )


@pytest.mark.skipif(
    "EXPECTED_PYDANTIC_VERSION" not in os.environ,
    reason="EXPECTED_PYDANTIC_VERSION environment variable not set.",
)
def test_expected_pydantic_version():
    """
    Sanity check that tests are running using the expected Pydantic version.
    """
    expected_pydantic_version = os.environ["EXPECTED_PYDANTIC_VERSION"]
    actual_pydantic_version = pydantic.__version__
    assert expected_pydantic_version == actual_pydantic_version, (
        f"expected_pydantic_version={expected_pydantic_version}, "
        f"actual_pydantic_version={actual_pydantic_version}"
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
