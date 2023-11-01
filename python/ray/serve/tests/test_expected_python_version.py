import os
import sys

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


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
