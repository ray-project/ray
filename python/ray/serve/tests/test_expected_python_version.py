import os
import sys

import pytest


def test_expected_python_version():
    """
    Sanity check that tests are running using the expected Python version.
    """
    expected_python_version = os.environ.get("EXPECTED_PYTHON_VERSION", "").strip()
    assert (
        expected_python_version
    ), f"EXPECTED_PYTHON_VERSION is {expected_python_version}"

    actual_major, actual_minor = sys.version_info[:2]
    actual_version = f"{actual_major}.{actual_minor}"
    assert actual_version == expected_python_version, (
        f"expected_python_version={expected_python_version}, "
        f"actual_version={actual_version}"
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
