import pytest

from ray.data._internal.util import _check_pyarrow_version
from ray.data.tests.conftest import *  # noqa: F401, F403


def test_check_pyarrow_version_bounds(unsupported_pyarrow_version):
    # Test that pyarrow versions outside of the defined bounds cause an ImportError to
    # be raised.
    with pytest.raises(ImportError):
        _check_pyarrow_version()


def test_check_pyarrow_version_bounds_disabled(
    unsupported_pyarrow_version,
    disable_pyarrow_version_check,
):
    # Test that pyarrow versions outside of the defined bounds DO NOT cause an
    # ImportError to be raised if the environment variable disabling the check is set.

    # Confirm that ImportError is not raised.
    try:
        _check_pyarrow_version()
    except ImportError as e:
        pytest.fail(f"_check_pyarrow_version failed unexpectedly: {e}")


def test_check_pyarrow_version_supported():
    # Test that the pyarrow installed in this testing environment satisfies the pyarrow
    # version bounds.
    try:
        _check_pyarrow_version()
    except ImportError as e:
        pytest.fail(f"_check_pyarrow_version failed unexpectedly: {e}")


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
