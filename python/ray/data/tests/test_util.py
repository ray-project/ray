import pytest

from ray.data._internal.util import _check_pyarrow_version
from ray.data.tests.conftest import *  # noqa: F401, F403


def test_check_pyarrow_version_bounds(unsupported_pyarrow_version):
    with pytest.raises(ImportError):
        _check_pyarrow_version()


def test_check_pyarrow_version_supported():
    try:
        _check_pyarrow_version()
    except ImportError as e:
        pytest.fail(f"_check_pyarrow_version failed unexpectedly: {e}")


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
