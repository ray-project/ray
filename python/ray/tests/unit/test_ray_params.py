import sys

import pytest

from ray._private.parameter import RayParams


def test_logs_dir_must_be_absolute():
    with pytest.raises(ValueError, match="logs_dir must be absolute"):
        RayParams(logs_dir="relative/logs")


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
