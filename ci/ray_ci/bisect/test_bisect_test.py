import sys
import pytest

from ci.ray_ci.bisect.bisect_test import _get_test_target


def test_get_test_target():
    input_to_output = {
        "linux://test": "test",
        "darwin://test": "test",
        "windows://test": "test",
        "test": "test",
    }
    for input, output in input_to_output.items():
        assert _get_test_target(input) == output


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
