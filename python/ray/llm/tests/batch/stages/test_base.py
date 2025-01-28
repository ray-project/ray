import sys

import pytest
import ray


def test_basic():
    print(ray.__version__)
    assert 1 + 1 == 2


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
