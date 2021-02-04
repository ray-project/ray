import pytest
import sys

from ray.experimental import shuffle


def test_shuffle():
    shuffle.main()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
