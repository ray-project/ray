import pytest
import sys

from ray.experimental import shuffle


def test_shuffle():
    shuffle.main()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-ra", "--durations=0", __file__]))
