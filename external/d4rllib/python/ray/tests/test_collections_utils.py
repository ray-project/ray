import sys

import pytest

from ray._private.collections_utils import split


def test_split():
    ints = list(range(0, 5))

    assert list(split(ints, 5)) == [ints]
    assert list(split(ints, 10)) == [ints]
    assert list(split(ints, 1)) == [[e] for e in ints]
    assert list(split(ints, 2)) == [[0, 1], [2, 3], [4]]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
