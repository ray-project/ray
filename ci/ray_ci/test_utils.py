import sys
import pytest

from ci.ray_ci.utils import chunk_into_n


def test_chunk_into_n() -> None:
    assert chunk_into_n([1, 2, 3, 4, 5], 2) == [[1, 2, 3], [4, 5]]
    assert chunk_into_n([1, 2], 3) == [[1], [2], []]
    assert chunk_into_n([1, 2], 1) == [[1, 2]]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
