import numpy as np

from ray.data.block import _get_block_boundaries


def test_groupby_map_groups_get_block_boundaries():
    indices = _get_block_boundaries(
        block={
            "x": np.array([1, 1, 2, 2, 3, 3]),
            "y": np.array([1, 1, 2, 2, 3, 4]),
        }
    )

    assert list(indices) == [0, 2, 4, 5, 6]

    indices = _get_block_boundaries(
        block={
            "x": np.array([1, 1, 2, 2, 3, 3]),
            "y": np.array(["a", "b", "a", "a", "b", "b"]),
        }
    )

    assert list(indices) == [0, 1, 2, 4, 6]

    indices = _get_block_boundaries(np.array([1, 1, 2, 2, 3, 3]))

    assert list(indices) == [0, 2, 4, 6]
