import numpy as np

from ray.data._internal.boundaries import get_key_boundaries


def test_groupby_map_groups_get_key_boundaries():
    indices = get_key_boundaries(
        keys={
            "x": np.array([1, 1, 2, 2, 3, 3]),
            "y": np.array([1, 1, 2, 2, 3, 4]),
        }
    )

    assert list(indices) == [0, 2, 4, 5, 6]

    indices = get_key_boundaries(
        keys={
            "x": np.array([1, 1, 2, 2, 3, 3]),
            "y": np.array(["a", "b", "a", "a", "b", "b"]),
        }
    )

    assert list(indices) == [0, 1, 2, 4, 6]

    indices = get_key_boundaries(np.array([1, 1, 2, 2, 3, 3]))

    assert list(indices) == [0, 2, 4, 6]
