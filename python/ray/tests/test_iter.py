from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
from ray.experimental.iter import from_items, from_generators, from_range, \
    from_actors


def test_from_items(ray_start_regular):
    it = from_items([1, 2, 3, 4])
    assert list(it.sync_iterator()) == [1, 2, 3, 4]


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
