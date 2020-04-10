import pytest

import ray
from ray.tests.conftest import ray_start_regular_shared


def test_foo(ray_start_regular_shared):
    ray.put("HI")


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
