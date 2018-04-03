from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import pytest
import ray


@pytest.fixture
def ray_start():
    # Start the Ray processes.
    ray.init(num_cpus=1, use_raylet=True)
    yield None
    # The code after the yield will run as teardown code.
    ray.worker.cleanup()


def test_basic_task_api(ray_start):

    # Test a simple function.

    @ray.remote
    def f_simple():
        return 1

    assert ray.get(f_simple.remote()) == 1

    # Test multiple return values.

    @ray.remote(num_return_vals=3)
    def f_multiple_returns():
        return 1, 2, 3

    x_id1, x_id2, x_id3 = f_multiple_returns.remote()
    assert ray.get([x_id1, x_id2, x_id3]) == [1, 2, 3]

    # Test arguments passed by value.

    @ray.remote
    def f_args_by_value(x):
        return x

    args = [1, 1.0, "test", b"test", (0, 1), [0, 1], {0: 1}]
    for arg in args:
        assert ray.get(f_args_by_value.remote(arg)) == arg

    # Test arguments passed by ID.

    # Test keyword arguments.
