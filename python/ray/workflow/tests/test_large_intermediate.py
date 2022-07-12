import time
import pytest
from ray.tests.conftest import *  # noqa

import numpy as np
import ray
from ray import workflow


def test_simple_large_intermediate(workflow_start_regular_shared):
    @ray.remote
    def large_input():
        return np.arange(2 ** 24)

    @ray.remote
    def identity(x):
        return x

    @ray.remote
    def average(x):
        return np.mean(x)

    @ray.remote
    def simple_large_intermediate():
        x = large_input.bind()
        y = identity.bind(x)
        return workflow.continuation(average.bind(y))

    start = time.time()
    outputs = workflow.run(simple_large_intermediate.bind())
    print(f"duration = {time.time() - start}")
    assert np.isclose(outputs, 8388607.5)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
