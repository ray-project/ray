import time
import pytest
from ray.tests.conftest import *  # noqa

import numpy as np
from ray import workflow


@workflow.step
def large_input():
    return np.arange(2 ** 24)


@workflow.step
def identity(x):
    return x


@workflow.step
def average(x):
    return np.mean(x)


@workflow.step
def simple_large_intermediate():
    x = large_input.step()
    y = identity.step(x)
    return average.step(y)


def test_simple_large_intermediate(workflow_start_regular_shared):
    start = time.time()
    outputs = simple_large_intermediate.step().run()
    print(f"duration = {time.time() - start}")
    assert np.isclose(outputs, 8388607.5)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
