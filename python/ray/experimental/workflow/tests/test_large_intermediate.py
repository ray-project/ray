import time

import numpy as np
from ray.experimental import workflow


@workflow.step
def large_input():
    return np.arange(2**24)


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


def test_simple_large_intermediate():
    import ray
    ray.init()

    start = time.time()
    outputs = workflow.run(simple_large_intermediate.step())
    outputs = ray.get(outputs)
    print(f"duration = {time.time() - start}")

    assert np.isclose(outputs, 8388607.5)
    ray.shutdown()
