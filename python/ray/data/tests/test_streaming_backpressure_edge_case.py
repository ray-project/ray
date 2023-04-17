import pytest
import time
import numpy as np

import ray
from ray._private.internal_api import memory_summary


def test_streaming_backpressure_e2e():

    # This test case is particularly challenging since there is a large input->output
    # increase in data size: https://github.com/ray-project/ray/issues/34041
    class TestSlow:
        def __call__(self, df: np.ndarray):
            time.sleep(2)
            return np.random.randn(1, 20, 1024, 1024)

    class TestFast:
        def __call__(self, df: np.ndarray):
            time.sleep(0.5)
            return np.random.randn(1, 20, 1024, 1024)

    ctx = ray.init(object_store_memory=4e9)
    ds = ray.data.range_tensor(20, shape=(3, 1024, 1024), parallelism=20)

    pipe = ds.map_batches(
        TestFast,
        batch_size=1,
        num_cpus=0.5,
        compute=ray.data.ActorPoolStrategy(size=2),
    ).map_batches(
        TestSlow,
        batch_size=1,
        compute=ray.data.ActorPoolStrategy(size=1),
    )

    for batch in pipe.iter_batches(batch_size=1, prefetch_batches=2):
        ...

    # If backpressure is not working right, we will spill.
    meminfo = memory_summary(ctx.address_info["address"], stats_only=True)
    assert "Spilled" not in meminfo, meminfo


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
