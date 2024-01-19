import time

import numpy as np
import pytest

import ray
from ray._private.internal_api import memory_summary
from ray.data._internal.execution.backpressure_policy import (
    ENABLED_BACKPRESSURE_POLICIES_CONFIG_KEY,
    ConcurrencyCapBackpressurePolicy,
    StreamingOutputBackpressurePolicy,
)


def test_spam(shutdown_only, restore_data_context):
    ctx = ray.init(object_store_memory=100 * 1024**2)

    ray.data.DataContext.get_current().use_runtime_metrics_scheduling = True
    ray.data.DataContext.get_current().set_config(
        ENABLED_BACKPRESSURE_POLICIES_CONFIG_KEY, [ConcurrencyCapBackpressurePolicy]
    )
    ray.data.DataContext.get_current().set_config(
        ConcurrencyCapBackpressurePolicy.INIT_CAP_CONFIG_KEY, 1
    )

    def f(batch):
        time.sleep(0.1)
        return {"data": np.zeros(20 * 1024**2, dtype=np.uint8)}

    ds = ray.data.range(10).repartition(10).materialize()
    ds = ds.map_batches(f, batch_size=None)

    for _ in ds.iter_batches(batch_size=None, batch_format="pyarrow"):
        pass

    meminfo = memory_summary(ctx.address_info["address"], stats_only=True)
    assert "Spilled" not in meminfo, meminfo


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
