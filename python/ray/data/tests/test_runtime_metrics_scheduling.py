import time

import numpy as np
import pytest

import ray
from ray._private.internal_api import memory_summary
from ray.data._internal.execution.backpressure_policy import (
    ENABLED_BACKPRESSURE_POLICIES_CONFIG_KEY,
    StreamingOutputBackpressurePolicy,
)


def test_scheduler_accounts_for_in_flight_tasks(shutdown_only, restore_data_context):
    # The executor launches multiple tasks in each scheduling step. If it doesn't
    # account for the potential output of in flight tasks, it may launch too many tasks
    # and cause spilling.
    ctx = ray.init(object_store_memory=100 * 1024**2)

    ray.data.DataContext.get_current().use_runtime_metrics_scheduling = True
    ray.data.DataContext.get_current().set_config(
        ENABLED_BACKPRESSURE_POLICIES_CONFIG_KEY, [StreamingOutputBackpressurePolicy]
    )

    def f(batch):
        time.sleep(0.1)
        return {"data": np.zeros(24 * 1024**2, dtype=np.uint8)}

    # If the executor doesn't account for the potential output of in flight tasks, it
    # will launch all 8 tasks at once, producing 8 * 24MiB = 192MiB > 100MiB of data.
    ds = ray.data.range(8, parallelism=8).map_batches(f, batch_size=None)

    for _ in ds.iter_batches(batch_size=None, batch_format="pyarrow"):
        pass

    meminfo = memory_summary(ctx.address_info["address"], stats_only=True)
    assert "Spilled" not in meminfo, meminfo


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
