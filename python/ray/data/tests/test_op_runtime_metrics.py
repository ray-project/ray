from unittest.mock import MagicMock

import pyarrow as pa
import pytest

import ray
from ray.data._internal.execution.interfaces import RefBundle
from ray.data._internal.execution.interfaces.op_runtime_metrics import OpRuntimeMetrics
from ray.data.block import BlockExecStats, BlockMetadata


def test_average_max_uss_per_task():
    # No tasks submitted yet.
    metrics = OpRuntimeMetrics(MagicMock())
    assert metrics.average_max_uss_per_task is None

    def create_bundle(uss_bytes: int):
        block = ray.put(pa.Table.from_pydict({}))
        stats = BlockExecStats()
        stats.max_uss_bytes = uss_bytes
        stats.wall_time_s = 0
        metadata = BlockMetadata(
            num_rows=0,
            size_bytes=0,
            input_files=None,
            exec_stats=stats,
        )
        return RefBundle([(block, metadata)], owns_blocks=False, schema=None)

    # Submit two tasks.
    bundle = create_bundle(uss_bytes=0)
    metrics.on_task_submitted(0, bundle)
    metrics.on_task_submitted(1, bundle)
    assert metrics.average_max_uss_per_task is None

    # Generate one output for the first task.
    bundle = create_bundle(uss_bytes=1)
    metrics.on_task_output_generated(0, bundle)
    assert metrics.average_max_uss_per_task == 1

    # Generate one output for the second task.
    bundle = create_bundle(uss_bytes=3)
    metrics.on_task_output_generated(0, bundle)
    assert metrics.average_max_uss_per_task == 2  # (1 + 3) / 2 = 2


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
