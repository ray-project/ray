"""Tests for InputDataBuffer streaming and micro-batching functionality."""

import time
from typing import List, Optional

import pyarrow as pa
import pytest

from ray.data._internal.execution.interfaces import ExecutionOptions
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.logical.operators import Read
from ray.data._internal.planner.plan_read_op import plan_read_op
from ray.data.block import Block, BlockMetadata
from ray.data.context import DataContext
from ray.data.datasource import Datasource, ReadTask
from ray.tests.conftest import *  # noqa


class MockStreamingDatasource(Datasource):
    """Mock streaming datasource that tracks get_read_tasks calls."""

    def __init__(
        self,
        batches_per_fetch: int = 1,
        polling_new_tasks_interval_s: Optional[float] = None,
    ):
        """Initialize mock streaming datasource.

        Args:
            batches_per_fetch: Number of RefBundles to return per get_read_tasks call.
            polling_new_tasks_interval_s: Optional metadata fetch interval in seconds.
        """
        super().__init__()
        self._call_count = 0
        self._batches_per_fetch = batches_per_fetch
        self._call_times = []
        self._polling_new_tasks_interval_s = polling_new_tasks_interval_s

    @property
    def is_streaming(self) -> bool:
        return True

    @property
    def polling_new_tasks_interval_s(self) -> Optional[float]:
        """Return the metadata fetch interval configured for this datasource."""
        return self._polling_new_tasks_interval_s

    def get_read_tasks(
        self,
        parallelism: int,
        per_task_row_limit: Optional[int] = None,
        data_context: Optional[DataContext] = None,
    ) -> List[ReadTask]:
        """Return read tasks, tracking each call."""
        self._call_count += 1
        self._call_times.append(time.time())
        read_tasks = []
        batch_id = self._call_count
        for i in range(self._batches_per_fetch):
            # Create a read task that returns a simple block
            def create_read_fn(batch_id=batch_id, task_id=i):
                def read_fn() -> List[Block]:
                    table = pa.table({"batch_id": [batch_id], "task_id": [task_id]})
                    return [table]

                return read_fn

            metadata = BlockMetadata(
                num_rows=1,
                size_bytes=100,
                input_files=None,
                exec_stats=None,
            )
            read_task = ReadTask(create_read_fn(), metadata)
            read_tasks.append(read_task)

        return read_tasks

    def estimate_inmemory_data_size(self) -> Optional[int]:
        return None


def test_streaming_input_data_buffer_from_datasource(ray_start_regular_shared):
    """Test that metadata fetch interval can be set from datasource."""

    # Create mock streaming datasource with interval configured
    datasource_interval = 1  # 1 second interval
    datasource = MockStreamingDatasource(
        batches_per_fetch=1, polling_new_tasks_interval_s=datasource_interval
    )

    # Create a Read operator
    read_op = Read(
        datasource=datasource,
        datasource_or_legacy_reader=datasource,
        parallelism=1,
    )
    # Set detected parallelism (required before planning)
    read_op.set_detected_parallelism(1)

    # Plan the read operation
    physical_op = plan_read_op(read_op, [], DataContext.get_current())
    input_buffer = physical_op.input_dependencies[0]
    assert isinstance(input_buffer, InputDataBuffer)

    # Verify the interval was set from datasource
    assert input_buffer._polling_new_tasks_interval_s == datasource_interval

    # Start execution
    input_buffer.start(ExecutionOptions())

    # First access triggers fetch
    assert input_buffer.has_next()
    assert datasource._call_count == 1

    # Wait for interval and verify second fetch
    time.sleep(datasource_interval + 0.1)
    assert input_buffer.has_next()
    assert datasource._call_count == 2


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
