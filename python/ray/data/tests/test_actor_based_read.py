from functools import partial

import pyarrow as pa
import pytest

import ray
from ray.data._internal.compute import ActorPoolStrategy, TaskPoolStrategy
from ray.data.block import BlockMetadata
from ray.data.datasource.datasource import Datasource, ReadTask


def test_callable_read_task_simple_function(ray_start_regular_shared):
    """Test that simple function readers work (backward compatibility)."""

    class SimpleFunctionDatasource(Datasource):
        def estimate_inmemory_data_size(self):
            return 400

        def get_read_tasks(
            self, parallelism, per_task_row_limit=None, data_context=None
        ):
            read_tasks = []
            for i in range(parallelism):
                # Use a simple function (not a callable class)
                def read_fn(partition_id):
                    return [
                        pa.Table.from_pydict(
                            {
                                "partition_id": [partition_id],
                                "value": [partition_id * 10],
                            }
                        )
                    ]

                read_task = ReadTask(
                    read_fn=partial(read_fn, i),
                    metadata=BlockMetadata(
                        num_rows=1,
                        size_bytes=100,
                        input_files=None,
                        exec_stats=None,
                    ),
                )
                read_tasks.append(read_task)
            return read_tasks

    ds = ray.data.read_datasource(
        SimpleFunctionDatasource(),
        override_num_blocks=4,
    )

    result = ds.take_all()

    # Verify we got data from all partitions
    assert len(result) == 4
    partition_ids = sorted([row["partition_id"] for row in result])
    assert partition_ids == [0, 1, 2, 3]
    values = sorted([row["value"] for row in result])
    assert values == [0, 10, 20, 30]


def test_read_task_properties():
    """Test ReadTask properties for constructor args."""

    # Test with constructor args
    task_with_args = ReadTask(
        read_fn=lambda: [],
        read_fn_constructor_args=(1, 2, 3),
        read_fn_constructor_kwargs={"key": "value"},
        metadata=BlockMetadata(
            num_rows=1,
            size_bytes=100,
            input_files=None,
            exec_stats=None,
        ),
    )

    assert task_with_args.read_fn_constructor_args == (1, 2, 3)
    assert task_with_args.read_fn_constructor_kwargs == {"key": "value"}

    # Test without constructor args
    task_without_args = ReadTask(
        read_fn=lambda: [],
        metadata=BlockMetadata(
            num_rows=1,
            size_bytes=100,
            input_files=None,
            exec_stats=None,
        ),
    )

    assert task_without_args.read_fn_constructor_args is None
    assert task_without_args.read_fn_constructor_kwargs is None


@pytest.mark.parametrize(
    "compute,expected_num_blocks",
    [
        (ActorPoolStrategy(size=2), 4),
        (TaskPoolStrategy(), 2),
    ],
)
def test_read_with_compute_strategy(
    ray_start_regular_shared, compute, expected_num_blocks
):
    """Test that different compute strategies work with read operations."""

    class TestDatasource(Datasource):
        def estimate_inmemory_data_size(self):
            return 100 * expected_num_blocks

        def get_read_tasks(
            self, parallelism, per_task_row_limit=None, data_context=None
        ):
            read_tasks = []
            for i in range(parallelism):

                def read_fn(partition_id):

                    return [
                        pa.Table.from_pydict(
                            {
                                "id": [partition_id],
                            }
                        )
                    ]

                read_task = ReadTask(
                    read_fn=partial(read_fn, i),
                    metadata=BlockMetadata(
                        num_rows=1,
                        size_bytes=100,
                        input_files=None,
                        exec_stats=None,
                    ),
                )
                read_tasks.append(read_task)
            return read_tasks

    ds = ray.data.read_datasource(
        TestDatasource(),
        override_num_blocks=expected_num_blocks,
        compute=compute,
    )

    result = ds.take_all()

    # Verify we got data from all partitions
    assert len(result) == expected_num_blocks
    ids = sorted([row["id"] for row in result])
    assert ids == list(range(expected_num_blocks))


@pytest.mark.parametrize(
    "compute,expected_init_calls",
    [
        (ActorPoolStrategy(size=2), 2),
        (TaskPoolStrategy(), 4),
    ],
)
def test_callable_class_with_init(
    ray_start_regular_shared, compute, expected_init_calls
):
    """Test callable class with __init__ using different compute strategies."""
    from ray.data.tests.callable_readers import CallableReaderWithInit, InitTracker

    # Create tracker actor
    tracker = InitTracker.remote()

    num_blocks = 4

    class CallableClassDatasource(Datasource):
        def __init__(self, tracker_handle):
            super().__init__()
            self.tracker_handle = tracker_handle

        def estimate_inmemory_data_size(self):
            return 100 * num_blocks

        def get_read_tasks(
            self, parallelism, per_task_row_limit=None, data_context=None
        ):
            read_tasks = []
            for i in range(parallelism):
                # Use callable class with constructor args, including tracker handle
                # We use i % 2 to create shared configurations (reuse test)
                # Tasks 0 and 2 share "connection_0"
                # Tasks 1 and 3 share "connection_1"
                read_task = ReadTask(
                    read_fn=CallableReaderWithInit,
                    read_fn_constructor_args=(
                        f"connection_{i % 2}",
                        self.tracker_handle,
                    ),
                    read_fn_call_args=(i,),
                    metadata=BlockMetadata(
                        num_rows=1,
                        size_bytes=100,
                        exec_stats=None,
                        input_files=None,
                    ),
                )
                read_tasks.append(read_task)
            return read_tasks

    # Read with specified Strategy
    if isinstance(compute, TaskPoolStrategy):
        with pytest.raises(
            ValueError, match="Callable class readers are only supported with"
        ):
            ray.data.read_datasource(
                CallableClassDatasource(tracker),
                override_num_blocks=num_blocks,
                compute=compute,
            ).take_all()
        return

    ds = ray.data.read_datasource(
        CallableClassDatasource(tracker),
        override_num_blocks=num_blocks,
        compute=compute,
    )

    result = ds.take_all()
    assert len(result) == num_blocks

    # Verify partition IDs (passed via call args)
    partition_ids = sorted([row["partition_id"] for row in result])
    assert partition_ids == list(range(num_blocks))

    # Verify connection strings (passed via constructor args)
    connection_strings = sorted([row["connection_string"] for row in result])
    # Expect "connection_0", "connection_0", "connection_1", "connection_1" (sorted)
    expected_connections = sorted([f"connection_{i % 2}" for i in range(num_blocks)])
    assert connection_strings == expected_connections

    # Verify initialization calls
    init_calls = ray.get(tracker.get_calls.remote())
    assert len(init_calls) == expected_init_calls, (
        f"Expected {expected_init_calls} initialization calls, "
        f"got {len(init_calls)}: {init_calls}"
    )

    if isinstance(compute, ActorPoolStrategy):
        # Verify each partition was initialized with correct args
        init_connections = [call["connection_string"] for call in init_calls]
        assert set(init_connections) == {
            "connection_0",
            "connection_1",
        }


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main([__file__, "-v"]))
