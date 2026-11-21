import asyncio
import sys
import time
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Optional

import pandas as pd
import pyarrow as pa
import pytest

import ray
from ray._common.test_utils import wait_for_condition
from ray.data._internal.execution.execution_callback import ExecutionCallback
from ray.data._internal.execution.interfaces.physical_operator import DataOpTask
from ray.data._internal.execution.operators.limit_operator import LimitOperator
from ray.data._internal.execution.streaming_executor import StreamingExecutor
from ray.data.block import BlockMetadata
from ray.data.context import DataContext
from ray.data.datasource.datasource import Datasource, ReadTask
from ray.data.tests.conftest import *  # noqa
from ray.data.tests.conftest import (
    CoreExecutionMetrics,
    assert_core_execution_metrics_equals,
    get_initial_core_execution_metrics_snapshot,
)
from ray.data.tests.util import extract_values
from ray.tests.conftest import *  # noqa


class _ReadFunctionsDatasource(Datasource):
    def __init__(
        self, reads: list[Callable[[], Iterator[pa.Table]]], rows_per_read: int = 1
    ) -> None:
        self.reads = reads
        self.rows_per_read = rows_per_read

    def estimate_inmemory_data_size(self) -> int:
        return len(self.reads) * self.rows_per_read * 8

    def get_read_tasks(
        self,
        parallelism: int,
        per_task_row_limit: Optional[int] = None,
        data_context: Optional[DataContext] = None,
    ) -> list[ReadTask]:
        return [
            ReadTask(
                read,
                BlockMetadata(
                    num_rows=self.rows_per_read,
                    size_bytes=self.rows_per_read * 8,
                    input_files=None,
                    exec_stats=None,
                ),
                schema=pa.schema([("id", pa.int64())]),
            )
            for read in self.reads
        ]


@ray.remote(num_cpus=0)
class _LimitCancellationSignals:
    def __init__(self) -> None:
        self.events: dict[str, asyncio.Event] = {}

    async def wait(self, name: str) -> None:
        await self.events.setdefault(name, asyncio.Event()).wait()

    async def signal(self, name: str) -> None:
        self.events.setdefault(name, asyncio.Event()).set()


@pytest.mark.parametrize(
    "ray_start_regular", [{"num_cpus": 4, "include_dashboard": True}], indirect=True
)
@pytest.mark.parametrize(
    ("limit", "preserve_order", "slow_first", "unfused"),
    [
        (1, False, False, False),
        (1, True, False, False),
        (1, False, False, True),
        (2, False, False, True),
        (1, True, True, True),
    ],
)
def test_limit_with_inflight_read(
    ray_start_regular: object,
    monkeypatch: pytest.MonkeyPatch,
    limit: int,
    preserve_order: bool,
    slow_first: bool,
    unfused: bool,
) -> None:
    """Cancel only a read that is outside the selected result."""
    monkeypatch.setattr(
        DataContext.get_current().execution_options, "preserve_order", preserve_order
    )
    monkeypatch.setattr(
        DataContext.get_current(), "op_resource_reservation_enabled", False
    )

    gate = _LimitCancellationSignals.remote()

    def fast_read() -> Iterator[pa.Table]:
        ray.get(gate.wait.remote("read_started"))
        ray.get(gate.signal.remote("fast_read_ready"))
        yield pa.table({"id": [0]})

    def slow_read() -> Iterator[pa.Table]:
        try:
            ray.get(gate.signal.remote("read_started"))
            released = gate.wait.remote("release_read")
            while not ray.wait([released], timeout=0.1)[0]:
                pass
            raise ValueError("injected second-read failure")
        finally:
            ray.get(gate.signal.remote("cleanup"), timeout=5)

    def keep_row(row: dict[str, int]) -> bool:
        return row["id"] >= 0

    def hold_output(batch: pa.Table) -> pa.Table:
        ray.get(gate.signal.remote("downstream_started"))
        ray.get(gate.wait.remote("release_downstream"))
        return batch

    reads = [slow_read, fast_read] if slow_first else [fast_read, slow_read]
    read_args = {"max_retries": 3} if unfused else None
    ds = (
        ray.data.read_datasource(
            _ReadFunctionsDatasource(reads),
            override_num_blocks=2,
            ray_remote_args=read_args,
        )
        .filter(keep_row)
        .limit(limit)
        .map_batches(hold_output, batch_format="pyarrow", batch_size=None)
    )
    with ThreadPoolExecutor(max_workers=1) as pool:
        result = pool.submit(ds.take_all)
        try:
            ray.get(gate.wait.remote("fast_read_ready"), timeout=25)
            if not slow_first:
                ray.get(gate.wait.remote("downstream_started"), timeout=25)
            if limit == 1 and not slow_first:
                # The only way out of the read before its gate is released is
                # cancellation. Its finally must run while the UDF is blocked.
                ray.get(gate.wait.remote("cleanup"), timeout=15)
                assert not result.done()
            else:
                ray.get(gate.signal.remote("release_read"), timeout=5)
                with pytest.raises(
                    ray.exceptions.RayTaskError, match="injected second-read failure"
                ):
                    result.result(timeout=15)
        finally:
            ray.get(gate.signal.remote("release_read"), timeout=5)
            ray.get(gate.signal.remote("release_downstream"), timeout=5)
    if limit == 1 and not slow_first:
        assert result.result(timeout=5) == [{"id": 0}]


@pytest.mark.parametrize(
    "ray_start_regular", [{"num_cpus": 4, "include_dashboard": True}], indirect=True
)
@pytest.mark.parametrize("tail_error", [False, True])
@pytest.mark.parametrize("cleanup_stage", [None, "read", "filter"])
def test_limit_does_not_wait_for_already_producing_read(
    ray_start_regular: object,
    monkeypatch: pytest.MonkeyPatch,
    tail_error: bool,
    cleanup_stage: Optional[str],
    tmp_path: Path,
) -> None:
    """Stop consuming an emitted read while a read or unfused filter cleans up."""
    pending_cleanup = cleanup_stage is not None
    gate = _LimitCancellationSignals.remote()
    cleanup_started = tmp_path / "cleanup_started"
    cleanup_release = tmp_path / "cleanup_release"
    cleanup_done = tmp_path / "cleanup_done"
    ctx = DataContext.get_current()
    monkeypatch.setattr(ctx, "target_max_block_size", 1)
    monkeypatch.setattr(ctx, "op_resource_reservation_enabled", False)

    class ObserveCompletion(ExecutionCallback):
        def __init__(self) -> None:
            self.read_completed: Optional[ray.ObjectRef] = None

        def on_execution_step(self, executor: StreamingExecutor) -> None:
            if pending_cleanup:
                assert executor._topology is not None
                for op in executor._topology:
                    if not op.name.startswith("Read"):
                        continue
                    for task in op.get_active_tasks():
                        if (
                            isinstance(task, DataOpTask)
                            and task._last_block_meta is not None
                        ):
                            self.read_completed = task._streaming_gen.completed()
                if (
                    self.read_completed is not None
                    and ray.wait([self.read_completed], timeout=0, fetch_local=False)[0]
                ):
                    ray.get(gate.signal.remote("tail_ready"), timeout=5)

    monkeypatch.setattr(ctx, "custom_execution_callback_classes", [ObserveCompletion])

    def read_selected() -> Iterator[pa.Table]:
        if pending_cleanup:
            ray.get(gate.wait.remote("slow_started"), timeout=25)
        yield pa.table({"id": [0]})
        released = gate.wait.remote("release_tail")
        while not ray.wait([released], timeout=0.1)[0]:
            pass
        if tail_error:
            raise ValueError("unused tail after selected block")
        yield pa.table({"id": [1]})

    def hold_unneeded_work() -> None:
        try:
            ray.get(gate.signal.remote("slow_started"), timeout=5)
            released = gate.wait.remote("release_slow")
            while not ray.wait([released], timeout=0.1)[0]:
                pass
        finally:
            cleanup_started.touch()
            while not cleanup_release.exists():
                time.sleep(0.05)
            cleanup_done.touch()

    def read_other() -> Iterator[pa.Table]:
        if cleanup_stage == "read":
            hold_unneeded_work()
        yield pa.table({"id": [2]})

    def keep_row(row: dict[str, int]) -> bool:
        if cleanup_stage == "filter" and row["id"] == 2:
            hold_unneeded_work()
        return row["id"] >= 0

    def identity(batch: pa.Table) -> pa.Table:
        ray.get(gate.signal.remote("downstream_started"), timeout=5)
        ray.get(gate.wait.remote("release_downstream"), timeout=25)
        return batch

    reads = [read_selected, read_other] if pending_cleanup else [read_selected]
    ds = (
        ray.data.read_datasource(
            _ReadFunctionsDatasource(reads, rows_per_read=2),
            override_num_blocks=len(reads),
            ray_remote_args={"max_retries": 3} if cleanup_stage == "filter" else None,
        )
        .filter(keep_row)
        .limit(1)
        .map_batches(identity, batch_format="pyarrow", batch_size=None)
    )
    with ThreadPoolExecutor(max_workers=1) as pool:
        result = pool.submit(ds.take_all)
        try:
            ray.get(gate.wait.remote("downstream_started"), timeout=25)
            if pending_cleanup:
                wait_for_condition(cleanup_started.exists, timeout=15)
                ray.get(gate.signal.remote("release_tail"), timeout=5)
                # The tail output/error is available to the executor, not just imminent.
                tail_ready = gate.wait.remote("tail_ready")
                wait_for_condition(
                    lambda: result.done() or bool(ray.wait([tail_ready], timeout=0)[0]),
                    timeout=15,
                )
                if result.done():
                    result.result()  # Surface an unexpected tail error before cleanup.
                assert not result.done(), "downstream is still blocked"
            ray.get(gate.signal.remote("release_downstream"), timeout=5)
            assert result.result(timeout=10) == [{"id": 0}]
            if pending_cleanup:
                assert not cleanup_done.exists()
                cleanup_release.touch()
                wait_for_condition(cleanup_done.exists, timeout=5)
            wait_for_condition(lambda: ray.available_resources().get("CPU", 0) == 4)
        finally:
            cleanup_release.touch()
            for name in ("release_tail", "release_slow", "release_downstream"):
                ray.get(gate.signal.remote(name), timeout=5)


@pytest.mark.parametrize("ray_start_regular", [{"num_cpus": 1}], indirect=True)
@pytest.mark.parametrize("tail_error", [False, True])
@pytest.mark.timeout(30)
def test_limit_drains_read_with_saturated_cpu(
    ray_start_regular: object,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    tail_error: bool,
) -> None:
    """An emitted read must release the only CPU so downstream work can start."""
    limit_reached = tmp_path / "limit_reached"
    cleanup_done = tmp_path / "cleanup_done"
    ctx = DataContext.get_current()
    monkeypatch.setattr(ctx, "target_max_block_size", 1)

    class ObserveLimit(ExecutionCallback):
        def on_execution_step(self, executor: StreamingExecutor) -> None:
            assert executor._topology is not None
            if any(
                isinstance(op, LimitOperator) and op.has_execution_finished()
                for op in executor._topology
            ):
                limit_reached.touch()

    monkeypatch.setattr(ctx, "custom_execution_callback_classes", [ObserveLimit])

    def read() -> Iterator[pa.Table]:
        try:
            yield pa.table({"id": [0]})
            # A local gate keeps the CPU reserved; ray.get/wait would yield it.
            wait_for_condition(limit_reached.exists, timeout=15)
            for i in range(1, 20):
                yield pa.table({"id": [i]})
            if tail_error:
                raise ValueError("unused tail after selected block")
        finally:
            cleanup_done.touch()

    rows = (
        ray.data.read_datasource(
            _ReadFunctionsDatasource([read], rows_per_read=20), override_num_blocks=1
        )
        .filter(lambda row: True)
        .limit(1)
        .map_batches(lambda batch: batch, batch_size=None)
        .take_all()
    )
    assert rows == [{"id": 0}]
    assert cleanup_done.exists()
    wait_for_condition(lambda: ray.available_resources().get("CPU", 0) == 1)


@pytest.mark.parametrize(
    "ray_start_regular", [{"num_cpus": 4, "include_dashboard": True}], indirect=True
)
def test_limit_cancels_nested_read_task(
    ray_start_regular: object, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Cancelling an unused read must also clean up the child it started."""
    gate = _LimitCancellationSignals.remote()
    release = tmp_path / "release_child"
    monkeypatch.setattr(
        DataContext.get_current(), "op_resource_reservation_enabled", False
    )

    @ray.remote(num_cpus=1, max_retries=0)
    def child() -> int:
        try:
            ray.get(gate.signal.remote("child_started"), timeout=5)
            # Model interruptible blocking IO without ray.wait yielding the CPU.
            while not release.exists():
                time.sleep(0.05)
            return 1
        finally:
            ray.get(gate.signal.remote("child_cleanup"), timeout=5)

    def slow_read() -> Iterator[pa.Table]:
        result = child.remote()
        while not ray.wait([result], timeout=0.1)[0]:
            pass
        yield pa.table({"id": [ray.get(result)]})

    def fast_read() -> Iterator[pa.Table]:
        ray.get(gate.wait.remote("child_started"), timeout=25)
        yield pa.table({"id": [0]})

    def keep_row(row: dict[str, int]) -> bool:
        return row["id"] >= 0

    try:
        rows = (
            ray.data.read_datasource(
                _ReadFunctionsDatasource([fast_read, slow_read]), override_num_blocks=2
            )
            .filter(keep_row)
            .limit(1)
            .take_all()
        )
        assert rows == [{"id": 0}]
        ray.get(gate.wait.remote("child_cleanup"), timeout=10)
        wait_for_condition(lambda: ray.available_resources().get("CPU", 0) == 4)
    finally:
        release.touch()


def test_limit_execution(ray_start_regular):
    last_snapshot = get_initial_core_execution_metrics_snapshot()
    override_num_blocks = 20
    ds = ray.data.range(100, override_num_blocks=override_num_blocks)

    # Add some delay to the output to prevent all tasks from finishing
    # immediately.
    def delay(row):
        time.sleep(0.1)
        return row

    ds = ds.map(delay)
    last_snapshot = assert_core_execution_metrics_equals(
        CoreExecutionMetrics(task_count={}),
        last_snapshot=last_snapshot,
    )

    # During lazy execution, we should not execute too many more tasks than is
    # needed to produce the requested number of rows.
    for i in [1, 11]:
        assert extract_values("id", ds.limit(i).take(200)) == list(range(i))
        last_snapshot = assert_core_execution_metrics_equals(
            CoreExecutionMetrics(
                task_count={
                    "ReadRange->Map(delay)": lambda count: count
                    < override_num_blocks / 2,
                    "slice_fn": lambda count: count <= 1,
                }
            ),
            last_snapshot=last_snapshot,
        )

    # .materialize().limit() should only trigger execution once.
    ds = ray.data.range(100, override_num_blocks=20).materialize()
    last_snapshot = assert_core_execution_metrics_equals(
        CoreExecutionMetrics(
            task_count={
                "ReadRange": 20,
            }
        ),
        last_snapshot=last_snapshot,
    )
    for i in [1, 10]:
        assert extract_values("id", ds.limit(i).take(200)) == list(range(i))
        assert_core_execution_metrics_equals(
            CoreExecutionMetrics(task_count={"slice_fn": lambda count: count <= 1}),
            last_snapshot=last_snapshot,
        )


@pytest.mark.parametrize("lazy", [False, True])
def test_limit(ray_start_regular_shared, lazy):
    ds = ray.data.range(100, override_num_blocks=20)
    if not lazy:
        ds = ds.materialize()
    for i in range(100):
        assert extract_values("id", ds.limit(i).take(200)) == list(range(i))


# NOTE: We test outside the power-of-2 range in order to ensure that we're not reading
# redundant files due to exponential ramp-up.
@pytest.mark.parametrize("limit", [10, 20, 30, 60])
def test_limit_no_redundant_read(
    ray_start_regular_shared,
    limit,
):
    # Test that dataset truncation eliminates redundant reads.
    @ray.remote
    class Counter:
        def __init__(self):
            self.count = 0

        def increment(self):
            self.count += 1

        def get(self):
            return self.count

        def reset(self):
            self.count = 0

    class CountingRangeDatasource(Datasource):
        def __init__(self):
            self.counter = Counter.remote()

        def prepare_read(self, parallelism, n):
            def range_(i):
                ray.get(self.counter.increment.remote())
                return [
                    pd.DataFrame({"id": range(parallelism * i, parallelism * i + n)})
                ]

            return [
                ReadTask(
                    lambda i=i: range_(i),
                    BlockMetadata(
                        num_rows=n,
                        size_bytes=sum(
                            sys.getsizeof(i)
                            for i in range(parallelism * i, parallelism * i + n)
                        ),
                        input_files=None,
                        exec_stats=None,
                    ),
                    schema=pa.lib.Schema.from_pandas(pd.DataFrame({"id": []})),
                )
                for i in range(parallelism)
            ]

    source = CountingRangeDatasource()

    total_rows = 1000
    override_num_blocks = 100
    ds = ray.data.read_datasource(
        source,
        override_num_blocks=override_num_blocks,
        n=total_rows // override_num_blocks,
    )
    # Apply multiple limit ops.
    # Once the smallest limit is reached, the entire dataset should stop execution.
    ds = ds.limit(total_rows)
    ds = ds.limit(limit)
    ds = ds.limit(total_rows)
    # Check content.
    assert len(ds.take(limit)) == limit
    # Check number of read tasks launched.
    # min_read_tasks is the minimum number of read tasks needed for the limit.
    # We may launch more tasks than this number, in order to to maximize throughput.
    # But the actual number of read tasks should be less than the parallelism.
    count = ray.get(source.counter.get.remote())
    min_read_tasks = limit // (total_rows // override_num_blocks)
    assert min_read_tasks <= count < override_num_blocks


def test_limit_no_num_row_info(ray_start_regular_shared):
    # Test that datasources with no number-of-rows metadata available are still able to
    # be truncated, falling back to kicking off all read tasks.
    class DumbOnesDatasource(Datasource):
        def prepare_read(self, parallelism, n):
            return parallelism * [
                ReadTask(
                    lambda: [pd.DataFrame({"id": [1] * n})],
                    BlockMetadata(
                        num_rows=None,
                        size_bytes=sys.getsizeof(1) * n,
                        input_files=None,
                        exec_stats=None,
                    ),
                    schema=pa.lib.Schema.from_pandas(pd.DataFrame({"id": []})),
                )
            ]

    ds = ray.data.read_datasource(DumbOnesDatasource(), override_num_blocks=10, n=10)
    for i in range(1, 100):
        assert extract_values("id", ds.limit(i).take(100)) == [1] * i


def test_per_task_row_limit_basic(ray_start_regular_shared, restore_data_context):
    """Test basic per-block limiting functionality."""
    # NOTE: It's critical to preserve ordering for assertions in this test to work
    DataContext.get_current().execution_options.preserve_order = True

    # Simple test that should work with the existing range datasource
    ds = ray.data.range(1000, override_num_blocks=10).limit(50)
    result = ds.take_all()

    # Verify we get the correct results
    assert len(result) == 50
    assert [row["id"] for row in result] == list(range(50))


def test_per_task_row_limit_with_custom_readtask(ray_start_regular_shared):
    """Test per-block limiting directly with ReadTask implementation."""

    def read_data_with_limit():
        # This simulates a ReadTask that reads 200 rows
        return [pd.DataFrame({"id": range(200)})]

    # Create ReadTask with per-block limit
    task_with_limit = ReadTask(
        read_fn=read_data_with_limit,
        metadata=BlockMetadata(
            num_rows=200, size_bytes=1600, input_files=None, exec_stats=None
        ),
        schema=pa.lib.Schema.from_pandas(pd.DataFrame({"id": []})),
        per_task_row_limit=50,
    )

    # Execute the ReadTask
    result_blocks = list(task_with_limit())

    # Should get only 50 rows due to per-block limiting
    assert len(result_blocks) == 1
    assert len(result_blocks[0]) == 50
    assert result_blocks[0]["id"].tolist() == list(range(50))


def test_per_task_row_limit_multiple_blocks_per_task(ray_start_regular_shared):
    """Test per-block limiting when ReadTasks return multiple blocks."""

    def read_multiple_blocks_with_limit():
        # This simulates a ReadTask that returns 3 blocks of 30 rows each
        return [
            pd.DataFrame({"id": range(0, 30)}),
            pd.DataFrame({"id": range(30, 60)}),
            pd.DataFrame({"id": range(60, 90)}),
        ]

    # Create ReadTask with per-block limit of 70 (should get 2.33 blocks)
    task = ReadTask(
        read_fn=read_multiple_blocks_with_limit,
        metadata=BlockMetadata(
            num_rows=90, size_bytes=720, input_files=None, exec_stats=None
        ),
        schema=pa.lib.Schema.from_pandas(pd.DataFrame({"id": []})),
        per_task_row_limit=70,
    )

    result_blocks = list(task())

    # Should get first 2 full blocks (60 rows) plus 10 rows from third block
    total_rows = sum(len(block) for block in result_blocks)
    assert total_rows == 70

    # Verify the data is correct
    all_ids = []
    for block in result_blocks:
        all_ids.extend(block["id"].tolist())
    assert all_ids == list(range(70))


def test_per_task_row_limit_larger_than_data(
    ray_start_regular_shared, restore_data_context
):
    """Test per-block limiting when limit is larger than available data."""

    # NOTE: It's critical to preserve ordering for assertions in this test to work
    DataContext.get_current().execution_options.preserve_order = True

    total_rows = 50
    ds = ray.data.range(total_rows, override_num_blocks=5)
    limited_ds = ds.limit(100)  # Limit larger than data
    result = limited_ds.take_all()

    assert len(result) == total_rows
    assert [row["id"] for row in result] == list(range(total_rows))


def test_per_task_row_limit_exact_block_boundary(
    ray_start_regular_shared, restore_data_context
):
    """Test per-block limiting when limit exactly matches block boundaries."""

    # NOTE: It's critical to preserve ordering for assertions in this test to work
    DataContext.get_current().execution_options.preserve_order = True

    rows_per_block = 20
    num_blocks = 5
    limit = rows_per_block * 2  # Exactly 2 blocks

    ds = ray.data.range(rows_per_block * num_blocks, override_num_blocks=num_blocks)
    limited_ds = ds.limit(limit)
    result = limited_ds.take_all()

    assert len(result) == limit
    assert [row["id"] for row in result] == list(range(limit))


@pytest.mark.parametrize("limit", [1, 5, 10, 25, 50, 99])
def test_per_task_row_limit_various_sizes(
    ray_start_regular_shared, limit, restore_data_context
):
    """Test per-block limiting with various limit sizes."""

    # NOTE: It's critical to preserve ordering for assertions in this test to work
    DataContext.get_current().execution_options.preserve_order = True

    total_rows = 100
    num_blocks = 10

    ds = ray.data.range(total_rows, override_num_blocks=num_blocks)
    limited_ds = ds.limit(limit)
    result = limited_ds.take_all()

    expected_len = min(limit, total_rows)
    assert len(result) == expected_len
    assert [row["id"] for row in result] == list(range(expected_len))


def test_per_task_row_limit_with_transformations(
    ray_start_regular_shared, restore_data_context
):
    """Test that per-block limiting works correctly with transformations."""

    # NOTE: It's critical to preserve ordering for assertions in this test to work
    DataContext.get_current().execution_options.preserve_order = True

    # Test with map operation after limit
    ds = ray.data.range(100, override_num_blocks=10)
    limited_ds = ds.limit(20).map(lambda x: {"doubled": x["id"] * 2})
    result = limited_ds.take_all()

    assert len(result) == 20
    assert [row["doubled"] for row in result] == [i * 2 for i in range(20)]

    # Test with map operation before limit
    ds = ray.data.range(100, override_num_blocks=10)
    limited_ds = ds.map(lambda x: {"doubled": x["id"] * 2}).limit(20)
    result = limited_ds.take_all()

    assert len(result) == 20
    assert [row["doubled"] for row in result] == [i * 2 for i in range(20)]


def test_per_task_row_limit_with_filter(ray_start_regular_shared, restore_data_context):
    """Test per-block limiting with filter operations."""

    # NOTE: It's critical to preserve ordering for assertions in this test to work
    DataContext.get_current().execution_options.preserve_order = True

    # Filter before limit - per-block limiting should still work at read level
    ds = ray.data.range(200, override_num_blocks=10)
    filtered_limited = ds.filter(lambda x: x["id"] % 2 == 0).limit(15)
    result = filtered_limited.take_all()

    assert len(result) == 15
    # Should get first 15 even numbers
    assert [row["id"] for row in result] == [i * 2 for i in range(15)]


def test_per_task_row_limit_readtask_properties(ray_start_regular_shared):
    """Test ReadTask per_block_limit property."""

    def dummy_read():
        return [pd.DataFrame({"id": [1, 2, 3]})]

    # Test ReadTask without per_block_limit
    task_no_limit = ReadTask(
        read_fn=dummy_read,
        metadata=BlockMetadata(
            num_rows=3, size_bytes=24, input_files=None, exec_stats=None
        ),
    )
    assert task_no_limit.per_task_row_limit is None

    # Test ReadTask with per_block_limit
    task_with_limit = ReadTask(
        read_fn=dummy_read,
        metadata=BlockMetadata(
            num_rows=3, size_bytes=24, input_files=None, exec_stats=None
        ),
        per_task_row_limit=10,
    )
    assert task_with_limit.per_task_row_limit == 10


def test_per_task_row_limit_edge_cases(ray_start_regular_shared, restore_data_context):
    """Test per-block limiting edge cases."""

    # NOTE: It's critical to preserve ordering for assertions in this test to work
    DataContext.get_current().execution_options.preserve_order = True

    # Test with single row
    ds = ray.data.range(1, override_num_blocks=1).limit(1)
    result = ds.take_all()
    assert len(result) == 1
    assert result[0]["id"] == 0

    # Test with limit of 1 on large dataset
    ds = ray.data.range(10000, override_num_blocks=100).limit(1)
    result = ds.take_all()
    assert len(result) == 1
    assert result[0]["id"] == 0

    # Test with very large limit
    ds = ray.data.range(100, override_num_blocks=10).limit(999999)
    result = ds.take_all()
    assert len(result) == 100
    assert [row["id"] for row in result] == list(range(100))


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
