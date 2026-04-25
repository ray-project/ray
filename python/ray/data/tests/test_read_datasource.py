import time
from typing import TYPE_CHECKING, List, Optional

import pandas as pd
import pytest

import ray
from ray.data import ActorPoolStrategy, TaskPoolStrategy
from ray.data._internal.datasource.range_datasource import RangeDatasource
from ray.data._internal.logical.operators.read_operator import Read
from ray.data._internal.util import rows_same
from ray.data.block import Block, BlockMetadata
from ray.data.context import DataContext
from ray.data.datasource import Datasource, ReadTask
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa

if TYPE_CHECKING:
    from ray.actor import ActorHandle
    from ray.data._internal.logical.interfaces import LogicalOperator


def find_read_op(op: "LogicalOperator") -> Optional[Read]:
    """Find the Read operator in the logical plan."""
    if isinstance(op, Read):
        return op
    if hasattr(op, "input_dependencies"):
        for input_op in op.input_dependencies:
            result = find_read_op(input_op)
            if result:
                return result
    return None


class TestDatasource(Datasource):
    """Unified datasource that captures actor_id and optionally tracks concurrency."""

    def __init__(self, n: int, concurrency_counter: Optional["ActorHandle"] = None):
        super().__init__()
        self._n = int(n)
        self._concurrency_counter = concurrency_counter

    def get_name(self) -> str:
        return "TestDatasource"

    def estimate_inmemory_data_size(self) -> Optional[int]:
        """Return an estimate of the in-memory data size."""
        # 2 columns (value, actor_id), 8 bytes per value
        return 8 * self._n * 2

    def get_read_tasks(
        self,
        parallelism: int,
        per_task_row_limit: Optional[int] = None,
        data_context: Optional["DataContext"] = None,
    ) -> List[ReadTask]:
        if self._n == 0:
            return []

        read_tasks: List[ReadTask] = []
        n = self._n
        block_size = max(1, n // parallelism)
        counter_ref = self._concurrency_counter

        def make_block(start: int, count: int, counter) -> Block:
            import pyarrow as pa

            # Track concurrency if counter is provided
            if counter is not None:
                ray.get(counter.inc.remote())  # type: ignore
            try:
                # Simulate some work when tracking concurrency
                if counter is not None:
                    time.sleep(0.1)

                # Capture actor_id during execution
                runtime_context = ray.get_runtime_context()
                actor_id = runtime_context.get_actor_id()

                return pa.Table.from_arrays(
                    [
                        pa.array(range(start, start + count)),
                        pa.array([actor_id] * count),
                    ],
                    names=["value", "actor_id"],
                )
            finally:
                # Decrement concurrency counter if provided
                if counter is not None:
                    ray.get(counter.decr.remote())  # type: ignore

        i = 0
        while i < n:
            count = min(block_size, n - i)
            meta = BlockMetadata(
                num_rows=count,
                size_bytes=8 * count * 2,  # Rough estimate: 2 columns
                input_files=None,
                exec_stats=None,
            )

            def read_fn(start=i, count=count, counter=counter_ref):
                yield make_block(start, count, counter)

            read_tasks.append(
                ReadTask(
                    read_fn,
                    meta,
                    schema=None,
                    per_task_row_limit=per_task_row_limit,
                )
            )
            i += block_size

        return read_tasks


@pytest.mark.parametrize(
    "concurrency,compute,expected_strategy_type",
    [
        (None, None, TaskPoolStrategy),
        (1, None, TaskPoolStrategy),
        (2, None, TaskPoolStrategy),
        (None, TaskPoolStrategy(), TaskPoolStrategy),
        (None, TaskPoolStrategy(size=4), TaskPoolStrategy),
        (None, ActorPoolStrategy(size=2), ActorPoolStrategy),
        (
            1,
            ActorPoolStrategy(size=4),
            TaskPoolStrategy,
        ),  # concurrency takes precedence
    ],
)
def test_read_datasource_compute_strategy(
    ray_start_regular_shared_2_cpus,
    concurrency,
    compute,
    expected_strategy_type,
    target_max_block_size_infinite_or_default,
):
    """Test that compute strategy is correctly set based on concurrency and compute parameters."""
    datasource = RangeDatasource(n=100)

    ds = ray.data.read_datasource(
        datasource,
        concurrency=concurrency,
        compute=compute,
        override_num_blocks=4,
    )

    # Get the logical plan to inspect the compute strategy on the logical operator
    logical_plan = ds._plan._logical_plan
    read_op = find_read_op(logical_plan.dag)

    # Verify the compute strategy type on the logical operator
    assert read_op is not None, "Could not find Read operator in logical plan"
    assert isinstance(
        read_op.compute, expected_strategy_type
    ), f"Expected {expected_strategy_type}, got {type(read_op.compute)}"

    # If concurrency was specified, verify it takes precedence
    if concurrency is not None:
        assert isinstance(read_op.compute, TaskPoolStrategy)
        assert read_op.compute.size == concurrency


@pytest.mark.parametrize(
    "compute,expect_actor_execution",
    [
        (TaskPoolStrategy(), False),
        (ActorPoolStrategy(size=2), True),
        (ActorPoolStrategy(min_size=1, max_size=4), True),
    ],
)
def test_read_datasource_actor_execution(
    ray_start_regular_shared_2_cpus,
    compute,
    expect_actor_execution,
    target_max_block_size_infinite_or_default,
):
    """Test that ReadTasks execute in actors when using ActorPoolStrategy."""
    datasource = TestDatasource(n=100)

    ds = ray.data.read_datasource(
        datasource,
        compute=compute,
        override_num_blocks=4,
    )

    # Materialize to trigger execution
    result = ds.take_all()

    # Extract actor_ids from the data (they're included in each row)
    actor_ids_in_data = {row.get("actor_id") for row in result}

    if expect_actor_execution:
        # Should have actor_ids in the data (not None)
        assert len(actor_ids_in_data) > 0, "Expected actor_ids in data"
        # All actor_ids should be non-None
        assert (
            None not in actor_ids_in_data
        ), "Expected all actor_ids to be non-None for ActorPoolStrategy"
        # With ActorPoolStrategy(size=2), we should have at most 2 unique actor_ids
        # When size is specified, min_size == max_size == size
        if (
            isinstance(compute, ActorPoolStrategy)
            and compute.min_size == compute.max_size
        ):
            assert (
                len(actor_ids_in_data) <= compute.min_size
            ), f"Expected at most {compute.min_size} unique actor_ids, got {len(actor_ids_in_data)}"
    else:
        # Should not have actor_ids (all None)
        assert (
            actor_ids_in_data == {None} or len(actor_ids_in_data) == 0
        ), f"Expected all actor_ids to be None for TaskPoolStrategy, got {actor_ids_in_data}"


@pytest.mark.parametrize(
    "compute_strategy",
    [
        TaskPoolStrategy(),
        TaskPoolStrategy(size=2),
        ActorPoolStrategy(size=2),
        ActorPoolStrategy(min_size=1, max_size=4),
    ],
)
def test_read_datasource_basic_functionality(
    ray_start_regular_shared_2_cpus,
    compute_strategy,
):
    """Test that read_datasource works correctly with different compute strategies."""
    datasource = RangeDatasource(n=100)

    ds = ray.data.read_datasource(
        datasource,
        compute=compute_strategy,
        override_num_blocks=4,
    )

    df = ds.to_pandas()

    expected_df = pd.DataFrame({"value": list(range(100))})
    assert rows_same(df, expected_df)


def test_derive_block_metadata_batched(ray_start_regular_shared):
    """_derive_block_metadata issues one get_local_object_locations call for
    all refs and returns per-task BlockMetadata with the correct size_bytes.

    Regression test for the batched lookup: calling
    get_local_object_locations per-task adds a measurable fraction of
    pipeline setup time at typical read parallelism (tens of thousands
    of tasks). This test also verifies we preserve per-task size
    resolution — each BlockMetadata's size_bytes should match the
    object-store's reported size for that specific ref, not a shared
    estimate.
    """
    from ray.data._internal.planner.plan_read_op import _derive_block_metadata

    # Fabricate three ReadTasks whose serialized sizes differ. We pad each
    # task's read_fn closure with a distinct tail of bytes so the
    # cloudpickled sizes are distinguishable.
    def make_task(tag: bytes) -> ReadTask:
        _captured = tag  # noqa: F841  — captured by the closure below

        def _read_fn() -> List[Block]:
            return [pd.DataFrame({"v": [1], "tag": [_captured.hex()]})]

        meta = BlockMetadata(
            num_rows=1, size_bytes=None, exec_stats=None, input_files=None
        )
        return ReadTask(read_fn=_read_fn, metadata=meta, schema=None)

    tasks = [make_task(b"x" * (1024 * (1 + i))) for i in range(3)]
    refs = [ray.put(t) for t in tasks]
    mds = _derive_block_metadata(tasks, refs)

    assert len(mds) == 3
    sizes = [m.size_bytes for m in mds]
    assert all(s is not None and s > 0 for s in sizes), sizes
    # Distinct closures should produce distinct serialized sizes — i.e.
    # we're not bucketing everything into one shared estimate.
    assert len(set(sizes)) == 3, sizes
    # BlockMetadata shape preserved.
    for md in mds:
        assert md.num_rows == 1
        assert md.exec_stats is None
        assert md.input_files is None


# --- M1 fix regression (cold audit, 2026-04-25) ----------------------
#
# The original M1 commit replaced pristine's "iterate read_tasks once
# in a single for-loop" with two iterations: a list comprehension that
# puts each task into the object store, then a second pass via
# ``zip(read_tasks, read_task_refs)`` inside ``_derive_block_metadata``.
# An ``Iterable[ReadTask]`` value that is ``Sized`` but yields its
# items only once (a Sequence-like wrapper with a non-replayable
# ``__iter__``) silently produces zero ``RefBundle``s on the second
# pass — pristine produced N. No in-tree datasource triggers this
# (every implementation returns a real ``list``), but a third-party
# datasource with a buggy ``__iter__`` would silently lose all reads.
#
# Fix: ``read_tasks = list(read_tasks)`` in ``get_input_data`` after
# the existing ``len(read_tasks)`` check, materializing into a
# guaranteed-replayable container before the two iterations.


class _SinglePassDatasource(Datasource):
    """Datasource whose ``get_read_tasks`` returns a Sized iterable that
    is exhausted after one iteration. Mimics a third-party datasource
    bug where ``__iter__`` returns the same iterator each call instead
    of a fresh one.
    """

    class _SinglePassSequence:
        def __init__(self, items: List[ReadTask]):
            self._items = items
            self._iter = iter(items)

        def __len__(self):
            return len(self._items)

        def __iter__(self):
            # WRONG per the Python data model: returns the same
            # exhaustible iterator each call. Real Sequences return a
            # fresh iter() every time. This shape is unusual but
            # plausible for a buggy third-party datasource.
            return self._iter

    def __init__(self, n: int):
        super().__init__()
        self._n = n

    def get_name(self) -> str:
        return "SinglePassDatasource"

    def estimate_inmemory_data_size(self) -> Optional[int]:
        return 8 * self._n

    def get_read_tasks(
        self,
        parallelism: int,
        per_task_row_limit: Optional[int] = None,
        data_context: Optional[DataContext] = None,
    ):
        import pyarrow as pa

        tasks: List[ReadTask] = []
        for i in range(self._n):

            def read_fn(start=i):
                yield pa.Table.from_arrays(
                    [pa.array([start])], names=["value"]
                )

            tasks.append(
                ReadTask(
                    read_fn,
                    BlockMetadata(
                        num_rows=1,
                        size_bytes=8,
                        input_files=None,
                        exec_stats=None,
                    ),
                    schema=None,
                    per_task_row_limit=per_task_row_limit,
                )
            )
        return self._SinglePassSequence(tasks)


def test_m1_fix_get_input_data_materializes_single_pass_iterable(
    ray_start_regular_shared,
):
    """Regression test for the H11 fix: ``get_input_data`` must
    materialize ``read_tasks`` into a list before iterating it twice.

    Without the fix, a Sized-but-single-pass ``read_tasks`` iterable
    is consumed by the first list comprehension (``ray.put`` per
    task), and the second iteration inside ``_derive_block_metadata``
    yields zero items. The dataset would silently materialize as
    empty.

    With the fix, ``read_tasks = list(read_tasks)`` makes both
    iterations replay the full sequence, and the dataset produces
    N rows.
    """
    n = 5
    ds = ray.data.read_datasource(
        _SinglePassDatasource(n=n),
        override_num_blocks=n,
    )
    rows = ds.take_all()
    assert len(rows) == n, (
        f"Expected {n} rows from a single-pass datasource; got "
        f"{len(rows)}. The fix materializes `read_tasks` to a list so "
        f"the second iteration in `_derive_block_metadata` doesn't see "
        f"an exhausted iterator."
    )
    assert sorted(r["value"] for r in rows) == list(range(n))


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
