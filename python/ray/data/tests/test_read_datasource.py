import time
from typing import TYPE_CHECKING, List, Optional

import pytest

import ray
from ray.data import ActorPoolStrategy, TaskPoolStrategy
from ray.data._internal.datasource.range_datasource import RangeDatasource
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.logical.interfaces import LogicalPlan
from ray.data._internal.logical.operators.map_operator import MapBatches
from ray.data._internal.logical.operators.read_operator import Read
from ray.data._internal.logical.optimizers import PhysicalOptimizer
from ray.data._internal.planner import create_planner
from ray.data.block import Block, BlockMetadata
from ray.data.context import DataContext
from ray.data.datasource import Datasource, ReadTask
from ray.data.tests.conftest import *  # noqa
from ray.data.tests.test_util import ConcurrencyCounter
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


@pytest.mark.parametrize("concurrency", [1, 2, 4])
def test_read_datasource_concurrency_limit(
    ray_start_regular_shared_2_cpus,
    concurrency,
    target_max_block_size_infinite_or_default,
):
    """Test that concurrency parameter limits the number of concurrent tasks."""
    concurrency_counter = ConcurrencyCounter.remote()  # type: ignore
    datasource = TestDatasource(n=100, concurrency_counter=concurrency_counter)

    ds = ray.data.read_datasource(
        datasource,
        concurrency=concurrency,
        override_num_blocks=8,  # More blocks than concurrency to test limiting
    )

    # Materialize the dataset to trigger execution
    ds.materialize()

    # Check that actual concurrency didn't exceed the limit
    actual_max_concurrency = ray.get(concurrency_counter.get_max_concurrency.remote())  # type: ignore
    assert (
        actual_max_concurrency <= concurrency
    ), f"Actual max concurrency ({actual_max_concurrency}) exceeded limit ({concurrency})"


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
        read_op._compute, expected_strategy_type
    ), f"Expected {expected_strategy_type}, got {type(read_op._compute)}"

    # If concurrency was specified, verify it takes precedence
    if concurrency is not None:
        assert isinstance(read_op._compute, TaskPoolStrategy)
        assert read_op._compute.size == concurrency


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


def test_read_datasource_default_behavior(
    ray_start_regular_shared_2_cpus, target_max_block_size_infinite_or_default
):
    """Test that default behavior uses TaskPoolStrategy when no parameters are specified."""
    datasource = RangeDatasource(n=100)

    ds = ray.data.read_datasource(datasource, override_num_blocks=4)

    # Get the logical plan to inspect the compute strategy
    logical_plan = ds._plan._logical_plan

    read_op = find_read_op(logical_plan.dag)
    assert read_op is not None
    assert isinstance(read_op._compute, TaskPoolStrategy)
    # Default TaskPoolStrategy should have size=None
    assert read_op._compute.size is None


def test_read_datasource_concurrency_precedence(
    ray_start_regular_shared_2_cpus,
    target_max_block_size_infinite_or_default,
):
    """Test that concurrency parameter takes precedence over compute parameter."""
    datasource = RangeDatasource(n=100)

    # Specify both concurrency and compute - concurrency should win
    concurrency = 3
    compute = ActorPoolStrategy(size=5)

    ds = ray.data.read_datasource(
        datasource,
        concurrency=concurrency,
        compute=compute,
        override_num_blocks=4,
    )

    # Get the logical plan to inspect the compute strategy
    logical_plan = ds._plan._logical_plan

    read_op = find_read_op(logical_plan.dag)
    assert read_op is not None
    # Should use TaskPoolStrategy with concurrency size, not ActorPoolStrategy
    assert isinstance(read_op._compute, TaskPoolStrategy)
    assert read_op._compute.size == concurrency


@pytest.mark.parametrize(
    "read_compute,map_compute,should_fuse",
    [
        (TaskPoolStrategy(), ActorPoolStrategy(size=2), True),  # Task->Actor fuses
        (
            ActorPoolStrategy(size=2),
            ActorPoolStrategy(size=2),
            False,
        ),  # Actor->Actor doesn't fuse
        (
            TaskPoolStrategy(size=4),
            TaskPoolStrategy(size=4),
            True,
        ),  # Task->Task fuses (same size)
        (
            TaskPoolStrategy(size=2),
            TaskPoolStrategy(size=4),
            False,
        ),  # Task->Task doesn't fuse (different size)
    ],
)
def test_read_datasource_map_batches_fusion(
    ray_start_regular_shared_2_cpus,
    read_compute,
    map_compute,
    should_fuse,
    target_max_block_size_infinite_or_default,
):
    """Test operator fusion behavior with different compute strategies."""
    ctx = DataContext.get_current()

    # Create Read operator with specified compute strategy
    datasource = RangeDatasource(n=100)
    # Set num_outputs to avoid metadata access during planning
    # For parallelism=4 and n=100, we expect roughly 4 output blocks
    read_op = Read(
        datasource=datasource,
        datasource_or_legacy_reader=datasource,
        parallelism=4,
        num_outputs=4,  # Set to match parallelism to avoid metadata access
        compute=read_compute,
    )

    # Create MapBatches with specified compute strategy
    map_op = MapBatches(read_op, lambda x: x, compute=map_compute)

    # Create logical plan
    logical_plan = LogicalPlan(map_op, ctx)

    # Plan and optimize
    planner = create_planner()
    physical_plan = planner.plan(logical_plan)
    physical_plan = PhysicalOptimizer().optimize(physical_plan)

    physical_op = physical_plan.dag

    if should_fuse:
        # Operators should be fused - check for "->" in name
        assert (
            "->" in physical_op.name
        ), f"Expected fused operator name, got {physical_op.name}"
        assert isinstance(physical_op, MapOperator)
        assert len(physical_op.input_dependencies) == 1
        assert isinstance(physical_op.input_dependencies[0], InputDataBuffer)
        # Verify that both logical operators are in the fused operator
        assert len(physical_op._logical_operators) >= 2
        assert any(isinstance(op, Read) for op in physical_op._logical_operators)
        assert any(isinstance(op, MapBatches) for op in physical_op._logical_operators)
    else:
        # Operators should NOT be fused - check that they remain separate
        assert isinstance(physical_op, MapOperator)
        assert physical_op.name == "MapBatches(<lambda>)"
        # The upstream operator should be separate
        assert len(physical_op.input_dependencies) == 1
        upstream_op = physical_op.input_dependencies[0]
        # Upstream should be a MapOperator (Read operator) that is NOT fused with MapBatches
        assert isinstance(upstream_op, MapOperator)
        # If Read uses ActorPoolStrategy, it should not fuse, so upstream should be separate
        if isinstance(read_compute, ActorPoolStrategy):
            assert "->" not in upstream_op.name or upstream_op.name.startswith("Read")


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
    target_max_block_size_infinite_or_default,
):
    """Test that read_datasource works correctly with different compute strategies."""
    datasource = RangeDatasource(n=100)

    ds = ray.data.read_datasource(
        datasource,
        compute=compute_strategy,
        override_num_blocks=4,
    )

    # Verify data is read correctly
    result = ds.take_all()
    assert len(result) == 100

    # Verify values are correct
    values = [row["value"] for row in result]
    assert sorted(values) == list(range(100))


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
