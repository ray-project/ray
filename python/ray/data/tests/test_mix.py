import numpy as np
import pytest

import ray
from ray.data._internal.execution.operators.mix_operator import MixOperator
from ray.data._internal.logical.interfaces import LogicalPlan
from ray.data._internal.logical.operators.n_ary_operator import (
    Mix,
    MixStoppingCondition,
    estimate_num_mix_outputs,
)
from ray.data._internal.plan import ExecutionPlan
from ray.data._internal.stats import DatasetStats
from ray.data.dataset import Dataset


def _mix_datasets(datasets, weights=None, stopping_condition=None):
    """Helper to construct a mixed dataset from the logical plan directly.

    TODO: Replace this with the public API once it is added.
    """
    if weights is None:
        weights = [1.0] * len(datasets)

    if stopping_condition is None:
        stopping_condition = MixStoppingCondition.STOP_ON_LONGEST_DROP

    logical_plans = [ds._plan._logical_plan for ds in datasets]
    op = Mix(
        *[plan.dag for plan in logical_plans],
        weights=weights,
        stopping_condition=stopping_condition,
    )
    logical_plan = LogicalPlan(op, datasets[0].context)

    stats = DatasetStats(
        metadata={"Mix": []},
        parent=[d._raw_stats() for d in datasets],
    )
    return Dataset(
        ExecutionPlan(stats, datasets[0].context.copy()),
        logical_plan,
    )


def _make_ds(source_id, num_rows, rows_per_block):
    """Create a dataset where every row has {"source": source_id}."""
    num_blocks = num_rows // rows_per_block
    return ray.data.from_items([{"source": source_id}] * num_rows).repartition(
        num_blocks
    )


@pytest.mark.parametrize("weights", [[1, 1], None])
def test_mix_equal_weights(ray_start_10_cpus_shared, weights):
    """Equal weights with uniform blocks should produce 50/50 every 2 blocks.
    If weights are not provided, they should default to 1.0.
    """
    rows_per_block = 10
    ds1 = _make_ds(source_id=0, num_rows=500, rows_per_block=rows_per_block)
    ds2 = _make_ds(source_id=1, num_rows=500, rows_per_block=rows_per_block)
    mixed = _mix_datasets(
        [ds1, ds2],
        weights=weights,
        stopping_condition=MixStoppingCondition.STOP_ON_LONGEST_DROP,
    )
    # We should round robin between the two datasets.
    # The output should alternate 10 rows for each dataset.
    for batch in mixed.iter_batches(batch_size=2 * rows_per_block):
        ratio = np.sum(batch["source"] == 0) / len(batch["source"])
        assert ratio == 0.5


def test_mix_uneven_weights(ray_start_10_cpus_shared):
    """75/25 weights with uniform blocks should produce the right ratio every 4 blocks."""
    rows_per_block = 10
    ds1 = _make_ds(source_id=0, num_rows=750, rows_per_block=rows_per_block)
    ds2 = _make_ds(source_id=1, num_rows=250, rows_per_block=rows_per_block)
    mixed = _mix_datasets(
        [ds1, ds2],
        weights=[0.75, 0.25],
        stopping_condition=MixStoppingCondition.STOP_ON_LONGEST_DROP,
    )
    for batch in mixed.iter_batches(batch_size=4 * rows_per_block):
        ratio = np.sum(batch["source"] == 0) / len(batch["source"])
        assert ratio == 0.75


def test_mix_single_dataset(ray_start_10_cpus_shared):
    ds1 = _make_ds(source_id=0, num_rows=100, rows_per_block=10)
    mixed = _mix_datasets([ds1], weights=[1.0])
    result = mixed.take_all()
    assert len(result) == 100


def test_mix_three_datasets(ray_start_10_cpus_shared):
    """Three datasets with 50/30/20 weights should produce the right ratio every 10 blocks."""
    rows_per_block = 10
    ds1 = _make_ds(source_id=0, num_rows=500, rows_per_block=rows_per_block)
    ds2 = _make_ds(source_id=1, num_rows=300, rows_per_block=rows_per_block)
    ds3 = _make_ds(source_id=2, num_rows=200, rows_per_block=rows_per_block)
    mixed = _mix_datasets(
        [ds1, ds2, ds3],
        weights=[0.5, 0.3, 0.2],
        stopping_condition=MixStoppingCondition.STOP_ON_LONGEST_DROP,
    )
    for batch in mixed.iter_batches(batch_size=10 * rows_per_block):
        ratio_ds1 = np.sum(batch["source"] == 0) / len(batch["source"])
        ratio_ds2 = np.sum(batch["source"] == 1) / len(batch["source"])
        ratio_ds3 = np.sum(batch["source"] == 2) / len(batch["source"])
        assert ratio_ds1 == 0.5
        assert ratio_ds2 == 0.3
        assert ratio_ds3 == 0.2


def test_mix_stop_on_shortest(ray_start_10_cpus_shared):
    """With STOP_ON_SHORTEST, the pipeline stops when the shorter
    dataset is exhausted. The ratio should hold up to the stop point."""
    ds1 = _make_ds(source_id=0, num_rows=20, rows_per_block=10)
    ds2 = _make_ds(source_id=1, num_rows=50, rows_per_block=10)
    mixed = _mix_datasets(
        [ds1, ds2],
        weights=[0.5, 0.5],
        stopping_condition=MixStoppingCondition.STOP_ON_SHORTEST,
    )
    result = mixed.take_all()
    count_0 = sum(1 for r in result if r["source"] == 0)
    count_1 = sum(1 for r in result if r["source"] == 1)
    # ds1 is shorter (20 rows). All of ds1 should be consumed.
    assert count_0 == 20
    # ds2 should have contributed roughly the same amount. The tolerance
    # of 1 block accounts for the case where we let another block through
    # before the ds1 input is marked as exhausted.
    assert abs(count_1 - count_0) <= 10


def test_mix_stop_on_longest_drop(ray_start_10_cpus_shared):
    """With STOP_ON_LONGEST_DROP, shorter datasets drop out and longer
    datasets continue until all are exhausted."""
    rows_per_block = 10
    ds1 = _make_ds(source_id=0, num_rows=500, rows_per_block=rows_per_block)
    ds2 = _make_ds(source_id=1, num_rows=200, rows_per_block=rows_per_block)
    mixed = _mix_datasets(
        [ds1, ds2],
        weights=[0.5, 0.5],
        stopping_condition=MixStoppingCondition.STOP_ON_LONGEST_DROP,
    )
    ds2_rows_seen = 0
    for batch in mixed.iter_batches(batch_size=2 * rows_per_block):
        # We should round robin between the two datasets, until ds2 is exhausted.
        if ds2_rows_seen < 200:
            ratio = np.sum(batch["source"] == 0) / len(batch["source"])
            assert ratio == 0.5
            ds2_rows_seen += np.sum(batch["source"] == 1)

        # After that point, we should only see rows from ds1.
        else:
            ratio = np.sum(batch["source"] == 0) / len(batch["source"])
            assert ratio == 1.0


def test_mix_non_uniform_block_sizes(ray_start_10_cpus_shared):
    """Non-uniform block sizes: the deficit algorithm should still track
    the target ratio, just with wider oscillations."""
    ds1 = _make_ds(source_id=0, num_rows=480, rows_per_block=120)  # 120-row blocks
    ds2 = _make_ds(source_id=1, num_rows=160, rows_per_block=10)  # 10-row blocks
    mixed = _mix_datasets(
        [ds1, ds2],
        weights=[0.75, 0.25],
        stopping_condition=MixStoppingCondition.STOP_ON_LONGEST_DROP,
    )

    # Mix ordering: [ds0: 120], [ds1: 10, 10, 10, 10], ...
    # Expect every window of 160 rows to have a ratio of 0.75:0.25
    for batch in mixed.iter_batches(batch_size=160):
        ratio = np.sum(batch["source"] == 0) / len(batch["source"])
        assert ratio == 0.75


class TestEstimateNumMixOutputs:
    def test_stop_on_longest_drop(self):
        # Sum of all inputs.
        assert (
            estimate_num_mix_outputs(
                [100, 200], [0.5, 0.5], MixStoppingCondition.STOP_ON_LONGEST_DROP
            )
            == 300
        )

    def test_stop_on_shortest_equal_weights(self):
        # Limited by the smaller input: 100 / 0.5 = 200.
        assert (
            estimate_num_mix_outputs(
                [100, 200], [0.5, 0.5], MixStoppingCondition.STOP_ON_SHORTEST
            )
            == 200
        )

    def test_stop_on_shortest_uneven_weights(self):
        # ds1: 300 / 0.75 = 400, ds2: 100 / 0.25 = 400. Both sustain 400.
        assert (
            estimate_num_mix_outputs(
                [300, 100], [0.75, 0.25], MixStoppingCondition.STOP_ON_SHORTEST
            )
            == 400
        )

    def test_stop_on_shortest_bottleneck(self):
        # ds1: 100 / 0.75 = 133, ds2: 100 / 0.25 = 400. ds1 is bottleneck.
        assert (
            estimate_num_mix_outputs(
                [100, 100], [0.75, 0.25], MixStoppingCondition.STOP_ON_SHORTEST
            )
            == 133
        )

    def test_none_input(self):
        assert (
            estimate_num_mix_outputs(
                [100, None], [0.5, 0.5], MixStoppingCondition.STOP_ON_SHORTEST
            )
            is None
        )

    def test_three_datasets(self):
        # ds1: 500/0.5=1000, ds2: 300/0.3=1000, ds3: 200/0.2=1000.
        assert (
            estimate_num_mix_outputs(
                [500, 300, 200],
                [0.5, 0.3, 0.2],
                MixStoppingCondition.STOP_ON_SHORTEST,
            )
            == 1000
        )

    def test_single_dataset(self):
        assert (
            estimate_num_mix_outputs(
                [100], [1.0], MixStoppingCondition.STOP_ON_SHORTEST
            )
            == 100
        )

    def test_invalid_stopping_condition(self):
        with pytest.raises(ValueError, match="Unknown stopping condition"):
            estimate_num_mix_outputs(
                [100, 200], [0.5, 0.5], "invalid"  # type: ignore[arg-type]
            )


class TestMixOperatorEstimates:
    """Test that MixOperator.num_outputs_total and num_output_rows_total
    query the correct methods on input operators (blocks vs rows)."""

    def _make_mix_op(self, num_blocks, num_rows, weights, stopping_condition):
        """Create a MixOperator stub with mock input dependencies.

        Bypasses the full constructor (which requires real PhysicalOperators)
        and only sets the fields needed by num_outputs_total / num_output_rows_total.
        """
        from unittest.mock import MagicMock

        from ray.data._internal.execution.interfaces import PhysicalOperator

        mock_inputs = []
        for blocks, rows in zip(num_blocks, num_rows):
            mock = MagicMock(spec=PhysicalOperator)
            mock._name = "MockInput"
            mock.num_outputs_total.return_value = blocks
            mock.num_output_rows_total.return_value = rows
            mock.num_output_splits.return_value = 1
            mock._output_dependencies = []
            mock_inputs.append(mock)

        op = MixOperator(
            ray.data.DataContext.get_current(),
            *mock_inputs,
            weights=weights,
            stopping_condition=stopping_condition,
        )
        return op, mock_inputs

    def test_num_outputs_total_stop_on_longest_drop(self):
        op, _ = self._make_mix_op(
            num_blocks=[10, 20],
            num_rows=[1000, 500],
            weights=[0.5, 0.5],
            stopping_condition=MixStoppingCondition.STOP_ON_LONGEST_DROP,
        )
        # Should sum block counts (10 + 20 = 30), not row counts.
        assert op.num_outputs_total() == 30

    def test_num_outputs_total_stop_on_shortest(self):
        op, _ = self._make_mix_op(
            num_blocks=[10, 20],
            num_rows=[1000, 500],
            weights=[0.5, 0.5],
            stopping_condition=MixStoppingCondition.STOP_ON_SHORTEST,
        )
        # Can't estimate block count for STOP_ON_SHORTEST.
        assert op.num_outputs_total() is None

    def test_num_output_rows_total_stop_on_longest_drop(self):
        op, mocks = self._make_mix_op(
            num_blocks=[10, 20],
            num_rows=[1000, 500],
            weights=[0.5, 0.5],
            stopping_condition=MixStoppingCondition.STOP_ON_LONGEST_DROP,
        )
        # Should sum row counts (1000 + 500 = 1500), not block counts.
        assert op.num_output_rows_total() == 1500

    def test_num_output_rows_total_stop_on_shortest(self):
        op, _ = self._make_mix_op(
            num_blocks=[10, 20],
            num_rows=[1000, 500],
            weights=[0.75, 0.25],
            stopping_condition=MixStoppingCondition.STOP_ON_SHORTEST,
        )
        # ds1: 1000/0.75 = 1333, ds2: 500/0.25 = 2000. Min = 1333.
        assert op.num_output_rows_total() == 1333


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
