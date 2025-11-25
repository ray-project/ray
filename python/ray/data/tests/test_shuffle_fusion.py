"""
Test shuffle fusion functionality in Ray Data.

This module tests the ShuffleFusion logical optimization rule that fuses
consecutive shuffle operations together to improve performance.
"""

import pytest

import ray
from ray.data._internal.logical.operators.all_to_all_operator import (
    Repartition,
)
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa


class TestShuffleFusion:
    """Test cases for shuffle fusion optimization."""

    @pytest.mark.parametrize(
        "full_shuffle1,full_shuffle2,expected_full_shuffle",
        [
            (False, False, False),
            (True, False, True),
            (False, True, True),
            (True, True, True),
        ],
        ids=["false_false", "true_false", "false_true", "true_true"],
    )
    @pytest.mark.parametrize(
        "num_outputs1,num_outputs2",
        [
            (10, 5),
        ],
        ids=["10_to_5"],
    )
    def test_repartition_repartition_fusion_e2e(
        self,
        full_shuffle1,
        full_shuffle2,
        expected_full_shuffle,
        num_outputs1,
        num_outputs2,
        shutdown_only,
    ):
        """Test that consecutive repartition calls are fused correctly in actual execution."""
        ds = (
            ray.data.range(10)
            .repartition(num_outputs1, shuffle=full_shuffle1)
            .repartition(num_outputs2, shuffle=full_shuffle2, sort=True, keys=["id"])
        )

        # Execute the dataset - this triggers logical optimization
        result = ds.materialize()
        assert result.num_blocks() == num_outputs2

        # After execution, ds._logical_plan.dag contains the optimized DAG
        # (it's mutated during execution in get_execution_plan)
        optimized_dag = ds._logical_plan.dag
        assert isinstance(optimized_dag, Repartition), "Should be a fused Repartition"

        # Verify the fused repartition has the correct parameters
        assert optimized_dag._num_outputs == num_outputs2
        assert optimized_dag._full_shuffle == expected_full_shuffle
        assert optimized_dag._sort
        assert optimized_dag._keys == ["id"]

        # Verify fusion removed the first repartition (connects directly to InputData)
        assert not isinstance(optimized_dag.input_dependencies[0], Repartition)


if __name__ == "__main__":
    # Run the tests
    pytest.main([__file__])
