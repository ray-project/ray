"""
Test shuffle fusion functionality in Ray Data.

This module tests the ShuffleFusion logical optimization rule that fuses
consecutive shuffle operations together to improve performance.
"""

import pytest

from ray.data._internal.logical.interfaces import LogicalPlan
from ray.data._internal.logical.operators.all_to_all_operator import (
    Repartition,
)
from ray.data._internal.logical.operators.input_data_operator import InputData
from ray.data._internal.logical.optimizers import LogicalOptimizer
from ray.data.context import DataContext
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa


class TestShuffleFusion:
    """Test cases for shuffle fusion optimization."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Set up test fixtures."""
        self.context = DataContext.get_current()

    @pytest.mark.parametrize(
        "keys1,keys2,expected_keys",
        [
            (None, None, None),
            (["a"], None, None),
            (None, ["b"], ["b"]),
            (["a"], ["b"], ["b"]),
        ],
        ids=["none_none", "keys1_none", "none_keys2", "keys1_keys2"],
    )
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
            (4, 8),
            (10, 5),
        ],
        ids=["4_to_8", "10_to_5"],
    )
    @pytest.mark.parametrize(
        "random_permute1,random_permute2,expected_random_permute",
        [
            (False, False, False),
            (True, False, True),
            (False, True, True),
            (True, True, True),
        ],
        ids=["false_false", "true_false", "false_true", "true_true"],
    )
    @pytest.mark.parametrize(
        "sort1,sort2,expected_sort",
        [
            (False, False, False),
            (True, False, False),
            (False, True, True),
            (True, True, True),
        ],
        ids=["false_false", "true_false", "false_true", "true_true"],
    )
    def test_repartition_repartition_fusion(
        self,
        keys1,
        keys2,
        expected_keys,
        full_shuffle1,
        full_shuffle2,
        expected_full_shuffle,
        num_outputs1,
        num_outputs2,
        random_permute1,
        random_permute2,
        expected_random_permute,
        sort1,
        sort2,
        expected_sort,
    ):
        """Test that consecutive Repartition operations are fused with correct parameters."""
        input_op = InputData("test_input")

        # First repartition
        repartition1 = Repartition(
            input_op=input_op,
            num_outputs=num_outputs1,
            full_shuffle=full_shuffle1,
            random_permute=random_permute1,
            keys=keys1,
            sort=sort1,
        )

        # Second repartition
        repartition2 = Repartition(
            input_op=repartition1,
            num_outputs=num_outputs2,
            full_shuffle=full_shuffle2,
            random_permute=random_permute2,
            keys=keys2,
            sort=sort2,
        )

        # Create logical plan
        logical_plan = LogicalPlan(repartition2, self.context)

        # Apply shuffle fusion
        optimized_plan = LogicalOptimizer().optimize(logical_plan)

        # Verify fusion occurred
        dag = optimized_plan.dag
        assert isinstance(dag, Repartition), "Should be a fused Repartition"

        # Verify fused parameters
        assert (
            dag._num_outputs == num_outputs2
        ), "Should use second repartition's num_outputs"
        assert (
            dag._full_shuffle == expected_full_shuffle
        ), f"Expected full_shuffle={expected_full_shuffle}, got {dag._full_shuffle}"
        assert (
            dag._random_permute == expected_random_permute
        ), f"Expected random_permute={expected_random_permute}, got {dag._random_permute}"
        assert (
            dag._keys == expected_keys
        ), f"Expected keys={expected_keys}, got {dag._keys}"
        assert (
            dag._sort == expected_sort
        ), f"Expected sort={expected_sort}, got {dag._sort}"

        # Verify it connects directly to input (fusion removed first repartition)
        assert dag.input_dependencies[0] == input_op, "Should connect directly to input"

        # Verify name format
        assert dag.name == "Repartition"


if __name__ == "__main__":
    # Run the tests
    pytest.main([__file__])
