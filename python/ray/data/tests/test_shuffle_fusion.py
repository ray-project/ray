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

    def test_repartition_repartition_fusion(self):
        """Test that consecutive Repartition operations are fused."""
        # Create a simple logical plan with two consecutive repartitions
        input_op = InputData("test_input")
        repartition1 = Repartition(
            input_op=input_op,
            num_outputs=4,
            full_shuffle=False,
            random_permute=False,
        )
        repartition2 = Repartition(
            input_op=repartition1,
            num_outputs=8,
            full_shuffle=True,
            random_permute=True,
        )

        # Create logical plan
        logical_plan = LogicalPlan(repartition2, self.context)

        # Apply shuffle fusion
        optimized_plan = LogicalOptimizer().optimize(logical_plan)

        # Verify fusion occurred
        dag = optimized_plan.dag
        assert isinstance(dag, Repartition)
        assert dag._num_outputs == 8  # Should use the second repartition's output count
        assert dag._full_shuffle  # Should use the second repartition's shuffle setting
        assert (
            dag._random_permute
        )  # Should use the second repartition's permute setting
        assert dag.input_dependencies[0] == input_op  # Should connect directly to input
        # Verify fused name format
        assert dag.name == "[Repartition->Repartition]"

    def test_repartition_repartition_no_fusion_different_keys(self):
        """Test that repartitions with different keys are not fused."""
        input_op = InputData("test_input")
        repartition1 = Repartition(
            input_op=input_op,
            num_outputs=4,
            full_shuffle=True,
            keys=["key1"],
        )
        repartition2 = Repartition(
            input_op=repartition1,
            num_outputs=8,
            full_shuffle=True,
            keys=["key2"],  # Different keys
        )

        logical_plan = LogicalPlan(repartition2, self.context)
        optimized_plan = LogicalOptimizer().optimize(logical_plan)

        # Verify no fusion occurred
        dag = optimized_plan.dag
        assert isinstance(dag, Repartition)
        assert dag._keys == ["key2"]  # Should keep original keys
        assert isinstance(
            dag.input_dependencies[0], Repartition
        )  # Should still have intermediate repartition
        # Verify no fusion occurred - should have original name
        assert dag.name == "Repartition"

    def test_no_fusion_incompatible_remote_args(self):
        """Test that operations with incompatible remote args are not fused."""
        input_op = InputData("test_input")
        repartition1 = Repartition(
            input_op=input_op,
            num_outputs=4,
            full_shuffle=False,
        )
        # Add incompatible remote args
        repartition1._ray_remote_args = {"num_cpus": 1}

        repartition2 = Repartition(
            input_op=repartition1,
            num_outputs=8,
            full_shuffle=True,
        )
        repartition2._ray_remote_args = {
            "num_cpus": 2
        }  # Different resource requirements

        logical_plan = LogicalPlan(repartition2, self.context)
        optimized_plan = LogicalOptimizer().optimize(logical_plan)

        # Verify no fusion occurred
        dag = optimized_plan.dag
        assert isinstance(dag, Repartition)
        assert isinstance(
            dag.input_dependencies[0], Repartition
        )  # Should still have intermediate repartition
        # Verify no fusion occurred - should have original name
        assert dag.name == "Repartition"


class TestShuffleFusionLongChains:
    """Test cases for long chains of shuffle operations."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Set up test fixtures."""
        self.context = DataContext.get_current()

    def test_chain_with_incompatible_operations(self):
        """Test that chains with incompatible operations don't fuse inappropriately."""
        input_op = InputData("test_input")

        # Create a chain where some operations can't fuse
        repartition1 = Repartition(
            input_op=input_op,
            num_outputs=4,
            full_shuffle=False,
            keys=["key1"],  # Different keys
        )
        repartition2 = Repartition(
            input_op=repartition1,
            num_outputs=8,
            full_shuffle=True,
            keys=["key2"],  # Different keys - should not fuse
        )
        repartition3 = Repartition(
            input_op=repartition2,
            num_outputs=16,
            full_shuffle=True,
            keys=["key2"],  # Same keys as prev - should fuse
        )

        logical_plan = LogicalPlan(repartition3, self.context)
        optimized_plan = LogicalOptimizer().optimize(logical_plan)

        # Should have two repartitions: one fused, one separate
        dag = optimized_plan.dag
        assert isinstance(dag, Repartition)
        assert dag._keys == ["key2"]  # Should use the final keys
        # The input should be another Repartition (the first one that couldn't fuse)
        assert isinstance(dag.input_dependencies[0], Repartition)
        assert dag.input_dependencies[0]._keys == ["key1"]
        # Verify fusion occurred for the compatible operations
        assert dag.name == "[Repartition->Repartition]"

    def test_very_long_chain(self):
        """Test fusion of very long chains (10+ operations)."""
        input_op = InputData("test_input")

        # Create a very long chain of repartitions
        current_op = input_op
        expected_fused_name = "Repartition"
        for i in range(10):
            current_op = Repartition(
                input_op=current_op,
                num_outputs=i,
                full_shuffle=(i % 2 == 1),  # Alternate shuffle
                random_permute=(i % 2 == 1),  # Every third one
            )
            if i == 0:
                expected_fused_name = "[Repartition->Repartition]"
            elif i < 9:
                expected_fused_name = f"[{expected_fused_name}->Repartition]"

        logical_plan = LogicalPlan(current_op, self.context)
        optimized_plan = LogicalOptimizer().optimize(logical_plan)

        # Should fuse into a single repartition
        dag = optimized_plan.dag
        assert isinstance(dag, Repartition)
        assert dag._num_outputs == 9  # Should use the final output count
        assert dag._full_shuffle  # Should be True if any were True
        assert dag._random_permute  # Should be True if any were True
        assert dag.input_dependencies[0] == input_op  # Should connect directly to input
        # Verify fused name format - should have nested fusion pattern
        # The actual name has 9 brackets, so let's build it correctly

        assert dag.name == expected_fused_name


if __name__ == "__main__":
    # Run the tests
    pytest.main([__file__])
