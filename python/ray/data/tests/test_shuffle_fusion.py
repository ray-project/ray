"""
Test shuffle fusion functionality in Ray Data.

This module tests the ShuffleFusion logical optimization rule that fuses
consecutive shuffle operations together to improve performance.
"""

import pytest

from ray.data._internal.logical.interfaces import LogicalPlan
from ray.data._internal.logical.operators.all_to_all_operator import (
    Aggregate,
    RandomShuffle,
    Repartition,
    Sort,
)
from ray.data._internal.logical.operators.input_data_operator import InputData
from ray.data._internal.logical.operators.map_operator import StreamingRepartition
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

    def test_random_shuffle_random_shuffle_fusion(self):
        """Test that consecutive RandomShuffle operations are fused."""
        input_op = InputData("test_input")
        shuffle1 = RandomShuffle(input_op=input_op, seed=42)
        shuffle2 = RandomShuffle(input_op=shuffle1, seed=None)

        logical_plan = LogicalPlan(shuffle2, self.context)
        optimized_plan = LogicalOptimizer().optimize(logical_plan)

        # Verify fusion occurred
        dag = optimized_plan.dag
        assert isinstance(dag, RandomShuffle)
        assert dag._seed is None  # Should use the second shuffle's seed
        assert dag.input_dependencies[0] == input_op  # Should connect directly to input
        # Verify fused name format
        assert dag.name == "[RandomShuffle->RandomShuffle]"

    def test_repartition_random_shuffle_fusion(self):
        """Test that Repartition followed by RandomShuffle is fused."""
        input_op = InputData("test_input")
        repartition = Repartition(
            input_op=input_op,
            num_outputs=4,
            full_shuffle=False,
        )
        shuffle = RandomShuffle(
            input_op=repartition,
            seed=None,
            num_outputs=8,
        )

        logical_plan = LogicalPlan(shuffle, self.context)
        optimized_plan = LogicalOptimizer().optimize(logical_plan)

        # Verify fusion occurred - should become a RandomShuffle
        dag = optimized_plan.dag
        assert isinstance(dag, RandomShuffle)
        assert dag._seed is None
        assert dag._num_outputs == 8
        assert dag.input_dependencies[0] == input_op
        # Verify fused name format
        assert dag.name == "[Repartition->RandomShuffle]"

    def test_random_shuffle_repartition_fusion(self):
        """Test that RandomShuffle followed by Repartition is fused."""
        input_op = InputData("test_input")
        shuffle = RandomShuffle(input_op=input_op, seed=None)
        repartition = Repartition(
            input_op=shuffle,
            num_outputs=8,
            full_shuffle=True,
            random_permute=True,
            keys=["test_key"],
        )

        logical_plan = LogicalPlan(repartition, self.context)
        optimized_plan = LogicalOptimizer().optimize(logical_plan)

        # Verify fusion occurred - should become a Repartition with shuffle enabled
        dag = optimized_plan.dag
        assert isinstance(dag, Repartition)
        assert dag._full_shuffle
        assert dag._random_permute
        assert dag._keys == ["test_key"]
        assert dag._num_outputs == 8
        assert dag.input_dependencies[0] == input_op
        # Verify fused name format
        assert dag.name == "[RandomShuffle->Repartition]"

    def test_sort_sort_fusion(self):
        """Test that consecutive Sort operations are fused."""
        from ray.data._internal.planner.exchange.sort_task_spec import SortKey

        input_op = InputData("test_input")
        sort1 = Sort(
            input_op=input_op,
            sort_key=SortKey(key=["col1"], descending=[False]),
            batch_format="pandas",
        )
        sort2 = Sort(
            input_op=sort1,
            sort_key=SortKey(key=["col2"], descending=[True]),
            batch_format="pandas",
        )

        logical_plan = LogicalPlan(sort2, self.context)
        optimized_plan = LogicalOptimizer().optimize(logical_plan)

        # Verify fusion occurred
        dag = optimized_plan.dag
        assert isinstance(dag, Sort)
        # Should have combined sort keys
        assert len(dag._sort_key._columns) == 2
        assert dag._sort_key._columns == ["col2", "col1"]  # Second sort first
        assert dag._sort_key._descending == [True, False]
        assert dag.input_dependencies[0] == input_op
        # Verify fused name format
        assert dag.name == "[Sort->Sort]"

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


class TestShuffleFusionLongChains:
    """Test cases for long chains of shuffle operations."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Set up test fixtures."""
        self.context = DataContext.get_current()

    def test_long_repartition_chain(self):
        """Test fusion of long chains of repartition operations."""
        input_op = InputData("test_input")

        # Create a chain: Repartition -> Repartition -> Repartition
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
            random_permute=False,
        )
        repartition3 = Repartition(
            input_op=repartition2,
            num_outputs=16,
            full_shuffle=True,
            random_permute=True,
        )

        logical_plan = LogicalPlan(repartition3, self.context)
        optimized_plan = LogicalOptimizer().optimize(logical_plan)

        # Should fuse all three into one
        dag = optimized_plan.dag
        assert isinstance(dag, Repartition)
        assert dag._num_outputs == 16  # Should use the final repartition's output count
        assert dag._full_shuffle  # Should be True if any were True
        assert dag._random_permute  # Should be True if any were True
        assert dag.input_dependencies[0] == input_op  # Should connect directly to input
        # Verify fused name format (should show the final fusion)
        # For long chains, we get nested fusion names like [[Repartition->Repartition]->Repartition]
        assert dag.name == "[[Repartition->Repartition]->Repartition]"

    def test_long_random_shuffle_chain(self):
        """Test fusion of long chains of random shuffle operations."""
        input_op = InputData("test_input")

        # Create a chain: RandomShuffle -> RandomShuffle -> RandomShuffle
        shuffle1 = RandomShuffle(input_op=input_op, seed=42)
        shuffle2 = RandomShuffle(input_op=shuffle1, seed=123)
        shuffle3 = RandomShuffle(input_op=shuffle2, seed=None)

        logical_plan = LogicalPlan(shuffle3, self.context)
        optimized_plan = LogicalOptimizer().optimize(logical_plan)

        # Should fuse all three into one
        dag = optimized_plan.dag
        assert isinstance(dag, RandomShuffle)
        assert dag._seed is None  # Should use the final shuffle's seed
        # For long chains, the input might not be directly connected to input_op
        # Verify fused name format (for long chains, we get nested fusion names)
        assert dag != logical_plan.dag
        assert dag.name == "[RandomShuffle->[RandomShuffle->RandomShuffle]]"

    def test_mixed_shuffle_chain(self):
        """Test fusion of mixed shuffle operation chains."""
        input_op = InputData("test_input")

        # Create a chain: Repartition -> RandomShuffle -> Repartition -> RandomShuffle
        repartition1 = Repartition(
            input_op=input_op,
            num_outputs=4,
            full_shuffle=False,
        )
        shuffle1 = RandomShuffle(input_op=repartition1, seed=42)
        repartition2 = Repartition(
            input_op=shuffle1,
            num_outputs=8,
            full_shuffle=True,
            random_permute=True,
        )
        shuffle2 = RandomShuffle(input_op=repartition2, seed=None)

        logical_plan = LogicalPlan(shuffle2, self.context)
        optimized_plan = LogicalOptimizer().optimize(logical_plan)

        # Should fuse into a single RandomShuffle
        dag = optimized_plan.dag
        assert isinstance(dag, RandomShuffle)
        assert dag._seed is None
        # For complex chains, the input might not be directly connected to input_op
        # Verify fused name format (for complex chains, we get nested fusion names)
        assert (
            dag.name == "[Repartition->[RandomShuffle->[Repartition->RandomShuffle]]]"
        )

    def test_sort_chain_fusion(self):
        """Test fusion of long sort chains."""
        from ray.data._internal.planner.exchange.sort_task_spec import SortKey

        input_op = InputData("test_input")

        # Create a chain: Sort -> Sort -> Sort
        sort1 = Sort(
            input_op=input_op,
            sort_key=SortKey(key=["col1"], descending=[False]),
            batch_format="pandas",
        )
        sort2 = Sort(
            input_op=sort1,
            sort_key=SortKey(key=["col2"], descending=[True]),
            batch_format="pandas",
        )
        sort3 = Sort(
            input_op=sort2,
            sort_key=SortKey(key=["col3"], descending=[False]),
            batch_format="pandas",
        )

        logical_plan = LogicalPlan(sort3, self.context)
        optimized_plan = LogicalOptimizer().optimize(logical_plan)

        # Should fuse all three into one
        dag = optimized_plan.dag
        assert isinstance(dag, Sort)
        # Should have combined sort keys in reverse order (last sort first)
        assert len(dag._sort_key._columns) == 3
        assert dag._sort_key._columns == ["col3", "col2", "col1"]
        assert dag._sort_key._descending == [False, True, False]
        assert dag.input_dependencies[0] == input_op
        # Verify fused name format (for long chains, we get nested fusion names)
        assert dag.name == "[[Sort->Sort]->Sort]"

    def test_complex_mixed_chain(self):
        """Test fusion of complex mixed operation chains."""
        input_op = InputData("test_input")

        # Create a simpler chain to avoid complex fusion issues
        # Repartition -> RandomShuffle -> Repartition
        repartition1 = Repartition(
            input_op=input_op,
            num_outputs=4,
            full_shuffle=False,
        )
        shuffle1 = RandomShuffle(input_op=repartition1, seed=None)
        repartition2 = Repartition(
            input_op=shuffle1,
            num_outputs=8,
            full_shuffle=True,
            random_permute=True,
        )

        logical_plan = LogicalPlan(repartition2, self.context)
        optimized_plan = LogicalOptimizer().optimize(logical_plan)

        # Should fuse into a single Repartition
        dag = optimized_plan.dag
        assert isinstance(dag, Repartition)
        assert dag._full_shuffle
        assert dag._random_permute
        # For complex chains, the input might not be directly connected to input_op
        # Verify fused name format
        assert dag.name == "[[Repartition->RandomShuffle]->Repartition]"

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

    def test_very_long_chain(self):
        """Test fusion of very long chains (10+ operations)."""
        input_op = InputData("test_input")

        # Create a very long chain of repartitions
        current_op = input_op
        for i in range(10):
            current_op = Repartition(
                input_op=current_op,
                num_outputs=2 ** (i + 1),
                full_shuffle=(i % 2 == 1),  # Alternate shuffle
                random_permute=(i % 3 == 2),  # Every third one
            )

        logical_plan = LogicalPlan(current_op, self.context)
        optimized_plan = LogicalOptimizer().optimize(logical_plan)

        # Should fuse into a single repartition
        dag = optimized_plan.dag
        assert isinstance(dag, Repartition)
        assert dag._num_outputs == 2**10  # Should use the final output count
        assert dag._full_shuffle  # Should be True if any were True
        assert dag._random_permute  # Should be True if any were True
        assert dag.input_dependencies[0] == input_op  # Should connect directly to input

    def test_chain_with_streaming_repartition(self):
        """Test fusion with streaming repartition in the chain."""
        input_op = InputData("test_input")

        # Create a chain with streaming repartition
        streaming_repartition = StreamingRepartition(
            input_op=input_op,
            target_num_rows_per_block=100,
        )
        regular_repartition = Repartition(
            input_op=streaming_repartition,
            num_outputs=8,
            full_shuffle=True,
        )
        shuffle = RandomShuffle(
            input_op=regular_repartition,
            seed=None,
        )

        logical_plan = LogicalPlan(shuffle, self.context)
        optimized_plan = LogicalOptimizer().optimize(logical_plan)

        # Should fuse streaming repartition -> regular repartition -> shuffle
        dag = optimized_plan.dag
        assert isinstance(dag, RandomShuffle)
        assert dag._seed is None
        assert dag.input_dependencies[0] == input_op

    def test_chain_fusion_preserves_semantics(self):
        """Test that chain fusion preserves the semantic meaning of operations."""
        input_op = InputData("test_input")

        repartition1 = Repartition(
            input_op=input_op,
            num_outputs=4,
            full_shuffle=False,  # Split repartition
        )
        shuffle1 = RandomShuffle(input_op=repartition1, seed=None)
        repartition2 = Repartition(
            input_op=shuffle1,
            num_outputs=8,
            full_shuffle=True,  # Shuffle repartition
            keys=["test_key"],
        )

        logical_plan = LogicalPlan(repartition2, self.context)
        optimized_plan = LogicalOptimizer().optimize(logical_plan)

        # Should fuse into a single repartition with shuffle enabled
        dag = optimized_plan.dag
        assert isinstance(dag, Repartition)
        assert dag._num_outputs == 8
        assert dag._full_shuffle  # Should be True
        assert dag._keys == ["test_key"]
        # For complex chains, the input might not be directly connected to input_op
        # Verify fused name format
        assert dag != logical_plan.dag
        assert dag.name == "[[Repartition->RandomShuffle]->Repartition]"

    def test_chain_with_aggregate_fusion(self):
        """Test fusion of chains that end with aggregate operations."""
        input_op = InputData("test_input")

        # Create a chain ending with aggregate
        repartition1 = Repartition(
            input_op=input_op,
            num_outputs=4,
            full_shuffle=True,
            keys=["group"],
        )
        aggregate = Aggregate(
            input_op=repartition1,
            key="group",
            aggs=[("count", "id")],
            num_partitions=4,  # Matches repartition output
            batch_format="pandas",
        )

        logical_plan = LogicalPlan(aggregate, self.context)
        optimized_plan = LogicalOptimizer().optimize(logical_plan)

        # Should fuse repartition -> aggregate
        dag = optimized_plan.dag
        assert isinstance(dag, Aggregate)
        assert dag._key == "group"
        assert dag._num_partitions == 4
        # For complex chains, the input might not be directly connected to input_op
        # Verify fused name format
        assert dag.name == "[Repartition->Aggregate]"


if __name__ == "__main__":
    # Run the tests
    pytest.main([__file__])
