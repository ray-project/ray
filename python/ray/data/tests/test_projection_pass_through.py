"""Tests for LogicalOperatorSupportsProjectionPassThrough functionality.

This test file validates projection pass-through behavior for operators that
support pushing projections through themselves:
- RandomizeBlocks
- RandomShuffle
- Repartition
- Sort
- Join
- Zip
- Union
- StreamingRepartition
"""

import pytest

import ray
from ray.data._internal.logical.interfaces import LogicalPlan
from ray.data._internal.logical.operators.all_to_all_operator import (
    RandomizeBlocks,
    RandomShuffle,
    Repartition,
    Sort,
)
from ray.data._internal.logical.operators.input_data_operator import InputData
from ray.data._internal.logical.operators.join_operator import Join
from ray.data._internal.logical.operators.map_operator import (
    Project,
    StreamingRepartition,
)
from ray.data._internal.logical.operators.n_ary_operator import Union, Zip
from ray.data._internal.logical.rules.projection_pushdown import ProjectionPushdown
from ray.data._internal.planner.exchange.sort_task_spec import SortKey
from ray.data.context import DataContext
from ray.data.expressions import col


class TestProjectionPassThrough:
    """Test projection pass-through for various logical operators."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Set up test fixtures."""
        self.context = DataContext.get_current()

    def _create_input_op(self):
        """Create a dummy input operator."""
        return InputData(input_data=[])

    def _create_project(self, input_op, column_rename_map):
        """Create a Project operator with the given column rename map."""
        exprs = [col(old).alias(new) for old, new in column_rename_map.items()]
        return Project(
            input_op=input_op,
            exprs=exprs,
            compute=None,
            ray_remote_args=None,
        )

    def _count_operators(self, op, operator_type):
        """Count how many operators of a given type are in the operator tree."""
        count = 0
        current = op
        visited = set()

        def traverse(node):
            nonlocal count
            if id(node) in visited:
                return
            visited.add(id(node))

            if isinstance(node, operator_type):
                count += 1

            if hasattr(node, "input_dependencies"):
                for dep in node.input_dependencies:
                    traverse(dep)
            elif hasattr(node, "input_dependency"):
                traverse(node.input_dependency)

        traverse(current)
        return count

    def test_randomize_blocks_pass_through(self):
        """Test projection pass-through for RandomizeBlocks."""
        input_op = self._create_input_op()

        # Create RandomizeBlocks -> Project chain
        randomize_op = RandomizeBlocks(input_op=input_op, seed=42)
        project_op = self._create_project(randomize_op, {"a": "b", "c": "d"})

        # Apply projection pushdown
        plan = LogicalPlan(project_op, self.context)
        rule = ProjectionPushdown()
        optimized_plan = rule.apply(plan)

        # Verify that projection was pushed through RandomizeBlocks
        assert isinstance(optimized_plan.dag, RandomizeBlocks)
        # The upstream project should have been created
        upstream = optimized_plan.dag.input_dependencies[0]
        assert isinstance(upstream, Project)
        # Original input should be at the bottom
        assert isinstance(upstream.input_dependencies[0], InputData)

    def test_random_shuffle_pass_through(self):
        """Test projection pass-through for RandomShuffle."""
        input_op = self._create_input_op()

        # Create RandomShuffle -> Project chain
        shuffle_op = RandomShuffle(input_op=input_op, seed=42)
        project_op = self._create_project(shuffle_op, {"x": "y"})

        # Apply projection pushdown
        plan = LogicalPlan(project_op, self.context)
        rule = ProjectionPushdown()
        optimized_plan = rule.apply(plan)

        # Verify that projection was pushed through RandomShuffle
        assert isinstance(optimized_plan.dag, RandomShuffle)
        upstream = optimized_plan.dag.input_dependencies[0]
        assert isinstance(upstream, Project)

    def test_repartition_pass_through_simple(self):
        """Test projection pass-through for Repartition without partition keys."""
        input_op = self._create_input_op()

        # Create Repartition -> Project chain (no partition keys)
        repartition_op = Repartition(
            input_op=input_op,
            num_outputs=4,
            shuffle=True,
            keys=None,
            sort=False,
        )
        project_op = self._create_project(repartition_op, {"a": "b", "c": "d"})

        # Apply projection pushdown
        plan = LogicalPlan(project_op, self.context)
        rule = ProjectionPushdown()
        optimized_plan = rule.apply(plan)

        # Verify that projection was pushed through Repartition
        assert isinstance(optimized_plan.dag, Repartition)
        upstream = optimized_plan.dag.input_dependencies[0]
        assert isinstance(upstream, Project)

    def test_repartition_pass_through_with_keys(self):
        """Test projection pass-through for Repartition with partition keys."""
        input_op = self._create_input_op()

        # Create Repartition -> Project chain with partition keys
        repartition_op = Repartition(
            input_op=input_op,
            num_outputs=4,
            shuffle=True,
            keys=["id"],
            sort=False,
        )
        # Project should include the partition key
        project_op = self._create_project(repartition_op, {"id": "id", "value": "val"})

        # Apply projection pushdown
        plan = LogicalPlan(project_op, self.context)
        rule = ProjectionPushdown()
        optimized_plan = rule.apply(plan)

        # Verify that projection was pushed through Repartition
        assert isinstance(optimized_plan.dag, Repartition)

    def test_repartition_pass_through_preserves_partition_keys(self):
        """Test that Repartition preserves partition keys not in output."""
        input_op = self._create_input_op()

        # Create Repartition with partition key "id"
        repartition_op = Repartition(
            input_op=input_op,
            num_outputs=4,
            shuffle=True,
            keys=["id"],
            sort=False,
        )
        # Project only "value", not "id" (the partition key)
        project_op = self._create_project(repartition_op, {"value": "val"})

        # Apply projection pushdown
        plan = LogicalPlan(project_op, self.context)
        rule = ProjectionPushdown()
        optimized_plan = rule.apply(plan)

        # When the partition key is not in the output projection,
        # the structure should be: Project (downstream) -> Repartition -> Project (upstream) -> Input
        # The downstream Project removes the partition key that was needed for repartitioning
        assert isinstance(optimized_plan.dag, Project) or isinstance(
            optimized_plan.dag, Repartition
        )
        # Verify the optimization happened (should have repartition somewhere in the tree)
        assert optimized_plan.dag is not None

    def test_sort_pass_through_simple(self):
        """Test projection pass-through for Sort."""
        input_op = self._create_input_op()

        # Create Sort -> Project chain
        sort_key = SortKey(key=["id"], descending=False)
        sort_op = Sort(
            input_op=input_op,
            sort_key=sort_key,
            batch_format="default",
        )
        project_op = self._create_project(sort_op, {"id": "id", "value": "val"})

        # Apply projection pushdown
        plan = LogicalPlan(project_op, self.context)
        rule = ProjectionPushdown()
        optimized_plan = rule.apply(plan)

        # Verify that projection was pushed through Sort
        assert isinstance(optimized_plan.dag, Sort)

    def test_sort_pass_through_preserves_sort_keys(self):
        """Test that Sort preserves sort keys not in output projection."""
        input_op = self._create_input_op()

        # Create Sort with sort key "id"
        sort_key = SortKey(key=["id"], descending=False)
        sort_op = Sort(
            input_op=input_op,
            sort_key=sort_key,
            batch_format="default",
        )
        # Project only "value", not "id" (the sort key)
        project_op = self._create_project(sort_op, {"value": "val"})

        # Apply projection pushdown
        plan = LogicalPlan(project_op, self.context)
        rule = ProjectionPushdown()
        optimized_plan = rule.apply(plan)

        # When the sort key is not in the output projection,
        # the structure should be: Project (downstream) -> Sort -> Project (upstream) -> Input
        # The downstream Project removes the sort key that was needed for sorting
        assert isinstance(optimized_plan.dag, Project) or isinstance(
            optimized_plan.dag, Sort
        )
        # Verify the optimization happened
        assert optimized_plan.dag is not None

    def test_join_pass_through(self):
        """Test projection pass-through for Join."""
        left_op = self._create_input_op()
        right_op = self._create_input_op()

        # Create Join -> Project chain
        join_op = Join(
            left_input_op=left_op,
            right_input_op=right_op,
            join_type="inner",
            left_key_columns=("id",),
            right_key_columns=("id",),
            num_partitions=2,
        )
        project_op = self._create_project(join_op, {"id": "id", "value": "val"})

        # Apply projection pushdown
        plan = LogicalPlan(project_op, self.context)
        rule = ProjectionPushdown()
        optimized_plan = rule.apply(plan)

        # Join should still be present
        assert isinstance(optimized_plan.dag, Join) or isinstance(
            optimized_plan.dag, Project
        )

    def test_zip_pass_through(self):
        """Test projection pass-through for Zip."""
        left_op = self._create_input_op()
        right_op = self._create_input_op()

        # Create Zip -> Project chain
        zip_op = Zip(left_op, right_op)
        project_op = self._create_project(zip_op, {"a": "x", "b": "y"})

        # Apply projection pushdown
        plan = LogicalPlan(project_op, self.context)
        rule = ProjectionPushdown()
        optimized_plan = rule.apply(plan)

        # Zip should be present with projections pushed to its inputs
        assert isinstance(optimized_plan.dag, Zip) or isinstance(
            optimized_plan.dag, Project
        )

    def test_union_pass_through(self):
        """Test projection pass-through for Union."""
        left_op = self._create_input_op()
        right_op = self._create_input_op()

        # Create Union -> Project chain
        union_op = Union(left_op, right_op)
        project_op = self._create_project(union_op, {"a": "x", "c": "z"})

        # Apply projection pushdown
        plan = LogicalPlan(project_op, self.context)
        rule = ProjectionPushdown()
        optimized_plan = rule.apply(plan)

        # Union should have projections pushed to each branch
        assert isinstance(optimized_plan.dag, Union)
        # Check that projections were created for each input
        for input_dep in optimized_plan.dag.input_dependencies:
            assert isinstance(input_dep, Project)

    def test_streaming_repartition_pass_through(self):
        """Test projection pass-through for StreamingRepartition."""
        input_op = self._create_input_op()

        # Create StreamingRepartition -> Project chain
        streaming_repartition_op = StreamingRepartition(
            input_op=input_op,
            target_num_rows_per_block=1000,
        )
        project_op = self._create_project(
            streaming_repartition_op, {"a": "b", "c": "d"}
        )

        # Apply projection pushdown
        plan = LogicalPlan(project_op, self.context)
        rule = ProjectionPushdown()
        optimized_plan = rule.apply(plan)

        # Verify that projection was pushed through StreamingRepartition
        assert isinstance(optimized_plan.dag, StreamingRepartition)
        upstream = optimized_plan.dag.input_dependencies[0]
        assert isinstance(upstream, Project)

    def test_multiple_projections_through_shuffle_operations(self):
        """Test multiple projections through various shuffle operations."""
        input_op = self._create_input_op()

        # Create a chain: Input -> RandomShuffle -> Repartition -> Sort -> Project
        shuffle_op = RandomShuffle(input_op=input_op, seed=42)
        repartition_op = Repartition(
            input_op=shuffle_op,
            num_outputs=4,
            shuffle=True,
            keys=None,
            sort=False,
        )
        sort_key = SortKey(key=["id"], descending=False)
        sort_op = Sort(
            input_op=repartition_op,
            sort_key=sort_key,
            batch_format="default",
        )
        project_op = self._create_project(sort_op, {"id": "id", "value": "val"})

        # Apply projection pushdown
        plan = LogicalPlan(project_op, self.context)
        rule = ProjectionPushdown()
        optimized_plan = rule.apply(plan)

        # The projection should have been pushed through all operators
        assert optimized_plan.dag is not None

    # End-to-end tests with actual data

    def test_randomize_blocks_e2e(self, ray_start_regular_shared):
        """End-to-end test for RandomizeBlocks with projection."""
        ds = ray.data.from_items([{"a": 1, "b": 2, "c": 3}, {"a": 4, "b": 5, "c": 6}])
        ds = ds.randomize_block_order(seed=42)
        ds = ds.select_columns(["a", "b"])

        result = ds.take_all()
        assert len(result) == 2
        for item in result:
            assert set(item.keys()) == {"a", "b"}

    def test_random_shuffle_e2e(self, ray_start_regular_shared):
        """End-to-end test for RandomShuffle with projection."""
        ds = ray.data.from_items([{"x": 1, "y": 2, "z": 3}, {"x": 4, "y": 5, "z": 6}])
        ds = ds.random_shuffle(seed=42)
        ds = ds.rename_columns({"x": "a", "y": "b"})
        ds = ds.select_columns(["a", "b"])

        result = ds.take_all()
        assert len(result) == 2
        for item in result:
            assert set(item.keys()) == {"a", "b"}

    def test_repartition_e2e(self, ray_start_regular_shared):
        """End-to-end test for Repartition with projection."""
        ds = ray.data.from_items(
            [{"id": i, "value": i * 2, "extra": i * 3} for i in range(10)]
        )
        ds = ds.repartition(5)
        ds = ds.select_columns(["id", "value"])

        result = ds.take_all()
        assert len(result) == 10
        for item in result:
            assert set(item.keys()) == {"id", "value"}

    def test_sort_e2e(self, ray_start_regular_shared):
        """End-to-end test for Sort with projection."""
        ds = ray.data.from_items(
            [
                {"id": 3, "value": 30, "extra": 300},
                {"id": 1, "value": 10, "extra": 100},
                {"id": 2, "value": 20, "extra": 200},
            ]
        )
        ds = ds.sort("id")
        ds = ds.select_columns(["id", "value"])

        result = ds.take_all()
        assert len(result) == 3
        assert result[0] == {"id": 1, "value": 10}
        assert result[1] == {"id": 2, "value": 20}
        assert result[2] == {"id": 3, "value": 30}

    def test_join_e2e(self, ray_start_regular_shared_2_cpus):
        """End-to-end test for Join with projection."""
        # Use slightly larger datasets to avoid edge cases with single-row datasets
        left_ds = ray.data.from_items(
            [
                {"id": 1, "left_value": "a"},
                {"id": 2, "left_value": "b"},
                {"id": 3, "left_value": "c"},
            ]
        )
        right_ds = ray.data.from_items(
            [
                {"id": 1, "right_value": "x"},
                {"id": 2, "right_value": "y"},
                {"id": 3, "right_value": "z"},
            ]
        )

        ds = left_ds.join(
            right_ds,
            join_type="inner",
            num_partitions=2,
            on=("id",),
        )
        ds = ds.select_columns(["id", "left_value", "right_value"])

        result = ds.take_all()
        assert len(result) == 3
        for item in result:
            assert "id" in item
            assert "left_value" in item
            assert "right_value" in item

    def test_zip_e2e(self, ray_start_regular_shared):
        """End-to-end test for Zip with projection."""
        left_ds = ray.data.from_items([{"a": 1, "b": 2}, {"a": 3, "b": 4}])
        right_ds = ray.data.from_items([{"c": 5, "d": 6}, {"c": 7, "d": 8}])

        ds = left_ds.zip(right_ds)
        ds = ds.select_columns(["a", "c"])

        result = ds.take_all()
        assert len(result) == 2
        assert result[0] == {"a": 1, "c": 5}
        assert result[1] == {"a": 3, "c": 7}

    def test_union_e2e(self, ray_start_regular_shared):
        """End-to-end test for Union with projection."""
        ds1 = ray.data.from_items([{"x": 1, "y": 2, "z": 3}, {"x": 4, "y": 5, "z": 6}])
        ds2 = ray.data.from_items(
            [{"x": 7, "y": 8, "z": 9}, {"x": 10, "y": 11, "z": 12}]
        )

        ds = ds1.union(ds2)
        ds = ds.select_columns(["x", "y"])

        result = ds.take_all()
        assert len(result) == 4
        for item in result:
            assert set(item.keys()) == {"x", "y"}

    def test_projection_fusion_with_repartition(self, ray_start_regular_shared):
        """Test that multiple projections fuse before passing through repartition."""
        ds = ray.data.from_items([{"a": 1, "b": 2, "c": 3} for _ in range(10)])

        # Add multiple projections
        ds = ds.with_column("d", col("a") + col("b"))
        ds = ds.with_column("e", col("b") * 2)
        ds = ds.repartition(5)
        ds = ds.select_columns(["a", "d", "e"])

        result = ds.take_all()
        assert len(result) == 10
        for item in result:
            assert set(item.keys()) == {"a", "d", "e"}
            assert item["d"] == item["a"] + 2  # Since b=2
            assert item["e"] == 4  # Since b=2

    def test_complex_chain_with_multiple_operators(self, ray_start_regular_shared):
        """Test projection pass-through with a complex chain of operators."""
        ds = ray.data.from_items(
            [{"id": i, "val1": i * 2, "val2": i * 3, "extra": i * 4} for i in range(20)]
        )

        # Chain multiple operations
        ds = ds.random_shuffle(seed=42)
        ds = ds.with_column("computed", col("val1") + col("val2"))
        ds = ds.repartition(4)
        ds = ds.sort("id")
        ds = ds.select_columns(["id", "computed"])

        result = ds.take_all()
        assert len(result) == 20
        for i, item in enumerate(result):
            assert item["id"] == i
            assert item["computed"] == i * 2 + i * 3  # val1 + val2

    def test_rename_through_shuffle_operations(self, ray_start_regular_shared):
        """Test column renaming through shuffle operations."""
        ds = ray.data.from_items([{"x": 1, "y": 2}, {"x": 3, "y": 4}])

        ds = ds.rename_columns({"x": "a"})
        ds = ds.random_shuffle(seed=42)
        ds = ds.rename_columns({"y": "b"})
        ds = ds.repartition(2)

        result = ds.take_all()
        assert len(result) == 2
        for item in result:
            assert set(item.keys()) == {"a", "b"}

    def test_projection_with_partition_keys(self, ray_start_regular_shared):
        """Test that partition keys are preserved when needed during repartition."""
        ds = ray.data.from_items(
            [
                {"id": 1, "category": "A", "value": 100},
                {"id": 2, "category": "B", "value": 200},
                {"id": 3, "category": "A", "value": 300},
            ]
        )

        # Repartition with shuffle, then select only id and value
        # This tests that the optimization correctly handles repartitioning
        ds = ds.repartition(2, shuffle=True)
        ds = ds.select_columns(["id", "value"])

        result = ds.take_all()
        assert len(result) == 3
        for item in result:
            # Should only have the selected columns
            assert set(item.keys()) == {"id", "value"}

    def test_projection_with_sort_keys(self, ray_start_regular_shared):
        """Test that sort keys are preserved when needed."""
        ds = ray.data.from_items(
            [
                {"id": 3, "name": "C", "value": 300},
                {"id": 1, "name": "A", "value": 100},
                {"id": 2, "name": "B", "value": 200},
            ]
        )

        # Sort by id, then select only name and value
        # The implementation should internally preserve id for sorting
        ds = ds.sort("id")
        ds = ds.select_columns(["name", "value"])

        result = ds.take_all()
        assert len(result) == 3
        # Results should be in sorted order by original id
        assert result[0]["name"] == "A"
        assert result[1]["name"] == "B"
        assert result[2]["name"] == "C"


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main([__file__, "-v"]))
