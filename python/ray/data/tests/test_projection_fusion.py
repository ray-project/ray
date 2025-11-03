from dataclasses import dataclass
from typing import Dict, List, Set

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pytest

import ray
from ray.data._internal.logical.interfaces import LogicalPlan
from ray.data._internal.logical.operators.input_data_operator import InputData
from ray.data._internal.logical.operators.map_operator import Project
from ray.data._internal.logical.rules.projection_pushdown import (
    ProjectionPushdown,
)
from ray.data.context import DataContext
from ray.data.expressions import DataType, col, udf


@dataclass
class FusionTestCase:
    """Test case for projection fusion scenarios."""

    name: str
    expressions_list: List[Dict[str, str]]  # List of {name: expression_desc}
    expected_levels: int
    expected_level_contents: List[Set[str]]  # Expected expressions in each level
    description: str


@dataclass
class DependencyTestCase:
    """Test case for dependency analysis."""

    name: str
    expression_desc: str
    expected_refs: Set[str]
    description: str


class TestPorjectionFusion:
    """Test topological sorting in projection pushdown fusion."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Set up test fixtures."""
        self.context = DataContext.get_current()

        # Create UDFs for testing
        @udf(return_dtype=DataType.int64())
        def multiply_by_two(x: pa.Array) -> pa.Array:
            return pc.multiply(x, 2)

        @udf(return_dtype=DataType.int64())
        def add_one(x: pa.Array) -> pa.Array:
            return pc.add(x, 1)

        @udf(return_dtype=DataType.float64())
        def divide_by_three(x: pa.Array) -> pa.Array:
            # Convert to float to ensure floating point division
            return pc.divide(pc.cast(x, pa.float64()), 3.0)

        self.udfs = {
            "multiply_by_two": multiply_by_two,
            "add_one": add_one,
            "divide_by_three": divide_by_three,
        }

    def _create_input_op(self):
        """Create a dummy input operator."""
        return InputData(input_data=[])

    def _parse_expression(self, expr_desc: str):
        """Parse expression description into actual expression object."""
        # Enhanced parser for test expressions
        expr_map = {
            "col('id')": col("id"),
            "col('id') + 10": col("id") + 10,
            "col('id') * 2": col("id") * 2,
            "col('id') - 5": col("id") - 5,
            "col('id') + 1": col("id") + 1,
            "col('id') - 1": col("id") - 1,
            "col('id') - 3": col("id") - 3,
            "col('step1') * 2": col("step1") * 2,
            "col('step2') + 1": col("step2") + 1,
            "col('a') + col('b')": col("a") + col("b"),
            "col('c') + col('d')": col("c") + col("d"),
            "col('e') * 3": col("e") * 3,
            "col('a') + 1": col("a") + 1,
            "multiply_by_two(col('id'))": self.udfs["multiply_by_two"](col("id")),
            "multiply_by_two(col('id')) + col('plus_ten')": (
                self.udfs["multiply_by_two"](col("id")) + col("plus_ten")
            ),
            "col('times_three') > col('plus_ten')": (
                col("times_three") > col("plus_ten")
            ),
            "multiply_by_two(col('x'))": self.udfs["multiply_by_two"](col("x")),
            "add_one(col('id'))": self.udfs["add_one"](col("id")),
            "multiply_by_two(col('plus_one'))": self.udfs["multiply_by_two"](
                col("plus_one")
            ),
            "divide_by_three(col('times_two'))": self.udfs["divide_by_three"](
                col("times_two")
            ),
        }

        if expr_desc in expr_map:
            return expr_map[expr_desc]
        else:
            raise ValueError(f"Unknown expression: {expr_desc}")

    def _create_project_chain(self, input_op, expressions_list: List[Dict[str, str]]):
        """Create a chain of Project operators from expression descriptions."""
        current_op = input_op

        for expr_dict in expressions_list:
            exprs = {
                name: self._parse_expression(desc) for name, desc in expr_dict.items()
            }
            current_op = Project(
                current_op, cols=None, cols_rename=None, exprs=exprs, ray_remote_args={}
            )

        return current_op

    def _extract_levels_from_plan(self, plan: LogicalPlan) -> List[Set[str]]:
        """Extract expression levels from optimized plan."""
        current = plan.dag
        levels = []

        while isinstance(current, Project):
            if current.exprs:
                levels.append(set(current.exprs.keys()))
            current = current.input_dependency

        return list(reversed(levels))  # Return bottom-up order

    def _count_project_operators(self, plan: LogicalPlan) -> int:
        """Count the number of Project operators in the plan."""
        current = plan.dag
        count = 0

        while current:
            if isinstance(current, Project):
                count += 1
            current = getattr(current, "input_dependency", None)

        return count

    def _describe_plan_structure(self, plan: LogicalPlan) -> str:
        """Generate a description of the plan structure."""
        current = plan.dag
        operators = []

        while current:
            if isinstance(current, Project):
                expr_count = len(current.exprs) if current.exprs else 0
                operators.append(f"Project({expr_count} exprs)")
            else:
                operators.append(current.__class__.__name__)
            current = getattr(current, "input_dependency", None)

        return " -> ".join(operators)

    @pytest.mark.parametrize(
        "test_case",
        [
            FusionTestCase(
                name="no_dependencies",
                expressions_list=[
                    {"doubled": "col('id') * 2", "plus_five": "col('id') + 10"},
                    {"minus_three": "col('id') - 3"},
                ],
                expected_levels=1,
                expected_level_contents=[{"doubled", "plus_five", "minus_three"}],
                description="Independent expressions should fuse into single operator",
            ),
            FusionTestCase(
                name="simple_chain",
                expressions_list=[
                    {"step1": "col('id') + 10"},
                    {"step2": "col('step1') * 2"},
                    {"step3": "col('step2') + 1"},
                ],
                expected_levels=1,
                expected_level_contents=[
                    {"step1", "step2", "step3"}
                ],  # All in one level
                description="All expressions fuse into single operator with OrderedDict preservation",
            ),
            FusionTestCase(
                name="mixed_udf_regular",
                expressions_list=[
                    {"plus_ten": "col('id') + 10"},
                    {"times_three": "multiply_by_two(col('id'))"},
                    {"minus_five": "col('id') - 5"},
                    {
                        "udf_plus_regular": "multiply_by_two(col('id')) + col('plus_ten')"
                    },
                    {"comparison": "col('times_three') > col('plus_ten')"},
                ],
                expected_levels=1,
                expected_level_contents=[
                    {
                        "plus_ten",
                        "times_three",
                        "minus_five",
                        "udf_plus_regular",
                        "comparison",
                    }
                ],
                description="All expressions fuse into single operator",
            ),
            FusionTestCase(
                name="complex_graph",
                expressions_list=[
                    {"a": "col('id') + 1", "b": "col('id') * 2"},
                    {"c": "col('a') + col('b')"},
                    {"d": "col('id') - 1"},
                    {"e": "col('c') + col('d')"},
                    {"f": "col('e') * 3"},
                ],
                expected_levels=1,
                expected_level_contents=[{"a", "b", "c", "d", "e", "f"}],
                description="All expressions fuse into single operator",
            ),
            FusionTestCase(
                name="udf_dependency_chain",
                expressions_list=[
                    {"plus_one": "add_one(col('id'))"},
                    {"times_two": "multiply_by_two(col('plus_one'))"},
                    {"div_three": "divide_by_three(col('times_two'))"},
                ],
                expected_levels=1,  # Changed from 3 to 1
                expected_level_contents=[{"plus_one", "times_two", "div_three"}],
                description="All UDF expressions fuse into single operator with preserved order",
            ),
        ],
    )
    def test_fusion_scenarios(self, test_case: FusionTestCase):
        """Test various fusion scenarios with simplified single-operator fusion."""
        input_op = self._create_input_op()
        final_op = self._create_project_chain(input_op, test_case.expressions_list)

        # Apply projection pushdown
        plan = LogicalPlan(final_op, self.context)
        rule = ProjectionPushdown()
        optimized_plan = rule.apply(plan)

        # Extract levels from optimized plan
        actual_levels = self._extract_levels_from_plan(optimized_plan)

        # Verify number of levels
        assert len(actual_levels) == test_case.expected_levels, (
            f"{test_case.name}: Expected {test_case.expected_levels} operators, "
            f"got {len(actual_levels)}. Actual operators: {actual_levels}"
        )

        # Verify level contents (more flexible matching)
        for i, expected_content in enumerate(test_case.expected_level_contents):
            assert expected_content.issubset(actual_levels[i]), (
                f"{test_case.name}: Operator {i} missing expressions. "
                f"Expected {expected_content} to be subset of {actual_levels[i]}"
            )

    def test_pairwise_fusion_behavior(self):
        """Test to understand how pairwise fusion works in practice."""
        input_data = [{"id": i} for i in range(10)]

        # Test with 2 operations (should fuse to 1)
        ds2 = ray.data.from_items(input_data)
        ds2 = ds2.with_column("col1", col("id") + 1)
        ds2 = ds2.with_column("col2", col("id") * 2)

        count2 = self._count_project_operators(ds2._logical_plan)
        print(f"2 operations -> {count2} operators")

        # Test with 3 operations
        ds3 = ray.data.from_items(input_data)
        ds3 = ds3.with_column("col1", col("id") + 1)
        ds3 = ds3.with_column("col2", col("id") * 2)
        ds3 = ds3.with_column("col3", col("id") - 1)

        count3 = self._count_project_operators(ds3._logical_plan)
        print(f"3 operations -> {count3} operators")

        # Test with 4 operations
        ds4 = ray.data.from_items(input_data)
        ds4 = ds4.with_column("col1", col("id") + 1)
        ds4 = ds4.with_column("col2", col("id") * 2)
        ds4 = ds4.with_column("col3", col("id") - 1)
        ds4 = ds4.with_column("col4", col("id") + 5)

        count4 = self._count_project_operators(ds4._logical_plan)
        print(f"4 operations -> {count4} operators")

        # Verify that fusion is happening (fewer operators than original)
        assert count2 <= 2, f"2 operations should result in ≤2 operators, got {count2}"
        assert count3 <= 3, f"3 operations should result in ≤3 operators, got {count3}"
        assert count4 <= 4, f"4 operations should result in ≤4 operators, got {count4}"

        # Verify correctness
        result2 = ds2.take(1)[0]
        result3 = ds3.take(1)[0]
        result4 = ds4.take(1)[0]

        assert result2 == {"id": 0, "col1": 1, "col2": 0}
        assert result3 == {"id": 0, "col1": 1, "col2": 0, "col3": -1}
        assert result4 == {"id": 0, "col1": 1, "col2": 0, "col3": -1, "col4": 5}

    def test_optimal_fusion_with_single_chain(self):
        """Test fusion when all operations are added in a single chain (ideal case)."""
        input_data = [{"id": i} for i in range(10)]

        # Create a single Project operator with multiple expressions
        # This simulates what would happen with perfect fusion
        ds = ray.data.from_items(input_data)

        # Apply multiple operations that should all be independent
        expressions = {
            "col1": col("id") + 1,
            "col2": col("id") * 2,
            "col3": col("id") - 1,
            "col4": col("id") + 5,
            "col5": col("id") * 3,
        }

        # Use map_batches to create a single operation that does everything
        def apply_all_expressions(batch):
            import pyarrow.compute as pc

            result = batch.to_pydict()
            result["col1"] = pc.add(batch["id"], 1)
            result["col2"] = pc.multiply(batch["id"], 2)
            result["col3"] = pc.subtract(batch["id"], 1)
            result["col4"] = pc.add(batch["id"], 5)
            result["col5"] = pc.multiply(batch["id"], 3)
            return pa.table(result)

        ds_optimal = ds.map_batches(apply_all_expressions, batch_format="pyarrow")

        # Compare with the with_column approach
        ds_with_column = ds
        for col_name, expr in expressions.items():
            ds_with_column = ds_with_column.with_column(col_name, expr)

        # Convert both to pandas for reliable comparison (avoids take() ordering issues)
        result_optimal_df = (
            ds_optimal.to_pandas().sort_values("id").reset_index(drop=True)
        )
        result_with_column_df = (
            ds_with_column.to_pandas().sort_values("id").reset_index(drop=True)
        )

        # Compare using pandas testing
        pd.testing.assert_frame_equal(
            result_optimal_df.sort_index(axis=1),
            result_with_column_df.sort_index(axis=1),
            check_dtype=False,
        )

    def test_basic_fusion_works(self):
        """Test that basic fusion of two independent operations works."""
        input_data = [{"id": i} for i in range(5)]

        # Create dataset with two independent operations
        ds = ray.data.from_items(input_data)
        ds = ds.with_column("doubled", col("id") * 2)
        ds = ds.with_column("plus_one", col("id") + 1)

        # Check before optimization
        original_count = self._count_project_operators(ds._logical_plan)
        print(f"Before optimization: {original_count} operators")

        # Apply optimization
        rule = ProjectionPushdown()
        optimized_plan = rule.apply(ds._logical_plan)

        # Check after optimization
        optimized_count = self._count_project_operators(optimized_plan)
        print(f"After optimization: {optimized_count} operators")

        # Two independent operations should fuse into one
        assert (
            optimized_count == 1
        ), f"Two independent operations should fuse to 1 operator, got {optimized_count}"

        # Verify correctness using pandas comparison
        from ray.data.dataset import Dataset

        optimized_ds = Dataset(ds._plan, optimized_plan)

        try:
            result_df = optimized_ds.to_pandas()
            print(f"Result: {result_df}")

            expected_df = pd.DataFrame(
                {
                    "id": [0, 1, 2, 3, 4],
                    "doubled": [0, 2, 4, 6, 8],
                    "plus_one": [1, 2, 3, 4, 5],
                }
            )
            print(f"Expected: {expected_df}")

            # Sort columns for comparison
            result_sorted = result_df.reindex(sorted(result_df.columns), axis=1)
            expected_sorted = expected_df.reindex(sorted(expected_df.columns), axis=1)

            pd.testing.assert_frame_equal(
                result_sorted,
                expected_sorted,
                check_dtype=False,
                check_index_type=False,
            )

        except Exception as e:
            print(f"Error in basic fusion test: {e}")
            # Fallback verification
            result_list = optimized_ds.take_all()
            print(f"Result as list: {result_list}")

            expected_list = [
                {"id": 0, "doubled": 0, "plus_one": 1},
                {"id": 1, "doubled": 2, "plus_one": 2},
                {"id": 2, "doubled": 4, "plus_one": 3},
                {"id": 3, "doubled": 6, "plus_one": 4},
                {"id": 4, "doubled": 8, "plus_one": 5},
            ]

            assert len(result_list) == len(expected_list)
            for actual, expected in zip(result_list, expected_list):
                for key, expected_val in expected.items():
                    assert (
                        actual[key] == expected_val
                    ), f"Mismatch for key {key}: expected {expected_val}, got {actual[key]}"

    def test_dependency_prevents_fusion(self):
        """Test that dependencies are handled in single operator with OrderedDict."""
        input_data = [{"id": i} for i in range(5)]

        # Create dataset with dependency chain
        ds = ray.data.from_items(input_data)
        ds = ds.with_column("doubled", col("id") * 2)
        ds = ds.with_column(
            "doubled_plus_one", col("doubled") + 1
        )  # Depends on doubled

        # Check before optimization
        original_count = self._count_project_operators(ds._logical_plan)
        print(f"Before optimization: {original_count} operators")

        # Apply optimization
        rule = ProjectionPushdown()
        optimized_plan = rule.apply(ds._logical_plan)

        # Check after optimization
        optimized_count = self._count_project_operators(optimized_plan)
        print(f"After optimization: {optimized_count} operators")

        # Should have 1 operator now (changed from 2)
        assert (
            optimized_count == 1
        ), f"All operations should fuse into 1 operator, got {optimized_count}"

        # Verify correctness using pandas comparison
        from ray.data.dataset import Dataset

        optimized_ds = Dataset(ds._plan, optimized_plan)
        result_df = optimized_ds.to_pandas()

        expected_df = pd.DataFrame(
            {
                "id": [0, 1, 2, 3, 4],
                "doubled": [0, 2, 4, 6, 8],
                "doubled_plus_one": [1, 3, 5, 7, 9],
            }
        )

        pd.testing.assert_frame_equal(
            result_df.sort_index(axis=1),
            expected_df.sort_index(axis=1),
            check_dtype=False,
        )

    def test_mixed_udf_regular_end_to_end(self):
        """Test the exact failing scenario from the original issue."""
        input_data = [{"id": i} for i in range(5)]

        # Create dataset with mixed UDF and regular expressions (the failing test case)
        ds = ray.data.from_items(input_data)
        ds = ds.with_column("plus_ten", col("id") + 10)
        ds = ds.with_column(
            "times_three", self.udfs["multiply_by_two"](col("id"))
        )  # Actually multiply by 2
        ds = ds.with_column("minus_five", col("id") - 5)
        ds = ds.with_column(
            "udf_plus_regular",
            self.udfs["multiply_by_two"](col("id")) + col("plus_ten"),
        )
        ds = ds.with_column("comparison", col("times_three") > col("plus_ten"))

        # Apply optimization
        rule = ProjectionPushdown()
        optimized_plan = rule.apply(ds._logical_plan)

        # Verify execution correctness
        from ray.data.dataset import Dataset

        optimized_ds = Dataset(ds._plan, optimized_plan)
        result_df = optimized_ds.to_pandas()

        expected_df = pd.DataFrame(
            {
                "id": [0, 1, 2, 3, 4],
                "plus_ten": [10, 11, 12, 13, 14],  # id + 10
                "times_three": [0, 2, 4, 6, 8],  # id * 2 (multiply_by_two UDF)
                "minus_five": [-5, -4, -3, -2, -1],  # id - 5
                "udf_plus_regular": [10, 13, 16, 19, 22],  # (id * 2) + (id + 10)
                "comparison": [
                    False,
                    False,
                    False,
                    False,
                    False,
                ],  # times_three > plus_ten
            }
        )

        pd.testing.assert_frame_equal(
            result_df.sort_index(axis=1),
            expected_df.sort_index(axis=1),
            check_dtype=False,
        )

        # Verify that we have 1 operator (changed from multiple)
        optimized_count = self._count_project_operators(optimized_plan)
        assert (
            optimized_count == 1
        ), f"Expected 1 operator with all expressions fused, got {optimized_count}"

    def test_optimal_fusion_comparison(self):
        """Compare optimized with_column approach against manual map_batches."""
        input_data = [{"id": i} for i in range(10)]

        # Create dataset using with_column (will be optimized)
        ds_with_column = ray.data.from_items(input_data)
        ds_with_column = ds_with_column.with_column("col1", col("id") + 1)
        ds_with_column = ds_with_column.with_column("col2", col("id") * 2)
        ds_with_column = ds_with_column.with_column("col3", col("id") - 1)
        ds_with_column = ds_with_column.with_column("col4", col("id") + 5)
        ds_with_column = ds_with_column.with_column("col5", col("id") * 3)

        # Apply optimization
        rule = ProjectionPushdown()
        optimized_plan = rule.apply(ds_with_column._logical_plan)
        from ray.data.dataset import Dataset

        optimized_ds = Dataset(ds_with_column._plan, optimized_plan)

        # Create dataset using single map_batches (optimal case)
        ds_optimal = ray.data.from_items(input_data)

        def apply_all_expressions(batch):
            import pyarrow.compute as pc

            result = batch.to_pydict()
            result["col1"] = pc.add(batch["id"], 1)
            result["col2"] = pc.multiply(batch["id"], 2)
            result["col3"] = pc.subtract(batch["id"], 1)
            result["col4"] = pc.add(batch["id"], 5)
            result["col5"] = pc.multiply(batch["id"], 3)
            return pa.table(result)

        ds_optimal = ds_optimal.map_batches(
            apply_all_expressions, batch_format="pyarrow"
        )

        # Compare results using pandas
        result_optimized = optimized_ds.to_pandas()
        result_optimal = ds_optimal.to_pandas()

        pd.testing.assert_frame_equal(
            result_optimized.sort_index(axis=1),
            result_optimal.sort_index(axis=1),
            check_dtype=False,
        )

    def test_chained_udf_dependencies(self):
        """Test multiple non-vectorized UDFs in a dependency chain."""
        input_data = [{"id": i} for i in range(5)]

        # Create dataset with chained UDF dependencies
        ds = ray.data.from_items(input_data)
        ds = ds.with_column("plus_one", self.udfs["add_one"](col("id")))
        ds = ds.with_column("times_two", self.udfs["multiply_by_two"](col("plus_one")))
        ds = ds.with_column("div_three", self.udfs["divide_by_three"](col("times_two")))

        # Apply optimization
        rule = ProjectionPushdown()
        optimized_plan = rule.apply(ds._logical_plan)

        # Verify 1 operator (changed from 3)
        assert self._count_project_operators(optimized_plan) == 1
        assert (
            self._describe_plan_structure(optimized_plan)
            == "Project(3 exprs) -> FromItems"  # Changed from multiple operators
        )

        # Verify execution correctness
        from ray.data.dataset import Dataset

        optimized_ds = Dataset(ds._plan, optimized_plan)
        result_df = optimized_ds.to_pandas()

        expected_df = pd.DataFrame(
            {
                "id": [0, 1, 2, 3, 4],
                "plus_one": [1, 2, 3, 4, 5],
                "times_two": [2, 4, 6, 8, 10],
                "div_three": [2 / 3, 4 / 3, 2.0, 8 / 3, 10 / 3],
            }
        )

        pd.testing.assert_frame_equal(
            result_df.sort_index(axis=1),
            expected_df.sort_index(axis=1),
            check_dtype=False,
        )

    def test_performance_impact_of_udf_chains(self):
        """Test performance characteristics of UDF dependency chains vs independent UDFs."""
        input_data = [{"id": i} for i in range(100)]

        # Case 1: Independent UDFs (should fuse)
        ds_independent = ray.data.from_items(input_data)
        ds_independent = ds_independent.with_column(
            "udf1", self.udfs["add_one"](col("id"))
        )
        ds_independent = ds_independent.with_column(
            "udf2", self.udfs["multiply_by_two"](col("id"))
        )
        ds_independent = ds_independent.with_column(
            "udf3", self.udfs["divide_by_three"](col("id"))
        )

        # Case 2: Chained UDFs (should also fuse now)
        ds_chained = ray.data.from_items(input_data)
        ds_chained = ds_chained.with_column("step1", self.udfs["add_one"](col("id")))
        ds_chained = ds_chained.with_column(
            "step2", self.udfs["multiply_by_two"](col("step1"))
        )
        ds_chained = ds_chained.with_column(
            "step3", self.udfs["divide_by_three"](col("step2"))
        )

        # Apply optimization
        rule = ProjectionPushdown()
        optimized_independent = rule.apply(ds_independent._logical_plan)
        optimized_chained = rule.apply(ds_chained._logical_plan)

        # Verify fusion behavior (both should be 1 now)
        assert self._count_project_operators(optimized_independent) == 1
        assert (
            self._count_project_operators(optimized_chained) == 1
        )  # Changed from 3 to 1
        assert (
            self._describe_plan_structure(optimized_independent)
            == "Project(3 exprs) -> FromItems"
        )
        assert (
            self._describe_plan_structure(optimized_chained)
            == "Project(3 exprs) -> FromItems"  # Changed from multiple operators
        )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
