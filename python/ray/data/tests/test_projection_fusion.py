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
from ray.data._internal.logical.optimizers import LogicalOptimizer
from ray.data._internal.logical.rules.projection_pushdown import (
    ProjectionPushdown,
)
from ray.data._internal.util import rows_same
from ray.data.context import DataContext
from ray.data.expressions import DataType, StarExpr, col, star, udf


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


class TestProjectionFusion:
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
            # Convert dictionary to list of named expressions
            exprs = []
            for name, desc in expr_dict.items():
                expr = self._parse_expression(desc)
                named_expr = expr.alias(name)
                exprs.append(named_expr)

            current_op = Project(current_op, exprs=[star()] + exprs, ray_remote_args={})

        return current_op

    def _extract_levels_from_plan(self, plan: LogicalPlan) -> List[Set[str]]:
        """Extract expression levels from optimized plan."""
        current = plan.dag
        levels = []

        while isinstance(current, Project):
            # Extract names, ignoring StarExpr (not a named column)
            levels.append(
                {expr.name for expr in current.exprs if not isinstance(expr, StarExpr)}
            )
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

    def test_pairwise_fusion_behavior(self, ray_start_regular_shared):
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

    def test_optimal_fusion_with_single_chain(self, ray_start_regular_shared):
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

        # Convert both to pandas for reliable comparison
        result_optimal_df = ds_optimal.to_pandas()
        result_with_column_df = ds_with_column.to_pandas()

        # Sort columns before comparison
        result_optimal_df = result_optimal_df[sorted(result_optimal_df.columns)]
        result_with_column_df = result_with_column_df[
            sorted(result_with_column_df.columns)
        ]

        # Compare using rows_same (deterministic, ignores order)
        assert rows_same(result_optimal_df, result_with_column_df)

    def test_basic_fusion_works(self, ray_start_regular_shared):
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

        # Verify correctness using rows_same
        from ray.data.dataset import Dataset

        optimized_ds = Dataset(ds._plan, optimized_plan)
        result_df = optimized_ds.to_pandas()

        expected_df = pd.DataFrame(
            {
                "id": [0, 1, 2, 3, 4],
                "doubled": [0, 2, 4, 6, 8],
                "plus_one": [1, 2, 3, 4, 5],
            }
        )

        # Sort columns before comparison
        result_df = result_df[sorted(result_df.columns)]
        expected_df = expected_df[sorted(expected_df.columns)]
        assert rows_same(result_df, expected_df)

    def test_dependency_prevents_fusion(self, ray_start_regular_shared):
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

        # Verify correctness using rows_same
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

        # Sort columns before comparison
        result_df = result_df[sorted(result_df.columns)]
        expected_df = expected_df[sorted(expected_df.columns)]
        assert rows_same(result_df, expected_df)

    def test_mixed_udf_regular_end_to_end(self, ray_start_regular_shared):
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

        # Sort columns before comparison
        result_df = result_df[sorted(result_df.columns)]
        expected_df = expected_df[sorted(expected_df.columns)]
        assert rows_same(result_df, expected_df)

        # Verify that we have 1 operator (changed from multiple)
        optimized_count = self._count_project_operators(optimized_plan)
        assert (
            optimized_count == 1
        ), f"Expected 1 operator with all expressions fused, got {optimized_count}"

    def test_optimal_fusion_comparison(self, ray_start_regular_shared):
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

        # Compare results using rows_same
        result_optimized = optimized_ds.to_pandas()
        result_optimal = ds_optimal.to_pandas()

        # Sort columns before comparison
        result_optimized = result_optimized[sorted(result_optimized.columns)]
        result_optimal = result_optimal[sorted(result_optimal.columns)]
        assert rows_same(result_optimized, result_optimal)

    def test_chained_udf_dependencies(self, ray_start_regular_shared):
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
            == "Project(4 exprs) -> FromItems"  # Changed from multiple operators
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

        # Sort columns before comparison
        result_df = result_df[sorted(result_df.columns)]
        expected_df = expected_df[sorted(expected_df.columns)]
        assert rows_same(result_df, expected_df)

    def test_performance_impact_of_udf_chains(self, ray_start_regular_shared):
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
            == "Project(4 exprs) -> FromItems"
        )
        assert (
            self._describe_plan_structure(optimized_chained)
            == "Project(4 exprs) -> FromItems"  # Changed from multiple operators
        )

    @pytest.mark.parametrize(
        "operations,expected",
        [
            # Single operations
            ([("rename", {"a": "A"})], {"A": 1, "b": 2, "c": 3}),
            ([("select", ["a", "b"])], {"a": 1, "b": 2}),
            ([("with_column", "d", 4)], {"a": 1, "b": 2, "c": 3, "d": 4}),
            # Two operations - rename then select
            ([("rename", {"a": "A"}), ("select", ["A"])], {"A": 1}),
            ([("rename", {"a": "A"}), ("select", ["b"])], {"b": 2}),
            (
                [("rename", {"a": "A", "b": "B"}), ("select", ["A", "B"])],
                {"A": 1, "B": 2},
            ),
            # Two operations - select then rename
            ([("select", ["a", "b"]), ("rename", {"a": "A"})], {"A": 1, "b": 2}),
            ([("select", ["a"]), ("rename", {"a": "x"})], {"x": 1}),
            # Two operations - with_column combinations
            ([("with_column", "d", 4), ("select", ["a", "d"])], {"a": 1, "d": 4}),
            ([("select", ["a"]), ("with_column", "d", 4)], {"a": 1, "d": 4}),
            (
                [("rename", {"a": "A"}), ("with_column", "d", 4)],
                {"A": 1, "b": 2, "c": 3, "d": 4},
            ),
            (
                [("with_column", "d", 4), ("rename", {"d": "D"})],
                {"a": 1, "b": 2, "c": 3, "D": 4},
            ),
            # Three operations
            (
                [
                    ("rename", {"a": "A"}),
                    ("select", ["A", "b"]),
                    ("with_column", "d", 4),
                ],
                {"A": 1, "b": 2, "d": 4},
            ),
            (
                [
                    ("with_column", "d", 4),
                    ("rename", {"a": "A"}),
                    ("select", ["A", "d"]),
                ],
                {"A": 1, "d": 4},
            ),
            (
                [
                    ("select", ["a", "b"]),
                    ("rename", {"a": "x"}),
                    ("with_column", "d", 4),
                ],
                {"x": 1, "b": 2, "d": 4},
            ),
            # Column swap (no actual changes)
            ([("rename", {"a": "b", "b": "a"}), ("select", ["a"])], {"a": 2}),
            ([("rename", {"a": "b", "b": "a"}), ("select", ["b"])], {"b": 1}),
            # Multiple same operations
            (
                [("rename", {"a": "x"}), ("rename", {"x": "y"})],
                {"y": 1, "b": 2, "c": 3},
            ),
            ([("select", ["a", "b"]), ("select", ["a"])], {"a": 1}),
            (
                [("with_column", "d", 4), ("with_column", "e", 5)],
                {"a": 1, "b": 2, "c": 3, "d": 4, "e": 5},
            ),
            # Complex expressions with with_column
            (
                [("rename", {"a": "x"}), ("with_column_expr", "sum", "x", 10)],
                {"x": 1, "b": 2, "c": 3, "sum": 10},
            ),
            (
                [
                    ("with_column", "d", 4),
                    ("with_column", "e", 5),
                    ("select", ["d", "e"]),
                ],
                {"d": 4, "e": 5},
            ),
        ],
    )
    def test_projection_operations_comprehensive(
        self, ray_start_regular_shared, operations, expected
    ):
        """Comprehensive test for projection operations combinations."""
        from ray.data.expressions import col, lit

        # Create initial dataset
        ds = ray.data.range(1).map(lambda row: {"a": 1, "b": 2, "c": 3})

        # Apply operations
        for op in operations:
            if op[0] == "rename":
                ds = ds.rename_columns(op[1])
            elif op[0] == "select":
                ds = ds.select_columns(op[1])
            elif op[0] == "with_column":
                ds = ds.with_column(op[1], lit(op[2]))
            elif op[0] == "with_column_expr":
                # Special case for expressions referencing columns
                ds = ds.with_column(op[1], col(op[2]) * op[3])

        # Verify result using rows_same
        result_df = ds.to_pandas()
        expected_df = pd.DataFrame([expected])
        # Ensure columns are in the same order for comparison
        result_df = result_df[sorted(result_df.columns)]
        expected_df = expected_df[sorted(expected_df.columns)]
        assert rows_same(result_df, expected_df)

    @pytest.mark.parametrize(
        "operations,expected",
        [
            # Basic count operations
            ([("count",)], 3),  # All 3 rows
            ([("rename", {"a": "A"}), ("count",)], 3),
            ([("select", ["a", "b"]), ("count",)], 3),
            ([("with_column", "d", 4), ("count",)], 3),
            # Filter operations affecting count
            ([("filter", col("a") > 1), ("count",)], 2),  # 2 rows have a > 1
            ([("filter", col("b") == 2), ("count",)], 3),  # All rows have b == 2
            ([("filter", col("c") < 10), ("count",)], 3),  # All rows have c < 10
            ([("filter", col("a") == 1), ("count",)], 1),  # 1 row has a == 1
            # Projection then filter then count
            ([("rename", {"a": "A"}), ("filter", col("A") > 1), ("count",)], 2),
            ([("select", ["a", "b"]), ("filter", col("a") > 1), ("count",)], 2),
            ([("with_column", "d", 4), ("filter", col("d") == 4), ("count",)], 3),
            # Filter then projection then count
            ([("filter", col("a") > 1), ("rename", {"a": "A"}), ("count",)], 2),
            ([("filter", col("b") == 2), ("select", ["a", "b"]), ("count",)], 3),
            ([("filter", col("c") < 10), ("with_column", "d", 4), ("count",)], 3),
            # Multiple projections with filter and count
            (
                [
                    ("rename", {"a": "A"}),
                    ("select", ["A", "b"]),
                    ("filter", col("A") > 1),
                    ("count",),
                ],
                2,
            ),
            (
                [
                    ("with_column", "d", 4),
                    ("rename", {"d": "D"}),
                    ("filter", col("D") == 4),
                    ("count",),
                ],
                3,
            ),
            (
                [
                    ("select", ["a", "b"]),
                    ("filter", col("a") > 1),
                    ("rename", {"a": "x"}),
                    ("count",),
                ],
                2,
            ),
            # Complex combinations
            (
                [
                    ("filter", col("a") > 0),
                    ("rename", {"b": "B"}),
                    ("select", ["a", "B"]),
                    ("filter", col("B") == 2),
                    ("count",),
                ],
                3,
            ),
            (
                [
                    ("with_column", "sum", 99),
                    ("filter", col("a") > 1),
                    ("select", ["a", "sum"]),
                    ("count",),
                ],
                2,
            ),
            (
                [
                    ("rename", {"a": "A", "b": "B"}),
                    ("filter", (col("A") + col("B")) > 3),
                    ("select", ["A"]),
                    ("count",),
                ],
                2,
            ),
        ],
    )
    def test_projection_fusion_with_count_and_filter(
        self, ray_start_regular_shared, operations, expected
    ):
        """Test projection fusion with count operations including filters."""
        from ray.data.expressions import lit

        # Create dataset with 3 rows: {"a": 1, "b": 2, "c": 3}, {"a": 2, "b": 2, "c": 3}, {"a": 3, "b": 2, "c": 3}
        ds = ray.data.from_items(
            [
                {"a": 1, "b": 2, "c": 3},
                {"a": 2, "b": 2, "c": 3},
                {"a": 3, "b": 2, "c": 3},
            ]
        )

        # Apply operations
        for op in operations:
            if op[0] == "rename":
                ds = ds.rename_columns(op[1])
            elif op[0] == "select":
                ds = ds.select_columns(op[1])
            elif op[0] == "with_column":
                ds = ds.with_column(op[1], lit(op[2]))
            elif op[0] == "filter":
                # Use the predicate expression directly
                ds = ds.filter(expr=op[1])
            elif op[0] == "count":
                # Count returns a scalar, not a dataset
                result = ds.count()
                assert result == expected
                return  # Early return since count() terminates the pipeline

        # This should not be reached for count operations
        assert False, "Count operation should have returned early"

    @pytest.mark.parametrize(
        "invalid_operations,error_type,error_message_contains",
        [
            # Try to filter on a column that doesn't exist yet
            (
                [("filter", col("d") > 0), ("with_column", "d", 4)],
                (KeyError, ray.exceptions.RayTaskError),
                "d",
            ),
            # Try to filter on a renamed column before the rename
            (
                [("filter", col("A") > 1), ("rename", {"a": "A"})],
                (KeyError, ray.exceptions.RayTaskError),
                "A",
            ),
            # Try to use a column that was removed by select
            (
                [("select", ["a"]), ("filter", col("b") == 2)],
                (KeyError, ray.exceptions.RayTaskError),
                "b",
            ),
            # Try to filter on a column after it was removed by select
            (
                [("select", ["a", "b"]), ("filter", col("c") < 10)],
                (KeyError, ray.exceptions.RayTaskError),
                "c",
            ),
            # Try to use with_column referencing a non-existent column
            (
                [("select", ["a"]), ("with_column", "new_col", col("b") + 1)],
                (KeyError, ray.exceptions.RayTaskError),
                "b",
            ),
            # Try to filter on a column that was renamed away
            (
                [("rename", {"b": "B"}), ("filter", col("b") == 2)],
                (KeyError, ray.exceptions.RayTaskError),
                "b",
            ),
            # Try to use with_column with old column name after rename
            (
                [("rename", {"a": "A"}), ("with_column", "result", col("a") + 1)],
                (KeyError, ray.exceptions.RayTaskError),
                "a",
            ),
            # Try to select using old column name after rename
            (
                [("rename", {"b": "B"}), ("select", ["a", "b", "c"])],
                (KeyError, ray.exceptions.RayTaskError),
                "b",
            ),
            # Try to filter on a computed column that was removed by select
            (
                [
                    ("with_column", "d", 4),
                    ("select", ["a", "b"]),
                    ("filter", col("d") == 4),
                ],
                (KeyError, ray.exceptions.RayTaskError),
                "d",
            ),
            # Try to rename a column that was removed by select
            (
                [("select", ["a", "b"]), ("rename", {"c": "C"})],
                (KeyError, ray.exceptions.RayTaskError),
                "c",
            ),
            # Complex: rename, select (removing renamed source), then use old name
            (
                [
                    ("rename", {"a": "A"}),
                    ("select", ["b", "c"]),
                    ("filter", col("a") > 0),
                ],
                (KeyError, ray.exceptions.RayTaskError),
                "a",
            ),
            # Complex: with_column, select (keeping new column), filter on removed original
            (
                [
                    ("with_column", "sum", col("a") + col("b")),
                    ("select", ["sum"]),
                    ("filter", col("a") > 0),
                ],
                (KeyError, ray.exceptions.RayTaskError),
                "a",
            ),
            # Try to use column in with_column expression after it was removed
            (
                [
                    ("select", ["a", "c"]),
                    ("with_column", "result", col("a") + col("b")),
                ],
                (KeyError, ray.exceptions.RayTaskError),
                "b",
            ),
        ],
    )
    def test_projection_operations_invalid_order(
        self,
        ray_start_regular_shared,
        invalid_operations,
        error_type,
        error_message_contains,
    ):
        """Test that operations fail gracefully when referencing non-existent columns."""
        import ray
        from ray.data.expressions import lit

        # Create dataset with 3 rows: {"a": 1, "b": 2, "c": 3}, {"a": 2, "b": 2, "c": 3}, {"a": 3, "b": 2, "c": 3}
        ds = ray.data.from_items(
            [
                {"a": 1, "b": 2, "c": 3},
                {"a": 2, "b": 2, "c": 3},
                {"a": 3, "b": 2, "c": 3},
            ]
        )

        # Apply operations and expect them to fail
        with pytest.raises(error_type) as exc_info:
            for op in invalid_operations:
                if op[0] == "rename":
                    ds = ds.rename_columns(op[1])
                elif op[0] == "select":
                    ds = ds.select_columns(op[1])
                elif op[0] == "with_column":
                    if len(op) == 3 and not isinstance(op[2], (int, float, str)):
                        # Expression-based with_column (op[2] is an expression)
                        ds = ds.with_column(op[1], op[2])
                    else:
                        # Literal-based with_column
                        ds = ds.with_column(op[1], lit(op[2]))
                elif op[0] == "filter":
                    ds = ds.filter(expr=op[1])
                elif op[0] == "count":
                    ds.count()
                    return

            # Force execution to trigger the error
            result = ds.take_all()
            print(f"Unexpected success: {result}")

        # Verify the error message contains the expected column name
        error_str = str(exc_info.value).lower()
        assert (
            error_message_contains.lower() in error_str
        ), f"Expected '{error_message_contains}' in error message: {error_str}"

    @pytest.mark.parametrize(
        "operations,expected_output",
        [
            # === Basic Select Operations ===
            pytest.param(
                [("select", ["a"])],
                [{"a": 1}, {"a": 2}, {"a": 3}],
                id="select_single_column",
            ),
            pytest.param(
                [("select", ["a", "b"])],
                [{"a": 1, "b": 4}, {"a": 2, "b": 5}, {"a": 3, "b": 6}],
                id="select_two_columns",
            ),
            pytest.param(
                [("select", ["a", "b", "c"])],
                [
                    {"a": 1, "b": 4, "c": 7},
                    {"a": 2, "b": 5, "c": 8},
                    {"a": 3, "b": 6, "c": 9},
                ],
                id="select_all_columns",
            ),
            pytest.param(
                [("select", ["c", "a"])],
                [{"c": 7, "a": 1}, {"c": 8, "a": 2}, {"c": 9, "a": 3}],
                id="select_reordered_columns",
            ),
            # === Basic Rename Operations ===
            pytest.param(
                [("rename", {"a": "alpha"})],
                [
                    {"alpha": 1, "b": 4, "c": 7},
                    {"alpha": 2, "b": 5, "c": 8},
                    {"alpha": 3, "b": 6, "c": 9},
                ],
                id="rename_single_column",
            ),
            pytest.param(
                [("rename", {"a": "alpha", "b": "beta"})],
                [
                    {"alpha": 1, "beta": 4, "c": 7},
                    {"alpha": 2, "beta": 5, "c": 8},
                    {"alpha": 3, "beta": 6, "c": 9},
                ],
                id="rename_multiple_columns",
            ),
            # === Basic with_column Operations ===
            pytest.param(
                [("with_column_expr", "sum", "add", "a", "b")],
                [
                    {"a": 1, "b": 4, "c": 7, "sum": 5},
                    {"a": 2, "b": 5, "c": 8, "sum": 7},
                    {"a": 3, "b": 6, "c": 9, "sum": 9},
                ],
                id="with_column_add_keep_all",
            ),
            pytest.param(
                [("with_column_expr", "product", "multiply", "b", "c")],
                [
                    {"a": 1, "b": 4, "c": 7, "product": 28},
                    {"a": 2, "b": 5, "c": 8, "product": 40},
                    {"a": 3, "b": 6, "c": 9, "product": 54},
                ],
                id="with_column_multiply_keep_all",
            ),
            # === Chained Selects ===
            pytest.param(
                [("select", ["a", "b", "c"]), ("select", ["a", "b"])],
                [{"a": 1, "b": 4}, {"a": 2, "b": 5}, {"a": 3, "b": 6}],
                id="chained_selects_two_levels",
            ),
            pytest.param(
                [
                    ("select", ["a", "b", "c"]),
                    ("select", ["a", "b"]),
                    ("select", ["a"]),
                ],
                [{"a": 1}, {"a": 2}, {"a": 3}],
                id="chained_selects_three_levels",
            ),
            # === Rename → Select ===
            pytest.param(
                [("rename", {"a": "x"}), ("select", ["x", "b"])],
                [{"x": 1, "b": 4}, {"x": 2, "b": 5}, {"x": 3, "b": 6}],
                id="rename_then_select",
            ),
            pytest.param(
                [("rename", {"a": "x", "c": "z"}), ("select", ["x", "z"])],
                [{"x": 1, "z": 7}, {"x": 2, "z": 8}, {"x": 3, "z": 9}],
                id="rename_multiple_then_select",
            ),
            # === Select → Rename ===
            pytest.param(
                [("select", ["a", "b"]), ("rename", {"a": "x"})],
                [{"x": 1, "b": 4}, {"x": 2, "b": 5}, {"x": 3, "b": 6}],
                id="select_then_rename",
            ),
            pytest.param(
                [("select", ["a", "b", "c"]), ("rename", {"a": "x", "b": "y"})],
                [
                    {"x": 1, "y": 4, "c": 7},
                    {"x": 2, "y": 5, "c": 8},
                    {"x": 3, "y": 6, "c": 9},
                ],
                id="select_all_then_rename_some",
            ),
            # === Multiple Renames ===
            pytest.param(
                [("rename", {"a": "x"}), ("rename", {"x": "y"})],
                [
                    {"y": 1, "b": 4, "c": 7},
                    {"y": 2, "b": 5, "c": 8},
                    {"y": 3, "b": 6, "c": 9},
                ],
                id="chained_renames",
            ),
            # === with_column → Select ===
            pytest.param(
                [("with_column_expr", "sum", "add", "a", "b"), ("select", ["sum"])],
                [{"sum": 5}, {"sum": 7}, {"sum": 9}],
                id="with_column_then_select_only_computed",
            ),
            pytest.param(
                [
                    ("with_column_expr", "sum", "add", "a", "b"),
                    ("select", ["a", "sum"]),
                ],
                [{"a": 1, "sum": 5}, {"a": 2, "sum": 7}, {"a": 3, "sum": 9}],
                id="with_column_then_select_mixed",
            ),
            pytest.param(
                [
                    ("with_column_expr", "result", "multiply", "b", "c"),
                    ("select", ["a", "result"]),
                ],
                [
                    {"a": 1, "result": 28},
                    {"a": 2, "result": 40},
                    {"a": 3, "result": 54},
                ],
                id="with_column_select_source_and_computed",
            ),
            # === Multiple with_column Operations ===
            pytest.param(
                [
                    ("with_column_expr", "sum", "add", "a", "b"),
                    ("with_column_expr", "product", "multiply", "a", "c"),
                ],
                [
                    {"a": 1, "b": 4, "c": 7, "sum": 5, "product": 7},
                    {"a": 2, "b": 5, "c": 8, "sum": 7, "product": 16},
                    {"a": 3, "b": 6, "c": 9, "sum": 9, "product": 27},
                ],
                id="multiple_with_column_keep_all",
            ),
            pytest.param(
                [
                    ("with_column_expr", "sum", "add", "a", "b"),
                    ("with_column_expr", "product", "multiply", "a", "c"),
                    ("select", ["sum", "product"]),
                ],
                [
                    {"sum": 5, "product": 7},
                    {"sum": 7, "product": 16},
                    {"sum": 9, "product": 27},
                ],
                id="multiple_with_column_then_select",
            ),
            pytest.param(
                [
                    ("with_column_expr", "sum", "add", "a", "b"),
                    ("with_column_expr", "diff", "add", "c", "a"),
                    ("select", ["sum", "diff"]),
                ],
                [{"sum": 5, "diff": 8}, {"sum": 7, "diff": 10}, {"sum": 9, "diff": 12}],
                id="multiple_with_column_independent_sources",
            ),
            # === with_column → Rename ===
            pytest.param(
                [
                    ("with_column_expr", "sum", "add", "a", "b"),
                    ("rename", {"sum": "total"}),
                ],
                [
                    {"a": 1, "b": 4, "c": 7, "total": 5},
                    {"a": 2, "b": 5, "c": 8, "total": 7},
                    {"a": 3, "b": 6, "c": 9, "total": 9},
                ],
                id="with_column_then_rename_computed",
            ),
            # === Rename → with_column ===
            pytest.param(
                [
                    ("rename", {"a": "x"}),
                    ("with_column_expr", "x_plus_b", "add", "x", "b"),
                ],
                [
                    {"x": 1, "b": 4, "c": 7, "x_plus_b": 5},
                    {"x": 2, "b": 5, "c": 8, "x_plus_b": 7},
                    {"x": 3, "b": 6, "c": 9, "x_plus_b": 9},
                ],
                id="rename_then_with_column_using_renamed",
            ),
            pytest.param(
                [
                    ("rename", {"a": "x"}),
                    ("with_column_expr", "result", "add", "x", "b"),
                    ("select", ["result"]),
                ],
                [{"result": 5}, {"result": 7}, {"result": 9}],
                id="rename_with_column_select_chain",
            ),
            # === Select → with_column → Select ===
            pytest.param(
                [
                    ("select", ["a", "b"]),
                    ("with_column_expr", "sum", "add", "a", "b"),
                    ("select", ["a", "sum"]),
                ],
                [{"a": 1, "sum": 5}, {"a": 2, "sum": 7}, {"a": 3, "sum": 9}],
                id="select_with_column_select_chain",
            ),
            pytest.param(
                [
                    ("select", ["a", "b", "c"]),
                    ("with_column_expr", "x", "add", "a", "b"),
                    ("with_column_expr", "y", "multiply", "b", "c"),
                    ("select", ["x", "y"]),
                ],
                [{"x": 5, "y": 28}, {"x": 7, "y": 40}, {"x": 9, "y": 54}],
                id="select_multiple_with_column_select_chain",
            ),
            # === Complex Multi-Step Chains ===
            pytest.param(
                [
                    ("select", ["a", "b", "c"]),
                    ("rename", {"a": "x"}),
                    ("with_column_expr", "result", "add", "x", "b"),
                    ("select", ["result", "c"]),
                ],
                [{"result": 5, "c": 7}, {"result": 7, "c": 8}, {"result": 9, "c": 9}],
                id="complex_select_rename_with_column_select",
            ),
            pytest.param(
                [
                    ("rename", {"a": "alpha", "b": "beta"}),
                    ("select", ["alpha", "beta", "c"]),
                    ("with_column_expr", "sum", "add", "alpha", "beta"),
                    ("rename", {"sum": "total"}),
                    ("select", ["total", "c"]),
                ],
                [{"total": 5, "c": 7}, {"total": 7, "c": 8}, {"total": 9, "c": 9}],
                id="complex_five_step_chain",
            ),
            pytest.param(
                [
                    ("select", ["a", "b", "c"]),
                    ("select", ["b", "c"]),
                    ("select", ["c"]),
                ],
                [{"c": 7}, {"c": 8}, {"c": 9}],
                id="select_chain",
            ),
        ],
    )
    def test_projection_pushdown_into_parquet_read(
        self, ray_start_regular_shared, tmp_path, operations, expected_output
    ):
        """Test that projection operations fuse and push down into parquet reads.

        Verifies:
        - Multiple projections fuse into single operator
        - Fused projection pushes down into Read operator
        - Only necessary columns are read from parquet
        - Results are correct for select, rename, and with_column operations
        """
        from ray.data.expressions import col

        # Create test parquet file
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6], "c": [7, 8, 9]})
        parquet_path = tmp_path / "test.parquet"
        df.to_parquet(parquet_path, index=False)

        # Build pipeline with operations
        ds = ray.data.read_parquet(str(parquet_path))

        for op_type, *op_args in operations:
            if op_type == "select":
                ds = ds.select_columns(op_args[0])
            elif op_type == "rename":
                ds = ds.rename_columns(op_args[0])
            elif op_type == "with_column_expr":
                col_name, operator, col1, col2 = op_args
                if operator == "add":
                    ds = ds.with_column(col_name, col(col1) + col(col2))
                elif operator == "multiply":
                    ds = ds.with_column(col_name, col(col1) * col(col2))

        result_df = ds.to_pandas()
        expected_df = pd.DataFrame(expected_output)
        # Ensure columns are in the same order for comparison
        result_df = result_df[sorted(result_df.columns)]
        expected_df = expected_df[sorted(expected_df.columns)]
        assert rows_same(result_df, expected_df)


@pytest.mark.parametrize("flavor", ["project_before", "project_after"])
def test_projection_pushdown_merge_rename_x(ray_start_regular_shared, flavor):
    """
    Test that valid select and renaming merges correctly.
    """
    path = "example://iris.parquet"
    ds = ray.data.read_parquet(path)
    ds = ds.map_batches(lambda d: d)

    if flavor == "project_before":
        ds = ds.select_columns(["sepal.length", "petal.width"])

    # First projection renames 'sepal.length' to 'length'
    ds = ds.rename_columns({"sepal.length": "length"})

    # Second projection renames 'petal.width' to 'width'
    ds = ds.rename_columns({"petal.width": "width"})

    if flavor == "project_after":
        ds = ds.select_columns(["length", "width"])

    logical_plan = ds._plan._logical_plan
    op = logical_plan.dag
    assert isinstance(op, Project), op.name

    optimized_logical_plan = LogicalOptimizer().optimize(logical_plan)
    assert isinstance(optimized_logical_plan.dag, Project)

    select_op = optimized_logical_plan.dag

    # Check that both "sepal.length" and "petal.width" are present in the columns,
    # regardless of their order.
    assert select_op.exprs == [
        # TODO fix (renaming doesn't remove prev columns)
        col("sepal.length").alias("length"),
        col("petal.width").alias("width"),
    ]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
