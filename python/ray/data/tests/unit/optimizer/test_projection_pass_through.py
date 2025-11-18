"""Unit tests for projection pass-through optimization.

Tests the logical plan transformations for operators that support
LogicalOperatorSupportsProjectionPassThrough.
"""

from typing import Callable, Dict, List, Optional, Union as UnionType

import pytest

from ray.data._internal.logical.interfaces import (
    LogicalOperator,
    LogicalOperatorSupportsProjectionPassThrough,
    LogicalPlan,
)
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
from ray.data.expressions import Expr, col


class TestProjectionPassThroughUnit:
    """Unit tests for projection pass-through transformations."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Set up test fixtures."""
        self.context = DataContext.get_current()

    def _create_input_op(self, schema_dict: Optional[Dict[str, str]] = None):
        """Create an input operator.

        Args:
            schema_dict: Optional dict mapping column names to types for mocking schema.
                        If None, creates InputData without schema.
                        If provided, creates a mock operator with schema.

        Returns:
            InputData operator (with or without mocked schema).
        """
        if schema_dict is None:
            return InputData(input_data=[])

        # Create a mock operator with schema
        import pyarrow as pa

        # NOTE: Since this is a unit test, we cannot use ray.put, which is required
        # for InputData. Instead we just mock the schema to test that the projection
        # pushdown is happening correctly.
        class MockInputWithSchema(InputData):
            """Mock input operator with schema for testing."""

            def __init__(self, schema_dict: Dict[str, str]):
                super().__init__(input_data=[])
                self._mock_schema = pa.schema(
                    [
                        (name, self._get_pa_type(typ))
                        for name, typ in schema_dict.items()
                    ]
                )

            def infer_schema(self):
                return self._mock_schema

            @staticmethod
            def _get_pa_type(typ: str):
                """Convert simple type string to PyArrow type."""
                type_map = {
                    "int": pa.int64(),
                    "str": pa.string(),
                    "float": pa.float64(),
                }
                return type_map.get(typ, pa.string())

        return MockInputWithSchema(schema_dict)

    def _assert_simple_pass_through(
        self,
        optimized_dag: LogicalOperatorSupportsProjectionPassThrough,
        operator_class: type[LogicalOperator],
    ):
        """Assert projection passed through without downstream project.

        Expected structure: Operator -> Project (upstream)
        """
        assert isinstance(optimized_dag, operator_class)
        for input_op in optimized_dag.input_dependencies:
            assert isinstance(input_op, Project)

    def _assert_pass_through_with_downstream(
        self,
        optimized_dag: LogicalOperatorSupportsProjectionPassThrough,
        operator_class: type[LogicalOperator],
    ):
        """Assert projection passed through with downstream project.

        Expected structure: Project (downstream, removes keys) -> Operator -> Project (upstream)
        """
        assert isinstance(optimized_dag, Project), "Expected downstream Project at top"
        op: LogicalOperatorSupportsProjectionPassThrough = (
            optimized_dag.input_dependency
        )
        assert isinstance(op, operator_class)
        assert isinstance(op.input_dependencies[0], Project)

    def _assert_keys_renamed_correctly(
        self,
        optimized_dag: UnionType[LogicalOperatorSupportsProjectionPassThrough, Project],
        operator_class: type[LogicalOperator],
        expected_keys: List[List[str]],
    ):
        """Verify that operator keys match expected (possibly renamed) values.

        Works for operators with keys: Sort, Repartition, Join.
        Handles both structures (operator at top, or Project -> operator).
        """
        # Find the operator (might be at top or under a downstream Project)
        if isinstance(optimized_dag, operator_class):
            op = optimized_dag
        elif isinstance(optimized_dag, Project):
            op = optimized_dag.input_dependency
            assert isinstance(op, operator_class)
        else:
            raise AssertionError(
                f"Expected {operator_class.__name__} or Project, got {type(optimized_dag)}"
            )

        actual_keys = op.get_referenced_keys()
        assert (
            actual_keys == expected_keys
        ), f"Keys not renamed correctly. Expected {expected_keys}, got {actual_keys}"

    def _assert_no_pass_through(
        self,
        optimized_dag: LogicalOperatorSupportsProjectionPassThrough,
        operator_class: type[LogicalOperator],
    ):
        """Assert that projection did NOT pass through (complex expression).

        Expected structure: Operator (unchanged) -> Project (complex, unchanged)
        """
        assert isinstance(optimized_dag, Project)
        assert isinstance(optimized_dag.input_dependency, operator_class)
        assert not isinstance(optimized_dag.input_dependency, Project)

    def _assert_keys_preserved(
        self,
        optimized_dag: LogicalOperatorSupportsProjectionPassThrough,
        operator_class: type[LogicalOperator],
        expected_keys: List[List[str]],
    ):
        """Assert that operator keys are preserved in the optimized plan.

        Works for any operator with keys (Sort, Repartition, Join).

        Checks both cases:
        - Operator at top level (keys in output)
        - Project -> Operator (keys not in output, downstream project)
        """
        if isinstance(optimized_dag, operator_class):
            assert optimized_dag.get_referenced_keys() == expected_keys
        elif isinstance(optimized_dag, Project):
            # Downstream project case
            assert isinstance(optimized_dag.input_dependency, operator_class)
            assert optimized_dag.input_dependency.get_referenced_keys() == expected_keys
        else:
            raise AssertionError(
                f"Expected {operator_class.__name__} or Project, got {type(optimized_dag)}"
            )

    # Tests for operators without keys (single or multi-input)

    @pytest.mark.parametrize(
        "operator_class,operator_factory",
        [
            (
                RandomizeBlocks,
                lambda inp: RandomizeBlocks(input_op=inp, seed=42),
            ),
            (RandomShuffle, lambda inp: RandomShuffle(input_op=inp, seed=42)),
            (
                StreamingRepartition,
                lambda inp: StreamingRepartition(
                    input_op=inp, target_num_rows_per_block=1000
                ),
            ),
            (
                Union,
                lambda inp: Union(inp, inp),
            ),
        ],
        ids=["RandomizeBlocks", "RandomShuffle", "StreamingRepartition", "Union"],
    )
    @pytest.mark.parametrize(
        "exprs,is_complex",
        [
            ([col("a")._rename("x"), col("b")], False),
            ([(col("a") + col("b")).alias("sum")], True),
        ],
        ids=["simple", "complex"],
    )
    def test_operators_without_keys(
        self,
        operator_class: type[LogicalOperator],
        operator_factory: Callable,
        exprs: List[Expr],
        is_complex: bool,
    ):
        """Test projection pass-through for operators without keys."""
        input_op = self._create_input_op()
        op = operator_factory(input_op)

        project_op = Project(
            input_op=op, exprs=exprs, compute=None, ray_remote_args=None
        )

        plan = LogicalPlan(project_op, self.context)
        rule = ProjectionPushdown()
        optimized_plan = rule.apply(plan)

        if is_complex:
            self._assert_no_pass_through(optimized_plan.dag, operator_class)
        else:
            self._assert_simple_pass_through(optimized_plan.dag, operator_class)

    # Tests for operators with keys

    @pytest.mark.parametrize(
        "operator_class,operator_factory",
        [
            (
                Repartition,
                lambda inp, keys: Repartition(
                    input_op=inp, num_outputs=4, shuffle=True, keys=keys, sort=False
                ),
            ),
            (
                Sort,
                lambda inp, keys: Sort(
                    input_op=inp,
                    sort_key=SortKey(key=keys if keys else ["id"], descending=False),
                    batch_format="default",
                ),
            ),
        ],
        ids=["Repartition", "Sort"],
    )
    @pytest.mark.parametrize(
        "keys,exprs,is_complex,expect_downstream,expected_keys",
        [
            # No keys (Repartition only)
            (None, [col("a")._rename("x"), col("b")], False, False, [[]]),
            (None, [(col("a") + col("b")).alias("sum")], True, False, []),
            # Has keys, keys in output
            (["id"], [col("id"), col("value")._rename("val")], False, False, [["id"]]),
            (["id"], [col("id")._rename("id"), col("value")], False, False, [["id"]]),
            # Has keys, keys NOT in output (needs downstream project)
            (["id"], [col("value")._rename("val")], False, True, [["id"]]),
            # Complex expressions with keys
            (["id"], [(col("value") * 2).alias("doubled")], True, False, []),
        ],
        ids=[
            "no_keys_simple",
            "no_keys_complex",
            "keys_in_simple",
            "keys_in_rename",
            "keys_out_simple",
            "keys_complex",
        ],
    )
    def test_operators_with_keys(
        self,
        operator_class: type[LogicalOperator],
        operator_factory: Callable,
        keys: Optional[List[str]],
        exprs: List[Expr],
        is_complex: bool,
        expect_downstream: bool,
        expected_keys: List[List[str]],
    ):
        """Test projection pass-through for operators with keys (Sort, Repartition)."""
        # Skip "no_keys" cases for Sort (Sort always requires keys)
        if operator_class == Sort and keys is None:
            pytest.skip("Sort requires keys")

        input_op = self._create_input_op()
        op = operator_factory(input_op, keys)

        project_op = Project(
            input_op=op, exprs=exprs, compute=None, ray_remote_args=None
        )

        plan = LogicalPlan(project_op, self.context)
        rule = ProjectionPushdown()
        optimized_plan = rule.apply(plan)

        if is_complex:
            self._assert_no_pass_through(optimized_plan.dag, operator_class)
        else:
            # Check structure
            if expect_downstream:
                self._assert_pass_through_with_downstream(
                    optimized_plan.dag, operator_class
                )
            else:
                self._assert_simple_pass_through(optimized_plan.dag, operator_class)

            # Check keys if expected
            if expected_keys:
                self._assert_keys_renamed_correctly(
                    optimized_plan.dag, operator_class, expected_keys
                )

    # Join tests

    @pytest.mark.parametrize(
        "join_type,expected_keys_template",
        [
            ("inner", [["id"], ["id"]]),
            ("left_outer", [["id"], ["id"]]),
            ("right_outer", [["id"], ["id"]]),
            ("full_outer", [["id"], ["id"]]),
            ("left_semi", [["id"], ["id"]]),  # Only left in output
            ("right_semi", [["id"], ["id"]]),  # Only right in output
            ("left_anti", [["id"], ["id"]]),  # Only left in output
            ("right_anti", [["id"], ["id"]]),  # Only right in output
        ],
    )
    @pytest.mark.parametrize(
        "exprs,is_complex,expect_downstream",
        [
            # Keys in output
            ([col("id"), col("left_value")._rename("lv")], False, False),
            ([col("id")._rename("id"), col("left_value")], False, False),
            # Keys NOT in output (needs downstream project)
            ([col("left_value")._rename("lv")], False, True),
            ([col("left_value")], False, True),
            # Complex expressions
            ([(col("left_value") * 2).alias("doubled")], True, False),
        ],
        ids=[
            "keys_in_mixed",
            "keys_in__rename",
            "keys_out__rename",
            "keys_out_simple",
            "complex",
        ],
    )
    def test_join(
        self,
        join_type: str,
        expected_keys_template: List[List[str]],
        exprs: List[Expr],
        is_complex: bool,
        expect_downstream: bool,
    ):
        """Test Join with various configurations and join types."""
        # Create inputs with mocked schema
        left_schema = {"id": "int", "left_value": "str"}
        right_schema = {"id": "int", "right_value": "str"}

        left_op = self._create_input_op(schema_dict=left_schema)
        right_op = self._create_input_op(schema_dict=right_schema)

        join_op = Join(
            left_input_op=left_op,
            right_input_op=right_op,
            join_type=join_type,
            left_key_columns=("id",),
            right_key_columns=("id",),
            num_partitions=2,
        )

        project_op = Project(
            input_op=join_op, exprs=exprs, compute=None, ray_remote_args=None
        )

        plan = LogicalPlan(project_op, self.context)
        rule = ProjectionPushdown()
        optimized_plan = rule.apply(plan)

        if is_complex:
            self._assert_no_pass_through(optimized_plan.dag, Join)
        else:
            # Verify Join structure and keys
            assert optimized_plan.dag is not None
            # Use the template for expected keys
            self._assert_keys_renamed_correctly(
                optimized_plan.dag, Join, expected_keys_template
            )

    def test_join_mixed_schema_with_renames_short_circuits(self):
        """Test Join with mixed schema and renames short-circuits (doesn't optimize).

        When Join requires schema analysis but one side has no schema AND there are
        non-trivial renames, the optimization is skipped to avoid incorrect transformations.

        Expected: NO optimization - Project -> Join -> [Input, Input] (unchanged)
        """
        # Left has mocked schema
        left_schema = {"id": "int", "left_value": "str"}
        left_op = self._create_input_op(schema_dict=left_schema)

        # Right has NO schema
        right_op = self._create_input_op(schema_dict=None)

        join_op = Join(
            left_input_op=left_op,
            right_input_op=right_op,
            join_type="inner",
            left_key_columns=("id",),
            right_key_columns=("id",),
            num_partitions=2,
        )

        # Select columns from left only (which has schema)
        project_exprs = [col("id")._rename("new_id"), col("left_value")._rename("lv")]
        project_op = Project(
            input_op=join_op, exprs=project_exprs, compute=None, ray_remote_args=None
        )

        plan = LogicalPlan(project_op, self.context)
        rule = ProjectionPushdown()
        optimized_plan = rule.apply(plan)

        # Verify NO optimization occurred due to short-circuit
        assert isinstance(optimized_plan.dag, Project), "Should be unchanged Project"
        assert isinstance(optimized_plan.dag.input_dependency, Join)
        # Original inputs should be unchanged (same instances)
        assert (
            optimized_plan.dag.input_dependency is join_op
        ), "Join should be unchanged"
        assert optimized_plan.dag.input_dependency.input_dependencies[0] is left_op
        assert optimized_plan.dag.input_dependency.input_dependencies[1] is right_op

    def test_join_mixed_schema_without_renames_optimizes(self):
        """Test Join with mixed schema but NO renames still optimizes.

        When there are only trivial renames (col("a") stays as "a"), the optimization
        can still proceed even with missing schemas.

        Expected: Optimization happens - Join -> [Project (left), InputData (right)]
        """
        # Left has mocked schema
        left_schema = {"id": "int", "left_value": "str"}
        left_op = self._create_input_op(schema_dict=left_schema)

        # Right has NO schema
        right_op = self._create_input_op(schema_dict=None)

        join_op = Join(
            left_input_op=left_op,
            right_input_op=right_op,
            join_type="inner",
            left_key_columns=("id",),
            right_key_columns=("id",),
            num_partitions=2,
        )

        # Use trivial selection (no renames - allows optimization despite missing schema)
        project_exprs = [col("id"), col("left_value")]
        project_op = Project(
            input_op=join_op, exprs=project_exprs, compute=None, ray_remote_args=None
        )

        plan = LogicalPlan(project_op, self.context)
        rule = ProjectionPushdown()
        optimized_plan = rule.apply(plan)

        # Verify optimization DID occur (no renames, so short-circuit doesn't apply)
        assert isinstance(optimized_plan.dag, Join), "Join should be at top"

        # Verify it's a NEW Join
        assert optimized_plan.dag is not join_op, "Should be new Join"

        # Left should have upstream project (has schema)
        left_input = optimized_plan.dag.input_dependencies[0]
        assert isinstance(left_input, Project), "Left should have Project"

        # Right stays unchanged (no schema)
        right_input = optimized_plan.dag.input_dependencies[1]
        assert isinstance(right_input, InputData), "Right should be unchanged"

    def test_join_different_key_names(self):
        """Test Join with different left and right key column names.

        Expected structure:
            Join[left.user_id = right.customer_id]
             ├── Project (left - selects user_id + name)
             └── Project (right - selects customer_id + order)
        """
        # Both sides have schema
        left_schema = {"user_id": "int", "name": "str"}
        right_schema = {"customer_id": "int", "order": "str"}

        left_op = self._create_input_op(schema_dict=left_schema)
        right_op = self._create_input_op(schema_dict=right_schema)

        join_op = Join(
            left_input_op=left_op,
            right_input_op=right_op,
            join_type="inner",
            left_key_columns=("user_id",),
            right_key_columns=("customer_id",),
            num_partitions=2,
        )

        # Select columns from both sides (including keys)
        project_exprs = [
            col("user_id"),
            col("name")._rename("customer_name"),
            col("order")._rename("order_info"),
        ]
        project_op = Project(
            input_op=join_op, exprs=project_exprs, compute=None, ray_remote_args=None
        )

        plan = LogicalPlan(project_op, self.context)
        rule = ProjectionPushdown()
        optimized_plan = rule.apply(plan)

        # Verify structure: Should be Join at top (keys are selected, so no downstream project)
        # But it might be Project -> Join if there's fusion happening
        if isinstance(optimized_plan.dag, Project):
            # Fusion occurred
            join_op_optimized = optimized_plan.dag.input_dependency
            assert isinstance(join_op_optimized, Join)
        else:
            # No fusion
            join_op_optimized = optimized_plan.dag
            assert isinstance(join_op_optimized, Join)

        # Both sides should have upstream projects (both have schema)
        left_input = join_op_optimized.input_dependencies[0]
        right_input = join_op_optimized.input_dependencies[1]
        assert isinstance(left_input, Project), "Left should have Project"
        assert isinstance(right_input, Project), "Right should have Project"

        # Verify keys are preserved with their original column names
        actual_keys = join_op_optimized.get_referenced_keys()
        assert actual_keys == [
            ["user_id"],
            ["customer_id"],
        ], f"Join keys should use original column names. Got {actual_keys}"

    # Zip tests

    @pytest.mark.parametrize(
        "exprs,is_complex",
        [
            ([col("a")._rename("x"), col("c")._rename("z")], False),
            ([col("a"), col("c")], False),
            ([(col("a") * col("c")).alias("product")], True),
            ([(col("a") + 1).alias("incremented")], True),
        ],
        ids=["_rename", "simple", "complex_mult", "complex_add"],
    )
    def test_zip(self, exprs: List[Expr], is_complex: bool):
        """Test Zip with various expression types."""
        # Create inputs with mocked schema
        left_schema = {"a": "int", "b": "int"}
        right_schema = {"c": "int", "d": "int"}

        left_op = self._create_input_op(schema_dict=left_schema)
        right_op = self._create_input_op(schema_dict=right_schema)

        zip_op = Zip(left_op, right_op)

        project_op = Project(
            input_op=zip_op, exprs=exprs, compute=None, ray_remote_args=None
        )

        plan = LogicalPlan(project_op, self.context)
        rule = ProjectionPushdown()
        optimized_plan = rule.apply(plan)

        if is_complex:
            self._assert_no_pass_through(optimized_plan.dag, Zip)
        else:
            # With schema, Zip should have projections pushed to branches
            self._assert_simple_pass_through(optimized_plan.dag, Zip)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main([__file__, "-v"]))
