import pyarrow as pa
import pyarrow.compute as pc

import ray
from ray.data._internal.compute import ActorPoolStrategy
from ray.data._internal.logical.interfaces import LogicalPlan
from ray.data._internal.logical.operators import (
    CSE_TEMP_COLUMN_PREFIX,
    Project,
)
from ray.data._internal.logical.operators.input_data_operator import InputData
from ray.data._internal.logical.optimizers import LogicalOptimizer
from ray.data._internal.logical.rules import CommonSubExprElimination
from ray.data.datatype import DataType
from ray.data.expressions import (
    AliasExpr,
    BinaryExpr,
    PyArrowComputeUDFExpr,
    UDFExpr,
    col,
    udf,
)


def _input_op():
    return InputData(input_data=[])


def _apply_cse(project: Project) -> Project:
    plan = LogicalPlan(project, ray.data.DataContext.get_current())
    optimized = CommonSubExprElimination().apply(plan)
    assert isinstance(optimized.dag, Project)
    return optimized.dag


def _temp_names(project: Project) -> list[str]:
    return [expr.name for expr in project.get_cse_common_exprs()]


@udf(return_dtype=DataType.int64())
def add_one(x: pa.Array) -> pa.Array:
    return pc.add(x, 1)


def _unwrap_alias(expr):
    return expr.expr if isinstance(expr, AliasExpr) else expr


def test_repeated_udf_in_one_project():
    subexpr = add_one(col("a"))
    project = Project(
        exprs=[(subexpr + subexpr + subexpr).alias("y")],
        input_dependencies=[_input_op()],
    )

    optimized = _apply_cse(project)

    assert len(optimized.get_cse_common_exprs()) == 1
    temp_name = _temp_names(optimized)[0]
    assert temp_name.startswith(CSE_TEMP_COLUMN_PREFIX)
    assert isinstance(_unwrap_alias(optimized.get_cse_common_exprs()[0]), UDFExpr)
    assert isinstance(optimized.exprs[0], AliasExpr)
    assert optimized.exprs[0].name == "y"
    assert temp_name in repr(optimized.exprs[0])


def test_structurally_equal_separately_constructed_udf_calls():
    project = Project(
        exprs=[(add_one(col("a")) + add_one(col("a"))).alias("y")],
        input_dependencies=[_input_op()],
    )

    optimized = _apply_cse(project)

    assert len(optimized.get_cse_common_exprs()) == 1


def test_nested_common_expressions_materialize_bottom_up():
    leaf_1 = add_one(col("a"))
    leaf_2 = add_one(col("a"))
    parent_1 = leaf_1 + leaf_2

    leaf_3 = add_one(col("a"))
    leaf_4 = add_one(col("a"))
    parent_2 = leaf_3 + leaf_4

    project = Project(
        exprs=[(parent_1 + parent_2).alias("y")],
        input_dependencies=[_input_op()],
    )

    optimized = _apply_cse(project)

    common_exprs = optimized.get_cse_common_exprs()
    assert len(common_exprs) == 2
    first_temp, second_temp = _temp_names(optimized)
    assert isinstance(_unwrap_alias(common_exprs[0]), UDFExpr)
    assert first_temp in repr(common_exprs[1])
    assert second_temp in repr(optimized.exprs[0])


def test_alias_root_is_ignored_but_child_is_extracted():
    project = Project(
        exprs=[
            add_one(col("a")).alias("x"),
            add_one(col("a")).alias("y"),
        ],
        input_dependencies=[_input_op()],
    )

    optimized = _apply_cse(project)

    assert len(optimized.get_cse_common_exprs()) == 1
    assert isinstance(_unwrap_alias(optimized.get_cse_common_exprs()[0]), UDFExpr)
    assert [expr.name for expr in optimized.exprs] == ["x", "y"]


def test_columns_and_literals_alone_are_not_materialized():
    project = Project(
        exprs=[
            col("a").alias("a1"),
            col("a").alias("a2"),
            (col("b") + 1).alias("b1"),
            (col("b") + 1).alias("b2"),
        ],
        input_dependencies=[_input_op()],
    )

    optimized = _apply_cse(project)

    assert len(optimized.get_cse_common_exprs()) == 1
    common_inner = _unwrap_alias(optimized.get_cse_common_exprs()[0])
    assert isinstance(common_inner, BinaryExpr)


def test_pyarrow_compute_udf_reuse():
    project = Project(
        exprs=[(col("a").abs() + col("a").abs()).alias("y")],
        input_dependencies=[_input_op()],
    )

    optimized = _apply_cse(project)

    assert len(optimized.get_cse_common_exprs()) == 1
    common_inner = _unwrap_alias(optimized.get_cse_common_exprs()[0])
    assert isinstance(common_inner, PyArrowComputeUDFExpr)


def test_output_schema_uses_visible_expressions_only(ray_start_regular_shared_2_cpus):
    ds = ray.data.from_arrow(pa.table({"a": [1, 2]}))
    ds = ds.with_column("x", add_one(col("a")))
    ds = ds.with_column("y", col("x") + col("x"))
    ds = ds.select_columns(["y"])

    optimized = LogicalOptimizer().optimize(ds._logical_plan)
    project = optimized.dag
    assert isinstance(project, Project)
    assert project.get_cse_common_exprs()
    assert project.infer_schema().names == ["y"]
    assert all(
        not name.startswith(CSE_TEMP_COLUMN_PREFIX)
        for name in project.infer_schema().names
    )


def test_compute_strategy_is_preserved():
    @udf(return_dtype=DataType.int64())
    class AddOffset:
        def __init__(self, offset):
            self.offset = offset

        def __call__(self, x: pa.Array) -> pa.Array:
            return pc.add(x, self.offset)

    add_ten = AddOffset(10)
    subexpr = add_ten(col("a"))
    project = Project(
        exprs=[(subexpr + subexpr).alias("y")],
        input_dependencies=[_input_op()],
    )

    optimized = _apply_cse(project)

    assert isinstance(optimized.compute, ActorPoolStrategy)


def test_cse_rule_is_idempotent():
    project = Project(
        exprs=[(add_one(col("a")) + add_one(col("a"))).alias("y")],
        input_dependencies=[_input_op()],
    )

    once = _apply_cse(project)
    twice = CommonSubExprElimination._try_optimize_project(once)

    assert twice is once


def test_logical_post_optimize_cse_executes_without_pushing_temps_into_read(
    ray_start_regular_shared_2_cpus,
):
    ds = ray.data.from_arrow(pa.table({"a": [1, 2]}))
    ds = ds.with_column("x", add_one(col("a")))
    ds = ds.with_column("y", col("x") + col("x"))
    ds = ds.select_columns(["y"])

    optimized = LogicalOptimizer().optimize(ds._logical_plan)
    project = optimized.dag
    assert isinstance(project, Project)
    assert project.get_cse_common_exprs()
    assert ds.take_all() == [{"y": 4}, {"y": 6}]


def test_udf_is_called_once_per_block(ray_start_regular_shared_2_cpus):
    @ray.remote
    class Counter:
        def __init__(self):
            self.count = 0

        def inc(self, n):
            self.count += n

        def get(self):
            return self.count

    counter = Counter.remote()

    @udf(return_dtype=DataType.int64())
    def counted_add_one(x: pa.Array) -> pa.Array:
        ray.get(counter.inc.remote(1))
        return pc.add(x, 1)

    expr = counted_add_one(col("id"))
    ds = ray.data.range(6, override_num_blocks=3)
    ds = ds.with_column("x", expr)
    ds = ds.with_column("y", col("x") + col("x") + col("x"))
    ds = ds.select_columns(["y"])

    assert ds.take_all() == [
        {"y": 3},
        {"y": 6},
        {"y": 9},
        {"y": 12},
        {"y": 15},
        {"y": 18},
    ]
    assert ray.get(counter.get.remote()) == 3


def test_callable_class_udf_still_initializes(ray_start_regular_shared_2_cpus):
    @udf(return_dtype=DataType.int64())
    class AddOffset:
        def __init__(self, offset):
            self.offset = offset

        def __call__(self, x: pa.Array) -> pa.Array:
            return pc.add(x, self.offset)

    add_ten = AddOffset(10)
    expr = add_ten(col("id"))
    ds = ray.data.range(4, override_num_blocks=2)
    ds = ds.with_column("x", expr)
    ds = ds.with_column("y", col("x") + col("x"))
    ds = ds.select_columns(["y"])

    rows = ds.take_all()
    assert rows == [
        {"y": 20},
        {"y": 22},
        {"y": 24},
        {"y": 26},
    ]
    assert all(
        not any(key.startswith(CSE_TEMP_COLUMN_PREFIX) for key in row) for row in rows
    )


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
