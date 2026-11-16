"""End-to-end tests for ``Dataset.explode`` and the ``explode()`` expression.

Scoped to wiring: lowering, schema inference, limit pushdown, block sources,
and the errors users hit. The per-list-type semantic matrix lives in
``tests/unit/test_explode_block.py`` (kernel) and
``tests/unit/expressions/test_schema_resolution.py`` (plan-time types).
"""

import pandas as pd
import pyarrow as pa
import pytest

import ray
from ray.data._internal.logical.operators import Explode, Project
from ray.data.expressions import ExplodeExpr, col, explode, lit, star, unnest
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa

# Four rows covering the semantic cases: a multi-element list, a null list,
# an empty list, and a single-element list.
ITEMS_ROWS = [
    {"id": 1, "items": [1, 2]},
    {"id": 2, "items": None},
    {"id": 3, "items": []},
    {"id": 4, "items": [3]},
]

EXPECTED_ITEMS_EXPLODED = [
    {"id": 1, "items": 1},
    {"id": 1, "items": 2},
    # A null list explodes to one null row; an empty list to no rows.
    {"id": 2, "items": None},
    {"id": 4, "items": 3},
]

# Exploding into a *new* column keeps the source list column, replicated.
EXPECTED_NEW_COLUMN = [
    {"id": 1, "items": [1, 2], "item": 1},
    {"id": 1, "items": [1, 2], "item": 2},
    {"id": 2, "items": None, "item": None},
    {"id": 4, "items": [3], "item": 3},
]


@pytest.fixture
def items_dataset(ray_start_regular_shared):
    return ray.data.from_items(ITEMS_ROWS)


@pytest.mark.parametrize(
    "source",
    [
        lambda: ray.data.from_items(ITEMS_ROWS),
        lambda: ray.data.from_arrow(pa.Table.from_pylist(ITEMS_ROWS)),
        # Pandas blocks take the same path: an empty list produces no rows,
        # unlike ``pd.Series.explode``, which turns it into a NaN row.
        lambda: ray.data.from_pandas(pd.DataFrame(ITEMS_ROWS)),
    ],
    ids=["from_items", "from_arrow", "from_pandas"],
)
def test_explode_across_block_types(ray_start_regular_shared, source):
    assert source().explode("items").take_all() == EXPECTED_ITEMS_EXPLODED


@pytest.mark.parametrize("column", ["items", col("items")], ids=["name", "col-expr"])
def test_explode_accepts_name_or_col(items_dataset, column):
    assert items_dataset.explode(column).take_all() == EXPECTED_ITEMS_EXPLODED


@pytest.mark.parametrize(
    "out_column,expects_project,expected",
    [
        ("items", False, EXPECTED_ITEMS_EXPLODED),
        ("item", True, EXPECTED_NEW_COLUMN),
    ],
    ids=["overwrite-source-column", "new-column"],
)
def test_explode_expression_lowering(
    items_dataset, out_column, expects_project, expected
):
    """Exploding a plain column under its own name lowers straight to an
    ``Explode``; writing a new column adds a ``Project`` below it."""
    ds = items_dataset.with_column(out_column, explode(col("items")))
    dag = ds._logical_plan.dag
    assert isinstance(dag, Explode)
    assert dag.columns == (out_column,)
    assert isinstance(dag.input_dependencies[0], Project) is expects_project
    assert ds.take_all() == expected


@pytest.mark.parametrize(
    "out_column", ["items", "item"], ids=["no-project", "with-project"]
)
def test_explode_expression_resources_reach_explode(items_dataset, out_column):
    """The caller's resources apply to the Explode in both lowering shapes:
    whether a Project is needed in between is an implementation detail."""
    ds = items_dataset.with_column(out_column, explode(col("items")), num_cpus=2)
    dag = ds._logical_plan.dag
    assert isinstance(dag, Explode)
    assert dag.ray_remote_args.get("num_cpus") == 2


def test_explode_derived_expression(ray_start_regular_shared):
    """The motivating example from issue #58279: explode a derived list."""
    ds = ray.data.from_items(
        [
            {"id": 1, "sentence": "hello world"},
            {"id": 2, "sentence": "ray data"},
        ]
    )
    result = ds.with_column("word", explode(col("sentence").str.split(" "))).take_all()
    assert result == [
        {"id": 1, "sentence": "hello world", "word": "hello"},
        {"id": 1, "sentence": "hello world", "word": "world"},
        {"id": 2, "sentence": "ray data", "word": "ray"},
        {"id": 2, "sentence": "ray data", "word": "data"},
    ]


def test_explode_replicates_sibling_expressions(items_dataset):
    """Sibling expressions are evaluated on the pre-explode rows and then
    replicated alongside the exploded values."""
    result = items_dataset.with_columns(
        {"id2": col("id") * 2, "item": explode(col("items"))}
    ).take_all()
    assert result == [{**row, "id2": row["id"] * 2} for row in EXPECTED_NEW_COLUMN]


def test_explode_mixed_with_unnest(ray_start_regular_shared):
    """unnest desugars inside the Project below the Explode; both coexist in
    one with_columns call."""
    ds = ray.data.from_items([{"s": {"a": 1, "b": 2}, "items": [10, 20]}])
    result = ds.with_columns(
        {"item": explode(col("items"))},
        unnest(col("s")),
    ).take_all()
    assert result == [
        {"s": {"a": 1, "b": 2}, "items": [10, 20], "a": 1, "b": 2, "item": 10},
        {"s": {"a": 1, "b": 2}, "items": [10, 20], "a": 1, "b": 2, "item": 20},
    ]


def test_explode_schema_inference_without_execution(items_dataset):
    """The exploded column's type is the list's element type, resolved from
    the plan alone."""
    inferred = items_dataset.explode("items")._logical_plan.dag.infer_schema()
    assert isinstance(inferred, pa.Schema)
    assert inferred.field("items").type == pa.int64()
    assert inferred.field("items").nullable
    assert inferred.field("id").type == pa.int64()


@pytest.mark.parametrize(
    "project,expected_per_block_limit,expected",
    [
        pytest.param(
            lambda ds: ds,
            # The Limit sits directly on the Explode, so there is nothing to
            # push it through and it stays its own operator.
            None,
            [{"id": 1, "items": 1}, {"id": 1, "items": 2}],
            id="limit-directly-above-explode",
        ),
        pytest.param(
            lambda ds: ds.select_columns(["items"]),
            2,
            [{"items": 1}, {"items": 2}],
            id="limit-pushed-through-select-columns",
        ),
    ],
)
def test_explode_limit(items_dataset, project, expected_per_block_limit, expected):
    """``LimitPushdown`` pushes a ``Limit`` down through row-preserving
    operators, stops at the row-count-changing ``Explode`` and recreates it
    with a per-block limit via ``dataclasses.replace`` — which requires
    ``per_block_limit`` to be a real init field on ``Explode``."""
    from ray.data._internal.logical.optimizers import LogicalOptimizer

    ds = project(items_dataset.explode("items")).limit(2)
    optimized = LogicalOptimizer().optimize(ds._logical_plan)
    explode_ops = [
        op for op in optimized.dag.post_order_iter() if isinstance(op, Explode)
    ]
    assert len(explode_ops) == 1
    assert explode_ops[0].per_block_limit == expected_per_block_limit
    assert ds.take_all() == expected


def _opaque_list_dataset(values):
    """A dataset with no plan-time schema, so explode validation defers to the
    runtime kernel."""

    def to_pandas_batch(batch):
        return pd.DataFrame({"v": values})

    return ray.data.range(1).map_batches(to_pandas_batch, batch_format="pandas")


def test_explode_without_plan_time_schema(ray_start_regular_shared):
    ds = _opaque_list_dataset([[1, 2], [3]])
    assert [row["v"] for row in ds.explode("v").take_all()] == [1, 2, 3]


def test_explode_non_list_raises_at_runtime(ray_start_regular_shared):
    ds = _opaque_list_dataset([1, 2])
    with pytest.raises(Exception, match="list, large_list, or fixed_size_list"):
        ds.explode("v").take_all()


@pytest.mark.parametrize(
    "call,error,match",
    [
        pytest.param(
            lambda ds: ds.explode("id"),
            ValueError,
            "list, large_list, or fixed_size_list",
            id="non-list-column",
        ),
        pytest.param(
            lambda ds: ds.with_column("x", explode(col("id"))),
            ValueError,
            "list, large_list, or fixed_size_list",
            id="non-list-expression",
        ),
        pytest.param(
            lambda ds: ds.explode("nope"),
            ValueError,
            "not found in input columns",
            id="missing-column",
        ),
        pytest.param(
            lambda ds: ds.explode(["items", "items"]),
            ValueError,
            "duplicate columns",
            id="duplicate-columns",
        ),
        pytest.param(
            lambda ds: ds.explode([]),
            ValueError,
            "at least one column",
            id="no-columns",
        ),
        pytest.param(
            lambda ds: ds.with_columns(explode(col("items"))),
            TypeError,
            "must be passed as a mapping value",
            id="positional-arg-to-with-columns",
        ),
        pytest.param(
            lambda ds: ds.explode(col("items").list.len()),
            TypeError,
            r"column name or col\(\) reference",
            id="derived-expr-to-dataset-method",
        ),
        pytest.param(
            lambda ds: ds.explode(explode(col("items"))),
            TypeError,
            r"column name or col\(\) reference",
            id="explode-expr-to-dataset-method",
        ),
        pytest.param(
            lambda ds: ds.explode(123),
            TypeError,
            r"column name or col\(\) reference",
            id="non-str-to-dataset-method",
        ),
        pytest.param(
            # An explode inside a larger expression is rejected when the
            # projection is built, with a message naming the real constraint.
            lambda ds: ds.with_column("x", explode(col("items")) + lit(1)),
            TypeError,
            "different number of rows",
            id="composed-into-larger-expression",
        ),
        pytest.param(
            lambda ds: ds.filter(expr=explode(col("items")).is_not_null()),
            TypeError,
            "different number of rows",
            id="inside-filter",
        ),
    ],
)
def test_explode_invalid_usage_raises(items_dataset, call, error, match):
    with pytest.raises(error, match=match):
        call(items_dataset)


@pytest.mark.parametrize(
    "build,match",
    [
        pytest.param(
            lambda: explode(col("items")).alias("x"), "cannot be aliased", id="alias"
        ),
        pytest.param(
            # pyrefly: ignore[bad-argument-type]  # deliberately wrong type
            lambda: explode([1, 2, 3]),
            "expects an expression",
            id="non-expr",
        ),
        pytest.param(lambda: explode(explode(col("x"))), None, id="explode-of-explode"),
        pytest.param(lambda: explode(unnest(col("x"))), None, id="explode-of-unnest"),
        pytest.param(lambda: explode(star()), None, id="explode-of-star"),
    ],
)
def test_explode_invalid_construction_raises(build, match):
    """Multi-output and non-expression inners are rejected where the
    expression is built, before any dataset is involved."""
    with pytest.raises(TypeError, match=match):
        build()


# Zipped explode. Expected values match DataFusion 54.1.0
# ``unnest_columns(..., preserve_nulls=True)``, which shares Ray Data's
# null/empty semantics.
RAGGED_ROWS = [
    {"id": 1, "a": [1, 2, 3], "b": ["x"]},
    {"id": 2, "a": None, "b": ["p", "q"]},
    {"id": 3, "a": [], "b": []},
]

EXPECTED_RAGGED = [
    {"id": 1, "a": 1, "b": "x"},
    {"id": 1, "a": 2, "b": None},
    {"id": 1, "a": 3, "b": None},
    {"id": 2, "a": None, "b": "p"},
    {"id": 2, "a": None, "b": "q"},
]


def test_explode_multiple_columns_zips(ray_start_regular_shared):
    """Several columns expand in lockstep; the shorter list pads with nulls
    rather than producing a cross product."""
    ds = ray.data.from_items(RAGGED_ROWS)
    assert ds.explode(["a", "b"]).take_all() == EXPECTED_RAGGED


def test_explode_multiple_columns_via_expressions(ray_start_regular_shared):
    """Two explode() expressions in one with_columns call lower to a single
    Explode operator and zip, exactly like ``explode([...])``."""
    ds = ray.data.from_items(RAGGED_ROWS).with_columns(
        {"a": explode(col("a")), "b": explode(col("b"))}
    )
    dag = ds._logical_plan.dag
    assert isinstance(dag, Explode)
    assert dag.columns == ("a", "b")
    assert ds.take_all() == EXPECTED_RAGGED


@pytest.mark.parametrize(
    "columns", [["a", "b"], ["b", "a"]], ids=["schema-order", "reverse-order"]
)
def test_explode_multiple_columns_order_independent(ray_start_regular_shared, columns):
    """Listing the columns in any order explodes the same rows and leaves the
    schema untouched -- the columns are rebuilt by schema index, not by the
    order they were passed in."""
    ds = ray.data.from_items(RAGGED_ROWS).explode(columns)
    assert ds.take_all() == EXPECTED_RAGGED
    assert ds.schema().names == ["id", "a", "b"]


def test_explode_expression_columns_order_independent(ray_start_regular_shared):
    """Same for the expression form, whose mapping order need not match the
    schema order."""
    ds = ray.data.from_items(RAGGED_ROWS).with_columns(
        {"b": explode(col("b")), "a": explode(col("a"))}
    )
    assert ds.take_all() == EXPECTED_RAGGED


def test_explode_chaining_gives_cross_product(ray_start_regular_shared):
    """Chained calls are the documented way to get a cross product, in
    contrast to the zip above."""
    ds = ray.data.from_items([{"a": [1, 2], "b": ["x", "y"]}])
    assert ds.explode("a").explode("b").take_all() == [
        {"a": 1, "b": "x"},
        {"a": 1, "b": "y"},
        {"a": 2, "b": "x"},
        {"a": 2, "b": "y"},
    ]


def test_explode_multiple_columns_schema_inference(ray_start_regular_shared):
    ds = ray.data.from_items(RAGGED_ROWS).explode(["a", "b"])
    inferred = ds._logical_plan.dag.infer_schema()
    assert isinstance(inferred, pa.Schema)
    assert inferred.field("a").type == pa.int64()
    assert inferred.field("b").type == pa.string()
    assert inferred.field("a").nullable and inferred.field("b").nullable


def test_explode_mixed_explode_and_sibling_expressions(ray_start_regular_shared):
    """A zip coexists with ordinary expressions, which are computed before the
    explode and replicated."""
    ds = ray.data.from_items([{"id": 2, "a": [1, 2], "b": ["x"]}])
    result = ds.with_columns(
        {"a": explode(col("a")), "b": explode(col("b")), "id2": col("id") * 2}
    ).take_all()
    assert result == [
        {"id": 2, "a": 1, "b": "x", "id2": 4},
        {"id": 2, "a": 2, "b": None, "id2": 4},
    ]


def test_explode_is_registered_with_expr_visitor():
    """``ExplodeExpr`` is dispatched by ``_ExprVisitor``, so introspecting an
    explode before passing it to ``with_columns`` works instead of raising
    the generic "unsupported expression type" error."""
    from ray.data.expressions import random as random_expr

    expr = explode(col("items"))
    assert isinstance(expr, ExplodeExpr)

    # ``repr`` renders the tree rather than raising.
    assert "EXPLODE" in repr(expr)
    assert "COL('items')" in repr(expr)

    # Idempotency delegates to the wrapped expression: exploding only
    # reshapes the value into rows.
    assert expr.is_idempotent()
    assert not explode(random_expr()).is_idempotent()

    # An explode changes the row count, so it has no PyArrow lowering. It
    # reports that rather than raising, and the raising path explains why.
    assert not expr._is_pyarrow_convertible()
    with pytest.raises(TypeError, match="different number of rows"):
        expr.to_pyarrow()

    assert expr.structurally_equals(explode(col("items")))
    assert not expr.structurally_equals(explode(col("other")))


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
