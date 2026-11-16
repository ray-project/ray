"""Unit tests for the block-level explode kernel.

Covers ``explode_block`` directly on Arrow tables and pandas DataFrames,
with no Ray cluster. Each list element becomes its own row, a null list
produces one null row, and an empty list produces no rows.

This file owns the per-block-type and per-list-type semantic matrix;
``tests/expressions/test_explode.py`` covers the end-to-end wiring and
``tests/unit/expressions/test_schema_resolution.py`` the plan-time types.
"""

import pandas as pd
import pyarrow as pa
import pytest

from ray.data._internal.planner.explode import explode_block

EXPLODE_CASES = [
    pytest.param(
        pa.table(
            {
                "id": [1, 2, 3, 4],
                "items": pa.array([[1, 2], None, [], [3]], type=pa.list_(pa.int64())),
            }
        ),
        "items",
        {"id": [1, 1, 2, 4], "items": [1, 2, None, 3]},
        pa.int64(),
        id="list-with-null-and-empty",
    ),
    pytest.param(
        pa.table({"v": pa.array([[1], [2, 3]], type=pa.large_list(pa.int64()))}),
        "v",
        {"v": [1, 2, 3]},
        pa.int64(),
        id="large-list",
    ),
    pytest.param(
        pa.table(
            {
                "v": pa.array([[1, 2], None, [3, 4]], type=pa.list_(pa.int64(), 2)),
                "k": ["a", "b", "c"],
            }
        ),
        "v",
        {"v": [1, 2, None, 3, 4], "k": ["a", "a", "b", "c", "c"]},
        pa.int64(),
        id="fixed-size-list",
    ),
    pytest.param(
        pa.table(
            {"v": pa.array([[[1], [2, 3]], [[4]]], type=pa.list_(pa.list_(pa.int64())))}
        ),
        "v",
        {"v": [[1], [2, 3], [4]]},
        pa.list_(pa.int64()),
        id="nested-list-removes-one-level",
    ),
    pytest.param(
        pa.table(
            {
                "v": pa.array(
                    [[{"a": 1}], None, [{"a": 2}, {"a": 3}]],
                    type=pa.list_(pa.struct([("a", pa.int64())])),
                )
            }
        ),
        "v",
        {"v": [{"a": 1}, None, {"a": 2}, {"a": 3}]},
        pa.struct([("a", pa.int64())]),
        id="list-of-struct",
    ),
    pytest.param(
        # A non-nullable element field does not stop a null list from
        # exploding to one null row: the output field is widened to nullable.
        pa.table(
            {
                "v": pa.array(
                    [[1, 2], None, [3]],
                    type=pa.list_(pa.field("item", pa.int64(), nullable=False)),
                )
            }
        ),
        "v",
        {"v": [1, 2, None, 3]},
        pa.int64(),
        id="non-nullable-elements-with-null-list",
    ),
    pytest.param(
        pa.table({"v": pa.array([[1, None], [None]], type=pa.list_(pa.int64()))}),
        "v",
        {"v": [1, None, None]},
        pa.int64(),
        id="null-elements-inside-lists",
    ),
    pytest.param(
        pa.table(
            {
                "v": pa.array([], type=pa.list_(pa.int64())),
                "k": pa.array([], type=pa.string()),
            }
        ),
        "v",
        {"v": [], "k": []},
        pa.int64(),
        id="empty-table",
    ),
    pytest.param(
        # A column inferred as ``null`` (all values null) is a column of null
        # lists: each row explodes to one null row.
        pa.table({"v": pa.array([None, None], type=pa.null()), "k": ["x", "y"]}),
        "v",
        {"v": [None, None], "k": ["x", "y"]},
        pa.null(),
        id="null-typed-column",
    ),
    pytest.param(
        pa.table(
            {"v": pa.array([[1, 2], None, [], [3]], type=pa.list_view(pa.int64()))}
        ),
        "v",
        {"v": [1, 2, None, 3]},
        pa.int64(),
        id="list-view",
    ),
    pytest.param(
        # A sliced block: the kernel's indices must be relative to the slice.
        pa.table(
            {
                "id": [1, 2, 3, 4, 5],
                "v": pa.array(
                    [[1, 2], None, [], [3], [4, 5]], type=pa.list_(pa.int64())
                ),
            }
        ).slice(1, 3),
        "v",
        {"id": [2, 4], "v": [None, 3]},
        pa.int64(),
        id="sliced-block",
    ),
    pytest.param(
        # The kernel combines chunks before exploding.
        pa.Table.from_arrays(
            [
                pa.chunked_array(
                    [
                        pa.array([[1, 2], None], type=pa.list_(pa.int64())),
                        pa.array([[], [3]], type=pa.list_(pa.int64())),
                    ]
                )
            ],
            names=["v"],
        ),
        "v",
        {"v": [1, 2, None, 3]},
        pa.int64(),
        id="chunked-column",
    ),
    pytest.param(
        # ``pd.Series.explode`` turns empty lists into NaN rows; the kernel
        # converts to Arrow instead, so empty lists produce no rows.
        pd.DataFrame({"v": [[1, 2], [], None, [3]], "k": ["a", "b", "c", "d"]}),
        "v",
        {"v": [1, 2, None, 3], "k": ["a", "a", "c", "d"]},
        pa.int64(),
        id="pandas-block",
    ),
]


@pytest.mark.parametrize("block,column,expected,expected_type", EXPLODE_CASES)
def testexplode_block(block, column, expected, expected_type):
    out = explode_block(block, [column])
    # Pandas blocks are converted, so the output is always Arrow-backed.
    assert isinstance(out, pa.Table)
    assert out.to_pydict() == expected

    field = out.schema.field(column)
    assert field.type == expected_type
    # A null list explodes to a null row, so the output is always nullable.
    assert field.nullable


@pytest.mark.parametrize(
    "block,column,match",
    [
        pytest.param(
            pa.table({"v": pa.array([[1]], type=pa.list_(pa.int64()))}),
            "nope",
            "not found in input columns",
            id="missing-column",
        ),
        pytest.param(
            pa.table({"v": [1, 2]}),
            "v",
            "list, large_list, or fixed_size_list",
            id="non-list-column",
        ),
    ],
)
def test_explode_block_raises(block, column, match):
    with pytest.raises(ValueError, match=match):
        explode_block(block, [column])


# Zipped (multi-column) explode. Expectations verified against DataFusion
# 54.1.0 ``unnest_columns(..., preserve_nulls=True)``, which shares Ray Data's
# null/empty semantics; a randomized differential over 300 cases agreed on
# every one.
ZIP_CASES = [
    pytest.param(
        {"a": [[1, 2]], "b": [["x", "y"]]},
        {"a": [1, 2], "b": ["x", "y"]},
        id="equal-lengths",
    ),
    pytest.param(
        # The shorter list pads with nulls up to the longest one.
        {"a": [[1, 2, 3]], "b": [["x"]]},
        {"a": [1, 2, 3], "b": ["x", None, None]},
        id="ragged-pads-with-nulls",
    ),
    pytest.param(
        # A null list still claims one output row; an empty list claims none,
        # so the other column alone decides the row count here.
        {"a": [None, []], "b": [["x", "y"], ["p"]]},
        {"a": [None, None, None], "b": ["x", "y", "p"]},
        id="null-and-empty-against-longer-list",
    ),
    pytest.param(
        # Both empty: no rows. Both null: one all-null row.
        {"a": [[], None], "b": [[], None]},
        {"a": [None], "b": [None]},
        id="all-empty-then-all-null",
    ),
    pytest.param(
        {"a": [[1, 2, 3]], "b": [["x"]], "c": [[True, False]]},
        {"a": [1, 2, 3], "b": ["x", None, None], "c": [True, False, None]},
        id="three-columns",
    ),
]


@pytest.mark.parametrize("data,expected", ZIP_CASES)
def test_explode_block_zips_columns(data, expected):
    columns = list(data)
    out = explode_block(pa.table(data), columns)
    assert out.to_pydict() == expected
    for column in columns:
        assert out.schema.field(column).nullable


def test_explode_block_zip_replicates_other_columns():
    table = pa.table(
        {"id": [1, 2], "a": [[1, 2, 3], [4]], "b": [["x"], ["y", "z"]], "k": ["p", "q"]}
    )
    assert explode_block(table, ["a", "b"]).to_pydict() == {
        "id": [1, 1, 1, 2, 2],
        "a": [1, 2, 3, 4, None],
        "b": ["x", None, None, "y", "z"],
        "k": ["p", "p", "p", "q", "q"],
    }


@pytest.mark.parametrize(
    "table,columns,expected_names",
    [
        pytest.param(
            pa.table({"a": [[1]], "k": ["p"], "b": [["x"]]}),
            ["a", "b"],
            ["a", "k", "b"],
            id="schema-order",
        ),
        pytest.param(
            # Listed right-to-left: each column still lands where it started.
            pa.table({"a": [[1]], "k": ["p"], "b": [["x"]]}),
            ["b", "a"],
            ["a", "k", "b"],
            id="reverse-order",
        ),
        pytest.param(
            # Every column exploded, listed out of order.
            pa.table({"a": [[1]], "b": [["x"]]}),
            ["b", "a"],
            ["a", "b"],
            id="reverse-order-all-columns",
        ),
        pytest.param(
            # Enough intervening columns that a stale index would be rejected
            # by Arrow rather than silently misplacing the column.
            pa.table({"a": [[1]], "k1": ["p"], "k2": ["q"], "b": [["x"]]}),
            ["b", "a"],
            ["a", "k1", "k2", "b"],
            id="reverse-order-with-gap",
        ),
    ],
)
def test_explode_block_zip_preserves_column_order(table, columns, expected_names):
    out = explode_block(table, columns)
    assert out.schema.names == expected_names
    # The values must follow their own column, not the insertion position.
    assert out.to_pydict() == {
        name: (
            [1]
            if name == "a"
            else ["x"]
            if name == "b"
            else [table.column(name)[0].as_py()]
        )
        for name in expected_names
    }


def test_explode_block_zip_empty_table():
    table = pa.table(
        {
            "a": pa.array([], type=pa.list_(pa.int64())),
            "b": pa.array([], type=pa.list_(pa.string())),
        }
    )
    assert explode_block(table, ["a", "b"]).to_pydict() == {"a": [], "b": []}


def test_explode_block_zip_mixed_list_types():
    table = pa.table(
        {
            "a": pa.array([[1, 2]], type=pa.large_list(pa.int64())),
            "b": pa.array([[3, 4]], type=pa.list_(pa.int64(), 2)),
            "c": pa.array([None], type=pa.null()),
        }
    )
    assert explode_block(table, ["a", "b", "c"]).to_pydict() == {
        "a": [1, 2],
        "b": [3, 4],
        "c": [None, None],
    }


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
