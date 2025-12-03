"""Tests for list, string, and struct namespace expressions.

This module tests the namespace accessor methods (list, str, struct) that provide
convenient access to PyArrow compute functions through the expression API.
"""

import datetime
from typing import Any

import pandas as pd
import pyarrow as pa
import pytest
from packaging import version

import ray
from ray.data._internal.util import rows_same
from ray.data.expressions import col

pytestmark = pytest.mark.skipif(
    version.parse(pa.__version__) < version.parse("19.0.0"),
    reason="Namespace expressions tests require PyArrow >= 19.0",
)


def _create_dataset(
    items_data: Any, dataset_format: str, arrow_table: pa.Table | None = None
):
    if dataset_format == "arrow":
        if arrow_table is not None:
            # Use pre-constructed arrow table (for complex types like structs)
            ds = ray.data.from_arrow(arrow_table)
        else:
            # Convert items to arrow table (infers types automatically)
            table = pa.Table.from_pylist(items_data)
            ds = ray.data.from_arrow(table)
    elif dataset_format == "pandas":
        if arrow_table is not None:
            # Convert arrow table to pandas
            df = arrow_table.to_pandas()
        else:
            # Create pandas DataFrame from items
            df = pd.DataFrame(items_data)
        ds = ray.data.from_blocks([df])
    return ds


# Pytest parameterization for all dataset creation formats
DATASET_FORMATS = ["pandas", "arrow"]


# ──────────────────────────────────────
# List Namespace Tests
# ──────────────────────────────────────


@pytest.mark.parametrize("dataset_format", DATASET_FORMATS)
class TestListNamespace:
    """Tests for list namespace operations."""

    def test_list_len(self, dataset_format):
        """Test list.len() returns length of each list."""

        data = [
            {"items": [1, 2, 3]},
            {"items": [4, 5]},
            {"items": []},
        ]
        ds = _create_dataset(data, dataset_format)
        result = ds.with_column("len", col("items").list.len()).to_pandas()
        expected = pd.DataFrame(
            {
                "items": [[1, 2, 3], [4, 5], []],
                "len": [3, 2, 0],
            }
        )
        assert rows_same(result, expected)

    def test_list_get(self, dataset_format):
        """Test list.get() extracts element at index."""

        data = [
            {"items": [10, 20, 30]},
            {"items": [40, 50, 60]},
        ]
        ds = _create_dataset(data, dataset_format)
        result = ds.with_column("first", col("items").list.get(0)).to_pandas()
        expected = pd.DataFrame(
            {
                "items": [[10, 20, 30], [40, 50, 60]],
                "first": [10, 40],
            }
        )
        assert rows_same(result, expected)

    def test_list_bracket_index(self, dataset_format):
        """Test list[i] bracket notation for element access."""

        data = [{"items": [10, 20, 30]}]
        ds = _create_dataset(data, dataset_format)
        result = ds.with_column("elem", col("items").list[1]).to_pandas()
        expected = pd.DataFrame(
            {
                "items": [[10, 20, 30]],
                "elem": [20],
            }
        )
        assert rows_same(result, expected)


# ──────────────────────────────────────
# String Namespace Tests
# ──────────────────────────────────────


@pytest.mark.parametrize("dataset_format", DATASET_FORMATS)
@pytest.mark.parametrize(
    "method_name,input_values,expected_results",
    [
        ("len", ["Alice", "Bob"], [5, 3]),
        ("byte_len", ["ABC"], [3]),
    ],
)
class TestStringLength:
    """Tests for string length operations."""

    def test_string_length(
        self, dataset_format, method_name, input_values, expected_results
    ):
        """Test string length methods."""

        data = [{"name": v} for v in input_values]
        ds = _create_dataset(data, dataset_format)

        method = getattr(col("name").str, method_name)
        result = ds.with_column("result", method()).to_pandas()

        expected = pd.DataFrame({"name": input_values, "result": expected_results})
        assert rows_same(result, expected)


@pytest.mark.parametrize("dataset_format", DATASET_FORMATS)
@pytest.mark.parametrize(
    "method_name,input_values,expected_values",
    [
        ("upper", ["alice", "bob"], ["ALICE", "BOB"]),
        ("lower", ["ALICE", "BOB"], ["alice", "bob"]),
        ("capitalize", ["alice", "bob"], ["Alice", "Bob"]),
        ("title", ["alice smith", "bob jones"], ["Alice Smith", "Bob Jones"]),
        ("swapcase", ["AlIcE"], ["aLiCe"]),
    ],
)
class TestStringCase:
    """Tests for string case conversion."""

    def test_string_case(
        self, dataset_format, method_name, input_values, expected_values
    ):
        """Test string case conversion methods."""

        data = [{"name": v} for v in input_values]
        ds = _create_dataset(data, dataset_format)

        method = getattr(col("name").str, method_name)
        result = ds.with_column("result", method()).to_pandas()

        expected = pd.DataFrame({"name": input_values, "result": expected_values})
        assert rows_same(result, expected)


@pytest.mark.parametrize("dataset_format", DATASET_FORMATS)
@pytest.mark.parametrize(
    "method_name,input_values,expected_results",
    [
        ("is_alpha", ["abc", "abc123", "123"], [True, False, False]),
        ("is_alnum", ["abc123", "abc-123"], [True, False]),
        ("is_digit", ["123", "12a"], [True, False]),
        ("is_space", ["   ", " a "], [True, False]),
        ("is_lower", ["abc", "Abc"], [True, False]),
        ("is_upper", ["ABC", "Abc"], [True, False]),
        ("is_ascii", ["hello", "hello😊"], [True, False]),
    ],
)
class TestStringPredicates:
    """Tests for string predicate methods (is_*)."""

    def test_string_predicate(
        self, dataset_format, method_name, input_values, expected_results
    ):
        """Test string predicate methods."""

        data = [{"val": v} for v in input_values]
        ds = _create_dataset(data, dataset_format)

        # Get the method dynamically
        method = getattr(col("val").str, method_name)
        result = ds.with_column("result", method()).to_pandas()

        expected = pd.DataFrame({"val": input_values, "result": expected_results})
        assert rows_same(result, expected)


@pytest.mark.parametrize("dataset_format", DATASET_FORMATS)
@pytest.mark.parametrize(
    "method_name,method_args,input_values,expected_values",
    [
        ("strip", (), ["  hello  ", " world "], ["hello", "world"]),
        ("strip", ("x",), ["xxxhelloxxx"], ["hello"]),
        ("lstrip", (), ["  hello  "], ["hello  "]),
        ("rstrip", (), ["  hello  "], ["  hello"]),
    ],
)
class TestStringTrimming:
    """Tests for string trimming operations."""

    def test_string_trimming(
        self, dataset_format, method_name, method_args, input_values, expected_values
    ):
        """Test string trimming methods."""

        data = [{"val": v} for v in input_values]
        ds = _create_dataset(data, dataset_format)

        method = getattr(col("val").str, method_name)
        result = ds.with_column("result", method(*method_args)).to_pandas()

        expected = pd.DataFrame({"val": input_values, "result": expected_values})
        assert rows_same(result, expected)


@pytest.mark.parametrize("dataset_format", DATASET_FORMATS)
@pytest.mark.parametrize(
    "method_name,method_kwargs,expected_value",
    [
        ("pad", {"width": 5, "fillchar": "*", "side": "right"}, "hi***"),
        ("pad", {"width": 5, "fillchar": "*", "side": "left"}, "***hi"),
        ("pad", {"width": 6, "fillchar": "*", "side": "both"}, "**hi**"),
        ("center", {"width": 6, "padding": "*"}, "**hi**"),
    ],
)
class TestStringPadding:
    """Tests for string padding operations."""

    def test_string_padding(
        self, dataset_format, method_name, method_kwargs, expected_value
    ):
        """Test string padding methods."""

        data = [{"val": "hi"}]
        ds = _create_dataset(data, dataset_format)

        method = getattr(col("val").str, method_name)
        result = ds.with_column("result", method(**method_kwargs)).to_pandas()

        expected = pd.DataFrame({"val": ["hi"], "result": [expected_value]})
        assert rows_same(result, expected)


@pytest.mark.parametrize("dataset_format", DATASET_FORMATS)
@pytest.mark.parametrize(
    "method_name,method_args,method_kwargs,input_values,expected_results",
    [
        ("starts_with", ("A",), {}, ["Alice", "Bob", "Alex"], [True, False, True]),
        ("starts_with", ("A",), {"ignore_case": True}, ["alice", "bob"], [True, False]),
        ("ends_with", ("e",), {}, ["Alice", "Bob"], [True, False]),
        ("contains", ("li",), {}, ["Alice", "Bob", "Charlie"], [True, False, True]),
        ("find", ("i",), {}, ["Alice", "Bob"], [2, -1]),
        ("count", ("a",), {}, ["banana", "apple"], [3, 1]),
        ("match", ("Al%",), {}, ["Alice", "Bob", "Alex"], [True, False, True]),
    ],
)
class TestStringSearch:
    """Tests for string searching operations."""

    def test_string_search(
        self,
        dataset_format,
        method_name,
        method_args,
        method_kwargs,
        input_values,
        expected_results,
    ):
        """Test string searching methods."""

        data = [{"val": v} for v in input_values]
        ds = _create_dataset(data, dataset_format)

        method = getattr(col("val").str, method_name)
        result = ds.with_column(
            "result", method(*method_args, **method_kwargs)
        ).to_pandas()

        expected = pd.DataFrame({"val": input_values, "result": expected_results})
        assert rows_same(result, expected)


@pytest.mark.parametrize("dataset_format", DATASET_FORMATS)
class TestStringTransform:
    """Tests for string transformation operations."""

    def test_reverse(self, dataset_format):
        """Test str.reverse() reverses strings."""

        data = [{"val": "hello"}, {"val": "world"}]
        ds = _create_dataset(data, dataset_format)
        result = ds.with_column("rev", col("val").str.reverse()).to_pandas()
        expected = pd.DataFrame({"val": ["hello", "world"], "rev": ["olleh", "dlrow"]})
        assert rows_same(result, expected)

    def test_slice(self, dataset_format):
        """Test str.slice() extracts substring."""

        data = [{"val": "hello"}]
        ds = _create_dataset(data, dataset_format)
        result = ds.with_column("sliced", col("val").str.slice(1, 4)).to_pandas()
        expected = pd.DataFrame({"val": ["hello"], "sliced": ["ell"]})
        assert rows_same(result, expected)

    def test_replace(self, dataset_format):
        """Test str.replace() replaces substring."""

        data = [{"val": "hello world"}]
        ds = _create_dataset(data, dataset_format)
        result = ds.with_column(
            "replaced", col("val").str.replace("world", "universe")
        ).to_pandas()
        expected = pd.DataFrame(
            {"val": ["hello world"], "replaced": ["hello universe"]}
        )
        assert rows_same(result, expected)

    def test_replace_with_max(self, dataset_format):
        """Test str.replace() with max_replacements."""

        data = [{"val": "aaa"}]
        ds = _create_dataset(data, dataset_format)
        result = ds.with_column(
            "replaced", col("val").str.replace("a", "X", max_replacements=2)
        ).to_pandas()
        expected = pd.DataFrame({"val": ["aaa"], "replaced": ["XXa"]})
        assert rows_same(result, expected)

    def test_repeat(self, dataset_format):
        """Test str.repeat() repeats strings."""

        data = [{"val": "A"}]
        ds = _create_dataset(data, dataset_format)
        result = ds.with_column("repeated", col("val").str.repeat(3)).to_pandas()
        expected = pd.DataFrame({"val": ["A"], "repeated": ["AAA"]})
        assert rows_same(result, expected)


# ──────────────────────────────────────
# Struct Namespace Tests
# ──────────────────────────────────────


@pytest.mark.parametrize("dataset_format", DATASET_FORMATS)
class TestStructNamespace:
    """Tests for struct namespace operations."""

    def test_struct_field(self, dataset_format):
        """Test struct.field() extracts field."""

        # Arrow table with explicit struct types
        arrow_table = pa.table(
            {
                "user": pa.array(
                    [
                        {"name": "Alice", "age": 30},
                        {"name": "Bob", "age": 25},
                    ],
                    type=pa.struct(
                        [
                            pa.field("name", pa.string()),
                            pa.field("age", pa.int32()),
                        ]
                    ),
                )
            }
        )
        # Items representation
        items_data = [
            {"user": {"name": "Alice", "age": 30}},
            {"user": {"name": "Bob", "age": 25}},
        ]
        ds = _create_dataset(items_data, dataset_format, arrow_table)

        result = ds.with_column("age", col("user").struct.field("age")).to_pandas()
        expected = pd.DataFrame(
            {
                "user": [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}],
                "age": [30, 25],
            }
        )
        assert rows_same(result, expected)

    def test_struct_bracket(self, dataset_format):
        """Test struct['field'] bracket notation."""

        arrow_table = pa.table(
            {
                "user": pa.array(
                    [
                        {"name": "Alice", "age": 30},
                        {"name": "Bob", "age": 25},
                    ],
                    type=pa.struct(
                        [
                            pa.field("name", pa.string()),
                            pa.field("age", pa.int32()),
                        ]
                    ),
                )
            }
        )
        items_data = [
            {"user": {"name": "Alice", "age": 30}},
            {"user": {"name": "Bob", "age": 25}},
        ]
        ds = _create_dataset(items_data, dataset_format, arrow_table)

        result = ds.with_column("name", col("user").struct["name"]).to_pandas()
        expected = pd.DataFrame(
            {
                "user": [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}],
                "name": ["Alice", "Bob"],
            }
        )
        assert rows_same(result, expected)

    def test_struct_nested_field(self, dataset_format):
        """Test nested struct field access with .field()."""

        arrow_table = pa.table(
            {
                "user": pa.array(
                    [
                        {"name": "Alice", "address": {"city": "NYC", "zip": "10001"}},
                        {"name": "Bob", "address": {"city": "LA", "zip": "90001"}},
                    ],
                    type=pa.struct(
                        [
                            pa.field("name", pa.string()),
                            pa.field(
                                "address",
                                pa.struct(
                                    [
                                        pa.field("city", pa.string()),
                                        pa.field("zip", pa.string()),
                                    ]
                                ),
                            ),
                        ]
                    ),
                )
            }
        )
        items_data = [
            {"user": {"name": "Alice", "address": {"city": "NYC", "zip": "10001"}}},
            {
                "user": {"name": "Bob", "address": {"city": "LA", "zip": "90001"}},
            },
        ]
        ds = _create_dataset(items_data, dataset_format, arrow_table)

        result = ds.with_column(
            "city", col("user").struct.field("address").struct.field("city")
        ).to_pandas()
        expected = pd.DataFrame(
            {
                "user": [
                    {"name": "Alice", "address": {"city": "NYC", "zip": "10001"}},
                    {"name": "Bob", "address": {"city": "LA", "zip": "90001"}},
                ],
                "city": ["NYC", "LA"],
            }
        )
        assert rows_same(result, expected)

    def test_struct_nested_bracket(self, dataset_format):
        """Test nested struct field access with brackets."""

        arrow_table = pa.table(
            {
                "user": pa.array(
                    [
                        {"name": "Alice", "address": {"city": "NYC", "zip": "10001"}},
                        {"name": "Bob", "address": {"city": "LA", "zip": "90001"}},
                    ],
                    type=pa.struct(
                        [
                            pa.field("name", pa.string()),
                            pa.field(
                                "address",
                                pa.struct(
                                    [
                                        pa.field("city", pa.string()),
                                        pa.field("zip", pa.string()),
                                    ]
                                ),
                            ),
                        ]
                    ),
                )
            }
        )
        items_data = [
            {"user": {"name": "Alice", "address": {"city": "NYC", "zip": "10001"}}},
            {
                "user": {"name": "Bob", "address": {"city": "LA", "zip": "90001"}},
            },
        ]
        ds = _create_dataset(items_data, dataset_format, arrow_table)

        result = ds.with_column(
            "zip", col("user").struct["address"].struct["zip"]
        ).to_pandas()
        expected = pd.DataFrame(
            {
                "user": [
                    {"name": "Alice", "address": {"city": "NYC", "zip": "10001"}},
                    {"name": "Bob", "address": {"city": "LA", "zip": "90001"}},
                ],
                "zip": ["10001", "90001"],
            }
        )
        assert rows_same(result, expected)


# ──────────────────────────────────────
# Datetime Namespace Tests
# ──────────────────────────────────────


def test_datetime_namespace_all_operations(ray_start_regular):
    """Test all datetime namespace operations on a datetime column."""

    ts = datetime.datetime(2024, 1, 2, 10, 30, 0)

    ds = ray.data.from_items([{"ts": ts}])

    result_ds = ds.select(
        [
            col("ts").dt.year().alias("year"),
            col("ts").dt.month().alias("month"),
            col("ts").dt.day().alias("day"),
            col("ts").dt.hour().alias("hour"),
            col("ts").dt.minute().alias("minute"),
            col("ts").dt.second().alias("second"),
            col("ts").dt.strftime("%Y-%m-%d").alias("date_str"),
            col("ts").dt.floor("day").alias("ts_floor"),
            col("ts").dt.ceil("day").alias("ts_ceil"),
            col("ts").dt.round("day").alias("ts_round"),
        ]
    )

    actual = result_ds.to_pandas()

    expected = pd.DataFrame(
        [
            {
                "year": 2024,
                "month": 1,
                "day": 2,
                "hour": 10,
                "minute": 30,
                "second": 0,
                "date_str": "2024-01-02",
                "ts_floor": datetime.datetime(2024, 1, 2, 0, 0, 0),
                "ts_ceil": datetime.datetime(2024, 1, 3, 0, 0, 0),
                "ts_round": datetime.datetime(2024, 1, 3, 0, 0, 0),
            }
        ]
    )

    assert rows_same(actual, expected)


def test_dt_namespace_invalid_dtype_raises(ray_start_regular):
    """Test that dt namespace on non-datetime column raises an error."""

    ds = ray.data.from_items([{"value": 1}])

    with pytest.raises(Exception):
        ds.select(col("value").dt.year()).to_pandas()


# ──────────────────────────────────────
# Integration Tests
# ──────────────────────────────────────


@pytest.mark.parametrize("dataset_format", DATASET_FORMATS)
class TestNamespaceIntegration:
    """Tests for chaining and combining namespace expressions."""

    def test_list_with_arithmetic(self, dataset_format):
        """Test list operations combined with arithmetic."""

        data = [{"items": [1, 2, 3]}]
        ds = _create_dataset(data, dataset_format)
        result = ds.with_column("len_plus_one", col("items").list.len() + 1).to_pandas()
        expected = pd.DataFrame({"items": [[1, 2, 3]], "len_plus_one": [4]})
        assert rows_same(result, expected)

    def test_string_with_comparison(self, dataset_format):
        """Test string operations combined with comparison."""

        data = [{"name": "Alice"}, {"name": "Bo"}]
        ds = _create_dataset(data, dataset_format)
        result = ds.with_column("long_name", col("name").str.len() > 3).to_pandas()
        expected = pd.DataFrame({"name": ["Alice", "Bo"], "long_name": [True, False]})
        assert rows_same(result, expected)

    def test_multiple_operations(self, dataset_format):
        """Test multiple namespace operations in single pipeline."""

        data = [{"name": "alice"}]
        ds = _create_dataset(data, dataset_format)
        result = (
            ds.with_column("upper", col("name").str.upper())
            .with_column("len", col("name").str.len())
            .with_column("starts_a", col("name").str.starts_with("a"))
            .to_pandas()
        )
        expected = pd.DataFrame(
            {
                "name": ["alice"],
                "upper": ["ALICE"],
                "len": [5],
                "starts_a": [True],
            }
        )
        assert rows_same(result, expected)


# ──────────────────────────────────────
# Error Handling Tests
# ──────────────────────────────────────


class TestNamespaceErrors:
    """Tests for proper error handling."""

    def test_list_invalid_index_type(self):
        """Test list bracket notation rejects invalid types."""

        with pytest.raises(TypeError, match="List indices must be integers or slices"):
            col("items").list["invalid"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
