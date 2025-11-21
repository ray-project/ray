"""Tests for list, string, and struct namespace expressions.

This module tests the namespace accessor methods (list, str, struct) that provide
convenient access to PyArrow compute functions through the expression API.
"""

from typing import Any

import pandas as pd
import pyarrow as pa
import pytest

import ray
from ray.data.expressions import col


def assert_df_equal(result: pd.DataFrame, expected: pd.DataFrame):
    """Assert dataframes are equal, ignoring dtype differences."""
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)


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
        assert_df_equal(result, expected)

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
        assert_df_equal(result, expected)

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
        assert_df_equal(result, expected)


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
        assert_df_equal(result, expected)


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
        assert_df_equal(result, expected)


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
        assert_df_equal(result, expected)


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
        assert_df_equal(result, expected)


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
        assert_df_equal(result, expected)


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
        assert_df_equal(result, expected)


@pytest.mark.parametrize("dataset_format", DATASET_FORMATS)
class TestStringTransform:
    """Tests for string transformation operations."""

    def test_reverse(self, dataset_format):
        """Test str.reverse() reverses strings."""

        data = [{"val": "hello"}, {"val": "world"}]
        ds = _create_dataset(data, dataset_format)
        result = ds.with_column("rev", col("val").str.reverse()).to_pandas()
        expected = pd.DataFrame({"val": ["hello", "world"], "rev": ["olleh", "dlrow"]})
        assert_df_equal(result, expected)

    def test_slice(self, dataset_format):
        """Test str.slice() extracts substring."""

        data = [{"val": "hello"}]
        ds = _create_dataset(data, dataset_format)
        result = ds.with_column("sliced", col("val").str.slice(1, 4)).to_pandas()
        expected = pd.DataFrame({"val": ["hello"], "sliced": ["ell"]})
        assert_df_equal(result, expected)

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
        assert_df_equal(result, expected)

    def test_replace_with_max(self, dataset_format):
        """Test str.replace() with max_replacements."""

        data = [{"val": "aaa"}]
        ds = _create_dataset(data, dataset_format)
        result = ds.with_column(
            "replaced", col("val").str.replace("a", "X", max_replacements=2)
        ).to_pandas()
        expected = pd.DataFrame({"val": ["aaa"], "replaced": ["XXa"]})
        assert_df_equal(result, expected)

    def test_repeat(self, dataset_format):
        """Test str.repeat() repeats strings."""

        data = [{"val": "A"}]
        ds = _create_dataset(data, dataset_format)
        result = ds.with_column("repeated", col("val").str.repeat(3)).to_pandas()
        expected = pd.DataFrame({"val": ["A"], "repeated": ["AAA"]})
        assert_df_equal(result, expected)


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
        assert_df_equal(result, expected)

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
        assert_df_equal(result, expected)

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
            {"user": {"name": "Bob", "address": {"city": "LA", "zip": "90001"}}},
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
        assert_df_equal(result, expected)

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
            {"user": {"name": "Bob", "address": {"city": "LA", "zip": "90001"}}},
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
        assert_df_equal(result, expected)


# ──────────────────────────────────────
# Array Namespace Tests
# ──────────────────────────────────────


def _make_fixed_size_list_table() -> pa.Table:
    # Build a FixedSizeListArray with 3 rows, each of length 2:
    # [[1, 2], [3, 4], [5, 6]]
    values = pa.array([1, 2, 3, 4, 5, 6], type=pa.int64())
    fixed = pa.FixedSizeListArray.from_arrays(values, list_size=2)
    return pa.Table.from_arrays([fixed], names=["features"])


def test_arr_to_list(ray_start_regular):
    table = _make_fixed_size_list_table()
    ds = ray.data.from_arrow(table)

    result = ds.select(col("features").arr.to_list().alias("features")).take(3)

    assert result == [
        {"features": [1, 2]},
        {"features": [3, 4]},
        {"features": [5, 6]},
    ]


def test_arr_flatten(ray_start_regular):
    table = _make_fixed_size_list_table()
    ds = ray.data.from_arrow(table)

    result = ds.select(col("features").arr.flatten().alias("features")).take(3)

    # For a simple FixedSizeListArray, flatten should behave like to_list
    assert result == [
        {"features": [1, 2]},
        {"features": [3, 4]},
        {"features": [5, 6]},
    ]


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
        assert_df_equal(result, expected)

    def test_string_with_comparison(self, dataset_format):
        """Test string operations combined with comparison."""

        data = [{"name": "Alice"}, {"name": "Bo"}]
        ds = _create_dataset(data, dataset_format)
        result = ds.with_column("long_name", col("name").str.len() > 3).to_pandas()
        expected = pd.DataFrame({"name": ["Alice", "Bo"], "long_name": [True, False]})
        assert_df_equal(result, expected)

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
        assert_df_equal(result, expected)


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
