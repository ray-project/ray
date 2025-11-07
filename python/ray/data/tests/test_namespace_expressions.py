"""Tests for list, string, and struct namespace expressions.

This module tests the namespace accessor methods (list, str, struct) that provide
convenient access to PyArrow compute functions through the expression API.
"""

import pandas as pd
import pyarrow as pa
import pytest

import ray
from ray.data.expressions import col


def assert_df_equal(result: pd.DataFrame, expected: pd.DataFrame):
    """Assert dataframes are equal, ignoring dtype differences."""
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)


# ──────────────────────────────────────
# List Namespace Tests
# ──────────────────────────────────────


class TestListNamespace:
    """Tests for list namespace operations."""

    def test_list_len(self):
        """Test list.len() returns length of each list."""

        ds = ray.data.from_items(
            [
                {"items": [1, 2, 3]},
                {"items": [4, 5]},
                {"items": []},
            ]
        )
        result = ds.with_column("len", col("items").list.len()).to_pandas()
        expected = pd.DataFrame(
            {
                "items": [[1, 2, 3], [4, 5], []],
                "len": [3, 2, 0],
            }
        )
        assert_df_equal(result, expected)

    def test_list_get(self):
        """Test list.get() extracts element at index."""

        ds = ray.data.from_items(
            [
                {"items": [10, 20, 30]},
                {"items": [40, 50, 60]},
            ]
        )
        result = ds.with_column("first", col("items").list.get(0)).to_pandas()
        expected = pd.DataFrame(
            {
                "items": [[10, 20, 30], [40, 50, 60]],
                "first": [10, 40],
            }
        )
        assert_df_equal(result, expected)

    def test_list_bracket_index(self):
        """Test list[i] bracket notation for element access."""

        ds = ray.data.from_items([{"items": [10, 20, 30]}])
        result = ds.with_column("elem", col("items").list[1]).to_pandas()
        expected = pd.DataFrame(
            {
                "items": [[10, 20, 30]],
                "elem": [20],
            }
        )
        assert_df_equal(result, expected)

    @pytest.mark.skip(reason="list_flatten changes row structure in complex ways")
    def test_list_flatten(self):
        """Test list.flatten() flattens one level of nested lists."""
        # Note: list_flatten is available but changes row count, making it
        # incompatible with with_column() which expects same row count
        pass


# ──────────────────────────────────────
# String Namespace Tests
# ──────────────────────────────────────


class TestStringLength:
    """Tests for string length operations."""

    def test_str_len(self):
        """Test str.len() returns character length."""

        ds = ray.data.from_items([{"name": "Alice"}, {"name": "Bob"}])
        result = ds.with_column("len", col("name").str.len()).to_pandas()
        expected = pd.DataFrame({"name": ["Alice", "Bob"], "len": [5, 3]})
        assert_df_equal(result, expected)

    def test_str_byte_len(self):
        """Test str.byte_len() returns byte length."""

        ds = ray.data.from_items([{"name": "ABC"}])
        result = ds.with_column("byte_len", col("name").str.byte_len()).to_pandas()
        expected = pd.DataFrame({"name": ["ABC"], "byte_len": [3]})
        assert_df_equal(result, expected)


class TestStringCase:
    """Tests for string case conversion."""

    def test_str_upper(self):
        """Test str.upper() converts to uppercase."""

        ds = ray.data.from_items([{"name": "alice"}, {"name": "bob"}])
        result = ds.with_column("upper", col("name").str.upper()).to_pandas()
        expected = pd.DataFrame({"name": ["alice", "bob"], "upper": ["ALICE", "BOB"]})
        assert_df_equal(result, expected)

    def test_str_lower(self):
        """Test str.lower() converts to lowercase."""

        ds = ray.data.from_items([{"name": "ALICE"}, {"name": "BOB"}])
        result = ds.with_column("lower", col("name").str.lower()).to_pandas()
        expected = pd.DataFrame({"name": ["ALICE", "BOB"], "lower": ["alice", "bob"]})
        assert_df_equal(result, expected)

    def test_str_capitalize(self):
        """Test str.capitalize() capitalizes first character."""

        ds = ray.data.from_items([{"name": "alice"}, {"name": "bob"}])
        result = ds.with_column("cap", col("name").str.capitalize()).to_pandas()
        expected = pd.DataFrame({"name": ["alice", "bob"], "cap": ["Alice", "Bob"]})
        assert_df_equal(result, expected)

    def test_str_title(self):
        """Test str.title() converts to title case."""

        ds = ray.data.from_items([{"name": "alice smith"}, {"name": "bob jones"}])
        result = ds.with_column("title", col("name").str.title()).to_pandas()
        expected = pd.DataFrame(
            {
                "name": ["alice smith", "bob jones"],
                "title": ["Alice Smith", "Bob Jones"],
            }
        )
        assert_df_equal(result, expected)

    def test_str_swapcase(self):
        """Test str.swapcase() swaps case."""

        ds = ray.data.from_items([{"name": "AlIcE"}])
        result = ds.with_column("swapped", col("name").str.swapcase()).to_pandas()
        expected = pd.DataFrame({"name": ["AlIcE"], "swapped": ["aLiCe"]})
        assert_df_equal(result, expected)


class TestStringPredicates:
    """Tests for string predicate methods (is_*)."""

    def test_is_alpha(self):
        """Test str.is_alpha() checks for alphabetic characters."""

        ds = ray.data.from_items([{"val": "abc"}, {"val": "abc123"}, {"val": "123"}])
        result = ds.with_column("is_alpha", col("val").str.is_alpha()).to_pandas()
        expected = pd.DataFrame(
            {
                "val": ["abc", "abc123", "123"],
                "is_alpha": [True, False, False],
            }
        )
        assert_df_equal(result, expected)

    def test_is_alnum(self):
        """Test str.is_alnum() checks for alphanumeric characters."""

        ds = ray.data.from_items([{"val": "abc123"}, {"val": "abc-123"}])
        result = ds.with_column("is_alnum", col("val").str.is_alnum()).to_pandas()
        expected = pd.DataFrame(
            {"val": ["abc123", "abc-123"], "is_alnum": [True, False]}
        )
        assert_df_equal(result, expected)

    def test_is_digit(self):
        """Test str.is_digit() checks for digits only."""

        ds = ray.data.from_items([{"val": "123"}, {"val": "12a"}])
        result = ds.with_column("is_digit", col("val").str.is_digit()).to_pandas()
        expected = pd.DataFrame({"val": ["123", "12a"], "is_digit": [True, False]})
        assert_df_equal(result, expected)

    def test_is_space(self):
        """Test str.is_space() checks for whitespace only."""

        ds = ray.data.from_items([{"val": "   "}, {"val": " a "}])
        result = ds.with_column("is_space", col("val").str.is_space()).to_pandas()
        expected = pd.DataFrame({"val": ["   ", " a "], "is_space": [True, False]})
        assert_df_equal(result, expected)

    def test_is_lower(self):
        """Test str.is_lower() checks for lowercase."""

        ds = ray.data.from_items([{"val": "abc"}, {"val": "Abc"}])
        result = ds.with_column("is_lower", col("val").str.is_lower()).to_pandas()
        expected = pd.DataFrame({"val": ["abc", "Abc"], "is_lower": [True, False]})
        assert_df_equal(result, expected)

    def test_is_upper(self):
        """Test str.is_upper() checks for uppercase."""

        ds = ray.data.from_items([{"val": "ABC"}, {"val": "Abc"}])
        result = ds.with_column("is_upper", col("val").str.is_upper()).to_pandas()
        expected = pd.DataFrame({"val": ["ABC", "Abc"], "is_upper": [True, False]})
        assert_df_equal(result, expected)

    def test_is_ascii(self):
        """Test str.is_ascii() checks for ASCII characters."""

        ds = ray.data.from_items([{"val": "hello"}, {"val": "hello😊"}])
        result = ds.with_column("is_ascii", col("val").str.is_ascii()).to_pandas()
        expected = pd.DataFrame({"val": ["hello", "hello😊"], "is_ascii": [True, False]})
        assert_df_equal(result, expected)


class TestStringTrimming:
    """Tests for string trimming operations."""

    def test_strip_whitespace(self):
        """Test str.strip() removes leading and trailing whitespace."""

        ds = ray.data.from_items([{"val": "  hello  "}, {"val": " world "}])
        result = ds.with_column("stripped", col("val").str.strip()).to_pandas()
        expected = pd.DataFrame(
            {"val": ["  hello  ", " world "], "stripped": ["hello", "world"]}
        )
        assert_df_equal(result, expected)

    def test_strip_characters(self):
        """Test str.strip() with custom characters."""

        ds = ray.data.from_items([{"val": "xxxhelloxxx"}])
        result = ds.with_column("stripped", col("val").str.strip("x")).to_pandas()
        expected = pd.DataFrame({"val": ["xxxhelloxxx"], "stripped": ["hello"]})
        assert_df_equal(result, expected)

    def test_lstrip(self):
        """Test str.lstrip() removes leading whitespace."""

        ds = ray.data.from_items([{"val": "  hello  "}])
        result = ds.with_column("lstripped", col("val").str.lstrip()).to_pandas()
        expected = pd.DataFrame({"val": ["  hello  "], "lstripped": ["hello  "]})
        assert_df_equal(result, expected)

    def test_rstrip(self):
        """Test str.rstrip() removes trailing whitespace."""

        ds = ray.data.from_items([{"val": "  hello  "}])
        result = ds.with_column("rstripped", col("val").str.rstrip()).to_pandas()
        expected = pd.DataFrame({"val": ["  hello  "], "rstripped": ["  hello"]})
        assert_df_equal(result, expected)


class TestStringPadding:
    """Tests for string padding operations."""

    def test_pad_right(self):
        """Test str.pad() with right padding."""

        ds = ray.data.from_items([{"val": "hi"}])
        result = ds.with_column(
            "padded", col("val").str.pad(5, fillchar="*", side="right")
        ).to_pandas()
        expected = pd.DataFrame({"val": ["hi"], "padded": ["hi***"]})
        assert_df_equal(result, expected)

    def test_pad_left(self):
        """Test str.pad() with left padding."""

        ds = ray.data.from_items([{"val": "hi"}])
        result = ds.with_column(
            "padded", col("val").str.pad(5, fillchar="*", side="left")
        ).to_pandas()
        expected = pd.DataFrame({"val": ["hi"], "padded": ["***hi"]})
        assert_df_equal(result, expected)

    def test_pad_both(self):
        """Test str.pad() with both-side padding."""

        ds = ray.data.from_items([{"val": "hi"}])
        result = ds.with_column(
            "padded", col("val").str.pad(6, fillchar="*", side="both")
        ).to_pandas()
        expected = pd.DataFrame({"val": ["hi"], "padded": ["**hi**"]})
        assert_df_equal(result, expected)

    def test_center(self):
        """Test str.center() centers strings."""

        ds = ray.data.from_items([{"val": "hi"}])
        result = ds.with_column("centered", col("val").str.center(6, "*")).to_pandas()
        expected = pd.DataFrame({"val": ["hi"], "centered": ["**hi**"]})
        assert_df_equal(result, expected)


class TestStringSearch:
    """Tests for string searching operations."""

    def test_starts_with(self):
        """Test str.starts_with() checks string prefix."""

        ds = ray.data.from_items([{"val": "Alice"}, {"val": "Bob"}, {"val": "Alex"}])
        result = ds.with_column("starts_a", col("val").str.starts_with("A")).to_pandas()
        expected = pd.DataFrame(
            {"val": ["Alice", "Bob", "Alex"], "starts_a": [True, False, True]}
        )
        assert_df_equal(result, expected)

    def test_starts_with_ignore_case(self):
        """Test str.starts_with() with case insensitivity."""

        ds = ray.data.from_items([{"val": "alice"}, {"val": "bob"}])
        result = ds.with_column(
            "starts_a", col("val").str.starts_with("A", ignore_case=True)
        ).to_pandas()
        expected = pd.DataFrame({"val": ["alice", "bob"], "starts_a": [True, False]})
        assert_df_equal(result, expected)

    def test_ends_with(self):
        """Test str.ends_with() checks string suffix."""

        ds = ray.data.from_items([{"val": "Alice"}, {"val": "Bob"}])
        result = ds.with_column("ends_e", col("val").str.ends_with("e")).to_pandas()
        expected = pd.DataFrame({"val": ["Alice", "Bob"], "ends_e": [True, False]})
        assert_df_equal(result, expected)

    def test_contains(self):
        """Test str.contains() checks for substring."""

        ds = ray.data.from_items([{"val": "Alice"}, {"val": "Bob"}, {"val": "Charlie"}])
        result = ds.with_column("has_li", col("val").str.contains("li")).to_pandas()
        expected = pd.DataFrame(
            {"val": ["Alice", "Bob", "Charlie"], "has_li": [True, False, True]}
        )
        assert_df_equal(result, expected)

    def test_find(self):
        """Test str.find() returns index of substring."""

        ds = ray.data.from_items([{"val": "Alice"}, {"val": "Bob"}])
        result = ds.with_column("pos", col("val").str.find("i")).to_pandas()
        expected = pd.DataFrame({"val": ["Alice", "Bob"], "pos": [2, -1]})
        assert_df_equal(result, expected)

    def test_count(self):
        """Test str.count() counts substring occurrences."""

        ds = ray.data.from_items([{"val": "banana"}, {"val": "apple"}])
        result = ds.with_column("count_a", col("val").str.count("a")).to_pandas()
        expected = pd.DataFrame({"val": ["banana", "apple"], "count_a": [3, 1]})
        assert_df_equal(result, expected)

    def test_match_like(self):
        """Test str.match() matches SQL LIKE patterns."""

        ds = ray.data.from_items([{"val": "Alice"}, {"val": "Bob"}, {"val": "Alex"}])
        result = ds.with_column("matches", col("val").str.match("Al%")).to_pandas()
        expected = pd.DataFrame(
            {"val": ["Alice", "Bob", "Alex"], "matches": [True, False, True]}
        )
        assert_df_equal(result, expected)


class TestStringTransform:
    """Tests for string transformation operations."""

    def test_reverse(self):
        """Test str.reverse() reverses strings."""

        ds = ray.data.from_items([{"val": "hello"}, {"val": "world"}])
        result = ds.with_column("rev", col("val").str.reverse()).to_pandas()
        expected = pd.DataFrame({"val": ["hello", "world"], "rev": ["olleh", "dlrow"]})
        assert_df_equal(result, expected)

    def test_slice(self):
        """Test str.slice() extracts substring."""

        ds = ray.data.from_items([{"val": "hello"}])
        result = ds.with_column("sliced", col("val").str.slice(1, 4)).to_pandas()
        expected = pd.DataFrame({"val": ["hello"], "sliced": ["ell"]})
        assert_df_equal(result, expected)

    def test_replace(self):
        """Test str.replace() replaces substring."""

        ds = ray.data.from_items([{"val": "hello world"}])
        result = ds.with_column(
            "replaced", col("val").str.replace("world", "universe")
        ).to_pandas()
        expected = pd.DataFrame(
            {"val": ["hello world"], "replaced": ["hello universe"]}
        )
        assert_df_equal(result, expected)

    def test_replace_with_max(self):
        """Test str.replace() with max_replacements."""

        ds = ray.data.from_items([{"val": "aaa"}])
        result = ds.with_column(
            "replaced", col("val").str.replace("a", "X", max_replacements=2)
        ).to_pandas()
        expected = pd.DataFrame({"val": ["aaa"], "replaced": ["XXa"]})
        assert_df_equal(result, expected)

    def test_repeat(self):
        """Test str.repeat() repeats strings."""

        ds = ray.data.from_items([{"val": "A"}])
        result = ds.with_column("repeated", col("val").str.repeat(3)).to_pandas()
        expected = pd.DataFrame({"val": ["A"], "repeated": ["AAA"]})
        assert_df_equal(result, expected)


# ──────────────────────────────────────
# Struct Namespace Tests
# ──────────────────────────────────────


class TestStructNamespace:
    """Tests for struct namespace operations."""

    @pytest.fixture
    def struct_ds(self):
        """Dataset with struct columns."""
        return ray.data.from_arrow(
            pa.table(
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
        )

    @pytest.fixture
    def nested_struct_ds(self):
        """Dataset with nested struct columns."""
        return ray.data.from_arrow(
            pa.table(
                {
                    "user": pa.array(
                        [
                            {
                                "name": "Alice",
                                "address": {"city": "NYC", "zip": "10001"},
                            },
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
        )

    def test_struct_field(self, struct_ds):
        """Test struct.field() extracts field."""

        result = struct_ds.with_column(
            "age", col("user").struct.field("age")
        ).to_pandas()
        expected = pd.DataFrame(
            {
                "user": [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}],
                "age": [30, 25],
            }
        )
        assert_df_equal(result, expected)

    def test_struct_bracket(self, struct_ds):
        """Test struct['field'] bracket notation."""

        result = struct_ds.with_column("name", col("user").struct["name"]).to_pandas()
        expected = pd.DataFrame(
            {
                "user": [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}],
                "name": ["Alice", "Bob"],
            }
        )
        assert_df_equal(result, expected)

    def test_struct_nested_field(self, nested_struct_ds):
        """Test nested struct field access with .field()."""

        result = nested_struct_ds.with_column(
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

    def test_struct_nested_bracket(self, nested_struct_ds):
        """Test nested struct field access with brackets."""

        result = nested_struct_ds.with_column(
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
# Integration Tests
# ──────────────────────────────────────


class TestNamespaceIntegration:
    """Tests for chaining and combining namespace expressions."""

    def test_list_with_arithmetic(self):
        """Test list operations combined with arithmetic."""

        ds = ray.data.from_items([{"items": [1, 2, 3]}])
        result = ds.with_column("len_plus_one", col("items").list.len() + 1).to_pandas()
        expected = pd.DataFrame({"items": [[1, 2, 3]], "len_plus_one": [4]})
        assert_df_equal(result, expected)

    def test_string_with_comparison(self):
        """Test string operations combined with comparison."""

        ds = ray.data.from_items([{"name": "Alice"}, {"name": "Bo"}])
        result = ds.with_column("long_name", col("name").str.len() > 3).to_pandas()
        expected = pd.DataFrame({"name": ["Alice", "Bo"], "long_name": [True, False]})
        assert_df_equal(result, expected)

    def test_multiple_operations(self):
        """Test multiple namespace operations in single pipeline."""

        ds = ray.data.from_items([{"name": "alice"}])
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
