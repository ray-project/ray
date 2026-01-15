"""Integration tests for string namespace expressions.

These tests require Ray and test end-to-end string namespace expression evaluation.
"""

import pandas as pd
import pyarrow as pa
import pytest
from packaging import version

import ray
from ray.data._internal.util import rows_same
from ray.data.expressions import col
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa

pytestmark = pytest.mark.skipif(
    version.parse(pa.__version__) < version.parse("19.0.0"),
    reason="Namespace expressions tests require PyArrow >= 19.0",
)


def _create_dataset(items_data, dataset_format, arrow_table=None):
    if dataset_format == "arrow":
        if arrow_table is not None:
            ds = ray.data.from_arrow(arrow_table)
        else:
            table = pa.Table.from_pylist(items_data)
            ds = ray.data.from_arrow(table)
    elif dataset_format == "pandas":
        if arrow_table is not None:
            df = arrow_table.to_pandas()
        else:
            df = pd.DataFrame(items_data)
        ds = ray.data.from_blocks([df])
    return ds


DATASET_FORMATS = ["pandas", "arrow"]


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
        self,
        ray_start_regular_shared,
        dataset_format,
        method_name,
        input_values,
        expected_results,
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
        self,
        ray_start_regular_shared,
        dataset_format,
        method_name,
        input_values,
        expected_values,
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
        self,
        ray_start_regular_shared,
        dataset_format,
        method_name,
        input_values,
        expected_results,
    ):
        """Test string predicate methods."""
        data = [{"val": v} for v in input_values]
        ds = _create_dataset(data, dataset_format)

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
        self,
        ray_start_regular_shared,
        dataset_format,
        method_name,
        method_args,
        input_values,
        expected_values,
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
        ("lpad", {"width": 5, "padding": "*"}, "***hi"),
        ("rpad", {"width": 5, "padding": "*"}, "hi***"),
        ("center", {"width": 6, "padding": "*"}, "**hi**"),
    ],
)
class TestStringPadding:
    """Tests for string padding operations."""

    def test_string_padding(
        self,
        ray_start_regular_shared,
        dataset_format,
        method_name,
        method_kwargs,
        expected_value,
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
        ray_start_regular_shared,
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

    def test_reverse(self, ray_start_regular_shared, dataset_format):
        """Test str.reverse() reverses strings."""
        data = [{"val": "hello"}, {"val": "world"}]
        ds = _create_dataset(data, dataset_format)
        result = ds.with_column("rev", col("val").str.reverse()).to_pandas()
        expected = pd.DataFrame({"val": ["hello", "world"], "rev": ["olleh", "dlrow"]})
        assert rows_same(result, expected)

    def test_slice(self, ray_start_regular_shared, dataset_format):
        """Test str.slice() extracts substring."""
        data = [{"val": "hello"}]
        ds = _create_dataset(data, dataset_format)
        result = ds.with_column("sliced", col("val").str.slice(1, 4)).to_pandas()
        expected = pd.DataFrame({"val": ["hello"], "sliced": ["ell"]})
        assert rows_same(result, expected)

    def test_replace(self, ray_start_regular_shared, dataset_format):
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

    def test_replace_with_max(self, ray_start_regular_shared, dataset_format):
        """Test str.replace() with max_replacements."""
        data = [{"val": "aaa"}]
        ds = _create_dataset(data, dataset_format)
        result = ds.with_column(
            "replaced", col("val").str.replace("a", "X", max_replacements=2)
        ).to_pandas()
        expected = pd.DataFrame({"val": ["aaa"], "replaced": ["XXa"]})
        assert rows_same(result, expected)

    def test_repeat(self, ray_start_regular_shared, dataset_format):
        """Test str.repeat() repeats strings."""
        data = [{"val": "A"}]
        ds = _create_dataset(data, dataset_format)
        result = ds.with_column("repeated", col("val").str.repeat(3)).to_pandas()
        expected = pd.DataFrame({"val": ["A"], "repeated": ["AAA"]})
        assert rows_same(result, expected)

    def test_string_with_comparison(self, ray_start_regular_shared, dataset_format):
        """Test string operations combined with comparison."""
        data = [{"name": "Alice"}, {"name": "Bo"}]
        ds = _create_dataset(data, dataset_format)
        result = ds.with_column("long_name", col("name").str.len() > 3).to_pandas()
        expected = pd.DataFrame({"name": ["Alice", "Bo"], "long_name": [True, False]})
        assert rows_same(result, expected)

    def test_multiple_string_operations(self, ray_start_regular_shared, dataset_format):
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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
