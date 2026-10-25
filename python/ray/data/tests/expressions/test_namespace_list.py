"""Integration tests for list namespace expressions.

These tests require Ray and test end-to-end list namespace expression evaluation.
"""

import pandas as pd
import pyarrow as pa
import pytest
from packaging import version

import ray
from ray.data._internal.util import rows_same
from ray.data.expressions import col
from ray.data.tests.conftest import *  # noqa
from ray.exceptions import RayTaskError
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
class TestListNamespace:
    """Tests for list namespace operations."""

    def test_list_len(self, ray_start_regular_shared, dataset_format):
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

    def test_list_get(self, ray_start_regular_shared, dataset_format):
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

    def test_list_bracket_index(self, ray_start_regular_shared, dataset_format):
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

    def test_list_with_arithmetic(self, ray_start_regular_shared, dataset_format):
        """Test list operations combined with arithmetic."""
        data = [{"items": [1, 2, 3]}]
        ds = _create_dataset(data, dataset_format)
        result = ds.with_column("len_plus_one", col("items").list.len() + 1).to_pandas()
        expected = pd.DataFrame({"items": [[1, 2, 3]], "len_plus_one": [4]})
        assert rows_same(result, expected)

    def test_list_sort(self, ray_start_regular_shared, dataset_format):
        """Test list.sort() sorts each list with custom options."""
        data = [
            {"items": [3, 1, 2]},
            {"items": [None, 4, 2]},
        ]
        ds = _create_dataset(data, dataset_format)
        method = col("items").list.sort(order="descending", null_placement="at_start")
        result = ds.with_column("sorted", method).to_pandas()
        expected = pd.DataFrame(
            {
                "items": [[3, 1, 2], [None, 4, 2]],
                "sorted": [[3, 2, 1], [None, 4, 2]],
            }
        )
        assert rows_same(result, expected)

    def test_list_flatten(self, ray_start_regular_shared, dataset_format):
        """Test list.flatten() removes one nesting level."""
        data = [
            {"items": [[1, 2], [3]]},
            {"items": [[], [4, 5]]},
        ]
        ds = _create_dataset(data, dataset_format)
        result = ds.with_column("flattened", col("items").list.flatten()).to_pandas()
        expected = pd.DataFrame(
            {
                "items": [[[1, 2], [3]], [[], [4, 5]]],
                "flattened": [[1, 2, 3], [4, 5]],
            }
        )
        assert rows_same(result, expected)

    def test_list_flatten_requires_nested_lists(
        self, ray_start_regular_shared, dataset_format
    ):
        """list.flatten() should raise if elements aren't lists."""
        data = [{"items": [1, 2]}, {"items": [3, 4]}]
        ds = _create_dataset(data, dataset_format)
        with pytest.raises(RayTaskError):
            ds.with_column("flattened", col("items").list.flatten()).materialize()

    def test_list_flatten_large_list_type(
        self, ray_start_regular_shared, dataset_format
    ):
        """Flatten should preserve LargeList type when present."""
        if dataset_format != "arrow":
            pytest.skip("LargeList type only available via Arrow tables.")

        arrow_type = pa.large_list(pa.list_(pa.int64()))
        table = pa.Table.from_arrays(
            [
                pa.array(
                    [
                        [[1, 2], [3]],
                        [[], [4, 5]],
                    ],
                    type=arrow_type,
                )
            ],
            names=["items"],
        )
        ds = _create_dataset(None, dataset_format, arrow_table=table)
        result = ds.with_column("flattened", col("items").list.flatten())
        arrow_refs = result.to_arrow_refs()
        tables = ray.get(arrow_refs)
        result_table = pa.concat_tables(tables) if len(tables) > 1 else tables[0]

        flattened_type = result_table.schema.field("flattened").type
        assert flattened_type == pa.large_list(pa.int64())
        expected = pa.Table.from_arrays(
            [
                pa.array([[1, 2, 3], [4, 5]], type=pa.large_list(pa.int64())),
            ],
            names=["flattened"],
        )
        assert result_table.select(["flattened"]).combine_chunks().equals(expected)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
