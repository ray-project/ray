"""Integration tests for struct namespace expressions.

These tests require Ray and test end-to-end struct namespace expression evaluation.
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
class TestStructNamespace:
    """Tests for struct namespace operations."""

    def test_struct_field(self, ray_start_regular_shared, dataset_format):
        """Test struct.field() extracts field."""
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

        result = ds.with_column("age", col("user").struct.field("age")).to_pandas()
        expected = pd.DataFrame(
            {
                "user": [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}],
                "age": [30, 25],
            }
        )
        assert rows_same(result, expected)

    def test_struct_bracket(self, ray_start_regular_shared, dataset_format):
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

    def test_struct_nested_field(self, ray_start_regular_shared, dataset_format):
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
        assert rows_same(result, expected)

    def test_struct_nested_bracket(self, ray_start_regular_shared, dataset_format):
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
        assert rows_same(result, expected)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
