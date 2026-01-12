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


@pytest.fixture
def map_dataset():
    """Fixture that creates a dataset backed by an Arrow MapArray column."""
    map_items = [
        {"attrs": {"color": "red", "size": "M"}},
        {"attrs": {"brand": "Ray"}},
    ]
    map_type = pa.map_(pa.string(), pa.string())
    arrow_table = pa.table(
        {"attrs": pa.array([row["attrs"] for row in map_items], type=map_type)}
    )
    return ray.data.from_arrow(arrow_table)


def _assert_result(result_df: pd.DataFrame, expected_df: pd.DataFrame, drop_cols: list):
    """Helper to drop columns and assert equality."""
    result_df = result_df.drop(columns=drop_cols)
    assert rows_same(result_df, expected_df)


class TestMapNamespace:
    """Tests for map namespace operations using the shared map_dataset fixture."""

    def test_map_keys(self, map_dataset):
        result = map_dataset.with_column("keys", col("attrs").map.keys()).to_pandas()
        expected = pd.DataFrame({"keys": [["color", "size"], ["brand"]]})
        _assert_result(result, expected, drop_cols=["attrs"])

    def test_map_values(self, map_dataset):
        result = map_dataset.with_column(
            "values", col("attrs").map.values()
        ).to_pandas()
        expected = pd.DataFrame({"values": [["red", "M"], ["Ray"]]})
        _assert_result(result, expected, drop_cols=["attrs"])

    def test_map_chaining(self, map_dataset):
        # map.keys() returns a list, so .list.len() should apply
        result = map_dataset.with_column(
            "num_keys", col("attrs").map.keys().list.len()
        ).to_pandas()
        expected = pd.DataFrame({"num_keys": [2, 1]})
        _assert_result(result, expected, drop_cols=["attrs"])


def test_physical_map_extraction():
    """Test extraction works on List<Struct> (Physical Maps)."""
    # Construct List<Struct<k, v>>
    struct_type = pa.struct([pa.field("k", pa.string()), pa.field("v", pa.int64())])
    list_type = pa.list_(struct_type)

    data_py = [[{"k": "a", "v": 1}], [{"k": "b", "v": 2}]]
    arrow_table = pa.Table.from_arrays(
        [pa.array(data_py, type=list_type)], names=["data"]
    )
    ds = ray.data.from_arrow(arrow_table)

    result = (
        ds.with_column("keys", col("data").map.keys())
        .with_column("values", col("data").map.values())
        .to_pandas()
    )

    expected = pd.DataFrame(
        {
            "data": data_py,
            "keys": [["a"], ["b"]],
            "values": [[1], [2]],
        }
    )
    assert rows_same(result, expected)


def test_map_sliced_offsets():
    """Test extraction works correctly on sliced Arrow arrays (offset > 0)."""
    items = [{"m": {"id": i}} for i in range(10)]
    map_type = pa.map_(pa.string(), pa.int64())
    arrays = pa.array([row["m"] for row in items], type=map_type)
    table = pa.Table.from_arrays([arrays], names=["m"])

    # Force offsets by slicing the table before ingestion
    sliced_table = table.slice(offset=7, length=3)
    ds = ray.data.from_arrow(sliced_table)

    result = ds.with_column("vals", col("m").map.values()).to_pandas()
    expected = pd.DataFrame({"vals": [[7], [8], [9]]})
    _assert_result(result, expected, drop_cols=["m"])


def test_map_nulls_and_empty():
    """Test handling of null maps and empty maps."""
    items_data = [{"m": {"a": 1}}, {"m": {}}, {"m": None}]

    map_type = pa.map_(pa.string(), pa.int64())
    arrays = pa.array([row["m"] for row in items_data], type=map_type)
    arrow_table = pa.Table.from_arrays([arrays], names=["m"])
    ds = ray.data.from_arrow(arrow_table)

    # Use take_all() to avoid pandas casting errors with mixed None/list types
    rows = (
        ds.with_column("keys", col("m").map.keys())
        .with_column("values", col("m").map.values())
        .take_all()
    )

    assert list(rows[0]["keys"]) == ["a"] and list(rows[0]["values"]) == [1]
    assert len(rows[1]["keys"]) == 0 and len(rows[1]["values"]) == 0
    assert rows[2]["keys"] is None and rows[2]["values"] is None


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
