import numpy as np
import pytest

import ray
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa


def test_read_zarr(ray_start_regular_shared, tmp_path):
    """Test reading zarr arrays: groups, single arrays, nested groups, multiple paths."""
    zarr = pytest.importorskip("zarr")

    # Test 1: Group with multiple arrays
    store_path = tmp_path / "group.zarr"
    root = zarr.open(str(store_path), mode="w")
    arr1 = root.create_array("arr1", shape=(4, 4), chunks=(2, 2), dtype=np.int64)
    arr1[:] = np.arange(16).reshape(4, 4)
    arr2 = root.create_array("arr2", shape=(4,), chunks=(2,), dtype=np.float32)
    arr2[:] = np.ones((4,), dtype=np.float32)

    ds = ray.data.read_zarr(str(store_path))

    # Verify schema
    schema = ds.schema()
    assert set(schema.names) == {"array_name", "data", "shape", "dtype", "chunk_index"}

    # Verify chunk count and data (4 chunks from arr1 + 2 from arr2 = 6)
    rows = ds.take_all()
    assert len(rows) == 6
    arr1_rows = [r for r in rows if r["array_name"] == "arr1"]
    arr2_rows = [r for r in rows if r["array_name"] == "arr2"]
    assert len(arr1_rows) == 4
    assert len(arr2_rows) == 2
    assert arr1_rows[0]["dtype"] == "int64"
    assert arr2_rows[0]["dtype"] == "float32"
    assert arr1_rows[0]["shape"] == [2, 2]
    assert arr2_rows[0]["shape"] == [2]

    # Test 2: Single array (not a group)
    arr = zarr.open(
        str(tmp_path / "single.zarr"),
        mode="w",
        shape=(4,),
        chunks=(2,),
        dtype=np.int64,
    )
    arr[:] = np.arange(4)

    ds = ray.data.read_zarr(str(tmp_path / "single.zarr"))
    rows = ds.take_all()
    assert len(rows) == 2
    assert all(row["array_name"] == "" for row in rows)

    # Test 3: Nested groups
    store_path = tmp_path / "nested.zarr"
    root = zarr.open(str(store_path), mode="w")
    root.create_group("level1")
    root["level1"].create_group("level2")
    arr = root["level1"]["level2"].create_array(
        "arr", shape=(4,), chunks=(2,), dtype=np.float64
    )
    arr[:] = np.ones((4,))

    ds = ray.data.read_zarr(str(store_path))
    rows = ds.take_all()
    assert len(rows) == 2
    assert rows[0]["array_name"] == "level1/level2/arr"

    # Test 4: Multiple paths
    arr1 = zarr.open(
        str(tmp_path / "store1.zarr"),
        mode="w",
        shape=(4,),
        chunks=(2,),
        dtype=np.int64,
    )
    arr1[:] = np.arange(4)
    arr2 = zarr.open(
        str(tmp_path / "store2.zarr"),
        mode="w",
        shape=(4,),
        chunks=(2,),
        dtype=np.int64,
    )
    arr2[:] = np.arange(4, 8)

    ds = ray.data.read_zarr(
        [str(tmp_path / "store1.zarr"), str(tmp_path / "store2.zarr")]
    )
    rows = ds.take_all()
    assert len(rows) == 4  # 2 chunks from each store

    # Test 5: Empty array
    zarr.open(
        str(tmp_path / "empty.zarr"), mode="w", shape=(0,), chunks=(1,), dtype=np.int64
    )

    ds = ray.data.read_zarr(str(tmp_path / "empty.zarr"))
    assert len(ds.take_all()) == 0


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
