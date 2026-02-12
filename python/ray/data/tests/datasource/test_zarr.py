"""Tests for Zarr datasource."""

import numpy as np
import pytest

import ray

zarr = pytest.importorskip("zarr")


# =============================================================================
# Unit tests for helper functions (no Ray required)
# =============================================================================


class TestGetChunkPath:
    """Test _get_chunk_path helper function."""

    def test_zarr_v3_root_array(self):
        from ray.data._internal.datasource.zarr_datasource import _get_chunk_path

        # zarr v3 root array: c/0/1
        assert _get_chunk_path("", (0, 1), is_zarr_v3=True) == "c/0/1"
        assert _get_chunk_path("", (0,), is_zarr_v3=True) == "c/0"
        assert _get_chunk_path("", (2, 3, 4), is_zarr_v3=True) == "c/2/3/4"

    def test_zarr_v3_named_array(self):
        from ray.data._internal.datasource.zarr_datasource import _get_chunk_path

        # zarr v3 named array: array_name/c/0/1
        assert _get_chunk_path("myarray", (0, 1), is_zarr_v3=True) == "myarray/c/0/1"
        assert _get_chunk_path("nested/arr", (0,), is_zarr_v3=True) == "nested/arr/c/0"

    def test_zarr_v2_dot_separator(self):
        from ray.data._internal.datasource.zarr_datasource import _get_chunk_path

        # zarr v2 with "." separator (default): 0.1
        assert _get_chunk_path("", (0, 1), is_zarr_v3=False) == "0.1"
        assert _get_chunk_path("", (0,), is_zarr_v3=False) == "0"
        assert _get_chunk_path("myarray", (2, 3), is_zarr_v3=False) == "myarray/2.3"

    def test_zarr_v2_slash_separator(self):
        from ray.data._internal.datasource.zarr_datasource import _get_chunk_path

        # zarr v2 with "/" separator: 0/1
        result = _get_chunk_path("", (0, 1), is_zarr_v3=False, dimension_separator="/")
        assert result == "0/1"
        result = _get_chunk_path(
            "myarray", (2, 3), is_zarr_v3=False, dimension_separator="/"
        )
        assert result == "myarray/2/3"

    def test_numeric_array_name(self):
        from ray.data._internal.datasource.zarr_datasource import _get_chunk_path

        # Array with numeric name should not be confused with chunk index
        assert _get_chunk_path("123", (0, 1), is_zarr_v3=False) == "123/0.1"
        assert _get_chunk_path("0", (0,), is_zarr_v3=True) == "0/c/0"


class TestGetChunkIndices:
    """Test _get_chunk_indices helper function."""

    def test_single_chunk(self):
        from ray.data._internal.datasource.zarr_datasource import _get_chunk_indices

        arr = zarr.zeros((10,), chunks=(10,))
        indices = _get_chunk_indices(arr)
        assert indices == [(0,)]

    def test_multiple_chunks_1d(self):
        from ray.data._internal.datasource.zarr_datasource import _get_chunk_indices

        arr = zarr.zeros((100,), chunks=(25,))
        indices = _get_chunk_indices(arr)
        assert indices == [(0,), (1,), (2,), (3,)]

    def test_multiple_chunks_2d(self):
        from ray.data._internal.datasource.zarr_datasource import _get_chunk_indices

        arr = zarr.zeros((10, 10), chunks=(5, 5))
        indices = _get_chunk_indices(arr)
        assert set(indices) == {(0, 0), (0, 1), (1, 0), (1, 1)}

    def test_uneven_chunks(self):
        from ray.data._internal.datasource.zarr_datasource import _get_chunk_indices

        # 10 elements with chunk size 4 = 3 chunks (4+4+2)
        arr = zarr.zeros((10,), chunks=(4,))
        indices = _get_chunk_indices(arr)
        assert indices == [(0,), (1,), (2,)]


class TestGetArrays:
    """Test _get_arrays helper function."""

    def test_single_array(self, tmp_path):
        from ray.data._internal.datasource.zarr_datasource import _get_arrays

        arr = zarr.open(str(tmp_path / "arr.zarr"), mode="w", shape=(10,), chunks=(5,))
        arr[:] = np.arange(10)
        store = zarr.open(str(tmp_path / "arr.zarr"), mode="r")

        arrays = _get_arrays(store)
        assert len(arrays) == 1
        assert arrays[0][0] == ""  # Root array has empty name

    def test_group_with_arrays(self, tmp_path):
        from ray.data._internal.datasource.zarr_datasource import _get_arrays

        root = zarr.open(str(tmp_path / "group.zarr"), mode="w")
        root.create_array("arr1", shape=(10,), dtype=np.int64)
        root["arr1"][:] = np.arange(10)
        root.create_array("arr2", shape=(5, 5), dtype=np.float64)
        root["arr2"][:] = np.ones((5, 5))

        store = zarr.open(str(tmp_path / "group.zarr"), mode="r")
        arrays = _get_arrays(store)

        names = {name for name, _ in arrays}
        assert names == {"arr1", "arr2"}

    def test_nested_groups(self, tmp_path):
        from ray.data._internal.datasource.zarr_datasource import _get_arrays

        root = zarr.open(str(tmp_path / "nested.zarr"), mode="w")
        root.create_group("subgroup")
        root["subgroup"].create_array("nested_arr", shape=(5,), dtype=np.int64)
        root["subgroup/nested_arr"][:] = np.arange(5)
        root.create_array("top_arr", shape=(3,), dtype=np.float64)
        root["top_arr"][:] = np.ones(3)

        store = zarr.open(str(tmp_path / "nested.zarr"), mode="r")
        arrays = _get_arrays(store)

        names = {name for name, _ in arrays}
        assert names == {"subgroup/nested_arr", "top_arr"}


# =============================================================================
# Unit tests for zarr format detection
# =============================================================================


class TestZarrFormats:
    """Test reading zarr v2 and v3 formats."""

    def test_zarr_v2_dot_separator(self, tmp_path):
        """Test zarr v2 with default '.' dimension separator."""
        from ray.data._internal.datasource.zarr_datasource import ZarrDatasource

        # Create v2 store with "." separator (default)
        arr = zarr.open(
            str(tmp_path / "v2_dot.zarr"),
            mode="w",
            shape=(10, 10),
            chunks=(5, 5),
            zarr_format=2,
        )
        arr[:] = np.arange(100).reshape(10, 10)

        # Verify it works
        ds = ZarrDatasource(str(tmp_path / "v2_dot.zarr"))
        tasks = ds.get_read_tasks(parallelism=4)
        assert len(tasks) == 4  # 2x2 chunks

    def test_zarr_v2_slash_separator(self, tmp_path):
        """Test zarr v2 with '/' dimension separator."""
        from ray.data._internal.datasource.zarr_datasource import ZarrDatasource

        # Create v2 store with "/" separator
        arr = zarr.open(
            str(tmp_path / "v2_slash.zarr"),
            mode="w",
            shape=(10, 10),
            chunks=(5, 5),
            zarr_format=2,
            dimension_separator="/",
        )
        arr[:] = np.arange(100).reshape(10, 10)

        # Verify it works
        ds = ZarrDatasource(str(tmp_path / "v2_slash.zarr"))
        tasks = ds.get_read_tasks(parallelism=4)
        assert len(tasks) == 4  # 2x2 chunks

    def test_zarr_v3(self, tmp_path):
        """Test zarr v3 format."""
        from ray.data._internal.datasource.zarr_datasource import ZarrDatasource

        # Create v3 store (default in zarr 3.x)
        arr = zarr.open(
            str(tmp_path / "v3.zarr"),
            mode="w",
            shape=(10, 10),
            chunks=(5, 5),
        )
        arr[:] = np.arange(100).reshape(10, 10)

        # Verify it works
        ds = ZarrDatasource(str(tmp_path / "v3.zarr"))
        tasks = ds.get_read_tasks(parallelism=4)
        assert len(tasks) == 4  # 2x2 chunks


# =============================================================================
# Integration tests with Ray Data
# =============================================================================


def test_read_zarr_single_array(ray_start_regular_shared, tmp_path):
    """Test reading a single zarr array."""
    arr = zarr.open(str(tmp_path / "single.zarr"), mode="w", shape=(100,), chunks=(25,))
    arr[:] = np.arange(100)

    ds = ray.data.read_zarr(str(tmp_path / "single.zarr"))

    assert ds.count() == 4  # 100 / 25 = 4 chunks
    rows = ds.take_all()
    assert all(row["array_name"] == "" for row in rows)


def test_read_zarr_group(ray_start_regular_shared, tmp_path):
    """Test reading a zarr group with multiple arrays."""
    root = zarr.open(str(tmp_path / "group.zarr"), mode="w")
    root.create_array("arr1", shape=(10, 10), chunks=(5, 5), dtype=np.int64)
    root["arr1"][:] = np.arange(100).reshape(10, 10)
    root.create_array("arr2", shape=(20,), chunks=(10,), dtype=np.float64)
    root["arr2"][:] = np.ones(20)

    ds = ray.data.read_zarr(str(tmp_path / "group.zarr"))

    # arr1: 4 chunks (2x2), arr2: 2 chunks
    assert ds.count() == 6


def test_read_zarr_multiple_paths(ray_start_regular_shared, tmp_path):
    """Test reading from multiple zarr stores."""
    arr1 = zarr.open(str(tmp_path / "store1.zarr"), mode="w", shape=(10,), chunks=(5,))
    arr1[:] = np.arange(10)
    arr2 = zarr.open(str(tmp_path / "store2.zarr"), mode="w", shape=(10,), chunks=(5,))
    arr2[:] = np.arange(10, 20)

    ds = ray.data.read_zarr(
        [str(tmp_path / "store1.zarr"), str(tmp_path / "store2.zarr")]
    )

    assert ds.count() == 4  # 2 chunks from each store


def test_read_zarr_data_integrity(ray_start_regular_shared, tmp_path):
    """Test that data is read correctly."""
    data = np.arange(100).reshape(10, 10)
    arr = zarr.open(
        str(tmp_path / "test.zarr"), mode="w", shape=data.shape, chunks=(5, 5)
    )
    arr[:] = data

    ds = ray.data.read_zarr(str(tmp_path / "test.zarr"))
    rows = ds.take_all()

    assert len(rows) == 4
    for row in rows:
        chunk_data = np.frombuffer(row["data"], dtype=row["dtype"]).reshape(
            row["shape"]
        )
        assert chunk_data.shape == (5, 5)


def test_read_zarr_nested_group(ray_start_regular_shared, tmp_path):
    """Test reading nested groups."""
    root = zarr.open(str(tmp_path / "nested.zarr"), mode="w")
    root.create_group("subgroup")
    root["subgroup"].create_array("arr", shape=(10,), chunks=(5,), dtype=np.float64)
    root["subgroup/arr"][:] = np.ones(10)

    ds = ray.data.read_zarr(str(tmp_path / "nested.zarr"))
    rows = ds.take_all()

    assert len(rows) == 2
    assert all(row["array_name"] == "subgroup/arr" for row in rows)


def test_read_zarr_schema(ray_start_regular_shared, tmp_path):
    """Test that the output schema is correct."""
    arr = zarr.open(str(tmp_path / "test.zarr"), mode="w", shape=(10,), chunks=(5,))
    arr[:] = np.arange(10)

    ds = ray.data.read_zarr(str(tmp_path / "test.zarr"))
    row = ds.take(1)[0]

    assert "array_name" in row
    assert "data" in row
    assert "shape" in row
    assert "dtype" in row
    assert "chunk_index" in row


if __name__ == "__main__":
    pytest.main(["-v", __file__])
