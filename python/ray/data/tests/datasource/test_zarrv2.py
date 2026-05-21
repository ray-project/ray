import json
import os
from pathlib import Path
from unittest.mock import patch

import fsspec
import numpy as np
import pandas as pd
import pyarrow.fs
import pytest
import zarr
from pytest_lazy_fixtures import lf as lazy_fixture

import ray
import ray.data.read_api as read_api
from ray.data._internal.datasource import zarrv2_datasource
from ray.data.tests.conftest import *  # noqa: F401, F403


def _execute_read_tasks(tasks) -> pd.DataFrame:
    frames = [block for task in tasks for block in task()]
    return pd.concat(frames, ignore_index=True)


def _write_real_zarr_store(
    store_path: Path,
    arrays: dict,  # {name: (data, chunks)}
) -> Path:
    """Write a real Zarr v2 store from numpy arrays and consolidate metadata."""
    root = zarr.open_group(str(store_path), mode="w")
    for name, (data, chunks) in arrays.items():
        root.create_dataset(name, data=data, chunks=chunks, dtype=data.dtype)
    zarr.consolidate_metadata(str(store_path))
    return store_path


def _write_unconsolidated_zarr_store(
    store_path: Path,
    arrays: dict,  # {name: (data, chunks)}
) -> Path:
    """Write a Zarr v2 store WITHOUT a consolidated ``.zmetadata`` file.

    Exercises the no-``.zmetadata`` code paths (per-array ``.zarray``
    discovery and full-store walk) — the common shape of real-world stores
    behind plain HTTPS or other listing-less filesystems.
    """
    root = zarr.open_group(str(store_path), mode="w")
    for name, (data, chunks) in arrays.items():
        root.create_dataset(name, data=data, chunks=chunks, dtype=data.dtype)
    return store_path


@pytest.fixture
def zarrv2_group_store(tmp_path) -> Path:
    """Group-rooted store with two arrays aligned on axis 0 (shape[0] == 5)."""
    return _write_real_zarr_store(
        tmp_path / "group.zarr",
        {
            "images": (np.arange(20, dtype="<i4").reshape(5, 4), (2, 4)),
            "nested": (np.arange(5, dtype="|u1"), (2,)),
        },
    )


@pytest.fixture
def zarrv2_root_store(tmp_path) -> Path:
    """Single-array store with the array sitting directly at the store root."""
    store_path = tmp_path / "root.zarr"
    arr = zarr.open(
        str(store_path),
        mode="w",
        shape=(5, 4),
        chunks=(2, 4),
        dtype="<i4",
    )
    arr[:] = np.arange(20, dtype="<i4").reshape(5, 4)
    zarr.consolidate_metadata(str(store_path))
    return store_path


@pytest.fixture
def local_fsspec_fs():
    """fsspec local filesystem (for parametrized cross-fs read tests)."""
    return fsspec.filesystem("file")


@pytest.fixture
def misaligned_zarrv2_store(tmp_path) -> Path:
    """Store with two arrays that disagree on axis 0 — for alignment-check tests."""
    return _write_real_zarr_store(
        tmp_path / "misaligned.zarr",
        {
            "images": (np.arange(20, dtype="<i4").reshape(5, 4), (2, 4)),
            "labels": (np.arange(3, dtype="|u1"), (2,)),
        },
    )


@pytest.fixture
def unconsolidated_zarrv2_store(tmp_path) -> Path:
    """Two axis-0-aligned arrays at the store root, no ``.zmetadata``."""
    return _write_unconsolidated_zarr_store(
        tmp_path / "unconsolidated.zarr",
        {
            "images": (np.arange(20, dtype="<i4").reshape(5, 4), (2, 4)),
            "nested": (np.arange(5, dtype="|u1"), (2,)),
        },
    )


# ---------------------------------------------------------------------------
# Metadata discovery and alignment validation
# ---------------------------------------------------------------------------


def test_normalizes_requested_root_array_path(zarrv2_root_store):
    """The empty string refers to the root array."""
    datasource = zarrv2_datasource.ZarrV2Datasource(
        str(zarrv2_root_store),
        array_paths=[""],
    )
    assert list(datasource._selected_arrays) == [""]


def test_normalizes_requested_array_paths(zarrv2_group_store):
    datasource = zarrv2_datasource.ZarrV2Datasource(
        str(zarrv2_group_store),
        array_paths=["images/", "nested"],
    )
    assert list(datasource._selected_arrays) == ["images", "nested"]


def test_rejects_missing_array_paths(zarrv2_group_store):
    with pytest.raises(
        ValueError,
        match=r"Array\(s\) not found: 'missing'\. Available: 'images', 'nested'",
    ):
        zarrv2_datasource.ZarrV2Datasource(
            str(zarrv2_group_store),
            array_paths=["missing"],
        )


def test_rejects_arrays_with_different_shape0(misaligned_zarrv2_store):
    with pytest.raises(
        ValueError,
        match=r"Arrays in a single read_zarr call must share axis 0",
    ):
        zarrv2_datasource.ZarrV2Datasource(str(misaligned_zarrv2_store))


def test_accepts_subset_of_misaligned_store(misaligned_zarrv2_store):
    """Filtering down to a single array dodges the alignment check."""
    datasource = zarrv2_datasource.ZarrV2Datasource(
        str(misaligned_zarrv2_store),
        array_paths=["images"],
    )
    assert list(datasource._selected_arrays) == ["images"]


def test_requires_consolidated_metadata(tmp_path):
    store_path = tmp_path / "broken.zarr"
    store_path.mkdir()
    (store_path / ".zmetadata").write_text(json.dumps({}))

    with pytest.raises(ValueError, match="Missing 'metadata'"):
        zarrv2_datasource.ZarrV2Datasource(str(store_path))


def test_rejects_scalar_arrays(tmp_path):
    """0-D (scalar) arrays can't be aligned on axis 0 — clear error.

    Real-world stores (ERA5, CMIP6) include scalar arrays like ``hybrid``
    or ``step`` carrying per-experiment hyperparameters. Indexing
    ``shape[0]`` on a scalar would raise ``IndexError`` with no context;
    we surface a specific ``ValueError`` instead.
    """
    store_path = tmp_path / "scalar.zarr"
    root = zarr.open_group(str(store_path), mode="w")
    # Real 1-D array — fine on its own.
    root.create_dataset("ok", data=np.arange(5, dtype="<i4"), chunks=(5,))
    # Scalar — should be rejected when included.
    root.create_dataset("hyperparam", data=np.array(3.14, dtype="<f4"))
    zarr.consolidate_metadata(str(store_path))

    with pytest.raises(
        ValueError,
        match=r"Cannot read 0-dimensional.*'hyperparam'",
    ):
        zarrv2_datasource.ZarrV2Datasource(str(store_path))


def test_rejects_empty_full_scan_with_actionable_error(tmp_path):
    """``allow_full_metadata_scan=True`` against a store the filesystem can't
    walk (or that genuinely has no arrays) raises a specific error pointing
    the user at ``array_paths=[...]`` as the workaround.

    Pins the diagnosis we surface for the real-world HTTPS-without-listing
    case (e.g., the vitessce anndata-zarr store hosted on plain HTTPS where
    fsspec's HTTPFileSystem can't walk).
    """
    empty_store = tmp_path / "empty.zarr"
    empty_store.mkdir()  # no .zmetadata, no .zarray files anywhere

    with pytest.raises(
        ValueError, match=r"Full-store scan of .* found no \.zarray files.*"
    ):
        zarrv2_datasource.ZarrV2Datasource(
            str(empty_store), allow_full_metadata_scan=True
        )


def test_loads_per_array_zarray_without_zmetadata(unconsolidated_zarrv2_store):
    """No ``.zmetadata`` + explicit ``array_paths`` → per-file ``.zarray`` reads."""
    datasource = zarrv2_datasource.ZarrV2Datasource(
        str(unconsolidated_zarrv2_store),
        array_paths=["images", "nested"],
    )
    assert set(datasource._selected_arrays) == {"images", "nested"}


def test_full_scan_discovers_arrays_without_zmetadata(unconsolidated_zarrv2_store):
    """No ``.zmetadata`` + full-scan opt-in → recursive walk discovers ``.zarray`` files."""
    datasource = zarrv2_datasource.ZarrV2Datasource(
        str(unconsolidated_zarrv2_store),
        allow_full_metadata_scan=True,
    )
    assert set(datasource._selected_arrays) == {"images", "nested"}


def test_full_scan_discovers_nested_arrays(tmp_path):
    """Full scan reports paths relative to the store root for nested groups."""
    store_path = tmp_path / "nested.zarr"
    root = zarr.open_group(str(store_path), mode="w")
    root.create_dataset("top", data=np.arange(5, dtype="<i4"), chunks=(5,))
    inner = root.create_group("group")
    inner.create_dataset("inner", data=np.arange(5, dtype="<i4"), chunks=(5,))
    # Intentionally no consolidate_metadata — exercise the recursive walk.

    datasource = zarrv2_datasource.ZarrV2Datasource(
        str(store_path),
        allow_full_metadata_scan=True,
    )
    assert set(datasource._selected_arrays) == {"top", "group/inner"}
    # Keys must be in canonical form: no trailing slashes, no double slashes.
    for key in datasource._selected_arrays:
        assert not key.endswith("/")
        assert "//" not in key


def test_full_scan_handles_trailing_slash_dirpaths(unconsolidated_zarrv2_store):
    """``fs.walk`` yielding trailing-slash dirpaths still produces canonical keys.

    Some fsspec implementations (older s3fs versions) return ``walk()``
    dirpaths with trailing slashes. The full-scan loader must strip these
    so the output keys match the format used elsewhere in the codebase.
    """
    from fsspec.implementations.local import LocalFileSystem

    class _TrailingSlashLocalFs(LocalFileSystem):
        def walk(self, path, *args, **kwargs):
            for dirpath, dirs, files in super().walk(path, *args, **kwargs):
                yield dirpath.rstrip("/") + "/", dirs, files

    datasource = zarrv2_datasource.ZarrV2Datasource(
        str(unconsolidated_zarrv2_store),
        filesystem=_TrailingSlashLocalFs(skip_instance_cache=True),
        allow_full_metadata_scan=True,
    )
    assert set(datasource._selected_arrays) == {"images", "nested"}
    for key in datasource._selected_arrays:
        assert not key.endswith("/")


def test_requires_array_paths_or_full_scan_when_unconsolidated(
    unconsolidated_zarrv2_store,
):
    """No ``.zmetadata`` and neither escape hatch given → clear ``ValueError``."""
    with pytest.raises(
        ValueError,
        match=(
            r"No array_paths were provided and this Zarr store does not "
            r"contain \.zmetadata"
        ),
    ):
        zarrv2_datasource.ZarrV2Datasource(str(unconsolidated_zarrv2_store))


def test_array_paths_missing_zarray_file_raises_value_error(
    unconsolidated_zarrv2_store,
):
    """Requested array_path with no ``.zarray`` file → translated ``ValueError``."""
    with pytest.raises(
        ValueError,
        match=r"Array path 'missing' not found: no \.zarray file at",
    ):
        zarrv2_datasource.ZarrV2Datasource(
            str(unconsolidated_zarrv2_store),
            array_paths=["missing"],
        )


def test_rejects_array_paths_with_dotdot_segment(zarrv2_group_store):
    """``..`` segments in array_paths bubble up zarr's normalizer ``ValueError``."""
    with pytest.raises(ValueError, match=r"\.\."):
        zarrv2_datasource.ZarrV2Datasource(
            str(zarrv2_group_store),
            array_paths=["images/../nested"],
        )


def test_rejects_zmetadata_with_malformed_zarray_entry(tmp_path):
    """A ``.zarray`` entry inside ``.zmetadata`` missing a required key errors."""
    store_path = tmp_path / "malformed.zarr"
    store_path.mkdir()
    (store_path / ".zmetadata").write_text(
        json.dumps(
            {
                "metadata": {
                    "broken/.zarray": {"shape": [5], "chunks": [2]},  # no dtype
                }
            }
        )
    )

    with pytest.raises(
        ValueError,
        match=r"missing required key\(s\) \['dtype'\]",
    ):
        zarrv2_datasource.ZarrV2Datasource(str(store_path))


# ---------------------------------------------------------------------------
# ZarrArrayMeta.from_json
# ---------------------------------------------------------------------------


def test_zarr_array_meta_from_json_parses_required_fields():
    meta = zarrv2_datasource.ZarrArrayMeta.from_json(
        {"shape": [5, 3], "chunks": [2, 3], "dtype": "<f8", "extra": "ignored"},
        "some/path",
    )
    assert meta.shape == (5, 3)
    assert meta.chunks == (2, 3)
    assert meta.dtype == "<f8"


@pytest.mark.parametrize("missing_key", ["shape", "chunks", "dtype"])
def test_zarr_array_meta_from_json_rejects_missing_key(missing_key):
    raw = {"shape": [5], "chunks": [2], "dtype": "<i4"}
    del raw[missing_key]
    with pytest.raises(
        ValueError,
        match=rf"missing required key\(s\) \['{missing_key}'\]",
    ):
        zarrv2_datasource.ZarrArrayMeta.from_json(raw, "some/path")


# ---------------------------------------------------------------------------
# chunk_size validation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("chunk_size", [0, -1, 1.5, "five"])
def test_rejects_invalid_chunk_size(zarrv2_group_store, chunk_size):
    with pytest.raises(ValueError, match="chunk_size must be a positive integer"):
        zarrv2_datasource.ZarrV2Datasource(
            str(zarrv2_group_store),
            chunk_size=chunk_size,
        )


def test_default_chunk_size_picks_smallest_axis0(zarrv2_group_store):
    """Default chunk_size = min of axis-0 chunks across selected arrays."""
    datasource = zarrv2_datasource.ZarrV2Datasource(str(zarrv2_group_store))
    # Both arrays use chunks[0] == 2.
    assert datasource.chunk_size == 2


# ---------------------------------------------------------------------------
# Filesystem handling
# ---------------------------------------------------------------------------


def test_accepts_pyarrow_fs_filesystem(zarrv2_group_store):
    """A pyarrow.fs.FileSystem passed in is wrapped into fsspec internally."""
    datasource = zarrv2_datasource.ZarrV2Datasource(
        str(zarrv2_group_store),
        filesystem=pyarrow.fs.LocalFileSystem(),
    )
    from fsspec.spec import AbstractFileSystem

    assert isinstance(datasource._fs, AbstractFileSystem)
    assert set(datasource._selected_arrays) == {"images", "nested"}


def test_rejects_unsupported_filesystem_type():
    """Filesystem that's neither pyarrow.fs nor fsspec raises ``TypeError``."""
    with pytest.raises(
        TypeError,
        match=r"filesystem must be pyarrow\.fs\.FileSystem or",
    ):
        zarrv2_datasource.ZarrV2Datasource(
            "/tmp/some.zarr",
            filesystem="not-a-filesystem",
        )


# ---------------------------------------------------------------------------
# Read task generation and execution (end-to-end)
# ---------------------------------------------------------------------------


def test_get_read_tasks_batches_slices_by_parallelism(tmp_path):
    """5 slices split across parallelism=3 produces batches [2, 2, 1]."""
    store_path = tmp_path / "store.zarr"
    _write_real_zarr_store(
        store_path,
        {"images": (np.arange(5 * 4, dtype="<i4").reshape(5, 4), (1, 4))},
    )
    datasource = zarrv2_datasource.ZarrV2Datasource(str(store_path), chunk_size=1)

    read_tasks = datasource.get_read_tasks(parallelism=3)

    assert len(read_tasks) == 3
    assert [task.metadata.num_rows for task in read_tasks] == [2, 2, 1]
    assert all(task.metadata.input_files == (str(store_path),) for task in read_tasks)


def test_materializes_aligned_arrays(tmp_path):
    """End-to-end: each row carries paired slices of every selected array."""
    store_path = tmp_path / "aligned.zarr"
    images_src = np.arange(20, dtype="<i4").reshape(5, 4)
    labels_src = np.arange(5, dtype="|u1")
    _write_real_zarr_store(
        store_path,
        {
            "images": (images_src, (2, 4)),
            "labels": (labels_src, (2,)),
        },
    )

    datasource = zarrv2_datasource.ZarrV2Datasource(str(store_path), chunk_size=2)
    rows = _execute_read_tasks(datasource.get_read_tasks(parallelism=16)).to_dict(
        "records"
    )

    # 5 samples, chunk_size=2 → slices [(0,2), (2,4), (4,5)] → 3 rows.
    assert len(rows) == 3
    # Reconstruct each array by concatenating its column.
    images_out = np.concatenate([row["images"] for row in rows], axis=0)
    labels_out = np.concatenate([row["labels"] for row in rows], axis=0)
    np.testing.assert_array_equal(images_out, images_src)
    np.testing.assert_array_equal(labels_out, labels_src)


def test_edge_slice_is_shorter_not_padded(tmp_path):
    """Final row has axis-0 length < chunk_size; nothing is zero-padded."""
    store_path = tmp_path / "edge.zarr"
    src = np.arange(7, dtype="<i4")
    _write_real_zarr_store(store_path, {"data": (src, (3,))})

    datasource = zarrv2_datasource.ZarrV2Datasource(str(store_path), chunk_size=3)
    rows = _execute_read_tasks(datasource.get_read_tasks(parallelism=16)).to_dict(
        "records"
    )
    lengths = [row["data"].shape[0] for row in rows]
    # 7 samples, chunk_size=3 → [3, 3, 1] — last is shorter, not padded.
    assert lengths == [3, 3, 1]


def test_preserves_nan_and_inf(tmp_path):
    """Materialize must not silently rewrite NaN / +-Inf to 0."""
    store_path = tmp_path / "floats.zarr"
    source = np.array(
        [
            [1.0, np.nan, 2.0],
            [np.inf, 0.0, -np.inf],
            [-1.0, 3.0, np.nan],
        ],
        dtype="<f4",
    )
    _write_real_zarr_store(store_path, {"data": (source, (2, 3))})

    datasource = zarrv2_datasource.ZarrV2Datasource(str(store_path), chunk_size=2)
    rows = _execute_read_tasks(datasource.get_read_tasks(parallelism=16)).to_dict(
        "records"
    )
    reconstructed = np.concatenate([row["data"] for row in rows], axis=0)

    # Bit-exact comparison so NaN, +Inf, -Inf, and finite values are all
    # checked uniformly.
    np.testing.assert_array_equal(reconstructed.view(np.uint32), source.view(np.uint32))


def test_chunk_size_override(tmp_path):
    """User-supplied chunk_size controls the row tiling along axis 0."""
    store_path = tmp_path / "tile.zarr"
    src = np.arange(10, dtype="<i4")
    _write_real_zarr_store(store_path, {"data": (src, (2,))})

    datasource = zarrv2_datasource.ZarrV2Datasource(str(store_path), chunk_size=5)
    rows = _execute_read_tasks(datasource.get_read_tasks(parallelism=16)).to_dict(
        "records"
    )
    assert [row["data"].shape[0] for row in rows] == [5, 5]


# ---------------------------------------------------------------------------
# _read_array_slice retry behavior
# ---------------------------------------------------------------------------


class _ScriptedArray:
    """Stand-in for a zarr Array: each ``[slice]`` returns the next scripted item.

    Items can be either an exception (raised) or an ndarray (returned). Used
    to drive ``_read_array_slice`` through specific retry scenarios without
    touching the network.
    """

    def __init__(self, *responses) -> None:
        self._responses = list(responses)

    def __getitem__(self, key):
        item = self._responses.pop(0)
        if isinstance(item, BaseException):
            raise item
        return item


class _ScriptedRoot:
    """Stand-in for a zarr Group: name → :class:`_ScriptedArray`."""

    def __init__(self, **arrays) -> None:
        self._arrays = arrays

    def __getitem__(self, name):
        return self._arrays[name]


def test_read_array_slice_retries_then_succeeds():
    """Retryable network errors retried with backoff, eventual read succeeds."""
    expected = np.array([1, 2, 3], dtype="<i4")
    arr = _ScriptedArray(
        ConnectionError("Connection reset by peer"),
        TimeoutError("Read timeout"),
        expected,
    )
    root = _ScriptedRoot(x=arr)

    out = zarrv2_datasource._read_array_slice(
        root, "x", 0, 3, max_retries=5, base_delay=0
    )
    np.testing.assert_array_equal(out, expected)


def test_read_array_slice_exhausts_retries():
    """Retryable error every attempt → ``RuntimeError`` chained from last error."""
    arr = _ScriptedArray(
        ConnectionError("Connection reset"),
        ConnectionError("Connection reset"),
        ConnectionError("Connection reset"),
    )
    root = _ScriptedRoot(x=arr)

    with pytest.raises(RuntimeError, match=r"after 3 attempts") as exc_info:
        zarrv2_datasource._read_array_slice(
            root, "x", 0, 3, max_retries=3, base_delay=0
        )
    assert isinstance(exc_info.value.__cause__, ConnectionError)


def test_read_array_slice_non_retryable_immediately_raises():
    """A non-retry-keyword error is raised on the first attempt, not retried."""
    arr = _ScriptedArray(
        ValueError("array is corrupt"),
        # A second response that would succeed if retried — should be unreached.
        np.array([1, 2, 3], dtype="<i4"),
    )
    root = _ScriptedRoot(x=arr)

    with pytest.raises(ValueError, match="corrupt"):
        zarrv2_datasource._read_array_slice(
            root, "x", 0, 3, max_retries=5, base_delay=0
        )


# ---------------------------------------------------------------------------
# Estimator
# ---------------------------------------------------------------------------


def test_estimate_inmemory_data_size(tmp_path):
    """Estimate = sum over arrays of numel * dtype.itemsize."""
    store_path = tmp_path / "est.zarr"
    _write_real_zarr_store(
        store_path,
        {
            "a": (np.zeros((5, 4), dtype="<i4"), (2, 4)),
            "b": (np.zeros(5, dtype="|u1"), (2,)),
        },
    )
    datasource = zarrv2_datasource.ZarrV2Datasource(str(store_path))
    # 5*4*4 (a) + 5*1 (b) = 80 + 5 = 85
    assert datasource.estimate_inmemory_data_size() == 85


# ---------------------------------------------------------------------------
# Public API delegation
# ---------------------------------------------------------------------------


def test_read_zarr_builds_datasource_and_delegates_to_read_datasource():
    datasource = object()
    dataset = object()

    with patch(
        "ray.data.read_api.ZarrV2Datasource",
        return_value=datasource,
    ) as mock_datasource:
        with patch(
            "ray.data.read_api.read_datasource",
            return_value=dataset,
        ) as mock_read_datasource:
            result = read_api.read_zarr(
                "/tmp/sample.zarr",
                chunk_size=4,
                array_paths=["nested"],
                concurrency=3,
                override_num_blocks=2,
                num_cpus=0.5,
                num_gpus=1,
                memory=1024,
                ray_remote_args={"resources": {"custom": 1}},
            )

    assert result is dataset
    mock_datasource.assert_called_once_with(
        path="/tmp/sample.zarr",
        filesystem=None,
        chunk_size=4,
        array_paths=["nested"],
        allow_full_metadata_scan=False,
    )
    mock_read_datasource.assert_called_once()
    args, kwargs = mock_read_datasource.call_args
    assert args == (datasource,)
    assert kwargs == {
        "ray_remote_args": {"resources": {"custom": 1}},
        "num_cpus": 0.5,
        "num_gpus": 1,
        "memory": 1024,
        "concurrency": 3,
        "override_num_blocks": 2,
    }


# ---------------------------------------------------------------------------
# Cross-filesystem end-to-end (Ray Data convention)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "fs",
    [
        None,
        lazy_fixture("local_fs"),  # pyarrow.fs (gets wrapped to fsspec internally)
        lazy_fixture("local_fsspec_fs"),  # native fsspec
    ],
)
def test_read_zarr_basic_across_filesystems(ray_start_regular_shared, fs, local_path):
    """Round-trip a real Zarr store through read_zarr for each filesystem flavor.

    Mirrors the parametrized read-path coverage other Ray Data datasources use
    (lance, parquet, json, hudi, …) — exercises None / pyarrow.fs / fsspec
    input shapes against the same store written to a local path.
    """
    store_path = os.path.join(local_path, "data.zarr")
    images_src = np.arange(20, dtype="<i4").reshape(5, 4)
    labels_src = np.arange(5, dtype="|u1")
    _write_real_zarr_store(
        Path(store_path),
        {
            "images": (images_src, (2, 4)),
            "labels": (labels_src, (2,)),
        },
    )

    ds = ray.data.read_zarr(store_path, filesystem=fs, chunk_size=2)

    assert ds.count() == 3  # 5 samples, chunk_size=2 → 3 rows
    rows = ds.take_all()
    images_out = np.concatenate([row["images"] for row in rows], axis=0)
    labels_out = np.concatenate([row["labels"] for row in rows], axis=0)
    np.testing.assert_array_equal(images_out, images_src)
    np.testing.assert_array_equal(labels_out, labels_src)


# ---------------------------------------------------------------------------
# Public-bucket integration test
# ---------------------------------------------------------------------------


def test_read_zarr_integration_public_s3(ray_start_regular_shared):
    """End-to-end read against a real Zarr store in a public S3 bucket.

    Uses ``s3://anonymous@ray-example-data/mnist-tiny.zarr`` — a 200-sample
    MNIST subset with two aligned arrays (``images`` shape (200, 28, 28),
    ``labels`` shape (200,)). Both with axis-0 chunks of 50, so the resulting
    dataset has 4 rows by default.
    """
    ds = ray.data.read_zarr("s3://anonymous@ray-example-data/mnist-tiny.zarr")

    assert ds.count() == 4
    rows = ds.take_all()
    assert {row["images"].shape for row in rows} == {(50, 28, 28)}
    assert {row["labels"].shape for row in rows} == {(50,)}
    assert all(row["images"].dtype == np.uint8 for row in rows)
    assert all(row["labels"].dtype == np.uint8 for row in rows)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
