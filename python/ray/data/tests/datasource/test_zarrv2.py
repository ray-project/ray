import json
import logging
import os
from pathlib import Path
from typing import Any, cast

import fsspec
import numpy as np
import pandas as pd
import pyarrow.fs
import pytest
import zarr
from pytest_lazy_fixtures import lf as lazy_fixture

import ray
from ray.data._internal.datasource import zarrv2_datasource
from ray.data.tests.conftest import *  # noqa: F401, F403


def _execute_read_tasks(tasks) -> pd.DataFrame:
    frames = [block for task in tasks for block in task()]
    return pd.concat(frames, ignore_index=True)


def _reconstruct_array(df: pd.DataFrame, array_name: str) -> np.ndarray:
    """Concatenate all chunks of one array from a long-form result frame.

    Assumes the array is 1-D along its chunked axis 0. For tests with
    higher-dim arrays, use ``_reconstruct_nd`` (which orders chunks by
    ``chunk_index`` and concatenates axis 0 first).
    """
    sub = df[df["array"] == array_name].sort_values("chunk_index")
    return np.concatenate(list(sub["chunk"]), axis=0)


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


@pytest.fixture
def zarrv2_group_store(tmp_path) -> Path:
    """Two arrays at the store root, both 2-D and 1-D, axis-0-aligned (shape[0]==5)."""
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
def heterogeneous_zarrv2_store(tmp_path) -> Path:
    """A store mixing different ranks, shape[0]s, dtypes, and native chunk sizes.

    Mirrors the UMI-style real-world layout where ``data/*`` arrays share an
    axis-0 timestep count but differ in everything else, and ``meta/*``
    arrays live in a separate axis-0 universe entirely. The chunk-per-row
    datasource handles all of these in one read; nothing has to align.
    """
    store_path = tmp_path / "heterogeneous.zarr"
    root = zarr.open_group(str(store_path), mode="w")
    # 4-D image tensor with tiny axis-0 chunks (1 image per chunk).
    root.create_dataset(
        "data/camera0_rgb",
        data=np.arange(20 * 2 * 2 * 3, dtype="|u1").reshape(20, 2, 2, 3),
        chunks=(1, 2, 2, 3),
    )
    # 2-D pose array, same shape[0]=20, much larger axis-0 chunks (10).
    root.create_dataset(
        "data/robot0_eef_pos",
        data=np.arange(20 * 3, dtype="<f4").reshape(20, 3),
        chunks=(10, 3),
    )
    # Episode-boundary metadata: separate axis-0 universe.
    root.create_dataset(
        "meta/episode_ends",
        data=np.array([5, 12, 20], dtype="<i8"),
        chunks=(3,),
    )
    zarr.consolidate_metadata(str(store_path))
    return store_path


@pytest.fixture
def unconsolidated_zarrv2_store(tmp_path) -> Path:
    """Two arrays at the store root, no ``.zmetadata``.

    Exercises the no-``.zmetadata`` code paths (per-array ``.zarray``
    discovery and full-store walk) — the common shape of real-world stores
    behind plain HTTPS or other listing-less filesystems.
    """
    store_path = tmp_path / "unconsolidated.zarr"
    root = zarr.open_group(str(store_path), mode="w")
    root.create_dataset(
        "images", data=np.arange(20, dtype="<i4").reshape(5, 4), chunks=(2, 4)
    )
    root.create_dataset("nested", data=np.arange(5, dtype="|u1"), chunks=(2,))
    return store_path


@pytest.fixture
def aligned_zarrv2_store(tmp_path) -> Path:
    """Three arrays sharing ``shape[0]=8``, different ranks and native chunks.

    Models the UMI-style case where data arrays co-stride on the timestep
    axis but differ in everything else.
    """
    store_path = tmp_path / "aligned.zarr"
    root = zarr.open_group(str(store_path), mode="w")
    root.create_dataset(
        "img",
        data=np.arange(8 * 4 * 4 * 3, dtype="|u1").reshape(8, 4, 4, 3),
        chunks=(2, 4, 4, 3),
    )
    root.create_dataset(
        "state",
        data=np.arange(8 * 3, dtype="<f4").reshape(8, 3),
        chunks=(4, 3),  # different native axis-0 chunks than img
    )
    root.create_dataset(
        "label",
        data=np.arange(8, dtype="<i8"),
        chunks=(8,),
    )
    zarr.consolidate_metadata(str(store_path))
    return store_path


@pytest.fixture
def zarr_zip_store(tmp_path) -> Path:
    """A small Zarr store packed into a ``.zip`` for URL-detection tests."""
    src = tmp_path / "src.zarr"
    _write_real_zarr_store(
        src,
        {
            "data": (np.arange(12, dtype="<i4").reshape(6, 2), (3, 2)),
        },
    )
    zip_path = tmp_path / "store.zarr.zip"
    import shutil

    shutil.make_archive(
        base_name=str(tmp_path / "store.zarr"),
        format="zip",
        root_dir=str(src),
    )
    assert zip_path.exists()
    return zip_path


# ---------------------------------------------------------------------------
# Metadata discovery
# ---------------------------------------------------------------------------


def test_normalizes_requested_root_array_path(zarrv2_root_store):
    datasource = zarrv2_datasource.ZarrV2Datasource(
        str(zarrv2_root_store),
        array_paths=[""],
    )
    assert list(datasource._metadata_by_path) == [""]


def test_normalizes_requested_array_paths(zarrv2_group_store):
    datasource = zarrv2_datasource.ZarrV2Datasource(
        str(zarrv2_group_store),
        array_paths=["images/", "nested"],
    )
    assert list(datasource._metadata_by_path) == ["images", "nested"]


def test_rejects_missing_array_paths(zarrv2_group_store):
    with pytest.raises(
        ValueError,
        match=r"Array\(s\) not found: 'missing'\. Available: 'images', 'nested'",
    ):
        zarrv2_datasource.ZarrV2Datasource(
            str(zarrv2_group_store),
            array_paths=["missing"],
        )


def test_requires_consolidated_metadata(tmp_path):
    store_path = tmp_path / "broken.zarr"
    store_path.mkdir()
    (store_path / ".zmetadata").write_text(json.dumps({}))

    with pytest.raises(ValueError, match="Missing 'metadata'"):
        zarrv2_datasource.ZarrV2Datasource(str(store_path))


def test_rejects_empty_full_scan_with_actionable_error(tmp_path):
    empty_store = tmp_path / "empty.zarr"
    empty_store.mkdir()  # no .zmetadata, no .zarray files anywhere

    with pytest.raises(
        ValueError, match=r"Full-store scan of .* found no \.zarray files.*"
    ):
        zarrv2_datasource.ZarrV2Datasource(
            str(empty_store), allow_full_metadata_scan=True
        )


def test_loads_per_array_zarray_without_zmetadata(unconsolidated_zarrv2_store):
    datasource = zarrv2_datasource.ZarrV2Datasource(
        str(unconsolidated_zarrv2_store),
        array_paths=["images", "nested"],
    )
    assert set(datasource._metadata_by_path) == {"images", "nested"}


def test_full_scan_discovers_arrays_without_zmetadata(unconsolidated_zarrv2_store):
    datasource = zarrv2_datasource.ZarrV2Datasource(
        str(unconsolidated_zarrv2_store),
        allow_full_metadata_scan=True,
    )
    assert set(datasource._metadata_by_path) == {"images", "nested"}


def test_requires_array_paths_or_full_scan_when_unconsolidated(
    unconsolidated_zarrv2_store,
):
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
    with pytest.raises(
        ValueError,
        match=r"Array path 'missing' not found: no \.zarray file at",
    ):
        zarrv2_datasource.ZarrV2Datasource(
            str(unconsolidated_zarrv2_store),
            array_paths=["missing"],
        )


def test_rejects_zmetadata_with_malformed_zarray_entry(tmp_path):
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
# ZarrArrayMeta
# ---------------------------------------------------------------------------


def test_zarr_array_meta_from_json_parses_required_fields():
    meta = zarrv2_datasource.ZarrArrayMeta.from_json(
        {"shape": [5, 3], "chunks": [2, 3], "dtype": "<f8", "extra": "ignored"},
        "some/path",
    )
    assert meta.shape == (5, 3)
    assert meta.chunks == (2, 3)
    assert meta.dtype == "<f8"
    assert meta.rank == 2
    assert meta.itemsize == 8


# ---------------------------------------------------------------------------
# chunk_shapes validation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "chunk_shapes",
    ["invalid", 42, b"bytes", {1, 2}],
)
def test_rejects_invalid_chunk_shapes(zarrv2_group_store, chunk_shapes):
    """Non-list/non-tuple/non-dict inputs are rejected at construction time."""
    with pytest.raises(
        ValueError,
        match="chunk_shapes must be a non-empty sequence of positive integers",
    ):
        zarrv2_datasource.ZarrV2Datasource(
            str(zarrv2_group_store),
            chunk_shapes=chunk_shapes,
        )


@pytest.mark.parametrize(
    "chunk_shapes,array_paths,expected",
    [
        # No chunk_shapes: every array reads at its native chunk size.
        # 4-D image with tiny chunks coexists with 2-D pose with big chunks —
        # nothing is forced into a shared min/max.
        (
            None,
            None,
            {
                "data/camera0_rgb": (1, 2, 2, 3),
                "data/robot0_eef_pos": (10, 3),
                "meta/episode_ends": (3,),
            },
        ),
        # ``[5]`` prefix overrides axis 0 across arrays of all ranks at once.
        (
            [5],
            None,
            {
                "data/camera0_rgb": (5, 2, 2, 3),
                "data/robot0_eef_pos": (5, 3),
                "meta/episode_ends": (5,),
            },
        ),
        # Length-2 prefix overrides axes 0+1; needs every selected array to
        # have rank >= 2, so we filter out ``meta/episode_ends`` (rank 1).
        (
            [5, 1],
            ["data/camera0_rgb", "data/robot0_eef_pos"],
            {
                "data/camera0_rgb": (5, 1, 2, 3),
                "data/robot0_eef_pos": (5, 1),
            },
        ),
        # Per-array overrides may retile only some arrays while others keep
        # their native chunks.
        (
            {
                "data/camera0_rgb": [5],
                "data/robot0_eef_pos": [7],
            },
            None,
            {
                "data/camera0_rgb": (5, 2, 2, 3),
                "data/robot0_eef_pos": (7, 3),
                "meta/episode_ends": (3,),
            },
        ),
    ],
)
def test_chunk_shapes_resolution_across_mixed_rank(
    heterogeneous_zarrv2_store, chunk_shapes, array_paths, expected
):
    datasource = zarrv2_datasource.ZarrV2Datasource(
        str(heterogeneous_zarrv2_store),
        chunk_shapes=chunk_shapes,
        array_paths=array_paths,
    )
    assert datasource._array_chunks == expected


@pytest.mark.parametrize(
    "chunk_shapes",
    [
        {"images": 1},
        {"images": None},
        {"images": []},
        {"images": [0]},
        {"images": [1.5]},
    ],
)
def test_rejects_invalid_chunk_shapes_dict_values(zarrv2_group_store, chunk_shapes):
    with pytest.raises(
        ValueError,
        match=r"chunk_shapes\['images'\] must be .*positive integers",
    ):
        zarrv2_datasource.ZarrV2Datasource(
            str(zarrv2_group_store),
            chunk_shapes=chunk_shapes,
        )


def test_rejects_invalid_chunk_shapes_dict_keys(zarrv2_group_store):
    with pytest.raises(
        ValueError,
        match="chunk_shapes dict keys must be array-path strings",
    ):
        zarrv2_datasource.ZarrV2Datasource(
            str(zarrv2_group_store),
            chunk_shapes=cast(Any, {1: [2]}),
        )


def test_rejects_duplicate_normalized_chunk_shapes_keys(zarrv2_group_store):
    with pytest.raises(
        ValueError,
        match="duplicate array paths after normalization",
    ):
        zarrv2_datasource.ZarrV2Datasource(
            str(zarrv2_group_store),
            chunk_shapes={"images": [2], "/images/": [3]},
        )


def test_rejects_unknown_chunk_shapes_keys(zarrv2_group_store):
    with pytest.raises(
        ValueError,
        match="Unknown array path\\(s\\) in chunk_shapes",
    ):
        zarrv2_datasource.ZarrV2Datasource(
            str(zarrv2_group_store),
            chunk_shapes={"does_not_exist": [2]},
        )


# ---------------------------------------------------------------------------
# align_axis_0 (wide-form mode)
# ---------------------------------------------------------------------------


def test_align_axis_0_emits_wide_rows(aligned_zarrv2_store):
    """Wide-row schema: ``t_start``, ``t_stop``, one column per selected array."""
    datasource = zarrv2_datasource.ZarrV2Datasource(
        str(aligned_zarrv2_store),
        align_axis_0=True,
        chunk_shapes=[4],
    )
    df = _execute_read_tasks(datasource.get_read_tasks(parallelism=4))
    assert set(df.columns) == {"t_start", "t_stop", "img", "state", "label"}
    # shape[0]=8, chunk_shapes=[4] -> 2 rows.
    assert len(df) == 2
    # Reconstruct each array by concatenating slices in order.
    img_recon = np.concatenate(list(df["img"]), axis=0)
    assert img_recon.shape == (8, 4, 4, 3)
    state_recon = np.concatenate(list(df["state"]), axis=0)
    assert state_recon.shape == (8, 3)
    label_recon = np.concatenate(list(df["label"]), axis=0)
    assert label_recon.shape == (8,)
    # t_start/t_stop are correct.
    starts = sorted(df["t_start"].tolist())
    stops = sorted(df["t_stop"].tolist())
    assert starts == [0, 4]
    assert stops == [4, 8]


@pytest.mark.parametrize(
    "array_paths,extra_cols",
    [
        # No filter: all discovered arrays end up aligned.
        (None, {"img", "state", "label"}),
        # array_paths selects which arrays to read; align_axis_0 just
        # asserts that the selected set is mutually aligned.
        (["img", "state"], {"img", "state"}),
    ],
)
def test_align_axis_0_column_set(aligned_zarrv2_store, array_paths, extra_cols):
    datasource = zarrv2_datasource.ZarrV2Datasource(
        str(aligned_zarrv2_store),
        array_paths=array_paths,
        align_axis_0=True,
        chunk_shapes=[4],
    )
    df = _execute_read_tasks(datasource.get_read_tasks(parallelism=4))
    assert set(df.columns) == {"t_start", "t_stop"} | extra_cols


def test_align_axis_0_accepts_per_array_chunk_shapes(aligned_zarrv2_store):
    datasource = zarrv2_datasource.ZarrV2Datasource(
        str(aligned_zarrv2_store),
        align_axis_0=True,
        chunk_shapes={"img": [4], "state": [4], "label": [4]},
    )
    df = _execute_read_tasks(datasource.get_read_tasks(parallelism=4))
    assert len(df) == 2
    assert sorted(zip(df["t_start"], df["t_stop"])) == [(0, 4), (4, 8)]


def test_align_axis_0_rejects_misaligned_shape0(heterogeneous_zarrv2_store):
    """Misalignment raises with the per-array shape[0] breakdown."""
    with pytest.raises(
        ValueError,
        match=r"All selected arrays must share shape\[0\]",
    ):
        zarrv2_datasource.ZarrV2Datasource(
            str(heterogeneous_zarrv2_store),
            align_axis_0=True,
            chunk_shapes=[5],
        )


def test_align_axis_0_rejects_non_bool(aligned_zarrv2_store):
    """``align_axis_0`` must be a bool — no list form."""
    with pytest.raises(TypeError, match=r"align_axis_0 must be a bool"):
        zarrv2_datasource.ZarrV2Datasource(
            str(aligned_zarrv2_store),
            align_axis_0=cast(Any, ["img", "state"]),
        )


def test_align_axis_0_rejects_divergent_axis_0_chunks(aligned_zarrv2_store):
    """If aligned arrays end up with different axis-0 chunks, error clearly.

    The native chunks differ (img=2, state=4, label=8) — without a
    ``chunk_shapes`` re-tile they all stay at native, and the validator
    catches the mismatch.
    """
    with pytest.raises(
        ValueError, match="Aligned arrays must share the same axis-0 chunk size"
    ):
        zarrv2_datasource.ZarrV2Datasource(
            str(aligned_zarrv2_store),
            align_axis_0=True,
        )


# ---------------------------------------------------------------------------
# overlap (aligned-mode lookahead)
# ---------------------------------------------------------------------------


def test_overlap_extends_chunk_data(aligned_zarrv2_store):
    """``overlap=N`` makes each row's per-array slice cover ``N`` extra timesteps.

    Aligned store has shape[0]=8, ``chunk_shapes=[4]`` -> rows own [0,4) and [4,8).
    With ``overlap=2``, row 0's data covers [0,6) and row 1's data covers [4,8)
    (clipped at the store end since 4+4+2 > 8).
    """
    datasource = zarrv2_datasource.ZarrV2Datasource(
        str(aligned_zarrv2_store),
        align_axis_0=True,
        chunk_shapes=[4],
        overlap=2,
    )
    df = _execute_read_tasks(datasource.get_read_tasks(parallelism=4))
    # Ownership unchanged: 2 rows of width 4 each.
    assert sorted(zip(df["t_start"], df["t_stop"])) == [(0, 4), (4, 8)]
    # Data extents: row 0 has 6 timesteps, row 1 has 4 (clipped at shape[0]=8).
    rows = sorted(df.to_dict("records"), key=lambda r: r["t_start"])
    assert rows[0]["img"].shape[0] == 6  # 4 owned + 2 overlap
    assert rows[0]["state"].shape[0] == 6
    assert rows[1]["img"].shape[0] == 4  # 4 owned + 0 overlap (clipped)
    assert rows[1]["state"].shape[0] == 4


def test_overlap_requires_align_axis_0(aligned_zarrv2_store):
    """``overlap`` in long-form (no ``align_axis_0``) is a clear error."""
    with pytest.raises(ValueError, match="overlap requires align_axis_0=True"):
        zarrv2_datasource.ZarrV2Datasource(
            str(aligned_zarrv2_store),
            overlap=2,
        )


def test_overlap_rejects_negative_and_non_int(aligned_zarrv2_store):
    bad_values: list[Any] = [-1, 1.5, "two"]

    for bad in bad_values:
        with pytest.raises(ValueError, match="overlap must be a non-negative integer"):
            zarrv2_datasource.ZarrV2Datasource(
                str(aligned_zarrv2_store),
                align_axis_0=True,
                chunk_shapes=[4],
                overlap=bad,
            )


def test_overlap_enables_windowing_without_cross_row_loss(aligned_zarrv2_store):
    window_len = 3
    datasource = zarrv2_datasource.ZarrV2Datasource(
        str(aligned_zarrv2_store),
        align_axis_0=True,
        chunk_shapes=[4],
        overlap=window_len - 1,
    )
    df = _execute_read_tasks(datasource.get_read_tasks(parallelism=4))
    starts = []
    for _, row in df.iterrows():
        t_start, t_stop = row["t_start"], row["t_stop"]
        img = row["img"]
        for local in range(t_stop - t_start):
            if local + window_len > img.shape[0]:
                continue  # only triggers at very end of store
            starts.append(t_start + local)
    # 8 timesteps, window_len=3 -> valid global starts are [0,6) = 6 windows.
    # Without overlap we would have lost ~33%. With overlap=2 we should
    # capture all 6.
    assert sorted(starts) == [0, 1, 2, 3, 4, 5]


def test_chunk_shapes_rejected_when_longer_than_smallest_array(
    heterogeneous_zarrv2_store,
):
    """A shared ``chunk_shapes`` override longer than a target rank is an error."""
    with pytest.raises(
        ValueError,
        match=r"chunk_shapes override for array .* has 2 axes but array of shape .* has rank 1",
    ):
        zarrv2_datasource.ZarrV2Datasource(
            str(heterogeneous_zarrv2_store),
            chunk_shapes=[2, 2],  # OK for 2-D and 4-D, fails for 1-D episode_ends
        )


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
    assert set(datasource._metadata_by_path) == {"images", "nested"}


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
# .zarr.zip URL support
# ---------------------------------------------------------------------------


def test_reads_zarr_zip_local_path(zarr_zip_store):
    """A local ``.zarr.zip`` path auto-wires fsspec's ZipFileSystem."""
    datasource = zarrv2_datasource.ZarrV2Datasource(str(zarr_zip_store))
    # The store has one array "data" of shape (6, 2) chunks (3, 2) -> 2 chunks.
    df = _execute_read_tasks(datasource.get_read_tasks(parallelism=2))
    assert len(df) == 2
    assert set(df["array"]) == {"data"}
    recon = _reconstruct_array(df, "data")
    np.testing.assert_array_equal(recon, np.arange(12, dtype="<i4").reshape(6, 2))


# ---------------------------------------------------------------------------
# Read task generation and execution (end-to-end)
# ---------------------------------------------------------------------------


def test_get_read_tasks_batches_chunks_by_parallelism(tmp_path):
    """5 chunks split across parallelism=3 produces batches [2, 2, 1]."""
    store_path = tmp_path / "store.zarr"
    _write_real_zarr_store(
        store_path,
        {"images": (np.arange(5 * 4, dtype="<i4").reshape(5, 4), (1, 4))},
    )
    datasource = zarrv2_datasource.ZarrV2Datasource(str(store_path))

    read_tasks = datasource.get_read_tasks(parallelism=3)

    assert len(read_tasks) == 3
    assert [task.metadata.num_rows for task in read_tasks] == [2, 2, 1]
    assert all(task.metadata.input_files == (str(store_path),) for task in read_tasks)


def test_long_form_schema_and_materialization(tmp_path):
    """End-to-end: long-form rows are emitted with the expected columns and data."""
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

    datasource = zarrv2_datasource.ZarrV2Datasource(str(store_path))
    df = _execute_read_tasks(datasource.get_read_tasks(parallelism=16))

    # Schema is the long-form quad.
    assert list(df.columns) == ["array", "chunk_index", "chunk_slices", "chunk"]
    # 3 chunks for images (5/2), 3 chunks for labels (5/2) = 6 rows total.
    assert len(df) == 6
    assert set(df["array"]) == {"images", "labels"}

    np.testing.assert_array_equal(_reconstruct_array(df, "images"), images_src)
    np.testing.assert_array_equal(_reconstruct_array(df, "labels"), labels_src)

    # ``chunk_slices`` matches the actual chunk shape and indexes back to
    # the source array: arr[start:stop, ...] equals the chunk.
    for _, row in df.iterrows():
        slices = row["chunk_slices"]
        chunk = row["chunk"]
        assert len(slices) == chunk.ndim
        for axis, (start, stop) in enumerate(slices):
            assert stop - start == chunk.shape[axis]
        if row["array"] == "images":
            np.testing.assert_array_equal(
                chunk,
                images_src[slices[0][0] : slices[0][1], slices[1][0] : slices[1][1]],
            )


def test_chunk_shapes_override_changes_grid(tmp_path):
    """User-supplied chunk_shapes controls the chunk grid and row count."""
    store_path = tmp_path / "tile.zarr"
    src = np.arange(10, dtype="<i4")
    _write_real_zarr_store(store_path, {"data": (src, (2,))})  # native: 5 chunks

    datasource = zarrv2_datasource.ZarrV2Datasource(str(store_path), chunk_shapes=[5])
    df = _execute_read_tasks(datasource.get_read_tasks(parallelism=16))
    assert sorted(chunk.shape[0] for chunk in df["chunk"]) == [5, 5]


def test_heterogeneous_store_emits_one_row_per_chunk(heterogeneous_zarrv2_store):
    """Mixed-rank/shape/dtype arrays each contribute their chunk count to the output."""
    datasource = zarrv2_datasource.ZarrV2Datasource(str(heterogeneous_zarrv2_store))
    df = _execute_read_tasks(datasource.get_read_tasks(parallelism=16))

    # Expected chunk counts:
    #   data/camera0_rgb       shape=(20,2,2,3) chunks=(1,2,2,3) → 20 chunks
    #   data/robot0_eef_pos    shape=(20,3)     chunks=(10,3)    → 2 chunks
    #   meta/episode_ends      shape=(3,)       chunks=(3,)      → 1 chunk
    counts = df.groupby("array").size().to_dict()
    assert counts == {
        "data/camera0_rgb": 20,
        "data/robot0_eef_pos": 2,
        "meta/episode_ends": 1,
    }


# ---------------------------------------------------------------------------
# _read_chunk retry behavior
# ---------------------------------------------------------------------------


class _ScriptedArray:
    """Stand-in for a zarr Array: each ``[indexer]`` returns the next scripted item.

    Items can be either an exception (raised) or an ndarray (returned). Used
    to drive ``_read_chunk`` through specific retry scenarios without
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


def test_read_chunk_retries_then_succeeds():
    """Retryable network errors retried with backoff, eventual read succeeds.

    Uses default ``match`` patterns (``DataContext.retried_io_errors`` plus
    zarr-specific entries like ``"Connection reset"`` and ``"Read timeout"``).
    """
    expected = np.array([1, 2, 3], dtype="<i4")
    arr = _ScriptedArray(
        ConnectionError("Connection reset by peer"),
        TimeoutError("Read timeout"),
        expected,
    )
    root = _ScriptedRoot(x=arr)

    out = zarrv2_datasource._read_chunk(
        root, "x", ((0, 3),), max_attempts=5, max_backoff_s=0
    )
    np.testing.assert_array_equal(out, expected)


def test_read_chunk_exhausts_retries():
    arr = _ScriptedArray(
        ConnectionError("Connection reset"),
        ConnectionError("Connection reset"),
        ConnectionError("Connection reset"),
    )
    root = _ScriptedRoot(x=arr)

    # call_with_retry re-raises the last exception itself (with ``from None``)
    # rather than wrapping in a RuntimeError. Match against the original
    # exception type to pin that behaviour.
    with pytest.raises(ConnectionError, match="Connection reset"):
        zarrv2_datasource._read_chunk(
            root, "x", ((0, 3),), max_attempts=3, max_backoff_s=0
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

    ds = ray.data.read_zarr(store_path, filesystem=fs)

    # 3 chunks each for images and labels (5/2 → ceil = 3) → 6 rows total.
    assert ds.count() == 6
    df = pd.DataFrame(ds.take_all())
    np.testing.assert_array_equal(_reconstruct_array(df, "images"), images_src)
    np.testing.assert_array_equal(_reconstruct_array(df, "labels"), labels_src)


# ---------------------------------------------------------------------------
# Public-bucket integration test
# ---------------------------------------------------------------------------


def test_read_zarr_integration_public_s3(ray_start_regular_shared):
    """End-to-end read against a real Zarr store in a public S3 bucket.

    Uses ``s3://anonymous@ray-example-data/mnist-tiny.zarr`` — a 200-sample
    MNIST subset with two arrays:
      * ``images``  shape (200, 28, 28), chunks (50, 28, 28)  → 4 chunks
      * ``labels``  shape (200,),        chunks (200,)        → 1 chunk

    Under the chunk-per-row schema the total row count is 4 + 1 = 5.
    """
    ds = ray.data.read_zarr("s3://anonymous@ray-example-data/mnist-tiny.zarr")

    assert ds.count() == 5
    df = pd.DataFrame(ds.take_all())
    assert set(df["array"]) == {"images", "labels"}
    image_rows = df[df["array"] == "images"]
    label_rows = df[df["array"] == "labels"]
    assert {c.shape for c in image_rows["chunk"]} == {(50, 28, 28)}
    assert {c.shape for c in label_rows["chunk"]} == {(200,)}
    assert all(c.dtype == np.uint8 for c in image_rows["chunk"])
    assert all(c.dtype == np.uint8 for c in label_rows["chunk"])


# ---------------------------------------------------------------------------
# Custom codec registration in Ray workers
# ---------------------------------------------------------------------------


# Hook string registers a custom (non-stdlib) codec in each worker process.
# numcodecs.registry is process-local — built-in codecs (blosc, gzip, zstd)
# register themselves at import time, but anything else (including
# ``imagecodecs_jpegxl``) must be explicitly registered in every process
# that decodes chunks. Ray workers are separate Python processes, so the
# driver's registration does NOT propagate. The standard fix is to run
# this registration in each worker via ``runtime_env``'s
# ``worker_process_setup_hook``.
_CUSTOM_CODEC_HOOK = """
import numcodecs
import numpy as np

class _RayZarrTestCodec(numcodecs.abc.Codec):
    codec_id = "ray_zarr_test_codec"

    def encode(self, buf):
        return bytes(buf)

    def decode(self, buf, out=None):
        arr = np.frombuffer(buf, dtype=np.uint8)
        if out is not None:
            out[:] = arr.view(out.dtype)
            return out
        return arr.copy()

numcodecs.register_codec(_RayZarrTestCodec)
"""


def test_custom_codec_succeeds_with_worker_setup_hook(tmp_path):
    """``worker_process_setup_hook`` runs once per worker, before any task,
    registering the codec in the worker's process. Chunk decode succeeds.

    Builds a tiny Zarr store compressed with a custom codec that numcodecs
    doesn't auto-register. The driver registers the codec briefly to write
    the store; Ray workers need their own registration to decode chunks,
    which the ``worker_process_setup_hook`` arranges.
    """
    import numcodecs

    # Register driver-side so we can write the store.
    exec(_CUSTOM_CODEC_HOOK, {})

    store_path = tmp_path / "codec_test.zarr"
    arr = zarr.open(
        str(store_path),
        mode="w",
        shape=(8,),
        chunks=(4,),
        dtype="u1",
        compressor=numcodecs.get_codec({"id": "ray_zarr_test_codec"}),
    )
    arr[:] = np.arange(8, dtype="u1")
    zarr.consolidate_metadata(str(store_path))

    if ray.is_initialized():
        ray.shutdown()
    ray.init(
        num_cpus=1,
        logging_level=logging.ERROR,
        log_to_driver=False,
        runtime_env={"worker_process_setup_hook": _CUSTOM_CODEC_HOOK},
    )
    try:
        ds = ray.data.read_zarr(str(store_path))
        rows = sorted(ds.take_all(), key=lambda r: tuple(r["chunk_index"]))
        recon = np.concatenate([r["chunk"] for r in rows])
        np.testing.assert_array_equal(recon, np.arange(8, dtype="u1"))
    finally:
        ray.shutdown()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
