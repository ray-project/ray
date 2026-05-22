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
    """Two arrays at the store root, no ``.zmetadata``."""
    return _write_unconsolidated_zarr_store(
        tmp_path / "unconsolidated.zarr",
        {
            "images": (np.arange(20, dtype="<i4").reshape(5, 4), (2, 4)),
            "nested": (np.arange(5, dtype="|u1"), (2,)),
        },
    )


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


def test_accepts_heterogeneous_arrays_in_one_read(heterogeneous_zarrv2_store):
    """No alignment required: arrays of any rank/shape coexist in one read."""
    datasource = zarrv2_datasource.ZarrV2Datasource(str(heterogeneous_zarrv2_store))
    assert set(datasource._selected_arrays) == {
        "data/camera0_rgb",
        "data/robot0_eef_pos",
        "meta/episode_ends",
    }


def test_requires_consolidated_metadata(tmp_path):
    store_path = tmp_path / "broken.zarr"
    store_path.mkdir()
    (store_path / ".zmetadata").write_text(json.dumps({}))

    with pytest.raises(ValueError, match="Missing 'metadata'"):
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


@pytest.mark.parametrize("missing_key", ["shape", "chunks", "dtype"])
def test_zarr_array_meta_from_json_rejects_missing_key(missing_key):
    raw = {"shape": [5], "chunks": [2], "dtype": "<i4"}
    del raw[missing_key]
    with pytest.raises(
        ValueError,
        match=rf"missing required key\(s\) \['{missing_key}'\]",
    ):
        zarrv2_datasource.ZarrArrayMeta.from_json(raw, "some/path")


def test_effective_chunks_returns_native_when_user_none():
    meta = zarrv2_datasource.ZarrArrayMeta(
        shape=(200, 28, 28), chunks=(50, 28, 28), dtype="<u1"
    )
    assert meta.effective_chunks(None) == (50, 28, 28)


def test_effective_chunks_full_override():
    meta = zarrv2_datasource.ZarrArrayMeta(
        shape=(200, 28, 28), chunks=(50, 28, 28), dtype="<u1"
    )
    assert meta.effective_chunks((16, 14, 14)) == (16, 14, 14)


def test_effective_chunks_prefix_broadcasts_trailing_axes():
    """``chunk_shape=[16]`` overrides axis 0 only; trailing axes stay native."""
    meta = zarrv2_datasource.ZarrArrayMeta(
        shape=(200, 28, 28), chunks=(50, 14, 7), dtype="<u1"
    )
    assert meta.effective_chunks((16,)) == (16, 14, 7)
    assert meta.effective_chunks((16, 28)) == (16, 28, 7)


def test_effective_chunks_rejects_chunk_shape_longer_than_rank():
    meta = zarrv2_datasource.ZarrArrayMeta(
        shape=(200, 28), chunks=(50, 14), dtype="<u1"
    )
    with pytest.raises(
        ValueError, match=r"chunk_shape has 3 axes but array of shape .* has rank 2"
    ):
        meta.effective_chunks((16, 14, 7))


# ---------------------------------------------------------------------------
# chunk_shape validation (datasource entry point)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("chunk_shape", [[0], [-1], [1.5], ["five"], [1, 0], []])
def test_rejects_invalid_chunk_shape(zarrv2_group_store, chunk_shape):
    with pytest.raises(
        ValueError, match="chunk_shape must be a non-empty list of positive integers"
    ):
        zarrv2_datasource.ZarrV2Datasource(
            str(zarrv2_group_store),
            chunk_shape=chunk_shape,
        )


def test_chunk_shape_dict_glob_pattern(aligned_zarrv2_store):
    """Dict ``chunk_shape`` resolves per-array via fnmatch glob patterns."""
    datasource = zarrv2_datasource.ZarrV2Datasource(
        str(aligned_zarrv2_store),
        chunk_shape={"img": [4], "state": [4], "label": [4]},
    )
    assert datasource._array_chunks["img"][0] == 4
    assert datasource._array_chunks["state"][0] == 4
    assert datasource._array_chunks["label"][0] == 4


def test_chunk_shape_dict_default_fallback(aligned_zarrv2_store):
    """``"default"`` is used for arrays that match no glob pattern."""
    datasource = zarrv2_datasource.ZarrV2Datasource(
        str(aligned_zarrv2_store),
        chunk_shape={"img": [4], "default": [2]},
    )
    assert datasource._array_chunks["img"][0] == 4
    assert datasource._array_chunks["state"][0] == 2
    assert datasource._array_chunks["label"][0] == 2


def test_chunk_shape_dict_none_keeps_native(aligned_zarrv2_store):
    """A ``None`` prefix in the dict form means keep native chunks."""
    datasource = zarrv2_datasource.ZarrV2Datasource(
        str(aligned_zarrv2_store),
        chunk_shape={"img": [4], "default": None},
    )
    assert datasource._array_chunks["img"][0] == 4
    # state native chunk[0]=4, label native chunk[0]=8.
    assert datasource._array_chunks["state"][0] == 4
    assert datasource._array_chunks["label"][0] == 8


def test_chunk_shape_dict_rejects_invalid_prefix(aligned_zarrv2_store):
    with pytest.raises(
        ValueError, match=r"chunk_shape\['img'\] must be None or a non-empty list"
    ):
        zarrv2_datasource.ZarrV2Datasource(
            str(aligned_zarrv2_store),
            chunk_shape={"img": [-1]},
        )


def test_chunk_shape_rejects_non_list_non_dict(aligned_zarrv2_store):
    with pytest.raises(TypeError, match="chunk_shape must be a list, dict, or None"):
        zarrv2_datasource.ZarrV2Datasource(
            str(aligned_zarrv2_store),
            chunk_shape="invalid",
        )


def test_default_chunk_shape_uses_each_arrays_native_chunks(
    heterogeneous_zarrv2_store,
):
    """When no chunk_shape given, every array reads at its native chunk size.

    Unlike the old min-of-axis-0 default, this lets a 4-D image array with
    tiny chunks coexist with a 2-D pose array with big chunks without
    forcing either into the other's chunk size.
    """
    datasource = zarrv2_datasource.ZarrV2Datasource(str(heterogeneous_zarrv2_store))
    assert datasource._array_chunks == {
        "data/camera0_rgb": (1, 2, 2, 3),
        "data/robot0_eef_pos": (10, 3),
        "meta/episode_ends": (3,),
    }


def test_chunk_shape_prefix_broadcast_across_mixed_rank(heterogeneous_zarrv2_store):
    """``chunk_shape=[5]`` overrides axis 0 across arrays of all ranks at once."""
    datasource = zarrv2_datasource.ZarrV2Datasource(
        str(heterogeneous_zarrv2_store),
        chunk_shape=[5],
    )
    assert datasource._array_chunks == {
        "data/camera0_rgb": (5, 2, 2, 3),  # 4-D: axis 0 overridden, rest native
        "data/robot0_eef_pos": (5, 3),  # 2-D: axis 0 overridden, rest native
        "meta/episode_ends": (5,),  # 1-D: axis 0 overridden
    }


# ---------------------------------------------------------------------------
# align_axis_0 (wide-form mode)
# ---------------------------------------------------------------------------


def test_align_axis_0_emits_wide_rows(aligned_zarrv2_store):
    """Wide-row schema: ``t_start``, ``t_stop``, one column per aligned array."""
    datasource = zarrv2_datasource.ZarrV2Datasource(
        str(aligned_zarrv2_store),
        align_axis_0=["img", "state", "label"],
        chunk_shape=[4],
    )
    df = _execute_read_tasks(datasource.get_read_tasks(parallelism=4))
    assert list(df.columns) == ["t_start", "t_stop", "img", "state", "label"]
    # shape[0]=8, chunk_shape=[4] -> 2 rows.
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


def test_align_axis_0_true_aligns_all_selected(aligned_zarrv2_store):
    """``align_axis_0=True`` aligns every selected array."""
    datasource = zarrv2_datasource.ZarrV2Datasource(
        str(aligned_zarrv2_store),
        align_axis_0=True,
        chunk_shape=[4],
    )
    df = _execute_read_tasks(datasource.get_read_tasks(parallelism=4))
    assert set(df.columns) == {"t_start", "t_stop", "img", "state", "label"}


def test_align_axis_0_filters_non_listed_arrays(aligned_zarrv2_store):
    """Arrays not in ``align_axis_0=[...]`` are dropped from the output."""
    datasource = zarrv2_datasource.ZarrV2Datasource(
        str(aligned_zarrv2_store),
        align_axis_0=["img", "state"],
        chunk_shape=[4],
    )
    df = _execute_read_tasks(datasource.get_read_tasks(parallelism=4))
    assert "label" not in df.columns
    assert set(df.columns) == {"t_start", "t_stop", "img", "state"}


def test_align_axis_0_rejects_misaligned_shape0(heterogeneous_zarrv2_store):
    """Misalignment raises and suggests the largest aligned subset."""
    with pytest.raises(
        ValueError,
        match=r"Largest aligned subset has shape\[0\]=20: "
        r"\['data/camera0_rgb', 'data/robot0_eef_pos'\]",
    ):
        zarrv2_datasource.ZarrV2Datasource(
            str(heterogeneous_zarrv2_store),
            align_axis_0=True,
            chunk_shape=[5],
        )


def test_align_axis_0_rejects_missing_array_name(aligned_zarrv2_store):
    with pytest.raises(
        ValueError, match=r"align_axis_0 names not found in store: \['nope'\]"
    ):
        zarrv2_datasource.ZarrV2Datasource(
            str(aligned_zarrv2_store),
            align_axis_0=["img", "nope"],
        )


def test_align_axis_0_rejects_divergent_axis_0_chunks(aligned_zarrv2_store):
    """If aligned arrays end up with different axis-0 chunks, error clearly.

    The native chunks differ (img=2, state=4, label=8) — without a
    ``chunk_shape`` re-tile they all stay at native, and the validator
    catches the mismatch.
    """
    with pytest.raises(
        ValueError, match="Aligned arrays must share the same axis-0 chunk size"
    ):
        zarrv2_datasource.ZarrV2Datasource(
            str(aligned_zarrv2_store),
            align_axis_0=True,
        )


def test_align_axis_0_with_per_pattern_chunk_shape(aligned_zarrv2_store):
    """A dict ``chunk_shape`` can resolve all aligned arrays to the same prefix."""
    datasource = zarrv2_datasource.ZarrV2Datasource(
        str(aligned_zarrv2_store),
        align_axis_0=True,
        chunk_shape={"img": [4], "state": [4], "label": [4]},
    )
    df = _execute_read_tasks(datasource.get_read_tasks(parallelism=4))
    # 2 rows.
    assert len(df) == 2


def test_chunk_shape_rejected_when_longer_than_smallest_array(
    heterogeneous_zarrv2_store,
):
    """``chunk_shape`` longer than any selected array's rank is an error."""
    with pytest.raises(
        ValueError, match=r"chunk_shape has 2 axes but array of shape .* has rank 1"
    ):
        zarrv2_datasource.ZarrV2Datasource(
            str(heterogeneous_zarrv2_store),
            chunk_shape=[2, 2],  # OK for 2-D and 4-D, fails for 1-D episode_ends
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


def test_edge_chunk_is_shorter_not_padded(tmp_path):
    """Final chunk has length < chunk_shape; nothing is zero-padded."""
    store_path = tmp_path / "edge.zarr"
    src = np.arange(7, dtype="<i4")
    _write_real_zarr_store(store_path, {"data": (src, (3,))})

    datasource = zarrv2_datasource.ZarrV2Datasource(str(store_path))
    df = _execute_read_tasks(datasource.get_read_tasks(parallelism=16))
    lengths = sorted(chunk.shape[0] for chunk in df["chunk"])
    # 7 samples, native chunks=3 → [3, 3, 1] — last is shorter, not padded.
    assert lengths == [1, 3, 3]


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

    datasource = zarrv2_datasource.ZarrV2Datasource(str(store_path))
    df = _execute_read_tasks(datasource.get_read_tasks(parallelism=16))
    reconstructed = _reconstruct_array(df, "data")

    # Bit-exact comparison so NaN, +Inf, -Inf, and finite values are all
    # checked uniformly.
    np.testing.assert_array_equal(reconstructed.view(np.uint32), source.view(np.uint32))


def test_chunk_shape_override_changes_grid(tmp_path):
    """User-supplied chunk_shape controls the chunk grid and row count."""
    store_path = tmp_path / "tile.zarr"
    src = np.arange(10, dtype="<i4")
    _write_real_zarr_store(store_path, {"data": (src, (2,))})  # native: 5 chunks

    datasource = zarrv2_datasource.ZarrV2Datasource(str(store_path), chunk_shape=[5])
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
    # Chunk dtypes match each array's storage dtype.
    rgb_rows = df[df["array"] == "data/camera0_rgb"]
    pos_rows = df[df["array"] == "data/robot0_eef_pos"]
    ep_rows = df[df["array"] == "meta/episode_ends"]
    assert all(c.dtype == np.uint8 for c in rgb_rows["chunk"])
    assert all(c.dtype == np.float32 for c in pos_rows["chunk"])
    assert all(c.dtype == np.int64 for c in ep_rows["chunk"])


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
    """Retryable network errors retried with backoff, eventual read succeeds."""
    expected = np.array([1, 2, 3], dtype="<i4")
    arr = _ScriptedArray(
        ConnectionError("Connection reset by peer"),
        TimeoutError("Read timeout"),
        expected,
    )
    root = _ScriptedRoot(x=arr)

    out = zarrv2_datasource._read_chunk(
        root, "x", ((0, 3),), max_retries=5, base_delay=0
    )
    np.testing.assert_array_equal(out, expected)


def test_read_chunk_exhausts_retries():
    """Retryable error every attempt → ``RuntimeError`` chained from last error."""
    arr = _ScriptedArray(
        ConnectionError("Connection reset"),
        ConnectionError("Connection reset"),
        ConnectionError("Connection reset"),
    )
    root = _ScriptedRoot(x=arr)

    with pytest.raises(RuntimeError, match=r"after 3 attempts") as exc_info:
        zarrv2_datasource._read_chunk(root, "x", ((0, 3),), max_retries=3, base_delay=0)
    assert isinstance(exc_info.value.__cause__, ConnectionError)


def test_read_chunk_non_retryable_immediately_raises():
    """A non-retry-keyword error is raised on the first attempt, not retried."""
    arr = _ScriptedArray(
        ValueError("array is corrupt"),
        # A second response that would succeed if retried — should be unreached.
        np.array([1, 2, 3], dtype="<i4"),
    )
    root = _ScriptedRoot(x=arr)

    with pytest.raises(ValueError, match="corrupt"):
        zarrv2_datasource._read_chunk(root, "x", ((0, 3),), max_retries=5, base_delay=0)


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
                chunk_shape=[4],
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
        chunk_shape=[4],
        array_paths=["nested"],
        allow_full_metadata_scan=False,
        align_axis_0=None,
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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
