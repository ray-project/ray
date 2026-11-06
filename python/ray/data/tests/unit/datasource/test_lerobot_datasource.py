import os
import sys

import fsspec
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ray.data._internal.datasource.lerobot_datasource import (
    _decode_video_delta,
    _LeRobotRoot,
    _prepare_delta_segment,
    _raise_on_pickle_object_meta_parquet,
    _read_lerobot_segment,
)
from ray.data._internal.object_extensions.arrow import (
    AUTOLOAD_PICKLE_OBJECT_SCALAR_ENV_VAR,
    ArrowPythonObjectArray,
)

N_ROWS = 3


def _exploit_column(marker, n):
    class Exploit:
        def __reduce__(self):
            return (os.system, (f"touch {marker}",))

    return ArrowPythonObjectArray.from_objects([Exploit()] * n)


def _shard_columns(n=N_ROWS):
    return {
        "index": pa.array(range(n), pa.int64()),
        "episode_index": pa.array([0] * n, pa.int64()),
        "frame_index": pa.array(range(n), pa.int64()),
        "timestamp": pa.array([i / 10 for i in range(n)], pa.float64()),
        "task_index": pa.array([0] * n, pa.int64()),
    }


def _make_root(tmp_path) -> _LeRobotRoot:
    """A no-camera, no-delta root; the segment reader only touches a few fields."""
    return _LeRobotRoot(
        root=str(tmp_path),
        fs=fsspec.filesystem("file"),
        fs_root=str(tmp_path),
        data_path="data/chunk-{chunk_index:03d}/file-{file_index:03d}.parquet",
        video_path=None,
        video_keys=[],
        image_keys=[],
        tasks_dict={0: "test_task"},
        schema=pa.schema([]),
        row_size_bytes=64,
        total_frames=N_ROWS,
        fps=10,
        storage_options={},
        stats_json="{}",
        frame_tolerance_s=0.05,
        delta_steps={},
    )


def _read_segment(root, shard):
    # ``ep_slice`` is only consulted for video or delta reads.
    return list(
        _read_lerobot_segment(root, 0, N_ROWS, 0, [str(shard)], pa.table({}), 1 << 20)
    )


def test_read_lerobot_segment_allows_plain_columns(tmp_path):
    shard = tmp_path / "shard.parquet"
    pq.write_table(pa.table(_shard_columns()), shard)

    tables = _read_segment(_make_root(tmp_path), shard)

    assert len(tables) == 1
    assert tables[0].num_rows == N_ROWS
    assert tables[0].column("task").to_pylist() == ["test_task"] * N_ROWS


@pytest.mark.parametrize("evil_col", ["evil", "task_index"])
def test_read_lerobot_segment_rejects_pickle_object_columns(tmp_path, evil_col):
    """A tampered data shard must be rejected before anything is unpickled, even
    when the evil column shadows ``task_index``, which the reader materializes."""
    marker = tmp_path / "exploit_marker"
    shard = tmp_path / "shard.parquet"
    columns = {**_shard_columns(), evil_col: _exploit_column(marker, N_ROWS)}
    pq.write_table(pa.table(columns), shard)

    with pytest.raises(ValueError, match="arrow_pickled_object"):
        _read_segment(_make_root(tmp_path), shard)

    assert not marker.exists(), "pickle.load executed attacker code"


def test_read_lerobot_segment_allows_pickle_object_columns_with_env_var(
    tmp_path, monkeypatch
):
    monkeypatch.setenv(AUTOLOAD_PICKLE_OBJECT_SCALAR_ENV_VAR, "1")
    shard = tmp_path / "shard.parquet"
    columns = {
        **_shard_columns(),
        "obj": ArrowPythonObjectArray.from_objects([{"key": "value"}] * N_ROWS),
    }
    pq.write_table(pa.table(columns), shard)

    tables = _read_segment(_make_root(tmp_path), shard)

    assert tables[0].column("obj").to_pylist() == [{"key": "value"}] * N_ROWS


def _write_clean_meta(meta_dir):
    pq.write_table(
        pa.table({"task_index": [0], "task": ["test_task"]}),
        meta_dir / "tasks.parquet",
    )
    ep_dir = meta_dir / "episodes" / "chunk-000"
    ep_dir.mkdir(parents=True)
    pq.write_table(
        pa.table({"episode_index": [0], "length": [N_ROWS]}),
        ep_dir / "episodes-000.parquet",
    )


@pytest.mark.parametrize(
    "victim", ["tasks.parquet", "episodes/chunk-000/episodes-000.parquet"]
)
def test_raise_on_pickle_object_meta_parquet_rejects(tmp_path, victim):
    """lerobot parses these files itself with pandas / HF datasets, so they are
    checked before they are handed over."""
    marker = tmp_path / "exploit_marker"
    meta_dir = tmp_path / "meta"
    meta_dir.mkdir()
    _write_clean_meta(meta_dir)
    path = meta_dir / victim
    original = pq.read_table(path)
    pq.write_table(
        original.append_column("evil", _exploit_column(marker, original.num_rows)),
        path,
    )

    with pytest.raises(ValueError, match="arrow_pickled_object") as exc_info:
        _raise_on_pickle_object_meta_parquet(str(meta_dir), "s3://bucket/ds")

    assert f"meta/{victim}" in str(exc_info.value)
    assert not marker.exists(), "pickle.load executed attacker code"


def test_raise_on_pickle_object_meta_parquet_allows_clean(tmp_path):
    meta_dir = tmp_path / "meta"
    meta_dir.mkdir()
    _write_clean_meta(meta_dir)

    _raise_on_pickle_object_meta_parquet(str(meta_dir), "s3://bucket/ds")

    empty = tmp_path / "empty_meta"
    empty.mkdir()
    _raise_on_pickle_object_meta_parquet(str(empty), "s3://bucket/ds")


def test_prepare_delta_segment_and_read_segment_vectorized(tmp_path):
    """Verify vectorized delta segment preparation, batch assembly, and contiguity check."""
    import numpy as np

    shard = tmp_path / "shard.parquet"
    cols = {
        "index": pa.array([10, 11, 12, 13, 14], pa.int64()),
        "episode_index": pa.array([2, 2, 2, 3, 3], pa.int64()),
        "frame_index": pa.array([0, 1, 2, 0, 1], pa.int64()),
        "timestamp": pa.array([0.0, 0.1, 0.2, 0.0, 0.1], pa.float64()),
        "task_index": pa.array([0, 0, 0, 1, 1], pa.int64()),
        "state": pa.array([1.0, 2.0, 3.0, 4.0, 5.0], pa.float32()),
    }
    full = pa.table(cols)
    pq.write_table(full, shard)

    ep_slice = pa.table(
        {
            "episode_index": pa.array([2, 3], pa.int64()),
            "_global_from_index": pa.array([10, 13], pa.int64()),
            "_global_to_index": pa.array([13, 15], pa.int64()),
        }
    )

    root = _make_root(tmp_path)._replace(
        tasks_dict={0: "pick", 1: "place"},
        total_frames=5,
        delta_steps={"state": [-1, 0, 1]},
    )

    seg = _prepare_delta_segment(root, full, ep_slice)
    assert isinstance(seg.global_idx, np.ndarray)
    assert isinstance(seg.episode_index_col, np.ndarray)
    assert isinstance(seg.row_from, np.ndarray)
    assert isinstance(seg.row_to, np.ndarray)
    np.testing.assert_array_equal(seg.global_idx, np.array([10, 11, 12, 13, 14]))
    np.testing.assert_array_equal(seg.episode_index_col, np.array([2, 2, 2, 3, 3]))
    np.testing.assert_array_equal(seg.row_from, np.array([10, 10, 10, 13, 13]))
    np.testing.assert_array_equal(seg.row_to, np.array([13, 13, 13, 15, 15]))

    tables = list(
        _read_lerobot_segment(
            root, 10, 15, 0, [str(shard)], ep_slice, max_block_bytes=128
        )
    )
    combined = pa.concat_tables(tables)
    assert combined.num_rows == 5
    assert combined.column("task").to_pylist() == [
        "pick",
        "pick",
        "pick",
        "place",
        "place",
    ]
    state_windows = np.stack(
        combined.column("state").combine_chunks().to_numpy(zero_copy_only=False)
    )
    pad_masks = np.stack(
        combined.column("state_is_pad").combine_chunks().to_numpy(zero_copy_only=False)
    )
    # Episode 2 spans [10, 13): states [1.0, 2.0, 3.0]
    # Episode 3 spans [13, 15): states [4.0, 5.0]
    expected_state_windows = np.array(
        [
            [1.0, 1.0, 2.0],  # anchor 10: step -1 clamped to 10 (padded)
            [1.0, 2.0, 3.0],  # anchor 11: in-bounds
            [2.0, 3.0, 3.0],  # anchor 12: step +1 clamped to 12 (padded)
            [4.0, 4.0, 5.0],  # anchor 13: step -1 clamped to 13 (padded)
            [4.0, 5.0, 5.0],  # anchor 14: step +1 clamped to 14 (padded)
        ],
        dtype=np.float32,
    )
    expected_pad_masks = np.array(
        [
            [True, False, False],
            [False, False, False],
            [False, False, True],
            [True, False, False],
            [False, False, True],
        ],
        dtype=bool,
    )
    np.testing.assert_array_equal(state_windows, expected_state_windows)
    np.testing.assert_array_equal(pad_masks, expected_pad_masks)


def test_prepare_delta_segment_rejects_interior_index_gap(tmp_path):
    """Interior gaps/duplicates with matching first/last index must be rejected."""
    root = _make_root(tmp_path)._replace(delta_steps={"state": [-1, 0, 1]})
    # First=0, last=3, len=4 -> len == last - first + 1, but interior has duplicate 1 and gap at 2.
    full = pa.table(
        {
            "index": pa.array([0, 1, 1, 3], pa.int64()),
            "episode_index": pa.array([0, 0, 0, 0], pa.int64()),
            "state": pa.array([1.0, 2.0, 3.0, 4.0], pa.float32()),
        }
    )
    ep_slice = pa.table(
        {
            "episode_index": pa.array([0], pa.int64()),
            "_global_from_index": pa.array([0], pa.int64()),
            "_global_to_index": pa.array([4], pa.int64()),
        }
    )
    with pytest.raises(RuntimeError, match="contiguous whole-episode"):
        _prepare_delta_segment(root, full, ep_slice)


def test_prepare_delta_segment_rejects_duplicate_episode_index(tmp_path):
    """Duplicate episode_index rows in ep_slice must raise ValueError."""
    root = _make_root(tmp_path)._replace(delta_steps={"state": [-1, 0, 1]})
    full = pa.table(
        {
            "index": pa.array([0, 1, 2, 3], pa.int64()),
            "episode_index": pa.array([0, 0, 1, 1], pa.int64()),
            "state": pa.array([1.0, 2.0, 3.0, 4.0], pa.float32()),
        }
    )
    ep_slice = pa.table(
        {
            "episode_index": pa.array([0, 1, 1], pa.int64()),
            "_global_from_index": pa.array([0, 2, 99], pa.int64()),
            "_global_to_index": pa.array([2, 4, 101], pa.int64()),
        }
    )
    with pytest.raises(ValueError, match="duplicate episode_index"):
        _prepare_delta_segment(root, full, ep_slice)


def test_decode_video_delta_vectorized(tmp_path, monkeypatch):
    """Verify _decode_video_delta resolves clamped timestamps and pads across anchors in one pass."""
    import numpy as np

    calls = []
    delta_target_calls = []
    orig_delta_targets = _decode_video_delta.__globals__["_delta_targets"]

    def spy_delta_targets(global_idx, row_from, row_to, steps):
        delta_target_calls.append(np.asarray(global_idx))
        return orig_delta_targets(global_idx, row_from, row_to, steps)

    def fake_decode_frames_by_file(root, vk, file_requests, tolerance_s, cache):
        calls.append(file_requests)
        out = {}
        for (_chunk, _fi), reqs in file_requests.items():
            for slot, ts in reqs:
                frame = np.full((2, 2, 3), int(round(ts * 10)), dtype=np.uint8)
                out[slot] = frame
        return out

    monkeypatch.setitem(
        _decode_video_delta.__globals__,
        "_delta_targets",
        spy_delta_targets,
    )
    monkeypatch.setitem(
        _decode_video_delta.__globals__,
        "_decode_frames_by_file",
        fake_decode_frames_by_file,
    )
    root = _make_root(tmp_path)._replace(fps=10)
    video_meta = {"cam": {0: (0, 0, 1.0)}}
    stacked, pads = _decode_video_delta(
        root=root,
        anchors=range(0, 3),
        global_idx=[10, 11, 12],
        row_from=[10, 10, 10],
        row_to=[13, 13, 13],
        episode_index_col=[0, 0, 0],
        video_meta=video_meta,
        vk="cam",
        steps=[-1, 0, 1],
        tolerance_s=0.05,
        cache=None,
    )
    # _delta_targets must be invoked once across all anchors rather than per anchor.
    assert len(delta_target_calls) == 1
    np.testing.assert_array_equal(delta_target_calls[0], np.array([10, 11, 12]))

    expected_windows = [
        np.stack(
            [np.full((2, 2, 3), v, dtype=np.uint8) for v in window_vals], axis=0
        )
        for window_vals in ([10, 10, 11], [10, 11, 12], [11, 12, 12])
    ]
    expected_pads = [
        np.array([True, False, False], dtype=bool),
        np.array([False, False, False], dtype=bool),
        np.array([False, False, True], dtype=bool),
    ]
    assert len(stacked) == 3
    for actual_win, expected_win in zip(stacked, expected_windows):
        np.testing.assert_array_equal(actual_win, expected_win)
    for actual_pad, expected_pad in zip(pads, expected_pads):
        np.testing.assert_array_equal(actual_pad, expected_pad)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
