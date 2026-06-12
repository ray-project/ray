"""Tests for the LeRobot v3 datasource.

These tests create minimal LeRobot v3 dataset layouts on disk (parquet
metadata files, data parquet files, and small mp4 video files) and verify
that ``ray.data.read_lerobot`` reads them correctly across all partitioning
modes.
"""

import importlib.util
import json
import os

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import ray
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa

# lerobot >=0.5 requires Python >=3.12, so the whole stack (lerobot, its
# torchcodec decoder, and the av writer these tests use) is only installed on
# py3.12+. Skip cleanly elsewhere rather than failing on the lerobot import.
AV_AVAILABLE = importlib.util.find_spec("av") is not None
FSSPEC_AVAILABLE = importlib.util.find_spec("fsspec") is not None
LEROBOT_AVAILABLE = importlib.util.find_spec("lerobot") is not None

pytestmark = pytest.mark.skipif(
    not (AV_AVAILABLE and FSSPEC_AVAILABLE and LEROBOT_AVAILABLE),
    reason="lerobot[dataset] (Python >=3.12), av, or fsspec not available. "
    "Install with: pip install 'lerobot[dataset]>=0.5.0' av fsspec",
)


# ---------------------------------------------------------------------------
# Helpers to create a minimal LeRobot v3 dataset on disk
# ---------------------------------------------------------------------------

FPS = 10
FRAME_H, FRAME_W, FRAME_C = 4, 4, 3


def _create_video(path: str, num_frames: int, fps: int = FPS) -> None:
    """Write a tiny mp4 video with solid-colored frames."""
    import av

    os.makedirs(os.path.dirname(path), exist_ok=True)
    container = av.open(path, mode="w")
    stream = container.add_stream("mpeg4", rate=fps)
    stream.width = FRAME_W
    stream.height = FRAME_H
    stream.pix_fmt = "yuv420p"

    for i in range(num_frames):
        img = np.full(
            (FRAME_H, FRAME_W, FRAME_C), fill_value=(i * 25) % 256, dtype=np.uint8
        )
        frame = av.VideoFrame.from_ndarray(img, format="rgb24")
        for packet in stream.encode(frame):
            container.mux(packet)

    for packet in stream.encode():
        container.mux(packet)
    container.close()


def create_lerobot_dataset(
    root: str,
    num_episodes: int = 3,
    frames_per_episode: int = 5,
    has_video: bool = True,
) -> str:
    """Create a minimal LeRobot v3 dataset directory structure.

    Returns the root path.
    """
    os.makedirs(root, exist_ok=True)
    total_frames = num_episodes * frames_per_episode

    # -- meta/info.json --
    features = {
        "index": {"dtype": "int64", "shape": [1]},
        "episode_index": {"dtype": "int64", "shape": [1]},
        "frame_index": {"dtype": "int64", "shape": [1]},
        "timestamp": {"dtype": "float64", "shape": [1]},
        "task_index": {"dtype": "int64", "shape": [1]},
        "action": {"dtype": "float32", "shape": [2]},
        "state": {"dtype": "float32", "shape": [2]},
    }
    info = {
        "codebase_version": "v3.0",
        "total_frames": total_frames,
        "total_episodes": num_episodes,
        "total_tasks": 1,
        "fps": FPS,
        "data_path": "data/chunk-{chunk_index:03d}/file-{file_index:03d}.parquet",
        "features": features,
    }
    if has_video:
        features["observation.image"] = {
            "dtype": "video",
            "shape": [FRAME_H, FRAME_W, FRAME_C],
        }
        info[
            "video_path"
        ] = "videos/{video_key}/chunk-{chunk_index:03d}/file-{file_index:03d}.mp4"

    meta_dir = os.path.join(root, "meta")
    os.makedirs(meta_dir, exist_ok=True)
    with open(os.path.join(meta_dir, "info.json"), "w") as f:
        json.dump(info, f)

    # -- meta/stats.json --
    stats = {
        "action": {"mean": [0.0, 0.0], "std": [1.0, 1.0]},
        "state": {"mean": [0.0, 0.0], "std": [1.0, 1.0]},
    }
    with open(os.path.join(meta_dir, "stats.json"), "w") as f:
        json.dump(stats, f)

    # -- meta/tasks.parquet --
    # lerobot expects task names as the DataFrame *index* (named "task")
    # with `task_index` as a column.  Write via pandas so the index round-
    # trips through parquet correctly.
    tasks_df = pd.DataFrame(
        {"task_index": [0]},
        index=pd.Index(["test_task"], name="task"),
    )
    tasks_df.to_parquet(os.path.join(meta_dir, "tasks.parquet"))

    # -- meta/episodes/chunk-000/*.parquet --
    ep_dir = os.path.join(meta_dir, "episodes", "chunk-000")
    os.makedirs(ep_dir, exist_ok=True)

    ep_data = {
        "episode_index": list(range(num_episodes)),
        "length": [frames_per_episode] * num_episodes,
        "data/chunk_index": [0] * num_episodes,
        "data/file_index": [0] * num_episodes,
    }
    if has_video:
        ep_data["videos/observation.image/chunk_index"] = [0] * num_episodes
        ep_data["videos/observation.image/file_index"] = list(range(num_episodes))
        ep_data["videos/observation.image/from_timestamp"] = [0.0] * num_episodes

    episodes_table = pa.table(ep_data)
    pq.write_table(episodes_table, os.path.join(ep_dir, "episodes-000.parquet"))

    # -- data/chunk-000/file-000.parquet --
    data_dir = os.path.join(root, "data", "chunk-000")
    os.makedirs(data_dir, exist_ok=True)

    indices = list(range(total_frames))
    ep_indices = []
    frame_indices = []
    timestamps = []
    for ep in range(num_episodes):
        for fr in range(frames_per_episode):
            ep_indices.append(ep)
            frame_indices.append(fr)
            timestamps.append(fr / FPS)

    data_table = pa.table(
        {
            "index": pa.array(indices, type=pa.int64()),
            "episode_index": pa.array(ep_indices, type=pa.int64()),
            "frame_index": pa.array(frame_indices, type=pa.int64()),
            "timestamp": pa.array(timestamps, type=pa.float64()),
            "task_index": pa.array([0] * total_frames, type=pa.int64()),
            "action": [
                np.array([float(i), float(i + 1)], dtype=np.float32) for i in indices
            ],
            "state": [
                np.array([float(i) * 0.1, float(i) * 0.2], dtype=np.float32)
                for i in indices
            ],
        }
    )
    pq.write_table(data_table, os.path.join(data_dir, "file-000.parquet"))

    # -- videos/ --
    if has_video:
        for ep in range(num_episodes):
            video_path = os.path.join(
                root,
                "videos",
                "observation.image",
                "chunk-000",
                f"file-{ep:03d}.mp4",
            )
            _create_video(video_path, frames_per_episode, FPS)

    return root


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def lerobot_dataset(tmp_path):
    """Create a minimal LeRobot dataset with video."""
    return create_lerobot_dataset(str(tmp_path / "ds"))


@pytest.fixture
def lerobot_dataset_no_video(tmp_path):
    """Create a minimal LeRobot dataset without video."""
    return create_lerobot_dataset(str(tmp_path / "ds_nv"), has_video=False)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_read_lerobot_basic(ray_start_regular_shared, lerobot_dataset):
    """Test basic reading of a LeRobot dataset."""
    ds = ray.data.read_lerobot(lerobot_dataset)
    assert ds.count() == 15  # 3 episodes * 5 frames

    rows = ds.take_all()
    for row in rows:
        assert "index" in row
        assert "episode_index" in row
        assert "frame_index" in row
        assert "timestamp" in row
        assert "action" in row
        assert "state" in row
        assert "task" in row
        assert "dataset_index" in row
        assert "observation.image" in row


def test_read_lerobot_no_video(ray_start_regular_shared, lerobot_dataset_no_video):
    """Test reading a dataset without video keys."""
    ds = ray.data.read_lerobot(lerobot_dataset_no_video)
    assert ds.count() == 15

    rows = ds.take_all()
    for row in rows:
        assert "action" in row
        assert "state" in row
        assert "observation.image" not in row


def test_read_lerobot_stats_column(ray_start_regular_shared, lerobot_dataset_no_video):
    """The ``stats`` column exposes per-feature normalization stats as JSON."""
    ds = ray.data.read_lerobot(lerobot_dataset_no_video)
    rows = ds.take_all()
    assert rows
    for row in rows:
        assert "stats" in row
        stats = json.loads(row["stats"])
        # create_lerobot_dataset writes mean/std for action and state.
        for feat in ("action", "state"):
            assert feat in stats
            np.testing.assert_allclose(
                np.asarray(stats[feat]["mean"]).flatten(), [0.0, 0.0]
            )
            np.testing.assert_allclose(
                np.asarray(stats[feat]["std"]).flatten(), [1.0, 1.0]
            )


def test_read_lerobot_frame_tolerance_default(
    ray_start_regular_shared, lerobot_dataset_no_video
):
    """frame_tolerance_s defaults to None (resolved to 0.5/fps at decode time)."""
    from ray.data.datasource import LeRobotDatasource

    source = LeRobotDatasource(lerobot_dataset_no_video)
    assert source._roots[0].frame_tolerance_s is None


def test_read_lerobot_frame_tolerance_override(
    ray_start_regular_shared, lerobot_dataset_no_video
):
    """An explicit frame_tolerance_s is threaded onto every root."""
    from ray.data.datasource import LeRobotDatasource

    source = LeRobotDatasource(lerobot_dataset_no_video, frame_tolerance_s=0.25)
    assert source._roots[0].frame_tolerance_s == 0.25


def test_read_lerobot_frame_tolerance_invalid(
    ray_start_regular_shared, lerobot_dataset_no_video
):
    """Non-positive frame_tolerance_s is rejected."""
    from ray.data.datasource import LeRobotDatasource

    with pytest.raises(ValueError, match="frame_tolerance_s must be"):
        LeRobotDatasource(lerobot_dataset_no_video, frame_tolerance_s=0)


def test_read_lerobot_file_uri(ray_start_regular_shared, lerobot_dataset):
    """A ``file://`` URI exercises the remote metadata-copy + torchcodec decode
    path (the non-local branch) end to end, using only local files."""
    ds = ray.data.read_lerobot("file://" + lerobot_dataset)
    assert ds.count() == 15

    rows = ds.take(1)
    assert len(rows) == 1
    frame = np.asarray(rows[0]["observation.image"], dtype=np.uint8)
    assert frame.size == FRAME_H * FRAME_W * FRAME_C


def test_lerobot_compat_warns_when_lerobot_supports_storage_options(monkeypatch):
    """When lerobot gains native storage_options support, the compat shim warns
    (so it gets removed) rather than silently switching to the native path."""
    from ray.data._internal.datasource import _lerobot_compat as compat

    monkeypatch.setattr(compat, "_native_storage_options", lambda: True)
    monkeypatch.setattr(compat, "_WARNED_NATIVE_AVAILABLE", False)
    with pytest.warns(RuntimeWarning, match="no longer necessary"):
        compat._warn_if_native_available()


def test_read_lerobot_sequential(ray_start_regular_shared, lerobot_dataset):
    """Test SEQUENTIAL partitioning."""
    from ray.data.datasource import LeRobotPartitioning

    ds = ray.data.read_lerobot(
        lerobot_dataset, partitioning=LeRobotPartitioning.SEQUENTIAL
    )
    assert ds.count() == 15


def test_read_lerobot_episode(ray_start_regular_shared, lerobot_dataset):
    """Test EPISODE partitioning."""
    from ray.data.datasource import LeRobotPartitioning

    ds = ray.data.read_lerobot(
        lerobot_dataset, partitioning=LeRobotPartitioning.EPISODE
    )
    assert ds.count() == 15

    rows = ds.take_all()
    episode_indices = {row["episode_index"] for row in rows}
    assert episode_indices == {0, 1, 2}


def test_read_lerobot_row_block(ray_start_regular_shared, lerobot_dataset):
    """Test ROW_BLOCK partitioning."""
    from ray.data.datasource import LeRobotPartitioning

    ds = ray.data.read_lerobot(
        lerobot_dataset,
        partitioning=LeRobotPartitioning.ROW_BLOCK,
        block_size=5,
    )
    assert ds.count() == 15


def test_read_lerobot_chain(ray_start_regular_shared, lerobot_dataset):
    """Test CHAIN partitioning."""
    from ray.data.datasource import LeRobotPartitioning

    ds = ray.data.read_lerobot(lerobot_dataset, partitioning=LeRobotPartitioning.CHAIN)
    assert ds.count() == 15


def test_read_lerobot_file_group(ray_start_regular_shared, lerobot_dataset):
    """Test FILE_GROUP partitioning (default)."""
    from ray.data.datasource import LeRobotPartitioning

    ds = ray.data.read_lerobot(
        lerobot_dataset, partitioning=LeRobotPartitioning.FILE_GROUP
    )
    assert ds.count() == 15


def test_read_lerobot_string_partitioning(ray_start_regular_shared, lerobot_dataset):
    """Test that string partitioning values work."""
    ds = ray.data.read_lerobot(lerobot_dataset, partitioning="episode")
    assert ds.count() == 15


def test_read_lerobot_scalar_parity(ray_start_regular_shared, lerobot_dataset):
    """Test that scalar columns are correct across partitioning modes."""
    from ray.data.datasource import LeRobotPartitioning

    ds = ray.data.read_lerobot(
        lerobot_dataset, partitioning=LeRobotPartitioning.SEQUENTIAL
    )
    rows = sorted(ds.take_all(), key=lambda r: r["index"])

    for i, row in enumerate(rows):
        assert row["index"] == i
        assert row["episode_index"] == i // 5
        assert row["frame_index"] == i % 5
        assert row["task"] == "test_task"
        assert row["dataset_index"] == 0


def test_read_lerobot_action_state_values(ray_start_regular_shared, lerobot_dataset):
    """Test that action and state vector values are correct."""
    from ray.data.datasource import LeRobotPartitioning

    ds = ray.data.read_lerobot(
        lerobot_dataset, partitioning=LeRobotPartitioning.SEQUENTIAL
    )
    rows = sorted(ds.take_all(), key=lambda r: r["index"])

    for row in rows:
        i = row["index"]
        action = np.asarray(row["action"], dtype=np.float32).flatten()
        state = np.asarray(row["state"], dtype=np.float32).flatten()
        np.testing.assert_allclose(action, [float(i), float(i + 1)], rtol=1e-5)
        np.testing.assert_allclose(state, [float(i) * 0.1, float(i) * 0.2], rtol=1e-5)


def test_read_lerobot_video_frames_present(ray_start_regular_shared, lerobot_dataset):
    """Test that video frames are decoded and present."""
    from ray.data.datasource import LeRobotPartitioning

    ds = ray.data.read_lerobot(
        lerobot_dataset, partitioning=LeRobotPartitioning.SEQUENTIAL
    )
    rows = ds.take(1)
    assert len(rows) == 1

    frame = np.asarray(rows[0]["observation.image"], dtype=np.uint8)
    # Frame should be reshapeable to (H, W, C)
    assert frame.size == FRAME_H * FRAME_W * FRAME_C


def test_read_lerobot_multi_root(ray_start_regular_shared, tmp_path):
    """Test reading multiple dataset roots."""
    root_a = create_lerobot_dataset(str(tmp_path / "ds_a"), num_episodes=2)
    root_b = create_lerobot_dataset(str(tmp_path / "ds_b"), num_episodes=2)

    ds = ray.data.read_lerobot([root_a, root_b])
    rows = ds.take_all()
    assert len(rows) == 20  # 2 roots * 2 episodes * 5 frames

    dataset_indices = {row["dataset_index"] for row in rows}
    assert dataset_indices == {0, 1}


def test_read_lerobot_multi_root_single_string(
    ray_start_regular_shared, lerobot_dataset
):
    """Single-string API must produce dataset_index == 0."""
    ds = ray.data.read_lerobot(lerobot_dataset)
    rows = ds.take_all()
    assert all(row["dataset_index"] == 0 for row in rows)


def test_read_lerobot_missing_dataset(ray_start_regular_shared):
    """Test error on non-existent dataset."""
    with pytest.raises(FileNotFoundError, match="meta/info.json"):
        ray.data.read_lerobot("/nonexistent/dataset")


def test_read_lerobot_invalid_partitioning(ray_start_regular_shared, lerobot_dataset):
    """Test error on invalid partitioning mode."""
    with pytest.raises(ValueError, match="Unknown partitioning"):
        ray.data.read_lerobot(lerobot_dataset, partitioning="invalid")


def test_read_lerobot_row_block_requires_block_size(
    ray_start_regular_shared, lerobot_dataset
):
    """Test that ROW_BLOCK requires block_size."""
    from ray.data.datasource import LeRobotPartitioning

    with pytest.raises(ValueError, match="block_size is required"):
        ds = ray.data.read_lerobot(
            lerobot_dataset, partitioning=LeRobotPartitioning.ROW_BLOCK
        )
        ds.materialize()


def test_read_lerobot_incompatible_fps_raises(ray_start_regular_shared, tmp_path):
    """Test that mismatched fps raises ValueError."""
    from ray.data.datasource import LeRobotDatasource

    root_a = create_lerobot_dataset(
        str(tmp_path / "ds_a"), num_episodes=1, has_video=False
    )
    root_b = create_lerobot_dataset(
        str(tmp_path / "ds_b"), num_episodes=1, has_video=False
    )

    # Modify fps in root_b
    info_path = os.path.join(root_b, "meta", "info.json")
    with open(info_path, "r") as f:
        info = json.load(f)
    info["fps"] = 30
    with open(info_path, "w") as f:
        json.dump(info, f)

    with pytest.raises(ValueError, match="fps mismatch"):
        LeRobotDatasource([root_a, root_b])


def test_read_lerobot_missing_dependency(ray_start_regular_shared, lerobot_dataset):
    """Test graceful failure when av is missing."""
    from unittest.mock import patch

    with patch.dict("sys.modules", {"av": None}):
        with pytest.raises(ImportError, match="LeRobotDatasource.*depends on 'av'"):
            ray.data.read_lerobot(lerobot_dataset)


def test_read_lerobot_metadata(ray_start_regular_shared, lerobot_dataset):
    """Test that metadata is accessible via the datasource as a pristine
    lerobot ``LeRobotDatasetMetadata`` instance."""
    from ray.data.datasource import LeRobotDatasource

    source = LeRobotDatasource(lerobot_dataset)
    assert source.meta.total_frames == 15
    assert source.meta.total_episodes == 3
    assert source.meta.video_keys == ["observation.image"]
    assert source.meta.fps == FPS


def test_read_lerobot_all_modes_same_row_count(
    ray_start_regular_shared, lerobot_dataset
):
    """All partitioning modes must produce the same row count."""
    from ray.data.datasource import LeRobotPartitioning

    expected = 15
    for mode in LeRobotPartitioning:
        kwargs = {}
        if mode == LeRobotPartitioning.ROW_BLOCK:
            kwargs["block_size"] = 5
        ds = ray.data.read_lerobot(lerobot_dataset, partitioning=mode, **kwargs)
        assert (
            ds.count() == expected
        ), f"Mode {mode.name}: expected {expected}, got {ds.count()}"


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
