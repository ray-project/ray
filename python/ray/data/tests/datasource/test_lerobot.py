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

# lerobot >=0.5 requires Python >=3.12
LEROBOT_AVAILABLE = importlib.util.find_spec("lerobot") is not None

pytestmark = pytest.mark.skipif(
    not LEROBOT_AVAILABLE,
    reason="lerobot[dataset] (Python >=3.12) not available. "
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


def _png_bytes(value: int) -> bytes:
    """Encode a tiny solid-color frame to PNG bytes (for image-camera datasets)."""
    import io

    from PIL import Image

    img = np.full((FRAME_H, FRAME_W, FRAME_C), value % 256, dtype=np.uint8)
    buf = io.BytesIO()
    Image.fromarray(img).save(buf, format="PNG")
    return buf.getvalue()


def create_lerobot_dataset(
    root: str,
    num_episodes: int = 3,
    frames_per_episode: int = 5,
    has_video: bool = True,
    image_camera: bool = False,
) -> str:
    """Create a minimal LeRobot v3 dataset directory structure.

    A camera is stored either as mp4 video (``has_video=True``, the default) or
    as encoded-image structs (``dtype: image``) inside the data parquet
    (``image_camera=True``, no ``videos/`` tree); the two are mutually
    exclusive. Pass ``has_video=False`` for a dataset with no camera at all.

    Returns the root path.
    """
    if has_video and image_camera:
        raise ValueError(
            "has_video and image_camera are mutually exclusive: a camera is "
            "stored as mp4 video or as in-parquet images, not both. For an "
            "image-camera dataset pass has_video=False, image_camera=True."
        )
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
    if image_camera:
        features["observation.image"] = {
            "dtype": "image",
            "shape": [FRAME_H, FRAME_W, FRAME_C],
        }

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
        # lerobot v3's authoritative per-episode global frame range -- a running
        # counter aligned with the data `index` column (0..total_frames).
        "dataset_from_index": [i * frames_per_episode for i in range(num_episodes)],
        "dataset_to_index": [(i + 1) * frames_per_episode for i in range(num_episodes)],
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
    if image_camera:
        # Camera frames live in the parquet as HF Image structs (bytes + path).
        data_table = data_table.append_column(
            "observation.image",
            pa.array(
                [{"bytes": _png_bytes(i), "path": None} for i in indices],
                type=pa.struct([("bytes", pa.binary()), ("path", pa.string())]),
            ),
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


@pytest.fixture
def lerobot_dataset_image(tmp_path):
    """Create a minimal LeRobot dataset with an in-parquet image camera."""
    return create_lerobot_dataset(
        str(tmp_path / "ds_img"), has_video=False, image_camera=True
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "fixture_name, has_camera",
    [
        ("lerobot_dataset", True),  # mp4 video camera
        ("lerobot_dataset_no_video", False),  # no camera, scalars only
        ("lerobot_dataset_image", True),  # in-parquet image camera
    ],
    ids=["video", "no_camera", "image"],
)
def test_read_lerobot_camera_kinds(
    ray_start_regular_shared, request, fixture_name, has_camera
):
    """Each dataset shape (video / no-camera / image) reads to the expected rows
    and scalar columns; a camera, when present, decodes to an HWC uint8 tensor
    (and is absent otherwise)."""
    ds = ray.data.read_lerobot(request.getfixturevalue(fixture_name))
    rows = ds.take_all()
    assert len(rows) == 15  # 3 episodes * 5 frames

    for row in rows:
        for col in (
            "index",
            "episode_index",
            "frame_index",
            "timestamp",
            "action",
            "state",
            "task",
            "dataset_index",
            "stats",
        ):
            assert col in row
        if has_camera:
            frame = np.asarray(row["observation.image"])
            assert frame.shape == (FRAME_H, FRAME_W, FRAME_C)
            assert frame.dtype == np.uint8
            # Decoded to a tensor, not the raw {bytes, path} struct.
            assert not isinstance(row["observation.image"], dict)
        else:
            assert "observation.image" not in row


def test_read_lerobot_image_camera_pixel_values(
    ray_start_regular_shared, lerobot_dataset_image
):
    """Image-camera frames (lossless PNG in the parquet) decode to the exact
    fill the fixture wrote: row ``index`` i was encoded as a solid ``i % 256``."""
    rows = ray.data.read_lerobot(lerobot_dataset_image).take_all()
    assert rows
    for row in rows:
        frame = np.asarray(row["observation.image"])
        expected = row["index"] % 256
        assert frame.shape == (FRAME_H, FRAME_W, FRAME_C)
        assert frame.dtype == np.uint8
        assert (frame == expected).all(), (
            f"row {row['index']}: expected solid {expected}, "
            f"got min={int(frame.min())} max={int(frame.max())}"
        )


def test_decode_image_frames_from_path(tmp_path):
    """An image-camera cell holding a path instead of inline bytes (the rare
    non-embedded HF Image layout) is read from the referenced file through the
    dataset filesystem and decoded to HWC uint8 pixels."""
    from ray.data._internal.datasource.lerobot_datasource import _LeRobotReadTask
    from ray.data.datasource import LeRobotDatasource

    ds = create_lerobot_dataset(
        str(tmp_path / "img_path"),
        num_episodes=1,
        frames_per_episode=1,
        has_video=False,
        image_camera=True,
    )
    root = LeRobotDatasource(ds)._roots[0]

    # Write a real PNG where _decode_image_frames resolves the path:
    # f"{fs_root}/{path}" -- the same join it uses for data/video files.
    rel = "images/observation.image/f0.png"
    target = f"{root.fs_root}/{rel}"
    os.makedirs(os.path.dirname(target), exist_ok=True)
    with open(target, "wb") as fh:
        fh.write(_png_bytes(7))

    table = pa.table(
        {
            "observation.image": pa.array(
                [{"bytes": None, "path": rel}],
                type=pa.struct([("bytes", pa.binary()), ("path", pa.string())]),
            )
        }
    )
    decoded = _LeRobotReadTask._decode_image_frames(root, table)
    frame = np.asarray(decoded["observation.image"][0])
    assert frame.shape == (FRAME_H, FRAME_W, FRAME_C)
    assert frame.dtype == np.uint8
    assert (frame == 7 % 256).all()


def test_decode_image_frames_requires_bytes_or_path(tmp_path):
    """A cell with neither inline bytes nor a path is an inconsistent image
    column and raises, rather than silently yielding an empty frame."""
    from ray.data._internal.datasource.lerobot_datasource import _LeRobotReadTask
    from ray.data.datasource import LeRobotDatasource

    ds = create_lerobot_dataset(
        str(tmp_path / "img_bad"),
        num_episodes=1,
        frames_per_episode=1,
        has_video=False,
        image_camera=True,
    )
    root = LeRobotDatasource(ds)._roots[0]
    table = pa.table(
        {
            "observation.image": pa.array(
                [{"bytes": None, "path": None}],
                type=pa.struct([("bytes", pa.binary()), ("path", pa.string())]),
            )
        }
    )
    with pytest.raises(ValueError, match="neither inline bytes nor a path"):
        _LeRobotReadTask._decode_image_frames(root, table)


def test_read_lerobot_video_pixel_values(ray_start_regular_shared, lerobot_dataset):
    """Video-camera frames decode (within mpeg4 tolerance) to the solid fill the
    fixture wrote: within each episode, local frame ``fr`` was filled
    ``(fr * 25) % 256``. This also guards the float[0,1] -> uint8 rescale."""
    rows = ray.data.read_lerobot(lerobot_dataset).take_all()
    assert rows
    for row in rows:
        frame = np.asarray(row["observation.image"])
        expected = (row["frame_index"] * 25) % 256
        assert frame.shape == (FRAME_H, FRAME_W, FRAME_C)
        assert frame.dtype == np.uint8
        # Fixture frames are solid, so the decode should be ~uniform and centered
        # on the fill (mpeg4 is lossy).
        assert (
            float(frame.std()) <= 8
        ), f"row {row['index']}: frame not ~uniform (std={frame.std():.1f})"
        assert abs(float(frame.mean()) - expected) <= 12, (
            f"row {row['index']} (frame {row['frame_index']}): "
            f"expected ~{expected}, got mean {frame.mean():.1f}"
        )


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


def test_stats_to_json_roundtrip():
    from ray.data._internal.datasource.lerobot_datasource import _stats_to_json

    stats = {
        "action": {
            "mean": np.array([1.0, 2.0, 3.0], dtype=np.float32),
            "std": np.array([0.5, 0.25, 0.125], dtype=np.float64),
            "count": np.int64(1000),
        },
        "observation.images.cam": {
            "min": np.array([[0.0], [0.0], [0.0]], dtype=np.float32),  # 2D / nested
        },
    }
    assert json.loads(_stats_to_json(stats)) == {
        "action": {"mean": [1.0, 2.0, 3.0], "std": [0.5, 0.25, 0.125], "count": 1000},
        "observation.images.cam": {"min": [[0.0], [0.0], [0.0]]},
    }

    assert _stats_to_json({}) == "{}"
    assert _stats_to_json(None) == "{}"


def test_read_lerobot_frame_tolerance_invalid(
    ray_start_regular_shared, lerobot_dataset_no_video
):
    """Non-positive frame_tolerance_s is rejected."""
    from ray.data.datasource import LeRobotDatasource

    with pytest.raises(ValueError, match="frame_tolerance_s must be"):
        LeRobotDatasource(lerobot_dataset_no_video, frame_tolerance_s=0)


def test_lerobot_compat_shim_still_needed():
    """Tripwire: fails once lerobot's ``decode_video_frames_torchcodec`` accepts
    ``storage_options`` natively (huggingface/lerobot#3669). When this fails,
    delete the ``_lerobot_compat`` shim and decode directly via the native
    parameter."""
    import inspect

    from lerobot.datasets.video_utils import decode_video_frames_torchcodec

    params = inspect.signature(decode_video_frames_torchcodec).parameters
    assert "storage_options" not in params, (
        "lerobot now threads storage_options through "
        "decode_video_frames_torchcodec natively (huggingface/lerobot#3669). "
        "Remove ray.data's _lerobot_compat shim and decode directly via "
        "decode_video_frames_torchcodec(..., storage_options=...)."
    )


def test_read_lerobot_row_values(ray_start_regular_shared, lerobot_dataset):
    """Scalar columns and action/state vectors carry the expected per-row values."""
    ds = ray.data.read_lerobot(lerobot_dataset)
    rows = sorted(ds.take_all(), key=lambda r: r["index"])

    for i, row in enumerate(rows):
        assert row["index"] == i
        assert row["episode_index"] == i // 5
        assert row["frame_index"] == i % 5
        assert row["task"] == "test_task"
        assert row["dataset_index"] == 0
        action = np.asarray(row["action"], dtype=np.float32).flatten()
        state = np.asarray(row["state"], dtype=np.float32).flatten()
        np.testing.assert_allclose(action, [float(i), float(i + 1)], rtol=1e-5)
        np.testing.assert_allclose(state, [float(i) * 0.1, float(i) * 0.2], rtol=1e-5)


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


@pytest.mark.parametrize(
    "kind, match",
    [
        ("video_keys", "video_keys mismatch"),
        ("image_keys", "image_keys mismatch"),
        ("fps", "fps mismatch"),
        ("feature", "Feature mismatch"),
    ],
)
def test_read_lerobot_multi_root_mismatch_raises(
    ray_start_regular_shared, tmp_path, kind, match
):
    """Combining roots with incompatible schemas must fail fast in __init__ with
    a clear error naming the mismatched dimension. The dimensions are checked in
    order -- video_keys, image_keys, fps, then non-camera features -- so each
    case below matches the earlier dimensions and differs only in its own.
    """
    from ray.data.datasource import LeRobotDatasource

    a, b = str(tmp_path / "ds_a"), str(tmp_path / "ds_b")
    if kind == "video_keys":
        # mp4-video camera vs no camera -> different video_keys.
        root_a = create_lerobot_dataset(a, num_episodes=1, has_video=True)
        root_b = create_lerobot_dataset(b, num_episodes=1, has_video=False)
    elif kind == "image_keys":
        # in-parquet image camera vs no camera -> video_keys both empty (match),
        # image_keys differ.
        root_a = create_lerobot_dataset(
            a, num_episodes=1, has_video=False, image_camera=True
        )
        root_b = create_lerobot_dataset(b, num_episodes=1, has_video=False)
    else:
        # fps / feature: identical scalar datasets, then perturb root_b's
        # info.json so only the dimension under test differs.
        root_a = create_lerobot_dataset(a, num_episodes=1, has_video=False)
        root_b = create_lerobot_dataset(b, num_episodes=1, has_video=False)
        info_path = os.path.join(root_b, "meta", "info.json")
        with open(info_path) as f:
            info = json.load(f)
        if kind == "fps":
            info["fps"] = 30
        else:  # feature: add a scalar feature root_a does not have
            info["features"]["extra_signal"] = {"dtype": "float32", "shape": [2]}
        with open(info_path, "w") as f:
            json.dump(info, f)

    with pytest.raises(ValueError, match=match):
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


def test_read_lerobot_local_scheme_pins_reads(
    ray_start_regular_shared, lerobot_dataset
):
    """A ``local://`` root reads correctly and pins reads to the driver
    (``supports_distributed_reads`` is False); a bare path stays distributed."""
    from ray.data.datasource import LeRobotDatasource

    assert LeRobotDatasource(lerobot_dataset).supports_distributed_reads is True

    source = LeRobotDatasource(f"local://{lerobot_dataset}")
    assert source.supports_distributed_reads is False
    # The local:// scheme is stripped, so reading still works.
    assert ray.data.read_lerobot(f"local://{lerobot_dataset}").count() == 15


@pytest.mark.parametrize("group_by_episode", [False, True])
def test_read_lerobot_grouping_same_row_count(
    ray_start_regular_shared, lerobot_dataset, group_by_episode
):
    """Both groupings (per file-group and per episode) yield the same rows."""
    ds = ray.data.read_lerobot(lerobot_dataset, group_by_episode=group_by_episode)
    assert ds.count() == 15


def test_read_lerobot_override_num_blocks_splits_and_merges(
    ray_start_regular_shared, lerobot_dataset
):
    """``override_num_blocks`` adjusts the base grouping in both directions --
    splitting groups into more tasks and merging them into fewer -- without
    dropping or duplicating rows."""
    from ray.data.datasource import LeRobotDatasource

    # The fixture has three file groups (one mp4 per episode); ask for more
    # tasks than that to force splitting, and check every row is covered once.
    source = LeRobotDatasource(lerobot_dataset)
    tasks = source.get_read_tasks(6)
    assert len(tasks) == 6
    indices = [
        i for t in tasks for block in t() for i in block.column("index").to_pylist()
    ]
    assert sorted(indices) == list(range(15)), "split tasks must cover every row once"

    # Ask for fewer tasks than groups to force merging.
    assert len(LeRobotDatasource(lerobot_dataset).get_read_tasks(2)) == 2

    # End to end, the override is honored without changing the row count.
    ds = ray.data.read_lerobot(lerobot_dataset, override_num_blocks=8)
    assert ds.count() == 15


def _covered_cells(ranges):
    """Flatten ``(root_idx, start, end)`` ranges into their ``(root_idx, row)``
    cells, so two partitions can be compared for exact coverage."""
    return [(ri, i) for ri, s, e in ranges for i in range(s, e)]


@pytest.mark.parametrize(
    "ranges, target",
    [
        # Equal ranges, one root: split each group in two.
        ([(0, 0, 10), (0, 10, 20), (0, 20, 30)], 6),
        # Skewed sizes: apportionment must be proportional (90:10), not 5/5 --
        # naive-equal would emit size-18 and size-2 pieces.
        ([(0, 0, 90), (1, 0, 10)], 10),
        # Base boundaries off any uniform grid, two roots: a global uniform-cut
        # scheme would strand a sliver and cross a root boundary; this must not.
        ([(0, 0, 100), (1, 0, 30)], 4),
        # One monolithic range parallelized, with an uneven remainder.
        ([(0, 0, 100)], 7),
        # More tasks than rows: capped at one row per task.
        ([(0, 0, 3), (0, 3, 5)], 10),
        # Small range, uneven remainder (5 -> 2, 2, 1).
        ([(0, 0, 5)], 3),
    ],
)
def test_split_ranges_partitions_and_balances(ranges, target):
    """``_split_ranges`` must produce a *balanced* exact partition: every row is
    covered once, no sub-range crosses a base range, within a base range the
    pieces differ in size by at most one (no slivers), and each base range's
    share of the splits is proportional to its row count."""
    from ray.data.datasource import LeRobotDatasource

    out = LeRobotDatasource._split_ranges(ranges, target)
    total = sum(e - s for _, s, e in ranges)

    # Exact partition: identical coverage -- no gaps, no duplicates.
    assert sorted(_covered_cells(out)) == sorted(_covered_cells(ranges))

    # Each piece is non-empty and lies inside exactly one base range.
    pieces = {i: [] for i in range(len(ranges))}
    for ri, s, e in out:
        assert e > s
        owners = [
            i
            for i, (bri, bs, be) in enumerate(ranges)
            if bri == ri and bs <= s and e <= be
        ]
        assert len(owners) == 1, f"{(ri, s, e)} crosses a base range boundary"
        pieces[owners[0]].append(e - s)

    # Count: hit target exactly, unless more tasks than rows were requested.
    assert len(out) == (target if target <= total else total)

    for i, (_, s, e) in enumerate(ranges):
        n = e - s
        sizes = pieces[i]
        # No slivers: a base range's pieces are as even as integer division allows.
        assert max(sizes) - min(sizes) <= 1, f"unbalanced split of range {i}: {sizes}"
        # Proportional: piece count tracks the range's row share (within rounding).
        ideal = max(1, min(round(n * target / total), n))
        assert (
            abs(len(sizes) - ideal) <= 1
        ), f"range {i} (n={n}) got {len(sizes)} pieces, expected ~{ideal}"


def test_split_ranges_noop_when_target_not_greater():
    """``_split_ranges`` returns the input unchanged when the requested task
    count does not exceed the number of base ranges (nothing to split)."""
    from ray.data.datasource import LeRobotDatasource

    ranges = [(0, 0, 10), (0, 10, 25), (1, 0, 7)]
    assert LeRobotDatasource._split_ranges(ranges, 3) == ranges
    assert LeRobotDatasource._split_ranges(ranges, 1) == ranges


@pytest.mark.parametrize(
    "video_keys, file_cols",
    [
        ([], {"data/chunk_index": [0, 0, 0], "data/file_index": [0, 1, 0]}),
        (
            ["cam"],
            {
                "videos/cam/chunk_index": [0, 0, 0],
                "videos/cam/file_index": [0, 1, 0],
            },
        ),
    ],
)
def test_slices_by_file_group_non_contiguous_splits(video_keys, file_cols):
    """Episodes that share a file group but are not contiguous in the global
    index (e.g. an ``episodes`` subset drops the episode between them) are split
    into separate contiguous ranges -- not merged into one wrong range. Here ep0
    and ep2 share a file but ep1 (a different file) sits between them, so the
    shared file yields two ranges."""
    from ray.data.datasource import LeRobotDatasource

    eps = pa.table(
        {
            "_global_from_index": [0, 10, 20],
            "_global_to_index": [10, 20, 30],
            **file_cols,
        }
    )
    out = LeRobotDatasource._slices_by_file_group(eps, video_keys)
    assert sorted(out) == [(0, 10), (10, 20), (20, 30)]


def test_episodes_for_row_range_uses_row_positions_not_episode_index():
    """_episodes_for_row_range must return episodes-table ROW positions (for
    eps.slice), not episode_index values -- they coincide only for the usual
    0..N-1 numbering. Here episode_index is offset, so a value-based slice would
    pick the wrong rows (the bug Cursor Bugbot flagged)."""
    from ray.data._internal.datasource.lerobot_datasource import _LeRobotReadTask

    # 3 episodes at rows 0/1/2, but episode_index offset to 100/101/102.
    eps = pa.table(
        {
            "episode_index": [100, 101, 102],
            "_global_from_index": [0, 10, 20],
            "_global_to_index": [10, 20, 30],
        }
    )
    # [10, 20) overlaps only the middle episode -> row position 1, not value 101.
    assert _LeRobotReadTask._episodes_for_row_range(eps, 10, 20) == (1, 2)
    # [5, 25) overlaps all three -> positions (0, 3), not values (100, 103).
    assert _LeRobotReadTask._episodes_for_row_range(eps, 5, 25) == (0, 3)
    with pytest.raises(ValueError, match="No episodes overlap"):
        _LeRobotReadTask._episodes_for_row_range(eps, 100, 200)


def test_read_lerobot_batches_sized_per_root(ray_start_regular_shared, tmp_path):
    """Batches must be sized from each segment's OWN root, not one global value.

    Regression test: a multi-root read mixing small (scalar-only) and large
    (video) rows must size each root's batches by that root's row size. The old
    code sized every batch from ``_roots[0]``, so a large root following a small
    one would over-fill its batches and blow past the target block size.
    """
    from ray.data.datasource import LeRobotDatasource

    root_a = create_lerobot_dataset(
        str(tmp_path / "ds_a"), num_episodes=1, frames_per_episode=10, has_video=False
    )
    root_b = create_lerobot_dataset(
        str(tmp_path / "ds_b"), num_episodes=1, frames_per_episode=10, has_video=False
    )
    source = LeRobotDatasource([root_a, root_b])

    # Give the roots very different estimated row sizes and a block budget that
    # yields a tiny batch for the big root but a huge one for the small root.
    small, big = 100, 10_000
    source._roots[0] = source._roots[0]._replace(row_size_bytes=small)
    source._roots[1] = source._roots[1]._replace(row_size_bytes=big)
    source._max_block_bytes = lambda data_context=None: big * 2  # 2 big rows/batch

    batch_rows = {0: [], 1: []}
    for task in source.get_read_tasks(2):
        for block in task():
            di = block.column("dataset_index")[0].as_py()
            batch_rows[di].append(block.num_rows)

    # Big-row root (dataset_index 1): budget // big == 2 rows/batch.
    assert batch_rows[1] and max(batch_rows[1]) <= 2
    # Small-row root (dataset_index 0): budget // small == 200 >> 10 rows -> one
    # batch. If batches were sized globally from _roots[0], the big root would
    # also use 200 and land all 10 rows in a single batch (max would be 10).
    assert max(batch_rows[0]) == 10
    assert max(batch_rows[0]) > max(batch_rows[1])


def test_read_lerobot_get_read_tasks_parallelism_zero(
    ray_start_regular_shared, lerobot_dataset
):
    """parallelism <= 0 falls back to the base (per-file-group) grouping and
    still covers every row exactly once."""
    from ray.data.datasource import LeRobotDatasource

    source = LeRobotDatasource(lerobot_dataset)
    tasks = source.get_read_tasks(0)
    assert len(tasks) == 3  # 3 episodes -> 3 video file groups
    indices = [
        i for t in tasks for block in t() for i in block.column("index").to_pylist()
    ]
    assert sorted(indices) == list(range(15))


def test_read_lerobot_default_num_blocks_no_oversplit(
    ray_start_regular_shared, lerobot_dataset
):
    """Without override_num_blocks, read_lerobot defaults to one read task per
    video-file group -- it must NOT over-split into Ray's generic block-count
    floor (which tanks video throughput: each split re-opens file + decoder)."""
    import re

    from ray.data.datasource import LeRobotDatasource

    source = LeRobotDatasource(lerobot_dataset)
    base = len(source._slice())  # 3 episodes -> 3 video file groups
    assert source.default_num_blocks() == base

    # A default read (no override) must produce exactly the base partition, not
    # the ~200-block floor Ray would otherwise apply to a tiny dataset.
    mds = ray.data.read_lerobot(lerobot_dataset).materialize()
    m = re.search(r"ReadLeRobot.*?(\d+) tasks", mds.stats())
    assert m is not None and int(m.group(1)) == base, mds.stats()


def test_memory_estimate_and_schema_correctness(
    ray_start_regular_shared,
    lerobot_dataset,
    lerobot_dataset_no_video,
    lerobot_dataset_image,
):
    """Test that our memory estimate and schema inference logic are correct.

    The memory estimate is a lower bound on the real per-row size.
    The schema is exactly what we declare up front and is exactly what we produce.
    """
    from ray.data._internal.datasource.lerobot_datasource import (
        LeRobotDatasource,
        _estimated_row_size_bytes,
    )

    for path in (lerobot_dataset, lerobot_dataset_no_video, lerobot_dataset_image):
        source = LeRobotDatasource(path)
        root = source._roots[0]
        estimate = _estimated_row_size_bytes(source.metas[0].features)
        tasks = source.get_read_tasks(1)
        block = pa.concat_tables([b for task in tasks for b in task()])

        # The schema _build_schema declares up front is exactly what _build_batch
        # emits -- so ds.schema() is exact (not just plausible) and the declared
        # and produced schemas can't silently drift apart.
        assert tasks[0].schema.equals(block.schema)

        # A row is exactly the dataset's feature columns (cameras decoded in
        # place) plus the three appended columns -- nothing more, nothing less.
        # Adding/removing an output column must be reflected in the estimate.
        assert set(block.schema.names) == set(source.metas[0].features) | {
            "task",
            "dataset_index",
            "stats",
        }

        # task and stats are dictionary-encoded, so the estimate charges them one
        # int32 index per row rather than the full string value.
        assert pa.types.is_dictionary(block.schema.field("task").type)
        assert pa.types.is_dictionary(block.schema.field("stats").type)

        # The estimate omits Arrow overhead and the once-per-block dictionary
        # values, so it's a lower bound on -- and within ~2x of -- the real
        # per-row size.
        actual_per_row = block.nbytes / block.num_rows
        assert estimate <= actual_per_row <= 2 * estimate

        # estimate_inmemory_data_size aggregates the per-row estimate.
        assert (
            source.estimate_inmemory_data_size()
            == root.total_frames * root.row_size_bytes
        )


def test_read_lerobot_per_task_row_limit_bounds_decode(
    ray_start_regular_shared, lerobot_dataset, monkeypatch
):
    """per_task_row_limit caps emitted rows AND stops the read early -- the task
    decodes only ~one batch past the limit, not the whole segment."""
    from ray.data._internal.datasource import lerobot_datasource as mod

    decoded = [0]
    real = mod.decode_frames

    def counting_decode(video_path, timestamps, *args, **kwargs):
        decoded[0] += len(timestamps)
        return real(video_path, timestamps, *args, **kwargs)

    monkeypatch.setattr(mod, "decode_frames", counting_decode)

    source = mod.LeRobotDatasource(lerobot_dataset)
    # Force ~2 rows per batch so the early stop is observable.
    rsz = source._roots[0].row_size_bytes
    monkeypatch.setattr(source, "_max_block_bytes", lambda data_context=None: rsz * 2)

    tasks = source.get_read_tasks(1, per_task_row_limit=2)
    rows = sum(block.num_rows for t in tasks for block in t())

    assert rows == 2  # output capped at the limit
    # Early stop: only ~one batch decoded, not all 15 frames.
    assert decoded[0] <= 4, f"decoded {decoded[0]} rows; the read did not stop early"


def test_read_lerobot_inconsistent_task_index_raises(
    ray_start_regular_shared, lerobot_dataset
):
    """A ``task_index`` present in the data but absent from the tasks metadata
    must raise a clear error, not a bare KeyError mid-stream."""
    from ray.data.datasource import LeRobotDatasource

    source = LeRobotDatasource(lerobot_dataset)
    # Drop a referenced task id from the metadata to simulate an inconsistent
    # dataset (data references a task with no meta/tasks.parquet entry).
    root = source._roots[0]
    broken = dict(root.tasks_dict)
    broken.pop(next(iter(broken)))
    source._roots[0] = root._replace(tasks_dict=broken)

    with pytest.raises(ValueError, match="tasks metadata"):
        for task in source.get_read_tasks(1):
            for _ in task():
                pass


def _fsspec_fs_with_creds():
    """An fsspec filesystem carrying credential-like storage_options."""
    import fsspec

    fs = fsspec.filesystem("memory")
    fs.storage_options = {"key": "SECRET"}
    return fs


def _pyarrow_local_fs():
    import pyarrow.fs as pafs

    return pafs.LocalFileSystem()


@pytest.mark.parametrize(
    "uri,fs_factory,expect_key",
    [
        # An fsspec filesystem's credentials reach the by-URI video path.
        ("/tmp/ds", _fsspec_fs_with_creds, "SECRET"),
        # A pyarrow filesystem can't expose creds; video credentials come from
        # storage_options instead, so none are threaded in here.
        ("s3://bucket/ds", _pyarrow_local_fs, None),
    ],
)
def test_resolve_filesystem_video_credentials(uri, fs_factory, expect_key):
    """``filesystem=`` extends to the by-URI video decode path: an fsspec
    filesystem's credentials are threaded into the video options; a pyarrow
    filesystem's are not (those credentials come from storage_options)."""
    from ray.data._internal.datasource.lerobot_datasource import _resolve_filesystem

    _, _, _, video_opts = _resolve_filesystem(uri, filesystem=fs_factory())
    if expect_key is not None:
        assert video_opts.get("key") == expect_key
    else:
        assert "key" not in video_opts


def test_resolve_filesystem_anonymous_uri_strips_and_sets_anon():
    """``s3://anonymous@bucket/...`` drops the ``anonymous@`` marker for the
    by-URI video path and threads ``anon=True`` into the video options."""
    import pyarrow.fs as pafs

    from ray.data._internal.datasource.lerobot_datasource import _resolve_filesystem

    # An explicit filesystem avoids the network-backed default resolver; the
    # anonymous@ rewrite happens before the filesystem branch either way.
    _, _, video_uri, video_opts = _resolve_filesystem(
        "s3://anonymous@my-bucket/path", filesystem=pafs.LocalFileSystem()
    )
    assert video_uri == "s3://my-bucket/path"
    assert video_opts.get("anon") is True


def test_resolve_filesystem_anonymous_uri_with_explicit_filesystem_strips_fs_root():
    """With an explicit ``filesystem`` AND an ``s3://anonymous@...`` URI, the
    metadata/parquet root must also drop the ``anonymous@`` marker -- otherwise
    opens hit ``anonymous@bucket/...`` instead of the real bucket path (the
    by-URI video path is covered separately above)."""
    import pyarrow.fs as pafs

    from ray.data._internal.datasource.lerobot_datasource import _resolve_filesystem

    _, fs_root, _, _ = _resolve_filesystem(
        "s3://anonymous@my-bucket/path", filesystem=pafs.LocalFileSystem()
    )
    assert "anonymous@" not in fs_root
    assert fs_root == "my-bucket/path"


def test_resolve_filesystem_storage_options_only(tmp_path):
    """With ``storage_options`` but no ``filesystem``, resolution goes through
    fsspec so the same options cover metadata, parquet, and video."""
    import fsspec

    from ray.data._internal.datasource.lerobot_datasource import _resolve_filesystem

    fs, fs_root, _, _ = _resolve_filesystem(
        str(tmp_path), storage_options={"auto_mkdir": True}
    )
    assert isinstance(fs, fsspec.AbstractFileSystem)
    assert os.path.realpath(fs_root) == os.path.realpath(str(tmp_path))


def test_lerobot_compat_creds_cache_closes_handles(tmp_path):
    """The credentialed decoder cache (used when storage_options are supplied)
    decodes through fsspec and closes every file handle it opened on clear() --
    lerobot's base cache leaks a file descriptor per video file. Exercised on a
    local mp4; no S3 needed."""
    from ray.data._internal.datasource._lerobot_compat import (
        decode_frames,
        new_decoder_cache,
    )

    video = str(tmp_path / "videos" / "v.mp4")
    _create_video(video, num_frames=5)

    # Non-empty storage_options selects the credentialed (handle-closing) cache.
    cache = new_decoder_cache({"anon": True})
    frames = decode_frames(
        video, [0.0, 1.0 / FPS], tolerance_s=1.0 / FPS, decoder_cache=cache
    )
    assert frames.shape[0] == 2  # decoded two frames through the creds cache

    handles = [entry[1] for entry in cache._cache.values()]
    assert handles, "creds cache should have cached an open file handle"
    assert all(not h.closed for h in handles)

    cache.clear()
    assert all(h.closed for h in handles), "clear() must close the fsspec handles"
    assert cache._cache == {}


# ---------------------------------------------------------------------------
# episodes filter (read-time pushdown)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("group_by_episode", [False, True])
def test_read_lerobot_episodes_filters_rows(
    ray_start_regular_shared, lerobot_dataset, group_by_episode
):
    """``episodes`` loads only the requested episode_indices (a read-time
    pushdown), under both groupings, leaving the kept episodes' rows intact."""
    ds = ray.data.read_lerobot(
        lerobot_dataset, episodes=[0, 2], group_by_episode=group_by_episode
    )
    rows = ds.take_all()
    assert len(rows) == 10  # 2 of 3 episodes * 5 frames
    assert {r["episode_index"] for r in rows} == {0, 2}
    # Frame values within a kept episode are unaffected by the filter.
    for r in rows:
        assert r["frame_index"] == r["index"] % 5


def test_read_lerobot_episodes_single(ray_start_regular_shared, lerobot_dataset):
    """A single-episode selection reads exactly that episode's frames."""
    rows = ray.data.read_lerobot(lerobot_dataset, episodes=[1]).take_all()
    assert len(rows) == 5
    assert all(r["episode_index"] == 1 for r in rows)


def test_read_lerobot_episodes_non_contiguous_shared_file(
    ray_start_regular_shared, lerobot_dataset_no_video
):
    """A non-contiguous subset of a dataset whose episodes SHARE one data parquet
    file still reads under the default (file-group) grouping: the file group is
    split into contiguous runs, not merged into one wrong range or errored."""
    rows = ray.data.read_lerobot(lerobot_dataset_no_video, episodes=[0, 2]).take_all()
    assert len(rows) == 10
    assert {r["episode_index"] for r in rows} == {0, 2}


def test_read_lerobot_episodes_unknown_raises(
    ray_start_regular_shared, lerobot_dataset
):
    """Requesting an episode_index absent from the dataset fails fast."""
    from ray.data.datasource import LeRobotDatasource

    with pytest.raises(ValueError, match="episode"):
        LeRobotDatasource(lerobot_dataset, episodes=[0, 99])


def test_read_lerobot_episodes_empty_raises(ray_start_regular_shared, lerobot_dataset):
    """An empty ``episodes`` list is a degenerate request and is rejected (pass
    ``None`` to read all episodes)."""
    from ray.data.datasource import LeRobotDatasource

    with pytest.raises(ValueError, match="episodes"):
        LeRobotDatasource(lerobot_dataset, episodes=[])


def test_read_lerobot_episodes_reduces_planning(
    ray_start_regular_shared, lerobot_dataset
):
    """Filtering to fewer episodes shrinks the default read-task count and the
    in-memory size estimate (fewer files, fewer frames)."""
    from ray.data.datasource import LeRobotDatasource

    full = LeRobotDatasource(lerobot_dataset)
    one = LeRobotDatasource(lerobot_dataset, episodes=[1])
    assert one.default_num_blocks() < full.default_num_blocks()
    assert one.estimate_inmemory_data_size() < full.estimate_inmemory_data_size()


def test_read_lerobot_episodes_multi_root(ray_start_regular_shared, tmp_path):
    """``episodes`` applies per-root: each root contributes only its matching
    episode_indices."""
    root_a = create_lerobot_dataset(str(tmp_path / "ds_a"), num_episodes=3)
    root_b = create_lerobot_dataset(str(tmp_path / "ds_b"), num_episodes=3)
    rows = ray.data.read_lerobot([root_a, root_b], episodes=[0]).take_all()
    # episode 0 from each of the two roots -> 2 * 5 frames.
    assert len(rows) == 10
    assert {r["episode_index"] for r in rows} == {0}
    assert {r["dataset_index"] for r in rows} == {0, 1}


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
