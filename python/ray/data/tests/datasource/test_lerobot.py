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


def test_read_lerobot_video_pixel_values(ray_start_regular_shared, lerobot_dataset):
    """Video-camera frames decode (within mpeg4 tolerance) to the solid fill the
    fixture wrote: within each episode, local frame ``fr`` was filled
    ``(fr * 25) % 256``. This also guards the float[0,1] -> uint8 rescale -- a
    regression there would zero or mangle the frames rather than just reshape
    them."""
    rows = ray.data.read_lerobot(lerobot_dataset).take_all()
    assert rows
    for row in rows:
        frame = np.asarray(row["observation.image"])
        expected = (row["frame_index"] * 25) % 256
        assert frame.shape == (FRAME_H, FRAME_W, FRAME_C)
        assert frame.dtype == np.uint8
        # Fixture frames are solid, so the decode should be ~uniform and centered
        # on the fill (mpeg4 is lossy; observed error is only a few levels).
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


def test_read_lerobot_estimate_inmemory_data_size(
    ray_start_regular_shared, lerobot_dataset_no_video
):
    """estimate_inmemory_data_size is total_frames * the estimated per-row size."""
    from ray.data.datasource import LeRobotDatasource

    source = LeRobotDatasource(lerobot_dataset_no_video)
    root = source._roots[0]
    assert (
        source.estimate_inmemory_data_size() == root.total_frames * root.row_size_bytes
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
    "uri,fs_factory,expect_unavailable,expect_key",
    [
        # An fsspec filesystem's credentials reach the by-URI video path.
        ("/tmp/ds", _fsspec_fs_with_creds, False, "SECRET"),
        # A pyarrow filesystem can't expose creds: a remote root is flagged...
        ("s3://bucket/ds", _pyarrow_local_fs, True, None),
        # ...but a local pyarrow root needs none, so it isn't flagged.
        ("/local/ds", _pyarrow_local_fs, False, None),
    ],
)
def test_resolve_filesystem_video_credentials(
    uri, fs_factory, expect_unavailable, expect_key
):
    """``filesystem=`` must extend to the by-URI video decode path: fsspec
    credentials are threaded through, while the unbridgeable pyarrow-remote
    case is flagged so the datasource can fail loudly when video is present."""
    from ray.data._internal.datasource.lerobot_datasource import _resolve_filesystem

    _, _, _, video_opts, unavailable = _resolve_filesystem(uri, filesystem=fs_factory())
    assert unavailable is expect_unavailable
    if expect_key is not None:
        assert video_opts.get("key") == expect_key


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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
