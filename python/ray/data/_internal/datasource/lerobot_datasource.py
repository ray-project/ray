"""Ray Data datasource for LeRobot Dataset v3.

LeRobot is a platform for sharing datasets and pretrained models for
real-world robotics.  A LeRobot v3 dataset is a flat table of timestep
samples combining low-dimensional data (state, action, etc.) from chunked
parquet files with decoded camera frames from chunked mp4 files.

This datasource reads LeRobot v3 datasets from local or cloud storage,
decoding video frames with PyAV and aligning them with parquet data using
episode metadata.  Metadata parsing is delegated to the upstream
``lerobot.datasets.dataset_metadata.LeRobotDatasetMetadata`` (patched at
import time to support fsspec-compatible cloud URIs); the lerobot
instances themselves are kept pristine — all Ray-Data-specific derived
state lives in :class:`_LeRobotRoot` bundles built on the driver and
shipped to workers via ``ray.put``.
"""

import enum
import json
import logging
from pathlib import Path
from typing import Any, Dict, Iterator, List, NamedTuple, Optional, Union

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

import ray
from ray.data._internal.datasource._lerobot_patch import (
    apply_lerobot_fsspec_patches,
)
from ray.data._internal.util import _check_import
from ray.data.block import BlockMetadata
from ray.data.context import DataContext
from ray.data.datasource.datasource import Datasource, ReadTask
from ray.data.extensions import (
    ArrowVariableShapedTensorArray,
    ArrowVariableShapedTensorType,
)
from ray.util.annotations import DeveloperAPI, PublicAPI

logger = logging.getLogger(__name__)


@PublicAPI(stability="alpha")
class LeRobotPartitioning(enum.Enum):
    """How the dataset is partitioned into Ray Data read tasks.

    Attributes:
        EPISODE: One task per episode. Best for small local datasets; maximum
            task count regardless of I/O cost.
        FILE_GROUP: One task per unique video-file set. Default — balanced
            tasks with each mp4 opened once per task.
        CHAIN: One task per connected component of episodes sharing video
            files. Best for large cloud datasets where minimising total
            video-file opens matters most.
        SEQUENTIAL: One task for the whole dataset. Best for cloud datasets
            where peak memory must be minimised over throughput.
        ROW_BLOCK: Fixed-size blocks of N rows (set via ``block_size``
            argument). Boundaries may split episodes.
    """

    EPISODE = "episode"
    FILE_GROUP = "file_group"
    CHAIN = "chain"
    SEQUENTIAL = "sequential"
    ROW_BLOCK = "row_block"


class _LeRobotRoot(NamedTuple):
    """Per-root derived state for the LeRobot datasource.

    Built once on the driver from a (pristine)
    :class:`lerobot.LeRobotDatasetMetadata` and shipped to workers via
    ``ray.put``.  Workers never need lerobot installed at runtime — all
    info required to read a dataset is captured here.
    """

    root: str
    """Original root URI (local path or cloud URI), used for ``fsspec.url_to_fs``."""

    fs_root: str
    """Filesystem-stripped root for path joining (``s3://b/x`` -> ``b/x``)."""

    data_path: str
    """``info.data_path`` format string for chunked parquet files."""

    video_path: Optional[str]
    """``info.video_path`` format string for chunked mp4 files (``None`` if no videos)."""

    video_keys: List[str]
    """Feature keys with ``dtype == 'video'`` (camera streams)."""

    episodes_table: pa.Table
    """Episode metadata as a pyarrow Table, augmented with
    ``_global_from_index`` / ``_global_to_index`` columns derived from
    cumulative episode lengths."""

    tasks_dict: Dict[int, str]
    """``{task_index: task_name}`` mapping."""

    schema: pa.Schema
    """Arrow schema of one fully-decoded output row (parquet columns +
    variable-shape-tensor video columns + ``task`` + ``dataset_index`` +
    ``stats``)."""

    row_size_bytes: int
    """Estimated in-memory size of one fully-decoded row, in bytes."""

    total_frames: int
    """Number of frames across all episodes."""

    fps: int
    """Frames per second — used to size the per-frame timestamp tolerance
    passed to :func:`lerobot.datasets.video_utils.decode_video_frames`."""

    storage_options: Dict[str, Any]
    """Extra options forwarded to ``fsspec`` (``url_to_fs`` / ``fsspec.open``)
    and to the video decode call when reading this root.  Empty dict means
    rely on ambient fsspec credential resolution."""

    stats_json: str
    """Per-feature normalization statistics (mean/std/min/max/…) from
    ``meta/stats.json``, serialized to a JSON string.  Emitted verbatim on
    every row as the ``stats`` column; ``"{}"`` when the dataset has none."""


# ---------------------------------------------------------------------------
# Driver-side derived-state builders.
# ---------------------------------------------------------------------------


def _build_episodes_table(hf_episodes) -> pa.Table:
    """Convert lerobot's HF ``Dataset`` of episodes to a pyarrow ``Table``
    with cumulative ``_global_from_index`` / ``_global_to_index`` columns
    derived from per-episode lengths."""
    episodes = hf_episodes.with_format("arrow")[:]
    ep_indices = episodes.column("episode_index").to_pylist()
    if ep_indices != list(range(len(ep_indices))):
        raise ValueError(
            f"Episodes are not 0-indexed and contiguous: "
            f"first={ep_indices[0]}, last={ep_indices[-1]}, "
            f"count={len(ep_indices)}"
        )
    lengths = episodes.column("length").to_pylist()
    global_from: list = []
    running = 0
    for ln in lengths:
        global_from.append(running)
        running += ln
    global_to = global_from[1:] + [running]
    return episodes.append_column(
        "_global_from_index", pa.array(global_from, type=pa.int64())
    ).append_column(
        "_global_to_index", pa.array(global_to, type=pa.int64())
    )


def _build_tasks_dict(tasks_df) -> Dict[int, str]:
    """Convert lerobot's ``tasks`` DataFrame (indexed by task name, with a
    ``task_index`` column) into a ``{task_index: task_name}`` dict."""
    return dict(
        zip(tasks_df["task_index"].astype(int).tolist(), tasks_df.index.tolist())
    )


def _build_schema(
    episodes_table: pa.Table,
    data_path: str,
    video_keys: List[str],
    fs,
    fs_root: str,
) -> pa.Schema:
    """Read the Arrow schema of the first data parquet file and append the
    variable-shape-tensor video columns plus ``task``, ``dataset_index`` and
    ``stats``."""
    ep = episodes_table.slice(0, 1).to_pylist()[0]
    path = (
        f"{fs_root}/"
        f"{data_path.format(chunk_index=ep['data/chunk_index'], file_index=ep['data/file_index'])}"
    )
    with fs.open(path, "rb") as f:
        pq_schema = pq.read_schema(f)
    fields = list(pq_schema)
    for vk in video_keys:
        fields.append(
            pa.field(vk, ArrowVariableShapedTensorType(pa.uint8(), ndim=3))
        )
    fields.append(pa.field("task", pa.string()))
    fields.append(pa.field("dataset_index", pa.int32()))
    fields.append(pa.field("stats", pa.string()))
    return pa.schema(fields)


def _estimated_row_size_bytes(features: dict, root_for_logging: str) -> int:
    """Estimated in-memory size of one fully-decoded frame row, in bytes."""
    total = 0
    for feat_name, feat in features.items():
        if feat.get("dtype") == "video":
            shape = feat.get("shape")
            if shape:
                total += int(np.prod(shape))
        else:
            shape = feat.get("shape", [1])
            try:
                total += int(np.prod(shape)) * np.dtype(feat["dtype"]).itemsize
            except (TypeError, KeyError):
                logger.warning(
                    "Could not estimate size for feature %r in dataset %r, "
                    "skipping.",
                    feat_name,
                    root_for_logging,
                )
                continue
    return total


def _stats_to_json(stats: Optional[dict]) -> str:
    """Serialize lerobot's per-feature stats dict (``{feature: {stat: ndarray}}``)
    to a JSON string, converting numpy arrays / scalars to plain lists / numbers.

    Returns ``"{}"`` when *stats* is missing or empty.  Emitted verbatim on every
    row as the ``stats`` column so downstream tasks (e.g. normalizing state/action
    for policy training) can recover mean/std without a second metadata read.
    """
    if not stats:
        return "{}"

    def _convert(value: Any) -> Any:
        if isinstance(value, dict):
            return {k: _convert(v) for k, v in value.items()}
        if isinstance(value, np.ndarray):
            return value.tolist()
        if isinstance(value, np.generic):
            return value.item()
        return value

    return json.dumps(_convert(stats))


def _load_lerobot_metadata(
    root: Union[str, Path], storage_options: Optional[Dict[str, Any]] = None
):
    """Construct a pristine :class:`lerobot.LeRobotDatasetMetadata` for
    *root*, applying the fsspec patch first so cloud URIs work.

    Pre-validates that ``meta/info.json`` exists at *root* before invoking
    lerobot, so missing-dataset errors stay as ``FileNotFoundError`` rather
    than getting transformed into HuggingFace Hub validation errors by
    lerobot's local-then-Hub fallback logic.

    *storage_options* is forwarded to ``fsspec`` for the existence check and
    captured by the patched metadata loader for its own reads.
    """
    import fsspec

    from lerobot.datasets.dataset_metadata import LeRobotDatasetMetadata

    apply_lerobot_fsspec_patches()
    root_uri = str(root).rstrip("/")
    storage_options = dict(storage_options or {})

    fs, fs_root = fsspec.core.url_to_fs(root_uri, **storage_options)
    fs_root = fs_root.rstrip("/")
    info_path = f"{fs_root}/meta/info.json"
    if not fs.exists(info_path):
        raise FileNotFoundError(
            f"No LeRobot dataset found at {root_uri!r}: meta/info.json is missing. "
            "Make sure the path points to the dataset root."
        )

    # repo_id is decorative for read-only use: it appears only in error
    # messages and as the HF Hub fallback target when files are missing.
    # We pass the root itself so any fallback fails clearly.
    return LeRobotDatasetMetadata(
        repo_id=root_uri, root=root_uri, storage_options=storage_options
    )


def _build_root(
    meta, storage_options: Optional[Dict[str, Any]] = None
) -> _LeRobotRoot:
    """Compute the per-root derived state bundle for a (pristine) lerobot
    ``LeRobotDatasetMetadata`` instance.  Does not mutate *meta*.

    *storage_options* is forwarded to ``fsspec`` and captured on the returned
    :class:`_LeRobotRoot` so workers reuse the same credentials.
    """
    import fsspec

    root_uri = str(meta.root).rstrip("/")
    storage_options = dict(storage_options or {})

    # In lerobot 0.5.x, meta.info is a dict; accessing meta.video_path
    # raises KeyError when the key is absent.  Use dict-aware lookup.
    video_path = meta.info.get("video_path") if isinstance(meta.info, dict) else getattr(meta.info, "video_path", None)
    if meta.video_keys and not video_path:
        raise ValueError(
            f"{root_uri!r}: dataset has video keys {meta.video_keys} "
            "but meta/info.json has no 'video_path' template"
        )

    fs, fs_root = fsspec.core.url_to_fs(root_uri, **storage_options)
    fs_root = fs_root.rstrip("/")
    episodes_table = _build_episodes_table(meta.episodes)
    tasks_dict = _build_tasks_dict(meta.tasks)
    schema = _build_schema(
        episodes_table, meta.data_path, meta.video_keys, fs, fs_root
    )
    row_size_bytes = _estimated_row_size_bytes(meta.features, root_uri)
    stats_json = _stats_to_json(getattr(meta, "stats", None))

    return _LeRobotRoot(
        root=root_uri,
        fs_root=fs_root,
        data_path=meta.data_path,
        video_path=video_path,
        video_keys=list(meta.video_keys),
        episodes_table=episodes_table,
        tasks_dict=tasks_dict,
        schema=schema,
        row_size_bytes=row_size_bytes,
        total_frames=meta.total_frames,
        fps=meta.fps,
        storage_options=storage_options,
        stats_json=stats_json,
    )


class _LeRobotReadTask(ReadTask):
    """A Ray Data read task covering one or more contiguous row segments.

    Each segment is a ``(root_index, start, end)`` triple referencing a
    contiguous row range within one root of :class:`LeRobotDatasource`.
    """

    def __init__(
        self,
        segments: List[tuple],
        roots_ref: "ray.ObjectRef",
        rows_per_batch: int,
        per_task_row_limit: Optional[int] = None,
    ) -> None:
        roots: List[_LeRobotRoot] = ray.get(roots_ref)
        total_rows = 0
        size_bytes = 0
        all_input_files: List[str] = []
        resolved: List[tuple] = []
        for root_idx, start, end in segments:
            root = roots[root_idx]
            parquet_segs, video_segs = _LeRobotReadTask._resolve_paths(
                root, start, end
            )
            all_input_files.extend(parquet_segs)
            all_input_files.extend(video_segs)
            total_rows += end - start
            size_bytes += (end - start) * root.row_size_bytes
            # Workers only need parquet_segs; video paths are derived from
            # episodes_table inside _decode_video_frames.
            resolved.append((root_idx, start, end, parquet_segs))

        schema = roots[segments[0][0]].schema
        block_metadata = BlockMetadata(
            num_rows=total_rows,
            size_bytes=size_bytes,
            input_files=all_input_files,
            exec_stats=None,
        )
        super().__init__(self._read, block_metadata, schema, per_task_row_limit)
        self._roots_ref = roots_ref
        self._segments_resolved = resolved
        self._rows_per_batch = rows_per_batch

    def _read(self) -> Iterator[pa.Table]:
        """Stream decoded rows as Arrow tables, iterating over all segments."""
        roots: List[_LeRobotRoot] = ray.get(self._roots_ref)
        for root_idx, start, end, parquet_segs in self._segments_resolved:
            yield from self._read_segment(
                roots[root_idx], start, end, root_idx, parquet_segs
            )

    def _read_segment(
        self,
        root: _LeRobotRoot,
        start: int,
        end: int,
        dataset_index: int,
        parquet_segs: List[str],
    ) -> Iterator[pa.Table]:
        """Stream decoded rows for one ``[start, end)`` range within a single root.

        Strategy:

        1. Read all parquet rows for the row range via predicate pushdown.
        2. For each camera, group rows by their video file (``(chunk, file)``
           tuple from the episode metadata), compute the per-row absolute
           timestamp (``from_timestamp[ep] + row_ts``), and batch-decode one
           video file at a time via :func:`lerobot.datasets.video_utils.decode_video_frames`.
        3. Yield Arrow batches of ``self._rows_per_batch`` rows.
        """
        import fsspec
        from lerobot.datasets.video_utils import decode_video_frames

        fs, _ = fsspec.core.url_to_fs(root.root, **root.storage_options)

        # 1. Read all parquet rows for this segment.
        filters = [("index", ">=", start), ("index", "<", end)]
        pq_tables = []
        for path in parquet_segs:
            with fs.open(path, "rb") as f:
                pq_tables.append(pq.read_table(f, filters=filters))
        full = pa.concat_tables(pq_tables) if pq_tables else None
        if full is None or full.num_rows == 0:
            return
        n_rows = full.num_rows

        # 2. Batch-decode video frames per (camera, video_file).
        # decoded_frames[vk] is an array of N HWC uint8 frames aligned to
        # full's row order.
        decoded_frames: dict = {}
        if root.video_keys:
            decoded_frames = self._decode_video_frames(
                root, full, decode_video_frames
            )

        # 3. Emit Arrow batches.
        task_idx_pylist = full.column("task_index").to_pylist()
        for batch_start in range(0, n_rows, self._rows_per_batch):
            batch_end = min(batch_start + self._rows_per_batch, n_rows)
            batch_slice = full.slice(batch_start, batch_end - batch_start)
            task_list = [
                root.tasks_dict[task_idx_pylist[i]]
                for i in range(batch_start, batch_end)
            ]
            frame_buffers = {
                vk: decoded_frames[vk][batch_start:batch_end]
                for vk in root.video_keys
            }
            yield self._build_batch(
                root.video_keys,
                [batch_slice],
                frame_buffers,
                task_list,
                dataset_index,
                root.stats_json,
            )

    @staticmethod
    def _decode_video_frames(
        root: _LeRobotRoot,
        full: pa.Table,
        decode_fn,
    ) -> dict:
        """Decode all video frames for ``full``, batched per video file.

        Returns ``{video_key: list[np.ndarray HWC uint8]}`` aligned to
        ``full``'s row order.  Uses ``decode_fn`` (typically the patched
        :func:`lerobot.datasets.video_utils.decode_video_frames`) which
        handles both local paths and cloud URIs.
        """
        n_rows = full.num_rows
        ep_idx_col = full.column("episode_index").to_pylist()
        ts_col = full.column("timestamp").to_pylist()
        # Match the per-frame tolerance used by our previous PyAV streaming
        # loop: half a frame's interval.  Default is ~0.05s at 10fps.
        tolerance_s = 0.5 / float(root.fps)

        # Build an O(1) lookup from episode_index to (chunk, file, from_ts)
        # per camera, by scanning the episodes_table once.
        eps = root.episodes_table
        ep_idx_in_eps = eps.column("episode_index").to_pylist()

        # ``video_path`` is guaranteed non-None whenever ``video_keys`` is
        # non-empty (enforced in :func:`_build_root`).
        assert root.video_path is not None
        video_path_template = root.video_path

        decoded: dict = {}
        for vk in root.video_keys:
            chunks = eps.column(f"videos/{vk}/chunk_index").to_pylist()
            files = eps.column(f"videos/{vk}/file_index").to_pylist()
            from_ts = eps.column(f"videos/{vk}/from_timestamp").to_pylist()
            ep_info = {
                ep_idx_in_eps[i]: (chunks[i], files[i], from_ts[i])
                for i in range(len(ep_idx_in_eps))
            }

            # Group rows by video file.
            file_to_rows: dict = {}
            for r in range(n_rows):
                chunk, fi, from_t = ep_info[ep_idx_col[r]]
                file_to_rows.setdefault((chunk, fi), []).append(
                    (r, from_t + ts_col[r])
                )

            frames_by_row: List[Any] = [None] * n_rows
            for (chunk, fi), rows_and_ts in file_to_rows.items():
                # Use the full URI (with protocol) so the patched
                # decode_video_frames can detect cloud vs local correctly.
                vpath = (
                    f"{root.root}/"
                    f"{video_path_template.format(video_key=vk, chunk_index=chunk, file_index=fi)}"
                )
                row_indices = [r for r, _ in rows_and_ts]
                timestamps = [t for _, t in rows_and_ts]
                # decode_fn returns torch.Tensor of shape (N, C, H, W).
                # torchvision/torchcodec backends produce uint8 CHW.
                frames = decode_fn(
                    vpath,
                    timestamps,
                    tolerance_s,
                    storage_options=root.storage_options,
                )
                # → numpy (N, H, W, C) uint8
                arr = frames.permute(0, 2, 3, 1).contiguous().numpy()
                if arr.dtype != np.uint8:
                    if arr.dtype.kind == "f":
                        arr = (arr * 255.0).clip(0, 255).astype(np.uint8)
                    else:
                        arr = arr.astype(np.uint8)
                for i, r in enumerate(row_indices):
                    frames_by_row[r] = arr[i]
            decoded[vk] = frames_by_row
        return decoded

    @staticmethod
    def _resolve_paths(
        root: _LeRobotRoot, start: int, end: int
    ) -> tuple:
        """Resolve all file paths touched by row range ``[start, end)``.

        Returns ``(parquet_segs, video_segs)`` — flat lists of unique file
        paths used for the ``BlockMetadata.input_files`` attribution.  The
        worker-side decode logic re-derives video paths per row from
        :attr:`_LeRobotRoot.episodes_table`, so we don't need to retain
        per-camera grouping here.
        """
        eps = root.episodes_table
        start_ep, end_ep = _LeRobotReadTask._episodes_for_row_range(eps, start, end)
        ep_slice = eps.slice(start_ep, end_ep - start_ep)

        pq_chunks = ep_slice.column("data/chunk_index").combine_chunks()
        pq_files = ep_slice.column("data/file_index").combine_chunks()
        pq_new = _LeRobotReadTask._segment_boundaries(pq_chunks, pq_files)
        parquet_segs: List[str] = [
            f"{root.fs_root}/{root.data_path.format(chunk_index=c, file_index=f)}"
            for c, f in zip(
                pc.filter(pq_chunks, pq_new).to_pylist(),
                pc.filter(pq_files, pq_new).to_pylist(),
            )
        ]

        video_segs: List[str] = []
        if root.video_keys:
            assert root.video_path is not None
            video_path_template = root.video_path
            for k in root.video_keys:
                chunks = ep_slice.column(f"videos/{k}/chunk_index").combine_chunks()
                files = ep_slice.column(f"videos/{k}/file_index").combine_chunks()
                is_new = _LeRobotReadTask._segment_boundaries(chunks, files)
                video_segs.extend(
                    f"{root.fs_root}/{video_path_template.format(video_key=k, chunk_index=c, file_index=f)}"
                    for c, f in zip(
                        pc.filter(chunks, is_new).to_pylist(),
                        pc.filter(files, is_new).to_pylist(),
                    )
                )

        return parquet_segs, video_segs

    @staticmethod
    def _episodes_for_row_range(
        episodes: pa.Table,
        start_row: int,
        end_row: int,
    ) -> tuple:
        """Return the half-open ``(start_ep, end_ep)`` range covering
        ``[start_row, end_row)``."""
        from_idx = episodes.column("_global_from_index")
        to_idx = episodes.column("_global_to_index")
        mask = pc.and_(
            pc.less(from_idx, end_row),
            pc.greater(to_idx, start_row),
        )
        indices = pc.filter(episodes.column("episode_index"), mask).to_pylist()
        if not indices:
            raise ValueError(
                f"No episodes overlap the row range [{start_row}, {end_row}). "
                f"Dataset has "
                f"{episodes.column('_global_to_index')[-1].as_py()} total frames "
                f"across {len(episodes)} episodes."
            )
        return (indices[0], indices[-1] + 1)

    @staticmethod
    def _segment_boundaries(
        col_a: pa.ChunkedArray, col_b: pa.ChunkedArray
    ) -> pa.BooleanArray:
        """Boolean mask: True at index 0 and wherever the pair changes."""
        n = len(col_a)
        if n == 0:
            return pa.array([], type=pa.bool_())
        return pa.concat_arrays(
            [
                pa.array([True]),
                pc.or_(
                    pc.not_equal(col_a.slice(1), col_a.slice(0, n - 1)),
                    pc.not_equal(col_b.slice(1), col_b.slice(0, n - 1)),
                ),
            ]
        )

    @staticmethod
    def _build_batch(
        video_keys: List[str],
        pq_buffer: List[pa.Table],
        frame_buffers: dict,
        task_list: List[str],
        dataset_index: int,
        stats_json: str,
    ) -> pa.Table:
        """Assemble one Arrow batch from buffered parquet rows, decoded frames,
        tasks, and per-dataset stats."""
        table = pa.concat_tables(pq_buffer)
        columns: dict = {
            table.schema.field(i).name: table.column(i)
            for i in range(table.num_columns)
        }
        for k in video_keys:
            columns[k] = ArrowVariableShapedTensorArray.from_numpy(frame_buffers[k])
        columns["task"] = pa.array(task_list, type=pa.string())
        columns["dataset_index"] = pa.array(
            [dataset_index] * len(task_list), type=pa.int32()
        )
        columns["stats"] = pa.array([stats_json] * len(task_list), type=pa.string())
        return pa.table(columns)

@DeveloperAPI
class LeRobotDatasource(Datasource):
    """Ray Data ``Datasource`` for LeRobot v3 datasets.

    Reads LeRobot v3 datasets from local or cloud storage, combining chunked
    parquet files with decoded video frames from mp4 files.

    Use :func:`ray.data.read_lerobot` for typical use. Construct this class
    directly when you need to inspect ``source.metas`` (a list of upstream
    :class:`lerobot.LeRobotDatasetMetadata` instances) before reading, or
    when passing Ray Data execution options to ``ray.data.read_datasource``.

    Examples:
        Basic usage:

        >>> import ray  # doctest: +SKIP
        >>> ds = ray.data.read_lerobot("/path/to/dataset")  # doctest: +SKIP

        With partitioning:

        >>> from ray.data.datasource import LeRobotPartitioning  # doctest: +SKIP
        >>> source = LeRobotDatasource(  # doctest: +SKIP
        ...     "/path/to/dataset",
        ...     partitioning=LeRobotPartitioning.EPISODE,
        ... )
        >>> ds = ray.data.read_datasource(source)  # doctest: +SKIP
    """

    # (importable_module_name, pip_install_string) — the lerobot entry checks
    # for the `dataset` extra (which pulls torch/datasets) rather than the
    # bare lerobot package.  torchcodec is required so cloud-URI videos can
    # be streamed via HTTP-range reads (its decode path is fsspec-aware);
    # without it, the pyav fallback would download each video to a temp
    # file before decoding.
    _LEROBOT_DATASOURCE_DEPENDENCIES = [
        ("av", "av"),
        ("fsspec", "fsspec"),
        ("torchcodec", "torchcodec"),
        ("lerobot.datasets.dataset_metadata", "lerobot[dataset]"),
    ]

    def __init__(
        self,
        root: Union[str, Path, List[Union[str, Path]]],
        partitioning: Union[LeRobotPartitioning, str] = LeRobotPartitioning.FILE_GROUP,
        storage_options: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ):
        """Initialize LeRobot datasource.

        Args:
            root: Path or URI to the dataset root (local, ``gs://``, ``s3://``),
                or a list of such paths to read multiple datasets as one.
                All roots must share the same ``video_keys``, ``fps``, and
                non-video feature names.
            partitioning: How to divide the dataset into read tasks.
                Accepts a :class:`LeRobotPartitioning` member or its string value.
                Defaults to ``FILE_GROUP``.
            storage_options: Extra options forwarded to ``fsspec`` when reading
                metadata, parquet, and video files from a cloud URI (e.g.
                ``{"anon": False}`` for S3, or credentials / a custom
                ``endpoint_url``).  Applied to every root.  When omitted,
                ``fsspec``'s ambient credential resolution is used.
            **kwargs: Forwarded to the partitioning helper at read time.
                ``ROW_BLOCK`` requires ``block_size``; other modes take none.

        Raises:
            ValueError: If *partitioning* is not recognised, if
                ``block_size`` is omitted for ``ROW_BLOCK`` mode, or if
                roots have incompatible schemas.
        """
        super().__init__()

        for module, package in self._LEROBOT_DATASOURCE_DEPENDENCIES:
            _check_import(self, module=module, package=package)

        self._storage_options: Dict[str, Any] = dict(storage_options or {})

        roots = [root] if isinstance(root, (str, Path)) else list(root)
        # Pristine upstream metadata instances, exposed via self.metas /
        # self.meta for callers that want full lerobot API access.
        self.metas = [
            _load_lerobot_metadata(r, self._storage_options) for r in roots
        ]

        if len(self.metas) > 1:
            ref = self.metas[0]
            for m in self.metas[1:]:
                if sorted(m.video_keys) != sorted(ref.video_keys):
                    raise ValueError(
                        f"video_keys mismatch: {ref.root!r} has "
                        f"{ref.video_keys} but {m.root!r} has {m.video_keys}"
                    )
                if m.fps != ref.fps:
                    raise ValueError(
                        f"fps mismatch: {ref.root!r} has {ref.fps} "
                        f"but {m.root!r} has {m.fps}"
                    )
                ref_feats = {
                    k
                    for k, v in ref.features.items()
                    if v.get("dtype") not in ("video",) and k != "task"
                }
                m_feats = {
                    k
                    for k, v in m.features.items()
                    if v.get("dtype") not in ("video",) and k != "task"
                }
                if ref_feats != m_feats:
                    raise ValueError(
                        f"Feature mismatch: {ref.root!r} has "
                        f"{sorted(ref_feats)} but {m.root!r} has "
                        f"{sorted(m_feats)}"
                    )

        # Derived state used by slicing + read tasks.  Computed once on the
        # driver and shipped to workers via ray.put — workers don't need
        # lerobot installed at runtime.
        self._roots: List[_LeRobotRoot] = [
            _build_root(m, self._storage_options) for m in self.metas
        ]

        if isinstance(partitioning, LeRobotPartitioning):
            partitioning = partitioning.value

        _valid_modes = ("sequential", "episode", "file_group", "chain", "row_block")
        if partitioning not in _valid_modes:
            raise ValueError(
                f"Unknown partitioning {partitioning!r}. "
                f"Choose from: {', '.join(_valid_modes)}"
            )

        self._partitioning: str = partitioning
        self._slice_kwargs: dict = kwargs

        logger.info(
            "LeRobotDatasource ready: %d roots, %d total frames, "
            "%d cameras %s, mode=%r",
            len(self._roots),
            sum(r.total_frames for r in self._roots),
            len(self._roots[0].video_keys),
            self._roots[0].video_keys,
            partitioning,
        )

    @property
    def meta(self):
        """First-root upstream :class:`lerobot.LeRobotDatasetMetadata`."""
        return self.metas[0]

    # ------------------------------------------------------------------
    # Slicing helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _slices_sequential(ds_root: _LeRobotRoot) -> List[tuple]:
        return [(0, ds_root.total_frames)]

    @staticmethod
    def _slices_by_episode(ds_root: _LeRobotRoot) -> List[tuple]:
        from_indices = ds_root.episodes_table.column("_global_from_index").to_pylist()
        to_indices = ds_root.episodes_table.column("_global_to_index").to_pylist()
        return list(zip(from_indices, to_indices))

    @staticmethod
    def _slices_by_file_group(ds_root: _LeRobotRoot) -> List[tuple]:
        eps = ds_root.episodes_table

        key_columns: List[list] = []
        for vk in ds_root.video_keys:
            key_columns.append(eps.column(f"videos/{vk}/chunk_index").to_pylist())
            key_columns.append(eps.column(f"videos/{vk}/file_index").to_pylist())

        from_indices = eps.column("_global_from_index").to_pylist()
        to_indices = eps.column("_global_to_index").to_pylist()

        ranges: dict = {}
        for i in range(len(eps)):
            key = tuple(col[i] for col in key_columns)
            from_idx, to_idx = from_indices[i], to_indices[i]
            if key in ranges:
                prev_from, prev_to = ranges[key]
                if from_idx != prev_to:
                    raise ValueError(
                        f"Non-contiguous episodes share video-file group "
                        f"key {key!r}: existing span ends at row {prev_to} "
                        f"but the next episode (index {i}) starts at "
                        f"row {from_idx}. Use CHAIN or EPISODE partitioning "
                        "for datasets with non-standard episode layouts."
                    )
                ranges[key] = (prev_from, to_idx)
            else:
                ranges[key] = (from_idx, to_idx)

        return list(ranges.values())

    @staticmethod
    def _slices_by_chain(ds_root: _LeRobotRoot) -> List[tuple]:
        eps = ds_root.episodes_table
        n = len(eps)

        parent = list(range(n))
        rank = [0] * n

        def find(x: int) -> int:
            while parent[x] != x:
                parent[x] = parent[parent[x]]
                x = parent[x]
            return x

        def union(a: int, b: int) -> None:
            ra, rb = find(a), find(b)
            if ra == rb:
                return
            if rank[ra] < rank[rb]:
                ra, rb = rb, ra
            parent[rb] = ra
            if rank[ra] == rank[rb]:
                rank[ra] += 1

        video_file_to_episode: dict = {}
        for vid_key in ds_root.video_keys:
            vid_chunks = eps.column(f"videos/{vid_key}/chunk_index").to_pylist()
            vid_files = eps.column(f"videos/{vid_key}/file_index").to_pylist()
            for ep_idx in range(n):
                file_key = (vid_key, vid_chunks[ep_idx], vid_files[ep_idx])
                if file_key in video_file_to_episode:
                    union(ep_idx, video_file_to_episode[file_key])
                else:
                    video_file_to_episode[file_key] = ep_idx

        from_indices = eps.column("_global_from_index").to_pylist()
        to_indices = eps.column("_global_to_index").to_pylist()

        component_ranges: dict = {}
        for ep_idx in range(n):
            root_rep = find(ep_idx)
            from_idx, to_idx = from_indices[ep_idx], to_indices[ep_idx]
            if root_rep in component_ranges:
                prev_from, prev_to = component_ranges[root_rep]
                component_ranges[root_rep] = (
                    min(prev_from, from_idx),
                    max(prev_to, to_idx),
                )
            else:
                component_ranges[root_rep] = (from_idx, to_idx)

        return sorted(component_ranges.values())

    @staticmethod
    def _slices_by_row_block(
        ds_root: _LeRobotRoot, block_size: Optional[int] = None
    ) -> List[tuple]:
        if block_size is None:
            raise ValueError("block_size is required when partitioning is 'row_block'")
        total = ds_root.total_frames
        return [(i, min(i + block_size, total)) for i in range(0, total, block_size)]

    def _slice(self) -> List[tuple]:
        """Return ``(root_index, start, end)`` triples for all roots, sorted."""
        slice_fns = {
            "sequential": self._slices_sequential,
            "episode": self._slices_by_episode,
            "file_group": self._slices_by_file_group,
            "chain": self._slices_by_chain,
            "row_block": self._slices_by_row_block,
        }
        all_ranges: List[tuple] = []
        for root_idx, ds_root in enumerate(self._roots):
            ranges = sorted(slice_fns[self._partitioning](ds_root, **self._slice_kwargs))
            for i in range(1, len(ranges)):
                if ranges[i - 1][1] != ranges[i][0]:
                    raise ValueError(
                        f"Non-contiguous slices in root {root_idx} "
                        f"({ds_root.root!r}): slice {i - 1} ends at row "
                        f"{ranges[i - 1][1]} but slice {i} starts at "
                        f"row {ranges[i][0]}."
                    )
            all_ranges.extend((root_idx, s, e) for s, e in ranges)
        return all_ranges

    def _rows_per_batch(self, data_context: Optional[DataContext] = None) -> int:
        try:
            ctx = data_context or DataContext.get_current()
            max_block_bytes = ctx.target_max_block_size or 128 * 1024 * 1024
        except (AttributeError, RuntimeError):
            max_block_bytes = 128 * 1024 * 1024
        row_size_bytes = self._roots[0].row_size_bytes or 1
        return max(1, max_block_bytes // row_size_bytes)

    @staticmethod
    def _merge_segments(group: List[tuple]) -> List[tuple]:
        """Collapse adjacent same-root consecutive triples into wider segments."""
        if not group:
            return []
        segments: List[tuple] = []
        prev_ri, prev_s, prev_e = group[0]
        for ri, s, e in group[1:]:
            if ri == prev_ri and s == prev_e:
                prev_e = e
            else:
                segments.append((prev_ri, prev_s, prev_e))
                prev_ri, prev_s, prev_e = ri, s, e
        segments.append((prev_ri, prev_s, prev_e))
        return segments

    # ------------------------------------------------------------------
    # Ray Data API
    # ------------------------------------------------------------------

    def estimate_inmemory_data_size(self) -> Optional[int]:
        return (
            sum(r.total_frames * r.row_size_bytes for r in self._roots) or None
        )

    def get_read_tasks(
        self,
        parallelism: int,
        per_task_row_limit: Optional[int] = None,
        data_context: Optional[DataContext] = None,
    ) -> List[ReadTask]:
        row_ranges = self._slice()

        if parallelism > 0 and len(row_ranges) > parallelism:
            n = len(row_ranges)
            base, remainder = divmod(n, parallelism)
            groups: List[list] = []
            i = 0
            for g in range(parallelism):
                chunk_size = base + (1 if g < remainder else 0)
                groups.append(row_ranges[i : i + chunk_size])
                i += chunk_size
        else:
            groups = [[r] for r in row_ranges]

        task_plan = []
        for group in groups:
            segments = self._merge_segments(group)
            task_plan.append({"segments": segments})

        logger.info(
            "%d tasks, %d total frames, %d roots, %d cameras",
            len(task_plan),
            sum(r.total_frames for r in self._roots),
            len(self._roots),
            len(self._roots[0].video_keys),
        )

        roots_ref = ray.put(self._roots)
        rows_per_batch = self._rows_per_batch(data_context)
        return [
            _LeRobotReadTask(
                segments=entry["segments"],
                roots_ref=roots_ref,
                rows_per_batch=rows_per_batch,
                per_task_row_limit=per_task_row_limit,
            )
            for entry in task_plan
        ]

    def get_name(self) -> str:
        return "LeRobot"

    @property
    def supports_distributed_reads(self) -> bool:
        return True
