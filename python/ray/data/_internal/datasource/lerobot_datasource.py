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
    variable-shape-tensor video columns + ``task`` + ``dataset_index``)."""

    row_size_bytes: int
    """Estimated in-memory size of one fully-decoded row, in bytes."""

    total_frames: int
    """Number of frames across all episodes."""


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
    variable-shape-tensor video columns plus ``task`` and ``dataset_index``."""
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


def _load_lerobot_metadata(root: Union[str, Path]):
    """Construct a pristine :class:`lerobot.LeRobotDatasetMetadata` for
    *root*, applying the fsspec patch first so cloud URIs work.

    Pre-validates that ``meta/info.json`` exists at *root* before invoking
    lerobot, so missing-dataset errors stay as ``FileNotFoundError`` rather
    than getting transformed into HuggingFace Hub validation errors by
    lerobot's local-then-Hub fallback logic.
    """
    import fsspec

    from lerobot.datasets.dataset_metadata import LeRobotDatasetMetadata

    apply_lerobot_fsspec_patches()
    root_uri = str(root).rstrip("/")

    fs, fs_root = fsspec.core.url_to_fs(root_uri)
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
    return LeRobotDatasetMetadata(repo_id=root_uri, root=root_uri)


def _build_root(meta) -> _LeRobotRoot:
    """Compute the per-root derived state bundle for a (pristine) lerobot
    ``LeRobotDatasetMetadata`` instance.  Does not mutate *meta*."""
    import fsspec

    root_uri = str(meta.root).rstrip("/")

    # In lerobot 0.5.x, meta.info is a dict; accessing meta.video_path
    # raises KeyError when the key is absent.  Use dict-aware lookup.
    video_path = meta.info.get("video_path") if isinstance(meta.info, dict) else getattr(meta.info, "video_path", None)
    if meta.video_keys and not video_path:
        raise ValueError(
            f"{root_uri!r}: dataset has video keys {meta.video_keys} "
            "but meta/info.json has no 'video_path' template"
        )

    fs, fs_root = fsspec.core.url_to_fs(root_uri)
    fs_root = fs_root.rstrip("/")
    episodes_table = _build_episodes_table(meta.episodes)
    tasks_dict = _build_tasks_dict(meta.tasks)
    schema = _build_schema(
        episodes_table, meta.data_path, meta.video_keys, fs, fs_root
    )
    row_size_bytes = _estimated_row_size_bytes(meta.features, root_uri)

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
            paths = _LeRobotReadTask._resolve_paths(root, start, end)
            parquet_segs, video_paths = paths[0], paths[1]
            all_input_files.extend(parquet_segs)
            all_input_files.extend(p for ps in video_paths.values() for p in ps)
            total_rows += end - start
            size_bytes += (end - start) * root.row_size_bytes
            resolved.append((root_idx, start, end, paths))

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
        for root_idx, start, end, resolved_paths in self._segments_resolved:
            yield from self._read_segment(
                roots[root_idx], start, end, root_idx, resolved_paths
            )

    def _read_segment(
        self,
        root: _LeRobotRoot,
        start: int,
        end: int,
        dataset_index: int,
        resolved_paths: tuple,
    ) -> Iterator[pa.Table]:
        """Stream decoded rows for one ``[start, end)`` range within a single root."""
        import fsspec

        parquet_segs, video_paths, video_start_ts, ep_from_ts = resolved_paths

        fs, _ = fsspec.core.url_to_fs(root.root)
        is_local = (
            fs.protocol == "file"
            if isinstance(fs.protocol, str)
            else "file" in fs.protocol
        )

        pq_buffer: List[pa.Table] = []
        frame_buffers: dict = {k: [] for k in root.video_keys}
        task_list: List[str] = []

        frame_iters = {
            k: self._frame_stream(fs, is_local, video_paths[k], video_start_ts[k])
            for k in root.video_keys
        }
        cur: dict = dict.fromkeys(root.video_keys)

        try:
            filters = [("index", ">=", start), ("index", "<", end)]

            for path in parquet_segs:
                with fs.open(path, "rb") as f:
                    pq_table = pq.read_table(f, filters=filters)

                task_idx_col = pq_table.column("task_index")
                timestamp_col = pq_table.column("timestamp")
                ep_idx_col = pq_table.column("episode_index")
                seg_start = 0

                for row_idx in range(pq_table.num_rows):
                    ep_idx = ep_idx_col[row_idx].as_py()
                    row_ts = timestamp_col[row_idx].as_py()

                    for k in root.video_keys:
                        target_ts = ep_from_ts[k][ep_idx] + row_ts
                        if cur[k] is None:
                            cur[k] = self._next_frame(
                                frame_iters, start, k, row_idx, ep_idx
                            )
                        while (
                            cur[k][0].time is None
                            or cur[k][0].time < target_ts - cur[k][1]
                        ):
                            cur[k] = self._next_frame(
                                frame_iters, start, k, row_idx, ep_idx
                            )
                        frame_buffers[k].append(cur[k][0].to_ndarray(format="rgb24"))

                    task_list.append(root.tasks_dict[task_idx_col[row_idx].as_py()])

                    if len(task_list) >= self._rows_per_batch:
                        pq_buffer.append(
                            pq_table.slice(seg_start, row_idx + 1 - seg_start)
                        )
                        yield self._build_batch(
                            root.video_keys,
                            pq_buffer,
                            frame_buffers,
                            task_list,
                            dataset_index,
                        )
                        pq_buffer = []
                        frame_buffers = {k: [] for k in root.video_keys}
                        task_list = []
                        seg_start = row_idx + 1

                if seg_start < pq_table.num_rows:
                    pq_buffer.append(pq_table.slice(seg_start))

        finally:
            for it in frame_iters.values():
                it.close()

        if pq_buffer:
            yield self._build_batch(
                root.video_keys,
                pq_buffer,
                frame_buffers,
                task_list,
                dataset_index,
            )

    @staticmethod
    def _resolve_paths(root: _LeRobotRoot, start: int, end: int) -> tuple:
        """Resolve all file paths needed to read rows ``[start, end)``.

        Returns ``(parquet_segs, video_paths, video_start_ts, ep_from_ts)``.
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

        video_paths: dict = {}
        video_start_ts: dict = {}
        ep_from_ts: dict = {}
        if root.video_keys:
            video_path_template = root.video_path
            ep_indices = ep_slice.column("episode_index").to_pylist()
            for k in root.video_keys:
                from_ts_vals = ep_slice.column(f"videos/{k}/from_timestamp").to_pylist()
                ep_from_ts[k] = dict(zip(ep_indices, from_ts_vals))
                chunks = ep_slice.column(f"videos/{k}/chunk_index").combine_chunks()
                files = ep_slice.column(f"videos/{k}/file_index").combine_chunks()
                is_new = _LeRobotReadTask._segment_boundaries(chunks, files)
                video_paths[k] = [
                    f"{root.fs_root}/{video_path_template.format(video_key=k, chunk_index=c, file_index=f)}"
                    for c, f in zip(
                        pc.filter(chunks, is_new).to_pylist(),
                        pc.filter(files, is_new).to_pylist(),
                    )
                ]
                video_start_ts[k] = from_ts_vals[0] if from_ts_vals else 0.0

        return parquet_segs, video_paths, video_start_ts, ep_from_ts

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
    def _frame_stream(fs: Any, is_local: bool, fs_paths: List[str], start_ts: float):
        """Yield ``(frame, half_frame)`` across all video files for one camera."""
        import av

        for i, path in enumerate(fs_paths):
            container = av.open(path) if is_local else av.open(fs.open(path, "rb"))
            try:
                stream = container.streams.video[0]
                if stream.time_base is None or stream.average_rate is None:
                    raise ValueError(
                        f"Video stream in {path!r} has no time_base or "
                        "average_rate; the file may be corrupt or encoded "
                        "without timing metadata."
                    )
                if i == 0 and start_ts > 0:
                    container.seek(int(start_ts / stream.time_base), stream=stream)
                half_frame = 0.5 / float(stream.average_rate)
                for packet in container.demux(video=0):
                    try:
                        for frame in packet.decode():
                            yield frame, half_frame
                    except av.InvalidDataError:
                        continue
            finally:
                container.close()

    @staticmethod
    def _build_batch(
        video_keys: List[str],
        pq_buffer: List[pa.Table],
        frame_buffers: dict,
        task_list: List[str],
        dataset_index: int,
    ) -> pa.Table:
        """Assemble one Arrow batch from buffered parquet rows, decoded frames,
        and tasks."""
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
        return pa.table(columns)

    @staticmethod
    def _next_frame(
        frame_iters: dict,
        start: int,
        cam_key: str,
        row_idx: int,
        ep_idx: int,
    ) -> Any:
        """Advance one camera's iterator; raise on exhaustion."""
        try:
            return next(frame_iters[cam_key])
        except StopIteration:
            raise RuntimeError(
                f"Video stream for camera {cam_key!r} exhausted at"
                f" row {start + row_idx} (episode {ep_idx})."
                " The video file may be truncated."
            ) from None


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
    # bare lerobot package.
    _LEROBOT_DATASOURCE_DEPENDENCIES = [
        ("av", "av"),
        ("fsspec", "fsspec"),
        ("lerobot.datasets.dataset_metadata", "lerobot[dataset]"),
    ]

    def __init__(
        self,
        root: Union[str, Path, List[Union[str, Path]]],
        partitioning: Union[LeRobotPartitioning, str] = LeRobotPartitioning.FILE_GROUP,
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

        roots = [root] if isinstance(root, (str, Path)) else list(root)
        # Pristine upstream metadata instances, exposed via self.metas /
        # self.meta for callers that want full lerobot API access.
        self.metas = [_load_lerobot_metadata(r) for r in roots]

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
        self._roots: List[_LeRobotRoot] = [_build_root(m) for m in self.metas]

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
