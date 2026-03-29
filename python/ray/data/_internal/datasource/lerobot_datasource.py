"""Ray Data datasource for LeRobot Dataset v3.

LeRobot is a platform for sharing datasets and pretrained models for
real-world robotics.  A LeRobot v3 dataset is a flat table of timestep
samples combining low-dimensional data (state, action, etc.) from chunked
parquet files with decoded camera frames from chunked mp4 files.

This datasource reads LeRobot v3 datasets from local or cloud storage,
decoding video frames with PyAV and aligning them with parquet data using
episode metadata.
"""

import enum
import json
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterator, List, Optional, Union

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

import ray
from ray.data._internal.util import _check_import
from ray.data.block import BlockMetadata
from ray.data.context import DataContext
from ray.data.datasource import Datasource
from ray.data.datasource.datasource import ReadTask
from ray.data.extensions import (
    ArrowVariableShapedTensorArray,
    ArrowVariableShapedTensorType,
)
from ray.util.annotations import DeveloperAPI, PublicAPI

if TYPE_CHECKING:
    import fsspec

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


@DeveloperAPI
class LeRobotDatasourceMetadata:
    """Lightweight metadata container for a LeRobot v3 dataset.

    Eagerly loads ``meta/info.json``, ``meta/stats.json``,
    ``meta/episodes/**/*.parquet``, and ``meta/tasks.parquet`` from
    local or cloud storage.  All subsequent I/O (data parquet files,
    video files) is deferred to workers.

    Examples:
        >>> meta = LeRobotDatasourceMetadata("/data/my_dataset")  # doctest: +SKIP
        >>> meta = LeRobotDatasourceMetadata("gs://bucket/my_dataset")  # doctest: +SKIP
        >>> print(meta.total_episodes, meta.video_keys)  # doctest: +SKIP
    """

    def __init__(self, root: Union[str, Path]) -> None:
        """Load dataset metadata from *root* (local path or cloud URI).

        Args:
            root: Path or URI to the dataset root (local, ``gs://``, ``s3://``).

        Raises:
            FileNotFoundError: If ``meta/info.json`` or the episodes parquet
                files are missing.
            ValueError: If ``meta/info.json`` is missing required keys, the
                episode index is not 0-based and contiguous, or
                ``meta/tasks.parquet`` has no recognised task column.
        """
        import fsspec

        self.root = str(root).rstrip("/")
        fs, self.fs_root = fsspec.core.url_to_fs(root)

        self.info = self._fetch_info(fs)
        self.video_keys: List[str] = [
            k for k, v in self.info["features"].items() if v.get("dtype") == "video"
        ]
        if self.video_keys and not self.info.get("video_path"):
            raise ValueError(
                f"{self.root!r}: dataset has video keys {self.video_keys} "
                "but meta/info.json has no 'video_path' template"
            )

        with fs.open(f"{self.fs_root}/meta/stats.json", "r") as f:
            self.stats: dict = json.load(f)

        self.episodes = self._fetch_episodes(fs)

        tasks_table = pq.read_table(fs.open(f"{self.fs_root}/meta/tasks.parquet", "rb"))
        if "task" in tasks_table.column_names:
            _task_col = "task"
        elif "__index_level_0__" in tasks_table.column_names:
            _task_col = "__index_level_0__"
        else:
            raise ValueError(
                f"{self.root!r}: meta/tasks.parquet has no recognised task column "
                f"(expected 'task' or '__index_level_0__'); "
                f"found {tasks_table.column_names}"
            )
        self.tasks: dict = dict(
            zip(
                tasks_table.column("task_index").to_pylist(),
                tasks_table.column(_task_col).to_pylist(),
            )
        )

        self.schema: pa.Schema = self._fetch_schema(fs)

    def _fetch_schema(self, fs: "fsspec.AbstractFileSystem") -> pa.Schema:
        """Read Arrow schema from the first data parquet file;
        append video and task fields."""
        ep = self.episodes.slice(0, 1).to_pylist()[0]
        path = (
            f"{self.fs_root}/"
            f"{self.data_path_template.format(chunk_index=ep['data/chunk_index'], file_index=ep['data/file_index'])}"
        )
        with fs.open(path, "rb") as f:
            pq_schema = pq.read_schema(f)
        fields = list(pq_schema)
        for vk in self.video_keys:
            fields.append(
                pa.field(vk, ArrowVariableShapedTensorType(pa.uint8(), ndim=3))
            )
        fields.append(pa.field("task", pa.string()))
        fields.append(pa.field("dataset_index", pa.int32()))
        return pa.schema(fields)

    def _fetch_info(self, fs: "fsspec.AbstractFileSystem") -> dict:
        """Load and validate ``meta/info.json``."""
        info_path = f"{self.fs_root}/meta/info.json"
        if not fs.exists(info_path):
            raise FileNotFoundError(
                f"No LeRobot dataset found at {self.root!r}: "
                "meta/info.json is missing. "
                "Make sure the path points to the dataset root."
            )
        with fs.open(info_path, "r") as f:
            info = json.load(f)
        for required in (
            "total_frames",
            "total_episodes",
            "fps",
            "data_path",
            "features",
        ):
            if required not in info:
                raise ValueError(
                    f"{self.root!r}: meta/info.json is missing required "
                    f"key {required!r}"
                )
        return info

    def _fetch_episodes(self, fs: "fsspec.AbstractFileSystem") -> pa.Table:
        """Load ``meta/episodes/**/*.parquet`` and append global index columns.

        Computes ``_global_from_index`` / ``_global_to_index`` from cumulative
        episode lengths.
        """
        ep_files = sorted(fs.glob(f"{self.fs_root}/meta/episodes/**/*.parquet"))
        if not ep_files:
            raise FileNotFoundError(
                f"No episode parquet files found under "
                f"{self.root!r}/meta/episodes/. "
                "The dataset may be incomplete or use an unsupported layout."
            )
        episodes = pa.concat_tables([pq.read_table(fs.open(f, "rb")) for f in ep_files])
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
        episodes = episodes.append_column(
            "_global_from_index", pa.array(global_from, type=pa.int64())
        ).append_column("_global_to_index", pa.array(global_to, type=pa.int64()))
        return episodes

    @property
    def total_frames(self) -> int:
        """Total number of frames across all episodes."""
        return self.info["total_frames"]

    @property
    def total_episodes(self) -> int:
        """Total number of episodes in the dataset."""
        return self.info["total_episodes"]

    @property
    def estimated_row_size_bytes(self) -> int:
        """Estimated in-memory size of one fully-decoded frame row (bytes)."""
        features = self.info.get("features", {})
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
                        self.root,
                    )
                    continue
        return total

    @property
    def video_path_template(self) -> str:
        """``video_path`` format string from ``meta/info.json``."""
        return self.info["video_path"]

    @property
    def data_path_template(self) -> str:
        """``data_path`` format string from ``meta/info.json``."""
        return self.info["data_path"]


class _LeRobotReadTask(ReadTask):
    """A Ray Data read task covering one or more contiguous row segments.

    Each segment is a ``(root_index, start, end)`` triple referencing a
    contiguous row range within one root of :class:`LeRobotDatasource`.
    """

    def __init__(
        self,
        segments: List[tuple],
        metas_ref: "ray.ObjectRef",
        rows_per_batch: int,
        per_task_row_limit: Optional[int] = None,
    ) -> None:
        metas: List[LeRobotDatasourceMetadata] = ray.get(metas_ref)
        total_rows = 0
        size_bytes = 0
        all_input_files: List[str] = []
        resolved: List[tuple] = []
        for root_idx, start, end in segments:
            meta = metas[root_idx]
            paths = _LeRobotReadTask._resolve_paths(meta, start, end)
            parquet_segs, video_paths = paths[0], paths[1]
            all_input_files.extend(parquet_segs)
            all_input_files.extend(p for ps in video_paths.values() for p in ps)
            total_rows += end - start
            size_bytes += (end - start) * meta.estimated_row_size_bytes
            resolved.append((root_idx, start, end, paths))

        schema = metas[segments[0][0]].schema
        block_metadata = BlockMetadata(
            num_rows=total_rows,
            size_bytes=size_bytes,
            input_files=all_input_files,
            exec_stats=None,
        )
        super().__init__(self._read, block_metadata, schema, per_task_row_limit)
        self._metas_ref = metas_ref
        self._segments_resolved = resolved
        self._rows_per_batch = rows_per_batch

    def _read(self) -> Iterator[pa.Table]:
        """Stream decoded rows as Arrow tables, iterating over all segments."""
        metas: List[LeRobotDatasourceMetadata] = ray.get(self._metas_ref)
        for root_idx, start, end, resolved_paths in self._segments_resolved:
            yield from self._read_segment(
                metas[root_idx], start, end, root_idx, resolved_paths
            )

    def _read_segment(
        self,
        meta: LeRobotDatasourceMetadata,
        start: int,
        end: int,
        dataset_index: int,
        resolved_paths: tuple,
    ) -> Iterator[pa.Table]:
        """Stream decoded rows for one ``[start, end)`` range within a single root."""
        import fsspec

        parquet_segs, video_paths, video_start_ts, ep_from_ts = resolved_paths

        fs, _ = fsspec.core.url_to_fs(meta.root)
        is_local = (
            fs.protocol == "file"
            if isinstance(fs.protocol, str)
            else "file" in fs.protocol
        )

        pq_buffer: List[pa.Table] = []
        frame_buffers: dict = {k: [] for k in meta.video_keys}
        task_list: List[str] = []

        frame_iters = {
            k: self._frame_stream(fs, is_local, video_paths[k], video_start_ts[k])
            for k in meta.video_keys
        }
        cur: dict = dict.fromkeys(meta.video_keys)

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

                    for k in meta.video_keys:
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

                    task_list.append(meta.tasks[task_idx_col[row_idx].as_py()])

                    if len(task_list) >= self._rows_per_batch:
                        pq_buffer.append(
                            pq_table.slice(seg_start, row_idx + 1 - seg_start)
                        )
                        yield self._build_batch(
                            meta.video_keys,
                            pq_buffer,
                            frame_buffers,
                            task_list,
                            dataset_index,
                        )
                        pq_buffer = []
                        frame_buffers = {k: [] for k in meta.video_keys}
                        task_list = []
                        seg_start = row_idx + 1

                if seg_start < pq_table.num_rows:
                    pq_buffer.append(pq_table.slice(seg_start))

        finally:
            for it in frame_iters.values():
                it.close()

        if pq_buffer:
            yield self._build_batch(
                meta.video_keys,
                pq_buffer,
                frame_buffers,
                task_list,
                dataset_index,
            )

    @staticmethod
    def _resolve_paths(
        meta: LeRobotDatasourceMetadata,
        start: int,
        end: int,
    ) -> tuple:
        """Resolve all file paths needed to read rows ``[start, end)``.

        Returns ``(parquet_segs, video_paths, video_start_ts, ep_from_ts)``.
        """
        start_ep, end_ep = _LeRobotReadTask._episodes_for_row_range(
            meta.episodes, start, end
        )
        ep_slice = meta.episodes.slice(start_ep, end_ep - start_ep)

        pq_chunks = ep_slice.column("data/chunk_index").combine_chunks()
        pq_files = ep_slice.column("data/file_index").combine_chunks()
        pq_new = _LeRobotReadTask._segment_boundaries(pq_chunks, pq_files)
        parquet_segs: List[str] = [
            f"{meta.fs_root}/{meta.data_path_template.format(chunk_index=c, file_index=f)}"
            for c, f in zip(
                pc.filter(pq_chunks, pq_new).to_pylist(),
                pc.filter(pq_files, pq_new).to_pylist(),
            )
        ]

        video_paths: dict = {}
        video_start_ts: dict = {}
        ep_from_ts: dict = {}
        if meta.video_keys:
            video_path_template = meta.video_path_template
            ep_indices = ep_slice.column("episode_index").to_pylist()
            for k in meta.video_keys:
                from_ts_vals = ep_slice.column(f"videos/{k}/from_timestamp").to_pylist()
                ep_from_ts[k] = dict(zip(ep_indices, from_ts_vals))
                chunks = ep_slice.column(f"videos/{k}/chunk_index").combine_chunks()
                files = ep_slice.column(f"videos/{k}/file_index").combine_chunks()
                is_new = _LeRobotReadTask._segment_boundaries(chunks, files)
                video_paths[k] = [
                    f"{meta.fs_root}/{video_path_template.format(video_key=k, chunk_index=c, file_index=f)}"
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
    directly when you need to inspect ``source.meta`` before reading, or when
    passing Ray Data execution options to ``ray.data.read_datasource``.

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

    _LEROBOT_DATASOURCE_DEPENDENCIES = ["av", "fsspec"]

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

        for dep in self._LEROBOT_DATASOURCE_DEPENDENCIES:
            _check_import(self, module=dep, package=dep)

        roots = [root] if isinstance(root, (str, Path)) else list(root)
        self.metas = [LeRobotDatasourceMetadata(r) for r in roots]

        if len(self.metas) > 1:
            ref = self.metas[0]
            for m in self.metas[1:]:
                if sorted(m.video_keys) != sorted(ref.video_keys):
                    raise ValueError(
                        f"video_keys mismatch: {ref.root!r} has "
                        f"{ref.video_keys} but {m.root!r} has {m.video_keys}"
                    )
                if m.info["fps"] != ref.info["fps"]:
                    raise ValueError(
                        f"fps mismatch: {ref.root!r} has {ref.info['fps']} "
                        f"but {m.root!r} has {m.info['fps']}"
                    )
                ref_feats = {
                    k
                    for k, v in ref.info["features"].items()
                    if v.get("dtype") not in ("video",) and k != "task"
                }
                m_feats = {
                    k
                    for k, v in m.info["features"].items()
                    if v.get("dtype") not in ("video",) and k != "task"
                }
                if ref_feats != m_feats:
                    raise ValueError(
                        f"Feature mismatch: {ref.root!r} has "
                        f"{sorted(ref_feats)} but {m.root!r} has "
                        f"{sorted(m_feats)}"
                    )

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
            len(self.metas),
            sum(m.total_frames for m in self.metas),
            len(self.meta.video_keys),
            self.meta.video_keys,
            partitioning,
        )

    @property
    def meta(self) -> LeRobotDatasourceMetadata:
        """Metadata for the first root; use ``self.metas`` for all roots."""
        return self.metas[0]

    # ------------------------------------------------------------------
    # Slicing helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _slices_sequential(
        ds_meta: LeRobotDatasourceMetadata,
    ) -> List[tuple]:
        return [(0, ds_meta.total_frames)]

    @staticmethod
    def _slices_by_episode(
        ds_meta: LeRobotDatasourceMetadata,
    ) -> List[tuple]:
        from_indices = ds_meta.episodes.column("_global_from_index").to_pylist()
        to_indices = ds_meta.episodes.column("_global_to_index").to_pylist()
        return list(zip(from_indices, to_indices))

    @staticmethod
    def _slices_by_file_group(
        ds_meta: LeRobotDatasourceMetadata,
    ) -> List[tuple]:
        eps = ds_meta.episodes

        key_columns: List[list] = []
        for vk in ds_meta.video_keys:
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
    def _slices_by_chain(
        ds_meta: LeRobotDatasourceMetadata,
    ) -> List[tuple]:
        eps = ds_meta.episodes
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
        for vid_key in ds_meta.video_keys:
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
            root = find(ep_idx)
            from_idx, to_idx = from_indices[ep_idx], to_indices[ep_idx]
            if root in component_ranges:
                prev_from, prev_to = component_ranges[root]
                component_ranges[root] = (
                    min(prev_from, from_idx),
                    max(prev_to, to_idx),
                )
            else:
                component_ranges[root] = (from_idx, to_idx)

        return sorted(component_ranges.values())

    @staticmethod
    def _slices_by_row_block(
        ds_meta: LeRobotDatasourceMetadata,
        block_size: Optional[int] = None,
    ) -> List[tuple]:
        if block_size is None:
            raise ValueError("block_size is required when partitioning is 'row_block'")
        total = ds_meta.total_frames
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
        for root_idx, meta in enumerate(self.metas):
            ranges = sorted(slice_fns[self._partitioning](meta, **self._slice_kwargs))
            for i in range(1, len(ranges)):
                if ranges[i - 1][1] != ranges[i][0]:
                    raise ValueError(
                        f"Non-contiguous slices in root {root_idx} "
                        f"({meta.root!r}): slice {i - 1} ends at row "
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
        row_size_bytes = self.meta.estimated_row_size_bytes or 1
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
            sum(m.total_frames * m.estimated_row_size_bytes for m in self.metas) or None
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
            sum(m.total_frames for m in self.metas),
            len(self.metas),
            len(self.meta.video_keys),
        )

        metas_ref = ray.put(self.metas)
        rows_per_batch = self._rows_per_batch(data_context)
        return [
            _LeRobotReadTask(
                segments=entry["segments"],
                metas_ref=metas_ref,
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
