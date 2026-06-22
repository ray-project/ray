"""Ray Data datasource for LeRobot Dataset v3."""

from __future__ import annotations

import json
import logging
import os
import shutil
import tempfile
import weakref
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterator,
    List,
    NamedTuple,
    Optional,
    Tuple,
    Union,
)

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

import ray
from ray.data._internal.datasource._lerobot_compat import (
    decode_frames,
    new_decoder_cache,
)
from ray.data._internal.util import (
    _check_import,
    _is_local_scheme,
    _resolve_custom_scheme,
)
from ray.data.block import BlockMetadata
from ray.data.context import DataContext
from ray.data.datasource.datasource import Datasource, ReadTask
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    import datasets
    import fsspec
    import pyarrow.fs
    from lerobot.datasets.dataset_metadata import LeRobotDatasetMetadata

logger = logging.getLogger(__name__)


class _LeRobotRoot(NamedTuple):
    """Per-root derived state for the LeRobot datasource built on the driver."""

    root: str
    """Root URI used to build the by-URI video paths streamed through torchcodec
    (``s3://anonymous@b/x`` is normalized to ``s3://b/x`` — see ``storage_options``)."""

    fs: "fsspec.AbstractFileSystem"
    """Resolved fsspec filesystem for metadata + parquet I/O, built once on the
    driver (see :func:`_resolve_filesystem`) and shipped to workers."""

    fs_root: str
    """Dataset path relative to ``fs``'s root for path joining
    (``s3://b/x`` -> ``b/x``)."""

    data_path: str
    """``info.data_path`` format string for chunked parquet files."""

    video_path: Optional[str]
    """``info.video_path`` format string for chunked mp4 files (``None`` if no videos)."""

    video_keys: List[str]
    """Feature keys with ``dtype == 'video'`` (camera streams stored as mp4)."""

    image_keys: List[str]
    """Feature keys with ``dtype == 'image'`` (camera streams stored as encoded
    image bytes — HF ``struct<bytes, path>`` — in the parquet rows, not as mp4)."""

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
    """fsspec options for the by-URI video decode path (lerobot opens video files
    itself via ``fsspec.open``, not through ``fs``).  For ``s3://anonymous@…``
    roots ``anon=True`` is threaded in here.  Empty dict means rely on ambient
    fsspec credential resolution."""

    stats_json: str
    """Per-feature normalization statistics (mean/std/min/max/…) from
    ``meta/stats.json``, serialized to a JSON string.  Emitted verbatim on
    every row as the ``stats`` column; ``"{}"`` when the dataset has none."""

    frame_tolerance_s: Optional[float]
    """Max seconds a decoded video frame's timestamp may differ from a row's
    timestamp before it is rejected (passed to lerobot's ``decode_video_frames``).
    ``None`` means use the default ``0.5 / fps`` (half a frame interval)."""


# ---------------------------------------------------------------------------
# Driver-side derived-state builders.
# ---------------------------------------------------------------------------


def _build_episodes_table(hf_episodes: "datasets.Dataset") -> pa.Table:
    """Convert lerobot's HF ``Dataset`` of episodes to a pyarrow ``Table``
    with cumulative ``_global_from_index`` / ``_global_to_index`` columns
    derived from per-episode lengths.
    """
    episodes = hf_episodes.with_format("arrow")[:]
    ep_indices = episodes.column("episode_index").to_numpy(zero_copy_only=False)
    n = len(ep_indices)
    if not np.array_equal(ep_indices, np.arange(n)):
        raise ValueError(
            f"Episodes are not 0-indexed and contiguous: "
            f"first={ep_indices[0] if n else None}, "
            f"last={ep_indices[-1] if n else None}, count={n}"
        )
    # Cast to int64 before cumsum: PB-scale cumulative frame counts may overflow int32.
    lengths = episodes.column("length").to_numpy(zero_copy_only=False).astype(np.int64)
    global_to = np.cumsum(lengths)
    global_from = global_to - lengths
    return episodes.append_column(
        "_global_from_index", pa.array(global_from, type=pa.int64())
    ).append_column("_global_to_index", pa.array(global_to, type=pa.int64()))


def _build_schema(
    episodes_table: pa.Table,
    data_path: str,
    video_keys: List[str],
    image_keys: List[str],
    fs: "fsspec.AbstractFileSystem",
    fs_root: str,
) -> pa.Schema:
    """Read the Arrow schema of the first data parquet file and produce the
    output schema: in-parquet ``image`` columns (HF ``struct<bytes, path>``) are
    replaced by decoded uint8 tensor columns, ``video`` columns are appended as
    decoded tensors, plus ``task``, ``dataset_index`` and ``stats``."""
    from ray.data.extensions import ArrowVariableShapedTensorType

    ep = episodes_table.slice(0, 1).to_pylist()[0]
    path = (
        f"{fs_root}/"
        f"{data_path.format(chunk_index=ep['data/chunk_index'], file_index=ep['data/file_index'])}"
    )
    with fs.open(path, "rb") as f:
        pq_schema = pq.read_schema(f)
    image_set = set(image_keys)
    frame_type = ArrowVariableShapedTensorType(pa.uint8(), ndim=3)
    # Image columns live in the parquet as encoded-byte structs; swap them for
    # the decoded-tensor type in place (preserving column order).
    fields = [
        pa.field(f.name, frame_type) if f.name in image_set else f for f in pq_schema
    ]
    # Video columns are not in the parquet; append them.
    for vk in video_keys:
        fields.append(pa.field(vk, frame_type))
    # task + stats are per-dataset constants repeated on every row, so they are
    # dictionary-encoded (one shared value per block + an int32 index per row)
    # instead of duplicating the (multi-KB) stats JSON on each row.
    dict_str = pa.dictionary(pa.int32(), pa.string())
    fields.append(pa.field("task", dict_str))
    fields.append(pa.field("dataset_index", pa.int32()))
    fields.append(pa.field("stats", dict_str))
    return pa.schema(fields)


def _estimated_row_size_bytes(features: dict, root_for_logging: str) -> int:
    """Estimated in-memory size of one fully-decoded frame row, in bytes."""
    total = 0
    for feat_name, feat in features.items():
        if feat.get("dtype") in ("video", "image"):
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
                    "skipping. Continuing without this estimate.",
                    feat_name,
                    root_for_logging,
                )
                continue
    # Output rows also carry task + dataset_index + stats columns. task and
    # stats are dictionary-encoded (the shared value lives once per block), so
    # the per-row cost is three int32s (two dictionary indices + dataset_index).
    total += 3 * 4
    return total


def _stats_to_json(stats: Optional[dict]) -> str:
    """Serialize lerobot's per-feature stats dict.

    Converts numpy arrays / scalars to plain lists / numbers.
    Returns ``"{}"`` when *stats* is missing or empty.  Emitted verbatim on every
    row as the ``stats`` column so downstream tasks.

    We serialize stats such that we can work with multiple datasets
    with possibly different nestings of stats. Ray Data requires us to
    have the same schema accross the datasets.
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


def _resolve_filesystem(
    root: Union[str, Path],
    filesystem: Optional["pyarrow.fs.FileSystem | fsspec.AbstractFileSystem"] = None,
    storage_options: Optional[Dict[str, Any]] = None,
) -> Tuple["fsspec.AbstractFileSystem", str, str, Dict[str, Any], bool]:
    """Resolve the fsspec filesystem and paths for one LeRobot dataset *root*.

    * **metadata + parquet** are read through a single fsspec filesystem,
      returned as ``fs`` with the dataset path ``fs_root`` relative to it;
    * **video files** are streamed *by URI* through torchcodec — lerobot opens
      those itself via ``fsspec.open``, not through ``fs`` — so they get the URI
      ``video_root_uri`` and options ``video_storage_options`` instead.

    Precedence for ``fs`` (matching every other ``read_*`` API):

    1. an explicit *filesystem* — a pyarrow ``FileSystem`` (wrapped with
       :class:`~fsspec.implementations.arrow.ArrowFSWrapper`) or an fsspec
       ``AbstractFileSystem``;
    2. otherwise, when *storage_options* is given, ``fsspec.url_to_fs`` so one
       set of options covers metadata, parquet, and video;
    3. otherwise Ray Data's standard :func:`_resolve_paths_and_filesystem`
       (scheme detection plus conventions such as ``s3://anonymous@bucket/…``).

    The ``s3://anonymous@…`` convention is also mapped onto the by-URI video
    path: the marker is stripped from ``video_root_uri`` and ``anon=True`` is
    threaded into ``video_storage_options`` (s3fs spells anonymous ``anon=True``).

    Returns ``(fs, fs_root, video_root_uri, video_storage_options,
    video_creds_unavailable)`` — the last flag is True when an explicit pyarrow
    *filesystem* cannot supply credentials to the by-URI video path (see below).
    """
    import fsspec
    from fsspec.core import split_protocol
    from fsspec.spec import AbstractFileSystem

    # Resolve Ray Data's custom schemes (``local://``, ``example://``) to plain
    # paths: they are scheduling/convenience markers, not real fsspec protocols,
    # so both fsspec resolution and the by-URI video path need the bare path.
    root_uri = _resolve_custom_scheme(str(root)).rstrip("/")
    storage_options = dict(storage_options or {})

    # Map Ray Data's anonymous@ convention onto the by-URI video path.
    video_root_uri = root_uri
    video_storage_options = dict(storage_options)
    protocol, rest = split_protocol(root_uri)
    if protocol and rest and rest.startswith("anonymous@"):
        video_root_uri = f"{protocol}://{rest[len('anonymous@') :]}"
        video_storage_options.setdefault("anon", True)

    video_creds_unavailable = False
    if filesystem is not None:
        from pyarrow.fs import FileSystem as _PaFileSystem

        if isinstance(filesystem, AbstractFileSystem):
            fs = filesystem
            # The same filesystem must also cover the by-URI video decode path
            # (torchcodec opens videos via fsspec.open, not through fs). fsspec
            # filesystems expose the kwargs that recreate them, so thread those
            # into the video options; any explicit storage_options still win.
            video_storage_options = {
                **(getattr(filesystem, "storage_options", None) or {}),
                **video_storage_options,
            }
        elif isinstance(filesystem, _PaFileSystem):
            from fsspec.implementations.arrow import ArrowFSWrapper

            fs = ArrowFSWrapper(filesystem)
            # pyarrow filesystems do not expose their credentials, so we cannot
            # thread them into the by-URI video path. For a remote root that
            # would leave video decode without credentials; flag it so we can
            # fail loudly later, but only if the dataset actually has video.
            video_creds_unavailable = bool(protocol) and protocol not in (
                "file",
                "local",
            )
        else:
            raise TypeError(
                f"filesystem must be a pyarrow.fs.FileSystem or an "
                f"fsspec.spec.AbstractFileSystem, got {type(filesystem).__name__}"
            )
        _, fs_root = split_protocol(root_uri)
        fs_root = (fs_root or root_uri).rstrip("/")
    elif storage_options:
        # Explicit fsspec options (credentials / endpoint_url / …): resolve via
        # fsspec so the same options cover metadata, parquet, and video.
        fs, fs_root = fsspec.core.url_to_fs(video_root_uri, **video_storage_options)
        fs_root = fs_root.rstrip("/")
    else:
        # Default: Ray Data's standard URI->filesystem resolver (the same one
        # every other read_* API uses); also handles s3://anonymous@… .
        from fsspec.implementations.arrow import ArrowFSWrapper

        from ray.data.datasource.path_util import _resolve_paths_and_filesystem

        resolved_paths, pa_fs = _resolve_paths_and_filesystem([root_uri])
        fs = ArrowFSWrapper(pa_fs)
        fs_root = resolved_paths[0].rstrip("/")

    return fs, fs_root, video_root_uri, video_storage_options, video_creds_unavailable


def _load_lerobot_metadata(
    root: Union[str, Path],
    fs: "fsspec.AbstractFileSystem",
    fs_root: str,
) -> "LeRobotDatasetMetadata":
    """Construct a pristine :class:`lerobot.LeRobotDatasetMetadata` for *root*.

    lerobot's metadata loader only reads local paths. For a local *root* we point
    it straight at the dataset; for a remote/URI *root* we materialize the small
    ``meta/`` tree (info/tasks/episodes/stats — KBs to a few MB) into a temp dir
    via ``fsspec`` and load from there. Either way lerobot's *own* parser runs —
    no reimplementation, no monkeypatch — which keeps us robust across lerobot
    versions.

    Pre-validates that ``meta/info.json`` exists at *root* so a missing dataset
    stays a clean ``FileNotFoundError`` rather than getting transformed into a
    HuggingFace Hub validation error by lerobot's local-then-Hub fallback.

    *fs* / *fs_root* are the resolved fsspec filesystem and the dataset path
    relative to it (see :func:`_resolve_filesystem`); they are used for the
    existence check and the metadata copy.
    """
    from lerobot.datasets.dataset_metadata import LeRobotDatasetMetadata

    root_uri = str(root).rstrip("/")
    fs_root = fs_root.rstrip("/")
    if not fs.exists(f"{fs_root}/meta/info.json"):
        raise FileNotFoundError(
            f"No LeRobot dataset found at {root_uri!r}: meta/info.json is missing. "
            "Make sure the path points to the dataset root."
        )

    # repo_id is decorative for read-only use (error messages / HF Hub fallback
    # target); we pass the root itself so any fallback fails clearly.
    if "://" not in root_uri:
        # Local path: lerobot reads it directly.
        return LeRobotDatasetMetadata(repo_id=root_uri, root=root_uri)

    # Remote URI: copy meta/ locally and let lerobot parse the local copy.
    local_root = tempfile.mkdtemp(prefix="ray_data_lerobot_")
    fs.get(f"{fs_root}/meta", os.path.join(local_root, "meta"), recursive=True)
    meta = LeRobotDatasetMetadata(repo_id=root_uri, root=local_root)
    # lerobot may read meta files lazily, and `meta` is exposed via
    # ``source.meta``; drop the temp copy when the object is garbage-collected.
    weakref.finalize(meta, shutil.rmtree, local_root, ignore_errors=True)
    return meta


def _build_root(
    meta: "LeRobotDatasetMetadata",
    root: Union[str, Path],
    fs: "fsspec.AbstractFileSystem",
    fs_root: str,
    video_root_uri: str,
    video_storage_options: Dict[str, Any],
    video_creds_unavailable: bool = False,
    frame_tolerance_s: Optional[float] = None,
) -> Tuple[_LeRobotRoot, pa.Table]:
    """Compute the per-root derived state bundle for a (pristine) lerobot
    ``LeRobotDatasetMetadata`` instance.  Does not mutate *meta*.

    *root* is the original dataset location (where data + video files live). For
    a remote root ``meta.root`` points at a local temp copy of ``meta/`` only, so
    data/video paths must be resolved against *root* — not ``meta.root``.

    *fs* / *fs_root* (the resolved fsspec filesystem and the relative dataset
    path) are used for the parquet schema read and captured on the returned
    :class:`_LeRobotRoot` so workers reuse them.  *video_root_uri* /
    *video_storage_options* are the URI + fsspec options for the by-URI video
    decode path (see :func:`_resolve_filesystem`).
    """
    root_uri = str(root).rstrip("/")
    fs_root = fs_root.rstrip("/")

    # In lerobot 0.5.x, meta.info is a dict; accessing meta.video_path
    # raises KeyError when the key is absent.  Use dict-aware lookup.
    video_path = (
        meta.info.get("video_path")
        if isinstance(meta.info, dict)
        else getattr(meta.info, "video_path", None)
    )
    if meta.video_keys and not video_path:
        raise ValueError(
            f"{root_uri!r}: dataset has video keys {meta.video_keys} "
            "but meta/info.json has no 'video_path' template"
        )
    if meta.video_keys and video_creds_unavailable:
        raise ValueError(
            f"{root_uri!r}: an explicit pyarrow `filesystem=` cannot supply "
            f"credentials to the video decode path (videos are streamed by URI "
            f"through torchcodec/fsspec, not through the filesystem object). For "
            f"credentialed cloud video, pass `storage_options=` (e.g. "
            f"{{'key': ..., 'secret': ...}}) or an fsspec filesystem instead."
        )

    image_keys = list(getattr(meta, "image_keys", []) or [])
    episodes_table = _build_episodes_table(meta.episodes)
    # Project to only the columns the planner (slicing) and worker decode use,
    # so each per-task episode slice shipped to workers stays small at PB scale.
    keep = [
        "episode_index",
        "_global_from_index",
        "_global_to_index",
        "data/chunk_index",
        "data/file_index",
    ]
    for vk in meta.video_keys:
        keep += [
            f"videos/{vk}/chunk_index",
            f"videos/{vk}/file_index",
            f"videos/{vk}/from_timestamp",
        ]
    episodes_table = episodes_table.select(keep)
    # lerobot's tasks DataFrame is indexed by task name with a task_index
    # column; invert it to {task_index: task_name}.
    tasks_dict = dict(
        zip(meta.tasks["task_index"].astype(int).tolist(), meta.tasks.index.tolist())
    )
    schema = _build_schema(
        episodes_table, meta.data_path, meta.video_keys, image_keys, fs, fs_root
    )
    row_size_bytes = _estimated_row_size_bytes(meta.features, root_uri)
    stats_json = _stats_to_json(getattr(meta, "stats", None))

    root_bundle = _LeRobotRoot(
        root=video_root_uri,
        fs=fs,
        fs_root=fs_root,
        data_path=meta.data_path,
        video_path=video_path,
        video_keys=list(meta.video_keys),
        image_keys=image_keys,
        tasks_dict=tasks_dict,
        schema=schema,
        row_size_bytes=row_size_bytes,
        total_frames=meta.total_frames,
        fps=meta.fps,
        storage_options=video_storage_options,
        stats_json=stats_json,
        frame_tolerance_s=frame_tolerance_s,
    )
    # The projected episodes table is kept driver-side (in LeRobotDatasource),
    # not stored on the shipped-to-workers root.
    return root_bundle, episodes_table


class _LeRobotReadTask(ReadTask):
    """A Ray Data read task covering one or more contiguous row segments.

    Each segment is a ``(root_index, start, end)`` triple referencing a
    contiguous row range within one root of :class:`LeRobotDatasource`.
    """

    def __init__(
        self,
        segments: List[tuple],
        roots: List[_LeRobotRoot],
        roots_ref: "ray.ObjectRef",
        episodes: List[pa.Table],
        rows_per_batch: int,
        per_task_row_limit: Optional[int] = None,
    ) -> None:
        # ``roots`` (slim per-root constants) is the driver-side list used to
        # compute this task's BlockMetadata; ``roots_ref`` carries it to workers,
        # where ``_read`` fetches it once. ``episodes`` is the driver-side list of
        # projected episode tables, used here only to cut each segment's slice --
        # it is NOT stored on the task; only the per-segment slice is shipped.
        total_rows = 0
        size_bytes = 0
        all_input_files: List[str] = []
        resolved: List[tuple] = []
        for root_idx, start, end in segments:
            root = roots[root_idx]
            eps = episodes[root_idx]
            start_ep, end_ep = _LeRobotReadTask._episodes_for_row_range(eps, start, end)
            # combine_chunks() materializes a standalone copy of just this slice,
            # so pyarrow ships only it -- not a view into the whole table buffer.
            ep_slice = eps.slice(start_ep, end_ep - start_ep).combine_chunks()
            parquet_segs, video_segs = _LeRobotReadTask._resolve_paths(root, ep_slice)
            all_input_files.extend(parquet_segs)
            all_input_files.extend(video_segs)
            total_rows += end - start
            size_bytes += (end - start) * root.row_size_bytes
            resolved.append((root_idx, start, end, parquet_segs, ep_slice))

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
        for root_idx, start, end, parquet_segs, ep_slice in self._segments_resolved:
            yield from self._read_segment(
                roots[root_idx], start, end, root_idx, parquet_segs, ep_slice
            )

    def _read_segment(
        self,
        root: _LeRobotRoot,
        start: int,
        end: int,
        dataset_index: int,
        parquet_segs: List[str],
        ep_slice: pa.Table,
    ) -> Iterator[pa.Table]:
        """Stream decoded rows for one ``[start, end)`` range within a single root.

        Reads the segment's parquet rows, then emits ``self._rows_per_batch``-row
        Arrow batches, decoding each batch's camera frames on demand. Video
        decoders are opened once per segment and reused across batches (a
        per-segment cache), so peak memory is bounded to one batch's frames
        rather than the whole segment's. Image cameras (encoded bytes in the
        parquet) are decoded per batch directly.
        """
        fs = root.fs

        # Read all parquet rows for this segment (predicate pushdown on index).
        filters = [("index", ">=", start), ("index", "<", end)]
        pq_tables = []
        for path in parquet_segs:
            with fs.open(path, "rb") as f:
                pq_tables.append(pq.read_table(f, filters=filters))
        full = pa.concat_tables(pq_tables) if pq_tables else None
        if full is None or full.num_rows == 0:
            return
        n_rows = full.num_rows
        camera_keys = root.video_keys + root.image_keys

        # Per-segment video state: an episode->file lookup scanned once, and a
        # decoder cache reused across batches so each video file is opened once.
        video_meta = (
            self._video_episode_meta(ep_slice, root.video_keys)
            if root.video_keys
            else {}
        )
        tolerance_s = (
            root.frame_tolerance_s
            if root.frame_tolerance_s is not None
            else 0.5 / float(root.fps)
        )
        cache = new_decoder_cache(root.storage_options) if root.video_keys else None

        task_idx_pylist = full.column("task_index").to_pylist()
        # Fail fast (once per segment) if the parquet data references a task id
        # that the tasks metadata (meta/tasks.parquet) does not define, rather
        # than raising a bare KeyError mid-stream.
        missing_tasks = {ti for ti in task_idx_pylist if ti not in root.tasks_dict}
        if missing_tasks:
            raise ValueError(
                f"task_index values {sorted(missing_tasks)} are absent from the "
                f"dataset's tasks metadata (meta/tasks.parquet); the data and "
                f"tasks metadata are inconsistent."
            )
        try:
            for batch_start in range(0, n_rows, self._rows_per_batch):
                batch_end = min(batch_start + self._rows_per_batch, n_rows)
                batch = full.slice(batch_start, batch_end - batch_start)
                frame_buffers: dict = {}
                if root.video_keys:
                    frame_buffers.update(
                        self._decode_video_batch(
                            root, batch, video_meta, tolerance_s, cache
                        )
                    )
                if root.image_keys:
                    frame_buffers.update(self._decode_image_frames(root, batch))
                task_list = [
                    root.tasks_dict[task_idx_pylist[i]]
                    for i in range(batch_start, batch_end)
                ]
                yield self._build_batch(
                    camera_keys,
                    [batch],
                    frame_buffers,
                    task_list,
                    dataset_index,
                    root.stats_json,
                )
        finally:
            if cache is not None:
                cache.clear()

    @staticmethod
    def _video_episode_meta(episodes: pa.Table, video_keys: List[str]) -> dict:
        """Per-video-camera ``{episode_index: (chunk, file, from_timestamp)}``
        for this task's episode slice -- O(episodes-per-task), not the whole
        dataset -- so per-batch decoding can resolve a row's video file in O(1)."""
        eps = episodes
        ep_idx = eps.column("episode_index").to_pylist()
        meta: dict = {}
        for vk in video_keys:
            chunks = eps.column(f"videos/{vk}/chunk_index").to_pylist()
            files = eps.column(f"videos/{vk}/file_index").to_pylist()
            from_ts = eps.column(f"videos/{vk}/from_timestamp").to_pylist()
            meta[vk] = {
                ep_idx[i]: (chunks[i], files[i], from_ts[i]) for i in range(len(ep_idx))
            }
        return meta

    @staticmethod
    def _decode_video_batch(
        root: _LeRobotRoot,
        batch: pa.Table,
        video_meta: dict,
        tolerance_s: float,
        cache: Any,
    ) -> dict:
        """Decode one batch's video frames to HWC uint8 arrays aligned to the
        batch's row order.

        Groups rows by video file and decodes each file's timestamps from the
        shared (per-segment) decoder *cache*, so a file's decoder is reused
        across batches instead of being reopened. Returns
        ``{video_key: list[np.ndarray HWC uint8]}``.
        """
        # ``video_path`` is guaranteed non-None whenever ``video_keys`` is
        # non-empty (enforced in :func:`_build_root`).
        assert root.video_path is not None
        n = batch.num_rows
        ep_idx_col = batch.column("episode_index").to_pylist()
        ts_col = batch.column("timestamp").to_pylist()
        out: dict = {}
        for vk in root.video_keys:
            ep_info = video_meta[vk]
            file_to_rows: dict = {}
            for r in range(n):
                chunk, fi, from_t = ep_info[ep_idx_col[r]]
                file_to_rows.setdefault((chunk, fi), []).append((r, from_t + ts_col[r]))
            frames_by_row: List[Any] = [None] * n
            for (chunk, fi), rows_and_ts in file_to_rows.items():
                # Full URI (with protocol) so torchcodec detects cloud vs local.
                vpath = (
                    f"{root.root}/"
                    f"{root.video_path.format(video_key=vk, chunk_index=chunk, file_index=fi)}"
                )
                row_indices = [r for r, _ in rows_and_ts]
                timestamps = [t for _, t in rows_and_ts]
                # decode_frames returns a torch.Tensor (N, C, H, W) uint8.
                frames = decode_frames(
                    vpath, timestamps, tolerance_s, decoder_cache=cache
                )
                # .cpu() so GPU-decoded frames (num_gpus>0) move to host before
                # the numpy conversion (CUDA tensors can't convert directly).
                arr = frames.permute(0, 2, 3, 1).contiguous().cpu().numpy()
                if arr.dtype != np.uint8:
                    if arr.dtype.kind == "f":
                        arr = (arr * 255.0).clip(0, 255).astype(np.uint8)
                    else:
                        arr = arr.astype(np.uint8)
                for i, r in enumerate(row_indices):
                    frames_by_row[r] = arr[i]
            out[vk] = frames_by_row
        return out

    @staticmethod
    def _decode_image_frames(root: _LeRobotRoot, full: pa.Table) -> dict:
        """Decode in-parquet image cameras to HWC uint8 frames, one per row.

        LeRobot stores ``dtype == 'image'`` cameras as HuggingFace ``Image``
        structs (``{bytes, path}``) inside the data parquet — so, unlike video,
        there is no separate file or timestamp matching: each row already holds
        its own encoded frame. Returns ``{image_key: list[np.ndarray HWC uint8]}``
        aligned to *full*'s row order.
        """
        import io

        from PIL import Image

        decoded: dict = {}
        for ik in root.image_keys:
            frames: List[Any] = []
            for cell in full.column(ik).to_pylist():
                data = cell.get("bytes") if isinstance(cell, dict) else cell
                if data is None and isinstance(cell, dict) and cell.get("path"):
                    p = cell["path"]
                    p = p if p.startswith(root.fs_root) else f"{root.fs_root}/{p}"
                    with root.fs.open(p, "rb") as fh:
                        data = fh.read()
                if data is None:
                    raise ValueError(
                        f"image column {ik!r}: row has neither inline bytes nor a path"
                    )
                arr = np.asarray(
                    Image.open(io.BytesIO(data)).convert("RGB"), dtype=np.uint8
                )
                frames.append(arr)
            decoded[ik] = frames
        return decoded

    @staticmethod
    def _resolve_paths(root: _LeRobotRoot, ep_slice: pa.Table) -> tuple:
        """Resolve the file paths touched by an episode slice.

        Returns ``(parquet_segs, video_segs)`` — flat lists of unique file
        paths used for the ``BlockMetadata.input_files`` attribution. The
        per-row video paths are re-derived at decode time, so we don't retain
        per-camera grouping here.
        """
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
        camera_keys: List[str],
        pq_buffer: List[pa.Table],
        frame_buffers: dict,
        task_list: List[str],
        dataset_index: int,
        stats_json: str,
    ) -> pa.Table:
        """Assemble one Arrow batch from buffered parquet rows, decoded camera
        frames, tasks, and per-dataset stats.  *camera_keys* covers both video
        and image cameras: image columns already exist in the parquet rows as
        encoded-byte structs and are overwritten in place by their decoded
        tensors, while video columns are added."""
        from ray.data.extensions import ArrowVariableShapedTensorArray

        table = pa.concat_tables(pq_buffer)
        columns: dict = {
            table.schema.field(i).name: table.column(i)
            for i in range(table.num_columns)
        }
        for k in camera_keys:
            columns[k] = ArrowVariableShapedTensorArray.from_numpy(frame_buffers[k])
        # Dictionary-encode the per-dataset-constant string columns so the
        # (multi-KB) stats JSON and the task label are stored once per block
        # rather than copied onto every row.
        columns["task"] = pa.array(task_list, type=pa.string()).dictionary_encode()
        columns["dataset_index"] = pa.array(
            [dataset_index] * len(task_list), type=pa.int32()
        )
        columns["stats"] = pa.array(
            [stats_json] * len(task_list), type=pa.string()
        ).dictionary_encode()
        return pa.table(columns)


@PublicAPI(stability="alpha")
class LeRobotDatasource(Datasource):
    """Ray Data ``Datasource`` for LeRobot v3 datasets.

    Reads LeRobot v3 datasets from local or cloud storage, combining the chunked
    parquet rows with decoded camera frames. Cameras may be stored as mp4 video
    (``dtype: video``, decoded via torchcodec) or as encoded images inside the
    parquet rows (``dtype: image``, decoded via Pillow); both are emitted as HWC
    uint8 tensor columns.

    Use :func:`ray.data.read_lerobot` for typical use. Construct this class
    directly when you need to inspect ``source.metas`` (a list of upstream
    :class:`lerobot.LeRobotDatasetMetadata` instances) before reading, or
    when passing Ray Data execution options to ``ray.data.read_datasource``.

    Examples:
        Basic usage:

        >>> import ray  # doctest: +SKIP
        >>> ds = ray.data.read_lerobot(  # doctest: +SKIP
        ...     "s3://anonymous@ray-example-data/lerobot/libero-mini",
        ... )

        One read task per episode:

        >>> source = LeRobotDatasource(  # doctest: +SKIP
        ...     "s3://anonymous@ray-example-data/lerobot/libero-mini",
        ...     group_by_episode=True,
        ... )
        >>> ds = ray.data.read_datasource(source)  # doctest: +SKIP
    """

    # (importable_module_name, pip_install_string).  lerobot (the `dataset`
    # extra pulls torch/datasets) and fsspec are always needed.  The video deps
    # are required only for datasets with mp4 cameras: torchcodec so cloud-URI
    # videos stream via HTTP-range reads (its decode path is fsspec-aware), and
    # av for its container support.  Pillow is required only for datasets whose
    # cameras are stored as in-parquet encoded images.  The conditional sets are
    # checked after metadata load, so an image-only dataset needn't install
    # torchcodec/av (and vice versa).
    _BASE_DEPENDENCIES = [
        ("fsspec", "fsspec"),
        ("lerobot.datasets.dataset_metadata", "lerobot[dataset]"),
    ]
    _VIDEO_DEPENDENCIES = [("torchcodec", "torchcodec"), ("av", "av")]
    _IMAGE_DEPENDENCIES = [("PIL", "pillow")]

    def __init__(
        self,
        root: Union[str, Path, List[Union[str, Path]]],
        *,
        group_by_episode: bool = False,
        filesystem: Optional[
            "pyarrow.fs.FileSystem | fsspec.AbstractFileSystem"
        ] = None,
        storage_options: Optional[Dict[str, Any]] = None,
        frame_tolerance_s: Optional[float] = None,
    ):
        """Initialize LeRobot datasource.

        Args:
            root: Path or URI to the dataset root (local, ``gs://``, ``s3://``),
                or a list of such paths to read multiple datasets as one.
                All roots must share the same camera keys (``video_keys`` and
                ``image_keys``), ``fps``, and non-camera feature names.
            group_by_episode: How to group rows into read tasks. ``False`` (the
                default) emits one task per video-file group (each mp4 opened
                once per task); ``True`` emits one task per episode.
                ``override_num_blocks`` then splits or merges these into the
                requested number of output blocks.
            filesystem: Filesystem for reading metadata + parquet. A pyarrow
                ``FileSystem`` (wrapped internally with
                :class:`~fsspec.implementations.arrow.ArrowFSWrapper`) or an
                fsspec ``AbstractFileSystem``. When omitted, it is selected from
                the URI scheme — including the ``s3://anonymous@bucket/…``
                convention for public buckets. Applied to every root.
            storage_options: Extra options forwarded to ``fsspec`` (e.g.
                credentials or a custom ``endpoint_url``). When *filesystem* is
                omitted these also select the metadata/parquet filesystem; they
                always supply credentials for the by-URI video decode path
                (lerobot opens video files itself via ``fsspec``). Applied to
                every root. ``s3://anonymous@…`` roots thread ``anon=True`` in
                automatically. When omitted, ambient fsspec resolution is used.
            frame_tolerance_s: Max seconds a decoded video frame's timestamp may
                differ from a row's timestamp before it is rejected. ``None``
                (the default) uses ``0.5 / fps`` — half a frame interval, e.g.
                ~0.05s at 10fps. Increase to tolerate timestamp jitter; decrease
                for stricter alignment.
        Raises:
            ValueError: If ``frame_tolerance_s`` is non-positive, or if roots
                have incompatible schemas.
        """
        super().__init__()

        for module, package in self._BASE_DEPENDENCIES:
            _check_import(self, module=module, package=package)

        self._filesystem = filesystem
        self._storage_options: Dict[str, Any] = dict(storage_options or {})

        if frame_tolerance_s is not None and frame_tolerance_s <= 0:
            raise ValueError(
                f"frame_tolerance_s must be a positive number of seconds, "
                f"got {frame_tolerance_s!r}."
            )
        self._frame_tolerance_s: Optional[float] = frame_tolerance_s

        roots = [root] if isinstance(root, (str, Path)) else list(root)
        # A ``local://`` root means the files live only on the driver's node, so
        # pin reads there rather than distributing them (the same convention as
        # FileBasedDatasource). Bare paths are assumed shared/distributed.
        self._supports_distributed_reads = not _is_local_scheme(roots)
        # Resolve the metadata/parquet filesystem + the by-URI video options
        # once per root on the driver: (fs, fs_root, video_root_uri, video_opts).
        self._resolved = [
            _resolve_filesystem(r, filesystem, self._storage_options) for r in roots
        ]
        # Pristine upstream metadata instances, exposed via self.metas /
        # self.meta for callers that want full lerobot API access.
        self.metas = [
            _load_lerobot_metadata(r, fs, fs_root)
            for r, (fs, fs_root, _, _, _) in zip(roots, self._resolved)
        ]

        # The video/image deps are needed only for the camera kinds actually
        # present, so we check them now that metadata is loaded.
        if any(m.video_keys for m in self.metas):
            for module, package in self._VIDEO_DEPENDENCIES:
                _check_import(self, module=module, package=package)
        if any(getattr(m, "image_keys", None) for m in self.metas):
            for module, package in self._IMAGE_DEPENDENCIES:
                _check_import(self, module=module, package=package)

        if len(self.metas) > 1:
            ref = self.metas[0]
            for m in self.metas[1:]:
                if sorted(m.video_keys) != sorted(ref.video_keys):
                    raise ValueError(
                        f"video_keys mismatch: {ref.root!r} has "
                        f"{ref.video_keys} but {m.root!r} has {m.video_keys}"
                    )
                if sorted(getattr(m, "image_keys", []) or []) != sorted(
                    getattr(ref, "image_keys", []) or []
                ):
                    raise ValueError(
                        f"image_keys mismatch: {ref.root!r} has "
                        f"{ref.image_keys} but {m.root!r} has {m.image_keys}"
                    )
                if m.fps != ref.fps:
                    raise ValueError(
                        f"fps mismatch: {ref.root!r} has {ref.fps} "
                        f"but {m.root!r} has {m.fps}"
                    )
                ref_feats = {
                    k
                    for k, v in ref.features.items()
                    if v.get("dtype") not in ("video", "image") and k != "task"
                }
                m_feats = {
                    k
                    for k, v in m.features.items()
                    if v.get("dtype") not in ("video", "image") and k != "task"
                }
                if ref_feats != m_feats:
                    raise ValueError(
                        f"Feature mismatch: {ref.root!r} has "
                        f"{sorted(ref_feats)} but {m.root!r} has "
                        f"{sorted(m_feats)}"
                    )

        # Derived state, computed once on the driver. The slim roots are
        # shipped to workers via ray.put; the projected episode tables stay
        # here for planning (slicing) -- each read task embeds only its own
        # episode slice rather than broadcasting the whole table.
        built = [
            _build_root(
                m,
                r,
                fs,
                fs_root,
                video_uri,
                video_opts,
                video_creds_unavailable=creds_unavail,
                frame_tolerance_s=self._frame_tolerance_s,
            )
            for m, r, (fs, fs_root, video_uri, video_opts, creds_unavail) in zip(
                self.metas, roots, self._resolved
            )
        ]
        self._roots: List[_LeRobotRoot] = [root for root, _ in built]
        self._episodes: List[pa.Table] = [episodes for _, episodes in built]

        self._group_by_episode: bool = group_by_episode

    @property
    def meta(self) -> "LeRobotDatasetMetadata":
        """First-root upstream :class:`lerobot.LeRobotDatasetMetadata`."""
        return self.metas[0]

    # ------------------------------------------------------------------
    # Slicing helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _slices_by_episode(episodes: pa.Table, video_keys: List[str]) -> List[tuple]:
        from_indices = episodes.column("_global_from_index").to_pylist()
        to_indices = episodes.column("_global_to_index").to_pylist()
        return list(zip(from_indices, to_indices))

    @staticmethod
    def _slices_by_file_group(episodes: pa.Table, video_keys: List[str]) -> List[tuple]:
        eps = episodes

        key_columns: List[list] = []
        if video_keys:
            for vk in video_keys:
                key_columns.append(eps.column(f"videos/{vk}/chunk_index").to_pylist())
                key_columns.append(eps.column(f"videos/{vk}/file_index").to_pylist())
        else:
            # No video files (image dataset): group by the data parquet file.
            key_columns.append(eps.column("data/chunk_index").to_pylist())
            key_columns.append(eps.column("data/file_index").to_pylist())

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
                        f"row {from_idx}. Use group_by_episode=True for "
                        "datasets with non-standard episode layouts."
                    )
                ranges[key] = (prev_from, to_idx)
            else:
                ranges[key] = (from_idx, to_idx)

        return list(ranges.values())

    def _slice(self) -> List[tuple]:
        """Return ``(root_index, start, end)`` triples for all roots, sorted."""
        slice_fn = (
            self._slices_by_episode
            if self._group_by_episode
            else self._slices_by_file_group
        )
        all_ranges: List[tuple] = []
        for root_idx, ds_root in enumerate(self._roots):
            ranges = sorted(slice_fn(self._episodes[root_idx], ds_root.video_keys))
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

    @staticmethod
    def _split_ranges(row_ranges: List[tuple], target: int) -> List[tuple]:
        """Split contiguous ``(root_idx, start, end)`` ranges into ``~target``
        sub-ranges, distributing splits proportionally to row count, so
        ``override_num_blocks`` can request more tasks than the base partitioning
        yields. Each sub-range stays within one base range (and hence one root).
        Splitting a video-file group means its files are reopened per sub-task —
        the parallelism-vs-reopen trade-off."""
        total = sum(e - s for _, s, e in row_ranges)
        if total <= 0 or target <= len(row_ranges):
            return list(row_ranges)
        out: List[tuple] = []
        remaining_target = target
        remaining_total = total
        ranges = list(row_ranges)
        for idx, (ri, s, e) in enumerate(ranges):
            n = e - s
            ranges_after = len(ranges) - idx - 1
            k = (
                max(1, round(n * remaining_target / remaining_total))
                if remaining_total
                else 1
            )
            # Leave >=1 task for each remaining range; never exceed this range's
            # row count.
            k = max(1, min(k, n, remaining_target - ranges_after))
            step, rem = divmod(n, k)
            pos = s
            for j in range(k):
                sz = step + (1 if j < rem else 0)
                out.append((ri, pos, pos + sz))
                pos += sz
            remaining_target -= k
            remaining_total -= n
        return out

    # ------------------------------------------------------------------
    # Ray Data API
    # ------------------------------------------------------------------

    def estimate_inmemory_data_size(self) -> Optional[int]:
        return sum(r.total_frames * r.row_size_bytes for r in self._roots) or None

    def get_read_tasks(
        self,
        parallelism: int,
        per_task_row_limit: Optional[int] = None,
        data_context: Optional[DataContext] = None,
    ) -> List[ReadTask]:
        row_ranges = self._slice()

        groups: List[list]
        if parallelism > 0 and parallelism > len(row_ranges):
            # More tasks requested than the partitioning yields: split ranges
            # into sub-ranges so override_num_blocks is honored (e.g. a single
            # monolithic-mp4 dataset can still be parallelized). Splitting a
            # file group re-opens its files per sub-task — the cost of trading
            # amortized opens for parallelism.
            groups = [[r] for r in self._split_ranges(row_ranges, parallelism)]
        elif parallelism > 0 and len(row_ranges) > parallelism:
            n = len(row_ranges)
            base, remainder = divmod(n, parallelism)
            groups = []
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
            len(self._roots[0].video_keys) + len(self._roots[0].image_keys),
        )

        roots_ref = ray.put(self._roots)
        rows_per_batch = self._rows_per_batch(data_context)
        return [
            _LeRobotReadTask(
                segments=entry["segments"],
                roots=self._roots,
                roots_ref=roots_ref,
                episodes=self._episodes,
                rows_per_batch=rows_per_batch,
                per_task_row_limit=per_task_row_limit,
            )
            for entry in task_plan
        ]

    def get_name(self) -> str:
        return "LeRobot"

    @property
    def supports_distributed_reads(self) -> bool:
        return self._supports_distributed_reads
