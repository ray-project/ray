"""Ray Data datasource for LeRobot Dataset v3."""

from __future__ import annotations

import enum
import functools
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
    Literal,
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
from ray.data._internal.datasource._lerobot_compat import new_decoder_cache
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
    driver (see ``_resolve_filesystem``) and shipped to workers."""

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
    image bytes in the parquet rows, not as mp4)."""

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
    every row as the ``stats`` column"""

    frame_tolerance_s: float
    """Max seconds a decoded video frame's timestamp may differ from a row's
    timestamp before it is rejected (passed to lerobot's ``decode_video_frames``).
    Resolved at construction: the caller's value, or ``0.5 / fps`` when unset."""

    delta_steps: Dict[str, List[int]]
    """``delta_timestamps`` resolved to integer frame offsets per feature key
    (``round(offset * fps)``). Empty means no temporal windows are requested.
    When non-empty, each listed feature is gathered over these offsets -- clamped
    to the anchor frame's episode -- into a leading time dimension, and a companion
    boolean ``{key}_is_pad`` column flags the offsets that fell outside the episode."""


class _ReadGranularity(str, enum.Enum):
    """How rows are grouped into base read tasks: one task per physical file
    (video-file group) or per episode. ``override_num_blocks`` splits/merges
    from whichever base this selects (see ``LeRobotDatasource._slice``)."""

    FILE = "file"
    EPISODE = "episode"


def _delta_tensor_type(data_type: pa.DataType) -> pa.ExtensionType:
    """Build the output tensor type for a windowed tabular Arrow field.

    LeRobot stores vectors as list columns and higher-rank features either as
    nested lists or Hugging Face ``ArrayXD`` extension types backed by nested
    lists. A temporal window adds one leading dimension to the stored feature.
    """
    from ray.data.extensions import ArrowVariableShapedTensorType

    # BaseExtensionType covers canonical (C++-defined) extension types like
    # pa.fixed_shape_tensor too, not just Python-defined ones like HF ArrayXD.
    if isinstance(data_type, pa.BaseExtensionType):
        data_type = data_type.storage_type

    ndim = 1
    while (
        pa.types.is_fixed_size_list(data_type)
        or pa.types.is_list(data_type)
        or pa.types.is_large_list(data_type)
    ):
        ndim += 1
        data_type = data_type.value_type

    return ArrowVariableShapedTensorType(data_type, ndim=ndim)


def _nested_list_column_to_numpy(column: pa.Array, name: str) -> np.ndarray:
    """Materialize a uniformly shaped nested-list column as a typed ndarray."""
    shape = [len(column)]
    values = column
    while (
        pa.types.is_fixed_size_list(values.type)
        or pa.types.is_list(values.type)
        or pa.types.is_large_list(values.type)
    ):
        if values.null_count:
            raise ValueError(
                f"Windowed LeRobot feature {name!r} cannot contain null lists."
            )
        if pa.types.is_fixed_size_list(values.type):
            length = values.type.list_size
        else:
            # Keep the common, uniformly shaped path inside Arrow. Converting
            # every row's length to Python is prohibitively expensive for the
            # large action/state columns found in real LeRobot datasets.
            lengths = pc.unique(pc.list_value_length(values))
            if len(lengths) != 1:
                raise ValueError(
                    f"Windowed LeRobot feature {name!r} must have one uniform "
                    f"shape, but found list lengths {sorted(lengths.to_pylist())}."
                )
            length = lengths[0].as_py()
        shape.append(length)
        values = values.flatten()

    return values.to_numpy(zero_copy_only=False).reshape(shape)


# ---------------------------------------------------------------------------
# Driver-side derived-state builders.
# ---------------------------------------------------------------------------


def _build_schema(
    episodes_table: pa.Table,
    data_path: str,
    video_keys: List[str],
    image_keys: List[str],
    fs: "fsspec.AbstractFileSystem",
    fs_root: str,
    delta_steps: Optional[Dict[str, List[int]]] = None,
) -> pa.Schema:
    """Read the Arrow schema of the first data parquet file and produce the
    output schema: in-parquet ``image`` columns (HF ``struct<bytes, path>``) are
    replaced by decoded uint8 tensor columns, ``video`` columns are appended as
    decoded tensors, plus ``task``, ``dataset_index`` and ``stats``.

    When ``delta_steps`` is given, each listed feature gains a leading time
    dimension (cameras become 4-D ``(T, H, W, C)`` tensors, tabular features gain
    one axis) and a boolean ``{key}_is_pad`` tensor column is appended for it."""

    # Note(Artur): Imported here rather than at module top so importing this module stays
    # docs-build-safe: an eager ``ray.data.extensions`` import pulls in pandas' tensor
    # extension, whose class body breaks under the docs build's mocked pandas.
    from ray.data.extensions import ArrowVariableShapedTensorType

    delta = delta_steps or {}
    ep = episodes_table.slice(0, 1).to_pylist()[0]
    path = (
        f"{fs_root}/"
        f"{data_path.format(chunk_index=ep['data/chunk_index'], file_index=ep['data/file_index'])}"
    )
    with fs.open(path, "rb") as f:
        pq_schema = pq.read_schema(f)
    image_set = set(image_keys)
    frame_type = ArrowVariableShapedTensorType(pa.uint8(), ndim=3)
    delta_frame_type = ArrowVariableShapedTensorType(pa.uint8(), ndim=4)

    def _field_for(f: pa.Field) -> pa.Field:
        if f.name in image_set:
            # Encoded-byte image struct -> decoded uint8 tensor (4-D if windowed).
            return pa.field(f.name, delta_frame_type if f.name in delta else frame_type)
        if f.name in delta:
            # A temporal window adds one leading dimension to the stored
            # scalar/vector/matrix/... feature.
            return pa.field(f.name, _delta_tensor_type(f.type))
        return f

    # Image columns live in the parquet as encoded-byte structs; swap them for
    # the decoded-tensor type in place (preserving column order).
    fields = [_field_for(f) for f in pq_schema]
    # Video columns are not in the parquet; append them.
    for vk in video_keys:
        fields.append(pa.field(vk, delta_frame_type if vk in delta else frame_type))
    # task + stats are per-dataset constants repeated on every row, so they are
    # dictionary-encoded (one shared value per block + an int32 index per row)
    # instead of duplicating the (multi-KB) stats JSON on each row.
    dict_str = pa.dictionary(pa.int32(), pa.string())
    fields.append(pa.field("task", dict_str))
    fields.append(pa.field("dataset_index", pa.int32()))
    fields.append(pa.field("stats", dict_str))
    # Per-delta-key pad masks, appended last in a fixed order (parquet columns in
    # file order, then video keys) so the schema matches the emitted block layout.
    if delta:
        pad_type = ArrowVariableShapedTensorType(pa.bool_(), ndim=1)
        for f in pq_schema:
            if f.name in delta:
                fields.append(pa.field(f"{f.name}_is_pad", pad_type))
        for vk in video_keys:
            if vk in delta:
                fields.append(pa.field(f"{vk}_is_pad", pad_type))
    return pa.schema(fields)


def _estimated_row_size_bytes(features: dict) -> int:
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
                    "Could not estimate size for feature %r; skipping it in the "
                    "row-size estimate.",
                    feat_name,
                )
                continue
    # Output rows also carry task + dataset_index + stats columns. task and
    # stats are dictionary-encoded (the shared value lives once per block), so
    # the per-row cost is three int32s (two dictionary indices + dataset_index).
    total += 3 * 4
    return total


def _stats_to_json(stats: Optional[dict]) -> str:
    """Serialize lerobot's per-feature stats dict to a JSON string."""
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


def _non_camera_features(features: dict) -> Dict[str, tuple]:
    """Map each non-camera feature to its ``(dtype, shape)``.

    Used for the cross-root compatibility check: camera features
    (``video`` / ``image``) are compared separately via ``video_keys`` /
    ``image_keys``, and comparing dtype and shape here -- not just the feature
    names -- ensures roots read together produce the same output schema.
    Roots that agree on names but differ in dtype or shape would otherwise
    yield schema-inconsistent blocks."""
    return {
        k: (v.get("dtype"), tuple(v.get("shape") or ()))
        for k, v in features.items()
        if v.get("dtype") not in ("video", "image")
    }


def _delta_targets(
    global_index, episode_from, episode_to, steps
) -> Tuple[np.ndarray, np.ndarray]:
    """Pick the neighbouring frames at the given ``steps`` offsets, staying inside
    the anchor frame's episode.

    For an anchor at global frame ``global_index`` whose episode spans
    ``[episode_from, episode_to)``, each offset ``s`` points at frame
    ``global_index + s``. Offsets that land outside the episode are clamped back
    to the nearest in-episode frame and flagged in the returned ``pad`` mask.

    Example: anchor 14 in episode ``[10, 15)`` with ``steps=[-1, 0, 1]`` gives raw
    targets ``[13, 14, 15]``; 15 is past the episode end, so it returns targets
    ``[13, 14, 14]`` with pad ``[False, False, True]``.

    Vectorized: ``global_index`` / ``episode_from`` / ``episode_to`` may be scalars
    (``(S,)`` result) or ``(A,)`` arrays (``(A, S)`` result). Mirrors lerobot's
    ``DatasetReader._get_query_indices`` clamp/pad rule, reimplemented here because
    that method is bound to a torch reader we don't build.
    """
    anchor = np.asarray(global_index)[..., None]
    low = np.asarray(episode_from)[..., None]
    high = np.asarray(episode_to)[..., None]
    target = anchor + np.asarray(steps)
    pad = (target < low) | (target >= high)
    return np.clip(target, low, high - 1), pad


def _resolve_filesystem(
    root: Union[str, Path],
    filesystem: Optional["pyarrow.fs.FileSystem | fsspec.AbstractFileSystem"] = None,
    storage_options: Optional[Dict[str, Any]] = None,
) -> Tuple["fsspec.AbstractFileSystem", str, str, Dict[str, Any]]:
    """Resolve the fsspec filesystem and paths for one LeRobot dataset root.

    The lerobot library uses fsspec under the hood, and fsspec has native
    support for ``hf://`` Hugging Face datasets, so any filesystem is resolved
    via fsspec internally:

    * **metadata + parquet** are read through a single fsspec filesystem,
      returned as ``fs`` with the dataset path ``fs_root`` relative to it;
    * **video files** are streamed by URI through torchcodec -- lerobot opens
      those itself via ``fsspec.open``, not through ``fs`` -- so they get the
      URI ``video_root_uri`` and options ``video_storage_options`` instead.

    The ``s3://anonymous@…`` convention is mapped onto the by-URI video path:
    the marker is stripped from ``video_root_uri`` and ``anon=True`` is threaded
    into ``video_storage_options`` (s3fs spells anonymous ``anon=True``).

    Args:
        root: Path or URI of the dataset root.
        filesystem: Optional pyarrow or fsspec filesystem. A pyarrow filesystem
            cannot expose credentials, so pass ``storage_options`` alongside it
            for credentialed cloud video.
        storage_options: Optional fsspec options (credentials, ``endpoint_url``,
            ...) applied to the resolved filesystem and the by-URI video path.

    Returns:
        A ``(fs, fs_root, video_root_uri, video_storage_options)`` tuple:
        ``fs`` / ``fs_root`` resolve metadata + parquet I/O; ``video_root_uri``
        / ``video_storage_options`` feed the by-URI video decode path (which
        torchcodec opens directly via ``fsspec``, not through ``fs``).
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
            # A pyarrow filesystem does not expose its credentials, so it can't
            # supply them to the by-URI video path; video credentials come from
            # storage_options instead (see the docstring).
        else:
            raise TypeError(
                f"filesystem must be a pyarrow.fs.FileSystem or an "
                f"fsspec.spec.AbstractFileSystem, got {type(filesystem).__name__}"
            )
        # Derive fs_root from the anonymous@-stripped URI (video_root_uri), not
        # root_uri: otherwise an explicit filesystem opens metadata/parquet at
        # "anonymous@bucket/..." instead of the real bucket path. Matches the
        # storage_options and default branches, which already strip the marker.
        _, fs_root = split_protocol(video_root_uri)
        fs_root = (fs_root or video_root_uri).rstrip("/")
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

    return fs, fs_root, video_root_uri, video_storage_options


def _load_lerobot_metadata(
    root: Union[str, Path],
    fs: "fsspec.AbstractFileSystem",
    fs_root: str,
) -> "LeRobotDatasetMetadata":
    """Construct a LeRobotDatasetMetadata for a root."""
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
    frame_tolerance_s: Optional[float] = None,
    delta_timestamps: Optional[Dict[str, List[float]]] = None,
    *,
    delta_tolerance_s: float,
) -> Tuple[_LeRobotRoot, pa.Table]:
    """Compute the per-root derived state bundle for a lerobot
    ``LeRobotDatasetMetadata`` instance.

    This is a lighter representation of the metadata that is distributed to workers.

    Args:
        meta: Upstream lerobot metadata for one dataset root. Not mutated.
        root: The original dataset location, where the data and video files
            live. For a remote root ``meta.root`` points at a local temp copy of
            ``meta/`` only, so data and video paths are resolved against
            ``root``, not ``meta.root``.
        fs: Resolved fsspec filesystem for metadata and parquet I/O; captured on
            the returned ``_LeRobotRoot`` so workers reuse it.
        fs_root: Dataset path relative to ``fs``'s root, for path joining.
        video_root_uri: Root URI for the by-URI video decode path (videos are
            streamed through torchcodec/fsspec, not through ``fs``).
        video_storage_options: fsspec options for that by-URI video path.
        frame_tolerance_s: Max seconds a decoded frame's timestamp may differ
            from a row's before the frame is rejected; ``None`` uses the
            ``0.5 / fps`` default.
        delta_timestamps: Optional ``{feature_key: [offsets_in_seconds]}``
            temporal-window request, validated against ``meta.features`` and
            converted to per-key integer frame steps via ``meta.fps``; ``None``
            disables windowing.
        delta_tolerance_s: Frame-grid tolerance (seconds) for the
            ``delta_timestamps`` offset check; forwarded to lerobot's
            ``check_delta_timestamps``.

    Returns:
        A ``(root_bundle, episodes_table)`` tuple: the ``_LeRobotRoot``
        worker-shipped state, and the per-root episodes table (augmented with
        ``_global_from_index`` / ``_global_to_index``) kept on the driver for
        planning.
    """
    root_uri = str(root).rstrip("/")
    fs_root = fs_root.rstrip("/")

    # meta.video_path raises KeyError when the dataset has no video; read the
    # info dict directly so a missing key resolves to None instead.
    video_path = meta.info.get("video_path")
    if meta.video_keys and not video_path:
        raise ValueError(
            f"{root_uri!r}: dataset has video keys {meta.video_keys} "
            "but meta/info.json has no 'video_path' template"
        )
    # Episode metadata as an Arrow table, with _global_from/to_index giving each
    # episode's [from, to) span in the global frame index. lerobot v3 records this
    # authoritatively as dataset_from_index / dataset_to_index -- the same running
    # counter the data `index` column is derived from.
    episodes_table = meta.episodes.with_format("arrow")[:]
    episodes_table = episodes_table.append_column(
        "_global_from_index",
        episodes_table.column("dataset_from_index").cast(pa.int64()),
    ).append_column(
        "_global_to_index",
        episodes_table.column("dataset_to_index").cast(pa.int64()),
    )
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
    if delta_timestamps:
        # Validate offsets align to the frame grid and convert seconds -> integer
        # frame offsets; both delegated to lerobot so semantics match LeRobotDataset.
        from lerobot.datasets.feature_utils import (
            check_delta_timestamps,
            get_delta_indices,
        )

        check_delta_timestamps(delta_timestamps, meta.fps, delta_tolerance_s)
        delta_steps = get_delta_indices(delta_timestamps, meta.fps)
    else:
        delta_steps = {}
    schema = _build_schema(
        episodes_table,
        meta.data_path,
        meta.video_keys,
        meta.image_keys,
        fs,
        fs_root,
        delta_steps=delta_steps,
    )
    row_size_bytes = _estimated_row_size_bytes(meta.features)
    stats_json = _stats_to_json(meta.stats)
    # Resolve the video-frame timestamp tolerance now (needs fps): the caller's
    # value, or half a frame interval (0.5 / fps) when unset.
    resolved_frame_tolerance_s = (
        frame_tolerance_s if frame_tolerance_s is not None else 0.5 / float(meta.fps)
    )

    root_bundle = _LeRobotRoot(
        root=video_root_uri,
        fs=fs,
        fs_root=fs_root,
        data_path=meta.data_path,
        video_path=video_path,
        video_keys=list(meta.video_keys),
        image_keys=list(meta.image_keys),
        tasks_dict=tasks_dict,
        schema=schema,
        row_size_bytes=row_size_bytes,
        total_frames=meta.total_frames,
        fps=meta.fps,
        storage_options=video_storage_options,
        stats_json=stats_json,
        frame_tolerance_s=resolved_frame_tolerance_s,
        delta_steps=delta_steps,
    )
    return root_bundle, episodes_table


def _resolve_root(
    root: Union[str, Path],
    filesystem: Optional["pyarrow.fs.FileSystem | fsspec.AbstractFileSystem"],
    storage_options: Dict[str, Any],
    frame_tolerance_s: Optional[float],
    delta_timestamps: Optional[Dict[str, List[float]]] = None,
    *,
    delta_tolerance_s: float,
) -> Tuple[_LeRobotRoot, pa.Table, "LeRobotDatasetMetadata"]:
    """Resolve one root into its read-task bundle + per-episode table.

    Runs the single-root pipeline: resolve the filesystem
    (``_resolve_filesystem``), load LeRobot metadata
    (``_load_lerobot_metadata``), and distill both (``_build_root``) into
    a slim ``_LeRobotRoot`` (shipped to read tasks) and a per-episode Arrow
    table (used for slicing). The upstream metadata is returned too, so the
    caller can run cross-root homogeneity checks and expose it via
    :attr:`LeRobotDatasource.meta`.
    """
    fs, fs_root, video_uri, video_opts = _resolve_filesystem(
        root, filesystem, storage_options
    )
    meta = _load_lerobot_metadata(root, fs, fs_root)
    built_root, built_episodes = _build_root(
        meta,
        root,
        fs,
        fs_root,
        video_uri,
        video_opts,
        frame_tolerance_s=frame_tolerance_s,
        delta_timestamps=delta_timestamps,
        delta_tolerance_s=delta_tolerance_s,
    )
    return built_root, built_episodes, meta


def _build_lerobot_read_task(
    segments: List[tuple],
    roots: List[_LeRobotRoot],
    roots_ref: "ray.ObjectRef",
    episodes: List[pa.Table],
    max_block_bytes: int,
    per_task_row_limit: Optional[int] = None,
) -> ReadTask:
    """Plan one read task on the driver and wrap it as a parameterized read
    function.

    Each ``segment`` is a ``(root_index, start, end)`` triple over a contiguous
    row range within one root. ``roots`` (slim per-root constants) is used here
    to compute BlockMetadata; ``roots_ref`` carries it to workers, where the read
    function fetches it once. ``episodes`` is the driver-side list of projected
    episode tables, used here only to cut each segment's slice -- it is NOT
    shipped; only the per-segment slice travels with the task.
    """
    total_rows = 0
    size_bytes = 0
    all_input_files: List[str] = []
    resolved: List[tuple] = []
    for root_idx, start, end in segments:
        root = roots[root_idx]
        eps = episodes[root_idx]
        start_ep, end_ep = _episodes_for_row_range(eps, start, end)
        # combine_chunks() materializes a standalone copy of just this slice,
        # so pyarrow ships only it -- not a view into the whole table buffer.
        ep_slice = eps.slice(start_ep, end_ep - start_ep).combine_chunks()
        parquet_segs, video_segs = _resolve_paths(root, ep_slice)
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
    read_fn = functools.partial(
        _read_lerobot_task, roots_ref, resolved, max_block_bytes
    )
    return ReadTask(read_fn, block_metadata, schema, per_task_row_limit)


def _read_lerobot_task(
    roots_ref: "ray.ObjectRef",
    segments_resolved: List[tuple],
    max_block_bytes: int,
) -> Iterator[pa.Table]:
    """Stream decoded rows as Arrow tables, iterating over all segments.

    Runs on a worker: fetches the shared slim per-root state once, then reads
    each pre-resolved segment.
    """
    roots: List[_LeRobotRoot] = ray.get(roots_ref)
    for root_idx, start, end, parquet_segs, ep_slice in segments_resolved:
        yield from _read_lerobot_segment(
            roots[root_idx],
            start,
            end,
            root_idx,
            parquet_segs,
            ep_slice,
            max_block_bytes,
        )


class _DeltaSegment(NamedTuple):
    """Per-segment state for the delta (temporal-window) read path, built once by
    ``_prepare_delta_segment`` and reused across the segment's batches."""

    segment_start: int
    """Global frame index of the segment's first row. Convert a clamped global
    index to its 0-based position in this segment's arrays via
    ``index - segment_start`` (valid because a segment is a contiguous run of
    whole episodes)."""

    global_idx: List[int]
    """Per-row global frame index (the parquet ``index`` column), one entry per
    segment row, in row order."""

    episode_index_col: List[int]
    """Per-row episode index (the ``episode_index`` column); maps a row to its
    episode when routing a video frame to its ``(file, timestamp)``."""

    row_from: List[int]
    """Per-row inclusive lower bound of the row's episode, as a global frame index.
    Windows are clamped to ``[row_from, row_to)`` so they never cross into a
    neighbouring episode."""

    row_to: List[int]
    """Per-row exclusive upper bound of the row's episode, as a global frame index
    (see ``row_from``)."""

    pad_order: List[str]
    """Windowed feature keys in the fixed order their ``{key}_is_pad`` mask
    columns are appended to the block -- parquet columns (file order) then video
    keys -- matching ``_build_schema``."""

    tabular_base: Dict[str, np.ndarray]
    """Each windowed tabular/scalar feature materialized once over the whole
    segment as ``(n_seg, *shape)``, dtype preserved; the per-batch gather
    fancy-indexes it with the clamped ``(A, S)`` positions."""

    segment_images: Dict[str, np.ndarray]
    """Each windowed in-parquet image camera decoded once for the whole segment
    and stacked to ``(n_seg, H, W, C)``; gathered like ``tabular_base``.
    Whole-segment (not per-batch) because a window near a batch edge reaches
    adjacent rows."""


def _read_lerobot_segment(
    root: _LeRobotRoot,
    start: int,
    end: int,
    dataset_index: int,
    parquet_segs: List[str],
    ep_slice: pa.Table,
    max_block_bytes: int,
) -> Iterator[pa.Table]:
    """Stream decoded rows for one ``[start, end)`` range within a single root.

    Reads the segment's parquet rows once, then emits Arrow batches sized from
    this root's estimated row size (so each batch targets one output block),
    decoding each batch's camera frames on demand via ``_build_batch``.
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

    task_idx_pylist = full.column("task_index").to_pylist()
    missing_tasks = {ti for ti in task_idx_pylist if ti not in root.tasks_dict}
    if missing_tasks:
        raise ValueError(
            f"task_index values {sorted(missing_tasks)} are absent from the "
            f"dataset's tasks metadata (meta/tasks.parquet); the data and "
            f"tasks metadata are inconsistent."
        )

    if root.video_keys:
        video_meta = _video_episode_meta(ep_slice, root.video_keys)
        cache = new_decoder_cache(root.storage_options)
        # Video-decoder timestamp tolerance, resolved at construction.
        tolerance_s = root.frame_tolerance_s
    else:
        video_meta = {}
        cache = None
        tolerance_s = None

    if root.delta_steps:
        # Delta reads gather a window per feature; build the per-segment state once
        # and divide the batch by the widest window so blocks stay near target size.
        delta_segment = _prepare_delta_segment(root, full, ep_slice)
        widest_window = max(
            (len(steps) for steps in root.delta_steps.values()), default=1
        )
        rows_per_batch = max(
            1, (max_block_bytes // root.row_size_bytes) // widest_window
        )
    else:
        delta_segment = None
        rows_per_batch = max(1, (max_block_bytes // root.row_size_bytes))

    try:
        for batch_start in range(0, n_rows, rows_per_batch):
            batch_end = min(batch_start + rows_per_batch, n_rows)
            batch = full.slice(batch_start, batch_end - batch_start)
            task_list = [
                root.tasks_dict[task_idx_pylist[i]]
                for i in range(batch_start, batch_end)
            ]
            if root.delta_steps:
                yield _build_delta_batch(
                    root,
                    batch,
                    batch_start,
                    dataset_index,
                    task_list,
                    video_meta,
                    tolerance_s,
                    cache,
                    delta_segment,
                )
            else:
                frame_buffers = {
                    **_decode_video_batch(root, batch, video_meta, tolerance_s, cache),
                    **_decode_image_frames(root, batch),
                }
                yield _build_batch(
                    camera_keys,
                    batch,
                    frame_buffers,
                    task_list,
                    dataset_index,
                    root.stats_json,
                )
    finally:
        if cache is not None:
            cache.clear()


def _prepare_delta_segment(
    root: _LeRobotRoot, full: pa.Table, ep_slice: pa.Table
) -> _DeltaSegment:
    """Build the per-segment state the delta gather reuses across batches:
    per-row episode bounds (for clamping), key routing, and the segment-level
    tabular / decoded-image buffers whose windows may span batch boundaries.

    Validates the invariant the gather relies on: the segment is a contiguous run
    of whole episodes in the global frame index (guaranteed by the
    ``get_read_tasks`` delta guard). That lets a clamped global index map to row
    position ``index - segment_start`` and guarantees every in-episode neighbour
    is loaded; a split episode would silently corrupt windows, so fail loudly.
    """
    global_idx = full.column("index").to_pylist()
    episode_index_col = full.column("episode_index").to_pylist()
    from_by_episode = dict(
        zip(
            ep_slice.column("episode_index").to_pylist(),
            ep_slice.column("_global_from_index").to_pylist(),
        )
    )
    to_by_episode = dict(
        zip(
            ep_slice.column("episode_index").to_pylist(),
            ep_slice.column("_global_to_index").to_pylist(),
        )
    )
    row_from = [from_by_episode[episode] for episode in episode_index_col]
    row_to = [to_by_episode[episode] for episode in episode_index_col]

    segment_start, segment_end = global_idx[0], global_idx[-1] + 1
    if (
        len(global_idx) != segment_end - segment_start
        or min(row_from) < segment_start
        or max(row_to) > segment_end
    ):
        raise RuntimeError(
            "delta_timestamps requires contiguous whole-episode read segments, "
            "but this segment splits an episode -- an internal invariant "
            "violation (get_read_tasks caps parallelism when delta_timestamps "
            "is set)."
        )

    delta_steps = root.delta_steps
    image_set = set(root.image_keys)
    delta_image_keys = [ik for ik in root.image_keys if ik in delta_steps]
    delta_tabular_keys = [
        name
        for name in full.column_names
        if name in delta_steps and name not in image_set
    ]
    # Pad-mask columns are appended last, in the schema's fixed order: parquet
    # columns (in file order), then video keys.
    pad_order = [name for name in full.column_names if name in delta_steps] + [
        vk for vk in root.video_keys if vk in delta_steps
    ]
    # Materialize tabular delta columns to numpy once, preserving the stored dtype
    # so stacked windows match the schema: nested-list columns (fixed or variable)
    # materialize to (n_rows, *shape) via the value array -- which keeps e.g.
    # float32 that to_pylist would widen to float64 -- and scalar columns stay 1-D.
    tabular_base: Dict[str, np.ndarray] = {}
    for name in delta_tabular_keys:
        col = full.column(name).combine_chunks()
        if isinstance(col.type, pa.BaseExtensionType):
            # In lerobot, multidimensional columns can be backed by Hugging Face ArrayXD.
            # Hugging Face ArrayXD is a pa.ExtensionType
            # Unwrap it so the one validated path below
            # materializes every encoding
            col = col.storage
        if (
            pa.types.is_fixed_size_list(col.type)
            or pa.types.is_list(col.type)
            or pa.types.is_large_list(col.type)
        ):
            tabular_base[name] = _nested_list_column_to_numpy(col, name)
        else:
            tabular_base[name] = col.to_numpy(zero_copy_only=False)
    return _DeltaSegment(
        segment_start=segment_start,
        global_idx=global_idx,
        episode_index_col=episode_index_col,
        row_from=row_from,
        row_to=row_to,
        pad_order=pad_order,
        tabular_base=tabular_base,
        # Stack each decoded image camera to one (n_seg, H, W, C) array so a batch
        # can gather its windows by fancy-indexing with the (A, S) positions.
        segment_images={
            image_key: np.stack(frames)
            for image_key, frames in _decode_image_frames(
                root, full, keys=delta_image_keys
            ).items()
        },
    )


def _build_delta_batch(
    root: _LeRobotRoot,
    batch: pa.Table,
    batch_start: int,
    dataset_index: int,
    task_list: List[str],
    video_meta: dict,
    tolerance_s: Optional[float],
    cache: Any,
    delta_segment: _DeltaSegment,
) -> pa.Table:
    """Assemble one temporal-window output batch.

    Each feature in ``root.delta_steps`` is gathered over its offsets -- clamped
    to the anchor's episode -- into a leading time dimension, with a boolean
    ``{key}_is_pad`` mask; non-windowed columns pass through unchanged. Anchors
    are ``batch``'s rows, at absolute segment positions
    ``[batch_start, batch_start + len(batch))``. Column order matches
    ``_build_schema``: parquet columns, then video keys, then
    ``task`` / ``dataset_index`` / ``stats``, then the pad masks.
    """
    from ray.data.extensions import ArrowVariableShapedTensorArray

    delta_steps = root.delta_steps
    image_set = set(root.image_keys)
    # Route cameras into windowed vs. one-frame-per-row.
    delta_video_keys = [vk for vk in root.video_keys if vk in delta_steps]
    nondelta_video_keys = [vk for vk in root.video_keys if vk not in delta_steps]
    nondelta_image_keys = [ik for ik in root.image_keys if ik not in delta_steps]
    anchors = range(batch_start, batch_start + batch.num_rows)
    pad_buffers: dict = {}

    # Per-anchor clamped target positions + pad mask, computed once per distinct
    # offset list and shared across keys that use it.
    targets_cache: Dict[tuple, tuple] = {}

    def _targets_for(steps):
        cached = targets_cache.get(steps)
        if cached is None:
            batch_end = batch_start + batch.num_rows
            clamped, pad = _delta_targets(
                np.asarray(delta_segment.global_idx[batch_start:batch_end]),
                np.asarray(delta_segment.row_from[batch_start:batch_end]),
                np.asarray(delta_segment.row_to[batch_start:batch_end]),
                steps,
            )
            # (A, S) row positions + pad mask; a clamped global index is at row
            # position ``index - segment_start`` (the segment is contiguous).
            cached = targets_cache[steps] = (
                clamped - delta_segment.segment_start,
                pad,
            )
        return cached

    # Decode all cameras up front, then assemble columns in schema order.
    nondelta_video_frames = _decode_video_batch(
        root, batch, video_meta, tolerance_s, cache, keys=nondelta_video_keys
    )
    nondelta_image_frames = _decode_image_frames(root, batch, keys=nondelta_image_keys)
    delta_video_frames: dict = {}
    for vk in delta_video_keys:
        frames, pads = _decode_video_delta(
            root,
            anchors,
            delta_segment.global_idx,
            delta_segment.row_from,
            delta_segment.row_to,
            delta_segment.episode_index_col,
            video_meta,
            vk,
            delta_steps[vk],
            tolerance_s,
            cache,
        )
        delta_video_frames[vk] = frames
        pad_buffers[vk] = pads

    columns: dict = {}
    # 1. Parquet columns, in file order: windowed image/tabular gathered from the
    #    per-segment buffers, everything else passed through.
    for name in batch.column_names:
        if name in image_set:
            if name in delta_steps:
                positions, pad = _targets_for(tuple(delta_steps[name]))
                # segment_images[name]: (n_seg, H, W, C); [positions] -> (A, S, H, W, C).
                columns[name] = ArrowVariableShapedTensorArray.from_numpy(
                    list(delta_segment.segment_images[name][positions])
                )
                pad_buffers[name] = list(pad)
            else:
                columns[name] = ArrowVariableShapedTensorArray.from_numpy(
                    nondelta_image_frames[name]
                )
        elif name in delta_steps:
            positions, pad = _targets_for(tuple(delta_steps[name]))
            # tabular_base[name]: (n_seg, *shape); [positions] -> (A, S, *shape).
            columns[name] = ArrowVariableShapedTensorArray.from_numpy(
                list(delta_segment.tabular_base[name][positions])
            )
            pad_buffers[name] = list(pad)
        else:
            columns[name] = batch.column(name)

    # 2. Video columns (not in the parquet), in root.video_keys order.
    for vk in root.video_keys:
        video_frames = (
            delta_video_frames[vk] if vk in delta_steps else nondelta_video_frames[vk]
        )
        columns[vk] = ArrowVariableShapedTensorArray.from_numpy(video_frames)

    # 3. Per-dataset constants.
    columns["task"] = pa.array(task_list, type=pa.string()).dictionary_encode()
    columns["dataset_index"] = pa.array(
        [dataset_index] * len(task_list), type=pa.int32()
    )
    columns["stats"] = pa.array(
        [root.stats_json] * len(task_list), type=pa.string()
    ).dictionary_encode()

    # 4. Pad masks, in the schema's fixed order.
    for name in delta_segment.pad_order:
        columns[f"{name}_is_pad"] = ArrowVariableShapedTensorArray.from_numpy(
            pad_buffers[name]
        )

    return pa.table(columns)


def _decode_frames_by_file(
    root: _LeRobotRoot,
    vk: str,
    file_requests: Dict[tuple, List[tuple]],
    tolerance_s: float,
    cache: Any,
) -> dict:
    """Decode video frames for one camera, grouped by physical file.

    ``file_requests`` maps ``(chunk_index, file_index)`` to
    ``[(slot, timestamp), ...]``; each file's timestamps are decoded once through
    the shared per-segment ``cache`` (a file's decoder is reused, not reopened).
    Returns ``{slot: HWC uint8 frame}`` -- ``slot`` is an opaque key (a row index,
    or an ``(anchor, step)`` pair) so both the per-row and windowed callers can
    reshape the result."""
    # Imported here, not at module top, so ``import ray.data`` stays lerobot-free
    # until video is actually decoded on a worker.
    from lerobot.datasets.video_utils import decode_video_frames_torchcodec

    assert root.video_path is not None
    frames_by_slot: dict = {}
    for (chunk, fi), requests in file_requests.items():
        # Full URI (with protocol) so torchcodec detects cloud vs local.
        vpath = (
            f"{root.root}/"
            f"{root.video_path.format(video_key=vk, chunk_index=chunk, file_index=fi)}"
        )
        # lerobot returns a torch.Tensor (N, C, H, W) normalized to float32 in
        # [0, 1]; rescale to uint8. .cpu() first -- a CUDA tensor can't convert
        # directly (decode is CPU-only today, so this is a no-op guard).
        frames = decode_video_frames_torchcodec(
            vpath,
            [timestamp for _, timestamp in requests],
            tolerance_s,
            decoder_cache=cache,
        )
        arr = frames.permute(0, 2, 3, 1).contiguous().cpu().numpy()
        if arr.dtype != np.uint8:
            if arr.dtype.kind == "f":
                # Undo lerobot's /255; round so pixels survive the round-trip.
                arr = (arr * 255.0).round().clip(0, 255).astype(np.uint8)
            else:
                arr = arr.astype(np.uint8)
        for i, (slot, _timestamp) in enumerate(requests):
            frames_by_slot[slot] = arr[i]
    return frames_by_slot


def _decode_video_delta(
    root: _LeRobotRoot,
    anchors: List[int],
    global_idx: List[int],
    row_from: List[int],
    row_to: List[int],
    episode_index_col: List[int],
    video_meta: dict,
    vk: str,
    steps: List[int],
    tolerance_s: float,
    cache: Any,
) -> Tuple[List[np.ndarray], List[np.ndarray]]:
    """Decode a temporal window of video frames per anchor for one video key.

    Resolves each anchor's ``steps`` offsets to in-episode timestamps (clamped to
    the episode), decodes them via ``_decode_frames_by_file``, then stacks the
    window per anchor. Returns ``([per-anchor (T, H, W, C) uint8], [per-anchor
    (T,) bool pad])``.
    """
    fps = float(root.fps)
    ep_info = video_meta[vk]
    n_steps = len(steps)

    pads: List[np.ndarray] = []
    # (chunk, fi) -> [((anchor, step), timestamp), ...]
    file_requests: Dict[tuple, list] = {}
    for anchor_idx, position in enumerate(anchors):
        episode_start = row_from[position]
        chunk, fi, from_ts = ep_info[episode_index_col[position]]
        targets, pad = _delta_targets(
            global_idx[position], episode_start, row_to[position], steps
        )
        for step_idx, target in enumerate(targets):
            # int(target) keeps the timestamp a Python float so lerobot's
            # torch.tensor(timestamps) is float32 and matches the decoder's
            # loaded_ts (cdist needs one dtype).
            file_requests.setdefault((chunk, fi), []).append(
                (
                    (anchor_idx, step_idx),
                    from_ts + (int(target) - episode_start) / fps,
                )
            )
        pads.append(np.asarray(pad, dtype=bool))

    frames = _decode_frames_by_file(root, vk, file_requests, tolerance_s, cache)
    stacked = [
        np.stack(
            [frames[(anchor_idx, step_idx)] for step_idx in range(n_steps)], axis=0
        )
        for anchor_idx in range(len(anchors))
    ]
    return stacked, pads


def _video_episode_meta(episodes: pa.Table, video_keys: List[str]) -> dict:
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


def _decode_video_batch(
    root: _LeRobotRoot,
    batch: pa.Table,
    video_meta: dict,
    tolerance_s: float,
    cache: Any,
    keys: Optional[List[str]] = None,
) -> dict:
    """Decode one batch's video frames to HWC uint8 arrays, one per row, aligned
    to the batch's row order. Returns ``{video_key: list[np.ndarray HWC uint8]}``.
    ``keys`` restricts decoding to a subset of ``root.video_keys`` (the delta path
    passes its non-windowed keys); an empty key set returns ``{}``."""
    keys = root.video_keys if keys is None else keys
    if not keys:
        return {}
    n = batch.num_rows
    ep_idx_col = batch.column("episode_index").to_pylist()
    ts_col = batch.column("timestamp").to_pylist()
    out: dict = {}
    for vk in keys:
        ep_info = video_meta[vk]
        file_requests: Dict[tuple, list] = {}  # (chunk, fi) -> [(row, ts), ...]
        for r in range(n):
            chunk, fi, from_t = ep_info[ep_idx_col[r]]
            file_requests.setdefault((chunk, fi), []).append((r, from_t + ts_col[r]))
        frames = _decode_frames_by_file(root, vk, file_requests, tolerance_s, cache)
        out[vk] = [frames[r] for r in range(n)]
    return out


def _decode_image_frames(
    root: _LeRobotRoot, full: pa.Table, keys: Optional[List[str]] = None
) -> dict:
    """Decode in-parquet image cameras to HWC uint8 frames, one per row.

    LeRobot stores ``dtype == 'image'`` cameras as HuggingFace ``Image``
    structs (``{bytes, path}``) inside the data parquet — so, unlike video,
    there is no separate file or timestamp matching: each row already holds
    its own encoded frame. Returns ``{image_key: list[np.ndarray HWC uint8]}``
    aligned to ``full``'s row order. ``keys`` restricts decoding to a subset of
    ``root.image_keys`` (used by the delta path to skip windowed keys); an empty
    key set returns ``{}`` without importing image libraries.
    """
    keys = root.image_keys if keys is None else keys
    if not keys:
        return {}

    import io

    from PIL import Image

    decoded: dict = {}
    for ik in keys:
        frames: List[Any] = []
        for cell in full.column(ik).to_pylist():
            data = cell.get("bytes") if isinstance(cell, dict) else cell
            if data is None and isinstance(cell, dict) and cell.get("path"):
                # Derived from how lerobot stores images
                # There are two ways: inline bytes or a path to the image file.
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


def _resolve_paths(root: _LeRobotRoot, ep_slice: pa.Table) -> tuple:
    """Resolve the unique file paths touched by an episode slice.

    Returns ``(parquet_segs, video_segs)`` — the unique parquet + video files
    used for the ``BlockMetadata.input_files`` attribution. The per-row video
    paths are re-derived at decode time, so we don't retain per-camera
    grouping here.
    """

    def _unique_files(chunk_col: str, file_col: str) -> set:
        return set(
            zip(
                ep_slice.column(chunk_col).to_pylist(),
                ep_slice.column(file_col).to_pylist(),
            )
        )

    parquet_segs: List[str] = [
        f"{root.fs_root}/{root.data_path.format(chunk_index=c, file_index=f)}"
        for c, f in sorted(_unique_files("data/chunk_index", "data/file_index"))
    ]

    video_segs: List[str] = []
    if root.video_keys:
        assert root.video_path is not None
        for k in root.video_keys:
            video_segs.extend(
                f"{root.fs_root}/"
                f"{root.video_path.format(video_key=k, chunk_index=c, file_index=f)}"
                for c, f in sorted(
                    _unique_files(f"videos/{k}/chunk_index", f"videos/{k}/file_index")
                )
            )

    return parquet_segs, video_segs


def _episodes_for_row_range(
    episodes: pa.Table,
    start_row: int,
    end_row: int,
) -> tuple:
    """Return the half-open ``(start, end)`` *row positions* in the episodes
    table covering ``[start_row, end_row)`` of the global frame index.

    These are episodes-table row positions (used by ``episodes.slice``), NOT
    ``episode_index`` values -- the two are equal only for the usual
    ``0..N-1`` numbering, so deriving the slice from positions keeps it
    correct for any ``episode_index`` numbering.
    """
    from_idx = episodes.column("_global_from_index")
    to_idx = episodes.column("_global_to_index")
    mask = pc.and_(
        pc.less(from_idx, end_row),
        pc.greater(to_idx, start_row),
    )
    # Row positions where the mask is true (NOT episode_index values).
    positions = pc.indices_nonzero(mask).to_pylist()
    if not positions:
        raise ValueError(
            f"No episodes overlap the row range [{start_row}, {end_row}). "
            f"Dataset has "
            f"{episodes.column('_global_to_index')[-1].as_py()} total frames "
            f"across {len(episodes)} episodes."
        )
    return (positions[0], positions[-1] + 1)


def _build_batch(
    camera_keys: List[str],
    table: pa.Table,
    frame_buffers: dict,
    task_list: List[str],
    dataset_index: int,
    stats_json: str,
) -> pa.Table:
    """Assemble one Arrow batch from a parquet-row table, decoded camera
    frames, tasks, and per-dataset stats.  ``camera_keys`` covers both video
    and image cameras: image columns already exist in the parquet rows as
    encoded-byte structs and are overwritten in place by their decoded
    tensors, while video columns are added."""
    from ray.data.extensions import ArrowVariableShapedTensorArray

    columns: dict = {
        table.schema.field(i).name: table.column(i) for i in range(table.num_columns)
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
    """Ray Data ``Datasource`` for LeRobot v3 datasets."""

    def __init__(
        self,
        root: Union[str, Path, List[Union[str, Path]]],
        *,
        episodes: Optional[List[int]] = None,
        read_granularity: Literal["file", "episode"] = "file",
        filesystem: Optional[
            "pyarrow.fs.FileSystem | fsspec.AbstractFileSystem"
        ] = None,
        storage_options: Optional[Dict[str, Any]] = None,
        frame_tolerance_s: Optional[float] = None,
        delta_timestamps: Optional[Dict[str, List[float]]] = None,
        delta_tolerance_s: float = 1e-4,
    ):
        """Initialize LeRobot datasource.

        Args:
            root: Path or URI to the dataset root (local, ``gs://``, ``s3://``),
                or a list of such paths to read multiple datasets as one.
                All roots must share the same camera keys (``video_keys`` and
                ``image_keys``), ``fps``, and non-camera feature names.
            episodes: If given, read only these ``episode_index`` values (a
                read-time pushdown -- other episodes' parquet rows and video
                files are never opened). Applied per root when reading multiple
                roots. Requesting an ``episode_index`` absent from every root
                raises. ``None`` (the default) reads all episodes.
            read_granularity: How rows are grouped into the base read tasks.
                ``"file"`` (the default) emits one task per video-file group
                (each mp4 opened once per task); ``"episode"`` emits one task
                per episode. ``override_num_blocks`` then splits or merges these
                into the requested number of output blocks.
            filesystem: Filesystem for reading metadata + parquet. A pyarrow
                ``FileSystem`` (wrapped internally with ``ArrowFSWrapper``) or an
                fsspec ``AbstractFileSystem``. When omitted, it is selected from
                the URI scheme — including the ``s3://anonymous@bucket/…``
                convention for public buckets. Applied to every root.
            storage_options: Extra options forwarded to ``fsspec`` (e.g.
                credentials or a custom ``endpoint_url``). When ``filesystem`` is
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
            delta_timestamps: Optional ``{feature_key: [offsets_in_seconds]}``. For
                each frame the listed feature is returned stacked over the offsets
                (a new leading time dimension) plus a boolean ``{key}_is_pad``
                column. Offsets are converted to frame steps via the dataset
                ``fps`` and clamped to the anchor frame's episode; out-of-range
                offsets are flagged in the pad mask. Setting this forces
                **episode-aligned** reads (see :func:`ray.data.read_lerobot`).
            delta_tolerance_s: Frame-grid tolerance (seconds) for
                ``delta_timestamps`` offsets; an offset must be a multiple of
                ``1 / fps`` within this tolerance. Defaults to ``1e-4`` (lerobot's
                ``LeRobotDataset`` default).
        Raises:
            ValueError: If ``frame_tolerance_s`` is non-positive, if a
                ``delta_timestamps`` key is not a feature or an offset does not
                align to the frame grid, or if roots have incompatible schemas.
        """
        super().__init__()

        _check_import(self, module="fsspec", package="fsspec")
        _check_import(
            self, module="lerobot.datasets.dataset_metadata", package="lerobot[dataset]"
        )

        self._filesystem = filesystem
        self._storage_options: Dict[str, Any] = dict(storage_options or {})

        if frame_tolerance_s is not None and frame_tolerance_s <= 0:
            raise ValueError(
                f"frame_tolerance_s must be a positive number of seconds, "
                f"got {frame_tolerance_s!r}."
            )
        self._frame_tolerance_s: Optional[float] = frame_tolerance_s
        # Validated + converted to per-root integer frame steps in ``_build_root``.
        self._delta_timestamps: Optional[Dict[str, List[float]]] = delta_timestamps
        self._delta_tolerance_s: float = delta_tolerance_s

        try:
            self._read_granularity = _ReadGranularity(read_granularity)
        except ValueError:
            valid = [g.value for g in _ReadGranularity]
            raise ValueError(
                f"read_granularity must be one of {valid}, got "
                f"{read_granularity!r}."
            ) from None

        roots = [root] if isinstance(root, (str, Path)) else list(root)
        self._supports_distributed_reads = not _is_local_scheme(roots)

        # Resolve every root on the driver via ``_resolve_root`` (filesystem ->
        # LeRobot metadata -> slim ``_LeRobotRoot`` bundle + per-episode table).
        # The bundles are ``ray.put`` for the read tasks; the episode tables
        # drive slicing, so each read task embeds only its own episode slice
        # rather than broadcasting the whole table.
        self.distilled_metas: List[_LeRobotRoot] = []
        self._episodes: List[pa.Table] = []
        self.original_metas: List["LeRobotDatasetMetadata"] = []
        for r in roots:
            built_root, built_episodes, meta = _resolve_root(
                r,
                filesystem,
                self._storage_options,
                self._frame_tolerance_s,
                delta_timestamps=self._delta_timestamps,
                delta_tolerance_s=self._delta_tolerance_s,
            )
            self.distilled_metas.append(built_root)
            self._episodes.append(built_episodes)
            self.original_metas.append(meta)

        if any(m.video_keys for m in self.original_metas):
            _check_import(self, module="torchcodec", package="torchcodec")
            _check_import(self, module="av", package="av")
        if any(m.image_keys for m in self.original_metas):
            _check_import(self, module="PIL", package="pillow")

        if len(self.original_metas) > 1:
            ref = self.original_metas[0]
            ref_feats = _non_camera_features(ref.features)
            for m in self.original_metas[1:]:
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
                m_feats = _non_camera_features(m.features)
                if m_feats != ref_feats:
                    raise ValueError(
                        f"Feature mismatch: {ref.root!r} has "
                        f"{sorted(ref_feats.items())} but {m.root!r} has "
                        f"{sorted(m_feats.items())}"
                    )

        if episodes is not None:
            self._apply_episodes_filter(episodes)

    def _apply_episodes_filter(self, episodes: List[int]) -> None:
        """Restrict each root to the requested ``episode_index`` values.

        Filters the per-root episode tables in place -- fewer ranges means fewer
        read tasks and less I/O -- and keeps each root's ``total_frames`` (which
        drives the size estimate) in sync. Raises if an ``episode_index`` is
        absent from every root.
        """
        if len(episodes) == 0:
            raise ValueError(
                "episodes must be a non-empty list of episode_index values, or "
                "None to read every episode."
            )
        requested = sorted({int(e) for e in episodes})
        found: set = set()
        for i, eps in enumerate(self._episodes):
            idx_col = eps.column("episode_index")
            mask = pc.is_in(idx_col, value_set=pa.array(requested, type=idx_col.type))
            kept = eps.filter(mask)
            found.update(kept.column("episode_index").to_pylist())
            self._episodes[i] = kept
            n_frames = int(
                pc.sum(
                    pc.subtract(
                        kept.column("_global_to_index"),
                        kept.column("_global_from_index"),
                    )
                ).as_py()
                or 0
            )
            self.distilled_metas[i] = self.distilled_metas[i]._replace(
                total_frames=n_frames
            )
        missing = sorted(set(requested) - found)
        if missing:
            raise ValueError(
                f"episodes {missing} were not found in any dataset root "
                f"(requested {requested})."
            )

    @property
    def meta(self) -> "LeRobotDatasetMetadata":
        """First-root upstream :class:`lerobot.LeRobotDatasetMetadata`."""
        return self.original_metas[0]

    # ------------------------------------------------------------------
    # Slicing helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _slices_by_episode(episodes: pa.Table) -> List[tuple]:
        from_indices = episodes.column("_global_from_index").to_pylist()
        to_indices = episodes.column("_global_to_index").to_pylist()
        return list(zip(from_indices, to_indices))

    @staticmethod
    def _slices_by_file_group(episodes: pa.Table, video_keys: List[str]) -> List[tuple]:
        """Group episodes into one row range per physical file.

        Episodes whose frames live in the same file are coalesced into a single
        contiguous range, so a read task opens each file once instead of
        re-opening it per episode.

        Args:
            episodes: The projected per-root episodes table -- one row per
                episode, ordered by episode index. Columns read here:
                ``_global_from_index`` / ``_global_to_index`` (the episode's
                ``[from, to)`` span in the root's global frame index), plus a
                per-episode file locator: ``videos/<vk>/chunk_index`` and
                ``videos/<vk>/file_index`` for each video key, or
                ``data/chunk_index`` / ``data/file_index`` when there are no
                videos (image / no-camera datasets keep frames in the data
                parquet).
            video_keys: The root's video camera keys; empty for image-only or
                camera-less datasets, where grouping falls back to the data
                parquet file.

        Returns:
            A list of ``(global_from_index, global_to_index)`` tuples, one per
            contiguous file run (``_slice`` sorts them afterward). Same
            ``(from, to)`` shape that ``_slices_by_episode`` returns one per
            episode -- this just merges the adjacent episodes that share a file.
            Episodes that share a file but are NOT adjacent (an ``episodes``
            subset dropped the episode between them, or a non-standard layout
            interleaves files) are emitted as separate ranges -- the shared file
            is re-opened once per run -- rather than merged into a wrong range.

        Example:
            Episodes spanning ``[0,30) [30,60) [60,90)`` in
            ``videos/cam/file-000.mp4`` and ``[90,120)`` in ``file-001.mp4``
            group to ``[(0, 90), (90, 120)]`` -- two ranges, one per mp4. A
            subset keeping only the first and third episode yields
            ``[(0, 30), (60, 90)]`` (the shared file re-opened per run).
        """
        eps = episodes

        # key_columns holds one per-episode value list per locator field --
        # (chunk_index, file_index) for each video key, or for the data parquet
        # when there are no videos. Episode i's group key is the tuple of these
        # columns at index i, so episodes whose frames share a file share a key.
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

        # Walk episodes in global-index order and extend a run while it stays in
        # the same file AND contiguous; a file change or a gap (a dropped episode
        # from an `episodes` subset, or a non-standard interleaved layout) starts
        # a new run. A file shared by non-adjacent episodes thus yields one range
        # per run rather than a single wrong range spanning the gap.
        order = sorted(range(len(eps)), key=lambda i: from_indices[i])
        ranges: List[tuple] = []
        cur_key = None
        cur_from = cur_to = None
        for i in order:
            key = tuple(col[i] for col in key_columns)
            from_idx, to_idx = from_indices[i], to_indices[i]
            if cur_key == key and from_idx == cur_to:
                cur_to = to_idx
            else:
                if cur_key is not None:
                    ranges.append((cur_from, cur_to))
                cur_key, cur_from, cur_to = key, from_idx, to_idx
        if cur_key is not None:
            ranges.append((cur_from, cur_to))
        return ranges

    def _slice(self) -> List[tuple]:
        """Create ``(root_index, start, end)`` triples for all roots, sorted."""
        all_ranges: List[tuple] = []
        for root_idx, ds_root in enumerate(self.distilled_metas):
            episodes = self._episodes[root_idx]
            if self._read_granularity is _ReadGranularity.EPISODE:
                ranges = self._slices_by_episode(episodes)
            else:  # _ReadGranularity.FILE
                ranges = self._slices_by_file_group(episodes, ds_root.video_keys)
            all_ranges.extend((root_idx, s, e) for s, e in sorted(ranges))
        return all_ranges

    def _max_block_bytes(self, data_context: Optional[DataContext] = None) -> int:
        ctx = data_context or DataContext.get_current()
        return ctx.target_max_block_size

    @staticmethod
    def _merge_segments(group: List[tuple]) -> List[tuple]:
        """Collapse adjacent same-root consecutive segments into wider segments."""
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
        return (
            sum(r.total_frames * r.row_size_bytes for r in self.distilled_metas) or None
        )

    def default_num_blocks(self) -> int:
        """The natural read-task count for the configured ``read_granularity``:
        one per video-file group (``"file"``, the default) or per episode
        (``"episode"``).

        ``read_lerobot`` uses this as the default ``override_num_blocks`` so a
        video read is not over-split by Ray's generic block-count floor -- each
        extra split re-opens a file and re-inits a decoder, a cost a small
        dataset cannot amortize. This mirrors how file-based readers cap at the
        file count; an explicit ``override_num_blocks`` still splits or merges
        from this base."""
        return len(self._slice())

    def get_read_tasks(
        self,
        parallelism: int,
        per_task_row_limit: Optional[int] = None,
        data_context: Optional[DataContext] = None,
    ) -> List[ReadTask]:
        row_ranges = self._slice()

        # delta_timestamps gathers a temporal window per frame, clamped to the
        # anchor's episode. That requires whole-episode read segments, so never
        # split below the base (episode / file-group) ranges.
        if self._delta_timestamps and parallelism > len(row_ranges):
            raise ValueError(
                f"Delta timestamps are enabled, but parallelism ({parallelism}) is greater than the number of row ranges ({len(row_ranges)}). "
                f"Delta timestamps require whole-episode read segments, so parallelism must be less than or equal to the number of row ranges."
            )

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

        task_plan = [self._merge_segments(group) for group in groups]

        roots_ref = ray.put(self.distilled_metas)
        max_block_bytes = self._max_block_bytes(data_context)
        return [
            _build_lerobot_read_task(
                segments,
                self.distilled_metas,
                roots_ref,
                self._episodes,
                max_block_bytes,
                per_task_row_limit,
            )
            for segments in task_plan
        ]

    def get_name(self) -> str:
        return "LeRobot"

    @property
    def supports_distributed_reads(self) -> bool:
        return self._supports_distributed_reads
