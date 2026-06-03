"""Monkey-patches for the ``lerobot`` library to support fsspec cloud URIs.

Two upstream APIs are patched:

1. ``lerobot.datasets.dataset_metadata.LeRobotDatasetMetadata`` only loads
   from local ``pathlib.Path`` roots, with a HuggingFace Hub download
   fallback when files are missing.  These patches let it accept
   fsspec-compatible cloud URIs (``s3://``, ``gs://``, ``abfs://``, ...) as
   ``root``, routing metadata reads through fsspec instead.

2. ``lerobot.datasets.video_utils.decode_video_frames`` accepts only a
   local file path (``torchvision.io.VideoReader`` opens the file directly).
   We wrap it to download cloud-URI videos to a temporary file before
   decoding, then clean up afterwards.

Apply via :func:`apply_lerobot_fsspec_patches`.  Idempotent and safe to call
multiple times.

Targets ``lerobot >= 0.5.0``.  Newer minor/major releases emit a warning the
first time the patch is applied.
"""

import json
import logging
import warnings
from typing import Any

import packaging.version

_LEROBOT_MIN_VERSION = "0.5.0"
_LEROBOT_MAX_TESTED = "0.5.1"

_PATCH_APPLIED = False

logger = logging.getLogger(__name__)


def is_cloud_uri(p: Any) -> bool:
    """True when *p* is an fsspec-style URI (anything containing ``://``).

    Covers ``s3://``, ``gs://``, ``abfs://``, ``hdfs://``, ``http(s)://``,
    ``file://``, ``memory://``, and any other fsspec-registered scheme.
    Plain filesystem paths (``/foo``, ``./bar``, ``C:\\foo``) return False.
    """
    return isinstance(p, str) and "://" in p


def _check_version() -> None:
    """Raise if installed lerobot is too old; warn if newer than tested."""
    import lerobot

    installed = packaging.version.parse(lerobot.__version__)
    minimum = packaging.version.parse(_LEROBOT_MIN_VERSION)
    if installed < minimum:
        raise RuntimeError(
            f"lerobot>={_LEROBOT_MIN_VERSION} is required for "
            f"ray.data.read_lerobot, but {installed} is installed. "
            f"Upgrade with: pip install -U 'lerobot[dataset]'"
        )
    ceiling = packaging.version.parse(_LEROBOT_MAX_TESTED)
    if installed.release[:2] > ceiling.release[:2]:
        warnings.warn(
            f"lerobot=={installed} is newer than the version this fsspec "
            f"patch was last validated against ({_LEROBOT_MAX_TESTED}). "
            f"If you see errors, please file an issue against "
            f"ray.data.read_lerobot.",
            RuntimeWarning,
            stacklevel=3,
        )


def apply_lerobot_fsspec_patches() -> None:
    """Patch lerobot APIs to support fsspec cloud URIs.

    Patches:

    - ``LeRobotDatasetMetadata.__init__`` / ``_load_metadata`` — accept
      cloud URIs as ``root`` and load metadata via fsspec.
    - ``lerobot.datasets.video_utils.decode_video_frames`` — accept cloud
      URIs as ``video_path`` by downloading to a temporary file first.

    No-op after the first successful application.
    """
    global _PATCH_APPLIED
    if _PATCH_APPLIED:
        return

    _check_version()

    from lerobot.datasets import dataset_metadata as _dm
    from lerobot.datasets import video_utils as _vu

    _original_init = _dm.LeRobotDatasetMetadata.__init__
    _original_load_metadata = _dm.LeRobotDatasetMetadata._load_metadata
    _original_decode_video_frames = _vu.decode_video_frames

    def _patched_init(
        self,
        repo_id: str,
        root=None,
        revision=None,
        force_cache_sync: bool = False,
        metadata_buffer_size: int = 10,
    ) -> None:
        if is_cloud_uri(root):
            self.repo_id = repo_id
            self.revision = revision if revision else _dm.CODEBASE_VERSION
            self._requested_root = root
            self.root = root
            self._pq_writer = None
            self.latest_episode = None
            self._metadata_buffer = []
            self._metadata_buffer_size = metadata_buffer_size
            self._finalized = False
            self._load_metadata()
        else:
            _original_init(
                self,
                repo_id,
                root,
                revision,
                force_cache_sync,
                metadata_buffer_size,
            )

    def _patched_load_metadata(self) -> None:
        if is_cloud_uri(getattr(self, "root", None)):
            _load_metadata_fsspec(self)
        else:
            _original_load_metadata(self)

    def _patched_decode_video_frames(video_path, timestamps, tolerance_s, backend=None):
        """Cloud-URI-aware wrapper around lerobot.video_utils.decode_video_frames.

        Backend handling:

        - **torchcodec** (default when installed) — lerobot's torchcodec path
          internally does ``fsspec.open(video_path).__enter__()`` and feeds
          the resulting file-like to ``torchcodec.VideoDecoder``, which
          performs HTTP-range / partial reads instead of downloading the
          whole file.  We pass cloud URIs straight through.
        - **pyav / video_reader** (torchvision-based) — only accepts a local
          file path, so for cloud URIs we download to a temp file first and
          delete it afterwards.

        For local paths, both backends are delegated unchanged.
        """
        if backend is None:
            backend = _vu.get_safe_default_codec()

        video_path_str = str(video_path)

        # torchcodec already speaks fsspec — let it stream.
        if backend == "torchcodec" or not is_cloud_uri(video_path_str):
            return _original_decode_video_frames(
                video_path, timestamps, tolerance_s, backend
            )

        # pyav / video_reader: need a local file.  Download to temp.
        import fsspec
        import os
        import tempfile

        fs, fs_path = fsspec.core.url_to_fs(video_path_str)
        suffix = os.path.splitext(fs_path)[-1] or ".mp4"
        # delete=False because we close the file before passing it to
        # torchvision (which on some platforms can't reopen an open fd).
        with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
            tmp_path = tmp.name
        try:
            fs.get(fs_path, tmp_path)
            return _original_decode_video_frames(
                tmp_path, timestamps, tolerance_s, backend
            )
        finally:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass

    _dm.LeRobotDatasetMetadata.__init__ = _patched_init
    _dm.LeRobotDatasetMetadata._load_metadata = _patched_load_metadata
    _vu.decode_video_frames = _patched_decode_video_frames
    _PATCH_APPLIED = True


def _load_metadata_fsspec(meta) -> None:
    """Read LeRobot metadata from a cloud URI and assign the same typed
    attributes (``info``, ``tasks``, ``subtasks``, ``episodes``, ``stats``)
    as the upstream local-path implementation in lerobot 0.5.x.

    In 0.5.x, ``meta.info`` is a plain ``dict`` (with feature shapes coerced
    from ``list`` to ``tuple``), not a dataclass.
    """
    import fsspec
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
    from datasets import Dataset
    from datasets.table import InMemoryTable

    from lerobot.datasets.dataset_metadata import (
        CODEBASE_VERSION,
        check_version_compatibility,
    )
    from lerobot.datasets.io_utils import cast_stats_to_numpy
    from lerobot.datasets.utils import (
        DEFAULT_SUBTASKS_PATH,
        DEFAULT_TASKS_PATH,
        EPISODES_DIR,
        INFO_PATH,
        STATS_PATH,
    )

    fs, fs_root = fsspec.core.url_to_fs(meta.root)
    fs_root = fs_root.rstrip("/")

    info_path = f"{fs_root}/{INFO_PATH}"
    if not fs.exists(info_path):
        raise FileNotFoundError(
            f"LeRobot dataset metadata missing: {meta.root}/{INFO_PATH}"
        )
    with fs.open(info_path, "r") as f:
        info = json.load(f)
    # Match upstream load_info: coerce feature shapes from list to tuple.
    for ft in info.get("features", {}).values():
        if isinstance(ft.get("shape"), list):
            ft["shape"] = tuple(ft["shape"])
    meta.info = info

    check_version_compatibility(meta.repo_id, meta._version, CODEBASE_VERSION)

    with fs.open(f"{fs_root}/{DEFAULT_TASKS_PATH}", "rb") as f:
        tasks = pd.read_parquet(f)
    tasks.index.name = "task"
    meta.tasks = tasks

    subtasks_path = f"{fs_root}/{DEFAULT_SUBTASKS_PATH}"
    if fs.exists(subtasks_path):
        with fs.open(subtasks_path, "rb") as f:
            meta.subtasks = pd.read_parquet(f)
    else:
        meta.subtasks = None

    ep_pattern = f"{fs_root}/{EPISODES_DIR}/*/*.parquet"
    ep_paths = sorted(fs.glob(ep_pattern))
    if not ep_paths:
        raise FileNotFoundError(
            f"No episode parquet files at {meta.root}/{EPISODES_DIR}/*/*.parquet"
        )
    tables = [pq.read_table(fs.open(p, "rb")) for p in ep_paths]
    table = pa.concat_tables(tables)
    keep = [c for c in table.column_names if not c.startswith("stats/")]
    meta.episodes = Dataset(InMemoryTable(table.select(keep)))

    stats_path = f"{fs_root}/{STATS_PATH}"
    if fs.exists(stats_path):
        with fs.open(stats_path, "r") as f:
            meta.stats = cast_stats_to_numpy(json.load(f))
    else:
        meta.stats = None
