"""Monkey-patches for the ``lerobot`` library to support fsspec cloud URIs.

Two upstream APIs are patched:

1. ``lerobot.datasets.dataset_metadata.LeRobotDatasetMetadata`` only loads
   from local ``pathlib.Path`` roots, with a HuggingFace Hub download
   fallback when files are missing.  These patches let it accept
   fsspec-compatible cloud URIs (``s3://``, ``gs://``, ``abfs://``, ...) as
   ``root``, routing metadata reads through fsspec instead.

2. ``lerobot.datasets.video_utils.decode_video_frames`` streams cloud-URI
   videos via torchcodec (``fsspec.open`` + ``torchcodec.VideoDecoder``,
   doing HTTP-range reads — never a full download), but its default decoder
   cache opens files without ``storage_options``.  We wrap it to route cloud
   URIs through torchcodec with a credentialed decoder cache so explicit
   ``storage_options`` reach ``fsspec.open`` while still streaming.

Apply via :func:`apply_lerobot_fsspec_patches`.  Idempotent and safe to call
multiple times.

Targets ``lerobot >= 0.5.0``.  Newer minor/major releases emit a warning the
first time the patch is applied.

Upstream tracking — https://github.com/huggingface/lerobot/pull/3669 adds
native fsspec-root + ``storage_options`` support to lerobot.  When it merges
and we can pin ``lerobot`` to a release containing it:

- Metadata (patch #1) becomes unnecessary and can be removed: the PR loads
  remote metadata directly via fsspec, so we'd just construct
  ``LeRobotDatasetMetadata(repo_id, root, storage_options=...)``.

- Video (patch #2) uses the *same streaming* approach as this patch — the PR
  threads ``storage_options`` into ``get_decoder``'s ``fsspec.open`` and feeds
  the handle to ``torchcodec.VideoDecoder`` (HTTP-range reads, no download).
  Note the PR adds ``storage_options`` only to ``decode_video_frames_torchcodec``
  / ``get_decoder``, *not* to the ``decode_video_frames`` dispatcher we wrap.
  So rather than dropping it outright, we'd replace the monkeypatch + custom
  decoder cache with a direct call to
  ``decode_video_frames_torchcodec(..., storage_options=...)``.
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
    - ``lerobot.datasets.video_utils.decode_video_frames`` — stream cloud
      URIs as ``video_path`` via torchcodec, injecting ``storage_options``
      into ``fsspec.open`` through a credentialed decoder cache.

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
        storage_options=None,
    ) -> None:
        # Captured for the fsspec reads regardless of branch; only the cloud
        # branch actually uses it.  Not forwarded to the upstream __init__,
        # which (on supported versions) has no storage_options parameter.
        self._storage_options = dict(storage_options or {})
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

    class _CredsVideoDecoderCache(_vu.VideoDecoderCache):
        """A :class:`lerobot.datasets.video_utils.VideoDecoderCache` that opens
        videos through ``fsspec`` with explicit ``storage_options``.

        lerobot's default cache calls ``fsspec.open(video_path)`` with no
        credentials.  This subclass injects *storage_options* so cloud videos
        stream (HTTP-range reads via ``torchcodec.VideoDecoder``) using the
        supplied credentials — without ever downloading the file.

        Mirrors the upstream ``get_decoder`` body but threads credentials in.
        The ``storage_options`` keyword is accepted (and overridden by the
        instance's own) so this is call-compatible with both the current
        signature and the future ``get_decoder(video_path, storage_options=)``.
        """

        def __init__(self, storage_options):
            super().__init__()
            self._storage_options = dict(storage_options or {})

        def get_decoder(self, video_path, storage_options=None):
            import fsspec
            from torchcodec.decoders import VideoDecoder

            opts = self._storage_options or dict(storage_options or {})
            video_path = str(video_path)
            with self._lock:
                if video_path not in self._cache:
                    file_handle = fsspec.open(video_path, **opts).__enter__()
                    try:
                        decoder = VideoDecoder(file_handle, seek_mode="approximate")
                    except Exception:
                        file_handle.close()
                        raise
                    self._cache[video_path] = (decoder, file_handle)
                return self._cache[video_path][0]

    def _patched_decode_video_frames(
        video_path, timestamps, tolerance_s, backend=None, storage_options=None
    ):
        """Cloud-URI-aware wrapper around lerobot.video_utils.decode_video_frames.

        Videos are always *streamed*, never downloaded:

        - **Local paths** (any backend) — delegated to lerobot unchanged.
        - **Cloud URIs + torchcodec** (the default, and what the datasource
          requires) — torchcodec streams via ``fsspec.open`` + range reads.
          When ``storage_options`` is empty, lerobot's default path is used
          as-is (ambient credentials, persistent decoder cache).  When
          ``storage_options`` is supplied, we call
          :func:`decode_video_frames_torchcodec` with a credentialed
          :class:`_CredsVideoDecoderCache` so the credentials reach
          ``fsspec.open`` while still streaming.
        - **Cloud URIs + pyav/video_reader** — these backends cannot stream a
          remote URI, and we refuse to download, so this raises.  Unreachable
          in practice: the datasource requires torchcodec.
        """
        if backend is None:
            backend = _vu.get_safe_default_codec()

        storage_options = dict(storage_options or {})
        video_path_str = str(video_path)
        cloud = is_cloud_uri(video_path_str)

        # Local paths: both backends read the file directly — delegate as-is.
        if not cloud:
            return _original_decode_video_frames(
                video_path, timestamps, tolerance_s, backend
            )

        if backend == "torchcodec":
            # No explicit credentials: lerobot's default streaming path (with
            # its persistent decoder cache) is exactly what we want.
            if not storage_options:
                return _original_decode_video_frames(
                    video_path, timestamps, tolerance_s, backend
                )
            # Explicit credentials: stream via torchcodec with a credentialed
            # decoder cache.  Clear it afterwards to close the fsspec handle
            # (the datasource decodes a whole video file in one call, so there
            # is nothing to reuse across calls).
            cache = _CredsVideoDecoderCache(storage_options)
            try:
                return _vu.decode_video_frames_torchcodec(
                    video_path_str, timestamps, tolerance_s, decoder_cache=cache
                )
            finally:
                cache.clear()

        # pyav / video_reader cannot stream a cloud URI and we will not
        # download.  Only reachable on platforms without a torchcodec wheel;
        # the datasource declares torchcodec as a hard dependency.
        raise NotImplementedError(
            f"Streaming cloud video {video_path_str!r} requires the torchcodec "
            f"backend, but the active backend is {backend!r}. Install torchcodec "
            "(pip install 'lerobot[dataset]') to stream videos from cloud storage."
        )

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

    storage_options = dict(getattr(meta, "_storage_options", None) or {})
    fs, fs_root = fsspec.core.url_to_fs(meta.root, **storage_options)
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
