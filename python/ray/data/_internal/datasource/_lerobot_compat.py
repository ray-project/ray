"""Compatibility shim over lerobot's torchcodec video decoding.

The datasource decodes frames by calling lerobot's ``decode_video_frames_torchcodec``.

The only gap in ``lerobot >= 0.5.0`` is that the function's decoder cache opens
files without credentials, so explicit ``storage_options`` never reach
``fsspec.open``. We bridge that by passing a credentialed decoder cache
(:func:`_creds_cache_cls`).

When lerobot threads ``storage_options`` through natively
(huggingface/lerobot#3669), we deliberately keep using this shim rather than
silently switching to the native parameter — a silent switch would let this
now-obsolete code linger unnoticed. Instead :func:`decode_frames` emits a
one-time ``RuntimeWarning`` the moment it detects native support, flagging that
the shim is no longer needed.

NOTE(Artur): Once lerobot threads ``storage_options`` natively, we can remove this.
"""

from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Any, Dict, List, Optional

if TYPE_CHECKING:
    import torch


def _native_storage_options() -> bool:
    """True if lerobot's ``decode_video_frames_torchcodec`` accepts
    ``storage_options`` directly (huggingface/lerobot#3669)."""
    import inspect

    from lerobot.datasets.video_utils import decode_video_frames_torchcodec

    return (
        "storage_options"
        in inspect.signature(decode_video_frames_torchcodec).parameters
    )


def _warn_if_native_available() -> None:
    from ray.util import log_once

    # Native support is fixed for the life of the process, so gate on log_once
    # first: the signature-inspecting check then runs at most once, and later
    # calls short-circuit on the spent gate instead of re-inspecting.
    if not (log_once("lerobot_storage_options_native") and _native_storage_options()):
        return
    warnings.warn(
        "lerobot's `decode_video_frames_torchcodec` now accepts `storage_options` "
        "natively (huggingface/lerobot#3669), but ray.data.read_lerobot is still "
        "routing video decoding through its bundled credentialed-decoder-cache "
        "compatibility shim and is not using the native parameter. The shim is no "
        "longer necessary and should be removed from Ray Data: decode directly "
        "via `decode_video_frames_torchcodec(..., storage_options=...)` and delete "
        "`ray.data._internal.datasource._lerobot_compat`.",
        RuntimeWarning,
        stacklevel=3,
    )


def _creds_cache_cls():
    """A ``VideoDecoderCache`` subclass that opens videos through ``fsspec`` with
    explicit ``storage_options``.

    Defined inside a function so importing this module never requires lerobot;
    constructed per credentialed decode (cheap — one class definition).
    """
    from lerobot.datasets.video_utils import VideoDecoderCache

    class _CredsVideoDecoderCache(VideoDecoderCache):
        def __init__(self, storage_options: Optional[Dict[str, Any]]):
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

        def clear(self):
            # The base cache drops its decoder references but does not close the
            # fsspec file handles we opened in get_decoder; close them here to
            # avoid leaking a file descriptor per decoded video file.
            with self._lock:
                for entry in self._cache.values():
                    handle = entry[1] if isinstance(entry, tuple) else None
                    if handle is not None:
                        try:
                            handle.close()
                        except Exception:
                            pass
                self._cache.clear()

    return _CredsVideoDecoderCache


def new_decoder_cache(storage_options: Optional[Dict[str, Any]] = None):
    """A torchcodec decoder cache to reuse across :func:`decode_frames` calls
    within one read task, so each video file is opened once and its decoder
    persists across batches (bounding per-task memory). The caller owns it and
    must call ``.clear()`` when done (which also closes any fsspec handles).

    With credentials this is our handle-closing cache; otherwise lerobot's
    default cache (the same one ``decode_frames`` uses implicitly).
    """
    storage_options = dict(storage_options or {})
    if storage_options:
        return _creds_cache_cls()(storage_options)
    from lerobot.datasets.video_utils import VideoDecoderCache

    return VideoDecoderCache()


def decode_frames(
    video_path: str,
    timestamps: List[float],
    tolerance_s: float,
    storage_options: Optional[Dict[str, Any]] = None,
    decoder_cache: Optional[Any] = None,
) -> torch.Tensor:
    """Decode the frames nearest *timestamps* (within *tolerance_s*) from
    *video_path* via torchcodec.

    Works for local paths and any fsspec URI; *storage_options* supplies cloud
    credentials. Pass a *decoder_cache* (from :func:`new_decoder_cache`) to reuse
    decoders across calls — the caller then owns its lifecycle. Returns a
    ``torch.Tensor`` of shape ``(N, C, H, W)``.
    """
    from lerobot.datasets.video_utils import decode_video_frames_torchcodec

    # We intentionally do not switch to the native path automatically: a silent
    # switch would bypass the bundled workaround and let it linger unmaintained.
    _warn_if_native_available()
    if decoder_cache is not None:
        # Caller-owned cache (credentials, if any, are baked into the cache).
        return decode_video_frames_torchcodec(
            video_path, timestamps, tolerance_s, decoder_cache=decoder_cache
        )
    if not storage_options:
        # No explicit credentials: lerobot's default decoder cache (ambient
        # fsspec resolution) is exactly what we want.
        return decode_video_frames_torchcodec(video_path, timestamps, tolerance_s)
    # Explicit credentials: stream via a credentialed decoder cache
    cache = _creds_cache_cls()(storage_options)
    try:
        return decode_video_frames_torchcodec(
            video_path, timestamps, tolerance_s, decoder_cache=cache
        )
    finally:
        cache.clear()
