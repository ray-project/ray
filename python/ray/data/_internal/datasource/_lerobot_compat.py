"""Compatibility shim over lerobot's torchcodec video decoding.

The datasource decodes frames by calling lerobot's *public*
``decode_video_frames_torchcodec`` directly — no monkeypatching. It streams both
local paths and fsspec cloud URIs via torchcodec (HTTP-range reads, never a full
download).

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

REMOVAL: once ``lerobot`` is pinned to a release where
``_native_storage_options()`` returns True (the warning fires), delete this
module and decode directly via
``decode_video_frames_torchcodec(..., storage_options=...)``.
"""

import functools
import warnings
from typing import Any, Dict, List, Optional

# Set once the "lerobot now supports storage_options natively" warning has been
# emitted, so it fires at most once per process.
_WARNED_NATIVE_AVAILABLE = False


@functools.lru_cache(maxsize=1)
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
    """Emit a one-time ``RuntimeWarning`` when lerobot has gained native
    ``storage_options`` support, signalling that this shim is obsolete.

    We intentionally do not switch to the native path automatically: a silent
    switch would bypass the bundled workaround and let it linger unmaintained.
    Surfacing it as a warning prompts its removal from Ray Data instead.
    """
    global _WARNED_NATIVE_AVAILABLE
    if _WARNED_NATIVE_AVAILABLE or not _native_storage_options():
        return
    _WARNED_NATIVE_AVAILABLE = True
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


@functools.lru_cache(maxsize=1)
def _creds_cache_cls():
    """A ``VideoDecoderCache`` subclass that opens videos through ``fsspec`` with
    explicit ``storage_options``.

    Defined lazily so importing this module never requires lerobot (workers only
    import it inside the read task).
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

    return _CredsVideoDecoderCache


def decode_frames(
    video_path: str,
    timestamps: List[float],
    tolerance_s: float,
    storage_options: Optional[Dict[str, Any]] = None,
):
    """Decode the frames nearest *timestamps* (within *tolerance_s*) from
    *video_path* via torchcodec.

    Works for local paths and any fsspec URI; *storage_options* supplies cloud
    credentials. Returns a ``torch.Tensor`` of shape ``(N, C, H, W)``.
    """
    from lerobot.datasets.video_utils import decode_video_frames_torchcodec

    _warn_if_native_available()
    storage_options = dict(storage_options or {})
    if not storage_options:
        # No explicit credentials: lerobot's default decoder cache (ambient
        # fsspec resolution) is exactly what we want.
        return decode_video_frames_torchcodec(video_path, timestamps, tolerance_s)
    # Explicit credentials: stream via a credentialed decoder cache, cleared
    # afterwards to close the fsspec handle (we decode a whole file per call, so
    # there is nothing to reuse across calls).
    cache = _creds_cache_cls()(storage_options)
    try:
        return decode_video_frames_torchcodec(
            video_path, timestamps, tolerance_s, decoder_cache=cache
        )
    finally:
        cache.clear()
