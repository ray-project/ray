"""Compatibility shim over lerobot's torchcodec video decoding.

The datasource decodes frames by calling lerobot's ``decode_video_frames_torchcodec``.

The only gap in ``lerobot >= 0.5.0`` is that the function's decoder cache opens
files without credentials, so explicit ``storage_options`` never reach
``fsspec.open``. We bridge that by passing a credentialed decoder cache
(:func:`_creds_cache_cls`).

When lerobot threads ``storage_options`` through natively
(huggingface/lerobot#3669) this shim is obsolete. We do not auto-switch to the
native parameter -- that would let dead code linger unnoticed. Instead a
tripwire test (``test_lerobot_compat_shim_still_needed`` in test_lerobot.py)
fails the moment lerobot gains native support, forcing this module's removal.

NOTE(Artur): Once lerobot threads ``storage_options`` natively, remove this.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List, Optional

if TYPE_CHECKING:
    import torch


def _creds_cache_cls():
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
    storage_options = dict(storage_options or {})
    if storage_options:
        return _creds_cache_cls()(storage_options)
    from lerobot.datasets.video_utils import VideoDecoderCache

    return VideoDecoderCache()


def decode_frames(
    video_path: str,
    timestamps: List[float],
    tolerance_s: float,
    decoder_cache: Any,
) -> torch.Tensor:
    from lerobot.datasets.video_utils import decode_video_frames_torchcodec

    return decode_video_frames_torchcodec(
        video_path, timestamps, tolerance_s, decoder_cache=decoder_cache
    )
