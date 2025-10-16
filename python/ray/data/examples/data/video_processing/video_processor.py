"""Video processing utilities for Ray Data examples.

`VideoProcessor` downloads, decodes, and samples frames from video sources. It is
intended to be composed via Ray Data primitives such as ``map_batches`` and is
kept lightweight so it can serve as a reference implementation for custom
pipelines.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import importlib
import io
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union
from urllib.parse import urlparse

from pydantic import BaseModel

from ray.data.examples.data.video_processing import envs as video_envs
from ray.data.examples.data.video_processing.http_utils import HTTPConnection

try:  # pragma: no cover - availability depends on environment
    import av as _av_mod  # type: ignore
except Exception:  # pragma: no cover
    _av_mod = None  # type: ignore

try:  # pragma: no cover
    from PIL import Image as _PIL_Image  # type: ignore
except Exception:  # pragma: no cover
    _PIL_Image = None  # type: ignore

FrameType = Any  # PIL.Image.Image or numpy.ndarray


def _is_http(url: str) -> bool:
    try:
        scheme = urlparse(url).scheme
        return scheme in ("http", "https")
    except Exception:
        return False


def _is_data_uri(url: str) -> bool:
    return isinstance(url, str) and url.startswith("data:")


def _sha256_16(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:16]


class Sampling(BaseModel):
    """Lightweight sampling configuration for ``VideoProcessor``."""

    fps: Optional[float] = None
    num_frames: Optional[int] = None

    class Config:
        extra = "forbid"


class VideoProcessor:
    """Decode and sample frames from video sources.

    - Uses PyAV for decoding.
    - Network fetch/caching via HTTPConnection.
    - CPU-heavy work done in a thread to avoid blocking the event loop.

    Args:
        sampling: {"fps": k} or {"num_frames": n}. Default fps=3.0.
        cache_dir: Optional directory for disk cache.
        cache_mode: One of "auto", "disk", or "memory".
        output_format: "pil" or "numpy".
        channels_first: When numpy, output (C, H, W) if True else (H, W, C).
        timeout_s: HTTP timeout for downloads.
        max_concurrency: Semaphore limit for parallel processing.
        retries: Number of retry attempts on retriable errors (default 2).
        retry_backoff_base: Base seconds for exponential backoff.
        bypass_if_frames_present: Reserved for future use.
        pack_for_model: Reserved for future use.
        keep_downloaded: If using disk cache, keep cached file after processing.
        preprocess: PIL preprocessing dict {resize, crop, convert}.
        max_sampled_frames: Optional cap for number of sampled frames.
    """

    def __init__(
        self,
        *,
        sampling: Optional[Dict[str, Any]] = None,
        cache_dir: Optional[str] = None,
        cache_mode: str = "auto",
        output_format: str = "pil",
        channels_first: bool = False,
        timeout_s: float = 30.0,
        max_concurrency: int = 8,
        retries: int = 2,
        retry_backoff_base: float = 0.5,
        bypass_if_frames_present: bool = False,
        pack_for_model: bool = False,
        keep_downloaded: bool = False,
        preprocess: Optional[Dict[str, Any]] = None,
        max_sampled_frames: Optional[int] = None,
    ) -> None:
        sampling_cfg = Sampling(**(sampling or {}))
        if sampling_cfg.fps is None and sampling_cfg.num_frames is None:
            sampling_cfg.fps = 3.0
        self._sampling = sampling_cfg
        self._cache_dir = Path(cache_dir) if cache_dir else None
        self._cache_mode = cache_mode
        self._output_format = output_format
        self._channels_first = channels_first
        self._timeout_s = timeout_s
        self._retries = int(retries)
        self._retry_backoff_base = float(retry_backoff_base)
        self._bypass_if_frames_present = bypass_if_frames_present
        self._pack_for_model = pack_for_model
        self._keep_downloaded = keep_downloaded
        self._preprocess = preprocess or {}
        self._max_sampled_frames = (
            int(max_sampled_frames) if max_sampled_frames is not None else None
        )

        self._http = HTTPConnection()
        self._sem = asyncio.Semaphore(max_concurrency)

    async def process(self, sources: List[str]) -> List[Dict[str, Any]]:
        if not sources:
            return []
        tasks = [self._process_one_safe(src) for src in sources]
        return await asyncio.gather(*tasks)

    async def _process_one_safe(self, source: str) -> Dict[str, Any]:
        async with self._sem:
            attempt = 0
            backoff = self._retry_backoff_base
            while attempt <= self._retries:
                try:
                    return await asyncio.to_thread(self._process_one_sync, source)
                except Exception as e:
                    if not self._should_retry(e) or attempt == self._retries:
                        return {
                            "frames": [],
                            "meta": {
                                "failed": True,
                                "error_type": type(e).__name__,
                                "error": str(e),
                                "attempts": attempt + 1,
                                "retried": attempt > 0,
                                "source": str(source),
                                "video_num_frames": 0,
                                "frame_timestamps": [],
                            },
                        }
                    await asyncio.sleep(max(backoff, 0))
                    backoff *= 2
                    attempt += 1

    def _should_retry(self, e: Exception) -> bool:
        non_retriable = (ImportError, ValueError)
        return not isinstance(e, non_retriable)

    def _process_one_sync(self, source: str) -> Dict[str, Any]:
        if _av_mod is None:
            raise ImportError(
                "PyAV is required for VideoProcessor. Install with `pip install av`."
            )
        if _PIL_Image is None:
            raise ImportError(
                "Pillow is required for VideoProcessor. Install with `pip install pillow`."
            )

        resolved, is_memory, cleanup_path = self._resolve_source_for_decode(source)

        container = None
        try:
            if is_memory:
                try:
                    container = _av_mod.open(resolved)
                except Exception:
                    fmt_guess = self._guess_format_from_source(source) or "mp4"
                    container = _av_mod.open(resolved, format=fmt_guess)
            else:
                container = _av_mod.open(resolved)

            try:
                vstream = next(
                    s for s in container.streams if getattr(s, "type", None) == "video"
                )
            except StopIteration:
                raise ValueError("No video stream found in source")

            frames: List[FrameType] = []
            timestamps: List[float] = []
            allow_zero_samples = False

            s = self._sampling
            if s.num_frames is not None:
                n = max(int(s.num_frames), 1)
                if (
                    self._max_sampled_frames is not None
                    and self._max_sampled_frames >= 0
                ):
                    n = min(n, self._max_sampled_frames)

                decoded = 0
                for frame in container.decode(video=vstream.index):
                    decoded += 1
                    if getattr(frame, "pts", None) is None:
                        fps_guess = None
                        try:
                            fps_guess = (
                                float(getattr(vstream, "average_rate", 0)) or None
                            )
                        except Exception:
                            fps_guess = None
                        current_ts = (
                            len(timestamps) / fps_guess
                            if fps_guess
                            else float(len(timestamps))
                        )
                    else:
                        current_ts = float(frame.pts * vstream.time_base)
                    frames.append(self._format_frame(frame))
                    timestamps.append(current_ts)
                    if len(frames) >= n:
                        break
                    if decoded >= video_envs.RAY_VIDEO_EXAMPLE_MAX_DECODE_FRAMES:
                        break
            else:
                targets = self._build_targets(container, vstream)
                if (
                    self._max_sampled_frames is not None
                    and self._max_sampled_frames >= 0
                ):
                    targets = targets[: self._max_sampled_frames]

                if not targets:
                    allow_zero_samples = True
                else:
                    target_idx = 0
                    next_target = targets[target_idx]

                    decoded = 0
                    for frame in container.decode(video=vstream.index):
                        decoded += 1
                        if getattr(frame, "pts", None) is None:
                            current_ts = len(timestamps) / ((s.fps or 30.0))
                        else:
                            current_ts = float(frame.pts * vstream.time_base)

                        if current_ts + 1e-6 >= next_target:
                            frames.append(self._format_frame(frame))
                            timestamps.append(current_ts)
                            target_idx += 1
                            if target_idx >= len(targets):
                                break
                            next_target = targets[target_idx]

                        if decoded >= video_envs.RAY_VIDEO_EXAMPLE_MAX_DECODE_FRAMES:
                            break
        finally:
            exc_type, _, _ = sys.exc_info()
            close_error: Optional[Exception] = None

            try:
                if container is not None:
                    container.close()
            except Exception as e:
                close_error = RuntimeError(
                    f"Failed to close PyAV container for source {self._source_repr(source, resolved, is_memory)}: {e}"
                )
                if exc_type is None:
                    raise close_error from e

            cleanup_error: Optional[Exception] = None
            if cleanup_path is not None and not self._keep_downloaded:
                try:
                    os.remove(cleanup_path)
                except Exception as e:
                    cleanup_error = RuntimeError(
                        f"Failed to remove cached file at {cleanup_path}: {e}"
                    )
                    if exc_type is None:
                        raise cleanup_error from e
                    if close_error is None:
                        close_error = cleanup_error

            if exc_type is None and close_error is not None:
                raise close_error

        if not frames and not allow_zero_samples:
            raise ValueError("No frames sampled")

        w = h = None
        if frames:
            if self._output_format == "pil":
                try:
                    w, h = frames[0].width, frames[0].height
                except Exception:
                    w = h = None
            else:
                arr0 = frames[0]
                try:
                    shape = getattr(arr0, "shape", None)
                    if shape is None:
                        raise ValueError("invalid numpy frame")
                    if self._channels_first:
                        _, h, w = shape
                    else:
                        h, w, _ = shape
                except Exception:
                    w = h = None

        result = {
            "frames": frames,
            "meta": {
                "video_size": [w, h] if (w and h) else None,
                "video_num_frames": len(frames),
                "frame_timestamps": timestamps,
                "source": self._source_repr(source, resolved, is_memory),
                "failed": False,
            },
        }

        return result

    def _guess_format_from_source(self, source: str) -> Optional[str]:
        try:
            if _is_data_uri(source):
                header = source.split(",", 1)[0]
                if "video/" in header:
                    mime = header.split("video/")[1].split(";")[0].strip()
                    return {
                        "mp4": "mp4",
                        "webm": "webm",
                        "ogg": "ogg",
                        "quicktime": "mov",
                        "x-matroska": "matroska",
                    }.get(mime, None)
            parsed = urlparse(source)
            ext = os.path.splitext(parsed.path or source)[1].lower().lstrip(".")
            return {
                "mp4": "mp4",
                "m4v": "mp4",
                "mov": "mov",
                "webm": "webm",
                "mkv": "matroska",
                "ogg": "ogg",
            }.get(ext, None)
        except Exception:
            return None

    def _source_repr(self, original: str, resolved: Any, is_memory: bool) -> str:
        try:
            if is_memory:
                return f"memory://{len(resolved.getbuffer())}b"
            return str(resolved)
        except Exception:
            return str(original)

    def _build_targets(self, container: Any, vstream: Any) -> List[float]:
        duration_s: Optional[float] = None
        try:
            if getattr(container, "duration", None) is not None and _av_mod is not None:
                duration_s = float(container.duration * _av_mod.time_base)
        except Exception:
            duration_s = None

        if duration_s is None:
            try:
                if getattr(vstream, "duration", None) is not None:
                    duration_s = float(vstream.duration * float(vstream.time_base))
            except Exception:
                duration_s = None

        s = self._sampling
        targets: List[float] = []
        if s.fps is not None:
            if duration_s is None:
                limit = max(int(s.fps * 2), 1)
                limit = min(limit, video_envs.RAY_VIDEO_EXAMPLE_MAX_TARGETS)
                targets = [i / s.fps for i in range(limit)]
            else:
                n = int(max(duration_s, 0.0) * s.fps) + 1
                n = max(1, min(n, video_envs.RAY_VIDEO_EXAMPLE_MAX_TARGETS))
                targets = [i / s.fps for i in range(n)]
        return targets

    def _apply_preprocess_pil(self, img: Any) -> Any:
        if not self._preprocess:
            return img
        r = self._preprocess.get("resize")
        if r and isinstance(r, dict) and "size" in r:
            resample_name = r.get("resample", "BILINEAR")
            method = None
            try:
                method = (
                    getattr(_PIL_Image, resample_name, None) if _PIL_Image else None
                )
                if method is None and _PIL_Image is not None:
                    Resampling = getattr(_PIL_Image, "Resampling", None)
                    if Resampling is not None:
                        method = getattr(Resampling, resample_name, None)
            except Exception:
                method = None
            if method is None:
                method = 2
            img = img.resize(tuple(r["size"]), method)
        c = self._preprocess.get("crop")
        if c and isinstance(c, dict) and "box" in c:
            img = img.crop(tuple(c["box"]))
        conv = self._preprocess.get("convert")
        if isinstance(conv, str):
            img = img.convert(conv)
        return img

    def _format_frame(self, frame: Any) -> FrameType:
        if self._output_format == "pil":
            img = frame.to_image()
            img = self._apply_preprocess_pil(img)
            return img
        else:
            try:
                np = importlib.import_module("numpy")
            except Exception as e:
                raise ImportError(
                    "NumPy is required for numpy output_format. Install with `pip install numpy`."
                ) from e

            if self._preprocess:
                img = frame.to_image()
                img = self._apply_preprocess_pil(img)
                arr = np.array(img)
                if getattr(arr, "ndim", 0) < 2 or arr.size == 0:
                    raise ValueError(
                        "Failed to convert preprocessed PIL image to a valid numpy array"
                    )
            else:
                arr = frame.to_ndarray(format="rgb24")
                if not hasattr(arr, "shape"):
                    raise ValueError("invalid numpy frame")
                if getattr(arr, "ndim", 0) == 2:
                    arr = np.expand_dims(arr, -1)

            if self._channels_first:
                return arr.transpose(2, 0, 1)
            return arr

    def _resolve_source_for_decode(
        self, source: str
    ) -> Tuple[Union[str, io.BytesIO], bool, Optional[str]]:
        """Return (resolved, is_memory, cleanup_path).

        cache_mode:
          - "auto": download to disk if uniform sampling (num_frames) likely needs seek; otherwise stream.
          - "disk": always download to disk when http/https.
          - "memory": fetch into BytesIO when http/https or data URI.
        """
        if _is_data_uri(source):
            try:
                header, b64 = source.split(",", 1)
                raw = base64.b64decode(b64)
                return io.BytesIO(raw), True, None
            except Exception as e:
                raise ValueError(f"Invalid data URI: {e}") from e

        parsed = urlparse(source)
        if parsed.scheme in ("file", "") and os.path.exists(parsed.path or source):
            return (parsed.path or source), False, None

        if _is_http(source):
            use_disk = self._cache_dir is not None and (
                self._cache_mode == "disk"
                or (
                    self._cache_mode == "auto" and self._sampling.num_frames is not None
                )
            )
            use_memory = self._cache_mode == "memory"

            if use_memory:
                data = self._http.download_bytes_chunked(
                    source, timeout=self._timeout_s
                )
                return io.BytesIO(data), True, None

            if use_disk:
                self._cache_dir.mkdir(parents=True, exist_ok=True)
                fname = f"video-{_sha256_16(source)}.bin"
                tmp = self._cache_dir / f".{fname}.tmp"
                final = self._cache_dir / fname
                self._http.download_file(source, tmp, timeout=self._timeout_s)
                os.replace(tmp, final)
                return (
                    str(final),
                    False,
                    (None if self._keep_downloaded else str(final)),
                )

            return source, False, None

        return source, False, None
